//! Pod log stream attach and cursor tracking.
//!
//! The attach semaphore limits how many log streams may *start* concurrently.
//! That cap is independent of mux/forward backpressure policies, which govern
//! behaviour when internal channels are full after a stream is running.

use chrono::DateTime;
use std::sync::Arc;

use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

use super::cursor_service::StreamEnd;
use super::mux::MuxCmd;
use super::pod_meta_cache::PodMetaCache;
use super::watch_ctx::PodWatchCtx;
use crate::pipeline::{ColorAssignOpts, apply_palette_to_meta};
use crate::source::ContextName;
use crate::source::retry::full_jitter_backoff;
use crate::source::{SourceKey, SourceKind, SourceMeta};

const MAX_REOPEN_START_RETRIES: u32 = 5;

struct AttachPodLogParams {
    ctx: Arc<PodWatchCtx>,
    meta: Arc<SourceMeta>,
    pod_token: CancellationToken,
    key: SourceKey,
}

async fn source_meta_for_key(
    context: &ContextName,
    cache: &PodMetaCache,
    key: &SourceKey,
    color_opts: ColorAssignOpts,
) -> Arc<SourceMeta> {
    let snap = cache.lookup(key).await;
    let mut meta = SourceMeta {
        context: context.clone(),
        namespace: key.namespace.clone(),
        pod: key.pod.clone(),
        container: key.container.clone(),
        kind: SourceKind::PodLog,
        node: snap.node,
        labels: Arc::new(snap.labels),
        uid: key.uid.clone(),
        palette_index: None,
        container_palette_index: None,
    };
    apply_palette_to_meta(&mut meta, color_opts);
    Arc::new(meta)
}

async fn attach_pod_log_stream(p: AttachPodLogParams) {
    let mut reopen = false;
    let mut reopen_start_failures = 0u32;
    let mut flush_target: Option<DateTime<chrono::Utc>> = None;

    loop {
        if p.pod_token.is_cancelled() || p.ctx.attach.root_child.is_cancelled() {
            return;
        }

        let request = p
            .ctx
            .attach
            .cursor
            .reopen_request(&p.key, &p.ctx.attach.pod_log, reopen, flush_target.take())
            .await;

        let permit = if p.ctx.attach.pod_log.follow {
            match Arc::clone(&p.ctx.attach.sem).try_acquire_owned() {
                Ok(permit) => permit,
                Err(_) => {
                    if let Some(tx) = &p.ctx.attach.follow_limit_notifier {
                        let _ = tx.try_send(());
                    }
                    p.ctx.attach.root_child.cancel();
                    return;
                }
            }
        } else {
            match Arc::clone(&p.ctx.attach.sem).acquire_owned().await {
                Ok(permit) => permit,
                Err(_) => return,
            }
        };

        match p
            .ctx
            .attach
            .log_opener
            .open(Arc::clone(&p.meta), p.pod_token.clone(), request)
            .await
        {
            Ok(src) => {
                reopen_start_failures = 0;
                let (stream, done_rx) = p.ctx.attach.cursor.track(
                    p.key.clone(),
                    p.pod_token.clone(),
                    src.into_stream(),
                );
                if p.ctx
                    .attach
                    .mux_tx
                    .send(MuxCmd::Add(p.key.clone(), Box::pin(stream)))
                    .await
                    .is_err()
                {
                    drop(permit);
                    return;
                }
                drop(permit);

                match done_rx.await {
                    Ok(StreamEnd::Eof { last_line_ts })
                        if p.ctx.attach.cursor.should_reconnect() =>
                    {
                        flush_target = last_line_ts;
                        reopen = true;
                        continue;
                    }
                    _ => return,
                }
            }
            Err(e) => {
                tracing::warn!(?e, "pod log start");
                drop(permit);
                if reopen
                    && p.ctx.attach.cursor.should_reconnect()
                    && !p.pod_token.is_cancelled()
                    && !p.ctx.attach.root_child.is_cancelled()
                    && reopen_start_failures < MAX_REOPEN_START_RETRIES
                {
                    reopen_start_failures += 1;
                    let delay = full_jitter_backoff(250, reopen_start_failures - 1);
                    tokio::select! {
                        _ = tokio::time::sleep(delay) => {}
                        _ = p.pod_token.cancelled() => return,
                        _ = p.ctx.attach.root_child.cancelled() => return,
                    }
                    continue;
                }
                if reopen_start_failures >= MAX_REOPEN_START_RETRIES {
                    tracing::warn!(
                        retries = MAX_REOPEN_START_RETRIES,
                        "cursor reconnect start retries exhausted"
                    );
                }
                return;
            }
        }
    }
}

pub fn build_log_request_semaphore(max: usize) -> Arc<Semaphore> {
    Arc::new(Semaphore::new(max.max(1)))
}

pub(crate) fn spawn_attach_pod_log(
    ctx: &Arc<PodWatchCtx>,
    key: SourceKey,
    pod_token: CancellationToken,
) {
    let ctx = Arc::clone(ctx);
    tokio::spawn(async move {
        let meta = source_meta_for_key(
            &ctx.admission.context_name(),
            &ctx.attach.pod_meta,
            &key,
            ctx.attach.color_assign,
        )
        .await;
        attach_pod_log_stream(AttachPodLogParams {
            ctx,
            meta,
            pod_token,
            key,
        })
        .await;
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;

    use crate::runtime::test_support::TestOrchestratorBuilder;
    use crate::source::ContextName;

    fn sample_key() -> SourceKey {
        SourceKey {
            context: ContextName("ctx".into()),
            namespace: "ns".into(),
            pod: "pod-a".into(),
            container: "app".into(),
            uid: "uid-1".into(),
        }
    }

    #[tokio::test]
    async fn source_meta_for_key_without_kube_mock() {
        use crate::source::Labels;
        use crate::source::pod_meta::{PodLocator, PodMetaSnapshot};

        let key = sample_key();
        let mut labels = BTreeMap::new();
        labels.insert("app".into(), "api".into());
        let cache = PodMetaCache::with_entry(
            PodLocator::from_source_key(&key),
            PodMetaSnapshot {
                node: Some("worker-1".into()),
                labels: Labels(labels),
            },
        );
        let meta = source_meta_for_key(
            &ContextName("ctx".into()),
            &cache,
            &key,
            ColorAssignOpts::default(),
        )
        .await;
        assert_eq!(meta.node.as_deref(), Some("worker-1"));
        assert_eq!(meta.labels.0.get("app").map(String::as_str), Some("api"));
    }

    #[tokio::test]
    async fn source_meta_for_key_uses_cached_pod_labels_and_node() {
        let mut labels = BTreeMap::new();
        labels.insert("app".into(), "api".into());
        let pod = k8s_openapi::api::core::v1::Pod {
            metadata: ObjectMeta {
                name: Some("pod-a".into()),
                namespace: Some("ns".into()),
                uid: Some("uid-1".into()),
                labels: Some(labels),
                ..Default::default()
            },
            spec: Some(k8s_openapi::api::core::v1::PodSpec {
                node_name: Some("worker-1".into()),
                ..Default::default()
            }),
            ..Default::default()
        };
        let fixture = TestOrchestratorBuilder::new().build();
        fixture
            .attach
            .pod_meta
            .update_from_pod(&fixture.admission.context_name(), &pod)
            .await;

        let meta = source_meta_for_key(
            &fixture.admission.context_name(),
            &fixture.attach.pod_meta,
            &sample_key(),
            fixture.attach.color_assign,
        )
        .await;
        assert_eq!(meta.node.as_deref(), Some("worker-1"));
        assert_eq!(meta.labels.0.get("app").map(String::as_str), Some("api"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn follow_without_cursor_reconnect_skips_tracking_and_reopen() {
        use std::sync::Arc;

        use chrono::{TimeZone, Utc};
        use futures::StreamExt;
        use tokio::sync::mpsc;

        use crate::source::log_opener::ScriptLogSourceOpener;
        use crate::source::pod_log::PodLogRequest;
        use crate::source::{LogEvent, SourceKind, SourceMeta};

        let key = sample_key();
        let base_meta = SourceMeta {
            context: key.context.clone(),
            namespace: key.namespace.clone(),
            pod: key.pod.clone(),
            container: key.container.clone(),
            kind: SourceKind::PodLog,
            node: None,
            labels: Arc::new(crate::source::Labels::default()),
            uid: key.uid.clone(),
            palette_index: None,
            container_palette_index: None,
        };
        let ts1 = Utc.with_ymd_and_hms(2026, 4, 28, 8, 0, 5).unwrap();
        let ts2 = Utc.with_ymd_and_hms(2026, 4, 28, 8, 0, 6).unwrap();
        let ev1 = LogEvent {
            source: Arc::new(base_meta.clone()),
            timestamp: ts1,
            message: Arc::from("first"),
            structured: None,
            level: None,
            palette_index: None,
            container_palette_index: None,
        };
        let ev2 = LogEvent {
            source: Arc::new(base_meta),
            timestamp: ts2,
            message: Arc::from("second"),
            structured: None,
            level: None,
            palette_index: None,
            container_palette_index: None,
        };

        let (mux_tx, mut mux_rx) = mpsc::channel(8);
        let pod_token = CancellationToken::new();
        let fixture = TestOrchestratorBuilder::new()
            .mux_tx(mux_tx)
            .pod_log(PodLogRequest {
                follow: true,
                ..Default::default()
            })
            .cursor_reconnect(false)
            .sem_permits(8)
            .log_opener(ScriptLogSourceOpener::new(vec![
                vec![Ok(ev1)],
                vec![Ok(ev2)],
            ]))
            .build();
        let ctx = fixture.arc();

        spawn_attach_pod_log(&ctx, key.clone(), pod_token.clone());

        let Some(MuxCmd::Add(_, first_stream)) = mux_rx.recv().await else {
            panic!("missing first attach");
        };
        let first_events: Vec<_> = first_stream.collect().await;
        assert_eq!(first_events.len(), 1);
        assert_eq!(&*first_events[0].as_ref().unwrap().message, "first");
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(100), mux_rx.recv())
                .await
                .is_err(),
            "cursor reconnect disabled: must not reopen after EOF"
        );
        assert!(
            ctx.attach.cursor.last_timestamp(&key).await.is_none(),
            "cursor store must stay empty without tracking"
        );

        pod_token.cancel();
    }
}
