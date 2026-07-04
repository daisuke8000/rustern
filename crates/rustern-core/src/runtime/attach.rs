//! Pod log stream attach and cursor tracking.
//!
//! The attach semaphore limits how many log streams may *start* concurrently.
//! That cap is independent of mux/forward backpressure policies, which govern
//! behaviour when internal channels are full after a stream is running.

use chrono::{DateTime, Utc};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use tokio::sync::{Semaphore, mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use super::cursor_store::{CursorUpdate, ReconnectCursorStore, pod_log_request_for_reopen};
use super::mux::MuxCmd;
use super::pod_meta_cache::PodMetaCache;
use super::watch_ctx::PodWatchCtx;
use crate::source::ContextName;
use crate::source::retry::full_jitter_backoff;
use crate::source::{BoxedLogStream, LogEvent, LogSourceError, SourceKey, SourceKind, SourceMeta};

const MAX_REOPEN_START_RETRIES: u32 = 5;
const CURSOR_FLUSH_RETRIES: usize = 16;

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum StreamEnd {
    Eof { last_line_ts: Option<DateTime<Utc>> },
    Cancelled,
}

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
) -> Arc<SourceMeta> {
    let snap = cache.lookup(key).await;
    Arc::new(SourceMeta {
        context: context.clone(),
        namespace: key.namespace.clone(),
        pod: key.pod.clone(),
        container: key.container.clone(),
        kind: SourceKind::PodLog,
        node: snap.node,
        labels: Arc::new(snap.labels),
        uid: key.uid.clone(),
    })
}

/// Wraps a pod log stream and forwards cursor advances through an unbounded channel.
///
/// Timestamp recording never runs inside `poll_next`; see [`super::cursor_store`].
struct CursorTrackingStream {
    inner: BoxedLogStream,
    key: SourceKey,
    pod_token: CancellationToken,
    cursor_tx: mpsc::UnboundedSender<CursorUpdate>,
    last_line_ts: Option<DateTime<Utc>>,
    done_tx: Option<oneshot::Sender<StreamEnd>>,
}

impl CursorTrackingStream {
    fn new(
        inner: BoxedLogStream,
        key: SourceKey,
        pod_token: CancellationToken,
        cursor_tx: mpsc::UnboundedSender<CursorUpdate>,
        done_tx: oneshot::Sender<StreamEnd>,
    ) -> Self {
        Self {
            inner,
            key,
            pod_token,
            cursor_tx,
            last_line_ts: None,
            done_tx: Some(done_tx),
        }
    }

    fn finish(&mut self, reason: StreamEnd) {
        if let Some(done_tx) = self.done_tx.take() {
            let _ = done_tx.send(reason);
        }
    }
}

impl futures::Stream for CursorTrackingStream {
    type Item = Result<LogEvent, LogSourceError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.inner.as_mut().poll_next(cx) {
            Poll::Ready(Some(Ok(event))) => {
                self.last_line_ts = Some(event.timestamp);
                let _ = self.cursor_tx.send(CursorUpdate {
                    key: self.key.clone(),
                    timestamp: event.timestamp,
                });
                Poll::Ready(Some(Ok(event)))
            }
            Poll::Ready(Some(item)) => Poll::Ready(Some(item)),
            Poll::Ready(None) => {
                let reason = if self.pod_token.is_cancelled() {
                    StreamEnd::Cancelled
                } else {
                    StreamEnd::Eof {
                        last_line_ts: self.last_line_ts,
                    }
                };
                self.finish(reason);
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl Drop for CursorTrackingStream {
    fn drop(&mut self) {
        let reason = if self.pod_token.is_cancelled() {
            StreamEnd::Cancelled
        } else {
            StreamEnd::Eof {
                last_line_ts: self.last_line_ts,
            }
        };
        self.finish(reason);
    }
}

async fn wait_cursor_flushed(
    store: &ReconnectCursorStore,
    key: &SourceKey,
    expected: Option<DateTime<Utc>>,
) {
    let Some(expected) = expected else {
        return;
    };
    for _ in 0..CURSOR_FLUSH_RETRIES {
        if store.get(key).await.is_some_and(|got| got >= expected) {
            return;
        }
        tokio::task::yield_now().await;
    }
    tracing::warn!(
        ?key,
        ?expected,
        retries = CURSOR_FLUSH_RETRIES,
        "cursor flush not confirmed before reconnect"
    );
}

async fn attach_pod_log_stream(p: AttachPodLogParams) {
    let mut reopen = false;
    let mut reopen_start_failures = 0u32;

    loop {
        if p.pod_token.is_cancelled() || p.ctx.attach.root_child.is_cancelled() {
            return;
        }

        let request = {
            let last_timestamp = p.ctx.attach.reconnect_cursor.get(&p.key).await;
            pod_log_request_for_reopen(&p.ctx.attach.pod_log, last_timestamp, reopen)
        };

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
                let (done_tx, done_rx) = oneshot::channel();
                let stream = Box::new(CursorTrackingStream::new(
                    src.into_stream(),
                    p.key.clone(),
                    p.pod_token.clone(),
                    p.ctx.attach.cursor_update_tx.clone(),
                    done_tx,
                ));
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

                let should_reconnect = p.ctx.attach.cursor_reconnect && p.ctx.attach.pod_log.follow;
                match done_rx.await {
                    Ok(StreamEnd::Eof { last_line_ts }) if should_reconnect => {
                        wait_cursor_flushed(&p.ctx.attach.reconnect_cursor, &p.key, last_line_ts)
                            .await;
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
                    && p.ctx.attach.cursor_reconnect
                    && p.ctx.attach.pod_log.follow
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
        let meta =
            source_meta_for_key(&ctx.admission.context_name(), &ctx.attach.pod_meta, &key).await;
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
    use crate::source::pod_log::PodLogRequest;

    use futures::StreamExt;
    use http::{Request, Response, StatusCode};
    use kube::Client;
    use tokio::sync::mpsc;

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
        let meta = source_meta_for_key(&ContextName("ctx".into()), &cache, &key).await;
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
        )
        .await;
        assert_eq!(meta.node.as_deref(), Some("worker-1"));
        assert_eq!(meta.labels.0.get("app").map(String::as_str), Some("api"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn reconnects_follow_stream_with_cursor_since_time() {
        let (mock, mut handle) =
            tower_test::mock::pair::<Request<kube::client::Body>, Response<kube::client::Body>>();
        let client = Client::new(mock, "default");
        let (mux_tx, mut mux_rx) = mpsc::channel(8);
        let pod_token = CancellationToken::new();

        let base_fixture = TestOrchestratorBuilder::new()
            .mux_tx(mux_tx)
            .pod_log(PodLogRequest {
                follow: true,
                tail: Some(25),
                since_seconds: Some(300),
                ..Default::default()
            })
            .cursor_reconnect(true)
            .sem_permits(8)
            .log_opener(Arc::new(
                crate::source::log_opener::PodLogSourceOpener::new(client),
            ))
            .build();
        let ctx = base_fixture.arc();

        let (second_req_tx, second_req_rx) = oneshot::channel();
        let (second_resp_tx, second_resp_rx) = oneshot::channel();
        let server = tokio::spawn(async move {
            let (req1, send1) = handle.next_request().await.expect("first request");
            let q1 = req1.uri().query().unwrap_or("");
            assert!(q1.contains("tailLines=25"));
            assert!(q1.contains("sinceSeconds=300"));
            assert!(!q1.contains("sinceTime="));
            let resp1 = Response::builder()
                .status(StatusCode::OK)
                .body(kube::client::Body::from(
                    b"2026-04-28T08:00:05Z first\n".to_vec(),
                ))
                .unwrap();
            send1.send_response(resp1);

            let (req2, send2) = handle.next_request().await.expect("second request");
            let _ = second_req_tx.send(());
            let q2 = req2.uri().query().unwrap_or("");
            assert!(q2.contains("sinceTime="));
            assert!(
                q2.contains("08%3A00%3A04") || q2.contains("08:00:04"),
                "unexpected reconnect query: {q2}"
            );
            assert!(!q2.contains("tailLines="));
            assert!(!q2.contains("sinceSeconds="));
            let resp2 = Response::builder()
                .status(StatusCode::OK)
                .body(kube::client::Body::from(
                    b"2026-04-28T08:00:06Z second\n".to_vec(),
                ))
                .unwrap();
            send2.send_response(resp2);
            let _ = second_resp_tx.send(());
        });

        spawn_attach_pod_log(&ctx, sample_key(), pod_token.clone());

        let Some(MuxCmd::Add(_, first_stream)) = mux_rx.recv().await else {
            panic!("missing first attach");
        };
        let first_events: Vec<_> = first_stream.collect().await;
        assert_eq!(first_events.len(), 1);
        assert_eq!(&*first_events[0].as_ref().unwrap().message, "first");

        let Some(MuxCmd::Add(_, mut second_stream)) = mux_rx.recv().await else {
            panic!("missing reconnect attach");
        };
        second_req_rx.await.expect("second mock request");
        second_resp_rx.await.expect("second mock response");
        let second_ev = second_stream
            .next()
            .await
            .expect("second stream ended without events")
            .expect("second stream error");
        assert_eq!(&*second_ev.message, "second");

        pod_token.cancel();
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(100), mux_rx.recv())
                .await
                .is_err()
        );

        server.await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn reconnects_follow_with_script_opener_without_kube_mock() {
        use chrono::{TimeZone, Utc};

        use crate::source::log_opener::ScriptLogSourceOpener;

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
            .cursor_reconnect(true)
            .sem_permits(8)
            .log_opener(ScriptLogSourceOpener::new(vec![
                vec![Ok(ev1)],
                vec![Ok(ev2)],
            ]))
            .build();
        let ctx = fixture.arc();

        spawn_attach_pod_log(&ctx, key, pod_token.clone());

        let Some(MuxCmd::Add(_, first_stream)) = mux_rx.recv().await else {
            panic!("missing first attach");
        };
        let first_events: Vec<_> = first_stream.collect().await;
        assert_eq!(first_events.len(), 1);
        assert_eq!(&*first_events[0].as_ref().unwrap().message, "first");

        let Some(MuxCmd::Add(_, mut second_stream)) = mux_rx.recv().await else {
            panic!("missing reconnect attach");
        };
        let second_ev = second_stream
            .next()
            .await
            .expect("second stream ended without events")
            .expect("second stream error");
        assert_eq!(&*second_ev.message, "second");

        pod_token.cancel();
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(100), mux_rx.recv())
                .await
                .is_err()
        );
    }
}
