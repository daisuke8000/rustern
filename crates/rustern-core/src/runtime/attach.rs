use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

use super::cursor_store::{ReconnectCursorStore, pod_log_request_for_reopen};
use super::mux::MuxCmd;
use super::pod_meta_cache::lookup_pod_meta;
use super::watch_ctx::PodWatchCtx;
use crate::source::pod_log::PodLogSource;
use crate::source::{
    BoxedLogStream, LogEvent, LogSource, LogSourceError, SourceKey, SourceKind, SourceMeta,
};

const MAX_REOPEN_START_RETRIES: u32 = 5;

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum StreamEnd {
    Eof,
    Cancelled,
}

struct AttachPodLogParams {
    ctx: Arc<PodWatchCtx>,
    meta: SourceMeta,
    pod_token: CancellationToken,
    key: SourceKey,
}

async fn source_meta_for_key(ctx: &PodWatchCtx, key: &SourceKey) -> SourceMeta {
    let snap = lookup_pod_meta(ctx, key).await;
    SourceMeta {
        context: ctx.context_name.clone(),
        namespace: key.namespace.clone(),
        pod: key.pod.clone(),
        container: key.container.clone(),
        kind: SourceKind::PodLog,
        node: snap.node,
        labels: Arc::new(snap.labels),
        uid: key.uid.clone(),
    }
}

struct CursorTrackingStream {
    inner: BoxedLogStream,
    key: SourceKey,
    pod_token: CancellationToken,
    reconnect_cursor: ReconnectCursorStore,
    done_tx: Option<oneshot::Sender<StreamEnd>>,
}

impl CursorTrackingStream {
    fn new(
        inner: BoxedLogStream,
        key: SourceKey,
        pod_token: CancellationToken,
        reconnect_cursor: ReconnectCursorStore,
        done_tx: oneshot::Sender<StreamEnd>,
    ) -> Self {
        Self {
            inner,
            key,
            pod_token,
            reconnect_cursor,
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
                self.reconnect_cursor.record(&self.key, event.timestamp);
                Poll::Ready(Some(Ok(event)))
            }
            Poll::Ready(Some(item)) => Poll::Ready(Some(item)),
            Poll::Ready(None) => {
                let reason = if self.pod_token.is_cancelled() {
                    StreamEnd::Cancelled
                } else {
                    StreamEnd::Eof
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
            StreamEnd::Eof
        };
        self.finish(reason);
    }
}

async fn attach_pod_log_stream(p: AttachPodLogParams) {
    let mut reopen = false;
    let mut reopen_start_failures = 0u32;

    loop {
        if p.pod_token.is_cancelled() || p.ctx.root_child.is_cancelled() {
            return;
        }

        let request = {
            let last_timestamp = p.ctx.reconnect_cursor.get(&p.key);
            pod_log_request_for_reopen(&p.ctx.pod_log, last_timestamp, reopen)
        };

        let permit = if p.ctx.pod_log.follow {
            match Arc::clone(&p.ctx.sem).try_acquire_owned() {
                Ok(permit) => permit,
                Err(_) => {
                    if let Some(tx) = &p.ctx.follow_limit_notifier {
                        let _ = tx.try_send(());
                    }
                    p.ctx.root_child.cancel();
                    return;
                }
            }
        } else {
            match Arc::clone(&p.ctx.sem).acquire_owned().await {
                Ok(permit) => permit,
                Err(_) => return,
            }
        };

        let client = p.ctx.client.clone();
        match PodLogSource::start(client, p.meta.clone(), p.pod_token.clone(), request).await {
            Ok(src) => {
                reopen_start_failures = 0;
                let (done_tx, done_rx) = oneshot::channel();
                let stream = Box::new(CursorTrackingStream::new(
                    Box::new(src).into_stream(),
                    p.key.clone(),
                    p.pod_token.clone(),
                    p.ctx.reconnect_cursor.clone(),
                    done_tx,
                ));
                if p.ctx
                    .mux_tx
                    .send(MuxCmd::Add(p.key.clone(), Box::pin(stream)))
                    .await
                    .is_err()
                {
                    drop(permit);
                    return;
                }
                drop(permit);

                let should_reconnect = p.ctx.cursor_reconnect && p.ctx.pod_log.follow;
                match done_rx.await {
                    Ok(StreamEnd::Eof) if should_reconnect => {
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
                    && p.ctx.cursor_reconnect
                    && p.ctx.pod_log.follow
                    && !p.pod_token.is_cancelled()
                    && !p.ctx.root_child.is_cancelled()
                    && reopen_start_failures < MAX_REOPEN_START_RETRIES
                {
                    reopen_start_failures += 1;
                    tokio::time::sleep(std::time::Duration::from_millis(250)).await;
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

pub(crate) fn spawn_attach_pod_log(
    ctx: &Arc<PodWatchCtx>,
    key: SourceKey,
    pod_token: CancellationToken,
) {
    let ctx = Arc::clone(ctx);
    tokio::spawn(async move {
        let meta = source_meta_for_key(ctx.as_ref(), &key).await;
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

    use crate::runtime::cursor_store::ReconnectCursorStore;
    use crate::runtime::pod_meta_cache::{new_pod_meta_cache, update_pod_meta_cache};
    use crate::source::ContextName;
    use crate::source::pod_log::PodLogRequest;

    use futures::StreamExt;
    use http::{Request, Response, StatusCode};
    use kube::Client;
    use tokio::sync::{Semaphore, mpsc};

    use crate::discovery::pod_watcher::{
        ContainerDiscoverOpts, ContainerLifecycleBucket, ContainerStatePolicy,
    };

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
    async fn source_meta_for_key_uses_cached_pod_labels_and_node() {
        let cache = new_pod_meta_cache();
        let (mux_tx, _) = mpsc::channel(1);
        let (mock, _handle) =
            tower_test::mock::pair::<Request<kube::client::Body>, Response<kube::client::Body>>();
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
        let ctx = PodWatchCtx {
            context_name: ContextName("ctx".into()),
            pod_regex: None,
            pod_condition: None,
            container_discovery: ContainerDiscoverOpts::default(),
            container_incl: regex::Regex::new(".*").unwrap(),
            container_excl: vec![],
            allowed_ns: None,
            exclude_pod: vec![],
            mux_tx,
            client: Client::new(mock, "default"),
            root_child: CancellationToken::new(),
            pod_log: PodLogRequest::default(),
            cursor_reconnect: false,
            reconnect_cursor: ReconnectCursorStore::new(),
            sem: Arc::new(Semaphore::new(1)),
            follow_limit_notifier: None,
            pod_meta: cache,
        };
        update_pod_meta_cache(&ctx, &pod).await;

        let meta = source_meta_for_key(&ctx, &sample_key()).await;
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

        let ctx = Arc::new(PodWatchCtx {
            context_name: ContextName("ctx".into()),
            pod_regex: None,
            pod_condition: None,
            container_discovery: ContainerDiscoverOpts {
                include_init_containers: true,
                include_ephemeral_containers: true,
                state_policy: ContainerStatePolicy::Subset(
                    [ContainerLifecycleBucket::Running].into_iter().collect(),
                ),
            },
            container_incl: regex::Regex::new(".*").unwrap(),
            container_excl: Vec::new(),
            allowed_ns: None,
            exclude_pod: Vec::new(),
            mux_tx,
            client,
            root_child: CancellationToken::new(),
            pod_log: PodLogRequest {
                follow: true,
                tail: Some(25),
                since_seconds: Some(300),
                ..Default::default()
            },
            sem: Arc::new(Semaphore::new(8)),
            follow_limit_notifier: None,
            cursor_reconnect: true,
            reconnect_cursor: ReconnectCursorStore::new(),
            pod_meta: new_pod_meta_cache(),
        });

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
}
