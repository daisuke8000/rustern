use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use chrono::{DateTime, SecondsFormat, TimeDelta, Utc};
use jiff::Timestamp;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

use super::mux::MuxCmd;
use super::watch_ctx::PodWatchCtx;
use crate::source::pod_log::{PodLogRequest, PodLogSource};
use crate::source::{
    BoxedLogStream, ContextName, Labels, LogEvent, LogSource, LogSourceError, SourceKey,
    SourceKind, SourceMeta,
};

const CURSOR_RECONNECT_OVERLAP: TimeDelta = TimeDelta::seconds(1);
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

fn source_meta_for_key(context_name: &ContextName, key: &SourceKey) -> SourceMeta {
    SourceMeta {
        context: context_name.clone(),
        namespace: key.namespace.clone(),
        pod: key.pod.clone(),
        container: key.container.clone(),
        kind: SourceKind::PodLog,
        node: None,
        labels: Arc::new(Labels::default()),
        uid: key.uid.clone(),
    }
}

fn overlap_since_time(last_timestamp: DateTime<Utc>) -> Option<Timestamp> {
    last_timestamp
        .checked_sub_signed(CURSOR_RECONNECT_OVERLAP)
        .unwrap_or(last_timestamp)
        .to_rfc3339_opts(SecondsFormat::Nanos, true)
        .parse()
        .ok()
}

fn pod_log_request_for_open(
    base: &PodLogRequest,
    last_timestamp: Option<DateTime<Utc>>,
    reopen: bool,
) -> PodLogRequest {
    if !reopen {
        return base.clone();
    }

    let mut request = base.clone();
    if let Some(since_time) = last_timestamp.and_then(overlap_since_time) {
        request.tail = None;
        request.since_seconds = None;
        request.since_time = Some(since_time);
    }
    request
}

struct CursorTrackingStream {
    inner: BoxedLogStream,
    key: SourceKey,
    pod_token: CancellationToken,
    reconnect_cursor: Arc<std::sync::Mutex<HashMap<SourceKey, DateTime<Utc>>>>,
    done_tx: Option<oneshot::Sender<StreamEnd>>,
}

impl CursorTrackingStream {
    fn new(
        inner: BoxedLogStream,
        key: SourceKey,
        pod_token: CancellationToken,
        reconnect_cursor: Arc<std::sync::Mutex<HashMap<SourceKey, DateTime<Utc>>>>,
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
                if let Ok(mut cursor) = self.reconnect_cursor.lock() {
                    cursor.insert(self.key.clone(), event.timestamp);
                }
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
            let last_timestamp = p
                .ctx
                .reconnect_cursor
                .lock()
                .ok()
                .and_then(|cursor| cursor.get(&p.key).copied());
            pod_log_request_for_open(&p.ctx.pod_log, last_timestamp, reopen)
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
                    Arc::clone(&p.ctx.reconnect_cursor),
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
    let meta = source_meta_for_key(&ctx.context_name, &key);
    tokio::spawn(attach_pod_log_stream(AttachPodLogParams {
        ctx: Arc::clone(ctx),
        meta,
        pod_token,
        key,
    }));
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Mutex;

    use chrono::{TimeZone, Utc};
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

    #[test]
    fn reopen_without_cursor_keeps_initial_tail_and_since() {
        let base = PodLogRequest {
            follow: true,
            tail: Some(25),
            since_seconds: Some(300),
            ..Default::default()
        };
        let req = pod_log_request_for_open(&base, None, true);

        assert_eq!(req.tail, Some(25));
        assert_eq!(req.since_seconds, Some(300));
        assert!(req.since_time.is_none());
    }

    #[test]
    fn reconnect_request_uses_overlap_and_drops_tail_and_since() {
        let ts = Utc.with_ymd_and_hms(2026, 4, 28, 8, 0, 5).unwrap();
        let req = pod_log_request_for_open(
            &PodLogRequest {
                follow: true,
                tail: Some(25),
                since_seconds: Some(300),
                ..Default::default()
            },
            Some(ts),
            true,
        );

        assert!(req.follow);
        assert!(req.tail.is_none());
        assert!(req.since_seconds.is_none());
        let overlap_ts = req.since_time.unwrap().to_string();
        assert!(overlap_ts.contains("2026-04-28T08:00:04"));
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
            reconnect_cursor: Arc::new(Mutex::new(HashMap::new())),
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
