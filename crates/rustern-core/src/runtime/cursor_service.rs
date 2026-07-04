//! Cursor reconnect wiring: store, async processor, stream tracking, flush, reopen.
//!
//! [`ReconnectCursorStore`] internals stay in [`super::cursor_store`]; this module
//! hides attach/run/test wiring behind [`CursorService`].

use std::pin::Pin;
use std::task::{Context, Poll};

use chrono::{DateTime, Utc};
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use super::cursor_store::{
    CursorUpdate, ReconnectCursorStore, pod_log_request_for_reopen, run_cursor_update_processor,
};
use crate::source::pod_log::PodLogRequest;
use crate::source::{BoxedLogStream, LogEvent, LogSourceError, SourceKey};

const CURSOR_FLUSH_RETRIES: usize = 16;

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(crate) enum StreamEnd {
    Eof { last_line_ts: Option<DateTime<Utc>> },
    Cancelled,
}

/// Hides cursor store, channel processor, stream tracking, flush, and reopen request build.
#[derive(Clone)]
pub(crate) struct CursorService {
    store: ReconnectCursorStore,
    update_tx: mpsc::UnboundedSender<CursorUpdate>,
    enabled: bool,
}

impl CursorService {
    pub(crate) fn spawn(
        enabled: bool,
        token: CancellationToken,
    ) -> (Self, Option<tokio::task::JoinHandle<()>>) {
        let store = ReconnectCursorStore::new();
        let (update_tx, update_rx) = mpsc::unbounded_channel();
        let handle = if enabled {
            Some(tokio::spawn(run_cursor_update_processor(
                update_rx,
                store.clone(),
                token,
            )))
        } else {
            drop(update_rx);
            None
        };
        (
            Self {
                store,
                update_tx,
                enabled,
            },
            handle,
        )
    }

    #[cfg(test)]
    pub(crate) async fn last_timestamp(&self, key: &SourceKey) -> Option<DateTime<Utc>> {
        self.store.get(key).await
    }

    pub(crate) fn should_reconnect(&self) -> bool {
        self.enabled
    }

    pub(crate) fn track(
        &self,
        key: SourceKey,
        pod_token: CancellationToken,
        stream: BoxedLogStream,
    ) -> (BoxedLogStream, oneshot::Receiver<StreamEnd>) {
        let (done_tx, done_rx) = oneshot::channel();
        if !self.enabled {
            let wrapped = Box::pin(EndNotifyingStream::new(stream, pod_token, done_tx));
            return (wrapped, done_rx);
        }
        let wrapped = Box::pin(CursorTrackingStream::new(
            stream,
            key,
            pod_token,
            self.update_tx.clone(),
            done_tx,
        ));
        (wrapped, done_rx)
    }

    pub(crate) async fn reopen_request(
        &self,
        key: &SourceKey,
        base: &PodLogRequest,
        reopen: bool,
        flush_target: Option<DateTime<Utc>>,
    ) -> PodLogRequest {
        if reopen && self.enabled {
            wait_cursor_flushed(&self.store, key, flush_target).await;
        }
        let last_timestamp = self.store.get(key).await;
        pod_log_request_for_reopen(base, last_timestamp, reopen && self.enabled)
    }

    pub(crate) async fn forget(&self, key: &SourceKey) {
        self.store.remove(key).await;
    }
}

struct EndNotifyingStream {
    inner: BoxedLogStream,
    pod_token: CancellationToken,
    done_tx: Option<oneshot::Sender<StreamEnd>>,
}

impl EndNotifyingStream {
    fn new(
        inner: BoxedLogStream,
        pod_token: CancellationToken,
        done_tx: oneshot::Sender<StreamEnd>,
    ) -> Self {
        Self {
            inner,
            pod_token,
            done_tx: Some(done_tx),
        }
    }

    fn finish(&mut self, reason: StreamEnd) {
        if let Some(done_tx) = self.done_tx.take() {
            let _ = done_tx.send(reason);
        }
    }
}

impl futures::Stream for EndNotifyingStream {
    type Item = Result<LogEvent, LogSourceError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.inner.as_mut().poll_next(cx) {
            Poll::Ready(Some(item)) => Poll::Ready(Some(item)),
            Poll::Ready(None) => {
                let reason = if self.pod_token.is_cancelled() {
                    StreamEnd::Cancelled
                } else {
                    StreamEnd::Eof { last_line_ts: None }
                };
                self.finish(reason);
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl Drop for EndNotifyingStream {
    fn drop(&mut self) {
        let reason = if self.pod_token.is_cancelled() {
            StreamEnd::Cancelled
        } else {
            StreamEnd::Eof { last_line_ts: None }
        };
        self.finish(reason);
    }
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use chrono::{TimeZone, Utc};
    use futures::StreamExt;

    use crate::source::{LogEvent, SourceKind, SourceMeta};

    use super::*;

    fn sample_key() -> SourceKey {
        SourceKey {
            context: crate::source::ContextName("ctx".into()),
            namespace: "ns".into(),
            pod: "pod-a".into(),
            container: "app".into(),
            uid: "uid-1".into(),
        }
    }

    fn sample_event(ts: DateTime<Utc>, message: &str) -> LogEvent {
        let key = sample_key();
        LogEvent {
            source: Arc::new(SourceMeta {
                context: key.context.clone(),
                namespace: key.namespace.clone(),
                pod: key.pod.clone(),
                container: key.container.clone(),
                kind: SourceKind::PodLog,
                node: None,
                labels: Arc::new(crate::source::Labels::default()),
                uid: key.uid.clone(),
            }),
            timestamp: ts,
            message: Arc::from(message),
            structured: None,
            level: None,
            palette_index: None,
            container_palette_index: None,
        }
    }

    #[tokio::test]
    async fn reopen_request_waits_for_flush_and_applies_overlap() {
        let token = CancellationToken::new();
        let (cursor, processor) = CursorService::spawn(true, token.clone());
        let key = sample_key();
        let ts = Utc.with_ymd_and_hms(2026, 4, 28, 8, 0, 5).unwrap();
        let ev = sample_event(ts, "line");
        let inner: BoxedLogStream = Box::pin(futures::stream::iter(vec![Ok(ev)]));
        let pod_token = CancellationToken::new();
        let (mut tracked, done_rx) = cursor.track(key.clone(), pod_token, inner);

        let events: Vec<_> = tracked.by_ref().collect().await;
        assert_eq!(events.len(), 1);
        let end = done_rx.await.expect("stream end");
        assert_eq!(
            end,
            StreamEnd::Eof {
                last_line_ts: Some(ts)
            }
        );

        let base = PodLogRequest {
            follow: true,
            tail: Some(25),
            since_seconds: Some(300),
            ..Default::default()
        };
        let req = cursor.reopen_request(&key, &base, true, Some(ts)).await;

        assert!(req.follow);
        assert!(req.tail.is_none());
        assert!(req.since_seconds.is_none());
        let overlap_ts = req.since_time.unwrap().to_string();
        assert!(overlap_ts.contains("2026-04-28T08:00:04"));

        drop(cursor);
        token.cancel();
        processor.expect("processor").await.expect("processor");
    }

    #[tokio::test]
    async fn disabled_service_passthrough_without_reopen() {
        let token = CancellationToken::new();
        let (cursor, processor) = CursorService::spawn(false, token.clone());
        assert!(processor.is_none());
        assert!(!cursor.should_reconnect());

        let base = PodLogRequest {
            follow: true,
            tail: Some(25),
            ..Default::default()
        };
        let req = cursor
            .reopen_request(&sample_key(), &base, true, None)
            .await;
        assert_eq!(req.tail, Some(25));

        token.cancel();
    }
}
