use std::sync::Arc;

use futures::StreamExt;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_stream::StreamMap;
use tokio_util::sync::CancellationToken;

use super::config::BackpressurePolicy;
use super::forward::{MuxMetrics, RunStats};
use crate::source::{BoxedLogStream, LogEvent, LogSourceError, SourceKey};

#[doc(hidden)]
pub enum MuxCmd {
    Add(SourceKey, BoxedLogStream),
    Remove(SourceKey),
}

async fn mux_multiplex_loop(
    mut mux_rx: mpsc::Receiver<MuxCmd>,
    raw_event_tx: mpsc::Sender<Result<LogEvent, LogSourceError>>,
    stats: Option<Arc<RunStats>>,
    policy: BackpressurePolicy,
    mux_metrics: Arc<MuxMetrics>,
    token: CancellationToken,
) {
    let mut map: StreamMap<SourceKey, BoxedLogStream> = StreamMap::new();
    loop {
        tokio::select! {
            biased;
            _ = token.cancelled() => break,
            cmd = mux_rx.recv() => {
                match cmd {
                    Some(MuxCmd::Add(key, stream)) => {
                        map.insert(key, stream);
                        if let Some(stats) = &stats {
                            stats.set_active_streams(map.len());
                        }
                    }
                    Some(MuxCmd::Remove(key)) => {
                        if map.remove(&key).is_some()
                            && let Some(stats) = &stats
                        {
                            stats.set_active_streams(map.len());
                        }
                    }
                    None => break,
                }
            }
            item = map.next(), if !map.is_empty() => {
                if let Some((_k, row)) = item {
                    match policy {
                        BackpressurePolicy::Blocking => {
                            tokio::select! {
                                biased;
                                _ = token.cancelled() => break,
                                sent = raw_event_tx.send(row) => {
                                    if sent.is_err() {
                                        break;
                                    }
                                }
                            }
                        }
                        BackpressurePolicy::Lossy => {
                            match raw_event_tx.try_send(row) {
                                Ok(()) => {}
                                Err(mpsc::error::TrySendError::Full(_)) => {
                                    mux_metrics.record_mux_drop();
                                }
                                Err(mpsc::error::TrySendError::Closed(_)) => break,
                            }
                        }
                    }
                }
            }
        }
    }
    if let Some(stats) = &stats {
        stats.set_active_streams(0);
    }
}

#[doc(hidden)]
pub fn spawn_mux_task(
    mux_rx: mpsc::Receiver<MuxCmd>,
    raw_event_tx: mpsc::Sender<Result<LogEvent, LogSourceError>>,
    stats: Option<Arc<RunStats>>,
    policy: BackpressurePolicy,
    mux_metrics: Arc<MuxMetrics>,
    token: CancellationToken,
) -> JoinHandle<()> {
    tokio::spawn(mux_multiplex_loop(
        mux_rx,
        raw_event_tx,
        stats,
        policy,
        mux_metrics,
        token,
    ))
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use std::pin::Pin;
    use std::task::{Context, Poll};

    use futures::stream;
    use tokio::sync::oneshot;

    use super::*;
    use crate::source::{ContextName, Labels, LogEvent, LogSourceError, SourceKind, SourceMeta};
    use tokio_util::sync::CancellationToken;

    fn source_key(name: &str) -> SourceKey {
        SourceKey {
            context: ContextName("ctx".into()),
            namespace: "default".into(),
            pod: format!("pod-{name}"),
            container: "app".into(),
            uid: format!("uid-{name}"),
        }
    }

    #[tokio::test]
    async fn add_and_remove_update_active_stream_stats() {
        let stats = RunStats::new(false);
        let (mux_tx, mux_rx) = mpsc::channel(4);
        let (raw_tx, _raw_rx) = mpsc::channel(4);
        let mux_metrics = MuxMetrics::new(None);
        let task = spawn_mux_task(
            mux_rx,
            raw_tx,
            Some(stats.clone()),
            BackpressurePolicy::Blocking,
            mux_metrics,
            CancellationToken::new(),
        );

        mux_tx
            .send(MuxCmd::Add(
                source_key("a"),
                Box::pin(stream::pending::<Result<LogEvent, LogSourceError>>()),
            ))
            .await
            .unwrap();
        tokio::task::yield_now().await;
        assert_eq!(stats.snapshot_and_reset().active_streams, 1);

        mux_tx.send(MuxCmd::Remove(source_key("a"))).await.unwrap();
        tokio::task::yield_now().await;
        assert_eq!(stats.snapshot_and_reset().active_streams, 0);

        drop(mux_tx);
        tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .expect("mux task timed out")
            .unwrap();
    }

    #[tokio::test]
    async fn mux_exits_promptly_on_cancellation() {
        let token = CancellationToken::new();
        let (mux_tx, mux_rx) = mpsc::channel(4);
        let (raw_tx, _raw_rx) = mpsc::channel(4);
        let mux_metrics = MuxMetrics::new(None);
        let task = spawn_mux_task(
            mux_rx,
            raw_tx,
            None,
            BackpressurePolicy::Blocking,
            mux_metrics,
            token.clone(),
        );

        token.cancel();
        tokio::time::timeout(Duration::from_millis(150), task)
            .await
            .expect("mux task did not exit after cancellation")
            .unwrap();
        drop(mux_tx);
    }

    #[tokio::test]
    async fn mux_blocking_send_exits_on_cancel_while_backpressured() {
        fn sample_ev() -> Result<LogEvent, LogSourceError> {
            Ok(LogEvent {
                source: Arc::new(SourceMeta {
                    context: ContextName("ctx".into()),
                    namespace: "default".into(),
                    pod: "pod".into(),
                    container: "app".into(),
                    kind: SourceKind::PodLog,
                    node: None,
                    labels: Arc::new(Labels::default()),
                    uid: "uid".into(),
                }),
                timestamp: chrono::Utc::now(),
                message: std::sync::Arc::from("line"),
                structured: None,
                level: None,
                palette_index: None,
                container_palette_index: None,
            })
        }

        struct SignalingStream {
            state: u8,
            polled: Option<oneshot::Sender<()>>,
        }

        impl futures::Stream for SignalingStream {
            type Item = Result<LogEvent, LogSourceError>;

            fn poll_next(
                mut self: Pin<&mut Self>,
                _cx: &mut Context<'_>,
            ) -> Poll<Option<Self::Item>> {
                match self.state {
                    0 => {
                        self.state = 1;
                        if let Some(tx) = self.polled.take() {
                            let _ = tx.send(());
                        }
                        Poll::Ready(Some(sample_ev()))
                    }
                    1 => Poll::Ready(Some(sample_ev())),
                    _ => Poll::Ready(None),
                }
            }
        }

        let token = CancellationToken::new();
        let (mux_tx, mux_rx) = mpsc::channel(4);
        let (raw_tx, _raw_rx) = mpsc::channel(1);
        raw_tx.send(sample_ev()).await.unwrap();
        let mux_metrics = MuxMetrics::new(None);
        let task = spawn_mux_task(
            mux_rx,
            raw_tx,
            None,
            BackpressurePolicy::Blocking,
            mux_metrics,
            token.clone(),
        );

        let (polled_tx, polled_rx) = oneshot::channel();
        mux_tx
            .send(MuxCmd::Add(
                source_key("blocked"),
                Box::pin(SignalingStream {
                    state: 0,
                    polled: Some(polled_tx),
                }),
            ))
            .await
            .unwrap();
        polled_rx
            .await
            .expect("mux should poll stream before cancel");
        tokio::task::yield_now().await;
        token.cancel();
        tokio::time::timeout(Duration::from_millis(150), task)
            .await
            .expect("mux task did not exit after cancel while backpressured")
            .unwrap();
        drop(mux_tx);
    }
}
