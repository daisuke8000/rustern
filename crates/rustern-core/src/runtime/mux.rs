use std::sync::Arc;

use futures::StreamExt;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_stream::StreamMap;

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
) {
    let mut map: StreamMap<SourceKey, BoxedLogStream> = StreamMap::new();
    loop {
        tokio::select! {
            biased;
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
                            if raw_event_tx.send(row).await.is_err() {
                                break;
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
                if let Some(stats) = &stats {
                    stats.set_active_streams(map.len());
                }
            }
        }
    }
}

#[doc(hidden)]
pub fn spawn_mux_task(
    mux_rx: mpsc::Receiver<MuxCmd>,
    raw_event_tx: mpsc::Sender<Result<LogEvent, LogSourceError>>,
    stats: Option<Arc<RunStats>>,
    policy: BackpressurePolicy,
    mux_metrics: Arc<MuxMetrics>,
) -> JoinHandle<()> {
    tokio::spawn(mux_multiplex_loop(
        mux_rx,
        raw_event_tx,
        stats,
        policy,
        mux_metrics,
    ))
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use futures::stream;

    use super::*;
    use crate::source::ContextName;

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
}
