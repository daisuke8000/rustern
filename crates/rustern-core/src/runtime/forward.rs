//! レンダラへの転送、`LossyMetrics`、同時ログ接続数セマフォ。

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use futures::Stream;
use futures::StreamExt;
use tokio::sync::{Mutex, Semaphore, mpsc};
use tokio_util::sync::CancellationToken;

use super::config::RuntimeFwdConfig;
use crate::render::RenderCommand;
use crate::source::{LogEvent, LogSourceError};

#[derive(Debug)]
pub struct LossyMetrics {
    last_warn_at: Mutex<Instant>,
    dropped_total: AtomicU64,
    warn_interval: Duration,
    cumulative_interval: Duration,
}

impl LossyMetrics {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            last_warn_at: Mutex::new(Instant::now() - Duration::from_secs(60)),
            dropped_total: AtomicU64::new(0),
            warn_interval: Duration::from_secs(5),
            cumulative_interval: Duration::from_secs(30),
        })
    }

    pub async fn record_drop(self: &Arc<Self>, reason: &str) {
        self.dropped_total.fetch_add(1, Ordering::Relaxed);
        let mut last = self.last_warn_at.lock().await;
        if last.elapsed() >= self.warn_interval {
            tracing::warn!(reason, "log event dropped due to backpressure");
            *last = Instant::now();
        }
    }

    pub async fn cumulative_reporter(self: Arc<Self>, token: CancellationToken) {
        let mut ticker = tokio::time::interval(self.cumulative_interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = token.cancelled() => break,
                _ = ticker.tick() => {
                    let total = self.dropped_total.swap(0, Ordering::Relaxed);
                    if total > 0 {
                        tracing::info!(dropped_in_window = total, "lossy drop summary");
                    }
                }
            }
        }
    }
}

pub async fn forward_to_render(
    mut source_stream: impl Stream<Item = Result<LogEvent, LogSourceError>> + Unpin,
    tx: mpsc::Sender<RenderCommand>,
    cfg: RuntimeFwdConfig,
    metrics: Arc<LossyMetrics>,
    token: CancellationToken,
) {
    while let Some(item) = source_stream.next().await {
        if token.is_cancelled() {
            break;
        }
        match item {
            Ok(ev) => {
                if cfg.lossy {
                    if tx.try_send(RenderCommand::Line(ev)).is_err() {
                        metrics.record_drop("channel_full").await;
                    }
                } else if tx.send(RenderCommand::Line(ev)).await.is_err() {
                    break;
                }
            }
            Err(e) => tracing::warn!(error = ?e, "source stream error"),
        }
    }
}

pub fn build_log_request_semaphore(max: usize) -> Arc<Semaphore> {
    Arc::new(Semaphore::new(max.max(1)))
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use futures::stream;
    use std::sync::Arc;

    use crate::source::{ContextName, Labels, LogEvent, SourceKind, SourceMeta};

    #[tokio::test]
    async fn forwards_all_events_when_buffer_sufficient() {
        let metrics = LossyMetrics::new();
        let (tx, mut rx) = mpsc::channel::<RenderCommand>(1024);
        let token = CancellationToken::new();
        fn sample_ev() -> LogEvent {
            LogEvent {
                source: Arc::new(SourceMeta {
                    context: ContextName("c".into()),
                    namespace: "n".into(),
                    pod: "p".into(),
                    container: "x".into(),
                    kind: SourceKind::PodLog,
                    node: None,
                    labels: Arc::new(Labels::default()),
                    uid: "u".into(),
                }),
                timestamp: chrono::Utc::now(),
                message: std::sync::Arc::from("x"),
                structured: None,
                level: None,
                palette_index: None,
            }
        }
        let v: Vec<_> = (0..50).map(|_| Ok(sample_ev())).collect();
        let s = stream::iter(v);
        let h = tokio::spawn(forward_to_render(
            s,
            tx,
            RuntimeFwdConfig {
                buffer_size: 1024,
                lossy: false,
                max_log_requests: 10,
            },
            metrics,
            token.clone(),
        ));
        let mut got = 0;
        while let Some(cmd) = rx.recv().await {
            if matches!(cmd, RenderCommand::Line(_)) {
                got += 1;
                if got == 50 {
                    break;
                }
            }
        }
        token.cancel();
        let _ = h.await;
        assert_eq!(got, 50);
    }

    #[tokio::test]
    async fn lossy_try_send_skips_when_render_backpressured() {
        let metrics = LossyMetrics::new();
        let (tx, rx) = mpsc::channel::<RenderCommand>(1);
        let token = CancellationToken::new();
        fn sample_ev() -> LogEvent {
            LogEvent {
                source: Arc::new(SourceMeta {
                    context: ContextName("c".into()),
                    namespace: "n".into(),
                    pod: "p".into(),
                    container: "x".into(),
                    kind: SourceKind::PodLog,
                    node: None,
                    labels: Arc::new(Labels::default()),
                    uid: "u".into(),
                }),
                timestamp: chrono::Utc::now(),
                message: std::sync::Arc::from("x"),
                structured: None,
                level: None,
                palette_index: None,
            }
        }
        let v: Vec<_> = (0..32).map(|_| Ok(sample_ev())).collect();
        let s = stream::iter(v);
        let h = tokio::spawn(forward_to_render(
            s,
            tx,
            RuntimeFwdConfig {
                buffer_size: 1,
                lossy: true,
                max_log_requests: 10,
            },
            metrics,
            token.clone(),
        ));
        drop(rx);
        tokio::time::timeout(Duration::from_secs(3), h)
            .await
            .expect("timeout")
            .unwrap();
        token.cancel();
    }
}
