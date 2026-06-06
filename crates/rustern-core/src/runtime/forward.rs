//! Forward to renderer, `LossyMetrics`, concurrent log stream semaphore.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use futures::Stream;
use futures::StreamExt;
use tokio::io::AsyncWriteExt;
use tokio::sync::{Mutex, Semaphore, mpsc};
use tokio_util::sync::CancellationToken;

use super::config::RuntimeFwdConfig;
use crate::render::RenderCommand;
use crate::source::{LogEvent, LogSourceError};

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct RunStatsSnapshot {
    pub active_streams: u64,
    pub forwarded_lines: u64,
    pub dropped_lines: u64,
}

#[derive(Debug)]
pub struct RunStats {
    active_streams: AtomicU64,
    forwarded_lines: AtomicU64,
    dropped_lines: AtomicU64,
    lossy: bool,
}

impl RunStats {
    pub fn new(lossy: bool) -> Arc<Self> {
        Arc::new(Self {
            active_streams: AtomicU64::new(0),
            forwarded_lines: AtomicU64::new(0),
            dropped_lines: AtomicU64::new(0),
            lossy,
        })
    }

    pub fn set_active_streams(&self, active_streams: usize) {
        self.active_streams
            .store(active_streams as u64, Ordering::Relaxed);
    }

    pub fn record_forwarded_line(&self) {
        self.forwarded_lines.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_dropped_line(&self) {
        self.dropped_lines.fetch_add(1, Ordering::Relaxed);
    }

    pub fn snapshot_and_reset(&self) -> RunStatsSnapshot {
        RunStatsSnapshot {
            active_streams: self.active_streams.load(Ordering::Relaxed),
            forwarded_lines: self.forwarded_lines.swap(0, Ordering::Relaxed),
            dropped_lines: self.dropped_lines.swap(0, Ordering::Relaxed),
        }
    }

    pub fn format_window(&self, interval: Duration) -> String {
        Self::format_snapshot(self.snapshot_and_reset(), interval, self.lossy)
    }

    fn format_snapshot(snapshot: RunStatsSnapshot, interval: Duration, lossy: bool) -> String {
        let interval = format!("{interval:?}");
        let interval = interval.as_str();
        if lossy {
            format!(
                "stats: active streams={}, forwarded lines={}/{}, dropped lines={}/{}",
                snapshot.active_streams,
                snapshot.forwarded_lines,
                interval,
                snapshot.dropped_lines,
                interval
            )
        } else {
            format!(
                "stats: active streams={}, forwarded lines={}/{}",
                snapshot.active_streams, snapshot.forwarded_lines, interval
            )
        }
    }

    pub async fn stderr_reporter(self: Arc<Self>, interval: Duration, token: CancellationToken) {
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        ticker.tick().await;
        let mut stderr = tokio::io::stderr();
        loop {
            tokio::select! {
                _ = token.cancelled() => break,
                _ = ticker.tick() => {
                    let line = self.format_window(interval);
                    if stderr.write_all(line.as_bytes()).await.is_err() {
                        break;
                    }
                    if stderr.write_all(b"\n").await.is_err() {
                        break;
                    }
                    if stderr.flush().await.is_err() {
                        break;
                    }
                }
            }
        }
    }
}

#[derive(Debug)]
pub struct LossyMetrics {
    last_warn_at: Mutex<Instant>,
    dropped_total: AtomicU64,
    warn_interval: Duration,
    cumulative_interval: Duration,
    stats: Option<Arc<RunStats>>,
}

impl LossyMetrics {
    pub fn new(stats: Option<Arc<RunStats>>) -> Arc<Self> {
        Arc::new(Self {
            last_warn_at: Mutex::new(Instant::now() - Duration::from_secs(60)),
            dropped_total: AtomicU64::new(0),
            warn_interval: Duration::from_secs(5),
            cumulative_interval: Duration::from_secs(30),
            stats,
        })
    }

    pub fn drop_count(&self) -> u64 {
        self.dropped_total.load(Ordering::Relaxed)
    }

    pub async fn record_drop(self: &Arc<Self>, reason: &str) {
        self.dropped_total.fetch_add(1, Ordering::Relaxed);
        if let Some(stats) = &self.stats {
            stats.record_dropped_line();
        }
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
                    match tx.try_send(RenderCommand::Line(ev)) {
                        Ok(()) => {
                            if let Some(stats) = &metrics.stats {
                                stats.record_forwarded_line();
                            }
                        }
                        Err(mpsc::error::TrySendError::Full(_)) => {
                            metrics.record_drop("channel_full").await;
                        }
                        Err(mpsc::error::TrySendError::Closed(_)) => break,
                    }
                } else if tx.send(RenderCommand::Line(ev)).await.is_err() {
                    break;
                } else if let Some(stats) = &metrics.stats {
                    stats.record_forwarded_line();
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
        let metrics = LossyMetrics::new(None);
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
                container_palette_index: None,
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
                stats: None,
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
        let metrics = LossyMetrics::new(None);
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
                container_palette_index: None,
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
                stats: None,
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

    #[tokio::test]
    async fn run_stats_track_forwarded_and_dropped_lines_per_window() {
        let stats = RunStats::new(true);
        let metrics = LossyMetrics::new(Some(stats.clone()));
        let (tx, _rx) = mpsc::channel::<RenderCommand>(1);
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
                container_palette_index: None,
            }
        }

        let s = stream::iter(vec![Ok(sample_ev()), Ok(sample_ev())]);
        let h = tokio::spawn(forward_to_render(
            s,
            tx,
            RuntimeFwdConfig {
                buffer_size: 1,
                lossy: true,
                max_log_requests: 10,
                stats: Some(crate::runtime::RuntimeStatsConfig {
                    interval: Duration::from_secs(30),
                }),
            },
            metrics,
            token.clone(),
        ));

        tokio::time::timeout(Duration::from_secs(3), h)
            .await
            .expect("timeout")
            .unwrap();

        let snapshot = stats.snapshot_and_reset();
        assert_eq!(snapshot.forwarded_lines, 1);
        assert_eq!(snapshot.dropped_lines, 1);
    }

    #[test]
    fn run_stats_report_includes_drop_count_only_for_lossy_mode() {
        let lossy = RunStats::new(true);
        lossy.set_active_streams(2);
        lossy.record_forwarded_line();
        lossy.record_dropped_line();
        assert_eq!(
            lossy.format_window(Duration::from_secs(30)),
            "stats: active streams=2, forwarded lines=1/30s, dropped lines=1/30s"
        );

        let lossless = RunStats::new(false);
        lossless.set_active_streams(1);
        lossless.record_forwarded_line();
        assert_eq!(
            lossless.format_window(Duration::from_secs(30)),
            "stats: active streams=1, forwarded lines=1/30s"
        );
    }
}
