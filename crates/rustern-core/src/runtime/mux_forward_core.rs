//! Production-shaped mux → pipeline → forward assembly.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;

use super::config::RuntimeFwdConfig;
use super::forward::{LossyMetrics, MuxMetrics, RunStats, forward_to_render};
use super::mux::{MuxCmd, spawn_mux_task};
use super::spec::PipelineSpec;
use crate::render::{LineFormatter, RenderCommand, flush_ticker, render_task};
use crate::source::{LogEvent, LogSourceError};

pub struct MuxForwardCoreHandles {
    pub mux_tx: mpsc::Sender<MuxCmd>,
    pub render_tx: mpsc::Sender<RenderCommand>,
    pub render_rx: Option<mpsc::Receiver<RenderCommand>>,
    pub stats: Arc<RunStats>,
    pub mux_h: JoinHandle<()>,
    pub pipe_h: JoinHandle<()>,
    pub render_h: Option<JoinHandle<()>>,
}

pub struct MuxForwardCore;

impl MuxForwardCore {
    /// Production assembly: mux → pipeline → forward with stats mirroring.
    ///
    /// When `formatter` is [`Some`], spawns the stdout render task and returns
    /// [`MuxForwardCoreHandles::render_h`]. When [`None`], leaves
    /// [`MuxForwardCoreHandles::render_rx`] for callers (e.g. load tests) to drain.
    pub fn spawn(
        pipeline: PipelineSpec,
        fwd_cfg: RuntimeFwdConfig,
        formatter: Option<Arc<dyn LineFormatter>>,
        token: CancellationToken,
        stats: Option<Arc<RunStats>>,
    ) -> MuxForwardCoreHandles {
        let stats = stats.unwrap_or_else(|| RunStats::from_fwd(&fwd_cfg));

        let (mux_tx, mux_rx) = mpsc::channel::<MuxCmd>(256);
        let (raw_event_tx, raw_event_rx) =
            mpsc::channel::<Result<LogEvent, LogSourceError>>(fwd_cfg.buffer_size.max(1));

        let mux_metrics = MuxMetrics::new(Some(stats.clone()));
        let mux_h = spawn_mux_task(
            mux_rx,
            raw_event_tx,
            Some(stats.clone()),
            fwd_cfg.resolved_mux_policy(),
            mux_metrics,
            token.clone(),
        );

        let metrics = LossyMetrics::new(Some(stats.clone()));
        let metrics_rep = metrics.clone();
        let rep_token = token.clone();
        tokio::spawn(async move {
            metrics_rep.cumulative_reporter(rep_token).await;
        });
        if let Some(stats_cfg) = fwd_cfg.stats {
            let stats_rep = stats.clone();
            let stats_token = token.clone();
            tokio::spawn(async move {
                stats_rep
                    .stderr_reporter(stats_cfg.interval, stats_token)
                    .await;
            });
        }

        let (render_tx, render_rx) =
            mpsc::channel::<RenderCommand>(fwd_cfg.render_channel_capacity());
        let flush_token = token.clone();
        let flush_tx = render_tx.clone();
        tokio::spawn(flush_ticker(
            flush_tx,
            flush_token,
            Duration::from_millis(50),
        ));

        let (render_h, render_rx) = match formatter {
            Some(formatter) => {
                let render_h = tokio::spawn(async move {
                    let stdout = tokio::io::stdout();
                    let _ = render_task(render_rx, stdout, formatter).await;
                });
                (Some(render_h), None)
            }
            None => (None, Some(render_rx)),
        };

        let pipe_stream = {
            let s = ReceiverStream::new(raw_event_rx);
            pipeline.apply(s)
        };
        let pipe_h = tokio::spawn(forward_to_render(
            pipe_stream,
            render_tx.clone(),
            fwd_cfg,
            metrics,
            token,
        ));

        MuxForwardCoreHandles {
            mux_tx,
            render_tx,
            render_rx,
            stats,
            mux_h,
            pipe_h,
            render_h,
        }
    }
}
