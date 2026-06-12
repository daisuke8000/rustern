//! Orchestrator: watch → mux → pipeline → stdout.

use std::sync::Arc;
use std::time::Duration;

use k8s_openapi::api::core::v1::Pod;
use kube::Api;
use kube::runtime::watcher::watcher;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;

use super::config::{CoreRunConfig, RunError, RunOutcome, RuntimeFwdConfig};
use super::cursor_store::ReconnectCursorStore;
use super::forward::{LossyMetrics, RunStats, build_log_request_semaphore, forward_to_render};
use super::mux::{MuxCmd, spawn_mux_task};
use super::pod_meta_cache::PodMetaCache;
use super::spec::PipelineSpec;
use super::watch::spawn_watch_task;
use super::watch_admission::WatchAdmissionPolicy;
use super::watch_ctx::{AttachDeps, PodWatchCtx};
use crate::discovery::context::{build_client, pick_context_name, resolve_kubeconfig};
use crate::discovery::pod_list::{PodWatchPlan, PodWatchPlanConfig};
use crate::pipeline::ExitWatchState;
use crate::render::setup::{RenderSetupError, build_line_formatter};
use crate::render::{LineFormatter, RenderCommand, flush_ticker, render_task};
use crate::source::log_opener::PodLogSourceOpener;
use crate::source::{ContextName, LogEvent, LogSourceError};

fn spawn_render_task(
    render_rx: mpsc::Receiver<RenderCommand>,
    formatter: Arc<dyn LineFormatter>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let stdout = tokio::io::stdout();
        let _ = render_task(render_rx, stdout, formatter).await;
    })
}

fn spawn_pipeline_forward_task(
    raw_event_rx: mpsc::Receiver<Result<LogEvent, LogSourceError>>,
    spec: PipelineSpec,
    render_tx: mpsc::Sender<RenderCommand>,
    fwd_cfg: RuntimeFwdConfig,
    metrics: Arc<LossyMetrics>,
    fwd_token: CancellationToken,
) -> JoinHandle<()> {
    let pipe_stream = {
        let s = ReceiverStream::new(raw_event_rx);
        spec.apply(s)
    };
    tokio::spawn(forward_to_render(
        pipe_stream,
        render_tx,
        fwd_cfg,
        metrics,
        fwd_token,
    ))
}

/// Main entry: watcher → `StreamMap` → pipeline → stdout.
pub async fn run(cfg: CoreRunConfig) -> Result<RunOutcome, RunError> {
    if cfg.only_log_lines {
        tracing::debug!(
            "--only-log-lines: stern hides +/- attach banners on stderr; rustern emits no stream lifecycle prefixes"
        );
    }

    let client = build_client(&cfg.context).await?;
    let plan_cfg = PodWatchPlanConfig {
        query: &cfg.query,
        selector: cfg.selector.as_deref(),
        field_selector: cfg.field_selector.as_deref(),
        node: cfg.node.as_deref(),
        namespaces: &cfg.namespaces,
        all_namespaces: cfg.all_namespaces,
    };
    let plan = PodWatchPlan::build(&client, &plan_cfg).await?;
    let pod_regex = plan.pod_regex;
    let watch_cfg = plan.watch_cfg;

    let kube_cfg = resolve_kubeconfig(&cfg.context)?;
    let ctx_name = pick_context_name(&kube_cfg, &cfg.context)?;
    let context_name = ContextName(ctx_name.to_string());

    let exit_watch = ExitWatchState::new(cfg.root_token.clone());
    let pipeline = PipelineSpec::from_run_config(&cfg, exit_watch)?;

    let api: Api<Pod> = if cfg.all_namespaces {
        Api::all(client.clone())
    } else if cfg.namespaces.len() == 1 {
        Api::namespaced(client.clone(), &cfg.namespaces[0])
    } else {
        Api::all(client.clone())
    };

    let admission = WatchAdmissionPolicy::try_new(
        context_name,
        pod_regex,
        &cfg.exclude_pod,
        &cfg.namespaces,
        cfg.all_namespaces,
        &cfg.container,
        &cfg.exclude_container,
        cfg.container_discovery.clone(),
        cfg.pod_condition.clone(),
    )
    .map_err(|e| RunError::Other(format!("invalid watch admission regex: {e}")))?;

    let w = watcher(api, watch_cfg);
    let (mux_tx, mux_rx) = mpsc::channel::<MuxCmd>(256);
    let (raw_event_tx, raw_event_rx) =
        mpsc::channel::<Result<LogEvent, LogSourceError>>(cfg.fwd.buffer_size.max(1));

    let sem = build_log_request_semaphore(cfg.fwd.max_log_requests);

    let mut follow_lim: Option<(mpsc::Sender<()>, mpsc::Receiver<()>)> = if cfg.pod_log.follow {
        Some(mpsc::channel(1))
    } else {
        None
    };
    let follow_limit_notifier = follow_lim.as_ref().map(|(s, _)| s.clone());

    let stats = RunStats::new(cfg.fwd.lossy);
    let h_mux = spawn_mux_task(mux_rx, raw_event_tx, Some(stats.clone()));

    let metrics = LossyMetrics::new(Some(stats.clone()));
    let metrics_rep = metrics.clone();
    let rep_token = cfg.root_token.clone();
    tokio::spawn(async move {
        metrics_rep.cumulative_reporter(rep_token).await;
    });
    if let Some(stats_cfg) = cfg.fwd.stats {
        let stats_rep = stats.clone();
        let stats_token = cfg.root_token.clone();
        tokio::spawn(async move {
            stats_rep
                .stderr_reporter(stats_cfg.interval, stats_token)
                .await;
        });
    }

    let (render_tx, render_rx) = mpsc::channel::<RenderCommand>(cfg.fwd.render_channel_capacity());
    let flush_token = cfg.root_token.clone();
    let flush_tx = render_tx.clone();
    tokio::spawn(flush_ticker(
        flush_tx,
        flush_token,
        Duration::from_millis(50),
    ));

    let formatter = build_line_formatter(&cfg.formatter, &cfg.include, &cfg.highlight).map_err(
        |e: RenderSetupError| match e {
            RenderSetupError::HighlightRegex(re) => {
                RunError::Other(format!("invalid highlight/include regex: {re}"))
            }
        },
    )?;

    {
        let _ = &cfg.output; // reserved for future strict validation
    }

    let render_h = spawn_render_task(render_rx, formatter);

    let pipeline_check = pipeline.clone();
    let pipe_h = spawn_pipeline_forward_task(
        raw_event_rx,
        pipeline,
        render_tx.clone(),
        cfg.fwd.clone(),
        metrics,
        cfg.root_token.clone(),
    );

    let watch_ctx = Arc::new(PodWatchCtx {
        admission,
        attach: AttachDeps {
            log_opener: Arc::new(PodLogSourceOpener::new(client)),
            root_child: cfg.root_token.clone(),
            pod_log: cfg.pod_log.clone(),
            cursor_reconnect: cfg.cursor_reconnect,
            reconnect_cursor: ReconnectCursorStore::new(),
            sem,
            mux_tx: mux_tx.clone(),
            follow_limit_notifier,
            pod_meta: PodMetaCache::new(),
        },
    });

    let root_w = cfg.root_token.clone();
    let mux_tx_w = mux_tx.clone();
    drop(mux_tx);
    let watch_h = spawn_watch_task(w, root_w, mux_tx_w, watch_ctx);

    let mut limit_hit = false;
    match follow_lim.take() {
        Some((_, mut lr)) => {
            tokio::select! {
                biased;
                r = lr.recv() => {
                    if r.is_some() {
                        limit_hit = true;
                        cfg.root_token.cancel();
                    }
                }
                _ = cfg.root_token.cancelled() => {}
            }
        }
        None => cfg.root_token.cancelled().await,
    }

    cfg.root_token.cancelled().await;
    let _ = render_tx.send(RenderCommand::Shutdown).await;
    let _ = tokio::time::timeout(Duration::from_millis(150), render_h).await;

    watch_h.abort();
    h_mux.abort();
    pipe_h.abort();

    if limit_hit {
        return Err(RunError::Other(
            "max concurrent log streams reached (--max-log-requests)".into(),
        ));
    }

    if pipeline_check.triggered() {
        return Err(RunError::ExitOnTriggered);
    }

    Ok(RunOutcome {
        had_source_errors: stats.had_source_errors(),
    })
}
