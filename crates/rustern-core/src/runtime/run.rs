//! Orchestrator: watch → mux → pipeline → stdout.

use std::sync::Arc;
use std::time::Duration;

use k8s_openapi::api::core::v1::Pod;
use kube::Api;
use kube::runtime::watcher::watcher;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use super::attach::{build_log_request_semaphore, spawn_attach_pod_log};
use super::config::{CoreRunConfig, RunError, RunOutcome};
use super::cursor_store::{CursorUpdate, ReconnectCursorStore, run_cursor_update_processor};
use super::list_pods::list_pods_paginated;
use super::mux_forward_core::MuxForwardCore;
use super::pod_meta_cache::PodMetaCache;
use super::spec::PipelineSpec;
use super::watch::spawn_watch_task;
use super::watch_admission::WatchAdmissionPolicy;
use super::watch_ctx::{AttachDeps, PodWatchCtx};
use crate::discovery::context::build_client;
use crate::discovery::pod_list::{PodWatchPlan, PodWatchPlanConfig};
use crate::pipeline::ExitWatchState;
use crate::render::RenderCommand;
use crate::render::setup::{RenderSetupError, build_line_formatter};
use crate::source::ContextName;
use crate::source::log_opener::PodLogSourceOpener;

/// Grace period for background tasks to exit after root cancellation before abort.
const SHUTDOWN_TIMEOUT: Duration = Duration::from_millis(150);

async fn shutdown_join_bounded(
    mut handle: JoinHandle<()>,
    task: &'static str,
) -> Option<tokio::task::JoinError> {
    if handle.is_finished() {
        return handle.await.err();
    }
    match tokio::time::timeout(SHUTDOWN_TIMEOUT, &mut handle).await {
        Ok(result) => result.err(),
        Err(_) => {
            handle.abort();
            tracing::debug!(task, "shutdown join timed out; aborted task");
            match tokio::time::timeout(SHUTDOWN_TIMEOUT, &mut handle).await {
                Ok(result) => result.err(),
                Err(_) => {
                    tracing::warn!(task, "shutdown task did not finish after abort");
                    None
                }
            }
        }
    }
}

/// Main entry: watcher → `StreamMap` → pipeline → stdout.
pub async fn run(cfg: CoreRunConfig) -> Result<RunOutcome, RunError> {
    let (client, context_name) = build_client(&cfg.context).await?;
    run_with_client(client, context_name, cfg).await
}

/// Run with a caller-supplied Kubernetes client (integration tests / mock apiserver).
#[doc(hidden)]
pub async fn run_with_client(
    client: kube::Client,
    context_name: ContextName,
    cfg: CoreRunConfig,
) -> Result<RunOutcome, RunError> {
    if cfg.only_log_lines {
        tracing::debug!(
            "--only-log-lines: stern hides +/- attach banners on stderr; rustern emits no stream lifecycle prefixes"
        );
    }

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
    let list_params = plan.list_params;

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
        context_name.clone(),
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

    let sem = build_log_request_semaphore(cfg.fwd.max_log_requests);

    let mut follow_lim: Option<(mpsc::Sender<()>, mpsc::Receiver<()>)> = if cfg.pod_log.follow {
        Some(mpsc::channel(1))
    } else {
        None
    };
    let follow_limit_notifier = follow_lim.as_ref().map(|(s, _)| s.clone());

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

    let pipeline_check = pipeline.clone();
    let core = MuxForwardCore::spawn(
        pipeline,
        cfg.fwd.clone(),
        Some(formatter),
        cfg.root_token.clone(),
        None,
    );
    let stats = core.stats;
    let mux_tx = core.mux_tx;
    let render_tx = core.render_tx;
    let h_mux = core.mux_h;
    let mut pipe_h = core.pipe_h;
    let render_h = core
        .render_h
        .expect("production run always spawns render task");

    let reconnect_cursor = ReconnectCursorStore::new();
    let (cursor_update_tx, cursor_update_rx) = mpsc::unbounded_channel::<CursorUpdate>();
    let cursor_h = tokio::spawn(run_cursor_update_processor(
        cursor_update_rx,
        reconnect_cursor.clone(),
        cfg.root_token.clone(),
    ));

    let watch_ctx = Arc::new(PodWatchCtx {
        admission,
        attach: AttachDeps {
            log_opener: Arc::new(PodLogSourceOpener::new(client)),
            root_child: cfg.root_token.clone(),
            pod_log: cfg.pod_log.clone(),
            cursor_reconnect: cfg.cursor_reconnect,
            reconnect_cursor,
            cursor_update_tx,
            sem,
            mux_tx: mux_tx.clone(),
            follow_limit_notifier,
            pod_meta: PodMetaCache::new(),
        },
    });

    let root_w = cfg.root_token.clone();
    let mux_tx_w = mux_tx.clone();
    drop(mux_tx);
    let watch_h = if cfg.pod_log.follow {
        let w = watcher(api.clone(), watch_cfg);
        Some(spawn_watch_task(w, root_w, mux_tx_w, watch_ctx))
    } else {
        let pods = list_pods_paginated(&api, &list_params).await?;
        for pod in &pods {
            if watch_ctx.admission.admit_pod(pod) {
                watch_ctx
                    .attach
                    .pod_meta
                    .update_from_pod(&context_name, pod)
                    .await;
            }
        }
        let keys = watch_ctx.admission.collect_snapshot(pods);
        for key in keys {
            let pod_t = cfg.root_token.child_token();
            spawn_attach_pod_log(&watch_ctx, key, pod_t);
        }
        drop(mux_tx_w);
        drop(watch_ctx);
        None
    };

    let mut limit_hit = false;
    let mut pipe_join_err = None;
    let mut pipe_joined = false;
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
        None => {
            tokio::select! {
                r = &mut pipe_h => {
                    pipe_joined = true;
                    if let Err(e) = r {
                        pipe_join_err = Some(e);
                    }
                    cfg.root_token.cancel();
                }
                _ = cfg.root_token.cancelled() => {}
            }
        }
    }

    cfg.root_token.cancelled().await;

    // Cooperative shutdown: upstream first (watch → mux → pipeline → cursor), then render.
    // The forward task holds a render_tx clone until pipeline join completes.
    if let Some(watch_h) = watch_h {
        shutdown_join_bounded(watch_h, "watch").await;
    }
    shutdown_join_bounded(h_mux, "mux").await;
    if !pipe_joined {
        if let Some(e) = shutdown_join_bounded(pipe_h, "pipeline").await {
            if !e.is_cancelled() {
                pipe_join_err = Some(e);
            }
        }
    }
    shutdown_join_bounded(cursor_h, "cursor").await;

    if tokio::time::timeout(SHUTDOWN_TIMEOUT, render_tx.send(RenderCommand::Shutdown))
        .await
        .is_err()
    {
        tracing::debug!("render shutdown send timed out; closing render channel");
    }
    drop(render_tx);
    shutdown_join_bounded(render_h, "render").await;

    if let Some(e) = pipe_join_err {
        return Err(RunError::Other(format!(
            "pipeline forward task failed: {e}"
        )));
    }

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
