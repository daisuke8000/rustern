//! Runner: watch → mux → pipeline → stdout (`run`).

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;

use k8s_openapi::api::core::v1::Pod;
use kube::Api;
use kube::Client;
use kube::runtime::watcher::{Event, watcher};
use regex::Regex;
use tokio::sync::{Semaphore, mpsc};
use tokio::task::JoinHandle;
use tokio_stream::StreamMap;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;

use super::config::{CoreRunConfig, RunError, RunOutcome, RuntimeFwdConfig};
use super::forward::{LossyMetrics, build_log_request_semaphore, forward_to_render};
use super::pipeline::{PipelineStages, apply_pipeline, compile_list};
use crate::discovery::context::{build_client, pick_context_name, resolve_kubeconfig};
use crate::discovery::pod_condition::{PodConditionFilter, pod_matches_condition};
use crate::discovery::pod_list::{PodWatchPlan, PodWatchPlanConfig};
use crate::discovery::pod_watcher::{ContainerDiscoverOpts, keys_from_pod, reconcile};
use crate::pipeline::validate_filter;
use crate::render::setup::{RenderSetupError, build_line_formatter, color_assign_opts};
use crate::render::{LineFormatter, RenderCommand, flush_ticker, render_task};
use crate::source::pod_log::{PodLogRequest, PodLogSource};
use crate::source::{
    BoxedLogStream, ContextName, Labels, LogEvent, LogSource, LogSourceError, SourceKey,
    SourceKind, SourceMeta,
};

enum MuxCmd {
    Add(SourceKey, BoxedLogStream),
    Remove(SourceKey),
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

/// Merge sources with `StreamMap` and forward into the pre-pipeline channel.
async fn mux_multiplex_loop(
    mut mux_rx: mpsc::Receiver<MuxCmd>,
    raw_event_tx: mpsc::Sender<Result<LogEvent, LogSourceError>>,
) {
    let mut map: StreamMap<SourceKey, BoxedLogStream> = StreamMap::new();
    loop {
        tokio::select! {
            cmd = mux_rx.recv() => {
                match cmd {
                    Some(MuxCmd::Add(key, stream)) => { map.insert(key, stream); }
                    Some(MuxCmd::Remove(key)) => { map.remove(&key); }
                    None => break,
                }
            }
            item = map.next() => {
                if let Some((_k, row)) = item
                    && raw_event_tx.send(row).await.is_err()
                {
                    break;
                }
            }
        }
    }
}

struct PodWatchCtx {
    context_name: ContextName,
    pod_regex: Option<Regex>,
    pod_condition: Option<PodConditionFilter>,
    container_discovery: ContainerDiscoverOpts,
    container_incl: Regex,
    container_excl: Vec<Regex>,
    allowed_ns: Option<HashSet<String>>,
    exclude_pod: Vec<Regex>,
    mux_tx: mpsc::Sender<MuxCmd>,
    client: Client,
    root_child: CancellationToken,
    pod_log: PodLogRequest,
    sem: Arc<Semaphore>,
    follow_limit_notifier: Option<mpsc::Sender<()>>,
}

struct AttachPodLogParams {
    ctx: Arc<PodWatchCtx>,
    meta: SourceMeta,
    pod_token: CancellationToken,
    key: SourceKey,
}

async fn attach_pod_log_stream(p: AttachPodLogParams) {
    let permit = if p.ctx.pod_log.follow {
        match Arc::clone(&p.ctx.sem).try_acquire_owned() {
            Ok(p) => p,
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
            Ok(p) => p,
            Err(_) => return,
        }
    };

    let client = p.ctx.client.clone();
    match PodLogSource::start(client, p.meta, p.pod_token, p.ctx.pod_log.clone()).await {
        Ok(src) => {
            let stream = Box::new(src).into_stream();
            let _ = p.ctx.mux_tx.send(MuxCmd::Add(p.key, stream)).await;
        }
        Err(e) => tracing::warn!(?e, "pod log start"),
    }
    drop(permit);
}

fn spawn_attach_pod_log(ctx: &Arc<PodWatchCtx>, key: SourceKey, pod_token: CancellationToken) {
    let meta = source_meta_for_key(&ctx.context_name, &key);
    tokio::spawn(attach_pod_log_stream(AttachPodLogParams {
        ctx: Arc::clone(ctx),
        meta,
        pod_token,
        key,
    }));
}

fn filtered_stream_keys(pod: &Pod, ctx: &PodWatchCtx) -> Vec<SourceKey> {
    keys_from_pod(pod, &ctx.context_name, &ctx.container_discovery)
        .into_iter()
        .filter(|k| ctx.container_incl.is_match(&k.container))
        .filter(|k| !ctx.container_excl.iter().any(|r| r.is_match(&k.container)))
        .collect()
}

/// Keys currently tracked for this Pod (`metadata.uid` disambiguates rollouts).
fn keys_for_deleted_pod(
    pod: &Pod,
    ctx: &PodWatchCtx,
    active: &HashSet<SourceKey>,
) -> Vec<SourceKey> {
    let ns = pod.metadata.namespace.clone().unwrap_or_default();
    let pod_name = pod.metadata.name.clone().unwrap_or_default();
    let uid_opt = pod.metadata.uid.as_deref();
    active
        .iter()
        .filter(|k| {
            k.context == ctx.context_name
                && k.namespace == ns
                && k.pod == pod_name
                && uid_opt.is_none_or(|u| k.uid == u)
        })
        .cloned()
        .collect()
}

async fn drop_pod_streams(
    pod: &Pod,
    active: &mut HashSet<SourceKey>,
    tokens: &mut HashMap<SourceKey, CancellationToken>,
    mux_tx: &mpsc::Sender<MuxCmd>,
    ctx: &PodWatchCtx,
) {
    let keys = keys_for_deleted_pod(pod, ctx, active);
    for k in keys {
        if let Some(t) = tokens.remove(&k) {
            t.cancel();
        }
        active.remove(&k);
        let _ = mux_tx.send(MuxCmd::Remove(k)).await;
    }
}

async fn handle_watch_delete(
    pod: Pod,
    active: &mut HashSet<SourceKey>,
    tokens: &mut HashMap<SourceKey, CancellationToken>,
    mux_tx: &mpsc::Sender<MuxCmd>,
    ctx: &PodWatchCtx,
) {
    drop_pod_streams(&pod, active, tokens, mux_tx, ctx).await;
}

fn pod_passes_watch_filters(pod: &Pod, ctx: &PodWatchCtx) -> bool {
    let name = pod.metadata.name.as_deref().unwrap_or("");
    if let Some(allowed) = &ctx.allowed_ns {
        let ns = pod.metadata.namespace.as_deref().unwrap_or("");
        if !allowed.contains(ns) {
            return false;
        }
    }
    if ctx.exclude_pod.iter().any(|re| re.is_match(name)) {
        return false;
    }
    if let Some(re) = &ctx.pod_regex
        && !re.is_match(name)
    {
        return false;
    }
    if let Some(cond) = &ctx.pod_condition
        && !pod_matches_condition(pod, cond)
    {
        return false;
    }
    true
}

async fn handle_watch_apply(
    pod: Pod,
    active: &mut HashSet<SourceKey>,
    tokens: &mut HashMap<SourceKey, CancellationToken>,
    ctx: &Arc<PodWatchCtx>,
) {
    if !pod_passes_watch_filters(&pod, ctx) {
        drop_pod_streams(&pod, active, tokens, &ctx.mux_tx, ctx).await;
        return;
    }
    let keys = filtered_stream_keys(&pod, ctx);
    for key in keys {
        if active.insert(key.clone()) {
            let pod_t = ctx.root_child.child_token();
            tokens.insert(key.clone(), pod_t.clone());
            spawn_attach_pod_log(ctx, key, pod_t);
        }
    }
}

fn collect_keys_snapshot(pending_pods: Vec<Pod>, ctx: &PodWatchCtx) -> HashSet<SourceKey> {
    let mut snap: HashSet<SourceKey> = HashSet::new();
    for pod in pending_pods {
        if !pod_passes_watch_filters(&pod, ctx) {
            continue;
        }
        for k in filtered_stream_keys(&pod, ctx) {
            snap.insert(k);
        }
    }
    snap
}

async fn handle_init_done(
    pending_pods: &mut Vec<Pod>,
    active: &mut HashSet<SourceKey>,
    tokens: &mut HashMap<SourceKey, CancellationToken>,
    ctx: &Arc<PodWatchCtx>,
) {
    let snap = collect_keys_snapshot(std::mem::take(pending_pods), ctx.as_ref());
    let diff = reconcile(active, &snap);
    for k in diff.to_drop {
        if let Some(t) = tokens.remove(&k) {
            t.cancel();
        }
        active.remove(&k);
        let _ = ctx.mux_tx.send(MuxCmd::Remove(k)).await;
    }
    for key in diff.to_add {
        if active.insert(key.clone()) {
            let pod_t = ctx.root_child.child_token();
            tokens.insert(key.clone(), pod_t.clone());
            spawn_attach_pod_log(ctx, key, pod_t);
        }
    }
}

fn spawn_mux_task(
    mux_rx: mpsc::Receiver<MuxCmd>,
    raw_event_tx: mpsc::Sender<Result<LogEvent, LogSourceError>>,
) -> JoinHandle<()> {
    tokio::spawn(mux_multiplex_loop(mux_rx, raw_event_tx))
}

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
    stages: PipelineStages,
    render_tx: mpsc::Sender<RenderCommand>,
    fwd_cfg: RuntimeFwdConfig,
    metrics: Arc<LossyMetrics>,
    fwd_token: CancellationToken,
) -> JoinHandle<()> {
    let pipe_stream = {
        let s = ReceiverStream::new(raw_event_rx);
        apply_pipeline(s, stages)
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

    let container_incl = Regex::new(&cfg.container)?;
    let container_excl: Vec<Regex> = cfg
        .exclude_container
        .iter()
        .map(|p| Regex::new(p))
        .collect::<Result<_, _>>()?;
    let includes = compile_list(&cfg.include)?;
    let excludes = compile_list(&cfg.exclude)?;

    let jq = match &cfg.json_query {
        Some(expr) => Some((validate_filter(expr)?, cfg.json_query_mode)),
        None => None,
    };

    let exclude_pod: Vec<Regex> = cfg
        .exclude_pod
        .iter()
        .map(|p| Regex::new(p))
        .collect::<Result<_, _>>()
        .map_err(|e| RunError::Other(format!("invalid exclude-pod regex: {e}")))?;

    let (api, allowed_ns): (Api<Pod>, Option<HashSet<String>>) = if cfg.all_namespaces {
        (Api::all(client.clone()), None)
    } else if cfg.namespaces.len() == 1 {
        (Api::namespaced(client.clone(), &cfg.namespaces[0]), None)
    } else {
        (
            Api::all(client.clone()),
            Some(cfg.namespaces.iter().cloned().collect()),
        )
    };

    let w = watcher(api, watch_cfg);
    let (mux_tx, mux_rx) = mpsc::channel::<MuxCmd>(256);
    let (raw_event_tx, raw_event_rx) =
        mpsc::channel::<Result<LogEvent, LogSourceError>>(cfg.fwd.buffer_size.max(1));

    let sem = build_log_request_semaphore(cfg.fwd.max_log_requests);

    let mut follow_lim: Option<(mpsc::Sender<()>, mpsc::Receiver<()>)> = if cfg.follow {
        Some(mpsc::channel(1))
    } else {
        None
    };
    let follow_limit_notifier = follow_lim.as_ref().map(|(s, _)| s.clone());

    let h_mux = spawn_mux_task(mux_rx, raw_event_tx);

    let metrics = LossyMetrics::new();
    let metrics_rep = metrics.clone();
    let rep_token = cfg.root_token.clone();
    tokio::spawn(async move {
        metrics_rep.cumulative_reporter(rep_token).await;
    });

    let render_cap = 128usize;
    let (render_tx, render_rx) = mpsc::channel::<RenderCommand>(render_cap);
    let flush_token = cfg.root_token.clone();
    let flush_tx = render_tx.clone();
    tokio::spawn(flush_ticker(
        flush_tx,
        flush_token,
        Duration::from_millis(50),
    ));

    let formatter = build_line_formatter(&cfg.formatter, &cfg.include, &cfg.highlight).map_err(
        |e: RenderSetupError| match e {
            RenderSetupError::HighlightRegex(re) => RunError::ContainerRegex(re),
        },
    )?;

    {
        let _ = &cfg.output; // reserved for future strict validation
    }

    let render_h = spawn_render_task(render_rx, formatter);

    let pipe_h = spawn_pipeline_forward_task(
        raw_event_rx,
        PipelineStages {
            container_incl: container_incl.clone(),
            container_excl: container_excl.clone(),
            includes: includes.clone(),
            excludes: excludes.clone(),
            filter_on: cfg.filter_on,
            jq: jq.clone(),
            level_key: cfg.level_key.clone(),
            color_assign: color_assign_opts(&cfg.formatter, cfg.diff_container),
        },
        render_tx.clone(),
        cfg.fwd.clone(),
        metrics,
        cfg.root_token.clone(),
    );

    let watch_ctx = Arc::new(PodWatchCtx {
        context_name,
        pod_regex,
        pod_condition: cfg.pod_condition.clone(),
        container_discovery: cfg.container_discovery.clone(),
        container_incl,
        container_excl,
        allowed_ns,
        exclude_pod,
        client,
        root_child: cfg.root_token.clone(),
        pod_log: PodLogRequest {
            follow: cfg.follow,
            tail: cfg.tail,
            since_seconds: cfg.since,
            since_time: cfg.since_time,
            previous: cfg.previous,
        },
        sem,
        mux_tx: mux_tx.clone(),
        follow_limit_notifier,
    });

    let root_w = cfg.root_token.clone();
    let mux_tx_w = mux_tx.clone();
    drop(mux_tx);
    let watch_h = tokio::spawn({
        let watch_ctx = Arc::clone(&watch_ctx);
        async move {
            let mut active: HashSet<SourceKey> = HashSet::new();
            let mut tokens: HashMap<SourceKey, CancellationToken> = HashMap::new();
            let mut pending_pods: Vec<Pod> = Vec::new();
            tokio::pin!(w);
            loop {
                tokio::select! {
                    _ = root_w.cancelled() => break,
                    ev = w.next() => {
                        let Some(ev) = ev else { break };
                        let ev = match ev {
                            Ok(e) => e,
                            Err(e) => { tracing::warn!(?e, "watch"); continue; }
                        };
                        match ev {
                            Event::Delete(pod) => {
                                handle_watch_delete(
                                    pod,
                                    &mut active,
                                    &mut tokens,
                                    &mux_tx_w,
                                    watch_ctx.as_ref(),
                                )
                                .await;
                            }
                            Event::Apply(pod) => {
                                handle_watch_apply(pod, &mut active, &mut tokens, &watch_ctx).await;
                            }
                            Event::Init => {
                                pending_pods.clear();
                            }
                            Event::InitApply(pod) => {
                                pending_pods.push(pod);
                            }
                            Event::InitDone => {
                                handle_init_done(
                                    &mut pending_pods,
                                    &mut active,
                                    &mut tokens,
                                    &watch_ctx,
                                )
                                .await;
                            }
                        }
                    }
                }
            }
            drop(mux_tx_w);
        }
    });

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

    Ok(RunOutcome {
        had_source_errors: false,
    })
}
