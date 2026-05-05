//! Watch → mux → pipeline → stdout lifecycle.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;

use k8s_openapi::api::core::v1::Pod;
use kube::Api;
use kube::Client;
use kube::api::ListParams;
use kube::runtime::watcher::{Config as WatchConfig, Event, watcher};
use regex::Regex;
use tokio::sync::{Semaphore, mpsc};
use tokio::task::JoinHandle;
use tokio_stream::StreamMap;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;

use super::config::{CoreRunConfig, FormatterChoice, RunError, RunOutcome, RuntimeFwdConfig};
use super::forward::{LossyMetrics, build_log_request_semaphore, forward_to_render};
use super::pipeline::{PipelineStages, apply_pipeline, compile_list};
use crate::discovery::context::{build_client, pick_context_name, resolve_kubeconfig};
use crate::discovery::pod_watcher::{keys_from_pod, reconcile};
use crate::discovery::resource::{Query, label_selector_for, parse_query};
use crate::pipeline::validate_filter;
use crate::render::default_renderer::DefaultLineFormatter;
use crate::render::json_renderer::JsonLineFormatter;
use crate::render::raw_renderer::RawLineFormatter;
use crate::render::{LineFormatter, RenderCommand, flush_ticker, render_task};
use crate::source::pod_log::PodLogSource;
use crate::source::{
    BoxedLogStream, ContextName, Labels, LogEvent, LogSource, LogSourceError, SourceKey,
    SourceKind, SourceMeta,
};

enum MuxCmd {
    Add(SourceKey, BoxedLogStream),
    Remove(SourceKey),
}

fn line_formatter(choice: &FormatterChoice) -> Arc<dyn LineFormatter> {
    match choice {
        FormatterChoice::Default {
            show_timestamps,
            color_enabled,
        } => Arc::new(DefaultLineFormatter {
            show_timestamps: *show_timestamps,
            color_enabled: *color_enabled,
        }),
        FormatterChoice::Json => Arc::new(JsonLineFormatter),
        FormatterChoice::Raw => Arc::new(RawLineFormatter),
    }
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
    container_incl: Regex,
    container_excl: Option<Regex>,
    allowed_ns: Option<HashSet<String>>,
    exclude_pod: Vec<Regex>,
    mux_tx: mpsc::Sender<MuxCmd>,
    client: Client,
    root_child: CancellationToken,
    follow: bool,
    tail: Option<i64>,
    since: Option<i64>,
    sem: Arc<Semaphore>,
}

struct AttachPodLogParams {
    ctx: Arc<PodWatchCtx>,
    meta: SourceMeta,
    pod_token: CancellationToken,
    key: SourceKey,
}

async fn attach_pod_log_stream(p: AttachPodLogParams) {
    let Ok(_permit) = Arc::clone(&p.ctx.sem).acquire_owned().await else {
        return;
    };
    let client = p.ctx.client.clone();
    match PodLogSource::start(
        client,
        p.meta,
        p.pod_token,
        p.ctx.follow,
        p.ctx.tail,
        p.ctx.since,
    )
    .await
    {
        Ok(src) => {
            let stream = Box::new(src).into_stream();
            let _ = p.ctx.mux_tx.send(MuxCmd::Add(p.key, stream)).await;
        }
        Err(e) => tracing::warn!(?e, "pod log start"),
    }
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

async fn handle_watch_delete(
    pod: Pod,
    active: &mut HashSet<SourceKey>,
    tokens: &mut HashMap<SourceKey, CancellationToken>,
    mux_tx: &mpsc::Sender<MuxCmd>,
    context_name: &ContextName,
) {
    let keys = keys_from_pod(&pod, context_name);
    for k in keys {
        if let Some(t) = tokens.remove(&k) {
            t.cancel();
        }
        active.remove(&k);
        let _ = mux_tx.send(MuxCmd::Remove(k)).await;
    }
}

async fn handle_watch_apply(
    pod: Pod,
    active: &mut HashSet<SourceKey>,
    tokens: &mut HashMap<SourceKey, CancellationToken>,
    ctx: &Arc<PodWatchCtx>,
) {
    let name = pod.metadata.name.clone().unwrap_or_default();
    if let Some(allowed) = &ctx.allowed_ns {
        let ns = pod.metadata.namespace.as_deref().unwrap_or("");
        if !allowed.contains(ns) {
            return;
        }
    }
    if ctx.exclude_pod.iter().any(|re| re.is_match(&name)) {
        return;
    }
    if let Some(re) = &ctx.pod_regex
        && !re.is_match(&name)
    {
        return;
    }
    let keys: Vec<_> = keys_from_pod(&pod, &ctx.context_name)
        .into_iter()
        .filter(|k| ctx.container_incl.is_match(&k.container))
        .filter(|k| {
            ctx.container_excl
                .as_ref()
                .map(|r| !r.is_match(&k.container))
                .unwrap_or(true)
        })
        .collect();
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
        let name = pod.metadata.name.clone().unwrap_or_default();
        if let Some(allowed) = &ctx.allowed_ns {
            let ns = pod.metadata.namespace.as_deref().unwrap_or("");
            if !allowed.contains(ns) {
                continue;
            }
        }
        if ctx.exclude_pod.iter().any(|re| re.is_match(&name)) {
            continue;
        }
        if let Some(re) = &ctx.pod_regex
            && !re.is_match(&name)
        {
            continue;
        }
        for k in keys_from_pod(&pod, &ctx.context_name) {
            if !ctx.container_incl.is_match(&k.container) {
                continue;
            }
            if ctx
                .container_excl
                .as_ref()
                .is_some_and(|r| r.is_match(&k.container))
            {
                continue;
            }
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
    let snap = collect_keys_snapshot(std::mem::take(pending_pods), ctx);
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

/// Merge user field selector fragments with stern-style node pinning (`spec.nodeName`).
fn combined_field_selector(cfg: &CoreRunConfig) -> Option<String> {
    let mut parts = Vec::new();
    if let Some(fs) = cfg.field_selector.as_ref() {
        let t = fs.trim();
        if !t.is_empty() {
            parts.push(t.to_string());
        }
    }
    if let Some(n) = cfg.node.as_ref() {
        let t = n.trim();
        if !t.is_empty() {
            parts.push(format!("spec.nodeName={t}"));
        }
    }
    if parts.is_empty() {
        None
    } else {
        Some(parts.join(","))
    }
}

/// Main entry: watcher → `StreamMap` → pipeline → stdout.
pub async fn run(cfg: CoreRunConfig) -> Result<RunOutcome, RunError> {
    let client = build_client(&cfg.context).await?;
    let query_src = if cfg.selector.is_some() && cfg.query == "." {
        ".*"
    } else {
        cfg.query.as_str()
    };
    let q = parse_query(query_src)?;
    let pod_regex = match &q {
        Query::PodNameRegex(re) => Some(Regex::new(re)?),
        Query::LabelSelector { .. } => None,
    };
    let kind_name = match &q {
        Query::LabelSelector { kind, name } => Some((*kind, name.clone())),
        Query::PodNameRegex(_) => None,
    };

    let mut list = ListParams::default();
    if let Some(sel) = cfg.selector.as_ref() {
        list = list.labels(sel);
    } else if let Some((kind, name)) = &kind_name {
        list = list.labels(&label_selector_for(*kind, name));
    }
    if let Some(fs) = combined_field_selector(&cfg) {
        list = list.fields(&fs);
    }

    let watch_cfg = {
        let mut wc = WatchConfig::default();
        if let Some(ls) = list.label_selector.as_deref() {
            wc = wc.labels(ls);
        }
        if let Some(fs) = list.field_selector.as_deref() {
            wc = wc.fields(fs);
        }
        wc
    };

    let kube_cfg = resolve_kubeconfig(&cfg.context)?;
    let ctx_name = pick_context_name(&kube_cfg, &cfg.context)?;
    let context_name = ContextName(ctx_name.to_string());

    let container_incl = Regex::new(&cfg.container)?;
    let container_excl = match &cfg.exclude_container {
        Some(p) => Some(Regex::new(p)?),
        None => None,
    };
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

    let formatter = line_formatter(&cfg.formatter);

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
        },
        render_tx.clone(),
        cfg.fwd.clone(),
        metrics,
        cfg.root_token.clone(),
    );

    let watch_ctx = Arc::new(PodWatchCtx {
        context_name,
        pod_regex,
        container_incl,
        container_excl,
        allowed_ns,
        exclude_pod,
        client,
        root_child: cfg.root_token.clone(),
        follow: cfg.follow,
        tail: cfg.tail,
        since: cfg.since,
        sem,
        mux_tx: mux_tx.clone(),
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
                                    &watch_ctx.context_name,
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

    cfg.root_token.cancelled().await;
    let _ = render_tx.send(RenderCommand::Shutdown).await;
    let _ = tokio::time::timeout(Duration::from_millis(150), render_h).await;

    watch_h.abort();
    h_mux.abort();
    pipe_h.abort();

    Ok(RunOutcome {
        had_source_errors: false,
    })
}
