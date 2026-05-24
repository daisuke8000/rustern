use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use futures::{Stream, StreamExt};
use k8s_openapi::api::core::v1::Pod;
use kube::Client;
use kube::runtime::watcher::Event;
use regex::Regex;
use tokio::sync::{Semaphore, mpsc};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use super::attach::spawn_attach_pod_log;
use super::mux::MuxCmd;
use crate::discovery::pod_condition::{PodConditionFilter, pod_matches_condition};
use crate::discovery::pod_watcher::{ContainerDiscoverOpts, keys_from_pod, reconcile};
use crate::source::pod_log::PodLogRequest;
use crate::source::{ContextName, SourceKey};

pub(crate) struct PodWatchCtx {
    pub(crate) context_name: ContextName,
    pub(crate) pod_regex: Option<Regex>,
    pub(crate) pod_condition: Option<PodConditionFilter>,
    pub(crate) container_discovery: ContainerDiscoverOpts,
    pub(crate) container_incl: Regex,
    pub(crate) container_excl: Vec<Regex>,
    pub(crate) allowed_ns: Option<HashSet<String>>,
    pub(crate) exclude_pod: Vec<Regex>,
    pub(crate) mux_tx: mpsc::Sender<MuxCmd>,
    pub(crate) client: Client,
    pub(crate) root_child: CancellationToken,
    pub(crate) pod_log: PodLogRequest,
    pub(crate) sem: Arc<Semaphore>,
    pub(crate) follow_limit_notifier: Option<mpsc::Sender<()>>,
}

fn filtered_stream_keys(pod: &Pod, ctx: &PodWatchCtx) -> Vec<SourceKey> {
    keys_from_pod(pod, &ctx.context_name, &ctx.container_discovery)
        .into_iter()
        .filter(|k| ctx.container_incl.is_match(&k.container))
        .filter(|k| !ctx.container_excl.iter().any(|r| r.is_match(&k.container)))
        .collect()
}

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

pub(crate) fn spawn_watch_task<S>(
    w: S,
    root_w: CancellationToken,
    mux_tx_w: mpsc::Sender<MuxCmd>,
    watch_ctx: Arc<PodWatchCtx>,
) -> JoinHandle<()>
where
    S: Stream<Item = Result<Event<Pod>, kube::runtime::watcher::Error>> + Send + 'static,
{
    tokio::spawn(async move {
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
    })
}
