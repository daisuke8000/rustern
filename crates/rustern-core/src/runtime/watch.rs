use std::collections::HashSet;
use std::sync::Arc;

use futures::{Stream, StreamExt};
use k8s_openapi::api::core::v1::Pod;
use kube::runtime::watcher::Event;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use super::mux::MuxCmd;
use super::pod_meta_cache::{remove_pod_meta_cache, update_pod_meta_cache};
use super::registry::PodStreamRegistry;
use super::watch_ctx::PodWatchCtx;
use crate::discovery::pod_condition::pod_matches_condition;
use crate::discovery::pod_watcher::keys_from_pod;
use crate::source::SourceKey;

fn filtered_stream_keys(pod: &Pod, ctx: &PodWatchCtx) -> Vec<SourceKey> {
    keys_from_pod(pod, &ctx.context_name, &ctx.container_discovery)
        .into_iter()
        .filter(|k| ctx.container_incl.is_match(&k.container))
        .filter(|k| !ctx.container_excl.iter().any(|r| r.is_match(&k.container)))
        .collect()
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
        let mut registry = PodStreamRegistry::new();
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
                            remove_pod_meta_cache(watch_ctx.as_ref(), &pod).await;
                            registry
                                .remove_pod(&pod, watch_ctx.as_ref(), &mux_tx_w)
                                .await;
                        }
                        Event::Apply(pod) => {
                            update_pod_meta_cache(watch_ctx.as_ref(), &pod).await;
                            if !pod_passes_watch_filters(&pod, watch_ctx.as_ref()) {
                                registry
                                    .remove_pod(&pod, watch_ctx.as_ref(), &mux_tx_w)
                                    .await;
                            } else {
                                let desired: HashSet<SourceKey> =
                                    filtered_stream_keys(&pod, watch_ctx.as_ref())
                                        .into_iter()
                                        .collect();
                                registry
                                    .reconcile_pod(&pod, desired, &watch_ctx)
                                    .await;
                            }
                        }
                        Event::Init => {
                            pending_pods.clear();
                        }
                        Event::InitApply(pod) => {
                            update_pod_meta_cache(watch_ctx.as_ref(), &pod).await;
                            pending_pods.push(pod);
                        }
                        Event::InitDone => {
                            let snap = collect_keys_snapshot(
                                std::mem::take(&mut pending_pods),
                                watch_ctx.as_ref(),
                            );
                            registry.reconcile_snapshot(snap, &watch_ctx).await;
                        }
                    }
                }
            }
        }
        drop(mux_tx_w);
    })
}
