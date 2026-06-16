use std::sync::Arc;

use futures::{Stream, StreamExt};
use k8s_openapi::api::core::v1::Pod;
use kube::runtime::watcher::Event;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use super::mux::MuxCmd;
use super::registry::PodStreamRegistry;
use super::watch_ctx::PodWatchCtx;
use crate::source::pod_meta::PodLocator;

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
                            watch_ctx
                                .attach
                                .pod_meta
                                .remove_pod(&watch_ctx.admission.context_name(), &pod)
                                .await;
                            registry
                                .remove_pod(&pod, watch_ctx.as_ref(), &mux_tx_w)
                                .await;
                        }
                        Event::Apply(pod) => {
                            if !watch_ctx.admission.admit_pod(&pod) {
                                watch_ctx
                                    .attach
                                    .pod_meta
                                    .remove_pod(&watch_ctx.admission.context_name(), &pod)
                                    .await;
                                registry
                                    .remove_pod(&pod, watch_ctx.as_ref(), &mux_tx_w)
                                    .await;
                            } else {
                                watch_ctx
                                    .attach
                                    .pod_meta
                                    .update_from_pod(&watch_ctx.admission.context_name(), &pod)
                                    .await;
                                let desired = watch_ctx
                                    .admission
                                    .admit_streams(&pod)
                                    .into_iter()
                                    .collect();
                                registry
                                    .reconcile_pod(&pod, desired, &watch_ctx)
                                    .await;
                            }
                        }
                        Event::Init => {
                            pending_pods.clear();
                            watch_ctx.attach.pod_meta.clear().await;
                        }
                        Event::InitApply(pod) => {
                            watch_ctx
                                .attach
                                .pod_meta
                                .update_from_pod(&watch_ctx.admission.context_name(), &pod)
                                .await;
                            pending_pods.push(pod);
                        }
                        Event::InitDone => {
                            let snap = watch_ctx.admission.collect_snapshot(std::mem::take(
                                &mut pending_pods,
                            ));
                            let keep = snap
                                .iter()
                                .map(PodLocator::from_source_key)
                                .collect();
                            watch_ctx.attach.pod_meta.prune(&keep).await;
                            registry.reconcile_snapshot(snap, &watch_ctx).await;
                            if !watch_ctx.attach.pod_log.follow {
                                // no-follow: exit after initial list reconcile completes.
                                break;
                            }
                        }
                    }
                }
            }
        }
        drop(mux_tx_w);
    })
}
