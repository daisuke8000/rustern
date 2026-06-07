use std::sync::Arc;

use futures::{Stream, StreamExt};
use k8s_openapi::api::core::v1::Pod;
use kube::runtime::watcher::Event;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use super::mux::MuxCmd;
use super::pod_meta_cache::{
    clear_pod_meta_cache, prune_pod_meta_cache, remove_pod_meta_cache, update_pod_meta_cache,
};
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
                            remove_pod_meta_cache(watch_ctx.as_ref(), &pod).await;
                            registry
                                .remove_pod(&pod, watch_ctx.as_ref(), &mux_tx_w)
                                .await;
                        }
                        Event::Apply(pod) => {
                            if !watch_ctx.admission.admit_pod(&pod) {
                                remove_pod_meta_cache(watch_ctx.as_ref(), &pod).await;
                                registry
                                    .remove_pod(&pod, watch_ctx.as_ref(), &mux_tx_w)
                                    .await;
                            } else {
                                update_pod_meta_cache(watch_ctx.as_ref(), &pod).await;
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
                            clear_pod_meta_cache(watch_ctx.as_ref()).await;
                        }
                        Event::InitApply(pod) => {
                            update_pod_meta_cache(watch_ctx.as_ref(), &pod).await;
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
                            prune_pod_meta_cache(watch_ctx.as_ref(), &keep).await;
                            registry.reconcile_snapshot(snap, &watch_ctx).await;
                        }
                    }
                }
            }
        }
        drop(mux_tx_w);
    })
}

#[cfg(test)]
mod tests {
    use crate::runtime::test_support::TestOrchestratorBuilder;
    use k8s_openapi::api::core::v1::{
        Container, ContainerState, ContainerStateRunning, ContainerStatus, Pod, PodSpec, PodStatus,
    };
    use kube::api::ObjectMeta;
    use std::collections::HashSet;

    fn test_pod(containers: &[&str]) -> Pod {
        Pod {
            metadata: ObjectMeta {
                name: Some("p".into()),
                namespace: Some("ns".into()),
                uid: Some("uid-1".into()),
                ..Default::default()
            },
            spec: Some(PodSpec {
                containers: containers
                    .iter()
                    .map(|name| Container {
                        name: name.to_string(),
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            }),
            status: Some(PodStatus {
                container_statuses: Some(
                    containers
                        .iter()
                        .map(|name| ContainerStatus {
                            name: name.to_string(),
                            ready: true,
                            state: Some(ContainerState {
                                running: Some(ContainerStateRunning::default()),
                                ..Default::default()
                            }),
                            ..Default::default()
                        })
                        .collect(),
                ),
                phase: Some("Running".into()),
                ..Default::default()
            }),
        }
    }

    #[tokio::test]
    async fn admit_streams_includes_matching_containers() {
        let pod = test_pod(&["app", "sidecar", "istio-proxy"]);
        let fixture = TestOrchestratorBuilder::new()
            .container_incl("app|sidecar")
            .build();
        let keys = fixture.admission.admit_streams(&pod);
        let names: HashSet<_> = keys.iter().map(|k| k.container.as_str()).collect();
        assert_eq!(names, HashSet::from(["app", "sidecar"]));
    }

    #[tokio::test]
    async fn admit_streams_excludes_matching_containers() {
        let pod = test_pod(&["app", "sidecar", "istio-proxy"]);
        let fixture = TestOrchestratorBuilder::new()
            .container_excl(&["istio-proxy"])
            .build();
        let keys = fixture.admission.admit_streams(&pod);
        let names: HashSet<_> = keys.iter().map(|k| k.container.as_str()).collect();
        assert_eq!(names, HashSet::from(["app", "sidecar"]));
    }

    #[tokio::test]
    async fn collect_snapshot_applies_container_filters() {
        let pod = test_pod(&["app", "istio-proxy"]);
        let fixture = TestOrchestratorBuilder::new().container_incl("app").build();
        let snap = fixture.admission.collect_snapshot(vec![pod]);
        assert_eq!(snap.len(), 1);
        assert_eq!(snap.iter().next().unwrap().container, "app");
    }
}
