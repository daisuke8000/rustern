//! Pod admission, metadata, and stream registry lifecycle in one place.
//!
//! Encapsulates ordering constraints: admit → meta update → reconcile on apply;
//! meta removal paired with registry drop on delete; snapshot init runs prune.

use std::sync::Arc;

use k8s_openapi::api::core::v1::Pod;
use tokio::sync::mpsc;

use super::mux::MuxCmd;
use super::registry::PodStreamRegistry;
use super::watch_ctx::PodWatchCtx;
use crate::source::pod_meta::PodLocator;

pub(crate) struct PodLifecycle {
    registry: PodStreamRegistry,
    pending_pods: Vec<Pod>,
}

impl PodLifecycle {
    pub(crate) fn new() -> Self {
        Self {
            registry: PodStreamRegistry::new(),
            pending_pods: Vec::new(),
        }
    }

    pub(crate) async fn on_delete(
        &mut self,
        pod: &Pod,
        ctx: &PodWatchCtx,
        mux_tx: &mpsc::Sender<MuxCmd>,
    ) {
        ctx.attach
            .pod_meta
            .remove_pod(ctx.admission.context_name(), pod)
            .await;
        self.registry.remove_pod(pod, ctx, mux_tx).await;
    }

    pub(crate) async fn on_apply(
        &mut self,
        pod: &Pod,
        ctx: &Arc<PodWatchCtx>,
        mux_tx: &mpsc::Sender<MuxCmd>,
    ) {
        if !ctx.admission.admit_pod(pod) {
            self.on_delete(pod, ctx.as_ref(), mux_tx).await;
            return;
        }
        ctx.attach
            .pod_meta
            .update_from_pod(ctx.admission.context_name(), pod)
            .await;
        let desired = ctx.admission.admit_streams(pod).into_iter().collect();
        self.registry.reconcile_pod(pod, desired, ctx).await;
    }

    pub(crate) async fn on_init_begin(&mut self, ctx: &PodWatchCtx) {
        self.pending_pods.clear();
        ctx.attach.pod_meta.clear().await;
    }

    pub(crate) async fn on_init_apply(&mut self, pod: Pod, ctx: &PodWatchCtx) {
        ctx.attach
            .pod_meta
            .update_from_pod(ctx.admission.context_name(), &pod)
            .await;
        self.pending_pods.push(pod);
    }

    pub(crate) async fn on_init_snapshot(&mut self, pods: Vec<Pod>, ctx: &Arc<PodWatchCtx>) {
        for pod in &pods {
            ctx.attach
                .pod_meta
                .update_from_pod(ctx.admission.context_name(), pod)
                .await;
        }
        let snap = ctx.admission.collect_snapshot(pods);
        let keep = snap.iter().map(PodLocator::from_source_key).collect();
        ctx.attach.pod_meta.prune(&keep).await;
        self.registry.reconcile_snapshot(snap, ctx).await;
    }

    pub(crate) async fn on_init_done(&mut self, ctx: &Arc<PodWatchCtx>) {
        let pods = std::mem::take(&mut self.pending_pods);
        self.on_init_snapshot(pods, ctx).await;
    }

    #[cfg(test)]
    pub(crate) fn active_keys(&self) -> &std::collections::HashSet<crate::source::SourceKey> {
        self.registry.active_keys()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::test_support::{TestOrchestratorBuilder, TestOrchestratorFixture};
    use crate::source::ContextName;
    use crate::source::SourceKey;
    use k8s_openapi::api::core::v1::{
        Container, ContainerState, ContainerStateRunning, ContainerStatus, PodSpec, PodStatus,
    };
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;

    fn sample_key(container: &str, uid: &str) -> SourceKey {
        SourceKey {
            context: ContextName("ctx".into()),
            namespace: "ns".into(),
            pod: "pod-a".into(),
            container: container.into(),
            uid: uid.into(),
        }
    }

    fn test_pod(name: &str, uid: &str, containers: &[&str]) -> Pod {
        Pod {
            metadata: ObjectMeta {
                name: Some(name.into()),
                namespace: Some("ns".into()),
                uid: Some(uid.into()),
                ..Default::default()
            },
            spec: Some(PodSpec {
                containers: containers
                    .iter()
                    .map(|n| Container {
                        name: n.to_string(),
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            }),
            status: Some(PodStatus {
                container_statuses: Some(
                    containers
                        .iter()
                        .map(|n| ContainerStatus {
                            name: n.to_string(),
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

    fn test_ctx() -> (
        TestOrchestratorFixture,
        mpsc::Sender<MuxCmd>,
        mpsc::Receiver<MuxCmd>,
    ) {
        let (mux_tx, mux_rx) = mpsc::channel(8);
        let fixture = TestOrchestratorBuilder::new()
            .mux_tx(mux_tx.clone())
            .build();
        (fixture, mux_tx, mux_rx)
    }

    #[tokio::test]
    async fn on_init_snapshot_admits_and_prunes_meta() {
        let (fixture, mux_tx, _mux_rx) = test_ctx();
        let ctx = fixture.arc();
        let mut lifecycle = PodLifecycle::new();

        let admitted = test_pod("pod-a", "uid-a", &["app"]);
        let mut no_streams = test_pod("idle", "uid-idle", &["app"]);
        no_streams.status = None;
        lifecycle
            .on_init_snapshot(vec![admitted.clone(), no_streams], &ctx)
            .await;

        assert_eq!(lifecycle.active_keys().len(), 1);
        assert!(
            lifecycle
                .active_keys()
                .contains(&sample_key("app", "uid-a"))
        );

        let stale_key = SourceKey {
            pod: "idle".into(),
            uid: "uid-idle".into(),
            container: "app".into(),
            ..sample_key("app", "uid-idle")
        };
        assert_eq!(
            ctx.attach.pod_meta.lookup(&stale_key).await,
            Default::default()
        );
        drop(mux_tx);
    }

    #[tokio::test]
    async fn on_apply_adds_streams_for_admitted_pod() {
        let (fixture, mux_tx, _mux_rx) = test_ctx();
        let ctx = fixture.arc();
        let mut lifecycle = PodLifecycle::new();

        let pod = test_pod("pod-a", "uid-a", &["app"]);
        lifecycle.on_apply(&pod, &ctx, &mux_tx).await;

        assert!(
            lifecycle
                .active_keys()
                .contains(&sample_key("app", "uid-a"))
        );
    }

    #[tokio::test]
    async fn on_apply_drops_streams_when_pod_no_longer_admitted() {
        let (fixture, mux_tx, mut mux_rx) = test_ctx();
        let ctx = fixture.arc();
        let mut lifecycle = PodLifecycle::new();

        let pod = test_pod("pod-a", "uid-a", &["app"]);
        lifecycle.on_apply(&pod, &ctx, &mux_tx).await;
        while mux_rx.try_recv().is_ok() {}

        let mut stale = pod.clone();
        stale.status = None;
        lifecycle.on_apply(&stale, &ctx, &mux_tx).await;

        assert!(lifecycle.active_keys().is_empty());
        assert!(matches!(mux_rx.try_recv(), Ok(MuxCmd::Remove(_))));
    }

    #[tokio::test]
    async fn on_delete_removes_streams_and_meta() {
        let (fixture, mux_tx, mut mux_rx) = test_ctx();
        let ctx = fixture.arc();
        let mut lifecycle = PodLifecycle::new();

        let pod = test_pod("pod-a", "uid-a", &["app"]);
        lifecycle.on_apply(&pod, &ctx, &mux_tx).await;
        while mux_rx.try_recv().is_ok() {}

        lifecycle.on_delete(&pod, &ctx, &mux_tx).await;

        assert!(lifecycle.active_keys().is_empty());
        assert!(matches!(mux_rx.try_recv(), Ok(MuxCmd::Remove(_))));
        assert_eq!(
            ctx.attach
                .pod_meta
                .lookup(&sample_key("app", "uid-a"))
                .await,
            Default::default()
        );
    }

    #[tokio::test]
    async fn init_sequence_matches_snapshot_path() {
        let (fixture, _mux_tx, _mux_rx) = test_ctx();
        let ctx = fixture.arc();
        let mut lifecycle = PodLifecycle::new();

        lifecycle.on_init_begin(&ctx).await;
        lifecycle
            .on_init_apply(test_pod("pod-a", "uid-a", &["app"]), &ctx)
            .await;
        lifecycle
            .on_init_apply(test_pod("pod-b", "uid-b", &["app"]), &ctx)
            .await;
        lifecycle.on_init_done(&ctx).await;

        assert_eq!(lifecycle.active_keys().len(), 2);
    }
}
