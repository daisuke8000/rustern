//! Active pod log stream registry for watch reconciliation.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use k8s_openapi::api::core::v1::Pod;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use super::attach::spawn_attach_pod_log;
use super::mux::MuxCmd;
use super::watch_ctx::PodWatchCtx;
use crate::discovery::reconcile;
use crate::source::SourceKey;

pub(crate) struct PodStreamRegistry {
    active: HashSet<SourceKey>,
    tokens: HashMap<SourceKey, CancellationToken>,
}

impl PodStreamRegistry {
    pub(crate) fn new() -> Self {
        Self {
            active: HashSet::new(),
            tokens: HashMap::new(),
        }
    }

    fn keys_for_pod_locator(
        &self,
        ctx: &PodWatchCtx,
        namespace: &str,
        pod_name: &str,
        uid: Option<&str>,
    ) -> Vec<SourceKey> {
        self.active
            .iter()
            .filter(|k| {
                k.context == *ctx.admission.context_name()
                    && k.namespace == namespace
                    && k.pod == pod_name
                    && uid.is_none_or(|u| k.uid == u)
            })
            .cloned()
            .collect()
    }

    async fn drop_keys(
        &mut self,
        keys: Vec<SourceKey>,
        ctx: &PodWatchCtx,
        mux_tx: &mpsc::Sender<MuxCmd>,
    ) {
        for k in keys {
            if let Some(t) = self.tokens.remove(&k) {
                t.cancel();
            }
            ctx.attach.cursor.forget(&k).await;
            self.active.remove(&k);
            if mux_tx.send(MuxCmd::Remove(k)).await.is_err() {
                tracing::debug!("mux channel closed, skipping remove");
            }
        }
    }

    fn add_key(&mut self, key: SourceKey, ctx: &Arc<PodWatchCtx>) {
        if self.active.insert(key.clone()) {
            let pod_t = ctx.attach.root_child.child_token();
            self.tokens.insert(key.clone(), pod_t.clone());
            spawn_attach_pod_log(ctx, key, pod_t);
        }
    }

    pub(crate) async fn remove_pod(
        &mut self,
        pod: &Pod,
        ctx: &PodWatchCtx,
        mux_tx: &mpsc::Sender<MuxCmd>,
    ) {
        let ns = pod.metadata.namespace.as_deref().unwrap_or("");
        let name = pod.metadata.name.as_deref().unwrap_or("");
        let uid = pod.metadata.uid.as_deref();
        let keys = self.keys_for_pod_locator(ctx, ns, name, uid);
        self.drop_keys(keys, ctx, mux_tx).await;
    }

    pub(crate) async fn reconcile_snapshot(
        &mut self,
        snapshot: HashSet<SourceKey>,
        ctx: &Arc<PodWatchCtx>,
    ) {
        let diff = reconcile(&self.active, &snapshot);
        self.drop_keys(diff.to_drop, ctx.as_ref(), &ctx.attach.mux_tx)
            .await;
        for key in diff.to_add {
            self.add_key(key, ctx);
        }
    }

    pub(crate) async fn reconcile_pod(
        &mut self,
        pod: &Pod,
        desired: HashSet<SourceKey>,
        ctx: &Arc<PodWatchCtx>,
    ) {
        let ns = pod.metadata.namespace.as_deref().unwrap_or("");
        let name = pod.metadata.name.as_deref().unwrap_or("");
        let current: HashSet<SourceKey> = self
            .keys_for_pod_locator(ctx.as_ref(), ns, name, None)
            .into_iter()
            .collect();
        let diff = reconcile(&current, &desired);
        self.drop_keys(diff.to_drop, ctx.as_ref(), &ctx.attach.mux_tx)
            .await;
        for key in diff.to_add {
            self.add_key(key, ctx);
        }
    }

    #[cfg(test)]
    pub(crate) fn active_keys(&self) -> &HashSet<SourceKey> {
        &self.active
    }

    #[cfg(test)]
    pub(crate) fn token_for(&self, key: &SourceKey) -> Option<&CancellationToken> {
        self.tokens.get(key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::test_support::{TestOrchestratorBuilder, TestOrchestratorFixture};
    use crate::source::ContextName;
    use k8s_openapi::api::core::v1::ContainerState;
    use k8s_openapi::api::core::v1::ContainerStateRunning;
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
    use tokio::sync::mpsc;

    fn sample_key(container: &str, uid: &str) -> SourceKey {
        SourceKey {
            context: ContextName("ctx".into()),
            namespace: "ns".into(),
            pod: "pod-a".into(),
            container: container.into(),
            uid: uid.into(),
        }
    }

    fn test_pod(uid: &str, containers: &[&str]) -> Pod {
        Pod {
            metadata: ObjectMeta {
                name: Some("pod-a".into()),
                namespace: Some("ns".into()),
                uid: Some(uid.into()),
                ..Default::default()
            },
            spec: Some(k8s_openapi::api::core::v1::PodSpec {
                containers: containers
                    .iter()
                    .map(|name| k8s_openapi::api::core::v1::Container {
                        name: name.to_string(),
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            }),
            status: Some(k8s_openapi::api::core::v1::PodStatus {
                container_statuses: Some(
                    containers
                        .iter()
                        .map(|name| k8s_openapi::api::core::v1::ContainerStatus {
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

    fn test_ctx(mux_tx: mpsc::Sender<MuxCmd>) -> (TestOrchestratorFixture, Arc<PodWatchCtx>) {
        let fixture = TestOrchestratorBuilder::new().mux_tx(mux_tx).build();
        let ctx = fixture.arc();
        (fixture, ctx)
    }

    #[tokio::test]
    async fn reconcile_pod_drops_removed_container_on_apply() {
        let (mux_tx, mut mux_rx) = mpsc::channel(8);
        let (_fixture, ctx) = test_ctx(mux_tx);
        let mut reg = PodStreamRegistry::new();

        let uid = "uid-1";
        reg.active.insert(sample_key("c1", uid));
        reg.active.insert(sample_key("c2", uid));
        reg.tokens
            .insert(sample_key("c1", uid), CancellationToken::new());
        reg.tokens
            .insert(sample_key("c2", uid), CancellationToken::new());

        let pod = test_pod(uid, &["c2"]);
        let desired: HashSet<SourceKey> = ctx.admission.admit_streams(&pod).into_iter().collect();

        reg.reconcile_pod(&pod, desired, &ctx).await;

        assert!(!reg.active_keys().contains(&sample_key("c1", uid)));
        assert!(reg.active_keys().contains(&sample_key("c2", uid)));
        assert!(reg.token_for(&sample_key("c1", uid)).is_none());
        let removed = mux_rx.try_recv().expect("mux remove");
        assert!(matches!(removed, MuxCmd::Remove(k) if k == sample_key("c1", uid)));
    }

    #[tokio::test]
    async fn reconcile_pod_drops_old_uid_on_rolling_update() {
        let (mux_tx, mut mux_rx) = mpsc::channel(8);
        let (_fixture, ctx) = test_ctx(mux_tx);
        let mut reg = PodStreamRegistry::new();

        reg.active.insert(sample_key("c1", "uid-old"));
        reg.tokens
            .insert(sample_key("c1", "uid-old"), CancellationToken::new());

        let pod = test_pod("uid-new", &["c1"]);
        let desired: HashSet<SourceKey> = ctx.admission.admit_streams(&pod).into_iter().collect();

        reg.reconcile_pod(&pod, desired, &ctx).await;

        assert!(!reg.active_keys().contains(&sample_key("c1", "uid-old")));
        assert!(reg.active_keys().contains(&sample_key("c1", "uid-new")));
        let removed = mux_rx.try_recv().expect("mux remove old uid");
        assert!(matches!(removed, MuxCmd::Remove(k) if k == sample_key("c1", "uid-old")));
    }
}
