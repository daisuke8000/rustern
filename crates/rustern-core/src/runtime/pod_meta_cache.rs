//! Watch-side cache of per-pod metadata for attach-time [`SourceMeta`] enrichment.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use k8s_openapi::api::core::v1::Pod;
use tokio::sync::RwLock;

use super::watch_ctx::PodWatchCtx;
use crate::source::SourceKey;
use crate::source::pod_meta::{PodLocator, PodMetaSnapshot, pod_meta_snapshot_from_pod};

pub(crate) fn new_pod_meta_cache() -> Arc<RwLock<HashMap<PodLocator, PodMetaSnapshot>>> {
    Arc::new(RwLock::new(HashMap::new()))
}

pub(crate) async fn update_pod_meta_cache(ctx: &PodWatchCtx, pod: &Pod) {
    let Some(locator) = PodLocator::try_from_pod(&ctx.admission.context_name, pod) else {
        tracing::warn!(
            pod = ?pod.metadata.name,
            namespace = ?pod.metadata.namespace,
            "pod missing required metadata for pod_meta cache update"
        );
        return;
    };
    let snapshot = pod_meta_snapshot_from_pod(pod);
    let mut cache = ctx.attach.pod_meta.write().await;
    cache.insert(locator, snapshot);
}

pub(crate) async fn clear_pod_meta_cache(ctx: &PodWatchCtx) {
    ctx.attach.pod_meta.write().await.clear();
}

pub(crate) async fn prune_pod_meta_cache(ctx: &PodWatchCtx, keep: &HashSet<PodLocator>) {
    let mut cache = ctx.attach.pod_meta.write().await;
    cache.retain(|loc, _| keep.contains(loc));
}

pub(crate) async fn remove_pod_meta_cache(ctx: &PodWatchCtx, pod: &Pod) {
    let Some(locator) = PodLocator::try_from_pod(&ctx.admission.context_name, pod) else {
        tracing::warn!(
            pod = ?pod.metadata.name,
            namespace = ?pod.metadata.namespace,
            "pod missing required metadata for pod_meta cache removal"
        );
        return;
    };
    let mut cache = ctx.attach.pod_meta.write().await;
    cache.remove(&locator);
}

pub(crate) async fn lookup_pod_meta(ctx: &PodWatchCtx, key: &SourceKey) -> PodMetaSnapshot {
    let locator = PodLocator::from_source_key(key);
    let cache = ctx.attach.pod_meta.read().await;
    if let Some(snapshot) = cache.get(&locator) {
        return snapshot.clone();
    }
    tracing::debug!(?locator, "pod_meta cache miss");
    PodMetaSnapshot::default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    use crate::runtime::test_support::{TestOrchestratorBuilder, TestOrchestratorFixture};
    use crate::source::{ContextName, SourceKey};
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;

    fn test_pod() -> Pod {
        let mut labels = BTreeMap::new();
        labels.insert("tier".into(), "api".into());
        Pod {
            metadata: ObjectMeta {
                name: Some("pod-a".into()),
                namespace: Some("ns".into()),
                uid: Some("uid-1".into()),
                labels: Some(labels),
                ..Default::default()
            },
            spec: Some(k8s_openapi::api::core::v1::PodSpec {
                node_name: Some("worker-3".into()),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    fn test_ctx(
        cache: Arc<RwLock<HashMap<PodLocator, PodMetaSnapshot>>>,
    ) -> TestOrchestratorFixture {
        let mut fixture = TestOrchestratorBuilder::new().build();
        fixture.ctx_mut().attach.pod_meta = cache;
        fixture
    }

    #[tokio::test]
    async fn cache_lookup_returns_snapshot_after_update() {
        let cache = new_pod_meta_cache();
        let fixture = test_ctx(cache);
        let pod = test_pod();
        update_pod_meta_cache(&fixture, &pod).await;

        let key = SourceKey {
            context: ContextName("ctx".into()),
            namespace: "ns".into(),
            pod: "pod-a".into(),
            container: "app".into(),
            uid: "uid-1".into(),
        };
        let snap = lookup_pod_meta(&fixture, &key).await;
        assert_eq!(snap.node.as_deref(), Some("worker-3"));
        assert_eq!(snap.labels.0.get("tier").map(String::as_str), Some("api"));
    }

    #[tokio::test]
    async fn cache_lookup_defaults_when_pod_unknown() {
        let cache = new_pod_meta_cache();
        let fixture = test_ctx(cache);
        let key = SourceKey {
            context: ContextName("ctx".into()),
            namespace: "ns".into(),
            pod: "missing".into(),
            container: "app".into(),
            uid: "uid-x".into(),
        };
        let snap = lookup_pod_meta(&fixture, &key).await;
        assert!(snap.node.is_none());
        assert!(snap.labels.0.is_empty());
    }

    #[tokio::test]
    async fn prune_pod_meta_cache_drops_stale_locators() {
        let cache = new_pod_meta_cache();
        let fixture = test_ctx(cache);
        update_pod_meta_cache(&fixture, &test_pod()).await;

        let stale_pod = Pod {
            metadata: ObjectMeta {
                name: Some("gone".into()),
                namespace: Some("ns".into()),
                uid: Some("uid-gone".into()),
                ..Default::default()
            },
            ..Default::default()
        };
        update_pod_meta_cache(&fixture, &stale_pod).await;

        let keep: HashSet<PodLocator> =
            HashSet::from([
                PodLocator::try_from_pod(&ContextName("ctx".into()), &test_pod()).expect("locator"),
            ]);
        prune_pod_meta_cache(&fixture, &keep).await;

        let active_key = SourceKey {
            context: ContextName("ctx".into()),
            namespace: "ns".into(),
            pod: "pod-a".into(),
            container: "app".into(),
            uid: "uid-1".into(),
        };
        let stale_key = SourceKey {
            context: ContextName("ctx".into()),
            namespace: "ns".into(),
            pod: "gone".into(),
            container: "app".into(),
            uid: "uid-gone".into(),
        };
        assert_ne!(
            lookup_pod_meta(&fixture, &active_key).await,
            PodMetaSnapshot::default()
        );
        assert_eq!(
            lookup_pod_meta(&fixture, &stale_key).await,
            PodMetaSnapshot::default()
        );
    }

    #[tokio::test]
    async fn remove_pod_meta_cache_drops_entry() {
        let cache = new_pod_meta_cache();
        let fixture = test_ctx(cache);
        let pod = test_pod();
        update_pod_meta_cache(&fixture, &pod).await;
        remove_pod_meta_cache(&fixture, &pod).await;

        let key = SourceKey {
            context: ContextName("ctx".into()),
            namespace: "ns".into(),
            pod: "pod-a".into(),
            container: "app".into(),
            uid: "uid-1".into(),
        };
        assert_eq!(
            lookup_pod_meta(&fixture, &key).await,
            PodMetaSnapshot::default()
        );
    }
}
