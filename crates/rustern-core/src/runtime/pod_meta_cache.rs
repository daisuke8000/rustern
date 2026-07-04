//! Watch-side cache of per-pod metadata for attach-time [`SourceMeta`] enrichment.

use std::collections::hash_map::DefaultHasher;
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use k8s_openapi::api::core::v1::Pod;
use tokio::sync::RwLock;

use crate::source::ContextName;
use crate::source::SourceKey;
use crate::source::pod_meta::{PodLocator, PodMetaSnapshot, pod_meta_snapshot_from_pod};

const SHARD_COUNT: usize = 16;

fn shard_index(key: &PodLocator) -> usize {
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    (hasher.finish() as usize) % SHARD_COUNT
}

/// In-memory pod metadata cache keyed by [`PodLocator`].
#[derive(Clone)]
pub(crate) struct PodMetaCache {
    inner: Arc<Vec<RwLock<HashMap<PodLocator, PodMetaSnapshot>>>>,
}

impl PodMetaCache {
    pub(crate) fn new() -> Self {
        Self {
            inner: Arc::new(
                (0..SHARD_COUNT)
                    .map(|_| RwLock::new(HashMap::new()))
                    .collect(),
            ),
        }
    }

    #[cfg(test)]
    pub(crate) fn with_entry(locator: PodLocator, snapshot: PodMetaSnapshot) -> Self {
        let mut maps: Vec<HashMap<PodLocator, PodMetaSnapshot>> =
            (0..SHARD_COUNT).map(|_| HashMap::new()).collect();
        maps[shard_index(&locator)].insert(locator, snapshot);
        Self {
            inner: Arc::new(maps.into_iter().map(RwLock::new).collect()),
        }
    }

    pub(crate) async fn update_from_pod(&self, context: &ContextName, pod: &Pod) {
        let Some(locator) = PodLocator::try_from_pod(context, pod) else {
            tracing::warn!(
                pod = ?pod.metadata.name,
                namespace = ?pod.metadata.namespace,
                "pod missing required metadata for pod_meta cache update"
            );
            return;
        };
        let snapshot = pod_meta_snapshot_from_pod(pod);
        let shard = shard_index(&locator);
        let shard_lock = &self.inner[shard];
        {
            let cache = shard_lock.read().await;
            if cache.get(&locator).is_some_and(|prev| prev == &snapshot) {
                return;
            }
        }
        let mut cache = shard_lock.write().await;
        if cache.get(&locator).is_some_and(|prev| prev == &snapshot) {
            return;
        }
        cache.insert(locator, snapshot);
    }

    pub(crate) async fn clear(&self) {
        for shard in self.inner.iter() {
            shard.write().await.clear();
        }
    }

    pub(crate) async fn prune(&self, keep: &HashSet<PodLocator>) {
        for shard in self.inner.iter() {
            shard.write().await.retain(|loc, _| keep.contains(loc));
        }
    }

    pub(crate) async fn remove_pod(&self, context: &ContextName, pod: &Pod) {
        let Some(locator) = PodLocator::try_from_pod(context, pod) else {
            tracing::warn!(
                pod = ?pod.metadata.name,
                namespace = ?pod.metadata.namespace,
                "pod missing required metadata for pod_meta cache removal"
            );
            return;
        };
        let shard = shard_index(&locator);
        let mut cache = self.inner[shard].write().await;
        cache.remove(&locator);
    }

    pub(crate) async fn lookup(&self, key: &SourceKey) -> PodMetaSnapshot {
        let locator = PodLocator::from_source_key(key);
        let shard = shard_index(&locator);
        let cache = self.inner[shard].read().await;
        if let Some(snapshot) = cache.get(&locator) {
            return snapshot.clone();
        }
        tracing::debug!(?locator, "pod_meta cache miss");
        PodMetaSnapshot::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    use crate::runtime::test_support::TestOrchestratorBuilder;
    use crate::source::ContextName;
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

    fn pod_named(name: &str, uid: &str) -> Pod {
        Pod {
            metadata: ObjectMeta {
                name: Some(name.into()),
                namespace: Some("ns".into()),
                uid: Some(uid.into()),
                ..Default::default()
            },
            ..Default::default()
        }
    }

    fn context() -> ContextName {
        ContextName("ctx".into())
    }

    #[tokio::test]
    async fn lookup_returns_snapshot_after_update() {
        let cache = PodMetaCache::new();
        let pod = test_pod();
        cache.update_from_pod(&context(), &pod).await;

        let key = SourceKey {
            context: context(),
            namespace: "ns".into(),
            pod: "pod-a".into(),
            container: "app".into(),
            uid: "uid-1".into(),
        };
        let snap = cache.lookup(&key).await;
        assert_eq!(snap.node.as_deref(), Some("worker-3"));
        assert_eq!(snap.labels.0.get("tier").map(String::as_str), Some("api"));
    }

    #[tokio::test]
    async fn lookup_defaults_when_pod_unknown() {
        let cache = PodMetaCache::new();
        let key = SourceKey {
            context: context(),
            namespace: "ns".into(),
            pod: "missing".into(),
            container: "app".into(),
            uid: "uid-x".into(),
        };
        let snap = cache.lookup(&key).await;
        assert!(snap.node.is_none());
        assert!(snap.labels.0.is_empty());
    }

    #[tokio::test]
    async fn prune_drops_stale_locators() {
        let cache = PodMetaCache::new();
        cache.update_from_pod(&context(), &test_pod()).await;

        let stale_pod = Pod {
            metadata: ObjectMeta {
                name: Some("gone".into()),
                namespace: Some("ns".into()),
                uid: Some("uid-gone".into()),
                ..Default::default()
            },
            ..Default::default()
        };
        cache.update_from_pod(&context(), &stale_pod).await;

        let keep: HashSet<PodLocator> =
            HashSet::from([PodLocator::try_from_pod(&context(), &test_pod()).expect("locator")]);
        cache.prune(&keep).await;

        let active_key = SourceKey {
            context: context(),
            namespace: "ns".into(),
            pod: "pod-a".into(),
            container: "app".into(),
            uid: "uid-1".into(),
        };
        let stale_key = SourceKey {
            context: context(),
            namespace: "ns".into(),
            pod: "gone".into(),
            container: "app".into(),
            uid: "uid-gone".into(),
        };
        assert_ne!(cache.lookup(&active_key).await, PodMetaSnapshot::default());
        assert_eq!(cache.lookup(&stale_key).await, PodMetaSnapshot::default());
    }

    #[tokio::test]
    async fn prune_drops_stale_entries_across_shards() {
        let cache = PodMetaCache::new();
        let ctx = context();
        for i in 0..32 {
            cache
                .update_from_pod(&ctx, &pod_named(&format!("pod-{i}"), &format!("uid-{i}")))
                .await;
        }
        cache.update_from_pod(&ctx, &test_pod()).await;

        let keep = HashSet::from([PodLocator::try_from_pod(&ctx, &test_pod()).expect("locator")]);
        cache.prune(&keep).await;

        for i in 0..32 {
            let key = SourceKey {
                context: ctx.clone(),
                namespace: "ns".into(),
                pod: format!("pod-{i}"),
                container: "app".into(),
                uid: format!("uid-{i}"),
            };
            assert_eq!(cache.lookup(&key).await, PodMetaSnapshot::default());
        }

        let active_key = SourceKey {
            context: ctx,
            namespace: "ns".into(),
            pod: "pod-a".into(),
            container: "app".into(),
            uid: "uid-1".into(),
        };
        assert_ne!(cache.lookup(&active_key).await, PodMetaSnapshot::default());
    }

    #[tokio::test]
    async fn remove_pod_drops_entry() {
        let cache = PodMetaCache::new();
        let pod = test_pod();
        cache.update_from_pod(&context(), &pod).await;
        cache.remove_pod(&context(), &pod).await;

        let key = SourceKey {
            context: context(),
            namespace: "ns".into(),
            pod: "pod-a".into(),
            container: "app".into(),
            uid: "uid-1".into(),
        };
        assert_eq!(cache.lookup(&key).await, PodMetaSnapshot::default());
    }

    #[tokio::test]
    async fn update_from_pod_is_idempotent_for_unchanged_snapshot() {
        let cache = PodMetaCache::new();
        let pod = test_pod();
        cache.update_from_pod(&context(), &pod).await;
        cache.update_from_pod(&context(), &pod).await;

        let key = SourceKey {
            context: context(),
            namespace: "ns".into(),
            pod: "pod-a".into(),
            container: "app".into(),
            uid: "uid-1".into(),
        };
        let snap = cache.lookup(&key).await;
        assert_eq!(snap.node.as_deref(), Some("worker-3"));
    }

    #[tokio::test]
    async fn update_from_pod_refreshes_when_labels_change() {
        let cache = PodMetaCache::new();
        let mut pod = test_pod();
        cache.update_from_pod(&context(), &pod).await;

        pod.metadata
            .labels
            .as_mut()
            .expect("labels")
            .insert("tier".into(), "worker".into());
        cache.update_from_pod(&context(), &pod).await;

        let key = SourceKey {
            context: context(),
            namespace: "ns".into(),
            pod: "pod-a".into(),
            container: "app".into(),
            uid: "uid-1".into(),
        };
        let snap = cache.lookup(&key).await;
        assert_eq!(
            snap.labels.0.get("tier").map(String::as_str),
            Some("worker")
        );
    }

    #[tokio::test]
    async fn attach_deps_uses_pod_meta_cache() {
        let fixture = TestOrchestratorBuilder::new().build();
        let pod = test_pod();
        fixture
            .attach
            .pod_meta
            .update_from_pod(fixture.admission.context_name(), &pod)
            .await;

        let key = SourceKey {
            context: ContextName("ctx".into()),
            namespace: "ns".into(),
            pod: "pod-a".into(),
            container: "app".into(),
            uid: "uid-1".into(),
        };
        let snap = fixture.attach.pod_meta.lookup(&key).await;
        assert_eq!(snap.node.as_deref(), Some("worker-3"));
    }
}
