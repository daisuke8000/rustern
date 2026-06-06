//! Watch-side cache of per-pod metadata for attach-time [`SourceMeta`] enrichment.

use std::collections::HashMap;
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
    let Some(locator) = PodLocator::try_from_pod(&ctx.context_name, pod) else {
        tracing::warn!(
            pod = ?pod.metadata.name,
            namespace = ?pod.metadata.namespace,
            "pod missing required metadata for pod_meta cache update"
        );
        return;
    };
    let snapshot = pod_meta_snapshot_from_pod(pod);
    let mut cache = ctx.pod_meta.write().await;
    cache.insert(locator, snapshot);
}

pub(crate) async fn remove_pod_meta_cache(ctx: &PodWatchCtx, pod: &Pod) {
    let Some(locator) = PodLocator::try_from_pod(&ctx.context_name, pod) else {
        tracing::warn!(
            pod = ?pod.metadata.name,
            namespace = ?pod.metadata.namespace,
            "pod missing required metadata for pod_meta cache removal"
        );
        return;
    };
    let mut cache = ctx.pod_meta.write().await;
    cache.remove(&locator);
}

pub(crate) async fn lookup_pod_meta(ctx: &PodWatchCtx, key: &SourceKey) -> PodMetaSnapshot {
    let locator = PodLocator::from_source_key(key);
    let cache = ctx.pod_meta.read().await;
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

    use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
    use regex::Regex;
    use tokio::sync::{Semaphore, mpsc};

    use crate::discovery::pod_watcher::{
        ContainerDiscoverOpts, ContainerLifecycleBucket, ContainerStatePolicy,
    };
    use crate::source::pod_log::PodLogRequest;
    use crate::source::{ContextName, SourceKey};
    use tokio_util::sync::CancellationToken;

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

    fn test_ctx(cache: Arc<RwLock<HashMap<PodLocator, PodMetaSnapshot>>>) -> PodWatchCtx {
        PodWatchCtx {
            context_name: ContextName("ctx".into()),
            pod_regex: None,
            pod_condition: None,
            container_discovery: ContainerDiscoverOpts {
                include_init_containers: false,
                include_ephemeral_containers: false,
                state_policy: ContainerStatePolicy::Subset(
                    [ContainerLifecycleBucket::Running].into_iter().collect(),
                ),
            },
            container_incl: Regex::new(".*").unwrap(),
            container_excl: vec![],
            allowed_ns: None,
            exclude_pod: vec![],
            mux_tx: mpsc::channel(1).0,
            client: {
                let (mock, _handle) = tower_test::mock::pair::<
                    http::Request<kube::client::Body>,
                    http::Response<kube::client::Body>,
                >();
                kube::Client::new(mock, "default")
            },
            root_child: CancellationToken::new(),
            pod_log: PodLogRequest::default(),
            cursor_reconnect: false,
            reconnect_cursor: Arc::new(std::sync::Mutex::new(HashMap::new())),
            sem: Arc::new(Semaphore::new(1)),
            follow_limit_notifier: None,
            pod_meta: cache,
        }
    }

    #[tokio::test]
    async fn cache_lookup_returns_snapshot_after_update() {
        let cache = new_pod_meta_cache();
        let ctx = test_ctx(cache);
        let pod = test_pod();
        update_pod_meta_cache(&ctx, &pod).await;

        let key = SourceKey {
            context: ContextName("ctx".into()),
            namespace: "ns".into(),
            pod: "pod-a".into(),
            container: "app".into(),
            uid: "uid-1".into(),
        };
        let snap = lookup_pod_meta(&ctx, &key).await;
        assert_eq!(snap.node.as_deref(), Some("worker-3"));
        assert_eq!(snap.labels.0.get("tier").map(String::as_str), Some("api"));
    }

    #[tokio::test]
    async fn cache_lookup_defaults_when_pod_unknown() {
        let cache = new_pod_meta_cache();
        let ctx = test_ctx(cache);
        let key = SourceKey {
            context: ContextName("ctx".into()),
            namespace: "ns".into(),
            pod: "missing".into(),
            container: "app".into(),
            uid: "uid-x".into(),
        };
        let snap = lookup_pod_meta(&ctx, &key).await;
        assert!(snap.node.is_none());
        assert!(snap.labels.0.is_empty());
    }

    #[tokio::test]
    async fn remove_pod_meta_cache_drops_entry() {
        let cache = new_pod_meta_cache();
        let ctx = test_ctx(cache);
        let pod = test_pod();
        update_pod_meta_cache(&ctx, &pod).await;
        remove_pod_meta_cache(&ctx, &pod).await;

        let key = SourceKey {
            context: ContextName("ctx".into()),
            namespace: "ns".into(),
            pod: "pod-a".into(),
            container: "app".into(),
            uid: "uid-1".into(),
        };
        assert_eq!(
            lookup_pod_meta(&ctx, &key).await,
            PodMetaSnapshot::default()
        );
    }
}
