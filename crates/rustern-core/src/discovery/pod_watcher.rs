use std::collections::HashSet;

use futures::StreamExt;
use futures::stream::BoxStream;
use k8s_openapi::api::core::v1::Pod;
use kube::runtime::watcher::{Config, Event, watcher};

use crate::source::SourceKey;

/// 純粋な reconcile: 既知集合 vs スナップショット。
pub fn reconcile(active: &HashSet<SourceKey>, snapshot: &HashSet<SourceKey>) -> ReconcileDiff {
    let to_drop: Vec<SourceKey> = active.difference(snapshot).cloned().collect();
    let to_add: Vec<SourceKey> = snapshot.difference(active).cloned().collect();
    ReconcileDiff { to_add, to_drop }
}

#[derive(Debug, Eq, PartialEq)]
pub struct ReconcileDiff {
    pub to_add: Vec<SourceKey>,
    pub to_drop: Vec<SourceKey>,
}

pub fn pod_event_stream(
    api: kube::Api<Pod>,
    cfg: Config,
) -> BoxStream<'static, Result<Event<Pod>, kube::runtime::watcher::Error>> {
    watcher(api, cfg).boxed()
}

pub fn keys_from_pod(pod: &Pod, context: &crate::source::ContextName) -> Vec<SourceKey> {
    let Some(uid) = pod.metadata.uid.clone() else {
        tracing::warn!(
            pod = ?pod.metadata.name,
            namespace = ?pod.metadata.namespace,
            "pod has no metadata.uid, skipping log tail"
        );
        return Vec::new();
    };
    let ns = pod.metadata.namespace.clone().unwrap_or_default();
    let pod_name = pod.metadata.name.clone().unwrap_or_default();
    pod.spec
        .as_ref()
        .map(|s| {
            s.containers
                .iter()
                .map(|c| c.name.clone())
                .collect::<Vec<_>>()
        })
        .unwrap_or_default()
        .into_iter()
        .map(|container| SourceKey {
            context: context.clone(),
            namespace: ns.clone(),
            pod: pod_name.clone(),
            container,
            uid: uid.clone(),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::source::ContextName;

    fn key(pod: &str) -> SourceKey {
        SourceKey {
            context: ContextName("ctx".into()),
            namespace: "ns".into(),
            pod: pod.into(),
            container: "c".into(),
            uid: format!("uid-{pod}"),
        }
    }

    #[test]
    fn diff_adds_new_keys() {
        let active: HashSet<_> = [key("p1")].into_iter().collect();
        let snapshot: HashSet<_> = [key("p1"), key("p2")].into_iter().collect();
        let diff = reconcile(&active, &snapshot);
        assert_eq!(diff.to_add, vec![key("p2")]);
        assert!(diff.to_drop.is_empty());
    }

    #[test]
    fn diff_drops_orphans() {
        let active: HashSet<_> = [key("p1"), key("p2")].into_iter().collect();
        let snapshot: HashSet<_> = [key("p2")].into_iter().collect();
        let diff = reconcile(&active, &snapshot);
        assert!(diff.to_add.is_empty());
        assert_eq!(diff.to_drop, vec![key("p1")]);
    }

    #[test]
    fn diff_empty_when_equal() {
        let active: HashSet<_> = [key("p1")].into_iter().collect();
        let snapshot: HashSet<_> = [key("p1")].into_iter().collect();
        let diff = reconcile(&active, &snapshot);
        assert!(diff.to_add.is_empty());
        assert!(diff.to_drop.is_empty());
    }
}

#[cfg(test)]
mod kube_tests {
    use super::*;
    use crate::source::ContextName;
    use k8s_openapi::api::core::v1::{Container, PodSpec};
    use kube::core::ObjectMeta;

    fn pod_with(name: &str, ns: &str, uid: Option<&str>, containers: Vec<&str>) -> Pod {
        Pod {
            metadata: ObjectMeta {
                name: Some(name.into()),
                namespace: Some(ns.into()),
                uid: uid.map(String::from),
                ..Default::default()
            },
            spec: Some(PodSpec {
                containers: containers
                    .into_iter()
                    .map(|n| Container {
                        name: n.into(),
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    #[test]
    fn keys_extracted_from_two_containers() {
        let pod = pod_with("p1", "ns", Some("uid-aaa"), vec!["app", "sidecar"]);
        let keys = keys_from_pod(&pod, &ContextName("ctx".into()));
        assert_eq!(keys.len(), 2);
        assert!(keys.iter().all(|k| k.uid == "uid-aaa"));
        assert!(keys.iter().any(|k| k.container == "app"));
        assert!(keys.iter().any(|k| k.container == "sidecar"));
    }

    #[test]
    fn keys_skipped_when_uid_missing() {
        let pod = pod_with("p1", "ns", None, vec!["app"]);
        let keys = keys_from_pod(&pod, &ContextName("ctx".into()));
        assert!(keys.is_empty());
    }

    #[test]
    fn keys_distinguish_uid_for_rolling_update() {
        let pod_v1 = pod_with("p1", "ns", Some("uid-old"), vec!["app"]);
        let pod_v2 = pod_with("p1", "ns", Some("uid-new"), vec!["app"]);
        let k1 = keys_from_pod(&pod_v1, &ContextName("ctx".into()));
        let k2 = keys_from_pod(&pod_v2, &ContextName("ctx".into()));
        assert_ne!(k1[0], k2[0]);
        assert_ne!(k1[0].uid, k2[0].uid);
    }
}
