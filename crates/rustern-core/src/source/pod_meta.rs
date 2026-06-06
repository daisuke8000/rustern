//! Pod metadata carried separately from [`super::SourceKey`] identity.

use std::collections::BTreeMap;

use k8s_openapi::api::core::v1::Pod;

use super::{ContextName, Labels, SourceKey};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PodMetaSnapshot {
    pub node: Option<String>,
    pub labels: Labels,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct PodLocator {
    pub context: ContextName,
    pub namespace: String,
    pub pod: String,
    pub uid: String,
}

impl PodLocator {
    pub fn from_source_key(key: &SourceKey) -> Self {
        Self {
            context: key.context.clone(),
            namespace: key.namespace.clone(),
            pod: key.pod.clone(),
            uid: key.uid.clone(),
        }
    }

    pub fn try_from_pod(context: &ContextName, pod: &Pod) -> Option<Self> {
        let uid = pod.metadata.uid.clone()?;
        let pod_name = pod.metadata.name.clone()?;
        Some(Self {
            context: context.clone(),
            namespace: pod.metadata.namespace.clone().unwrap_or_default(),
            pod: pod_name,
            uid,
        })
    }
}

pub fn pod_meta_snapshot_from_pod(pod: &Pod) -> PodMetaSnapshot {
    let node = pod.spec.as_ref().and_then(|spec| spec.node_name.clone());
    let labels = pod
        .metadata
        .labels
        .clone()
        .unwrap_or_default()
        .into_iter()
        .collect::<BTreeMap<_, _>>();
    PodMetaSnapshot {
        node,
        labels: Labels(labels),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;

    fn pod_with_meta(labels: Option<BTreeMap<String, String>>, node: Option<&str>) -> Pod {
        Pod {
            metadata: ObjectMeta {
                name: Some("pod-a".into()),
                namespace: Some("ns".into()),
                uid: Some("uid-1".into()),
                labels,
                ..Default::default()
            },
            spec: node.map(|n| k8s_openapi::api::core::v1::PodSpec {
                node_name: Some(n.into()),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    #[test]
    fn snapshot_extracts_node_and_labels() {
        let mut labels = BTreeMap::new();
        labels.insert("app".into(), "web".into());
        let snap = pod_meta_snapshot_from_pod(&pod_with_meta(Some(labels), Some("node-1")));
        assert_eq!(snap.node.as_deref(), Some("node-1"));
        assert_eq!(snap.labels.0.get("app").map(String::as_str), Some("web"));
    }

    #[test]
    fn snapshot_defaults_when_metadata_absent() {
        let snap = pod_meta_snapshot_from_pod(&pod_with_meta(None, None));
        assert!(snap.node.is_none());
        assert!(snap.labels.0.is_empty());
    }

    #[test]
    fn locator_round_trips_source_key_fields() {
        let key = SourceKey {
            context: ContextName("ctx".into()),
            namespace: "ns".into(),
            pod: "pod-a".into(),
            container: "app".into(),
            uid: "uid-1".into(),
        };
        let loc = PodLocator::from_source_key(&key);
        assert_eq!(loc.context, key.context);
        assert_eq!(loc.namespace, key.namespace);
        assert_eq!(loc.pod, key.pod);
        assert_eq!(loc.uid, key.uid);
    }
}
