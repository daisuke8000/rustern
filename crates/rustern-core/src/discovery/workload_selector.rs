//! Resolve `kind/name` queries to Kubernetes pod label selectors (`GET` workload).
//!
//! With `--all-namespaces` or multiple `--namespace` values we keep the legacy `app=<name>`
//! selector: each namespace might use different template labels.

use std::collections::BTreeMap;

use k8s_openapi::api::apps::v1::{DaemonSet, Deployment, ReplicaSet, StatefulSet};
use k8s_openapi::api::batch::v1::Job;
use k8s_openapi::api::core::v1::{PodTemplateSpec, ReplicationController, Service};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::LabelSelector;
use kube::Client;
use kube::api::Api;

use super::resource::{ResourceKind, label_selector_for};

/// Build a `-l`/ListParams-compatible label selector string from workload label pairs.
///
/// Uses lexicographic key order (`BTreeMap` iterator order).
pub fn btree_to_label_selector(m: &BTreeMap<String, String>) -> String {
    let mut pairs: Vec<(&String, &String)> = m.iter().collect();
    pairs.sort_by(|a, b| a.0.cmp(b.0));
    pairs
        .iter()
        .map(|(k, v)| format!("{}={}", k.as_str(), v.as_str()))
        .collect::<Vec<_>>()
        .join(",")
}

fn fallback_app_keys(kind: ResourceKind, workload_name: &str) -> BTreeMap<String, String> {
    let mut m = BTreeMap::new();
    m.insert("app".to_string(), workload_name.to_string());
    tracing::warn!(
        workload = workload_name,
        ?kind,
        "could not derive pod label selector from API object; falling back to app=<name>"
    );
    m
}

fn pairs_from_meta_selector(
    ls: &LabelSelector,
    template: Option<&PodTemplateSpec>,
    kind: ResourceKind,
    workload_name: &str,
) -> BTreeMap<String, String> {
    if ls.match_expressions.as_ref().is_some_and(|e| !e.is_empty()) {
        tracing::warn!(
            workload = workload_name,
            ?kind,
            "selector includes matchExpressions; using matchLabels/template labels only — verify this matches kubectl watch scope"
        );
    }

    let mut m = ls.match_labels.clone().unwrap_or_default();

    if m.is_empty()
        && let Some(labs) = template
            .and_then(|t| t.metadata.as_ref())
            .and_then(|md| md.labels.as_ref())
    {
        m.clone_from(labs);
    }

    if m.is_empty() {
        return fallback_app_keys(kind, workload_name);
    }

    m
}

fn string_from_ls(
    ls: &LabelSelector,
    template: Option<&PodTemplateSpec>,
    kind: ResourceKind,
    workload_name: &str,
) -> String {
    btree_to_label_selector(&pairs_from_meta_selector(ls, template, kind, workload_name))
}

async fn deployment_selector(
    client: &Client,
    ns: &str,
    workload_name: &str,
) -> Result<String, kube::Error> {
    let api: Api<Deployment> = Api::namespaced(client.clone(), ns);
    let d = api.get(workload_name).await?;
    let Some(spec) = d.spec.as_ref() else {
        return Ok(label_selector_for(ResourceKind::Deployment, workload_name));
    };
    Ok(string_from_ls(
        &spec.selector,
        Some(&spec.template),
        ResourceKind::Deployment,
        workload_name,
    ))
}

async fn statefulset_selector(
    client: &Client,
    ns: &str,
    workload_name: &str,
) -> Result<String, kube::Error> {
    let api: Api<StatefulSet> = Api::namespaced(client.clone(), ns);
    let s = api.get(workload_name).await?;
    let Some(spec) = s.spec.as_ref() else {
        return Ok(label_selector_for(ResourceKind::StatefulSet, workload_name));
    };
    Ok(string_from_ls(
        &spec.selector,
        Some(&spec.template),
        ResourceKind::StatefulSet,
        workload_name,
    ))
}

async fn daemonset_selector(
    client: &Client,
    ns: &str,
    workload_name: &str,
) -> Result<String, kube::Error> {
    let api: Api<DaemonSet> = Api::namespaced(client.clone(), ns);
    let d = api.get(workload_name).await?;
    let Some(spec) = d.spec.as_ref() else {
        return Ok(label_selector_for(ResourceKind::DaemonSet, workload_name));
    };
    Ok(string_from_ls(
        &spec.selector,
        Some(&spec.template),
        ResourceKind::DaemonSet,
        workload_name,
    ))
}

async fn replicaset_selector(
    client: &Client,
    ns: &str,
    workload_name: &str,
) -> Result<String, kube::Error> {
    let api: Api<ReplicaSet> = Api::namespaced(client.clone(), ns);
    let rs = api.get(workload_name).await?;
    let Some(spec) = rs.spec.as_ref() else {
        return Ok(label_selector_for(ResourceKind::ReplicaSet, workload_name));
    };
    Ok(string_from_ls(
        &spec.selector,
        spec.template.as_ref(),
        ResourceKind::ReplicaSet,
        workload_name,
    ))
}

async fn job_selector(
    client: &Client,
    ns: &str,
    workload_name: &str,
) -> Result<String, kube::Error> {
    let api: Api<Job> = Api::namespaced(client.clone(), ns);
    let j = api.get(workload_name).await?;
    let Some(spec) = j.spec.as_ref() else {
        return Ok(label_selector_for(ResourceKind::Job, workload_name));
    };

    match &spec.selector {
        Some(ls) => Ok(string_from_ls(
            ls,
            Some(&spec.template),
            ResourceKind::Job,
            workload_name,
        )),
        None => Ok(label_selector_for(ResourceKind::Job, workload_name)),
    }
}

async fn service_selector(
    client: &Client,
    ns: &str,
    workload_name: &str,
) -> Result<String, kube::Error> {
    let api: Api<Service> = Api::namespaced(client.clone(), ns);
    let svc = api.get(workload_name).await?;
    let selector = svc
        .spec
        .as_ref()
        .and_then(|sp| sp.selector.as_ref())
        .cloned()
        .unwrap_or_default();
    if selector.is_empty() {
        tracing::warn!(
            workload = workload_name,
            "service has no selector; falling back to app=<name>"
        );
        Ok(label_selector_for(ResourceKind::Service, workload_name))
    } else {
        Ok(btree_to_label_selector(&selector))
    }
}

async fn replication_controller_selector(
    client: &Client,
    ns: &str,
    workload_name: &str,
) -> Result<String, kube::Error> {
    let api: Api<ReplicationController> = Api::namespaced(client.clone(), ns);
    let rc = api.get(workload_name).await?;
    let Some(spec) = rc.spec.as_ref() else {
        return Ok(label_selector_for(
            ResourceKind::ReplicationController,
            workload_name,
        ));
    };

    let mut selector = spec.selector.clone().unwrap_or_default();
    if selector.is_empty()
        && let Some(labels) = spec
            .template
            .as_ref()
            .and_then(|t| t.metadata.as_ref())
            .and_then(|md| md.labels.as_ref())
    {
        selector = labels.clone();
    }

    if selector.is_empty() {
        tracing::warn!(
            workload = workload_name,
            "replicationcontroller has no selector/template labels; falling back to app=<name>"
        );
        Ok(label_selector_for(
            ResourceKind::ReplicationController,
            workload_name,
        ))
    } else {
        Ok(btree_to_label_selector(&selector))
    }
}

pub async fn resolve_label_selector_for_kind_query(
    client: &Client,
    kind: ResourceKind,
    name: &str,
    single_namespace: Option<&str>,
) -> Result<String, kube::Error> {
    if matches!(kind, ResourceKind::Pod) {
        return Ok(label_selector_for(kind, name));
    }
    let Some(ns) = single_namespace else {
        return Ok(label_selector_for(kind, name));
    };

    match kind {
        ResourceKind::Pod => Ok(label_selector_for(kind, name)),
        ResourceKind::Deployment => deployment_selector(client, ns, name).await,
        ResourceKind::StatefulSet => statefulset_selector(client, ns, name).await,
        ResourceKind::DaemonSet => daemonset_selector(client, ns, name).await,
        ResourceKind::ReplicaSet => replicaset_selector(client, ns, name).await,
        ResourceKind::Job => job_selector(client, ns, name).await,
        ResourceKind::Service => service_selector(client, ns, name).await,
        ResourceKind::ReplicationController => {
            replication_controller_selector(client, ns, name).await
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::{LabelSelectorRequirement, ObjectMeta};

    #[test]
    fn btree_join_sorted() {
        let mut m = BTreeMap::new();
        m.insert("z".into(), "1".into());
        m.insert("a".into(), "2".into());
        assert_eq!(btree_to_label_selector(&m), "a=2,z=1");
    }

    #[test]
    fn match_labels_used_when_expression_also_present() {
        let ls = LabelSelector {
            match_expressions: Some(vec![LabelSelectorRequirement {
                key: "role".into(),
                operator: "In".into(),
                values: Some(vec!["x".into()]),
            }]),
            match_labels: Some(BTreeMap::from([("tier".into(), "web".into())])),
            ..Default::default()
        };
        let m = pairs_from_meta_selector(&ls, None, ResourceKind::Deployment, "api");
        assert_eq!(m.get("tier").map(String::as_str), Some("web"));
    }

    #[test]
    fn template_labels_when_selector_maps_empty() {
        let tpl_with = PodTemplateSpec {
            metadata: Some(ObjectMeta {
                labels: Some(BTreeMap::from([(
                    "app.kubernetes.io/name".into(),
                    "payments".into(),
                )])),
                ..Default::default()
            }),
            spec: None,
        };
        let ls = LabelSelector::default();
        let m = pairs_from_meta_selector(&ls, Some(&tpl_with), ResourceKind::Deployment, "api");
        assert_eq!(
            m.get("app.kubernetes.io/name").map(String::as_str),
            Some("payments")
        );
    }
}
