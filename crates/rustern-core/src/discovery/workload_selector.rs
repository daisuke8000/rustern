//! Resolve `kind/name` queries to Kubernetes pod label selectors (`GET` workload).
//!
//! With `--all-namespaces` or multiple `--namespace` values we keep the legacy `app=<name>`
//! selector: each namespace might use different template labels.

use std::collections::BTreeMap;

use k8s_openapi::api::apps::v1::{DaemonSet, Deployment, ReplicaSet, StatefulSet};
use k8s_openapi::api::batch::v1::Job;
use k8s_openapi::api::core::v1::{PodTemplateSpec, ReplicationController, Service};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{LabelSelector, LabelSelectorRequirement};
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

fn fallback_app_selector(kind: ResourceKind, workload_name: &str) -> String {
    tracing::warn!(
        workload = workload_name,
        ?kind,
        "could not derive pod label selector from API object; falling back to app=<name>"
    );
    format!("app={workload_name}")
}

fn match_expression_fragment(req: &LabelSelectorRequirement) -> Option<String> {
    let key = req.key.as_str();
    match req.operator.as_str() {
        "In" | "NotIn" => {
            let values = req.values.as_ref().filter(|v| !v.is_empty())?;
            let mut sorted: Vec<&str> = values.iter().map(String::as_str).collect();
            sorted.sort_unstable();
            let op = if req.operator == "In" { "in" } else { "notin" };
            Some(format!("{key} {op} ({})", sorted.join(",")))
        }
        "Exists" => Some(key.to_string()),
        "DoesNotExist" => Some(format!("!{key}")),
        other => {
            tracing::warn!(
                key,
                operator = other,
                "unsupported matchExpression operator; skipping"
            );
            None
        }
    }
}

fn label_selector_from_meta(
    ls: &LabelSelector,
    template: Option<&PodTemplateSpec>,
    kind: ResourceKind,
    workload_name: &str,
) -> String {
    let mut parts: Vec<String> = Vec::new();

    if let Some(labels) = &ls.match_labels {
        for (k, v) in labels {
            parts.push(format!("{k}={v}"));
        }
    }

    if let Some(exprs) = &ls.match_expressions {
        for req in exprs {
            if let Some(fragment) = match_expression_fragment(req) {
                parts.push(fragment);
            }
        }
    }

    let no_match_labels = ls.match_labels.as_ref().is_none_or(BTreeMap::is_empty);
    let no_match_expressions = ls
        .match_expressions
        .as_ref()
        .is_none_or(|exprs| exprs.is_empty());
    if no_match_labels
        && no_match_expressions
        && let Some(labs) = template
            .and_then(|t| t.metadata.as_ref())
            .and_then(|md| md.labels.as_ref())
    {
        for (k, v) in labs {
            parts.push(format!("{k}={v}"));
        }
    }

    if parts.is_empty() {
        return fallback_app_selector(kind, workload_name);
    }

    parts.sort();
    parts.join(",")
}

fn string_from_ls(
    ls: &LabelSelector,
    template: Option<&PodTemplateSpec>,
    kind: ResourceKind,
    workload_name: &str,
) -> String {
    label_selector_from_meta(ls, template, kind, workload_name)
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

/// Resolve a `kind/name` query to one list/watch label selector for the given namespace scope.
pub async fn resolve_label_selector_for_namespaces(
    client: &Client,
    kind: ResourceKind,
    name: &str,
    namespaces: &[String],
    all_namespaces: bool,
) -> Result<String, kube::Error> {
    if matches!(kind, ResourceKind::Pod) {
        return Ok(label_selector_for(kind, name));
    }
    if all_namespaces {
        tracing::warn!(
            ?kind,
            name,
            "kind/name with --all-namespaces uses legacy app=<name> selector"
        );
        return Ok(label_selector_for(kind, name));
    }
    match namespaces.len() {
        0 => Ok(label_selector_for(kind, name)),
        1 => resolve_label_selector_for_kind_query(client, kind, name, Some(&namespaces[0])).await,
        n => {
            let mut selectors = Vec::with_capacity(n);
            for ns in namespaces {
                match resolve_label_selector_for_kind_query(client, kind, name, Some(ns.as_str()))
                    .await
                {
                    Ok(selector) => selectors.push(selector),
                    Err(err) => {
                        tracing::warn!(
                            ?kind,
                            name,
                            namespace = ns.as_str(),
                            ?err,
                            "failed to resolve selector in one namespace; falling back to app=<name>"
                        );
                        return Ok(label_selector_for(kind, name));
                    }
                }
            }
            Ok(unify_label_selectors(&selectors, kind, name))
        }
    }
}

/// When every namespace yields the same selector, use it; otherwise fall back to `app=<name>`.
pub fn unify_label_selectors(selectors: &[String], kind: ResourceKind, name: &str) -> String {
    if selectors.is_empty() {
        return label_selector_for(kind, name);
    }
    let mut unique = selectors.to_vec();
    unique.sort();
    unique.dedup();
    if unique.len() == 1 {
        unique.into_iter().next().expect("len checked")
    } else {
        tracing::warn!(
            ?kind,
            name,
            distinct = unique.len(),
            "workload label selectors differ across namespaces; falling back to app=<name>"
        );
        label_selector_for(kind, name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::{LabelSelectorRequirement, ObjectMeta};

    #[test]
    fn unify_same_selectors_across_namespaces() {
        assert_eq!(
            unify_label_selectors(
                &["app=api,tier=web".into(), "app=api,tier=web".into()],
                ResourceKind::Deployment,
                "api"
            ),
            "app=api,tier=web"
        );
    }

    #[test]
    fn unify_differing_selectors_falls_back_to_app() {
        assert_eq!(
            unify_label_selectors(
                &["app=api".into(), "app=api,tier=web".into()],
                ResourceKind::Deployment,
                "api"
            ),
            "app=api"
        );
    }

    #[test]
    fn btree_join_sorted() {
        let mut m = BTreeMap::new();
        m.insert("z".into(), "1".into());
        m.insert("a".into(), "2".into());
        assert_eq!(btree_to_label_selector(&m), "a=2,z=1");
    }

    #[test]
    fn match_labels_and_expressions_anded() {
        let ls = LabelSelector {
            match_expressions: Some(vec![LabelSelectorRequirement {
                key: "role".into(),
                operator: "In".into(),
                values: Some(vec!["x".into()]),
            }]),
            match_labels: Some(BTreeMap::from([("tier".into(), "web".into())])),
            ..Default::default()
        };
        assert_eq!(
            label_selector_from_meta(&ls, None, ResourceKind::Deployment, "api"),
            "role in (x),tier=web"
        );
    }

    #[test]
    fn match_expression_operators() {
        let ls = LabelSelector {
            match_expressions: Some(vec![
                LabelSelectorRequirement {
                    key: "env".into(),
                    operator: "NotIn".into(),
                    values: Some(vec!["dev".into(), "qa".into()]),
                },
                LabelSelectorRequirement {
                    key: "sidecar".into(),
                    operator: "Exists".into(),
                    values: None,
                },
                LabelSelectorRequirement {
                    key: "legacy".into(),
                    operator: "DoesNotExist".into(),
                    values: None,
                },
            ]),
            ..Default::default()
        };
        assert_eq!(
            label_selector_from_meta(&ls, None, ResourceKind::Deployment, "api"),
            "!legacy,env notin (dev,qa),sidecar"
        );
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
        assert_eq!(
            label_selector_from_meta(&ls, Some(&tpl_with), ResourceKind::Deployment, "api"),
            "app.kubernetes.io/name=payments"
        );
    }

    #[test]
    fn expressions_only_ignores_template_labels() {
        let tpl_with = PodTemplateSpec {
            metadata: Some(ObjectMeta {
                labels: Some(BTreeMap::from([("app".into(), "shop".into())])),
                ..Default::default()
            }),
            spec: None,
        };
        let ls = LabelSelector {
            match_expressions: Some(vec![LabelSelectorRequirement {
                key: "tier".into(),
                operator: "In".into(),
                values: Some(vec!["prod".into()]),
            }]),
            ..Default::default()
        };
        assert_eq!(
            label_selector_from_meta(&ls, Some(&tpl_with), ResourceKind::Deployment, "api"),
            "tier in (prod)"
        );
    }
}
