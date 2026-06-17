//! Pod list/watch parameter resolution (`ListParams` → `WatchConfig`, query → selectors).

use kube::Client;
use kube::runtime::watcher::Config as WatchConfig;
use regex::Regex;

use super::resource::{Query, ResourceKind, parse_query};
use super::workload_selector;

#[derive(Debug, thiserror::Error)]
pub enum PodListError {
    #[error(transparent)]
    Query(#[from] super::resource::QueryParseError),
    #[error(transparent)]
    Regex(#[from] regex::Error),
    #[error(transparent)]
    Kube(#[from] kube::Error),
}

/// Inputs for [`PodWatchPlan::build`] (subset of run config, kept in discovery to avoid layer cycles).
pub(crate) struct PodWatchPlanConfig<'a> {
    pub(crate) query: &'a str,
    pub(crate) selector: Option<&'a str>,
    pub(crate) field_selector: Option<&'a str>,
    pub(crate) node: Option<&'a str>,
    pub(crate) namespaces: &'a [String],
    pub(crate) all_namespaces: bool,
}

pub(crate) struct PodWatchPlan {
    pub(crate) list_params: kube::api::ListParams,
    pub(crate) watch_cfg: WatchConfig,
    pub(crate) pod_regex: Option<Regex>,
}

impl PodWatchPlan {
    pub(crate) async fn build(
        client: &Client,
        cfg: &PodWatchPlanConfig<'_>,
    ) -> Result<Self, PodListError> {
        let query_src = if cfg.selector.is_some() && cfg.query == "." {
            ".*"
        } else {
            cfg.query
        };
        let q = parse_query(query_src)?;
        let pod_regex = match &q {
            Query::PodNameRegex(re) => Some(Regex::new(re)?),
            Query::LabelSelector { .. } => None,
        };
        let kind_name = match &q {
            Query::LabelSelector { kind, name } => Some((*kind, name.clone())),
            Query::PodNameRegex(_) => None,
        };

        let pod_kind_field_query =
            cfg.selector.is_none() && matches!(kind_name.as_ref(), Some((ResourceKind::Pod, _)));

        let mut list = kube::api::ListParams::default();
        if let Some(sel) = cfg.selector {
            list = list.labels(sel);
        } else if let Some((kind, name)) = &kind_name {
            match kind {
                ResourceKind::Pod => {
                    list = list.fields(&merged_field_selector_for_pod_name(name, cfg));
                }
                _ => {
                    let resolved = workload_selector::resolve_label_selector_for_namespaces(
                        client,
                        *kind,
                        name.as_str(),
                        cfg.namespaces,
                        cfg.all_namespaces,
                    )
                    .await?;
                    list = list.labels(&resolved);
                }
            }
        }

        if !pod_kind_field_query && let Some(fs) = combined_field_selector(cfg) {
            list = list.fields(&fs);
        }

        let watch_cfg = {
            let mut wc = WatchConfig::default();
            if let Some(ls) = list.label_selector.as_deref() {
                wc = wc.labels(ls);
            }
            if let Some(fs) = list.field_selector.as_deref() {
                wc = wc.fields(fs);
            }
            wc
        };

        Ok(Self {
            list_params: list,
            watch_cfg,
            pod_regex,
        })
    }
}

pub(crate) fn combined_field_selector(cfg: &PodWatchPlanConfig<'_>) -> Option<String> {
    let mut parts = Vec::new();
    if let Some(fs) = cfg.field_selector {
        let t = fs.trim();
        if !t.is_empty() {
            parts.push(t.to_string());
        }
    }
    if let Some(n) = cfg.node {
        let t = n.trim();
        if !t.is_empty() {
            parts.push(format!("spec.nodeName={t}"));
        }
    }
    if parts.is_empty() {
        None
    } else {
        Some(parts.join(","))
    }
}

fn merged_field_selector_for_pod_name(pod_name: &str, cfg: &PodWatchPlanConfig<'_>) -> String {
    let nm = pod_name.trim();
    let mut parts = vec![format!("metadata.name={nm}")];
    if let Some(rest) = combined_field_selector(cfg) {
        parts.push(rest);
    }
    parts.join(",")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg<'a>(
        query: &'a str,
        selector: Option<&'a str>,
        field_selector: Option<&'a str>,
        node: Option<&'a str>,
        namespaces: &'a [String],
    ) -> PodWatchPlanConfig<'a> {
        PodWatchPlanConfig {
            query,
            selector,
            field_selector,
            node,
            namespaces,
            all_namespaces: false,
        }
    }

    #[test]
    fn combined_field_selector_merges_node_and_field() {
        let ns = ["default".to_string()];
        let c = cfg(
            ".*",
            None,
            Some("status.phase=Running"),
            Some("node-1"),
            &ns,
        );
        assert_eq!(
            combined_field_selector(&c).as_deref(),
            Some("status.phase=Running,spec.nodeName=node-1")
        );
    }

    #[test]
    fn merged_field_selector_for_pod_name_includes_metadata_name() {
        let ns = ["default".to_string()];
        let c = cfg("pod/foo", None, Some("status.phase=Running"), None, &ns);
        assert_eq!(
            merged_field_selector_for_pod_name("foo", &c),
            "metadata.name=foo,status.phase=Running"
        );
    }
}
