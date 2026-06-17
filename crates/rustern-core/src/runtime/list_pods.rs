//! One-shot pod list for no-follow runs (avoids opening a watch stream).

use std::collections::HashSet;

use k8s_openapi::api::core::v1::Pod;
use kube::Api;
use kube::api::ListParams;

use super::config::RunError;
use super::watch_admission::WatchAdmissionPolicy;
use crate::source::SourceKey;

const MAX_LIST_PAGES: usize = 1000;

pub(crate) async fn list_pods_paginated(
    api: &Api<Pod>,
    list_params: &ListParams,
) -> Result<Vec<Pod>, RunError> {
    let mut params = list_params.clone();
    let mut pods = Vec::new();
    for _ in 0..MAX_LIST_PAGES {
        let list = api.list(&params).await?;
        pods.extend(list.items);
        match list.metadata.continue_.filter(|c| !c.is_empty()) {
            Some(token) => params = params.continue_token(&token),
            None => return Ok(pods),
        }
    }
    Err(RunError::Other(format!(
        "pod list pagination exceeded {MAX_LIST_PAGES} pages"
    )))
}

#[allow(dead_code)]
pub(crate) async fn list_pods_once(
    api: &Api<Pod>,
    list_params: &ListParams,
    admission: &WatchAdmissionPolicy,
) -> Result<HashSet<SourceKey>, RunError> {
    let pods = list_pods_paginated(api, list_params).await?;
    Ok(admission.collect_snapshot(pods))
}
