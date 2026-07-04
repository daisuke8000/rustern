//! One-shot pod list for no-follow runs (avoids opening a watch stream).

use k8s_openapi::api::core::v1::Pod;
use kube::Api;
use kube::api::ListParams;

use super::config::RunError;

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
