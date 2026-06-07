use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use k8s_openapi::api::core::v1::Pod;
use regex::Regex;
use tokio::sync::{RwLock, Semaphore, mpsc};
use tokio_util::sync::CancellationToken;

use super::cursor_store::ReconnectCursorStore;
use super::mux::MuxCmd;
use crate::discovery::pod_condition::{PodConditionFilter, pod_matches_condition};
use crate::discovery::pod_watcher::{ContainerDiscoverOpts, keys_from_pod};
use crate::source::ContextName;
use crate::source::SourceKey;
use crate::source::pod_log::PodLogRequest;
use crate::source::pod_meta::{PodLocator, PodMetaSnapshot};

/// Event-time pod and container admission policy for the watch loop.
#[derive(Clone)]
pub(crate) struct WatchAdmission {
    pub(crate) context_name: ContextName,
    pub(crate) pod_regex: Option<Regex>,
    pub(crate) pod_condition: Option<PodConditionFilter>,
    pub(crate) container_discovery: ContainerDiscoverOpts,
    pub(crate) container_incl: Regex,
    pub(crate) container_excl: Vec<Regex>,
    pub(crate) allowed_ns: Option<HashSet<String>>,
    pub(crate) exclude_pod: Vec<Regex>,
}

impl WatchAdmission {
    pub(crate) fn admit_pod(&self, pod: &Pod) -> bool {
        let Some(name) = pod.metadata.name.as_deref() else {
            return false;
        };
        if let Some(allowed) = &self.allowed_ns {
            let Some(ns) = pod.metadata.namespace.as_deref() else {
                return false;
            };
            if !allowed.contains(ns) {
                return false;
            }
        }
        if self.exclude_pod.iter().any(|re| re.is_match(name)) {
            return false;
        }
        if let Some(re) = &self.pod_regex
            && !re.is_match(name)
        {
            return false;
        }
        if let Some(cond) = &self.pod_condition
            && !pod_matches_condition(pod, cond)
        {
            return false;
        }
        true
    }

    pub(crate) fn admit_streams(&self, pod: &Pod) -> Vec<SourceKey> {
        keys_from_pod(pod, &self.context_name, &self.container_discovery)
            .into_iter()
            .filter(|k| self.container_incl.is_match(&k.container))
            .filter(|k| !self.container_excl.iter().any(|r| r.is_match(&k.container)))
            .collect()
    }

    pub(crate) fn collect_snapshot(&self, pods: Vec<Pod>) -> HashSet<SourceKey> {
        let mut snap = HashSet::new();
        for pod in pods {
            if !self.admit_pod(&pod) {
                continue;
            }
            for k in self.admit_streams(&pod) {
                snap.insert(k);
            }
        }
        snap
    }
}

/// Runtime dependencies shared by attach and stream registry reconciliation.
#[derive(Clone)]
pub(crate) struct AttachDeps {
    pub(crate) mux_tx: mpsc::Sender<MuxCmd>,
    pub(crate) client: kube::Client,
    pub(crate) root_child: CancellationToken,
    pub(crate) pod_log: PodLogRequest,
    pub(crate) cursor_reconnect: bool,
    pub(crate) reconnect_cursor: ReconnectCursorStore,
    pub(crate) sem: Arc<Semaphore>,
    pub(crate) follow_limit_notifier: Option<mpsc::Sender<()>>,
    pub(crate) pod_meta: Arc<RwLock<HashMap<PodLocator, PodMetaSnapshot>>>,
}

/// Composed watch orchestration context: admission policy plus attach/runtime deps.
#[derive(Clone)]
pub(crate) struct PodWatchCtx {
    pub(crate) admission: WatchAdmission,
    pub(crate) attach: AttachDeps,
}
