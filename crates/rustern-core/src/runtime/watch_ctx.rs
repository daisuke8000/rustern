use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use chrono::{DateTime, Utc};
use regex::Regex;
use tokio::sync::{Semaphore, mpsc};
use tokio_util::sync::CancellationToken;

use super::mux::MuxCmd;
use crate::discovery::pod_condition::PodConditionFilter;
use crate::discovery::pod_watcher::ContainerDiscoverOpts;
use crate::source::pod_log::PodLogRequest;
use crate::source::{ContextName, SourceKey};

pub(crate) struct PodWatchCtx {
    pub(crate) context_name: ContextName,
    pub(crate) pod_regex: Option<Regex>,
    pub(crate) pod_condition: Option<PodConditionFilter>,
    pub(crate) container_discovery: ContainerDiscoverOpts,
    pub(crate) container_incl: Regex,
    pub(crate) container_excl: Vec<Regex>,
    pub(crate) allowed_ns: Option<HashSet<String>>,
    pub(crate) exclude_pod: Vec<Regex>,
    pub(crate) mux_tx: mpsc::Sender<MuxCmd>,
    pub(crate) client: kube::Client,
    pub(crate) root_child: CancellationToken,
    pub(crate) pod_log: PodLogRequest,
    pub(crate) cursor_reconnect: bool,
    pub(crate) reconnect_cursor: Arc<std::sync::Mutex<HashMap<SourceKey, DateTime<Utc>>>>,
    pub(crate) sem: Arc<Semaphore>,
    pub(crate) follow_limit_notifier: Option<mpsc::Sender<()>>,
}
