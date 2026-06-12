use std::sync::Arc;

use tokio::sync::{Semaphore, mpsc};
use tokio_util::sync::CancellationToken;

use super::cursor_store::ReconnectCursorStore;
use super::mux::MuxCmd;
use super::pod_meta_cache::PodMetaCache;
use super::watch_admission::WatchAdmissionPolicy;
use crate::source::log_opener::LogSourceOpener;
use crate::source::pod_log::PodLogRequest;

/// Runtime dependencies shared by attach and stream registry reconciliation.
#[derive(Clone)]
pub(crate) struct AttachDeps {
    pub(crate) mux_tx: mpsc::Sender<MuxCmd>,
    pub(crate) log_opener: Arc<dyn LogSourceOpener>,
    pub(crate) root_child: CancellationToken,
    pub(crate) pod_log: PodLogRequest,
    pub(crate) cursor_reconnect: bool,
    pub(crate) reconnect_cursor: ReconnectCursorStore,
    pub(crate) sem: Arc<Semaphore>,
    pub(crate) follow_limit_notifier: Option<mpsc::Sender<()>>,
    pub(crate) pod_meta: PodMetaCache,
}

/// Composed watch orchestration context: admission policy plus attach/runtime deps.
#[derive(Clone)]
pub(crate) struct PodWatchCtx {
    pub(crate) admission: WatchAdmissionPolicy,
    pub(crate) attach: AttachDeps,
}
