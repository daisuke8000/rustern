//! Shared test fixtures for watch orchestration modules.

use std::collections::HashSet as StdHashSet;
use std::ops::Deref;
use std::sync::Arc;

use tokio::sync::{Semaphore, mpsc};
use tokio_util::sync::CancellationToken;

use super::cursor_store::{ReconnectCursorStore, run_cursor_update_processor};
use super::mux::MuxCmd;
use super::pod_meta_cache::PodMetaCache;
use super::watch_admission::WatchAdmissionPolicy;
use super::watch_ctx::{AttachDeps, PodWatchCtx};
use crate::discovery::{ContainerDiscoverOpts, ContainerLifecycleBucket, ContainerStatePolicy};
use crate::source::ContextName;
use crate::source::log_opener::{LogSourceOpener, ScriptLogSourceOpener};
use crate::source::pod_log::PodLogRequest;

struct TestKeepalive {
    _root_token: CancellationToken,
    _mux_drain: Option<tokio::task::JoinHandle<()>>,
    _cursor_processor: Option<tokio::task::JoinHandle<()>>,
}

impl Drop for TestKeepalive {
    fn drop(&mut self) {
        self._root_token.cancel();
        if let Some(h) = self._cursor_processor.take() {
            h.abort();
        }
        if let Some(h) = self._mux_drain.take() {
            h.abort();
        }
    }
}

/// Built watch context plus handles that must outlive the test.
pub(crate) struct TestOrchestratorFixture {
    inner: Arc<PodWatchCtx>,
    _keepalive: TestKeepalive,
}

impl Deref for TestOrchestratorFixture {
    type Target = PodWatchCtx;

    fn deref(&self) -> &PodWatchCtx {
        &self.inner
    }
}

impl TestOrchestratorFixture {
    pub(crate) fn arc(&self) -> Arc<PodWatchCtx> {
        Arc::clone(&self.inner)
    }
}

pub(crate) struct TestOrchestratorBuilder {
    context_name: ContextName,
    container_incl: String,
    container_excl: Vec<String>,
    mux_tx: Option<mpsc::Sender<MuxCmd>>,
    pod_log: PodLogRequest,
    cursor_reconnect: bool,
    sem_permits: usize,
    log_opener: Option<Arc<dyn LogSourceOpener>>,
}

impl TestOrchestratorBuilder {
    pub(crate) fn new() -> Self {
        Self {
            context_name: ContextName("ctx".into()),
            container_incl: ".*".into(),
            container_excl: Vec::new(),
            mux_tx: None,
            pod_log: PodLogRequest::default(),
            cursor_reconnect: false,
            sem_permits: 1,
            log_opener: None,
        }
    }

    pub(crate) fn mux_tx(mut self, tx: mpsc::Sender<MuxCmd>) -> Self {
        self.mux_tx = Some(tx);
        self
    }

    pub(crate) fn pod_log(mut self, req: PodLogRequest) -> Self {
        self.pod_log = req;
        self
    }

    pub(crate) fn cursor_reconnect(mut self, enabled: bool) -> Self {
        self.cursor_reconnect = enabled;
        self
    }

    pub(crate) fn sem_permits(mut self, n: usize) -> Self {
        self.sem_permits = n;
        self
    }

    pub(crate) fn log_opener(mut self, opener: Arc<dyn LogSourceOpener>) -> Self {
        self.log_opener = Some(opener);
        self
    }

    pub(crate) fn build(self) -> TestOrchestratorFixture {
        let (mux_tx, mux_drain) = match self.mux_tx {
            Some(tx) => (tx, None),
            None => {
                let (tx, mut rx) = mpsc::channel(8);
                let drain = tokio::spawn(async move { while rx.recv().await.is_some() {} });
                (tx, Some(drain))
            }
        };
        let log_opener = self
            .log_opener
            .unwrap_or_else(|| ScriptLogSourceOpener::new(vec![]));
        let reconnect_cursor = ReconnectCursorStore::new();
        let (cursor_update_tx, cursor_update_rx) = mpsc::unbounded_channel();
        let root_token = CancellationToken::new();
        let cursor_processor = tokio::spawn(run_cursor_update_processor(
            cursor_update_rx,
            reconnect_cursor.clone(),
            root_token.clone(),
        ));
        let ctx = PodWatchCtx {
            admission: WatchAdmissionPolicy::try_new(
                self.context_name,
                None,
                &[],
                &["ns".into()],
                false,
                &self.container_incl,
                &self.container_excl,
                ContainerDiscoverOpts {
                    include_init_containers: false,
                    include_ephemeral_containers: false,
                    state_policy: ContainerStatePolicy::Subset(StdHashSet::from([
                        ContainerLifecycleBucket::Running,
                    ])),
                },
                None,
            )
            .expect("admission policy"),
            attach: AttachDeps {
                mux_tx,
                log_opener,
                root_child: root_token.child_token(),
                pod_log: self.pod_log,
                cursor_reconnect: self.cursor_reconnect,
                reconnect_cursor,
                cursor_update_tx,
                sem: Arc::new(Semaphore::new(self.sem_permits)),
                follow_limit_notifier: None,
                pod_meta: PodMetaCache::new(),
            },
        };
        TestOrchestratorFixture {
            inner: Arc::new(ctx),
            _keepalive: TestKeepalive {
                _root_token: root_token,
                _mux_drain: mux_drain,
                _cursor_processor: Some(cursor_processor),
            },
        }
    }
}
