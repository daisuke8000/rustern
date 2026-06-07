//! Shared test fixtures for watch orchestration modules.

use std::collections::HashSet as StdHashSet;
use std::ops::Deref;
use std::sync::Arc;

use regex::Regex;
use tokio::sync::{Semaphore, mpsc};
use tokio_util::sync::CancellationToken;

use super::cursor_store::ReconnectCursorStore;
use super::mux::MuxCmd;
use super::pod_meta_cache::PodMetaCache;
use super::watch_ctx::{AttachDeps, PodWatchCtx, WatchAdmission};
use crate::discovery::pod_watcher::{
    ContainerDiscoverOpts, ContainerLifecycleBucket, ContainerStatePolicy,
};
use crate::source::ContextName;
use crate::source::pod_log::PodLogRequest;

type MockHandle =
    tower_test::mock::Handle<http::Request<kube::client::Body>, http::Response<kube::client::Body>>;

struct TestKeepalive {
    _mock_handle: MockHandle,
    _mux_drain: Option<tokio::task::JoinHandle<()>>,
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
        }
    }

    pub(crate) fn container_incl(mut self, pattern: &str) -> Self {
        self.container_incl = pattern.into();
        self
    }

    pub(crate) fn container_excl(mut self, patterns: &[&str]) -> Self {
        self.container_excl = patterns.iter().map(|s| (*s).to_string()).collect();
        self
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

    pub(crate) fn build(self) -> TestOrchestratorFixture {
        let (mux_tx, mux_drain) = match self.mux_tx {
            Some(tx) => (tx, None),
            None => {
                let (tx, mut rx) = mpsc::channel(8);
                let drain = tokio::spawn(async move { while rx.recv().await.is_some() {} });
                (tx, Some(drain))
            }
        };
        let (mock, mock_handle) = tower_test::mock::pair::<
            http::Request<kube::client::Body>,
            http::Response<kube::client::Body>,
        >();
        let ctx = PodWatchCtx {
            admission: WatchAdmission {
                context_name: self.context_name,
                pod_regex: None,
                pod_condition: None,
                container_discovery: ContainerDiscoverOpts {
                    include_init_containers: false,
                    include_ephemeral_containers: false,
                    state_policy: ContainerStatePolicy::Subset(StdHashSet::from([
                        ContainerLifecycleBucket::Running,
                    ])),
                },
                container_incl: Regex::new(&self.container_incl).expect("container_incl regex"),
                container_excl: self
                    .container_excl
                    .iter()
                    .map(|p| Regex::new(p).expect("container_excl regex"))
                    .collect(),
                allowed_ns: None,
                exclude_pod: vec![],
            },
            attach: AttachDeps {
                mux_tx,
                client: kube::Client::new(mock, "default"),
                root_child: CancellationToken::new(),
                pod_log: self.pod_log,
                cursor_reconnect: self.cursor_reconnect,
                reconnect_cursor: ReconnectCursorStore::new(),
                sem: Arc::new(Semaphore::new(self.sem_permits)),
                follow_limit_notifier: None,
                pod_meta: PodMetaCache::new(),
            },
        };
        TestOrchestratorFixture {
            inner: Arc::new(ctx),
            _keepalive: TestKeepalive {
                _mock_handle: mock_handle,
                _mux_drain: mux_drain,
            },
        }
    }
}
