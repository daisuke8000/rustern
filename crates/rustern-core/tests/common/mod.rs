//! Shared harness for driving [`run_with_client`] end-to-end without a real cluster.
//!
//! Build `(mock, handle)` via `tower_test::mock::pair` and pass `mock` to
//! `kube::Client::new(mock, "default")`. Spawn a task that loops on
//! `handle.next_request()` to emulate the Kubernetes API.
//!
//! Wrap `run_with_client` in `tokio::time::timeout` (typically 2–5s). Hangs fail
//! the test instead of blocking CI.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use http::{Request, Response, StatusCode};
use k8s_openapi::api::core::v1::{
    Container, ContainerState, ContainerStateRunning, ContainerStatus, Pod, PodSpec, PodStatus,
};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{ListMeta, ObjectMeta};
use kube::api::ObjectList;
use kube::core::TypeMeta;
use rustern_core::discovery::context::ContextSelector;
use rustern_core::discovery::{
    ContainerDiscoverOpts, ContainerLifecycleBucket, ContainerStatePolicy,
};
use rustern_core::pipeline::{FilterOn, QueryMode};
use rustern_core::source::ContextName;
use rustern_core::source::pod_log::PodLogRequest;
use rustern_core::{
    BackpressurePolicy, CoreRunConfig, FormatterChoice, OutputMode, RuntimeFwdConfig,
};
use tokio_util::sync::CancellationToken;

pub fn test_context_name() -> ContextName {
    ContextName("default".into())
}

pub fn mock_client_pair() -> (
    kube::Client,
    tower_test::mock::Handle<Request<kube::client::Body>, Response<kube::client::Body>>,
) {
    let (mock, handle) =
        tower_test::mock::pair::<Request<kube::client::Body>, Response<kube::client::Body>>();
    (kube::Client::new(mock, "default"), handle)
}

pub fn core_run_config_for_test(follow: bool, root_token: CancellationToken) -> CoreRunConfig {
    core_run_config_for_test_with_max_log_requests(follow, 5, root_token)
}

pub fn core_run_config_for_test_with_max_log_requests(
    follow: bool,
    max_log_requests: usize,
    root_token: CancellationToken,
) -> CoreRunConfig {
    use std::collections::HashSet;

    CoreRunConfig {
        context: ContextSelector::default(),
        query: ".*".into(),
        namespaces: vec!["default".into()],
        all_namespaces: false,
        selector: None,
        field_selector: None,
        node: None,
        exclude_pod: Vec::new(),
        container: ".*".into(),
        exclude_container: Vec::new(),
        container_discovery: ContainerDiscoverOpts {
            include_init_containers: false,
            include_ephemeral_containers: false,
            state_policy: ContainerStatePolicy::Subset(HashSet::from([
                ContainerLifecycleBucket::Running,
            ])),
        },
        pod_condition: None,
        pod_log: PodLogRequest {
            follow,
            ..Default::default()
        },
        cursor_reconnect: false,
        include: Vec::new(),
        exclude: Vec::new(),
        highlight: Vec::new(),
        only_log_lines: true,
        filter_on: FilterOn::Original,
        json_query: None,
        json_query_mode: QueryMode::Filter,
        level_key: None,
        exit_on: Vec::new(),
        exit_on_level: None,
        output: OutputMode::Raw,
        formatter: FormatterChoice::Raw,
        diff_container: false,
        fwd: RuntimeFwdConfig {
            buffer_size: 64,
            lossy: false,
            mux_policy: BackpressurePolicy::Blocking,
            stats: None,
            max_log_requests,
        },
        root_token,
    }
}

pub fn test_pod(name: &str, uid: &str, container: &str) -> Pod {
    Pod {
        metadata: ObjectMeta {
            name: Some(name.into()),
            namespace: Some("default".into()),
            uid: Some(uid.into()),
            ..Default::default()
        },
        spec: Some(PodSpec {
            containers: vec![Container {
                name: container.into(),
                ..Default::default()
            }],
            ..Default::default()
        }),
        status: Some(PodStatus {
            container_statuses: Some(vec![ContainerStatus {
                name: container.into(),
                ready: true,
                state: Some(ContainerState {
                    running: Some(ContainerStateRunning::default()),
                    ..Default::default()
                }),
                ..Default::default()
            }]),
            phase: Some("Running".into()),
            ..Default::default()
        }),
    }
}

pub fn is_watch_query(query: &str) -> bool {
    query
        .split('&')
        .any(|part| part == "watch=1" || part == "watch=true")
}

pub struct MockApiserverConfig {
    pub pods: Vec<Pod>,
    pub log_lines: usize,
    pub log_line_prefix: &'static str,
    pub log_requests_count: Option<Arc<AtomicUsize>>,
    pub allow_watch: bool,
    pub hold_first_log: bool,
}

impl MockApiserverConfig {
    pub fn new(pods: Vec<Pod>) -> Self {
        Self {
            pods,
            log_lines: 1,
            log_line_prefix: "line",
            log_requests_count: None,
            allow_watch: true,
            hold_first_log: false,
        }
    }
}

pub async fn serve_mock_apiserver(
    mut handle: tower_test::mock::Handle<Request<kube::client::Body>, Response<kube::client::Body>>,
    config: MockApiserverConfig,
) {
    let list = ObjectList {
        types: TypeMeta {
            api_version: "v1".into(),
            kind: "PodList".into(),
        },
        metadata: ListMeta {
            resource_version: Some("100".into()),
            ..Default::default()
        },
        items: config.pods,
    };
    let list_body = serde_json::to_vec(&list).expect("pod list json");
    let in_flight_logs = Arc::new(AtomicUsize::new(0));

    while let Some((req, send)) = handle.next_request().await {
        let path = req.uri().path();
        let query = req.uri().query().unwrap_or("");
        let is_watch = is_watch_query(query);

        if is_watch {
            if !config.allow_watch {
                panic!(
                    "watch stream not allowed; got {} {}",
                    req.method(),
                    req.uri()
                );
            }
            let resp = Response::builder()
                .status(StatusCode::OK)
                .body(kube::client::Body::empty())
                .unwrap();
            send.send_response(resp);
            continue;
        }

        if path.ends_with("/pods") {
            let resp = Response::builder()
                .status(StatusCode::OK)
                .body(kube::client::Body::from(list_body.clone()))
                .unwrap();
            send.send_response(resp);
            continue;
        }

        if path.contains("/log") {
            if config.hold_first_log {
                // Intentionally blocks this handler: max-log-requests is enforced in attach
                // before later pods reach the mock; only the first held stream matters.
                let n = in_flight_logs.fetch_add(1, Ordering::Relaxed);
                if n == 0 {
                    tokio::time::sleep(Duration::from_secs(30)).await;
                }
                let resp = Response::builder()
                    .status(StatusCode::OK)
                    .body(kube::client::Body::from(
                        b"2026-04-28T08:00:00Z held\n".to_vec(),
                    ))
                    .unwrap();
                send.send_response(resp);
                continue;
            }

            let mut body = String::new();
            for i in 0..config.log_lines {
                use std::fmt::Write;
                let _ = writeln!(
                    body,
                    "2026-04-28T08:00:{i:02}Z {}-{i}",
                    config.log_line_prefix
                );
            }
            if let Some(counter) = &config.log_requests_count {
                counter.fetch_add(1, Ordering::Relaxed);
            }
            let resp = Response::builder()
                .status(StatusCode::OK)
                .body(kube::client::Body::from(body.into_bytes()))
                .unwrap();
            send.send_response(resp);
            continue;
        }

        panic!("unexpected mock request: {} {}", req.method(), req.uri());
    }
}

pub async fn join_mock_server(server: tokio::task::JoinHandle<()>) {
    tokio::time::timeout(Duration::from_secs(1), server)
        .await
        .expect("mock apiserver did not stop")
        .expect("mock apiserver task panicked");
}
