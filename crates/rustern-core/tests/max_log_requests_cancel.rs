//! Integration: follow-mode `--max-log-requests` exhaustion cancels the run.

use std::collections::HashSet;
use std::io::Write;
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
use rustern_core::source::pod_log::PodLogRequest;
use rustern_core::{
    BackpressurePolicy, CoreRunConfig, FormatterChoice, OutputMode, RunError, RuntimeFwdConfig,
    run_with_client,
};
use tempfile::NamedTempFile;
use tokio_util::sync::CancellationToken;

const KUBECONFIG: &str = r#"
apiVersion: v1
kind: Config
current-context: default
contexts:
  - name: default
    context:
      cluster: local
      user: dev
clusters:
  - name: local
    cluster:
      server: https://localhost
users:
  - name: dev
    user: {}
"#;

fn temp_context() -> (NamedTempFile, ContextSelector) {
    let mut f = NamedTempFile::new().expect("temp kubeconfig");
    f.write_all(KUBECONFIG.as_bytes())
        .expect("write kubeconfig");
    let sel = ContextSelector {
        kubeconfig_path: Some(f.path().to_path_buf()),
        context_name: Some("default".into()),
    };
    (f, sel)
}

fn test_pod(name: &str, uid: &str, container: &str) -> Pod {
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

fn core_run_config(
    context: ContextSelector,
    follow: bool,
    max_log_requests: usize,
    root_token: CancellationToken,
) -> CoreRunConfig {
    CoreRunConfig {
        context,
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

async fn serve_mock_apiserver(
    mut handle: tower_test::mock::Handle<Request<kube::client::Body>, Response<kube::client::Body>>,
    pods: Vec<Pod>,
    in_flight_logs: Arc<AtomicUsize>,
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
        items: pods,
    };
    let list_body = serde_json::to_vec(&list).expect("pod list json");

    while let Some((req, send)) = handle.next_request().await {
        let path = req.uri().path();
        let query = req.uri().query().unwrap_or("");

        if path.ends_with("/pods") && !query.contains("watch=1") {
            let resp = Response::builder()
                .status(StatusCode::OK)
                .body(kube::client::Body::from(list_body.clone()))
                .unwrap();
            send.send_response(resp);
            continue;
        }

        if path.contains("/log") {
            let n = in_flight_logs.fetch_add(1, Ordering::Relaxed);
            if n == 0 {
                // Hold the first follow log open so the attach semaphore stays contended.
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

        if query.contains("watch=1") {
            let resp = Response::builder()
                .status(StatusCode::OK)
                .body(kube::client::Body::empty())
                .unwrap();
            send.send_response(resp);
            continue;
        }

        panic!("unexpected mock request: {} {}", req.method(), req.uri());
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn follow_mode_cancels_on_max_log_requests_exhausted() {
    // Regression guard: attach must call root_child.cancel() and notify follow_limit_notifier
    // when follow-mode try_acquire fails; removing either leaves run() hanging past this timeout.
    let pods = vec![
        test_pod("pod-a", "uid-a", "app"),
        test_pod("pod-b", "uid-b", "app"),
        test_pod("pod-c", "uid-c", "app"),
    ];

    let (mock, handle) =
        tower_test::mock::pair::<Request<kube::client::Body>, Response<kube::client::Body>>();
    let client = kube::Client::new(mock, "default");
    let server = tokio::spawn(serve_mock_apiserver(
        handle,
        pods,
        Arc::new(AtomicUsize::new(0)),
    ));

    let (_kubeconfig, context) = temp_context();
    let root_token = CancellationToken::new();
    let cfg = core_run_config(context, true, 1, root_token);

    let result = tokio::time::timeout(Duration::from_secs(5), run_with_client(client, cfg))
        .await
        .expect("run hung instead of failing on max-log-requests exhaustion");

    let err = match result {
        Err(e) => e,
        Ok(_) => panic!("expected max-log-requests error"),
    };

    assert!(
        matches!(
            &err,
            RunError::Other(msg)
                if msg.contains("max concurrent log streams reached (--max-log-requests)")
        ),
        "unexpected error: {err:?}"
    );

    server.abort();
}
