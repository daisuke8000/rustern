//! Integration: `--no-follow` exits after initial list reconcile and log drain.

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
    BackpressurePolicy, CoreRunConfig, FormatterChoice, OutputMode, RuntimeFwdConfig,
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
            max_log_requests: 5,
        },
        root_token,
    }
}

async fn serve_mock_apiserver(
    mut handle: tower_test::mock::Handle<Request<kube::client::Body>, Response<kube::client::Body>>,
    pods: Vec<Pod>,
    log_lines: usize,
    log_requests_count: Arc<AtomicUsize>,
    allow_watch: bool,
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
        let is_watch = query
            .split('&')
            .any(|part| part == "watch=1" || part == "watch=true");

        if is_watch {
            if !allow_watch {
                panic!(
                    "no-follow must not open a watch stream; got {} {}",
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
            let mut body = String::new();
            for i in 0..log_lines {
                use std::fmt::Write;
                let _ = writeln!(body, "2026-04-28T08:00:{i:02}Z line-{i}");
            }
            log_requests_count.fetch_add(1, Ordering::Relaxed);
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

async fn join_mock_server(server: tokio::task::JoinHandle<()>) {
    tokio::time::timeout(Duration::from_secs(1), server)
        .await
        .expect("mock apiserver did not stop")
        .expect("mock apiserver task panicked");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn no_follow_exits_after_all_logs_consumed() {
    let pods = vec![
        test_pod("pod-a", "uid-a", "app"),
        test_pod("pod-b", "uid-b", "app"),
    ];
    let expected_log_requests = pods.len();
    let log_lines_per_pod = 1usize;

    let (mock, handle) =
        tower_test::mock::pair::<Request<kube::client::Body>, Response<kube::client::Body>>();
    let client = kube::Client::new(mock, "default");
    let log_requests_count = Arc::new(AtomicUsize::new(0));
    let served = Arc::clone(&log_requests_count);
    let server = tokio::spawn(serve_mock_apiserver(
        handle,
        pods,
        log_lines_per_pod,
        served,
        false,
    ));

    let (_kubeconfig, context) = temp_context();
    let root_token = CancellationToken::new();
    let cfg = core_run_config(context, false, root_token);

    let outcome = tokio::time::timeout(Duration::from_secs(2), run_with_client(client, cfg))
        .await
        .expect("no-follow run hung")
        .expect("run failed");

    assert!(!outcome.had_source_errors);
    assert_eq!(
        log_requests_count.load(Ordering::Relaxed),
        expected_log_requests,
        "expected one log fetch per pod"
    );

    join_mock_server(server).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn follow_mode_does_not_auto_exit_after_logs_eof() {
    let pods = vec![test_pod("pod-a", "uid-a", "app")];

    let (mock, handle) =
        tower_test::mock::pair::<Request<kube::client::Body>, Response<kube::client::Body>>();
    let client = kube::Client::new(mock, "default");
    let log_requests_count = Arc::new(AtomicUsize::new(0));
    let served = Arc::clone(&log_requests_count);
    let server = tokio::spawn(serve_mock_apiserver(handle, pods, 1, served, true));

    let (_kubeconfig, context) = temp_context();
    let root_token = CancellationToken::new();
    let cancel = root_token.clone();
    let cfg = core_run_config(context, true, root_token);

    let mut run_h = tokio::spawn(async move { run_with_client(client, cfg).await });

    tokio::select! {
        _ = &mut run_h => {
            panic!("follow mode exited without cancel");
        }
        _ = tokio::time::sleep(Duration::from_millis(400)) => {}
    }

    cancel.cancel();
    tokio::time::timeout(Duration::from_secs(2), &mut run_h)
        .await
        .expect("follow run hung after cancel")
        .expect("follow run task panicked")
        .expect("follow run failed");

    join_mock_server(server).await;
}
