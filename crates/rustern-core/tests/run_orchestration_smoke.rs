//! Orchestration smoke: no-follow list + attach completes without a real cluster.

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use common::{
    MockApiserverConfig, core_run_config_for_test, join_mock_server, mock_client_pair,
    serve_mock_apiserver, test_context_name,
};
use rustern_core::run_with_client;
use tokio_util::sync::CancellationToken;

const SMOKE_LOG_LINES: usize = 3;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn run_orchestration_smoke_no_follow_completes() {
    let pods = vec![common::test_pod("pod-a", "uid-a", "app")];

    let (client, handle) = mock_client_pair();
    let log_requests_count = Arc::new(AtomicUsize::new(0));
    let served = Arc::clone(&log_requests_count);
    let server = tokio::spawn(serve_mock_apiserver(
        handle,
        MockApiserverConfig {
            log_lines: SMOKE_LOG_LINES,
            log_line_prefix: "smoke-line",
            log_requests_count: Some(served),
            allow_watch: false,
            ..MockApiserverConfig::new(pods)
        },
    ));

    let root_token = CancellationToken::new();
    let cfg = core_run_config_for_test(false, root_token);

    let outcome = tokio::time::timeout(
        Duration::from_secs(3),
        run_with_client(client, test_context_name(), cfg),
    )
    .await
    .expect("orchestration smoke hung")
    .expect("run failed");

    assert!(!outcome.had_source_errors);
    assert_eq!(
        log_requests_count.load(Ordering::Relaxed),
        1,
        "expected one log fetch for the single pod"
    );

    join_mock_server(server).await;
}
