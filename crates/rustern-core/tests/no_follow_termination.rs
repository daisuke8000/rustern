//! Integration: `--no-follow` exits after initial list reconcile and log drain.

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use common::{
    MockApiserverConfig, core_run_config_for_test, join_mock_server, mock_client_pair,
    serve_mock_apiserver, test_context_name, test_pod,
};
use rustern_core::run_with_client;
use tokio_util::sync::CancellationToken;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn no_follow_exits_after_all_logs_consumed() {
    let pods = vec![
        test_pod("pod-a", "uid-a", "app"),
        test_pod("pod-b", "uid-b", "app"),
    ];
    let expected_log_requests = pods.len();
    let log_lines_per_pod = 1usize;

    let (client, handle) = mock_client_pair();
    let log_requests_count = Arc::new(AtomicUsize::new(0));
    let served = Arc::clone(&log_requests_count);
    let server = tokio::spawn(serve_mock_apiserver(
        handle,
        MockApiserverConfig {
            log_lines: log_lines_per_pod,
            log_requests_count: Some(served),
            allow_watch: false,
            ..MockApiserverConfig::new(pods)
        },
    ));

    let root_token = CancellationToken::new();
    let cfg = core_run_config_for_test(false, root_token);

    let outcome = tokio::time::timeout(
        Duration::from_secs(2),
        run_with_client(client, test_context_name(), cfg),
    )
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

    let (client, handle) = mock_client_pair();
    let log_requests_count = Arc::new(AtomicUsize::new(0));
    let served = Arc::clone(&log_requests_count);
    let server = tokio::spawn(serve_mock_apiserver(
        handle,
        MockApiserverConfig {
            log_lines: 1,
            log_requests_count: Some(served),
            allow_watch: true,
            ..MockApiserverConfig::new(pods)
        },
    ));

    let root_token = CancellationToken::new();
    let cancel = root_token.clone();
    let cfg = core_run_config_for_test(true, root_token);

    let mut run_h =
        tokio::spawn(async move { run_with_client(client, test_context_name(), cfg).await });

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
