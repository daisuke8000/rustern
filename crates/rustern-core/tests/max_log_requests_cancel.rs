//! Integration: follow-mode `--max-log-requests` exhaustion cancels the run.

mod common;

use std::time::Duration;

use common::{
    MockApiserverConfig, core_run_config_for_test_with_max_log_requests, mock_client_pair,
    serve_mock_apiserver, test_context_name, test_pod,
};
use rustern_core::{RunError, run_with_client};
use tokio_util::sync::CancellationToken;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn follow_mode_cancels_on_max_log_requests_exhausted() {
    // Regression guard: attach must call root_child.cancel() and notify follow_limit_notifier
    // when follow-mode try_acquire fails; removing either leaves run() hanging past this timeout.
    let pods = vec![
        test_pod("pod-a", "uid-a", "app"),
        test_pod("pod-b", "uid-b", "app"),
        test_pod("pod-c", "uid-c", "app"),
    ];

    let (client, handle) = mock_client_pair();
    let server = tokio::spawn(serve_mock_apiserver(
        handle,
        MockApiserverConfig {
            hold_first_log: true,
            allow_watch: true,
            ..MockApiserverConfig::new(pods)
        },
    ));

    let root_token = CancellationToken::new();
    let cfg = core_run_config_for_test_with_max_log_requests(true, 1, root_token);

    let result = tokio::time::timeout(
        Duration::from_secs(5),
        run_with_client(client, test_context_name(), cfg),
    )
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
