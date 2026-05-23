//! Minimal test for `CancellationToken` parent/child cancellation.

use futures::StreamExt;
use http::{Request, Response, StatusCode};
use rustern_core::source::pod_log::{PodLogRequest, PodLogSource};
use rustern_core::source::{ContextName, Labels, LogSource, SourceKind, SourceMeta};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

fn sample_meta(pod: &str) -> SourceMeta {
    SourceMeta {
        context: ContextName("ctx".into()),
        namespace: "ns".into(),
        pod: pod.into(),
        container: "c".into(),
        kind: SourceKind::PodLog,
        node: None,
        labels: Arc::new(Labels::default()),
        uid: format!("uid-{pod}"),
    }
}

#[tokio::test]
async fn pod_token_cancellation_stops_source() {
    let root = CancellationToken::new();
    let pod_t = root.child_token();

    let (mock, mut handle) =
        tower_test::mock::pair::<Request<kube::client::Body>, Response<kube::client::Body>>();
    let client = kube::Client::new(mock, "default");

    tokio::spawn(async move {
        let (_req, send) = handle.next_request().await.expect("req");
        let body = "2026-04-28T08:00:00Z a\n2026-04-28T08:01:00Z b\n";
        let resp = Response::builder()
            .status(StatusCode::OK)
            .body(kube::client::Body::from(body.as_bytes().to_vec()))
            .unwrap();
        send.send_response(resp);
    });

    let source = PodLogSource::start(
        client,
        sample_meta("p1"),
        pod_t.clone(),
        PodLogRequest {
            follow: true,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let mut stream = Box::new(source).into_stream();
    let first = stream.next().await;
    assert!(first.is_some());
    pod_t.cancel();
    let rest = timeout(Duration::from_millis(200), stream.collect::<Vec<_>>()).await;
    assert!(rest.is_ok());
    root.cancel();
}

#[tokio::test]
async fn root_token_cancels_child_sources() {
    let root = CancellationToken::new();
    let pod_t = root.child_token();

    let (mock, mut handle) =
        tower_test::mock::pair::<Request<kube::client::Body>, Response<kube::client::Body>>();
    let client = kube::Client::new(mock, "default");
    tokio::spawn(async move {
        let (_req, send) = handle.next_request().await.expect("req");
        let body = "2026-04-28T08:00:00Z x\n";
        let resp = Response::builder()
            .status(StatusCode::OK)
            .body(kube::client::Body::from(body.as_bytes().to_vec()))
            .unwrap();
        send.send_response(resp);
    });

    let source = PodLogSource::start(
        client,
        sample_meta("p1"),
        pod_t,
        PodLogRequest {
            follow: true,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let mut stream = Box::new(source).into_stream();
    let _ = stream.next().await;
    root.cancel();
    let _ = timeout(Duration::from_millis(200), stream.collect::<Vec<_>>()).await;
}
