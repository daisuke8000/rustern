//! Swap in `kube::Client` via tower-test and exercise `Api::log_stream`.

use futures::StreamExt;
use http::{Request, Response, StatusCode};
use rustern_core::source::pod_log::{PodLogRequest, PodLogSource};
use rustern_core::source::{ContextName, Labels, LogSource, SourceKind, SourceMeta};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

#[tokio::test]
async fn streams_two_lines_from_mock_apiserver() {
    let (mock, mut handle) =
        tower_test::mock::pair::<Request<kube::client::Body>, Response<kube::client::Body>>();
    let client = kube::Client::new(mock, "default");

    let server = tokio::spawn(async move {
        let (req, send) = handle.next_request().await.expect("request");
        assert!(
            req.uri()
                .path()
                .contains("/api/v1/namespaces/ns/pods/p1/log")
        );
        let body = "2026-04-28T08:00:00Z hello\n2026-04-28T08:00:01Z world\n";
        let resp = Response::builder()
            .status(StatusCode::OK)
            .body(kube::client::Body::from(body.as_bytes().to_vec()))
            .unwrap();
        send.send_response(resp);
    });

    let meta = SourceMeta {
        context: ContextName("ctx".into()),
        namespace: "ns".into(),
        pod: "p1".into(),
        container: "c".into(),
        kind: SourceKind::PodLog,
        node: None,
        labels: Arc::new(Labels::default()),
        uid: "uid-1".into(),
    };

    let source = PodLogSource::start(
        client,
        Arc::new(meta),
        CancellationToken::new(),
        PodLogRequest {
            follow: true,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let stream = Box::new(source).into_stream();
    let events: Vec<_> = stream.collect().await;
    server.await.unwrap();

    assert_eq!(events.len(), 2);
    let first = events[0].as_ref().unwrap();
    assert_eq!(&*first.message, "hello");
    let second = events[1].as_ref().unwrap();
    assert_eq!(&*second.message, "world");
}

#[tokio::test]
async fn passes_previous_and_since_time_query_params() {
    let (mock, mut handle) =
        tower_test::mock::pair::<Request<kube::client::Body>, Response<kube::client::Body>>();
    let client = kube::Client::new(mock, "default");

    let server = tokio::spawn(async move {
        let (req, send) = handle.next_request().await.expect("request");
        let q = req.uri().query().unwrap_or("");
        assert!(q.contains("previous=true"));
        assert!(q.contains("sinceTime="));
        let body = "2026-04-28T08:00:00Z ok\n";
        let resp = Response::builder()
            .status(StatusCode::OK)
            .body(kube::client::Body::from(body.as_bytes().to_vec()))
            .unwrap();
        send.send_response(resp);
    });

    let meta = SourceMeta {
        context: ContextName("ctx".into()),
        namespace: "ns".into(),
        pod: "p1".into(),
        container: "c".into(),
        kind: SourceKind::PodLog,
        node: None,
        labels: Arc::new(Labels::default()),
        uid: "uid-1".into(),
    };

    let ts: jiff::Timestamp = "2024-03-15T10:30:45Z".parse().unwrap();
    let source = PodLogSource::start(
        client,
        Arc::new(meta),
        CancellationToken::new(),
        PodLogRequest {
            follow: false,
            since_time: Some(ts),
            previous: true,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let stream = Box::new(source).into_stream();
    let events: Vec<_> = stream.collect().await;
    server.await.unwrap();
    assert_eq!(events.len(), 1);
}
