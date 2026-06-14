//! Mock: `log_stream` succeeds on the third attempt.

use futures::StreamExt;
use http::{Request, Response, StatusCode};
use rustern_core::source::pod_log::{PodLogRequest, PodLogSource};
use rustern_core::source::{ContextName, Labels, LogSource, SourceKind, SourceMeta};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

#[tokio::test]
async fn retries_until_success() {
    let (mock, mut handle) =
        tower_test::mock::pair::<Request<kube::client::Body>, Response<kube::client::Body>>();
    let client = kube::Client::new(mock, "default");

    let server = tokio::spawn(async move {
        for i in 0..3 {
            let (_req, send) = handle.next_request().await.expect("request");
            if i < 2 {
                let resp = Response::builder()
                    .status(StatusCode::INTERNAL_SERVER_ERROR)
                    .body(kube::client::Body::from(Vec::from(*b"err")))
                    .unwrap();
                send.send_response(resp);
            } else {
                let body = "2026-04-28T08:00:00Z ok\n";
                let resp = Response::builder()
                    .status(StatusCode::OK)
                    .body(kube::client::Body::from(body.as_bytes().to_vec()))
                    .unwrap();
                send.send_response(resp);
            }
        }
    });

    let meta = SourceMeta {
        context: ContextName("ctx".into()),
        namespace: "ns".into(),
        pod: "p1".into(),
        container: "c".into(),
        kind: SourceKind::PodLog,
        node: None,
        labels: Arc::new(Labels::default()),
        uid: "u1".into(),
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

    assert_eq!(events.len(), 1);
    assert_eq!(&*events[0].as_ref().unwrap().message, "ok");
}
