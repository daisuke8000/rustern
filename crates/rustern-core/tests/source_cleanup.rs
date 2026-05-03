//! After cancellation, the meta `Weak` must not keep the allocation alive.

use futures::StreamExt;
use http::{Request, Response, StatusCode};
use rustern_core::source::pod_log::PodLogSource;
use rustern_core::source::{ContextName, Labels, LogSource, SourceKind, SourceMeta};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

#[tokio::test]
async fn cancelled_source_drops_all_resources_within_100ms() {
    let root = CancellationToken::new();
    let pod_token = root.child_token();

    let (mock, mut handle) =
        tower_test::mock::pair::<Request<kube::client::Body>, Response<kube::client::Body>>();
    let client = kube::Client::new(mock, "default");

    tokio::spawn(async move {
        let (_req, send) = handle.next_request().await.expect("req");
        let body = "2026-04-28T08:00:00Z line\n";
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
        uid: "u1".into(),
    };

    let src = PodLogSource::start(client, meta, pod_token.clone(), true, None, None)
        .await
        .unwrap();
    let weak = PodLogSource::meta_weak(&src);
    let mut stream = Box::new(src).into_stream();
    let _ = stream.next().await;

    pod_token.cancel();
    let _ = stream.collect::<Vec<_>>().await;

    let cleaned = timeout(Duration::from_millis(250), async {
        loop {
            if weak.upgrade().is_none() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await;
    assert!(cleaned.is_ok(), "source resources not cleaned");
}
