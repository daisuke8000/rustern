//! End-to-end: tower-test log bytes → `PodLogSource` → pipeline → `DefaultLineFormatter` / `render_task`.

use futures::StreamExt;
use http::{Request, Response, StatusCode};
use rustern_core::pipeline::{ColorAssignOpts, color_assign, json_annotate, level_classify};
use rustern_core::render::default_renderer::DefaultLineFormatter;
use rustern_core::render::{RenderCommand, render_task};
use rustern_core::source::pod_log::{PodLogRequest, PodLogSource};
use rustern_core::source::{ContextName, Labels, LogLevel, LogSource, SourceKind, SourceMeta};
use rustern_core::{TimestampStyle, TimestampZone};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, duplex};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

#[tokio::test]
async fn mock_logs_through_pipeline_and_renderer() {
    let (mock, mut handle) =
        tower_test::mock::pair::<Request<kube::client::Body>, Response<kube::client::Body>>();
    let client = kube::Client::new(mock, "default");

    let server = tokio::spawn(async move {
        let (_req, send) = handle.next_request().await.expect("request");
        let body = concat!(
            "2026-04-28T08:00:00Z {\"level\":\"info\",\"msg\":\"hello\"}\n",
            "2026-04-28T08:00:01Z plain\n",
        );
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
        container: "app".into(),
        kind: SourceKind::PodLog,
        node: None,
        labels: Arc::new(Labels::default()),
        uid: "uid-1".into(),
        palette_index: None,
        container_palette_index: None,
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
    let stream = json_annotate(stream);
    let stream = level_classify(stream, Some("level".into()));
    let stream = color_assign(
        stream,
        ColorAssignOpts {
            pod_colors: true,
            container_colors: true,
            diff_container: false,
        },
    );
    let events: Vec<_> = stream.collect().await;
    server.await.unwrap();

    assert_eq!(events.len(), 2);
    let e0 = events[0].as_ref().unwrap();
    assert!(e0.structured.is_some());
    assert_eq!(*e0.level.as_ref().unwrap(), LogLevel::Info);
    assert!(e0.palette_index.is_some());
    let e1 = events[1].as_ref().unwrap();
    assert!(e1.structured.is_none());

    let (mut rd, wr) = duplex(4096);
    let (tx, rx) = mpsc::channel::<RenderCommand>(8);
    let fmt = Arc::new(DefaultLineFormatter {
        timestamp_style: TimestampStyle::Omit,
        timestamp_zone: TimestampZone::Utc,
        color_enabled: false,
        pod_colors: true,
        container_colors: true,
    });
    let rh = tokio::spawn(async move { render_task(rx, wr, fmt).await.unwrap() });

    tx.send(RenderCommand::Line(events[0].as_ref().unwrap().clone()))
        .await
        .unwrap();
    tx.send(RenderCommand::Line(events[1].as_ref().unwrap().clone()))
        .await
        .unwrap();
    tx.send(RenderCommand::Shutdown).await.unwrap();
    rh.await.unwrap();

    let mut buf = vec![0u8; 256];
    let n = rd.read(&mut buf).await.unwrap();
    let s = String::from_utf8_lossy(&buf[..n]);
    assert!(s.contains("p1/app"));
    assert!(s.contains("hello") || s.contains("plain"));
}
