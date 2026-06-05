//! Synthetic multi-stream load: N mock pods × M lines → mux → pipeline → render.
//!
//! CI uses a modest total line count (see [`load_scale`]). Set `RUSTERN_LOAD_LINES`
//! locally (e.g. `50000` or `100000`) to exercise higher throughput.
//!
//! Every phase has a hard timeout so a scheduling deadlock fails fast instead of
//! hanging the test runner.

use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};

use http::{Request, Response, StatusCode};
use regex::Regex;
use rustern_core::pipeline::{ColorAssignOpts, ExitWatchState, FilterOn};
use rustern_core::render::default_renderer::DefaultLineFormatter;
use rustern_core::render::{LineFormatter, RenderCommand, flush_ticker};
use rustern_core::runtime::{
    LossyMetrics, MuxCmd, PipelineStages, RuntimeFwdConfig, apply_pipeline, forward_to_render,
    spawn_mux_task,
};
use rustern_core::source::pod_log::{PodLogRequest, PodLogSource};
use rustern_core::source::{
    BoxedLogStream, ContextName, Labels, LogEvent, LogSource, LogSourceError, SourceKey,
    SourceKind, SourceMeta,
};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;

const DEFAULT_CI_TOTAL_LINES: usize = 3_000;
const DEFAULT_POD_COUNT: usize = 6;
const RAW_MUX_BUFFER: usize = 4096;
const BLOCKING_RENDER_BUFFER: usize = 4096;
const TEST_HARD_LIMIT: Duration = Duration::from_secs(20);
const CONNECT_LIMIT: Duration = Duration::from_secs(8);
const MUX_ADD_LIMIT: Duration = Duration::from_secs(3);
const JOIN_LIMIT: Duration = Duration::from_secs(2);
const LOSSY_OBSERVE: Duration = Duration::from_millis(1500);

struct LoadScale {
    pods: usize,
    lines_per_pod: usize,
}

fn load_scale() -> LoadScale {
    let total = std::env::var("RUSTERN_LOAD_LINES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_CI_TOTAL_LINES)
        .max(1);
    let pods = DEFAULT_POD_COUNT.min(total);
    let lines_per_pod = total.div_ceil(pods);
    LoadScale {
        pods,
        lines_per_pod,
    }
}

fn recv_deadline() -> Duration {
    let scale = load_scale();
    let total = (scale.pods * scale.lines_per_pod) as u64;
    Duration::from_millis(total.saturating_mul(2).clamp(2_000, 8_000))
}

async fn with_deadline<F, T>(label: &str, limit: Duration, f: F) -> T
where
    F: Future<Output = T>,
{
    timeout(limit, f)
        .await
        .unwrap_or_else(|_| panic!("{label} timed out after {limit:?}"))
}

async fn mux_add(mux_tx: &mpsc::Sender<MuxCmd>, key: SourceKey, stream: BoxedLogStream) {
    match timeout(MUX_ADD_LIMIT, mux_tx.send(MuxCmd::Add(key, stream))).await {
        Ok(Ok(())) => {}
        Ok(Err(_)) => panic!("mux add channel closed"),
        Err(_) => panic!("mux add timed out after {MUX_ADD_LIMIT:?}"),
    }
}

async fn join_with_deadline<T>(label: &str, handle: JoinHandle<T>) -> T
where
    T: Send + 'static,
{
    timeout(JOIN_LIMIT, handle)
        .await
        .unwrap_or_else(|_| panic!("{label} join timed out after {JOIN_LIMIT:?}"))
        .unwrap_or_else(|e| panic!("{label} task panicked: {e:?}"))
}

fn log_body(pod: &str, lines: usize) -> String {
    let mut body = String::with_capacity(lines.saturating_mul(56));
    for i in 0..lines {
        use std::fmt::Write;
        let _ = writeln!(
            body,
            "2026-04-28T08:{:02}:{:02}Z {pod}/app line {i}",
            (i / 60) % 60,
            i % 60
        );
    }
    body
}

fn default_pipeline_stages() -> PipelineStages {
    PipelineStages {
        container_incl: Regex::new(".*").unwrap(),
        container_excl: vec![],
        includes: vec![],
        excludes: vec![],
        filter_on: FilterOn::Original,
        jq: None,
        level_key: None,
        color_assign: ColorAssignOpts {
            pod_colors: false,
            container_colors: false,
            diff_container: false,
        },
        exit_on: vec![],
        exit_on_level: None,
        exit_watch: ExitWatchState::new(CancellationToken::new()),
    }
}

async fn connect_mock_sources(
    mock: tower_test::mock::Mock<
        http::Request<kube::client::Body>,
        http::Response<kube::client::Body>,
    >,
    pods: usize,
    token: CancellationToken,
) -> Vec<(SourceKey, BoxedLogStream)> {
    with_deadline("connect_mock_sources", CONNECT_LIMIT, async move {
        let mut streams = Vec::with_capacity(pods);
        for p in 0..pods {
            let meta = SourceMeta {
                context: ContextName("ctx".into()),
                namespace: "ns".into(),
                pod: format!("pod-{p}"),
                container: "app".into(),
                kind: SourceKind::PodLog,
                node: None,
                labels: Arc::new(Labels::default()),
                uid: format!("uid-{p}"),
            };
            let client = kube::Client::new(mock.clone(), "default");
            let source = PodLogSource::start(
                client,
                meta.clone(),
                token.clone(),
                PodLogRequest {
                    follow: false,
                    ..Default::default()
                },
            )
            .await
            .expect("pod log source");
            let key = SourceKey {
                context: meta.context.clone(),
                namespace: meta.namespace.clone(),
                pod: meta.pod.clone(),
                container: meta.container.clone(),
                uid: meta.uid.clone(),
            };
            streams.push((key, Box::new(source).into_stream()));
        }
        streams
    })
    .await
}

async fn count_render_lines(
    mut render_rx: mpsc::Receiver<RenderCommand>,
    expected: u64,
    fmt: Arc<DefaultLineFormatter>,
) -> u64 {
    let mut delivered = 0u64;
    let deadline = Instant::now() + recv_deadline();
    while delivered < expected && Instant::now() < deadline {
        let remaining = deadline.saturating_duration_since(Instant::now());
        let cmd = match timeout(remaining, render_rx.recv()).await {
            Ok(Some(cmd)) => cmd,
            Ok(None) => break,
            Err(_) => break,
        };
        match cmd {
            RenderCommand::Line(ev) => {
                let _ = fmt.format_line(&ev);
                delivered += 1;
            }
            RenderCommand::Flush => {}
            RenderCommand::Shutdown => break,
        }
    }
    delivered
}

async fn run_mux_raw_load(pods: usize, lines_per_pod: usize) -> u64 {
    let (mock, mut handle) =
        tower_test::mock::pair::<Request<kube::client::Body>, Response<kube::client::Body>>();
    let server = tokio::spawn(async move {
        for p in 0..pods {
            let (_req, send) = handle.next_request().await.expect("mock log request");
            let body = log_body(&format!("pod-{p}"), lines_per_pod);
            let resp = Response::builder()
                .status(StatusCode::OK)
                .body(kube::client::Body::from(body.into_bytes()))
                .unwrap();
            send.send_response(resp);
        }
    });

    let token = CancellationToken::new();
    let streams = connect_mock_sources(mock, pods, token.clone()).await;

    let (mux_tx, mux_rx) = mpsc::channel::<MuxCmd>(256);
    let (raw_tx, mut raw_rx) = mpsc::channel::<Result<LogEvent, LogSourceError>>(RAW_MUX_BUFFER);
    let mux_h = spawn_mux_task(mux_rx, raw_tx);

    for (key, stream) in streams {
        mux_add(&mux_tx, key, stream).await;
    }

    let expected = (pods * lines_per_pod) as u64;
    let mut got = 0u64;
    let deadline = Instant::now() + recv_deadline();
    while got < expected && Instant::now() < deadline {
        let remaining = deadline.saturating_duration_since(Instant::now());
        match timeout(remaining, raw_rx.recv()).await {
            Ok(Some(Ok(_))) => got += 1,
            Ok(Some(Err(_))) | Ok(None) => break,
            Err(_) => break,
        }
    }

    drop(mux_tx);
    token.cancel();
    let _ = timeout(JOIN_LIMIT, mux_h).await;
    join_with_deadline("mock_log_server", server).await;
    got
}

async fn run_pipeline_render_load(lossy: bool, render_buffer: usize) -> (u64, u64) {
    let scale = load_scale();
    let expected = (scale.pods * scale.lines_per_pod) as u64;

    let (mock, mut handle) =
        tower_test::mock::pair::<Request<kube::client::Body>, Response<kube::client::Body>>();
    let pods = scale.pods;
    let lines_per_pod = scale.lines_per_pod;
    let server = tokio::spawn(async move {
        for p in 0..pods {
            let (_req, send) = handle.next_request().await.expect("mock log request");
            let body = log_body(&format!("pod-{p}"), lines_per_pod);
            let resp = Response::builder()
                .status(StatusCode::OK)
                .body(kube::client::Body::from(body.into_bytes()))
                .unwrap();
            send.send_response(resp);
        }
    });

    let token = CancellationToken::new();
    let streams = connect_mock_sources(mock, pods, token.clone()).await;

    let (mux_tx, mux_rx) = mpsc::channel::<MuxCmd>(256);
    let (raw_tx, raw_rx) = mpsc::channel::<Result<LogEvent, LogSourceError>>(RAW_MUX_BUFFER);
    let mux_h = spawn_mux_task(mux_rx, raw_tx);

    let metrics = LossyMetrics::new();
    let (render_tx, render_rx) = mpsc::channel::<RenderCommand>(render_buffer.max(1));
    let pipe_stream = apply_pipeline(ReceiverStream::new(raw_rx), default_pipeline_stages());
    let fwd_cfg = RuntimeFwdConfig {
        buffer_size: render_buffer,
        lossy,
        max_log_requests: pods,
    };

    let fmt = Arc::new(DefaultLineFormatter {
        timestamp_style: rustern_core::TimestampStyle::Omit,
        timestamp_zone: rustern_core::TimestampZone::Utc,
        color_enabled: false,
        pod_colors: false,
        container_colors: false,
    });

    let _flush_h = tokio::spawn(flush_ticker(
        render_tx.clone(),
        token.clone(),
        Duration::from_millis(50),
    ));

    let render_h = if lossy {
        drop(render_rx);
        None
    } else {
        Some(tokio::spawn(async move {
            count_render_lines(render_rx, expected, fmt).await
        }))
    };

    let forward_token = token.clone();
    let forward_h = tokio::spawn(forward_to_render(
        pipe_stream,
        render_tx.clone(),
        fwd_cfg,
        metrics.clone(),
        forward_token,
    ));

    tokio::task::yield_now().await;

    for (key, stream) in streams {
        mux_add(&mux_tx, key, stream).await;
    }

    let delivered = if lossy {
        tokio::time::sleep(LOSSY_OBSERVE).await;
        0
    } else {
        let render_h = render_h.expect("render task");
        timeout(recv_deadline(), render_h)
            .await
            .unwrap_or_else(|_| panic!("render count timed out after {:?}", recv_deadline()))
            .expect("render task panicked")
    };

    token.cancel();
    let _ = render_tx.send(RenderCommand::Shutdown).await;
    let _ = timeout(JOIN_LIMIT, forward_h).await;
    let _ = timeout(JOIN_LIMIT, mux_h).await;
    join_with_deadline("mock_log_server", server).await;

    (delivered, metrics.drop_count())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mux_forwards_all_mock_pod_lines() {
    with_deadline("mux_forwards_all_mock_pod_lines", TEST_HARD_LIMIT, async {
        let scale = load_scale();
        let expected = (scale.pods * scale.lines_per_pod) as u64;
        let got = run_mux_raw_load(scale.pods, scale.lines_per_pod).await;
        assert_eq!(got, expected, "mux should forward every mock log line");
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn blocking_pipeline_renders_all_multistream_lines() {
    with_deadline(
        "blocking_pipeline_renders_all_multistream_lines",
        TEST_HARD_LIMIT,
        async {
            let scale = load_scale();
            let expected = (scale.pods * scale.lines_per_pod) as u64;
            let (delivered, dropped) =
                run_pipeline_render_load(false, BLOCKING_RENDER_BUFFER).await;
            assert_eq!(delivered, expected, "all lines should reach render");
            assert_eq!(dropped, 0, "blocking mode must not drop");
        },
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lossy_drops_when_render_backpressured() {
    with_deadline(
        "lossy_drops_when_render_backpressured",
        TEST_HARD_LIMIT,
        async {
            let scale = load_scale();
            let expected = (scale.pods * scale.lines_per_pod) as u64;
            let (delivered, dropped) = run_pipeline_render_load(true, 4).await;
            assert!(
                dropped > 0,
                "lossy mode with tiny render channel should drop events"
            );
            assert!(
                delivered < expected,
                "backpressure should prevent full delivery in lossy mode"
            );
        },
    )
    .await;
}
