use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use futures::stream;
use rustern_core::pipeline::ExitWatchState;
use rustern_core::render::default_renderer::DefaultLineFormatter;
use rustern_core::render::{RenderCommand, flush_ticker, render_task};
use rustern_core::runtime::{
    BackpressurePolicy, LossyMetrics, MuxCmd, MuxMetrics, PipelineSpecBuilder, RuntimeFwdConfig,
    forward_to_render, spawn_mux_task,
};
use rustern_core::source::{ContextName, Labels, LogEvent, SourceKey, SourceKind, SourceMeta};
use rustern_core::{TimestampStyle, TimestampZone};
use tokio::io::AsyncReadExt;
use tokio::runtime::Runtime;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;

const BATCH: u64 = 2_000;
const LINES_PER_STREAM: usize = 32;
const MATRIX_STREAMS: [usize; 4] = [1, 16, 128, 512];
const RAW_BUFFER: usize = 4_096;
const TIERED_RAW_BUFFER: usize = 64;
const RENDER_BUFFER: usize = 4_096;
const SLOW_RENDER_BUFFER: usize = 4;
const SLOW_CONSUMER_DELAY: Duration = Duration::from_micros(50);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ConsumerMode {
    Unbounded,
    Slow,
}

struct MatrixRun {
    delivered: usize,
    mux_drops: u64,
    forward_drops: u64,
}

fn fwd_cfg(lossy: bool) -> RuntimeFwdConfig {
    RuntimeFwdConfig {
        buffer_size: RENDER_BUFFER,
        lossy,
        mux_policy: BackpressurePolicy::from_lossy(lossy),
        stats: None,
        max_log_requests: 50,
    }
}

fn spawn_blocking_mux(
    mux_rx: mpsc::Receiver<MuxCmd>,
    raw_tx: mpsc::Sender<Result<LogEvent, rustern_core::source::LogSourceError>>,
) -> tokio::task::JoinHandle<()> {
    spawn_mux_task(
        mux_rx,
        raw_tx,
        None,
        BackpressurePolicy::Blocking,
        MuxMetrics::new(None),
        CancellationToken::new(),
    )
}

fn spawn_policy_mux(
    mux_rx: mpsc::Receiver<MuxCmd>,
    raw_tx: mpsc::Sender<Result<LogEvent, rustern_core::source::LogSourceError>>,
    policy: BackpressurePolicy,
) -> tokio::task::JoinHandle<()> {
    spawn_mux_task(
        mux_rx,
        raw_tx,
        None,
        policy,
        MuxMetrics::new(None),
        CancellationToken::new(),
    )
}

fn sample_key(i: usize) -> SourceKey {
    SourceKey {
        context: ContextName("bench".into()),
        namespace: "default".into(),
        pod: format!("pod-{i}"),
        container: "app".into(),
        uid: format!("uid-{i}"),
    }
}

async fn shutdown_mux_after_render_batch(
    mux_tx: mpsc::Sender<MuxCmd>,
    mux_h: tokio::task::JoinHandle<()>,
    mut render_rx: mpsc::Receiver<rustern_core::render::RenderCommand>,
    fwd_h: tokio::task::JoinHandle<()>,
    token: CancellationToken,
    expected: usize,
) -> usize {
    let (batch_tx, batch_rx) = oneshot::channel();
    let drain_h = tokio::spawn(async move {
        let mut received = 0usize;
        let mut batch_tx = Some(batch_tx);
        while let Some(cmd) = render_rx.recv().await {
            if !matches!(cmd, RenderCommand::Line(_)) {
                continue;
            }
            received += 1;
            if received == expected
                && let Some(tx) = batch_tx.take()
            {
                let _ = tx.send(());
            }
        }
        received
    });

    batch_rx
        .await
        .expect("render batch not delivered before stream ended");
    drop(mux_tx);
    mux_h.await.expect("mux task");
    token.cancel();
    fwd_h.await.expect("forward task");
    drain_h.await.expect("render drain")
}

async fn shutdown_mux_after_batch(
    mux_tx: mpsc::Sender<MuxCmd>,
    mux_h: tokio::task::JoinHandle<()>,
    mut raw_rx: mpsc::Receiver<Result<LogEvent, rustern_core::source::LogSourceError>>,
    expected: usize,
) -> usize {
    let mut received = 0usize;
    while received < expected {
        let row = raw_rx
            .recv()
            .await
            .expect("raw stream ended before batch complete");
        let _ = black_box(row);
        received += 1;
    }
    drop(mux_tx);
    mux_h.await.expect("mux task");
    while raw_rx.recv().await.is_some() {}
    received
}

async fn shutdown_mux_after_batch_lossy(
    mux_tx: mpsc::Sender<MuxCmd>,
    mux_h: tokio::task::JoinHandle<()>,
    mut raw_rx: mpsc::Receiver<Result<LogEvent, rustern_core::source::LogSourceError>>,
) -> usize {
    drop(mux_tx);
    tokio::time::timeout(Duration::from_secs(5), mux_h)
        .await
        .expect("mux task timed out")
        .expect("mux task");
    let mut received = 0usize;
    loop {
        match tokio::time::timeout(Duration::from_millis(10), raw_rx.recv()).await {
            Ok(Some(row)) => {
                let _ = black_box(row);
                received += 1;
            }
            Ok(None) => break,
            Err(_) => break,
        }
    }
    received
}

fn sample_event(i: usize) -> LogEvent {
    LogEvent {
        source: Arc::new(SourceMeta {
            context: ContextName("bench".into()),
            namespace: "default".into(),
            pod: format!("pod-{}", i % 6),
            container: "app".into(),
            kind: SourceKind::PodLog,
            node: None,
            labels: Arc::new(Labels::default()),
            uid: format!("uid-{}", i % 6),
        }),
        timestamp: Utc::now(),
        message: Arc::from(format!("bench line {i} padding=0123456789")),
        structured: None,
        level: None,
        palette_index: None,
        container_palette_index: None,
    }
}

fn bench_mux_drain(c: &mut Criterion) {
    let rt = Runtime::new().expect("tokio runtime");
    let mut group = c.benchmark_group("mux_drain");
    group.throughput(Throughput::Elements(BATCH));

    group.bench_function("single_stream", |b| {
        b.iter(|| {
            let count = rt.block_on(async {
                let (mux_tx, mux_rx) = mpsc::channel(256);
                let (raw_tx, raw_rx) = mpsc::channel(RAW_BUFFER);
                let mux_h = spawn_blocking_mux(mux_rx, raw_tx);

                let events: Vec<_> = (0..BATCH as usize).map(|i| Ok(sample_event(i))).collect();
                let key = sample_key(0);
                mux_tx
                    .send(MuxCmd::Add(key, Box::pin(stream::iter(events.into_iter()))))
                    .await
                    .expect("mux add");

                shutdown_mux_after_batch(mux_tx, mux_h, raw_rx, BATCH as usize).await
            });
            assert_eq!(count, BATCH as usize);
        });
    });

    group.finish();
}

fn bench_mux_forward(c: &mut Criterion) {
    let rt = Runtime::new().expect("tokio runtime");
    let mut group = c.benchmark_group("mux_forward");
    group.throughput(Throughput::Elements(BATCH));

    for lossy in [false, true] {
        group.bench_with_input(
            BenchmarkId::new("blocking_render", lossy),
            &lossy,
            |b, &lossy| {
                b.iter(|| {
                    let count = rt.block_on(async {
                        let (mux_tx, mux_rx) = mpsc::channel(256);
                        let (raw_tx, raw_rx) = mpsc::channel(RAW_BUFFER);
                        let (render_tx, render_rx) = mpsc::channel(RENDER_BUFFER);
                        let token = CancellationToken::new();
                        let fwd_h = tokio::spawn(forward_to_render(
                            ReceiverStream::new(raw_rx),
                            render_tx,
                            fwd_cfg(lossy),
                            LossyMetrics::new(None),
                            token.clone(),
                        ));
                        let mux_h = spawn_blocking_mux(mux_rx, raw_tx);
                        let events: Vec<_> =
                            (0..BATCH as usize).map(|i| Ok(sample_event(i))).collect();
                        mux_tx
                            .send(MuxCmd::Add(
                                sample_key(0),
                                Box::pin(stream::iter(events.into_iter())),
                            ))
                            .await
                            .expect("mux add");

                        shutdown_mux_after_render_batch(
                            mux_tx,
                            mux_h,
                            render_rx,
                            fwd_h,
                            token,
                            BATCH as usize,
                        )
                        .await
                    });
                    assert_eq!(count, BATCH as usize);
                });
            },
        );
    }

    group.finish();
}

fn bench_mux_pipeline_forward(c: &mut Criterion) {
    let rt = Runtime::new().expect("tokio runtime");
    let mut group = c.benchmark_group("mux_pipeline_forward");
    group.throughput(Throughput::Elements(BATCH));

    for lossy in [false, true] {
        group.bench_with_input(BenchmarkId::new("e2e", lossy), &lossy, |b, &lossy| {
            b.iter(|| {
                let count = rt.block_on(async {
                    let (mux_tx, mux_rx) = mpsc::channel(256);
                    let (raw_tx, raw_rx) = mpsc::channel(RAW_BUFFER);
                    let (render_tx, render_rx) = mpsc::channel(RENDER_BUFFER);
                    let token = CancellationToken::new();
                    let pipe_stream = PipelineSpecBuilder::new()
                        .build(ExitWatchState::new(token.clone()))
                        .apply(ReceiverStream::new(raw_rx));
                    let fwd_h = tokio::spawn(forward_to_render(
                        pipe_stream,
                        render_tx,
                        fwd_cfg(lossy),
                        LossyMetrics::new(None),
                        token.clone(),
                    ));
                    let mux_h = spawn_blocking_mux(mux_rx, raw_tx);
                    let events: Vec<_> = (0..BATCH as usize).map(|i| Ok(sample_event(i))).collect();
                    mux_tx
                        .send(MuxCmd::Add(
                            sample_key(0),
                            Box::pin(stream::iter(events.into_iter())),
                        ))
                        .await
                        .expect("mux add");

                    shutdown_mux_after_render_batch(
                        mux_tx,
                        mux_h,
                        render_rx,
                        fwd_h,
                        token,
                        BATCH as usize,
                    )
                    .await
                });
                assert_eq!(count, BATCH as usize);
            });
        });
    }

    group.finish();
}

fn bench_mux_tiered_policy(c: &mut Criterion) {
    let rt = Runtime::new().expect("tokio runtime");
    let mut group = c.benchmark_group("bench_mux_tiered_policy");
    group.throughput(Throughput::Elements(BATCH));

    for policy in [BackpressurePolicy::Blocking, BackpressurePolicy::Lossy] {
        group.bench_with_input(
            BenchmarkId::new("mux_policy", format!("{policy:?}")),
            &policy,
            |b, &policy| {
                b.iter(|| {
                    let count = rt.block_on(async {
                        let (mux_tx, mux_rx) = mpsc::channel(256);
                        let (raw_tx, raw_rx) = mpsc::channel(TIERED_RAW_BUFFER);
                        let mux_h = spawn_policy_mux(mux_rx, raw_tx, policy);
                        let events: Vec<_> =
                            (0..BATCH as usize).map(|i| Ok(sample_event(i))).collect();
                        mux_tx
                            .send(MuxCmd::Add(
                                sample_key(0),
                                Box::pin(stream::iter(events.into_iter())),
                            ))
                            .await
                            .expect("mux add");

                        match policy {
                            BackpressurePolicy::Blocking => {
                                shutdown_mux_after_batch(mux_tx, mux_h, raw_rx, BATCH as usize)
                                    .await
                            }
                            BackpressurePolicy::Lossy => {
                                shutdown_mux_after_batch_lossy(mux_tx, mux_h, raw_rx).await
                            }
                        }
                    });
                    if policy == BackpressurePolicy::Blocking {
                        assert_eq!(count, BATCH as usize);
                    } else {
                        black_box(count);
                    }
                });
            },
        );
    }

    group.finish();
}

async fn drain_render_consumer(
    mut render_rx: mpsc::Receiver<RenderCommand>,
    mode: ConsumerMode,
    expected: usize,
) -> usize {
    let mut received = 0usize;
    while received < expected {
        let Some(cmd) = render_rx.recv().await else {
            break;
        };
        if matches!(cmd, RenderCommand::Line(_)) {
            received += 1;
            if mode == ConsumerMode::Slow {
                tokio::time::sleep(SLOW_CONSUMER_DELAY).await;
            }
        }
    }
    received
}

async fn run_multistream_matrix(
    streams: usize,
    consumer: ConsumerMode,
    policy: BackpressurePolicy,
    lossy_forward: bool,
) -> MatrixRun {
    let total_lines = streams.saturating_mul(LINES_PER_STREAM);
    let render_buffer = match consumer {
        ConsumerMode::Unbounded => RENDER_BUFFER,
        ConsumerMode::Slow => SLOW_RENDER_BUFFER,
    };
    let raw_buffer = if policy == BackpressurePolicy::Lossy && consumer == ConsumerMode::Slow {
        TIERED_RAW_BUFFER
    } else {
        RAW_BUFFER
    };

    let (mux_tx, mux_rx) = mpsc::channel(256);
    let (raw_tx, raw_rx) = mpsc::channel(raw_buffer);
    let (render_tx, render_rx) = mpsc::channel(render_buffer);
    let token = CancellationToken::new();
    let mux_metrics = MuxMetrics::new(None);
    let forward_metrics = LossyMetrics::new(None);
    let mux_h = spawn_mux_task(
        mux_rx,
        raw_tx,
        None,
        policy,
        mux_metrics.clone(),
        token.clone(),
    );
    let pipe_stream = PipelineSpecBuilder::new()
        .build(ExitWatchState::new(token.clone()))
        .apply(ReceiverStream::new(raw_rx));
    let fwd_h = tokio::spawn(forward_to_render(
        pipe_stream,
        render_tx.clone(),
        fwd_cfg(lossy_forward),
        forward_metrics.clone(),
        token.clone(),
    ));
    drop(render_tx);
    let drain_h = tokio::spawn(drain_render_consumer(render_rx, consumer, total_lines));

    for s in 0..streams {
        let events: Vec<_> = (0..LINES_PER_STREAM)
            .map(|i| Ok(sample_event(s * LINES_PER_STREAM + i)))
            .collect();
        mux_tx
            .send(MuxCmd::Add(
                sample_key(s),
                Box::pin(stream::iter(events.into_iter())),
            ))
            .await
            .expect("mux add");
    }

    drop(mux_tx);
    mux_h.await.expect("mux task");
    fwd_h.await.expect("forward task");
    token.cancel();
    let delivered = drain_h.await.expect("render drain");

    MatrixRun {
        delivered,
        mux_drops: mux_metrics.mux_drop_count(),
        forward_drops: forward_metrics.drop_count(),
    }
}

fn bench_multistream_matrix(c: &mut Criterion) {
    let rt = Runtime::new().expect("tokio runtime");
    let mut group = c.benchmark_group("multistream_matrix");
    group.sample_size(10);

    for streams in MATRIX_STREAMS {
        let total = (streams * LINES_PER_STREAM) as u64;
        group.throughput(Throughput::Elements(total));

        for consumer in [ConsumerMode::Unbounded, ConsumerMode::Slow] {
            for policy in [BackpressurePolicy::Blocking, BackpressurePolicy::Lossy] {
                let lossy_forward = policy == BackpressurePolicy::Lossy;
                let id = BenchmarkId::from_parameter(format!(
                    "streams={streams}/consumer={consumer:?}/policy={policy:?}"
                ));
                group.bench_with_input(id, &(), |b, _| {
                    b.iter(|| {
                        let run = rt.block_on(run_multistream_matrix(
                            streams,
                            consumer,
                            policy,
                            lossy_forward,
                        ));
                        if policy == BackpressurePolicy::Blocking
                            && consumer == ConsumerMode::Unbounded
                        {
                            assert_eq!(
                                run.delivered,
                                streams.saturating_mul(LINES_PER_STREAM),
                                "blocking unbounded consumer should deliver all lines"
                            );
                        }
                        black_box((run.delivered, run.mux_drops, run.forward_drops));
                    });
                });
            }
        }
    }

    group.finish();
}

async fn run_render_task_sink(lines: usize) -> usize {
    let formatter = Arc::new(DefaultLineFormatter {
        timestamp_style: TimestampStyle::Omit,
        timestamp_zone: TimestampZone::Utc,
        color_enabled: false,
        pod_colors: false,
        container_colors: false,
    });
    let (render_tx, render_rx) = mpsc::channel(RENDER_BUFFER);
    let sink = tokio::io::sink();
    let render_h = tokio::spawn(render_task(render_rx, sink, formatter));

    for i in 0..lines {
        render_tx
            .send(RenderCommand::Line(sample_event(i)))
            .await
            .expect("render send");
    }
    render_tx
        .send(RenderCommand::Shutdown)
        .await
        .expect("render shutdown");
    render_h.await.expect("render task").expect("render io");
    lines
}

async fn run_render_task_duplex(lines: usize) -> usize {
    let formatter = Arc::new(DefaultLineFormatter {
        timestamp_style: TimestampStyle::Omit,
        timestamp_zone: TimestampZone::Utc,
        color_enabled: false,
        pod_colors: false,
        container_colors: false,
    });
    let (mut rd, wr) = tokio::io::duplex(16_384);
    let (render_tx, render_rx) = mpsc::channel(RENDER_BUFFER);
    let token = CancellationToken::new();
    let render_h = tokio::spawn(render_task(render_rx, wr, formatter));
    let read_h = tokio::spawn(async move {
        let mut buf = vec![0u8; 256];
        let mut read_total = 0usize;
        loop {
            match rd.read(&mut buf).await {
                Ok(0) => break,
                Ok(n) => read_total += n,
                Err(_) => break,
            }
        }
        read_total
    });
    let _flush_h = tokio::spawn(flush_ticker(
        render_tx.clone(),
        token.clone(),
        Duration::from_millis(50),
    ));

    for i in 0..lines {
        render_tx
            .send(RenderCommand::Line(sample_event(i)))
            .await
            .expect("render send");
    }
    render_tx
        .send(RenderCommand::Shutdown)
        .await
        .expect("render shutdown");
    render_h.await.expect("render task").expect("render io");
    token.cancel();
    read_h.await.expect("duplex drain")
}

async fn run_mux_render_task_e2e(lines: usize, sink: bool) -> usize {
    let formatter = Arc::new(DefaultLineFormatter {
        timestamp_style: TimestampStyle::Omit,
        timestamp_zone: TimestampZone::Utc,
        color_enabled: false,
        pod_colors: false,
        container_colors: false,
    });
    let (mux_tx, mux_rx) = mpsc::channel(256);
    let (raw_tx, raw_rx) = mpsc::channel(RAW_BUFFER);
    let (render_tx, render_rx) = mpsc::channel(RENDER_BUFFER);
    let token = CancellationToken::new();
    let fwd_h = tokio::spawn(forward_to_render(
        ReceiverStream::new(raw_rx),
        render_tx.clone(),
        fwd_cfg(false),
        LossyMetrics::new(None),
        token.clone(),
    ));
    let mux_h = spawn_blocking_mux(mux_rx, raw_tx);
    let events: Vec<_> = (0..lines).map(|i| Ok(sample_event(i))).collect();
    mux_tx
        .send(MuxCmd::Add(
            sample_key(0),
            Box::pin(stream::iter(events.into_iter())),
        ))
        .await
        .expect("mux add");

    let render_h = if sink {
        let sink = tokio::io::sink();
        tokio::spawn(render_task(render_rx, sink, formatter))
    } else {
        let (mut rd, wr) = tokio::io::duplex(16_384);
        let h = tokio::spawn(render_task(render_rx, wr, formatter));
        tokio::spawn(async move {
            let mut buf = vec![0u8; 512];
            loop {
                match rd.read(&mut buf).await {
                    Ok(0) | Err(_) => break,
                    Ok(_) => {}
                }
            }
        });
        h
    };

    drop(mux_tx);
    mux_h.await.expect("mux task");
    fwd_h.await.expect("forward task");
    token.cancel();
    render_tx
        .send(RenderCommand::Shutdown)
        .await
        .expect("render shutdown");
    render_h.await.expect("render task").expect("render io");
    lines
}

fn bench_render_task(c: &mut Criterion) {
    let rt = Runtime::new().expect("tokio runtime");
    let lines = BATCH as usize;

    let mut sink_group = c.benchmark_group("render_task_sink");
    sink_group.throughput(Throughput::Elements(BATCH));
    sink_group.bench_function("direct", |b| {
        b.iter(|| {
            let count = rt.block_on(run_render_task_sink(lines));
            assert_eq!(count, lines);
        });
    });
    sink_group.finish();

    let mut duplex_group = c.benchmark_group("render_task_duplex");
    duplex_group.throughput(Throughput::Elements(BATCH));
    duplex_group.bench_function("with_flush_ticker", |b| {
        b.iter(|| {
            let nbytes = rt.block_on(run_render_task_duplex(lines));
            black_box(nbytes);
        });
    });
    duplex_group.finish();

    let mut e2e_group = c.benchmark_group("mux_render_task_e2e");
    e2e_group.throughput(Throughput::Elements(BATCH));
    for (name, sink) in [("sink", true), ("duplex", false)] {
        e2e_group.bench_with_input(BenchmarkId::new("writer", name), &sink, |b, &sink| {
            b.iter(|| {
                let count = rt.block_on(run_mux_render_task_e2e(lines, sink));
                assert_eq!(count, lines);
            });
        });
    }
    e2e_group.finish();
}

criterion_group!(
    benches,
    bench_mux_drain,
    bench_mux_forward,
    bench_mux_pipeline_forward,
    bench_mux_tiered_policy,
    bench_multistream_matrix,
    bench_render_task
);
criterion_main!(benches);
