use std::sync::Arc;

use chrono::Utc;
use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use futures::stream;
use rustern_core::pipeline::ExitWatchState;
use rustern_core::render::RenderCommand;
use rustern_core::runtime::{
    LossyMetrics, MuxCmd, PipelineSpecBuilder, RuntimeFwdConfig, forward_to_render, spawn_mux_task,
};
use rustern_core::source::{ContextName, Labels, LogEvent, SourceKey, SourceKind, SourceMeta};
use tokio::runtime::Runtime;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;

const BATCH: u64 = 2_000;
const RAW_BUFFER: usize = 4_096;
const RENDER_BUFFER: usize = 4_096;

fn fwd_cfg(lossy: bool) -> RuntimeFwdConfig {
    RuntimeFwdConfig {
        buffer_size: RENDER_BUFFER,
        lossy,
        stats: None,
        max_log_requests: 50,
    }
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
                let mux_h = spawn_mux_task(mux_rx, raw_tx, None);

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
                        let mux_h = spawn_mux_task(mux_rx, raw_tx, None);
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
                    let mux_h = spawn_mux_task(mux_rx, raw_tx, None);
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

criterion_group!(
    benches,
    bench_mux_drain,
    bench_mux_forward,
    bench_mux_pipeline_forward
);
criterion_main!(benches);
