use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use chrono::{DateTime, TimeZone, Utc};
use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use futures::stream::{self, Stream, StreamExt};
use regex::Regex;
use rustern_core::format_display::{TimestampStyle, TimestampZone};
use rustern_core::pipeline::{
    ColorAssignOpts, ExitWatchState, QueryMode, include_exclude, jq_evaluate, json_annotate,
    level_classify, validate_filter,
};
use rustern_core::render::LineFormatter;
use rustern_core::render::default_renderer::DefaultLineFormatter;
use rustern_core::render::ext_json_renderer::ExtJsonLineFormatter;
use rustern_core::render::highlight::{SternHighlightLineFormatter, compile_stern_highlight_regex};
use rustern_core::render::{RenderCommand, render_task};
use rustern_core::runtime::PipelineSpecBuilder;
use rustern_core::source::{
    ContextName, Labels, LogEvent, LogSourceError, SourceKey, SourceKind, SourceMeta,
};
use rustern_core::{LogLineTimestampResolver, split_log_line};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

const BATCH: u64 = 1_000;
const CURSOR_BATCH: u64 = 2_000;

fn plain_message_256() -> String {
    "GET /api/v1/widgets?page=1&limit=50 HTTP/1.1 200 OK upstream latency=42ms trace_id=abc123def456 user_agent=rustern-bench/0.1 request_size=512 response_size=2048 cache=miss region=us-west-2 pod=frontend-7d8f9c-xk2m9 container=app extra_padding=0123456789"
        .to_string()
}

fn json_message_4k() -> String {
    let mut s = String::from(
        r#"{"level":"info","msg":"request completed","trace_id":"abc123","user_id":4242,"method":"GET","path":"/api/v1/widgets","status":200,"latency_ms":42,"bytes_sent":2048,"labels":{"env":"prod","team":"platform"},"payload":""#,
    );
    while s.len() < 3_900 {
        s.push('x');
    }
    s.push_str(r#""}"#);
    s
}

fn kube_log_line(message: &str) -> String {
    format!("2024-03-15T10:30:45.123456789Z {message}")
}

fn sample_event(message: &str) -> LogEvent {
    LogEvent {
        source: Arc::new(SourceMeta {
            context: ContextName("ctx".into()),
            namespace: "default".into(),
            pod: "frontend-7d8f9c-xk2m9".into(),
            container: "app".into(),
            kind: SourceKind::PodLog,
            node: None,
            labels: Arc::new(Labels::default()),
            uid: "uid-bench".into(),
            palette_index: None,
            container_palette_index: None,
        }),
        timestamp: Utc.with_ymd_and_hms(2024, 3, 15, 10, 30, 45).unwrap(),
        message: Arc::from(message),
        structured: None,
        level: None,
        palette_index: Some(2),
        container_palette_index: Some(1),
    }
}

fn event_batch(message: &str) -> Vec<LogEvent> {
    (0..BATCH).map(|_| sample_event(message)).collect()
}

fn bench_include_exclude(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().expect("runtime");
    let mut group = c.benchmark_group("include_exclude");
    group.throughput(Throughput::Elements(BATCH));

    for (name, msg) in [
        ("plain_256", plain_message_256()),
        ("json_4k", json_message_4k()),
    ] {
        let batch = event_batch(&msg);
        let includes: Arc<[Regex]> = vec![Regex::new("error|warn|GET").unwrap()].into();
        let excludes: Arc<[Regex]> = vec![Regex::new("healthz").unwrap()].into();

        group.bench_with_input(BenchmarkId::new("filter", name), &batch, |b, batch| {
            b.iter(|| {
                let events = batch.clone();
                let includes = Arc::clone(&includes);
                let excludes = Arc::clone(&excludes);
                let out = rt.block_on(async move {
                    include_exclude(
                        stream::iter(events.into_iter().map(Ok::<_, LogSourceError>)),
                        includes,
                        excludes,
                    )
                    .collect::<Vec<_>>()
                    .await
                });
                black_box(out);
            });
        });
    }
    group.finish();
}

fn bench_json_pipeline(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().expect("runtime");
    let mut group = c.benchmark_group("json_pipeline");
    group.throughput(Throughput::Elements(BATCH));

    let batch = event_batch(&json_message_4k());
    let jq = validate_filter(".msg").expect("jq compile");

    group.bench_function("annotate_classify_jq", |b| {
        b.iter(|| {
            let events = batch.clone();
            let jq = jq.clone();
            let out = rt.block_on(async move {
                let s = stream::iter(events.into_iter().map(Ok::<_, LogSourceError>));
                let s = json_annotate(s, true);
                let s = level_classify(s, Some("level".into()));
                let s = jq_evaluate(s, jq, QueryMode::Filter);
                s.collect::<Vec<_>>().await
            });
            black_box(out);
        });
    });

    for mode in [QueryMode::Replace, QueryMode::Append] {
        let mode_name = match mode {
            QueryMode::Replace => "jq_replace",
            QueryMode::Append => "jq_append",
            QueryMode::Filter => unreachable!(),
        };
        group.bench_function(mode_name, |b| {
            b.iter(|| {
                let events = batch.clone();
                let jq = jq.clone();
                let out = rt.block_on(async move {
                    let s = stream::iter(events.into_iter().map(Ok::<_, LogSourceError>));
                    let s = json_annotate(s, true);
                    let s = level_classify(s, Some("level".into()));
                    let s = jq_evaluate(s, jq, mode);
                    s.collect::<Vec<_>>().await
                });
                black_box(out);
            });
        });
    }

    let skip_spec = PipelineSpecBuilder::new().build(ExitWatchState::new(CancellationToken::new()));
    let full_spec = PipelineSpecBuilder::new()
        .with_level_key(Some("level".into()))
        .with_jq(Some((jq.clone(), QueryMode::Filter)))
        .build(ExitWatchState::new(CancellationToken::new()));

    for (name, spec) in [("skip_annotate", skip_spec), ("needs_annotate", full_spec)] {
        group.bench_with_input(
            BenchmarkId::new("pipeline_spec", name),
            &batch,
            |b, batch| {
                b.iter(|| {
                    let events = batch.clone();
                    let spec = spec.clone();
                    let out = rt.block_on(async move {
                        spec.apply(stream::iter(
                            events.into_iter().map(Ok::<_, LogSourceError>),
                        ))
                        .collect::<Vec<_>>()
                        .await
                    });
                    black_box(out);
                });
            },
        );
    }

    group.finish();
}

fn bench_extjson_formatter(c: &mut Criterion) {
    let mut group = c.benchmark_group("extjson_formatter");

    let json_msg = json_message_4k();
    let json_event = sample_event(&json_msg);
    let plain_event = sample_event(&plain_message_256());

    for (name, pretty, event) in [
        ("extjson_json_4k", false, &json_event),
        ("extjson_plain_256", false, &plain_event),
        ("ppextjson_json_4k", true, &json_event),
    ] {
        let formatter = ExtJsonLineFormatter {
            all_namespaces: false,
            pretty,
        };
        group.bench_with_input(BenchmarkId::new("format_line", name), event, |b, event| {
            b.iter(|| black_box(formatter.format_line(black_box(event))));
        });
    }
    group.finish();
}

fn bench_default_formatter(c: &mut Criterion) {
    let mut group = c.benchmark_group("default_formatter");

    let formatter = DefaultLineFormatter {
        timestamp_style: TimestampStyle::SternShort,
        timestamp_zone: TimestampZone::Utc,
        color_enabled: true,
        pod_colors: true,
        container_colors: true,
    };

    for (name, msg) in [
        ("plain_256", plain_message_256()),
        ("json_4k", json_message_4k()),
    ] {
        let event = sample_event(&msg);
        group.bench_with_input(BenchmarkId::new("format_into", name), &event, |b, event| {
            let mut buf = String::with_capacity(512);
            b.iter(|| {
                buf.clear();
                formatter.format_into(black_box(event), &mut buf);
                black_box(&buf);
            });
        });
    }
    group.finish();
}

fn bench_highlight_formatter(c: &mut Criterion) {
    let mut group = c.benchmark_group("highlight_formatter");

    let inner = Arc::new(DefaultLineFormatter {
        timestamp_style: TimestampStyle::SternShort,
        timestamp_zone: TimestampZone::Utc,
        color_enabled: true,
        pod_colors: true,
        container_colors: false,
    });
    let re = compile_stern_highlight_regex(&["error".into(), "GET".into()], &["warn".into()])
        .expect("regex")
        .expect("pattern");
    let formatter = SternHighlightLineFormatter::new(inner, re);
    let event = sample_event(&plain_message_256());

    group.bench_function("format_into", |b| {
        let mut buf = String::with_capacity(512);
        b.iter(|| {
            buf.clear();
            formatter.format_into(black_box(&event), &mut buf);
            black_box(&buf);
        });
    });
    group.finish();
}

fn bench_render_task(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().expect("runtime");
    let mut group = c.benchmark_group("render_task");
    group.throughput(Throughput::Elements(BATCH));

    let formatter = Arc::new(DefaultLineFormatter {
        timestamp_style: TimestampStyle::SternShort,
        timestamp_zone: TimestampZone::Utc,
        color_enabled: true,
        pod_colors: true,
        container_colors: true,
    });
    let msg = plain_message_256();
    let events: Vec<_> = event_batch(&msg);

    group.bench_function("format_channel_sink", |b| {
        b.iter(|| {
            let events = events.clone();
            let formatter = Arc::clone(&formatter);
            rt.block_on(async move {
                let (tx, rx) = mpsc::channel::<RenderCommand>(256);
                let sink = tokio::io::sink();
                let render = tokio::spawn(render_task(rx, sink, formatter));
                for ev in events {
                    tx.send(RenderCommand::Line(ev)).await.unwrap();
                }
                tx.send(RenderCommand::Shutdown).await.unwrap();
                render.await.unwrap().unwrap();
            });
        });
    });

    group.finish();
}

fn bench_pipeline_spec_apply(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().expect("runtime");
    let mut group = c.benchmark_group("pipeline_spec_apply");
    group.throughput(Throughput::Elements(BATCH));

    let msg = plain_message_256();
    let batch = event_batch(&msg);

    let cases: Vec<(&str, _)> = vec![
        (
            "minimal",
            PipelineSpecBuilder::new().build(ExitWatchState::new(CancellationToken::new())),
        ),
        (
            "filtered",
            PipelineSpecBuilder::new()
                .with_includes(vec![Regex::new("GET|POST").unwrap()])
                .with_excludes(vec![Regex::new("healthz").unwrap()])
                .build(ExitWatchState::new(CancellationToken::new())),
        ),
        (
            "full",
            PipelineSpecBuilder::new()
                .with_includes(vec![Regex::new("GET|error").unwrap()])
                .with_excludes(vec![Regex::new("healthz").unwrap()])
                .with_level_key(Some("level".into()))
                .with_color_assign(ColorAssignOpts {
                    pod_colors: true,
                    container_colors: true,
                    diff_container: true,
                })
                .build(ExitWatchState::new(CancellationToken::new())),
        ),
    ];

    for (name, spec) in cases {
        group.bench_with_input(BenchmarkId::new("apply", name), &batch, |b, batch| {
            b.iter(|| {
                let events = batch.clone();
                let spec = spec.clone();
                let out = rt.block_on(async move {
                    spec.apply(stream::iter(
                        events.into_iter().map(Ok::<_, LogSourceError>),
                    ))
                    .collect::<Vec<_>>()
                    .await
                });
                black_box(out);
            });
        });
    }
    group.finish();
}

fn bench_parse_log_line(c: &mut Criterion) {
    let mut group = c.benchmark_group("parse_log_line");

    for (name, msg) in [
        ("plain_256", plain_message_256()),
        ("json_4k", json_message_4k()),
    ] {
        let line = kube_log_line(&msg);
        group.bench_with_input(BenchmarkId::new("parse", name), &line, |b, line| {
            b.iter(|| {
                let (parsed, msg) = split_log_line(black_box(line.as_bytes()));
                black_box((parsed, msg));
            });
        });
    }
    group.finish();
}

fn bench_ingest_log_line(c: &mut Criterion) {
    let mut group = c.benchmark_group("ingest_log_line");

    for (name, msg) in [
        ("plain_256", plain_message_256()),
        ("json_4k", json_message_4k()),
    ] {
        let mut buf = kube_log_line(&msg).into_bytes();
        buf.push(b'\n');
        group.bench_with_input(BenchmarkId::new("buffer_to_event", name), &buf, |b, buf| {
            let mut resolver = LogLineTimestampResolver::default();
            b.iter(|| {
                let (parsed, msg) = split_log_line(black_box(buf.as_slice()));
                let (ts, message) = resolver.resolve(parsed, msg);
                black_box((ts, message));
            });
        });
    }
    group.finish();
}

/// Mirrors production cursor-tracking stream poll overhead for A/B comparison
/// without attach/mux wiring (see DSK-97 / benches README).
struct BenchCursorTrackingStream<S> {
    inner: S,
    key: SourceKey,
    cursor_tx: mpsc::UnboundedSender<(SourceKey, DateTime<Utc>)>,
}

impl<S> BenchCursorTrackingStream<S> {
    fn new(
        inner: S,
        key: SourceKey,
        cursor_tx: mpsc::UnboundedSender<(SourceKey, DateTime<Utc>)>,
    ) -> Self {
        Self {
            inner,
            key,
            cursor_tx,
        }
    }
}

impl<S> Stream for BenchCursorTrackingStream<S>
where
    S: Stream<Item = Result<LogEvent, LogSourceError>> + Unpin,
{
    type Item = Result<LogEvent, LogSourceError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.inner).poll_next(cx) {
            Poll::Ready(Some(Ok(event))) => {
                let _ = self.cursor_tx.send((self.key.clone(), event.timestamp));
                Poll::Ready(Some(Ok(event)))
            }
            other => other,
        }
    }
}

fn bench_cursor_tracking_ab(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().expect("runtime");
    let mut group = c.benchmark_group("cursor_tracking_ab");
    group.throughput(Throughput::Elements(CURSOR_BATCH));

    let msg = plain_message_256();
    let batch: Vec<LogEvent> = (0..CURSOR_BATCH).map(|_| sample_event(&msg)).collect();
    let key = SourceKey {
        context: ContextName("ctx".into()),
        namespace: "default".into(),
        pod: "frontend-7d8f9c-xk2m9".into(),
        container: "app".into(),
        uid: "uid-bench".into(),
    };
    let spec = PipelineSpecBuilder::new().build(ExitWatchState::new(CancellationToken::new()));

    for (name, with_cursor) in [("direct", false), ("cursor_wrapped", true)] {
        group.bench_with_input(
            BenchmarkId::new("pipeline", name),
            &with_cursor,
            |b, &wrap| {
                b.iter(|| {
                    let events = batch.clone();
                    let spec = spec.clone();
                    let key = key.clone();
                    let out = rt.block_on(async move {
                        let base = stream::iter(events.into_iter().map(Ok::<_, LogSourceError>));
                        if wrap {
                            let (cursor_tx, mut cursor_rx) = mpsc::unbounded_channel();
                            let piped = spec.apply(Box::pin(BenchCursorTrackingStream::new(
                                base, key, cursor_tx,
                            )));
                            let out = piped.collect::<Vec<_>>().await;
                            while cursor_rx.try_recv().is_ok() {}
                            out
                        } else {
                            spec.apply(base).collect::<Vec<_>>().await
                        }
                    });
                    black_box(out);
                });
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_include_exclude,
    bench_json_pipeline,
    bench_extjson_formatter,
    bench_default_formatter,
    bench_highlight_formatter,
    bench_render_task,
    bench_pipeline_spec_apply,
    bench_parse_log_line,
    bench_ingest_log_line,
    bench_cursor_tracking_ab,
);
criterion_main!(benches);
