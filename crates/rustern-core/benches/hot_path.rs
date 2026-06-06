use std::sync::Arc;

use chrono::{TimeZone, Utc};
use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use futures::stream::{self, StreamExt};
use regex::Regex;
use rustern_core::format_display::{TimestampStyle, TimestampZone};
use rustern_core::parse_log_line;
use rustern_core::pipeline::{
    QueryMode, include_exclude, jq_evaluate, json_annotate, level_classify, validate_filter,
};
use rustern_core::render::LineFormatter;
use rustern_core::render::default_renderer::DefaultLineFormatter;
use rustern_core::render::highlight::{SternHighlightLineFormatter, compile_stern_highlight_regex};
use rustern_core::source::{ContextName, Labels, LogEvent, LogSourceError, SourceKind, SourceMeta};

const BATCH: u64 = 1_000;

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
        let includes = vec![Regex::new("error|warn|GET").unwrap()];
        let excludes = vec![Regex::new("healthz").unwrap()];

        group.bench_with_input(BenchmarkId::new("filter", name), &batch, |b, batch| {
            b.iter(|| {
                let events = batch.clone();
                let includes = includes.clone();
                let excludes = excludes.clone();
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
                let s = json_annotate(s);
                let s = level_classify(s, Some("level".into()));
                let s = jq_evaluate(s, jq, QueryMode::Filter);
                s.collect::<Vec<_>>().await
            });
            black_box(out);
        });
    });
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
        group.bench_with_input(BenchmarkId::new("format_line", name), &event, |b, event| {
            b.iter(|| black_box(formatter.format_line(black_box(event))));
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

    group.bench_function("format_line", |b| {
        b.iter(|| black_box(formatter.format_line(black_box(&event))));
    });
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
            b.iter(|| black_box(parse_log_line(black_box(line.as_str()))));
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_include_exclude,
    bench_json_pipeline,
    bench_default_formatter,
    bench_highlight_formatter,
    bench_parse_log_line,
);
criterion_main!(benches);
