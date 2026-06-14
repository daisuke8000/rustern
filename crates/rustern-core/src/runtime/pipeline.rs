//! `run` only: apply pipeline stages to a raw `LogEvent` stream.

use std::sync::Arc;

use futures::stream::{BoxStream, Stream};
use regex::Regex;

use crate::pipeline::{
    ColorAssignOpts, CompiledFilter, ExitOnLevel, ExitWatchState, FilterOn, PipelineStageOrder,
    QueryMode, color_assign, exit_watch_level, exit_watch_message, include_exclude, jq_evaluate,
    json_annotate, level_classify,
};
use crate::source::{LogEvent, LogSourceError};

// Hidden migration surface — prefer `spec::PipelineSpec`.
#[doc(hidden)]
#[derive(Clone)]
pub struct PipelineStages {
    pub(crate) includes: Arc<[Regex]>,
    pub(crate) excludes: Arc<[Regex]>,
    pub(crate) filter_on: FilterOn,
    pub(crate) jq: Option<(CompiledFilter, QueryMode)>,
    pub(crate) level_key: Option<String>,
    pub(crate) color_assign: ColorAssignOpts,
    pub(crate) exit_on: Arc<[Regex]>,
    pub(crate) exit_on_level: Option<ExitOnLevel>,
    pub(crate) exit_watch: ExitWatchState,
    pub(crate) needs_json_annotation: bool,
}

pub(crate) fn needs_json_annotation(
    jq: &Option<(CompiledFilter, QueryMode)>,
    level_key: &Option<String>,
) -> bool {
    jq.is_some() || level_key.is_some()
}

#[doc(hidden)]
pub fn apply_pipeline<S>(
    stream: S,
    stages: PipelineStages,
) -> BoxStream<'static, Result<LogEvent, LogSourceError>>
where
    S: Stream<Item = Result<LogEvent, LogSourceError>> + Send + 'static,
{
    let PipelineStages {
        includes,
        excludes,
        filter_on,
        jq,
        level_key,
        color_assign: color_opts,
        exit_on,
        exit_on_level,
        exit_watch,
        needs_json_annotation,
    } = stages;

    let order =
        PipelineStageOrder::resolve(filter_on, !exit_on.is_empty(), exit_on_level.is_some());

    let mut s: BoxStream<'static, Result<LogEvent, LogSourceError>> = Box::pin(stream);

    if order.exit_on_message_before_filters {
        s = Box::pin(exit_watch_message(
            s,
            Arc::clone(&exit_on),
            exit_watch.clone(),
        ));
    }

    if order.include_before_container {
        s = Box::pin(include_exclude(
            s,
            Arc::clone(&includes),
            Arc::clone(&excludes),
        ));
    }

    if !order.exit_on_message_before_filters && !exit_on.is_empty() {
        s = Box::pin(exit_watch_message(s, exit_on, exit_watch.clone()));
    }

    if needs_json_annotation {
        s = Box::pin(json_annotate(s));
    }
    s = Box::pin(level_classify(s, level_key));

    if let Some(min_level) = exit_on_level {
        s = Box::pin(exit_watch_level(s, min_level, exit_watch));
    }

    if order.include_after_classify_before_transform {
        s = Box::pin(include_exclude(
            s,
            Arc::clone(&includes),
            Arc::clone(&excludes),
        ));
    }

    s = if let Some((f, mode)) = jq {
        Box::pin(jq_evaluate(s, f, mode))
    } else {
        s
    };

    if order.include_after_transform {
        s = Box::pin(include_exclude(s, includes, excludes));
    }

    Box::pin(color_assign(s, color_opts))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipeline::ExitOnLevel;
    use crate::runtime::{PipelineSpec, PipelineSpecBuilder};
    use crate::source::{ContextName, Labels, SourceKind, SourceMeta};
    use chrono::Utc;
    use futures::StreamExt;
    use std::sync::Arc;
    use tokio_util::sync::CancellationToken;

    fn ev(msg: &str) -> LogEvent {
        LogEvent {
            source: Arc::new(SourceMeta {
                context: ContextName("ctx".into()),
                namespace: "ns".into(),
                pod: "p".into(),
                container: "c".into(),
                kind: SourceKind::PodLog,
                node: None,
                labels: Arc::new(Labels::default()),
                uid: "u".into(),
            }),
            timestamp: Utc::now(),
            message: Arc::from(msg),
            structured: None,
            level: None,
            palette_index: None,
            container_palette_index: None,
        }
    }

    fn base_spec(token: CancellationToken) -> PipelineSpec {
        PipelineSpecBuilder::new()
            .with_includes(vec![Regex::new("visible").unwrap()])
            .with_exit_on(vec![Regex::new("secret").unwrap()])
            .build(ExitWatchState::new(token))
    }

    #[tokio::test]
    async fn exit_on_fires_before_include_filter() {
        let token = CancellationToken::new();
        let spec = base_spec(token.clone());
        let s = futures::stream::iter(vec![Ok(ev("secret hidden line"))]);
        let out: Vec<_> = spec.apply(s).collect().await;
        assert!(out.is_empty(), "include filter hides the line from output");
        assert!(
            token.is_cancelled(),
            "exit-on still triggers on hidden line"
        );
    }

    #[tokio::test]
    async fn exit_on_level_fires_before_include_filter() {
        let token = CancellationToken::new();
        let raw = r#"{"level":"error","msg":"hidden"}"#;
        let mut event = ev(raw);
        event.structured = Some(serde_json::from_str(raw).unwrap());

        let spec = PipelineSpecBuilder::new()
            .with_includes(vec![Regex::new("visible").unwrap()])
            .with_level_key(Some("level".into()))
            .with_exit_on_level(Some(ExitOnLevel::Warn))
            .build(ExitWatchState::new(token.clone()));
        let s = futures::stream::iter(vec![Ok(event)]);
        let out: Vec<_> = spec.apply(s).collect().await;
        assert!(out.is_empty(), "include filter hides the line from output");
        assert!(
            token.is_cancelled(),
            "exit-on-level still triggers on hidden line"
        );
    }

    #[tokio::test]
    async fn exit_on_level_triggers_after_classify() {
        use crate::source::LogLevel;
        let token = CancellationToken::new();
        let raw = r#"{"level":"error","msg":"boom"}"#;
        let mut event = ev(raw);
        event.structured = Some(serde_json::from_str(raw).unwrap());

        let spec = PipelineSpecBuilder::new()
            .with_level_key(Some("level".into()))
            .with_exit_on_level(Some(ExitOnLevel::Warn))
            .build(ExitWatchState::new(token.clone()));
        let s = futures::stream::iter(vec![Ok(event)]);
        let out: Vec<_> = spec.apply(s).collect().await;
        assert_eq!(out.len(), 1);
        assert!(matches!(
            out[0].as_ref().unwrap().level,
            Some(LogLevel::Error)
        ));
        assert!(token.is_cancelled());
    }

    #[tokio::test]
    async fn original_exit_on_level_include_runs_before_jq() {
        let token = CancellationToken::new();
        let raw = r#"{"level":"error","msg":"visible line"}"#;
        let mut event = ev(raw);
        event.structured = Some(serde_json::from_str(raw).unwrap());

        let spec = PipelineSpecBuilder::new()
            .with_includes(vec![Regex::new("^\\{").unwrap()])
            .with_jq(Some((
                crate::pipeline::validate_filter(".msg").unwrap(),
                QueryMode::Replace,
            )))
            .with_level_key(Some("level".into()))
            .with_exit_on_level(Some(ExitOnLevel::Warn))
            .build(ExitWatchState::new(token.clone()));
        let s = futures::stream::iter(vec![Ok(event)]);
        let out: Vec<_> = spec.apply(s).collect().await;
        assert_eq!(
            out.len(),
            1,
            "include matches original JSON before jq rewrite"
        );
        assert!(token.is_cancelled());
    }

    #[tokio::test]
    async fn transformed_exit_on_fires_on_raw_message_before_jq() {
        let token = CancellationToken::new();
        let raw = r#"{"msg":"secret payload"}"#;
        let mut event = ev(raw);
        event.structured = Some(serde_json::from_str(raw).unwrap());

        let spec = PipelineSpecBuilder::new()
            .with_includes(vec![Regex::new("visible").unwrap()])
            .with_filter_on(FilterOn::Transformed)
            .with_jq(Some((
                crate::pipeline::validate_filter(".msg").unwrap(),
                QueryMode::Replace,
            )))
            .with_exit_on(vec![Regex::new("secret").unwrap()])
            .build(ExitWatchState::new(token.clone()));
        let s = futures::stream::iter(vec![Ok(event)]);
        let out: Vec<_> = spec.apply(s).collect().await;
        assert!(out.is_empty(), "include filter hides transformed line");
        assert!(
            token.is_cancelled(),
            "transformed path runs exit-on on raw message before jq/include"
        );
    }

    #[tokio::test]
    async fn transformed_include_runs_after_jq() {
        let token = CancellationToken::new();
        let raw = r#"{"msg":"visible line"}"#;
        let mut event = ev(raw);
        event.structured = Some(serde_json::from_str(raw).unwrap());

        let spec = PipelineSpecBuilder::new()
            .with_includes(vec![Regex::new("\"visible").unwrap()])
            .with_filter_on(FilterOn::Transformed)
            .with_jq(Some((
                crate::pipeline::validate_filter(".msg").unwrap(),
                QueryMode::Replace,
            )))
            .build(ExitWatchState::new(token));
        let s = futures::stream::iter(vec![Ok(event)]);
        let out: Vec<_> = spec.apply(s).collect().await;
        assert_eq!(out.len(), 1);
        assert!(
            out[0].as_ref().unwrap().message.contains("visible"),
            "include matches jq-replaced message text"
        );
    }

    #[tokio::test]
    async fn skips_json_annotate_without_jq_or_level_key() {
        let raw = r#"{"level":"error","msg":"boom"}"#;
        let spec = PipelineSpecBuilder::new().build(ExitWatchState::new(CancellationToken::new()));
        let s = futures::stream::iter(vec![Ok(ev(raw))]);
        let out: Vec<_> = spec.apply(s).collect().await;
        assert!(
            out[0].as_ref().unwrap().structured.is_none(),
            "json annotate should be skipped when jq and level_key are unset"
        );
    }

    #[tokio::test]
    async fn json_annotate_runs_when_level_key_set() {
        use crate::source::LogLevel;
        let raw = r#"{"level":"error","msg":"boom"}"#;
        let spec = PipelineSpecBuilder::new()
            .with_level_key(Some("level".into()))
            .build(ExitWatchState::new(CancellationToken::new()));
        let s = futures::stream::iter(vec![Ok(ev(raw))]);
        let out: Vec<_> = spec.apply(s).collect().await;
        assert!(out[0].as_ref().unwrap().structured.is_some());
        assert!(matches!(
            out[0].as_ref().unwrap().level,
            Some(LogLevel::Error)
        ));
    }
}
