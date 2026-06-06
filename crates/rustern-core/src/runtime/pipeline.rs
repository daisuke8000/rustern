//! `run` only: apply pipeline stages to a raw `LogEvent` stream.

use futures::stream::{BoxStream, Stream};
use regex::Regex;

use crate::pipeline::{
    ColorAssignOpts, CompiledFilter, ExitOnLevel, ExitWatchState, FilterOn, PipelineStageOrder,
    QueryMode, color_assign, exit_watch_level, exit_watch_message, include_exclude, jq_evaluate,
    json_annotate, level_classify,
};
use crate::source::{LogEvent, LogSourceError};

pub(super) fn compile_list(p: &[String]) -> Result<Vec<Regex>, regex::Error> {
    p.iter().map(|s| Regex::new(s)).collect()
}

#[doc(hidden)]
pub struct PipelineStages {
    pub includes: Vec<Regex>,
    pub excludes: Vec<Regex>,
    pub filter_on: FilterOn,
    pub jq: Option<(CompiledFilter, QueryMode)>,
    pub level_key: Option<String>,
    pub color_assign: ColorAssignOpts,
    pub exit_on: Vec<Regex>,
    pub exit_on_level: Option<ExitOnLevel>,
    pub exit_watch: ExitWatchState,
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
    } = stages;

    let order =
        PipelineStageOrder::resolve(filter_on, !exit_on.is_empty(), exit_on_level.is_some());

    let mut s: BoxStream<'static, Result<LogEvent, LogSourceError>> = Box::pin(stream);

    if order.exit_on_message_before_filters {
        s = Box::pin(exit_watch_message(s, exit_on.clone(), exit_watch.clone()));
    }

    if order.include_before_container {
        s = Box::pin(include_exclude(s, includes.clone(), excludes.clone()));
    }

    if !order.exit_on_message_before_filters && !exit_on.is_empty() {
        s = Box::pin(exit_watch_message(s, exit_on, exit_watch.clone()));
    }

    s = Box::pin(json_annotate(s));
    s = Box::pin(level_classify(s, level_key));

    if let Some(min_level) = exit_on_level {
        s = Box::pin(exit_watch_level(s, min_level, exit_watch));
    }

    if order.include_after_classify_before_transform {
        s = Box::pin(include_exclude(s, includes.clone(), excludes.clone()));
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
    use crate::pipeline::color_assign::ColorAssignOpts;
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

    fn base_stages(token: CancellationToken) -> PipelineStages {
        PipelineStages {
            includes: vec![Regex::new("visible").unwrap()],
            excludes: vec![],
            filter_on: FilterOn::Original,
            jq: None,
            level_key: None,
            color_assign: ColorAssignOpts {
                pod_colors: false,
                container_colors: false,
                diff_container: false,
            },
            exit_on: vec![Regex::new("secret").unwrap()],
            exit_on_level: None,
            exit_watch: ExitWatchState::new(token),
        }
    }

    #[tokio::test]
    async fn exit_on_fires_before_include_filter() {
        let token = CancellationToken::new();
        let stages = base_stages(token.clone());
        let s = futures::stream::iter(vec![Ok(ev("secret hidden line"))]);
        let out: Vec<_> = apply_pipeline(s, stages).collect().await;
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

        let stages = PipelineStages {
            includes: vec![Regex::new("visible").unwrap()],
            excludes: vec![],
            filter_on: FilterOn::Original,
            jq: None,
            level_key: Some("level".into()),
            color_assign: ColorAssignOpts {
                pod_colors: false,
                container_colors: false,
                diff_container: false,
            },
            exit_on: vec![],
            exit_on_level: Some(ExitOnLevel::Warn),
            exit_watch: ExitWatchState::new(token.clone()),
        };
        let s = futures::stream::iter(vec![Ok(event)]);
        let out: Vec<_> = apply_pipeline(s, stages).collect().await;
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

        let stages = PipelineStages {
            includes: vec![],
            excludes: vec![],
            filter_on: FilterOn::Original,
            jq: None,
            level_key: Some("level".into()),
            color_assign: ColorAssignOpts {
                pod_colors: false,
                container_colors: false,
                diff_container: false,
            },
            exit_on: vec![],
            exit_on_level: Some(ExitOnLevel::Warn),
            exit_watch: ExitWatchState::new(token.clone()),
        };
        let s = futures::stream::iter(vec![Ok(event)]);
        let out: Vec<_> = apply_pipeline(s, stages).collect().await;
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

        let stages = PipelineStages {
            includes: vec![Regex::new("^\\{").unwrap()],
            excludes: vec![],
            filter_on: FilterOn::Original,
            jq: Some((
                crate::pipeline::validate_filter(".msg").unwrap(),
                QueryMode::Replace,
            )),
            level_key: Some("level".into()),
            color_assign: ColorAssignOpts {
                pod_colors: false,
                container_colors: false,
                diff_container: false,
            },
            exit_on: vec![],
            exit_on_level: Some(ExitOnLevel::Warn),
            exit_watch: ExitWatchState::new(token.clone()),
        };
        let s = futures::stream::iter(vec![Ok(event)]);
        let out: Vec<_> = apply_pipeline(s, stages).collect().await;
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

        let stages = PipelineStages {
            includes: vec![Regex::new("visible").unwrap()],
            excludes: vec![],
            filter_on: FilterOn::Transformed,
            jq: Some((
                crate::pipeline::validate_filter(".msg").unwrap(),
                QueryMode::Replace,
            )),
            level_key: None,
            color_assign: ColorAssignOpts {
                pod_colors: false,
                container_colors: false,
                diff_container: false,
            },
            exit_on: vec![Regex::new("secret").unwrap()],
            exit_on_level: None,
            exit_watch: ExitWatchState::new(token.clone()),
        };
        let s = futures::stream::iter(vec![Ok(event)]);
        let out: Vec<_> = apply_pipeline(s, stages).collect().await;
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

        let stages = PipelineStages {
            includes: vec![Regex::new("\"visible").unwrap()],
            excludes: vec![],
            filter_on: FilterOn::Transformed,
            jq: Some((
                crate::pipeline::validate_filter(".msg").unwrap(),
                QueryMode::Replace,
            )),
            level_key: None,
            color_assign: ColorAssignOpts {
                pod_colors: false,
                container_colors: false,
                diff_container: false,
            },
            exit_on: vec![],
            exit_on_level: None,
            exit_watch: ExitWatchState::new(token),
        };
        let s = futures::stream::iter(vec![Ok(event)]);
        let out: Vec<_> = apply_pipeline(s, stages).collect().await;
        assert_eq!(out.len(), 1);
        assert!(
            out[0].as_ref().unwrap().message.contains("visible"),
            "include matches jq-replaced message text"
        );
    }
}
