//! `run` only: apply pipeline stages to a raw `LogEvent` stream.

use futures::stream::{BoxStream, Stream};
use regex::Regex;

use crate::pipeline::{
    ColorAssignOpts, CompiledFilter, ExitOnLevel, ExitWatchState, FilterOn, QueryMode,
    color_assign, container_filter, exit_watch_level, exit_watch_message, include_exclude,
    jq_evaluate, json_annotate, level_classify,
};
use crate::source::{LogEvent, LogSourceError};

pub(super) fn compile_list(p: &[String]) -> Result<Vec<Regex>, regex::Error> {
    p.iter().map(|s| Regex::new(s)).collect()
}

pub(super) struct PipelineStages {
    pub container_incl: Regex,
    pub container_excl: Vec<Regex>,
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

pub(super) fn apply_pipeline<S>(
    stream: S,
    stages: PipelineStages,
) -> BoxStream<'static, Result<LogEvent, LogSourceError>>
where
    S: Stream<Item = Result<LogEvent, LogSourceError>> + Send + 'static,
{
    let PipelineStages {
        container_incl,
        container_excl,
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

    let has_exit_msg = !exit_on.is_empty();

    let mut s: BoxStream<'static, Result<LogEvent, LogSourceError>> =
        if filter_on == FilterOn::Original {
            if has_exit_msg {
                let s = exit_watch_message(stream, exit_on, exit_watch.clone());
                let s = include_exclude(s, includes.clone(), excludes.clone());
                Box::pin(container_filter(
                    s,
                    container_incl.clone(),
                    container_excl.clone(),
                ))
            } else {
                let s = include_exclude(stream, includes.clone(), excludes.clone());
                Box::pin(container_filter(
                    s,
                    container_incl.clone(),
                    container_excl.clone(),
                ))
            }
        } else {
            let s = container_filter(stream, container_incl, container_excl);
            if has_exit_msg {
                Box::pin(exit_watch_message(s, exit_on, exit_watch.clone()))
            } else {
                Box::pin(s)
            }
        };

    s = Box::pin(json_annotate(s));
    s = Box::pin(level_classify(s, level_key));

    if let Some(min_level) = exit_on_level {
        s = Box::pin(exit_watch_level(s, min_level, exit_watch));
    }

    s = if let Some((f, mode)) = jq {
        Box::pin(jq_evaluate(s, f, mode))
    } else {
        s
    };

    s = if filter_on == FilterOn::Transformed {
        Box::pin(include_exclude(s, includes, excludes))
    } else {
        s
    };

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
            container_incl: Regex::new(".*").unwrap(),
            container_excl: vec![],
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
    async fn exit_on_level_triggers_after_classify() {
        use crate::source::LogLevel;
        use serde_json::value::RawValue;

        let token = CancellationToken::new();
        let raw = r#"{"level":"error","msg":"boom"}"#;
        let mut event = ev(raw);
        event.structured = Some(RawValue::from_string(raw.to_string()).unwrap());

        let stages = PipelineStages {
            container_incl: Regex::new(".*").unwrap(),
            container_excl: vec![],
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
}
