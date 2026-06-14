//! Pipeline specification: compile run config into a log-event stream transformer.
//!
//! Prefer [`PipelineSpec`] over the hidden [`super::pipeline::PipelineStages`] /
//! [`super::pipeline::apply_pipeline`] pair, which remain for migration only.
//!
//! ## Run path
//!
//! ```ignore
//! let exit_watch = ExitWatchState::new(root_token.clone());
//! let spec = PipelineSpec::from_run_config(&cfg, exit_watch)?;
//! let pipeline_check = spec.clone();
//! let stream = spec.apply(ReceiverStream::new(raw_rx));
//! // after cooperative shutdown:
//! if pipeline_check.triggered() { return Err(RunError::ExitOnTriggered); }
//! ```
//!
//! ## Tests and benches
//!
//! Use [`PipelineSpecBuilder`] for minimal defaults (`default_pipeline_stages` equivalent).
//!
//! Stage ordering stays inside [`super::pipeline::apply_pipeline`]; `PipelineStageOrder`
//! remains crate-private. Future stern-plus filters should extend ordering there, then
//! surface new knobs on this spec type.

use futures::stream::{BoxStream, Stream};
use regex::Regex;
use std::sync::Arc;

use super::config::{CoreRunConfig, RunError};
use super::pipeline::{PipelineStages, apply_pipeline, needs_json_annotation};
use crate::pipeline::{
    ColorAssignOpts, CompiledFilter, ExitOnLevel, ExitWatchState, FilterOn, QueryMode,
    validate_filter,
};
use crate::runtime::FormatterChoice;
use crate::source::{LogEvent, LogSourceError};

fn compile_list(patterns: &[String]) -> Result<Arc<[Regex]>, regex::Error> {
    patterns
        .iter()
        .map(|s| Regex::new(s))
        .collect::<Result<Vec<_>, _>>()
        .map(|vec| vec.into())
}

fn color_assign_opts(formatter: &FormatterChoice, diff_container: bool) -> ColorAssignOpts {
    let FormatterChoice::Default {
        pod_colors,
        container_colors,
        ..
    } = formatter
    else {
        return ColorAssignOpts {
            pod_colors: false,
            container_colors: false,
            diff_container: false,
        };
    };
    ColorAssignOpts {
        pod_colors: *pod_colors,
        container_colors: *container_colors,
        diff_container,
    }
}

/// Compiled pipeline applied to the raw multiplexed log stream in [`super::run`].
#[derive(Clone)]
pub struct PipelineSpec {
    stages: PipelineStages,
}

impl PipelineSpec {
    /// Build from a fully wired [`CoreRunConfig`] and shared exit-watch state.
    pub fn from_run_config(
        cfg: &CoreRunConfig,
        exit_watch: ExitWatchState,
    ) -> Result<Self, RunError> {
        let includes = compile_list(&cfg.include)
            .map_err(|e| RunError::Other(format!("invalid --include regex: {e}")))?;
        let excludes = compile_list(&cfg.exclude)
            .map_err(|e| RunError::Other(format!("invalid --exclude regex: {e}")))?;
        let jq = match &cfg.json_query {
            Some(expr) => Some((validate_filter(expr)?, cfg.json_query_mode)),
            None => None,
        };
        let exit_on = compile_list(&cfg.exit_on)
            .map_err(|e| RunError::Other(format!("invalid --exit-on regex: {e}")))?;

        Ok(Self {
            stages: PipelineStages {
                includes,
                excludes,
                filter_on: cfg.filter_on,
                jq: jq.clone(),
                level_key: cfg.level_key.clone(),
                color_assign: color_assign_opts(&cfg.formatter, cfg.diff_container),
                exit_on,
                exit_on_level: cfg.exit_on_level,
                exit_watch,
                needs_json_annotation: needs_json_annotation(&jq, &cfg.level_key),
            },
        })
    }

    /// Apply all pipeline stages to `stream` (include/exclude, jq, color assign, exit hooks).
    pub fn apply<S>(self, stream: S) -> BoxStream<'static, Result<LogEvent, LogSourceError>>
    where
        S: Stream<Item = Result<LogEvent, LogSourceError>> + Send + 'static,
    {
        apply_pipeline(stream, self.stages)
    }

    /// Whether `--exit-on` or `--exit-on-level` fired during the last apply pass.
    ///
    /// `ExitWatchState` is shared (via `Clone`) with the stages consumed by `apply`;
    /// call `spec.clone().apply(...)` so this spec can still read the shared flag afterward.
    pub fn triggered(&self) -> bool {
        self.stages.exit_watch.triggered()
    }
}

/// Fluent builder for tests, benches, and ad-hoc pipeline wiring.
#[derive(Clone)]
pub struct PipelineSpecBuilder {
    includes: Arc<[Regex]>,
    excludes: Arc<[Regex]>,
    filter_on: FilterOn,
    jq: Option<(CompiledFilter, QueryMode)>,
    level_key: Option<String>,
    color_assign: ColorAssignOpts,
    exit_on: Arc<[Regex]>,
    exit_on_level: Option<ExitOnLevel>,
}

impl Default for PipelineSpecBuilder {
    fn default() -> Self {
        Self {
            includes: Vec::new().into(),
            excludes: Vec::new().into(),
            filter_on: FilterOn::Original,
            jq: None,
            level_key: None,
            color_assign: ColorAssignOpts {
                pod_colors: false,
                container_colors: false,
                diff_container: false,
            },
            exit_on: Vec::new().into(),
            exit_on_level: None,
        }
    }
}

impl PipelineSpecBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_includes(mut self, patterns: Vec<Regex>) -> Self {
        self.includes = patterns.into();
        self
    }

    pub fn with_excludes(mut self, patterns: Vec<Regex>) -> Self {
        self.excludes = patterns.into();
        self
    }

    pub fn with_filter_on(mut self, filter_on: FilterOn) -> Self {
        self.filter_on = filter_on;
        self
    }

    pub fn with_jq(mut self, jq: Option<(CompiledFilter, QueryMode)>) -> Self {
        self.jq = jq;
        self
    }

    pub fn with_level_key(mut self, level_key: Option<String>) -> Self {
        self.level_key = level_key;
        self
    }

    pub fn with_color_assign(mut self, color_assign: ColorAssignOpts) -> Self {
        self.color_assign = color_assign;
        self
    }

    pub fn with_exit_on(mut self, patterns: Vec<Regex>) -> Self {
        self.exit_on = patterns.into();
        self
    }

    pub fn with_exit_on_level(mut self, level: Option<ExitOnLevel>) -> Self {
        self.exit_on_level = level;
        self
    }

    pub fn build(self, exit_watch: ExitWatchState) -> PipelineSpec {
        PipelineSpec {
            stages: PipelineStages {
                includes: self.includes,
                excludes: self.excludes,
                filter_on: self.filter_on,
                jq: self.jq.clone(),
                level_key: self.level_key.clone(),
                color_assign: self.color_assign,
                exit_on: self.exit_on,
                exit_on_level: self.exit_on_level,
                exit_watch,
                needs_json_annotation: needs_json_annotation(&self.jq, &self.level_key),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipeline::ExitOnLevel;
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

    fn spec_with_exit_on(token: CancellationToken, exit_on: &str) -> PipelineSpec {
        PipelineSpecBuilder::new()
            .with_includes(vec![Regex::new("visible").unwrap()])
            .with_exit_on(vec![Regex::new(exit_on).unwrap()])
            .build(ExitWatchState::new(token))
    }

    #[tokio::test]
    async fn triggered_reflects_exit_on_via_apply() {
        let token = CancellationToken::new();
        let spec = spec_with_exit_on(token.clone(), "secret");
        let s = futures::stream::iter(vec![Ok(ev("secret hidden line"))]);
        let out: Vec<_> = spec.clone().apply(s).collect().await;
        assert!(out.is_empty());
        assert!(spec.triggered());
        assert!(token.is_cancelled());
    }

    #[tokio::test]
    async fn triggered_false_when_exit_on_does_not_match() {
        let token = CancellationToken::new();
        let spec = spec_with_exit_on(token.clone(), "secret");
        let s = futures::stream::iter(vec![Ok(ev("visible line"))]);
        let out: Vec<_> = spec.clone().apply(s).collect().await;
        assert_eq!(out.len(), 1);
        assert!(!spec.triggered());
    }

    #[tokio::test]
    async fn exit_on_level_triggers_via_builder_and_apply() {
        use crate::source::LogLevel;
        let token = CancellationToken::new();
        let raw = r#"{"level":"error","msg":"boom"}"#;
        let mut event = ev(raw);
        event.structured = Some(serde_json::from_str(raw).unwrap());

        let spec = PipelineSpecBuilder::new()
            .with_level_key(Some("level".into()))
            .with_exit_on_level(Some(ExitOnLevel::Warn))
            .build(ExitWatchState::new(token.clone()));

        let out: Vec<_> = spec
            .clone()
            .apply(futures::stream::iter(vec![Ok(event)]))
            .collect()
            .await;
        assert_eq!(out.len(), 1);
        assert!(matches!(
            out[0].as_ref().unwrap().level,
            Some(LogLevel::Error)
        ));
        assert!(spec.triggered());
    }
}
