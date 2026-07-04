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

#[derive(Clone)]
pub(crate) struct PipelineStages {
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

fn needs_include_exclude(includes: &Arc<[Regex]>, excludes: &Arc<[Regex]>) -> bool {
    !includes.is_empty() || !excludes.is_empty()
}

fn needs_color_assign(opts: ColorAssignOpts) -> bool {
    opts.pod_colors || opts.container_colors
}

pub(crate) fn apply_pipeline<S>(
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

    if order.include_before_container && needs_include_exclude(&includes, &excludes) {
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
        let use_jaq_val = jq.is_some();
        s = Box::pin(json_annotate(s, use_jaq_val));
    }
    if let Some(key) = level_key {
        s = Box::pin(level_classify(s, Some(key)));
    }

    if let Some(min_level) = exit_on_level {
        s = Box::pin(exit_watch_level(s, min_level, exit_watch));
    }

    if order.include_after_classify_before_transform && needs_include_exclude(&includes, &excludes)
    {
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

    if order.include_after_transform && needs_include_exclude(&includes, &excludes) {
        s = Box::pin(include_exclude(s, includes, excludes));
    }

    if needs_color_assign(color_opts) {
        s = Box::pin(color_assign(s, color_opts));
    }

    s
}
