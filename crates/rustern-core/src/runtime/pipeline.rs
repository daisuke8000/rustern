//! `run` 専用: 生ストリームにパイプライン段をラップする。

use futures::stream::{BoxStream, Stream};
use regex::Regex;

use crate::pipeline::{
    CompiledFilter, FilterOn, QueryMode, color_assign, container_filter, include_exclude,
    jq_evaluate, json_annotate, level_classify,
};
use crate::source::{LogEvent, LogSourceError};

pub(super) fn compile_list(p: &[String]) -> Result<Vec<Regex>, regex::Error> {
    p.iter().map(|s| Regex::new(s)).collect()
}

pub(super) struct PipelineStages {
    pub container_incl: Regex,
    pub container_excl: Option<Regex>,
    pub includes: Vec<Regex>,
    pub excludes: Vec<Regex>,
    pub filter_on: FilterOn,
    pub jq: Option<(CompiledFilter, QueryMode)>,
    pub level_key: Option<String>,
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
    } = stages;

    let s: BoxStream<'static, Result<LogEvent, LogSourceError>> = if filter_on == FilterOn::Original
    {
        let s = include_exclude(stream, includes.clone(), excludes.clone());
        Box::pin(container_filter(
            s,
            container_incl.clone(),
            container_excl.clone(),
        ))
    } else {
        Box::pin(container_filter(stream, container_incl, container_excl))
    };

    let s = json_annotate(s);
    let s = level_classify(s, level_key);
    let s: BoxStream<'static, Result<LogEvent, LogSourceError>> = if let Some((f, mode)) = jq {
        Box::pin(jq_evaluate(s, f, mode))
    } else {
        Box::pin(s)
    };

    let s: BoxStream<'static, Result<LogEvent, LogSourceError>> =
        if filter_on == FilterOn::Transformed {
            Box::pin(include_exclude(s, includes, excludes))
        } else {
            s
        };

    Box::pin(color_assign(s))
}
