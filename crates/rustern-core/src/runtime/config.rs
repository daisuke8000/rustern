//! `CoreRunConfig` と実行結果・エラー型。

use tokio_util::sync::CancellationToken;

use crate::discovery::context::ContextSelector;
use crate::pipeline::{FilterOn, QueryMode};

#[derive(Debug, Clone)]
pub struct RuntimeFwdConfig {
    pub buffer_size: usize,
    pub lossy: bool,
    pub max_log_requests: usize,
}

#[derive(Debug, Clone)]
pub enum OutputMode {
    Default,
    Raw,
    Json,
}

#[derive(Debug, Clone)]
pub enum FormatterChoice {
    Default {
        show_timestamps: bool,
        color_enabled: bool,
    },
    Json,
    Raw,
}

#[derive(Debug, Clone)]
pub struct CoreRunConfig {
    pub context: ContextSelector,
    pub query: String,
    pub namespace: Option<String>,
    pub all_namespaces: bool,
    pub selector: Option<String>,
    pub container: String,
    pub exclude_container: Option<String>,
    pub follow: bool,
    pub tail: Option<i64>,
    pub since: Option<i64>,
    pub include: Vec<String>,
    pub exclude: Vec<String>,
    pub filter_on: FilterOn,
    pub json_query: Option<String>,
    pub json_query_mode: QueryMode,
    pub level_key: Option<String>,
    pub output: OutputMode,
    pub formatter: FormatterChoice,
    pub fwd: RuntimeFwdConfig,
    pub root_token: CancellationToken,
}

#[derive(Debug, thiserror::Error)]
pub enum RunError {
    #[error(transparent)]
    Context(#[from] crate::discovery::context::ContextError),
    #[error(transparent)]
    Query(#[from] crate::discovery::resource::QueryParseError),
    #[error("invalid container regex: {0}")]
    ContainerRegex(#[from] regex::Error),
    #[error(transparent)]
    Jq(#[from] crate::pipeline::JqError),
    #[error("{0}")]
    Other(String),
}

pub struct RunOutcome {
    pub had_source_errors: bool,
}
