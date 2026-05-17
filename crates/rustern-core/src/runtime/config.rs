//! `CoreRunConfig`, run outcome, and error types.

use tokio_util::sync::CancellationToken;

use crate::discovery::context::ContextSelector;
use crate::discovery::pod_watcher::ContainerDiscoverOpts;
use crate::format_display::{TimestampStyle, TimestampZone};
use crate::pipeline::{FilterOn, QueryMode};

/// Bounded queue and parallelism hints for forwarded log events (`run` runtime).
#[derive(Debug, Clone)]
pub struct RuntimeFwdConfig {
    /// Render channel capacity.
    pub buffer_size: usize,
    /// Skip lines instead of blocking when the render queue is full.
    pub lossy: bool,
    /// Upper bound on concurrent pod log streams.
    pub max_log_requests: usize,
}

/// High-level stdout rendering mode (`run` selects a formatter from this plus [`FormatterChoice`]).
#[derive(Debug, Clone)]
pub enum OutputMode {
    /// Human-readable prefixed lines via [`FormatterChoice::Default`].
    Default,
    /// Raw message-only lines (no prefixes).
    Raw,
    /// One JSON object per log line (`message` rewritten when jq is enabled).
    Json,
}

/// Line formatter preset matching [`OutputMode`] (timing and color knobs apply to the default formatter only).
#[derive(Debug, Clone)]
pub enum FormatterChoice {
    /// Prefix / color / timestamps for terminal-friendly output.
    Default {
        timestamp_style: TimestampStyle,
        timestamp_zone: TimestampZone,
        color_enabled: bool,
    },
    /// NDJSON emitter.
    Json,
    /// Transparent passthrough (`message` only).
    Raw,
}

/// Fully wired configuration for [`crate::run`].
#[derive(Debug, Clone)]
pub struct CoreRunConfig {
    /// Cluster access (kube context + kubeconfig hints).
    pub context: ContextSelector,
    /// User query (`pod/name` regex, `deploy/name`, `.` sentinel with selectors, …).
    pub query: String,
    /// Namespaces to watch. Empty iff `all_namespaces` (ignored for API scope).
    pub namespaces: Vec<String>,
    /// Watch pods in every namespace (API-global list/watch).
    pub all_namespaces: bool,
    /// Optional pod label selector (server-side filtering).
    pub selector: Option<String>,
    /// Optional pod field selector fragment (combined with [`Self::node`] if set).
    pub field_selector: Option<String>,
    /// Optional node constraint (`spec.nodeName` merged into field selector).
    pub node: Option<String>,
    /// Pod name regex patterns; exclude pods matching any.
    pub exclude_pod: Vec<String>,
    /// Container name regex to include.
    pub container: String,
    /// Container-name regex exclusions (any match hides the container stream).
    pub exclude_container: Vec<String>,
    /// Which pod containers are surfaced as log sources (init / ephemeral / lifecycle).
    pub container_discovery: ContainerDiscoverOpts,
    /// Stream logs (`kubectl logs -f`).
    pub follow: bool,
    /// Tail line window per stream (non-negative seconds / lines).
    pub tail: Option<i64>,
    /// Only logs newer than this age (already resolved seconds in core).
    pub since: Option<i64>,
    /// Line-level include regex filters.
    pub include: Vec<String>,
    /// Line-level exclude regex filters.
    pub exclude: Vec<String>,
    /// Substrings emphasized (bold/red) after default formatter output; merges with [`Self::include`] like stern `-H`.
    pub highlight: Vec<String>,
    /// Omit stream lifecycle chatter on stderr (stern +/- lines); rustern currently emits none.
    pub only_log_lines: bool,
    /// Regex stage selector (plain vs jq-transformed payload).
    pub filter_on: FilterOn,
    /// Optional jaq-like transform filter.
    pub json_query: Option<String>,
    /// Rewrite mode when `json_query` is set.
    pub json_query_mode: QueryMode,
    /// Structured field path used for inferred log level tagging.
    pub level_key: Option<String>,
    /// Output family (default/raw/json skeleton).
    pub output: OutputMode,
    /// Concrete formatter knobs.
    pub formatter: FormatterChoice,
    /// Forwarding knobs (buffer sizing, concurrency).
    pub fwd: RuntimeFwdConfig,
    /// Cooperative shutdown for all spawned tasks.
    pub root_token: CancellationToken,
}

/// Recoverable failures while resolving context, compiling filters, or during `run`.
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
    #[error(transparent)]
    Kube(#[from] kube::Error),
    #[error("{0}")]
    Other(String),
}

/// High-level outcome after [`crate::run`] returns (streaming ended cooperatively).
pub struct RunOutcome {
    /// Currently always `false` (reserved for future error aggregation).
    pub had_source_errors: bool,
}
