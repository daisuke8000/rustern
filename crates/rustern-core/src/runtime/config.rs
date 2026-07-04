//! `CoreRunConfig`, run outcome, and error types.

use std::time::Duration;

use tokio_util::sync::CancellationToken;

use crate::discovery::ContainerDiscoverOpts;
use crate::discovery::context::ContextSelector;
use crate::discovery::pod_condition::PodConditionFilter;
use crate::format_display::{TimestampStyle, TimestampZone};
use crate::pipeline::{FilterOn, QueryMode};
use crate::source::pod_log::PodLogRequest;

/// Backpressure policy for a bounded channel tier (mux raw queue or forward render queue).
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum BackpressurePolicy {
    /// Block the producer until capacity is available.
    Blocking,
    /// Drop events when the channel is full.
    Lossy,
}

impl BackpressurePolicy {
    /// Map the legacy forward `--lossy` flag to a mux-tier policy (same knob until split).
    pub fn from_lossy(lossy: bool) -> Self {
        if lossy { Self::Lossy } else { Self::Blocking }
    }
}

/// Bounded queue and parallelism hints for forwarded log events (`run` runtime).
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct RuntimeStatsConfig {
    /// Emit one stderr report per interval.
    pub interval: Duration,
}

/// Bounded queue and parallelism hints for forwarded log events (`run` runtime).
#[derive(Debug, Clone)]
pub struct RuntimeFwdConfig {
    /// Render channel capacity.
    pub buffer_size: usize,
    /// Skip lines instead of blocking when the forward → render queue is full (tier 2).
    pub lossy: bool,
    /// Skip lines instead of blocking when the mux → raw pipeline queue is full (tier 1).
    pub mux_policy: BackpressurePolicy,
    /// Optional stderr stats reporter.
    pub stats: Option<RuntimeStatsConfig>,
    /// Upper bound on concurrent pod log streams.
    pub max_log_requests: usize,
}

impl RuntimeFwdConfig {
    pub(crate) fn render_channel_capacity(&self) -> usize {
        self.buffer_size.max(1)
    }

    /// Mux-tier backpressure policy (`spawn_mux_task`).
    pub fn resolved_mux_policy(&self) -> BackpressurePolicy {
        self.mux_policy
    }
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
    /// Stern-compatible extended JSON (plain metadata fields).
    ExtJson,
    /// Pretty-printed [`OutputMode::ExtJson`].
    PpExtJson,
}

/// Line formatter preset matching [`OutputMode`] (timing and color knobs apply to the default formatter only).
#[derive(Debug, Clone)]
pub enum FormatterChoice {
    /// Prefix / color / timestamps for terminal-friendly output.
    Default {
        timestamp_style: TimestampStyle,
        timestamp_zone: TimestampZone,
        color_enabled: bool,
        pod_colors: bool,
        container_colors: bool,
    },
    /// NDJSON emitter.
    Json,
    /// Stern-compatible extended JSON.
    ExtJson { all_namespaces: bool },
    /// Pretty extended JSON.
    PpExtJson { all_namespaces: bool },
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
    /// Optional pod status condition filter (`stern --condition`; requires `--no-follow` or `--tail=0`).
    pub pod_condition: Option<PodConditionFilter>,
    /// Kubernetes log subresource knobs (`kubectl logs` flags).
    pub pod_log: PodLogRequest,
    /// Re-open follow streams from the last seen event timestamp after disconnect.
    pub cursor_reconnect: bool,
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
    /// CI/smoke: exit when raw message matches any regex (before `-i`/`-e`).
    pub exit_on: Vec<String>,
    /// CI/smoke: exit when classified log level is at or above this threshold.
    pub exit_on_level: Option<crate::pipeline::ExitOnLevel>,
    /// Output family (default/raw/json skeleton).
    pub output: OutputMode,
    /// Concrete formatter knobs.
    pub formatter: FormatterChoice,
    /// Per-container palette when true (stern `--diff-container` / `-d`).
    pub diff_container: bool,
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
    #[error(transparent)]
    PodList(#[from] crate::discovery::pod_list::PodListError),
    #[error("invalid container regex: {0}")]
    ContainerRegex(#[from] regex::Error),
    #[error(transparent)]
    Jq(#[from] crate::pipeline::JqError),
    #[error(transparent)]
    Kube(#[from] kube::Error),
    #[error("{0}")]
    Other(String),
    #[error("--exit-on / --exit-on-level condition matched")]
    ExitOnTriggered,
}

/// High-level outcome after [`crate::run`] returns (streaming ended cooperatively).
pub struct RunOutcome {
    /// `true` when one or more multiplexed sources reported `LogSourceError::Api`.
    pub had_source_errors: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn render_channel_capacity_matches_buffer_size() {
        let fwd = RuntimeFwdConfig {
            buffer_size: 8192,
            lossy: false,
            mux_policy: BackpressurePolicy::Blocking,
            stats: None,
            max_log_requests: 10,
        };
        assert_eq!(fwd.render_channel_capacity(), 8192);
    }

    #[test]
    fn render_channel_capacity_clamps_zero_to_one() {
        let fwd = RuntimeFwdConfig {
            buffer_size: 0,
            lossy: false,
            mux_policy: BackpressurePolicy::Blocking,
            stats: None,
            max_log_requests: 10,
        };
        assert_eq!(fwd.render_channel_capacity(), 1);
    }

    #[test]
    fn backpressure_policy_from_lossy_maps_both_branches() {
        assert_eq!(
            BackpressurePolicy::from_lossy(false),
            BackpressurePolicy::Blocking
        );
        assert_eq!(
            BackpressurePolicy::from_lossy(true),
            BackpressurePolicy::Lossy
        );
    }

    #[test]
    fn resolved_mux_policy_returns_configured_policy() {
        let blocking = RuntimeFwdConfig {
            buffer_size: 1,
            lossy: true,
            mux_policy: BackpressurePolicy::Blocking,
            stats: None,
            max_log_requests: 1,
        };
        let lossy = RuntimeFwdConfig {
            mux_policy: BackpressurePolicy::Lossy,
            ..blocking.clone()
        };
        assert_eq!(blocking.resolved_mux_policy(), BackpressurePolicy::Blocking);
        assert_eq!(lossy.resolved_mux_policy(), BackpressurePolicy::Lossy);
    }
}
