//! Core library: LogSource, discovery, pipeline, renderer, runtime.
#![forbid(unsafe_code)]

pub mod discovery;
pub mod format_display;
pub mod pipeline;
pub mod regex_limits;
pub mod render;
pub mod runtime;
pub mod source;

pub use discovery::context::ContextSelector;
pub use discovery::run_resolution::{
    RunResolutionError, RunResolutionInput, RunResolutionOutput, RunResolutionValidation,
    resolve_run_input, resolve_run_namespaces, resolve_run_query,
};
pub use format_display::{TimestampStyle, TimestampZone};
pub use pipeline::{CompiledFilter, FilterOn, JqError, QueryMode, validate_filter};
pub use regex_limits::{MAX_USER_REGEX_PATTERN_LEN, compile_user_regex};
pub use runtime::{
    BackpressurePolicy, CoreRunConfig, FormatterChoice, LossyMetrics, MuxCmd, MuxForwardCore,
    MuxForwardCoreHandles, MuxMetrics, PipelineSpec, PipelineSpecBuilder, RunError, RunOutcome,
    RunStats, RuntimeFwdConfig, RuntimeStatsConfig, build_log_request_semaphore, forward_to_render,
    run, run_with_client, spawn_mux_task,
};
#[cfg(feature = "bench")]
pub use source::ScriptLogSourceOpener;
pub use source::pod_log::PodLogRequest;
#[cfg(feature = "bench")]
pub use source::pod_log::{LogLineTimestampResolver, split_log_line};
