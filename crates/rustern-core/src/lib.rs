//! Core library: LogSource, discovery, pipeline, renderer, runtime.
#![forbid(unsafe_code)]

pub mod discovery;
pub mod format_display;
pub mod pipeline;
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
pub use runtime::{
    CoreRunConfig, FormatterChoice, LossyMetrics, MuxCmd, OutputMode, PipelineSpec,
    PipelineSpecBuilder, RunError, RunOutcome, RunStats, RuntimeFwdConfig, RuntimeStatsConfig,
    build_log_request_semaphore, forward_to_render, run, spawn_mux_task,
};
#[doc(hidden)]
pub use runtime::{PipelineStages, apply_pipeline};
#[cfg(feature = "bench")]
pub use source::ScriptLogSourceOpener;
pub use source::pod_log::{PodLogRequest, parse_log_line};
