//! Core library: LogSource, discovery, pipeline, renderer, runtime.
#![forbid(unsafe_code)]

pub mod discovery;
pub mod pipeline;
pub mod render;
pub mod runtime;
pub mod source;

pub use discovery::context::ContextSelector;
pub use pipeline::{CompiledFilter, FilterOn, JqError, QueryMode, validate_filter};
pub use runtime::{
    CoreRunConfig, FormatterChoice, LossyMetrics, OutputMode, RunError, RunOutcome,
    RuntimeFwdConfig, build_log_request_semaphore, forward_to_render, run,
};
