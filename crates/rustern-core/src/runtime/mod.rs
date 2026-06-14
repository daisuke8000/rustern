//! Runtime: backpressure, mux, pipeline wiring, renderer hookup.
//!
//! | Module | Role |
//! |--------|------|
//! | [`config`] | `CoreRunConfig`, formatter choice, errors/results |
//! | [`forward`] | `LossyMetrics`, `forward_to_render`, log concurrency semaphore |
//! | [`spec`] | [`PipelineSpec`] — compiled pipeline for the `run` stream |
//! | [`pipeline`] | Internal stage wiring (`apply_pipeline`; migration only) |
//! | [`run`] | `run` — watch, channels, `tokio::spawn` wiring |
//! | [`watch`] | Pod watch loop and reconcile handlers |
//! | [`attach`] | Pod log stream attach |
//! | [`mux`] | `StreamMap` multiplexing |

mod attach;
mod config;
mod cursor_store;
mod forward;
mod mux;
mod pipeline;
mod pod_meta_cache;
mod registry;
mod run;
mod spec;
#[cfg(test)]
mod test_support;
mod watch;
mod watch_admission;
mod watch_ctx;

pub use config::{
    BackpressurePolicy, CoreRunConfig, FormatterChoice, OutputMode, RunError, RunOutcome,
    RuntimeFwdConfig, RuntimeStatsConfig,
};
pub use forward::{
    LossyMetrics, MuxMetrics, RunStats, build_log_request_semaphore, forward_to_render,
};
pub use mux::{MuxCmd, spawn_mux_task};
// Migration-only exports; use `PipelineSpec` instead (scheduled for removal).
#[doc(hidden)]
pub use pipeline::{PipelineStages, apply_pipeline};
pub use run::run;
pub use spec::{PipelineSpec, PipelineSpecBuilder};
