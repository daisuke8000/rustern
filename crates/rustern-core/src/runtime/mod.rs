//! Runtime: backpressure, mux, pipeline wiring, renderer hookup.
//!
//! | Module | Role |
//! |--------|------|
//! | [`config`] | `CoreRunConfig`, formatter choice, errors/results |
//! | [`forward`] | `LossyMetrics`, `forward_to_render` |
//! | [`attach`] | Pod log attach, concurrent stream start semaphore |
//! | [`spec`] | [`PipelineSpec`] — compiled pipeline for the `run` stream |
//! | [`run`] | `run` — watch, channels, `tokio::spawn` wiring |
//! | [`watch`] | Pod watch loop and reconcile handlers |
//! | [`mux`] | `StreamMap` multiplexing |

mod attach;
mod config;
mod cursor_service;
mod cursor_store;
mod forward;
mod list_pods;
mod mux;
mod mux_forward_core;
mod pod_lifecycle;
mod pod_meta_cache;
mod registry;
mod run;
mod spec;
#[cfg(test)]
mod test_support;
mod watch;
mod watch_admission;
mod watch_ctx;

pub use attach::build_log_request_semaphore;
pub use config::{
    BackpressurePolicy, CoreRunConfig, FormatterChoice, RunError, RunOutcome, RuntimeFwdConfig,
    RuntimeStatsConfig,
};
pub use forward::{LossyMetrics, MuxMetrics, RunStats, forward_to_render};
pub use mux::{MuxCmd, spawn_mux_task};
pub use mux_forward_core::{MuxForwardCore, MuxForwardCoreHandles};
pub use run::{run, run_with_client};
pub use spec::{PipelineSpec, PipelineSpecBuilder};
