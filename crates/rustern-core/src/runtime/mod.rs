//! Runtime: backpressure, mux, pipeline wiring, renderer hookup.
//!
//! | Module | Role |
//! |--------|------|
//! | [`config`] | `CoreRunConfig`, formatter choice, errors/results |
//! | [`forward`] | `LossyMetrics`, `forward_to_render`, log concurrency semaphore |
//! | [`pipeline`] | Pipeline stages on the `run` stream (`apply_pipeline`) |
//! | [`run`] | `run` — watch, channels, `tokio::spawn` wiring |
//! | [`watch`] | Pod watch loop and reconcile handlers |
//! | [`attach`] | Pod log stream attach |
//! | [`mux`] | `StreamMap` multiplexing |

mod attach;
mod config;
mod forward;
mod mux;
mod pipeline;
mod run;
mod watch;

pub use config::{
    CoreRunConfig, FormatterChoice, OutputMode, RunError, RunOutcome, RuntimeFwdConfig,
};
pub use forward::{LossyMetrics, build_log_request_semaphore, forward_to_render};
pub use mux::{MuxCmd, spawn_mux_task};
pub use pipeline::{PipelineStages, apply_pipeline};
pub use run::run;
