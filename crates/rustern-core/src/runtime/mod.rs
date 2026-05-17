//! Runtime: backpressure, mux, pipeline wiring, renderer hookup.
//!
//! | Module | Role |
//! |--------|------|
//! | [`config`] | `CoreRunConfig`, formatter choice, errors/results |
//! | [`forward`] | `LossyMetrics`, `forward_to_render`, log concurrency semaphore |
//! | [`pipeline`] | Pipeline stages on the `run` stream (`apply_pipeline`) |
//! | [`runner`] | `run` — watch, channels, `tokio::spawn` wiring |

mod config;
mod forward;
mod pipeline;
mod runner;

pub use config::{
    CoreRunConfig, FormatterChoice, OutputMode, RunError, RunOutcome, RuntimeFwdConfig,
};
pub use forward::{LossyMetrics, build_log_request_semaphore, forward_to_render};
pub use runner::run;
