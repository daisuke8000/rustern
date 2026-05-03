//! Runtime: backpressure、mux、パイプライン、レンダラ接続。
//!
//! | モジュール | 内容 |
//! |------------|------|
//! | [`config`] | `CoreRunConfig`、formatter 指定、エラー・結果型 |
//! | [`forward`] | `LossyMetrics`、`forward_to_render`、ログ接続セマフォ |
//! | [`pipeline`] | `run` 専用のストリームにパイプライン段を載せる（`apply_pipeline`） |
//! | [`orchestrate`] | `run` — watch、mpsc、各 `tokio::spawn` の配線 |

mod config;
mod forward;
mod orchestrate;
mod pipeline;

pub use config::{
    CoreRunConfig, FormatterChoice, OutputMode, RunError, RunOutcome, RuntimeFwdConfig,
};
pub use forward::{LossyMetrics, build_log_request_semaphore, forward_to_render};
pub use orchestrate::run;
