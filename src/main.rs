use clap::Parser;
use tokio_util::sync::CancellationToken;

mod cli;
mod run_config;

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let cli = cli::Cli::parse();
    if let Err(msg) = cli.validate() {
        eprintln!("error: {msg}");
        std::process::exit(1);
    }

    let cfg = cli.core_run_config(CancellationToken::new());

    match rustern_core::run(cfg).await {
        Ok(outcome) => {
            if outcome.had_source_errors {
                std::process::exit(2);
            }
        }
        Err(e) => {
            eprintln!("error: {e}");
            std::process::exit(1);
        }
    }
}
