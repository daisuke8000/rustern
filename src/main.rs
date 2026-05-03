use clap::Parser;
use tokio_util::sync::CancellationToken;

mod cli;
mod run_config;

fn main() {
    let cli = cli::Cli::parse();
    if let Err(msg) = cli.validate() {
        eprintln!("error: {msg}");
        std::process::exit(1);
    }
    let _cfg = cli.core_run_config(CancellationToken::new());

    // TODO: rustern_core::run
    eprintln!("rstn: not implemented");
    std::process::exit(2);
}
