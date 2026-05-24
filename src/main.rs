use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};

use clap::Parser;
use tokio_util::sync::CancellationToken;

mod cli;
mod report;
mod run_config;
mod run_defaults;

const SHUTDOWN_NONE: u8 = 0;
const SHUTDOWN_SIGINT: u8 = 1;
const SHUTDOWN_SIGTERM: u8 = 2;

fn install_crypto_provider() {
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("failed to install rustls ring crypto provider");
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    install_crypto_provider();
    report::install_hook();

    let cli = cli::Cli::parse();
    if let Err(msg) = cli.validate() {
        report::fail_msg(msg);
    }

    let shutdown_reason = Arc::new(AtomicU8::new(SHUTDOWN_NONE));
    let root_token = CancellationToken::new();

    let sig_reason = Arc::clone(&shutdown_reason);
    let sig_token = root_token.clone();

    #[cfg(unix)]
    {
        let (sigterm, sigint) = match register_unix_signals().await {
            Ok(pair) => pair,
            Err(e) => report::fail_msg(format!("failed to register signal handlers: {e}")),
        };
        tokio::spawn(async move {
            listen_unix_shutdown(sig_reason, sig_token, sigterm, sigint).await;
        });
    }

    #[cfg(not(unix))]
    tokio::spawn(async move {
        listen_windows_shutdown(sig_reason, sig_token).await;
    });

    let cfg = match cli.core_run_config(root_token) {
        Ok(cfg) => cfg,
        Err(msg) => report::fail_msg(msg),
    };

    match rustern_core::run(cfg).await {
        Ok(outcome) => {
            if outcome.had_source_errors {
                report::fail_with_code(
                    miette::Report::msg("one or more log sources reported errors"),
                    2,
                );
            }
            exit_after_ok(shutdown_reason.load(Ordering::SeqCst));
        }
        Err(e) => report::fail_run(e),
    }
}

fn exit_after_ok(reason: u8) -> ! {
    match reason {
        SHUTDOWN_SIGINT => std::process::exit(130),
        SHUTDOWN_SIGTERM => std::process::exit(143),
        _ => std::process::exit(0),
    }
}

#[cfg(unix)]
async fn register_unix_signals()
-> std::io::Result<(tokio::signal::unix::Signal, tokio::signal::unix::Signal)> {
    use tokio::signal::unix::{SignalKind, signal};
    Ok((
        signal(SignalKind::terminate())?,
        signal(SignalKind::interrupt())?,
    ))
}

#[cfg(unix)]
async fn listen_unix_shutdown(
    reason: Arc<AtomicU8>,
    root: CancellationToken,
    mut sigterm: tokio::signal::unix::Signal,
    mut sigint: tokio::signal::unix::Signal,
) {
    tokio::select! {
        _ = sigterm.recv() => {
            reason.store(SHUTDOWN_SIGTERM, Ordering::SeqCst);
        }
        _ = sigint.recv() => {
            reason.store(SHUTDOWN_SIGINT, Ordering::SeqCst);
        }
    }
    root.cancel();
}

#[cfg(not(unix))]
async fn listen_windows_shutdown(reason: Arc<AtomicU8>, root: CancellationToken) {
    match tokio::signal::ctrl_c().await {
        Ok(()) => {
            reason.store(SHUTDOWN_SIGINT, Ordering::SeqCst);
            root.cancel();
        }
        Err(e) => report::fail_msg(format!("failed to register Ctrl+C handler: {e}")),
    }
}
