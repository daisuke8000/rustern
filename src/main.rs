//! `rstn` binary entrypoint.

use clap::Parser;

mod cli;

fn main() {
    let cli = cli::Cli::parse();
    // Read so follow / no-follow stay live for the next PR; not used for I/O yet.
    let _ = cli.follow();

    // PR1: argument definitions only; config assembly and `rustern_core::run` come next.
    eprintln!(
        "rstn: log tail is not wired yet (CLI skeleton only; see next PR for `rustern_core::run`)"
    );
    std::process::exit(2);
}
