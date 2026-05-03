use clap::Parser;

mod cli;

fn main() {
    let cli = cli::Cli::parse();
    let _ = cli.follow();
    let _ = cli.context_selector();

    // TODO: rustern_core::run
    eprintln!("rstn: not implemented");
    std::process::exit(2);
}
