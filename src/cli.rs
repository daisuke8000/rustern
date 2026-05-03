//! CLI flags for `rstn`. Argument shapes align with [`rustern_core::runtime::CoreRunConfig`];
//! mapping into config types and calling [`rustern_core::run`] happen in follow-up PRs.

use std::path::PathBuf;

use clap::Parser;
use clap::ValueEnum;

/// Tail logs from multiple Kubernetes pods and containers (stern-inspired).
#[derive(Debug, Parser)]
#[command(
    name = "rstn",
    version,
    about = "Kubernetes multi pod and container log tailing",
    long_about = None
)]
pub struct Cli {
    /// Pod name regex or `kind/name` query (e.g. `deploy/api`, `pod/foo`)
    #[arg(value_name = "QUERY")]
    pub query: String,

    // --- Kubernetes context (see `ContextSelector`) ---
    /// Path to kubeconfig (overrides `KUBECONFIG` / `~/.kube/config` resolution)
    #[arg(long, global = true, env = "KUBECONFIG", value_name = "PATH")]
    pub kubeconfig: Option<PathBuf>,

    /// Context name in kubeconfig (overrides `current-context`)
    #[arg(long, global = true, value_name = "NAME")]
    pub context: Option<String>,

    // --- Namespace & selection ---
    /// Kubernetes namespace
    #[arg(short = 'n', long, value_name = "NS")]
    pub namespace: Option<String>,

    /// Log across all namespaces
    #[arg(short = 'A', long = "all-namespaces")]
    pub all_namespaces: bool,

    /// Label selector (optional; see server behavior for interaction with `QUERY`)
    #[arg(long, value_name = "SELECTOR")]
    pub selector: Option<String>,

    // --- Container ---
    /// Container name regex to include
    #[arg(short = 'c', long, default_value = ".*", value_name = "REGEX")]
    pub container: String,

    /// Container name regex to exclude
    #[arg(long, value_name = "REGEX")]
    pub exclude_container: Option<String>,

    // --- Log stream API ---
    /// Stream logs (default on). Same idea as `kubectl logs -f`
    #[arg(short = 'f', long = "follow", action = clap::ArgAction::SetTrue)]
    pub follow_short: bool,

    /// Print logs then exit (disables streaming)
    #[arg(
        long = "no-follow",
        action = clap::ArgAction::SetTrue,
        conflicts_with = "follow_short"
    )]
    pub no_follow: bool,

    /// Lines of recent log to retrieve on startup (`kubectl logs --tail`)
    #[arg(long, value_name = "N")]
    pub tail: Option<i64>,

    /// Return logs newer than a relative duration in seconds (`kubectl logs --since`)
    #[arg(long, value_name = "SECONDS")]
    pub since: Option<i64>,

    // --- Line filters ---
    /// Include log lines matching this regex (repeatable)
    #[arg(short = 'i', long = "include", action = clap::ArgAction::Append)]
    pub include: Vec<String>,

    /// Exclude log lines matching this regex (repeatable)
    #[arg(short = 'e', long = "exclude", action = clap::ArgAction::Append)]
    pub exclude: Vec<String>,

    /// Apply include/exclude to raw or transformed message text
    #[arg(long, value_enum, default_value_t = FilterOnArg::Original)]
    pub filter_on: FilterOnArg,

    /// Optional jaq expression applied to JSON log lines
    #[arg(long = "jq", value_name = "EXPR")]
    pub json_query: Option<String>,

    /// How the jaq expression mutates or filters each line
    #[arg(long = "jq-mode", value_enum, default_value_t = JqModeArg::Filter)]
    pub jq_mode: JqModeArg,

    /// JSON field path used to infer log level (optional)
    #[arg(long, value_name = "PATH")]
    pub level_key: Option<String>,

    // --- Output ---
    /// Output format
    #[arg(long, value_enum, default_value_t = FormatArg::Default)]
    pub format: FormatArg,

    /// Prefix lines with timestamps (default formatter only)
    #[arg(long, default_value_t = true)]
    pub timestamps: bool,

    /// Colorize output (default formatter only)
    #[arg(long, default_value_t = true)]
    pub color: bool,

    // --- Forwarding / backpressure ---
    /// Channel buffer size between pipeline and renderer
    #[arg(long, default_value_t = 4096)]
    pub buffer_size: usize,

    /// Drop log lines when the render channel is full instead of blocking
    #[arg(long, default_value_t = false)]
    pub lossy: bool,

    /// Upper bound on concurrent log API streams
    #[arg(long, default_value_t = 32)]
    pub max_log_requests: usize,
}

/// Matches [`rustern_core::pipeline::FilterOn`] for CLI parsing.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, ValueEnum)]
pub enum FilterOnArg {
    /// Filter on the original message before pipeline transforms
    #[default]
    Original,
    /// Filter after annotation / level / jq stages
    Transformed,
}

/// Matches [`rustern_core::pipeline::QueryMode`] for CLI parsing.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, ValueEnum)]
pub enum JqModeArg {
    #[default]
    Filter,
    Replace,
    Append,
}

/// Chooses default vs JSON vs raw line formatter (maps to [`rustern_core::runtime::FormatterChoice`]).
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, ValueEnum)]
pub enum FormatArg {
    #[default]
    Default,
    Json,
    Raw,
}

impl Cli {
    /// Effective follow flag: stream unless `--no-follow` was passed without `-f`.
    #[must_use]
    pub fn follow(&self) -> bool {
        self.follow_short || !self.no_follow
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_minimal_query() {
        let cli = Cli::try_parse_from(["rstn", "myapp.*"]).unwrap();
        assert_eq!(cli.query, "myapp.*");
        assert!(cli.follow());
    }

    #[test]
    fn no_follow_wins() {
        let cli = Cli::try_parse_from(["rstn", "--no-follow", "x"]).unwrap();
        assert!(!cli.follow());
    }

    #[test]
    fn follow_short_overrides_no_follow_when_not_conflicting() {
        let cli = Cli::try_parse_from(["rstn", "-f", "x"]).unwrap();
        assert!(cli.follow());
    }
}
