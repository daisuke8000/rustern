use std::path::PathBuf;

use clap::Parser;
use clap::ValueEnum;
use regex::Regex;
use rustern_core::ContextSelector;

/// Tail logs from multiple Kubernetes pods and containers (stern-inspired).
#[derive(Debug, Parser)]
#[command(
    name = "rstn",
    version,
    about = "Kubernetes multi pod and container log tailing",
    long_about = None
)]
pub struct Cli {
    /// Pod name regex or `kind/name` (e.g. `deploy/api`)
    #[arg(value_name = "QUERY")]
    pub query: String,

    /// Kubeconfig file; omit for `rustern-core` default lookup (`KUBECONFIG` / `~/.kube/config`).
    #[arg(long, global = true, value_name = "PATH")]
    pub kubeconfig: Option<PathBuf>,

    /// Context name
    #[arg(long, global = true, env = "KUBE_CONTEXT", value_name = "NAME")]
    pub context: Option<String>,

    /// Namespace (repeat; comma-separated in one value is allowed)
    #[arg(
        short = 'n',
        long = "namespace",
        value_name = "NS",
        conflicts_with = "all_namespaces",
        action = clap::ArgAction::Append
    )]
    pub namespaces: Vec<String>,

    /// All namespaces
    #[arg(short = 'A', long = "all-namespaces", conflicts_with = "namespaces")]
    pub all_namespaces: bool,

    /// Label selector
    #[arg(short = 'l', long, value_name = "SELECTOR")]
    pub selector: Option<String>,

    /// Field selector for pods (server-side)
    #[arg(long, value_name = "SELECTOR")]
    pub field_selector: Option<String>,

    /// Node name (adds spec.nodeName to field selector)
    #[arg(long, value_name = "NAME")]
    pub node: Option<String>,

    /// Exclude pods whose name matches this regex (repeatable)
    #[arg(long = "exclude-pod", value_name = "REGEX", action = clap::ArgAction::Append)]
    pub exclude_pod: Vec<String>,

    /// Container name regex
    #[arg(short = 'c', long, default_value = ".*", value_name = "REGEX")]
    pub container: String,

    /// Exclude containers matching this regex
    #[arg(short = 'E', long, value_name = "REGEX")]
    pub exclude_container: Option<String>,

    /// Stream logs (`kubectl logs -f`)
    #[arg(short = 'f', long = "follow", action = clap::ArgAction::SetTrue)]
    pub follow_short: bool,

    /// One-shot: do not stream
    #[arg(
        long = "no-follow",
        action = clap::ArgAction::SetTrue,
        conflicts_with = "follow_short"
    )]
    pub no_follow: bool,

    /// Tail line count
    #[arg(long, value_name = "N")]
    pub tail: Option<i64>,

    /// Only logs newer than this duration (`5m`, `2h`, `90s`) or a non-negative integer (seconds)
    #[arg(long, value_name = "DURATION|SECONDS")]
    pub since: Option<String>,

    /// Include lines matching regex (repeatable)
    #[arg(short = 'i', long = "include", action = clap::ArgAction::Append)]
    pub include: Vec<String>,

    /// Exclude lines matching regex (repeatable)
    #[arg(short = 'e', long = "exclude", action = clap::ArgAction::Append)]
    pub exclude: Vec<String>,

    /// Stage for include/exclude regex
    #[arg(long, value_enum, default_value_t = FilterOnArg::Original)]
    pub filter_on: FilterOnArg,

    /// jaq expression for JSON lines
    #[arg(long = "jq", value_name = "EXPR")]
    pub json_query: Option<String>,

    /// jaq mode
    #[arg(long = "jq-mode", value_enum, default_value_t = JqModeArg::Filter)]
    pub jq_mode: JqModeArg,

    /// JSON field path for log level
    #[arg(long, value_name = "PATH")]
    pub level_key: Option<String>,

    /// Line format
    #[arg(long, value_enum, default_value_t = FormatArg::Default)]
    pub format: FormatArg,

    /// Timestamps (default formatter)
    #[arg(long, default_value_t = true)]
    pub timestamps: bool,

    /// Color output policy for the default formatter (`auto` if stdout is a TTY)
    #[arg(long, value_enum, default_value_t = ColorArg::Auto)]
    pub color: ColorArg,

    /// Pipeline→renderer channel size
    #[arg(long, default_value_t = 4096)]
    pub buffer_size: usize,

    /// Drop lines when the render channel is full
    #[arg(long, default_value_t = false)]
    pub lossy: bool,

    /// Max concurrent log streams
    #[arg(long, default_value_t = 32)]
    pub max_log_requests: usize,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, ValueEnum)]
pub enum FilterOnArg {
    #[default]
    Original,
    Transformed,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, ValueEnum)]
pub enum JqModeArg {
    #[default]
    Filter,
    Replace,
    Append,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, ValueEnum)]
pub enum ColorArg {
    #[default]
    Auto,
    Always,
    Never,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, ValueEnum)]
pub enum FormatArg {
    #[default]
    Default,
    Json,
    Raw,
}

impl Cli {
    #[must_use]
    pub fn context_selector(&self) -> ContextSelector {
        ContextSelector {
            kubeconfig_path: self.kubeconfig.clone(),
            context_name: self.context.clone(),
        }
    }

    #[must_use]
    pub fn follow(&self) -> bool {
        self.follow_short || !self.no_follow
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.tail.is_some_and(|v| v < 0) {
            return Err("--tail must be >= 0".into());
        }
        if let Some(s) = &self.since {
            parse_since(s)?;
        }
        if self.buffer_size == 0 {
            return Err("--buffer-size must be > 0".into());
        }
        if self.max_log_requests == 0 {
            return Err("--max-log-requests must be > 0".into());
        }
        for p in &self.exclude_pod {
            Regex::new(p).map_err(|e| format!("invalid --exclude-pod regex: {e}"))?;
        }
        Ok(())
    }
}

/// Parse `--since` as a humantime duration or a non‑negative integer (seconds).
pub(crate) fn parse_since(s: &str) -> Result<i64, String> {
    let s = s.trim();
    if s.is_empty() {
        return Err("empty --since".into());
    }
    if let Ok(d) = humantime::parse_duration(s) {
        let secs = d.as_secs();
        return i64::try_from(secs).map_err(|_| "--since duration too large".to_string());
    }
    let n: i64 = s
        .parse()
        .map_err(|_| format!("invalid --since (expected duration or seconds): {s}"))?;
    if n < 0 {
        return Err("--since must be >= 0".into());
    }
    Ok(n)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_minimal_query() {
        let cli = Cli::try_parse_from(["rstn", "myapp.*"]).unwrap();
        assert_eq!(cli.query, "myapp.*");
        assert!(cli.follow());
        let sel = cli.context_selector();
        assert!(sel.kubeconfig_path.is_none());
        assert!(sel.context_name.is_none());
    }

    #[test]
    fn label_selector_accepts_short_l() {
        let cli = Cli::try_parse_from(["rstn", "-l", "app=myapp", "q"]).unwrap();
        assert_eq!(cli.selector.as_deref(), Some("app=myapp"));
    }

    #[test]
    fn exclude_container_accepts_short_cap_e() {
        let cli = Cli::try_parse_from(["rstn", "-E", "sidecar", "q"]).unwrap();
        assert_eq!(cli.exclude_container.as_deref(), Some("sidecar"));
    }

    #[test]
    fn context_selector_roundtrips_explicit_kubeconfig() {
        let cli = Cli::try_parse_from(["rstn", "--kubeconfig", "/tmp/kube", "q"]).unwrap();
        assert_eq!(
            cli.context_selector().kubeconfig_path,
            Some(PathBuf::from("/tmp/kube"))
        );
    }

    #[test]
    fn no_follow_wins() {
        let cli = Cli::try_parse_from(["rstn", "--no-follow", "x"]).unwrap();
        assert!(!cli.follow());
    }

    #[test]
    fn follow_flag_sets_streaming() {
        let cli = Cli::try_parse_from(["rstn", "-f", "x"]).unwrap();
        assert!(cli.follow());
    }

    #[test]
    fn namespace_and_all_namespaces_conflict() {
        assert!(Cli::try_parse_from(["rstn", "-n", "ns", "-A", "q"]).is_err());
    }

    #[test]
    fn validate_rejects_negative_tail() {
        let cli = Cli::try_parse_from(["rstn", "--tail=-1", "q"]).unwrap();
        assert!(cli.validate().is_err());
    }

    #[test]
    fn validate_rejects_zero_buffer_size() {
        let cli = Cli::try_parse_from(["rstn", "--buffer-size", "0", "q"]).unwrap();
        assert!(cli.validate().is_err());
    }

    #[test]
    fn parse_since_accepts_duration_and_integer_seconds() {
        assert_eq!(parse_since("5m").unwrap(), 300);
        assert_eq!(parse_since("90").unwrap(), 90);
        assert_eq!(parse_since("0").unwrap(), 0);
        assert!(parse_since("not-a-time").is_err());
        assert!(parse_since("-1").is_err());
    }

    #[test]
    fn validate_rejects_invalid_since() {
        let cli = Cli::try_parse_from(["rstn", "--since", "bogus", "q"]).unwrap();
        assert!(cli.validate().is_err());
    }

    #[test]
    fn validate_rejects_invalid_exclude_pod_regex() {
        let cli = Cli::try_parse_from(["rstn", "--exclude-pod", "(unclosed", "q"]).unwrap();
        assert!(cli.validate().is_err());
    }

    #[test]
    fn validate_accepts_defaults() {
        let cli = Cli::try_parse_from(["rstn", "x"]).unwrap();
        cli.validate().unwrap();
    }
}
