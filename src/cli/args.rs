use std::path::PathBuf;
use std::time::Duration;

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
    /// Pod name regex or `kind/name` (e.g. `deploy/api`); omit with `-l` or `--field-selector` (implicit `.*`)
    #[arg(value_name = "QUERY", required = false)]
    pub query: Option<String>,

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

    /// Exclude containers matching this regex (repeat; comma-separated accepted)
    #[arg(
        short = 'E',
        long = "exclude-container",
        value_name = "REGEX",
        action = clap::ArgAction::Append,
        value_delimiter = ','
    )]
    pub exclude_container: Vec<String>,

    /// Tail init containers alongside regular containers (`--no-init-containers` to omit; stern-like default yes)
    #[arg(
        long = "init-containers",
        num_args = 0..=1,
        default_missing_value = "true",
        require_equals = false,
        value_parser = clap::builder::BoolishValueParser::new()
    )]
    pub init_containers: Option<bool>,

    #[arg(long = "no-init-containers", action = clap::ArgAction::SetTrue)]
    pub no_init_containers: bool,

    /// Tail ephemeral containers (stern-like default yes); pass `--no-ephemeral-containers` to omit
    #[arg(
        long = "ephemeral-containers",
        num_args = 0..=1,
        default_missing_value = "true",
        require_equals = false,
        value_parser = clap::builder::BoolishValueParser::new()
    )]
    pub ephemeral_containers: Option<bool>,

    #[arg(long = "no-ephemeral-containers", action = clap::ArgAction::SetTrue)]
    pub no_ephemeral_containers: bool,

    /// Filter container streams by reported lifecycle bucket (`running`|`waiting`|`terminated`|`all`; repeat or comma-separated)
    #[arg(
        long = "container-state",
        value_enum,
        action = clap::ArgAction::Append,
        value_delimiter = ','
    )]
    pub container_states: Vec<ContainerStateArg>,

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

    /// Filter pods by status condition (`ready=false`, etc.; requires `--no-follow` or `--tail=0`)
    #[arg(long = "condition", value_name = "NAME[=VALUE]")]
    pub condition: Option<String>,

    /// Only logs newer than this duration (`5m`, `2h`, `90s`) or a non-negative integer (seconds)
    #[arg(
        short = 's',
        long = "since",
        value_name = "DURATION|SECONDS",
        conflicts_with = "since_time"
    )]
    pub since: Option<String>,

    /// Only logs newer than this RFC3339 timestamp (kubectl `--since-time`; exclusive with `--since`)
    #[arg(long = "since-time", value_name = "RFC3339", conflicts_with = "since")]
    pub since_time: Option<String>,

    /// Logs from the previous terminated container instance (kubectl `--previous`)
    #[arg(long = "previous", action = clap::ArgAction::SetTrue)]
    pub previous: bool,

    /// Include lines matching regex (repeatable)
    #[arg(short = 'i', long = "include", action = clap::ArgAction::Append)]
    pub include: Vec<String>,

    /// Exclude lines matching regex (repeatable)
    #[arg(short = 'e', long = "exclude", action = clap::ArgAction::Append)]
    pub exclude: Vec<String>,

    /// Highlight matching text on default-formatted lines (`stern`'s `-H`; merged with `--include`).
    #[arg(short = 'H', long = "highlight", value_name = "REGEX", action = clap::ArgAction::Append)]
    pub highlight: Vec<String>,

    /// Hide stern-style +/- stream banners on stderr (rustern has no equivalents today).
    #[arg(long = "only-log-lines", action = clap::ArgAction::SetTrue)]
    pub only_log_lines: bool,

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

    /// Exit with code 1 when raw message matches regex (repeatable; rustern-plus; before `-i`/`-e`)
    #[arg(long = "exit-on", value_name = "REGEX", action = clap::ArgAction::Append)]
    pub exit_on: Vec<String>,

    /// Exit with code 1 when log level is at or above LEVEL (rustern-plus; after level classify)
    #[arg(long = "exit-on-level", value_name = "LEVEL")]
    pub exit_on_level: Option<String>,

    /// Line format
    #[arg(long, value_enum, default_value_t = FormatArg::Default)]
    pub format: FormatArg,

    /// Stern-style timestamp prefix for the default formatter (`-t` / `--timestamps`; off unless set)
    #[arg(
        short = 't',
        long = "timestamps",
        value_enum,
        num_args = 0..=1,
        default_missing_value = "default"
    )]
    pub timestamps: Option<TimestampArg>,

    #[arg(long, value_name = "ZONE")]
    pub timezone: Option<String>,

    /// Color output policy for the default formatter (`auto` if stdout is a TTY)
    #[arg(long, value_enum, default_value_t = ColorArg::Auto)]
    pub color: ColorArg,

    /// Highlight pod names in the default formatter (stern `--pod-colors`; default on)
    #[arg(
        long = "pod-colors",
        num_args = 0..=1,
        default_missing_value = "true",
        require_equals = false,
        value_parser = clap::builder::BoolishValueParser::new()
    )]
    pub pod_colors: Option<bool>,

    #[arg(long = "no-pod-colors", action = clap::ArgAction::SetTrue)]
    pub no_pod_colors: bool,

    /// Highlight container names (defaults to same enablement as `--pod-colors`)
    #[arg(
        long = "container-colors",
        num_args = 0..=1,
        default_missing_value = "true",
        require_equals = false,
        value_parser = clap::builder::BoolishValueParser::new()
    )]
    pub container_colors: Option<bool>,

    #[arg(long = "no-container-colors", action = clap::ArgAction::SetTrue)]
    pub no_container_colors: bool,

    /// Use a distinct palette slot per container (`stern` `-d` / `--diff-container`)
    #[arg(short = 'd', long = "diff-container", action = clap::ArgAction::SetTrue)]
    pub diff_container: bool,

    /// Pipeline→renderer channel size
    #[arg(long, default_value_t = 4096)]
    pub buffer_size: usize,

    /// Drop lines when the render channel is full
    #[arg(long, default_value_t = false)]
    pub lossy: bool,

    /// Emit periodic runtime stats to stderr (rustern-plus)
    #[arg(long, default_value_t = false)]
    pub stats: bool,

    /// Stats report interval (`30s`, `5m`, etc.; rustern-plus)
    #[arg(
        long = "stats-interval",
        default_value = "30s",
        value_name = "DURATION",
        value_parser = parse_stats_interval
    )]
    pub stats_interval: Duration,

    #[arg(long, value_name = "N")]
    pub max_log_requests: Option<usize>,
}

fn parse_stats_interval(s: &str) -> Result<Duration, String> {
    let interval = humantime::parse_duration(s)
        .map_err(|e| format!("invalid --stats-interval duration: {e}"))?;
    if interval.is_zero() {
        return Err("--stats-interval must be > 0".into());
    }
    Ok(interval)
}

/// Mirrors stern's `--container-state` choices.
#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub enum ContainerStateArg {
    Running,
    Waiting,
    Terminated,
    All,
}

/// Default-formatter stamp shape (stern-aligned names).
#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub enum TimestampArg {
    #[value(alias = "rfc3339")]
    Default,
    #[value(alias = "off")]
    Omit,
    Short,
    Epoch,
}

/// Regex stage knob for `-i`/`-e` (plain text vs jq output).
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, ValueEnum)]
pub enum FilterOnArg {
    /// Match include/exclude on the raw NDJSON/message line.
    #[default]
    Original,
    /// Match after jaq rewriting when `--jq` is present.
    Transformed,
}

/// How `--jq` rewrites or filters JSON log payloads.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, ValueEnum)]
pub enum JqModeArg {
    /// Drop falsy jq results.
    #[default]
    Filter,
    Replace,
    Append,
}

/// Default formatter ANSI color policy.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, ValueEnum)]
pub enum ColorArg {
    /// Color when stdout is a TTY.
    #[default]
    Auto,
    Always,
    Never,
}

/// High-level output layout (mirrors [`rustern_core::OutputMode`]).
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, ValueEnum)]
pub enum FormatArg {
    #[default]
    Default,
    Json,
    ExtJson,
    PpExtJson,
    Raw,
}
