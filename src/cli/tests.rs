use std::path::PathBuf;
use std::time::Duration;

use clap::Parser;

use super::{Cli, parse_since};

fn cli_with_default_ns(args: &[&str]) -> Cli {
    let mut argv = vec!["rstn"];
    let has_ns = args.iter().any(|a| {
        matches!(*a, "-n" | "--namespace" | "-A" | "--all-namespaces")
            || a.starts_with("--namespace=")
            || a.starts_with("-n=")
    });
    if !has_ns {
        argv.extend(["-n", "default"]);
    }
    argv.extend(args.iter().copied().filter(|&a| a != "rstn"));
    Cli::try_parse_from(argv).unwrap()
}

#[test]
fn parses_minimal_query() {
    let cli = Cli::try_parse_from(["rstn", "myapp.*"]).unwrap();
    assert_eq!(cli.query.as_deref(), Some("myapp.*"));
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
fn init_containers_defaults_match_stern_until_flag() {
    let cli = Cli::try_parse_from(["rstn", "q"]).unwrap();
    assert!(cli.init_containers.is_none());
    assert!(!cli.no_init_containers);
    assert!(cli.ephemeral_containers.is_none());
    assert!(!cli.no_ephemeral_containers);
    assert!(cli.container_states.is_empty());
}

#[test]
fn exclude_container_accepts_short_cap_e() {
    let cli = Cli::try_parse_from(["rstn", "-E", "sidecar", "q"]).unwrap();
    assert_eq!(cli.exclude_container, vec!["sidecar".to_string()]);
}

#[test]
fn no_init_containers_sets_exclusion_semantics_on_parse() {
    let cli = cli_with_default_ns(&["--no-init-containers", "q"]);
    assert!(cli.no_init_containers);
    cli.validate().unwrap();
}

#[test]
fn init_containers_eq_false_via_boolish_parser() {
    let cli = cli_with_default_ns(&["--init-containers=false", "q"]);
    assert_eq!(cli.init_containers, Some(false));
    assert!(cli.validate().is_ok());
}

#[test]
fn validate_rejects_conflicting_init_flags() {
    let cli = Cli::try_parse_from([
        "rstn",
        "--no-init-containers",
        "--init-containers=true",
        "q",
    ])
    .unwrap();
    assert!(cli.validate().is_err());
}

#[test]
fn validate_rejects_conflicting_ephemeral_flags() {
    let cli = Cli::try_parse_from([
        "rstn",
        "--no-ephemeral-containers",
        "--ephemeral-containers=true",
        "q",
    ])
    .unwrap();
    assert!(cli.validate().is_err());
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
fn validate_rejects_invalid_highlight_regex() {
    let cli = Cli::try_parse_from(["rstn", "--highlight", "(unclosed", "q"]).unwrap();
    assert!(cli.validate().is_err());
}

#[test]
fn validate_rejects_invalid_include_regex() {
    let cli = Cli::try_parse_from(["rstn", "--include", "(unclosed", "q"]).unwrap();
    assert!(cli.validate().is_err());
}

#[test]
fn validate_rejects_invalid_exclude_regex() {
    let cli = Cli::try_parse_from(["rstn", "--exclude", "(unclosed", "q"]).unwrap();
    assert!(cli.validate().is_err());
}

#[test]
fn validate_rejects_invalid_container_regex() {
    let cli = Cli::try_parse_from(["rstn", "--container", "(unclosed", "q"]).unwrap();
    assert!(cli.validate().is_err());
}

#[test]
fn validate_rejects_invalid_json_query() {
    let cli = Cli::try_parse_from(["rstn", "--jq", "(unclosed", "q"]).unwrap();
    assert!(cli.validate().is_err());
}

#[test]
fn validate_accepts_defaults() {
    let cli = cli_with_default_ns(&["x"]);
    cli.validate().unwrap();
}

#[test]
fn since_accepts_short_s_flag() {
    let cli = cli_with_default_ns(&["-s", "2m", "q"]);
    cli.validate().unwrap();
    assert_eq!(cli.since.as_deref(), Some("2m"));
}

#[test]
fn since_and_since_time_conflict() {
    assert!(
        Cli::try_parse_from([
            "rstn",
            "--since",
            "5m",
            "--since-time",
            "2024-01-01T00:00:00Z",
            "q"
        ])
        .is_err()
    );
}

#[test]
fn validate_rejects_invalid_since_time() {
    let cli = Cli::try_parse_from(["rstn", "--since-time", "bogus", "q"]).unwrap();
    assert!(cli.validate().is_err());
}

#[test]
fn validate_accepts_label_selector_without_query() {
    let cli = cli_with_default_ns(&["-l", "app=foo"]);
    cli.validate().unwrap();
}

#[test]
fn validate_accepts_field_selector_without_query() {
    let cli = cli_with_default_ns(&["--field-selector", "metadata.name=foo"]);
    cli.validate().unwrap();
}

#[test]
fn exit_on_flags_parse() {
    let cli = cli_with_default_ns(&["--exit-on", "panic", "--exit-on-level", "warn", "q"]);
    cli.validate().unwrap();
    assert_eq!(cli.exit_on, vec!["panic".to_string()]);
    assert_eq!(cli.exit_on_level.as_deref(), Some("warn"));
}

#[test]
fn stats_flags_parse() {
    let cli = cli_with_default_ns(&["--stats", "--stats-interval", "45s", "q"]);
    cli.validate().unwrap();
    assert!(cli.stats);
    assert_eq!(cli.stats_interval, Duration::from_secs(45));
}

#[test]
fn stats_interval_defaults_to_thirty_seconds() {
    let cli = cli_with_default_ns(&["q"]);
    cli.validate().unwrap();
    assert!(!cli.stats);
    assert_eq!(cli.stats_interval, Duration::from_secs(30));
}

#[test]
fn validate_rejects_invalid_exit_on_regex() {
    let cli = cli_with_default_ns(&["--exit-on", "(unclosed", "q"]);
    assert!(cli.validate().is_err());
}

#[test]
fn validate_rejects_invalid_exit_on_level() {
    let cli = cli_with_default_ns(&["--exit-on-level", "bogus", "q"]);
    assert!(cli.validate().is_err());
}

#[test]
fn previous_flag_parses() {
    let cli = cli_with_default_ns(&["--previous", "q"]);
    cli.validate().unwrap();
    assert!(cli.previous);
}

#[test]
fn cursor_reconnect_flag_parses() {
    let cli = cli_with_default_ns(&["--cursor-reconnect", "q"]);
    cli.validate().unwrap();
    assert!(cli.cursor_reconnect);
}
