//! `core_run_config` mapping tests: formatter, flags, and CLI→config wiring.
//!
//! Stern-compatible query/namespace defaults live in `rustern_core::discovery::run_resolution`
//! unit tests; scope-resolution duplicates were removed from this module (DSK-69).

use std::io::{self, IsTerminal};
use std::path::PathBuf;
use std::time::Duration;

use clap::Parser;
use tokio_util::sync::CancellationToken;

use crate::cli::Cli;
use rustern_core::discovery::{ContainerLifecycleBucket, ContainerStatePolicy};
use rustern_core::{FilterOn, FormatterChoice, QueryMode, TimestampStyle, TimestampZone};

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
fn maps_container_discovery_exclude_and_state() {
    let cli = cli_with_default_ns(&[
        "-E",
        "a",
        "-E",
        "b",
        "--no-init-containers",
        "--container-state",
        "running",
        "q",
    ]);
    cli.validate().unwrap();
    let cfg = cli.core_run_config(CancellationToken::new()).unwrap();
    assert_eq!(cfg.exclude_container, vec!["a", "b"]);
    assert!(!cfg.container_discovery.include_init_containers);
    assert!(cfg.container_discovery.include_ephemeral_containers);
    let ContainerStatePolicy::Subset(ref hs) = cfg.container_discovery.state_policy else {
        panic!("expected subset policy");
    };
    assert!(hs.len() == 1 && hs.contains(&ContainerLifecycleBucket::Running));

    let cli_default = cli_with_default_ns(&["q"]);
    cli_default.validate().unwrap();
    assert!(
        cli_default
            .core_run_config(CancellationToken::new())
            .unwrap()
            .container_discovery
            .include_init_containers
    );
}

#[test]
fn maps_no_ephemeral_containers_flag() {
    let cli = cli_with_default_ns(&["--no-ephemeral-containers", "q"]);
    cli.validate().unwrap();
    let cfg = cli.core_run_config(CancellationToken::new()).unwrap();
    assert!(!cfg.container_discovery.include_ephemeral_containers);
}

#[test]
fn maps_container_state_all_precedence() {
    let cli = cli_with_default_ns(&["--container-state", "running,all", "q"]);
    cli.validate().unwrap();
    let cfg = cli.core_run_config(CancellationToken::new()).unwrap();
    assert!(matches!(
        cfg.container_discovery.state_policy,
        ContainerStatePolicy::All
    ));
}

#[test]
fn maps_minimal_cli_to_core_config() {
    use std::io::Write;
    let kube = r#"
apiVersion: v1
kind: Config
current-context: ctx
contexts:
  - name: ctx
    context:
      cluster: c
      user: u
clusters:
  - name: c
    cluster:
      server: https://localhost
users:
  - name: u
    user: {}
"#;
    let mut f = tempfile::NamedTempFile::new().unwrap();
    f.write_all(kube.as_bytes()).unwrap();
    let cli = Cli::try_parse_from([
        "rstn",
        "--kubeconfig",
        f.path().to_str().unwrap(),
        "myapp.*",
    ])
    .unwrap();
    cli.validate().unwrap();
    let cfg = cli.core_run_config(CancellationToken::new()).unwrap();
    assert_eq!(cfg.query, "myapp.*");
    assert!(cfg.pod_log.follow);
    assert!(!cfg.all_namespaces);
    assert_eq!(cfg.namespaces, vec!["default"]);
    assert_eq!(cfg.container, ".*");
    assert!(matches!(cfg.formatter, FormatterChoice::Default { .. }));
    assert!(matches!(
        cfg.formatter,
        FormatterChoice::Default {
            timestamp_style: TimestampStyle::Omit,
            ..
        }
    ));
    assert_eq!(cfg.fwd.buffer_size, 4096);
    assert_eq!(cfg.fwd.max_log_requests, 50);
}

#[test]
fn maps_log_api_flags() {
    let cli = cli_with_default_ns(&["--since-time", "2024-03-15T10:30:45Z", "--previous", "q"]);
    cli.validate().unwrap();
    let cfg = cli.core_run_config(CancellationToken::new()).unwrap();
    assert!(cfg.pod_log.since_time.is_some());
    assert!(cfg.pod_log.since_seconds.is_none());
    assert!(cfg.pod_log.previous);
}

#[test]
fn maps_cursor_reconnect_only_when_following() {
    let cli_follow = cli_with_default_ns(&["--cursor-reconnect", "q"]);
    cli_follow.validate().unwrap();
    let cfg_follow = cli_follow
        .core_run_config(CancellationToken::new())
        .unwrap();
    assert!(cfg_follow.cursor_reconnect);

    let cli_no_follow = cli_with_default_ns(&["--cursor-reconnect", "--no-follow", "q"]);
    cli_no_follow.validate().unwrap();
    let cfg_no_follow = cli_no_follow
        .core_run_config(CancellationToken::new())
        .unwrap();
    assert!(!cfg_no_follow.cursor_reconnect);
}

#[test]
fn stern_aligned_default_max_when_no_follow() {
    let cli = cli_with_default_ns(&["--no-follow", "q"]);
    cli.validate().unwrap();
    let cfg = cli.core_run_config(CancellationToken::new()).unwrap();
    assert!(!cfg.pod_log.follow);
    assert_eq!(cfg.fwd.max_log_requests, 5);
}

#[test]
fn maps_flags_namespace_format_and_fwd() {
    let cli = Cli::try_parse_from([
        "rstn",
        "-n",
        "kube-system",
        "--no-follow",
        "--format",
        "json",
        "--buffer-size",
        "8192",
        "--lossy",
        "--max-log-requests",
        "8",
        "deploy/.*",
    ])
    .unwrap();
    cli.validate().unwrap();
    let cfg = cli.core_run_config(CancellationToken::new()).unwrap();
    assert_eq!(cfg.namespaces, vec!["kube-system"]);
    assert!(!cfg.pod_log.follow);
    assert!(matches!(cfg.formatter, FormatterChoice::Json));
    assert!(matches!(cfg.formatter, FormatterChoice::Json));
    assert_eq!(cfg.fwd.buffer_size, 8192);
    assert!(cfg.fwd.lossy);
    assert_eq!(cfg.fwd.max_log_requests, 8);
}

#[test]
fn maps_all_namespaces_and_selector() {
    let cli = Cli::try_parse_from(["rstn", "-A", "-l", "app=myapp", "pod/foo"]).unwrap();
    cli.validate().unwrap();
    let cfg = cli.core_run_config(CancellationToken::new()).unwrap();
    assert!(cfg.all_namespaces);
    assert!(cfg.namespaces.is_empty());
    assert_eq!(cfg.selector.as_deref(), Some("app=myapp"));
}

#[test]
fn maps_context_selector_fields() {
    let cli = cli_with_default_ns(&[
        "--kubeconfig",
        "/etc/kube",
        "--context",
        "prod",
        "-n",
        "default",
        "x",
    ]);
    cli.validate().unwrap();
    let cfg = cli.core_run_config(CancellationToken::new()).unwrap();
    assert_eq!(
        cfg.context.kubeconfig_path,
        Some(PathBuf::from("/etc/kube"))
    );
    assert_eq!(cfg.context.context_name.as_deref(), Some("prod"));
}

#[test]
fn maps_filter_on_and_jq_mode() {
    let cli = cli_with_default_ns(&[
        "--filter-on",
        "transformed",
        "--jq-mode",
        "append",
        "--jq",
        ".msg",
        "q",
    ]);
    cli.validate().unwrap();
    let cfg = cli.core_run_config(CancellationToken::new()).unwrap();
    assert_eq!(cfg.filter_on, FilterOn::Transformed);
    assert_eq!(cfg.json_query_mode, QueryMode::Append);
    assert_eq!(cfg.json_query.as_deref(), Some(".msg"));
}

#[test]
fn omits_timestamps_by_default() {
    let cli = cli_with_default_ns(&["q"]);
    cli.validate().unwrap();
    let cfg = cli.core_run_config(CancellationToken::new()).unwrap();
    assert!(matches!(
        cfg.formatter,
        FormatterChoice::Default {
            timestamp_style: TimestampStyle::Omit,
            ..
        }
    ));
}

#[test]
fn maps_bare_timestamps_flag_to_stern_default() {
    let cli = cli_with_default_ns(&["q", "-t"]);
    cli.validate().unwrap();
    let cfg = cli.core_run_config(CancellationToken::new()).unwrap();
    assert!(matches!(
        cfg.formatter,
        FormatterChoice::Default {
            timestamp_style: TimestampStyle::Rfc3339,
            timestamp_zone: TimestampZone::Local,
            ..
        }
    ));
}

#[test]
fn maps_timestamps_short_and_timezone() {
    let cli = cli_with_default_ns(&["--timestamps=short", "--timezone", "UTC", "q"]);
    cli.validate().unwrap();
    let cfg = cli.core_run_config(CancellationToken::new()).unwrap();
    assert!(matches!(
        cfg.formatter,
        FormatterChoice::Default {
            timestamp_style: TimestampStyle::SternShort,
            timestamp_zone: TimestampZone::Utc,
            ..
        }
    ));
}

#[test]
fn maps_exit_on_flags() {
    let cli = cli_with_default_ns(&["--exit-on", "ERR", "--exit-on-level", "error", "q"]);
    cli.validate().unwrap();
    let cfg = cli.core_run_config(CancellationToken::new()).unwrap();
    assert_eq!(cfg.exit_on, vec!["ERR".to_string()]);
    assert_eq!(
        cfg.exit_on_level,
        Some(rustern_core::pipeline::ExitOnLevel::Error)
    );
}

#[test]
fn maps_stats_flags() {
    let cli = cli_with_default_ns(&["--stats", "--stats-interval", "45s", "q"]);
    cli.validate().unwrap();
    let cfg = cli.core_run_config(CancellationToken::new()).unwrap();
    let stats = cfg.fwd.stats.expect("stats config");
    assert_eq!(stats.interval, Duration::from_secs(45));
}

#[test]
fn leaves_stats_disabled_without_flag() {
    let cli = cli_with_default_ns(&["q"]);
    cli.validate().unwrap();
    let cfg = cli.core_run_config(CancellationToken::new()).unwrap();
    assert!(cfg.fwd.stats.is_none());
}

#[test]
fn maps_since_duration_to_seconds() {
    let cli = cli_with_default_ns(&["--since", "5m", "q"]);
    cli.validate().unwrap();
    let cfg = cli.core_run_config(CancellationToken::new()).unwrap();
    assert_eq!(cfg.pod_log.since_seconds, Some(300));
}

#[test]
fn core_run_config_errors_on_invalid_since_without_validate() {
    let cli = cli_with_default_ns(&["--since", "not-a-time", "q"]);
    assert!(cli.core_run_config(CancellationToken::new()).is_err());
}

#[test]
fn maps_color_auto_matches_stdout_tty() {
    let cli = cli_with_default_ns(&["q"]);
    cli.validate().unwrap();
    let cfg = cli.core_run_config(CancellationToken::new()).unwrap();
    let expect = io::stdout().is_terminal();
    assert!(matches!(
        cfg.formatter,
        FormatterChoice::Default {
            color_enabled,
            ..
        } if color_enabled == expect
    ));
}

#[test]
fn maps_color_always_enables_color() {
    let cli = cli_with_default_ns(&["--color", "always", "q"]);
    cli.validate().unwrap();
    let cfg = cli.core_run_config(CancellationToken::new()).unwrap();
    assert!(matches!(
        cfg.formatter,
        FormatterChoice::Default {
            color_enabled: true,
            ..
        }
    ));
}

#[test]
fn maps_comma_and_repeat_namespaces() {
    let cli = Cli::try_parse_from([
        "rstn",
        "-n",
        "a,b",
        "--namespace",
        "b",
        "--namespace",
        "c",
        "x",
    ])
    .unwrap();
    cli.validate().unwrap();
    let cfg = cli.core_run_config(CancellationToken::new()).unwrap();
    assert_eq!(cfg.namespaces, vec!["a", "b", "c"]);
}

#[test]
fn maps_field_selector_and_node_and_exclude_pod() {
    let cli = cli_with_default_ns(&[
        "--field-selector",
        "status.phase=Running",
        "--node",
        "worker-1",
        "--exclude-pod",
        "junk-.*",
        ".*",
    ]);
    cli.validate().unwrap();
    let cfg = cli.core_run_config(CancellationToken::new()).unwrap();
    assert_eq!(cfg.field_selector.as_deref(), Some("status.phase=Running"));
    assert_eq!(cfg.node.as_deref(), Some("worker-1"));
    assert_eq!(cfg.exclude_pod, vec!["junk-.*".to_string()]);
}

#[test]
fn maps_color_never_disables_color() {
    let cli = cli_with_default_ns(&["--color", "never", "q"]);
    cli.validate().unwrap();
    let cfg = cli.core_run_config(CancellationToken::new()).unwrap();
    assert!(matches!(
        cfg.formatter,
        FormatterChoice::Default {
            color_enabled: false,
            ..
        }
    ));
}

#[test]
fn maps_pod_and_container_color_flags() {
    let cli = cli_with_default_ns(&[
        "--no-pod-colors",
        "--container-colors",
        "--diff-container",
        "q",
    ]);
    cli.validate().unwrap();
    let cfg = cli.core_run_config(CancellationToken::new()).unwrap();
    assert!(matches!(
        cfg.formatter,
        FormatterChoice::Default {
            pod_colors: false,
            container_colors: true,
            ..
        }
    ));
    assert!(cfg.diff_container);
}

#[test]
fn maps_no_container_colors() {
    let cli = cli_with_default_ns(&["--no-container-colors", "q"]);
    cli.validate().unwrap();
    let cfg = cli.core_run_config(CancellationToken::new()).unwrap();
    assert!(matches!(
        cfg.formatter,
        FormatterChoice::Default {
            container_colors: false,
            ..
        }
    ));
}

#[test]
fn validate_rejects_conflicting_pod_color_flags() {
    let cli = cli_with_default_ns(&["--no-pod-colors", "--pod-colors=true", "q"]);
    assert!(cli.validate().is_err());
}

#[test]
fn maps_highlight_and_only_log_lines() {
    let cli = cli_with_default_ns(&["--no-follow", "-H", "panic", "--only-log-lines", "."]);
    cli.validate().unwrap();
    let cfg = cli.core_run_config(CancellationToken::new()).unwrap();
    assert_eq!(cfg.highlight, vec!["panic".to_string()]);
    assert!(cfg.only_log_lines);
    assert!(cfg.include.is_empty());
}

#[test]
fn maps_condition_with_no_follow() {
    let cli = cli_with_default_ns(&["--no-follow", "--condition=ready=false", "q"]);
    cli.validate().unwrap();
    let cfg = cli.core_run_config(CancellationToken::new()).unwrap();
    let cond = cfg.pod_condition.as_ref().unwrap();
    assert_eq!(cond.type_name, "ready");
    assert_eq!(cond.status, "False");
}

#[test]
fn rejects_condition_with_follow_without_tail_zero() {
    let cli = cli_with_default_ns(&["-f", "--condition=ready", "q"]);
    assert!(cli.validate().is_err());
}
