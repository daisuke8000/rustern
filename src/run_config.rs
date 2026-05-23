//! Map [`crate::cli::Cli`] into [`rustern_core::CoreRunConfig`].

use std::collections::HashSet;
use std::io::{self, IsTerminal};

use tokio_util::sync::CancellationToken;

use rustern_core::discovery::pod_condition::parse_pod_condition;
use rustern_core::discovery::pod_watcher::{
    ContainerDiscoverOpts, ContainerLifecycleBucket, ContainerStatePolicy,
};
use rustern_core::{CoreRunConfig, FormatterChoice, OutputMode, RuntimeFwdConfig};
use rustern_core::{FilterOn, QueryMode, TimestampStyle, TimestampZone};

use crate::cli::{
    Cli, ColorArg, ContainerStateArg, FilterOnArg, FormatArg, JqModeArg, TimestampArg, parse_since,
};
use crate::run_defaults::resolved_pod_query;

/// Deduped namespaces from repeatable `--namespace`/comma inputs; defaults to `[default]` when omitted.
fn normalized_namespaces(cli: &Cli) -> Vec<String> {
    let mut out = Vec::new();
    for part in &cli.namespaces {
        for seg in part.split(',') {
            let t = seg.trim();
            if t.is_empty() {
                continue;
            }
            if !out.iter().any(|n| n == t) {
                out.push(t.to_string());
            }
        }
    }
    if out.is_empty() {
        vec!["default".to_string()]
    } else {
        out
    }
}

fn resolved_init_include(cli: &Cli) -> bool {
    if cli.no_init_containers {
        false
    } else {
        cli.init_containers.unwrap_or(true)
    }
}

fn resolved_ephemeral_include(cli: &Cli) -> bool {
    if cli.no_ephemeral_containers {
        false
    } else {
        cli.ephemeral_containers.unwrap_or(true)
    }
}

fn resolved_pod_colors(cli: &Cli) -> bool {
    if cli.no_pod_colors {
        false
    } else {
        cli.pod_colors.unwrap_or(true)
    }
}

fn resolved_container_colors(cli: &Cli) -> bool {
    if cli.no_container_colors {
        false
    } else if let Some(v) = cli.container_colors {
        v
    } else {
        resolved_pod_colors(cli)
    }
}

fn resolved_container_state_policy(states: &[ContainerStateArg]) -> ContainerStatePolicy {
    use ContainerStateArg as A;
    if states.is_empty() {
        return ContainerStatePolicy::All;
    }
    if states.iter().copied().any(|s| s == A::All) {
        return ContainerStatePolicy::All;
    }
    let mut hs = HashSet::new();
    for s in states {
        match s {
            A::Running => {
                hs.insert(ContainerLifecycleBucket::Running);
            }
            A::Waiting => {
                hs.insert(ContainerLifecycleBucket::Waiting);
            }
            A::Terminated => {
                hs.insert(ContainerLifecycleBucket::Terminated);
            }
            A::All => {}
        }
    }
    ContainerStatePolicy::Subset(hs)
}

impl Cli {
    /// Build a [`CoreRunConfig`] from parsed CLI flags. Does not run the pipeline.
    ///
    /// Call [`Cli::validate`] first so flags are checked. If `validate` is skipped, `--since`
    /// parsing may still fail here with the same error strings as validation.
    pub fn core_run_config(&self, root_token: CancellationToken) -> Result<CoreRunConfig, String> {
        let since = self.since.as_deref().map(parse_since).transpose()?;
        let since_time = self
            .since_time
            .as_deref()
            .map(rustern_core::source::pod_log::parse_since_time)
            .transpose()?;

        let timestamp_style = match self.timestamps {
            None => TimestampStyle::Omit,
            Some(TimestampArg::Omit) => TimestampStyle::Omit,
            Some(TimestampArg::Default) => TimestampStyle::Rfc3339,
            Some(TimestampArg::Short) => TimestampStyle::SternShort,
            Some(TimestampArg::Epoch) => TimestampStyle::EpochSeconds,
        };

        let timestamp_zone = match self.timezone.as_deref() {
            Some(z) => TimestampZone::parse_arg(z)?,
            None => TimestampZone::Local,
        };

        let resolved_max = self
            .max_log_requests
            .unwrap_or(if self.follow() { 50 } else { 5 });

        let output_and_formatter = match self.format {
            FormatArg::Default => (
                OutputMode::Default,
                FormatterChoice::Default {
                    timestamp_style,
                    timestamp_zone,
                    color_enabled: match self.color {
                        ColorArg::Never => false,
                        ColorArg::Always => true,
                        ColorArg::Auto => io::stdout().is_terminal(),
                    },
                    pod_colors: resolved_pod_colors(self),
                    container_colors: resolved_container_colors(self),
                },
            ),
            FormatArg::Json => (OutputMode::Json, FormatterChoice::Json),
            FormatArg::ExtJson => (
                OutputMode::ExtJson,
                FormatterChoice::ExtJson {
                    all_namespaces: self.all_namespaces,
                },
            ),
            FormatArg::PpExtJson => (
                OutputMode::PpExtJson,
                FormatterChoice::PpExtJson {
                    all_namespaces: self.all_namespaces,
                },
            ),
            FormatArg::Raw => (OutputMode::Raw, FormatterChoice::Raw),
        };

        let namespaces = if self.all_namespaces {
            Vec::new()
        } else {
            normalized_namespaces(self)
        };

        Ok(CoreRunConfig {
            context: self.context_selector(),
            query: resolved_pod_query(self)?,
            namespaces,
            all_namespaces: self.all_namespaces,
            selector: self.selector.clone(),
            field_selector: self.field_selector.clone(),
            node: self.node.clone(),
            exclude_pod: self.exclude_pod.clone(),
            container: self.container.clone(),
            exclude_container: self.exclude_container.clone(),
            container_discovery: ContainerDiscoverOpts {
                include_init_containers: resolved_init_include(self),
                include_ephemeral_containers: resolved_ephemeral_include(self),
                state_policy: resolved_container_state_policy(&self.container_states),
            },
            pod_condition: self
                .condition
                .as_deref()
                .map(parse_pod_condition)
                .transpose()
                .map_err(|e| e.to_string())?,
            follow: self.follow(),
            tail: self.tail,
            since,
            since_time,
            previous: self.previous,
            include: self.include.clone(),
            exclude: self.exclude.clone(),
            highlight: self.highlight.clone(),
            only_log_lines: self.only_log_lines,
            filter_on: match self.filter_on {
                FilterOnArg::Original => FilterOn::Original,
                FilterOnArg::Transformed => FilterOn::Transformed,
            },
            json_query: self.json_query.clone(),
            json_query_mode: match self.jq_mode {
                JqModeArg::Filter => QueryMode::Filter,
                JqModeArg::Replace => QueryMode::Replace,
                JqModeArg::Append => QueryMode::Append,
            },
            level_key: self.level_key.clone(),
            output: output_and_formatter.0,
            formatter: output_and_formatter.1,
            diff_container: self.diff_container,
            fwd: RuntimeFwdConfig {
                buffer_size: self.buffer_size,
                lossy: self.lossy,
                max_log_requests: resolved_max,
            },
            root_token,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    use clap::Parser;
    use rustern_core::FilterOn;
    use rustern_core::QueryMode;
    use rustern_core::discovery::pod_watcher::{ContainerLifecycleBucket, ContainerStatePolicy};

    #[test]
    fn maps_container_discovery_exclude_and_state() {
        let cli = Cli::try_parse_from([
            "rstn",
            "-E",
            "a",
            "-E",
            "b",
            "--no-init-containers",
            "--container-state",
            "running",
            "q",
        ])
        .unwrap();
        cli.validate().unwrap();
        let cfg = cli.core_run_config(CancellationToken::new()).unwrap();
        assert_eq!(cfg.exclude_container, vec!["a", "b"]);
        assert!(!cfg.container_discovery.include_init_containers);
        assert!(cfg.container_discovery.include_ephemeral_containers);
        let ContainerStatePolicy::Subset(ref hs) = cfg.container_discovery.state_policy else {
            panic!("expected subset policy");
        };
        assert!(hs.len() == 1 && hs.contains(&ContainerLifecycleBucket::Running));

        let cli_default = Cli::try_parse_from(["rstn", "q"]).unwrap();
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
        let cli = Cli::try_parse_from(["rstn", "--no-ephemeral-containers", "q"]).unwrap();
        cli.validate().unwrap();
        let cfg = cli.core_run_config(CancellationToken::new()).unwrap();
        assert!(!cfg.container_discovery.include_ephemeral_containers);
    }

    #[test]
    fn maps_container_state_all_precedence() {
        let cli = Cli::try_parse_from(["rstn", "--container-state", "running,all", "q"]).unwrap();
        cli.validate().unwrap();
        let cfg = cli.core_run_config(CancellationToken::new()).unwrap();
        assert!(matches!(
            cfg.container_discovery.state_policy,
            ContainerStatePolicy::All
        ));
    }

    #[test]
    fn maps_minimal_cli_to_core_config() {
        let cli = Cli::try_parse_from(["rstn", "myapp.*"]).unwrap();
        cli.validate().unwrap();
        let cfg = cli.core_run_config(CancellationToken::new()).unwrap();
        assert_eq!(cfg.query, "myapp.*");
        assert!(cfg.follow);
        assert!(!cfg.all_namespaces);
        assert_eq!(cfg.namespaces, vec!["default"]);
        assert_eq!(cfg.container, ".*");
        assert!(matches!(cfg.output, OutputMode::Default));
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
        let cli = Cli::try_parse_from([
            "rstn",
            "--since-time",
            "2024-03-15T10:30:45Z",
            "--previous",
            "q",
        ])
        .unwrap();
        cli.validate().unwrap();
        let cfg = cli.core_run_config(CancellationToken::new()).unwrap();
        assert!(cfg.since_time.is_some());
        assert!(cfg.since.is_none());
        assert!(cfg.previous);
    }

    #[test]
    fn stern_aligned_default_max_when_no_follow() {
        let cli = Cli::try_parse_from(["rstn", "--no-follow", "q"]).unwrap();
        cli.validate().unwrap();
        let cfg = cli.core_run_config(CancellationToken::new()).unwrap();
        assert!(!cfg.follow);
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
        assert!(!cfg.follow);
        assert!(matches!(cfg.output, OutputMode::Json));
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
        let cli = Cli::try_parse_from([
            "rstn",
            "--kubeconfig",
            "/etc/kube",
            "--context",
            "prod",
            "x",
        ])
        .unwrap();
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
        let cli = Cli::try_parse_from([
            "rstn",
            "--filter-on",
            "transformed",
            "--jq-mode",
            "append",
            "--jq",
            ".msg",
            "q",
        ])
        .unwrap();
        cli.validate().unwrap();
        let cfg = cli.core_run_config(CancellationToken::new()).unwrap();
        assert_eq!(cfg.filter_on, FilterOn::Transformed);
        assert_eq!(cfg.json_query_mode, QueryMode::Append);
        assert_eq!(cfg.json_query.as_deref(), Some(".msg"));
    }

    #[test]
    fn omits_timestamps_by_default() {
        let cli = Cli::try_parse_from(["rstn", "q"]).unwrap();
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
        let cli = Cli::try_parse_from(["rstn", "q", "-t"]).unwrap();
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
        let cli =
            Cli::try_parse_from(["rstn", "--timestamps=short", "--timezone", "UTC", "q"]).unwrap();
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
    fn maps_since_duration_to_seconds() {
        let cli = Cli::try_parse_from(["rstn", "--since", "5m", "q"]).unwrap();
        cli.validate().unwrap();
        let cfg = cli.core_run_config(CancellationToken::new()).unwrap();
        assert_eq!(cfg.since, Some(300));
    }

    #[test]
    fn core_run_config_errors_on_invalid_since_without_validate() {
        let cli = Cli::try_parse_from(["rstn", "--since", "not-a-time", "q"]).unwrap();
        assert!(cli.core_run_config(CancellationToken::new()).is_err());
    }

    #[test]
    fn maps_color_auto_matches_stdout_tty() {
        let cli = Cli::try_parse_from(["rstn", "q"]).unwrap();
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
        let cli = Cli::try_parse_from(["rstn", "--color", "always", "q"]).unwrap();
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
        let cli = Cli::try_parse_from([
            "rstn",
            "--field-selector",
            "status.phase=Running",
            "--node",
            "worker-1",
            "--exclude-pod",
            "junk-.*",
            ".*",
        ])
        .unwrap();
        cli.validate().unwrap();
        let cfg = cli.core_run_config(CancellationToken::new()).unwrap();
        assert_eq!(cfg.field_selector.as_deref(), Some("status.phase=Running"));
        assert_eq!(cfg.node.as_deref(), Some("worker-1"));
        assert_eq!(cfg.exclude_pod, vec!["junk-.*".to_string()]);
    }

    #[test]
    fn maps_color_never_disables_color() {
        let cli = Cli::try_parse_from(["rstn", "--color", "never", "q"]).unwrap();
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
        let cli = Cli::try_parse_from([
            "rstn",
            "--no-pod-colors",
            "--container-colors",
            "--diff-container",
            "q",
        ])
        .unwrap();
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
        let cli = Cli::try_parse_from(["rstn", "--no-container-colors", "q"]).unwrap();
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
        let cli =
            Cli::try_parse_from(["rstn", "--no-pod-colors", "--pod-colors=true", "q"]).unwrap();
        assert!(cli.validate().is_err());
    }

    #[test]
    fn maps_highlight_and_only_log_lines() {
        let cli = Cli::try_parse_from([
            "rstn",
            "--no-follow",
            "-H",
            "panic",
            "--only-log-lines",
            ".",
        ])
        .unwrap();
        cli.validate().unwrap();
        let cfg = cli.core_run_config(CancellationToken::new()).unwrap();
        assert_eq!(cfg.highlight, vec!["panic".to_string()]);
        assert!(cfg.only_log_lines);
        assert!(cfg.include.is_empty());
    }

    #[test]
    fn maps_condition_with_no_follow() {
        let cli =
            Cli::try_parse_from(["rstn", "--no-follow", "--condition=ready=false", "q"]).unwrap();
        cli.validate().unwrap();
        let cfg = cli.core_run_config(CancellationToken::new()).unwrap();
        let cond = cfg.pod_condition.as_ref().unwrap();
        assert_eq!(cond.type_name, "ready");
        assert_eq!(cond.status, "False");
    }

    #[test]
    fn maps_implicit_query_with_label_selector() {
        let cli = Cli::try_parse_from(["rstn", "-l", "app=myapp"]).unwrap();
        cli.validate().unwrap();
        let cfg = cli.core_run_config(CancellationToken::new()).unwrap();
        assert_eq!(cfg.query, ".*");
        assert_eq!(cfg.selector.as_deref(), Some("app=myapp"));
    }

    #[test]
    fn maps_implicit_query_with_field_selector() {
        let cli = Cli::try_parse_from(["rstn", "--field-selector", "metadata.name=foo"]).unwrap();
        cli.validate().unwrap();
        let cfg = cli.core_run_config(CancellationToken::new()).unwrap();
        assert_eq!(cfg.query, ".*");
        assert_eq!(cfg.field_selector.as_deref(), Some("metadata.name=foo"));
    }

    #[test]
    fn rejects_condition_with_follow_without_tail_zero() {
        let cli = Cli::try_parse_from(["rstn", "-f", "--condition=ready", "q"]).unwrap();
        assert!(cli.validate().is_err());
    }
}
