//! Map [`crate::cli::Cli`] into [`rustern_core::CoreRunConfig`].

use tokio_util::sync::CancellationToken;

use rustern_core::{CoreRunConfig, FormatterChoice, OutputMode, RuntimeFwdConfig};
use rustern_core::{FilterOn, QueryMode};

use crate::cli::{Cli, FilterOnArg, FormatArg, JqModeArg, parse_since};

impl Cli {
    /// Build a [`CoreRunConfig`] from parsed CLI flags. Does not run the pipeline.
    ///
    /// Call [`Cli::validate`] first so numeric bounds are checked.
    #[must_use]
    pub fn core_run_config(&self, root_token: CancellationToken) -> CoreRunConfig {
        let output_and_formatter = match self.format {
            FormatArg::Default => (
                OutputMode::Default,
                FormatterChoice::Default {
                    show_timestamps: self.timestamps,
                    color_enabled: self.color,
                },
            ),
            FormatArg::Json => (OutputMode::Json, FormatterChoice::Json),
            FormatArg::Raw => (OutputMode::Raw, FormatterChoice::Raw),
        };

        CoreRunConfig {
            context: self.context_selector(),
            query: self.query.clone(),
            namespace: self.namespace.clone(),
            all_namespaces: self.all_namespaces,
            selector: self.selector.clone(),
            container: self.container.clone(),
            exclude_container: self.exclude_container.clone(),
            follow: self.follow(),
            tail: self.tail,
            since: self
                .since
                .as_deref()
                .map(|s| {
                    parse_since(s).expect("call Cli::validate before core_run_config")
                }),
            include: self.include.clone(),
            exclude: self.exclude.clone(),
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
            fwd: RuntimeFwdConfig {
                buffer_size: self.buffer_size,
                lossy: self.lossy,
                max_log_requests: self.max_log_requests,
            },
            root_token,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    use clap::Parser;
    use rustern_core::FilterOn;
    use rustern_core::QueryMode;

    #[test]
    fn maps_minimal_cli_to_core_config() {
        let cli = Cli::try_parse_from(["rstn", "myapp.*"]).unwrap();
        cli.validate().unwrap();
        let cfg = cli.core_run_config(CancellationToken::new());
        assert_eq!(cfg.query, "myapp.*");
        assert!(cfg.follow);
        assert!(!cfg.all_namespaces);
        assert_eq!(cfg.namespace, None);
        assert_eq!(cfg.container, ".*");
        assert!(matches!(cfg.output, OutputMode::Default));
        assert!(matches!(
            cfg.formatter,
            FormatterChoice::Default {
                show_timestamps: true,
                color_enabled: true,
            }
        ));
        assert_eq!(cfg.fwd.buffer_size, 4096);
        assert_eq!(cfg.fwd.max_log_requests, 32);
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
        let cfg = cli.core_run_config(CancellationToken::new());
        assert_eq!(cfg.namespace.as_deref(), Some("kube-system"));
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
        let cfg = cli.core_run_config(CancellationToken::new());
        assert!(cfg.all_namespaces);
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
        let cfg = cli.core_run_config(CancellationToken::new());
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
        let cfg = cli.core_run_config(CancellationToken::new());
        assert_eq!(cfg.filter_on, FilterOn::Transformed);
        assert_eq!(cfg.json_query_mode, QueryMode::Append);
        assert_eq!(cfg.json_query.as_deref(), Some(".msg"));
    }

    #[test]
    fn maps_since_duration_to_seconds() {
        let cli = Cli::try_parse_from(["rstn", "--since", "5m", "q"]).unwrap();
        cli.validate().unwrap();
        let cfg = cli.core_run_config(CancellationToken::new());
        assert_eq!(cfg.since, Some(300));
    }
}
