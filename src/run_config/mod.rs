//! Map [`crate::cli::Cli`] into [`rustern_core::CoreRunConfig`].

use std::collections::HashSet;
use std::io::{self, IsTerminal};

use tokio_util::sync::CancellationToken;

use rustern_core::discovery::pod_condition::parse_pod_condition;
use rustern_core::discovery::pod_watcher::{
    ContainerDiscoverOpts, ContainerLifecycleBucket, ContainerStatePolicy,
};
use rustern_core::{
    CoreRunConfig, FormatterChoice, OutputMode, PodLogRequest, RuntimeFwdConfig, RuntimeStatsConfig,
};
use rustern_core::{FilterOn, QueryMode, TimestampStyle, TimestampZone};

use crate::cli::{
    Cli, ColorArg, ContainerStateArg, FilterOnArg, FormatArg, JqModeArg, TimestampArg, parse_since,
};
use crate::run_defaults::resolved_run;

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

        let resolution = resolved_run(self)?;
        let all_namespaces = resolution.all_namespaces();

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
                FormatterChoice::ExtJson { all_namespaces },
            ),
            FormatArg::PpExtJson => (
                OutputMode::PpExtJson,
                FormatterChoice::PpExtJson { all_namespaces },
            ),
            FormatArg::Raw => (OutputMode::Raw, FormatterChoice::Raw),
        };

        let (query, namespaces, all_namespaces) = resolution.into_resolved();

        Ok(CoreRunConfig {
            context: self.context_selector(),
            query,
            namespaces,
            all_namespaces,
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
            pod_log: PodLogRequest {
                follow: self.follow(),
                tail: self.tail,
                since_seconds: since,
                since_time,
                previous: self.previous,
            },
            cursor_reconnect: self.cursor_reconnect && self.follow(),
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
            exit_on: self.exit_on.clone(),
            exit_on_level: self
                .exit_on_level
                .as_deref()
                .map(rustern_core::pipeline::ExitOnLevel::parse)
                .transpose()?,
            output: output_and_formatter.0,
            formatter: output_and_formatter.1,
            diff_container: self.diff_container,
            fwd: RuntimeFwdConfig {
                buffer_size: self.buffer_size,
                lossy: self.lossy,
                mux_policy: rustern_core::runtime::BackpressurePolicy::from_lossy(self.lossy),
                stats: self.stats.then_some(RuntimeStatsConfig {
                    interval: self.stats_interval,
                }),
                max_log_requests: resolved_max,
            },
            root_token,
        })
    }
}

#[cfg(test)]
mod tests;
