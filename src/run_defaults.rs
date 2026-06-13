//! Thin CLI → core mapper for pod query and namespace resolution.

use rustern_core::discovery::run_resolution::{
    RunResolutionError, RunResolutionInput, RunResolutionOutput, resolve_run_input,
    resolve_run_namespaces, resolve_run_query,
};

use crate::cli::Cli;

fn run_resolution_input(cli: &Cli) -> RunResolutionInput<'_> {
    RunResolutionInput {
        query: cli.query.as_deref(),
        selector: cli.selector.as_deref(),
        field_selector: cli.field_selector.as_deref(),
        node: cli.node.as_deref(),
        all_namespaces: cli.all_namespaces,
        namespace_flags: &cli.namespaces,
    }
}

fn map_run_resolution_error(err: RunResolutionError) -> String {
    err.to_string()
}

/// Resolve pod query, namespaces, and validation in one call.
pub fn resolved_run(cli: &Cli) -> Result<RunResolutionOutput, String> {
    resolve_run_input(&run_resolution_input(cli), &cli.context_selector())
        .map_err(map_run_resolution_error)
}

/// Resolve the pod query positional, applying stern-like defaults.
pub fn resolved_pod_query(cli: &Cli) -> Result<String, String> {
    resolve_run_query(&run_resolution_input(cli)).map_err(map_run_resolution_error)
}

/// Resolve namespace scope: `-A` → empty; explicit `-n` → deduped list; else active context namespace.
pub fn resolved_namespaces(cli: &Cli) -> Result<Vec<String>, String> {
    resolve_run_namespaces(&run_resolution_input(cli), &cli.context_selector())
        .map_err(map_run_resolution_error)
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    use crate::cli::Cli;

    #[test]
    fn mapper_preserves_cli_fields_in_resolution_input() {
        let cli = Cli::try_parse_from(["rstn", "-l", "app=foo", "-n", "ns1", "myapp.*"]).unwrap();
        let out = resolved_run(&cli).unwrap();
        assert_eq!(out.resolved_query(), "myapp.*");
        assert_eq!(out.resolved_namespaces(), &["ns1".to_string()]);
    }

    #[test]
    fn mapper_delegates_implicit_query_to_core() {
        let cli = Cli::try_parse_from(["rstn", "-n", "default", "-l", "app=foo"]).unwrap();
        let out = resolved_run(&cli).unwrap();
        assert_eq!(out.resolved_query(), ".*");
        assert!(out.validation.implicit_query_from_selector);
    }
}
