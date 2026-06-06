//! Shared CLI default resolution for pod query and namespace.

use rustern_core::discovery::watch_scope::{
    WatchScopeError, WatchScopeInput, resolve_namespaces, resolve_pod_query,
};

use crate::cli::Cli;

fn watch_scope_input(cli: &Cli) -> WatchScopeInput<'_> {
    WatchScopeInput {
        query: cli.query.as_deref(),
        selector: cli.selector.as_deref(),
        field_selector: cli.field_selector.as_deref(),
        all_namespaces: cli.all_namespaces,
        namespace_flags: &cli.namespaces,
    }
}

fn map_watch_scope_error(err: WatchScopeError) -> String {
    err.to_string()
}

/// Resolve the pod query positional, applying stern-like defaults.
///
/// When `-l` / `--selector` or `--field-selector` is set, an omitted QUERY becomes `.*`.
pub fn resolved_pod_query(cli: &Cli) -> Result<String, String> {
    resolve_pod_query(&watch_scope_input(cli)).map_err(map_watch_scope_error)
}

/// Resolve namespace scope: `-A` → empty; explicit `-n` → deduped list; else active context namespace.
pub fn resolved_namespaces(cli: &Cli) -> Result<Vec<String>, String> {
    resolve_namespaces(&watch_scope_input(cli), &cli.context_selector())
        .map_err(map_watch_scope_error)
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;
    use std::io::Write;

    use crate::cli::Cli;

    fn write_kubeconfig(content: &str) -> tempfile::NamedTempFile {
        let mut f = tempfile::NamedTempFile::new().unwrap();
        f.write_all(content.as_bytes()).unwrap();
        f
    }

    #[test]
    fn explicit_query_is_preserved() {
        let cli = Cli::try_parse_from(["rstn", "myapp.*"]).unwrap();
        assert_eq!(resolved_pod_query(&cli).unwrap(), "myapp.*");
    }

    #[test]
    fn label_selector_implies_wildcard_query() {
        let cli = Cli::try_parse_from(["rstn", "-l", "app=foo"]).unwrap();
        assert!(cli.query.is_none());
        assert_eq!(resolved_pod_query(&cli).unwrap(), ".*");
    }

    #[test]
    fn field_selector_implies_wildcard_query() {
        let cli = Cli::try_parse_from(["rstn", "--field-selector", "metadata.name=foo"]).unwrap();
        assert!(cli.query.is_none());
        assert_eq!(resolved_pod_query(&cli).unwrap(), ".*");
    }

    #[test]
    fn dot_sentinel_with_label_selector_is_preserved_for_runner_compat() {
        let cli = Cli::try_parse_from(["rstn", "-l", "app=foo", "."]).unwrap();
        assert_eq!(resolved_pod_query(&cli).unwrap(), ".");
    }

    #[test]
    fn dot_sentinel_with_field_selector_normalizes_to_wildcard() {
        let cli =
            Cli::try_parse_from(["rstn", "--field-selector", "metadata.name=foo", "."]).unwrap();
        assert_eq!(resolved_pod_query(&cli).unwrap(), ".*");
    }

    #[test]
    fn missing_query_without_selector_fails() {
        let cli = Cli::try_parse_from(["rstn"]).unwrap();
        assert!(resolved_pod_query(&cli).is_err());
    }

    #[test]
    fn all_namespaces_yields_empty_list() {
        let cli = Cli::try_parse_from(["rstn", "-A", "q"]).unwrap();
        assert!(resolved_namespaces(&cli).unwrap().is_empty());
    }

    #[test]
    fn explicit_namespace_is_deduped() {
        let cli = Cli::try_parse_from(["rstn", "-n", "a,b", "-n", "a", "q"]).unwrap();
        assert_eq!(resolved_namespaces(&cli).unwrap(), vec!["a", "b"]);
    }

    #[test]
    fn implicit_namespace_from_kubeconfig() {
        let kube = r#"
apiVersion: v1
kind: Config
current-context: ctx
contexts:
  - name: ctx
    context:
      cluster: c
      user: u
      namespace: team-ns
clusters:
  - name: c
    cluster:
      server: https://localhost
users:
  - name: u
    user: {}
"#;
        let f = write_kubeconfig(kube);
        let cli =
            Cli::try_parse_from(["rstn", "--kubeconfig", f.path().to_str().unwrap(), "q"]).unwrap();
        assert_eq!(resolved_namespaces(&cli).unwrap(), vec!["team-ns"]);
    }

    #[test]
    fn kubeconfig_read_failure_surfaces_in_resolved_namespaces() {
        let cli = Cli::try_parse_from([
            "rstn",
            "--kubeconfig",
            "/nonexistent/rustern-kubeconfig-test",
            "q",
        ])
        .unwrap();
        assert!(resolved_namespaces(&cli).is_err());
    }
}
