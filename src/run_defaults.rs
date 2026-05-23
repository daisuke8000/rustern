//! Shared CLI default resolution for pod query and namespace.

use rustern_core::discovery::context::{default_namespace, resolve_kubeconfig};

use crate::cli::Cli;

const IMPLICIT_POD_QUERY: &str = ".*";

/// Resolve the pod query positional, applying stern-like defaults.
///
/// When `-l` / `--selector` or `--field-selector` is set, an omitted QUERY becomes `.*`.
pub fn resolved_pod_query(cli: &Cli) -> Result<String, String> {
    if let Some(q) = cli.query.as_deref() {
        return Ok(q.to_string());
    }
    if cli.selector.is_some() || cli.field_selector.is_some() {
        return Ok(IMPLICIT_POD_QUERY.to_string());
    }
    Err("pod query (QUERY) is required unless `-l` or `--field-selector` is set".into())
}

fn deduped_explicit_namespaces(cli: &Cli) -> Vec<String> {
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
    out
}

/// Resolve namespace scope: `-A` → empty; explicit `-n` → deduped list; else active context namespace.
pub fn resolved_namespaces(cli: &Cli) -> Result<Vec<String>, String> {
    if cli.all_namespaces {
        return Ok(Vec::new());
    }
    let explicit = deduped_explicit_namespaces(cli);
    if !explicit.is_empty() {
        return Ok(explicit);
    }
    let selector = cli.context_selector();
    let kubeconfig = resolve_kubeconfig(&selector).map_err(|e| e.to_string())?;
    let ns = default_namespace(&kubeconfig, &selector).map_err(|e| e.to_string())?;
    Ok(vec![ns])
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
