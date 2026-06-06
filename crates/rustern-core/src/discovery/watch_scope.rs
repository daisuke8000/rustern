//! Watch scope resolution: pod query and namespace scope before list/watch planning.

use super::context::{ContextError, ContextSelector, default_namespace, resolve_kubeconfig};

const IMPLICIT_POD_QUERY: &str = ".*";

/// CLI-agnostic inputs for watch scope resolution.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WatchScopeInput<'a> {
    pub query: Option<&'a str>,
    pub selector: Option<&'a str>,
    pub field_selector: Option<&'a str>,
    pub all_namespaces: bool,
    pub namespace_flags: &'a [String],
}

/// Resolved watch scope used by run config and [`super::pod_list::PodWatchPlan`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WatchScopeResolved {
    pub query: String,
    pub namespaces: Vec<String>,
    pub all_namespaces: bool,
}

#[derive(Debug, thiserror::Error)]
pub enum WatchScopeError {
    #[error("pod query (QUERY) is required unless `-l` or `--field-selector` is set")]
    QueryRequired,
    #[error(transparent)]
    Context(#[from] ContextError),
}

/// Resolve the pod query positional, applying stern-like defaults.
///
/// When `-l` / `--selector` or `--field-selector` is set, an omitted QUERY becomes `.*`.
/// An explicit `.` with a label selector is preserved for runner compat (see `PodWatchPlan::build`).
/// An explicit `.` with only `--field-selector` normalizes to `.*` for stern-like wildcard scope.
pub fn resolve_pod_query(input: &WatchScopeInput<'_>) -> Result<String, WatchScopeError> {
    if let Some(q) = input.query {
        if q == "." && input.selector.is_none() && input.field_selector.is_some() {
            return Ok(IMPLICIT_POD_QUERY.to_string());
        }
        return Ok(q.to_string());
    }
    if input.selector.is_some() || input.field_selector.is_some() {
        return Ok(IMPLICIT_POD_QUERY.to_string());
    }
    Err(WatchScopeError::QueryRequired)
}

fn deduped_explicit_namespaces(namespace_flags: &[String]) -> Vec<String> {
    let mut out = Vec::new();
    for part in namespace_flags {
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
pub fn resolve_namespaces(
    input: &WatchScopeInput<'_>,
    context: &ContextSelector,
) -> Result<Vec<String>, WatchScopeError> {
    if input.all_namespaces {
        return Ok(Vec::new());
    }
    let explicit = deduped_explicit_namespaces(input.namespace_flags);
    if !explicit.is_empty() {
        return Ok(explicit);
    }
    let kubeconfig = resolve_kubeconfig(context)?;
    let ns = default_namespace(&kubeconfig, context)?;
    Ok(vec![ns])
}

/// Resolve pod query and namespace scope together.
pub fn resolve_watch_scope(
    input: &WatchScopeInput<'_>,
    context: &ContextSelector,
) -> Result<WatchScopeResolved, WatchScopeError> {
    Ok(WatchScopeResolved {
        query: resolve_pod_query(input)?,
        all_namespaces: input.all_namespaces,
        namespaces: resolve_namespaces(input, context)?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn input<'a>(
        query: Option<&'a str>,
        selector: Option<&'a str>,
        field_selector: Option<&'a str>,
        all_namespaces: bool,
        namespace_flags: &'a [String],
    ) -> WatchScopeInput<'a> {
        WatchScopeInput {
            query,
            selector,
            field_selector,
            all_namespaces,
            namespace_flags,
        }
    }

    fn write_kubeconfig(content: &str) -> tempfile::NamedTempFile {
        let mut f = tempfile::NamedTempFile::new().unwrap();
        f.write_all(content.as_bytes()).unwrap();
        f
    }

    fn context_with_kubeconfig(path: &std::path::Path) -> ContextSelector {
        ContextSelector {
            kubeconfig_path: Some(path.to_path_buf()),
            context_name: None,
        }
    }

    #[test]
    fn explicit_query_is_preserved() {
        let ns: [String; 0] = [];
        let i = input(Some("myapp.*"), None, None, false, &ns);
        assert_eq!(resolve_pod_query(&i).unwrap(), "myapp.*");
    }

    #[test]
    fn label_selector_implies_wildcard_query() {
        let ns: [String; 0] = [];
        let i = input(None, Some("app=foo"), None, false, &ns);
        assert_eq!(resolve_pod_query(&i).unwrap(), ".*");
    }

    #[test]
    fn field_selector_implies_wildcard_query() {
        let ns: [String; 0] = [];
        let i = input(None, None, Some("metadata.name=foo"), false, &ns);
        assert_eq!(resolve_pod_query(&i).unwrap(), ".*");
    }

    #[test]
    fn dot_sentinel_with_label_selector_is_preserved_for_runner_compat() {
        let ns: [String; 0] = [];
        let i = input(Some("."), Some("app=foo"), None, false, &ns);
        assert_eq!(resolve_pod_query(&i).unwrap(), ".");
    }

    #[test]
    fn dot_sentinel_with_field_selector_normalizes_to_wildcard() {
        let ns: [String; 0] = [];
        let i = input(Some("."), None, Some("metadata.name=foo"), false, &ns);
        assert_eq!(resolve_pod_query(&i).unwrap(), ".*");
    }

    #[test]
    fn missing_query_without_selector_fails() {
        let ns: [String; 0] = [];
        let i = input(None, None, None, false, &ns);
        assert!(matches!(
            resolve_pod_query(&i),
            Err(WatchScopeError::QueryRequired)
        ));
    }

    #[test]
    fn all_namespaces_yields_empty_list() {
        let ns: [String; 0] = [];
        let i = input(Some("q"), None, None, true, &ns);
        let ctx = ContextSelector::default();
        assert!(resolve_namespaces(&i, &ctx).unwrap().is_empty());
    }

    #[test]
    fn explicit_namespace_is_deduped() {
        let flags = vec!["a,b".to_string(), "a".to_string()];
        let i = input(Some("q"), None, None, false, &flags);
        let ctx = ContextSelector::default();
        assert_eq!(resolve_namespaces(&i, &ctx).unwrap(), vec!["a", "b"]);
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
        let ns: [String; 0] = [];
        let i = input(Some("q"), None, None, false, &ns);
        let ctx = context_with_kubeconfig(f.path());
        assert_eq!(resolve_namespaces(&i, &ctx).unwrap(), vec!["team-ns"]);
    }

    #[test]
    fn kubeconfig_read_failure_surfaces_in_resolved_namespaces() {
        let ns: [String; 0] = [];
        let i = input(Some("q"), None, None, false, &ns);
        let ctx = ContextSelector {
            kubeconfig_path: Some("/nonexistent/rustern-kubeconfig-test".into()),
            context_name: None,
        };
        assert!(resolve_namespaces(&i, &ctx).is_err());
    }

    #[test]
    fn resolve_watch_scope_combines_query_and_namespaces() {
        let flags = vec!["ns1".to_string()];
        let i = input(Some("deploy/.*"), None, None, false, &flags);
        let ctx = ContextSelector::default();
        let resolved = resolve_watch_scope(&i, &ctx).unwrap();
        assert_eq!(
            resolved,
            WatchScopeResolved {
                query: "deploy/.*".to_string(),
                namespaces: vec!["ns1".to_string()],
                all_namespaces: false,
            }
        );
    }
}
