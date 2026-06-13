//! Unified run resolution: pod query and namespace scope before [`CoreRunConfig`](crate::runtime::CoreRunConfig).
//!
//! Prefer [`RunResolutionInput`] and [`resolve_run_input`] over [`super::watch_scope`] during
//! the migration period. `watch_scope` remains the internal implementation; names do not collide
//! because `watch_scope` exports `WatchScope*` while this module exports `RunResolution*`.
//!
//! ## Data flow
//!
//! ```text
//! RunResolutionInput → resolve_run_input → RunResolutionOutput
//!   → CoreRunConfig { query, namespaces, all_namespaces }
//!   → PodWatchPlanConfig { query, namespaces, all_namespaces, selector, field_selector, node }
//! ```
//!
//! `selector`, `field_selector`, and `node` are not resolved here; they pass through CLI →
//! [`CoreRunConfig`] unchanged into [`super::pod_list::PodWatchPlanConfig`].

use super::context::ContextSelector;
use super::watch_scope::{
    WatchScopeError, WatchScopeInput, WatchScopeResolved, resolve_namespaces, resolve_pod_query,
    resolve_watch_scope,
};

/// CLI-agnostic inputs for unified run resolution.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RunResolutionInput<'a> {
    pub query: Option<&'a str>,
    pub selector: Option<&'a str>,
    pub field_selector: Option<&'a str>,
    /// Optional node constraint; forwarded to [`super::pod_list::PodWatchPlanConfig::node`].
    pub node: Option<&'a str>,
    pub all_namespaces: bool,
    pub namespace_flags: &'a [String],
}

impl<'a> From<RunResolutionInput<'a>> for WatchScopeInput<'a> {
    fn from(input: RunResolutionInput<'a>) -> Self {
        WatchScopeInput {
            query: input.query,
            selector: input.selector,
            field_selector: input.field_selector,
            all_namespaces: input.all_namespaces,
            namespace_flags: input.namespace_flags,
        }
    }
}

impl<'a> From<&RunResolutionInput<'a>> for WatchScopeInput<'a> {
    fn from(input: &RunResolutionInput<'a>) -> Self {
        WatchScopeInput::from(*input)
    }
}

/// How query and namespace defaults were applied (diagnostics / future R2 gates).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RunResolutionValidation {
    /// Query was omitted and `-l` / `--field-selector` supplied the implicit `.*` default.
    pub implicit_query_from_selector: bool,
}

/// Resolved run scope consumed by [`crate::runtime::CoreRunConfig`] and watch planning.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunResolutionOutput {
    resolved_query: String,
    resolved_namespaces: Vec<String>,
    all_namespaces: bool,
    pub validation: RunResolutionValidation,
}

impl RunResolutionOutput {
    pub fn resolved_query(&self) -> &str {
        &self.resolved_query
    }

    pub fn resolved_namespaces(&self) -> &[String] {
        &self.resolved_namespaces
    }

    pub fn all_namespaces(&self) -> bool {
        self.all_namespaces
    }

    pub fn into_resolved(self) -> (String, Vec<String>, bool) {
        (
            self.resolved_query,
            self.resolved_namespaces,
            self.all_namespaces,
        )
    }

    /// Backward-compatible view for code still using [`WatchScopeResolved`].
    pub fn as_watch_scope_resolved(&self) -> WatchScopeResolved {
        WatchScopeResolved {
            query: self.resolved_query.clone(),
            namespaces: self.resolved_namespaces.clone(),
            all_namespaces: self.all_namespaces,
        }
    }
}

impl From<WatchScopeResolved> for RunResolutionOutput {
    /// Legacy conversion; validation flags are unavailable from [`WatchScopeResolved`] alone.
    ///
    /// `implicit_query_from_selector` is always `false` here. Use [`resolve_run_input`] when
    /// accurate validation metadata is required.
    fn from(resolved: WatchScopeResolved) -> Self {
        Self {
            resolved_query: resolved.query,
            resolved_namespaces: resolved.namespaces,
            all_namespaces: resolved.all_namespaces,
            validation: RunResolutionValidation {
                implicit_query_from_selector: false,
            },
        }
    }
}

pub type RunResolutionError = WatchScopeError;

fn implicit_query_from_selector(input: &RunResolutionInput<'_>) -> bool {
    input.query.is_none() && (input.selector.is_some() || input.field_selector.is_some())
}

/// Resolve pod query and namespace scope together (stern-compatible defaults).
pub fn resolve_run_input(
    input: &RunResolutionInput<'_>,
    context: &ContextSelector,
) -> Result<RunResolutionOutput, RunResolutionError> {
    let scope: WatchScopeInput<'_> = input.into();
    let implicit_query = implicit_query_from_selector(input);
    let resolved = resolve_watch_scope(&scope, context)?;
    Ok(RunResolutionOutput {
        resolved_query: resolved.query,
        resolved_namespaces: resolved.namespaces,
        all_namespaces: resolved.all_namespaces,
        validation: RunResolutionValidation {
            implicit_query_from_selector: implicit_query,
        },
    })
}

/// Resolve only the pod query positional (stern-like defaults).
pub fn resolve_run_query(input: &RunResolutionInput<'_>) -> Result<String, RunResolutionError> {
    resolve_pod_query(&WatchScopeInput::from(input))
}

/// Resolve only namespace scope (`-A`, explicit `-n`, or kube context default).
pub fn resolve_run_namespaces(
    input: &RunResolutionInput<'_>,
    context: &ContextSelector,
) -> Result<Vec<String>, RunResolutionError> {
    resolve_namespaces(&WatchScopeInput::from(input), context)
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
    ) -> RunResolutionInput<'a> {
        RunResolutionInput {
            query,
            selector,
            field_selector,
            node: None,
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
        assert_eq!(resolve_run_query(&i).unwrap(), "myapp.*");
    }

    #[test]
    fn label_selector_implies_wildcard_query() {
        let ns: [String; 0] = [];
        let i = input(None, Some("app=foo"), None, false, &ns);
        assert_eq!(resolve_run_query(&i).unwrap(), ".*");
    }

    #[test]
    fn field_selector_implies_wildcard_query() {
        let ns: [String; 0] = [];
        let i = input(None, None, Some("metadata.name=foo"), false, &ns);
        assert_eq!(resolve_run_query(&i).unwrap(), ".*");
    }

    #[test]
    fn dot_sentinel_with_label_selector_is_preserved_for_runner_compat() {
        let ns: [String; 0] = [];
        let i = input(Some("."), Some("app=foo"), None, false, &ns);
        assert_eq!(resolve_run_query(&i).unwrap(), ".");
    }

    #[test]
    fn dot_sentinel_with_field_selector_normalizes_to_wildcard() {
        let ns: [String; 0] = [];
        let i = input(Some("."), None, Some("metadata.name=foo"), false, &ns);
        assert_eq!(resolve_run_query(&i).unwrap(), ".*");
    }

    #[test]
    fn missing_query_without_selector_fails() {
        let ns: [String; 0] = [];
        let i = input(None, None, None, false, &ns);
        assert!(matches!(
            resolve_run_query(&i),
            Err(RunResolutionError::QueryRequired)
        ));
    }

    #[test]
    fn all_namespaces_yields_empty_list() {
        let ns: [String; 0] = [];
        let i = input(Some("q"), None, None, true, &ns);
        let ctx = ContextSelector::default();
        assert!(resolve_run_namespaces(&i, &ctx).unwrap().is_empty());
    }

    #[test]
    fn explicit_namespace_is_deduped() {
        let flags = vec!["a,b".to_string(), "a".to_string()];
        let i = input(Some("q"), None, None, false, &flags);
        let ctx = ContextSelector::default();
        assert_eq!(resolve_run_namespaces(&i, &ctx).unwrap(), vec!["a", "b"]);
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
        assert_eq!(resolve_run_namespaces(&i, &ctx).unwrap(), vec!["team-ns"]);
    }

    #[test]
    fn kubeconfig_read_failure_surfaces_in_resolved_namespaces() {
        let ns: [String; 0] = [];
        let i = input(Some("q"), None, None, false, &ns);
        let ctx = ContextSelector {
            kubeconfig_path: Some("/nonexistent/rustern-kubeconfig-test".into()),
            context_name: None,
        };
        assert!(resolve_run_namespaces(&i, &ctx).is_err());
    }

    #[test]
    fn resolve_run_input_combines_query_and_namespaces() {
        let flags = vec!["ns1".to_string()];
        let i = input(Some("deploy/.*"), None, None, false, &flags);
        let ctx = ContextSelector::default();
        let resolved = resolve_run_input(&i, &ctx).unwrap();
        assert_eq!(resolved.resolved_query(), "deploy/.*");
        assert_eq!(resolved.resolved_namespaces(), &["ns1".to_string()]);
        assert!(!resolved.all_namespaces());
        assert!(!resolved.validation.implicit_query_from_selector);
    }

    #[test]
    fn implicit_query_sets_validation_flag() {
        let flags = vec!["default".to_string()];
        let i = input(None, Some("app=foo"), None, false, &flags);
        let ctx = ContextSelector::default();
        let resolved = resolve_run_input(&i, &ctx).unwrap();
        assert_eq!(resolved.resolved_query(), ".*");
        assert!(resolved.validation.implicit_query_from_selector);
    }
}
