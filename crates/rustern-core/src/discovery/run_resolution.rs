//! Unified run resolution: pod query and namespace scope before [`CoreRunConfig`](crate::runtime::CoreRunConfig).
//!
//! ## Recommended usage
//!
//! Build a [`RunResolutionInput`] from CLI flags (or test fixtures), then call [`resolve_run_input`]
//! with a [`ContextSelector`](super::context::ContextSelector):
//!
//! ```
//! use rustern_core::discovery::context::ContextSelector;
//! use rustern_core::discovery::run_resolution::{RunResolutionInput, resolve_run_input};
//!
//! let input = RunResolutionInput {
//!     query: None,
//!     selector: Some("app=foo"),
//!     field_selector: None,
//!     node: None,
//!     all_namespaces: false,
//!     namespace_flags: &["default".to_string()],
//! };
//! let ctx = ContextSelector::default();
//! let resolved = resolve_run_input(&input, &ctx).unwrap();
//! assert_eq!(resolved.resolved_query(), ".*");
//! assert_eq!(resolved.resolved_namespaces(), &["default".to_string()]);
//! assert!(resolved.validation().implicit_query_from_selector);
//! ```
//!
//! `selector`, `field_selector`, and `node` are not resolved here; they pass through CLI →
//! [`CoreRunConfig`] unchanged into [`super::pod_list::PodWatchPlanConfig`].
//!
//! ## Data flow
//!
//! ```text
//! RunResolutionInput → resolve_run_input → RunResolutionOutput
//!   → CoreRunConfig { query, namespaces, all_namespaces }
//!   → PodWatchPlanConfig { query, namespaces, all_namespaces, selector, field_selector, node }
//! ```

use super::context::{ContextError, ContextSelector, default_namespace, resolve_kubeconfig};

const IMPLICIT_POD_QUERY: &str = ".*";

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
    validation: RunResolutionValidation,
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

    pub fn validation(&self) -> RunResolutionValidation {
        self.validation
    }

    pub fn into_resolved(self) -> (String, Vec<String>, bool) {
        (
            self.resolved_query,
            self.resolved_namespaces,
            self.all_namespaces,
        )
    }
}

#[derive(Debug, thiserror::Error)]
pub enum RunResolutionError {
    #[error("pod query (QUERY) is required unless `-l` or `--field-selector` is set")]
    QueryRequired,
    #[error("namespace flag value must not be empty")]
    InvalidNamespaceFlags,
    #[error(transparent)]
    Context(#[from] ContextError),
}

fn implicit_query_from_selector(input: &RunResolutionInput<'_>) -> bool {
    input.query.is_none() && (input.selector.is_some() || input.field_selector.is_some())
}

/// Resolve the pod query positional, applying stern-like defaults.
///
/// When `-l` / `--selector` or `--field-selector` is set, an omitted QUERY becomes `.*`.
/// An explicit `.` with a label selector is preserved for runner compat (see `PodWatchPlan::build`).
/// An explicit `.` with only `--field-selector` normalizes to `.*` for stern-like wildcard scope.
fn resolve_pod_query(input: &RunResolutionInput<'_>) -> Result<String, RunResolutionError> {
    if let Some(q) = input.query {
        if q == "." && input.selector.is_none() && input.field_selector.is_some() {
            return Ok(IMPLICIT_POD_QUERY.to_string());
        }
        return Ok(q.to_string());
    }
    if input.selector.is_some() || input.field_selector.is_some() {
        return Ok(IMPLICIT_POD_QUERY.to_string());
    }
    Err(RunResolutionError::QueryRequired)
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
fn resolve_namespaces(
    input: &RunResolutionInput<'_>,
    context: &ContextSelector,
) -> Result<Vec<String>, RunResolutionError> {
    if input.all_namespaces {
        return Ok(Vec::new());
    }
    let explicit = deduped_explicit_namespaces(input.namespace_flags);
    if !explicit.is_empty() {
        return Ok(explicit);
    }
    if !input.namespace_flags.is_empty() {
        return Err(RunResolutionError::InvalidNamespaceFlags);
    }
    let kubeconfig = resolve_kubeconfig(context)?;
    let ns = default_namespace(&kubeconfig, context)?;
    Ok(vec![ns])
}

/// Resolve pod query and namespace scope together (stern-compatible defaults).
pub fn resolve_run_input(
    input: &RunResolutionInput<'_>,
    context: &ContextSelector,
) -> Result<RunResolutionOutput, RunResolutionError> {
    let implicit_query = implicit_query_from_selector(input);
    Ok(RunResolutionOutput {
        resolved_query: resolve_pod_query(input)?,
        resolved_namespaces: resolve_namespaces(input, context)?,
        all_namespaces: input.all_namespaces,
        validation: RunResolutionValidation {
            implicit_query_from_selector: implicit_query,
        },
    })
}

/// Resolve only the pod query positional (stern-like defaults).
pub fn resolve_run_query(input: &RunResolutionInput<'_>) -> Result<String, RunResolutionError> {
    resolve_pod_query(input)
}

/// Resolve only namespace scope (`-A`, explicit `-n`, or kube context default).
pub fn resolve_run_namespaces(
    input: &RunResolutionInput<'_>,
    context: &ContextSelector,
) -> Result<Vec<String>, RunResolutionError> {
    resolve_namespaces(input, context)
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

    fn input_with_node<'a>(
        query: Option<&'a str>,
        node: Option<&'a str>,
        all_namespaces: bool,
        namespace_flags: &'a [String],
    ) -> RunResolutionInput<'a> {
        RunResolutionInput {
            query,
            selector: None,
            field_selector: None,
            node,
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
    fn pod_query_resolution_cases() {
        let empty_ns: [String; 0] = [];
        type Case = (
            Option<&'static str>,
            Option<&'static str>,
            Option<&'static str>,
            Result<&'static str, RunResolutionError>,
        );
        let cases: &[Case] = &[
            (Some("myapp.*"), None, None, Ok("myapp.*")),
            (None, Some("app=foo"), None, Ok(".*")),
            (None, None, Some("metadata.name=foo"), Ok(".*")),
            (Some("."), Some("app=foo"), None, Ok(".")),
            (Some("."), None, Some("metadata.name=foo"), Ok(".*")),
            (None, None, None, Err(RunResolutionError::QueryRequired)),
        ];

        for (query, selector, field_selector, expect) in cases {
            let i = input(*query, *selector, *field_selector, false, &empty_ns);
            match expect {
                Ok(want) => assert_eq!(resolve_run_query(&i).unwrap(), *want),
                Err(RunResolutionError::QueryRequired) => assert!(matches!(
                    resolve_run_query(&i),
                    Err(RunResolutionError::QueryRequired)
                )),
                Err(_) => panic!("unexpected error variant in test table"),
            }
        }
    }

    #[test]
    fn namespace_resolution_cases() {
        let empty_ns: [String; 0] = [];
        let ctx = ContextSelector::default();

        let i = input(Some("q"), None, None, true, &empty_ns);
        assert!(resolve_run_namespaces(&i, &ctx).unwrap().is_empty());

        let flags = vec!["a,b".to_string(), "a".to_string()];
        let i = input(Some("q"), None, None, false, &flags);
        assert_eq!(resolve_run_namespaces(&i, &ctx).unwrap(), vec!["a", "b"]);

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
        let i = input(Some("q"), None, None, false, &empty_ns);
        let ctx = context_with_kubeconfig(f.path());
        assert_eq!(resolve_run_namespaces(&i, &ctx).unwrap(), vec!["team-ns"]);

        let i = input(Some("q"), None, None, false, &empty_ns);
        let ctx = ContextSelector {
            kubeconfig_path: Some("/nonexistent/rustern-kubeconfig-test".into()),
            context_name: None,
        };
        assert!(resolve_run_namespaces(&i, &ctx).is_err());
    }

    #[test]
    fn explicit_empty_namespace_flags_return_invalid_namespace_flags() {
        let ctx = ContextSelector::default();
        for flags in [
            vec!["".to_string()],
            vec![",".to_string()],
            vec!["  ".to_string()],
            vec!["".to_string(), "  ".to_string()],
        ] {
            let i = input(Some("q"), None, None, false, &flags);
            assert!(matches!(
                resolve_run_namespaces(&i, &ctx),
                Err(RunResolutionError::InvalidNamespaceFlags)
            ));
        }
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
        assert!(!resolved.validation().implicit_query_from_selector);
    }

    #[test]
    fn implicit_query_sets_validation_flag() {
        let flags = vec!["default".to_string()];
        let i = input(None, Some("app=foo"), None, false, &flags);
        let ctx = ContextSelector::default();
        let resolved = resolve_run_input(&i, &ctx).unwrap();
        assert_eq!(resolved.resolved_query(), ".*");
        assert!(resolved.validation().implicit_query_from_selector);
    }

    #[test]
    fn resolve_run_input_all_namespaces_with_selector() {
        let ns: [String; 0] = [];
        let i = input(None, Some("app=foo"), None, true, &ns);
        let ctx = ContextSelector::default();
        let resolved = resolve_run_input(&i, &ctx).unwrap();
        assert_eq!(resolved.resolved_query(), ".*");
        assert!(resolved.resolved_namespaces().is_empty());
        assert!(resolved.all_namespaces());
        assert!(resolved.validation().implicit_query_from_selector);
    }

    #[test]
    fn resolve_run_input_multiple_namespaces_with_explicit_query() {
        let flags = vec!["ns1".to_string(), "ns2".to_string()];
        let i = input(Some("pod-.*"), None, None, false, &flags);
        let ctx = ContextSelector::default();
        let resolved = resolve_run_input(&i, &ctx).unwrap();
        assert_eq!(resolved.resolved_query(), "pod-.*");
        assert_eq!(
            resolved.resolved_namespaces(),
            &["ns1".to_string(), "ns2".to_string()]
        );
        assert!(!resolved.all_namespaces());
    }

    #[test]
    fn resolve_run_input_dot_sentinel_with_kubeconfig_namespace() {
        let kube = r#"
apiVersion: v1
kind: Config
current-context: ctx
contexts:
  - name: ctx
    context:
      cluster: c
      user: u
      namespace: ctx-ns
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
        let i = input(Some("."), Some("app=foo"), None, false, &ns);
        let ctx = context_with_kubeconfig(f.path());
        let resolved = resolve_run_input(&i, &ctx).unwrap();
        assert_eq!(resolved.resolved_query(), ".");
        assert_eq!(resolved.resolved_namespaces(), &["ctx-ns".to_string()]);
    }

    #[test]
    fn node_field_does_not_affect_resolution() {
        let flags = vec!["ns1".to_string()];
        let i = input_with_node(Some("q"), Some("worker-1"), false, &flags);
        assert_eq!(i.node, Some("worker-1"));
        let ctx = ContextSelector::default();
        let resolved = resolve_run_input(&i, &ctx).unwrap();
        assert_eq!(resolved.resolved_query(), "q");
        assert_eq!(resolved.resolved_namespaces(), &["ns1".to_string()]);
    }
}
