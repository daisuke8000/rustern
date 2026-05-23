//! Shared CLI default resolution for pod query and (later) namespace.

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

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    use crate::cli::Cli;

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
}
