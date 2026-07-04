#[derive(Debug, Clone, Eq, PartialEq)]
pub enum Query {
    /// Pod name regex (stern-compatible).
    PodNameRegex(String),
    /// `kind/name` query mapped to a label selector.
    LabelSelector { kind: ResourceKind, name: String },
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum ResourceKind {
    Pod,
    ReplicationController,
    Service,
    DaemonSet,
    Deployment,
    ReplicaSet,
    StatefulSet,
    Job,
}

#[derive(Debug, thiserror::Error)]
pub enum QueryParseError {
    #[error("invalid regex: {0}")]
    Regex(#[from] regex::Error),
    #[error("{0}")]
    UserRegex(String),
    #[error("unknown resource kind: {0}")]
    UnknownKind(String),
}

pub fn parse_query(arg: &str) -> Result<Query, QueryParseError> {
    if let Some((kind_s, name)) = arg.split_once('/') {
        let kind = match kind_s {
            "pod" | "po" => ResourceKind::Pod,
            "replicationcontroller" | "rc" => ResourceKind::ReplicationController,
            "service" | "svc" => ResourceKind::Service,
            "daemonset" | "ds" => ResourceKind::DaemonSet,
            "deployment" | "deploy" => ResourceKind::Deployment,
            "replicaset" | "rs" => ResourceKind::ReplicaSet,
            "statefulset" | "sts" => ResourceKind::StatefulSet,
            "job" => ResourceKind::Job,
            other => return Err(QueryParseError::UnknownKind(other.to_string())),
        };
        Ok(Query::LabelSelector {
            kind,
            name: name.to_string(),
        })
    } else {
        crate::regex_limits::compile_user_regex("pod query", arg)
            .map_err(QueryParseError::UserRegex)?;
        Ok(Query::PodNameRegex(arg.to_string()))
    }
}

pub fn label_selector_for(kind: ResourceKind, name: &str) -> String {
    match kind {
        ResourceKind::Pod => format!("metadata.name={name}"),
        _ => format!("app={name}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn regex_query() {
        assert_eq!(
            parse_query("auth-.*").unwrap(),
            Query::PodNameRegex("auth-.*".into())
        );
    }

    #[test]
    fn deployment_short() {
        assert_eq!(
            parse_query("deploy/api").unwrap(),
            Query::LabelSelector {
                kind: ResourceKind::Deployment,
                name: "api".into()
            }
        );
    }

    #[test]
    fn stern_supported_kind_aliases_parse() {
        for (q, kind, name) in [
            ("pod/p", ResourceKind::Pod, "p"),
            ("po/p", ResourceKind::Pod, "p"),
            ("deploy/api", ResourceKind::Deployment, "api"),
            ("deployment/api", ResourceKind::Deployment, "api"),
            ("rs/api", ResourceKind::ReplicaSet, "api"),
            ("replicaset/api", ResourceKind::ReplicaSet, "api"),
            ("ds/api", ResourceKind::DaemonSet, "api"),
            ("daemonset/api", ResourceKind::DaemonSet, "api"),
            ("sts/api", ResourceKind::StatefulSet, "api"),
            ("statefulset/api", ResourceKind::StatefulSet, "api"),
            ("svc/api", ResourceKind::Service, "api"),
            ("service/api", ResourceKind::Service, "api"),
            ("rc/api", ResourceKind::ReplicationController, "api"),
            (
                "replicationcontroller/api",
                ResourceKind::ReplicationController,
                "api",
            ),
            ("job/batch", ResourceKind::Job, "batch"),
        ] {
            assert_eq!(
                parse_query(q).unwrap(),
                Query::LabelSelector {
                    kind,
                    name: name.into(),
                },
                "expected exact kind/name parse for {q}"
            );
        }
    }

    #[test]
    fn rejects_unknown_kind() {
        assert!(matches!(
            parse_query("foo/bar").unwrap_err(),
            QueryParseError::UnknownKind(_)
        ));
    }

    #[test]
    fn rejects_invalid_regex() {
        assert!(matches!(
            parse_query("(unclosed").unwrap_err(),
            QueryParseError::UserRegex(_)
        ));
    }

    #[test]
    fn fallback_deployment_maps_to_app_label() {
        assert_eq!(
            label_selector_for(ResourceKind::Deployment, "api"),
            "app=api"
        );
    }
}
