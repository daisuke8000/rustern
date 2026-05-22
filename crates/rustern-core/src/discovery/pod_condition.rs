//! Pod `status.conditions` filter (`stern --condition` parity).

use k8s_openapi::api::core::v1::Pod;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PodConditionFilter {
    pub type_name: String,
    pub status: String,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ParsePodConditionError {
    #[error("empty --condition")]
    Empty,
    #[error("invalid condition value `{0}` (use true, false, or unknown)")]
    InvalidValue(String),
}

pub fn parse_pod_condition(raw: &str) -> Result<PodConditionFilter, ParsePodConditionError> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(ParsePodConditionError::Empty);
    }
    let (name, value) = trimmed
        .split_once('=')
        .map(|(n, v)| (n.trim(), v.trim()))
        .unwrap_or((trimmed, "true"));
    if name.is_empty() {
        return Err(ParsePodConditionError::Empty);
    }
    let status = normalize_condition_status(value)?;
    Ok(PodConditionFilter {
        type_name: name.to_ascii_lowercase(),
        status,
    })
}

fn normalize_condition_status(value: &str) -> Result<String, ParsePodConditionError> {
    match value.to_ascii_lowercase().as_str() {
        "true" => Ok("True".into()),
        "false" => Ok("False".into()),
        "unknown" => Ok("Unknown".into()),
        other => Err(ParsePodConditionError::InvalidValue(other.to_string())),
    }
}

pub fn pod_matches_condition(pod: &Pod, filter: &PodConditionFilter) -> bool {
    let Some(conditions) = pod.status.as_ref().and_then(|s| s.conditions.as_ref()) else {
        return missing_condition_matches(filter);
    };

    for c in conditions {
        if c.type_.eq_ignore_ascii_case(&filter.type_name) {
            return c.status.eq_ignore_ascii_case(&filter.status);
        }
    }

    missing_condition_matches(filter)
}

fn missing_condition_matches(filter: &PodConditionFilter) -> bool {
    filter.type_name == "ready" && filter.status.eq_ignore_ascii_case("False")
}

#[cfg(test)]
mod tests {
    use super::*;
    use k8s_openapi::api::core::v1::PodCondition;
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;

    fn pod_with_conditions(conditions: Vec<PodCondition>) -> Pod {
        Pod {
            metadata: ObjectMeta {
                name: Some("p".into()),
                namespace: Some("ns".into()),
                uid: Some("uid".into()),
                ..Default::default()
            },
            spec: None,
            status: Some(k8s_openapi::api::core::v1::PodStatus {
                conditions: Some(conditions),
                ..Default::default()
            }),
        }
    }

    #[test]
    fn parse_name_defaults_true() {
        let f = parse_pod_condition("Ready").unwrap();
        assert_eq!(f.type_name, "ready");
        assert_eq!(f.status, "True");
    }

    #[test]
    fn parse_name_equals_value() {
        let f = parse_pod_condition("ready=false").unwrap();
        assert_eq!(f.status, "False");
    }

    #[test]
    fn matches_ready_true() {
        let pod = pod_with_conditions(vec![PodCondition {
            type_: "Ready".into(),
            status: "True".into(),
            ..Default::default()
        }]);
        let f = parse_pod_condition("ready").unwrap();
        assert!(pod_matches_condition(&pod, &f));
    }

    #[test]
    fn ready_false_without_condition_matches_job_like_pod() {
        let pod = Pod {
            metadata: ObjectMeta {
                name: Some("job".into()),
                ..Default::default()
            },
            spec: None,
            status: Some(k8s_openapi::api::core::v1::PodStatus::default()),
        };
        let f = parse_pod_condition("ready=false").unwrap();
        assert!(pod_matches_condition(&pod, &f));
    }

    #[test]
    fn ready_true_without_condition_does_not_match() {
        let pod = Pod {
            metadata: ObjectMeta::default(),
            spec: None,
            status: Some(k8s_openapi::api::core::v1::PodStatus::default()),
        };
        let f = parse_pod_condition("ready=true").unwrap();
        assert!(!pod_matches_condition(&pod, &f));
    }
}
