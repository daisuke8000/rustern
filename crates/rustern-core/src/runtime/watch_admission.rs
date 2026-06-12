//! Event-time pod and container admission for the watch reconcile loop.
//!
//! - **Plan time** ([`crate::discovery::pod_list::PodWatchPlan`], [`crate::discovery::watch_scope`]):
//!   resolves CLI query, namespaces, label/field selectors, and API list/watch parameters.
//! - **Event time** (this module): decides which watched [`Pod`] objects and container
//!   [`SourceKey`] streams are admitted when watch events arrive.
//!
//! Label and field selectors stay server-side at plan time; they are not re-evaluated here.

use std::collections::HashSet;

use k8s_openapi::api::core::v1::Pod;
use regex::Regex;

use crate::discovery::pod_condition::{PodConditionFilter, pod_matches_condition};
use crate::discovery::pod_watcher::{ContainerDiscoverOpts, keys_from_pod};
use crate::source::{ContextName, SourceKey};

/// Event-time admission policy built once in [`super::run::run`].
#[derive(Clone)]
pub(crate) struct WatchAdmissionPolicy {
    context_name: ContextName,
    pod_regex: Option<Regex>,
    exclude_pod: Vec<Regex>,
    allowed_ns: Option<HashSet<String>>,
    pod_condition: Option<PodConditionFilter>,
    container_incl: Regex,
    container_excl: Vec<Regex>,
    container_discovery: ContainerDiscoverOpts,
}

impl WatchAdmissionPolicy {
    pub(crate) fn context_name(&self) -> &ContextName {
        &self.context_name
    }

    pub(crate) fn try_new(
        context_name: ContextName,
        pod_regex: Option<Regex>,
        exclude_pod: &[String],
        namespaces: &[String],
        all_namespaces: bool,
        container: &str,
        exclude_container: &[String],
        container_discovery: ContainerDiscoverOpts,
        pod_condition: Option<PodConditionFilter>,
    ) -> Result<Self, regex::Error> {
        let exclude_pod: Vec<Regex> = exclude_pod
            .iter()
            .map(|p| Regex::new(p))
            .collect::<Result<_, _>>()?;
        let container_incl = Regex::new(container)?;
        let container_excl: Vec<Regex> = exclude_container
            .iter()
            .map(|p| Regex::new(p))
            .collect::<Result<_, _>>()?;
        let allowed_ns = resolve_allowed_ns(namespaces, all_namespaces);

        Ok(Self {
            context_name,
            pod_regex,
            exclude_pod,
            allowed_ns,
            pod_condition,
            container_incl,
            container_excl,
            container_discovery,
        })
    }

    pub(crate) fn admit_pod(&self, pod: &Pod) -> bool {
        let Some(name) = pod.metadata.name.as_deref() else {
            return false;
        };
        let Some(ns) = pod.metadata.namespace.as_deref() else {
            return false;
        };
        if let Some(allowed) = &self.allowed_ns {
            if !allowed.contains(ns) {
                return false;
            }
        }
        if self.exclude_pod.iter().any(|re| re.is_match(name)) {
            return false;
        }
        if let Some(re) = &self.pod_regex
            && !re.is_match(name)
        {
            return false;
        }
        if let Some(cond) = &self.pod_condition
            && !pod_matches_condition(pod, cond)
        {
            return false;
        }
        true
    }

    pub(crate) fn admit_streams(&self, pod: &Pod) -> Vec<SourceKey> {
        keys_from_pod(pod, &self.context_name, &self.container_discovery)
            .into_iter()
            .filter(|k| self.container_incl.is_match(&k.container))
            .filter(|k| !self.container_excl.iter().any(|r| r.is_match(&k.container)))
            .collect()
    }

    pub(crate) fn collect_snapshot(&self, pods: Vec<Pod>) -> HashSet<SourceKey> {
        let mut snap = HashSet::new();
        for pod in pods {
            if !self.admit_pod(&pod) {
                continue;
            }
            for k in self.admit_streams(&pod) {
                snap.insert(k);
            }
        }
        snap
    }
}

fn resolve_allowed_ns(namespaces: &[String], all_namespaces: bool) -> Option<HashSet<String>> {
    // None = skip client-side namespace filtering:
    // - all_namespaces: watch all namespaces
    // - len <= 1: default or single namespace already scoped server-side at plan time
    if all_namespaces || namespaces.len() <= 1 {
        None
    } else {
        Some(namespaces.iter().cloned().collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use k8s_openapi::api::core::v1::{
        Container, ContainerState, ContainerStateRunning, ContainerStatus, Pod, PodCondition,
        PodSpec, PodStatus,
    };
    use kube::api::ObjectMeta;

    use crate::discovery::pod_watcher::{ContainerLifecycleBucket, ContainerStatePolicy};

    fn test_policy() -> WatchAdmissionPolicyBuilder {
        WatchAdmissionPolicyBuilder::new()
    }

    struct WatchAdmissionPolicyBuilder {
        context_name: ContextName,
        pod_regex: Option<Regex>,
        exclude_pod: Vec<String>,
        namespaces: Vec<String>,
        all_namespaces: bool,
        container: String,
        exclude_container: Vec<String>,
        container_discovery: ContainerDiscoverOpts,
        pod_condition: Option<PodConditionFilter>,
    }

    impl WatchAdmissionPolicyBuilder {
        fn new() -> Self {
            Self {
                context_name: ContextName("ctx".into()),
                pod_regex: None,
                exclude_pod: Vec::new(),
                namespaces: vec!["ns".into()],
                all_namespaces: false,
                container: ".*".into(),
                exclude_container: Vec::new(),
                container_discovery: ContainerDiscoverOpts {
                    include_init_containers: false,
                    include_ephemeral_containers: false,
                    state_policy: ContainerStatePolicy::Subset(
                        [ContainerLifecycleBucket::Running].into_iter().collect(),
                    ),
                },
                pod_condition: None,
            }
        }

        fn pod_regex(mut self, pattern: &str) -> Self {
            self.pod_regex = Some(Regex::new(pattern).expect("pod_regex"));
            self
        }

        fn exclude_pod(mut self, patterns: &[&str]) -> Self {
            self.exclude_pod = patterns.iter().map(|s| (*s).to_string()).collect();
            self
        }

        fn namespaces(mut self, ns: &[&str]) -> Self {
            self.namespaces = ns.iter().map(|s| (*s).to_string()).collect();
            self
        }

        fn all_namespaces(mut self, enabled: bool) -> Self {
            self.all_namespaces = enabled;
            self
        }

        fn container(mut self, pattern: &str) -> Self {
            self.container = pattern.into();
            self
        }

        fn exclude_container(mut self, patterns: &[&str]) -> Self {
            self.exclude_container = patterns.iter().map(|s| (*s).to_string()).collect();
            self
        }

        fn pod_condition(mut self, filter: PodConditionFilter) -> Self {
            self.pod_condition = Some(filter);
            self
        }

        fn build(self) -> WatchAdmissionPolicy {
            WatchAdmissionPolicy::try_new(
                self.context_name,
                self.pod_regex,
                &self.exclude_pod,
                &self.namespaces,
                self.all_namespaces,
                &self.container,
                &self.exclude_container,
                self.container_discovery,
                self.pod_condition,
            )
            .expect("policy")
        }
    }

    fn pod_named(ns: &str, name: &str, containers: &[&str]) -> Pod {
        Pod {
            metadata: ObjectMeta {
                name: Some(name.into()),
                namespace: Some(ns.into()),
                uid: Some(format!("uid-{name}")),
                ..Default::default()
            },
            spec: Some(PodSpec {
                containers: containers
                    .iter()
                    .map(|n| Container {
                        name: n.to_string(),
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            }),
            status: Some(PodStatus {
                container_statuses: Some(
                    containers
                        .iter()
                        .map(|n| ContainerStatus {
                            name: n.to_string(),
                            ready: true,
                            state: Some(ContainerState {
                                running: Some(ContainerStateRunning::default()),
                                ..Default::default()
                            }),
                            ..Default::default()
                        })
                        .collect(),
                ),
                phase: Some("Running".into()),
                ..Default::default()
            }),
        }
    }

    fn pod_with_conditions(conditions: Vec<PodCondition>) -> Pod {
        let mut pod = pod_named("ns", "p", &["app"]);
        pod.status.as_mut().unwrap().conditions = Some(conditions);
        pod
    }

    #[test]
    fn selector_only_implicit_dot_admits_all_pod_names() {
        let policy = test_policy().pod_regex(".*").build();
        let pod = pod_named("ns", "anything", &["app"]);
        assert!(policy.admit_pod(&pod));
    }

    #[test]
    fn pod_regex_filters_by_name() {
        let policy = test_policy().pod_regex("^api-.*").build();
        assert!(policy.admit_pod(&pod_named("ns", "api-v1", &["app"])));
        assert!(!policy.admit_pod(&pod_named("ns", "worker-1", &["app"])));
    }

    #[test]
    fn allowed_ns_rejects_out_of_list() {
        let policy = test_policy().namespaces(&["a", "b"]).build();
        assert!(policy.admit_pod(&pod_named("a", "p", &["app"])));
        assert!(!policy.admit_pod(&pod_named("other", "p", &["app"])));
    }

    #[test]
    fn exclude_pod_rejects_matching_names() {
        let policy = test_policy().exclude_pod(&["^debug-"]).build();
        assert!(!policy.admit_pod(&pod_named("ns", "debug-1", &["app"])));
        assert!(policy.admit_pod(&pod_named("ns", "app-1", &["app"])));
    }

    #[test]
    fn pod_condition_filters_ready_pods() {
        let policy = test_policy()
            .pod_condition(PodConditionFilter {
                type_name: "ready".into(),
                status: "True".into(),
            })
            .build();
        let ready = pod_with_conditions(vec![PodCondition {
            type_: "Ready".into(),
            status: "True".into(),
            ..Default::default()
        }]);
        let not_ready = pod_with_conditions(vec![PodCondition {
            type_: "Ready".into(),
            status: "False".into(),
            ..Default::default()
        }]);
        assert!(policy.admit_pod(&ready));
        assert!(!policy.admit_pod(&not_ready));
    }

    #[test]
    fn admit_streams_includes_matching_containers() {
        let pod = pod_named("ns", "p", &["app", "sidecar", "istio-proxy"]);
        let policy = test_policy().container("app|sidecar").build();
        let names: HashSet<_> = policy
            .admit_streams(&pod)
            .into_iter()
            .map(|k| k.container)
            .collect();
        assert_eq!(names, HashSet::from(["app".into(), "sidecar".into()]));
    }

    #[test]
    fn admit_streams_excludes_matching_containers() {
        let pod = pod_named("ns", "p", &["app", "sidecar", "istio-proxy"]);
        let policy = test_policy().exclude_container(&["istio-proxy"]).build();
        let names: HashSet<_> = policy
            .admit_streams(&pod)
            .into_iter()
            .map(|k| k.container)
            .collect();
        assert_eq!(names, HashSet::from(["app".into(), "sidecar".into()]));
    }

    #[test]
    fn collect_snapshot_applies_pod_and_container_filters() {
        let allowed = pod_named("ns", "app-1", &["app", "istio-proxy"]);
        let excluded = pod_named("ns", "debug-1", &["app"]);
        let policy = test_policy().pod_regex("app-.*").container("app").build();
        let snap = policy.collect_snapshot(vec![allowed, excluded]);
        assert_eq!(snap.len(), 1);
        assert_eq!(snap.iter().next().unwrap().container, "app");
    }

    #[test]
    fn single_namespace_skips_client_side_filter() {
        let policy = test_policy().namespaces(&["prod"]).build();
        assert!(policy.admit_pod(&pod_named("prod", "p", &["app"])));
        assert!(policy.admit_pod(&pod_named("other", "p", &["app"])));
    }

    #[test]
    fn admit_pod_rejects_missing_namespace() {
        let mut pod = pod_named("ns", "p", &["app"]);
        pod.metadata.namespace = None;
        let policy = test_policy().build();
        assert!(!policy.admit_pod(&pod));
    }

    #[test]
    fn all_namespaces_skips_client_side_namespace_filter() {
        let policy = test_policy()
            .namespaces(&["prod"])
            .all_namespaces(true)
            .build();
        assert!(policy.admit_pod(&pod_named("any-ns", "p", &["app"])));
    }

    #[test]
    fn combined_namespace_pod_and_container_filters() {
        let policy = test_policy()
            .namespaces(&["prod", "staging"])
            .pod_regex("^web-")
            .exclude_pod(&["web-canary"])
            .container("app")
            .exclude_container(&["istio-proxy"])
            .build();
        let ok = pod_named("prod", "web-1", &["app", "istio-proxy"]);
        let bad_ns = pod_named("dev", "web-1", &["app"]);
        let bad_name = pod_named("prod", "web-canary", &["app"]);
        assert!(policy.admit_pod(&ok));
        assert!(!policy.admit_pod(&bad_ns));
        assert!(!policy.admit_pod(&bad_name));
        let snap = policy.collect_snapshot(vec![ok]);
        assert_eq!(snap.len(), 1);
    }
}
