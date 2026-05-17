use std::collections::HashSet;

use futures::StreamExt;
use futures::stream::BoxStream;
use k8s_openapi::api::core::v1::{ContainerStatus, Pod};
use kube::runtime::watcher::{Config, Event, watcher};

use crate::source::SourceKey;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum ContainerLifecycleBucket {
    Running,
    Waiting,
    Terminated,
}

#[derive(Clone, Debug, Default)]
pub enum ContainerStatePolicy {
    #[default]
    All,
    Subset(HashSet<ContainerLifecycleBucket>),
}

/// Which workload containers contribute log stream keys (`stern`-aligned defaults).
#[derive(Clone, Debug)]
pub struct ContainerDiscoverOpts {
    pub include_init_containers: bool,
    pub include_ephemeral_containers: bool,
    pub state_policy: ContainerStatePolicy,
}

impl Default for ContainerDiscoverOpts {
    fn default() -> Self {
        Self {
            include_init_containers: true,
            include_ephemeral_containers: true,
            state_policy: ContainerStatePolicy::All,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ContainerPlacement {
    Main,
    Init,
    Ephemeral,
}

fn bucket(cs: Option<&ContainerStatus>) -> Option<ContainerLifecycleBucket> {
    let st = cs.and_then(|s| s.state.as_ref())?;
    if st.running.is_some() {
        return Some(ContainerLifecycleBucket::Running);
    }
    if st.waiting.is_some() {
        return Some(ContainerLifecycleBucket::Waiting);
    }
    if st.terminated.is_some() {
        return Some(ContainerLifecycleBucket::Terminated);
    }
    None
}

fn find_container_status<'a>(
    pod: &'a Pod,
    placement: ContainerPlacement,
    name: &str,
) -> Option<&'a ContainerStatus> {
    let st = pod.status.as_ref()?;
    let maybe_list = match placement {
        ContainerPlacement::Main => st.container_statuses.as_ref(),
        ContainerPlacement::Init => st.init_container_statuses.as_ref(),
        ContainerPlacement::Ephemeral => st.ephemeral_container_statuses.as_ref(),
    }?;
    maybe_list.iter().find(|cs| cs.name == name)
}

fn accepts_by_placement(
    policy: &ContainerStatePolicy,
    pod: &Pod,
    placement: ContainerPlacement,
    name: &str,
) -> bool {
    match policy {
        ContainerStatePolicy::All => true,
        ContainerStatePolicy::Subset(allowed) => {
            let cs = find_container_status(pod, placement, name);
            match bucket(cs) {
                Some(b) => allowed.contains(&b),
                None => false,
            }
        }
    }
}

fn collect_candidates(
    pod: &Pod,
    opts: &ContainerDiscoverOpts,
) -> Vec<(String, ContainerPlacement)> {
    let Some(spec) = pod.spec.as_ref() else {
        return Vec::new();
    };

    let mut out: Vec<(String, ContainerPlacement)> = Vec::new();
    for c in &spec.containers {
        out.push((c.name.clone(), ContainerPlacement::Main));
    }

    if opts.include_init_containers {
        if let Some(inits) = spec.init_containers.as_ref() {
            for c in inits {
                out.push((c.name.clone(), ContainerPlacement::Init));
            }
        }
    }

    if opts.include_ephemeral_containers {
        if let Some(ephems) = spec.ephemeral_containers.as_ref() {
            for c in ephems {
                out.push((c.name.clone(), ContainerPlacement::Ephemeral));
            }
        }
    }

    out
}

/// Resolve log stream [`SourceKey`]s for a Pod (`spec` names + lifecycle filter).
pub fn keys_from_pod(
    pod: &Pod,
    context: &crate::source::ContextName,
    opts: &ContainerDiscoverOpts,
) -> Vec<SourceKey> {
    let Some(uid) = pod.metadata.uid.clone() else {
        tracing::warn!(
            pod = ?pod.metadata.name,
            namespace = ?pod.metadata.namespace,
            "pod has no metadata.uid, skipping log tail"
        );
        return Vec::new();
    };

    let ns = pod.metadata.namespace.clone().unwrap_or_default();
    let pod_name = pod.metadata.name.clone().unwrap_or_default();

    collect_candidates(pod, opts)
        .into_iter()
        .filter(|(nm, plc)| accepts_by_placement(&opts.state_policy, pod, *plc, nm))
        .map(|(container, _)| SourceKey {
            context: context.clone(),
            namespace: ns.clone(),
            pod: pod_name.clone(),
            container,
            uid: uid.clone(),
        })
        .collect()
}

/// Pure reconcile: known keys vs current snapshot.
pub fn reconcile(active: &HashSet<SourceKey>, snapshot: &HashSet<SourceKey>) -> ReconcileDiff {
    let to_drop: Vec<SourceKey> = active.difference(snapshot).cloned().collect();
    let to_add: Vec<SourceKey> = snapshot.difference(active).cloned().collect();
    ReconcileDiff { to_add, to_drop }
}

#[derive(Debug, Eq, PartialEq)]
pub struct ReconcileDiff {
    pub to_add: Vec<SourceKey>,
    pub to_drop: Vec<SourceKey>,
}

pub fn pod_event_stream(
    api: kube::Api<Pod>,
    cfg: Config,
) -> BoxStream<'static, Result<Event<Pod>, kube::runtime::watcher::Error>> {
    watcher(api, cfg).boxed()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::source::ContextName;

    fn key(pod: &str) -> SourceKey {
        SourceKey {
            context: ContextName("ctx".into()),
            namespace: "ns".into(),
            pod: pod.into(),
            container: "c".into(),
            uid: format!("uid-{pod}"),
        }
    }

    #[test]
    fn diff_adds_new_keys() {
        let active: HashSet<_> = [key("p1")].into_iter().collect();
        let snapshot: HashSet<_> = [key("p1"), key("p2")].into_iter().collect();
        let diff = reconcile(&active, &snapshot);
        assert_eq!(diff.to_add, vec![key("p2")]);
        assert!(diff.to_drop.is_empty());
    }

    #[test]
    fn diff_drops_orphans() {
        let active: HashSet<_> = [key("p1"), key("p2")].into_iter().collect();
        let snapshot: HashSet<_> = [key("p2")].into_iter().collect();
        let diff = reconcile(&active, &snapshot);
        assert!(diff.to_add.is_empty());
        assert_eq!(diff.to_drop, vec![key("p1")]);
    }

    #[test]
    fn diff_empty_when_equal() {
        let active: HashSet<_> = [key("p1")].into_iter().collect();
        let snapshot: HashSet<_> = [key("p1")].into_iter().collect();
        let diff = reconcile(&active, &snapshot);
        assert!(diff.to_add.is_empty());
        assert!(diff.to_drop.is_empty());
    }
}

#[cfg(test)]
mod kube_tests {
    use super::*;
    use crate::source::ContextName;
    use k8s_openapi::api::core::v1::{
        Container, ContainerState, ContainerStateRunning, ContainerStateTerminated,
        ContainerStatus, PodSpec, PodStatus,
    };
    use kube::core::ObjectMeta;

    fn pod_with(
        name: &str,
        ns: &str,
        uid: Option<&str>,
        containers: Vec<&str>,
        extra: ExtraPodPieces<'_>,
        status: PodStatusPieces,
    ) -> Pod {
        let mut init_vec = Vec::new();
        for n in extra.init {
            init_vec.push(Container {
                name: (*n).into(),
                ..Default::default()
            });
        }

        Pod {
            metadata: ObjectMeta {
                name: Some(name.into()),
                namespace: Some(ns.into()),
                uid: uid.map(String::from),
                ..Default::default()
            },
            spec: Some(PodSpec {
                containers: containers
                    .into_iter()
                    .map(|n| Container {
                        name: n.into(),
                        ..Default::default()
                    })
                    .collect(),
                init_containers: if init_vec.is_empty() {
                    None
                } else {
                    Some(init_vec)
                },
                ..Default::default()
            }),
            status: Some(PodStatus {
                container_statuses: status.container_statuses,
                init_container_statuses: status.init_container_statuses,
                ephemeral_container_statuses: status.ephemeral_container_statuses,
                ..Default::default()
            }),
        }
    }

    struct ExtraPodPieces<'a> {
        init: &'a [&'a str],
    }

    struct PodStatusPieces {
        container_statuses: Option<Vec<ContainerStatus>>,
        init_container_statuses: Option<Vec<ContainerStatus>>,
        ephemeral_container_statuses: Option<Vec<ContainerStatus>>,
    }

    fn running_status(nm: &str) -> ContainerStatus {
        ContainerStatus {
            name: nm.into(),
            state: Some(ContainerState {
                running: Some(ContainerStateRunning::default()),
                waiting: None,
                terminated: None,
            }),
            ..Default::default()
        }
    }

    #[test]
    fn keys_extracted_from_two_containers() {
        let pod = pod_with(
            "p1",
            "ns",
            Some("uid-aaa"),
            vec!["app", "sidecar"],
            ExtraPodPieces { init: &[] },
            PodStatusPieces {
                container_statuses: Some(vec![running_status("app"), running_status("sidecar")]),
                init_container_statuses: None,
                ephemeral_container_statuses: None,
            },
        );
        let keys = keys_from_pod(
            &pod,
            &ContextName("ctx".into()),
            &ContainerDiscoverOpts::default(),
        );
        assert_eq!(keys.len(), 2);
        assert!(keys.iter().all(|k| k.uid == "uid-aaa"));
        assert!(keys.iter().any(|k| k.container == "app"));
        assert!(keys.iter().any(|k| k.container == "sidecar"));
    }

    #[test]
    fn keys_include_init_when_enabled() {
        let pod = pod_with(
            "p1",
            "ns",
            Some("uid-1"),
            vec!["app"],
            ExtraPodPieces { init: &["migrate"] },
            PodStatusPieces {
                container_statuses: Some(vec![running_status("app")]),
                init_container_statuses: Some(vec![running_status("migrate")]),
                ephemeral_container_statuses: None,
            },
        );

        let with_init = ContainerDiscoverOpts {
            include_init_containers: true,
            include_ephemeral_containers: false,
            ..Default::default()
        };
        let keys = keys_from_pod(&pod, &ContextName("ctx".into()), &with_init);
        assert!(keys.iter().any(|k| k.container == "migrate"));
        assert!(keys.iter().any(|k| k.container == "app"));

        let sans_init = ContainerDiscoverOpts {
            include_init_containers: false,
            include_ephemeral_containers: false,
            ..Default::default()
        };
        let keys2 = keys_from_pod(&pod, &ContextName("ctx".into()), &sans_init);
        assert!(!keys2.iter().any(|k| k.container == "migrate"));
        assert!(keys2.iter().any(|k| k.container == "app"));
    }

    #[test]
    fn keys_skipped_when_uid_missing() {
        let pod = pod_with(
            "p1",
            "ns",
            None,
            vec!["app"],
            ExtraPodPieces { init: &[] },
            PodStatusPieces {
                container_statuses: Some(vec![running_status("app")]),
                init_container_statuses: None,
                ephemeral_container_statuses: None,
            },
        );
        let keys = keys_from_pod(
            &pod,
            &ContextName("ctx".into()),
            &ContainerDiscoverOpts::default(),
        );
        assert!(keys.is_empty());
    }

    #[test]
    fn keys_distinguish_uid_for_rolling_update() {
        let pod_v1 = pod_with(
            "p1",
            "ns",
            Some("uid-old"),
            vec!["app"],
            ExtraPodPieces { init: &[] },
            PodStatusPieces {
                container_statuses: Some(vec![running_status("app")]),
                init_container_statuses: None,
                ephemeral_container_statuses: None,
            },
        );
        let pod_v2 = pod_with(
            "p1",
            "ns",
            Some("uid-new"),
            vec!["app"],
            ExtraPodPieces { init: &[] },
            PodStatusPieces {
                container_statuses: Some(vec![running_status("app")]),
                init_container_statuses: None,
                ephemeral_container_statuses: None,
            },
        );

        let k1 = keys_from_pod(
            &pod_v1,
            &ContextName("ctx".into()),
            &ContainerDiscoverOpts::default(),
        );
        let k2 = keys_from_pod(
            &pod_v2,
            &ContextName("ctx".into()),
            &ContainerDiscoverOpts::default(),
        );

        assert_ne!(k1[0], k2[0]);
        assert_ne!(k1[0].uid, k2[0].uid);
    }

    #[test]
    fn lifecycle_filter_keeps_running_only() {
        let terminated = ContainerStatus {
            name: "done".into(),
            state: Some(ContainerState {
                running: None,
                waiting: None,
                terminated: Some(ContainerStateTerminated {
                    exit_code: 0,
                    ..Default::default()
                }),
            }),
            ..Default::default()
        };
        let pod = pod_with(
            "p1",
            "ns",
            Some("uid-x"),
            vec!["run", "done"],
            ExtraPodPieces { init: &[] },
            PodStatusPieces {
                container_statuses: Some(vec![running_status("run"), terminated]),
                init_container_statuses: None,
                ephemeral_container_statuses: None,
            },
        );
        let policy =
            ContainerStatePolicy::Subset([ContainerLifecycleBucket::Running].into_iter().collect());
        let opts = ContainerDiscoverOpts {
            include_init_containers: false,
            include_ephemeral_containers: false,
            state_policy: policy,
        };
        let keys = keys_from_pod(&pod, &ContextName("ctx".into()), &opts);
        assert_eq!(keys.len(), 1);
        assert_eq!(keys[0].container, "run");
    }
}
