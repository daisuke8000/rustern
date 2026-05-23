use std::collections::HashSet;

use futures::StreamExt;
use futures::stream::BoxStream;
use k8s_openapi::api::core::v1::Pod;
use kube::runtime::watcher::{Config, Event, watcher};

use crate::source::SourceKey;

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
