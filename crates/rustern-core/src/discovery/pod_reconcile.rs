use std::collections::HashSet;

use crate::source::SourceKey;

fn sort_keys(keys: &mut [SourceKey]) {
    keys.sort_unstable_by(|a, b| {
        a.context
            .0
            .cmp(&b.context.0)
            .then_with(|| a.namespace.cmp(&b.namespace))
            .then_with(|| a.pod.cmp(&b.pod))
            .then_with(|| a.container.cmp(&b.container))
            .then_with(|| a.uid.cmp(&b.uid))
    });
}

/// Pure reconcile: known keys vs current snapshot.
pub fn reconcile(active: &HashSet<SourceKey>, snapshot: &HashSet<SourceKey>) -> ReconcileDiff {
    let mut to_drop: Vec<SourceKey> = active.difference(snapshot).cloned().collect();
    let mut to_add: Vec<SourceKey> = snapshot.difference(active).cloned().collect();
    sort_keys(&mut to_drop);
    sort_keys(&mut to_add);
    ReconcileDiff { to_add, to_drop }
}

#[derive(Debug, Eq, PartialEq)]
pub struct ReconcileDiff {
    pub to_add: Vec<SourceKey>,
    pub to_drop: Vec<SourceKey>,
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

    #[test]
    fn diff_returns_keys_in_stable_order() {
        let active: HashSet<_> = [key("p3"), key("p1")].into_iter().collect();
        let snapshot: HashSet<_> = [key("p2")].into_iter().collect();
        let diff = reconcile(&active, &snapshot);
        assert_eq!(diff.to_add, vec![key("p2")]);
        assert_eq!(diff.to_drop, vec![key("p1"), key("p3")]);
    }
}
