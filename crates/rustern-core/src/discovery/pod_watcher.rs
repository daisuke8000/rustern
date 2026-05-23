//! Pod → [`SourceKey`] discovery and watch reconcile (facade over split modules).

pub use super::container_keys::{
    ContainerDiscoverOpts, ContainerLifecycleBucket, ContainerStatePolicy, keys_from_pod,
};
pub use super::pod_reconcile::{ReconcileDiff, pod_event_stream, reconcile};
