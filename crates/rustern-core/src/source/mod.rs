//! Only `PodLogSource` is implemented; `Event` / `File` are reserved.

use std::pin::Pin;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use futures::Stream;
use serde_json::Value;
use tokio_util::sync::CancellationToken;

pub mod log_opener;
pub mod pod_log;
pub mod pod_meta;

#[cfg(any(test, feature = "bench"))]
pub use log_opener::ScriptLogSourceOpener;

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct ContextName(pub String);

#[derive(Debug, Clone, Eq, PartialEq, Hash, Default)]
pub struct Labels(pub std::collections::BTreeMap<String, String>);

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub enum SourceKind {
    PodLog,
    Event,
    File,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub enum LogLevel {
    Error,
    Warn,
    Info,
    Debug,
    Trace,
    Other(String),
}

impl LogLevel {
    pub fn as_str(&self) -> &str {
        match self {
            LogLevel::Error => "error",
            LogLevel::Warn => "warn",
            LogLevel::Info => "info",
            LogLevel::Debug => "debug",
            LogLevel::Trace => "trace",
            LogLevel::Other(s) => s.as_str(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct SourceMeta {
    pub context: ContextName,
    pub namespace: String,
    pub pod: String,
    pub container: String,
    pub kind: SourceKind,
    pub node: Option<String>,
    pub labels: Arc<Labels>,
    /// Pod `metadata.uid` (disambiguates reused names across rollouts).
    pub uid: String,
    /// Precomputed pod palette slot (set at attach when `--pod-colors` is on).
    pub palette_index: Option<u8>,
    /// Precomputed container palette slot (set at attach when `--container-colors` is on).
    pub container_palette_index: Option<u8>,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct SourceKey {
    pub context: ContextName,
    pub namespace: String,
    pub pod: String,
    pub container: String,
    /// Pod `metadata.uid`.
    pub uid: String,
}

#[derive(Debug, Clone)]
pub struct LogEvent {
    pub source: Arc<SourceMeta>,
    pub timestamp: DateTime<Utc>,
    pub message: Arc<str>,
    pub structured: Option<Value>,
    pub level: Option<LogLevel>,
    /// Stable palette slot for pod-name highlighting.
    pub palette_index: Option<u8>,
    /// Stable palette slot for container-name highlighting (`diff-container` uses container name).
    pub container_palette_index: Option<u8>,
}

#[derive(Debug, thiserror::Error)]
pub enum LogSourceError {
    #[error("upstream stream ended")]
    Eof,
    #[error("api error: {0}")]
    Api(String),
    #[error("cancelled")]
    Cancelled,
}

pub type BoxedLogStream = Pin<Box<dyn Stream<Item = Result<LogEvent, LogSourceError>> + Send>>;

/// Source that owns metadata and exposes a log line stream (for `StreamMap`).
pub trait LogSource: Send {
    fn meta(&self) -> &SourceMeta;
    fn cancellation_token(&self) -> CancellationToken;
    fn into_stream(self: Box<Self>) -> BoxedLogStream;
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;
    use tokio_stream::StreamMap;

    fn meta(pod: &str, uid: &str) -> SourceMeta {
        SourceMeta {
            context: ContextName("ctx".into()),
            namespace: "ns".into(),
            pod: pod.into(),
            container: "c".into(),
            kind: SourceKind::PodLog,
            node: None,
            labels: Arc::new(Labels::default()),
            uid: uid.into(),
            palette_index: None,
            container_palette_index: None,
        }
    }

    fn key(m: &SourceMeta) -> SourceKey {
        SourceKey {
            context: m.context.clone(),
            namespace: m.namespace.clone(),
            pod: m.pod.clone(),
            container: m.container.clone(),
            uid: m.uid.clone(),
        }
    }

    #[tokio::test]
    async fn stream_map_can_hold_two_log_sources() {
        let token = CancellationToken::new();
        let m1 = meta("p1", "uid-1");
        let m2 = meta("p2", "uid-2");
        let s1: Box<dyn LogSource> =
            Box::new(pod_log::PodLogSource::new_stub(m1.clone(), token.clone()));
        let s2: Box<dyn LogSource> =
            Box::new(pod_log::PodLogSource::new_stub(m2.clone(), token.clone()));

        let mut all: StreamMap<SourceKey, BoxedLogStream> = StreamMap::new();
        all.insert(key(&m1), s1.into_stream());
        all.insert(key(&m2), s2.into_stream());

        let collected: Vec<_> = all.collect().await;
        assert_eq!(collected.len(), 2);
        assert!(collected.iter().all(|(_k, r)| r.is_ok()));

        let uids: std::collections::HashSet<_> =
            collected.iter().map(|(k, _)| k.uid.clone()).collect();
        assert_eq!(uids.len(), 2);
    }

    #[test]
    fn log_source_error_displays_messages() {
        assert_eq!(format!("{}", LogSourceError::Eof), "upstream stream ended");
        assert_eq!(
            format!("{}", LogSourceError::Api("conn reset".into())),
            "api error: conn reset"
        );
        assert_eq!(format!("{}", LogSourceError::Cancelled), "cancelled");
    }
}
