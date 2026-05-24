use futures::stream::{Stream, StreamExt};

use crate::source::{LogEvent, LogLevel, LogSourceError};

fn dot_pointer(path: &str) -> String {
    if path.is_empty() {
        return String::new();
    }
    format!(
        "/{}",
        path.split('.')
            .map(|p| p.replace('/', "~1"))
            .collect::<Vec<_>>()
            .join("/")
    )
}

fn classify_str(s: &str) -> LogLevel {
    match s.to_ascii_lowercase().as_str() {
        "error" | "err" | "fatal" => LogLevel::Error,
        "warn" | "warning" => LogLevel::Warn,
        "info" => LogLevel::Info,
        "debug" => LogLevel::Debug,
        "trace" => LogLevel::Trace,
        other => LogLevel::Other(other.to_string()),
    }
}

pub fn level_classify<S>(
    inner: S,
    level_key: Option<String>,
) -> impl Stream<Item = Result<LogEvent, LogSourceError>>
where
    S: Stream<Item = Result<LogEvent, LogSourceError>> + Send + 'static,
{
    let path = level_key.map(|k| dot_pointer(&k));
    inner.map(move |r| {
        r.map(|mut ev| {
            let Some(ref ptr) = path else {
                return ev;
            };
            let Some(ref v) = ev.structured else {
                return ev;
            };
            if let Some(lv) = v.pointer(ptr).and_then(|x| x.as_str()) {
                ev.level = Some(classify_str(lv));
            }
            ev
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::source::{ContextName, Labels, SourceKind, SourceMeta};
    use chrono::Utc;
    use std::sync::Arc;

    #[tokio::test]
    async fn extracts_error_level() {
        let raw = r#"{"level":"error","msg":"boom"}"#;
        let ev = LogEvent {
            source: Arc::new(SourceMeta {
                context: ContextName("ctx".into()),
                namespace: "ns".into(),
                pod: "p".into(),
                container: "c".into(),
                kind: SourceKind::PodLog,
                node: None,
                labels: Arc::new(Labels::default()),
                uid: "u".into(),
            }),
            timestamp: Utc::now(),
            message: Arc::from(raw),
            structured: Some(serde_json::from_str(raw).unwrap()),
            level: None,
            palette_index: None,
            container_palette_index: None,
        };
        let s = futures::stream::iter(vec![Ok(ev)]);
        let out: Vec<_> = level_classify(s, Some("level".into())).collect().await;
        let lv = out[0].as_ref().unwrap().level.as_ref().unwrap();
        assert!(matches!(lv, LogLevel::Error));
    }
}
