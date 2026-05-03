use serde::Serialize;

use crate::render::LineFormatter;
use crate::source::LogEvent;

#[derive(Serialize)]
struct JsonLine<'a> {
    pod: &'a str,
    container: &'a str,
    namespace: &'a str,
    timestamp: String,
    message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    level: Option<String>,
}

pub struct JsonLineFormatter;

impl LineFormatter for JsonLineFormatter {
    fn format_line(&self, event: &LogEvent) -> String {
        let level = event.level.as_ref().map(|l| format!("{l:?}"));
        let row = JsonLine {
            pod: &event.source.pod,
            container: &event.source.container,
            namespace: &event.source.namespace,
            timestamp: event.timestamp.to_rfc3339(),
            message: event.message.to_string(),
            level,
        };
        serde_json::to_string(&row).expect("json") + "\n"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::source::{ContextName, Labels, LogLevel, SourceKind, SourceMeta};
    use chrono::Utc;
    use std::sync::Arc;

    #[test]
    fn ndjson_has_required_keys() {
        let ev = LogEvent {
            source: Arc::new(SourceMeta {
                context: ContextName("c".into()),
                namespace: "ns".into(),
                pod: "p".into(),
                container: "ctn".into(),
                kind: SourceKind::PodLog,
                node: None,
                labels: Arc::new(Labels::default()),
                uid: "u".into(),
            }),
            timestamp: Utc::now(),
            message: Arc::from("hi"),
            structured: None,
            level: Some(LogLevel::Error),
            palette_index: None,
        };
        let f = JsonLineFormatter;
        let s = f.format_line(&ev);
        let v: serde_json::Value = serde_json::from_str(s.trim()).unwrap();
        assert_eq!(v["pod"], "p");
        assert_eq!(v["container"], "ctn");
        assert_eq!(v["namespace"], "ns");
        assert_eq!(v["message"], "hi");
        assert!(v.get("timestamp").is_some());
        assert!(v.get("level").is_some());
        assert!(s.ends_with('\n'));
    }
}
