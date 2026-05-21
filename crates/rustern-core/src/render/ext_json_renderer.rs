//! Stern-compatible `extjson` / `ppextjson` output (see stern `generateTemplate`).
//!
//! Pod/container/namespace fields are plain text (no ANSI in JSON values; stern parity).

use serde_json::{Map, Value, json};

use crate::render::LineFormatter;
use crate::source::LogEvent;

pub struct ExtJsonLineFormatter {
    pub all_namespaces: bool,
    pub pretty: bool,
}

/// Embed `message` as raw JSON when valid (stern `extjson` helper), else a JSON string.
fn encode_message(message: &str) -> Value {
    let trimmed = message.trim_end_matches('\n');
    if serde_json::from_str::<Value>(trimmed).is_ok() {
        serde_json::from_str(trimmed).expect("valid json checked")
    } else {
        Value::String(trimmed.to_string())
    }
}

fn build_object(event: &LogEvent, all_namespaces: bool) -> Map<String, Value> {
    let mut obj = Map::new();

    if all_namespaces {
        obj.insert("namespace".into(), json!(event.source.namespace));
    }
    obj.insert("pod".into(), json!(event.source.pod));
    obj.insert("container".into(), json!(event.source.container));
    obj.insert("message".into(), encode_message(&event.message));
    obj.insert("timestamp".into(), json!(event.timestamp.to_rfc3339()));
    if let Some(node) = &event.source.node {
        obj.insert("node".into(), json!(node));
    }
    if !event.source.labels.0.is_empty() {
        obj.insert(
            "labels".into(),
            serde_json::to_value(&event.source.labels.0).expect("labels map"),
        );
    }
    if let Some(level) = &event.level {
        obj.insert("level".into(), json!(level.as_str()));
    }
    obj
}

impl LineFormatter for ExtJsonLineFormatter {
    fn format_line(&self, event: &LogEvent) -> String {
        let obj = build_object(event, self.all_namespaces);
        let line = if self.pretty {
            serde_json::to_string_pretty(&Value::Object(obj)).expect("json")
        } else {
            serde_json::to_string(&Value::Object(obj)).expect("json")
        };
        line + "\n"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::source::{ContextName, Labels, LogLevel, SourceKind, SourceMeta};
    use chrono::{TimeZone, Utc};
    use std::collections::BTreeMap;
    use std::sync::Arc;

    fn sample_event(message: &str) -> LogEvent {
        let mut labels = BTreeMap::new();
        labels.insert("app".into(), "web".into());
        LogEvent {
            source: Arc::new(SourceMeta {
                context: ContextName("ctx".into()),
                namespace: "production".into(),
                pod: "web-abc".into(),
                container: "app".into(),
                kind: SourceKind::PodLog,
                node: Some("node-1".into()),
                labels: Arc::new(Labels(labels)),
                uid: "uid-1".into(),
            }),
            timestamp: Utc.with_ymd_and_hms(2024, 6, 15, 12, 0, 0).unwrap(),
            message: Arc::from(message),
            structured: None,
            level: Some(LogLevel::Info),
            palette_index: Some(2),
            container_palette_index: Some(1),
        }
    }

    fn formatter(all_namespaces: bool, pretty: bool) -> ExtJsonLineFormatter {
        ExtJsonLineFormatter {
            all_namespaces,
            pretty,
        }
    }

    #[test]
    fn extjson_compact_has_stern_core_keys_and_extensions() {
        let f = formatter(false, false);
        let s = f.format_line(&sample_event("plain log"));
        let v: Value = serde_json::from_str(s.trim()).unwrap();

        assert_eq!(v["pod"], "web-abc");
        assert_eq!(v["container"], "app");
        assert_eq!(v["message"], "plain log");
        assert_eq!(v["timestamp"], "2024-06-15T12:00:00+00:00");
        assert_eq!(v["node"], "node-1");
        assert_eq!(v["labels"]["app"], "web");
        assert_eq!(v["level"], "info");
        assert!(v.get("namespace").is_none());
        assert!(s.ends_with('\n'));
        assert_eq!(s.matches('\n').count(), 1);
    }

    #[test]
    fn extjson_includes_namespace_when_all_namespaces() {
        let f = formatter(true, false);
        let s = f.format_line(&sample_event("x"));
        let v: Value = serde_json::from_str(s.trim()).unwrap();
        assert_eq!(v["namespace"], "production");
    }

    #[test]
    fn extjson_embeds_json_message_inline() {
        let f = formatter(false, false);
        let s = f.format_line(&sample_event(r#"{"level":"error","msg":"boom"}"#));
        let v: Value = serde_json::from_str(s.trim()).unwrap();
        assert_eq!(v["message"]["level"], "error");
        assert_eq!(v["message"]["msg"], "boom");
    }

    #[test]
    fn ppextjson_is_pretty_printed() {
        let f = formatter(false, true);
        let s = f.format_line(&sample_event("hello"));
        assert!(s.starts_with("{\n"));
        assert!(s.contains("\n  \"pod\""));
        assert!(s.ends_with("\n}\n"));
        let v: Value = serde_json::from_str(s.trim()).unwrap();
        assert_eq!(v["message"], "hello");
    }

    #[test]
    fn extjson_fields_are_plain_text_for_jq() {
        let f = formatter(false, false);
        let s = f.format_line(&sample_event("x"));
        let v: Value = serde_json::from_str(s.trim()).unwrap();
        let pod = v["pod"].as_str().unwrap();
        assert!(!pod.contains('\u{1b}'), "extjson must not embed ANSI in JSON values");
    }
}
