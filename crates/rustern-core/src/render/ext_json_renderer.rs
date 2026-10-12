//! Stern-compatible `extjson` / `ppextjson` output (see stern `generateTemplate`).
//!
//! Pod/container/namespace fields are plain text (no ANSI in JSON values; stern parity).

use std::collections::BTreeMap;

use serde::Serialize;
use serde_json::Value;

use crate::render::LineFormatter;
use crate::source::LogEvent;

pub struct ExtJsonLineFormatter {
    pub all_namespaces: bool,
    pub pretty: bool,
}

/// Embed `message` as raw JSON when valid (stern `extjson` helper), else a JSON string.
fn encode_message(message: &str, structured: Option<&crate::source::ParsedJson>) -> Value {
    if let Some(parsed) = structured {
        return parsed.to_serde_value();
    }
    let trimmed = message.trim_end_matches('\n');
    match serde_json::from_str::<Value>(trimmed) {
        Ok(v) => v,
        Err(_) => Value::String(trimmed.to_string()),
    }
}

// Field order preserves the sorted keys of the previous serde_json::Map.
#[derive(Serialize)]
struct ExtJsonLine<'a> {
    container: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    labels: Option<&'a BTreeMap<String, String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    level: Option<&'a str>,
    message: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    namespace: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    node: Option<&'a str>,
    pod: &'a str,
    timestamp: String,
}

fn build_object(event: &LogEvent, all_namespaces: bool) -> ExtJsonLine<'_> {
    ExtJsonLine {
        container: &event.source.container,
        labels: (!event.source.labels.0.is_empty()).then_some(&event.source.labels.0),
        level: event.level.as_ref().map(|level| level.as_str()),
        message: encode_message(&event.message, event.structured.as_ref()),
        namespace: all_namespaces.then_some(event.source.namespace.as_str()),
        node: event.source.node.as_deref(),
        pod: &event.source.pod,
        timestamp: event.timestamp.to_rfc3339(),
    }
}

impl LineFormatter for ExtJsonLineFormatter {
    fn format_into(&self, event: &LogEvent, buf: &mut String) {
        let obj = build_object(event, self.all_namespaces);
        if self.pretty {
            buf.push_str(&serde_json::to_string_pretty(&obj).expect("json"));
        } else {
            buf.push_str(&serde_json::to_string(&obj).expect("json"));
        }
        buf.push('\n');
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::source::{ContextName, Labels, LogLevel, SourceKind, SourceMeta};
    use chrono::{TimeZone, Utc};
    use serde_json::{Map, json};
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
                palette_index: None,
                container_palette_index: None,
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

    fn legacy_line(event: &LogEvent, all_namespaces: bool, pretty: bool) -> String {
        let mut obj = Map::new();
        if all_namespaces {
            obj.insert("namespace".into(), json!(event.source.namespace));
        }
        obj.insert("pod".into(), json!(event.source.pod));
        obj.insert("container".into(), json!(event.source.container));
        obj.insert(
            "message".into(),
            encode_message(&event.message, event.structured.as_ref()),
        );
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
        let mut line = if pretty {
            serde_json::to_string_pretty(&Value::Object(obj)).unwrap()
        } else {
            serde_json::to_string(&Value::Object(obj)).unwrap()
        };
        line.push('\n');
        line
    }

    #[test]
    fn extjson_output_bytes_match_legacy_with_optional_fields_and_messages() {
        use crate::source::ParsedJson;

        for message in [
            "plain \"quoted\" \\ log\n\n",
            "{invalid\n",
            r#"{"z":[null,true,1.5],"a":{"z":2,"a":1}}"#,
            "[1,true,null]",
            "null",
            "42",
            "\"json string\"",
            "",
        ] {
            for present in 0..8 {
                let mut event = sample_event(message);
                let source = Arc::make_mut(&mut event.source);
                if present & 1 == 0 {
                    source.node = None;
                }
                if present & 2 == 0 {
                    source.labels = Arc::new(Labels::default());
                } else {
                    source.labels = Arc::new(Labels(BTreeMap::from([
                        ("pod".into(), "label pod".into()),
                        ("message".into(), "label message".into()),
                        ("z\"\\".into(), "escaped\nvalue".into()),
                    ])));
                }
                event.level =
                    (present & 4 != 0).then(|| LogLevel::Other("custom \"level\"".into()));
                for structured in [false, true] {
                    event.structured = structured
                        .then(|| ParsedJson::Serde(json!({"z": "cached", "a": [null, true]})));
                    for all_namespaces in [false, true] {
                        for pretty in [false, true] {
                            let mut actual = String::from("existing prefix\n");
                            formatter(all_namespaces, pretty).format_into(&event, &mut actual);
                            assert_eq!(
                                actual,
                                format!(
                                    "existing prefix\n{}",
                                    legacy_line(&event, all_namespaces, pretty)
                                ),
                                "message={message:?}, present={present}, structured={structured}, namespace={all_namespaces}, pretty={pretty}"
                            );
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn extjson_key_order_and_label_collisions_are_nested() {
        let mut event = sample_event("plain log");
        Arc::make_mut(&mut event.source).labels = Arc::new(Labels(BTreeMap::from([
            ("pod".into(), "label pod".into()),
            ("container".into(), "label container".into()),
            ("message".into(), "label message".into()),
        ])));
        assert_eq!(
            formatter(true, false).format_line(&event),
            concat!(
                "{\"container\":\"app\",\"labels\":{\"container\":\"label container\",",
                "\"message\":\"label message\",\"pod\":\"label pod\"},",
                "\"level\":\"info\",\"message\":\"plain log\",",
                "\"namespace\":\"production\",\"node\":\"node-1\",",
                "\"pod\":\"web-abc\",\"timestamp\":\"2024-06-15T12:00:00+00:00\"}\n"
            )
        );
    }

    #[test]
    fn renderers_preserve_rfc3339_timestamp_bytes() {
        use crate::format_display::{TimestampStyle, TimestampZone};
        use crate::render::default_renderer::DefaultLineFormatter;
        use crate::render::json_renderer::JsonLineFormatter;
        use chrono::{DateTime, Local, SecondsFormat};

        for timestamp in [
            "2024-06-15T12:00:00+00:00",
            "2024-06-15T12:00:00.123+00:00",
            "2024-06-15T12:00:00.123456+00:00",
            "2024-06-15T12:00:00.123456789+00:00",
            "2024-06-15T12:00:00.000000001+00:00",
            "2016-12-31T23:59:60.123456789+00:00",
            "1969-12-31T23:59:59.999999999+00:00",
            "-0001-01-01T00:00:00+00:00",
            "+10000-01-01T00:00:00+00:00",
        ] {
            let mut event = sample_event("x");
            event.timestamp = timestamp.parse::<DateTime<Utc>>().unwrap();
            assert_eq!(event.timestamp.to_rfc3339(), timestamp);

            assert_eq!(
                JsonLineFormatter.format_line(&event),
                format!(
                    "{{\"pod\":\"web-abc\",\"container\":\"app\",\"namespace\":\"production\",\"timestamp\":\"{timestamp}\",\"message\":\"x\",\"level\":\"info\"}}\n"
                )
            );
            for pretty in [false, true] {
                let output = formatter(true, pretty).format_line(&event);
                let value: Value = serde_json::from_str(&output).unwrap();
                assert_eq!(value["timestamp"], timestamp);
            }

            for zone in [
                TimestampZone::Utc,
                TimestampZone::Local,
                TimestampZone::Iana(chrono_tz::America::New_York),
                TimestampZone::Iana(chrono_tz::Asia::Kolkata),
            ] {
                let expected = match zone {
                    TimestampZone::Utc => {
                        event.timestamp.to_rfc3339_opts(SecondsFormat::Nanos, false)
                    }
                    TimestampZone::Local => event
                        .timestamp
                        .with_timezone(&Local)
                        .to_rfc3339_opts(SecondsFormat::Nanos, false),
                    TimestampZone::Iana(tz) => event
                        .timestamp
                        .with_timezone(&tz)
                        .to_rfc3339_opts(SecondsFormat::Nanos, false),
                };
                let default = DefaultLineFormatter {
                    timestamp_style: TimestampStyle::Rfc3339,
                    timestamp_zone: zone,
                    color_enabled: false,
                    pod_colors: false,
                    container_colors: false,
                };
                assert_eq!(
                    default.format_line(&event),
                    format!("{expected} web-abc/app | x\n")
                );
            }
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
    fn extjson_reuses_structured_json_without_reparse() {
        use crate::source::ParsedJson;
        let raw = r#"{"level":"error","msg":"boom"}"#;
        let mut event = sample_event(raw);
        event.structured = Some(ParsedJson::Serde(serde_json::from_str(raw).unwrap()));
        let f = formatter(false, false);
        let s = f.format_line(&event);
        let v: Value = serde_json::from_str(s.trim()).unwrap();
        assert_eq!(v["message"]["level"], "error");
        assert_eq!(v["message"]["msg"], "boom");
    }

    #[test]
    fn extjson_fields_are_plain_text_for_jq() {
        let f = formatter(false, false);
        let s = f.format_line(&sample_event("x"));
        let v: Value = serde_json::from_str(s.trim()).unwrap();
        let pod = v["pod"].as_str().unwrap();
        assert!(
            !pod.contains('\u{1b}'),
            "extjson must not embed ANSI in JSON values"
        );
    }
}
