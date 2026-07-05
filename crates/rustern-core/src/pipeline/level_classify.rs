use futures::stream::{Stream, StreamExt};

use crate::source::{LogEvent, LogLevel, LogSourceError};

/// Map a JSON level field to [`LogLevel`].
///
/// Non-standard tokens become [`LogLevel::Other`] with an owned copy of the raw
/// string so `--exit-watch-level` can match arbitrary values later in the pipeline.
fn classify_str(s: &str) -> LogLevel {
    if s.eq_ignore_ascii_case("error")
        || s.eq_ignore_ascii_case("err")
        || s.eq_ignore_ascii_case("fatal")
    {
        LogLevel::Error
    } else if s.eq_ignore_ascii_case("warn") || s.eq_ignore_ascii_case("warning") {
        LogLevel::Warn
    } else if s.eq_ignore_ascii_case("info") {
        LogLevel::Info
    } else if s.eq_ignore_ascii_case("debug") {
        LogLevel::Debug
    } else if s.eq_ignore_ascii_case("trace") {
        LogLevel::Trace
    } else {
        LogLevel::Other(s.to_string())
    }
}

pub fn level_classify<S>(
    inner: S,
    level_key: Option<String>,
) -> impl Stream<Item = Result<LogEvent, LogSourceError>>
where
    S: Stream<Item = Result<LogEvent, LogSourceError>> + Send + 'static,
{
    inner.map(move |r| {
        r.map(|mut ev| {
            let Some(ref key) = level_key else {
                return ev;
            };
            let Some(ref parsed) = ev.structured else {
                return ev;
            };
            if let Some(lv) = parsed.level_str_at_dot_path(key) {
                ev.level = Some(classify_str(lv));
            }
            ev
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::source::{ContextName, Labels, ParsedJson, SourceKind, SourceMeta};
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
                palette_index: None,
                container_palette_index: None,
            }),
            timestamp: Utc::now(),
            message: Arc::from(raw),
            structured: Some(ParsedJson::Serde(serde_json::from_str(raw).unwrap())),
            level: None,
            palette_index: None,
            container_palette_index: None,
        };
        let s = futures::stream::iter(vec![Ok(ev)]);
        let out: Vec<_> = level_classify(s, Some("level".into())).collect().await;
        let lv = out[0].as_ref().unwrap().level.as_ref().unwrap();
        assert!(matches!(lv, LogLevel::Error));
    }

    #[tokio::test]
    async fn extracts_level_from_jaq_val() {
        let raw = r#"{"level":"warn","msg":"boom"}"#;
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
                palette_index: None,
                container_palette_index: None,
            }),
            timestamp: Utc::now(),
            message: Arc::from(raw),
            structured: Some(ParsedJson::Jaq(serde_json::from_str(raw).unwrap())),
            level: None,
            palette_index: None,
            container_palette_index: None,
        };
        let s = futures::stream::iter(vec![Ok(ev)]);
        let out: Vec<_> = level_classify(s, Some("level".into())).collect().await;
        let lv = out[0].as_ref().unwrap().level.as_ref().unwrap();
        assert!(matches!(lv, LogLevel::Warn));
    }

    #[tokio::test]
    async fn preserves_unknown_level_token() {
        let raw = r#"{"level":"notice","msg":"hey"}"#;
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
                palette_index: None,
                container_palette_index: None,
            }),
            timestamp: Utc::now(),
            message: Arc::from(raw),
            structured: Some(ParsedJson::Serde(serde_json::from_str(raw).unwrap())),
            level: None,
            palette_index: None,
            container_palette_index: None,
        };
        let s = futures::stream::iter(vec![Ok(ev)]);
        let out: Vec<_> = level_classify(s, Some("level".into())).collect().await;
        let lv = out[0].as_ref().unwrap().level.as_ref().unwrap();
        assert!(matches!(lv, LogLevel::Other(s) if s == "notice"));
    }
}
