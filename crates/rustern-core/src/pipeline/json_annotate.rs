use futures::stream::{Stream, StreamExt};
use jaq_json::Val;
use serde_json::Value;

use crate::source::{LogEvent, LogSourceError, ParsedJson};

pub fn json_annotate<S>(
    inner: S,
    use_jaq_val: bool,
) -> impl Stream<Item = Result<LogEvent, LogSourceError>>
where
    S: Stream<Item = Result<LogEvent, LogSourceError>> + Send + 'static,
{
    inner.map(move |r| {
        r.map(|mut ev| {
            if !ev.message.trim_start().starts_with('{') {
                return ev;
            }
            if use_jaq_val {
                if let Ok(v) = serde_json::from_str::<Val>(ev.message.as_ref()) {
                    ev.structured = Some(ParsedJson::Jaq(v));
                }
            } else if let Ok(v) = serde_json::from_str::<Value>(ev.message.as_ref()) {
                ev.structured = Some(ParsedJson::Serde(v));
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

    fn ev(msg: &str) -> LogEvent {
        LogEvent {
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
            message: Arc::from(msg),
            structured: None,
            level: None,
            palette_index: None,
            container_palette_index: None,
        }
    }

    #[tokio::test]
    async fn annotates_json_object_line() {
        let s = futures::stream::iter(vec![Ok(ev(r#"{"a":1}"#))]);
        let out: Vec<_> = json_annotate(s, false).collect().await;
        assert!(out[0].as_ref().unwrap().structured.is_some());
    }

    #[tokio::test]
    async fn annotates_jaq_val_when_requested() {
        let s = futures::stream::iter(vec![Ok(ev(r#"{"a":1}"#))]);
        let out: Vec<_> = json_annotate(s, true).collect().await;
        assert!(matches!(
            out[0].as_ref().unwrap().structured,
            Some(ParsedJson::Jaq(_))
        ));
    }

    #[tokio::test]
    async fn leaves_plain_text() {
        let s = futures::stream::iter(vec![Ok(ev("hello"))]);
        let out: Vec<_> = json_annotate(s, false).collect().await;
        assert!(out[0].as_ref().unwrap().structured.is_none());
    }
}
