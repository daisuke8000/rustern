use futures::stream::{Stream, StreamExt};
use serde_json::value::RawValue;

use crate::source::{LogEvent, LogSourceError};

pub fn json_annotate<S>(inner: S) -> impl Stream<Item = Result<LogEvent, LogSourceError>>
where
    S: Stream<Item = Result<LogEvent, LogSourceError>> + Send + 'static,
{
    inner.map(|r| {
        r.map(|mut ev| {
            if ev.message.trim_start().starts_with('{')
                && let Ok(rv) = RawValue::from_string(ev.message.to_string())
            {
                ev.structured = Some(rv);
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
        let out: Vec<_> = json_annotate(s).collect().await;
        assert!(out[0].as_ref().unwrap().structured.is_some());
    }

    #[tokio::test]
    async fn leaves_plain_text() {
        let s = futures::stream::iter(vec![Ok(ev("hello"))]);
        let out: Vec<_> = json_annotate(s).collect().await;
        assert!(out[0].as_ref().unwrap().structured.is_none());
    }
}
