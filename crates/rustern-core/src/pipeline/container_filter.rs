use futures::stream::{Stream, StreamExt};
use regex::Regex;

use crate::source::{LogEvent, LogSourceError};

/// Filter streamed log lines to containers matching `include` but not any regex in `exclude`.
pub fn container_filter<S>(
    inner: S,
    include: Regex,
    exclude: Vec<Regex>,
) -> impl Stream<Item = Result<LogEvent, LogSourceError>>
where
    S: Stream<Item = Result<LogEvent, LogSourceError>> + Send + 'static,
{
    inner.filter_map(move |r| {
        let include = include.clone();
        let exclude = exclude.clone();
        async move {
            match r {
                Ok(ev) => {
                    let name = &ev.source.container;
                    if !include.is_match(name) {
                        return None;
                    }
                    if exclude.iter().any(|re| re.is_match(name)) {
                        return None;
                    }
                    Some(Ok(ev))
                }
                e => Some(e),
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::source::{ContextName, Labels, SourceKind, SourceMeta};
    use chrono::Utc;
    use std::sync::Arc;

    fn ev(container: &str) -> LogEvent {
        LogEvent {
            source: Arc::new(SourceMeta {
                context: ContextName("ctx".into()),
                namespace: "ns".into(),
                pod: "p".into(),
                container: container.into(),
                kind: SourceKind::PodLog,
                node: None,
                labels: Arc::new(Labels::default()),
                uid: "u1".into(),
            }),
            timestamp: Utc::now(),
            message: Arc::from("m"),
            structured: None,
            level: None,
            palette_index: None,
            container_palette_index: None,
        }
    }

    #[tokio::test]
    async fn keeps_matching_only() {
        let s = futures::stream::iter(vec![Ok(ev("app")), Ok(ev("sidecar"))]);
        let out: Vec<_> = container_filter(s, Regex::new("app").unwrap(), Vec::new())
            .collect()
            .await;
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].as_ref().unwrap().source.container, "app");
    }

    #[tokio::test]
    async fn excludes_when_pattern_set() {
        let s = futures::stream::iter(vec![Ok(ev("app")), Ok(ev("istio-proxy"))]);
        let out: Vec<_> = container_filter(
            s,
            Regex::new(".*").unwrap(),
            vec![Regex::new("istio-proxy").unwrap()],
        )
        .collect()
        .await;
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].as_ref().unwrap().source.container, "app");
    }
}
