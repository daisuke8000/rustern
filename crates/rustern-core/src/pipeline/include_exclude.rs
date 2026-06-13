use std::sync::Arc;

use futures::stream::{Stream, StreamExt};
use regex::Regex;

use crate::source::{LogEvent, LogSourceError};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FilterOn {
    Transformed,
    Original,
}

pub fn include_exclude<S>(
    inner: S,
    includes: Arc<[Regex]>,
    excludes: Arc<[Regex]>,
) -> impl Stream<Item = Result<LogEvent, LogSourceError>>
where
    S: Stream<Item = Result<LogEvent, LogSourceError>> + Send + 'static,
{
    inner.filter_map(move |r| {
        let includes = Arc::clone(&includes);
        let excludes = Arc::clone(&excludes);
        async move {
            match r {
                Ok(ev) => {
                    let msg = ev.message.as_ref();
                    let include_ok =
                        includes.is_empty() || includes.iter().any(|re| re.is_match(msg));
                    if !include_ok {
                        return None;
                    }
                    if !includes.is_empty() {
                        return Some(Ok(ev));
                    }
                    if excludes.iter().any(|re| re.is_match(msg)) {
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
    async fn includes_matching_message() {
        let s = futures::stream::iter(vec![Ok(ev("boom error")), Ok(ev("boom info"))]);
        let incl: Arc<[Regex]> = Arc::from(vec![Regex::new("error").unwrap()]);
        let out: Vec<_> = include_exclude(s, incl, Arc::from([])).collect().await;
        assert_eq!(out.len(), 1);
        assert_eq!(&*out[0].as_ref().unwrap().message, "boom error");
    }

    #[tokio::test]
    async fn excludes_matching_message() {
        let s = futures::stream::iter(vec![Ok(ev("GET /healthz")), Ok(ev("POST /api"))]);
        let excl: Arc<[Regex]> = Arc::from(vec![Regex::new("health").unwrap()]);
        let out: Vec<_> = include_exclude(s, Arc::from([]), excl).collect().await;
        assert_eq!(out.len(), 1);
        assert_eq!(&*out[0].as_ref().unwrap().message, "POST /api");
    }

    #[tokio::test]
    async fn include_takes_priority_over_exclude_when_both_match() {
        let s = futures::stream::iter(vec![Ok(ev("error and /healthz"))]);
        let incl: Arc<[Regex]> = Arc::from(vec![Regex::new("error").unwrap()]);
        let excl: Arc<[Regex]> = Arc::from(vec![Regex::new("health").unwrap()]);
        let out: Vec<_> = include_exclude(s, incl, excl).collect().await;
        assert_eq!(out.len(), 1);
    }
}
