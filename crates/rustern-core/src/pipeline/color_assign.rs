use futures::stream::{Stream, StreamExt};
use seahash::SeaHasher;
use std::hash::{Hash, Hasher};

use crate::source::{LogEvent, LogSourceError};

fn stable_palette_index(pod: &str) -> u8 {
    let mut h = SeaHasher::new();
    pod.hash(&mut h);
    (h.finish() % 16) as u8
}

pub fn color_assign<S>(inner: S) -> impl Stream<Item = Result<LogEvent, LogSourceError>>
where
    S: Stream<Item = Result<LogEvent, LogSourceError>> + Send + 'static,
{
    inner.map(|r| {
        r.map(|mut ev| {
            ev.palette_index = Some(stable_palette_index(&ev.source.pod));
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

    fn ev(pod: &str) -> LogEvent {
        LogEvent {
            source: Arc::new(SourceMeta {
                context: ContextName("ctx".into()),
                namespace: "ns".into(),
                pod: pod.into(),
                container: "c".into(),
                kind: SourceKind::PodLog,
                node: None,
                labels: Arc::new(Labels::default()),
                uid: "u".into(),
            }),
            timestamp: Utc::now(),
            message: Arc::from("m"),
            structured: None,
            level: None,
            palette_index: None,
        }
    }

    #[tokio::test]
    async fn stable_per_pod_name() {
        let s = futures::stream::iter(vec![Ok(ev("web-1")), Ok(ev("web-1"))]);
        let out: Vec<_> = color_assign(s).collect().await;
        let a = out[0].as_ref().unwrap().palette_index;
        let b = out[1].as_ref().unwrap().palette_index;
        assert_eq!(a, b);
    }

    #[tokio::test]
    async fn often_differs_between_pods() {
        let s = futures::stream::iter(vec![Ok(ev("pod-aaaaaa")), Ok(ev("pod-bbbbbb"))]);
        let out: Vec<_> = color_assign(s).collect().await;
        let a = out[0].as_ref().unwrap().palette_index.unwrap();
        let b = out[1].as_ref().unwrap().palette_index.unwrap();
        assert_ne!(a, b);
    }
}
