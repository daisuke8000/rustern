use futures::stream::{Stream, StreamExt};
use seahash::SeaHasher;
use std::hash::{Hash, Hasher};

use crate::source::{LogEvent, LogSourceError, SourceMeta};

/// Knobs for [`color_assign`] (stern `--pod-colors` / `--container-colors` / `--diff-container`).
#[derive(Debug, Clone, Copy, Default)]
pub struct ColorAssignOpts {
    pub pod_colors: bool,
    pub container_colors: bool,
    pub diff_container: bool,
}

fn stable_palette_index(key: &str) -> u8 {
    let mut h = SeaHasher::new();
    key.hash(&mut h);
    (h.finish() % 16) as u8
}

fn container_palette_key(meta: &SourceMeta, opts: ColorAssignOpts) -> &str {
    if opts.diff_container {
        meta.container.as_str()
    } else {
        meta.pod.as_str()
    }
}

pub fn apply_palette_to_meta(meta: &mut SourceMeta, opts: ColorAssignOpts) {
    if opts.pod_colors {
        meta.palette_index = Some(stable_palette_index(&meta.pod));
    }
    if opts.container_colors {
        meta.container_palette_index =
            Some(stable_palette_index(container_palette_key(meta, opts)));
    }
}

pub fn color_assign<S>(
    inner: S,
    opts: ColorAssignOpts,
) -> impl Stream<Item = Result<LogEvent, LogSourceError>>
where
    S: Stream<Item = Result<LogEvent, LogSourceError>> + Send + 'static,
{
    inner.map(move |r| {
        r.map(|mut ev| {
            if opts.pod_colors && ev.palette_index.is_none() {
                ev.palette_index = Some(stable_palette_index(&ev.source.pod));
            }
            if opts.container_colors && ev.container_palette_index.is_none() {
                ev.container_palette_index = Some(stable_palette_index(container_palette_key(
                    &ev.source, opts,
                )));
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

    fn ev(pod: &str) -> LogEvent {
        ev_pod_container(pod, "c")
    }

    fn ev_pod_container(pod: &str, container: &str) -> LogEvent {
        LogEvent {
            source: Arc::new(SourceMeta {
                context: ContextName("ctx".into()),
                namespace: "ns".into(),
                pod: pod.into(),
                container: container.into(),
                kind: SourceKind::PodLog,
                node: None,
                labels: Arc::new(Labels::default()),
                uid: "u".into(),
                palette_index: None,
                container_palette_index: None,
            }),
            timestamp: Utc::now(),
            message: Arc::from("m"),
            structured: None,
            level: None,
            palette_index: None,
            container_palette_index: None,
        }
    }

    const ALL_COLORS: ColorAssignOpts = ColorAssignOpts {
        pod_colors: true,
        container_colors: true,
        diff_container: false,
    };

    #[tokio::test]
    async fn stable_per_pod_name() {
        let s = futures::stream::iter(vec![Ok(ev("web-1")), Ok(ev("web-1"))]);
        let out: Vec<_> = color_assign(s, ALL_COLORS).collect().await;
        let a = out[0].as_ref().unwrap().palette_index;
        let b = out[1].as_ref().unwrap().palette_index;
        assert_eq!(a, b);
    }

    #[tokio::test]
    async fn often_differs_between_pods() {
        let s = futures::stream::iter(vec![Ok(ev("pod-aaaaaa")), Ok(ev("pod-bbbbbb"))]);
        let out: Vec<_> = color_assign(s, ALL_COLORS).collect().await;
        let a = out[0].as_ref().unwrap().palette_index.unwrap();
        let b = out[1].as_ref().unwrap().palette_index.unwrap();
        assert_ne!(a, b);
    }

    #[tokio::test]
    async fn diff_container_uses_container_name() {
        let opts = ColorAssignOpts {
            pod_colors: false,
            container_colors: true,
            diff_container: true,
        };
        let s = futures::stream::iter(vec![
            Ok(ev_pod_container("p", "app")),
            Ok(ev_pod_container("p", "sidecar")),
        ]);
        let out: Vec<_> = color_assign(s, opts).collect().await;
        let a = out[0].as_ref().unwrap().container_palette_index.unwrap();
        let b = out[1].as_ref().unwrap().container_palette_index.unwrap();
        assert_ne!(a, b);
        assert!(out[0].as_ref().unwrap().palette_index.is_none());
    }

    #[tokio::test]
    async fn container_without_diff_matches_pod_slot() {
        let opts = ColorAssignOpts {
            pod_colors: true,
            container_colors: true,
            diff_container: false,
        };
        let s = futures::stream::iter(vec![Ok(ev("web-1"))]);
        let out: Vec<_> = color_assign(s, opts).collect().await;
        let e = out[0].as_ref().unwrap();
        assert_eq!(e.palette_index, e.container_palette_index);
    }
}
