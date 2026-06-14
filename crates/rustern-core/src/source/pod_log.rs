use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use futures::AsyncBufRead;
use futures::StreamExt;
use futures::io::{AsyncBufReadExt, BufReader};
use jiff::Timestamp;
use k8s_openapi::api::core::v1::Pod;
use kube::api::LogParams;
use kube::{Api, Client};
use tokio_util::sync::CancellationToken;

use super::{BoxedLogStream, LogEvent, LogSource, LogSourceError, SourceMeta};

/// Kubernetes log subresource knobs (maps to [`LogParams`]).
#[derive(Debug, Clone, Default)]
pub struct PodLogRequest {
    pub follow: bool,
    pub tail: Option<i64>,
    pub since_seconds: Option<i64>,
    pub since_time: Option<Timestamp>,
    pub previous: bool,
}

pub fn parse_since_time(raw: &str) -> Result<Timestamp, String> {
    let s = raw.trim();
    if s.is_empty() {
        return Err("empty --since-time".into());
    }
    s.parse::<Timestamp>()
        .map_err(|_| format!("invalid --since-time (expected RFC3339): {s}"))
}

/// Build `kube::api::LogParams` for the Kubernetes log API.
pub fn build_log_params(meta: &SourceMeta, req: &PodLogRequest) -> LogParams {
    LogParams {
        container: Some(meta.container.clone()),
        follow: req.follow,
        timestamps: true,
        tail_lines: req.tail,
        since_seconds: if req.since_time.is_some() {
            None
        } else {
            req.since_seconds
        },
        since_time: req.since_time,
        previous: req.previous,
        ..Default::default()
    }
}

/// Live pod log [`LogSource`] that streams newline-delimited events from kube.
pub struct PodLogSource {
    meta: Arc<SourceMeta>,
    token: CancellationToken,
    inner: BoxedLogStream,
}

impl PodLogSource {
    #[cfg(test)]
    pub fn new_stub(meta: SourceMeta, token: CancellationToken) -> Self {
        let meta = Arc::new(meta);
        let event = LogEvent {
            source: meta.clone(),
            timestamp: Utc::now(),
            message: std::sync::Arc::from("stub"),
            structured: None,
            level: None,
            palette_index: None,
            container_palette_index: None,
        };
        let s: Pin<Box<dyn futures::Stream<Item = Result<LogEvent, LogSourceError>> + Send>> =
            Box::pin(futures::stream::iter(vec![Ok(event)]));
        Self {
            meta,
            token,
            inner: s,
        }
    }

    /// Weak pointer to meta (tests assert drop after `pod_token.cancel()`).
    pub fn meta_weak(this: &Self) -> std::sync::Weak<SourceMeta> {
        Arc::downgrade(&this.meta)
    }

    /// Start tailing stdout/stderr logs for [`SourceMeta`] (connects kube log subresource).
    pub async fn start(
        client: Client,
        meta: Arc<SourceMeta>,
        token: CancellationToken,
        req: PodLogRequest,
    ) -> Result<Self, LogSourceError> {
        let api: Api<Pod> = Api::namespaced(client, &meta.namespace);
        let params = build_log_params(meta.as_ref(), &req);
        let reader = log_stream_with_retry(&api, &meta.pod, &params, 3).await?;
        let reader = BufReader::new(reader);
        let lines = reader.lines();

        let meta_arc = Arc::clone(&meta);
        let stream = futures::stream::unfold(
            (lines, meta_arc, token.clone()),
            |(mut lines, meta, token)| async move {
                tokio::select! {
                    _ = token.cancelled() => None,
                    line = lines.next() => match line {
                        Some(Ok(raw)) => {
                            let (ts, msg) = parse_log_line(&raw);
                            let ev = LogEvent {
                                source: meta.clone(),
                                timestamp: ts,
                                message: msg,
                                structured: None,
                                level: None,
                                palette_index: None,
                                container_palette_index: None,
                            };
                            Some((Ok(ev), (lines, meta, token)))
                        }
                        Some(Err(e)) => Some((
                            Err(LogSourceError::Api(e.to_string())),
                            (lines, meta, token),
                        )),
                        None => None,
                    },
                }
            },
        );

        Ok(Self {
            meta,
            token,
            inner: Box::pin(stream),
        })
    }
}

async fn log_stream_with_retry(
    api: &Api<Pod>,
    pod: &str,
    params: &LogParams,
    max_attempts: u32,
) -> Result<Pin<Box<dyn AsyncBufRead + Send>>, LogSourceError> {
    let mut attempt: u32 = 0;
    let mut backoff_ms: u64 = 200;
    loop {
        match api.log_stream(pod, params).await {
            Ok(s) => return Ok(Box::pin(s)),
            Err(e) if attempt + 1 >= max_attempts => {
                return Err(LogSourceError::Api(format!(
                    "after {} attempts: {}",
                    max_attempts, e
                )));
            }
            Err(e) => {
                tracing::warn!(error = %e, attempt, "log_stream failed, retrying");
                tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                attempt += 1;
                backoff_ms = backoff_ms.saturating_mul(2);
            }
        }
    }
}

/// Parse a kube log line with an optional RFC3339 prefix into `(timestamp, message)`.
pub fn parse_log_line(raw: &str) -> (DateTime<Utc>, Arc<str>) {
    if let Some((ts_s, msg)) = raw.split_once(' ')
        && let Ok(ts) = DateTime::parse_from_rfc3339(ts_s)
    {
        return (ts.with_timezone(&Utc), Arc::from(msg));
    }
    (Utc::now(), Arc::from(raw))
}

impl LogSource for PodLogSource {
    fn meta(&self) -> &SourceMeta {
        &self.meta
    }
    fn cancellation_token(&self) -> CancellationToken {
        self.token.clone()
    }
    fn into_stream(self: Box<Self>) -> BoxedLogStream {
        self.inner
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::source::{ContextName, Labels, SourceKind, SourceMeta};

    fn sample_meta() -> SourceMeta {
        SourceMeta {
            context: ContextName("c".into()),
            namespace: "ns".into(),
            pod: "p".into(),
            container: "app".into(),
            kind: SourceKind::PodLog,
            node: None,
            labels: Arc::new(Labels::default()),
            uid: "uid".into(),
        }
    }

    #[test]
    fn build_log_params_sets_previous_and_since_time() {
        let ts: Timestamp = "2024-03-15T10:30:45Z".parse().unwrap();
        let params = build_log_params(
            &sample_meta(),
            &PodLogRequest {
                follow: true,
                tail: Some(100),
                since_time: Some(ts),
                previous: true,
                ..Default::default()
            },
        );
        assert!(params.previous);
        assert!(params.since_time.is_some());
        assert!(params.since_seconds.is_none());
        assert_eq!(params.tail_lines, Some(100));
    }

    #[test]
    fn build_log_params_uses_since_seconds_when_no_since_time() {
        let params = build_log_params(
            &sample_meta(),
            &PodLogRequest {
                since_seconds: Some(300),
                ..Default::default()
            },
        );
        assert_eq!(params.since_seconds, Some(300));
        assert!(params.since_time.is_none());
    }

    #[test]
    fn parse_since_time_accepts_rfc3339() {
        parse_since_time("2024-03-15T10:30:45Z").unwrap();
        assert!(parse_since_time("not-a-time").is_err());
        assert!(parse_since_time("").is_err());
    }

    #[test]
    fn parse_log_line_strips_rfc3339_prefix() {
        let (ts, msg) = parse_log_line("2024-03-15T10:30:45Z hello world");
        assert_eq!(
            ts,
            DateTime::parse_from_rfc3339("2024-03-15T10:30:45Z")
                .unwrap()
                .with_timezone(&Utc)
        );
        assert_eq!(&*msg, "hello world");
    }

    #[test]
    fn parse_log_line_fallback_without_timestamp() {
        let (_, msg) = parse_log_line("plain line without prefix");
        assert_eq!(&*msg, "plain line without prefix");
    }
}
