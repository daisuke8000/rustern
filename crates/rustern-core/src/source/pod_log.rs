use std::pin::Pin;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use futures::AsyncBufRead;
use futures::io::AsyncBufReadExt;
use jiff::Timestamp;
use k8s_openapi::api::core::v1::Pod;
use kube::api::LogParams;
use kube::{Api, Client};
use tokio_util::sync::CancellationToken;

use super::retry::{full_jitter_backoff, kube_error_is_client_error};
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

        let meta_arc = Arc::clone(&meta);
        let stream = futures::stream::unfold(
            (reader, meta_arc, token.clone(), Vec::new(), false),
            |(mut reader, meta, token, mut buf, done)| async move {
                if done {
                    return None;
                }
                tokio::select! {
                    _ = token.cancelled() => None,
                    read = async {
                        buf.clear();
                        reader.read_until(b'\n', &mut buf).await
                    } => match read {
                        Ok(0) => None,
                        Ok(_) => {
                            let (ts, msg) = parse_log_line_bytes(&buf);
                            let ev = LogEvent {
                                source: meta.clone(),
                                timestamp: ts,
                                message: msg,
                                structured: None,
                                level: None,
                                palette_index: meta.palette_index,
                                container_palette_index: meta.container_palette_index,
                            };
                            Some((Ok(ev), (reader, meta, token, buf, false)))
                        }
                        Err(e) => Some((
                            Err(LogSourceError::Api(e.to_string())),
                            (reader, meta, token, buf, true),
                        )),
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
    loop {
        match api.log_stream(pod, params).await {
            Ok(s) => return Ok(Box::pin(s)),
            Err(e) if kube_error_is_client_error(&e) => {
                return Err(LogSourceError::Api(e.to_string()));
            }
            Err(e) if attempt + 1 >= max_attempts => {
                return Err(LogSourceError::Api(format!(
                    "after {} attempts: {}",
                    max_attempts, e
                )));
            }
            Err(e) => {
                tracing::warn!(error = %e, attempt, "log_stream failed, retrying");
                tokio::time::sleep(full_jitter_backoff(200, attempt)).await;
                attempt += 1;
            }
        }
    }
}

fn trim_line_end(mut raw: &[u8]) -> &[u8] {
    if raw.last() == Some(&b'\n') {
        raw = &raw[..raw.len() - 1];
    }
    if raw.last() == Some(&b'\r') {
        raw = &raw[..raw.len() - 1];
    }
    raw
}

/// Parse a kube log line with an optional RFC3339 prefix into `(timestamp, message)`.
pub fn parse_log_line(raw: &str) -> (DateTime<Utc>, Arc<str>) {
    parse_log_line_bytes(raw.as_bytes())
}

/// Parse a newline-terminated kube log line from a reusable read buffer.
pub fn parse_log_line_bytes(raw: &[u8]) -> (DateTime<Utc>, Arc<str>) {
    let raw = trim_line_end(raw);
    if let Some(sp) = raw.iter().position(|&b| b == b' ') {
        if let Ok(ts_s) = std::str::from_utf8(&raw[..sp]) {
            if let Ok(ts) = DateTime::parse_from_rfc3339(ts_s) {
                let msg_raw = &raw[sp + 1..];
                let msg: Arc<str> = match std::str::from_utf8(msg_raw) {
                    Ok(msg) => Arc::from(msg),
                    Err(_) => Arc::from(String::from_utf8_lossy(msg_raw).into_owned()),
                };
                return (ts.with_timezone(&Utc), msg);
            }
        }
    }
    match std::str::from_utf8(raw) {
        Ok(s) => (Utc::now(), Arc::from(s)),
        Err(_) => (
            Utc::now(),
            Arc::from(String::from_utf8_lossy(raw).into_owned()),
        ),
    }
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
            palette_index: None,
            container_palette_index: None,
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

    #[test]
    fn parse_log_line_bytes_matches_str_parser() {
        let line = b"2024-03-15T10:30:45Z hello world\n";
        let (ts_str, msg_str) = parse_log_line("2024-03-15T10:30:45Z hello world");
        let (ts_bytes, msg_bytes) = parse_log_line_bytes(line);
        assert_eq!(ts_str, ts_bytes);
        assert_eq!(msg_str, msg_bytes);
    }
}
