use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use futures::AsyncBufRead;
use futures::StreamExt;
use futures::io::{AsyncBufReadExt, BufReader};
use k8s_openapi::api::core::v1::Pod;
use kube::api::LogParams;
use kube::{Api, Client};
use tokio_util::sync::CancellationToken;

use super::{BoxedLogStream, LogEvent, LogSource, LogSourceError, SourceMeta};

/// Build `kube::api::LogParams` for the Kubernetes log API.
pub fn build_log_params(
    meta: &SourceMeta,
    follow: bool,
    tail: Option<i64>,
    since: Option<i64>,
) -> LogParams {
    LogParams {
        container: Some(meta.container.clone()),
        follow,
        timestamps: true,
        tail_lines: tail,
        since_seconds: since,
        ..Default::default()
    }
}

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

    pub async fn start(
        client: Client,
        meta: SourceMeta,
        token: CancellationToken,
        follow: bool,
        tail: Option<i64>,
        since: Option<i64>,
    ) -> Result<Self, LogSourceError> {
        let api: Api<Pod> = Api::namespaced(client, &meta.namespace);
        let params = build_log_params(&meta, follow, tail, since);
        let reader = log_stream_with_retry(&api, &meta.pod, &params, 3).await?;
        let reader = BufReader::new(reader);
        let lines = reader.lines();

        let meta_arc = Arc::new(meta.clone());
        let stream = futures::stream::unfold(
            (lines, meta_arc, token.clone()),
            |(mut lines, meta, token)| async move {
                tokio::select! {
                    _ = token.cancelled() => None,
                    line = lines.next() => match line {
                        Some(Ok(raw)) => {
                            let (ts, msg) = parse_timestamp(&raw);
                            let ev = LogEvent {
                                source: meta.clone(),
                                timestamp: ts,
                                message: Arc::from(msg),
                                structured: None,
                                level: None,
                                palette_index: None,
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
            meta: Arc::new(meta),
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

fn parse_timestamp(raw: &str) -> (DateTime<Utc>, String) {
    if let Some((ts_s, msg)) = raw.split_once(' ')
        && let Ok(ts) = DateTime::parse_from_rfc3339(ts_s)
    {
        return (ts.with_timezone(&Utc), msg.to_string());
    }
    (Utc::now(), raw.to_string())
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
