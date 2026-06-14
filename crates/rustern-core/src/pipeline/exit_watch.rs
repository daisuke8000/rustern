use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use futures::stream::{Stream, StreamExt};
use regex::Regex;
use tokio_util::sync::CancellationToken;

use crate::source::{LogEvent, LogLevel, LogSourceError};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExitOnLevel {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
}

impl ExitOnLevel {
    pub fn parse(raw: &str) -> Result<Self, String> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "trace" => Ok(Self::Trace),
            "debug" => Ok(Self::Debug),
            "info" => Ok(Self::Info),
            "warn" | "warning" => Ok(Self::Warn),
            "error" | "err" | "fatal" => Ok(Self::Error),
            "" => Err("empty --exit-on-level".into()),
            other => Err(format!("invalid --exit-on-level: {other}")),
        }
    }

    fn rank(self) -> u8 {
        match self {
            Self::Trace => 0,
            Self::Debug => 1,
            Self::Info => 2,
            Self::Warn => 3,
            Self::Error => 4,
        }
    }
}

fn event_level_rank(level: &LogLevel) -> u8 {
    match level {
        LogLevel::Trace => 0,
        LogLevel::Debug => 1,
        LogLevel::Info => 2,
        LogLevel::Warn => 3,
        LogLevel::Error => 4,
        LogLevel::Other(s) => match s.to_ascii_lowercase().as_str() {
            "error" | "err" | "fatal" => 4,
            "warn" | "warning" => 3,
            "info" => 2,
            "debug" => 1,
            "trace" => 0,
            _ => 2,
        },
    }
}

#[derive(Clone)]
pub struct ExitWatchState {
    token: CancellationToken,
    triggered: Arc<AtomicBool>,
}

impl ExitWatchState {
    pub fn new(token: CancellationToken) -> Self {
        Self {
            token,
            triggered: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn triggered(&self) -> bool {
        self.triggered.load(Ordering::SeqCst)
    }

    fn fire(&self) {
        if !self.triggered.swap(true, Ordering::SeqCst) {
            self.token.cancel();
        }
    }
}

pub fn exit_watch_message<S, P>(
    inner: S,
    patterns: P,
    state: ExitWatchState,
) -> impl Stream<Item = Result<LogEvent, LogSourceError>>
where
    S: Stream<Item = Result<LogEvent, LogSourceError>> + Send + 'static,
    P: Into<Arc<[Regex]>>,
{
    let patterns: Arc<[Regex]> = patterns.into();
    inner.then(move |r| {
        let state = state.clone();
        let patterns = Arc::clone(&patterns);
        async move {
            if let Ok(ref ev) = r
                && patterns.iter().any(|re| re.is_match(ev.message.as_ref()))
            {
                state.fire();
            }
            r
        }
    })
}

pub fn exit_watch_level<S>(
    inner: S,
    min_level: ExitOnLevel,
    state: ExitWatchState,
) -> impl Stream<Item = Result<LogEvent, LogSourceError>>
where
    S: Stream<Item = Result<LogEvent, LogSourceError>> + Send + 'static,
{
    let threshold = min_level.rank();
    inner.then(move |r| {
        let state = state.clone();
        async move {
            if let Ok(ref ev) = r
                && let Some(ref lv) = ev.level
                && event_level_rank(lv) >= threshold
            {
                state.fire();
            }
            r
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::source::{ContextName, Labels, SourceKind, SourceMeta};
    use chrono::Utc;
    use futures::StreamExt;
    use std::sync::Arc;

    fn ev(msg: &str, level: Option<LogLevel>) -> LogEvent {
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
            level,
            palette_index: None,
            container_palette_index: None,
        }
    }

    #[tokio::test]
    async fn message_match_triggers_cancel() {
        let token = CancellationToken::new();
        let state = ExitWatchState::new(token.clone());
        let patterns = vec![Regex::new("panic").unwrap()];
        let s = futures::stream::iter(vec![Ok(ev("oh no panic", None))]);
        let out: Vec<_> = exit_watch_message(s, patterns, state.clone())
            .collect()
            .await;
        assert_eq!(out.len(), 1);
        assert!(state.triggered());
        assert!(token.is_cancelled());
    }

    #[tokio::test]
    async fn message_no_match_does_not_trigger() {
        let token = CancellationToken::new();
        let state = ExitWatchState::new(token.clone());
        let patterns = vec![Regex::new("panic").unwrap()];
        let s = futures::stream::iter(vec![Ok(ev("all fine", None))]);
        let _: Vec<_> = exit_watch_message(s, patterns, state.clone())
            .collect()
            .await;
        assert!(!state.triggered());
        assert!(!token.is_cancelled());
    }

    #[tokio::test]
    async fn level_warn_matches_warn_and_error() {
        let token = CancellationToken::new();
        let state = ExitWatchState::new(token.clone());
        let s = futures::stream::iter(vec![
            Ok(ev("a", Some(LogLevel::Info))),
            Ok(ev("b", Some(LogLevel::Warn))),
            Ok(ev("c", Some(LogLevel::Error))),
        ]);
        let out: Vec<_> = exit_watch_level(s, ExitOnLevel::Warn, state.clone())
            .collect()
            .await;
        assert_eq!(out.len(), 3);
        assert!(state.triggered());
        assert!(token.is_cancelled());
    }

    #[tokio::test]
    async fn level_info_does_not_match_debug() {
        let token = CancellationToken::new();
        let state = ExitWatchState::new(token.clone());
        let s = futures::stream::iter(vec![Ok(ev("a", Some(LogLevel::Debug)))]);
        let _: Vec<_> = exit_watch_level(s, ExitOnLevel::Info, state.clone())
            .collect()
            .await;
        assert!(!state.triggered());
    }

    #[test]
    fn parses_exit_on_level_aliases() {
        assert_eq!(ExitOnLevel::parse("warn").unwrap(), ExitOnLevel::Warn);
        assert_eq!(ExitOnLevel::parse("WARNING").unwrap(), ExitOnLevel::Warn);
        assert!(ExitOnLevel::parse("bogus").is_err());
    }
}
