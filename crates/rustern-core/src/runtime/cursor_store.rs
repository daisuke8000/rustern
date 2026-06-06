//! In-memory per-stream cursor timestamps for `--cursor-reconnect`.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use chrono::{DateTime, SecondsFormat, TimeDelta, Utc};
use jiff::Timestamp;

use crate::source::SourceKey;
use crate::source::pod_log::PodLogRequest;

pub(crate) const CURSOR_RECONNECT_OVERLAP: TimeDelta = TimeDelta::seconds(1);

/// Last-seen log line timestamp per stream, used to resume follow streams after EOF.
#[derive(Clone, Default)]
pub(crate) struct ReconnectCursorStore {
    inner: Arc<Mutex<HashMap<SourceKey, DateTime<Utc>>>>,
}

impl ReconnectCursorStore {
    pub(crate) fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub(crate) fn get(&self, key: &SourceKey) -> Option<DateTime<Utc>> {
        self.inner.lock().ok()?.get(key).copied()
    }

    pub(crate) fn record(&self, key: &SourceKey, timestamp: DateTime<Utc>) {
        if let Ok(mut cursor) = self.inner.lock() {
            cursor.insert(key.clone(), timestamp);
        }
    }

    pub(crate) fn remove(&self, key: &SourceKey) {
        if let Ok(mut cursor) = self.inner.try_lock() {
            cursor.remove(key);
        } else {
            tracing::trace!(key = ?key, "reconnect_cursor lock contended, skipping cursor cleanup");
        }
    }
}

pub(crate) fn overlap_since_time(last_timestamp: DateTime<Utc>) -> Option<Timestamp> {
    last_timestamp
        .checked_sub_signed(CURSOR_RECONNECT_OVERLAP)
        .unwrap_or(last_timestamp)
        .to_rfc3339_opts(SecondsFormat::Nanos, true)
        .parse()
        .ok()
}

pub(crate) fn pod_log_request_for_reopen(
    base: &PodLogRequest,
    last_timestamp: Option<DateTime<Utc>>,
    reopen: bool,
) -> PodLogRequest {
    if !reopen {
        return base.clone();
    }

    let mut request = base.clone();
    if let Some(since_time) = last_timestamp.and_then(overlap_since_time) {
        request.tail = None;
        request.since_seconds = None;
        request.since_time = Some(since_time);
    }
    request
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn reopen_without_cursor_keeps_initial_tail_and_since() {
        let base = PodLogRequest {
            follow: true,
            tail: Some(25),
            since_seconds: Some(300),
            ..Default::default()
        };
        let req = pod_log_request_for_reopen(&base, None, true);

        assert_eq!(req.tail, Some(25));
        assert_eq!(req.since_seconds, Some(300));
        assert!(req.since_time.is_none());
    }

    #[test]
    fn reconnect_request_uses_overlap_and_drops_tail_and_since() {
        let ts = Utc.with_ymd_and_hms(2026, 4, 28, 8, 0, 5).unwrap();
        let req = pod_log_request_for_reopen(
            &PodLogRequest {
                follow: true,
                tail: Some(25),
                since_seconds: Some(300),
                ..Default::default()
            },
            Some(ts),
            true,
        );

        assert!(req.follow);
        assert!(req.tail.is_none());
        assert!(req.since_seconds.is_none());
        let overlap_ts = req.since_time.unwrap().to_string();
        assert!(overlap_ts.contains("2026-04-28T08:00:04"));
    }

    #[test]
    fn store_records_gets_and_removes_cursor() {
        let store = ReconnectCursorStore::new();
        let key = SourceKey {
            context: crate::source::ContextName("ctx".into()),
            namespace: "ns".into(),
            pod: "p".into(),
            container: "c".into(),
            uid: "u".into(),
        };
        let ts = Utc.with_ymd_and_hms(2026, 4, 28, 8, 0, 5).unwrap();

        assert!(store.get(&key).is_none());
        store.record(&key, ts);
        assert_eq!(store.get(&key), Some(ts));
        store.remove(&key);
        assert!(store.get(&key).is_none());
    }
}
