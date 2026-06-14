//! In-memory per-stream cursor timestamps for `--cursor-reconnect`.

use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex, TryLockError};

use chrono::{DateTime, SecondsFormat, TimeDelta, Utc};
use jiff::Timestamp;

use crate::source::SourceKey;
use crate::source::pod_log::PodLogRequest;

pub(crate) const CURSOR_RECONNECT_OVERLAP: TimeDelta = TimeDelta::seconds(1);

const SHARD_COUNT: usize = 16;
const LOCK_RETRIES: usize = 16;

enum TryCursorMutOutcome {
    Success,
    Poisoned,
    Contended,
}

fn shard_index(key: &SourceKey) -> usize {
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    (hasher.finish() as usize) % SHARD_COUNT
}

/// Last-seen log line timestamp per stream, used to resume follow streams after EOF.
#[derive(Clone)]
pub(crate) struct ReconnectCursorStore {
    inner: Arc<Vec<Mutex<HashMap<SourceKey, DateTime<Utc>>>>>,
}

impl Default for ReconnectCursorStore {
    fn default() -> Self {
        Self::new()
    }
}

impl ReconnectCursorStore {
    pub(crate) fn new() -> Self {
        Self {
            inner: Arc::new(
                (0..SHARD_COUNT)
                    .map(|_| Mutex::new(HashMap::new()))
                    .collect(),
            ),
        }
    }

    pub(crate) async fn get(&self, key: &SourceKey) -> Option<DateTime<Utc>> {
        let shard = shard_index(key);
        for _ in 0..LOCK_RETRIES {
            match self.inner[shard].try_lock() {
                Ok(cursor) => return cursor.get(key).copied(),
                Err(TryLockError::Poisoned(e)) => {
                    tracing::warn!(key = ?key, error = %e, "reconnect_cursor lock poisoned, skipping get");
                    return None;
                }
                Err(TryLockError::WouldBlock) => {}
            }
            tokio::task::yield_now().await;
        }
        tracing::warn!(key = ?key, "reconnect_cursor lock contended, skipping get after retries");
        None
    }

    pub(crate) fn record(&self, key: &SourceKey, timestamp: DateTime<Utc>) {
        let key = key.clone();
        match self.try_cursor_mut_sync(&key, |cursor| {
            cursor.insert(key.clone(), timestamp);
        }) {
            TryCursorMutOutcome::Success | TryCursorMutOutcome::Poisoned => return,
            TryCursorMutOutcome::Contended => {}
        }
        tracing::warn!(
            key = ?key,
            ?timestamp,
            "reconnect_cursor lock contended, skipping record after retries"
        );
    }

    pub(crate) async fn remove(&self, key: &SourceKey) {
        let shard = shard_index(key);
        for _ in 0..LOCK_RETRIES {
            match self.inner[shard].try_lock() {
                Ok(mut cursor) => {
                    cursor.remove(key);
                    return;
                }
                Err(TryLockError::Poisoned(e)) => {
                    tracing::warn!(key = ?key, error = %e, "reconnect_cursor lock poisoned, skipping remove");
                    return;
                }
                Err(TryLockError::WouldBlock) => {}
            }
            tokio::task::yield_now().await;
        }
        tracing::warn!(key = ?key, "reconnect_cursor lock contended, skipping remove after retries");
    }

    fn try_cursor_mut_sync(
        &self,
        key: &SourceKey,
        mut op: impl FnMut(&mut HashMap<SourceKey, DateTime<Utc>>),
    ) -> TryCursorMutOutcome {
        let shard = shard_index(key);
        for _ in 0..LOCK_RETRIES {
            match self.inner[shard].try_lock() {
                Ok(mut cursor) => {
                    op(&mut cursor);
                    return TryCursorMutOutcome::Success;
                }
                Err(TryLockError::Poisoned(e)) => {
                    tracing::warn!(key = ?key, error = %e, "reconnect_cursor lock poisoned, skipping record");
                    return TryCursorMutOutcome::Poisoned;
                }
                Err(TryLockError::WouldBlock) => std::thread::yield_now(),
            }
        }
        TryCursorMutOutcome::Contended
    }
}

pub(crate) fn overlap_since_time(last_timestamp: DateTime<Utc>) -> Option<Timestamp> {
    let serialized = last_timestamp
        .checked_sub_signed(CURSOR_RECONNECT_OVERLAP)
        .unwrap_or(last_timestamp)
        .to_rfc3339_opts(SecondsFormat::Nanos, true);
    match serialized.parse::<Timestamp>() {
        Ok(ts) => Some(ts),
        Err(e) => {
            tracing::warn!(
                %last_timestamp,
                serialized = %serialized,
                error = %e,
                "failed to parse cursor overlap since_time"
            );
            None
        }
    }
}

pub(crate) fn pod_log_request_for_reopen(
    base: &PodLogRequest,
    last_timestamp: Option<DateTime<Utc>>,
    reopen: bool,
) -> PodLogRequest {
    pod_log_request_for_reopen_with_overlap(base, last_timestamp, reopen, overlap_since_time)
}

fn pod_log_request_for_reopen_with_overlap(
    base: &PodLogRequest,
    last_timestamp: Option<DateTime<Utc>>,
    reopen: bool,
    overlap: fn(DateTime<Utc>) -> Option<Timestamp>,
) -> PodLogRequest {
    if !reopen {
        return base.clone();
    }

    let mut request = base.clone();
    if let Some(since_time) = last_timestamp.and_then(overlap) {
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

    fn sample_key(pod: &str, uid: &str) -> SourceKey {
        SourceKey {
            context: crate::source::ContextName("ctx".into()),
            namespace: "ns".into(),
            pod: pod.into(),
            container: "c".into(),
            uid: uid.into(),
        }
    }

    fn keys_on_different_shards() -> (SourceKey, SourceKey) {
        let left = sample_key("pod-a", "uid-a");
        for i in 0..256 {
            let mut right = sample_key(&format!("pod-{i}"), &format!("uid-{i}"));
            right.container = format!("c-{i}");
            if shard_index(&left) != shard_index(&right) {
                return (left, right);
            }
        }
        panic!("could not find keys on different shards");
    }

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
    fn reopen_keeps_base_when_overlap_returns_none() {
        let base = PodLogRequest {
            follow: true,
            tail: Some(25),
            since_seconds: Some(300),
            ..Default::default()
        };
        let ts = Utc.with_ymd_and_hms(2026, 4, 28, 8, 0, 5).unwrap();
        let req = pod_log_request_for_reopen_with_overlap(&base, Some(ts), true, |_| None);

        assert_eq!(req.tail, Some(25));
        assert_eq!(req.since_seconds, Some(300));
        assert!(req.since_time.is_none());
    }

    #[tokio::test]
    async fn store_records_gets_and_removes_cursor() {
        let store = ReconnectCursorStore::new();
        let key = sample_key("p", "u");
        let ts = Utc.with_ymd_and_hms(2026, 4, 28, 8, 0, 5).unwrap();

        assert!(store.get(&key).await.is_none());
        store.record(&key, ts);
        assert_eq!(store.get(&key).await, Some(ts));
        store.remove(&key).await;
        assert!(store.get(&key).await.is_none());
    }

    #[tokio::test]
    async fn different_shards_record_and_get_independently() {
        let store = ReconnectCursorStore::new();
        let (key_a, key_b) = keys_on_different_shards();
        let ts_a = Utc.with_ymd_and_hms(2026, 4, 28, 8, 0, 1).unwrap();
        let ts_b = Utc.with_ymd_and_hms(2026, 4, 28, 8, 0, 2).unwrap();

        let store_a = store.clone();
        let store_b = store.clone();
        let key_a_c = key_a.clone();
        let key_b_c = key_b.clone();
        let (got_a, got_b) = tokio::join!(
            async move {
                store_a.record(&key_a_c, ts_a);
                store_a.get(&key_a_c).await
            },
            async move {
                store_b.record(&key_b_c, ts_b);
                store_b.get(&key_b_c).await
            },
        );

        assert_eq!(got_a, Some(ts_a));
        assert_eq!(got_b, Some(ts_b));
    }

    #[tokio::test]
    async fn same_shard_concurrent_records_last_write_visible() {
        let store = ReconnectCursorStore::new();
        let key = sample_key("shared", "uid-shared");
        let ts_early = Utc.with_ymd_and_hms(2026, 4, 28, 8, 0, 1).unwrap();
        let ts_late = Utc.with_ymd_and_hms(2026, 4, 28, 8, 0, 9).unwrap();

        let store_a = store.clone();
        let store_b = store.clone();
        let key_a = key.clone();
        let key_b = key.clone();
        tokio::join!(
            async move {
                store_a.record(&key_a, ts_early);
            },
            async move {
                store_b.record(&key_b, ts_late);
            },
        );

        let got = store.get(&key).await;
        assert!(got == Some(ts_early) || got == Some(ts_late));
    }
}
