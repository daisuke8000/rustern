//! In-memory per-stream cursor timestamps for `--cursor-reconnect`.
//!
//! Stream attach records the latest line timestamp through an unbounded channel so
//! log streams never touch [`ReconnectCursorStore`] locks inside `Stream::poll_next`.
//! A dedicated processor task ([`run_cursor_update_processor`]) applies updates.

use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use chrono::{DateTime, SecondsFormat, TimeDelta, Utc};
use jiff::Timestamp;
use tokio::sync::{RwLock, mpsc};
use tokio_util::sync::CancellationToken;

use crate::source::SourceKey;
use crate::source::pod_log::PodLogRequest;

pub(crate) const CURSOR_RECONNECT_OVERLAP: TimeDelta = TimeDelta::seconds(1);

const SHARD_COUNT: usize = 16;

/// Cursor advance emitted from a log stream; processed asynchronously.
#[derive(Debug, Clone)]
pub(crate) struct CursorUpdate {
    pub(crate) key: SourceKey,
    pub(crate) timestamp: DateTime<Utc>,
}

#[derive(Debug)]
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) struct CursorStoreStats {
    cursor_updates: AtomicU64,
    cursor_gets: AtomicU64,
}

#[cfg_attr(not(test), allow(dead_code))]
impl CursorStoreStats {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            cursor_updates: AtomicU64::new(0),
            cursor_gets: AtomicU64::new(0),
        })
    }

    pub(crate) fn cursor_updates(&self) -> u64 {
        self.cursor_updates.load(Ordering::Relaxed)
    }

    pub(crate) fn cursor_gets(&self) -> u64 {
        self.cursor_gets.load(Ordering::Relaxed)
    }
}

fn shard_index(key: &SourceKey) -> usize {
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    (hasher.finish() as usize) % SHARD_COUNT
}

/// Last-seen log line timestamp per stream, used to resume follow streams after EOF.
#[derive(Clone)]
pub(crate) struct ReconnectCursorStore {
    inner: Arc<Vec<RwLock<HashMap<SourceKey, DateTime<Utc>>>>>,
    stats: Option<Arc<CursorStoreStats>>,
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
                    .map(|_| RwLock::new(HashMap::new()))
                    .collect(),
            ),
            stats: None,
        }
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn new_with_stats(stats: Arc<CursorStoreStats>) -> Self {
        Self {
            inner: Arc::new(
                (0..SHARD_COUNT)
                    .map(|_| RwLock::new(HashMap::new()))
                    .collect(),
            ),
            stats: Some(stats),
        }
    }

    pub(crate) async fn get(&self, key: &SourceKey) -> Option<DateTime<Utc>> {
        if let Some(stats) = &self.stats {
            stats.cursor_gets.fetch_add(1, Ordering::Relaxed);
        }
        let shard = shard_index(key);
        self.inner[shard].read().await.get(key).copied()
    }

    pub(crate) async fn record(&self, key: &SourceKey, timestamp: DateTime<Utc>) {
        if let Some(stats) = &self.stats {
            stats.cursor_updates.fetch_add(1, Ordering::Relaxed);
        }
        let key = key.clone();
        let shard = shard_index(&key);
        self.inner[shard].write().await.insert(key, timestamp);
    }

    pub(crate) async fn remove(&self, key: &SourceKey) {
        let shard = shard_index(key);
        self.inner[shard].write().await.remove(key);
    }
}

pub(crate) async fn run_cursor_update_processor(
    mut rx: mpsc::UnboundedReceiver<CursorUpdate>,
    store: ReconnectCursorStore,
    token: CancellationToken,
) {
    loop {
        tokio::select! {
            biased;
            _ = token.cancelled() => break,
            update = rx.recv() => {
                let Some(update) = update else { break };
                store.record(&update.key, update.timestamp).await;
            }
        }
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

    fn keys_on_same_shard(count: usize) -> Vec<SourceKey> {
        assert!(count > 0, "count must be positive");
        let anchor = sample_key("shard-anchor", "uid-anchor");
        let target_shard = shard_index(&anchor);
        let mut keys = Vec::with_capacity(count);
        for i in 0.. {
            let mut key = sample_key(&format!("pod-{i}"), &format!("uid-{i}"));
            key.container = format!("c-{i}");
            if shard_index(&key) == target_shard {
                keys.push(key);
                if keys.len() == count {
                    return keys;
                }
            }
            if i > 4096 {
                panic!("could not collect {count} keys on same shard");
            }
        }
        unreachable!()
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
        store.record(&key, ts).await;
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
                store_a.record(&key_a_c, ts_a).await;
                store_a.get(&key_a_c).await
            },
            async move {
                store_b.record(&key_b_c, ts_b).await;
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
                store_a.record(&key_a, ts_early).await;
            },
            async move {
                store_b.record(&key_b, ts_late).await;
            },
        );

        let got = store.get(&key).await;
        assert!(got == Some(ts_early) || got == Some(ts_late));
    }

    #[tokio::test]
    async fn processor_applies_channel_updates_to_store() {
        let store = ReconnectCursorStore::new();
        let key = sample_key("p", "u");
        let ts = Utc.with_ymd_and_hms(2026, 4, 28, 8, 0, 5).unwrap();
        let (tx, rx) = mpsc::unbounded_channel();
        let processor = tokio::spawn(run_cursor_update_processor(
            rx,
            store.clone(),
            CancellationToken::new(),
        ));

        tx.send(CursorUpdate {
            key: key.clone(),
            timestamp: ts,
        })
        .expect("send update");

        let deadline = tokio::time::Instant::now() + std::time::Duration::from_millis(500);
        loop {
            if store.get(&key).await == Some(ts) {
                break;
            }
            if tokio::time::Instant::now() >= deadline {
                panic!("cursor update not applied within deadline");
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }

        drop(tx);
        processor.await.expect("processor task");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn same_shard_high_concurrency_records_all_keys() {
        const TASK_COUNT: usize = 64;
        let store = ReconnectCursorStore::new();
        let keys = keys_on_same_shard(TASK_COUNT);
        let base_ts = Utc.with_ymd_and_hms(2026, 4, 28, 8, 0, 0).unwrap();

        let mut join_set = tokio::task::JoinSet::new();
        for (i, key) in keys.iter().cloned().enumerate() {
            let store = store.clone();
            let ts = base_ts + chrono::Duration::milliseconds(i as i64);
            join_set.spawn(async move {
                store.record(&key, ts).await;
                (key, ts)
            });
        }

        let mut expected: HashMap<SourceKey, DateTime<Utc>> = HashMap::new();
        while let Some(result) = join_set.join_next().await {
            let (key, ts) = result.expect("record task");
            expected.insert(key, ts);
        }

        for (key, ts) in expected {
            assert_eq!(store.get(&key).await, Some(ts));
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn processor_applies_many_same_shard_updates() {
        const UPDATE_COUNT: usize = 200;
        let store = ReconnectCursorStore::new();
        let keys = keys_on_same_shard(UPDATE_COUNT);
        let base_ts = Utc.with_ymd_and_hms(2026, 4, 28, 9, 0, 0).unwrap();
        let (tx, rx) = mpsc::unbounded_channel();
        let processor = tokio::spawn(run_cursor_update_processor(
            rx,
            store.clone(),
            CancellationToken::new(),
        ));

        let expected: Vec<(SourceKey, DateTime<Utc>)> = keys
            .into_iter()
            .enumerate()
            .map(|(i, key)| {
                let ts = base_ts + chrono::Duration::milliseconds(i as i64);
                (key, ts)
            })
            .collect();

        for (key, ts) in &expected {
            tx.send(CursorUpdate {
                key: key.clone(),
                timestamp: *ts,
            })
            .expect("send update");
        }
        drop(tx);
        processor.await.expect("processor task");

        for (key, ts) in expected {
            assert_eq!(store.get(&key).await, Some(ts));
        }
    }

    #[tokio::test]
    async fn processor_exits_promptly_on_cancellation() {
        let store = ReconnectCursorStore::new();
        let token = CancellationToken::new();
        let (_tx, rx) = mpsc::unbounded_channel();
        let processor = tokio::spawn(run_cursor_update_processor(rx, store, token.clone()));

        token.cancel();
        tokio::time::timeout(std::time::Duration::from_millis(150), processor)
            .await
            .expect("cursor processor did not exit after cancellation")
            .unwrap();
    }

    #[tokio::test]
    async fn stats_count_updates_and_gets() {
        let stats = CursorStoreStats::new();
        let store = ReconnectCursorStore::new_with_stats(stats.clone());
        let key = sample_key("p", "u");
        let ts = Utc.with_ymd_and_hms(2026, 4, 28, 8, 0, 5).unwrap();

        store.record(&key, ts).await;
        store.get(&key).await;
        store.get(&key).await;

        assert_eq!(stats.cursor_updates(), 1);
        assert_eq!(stats.cursor_gets(), 2);
    }
}
