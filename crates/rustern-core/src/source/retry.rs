use std::time::Duration;

const MAX_BACKOFF_MS: u64 = 30_000;

/// Exponential cap with full jitter in `[0, min(base * 2^attempt, MAX_BACKOFF_MS)]`.
pub fn full_jitter_backoff(base_ms: u64, attempt: u32) -> Duration {
    let cap = base_ms
        .saturating_mul(1u64 << attempt.min(8))
        .min(MAX_BACKOFF_MS);
    let jitter_ms = if cap == 0 { 0 } else { fastrand::u64(0..=cap) };
    Duration::from_millis(jitter_ms)
}

pub fn kube_error_is_client_error(err: &kube::Error) -> bool {
    matches!(
        err,
        kube::Error::Api(ae) if (400..500).contains(&ae.code) && ae.code != 429
    )
}
