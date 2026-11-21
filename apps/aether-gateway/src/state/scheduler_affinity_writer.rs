//! Coalescing writer for the scheduler-affinity runtime copies.
//!
//! The runtime KV copy of an affinity entry is a read-modify-write: read the
//! stored JSON, bump `request_count`, write it back. Doing that in a detached task
//! per request loses updates (two requests read the same count, both write `n+1`)
//! and puts an unbounded number of in-flight KV round trips on the runtime store
//! under load.
//!
//! This module funnels those updates through one writer task: requests enqueue a
//! counter increment, the task drains the queue in bounded batches and performs one
//! read-modify-write per key. The count stays best-effort metadata (it is only used
//! for observability), but it no longer loses most of its increments and the
//! number of concurrent KV writers is capped at one.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex};

use aether_runtime_state::RuntimeState;

use tracing::debug;

use aether_scheduler_core::SchedulerAffinityTarget;

/// Upper bound on keys handled per batch, so one hot key cannot starve the rest
/// and the writer keeps re-checking the epoch between batches.
const MAX_KEYS_PER_BATCH: usize = 64;

#[derive(Debug, Clone)]
struct PendingWrite {
    target: SchedulerAffinityTarget,
    ttl_seconds: u64,
    created_at_unix_secs: u64,
    epoch: u64,
    delta: u64,
}

static PENDING: LazyLock<Mutex<HashMap<String, PendingWrite>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));
static WRITER_RUNNING: AtomicBool = AtomicBool::new(false);

/// Records one more request for `cache_key` and makes sure a writer is running.
///
/// Coalesces with anything already queued for the same key, so N requests in the
/// same window cost one read-modify-write instead of N.
pub(crate) fn enqueue(
    runtime_state: Arc<RuntimeState>,
    scheduler_affinity_epoch: Arc<AtomicU64>,
    cache_key: &str,
    target: &SchedulerAffinityTarget,
    ttl: std::time::Duration,
    epoch: u64,
    now_unix_secs: u64,
) {
    let Ok(handle) = tokio::runtime::Handle::try_current() else {
        return;
    };
    {
        let Ok(mut pending) = PENDING.lock() else {
            return;
        };
        merge_into(
            &mut pending,
            cache_key,
            target,
            ttl.as_secs(),
            epoch,
            now_unix_secs,
        );
    }
    if WRITER_RUNNING.swap(true, Ordering::AcqRel) {
        return;
    }
    handle.spawn(run_writer(runtime_state, scheduler_affinity_epoch));
}

fn merge_into(
    pending: &mut HashMap<String, PendingWrite>,
    cache_key: &str,
    target: &SchedulerAffinityTarget,
    ttl_seconds: u64,
    epoch: u64,
    now_unix_secs: u64,
) {
    {
        match pending.get_mut(cache_key) {
            Some(entry) if entry.epoch == epoch => {
                entry.delta = entry.delta.saturating_add(1);
                entry.target = target.clone();
                entry.ttl_seconds = ttl_seconds.max(entry.ttl_seconds);
            }
            _ => {
                pending.insert(
                    cache_key.to_string(),
                    PendingWrite {
                        target: target.clone(),
                        ttl_seconds,
                        created_at_unix_secs: now_unix_secs,
                        epoch,
                        delta: 1,
                    },
                );
            }
        }
    }
}

async fn run_writer(runtime_state: Arc<RuntimeState>, scheduler_affinity_epoch: Arc<AtomicU64>) {
    loop {
        let batch = take_batch();
        if batch.is_empty() {
            // Publish "not running" before re-checking: an enqueue that lands
            // between the two steps either starts a writer or finds this one still
            // marked running, so no update can be stranded.
            WRITER_RUNNING.store(false, Ordering::Release);
            if has_pending() && !WRITER_RUNNING.swap(true, Ordering::AcqRel) {
                continue;
            }
            return;
        }
        for (cache_key, pending) in batch {
            if scheduler_affinity_epoch.load(Ordering::Acquire) != pending.epoch {
                // Affinity was invalidated: the whole entry is stale, not just the
                // count, so drop the write instead of resurrecting it.
                continue;
            }
            write_entry(&runtime_state, &cache_key, &pending).await;
        }
    }
}

fn take_batch() -> Vec<(String, PendingWrite)> {
    let Ok(mut pending) = PENDING.lock() else {
        return Vec::new();
    };
    let keys: Vec<String> = pending.keys().take(MAX_KEYS_PER_BATCH).cloned().collect();
    keys.into_iter()
        .filter_map(|key| pending.remove(&key).map(|entry| (key, entry)))
        .collect()
}

fn has_pending() -> bool {
    PENDING
        .lock()
        .map(|pending| !pending.is_empty())
        .unwrap_or(false)
}

async fn write_entry(runtime_state: &RuntimeState, cache_key: &str, pending: &PendingWrite) {
    let existing = runtime_state
        .kv_get(cache_key)
        .await
        .ok()
        .flatten()
        .and_then(|raw| serde_json::from_str::<serde_json::Value>(&raw).ok());
    let previous_count = existing
        .as_ref()
        .and_then(|value| value.get("request_count"))
        .and_then(serde_json::Value::as_u64)
        .unwrap_or_default();
    let created_at = existing
        .as_ref()
        .and_then(|value| value.get("created_at"))
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(pending.created_at_unix_secs);
    let expire_at = pending
        .created_at_unix_secs
        .saturating_add(pending.ttl_seconds);
    let payload = serde_json::json!({
        "provider_id": pending.target.provider_id,
        "endpoint_id": pending.target.endpoint_id,
        "key_id": pending.target.key_id,
        "created_at": created_at,
        "expire_at": expire_at,
        "request_count": previous_count.saturating_add(pending.delta),
        "scheduler_affinity_epoch": pending.epoch,
    });
    let Ok(serialized) = serde_json::to_string(&payload) else {
        return;
    };
    if runtime_state
        .kv_set(
            cache_key,
            serialized,
            Some(std::time::Duration::from_secs(pending.ttl_seconds)),
        )
        .await
        .is_err()
    {
        debug!(
            event_name = "scheduler_affinity_runtime_write_failed",
            log_type = "ops",
            "failed to persist scheduler affinity runtime entry"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::{merge_into, take_batch, PendingWrite, MAX_KEYS_PER_BATCH, PENDING};
    use aether_scheduler_core::SchedulerAffinityTarget;
    use std::collections::HashMap;
    use std::time::Duration;

    fn target() -> SchedulerAffinityTarget {
        SchedulerAffinityTarget {
            provider_id: "provider".to_string(),
            endpoint_id: "endpoint".to_string(),
            key_id: "key".to_string(),
        }
    }

    #[test]
    fn repeated_updates_for_one_key_coalesce_into_a_single_write() {
        let mut pending: HashMap<String, PendingWrite> = HashMap::new();
        for _ in 0..5 {
            merge_into(&mut pending, "key-a", &target(), 60, 7, 1);
        }
        assert_eq!(pending.len(), 1);
        let entry = pending.get("key-a").expect("coalesced entry");
        // Five requests must become one read-modify-write carrying delta 5; the
        // old per-request task wrote five conflicting `previous + 1` values.
        assert_eq!(entry.delta, 5);
        assert_eq!(entry.epoch, 7);
    }

    #[test]
    fn coalescing_never_merges_across_epochs() {
        let mut pending: HashMap<String, PendingWrite> = HashMap::new();
        merge_into(&mut pending, "key-a", &target(), 60, 1, 1);
        merge_into(&mut pending, "key-a", &target(), 60, 2, 1);
        // The stale increment must not be added to the new epoch's entry.
        assert_eq!(pending.get("key-a").expect("entry").delta, 1);
        assert_eq!(pending.get("key-a").expect("entry").epoch, 2);
    }

    #[test]
    fn batches_are_bounded_so_a_hot_key_cannot_starve_the_rest() {
        {
            let mut pending = PENDING.lock().expect("pending queue lock");
            pending.clear();
            for index in 0..(MAX_KEYS_PER_BATCH + 5) {
                merge_into(&mut pending, &format!("key-{index}"), &target(), 60, 1, 1);
            }
        }
        let first = take_batch();
        assert_eq!(first.len(), MAX_KEYS_PER_BATCH);
        assert_eq!(take_batch().len(), 5);
        assert_eq!(take_batch().len(), 0);
        let _ = Duration::from_secs(60);
    }
}
