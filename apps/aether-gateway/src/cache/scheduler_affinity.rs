use std::time::Duration;

use aether_cache::{value_weight, CacheWeight, PublishedMap};
pub(crate) use aether_scheduler_core::SchedulerAffinityTarget;

/// Remembers which candidate a session was last routed to.
///
/// Every read supplies its own window, so entries carry the lifetime they were
/// written with instead of relying on a cache-wide TTL.
#[derive(Debug)]
pub(crate) struct SchedulerAffinityCache {
    entries: PublishedMap<String, SchedulerAffinityCacheValue>,
}

#[derive(Debug, Clone)]
struct SchedulerAffinityCacheValue {
    target: SchedulerAffinityTarget,
    epoch: u64,
}

impl CacheWeight for SchedulerAffinityCacheValue {
    fn cache_weight(&self) -> usize {
        value_weight::<Self>(
            self.target.provider_id.capacity()
                + self.target.endpoint_id.capacity()
                + self.target.key_id.capacity(),
        )
    }
}

#[derive(Debug, Clone)]
pub(crate) struct SchedulerAffinitySnapshotEntry {
    pub(crate) cache_key: String,
    pub(crate) target: SchedulerAffinityTarget,
    pub(crate) epoch: u64,
    pub(crate) age: Duration,
}

/// Affinity is keyed by session and expires in minutes, so the budget only has
/// to hold the sessions currently in flight rather than a history of them.
const SCHEDULER_AFFINITY_CACHE_CAPACITY_BYTES: usize = 4 * 1024 * 1024;

impl Default for SchedulerAffinityCache {
    fn default() -> Self {
        Self {
            entries: PublishedMap::builder()
                .capacity_bytes(SCHEDULER_AFFINITY_CACHE_CAPACITY_BYTES)
                .build(),
        }
    }
}

impl SchedulerAffinityCache {
    pub(crate) fn get_fresh_for_epoch(
        &self,
        cache_key: &str,
        ttl: Duration,
        epoch: u64,
    ) -> Option<SchedulerAffinityTarget> {
        let value = self.entries.peek_within(cache_key, ttl)?;
        (value.epoch == epoch).then(|| value.target.clone())
    }

    pub(crate) fn insert_for_epoch(
        &self,
        cache_key: String,
        target: SchedulerAffinityTarget,
        ttl: Duration,
        epoch: u64,
    ) {
        self.entries.publish(
            cache_key,
            SchedulerAffinityCacheValue { target, epoch },
            ttl,
        );
    }

    pub(crate) fn remove(&self, cache_key: &str) -> Option<SchedulerAffinityTarget> {
        // Removal is a single step: peeking first would hand back whatever a
        // concurrent writer put there in between.
        self.entries
            .remove(cache_key)
            .map(|value| value.target.clone())
    }

    pub(crate) fn clear(&self) {
        self.entries.invalidate_all();
    }

    pub(crate) fn fresh_entries_for_epoch(
        &self,
        ttl: Duration,
        epoch: u64,
    ) -> Vec<SchedulerAffinitySnapshotEntry> {
        self.entries
            .iter_fresh()
            .into_iter()
            .filter_map(|(cache_key, value, age)| {
                (value.epoch == epoch && age <= ttl).then(|| SchedulerAffinitySnapshotEntry {
                    cache_key,
                    target: value.target.clone(),
                    epoch: value.epoch,
                    age,
                })
            })
            .collect()
    }
}
