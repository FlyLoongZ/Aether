use aether_cache::PublishedMap;
use bytes::Bytes;
use std::time::Duration;

/// Serialized dashboard payloads, keyed by endpoint plus query.
///
/// The payloads are cached as `Bytes` so a hit hands the response body the same
/// allocation instead of copying it. Capacity is a byte budget rather than a
/// number of entries, because these payloads vary in size by orders of
/// magnitude.
///
/// Each endpoint keeps its own window: the aggregate stats panel is cheap to
/// recompute, the daily series is the expensive query, and the provider status
/// strip is the one operators expect to move first.
pub(crate) const DASHBOARD_STATS_CACHE_TTL: Duration = Duration::from_secs(30);
pub(crate) const DASHBOARD_DAILY_CACHE_TTL: Duration = Duration::from_secs(60);
pub(crate) const DASHBOARD_PROVIDER_STATUS_CACHE_TTL: Duration = Duration::from_secs(20);
const DASHBOARD_RESPONSE_CACHE_CAPACITY_BYTES: usize = 16 * 1024 * 1024;

#[derive(Debug)]
pub(crate) struct DashboardResponseCache {
    entries: PublishedMap<String, Bytes>,
}

impl Default for DashboardResponseCache {
    fn default() -> Self {
        Self {
            entries: PublishedMap::builder()
                .capacity_bytes(DASHBOARD_RESPONSE_CACHE_CAPACITY_BYTES)
                .build(),
        }
    }
}

impl DashboardResponseCache {
    pub(crate) fn get(&self, key: &str, ttl: Duration) -> Option<Bytes> {
        self.entries
            .peek_within(key, ttl)
            .map(|cached| (*cached).clone())
    }

    pub(crate) fn insert(&self, key: String, value: Bytes, ttl: Duration) {
        self.entries.publish(key, value, ttl);
    }
}
