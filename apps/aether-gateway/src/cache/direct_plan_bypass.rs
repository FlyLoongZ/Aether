use std::time::Duration;

use aether_cache::ExpiringMap;

#[derive(Debug, Default)]
pub(crate) struct DirectPlanBypassCache {
    entries: ExpiringMap<String, ()>,
}

impl DirectPlanBypassCache {
    pub(crate) fn should_skip(&self, cache_key: &str, ttl: Duration) -> bool {
        self.entries.contains_fresh(&cache_key.to_string(), ttl)
    }
}
