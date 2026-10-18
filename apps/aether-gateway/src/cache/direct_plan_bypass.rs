use std::time::Duration;

use aether_cache::Gate;

/// Remembers which request shapes recently bypassed direct planning.
#[derive(Debug, Default)]
pub(crate) struct DirectPlanBypassCache {
    gate: Gate<String>,
}

impl DirectPlanBypassCache {
    pub(crate) fn should_skip(&self, cache_key: &str, window: Duration) -> bool {
        self.gate.peek(cache_key, window).is_some()
    }

    pub(crate) fn mark(&self, cache_key: String, window: Duration) {
        self.gate.enter(cache_key, window);
    }
}
