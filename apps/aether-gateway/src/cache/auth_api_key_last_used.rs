use std::time::Duration;

use aether_cache::Gate;

/// Admits at most one `last_used_at` write per API key per window.
#[derive(Debug, Default)]
pub(crate) struct AuthApiKeyLastUsedCache {
    gate: Gate<String>,
}

impl AuthApiKeyLastUsedCache {
    pub(crate) fn should_touch(&self, api_key_id: &str, window: Duration) -> bool {
        self.gate.enter(api_key_id.to_string(), window)
    }
}
