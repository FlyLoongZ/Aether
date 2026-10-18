use std::future::Future;
use std::time::Duration;

use aether_cache::OnceMap;

use crate::GatewayError;

const SYSTEM_CONFIG_CACHE_CAPACITY_BYTES: usize = 4 * 1024 * 1024;
const SYSTEM_CONFIG_CACHE_LOAD_TIMEOUT: Duration = Duration::from_secs(10);

/// One `OnceMap` for every system config key. The data layer no longer caches
/// config values at all; freshness is decided per call here.
#[derive(Debug)]
pub(crate) struct SystemConfigCache {
    entries: OnceMap<String, Option<serde_json::Value>, GatewayError>,
}

impl Default for SystemConfigCache {
    fn default() -> Self {
        Self {
            entries: OnceMap::builder()
                .capacity_bytes(SYSTEM_CONFIG_CACHE_CAPACITY_BYTES)
                .load_timeout(SYSTEM_CONFIG_CACHE_LOAD_TIMEOUT)
                .build(),
        }
    }
}

impl SystemConfigCache {
    /// A value within `fresh` is served as is, one within `stale` is served
    /// while a single background refresh runs, and anything older blocks on a
    /// load.
    pub(crate) async fn get_or_load<C, CF, B, BF>(
        &self,
        key: String,
        fresh: Duration,
        stale: Duration,
        cold_load: C,
        background_refresh: B,
    ) -> Result<Option<serde_json::Value>, GatewayError>
    where
        C: FnOnce() -> CF,
        CF: Future<Output = Result<Option<serde_json::Value>, GatewayError>>,
        B: FnOnce() -> BF,
        BF: Future<Output = Result<Option<serde_json::Value>, GatewayError>> + Send + 'static,
    {
        self.entries
            .get_or_load_revalidating(key, fresh, stale, cold_load, background_refresh)
            .await
            .map(|value| (*value).clone())
            .map_err(|error| error.or_else_load(GatewayError::Internal))
    }

    /// Publishes a value written through the application and drops any load in
    /// flight, so a slower stale read cannot overwrite it.
    pub(crate) fn publish(&self, key: String, value: Option<serde_json::Value>, window: Duration) {
        self.entries.publish(key, value, window);
    }

    pub(crate) fn invalidate(&self, key: &str) {
        self.entries.invalidate(key);
    }

    pub(crate) fn invalidate_all(&self) {
        self.entries.invalidate_all();
    }

    #[cfg(test)]
    pub(crate) fn peek(&self, key: &str, max_age: Duration) -> Option<Option<serde_json::Value>> {
        self.entries
            .peek_within(key, max_age)
            .map(|value| (*value).clone())
    }
}

#[cfg(test)]
mod tests {
    use super::SystemConfigCache;
    use crate::GatewayError;
    use serde_json::json;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    fn windows() -> (Duration, Duration) {
        (Duration::from_secs(30), Duration::from_secs(300))
    }

    #[tokio::test]
    async fn a_repeat_read_is_served_from_the_cache() {
        let cache = SystemConfigCache::default();
        let loads = Arc::new(AtomicUsize::new(0));
        let (fresh, stale) = windows();

        for _ in 0..3 {
            let loads = Arc::clone(&loads);
            cache
                .get_or_load(
                    "smtp_host".to_string(),
                    fresh,
                    stale,
                    || async move {
                        loads.fetch_add(1, Ordering::SeqCst);
                        Ok(Some(json!("mail.example")))
                    },
                    || async { Ok(Some(json!("refreshed"))) },
                )
                .await
                .expect("read should succeed");
        }

        assert_eq!(loads.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn concurrent_reads_of_one_key_load_once() {
        let cache = Arc::new(SystemConfigCache::default());
        let loads = Arc::new(AtomicUsize::new(0));
        let (fresh, stale) = windows();

        let mut tasks = Vec::new();
        for _ in 0..16 {
            let cache = Arc::clone(&cache);
            let loads = Arc::clone(&loads);
            tasks.push(tokio::spawn(async move {
                cache
                    .get_or_load(
                        "smtp_host".to_string(),
                        fresh,
                        stale,
                        || async move {
                            loads.fetch_add(1, Ordering::SeqCst);
                            Ok(Some(json!("mail.example")))
                        },
                        || async { Ok(Some(json!("refreshed"))) },
                    )
                    .await
                    .expect("read should succeed")
            }));
        }
        for task in tasks {
            task.await.expect("task should join");
        }

        assert_eq!(loads.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn an_application_write_is_visible_immediately() {
        let cache = SystemConfigCache::default();
        let (fresh, stale) = windows();
        cache
            .get_or_load(
                "smtp_host".to_string(),
                fresh,
                stale,
                || async { Ok(Some(json!("old"))) },
                || async { Ok(Some(json!("refreshed"))) },
            )
            .await
            .expect("initial read should succeed");

        cache.publish(
            "smtp_host".to_string(),
            Some(json!("written")),
            Duration::from_secs(300),
        );

        let value = cache
            .get_or_load(
                "smtp_host".to_string(),
                fresh,
                stale,
                || async { Ok(Some(json!("reloaded"))) },
                || async { Ok(Some(json!("refreshed"))) },
            )
            .await
            .expect("read after a write should succeed");
        assert_eq!(value, Some(json!("written")));
    }

    #[tokio::test]
    async fn invalidate_forces_the_next_read_to_load() {
        let cache = SystemConfigCache::default();
        let (fresh, stale) = windows();
        let loads = Arc::new(AtomicUsize::new(0));

        for invalidate in [false, true] {
            if invalidate {
                cache.invalidate("smtp_host");
            }
            let loads = Arc::clone(&loads);
            cache
                .get_or_load(
                    "smtp_host".to_string(),
                    fresh,
                    stale,
                    || async move {
                        loads.fetch_add(1, Ordering::SeqCst);
                        Ok(Some(json!("mail.example")))
                    },
                    || async { Ok(Some(json!("refreshed"))) },
                )
                .await
                .expect("read should succeed");
        }

        assert_eq!(loads.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn a_failed_load_publishes_nothing() {
        let cache = SystemConfigCache::default();
        let (fresh, stale) = windows();

        let error = cache
            .get_or_load(
                "smtp_host".to_string(),
                fresh,
                stale,
                || async { Err(GatewayError::Internal("forced failure".to_string())) },
                || async { Ok(Some(json!("refreshed"))) },
            )
            .await
            .expect_err("the failure should surface");
        assert_eq!(error.into_message(), "forced failure");

        let value = cache
            .get_or_load(
                "smtp_host".to_string(),
                fresh,
                stale,
                || async { Ok(Some(json!("recovered"))) },
                || async { Ok(Some(json!("refreshed"))) },
            )
            .await
            .expect("a failed load must not be cached");
        assert_eq!(value, Some(json!("recovered")));
    }
}
