use std::future::Future;
use std::hash::Hash;
use std::sync::Arc;
use std::time::Duration;

use aether_cache::{value_weight, CacheObserver, CacheWeight, OnceMap};
use serde_json::Value;

use crate::data::auth::GatewayAuthApiKeySnapshot;

/// Every runtime cache here holds short-lived per-request records. A single
/// byte budget replaces the 16,384-entry guess they all shared.
const AUTH_RUNTIME_CACHE_CAPACITY_BYTES: usize = 4 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct AuthApiKeyIdentityCacheKey {
    user_id: String,
    api_key_id: String,
}

impl AuthApiKeyIdentityCacheKey {
    pub(crate) fn new(user_id: &str, api_key_id: &str) -> Self {
        Self {
            user_id: user_id.trim().to_string(),
            api_key_id: api_key_id.trim().to_string(),
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.user_id.is_empty() || self.api_key_id.is_empty()
    }
}

impl CacheWeight for AuthApiKeyIdentityCacheKey {
    fn cache_weight(&self) -> usize {
        value_weight::<Self>(self.user_id.capacity() + self.api_key_id.capacity())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct AuthApiKeyFeatureCacheKey {
    user_id: String,
    api_key_id: String,
    is_standalone: bool,
}

impl AuthApiKeyFeatureCacheKey {
    pub(crate) fn new(user_id: &str, api_key_id: &str, is_standalone: bool) -> Self {
        Self {
            user_id: user_id.trim().to_string(),
            api_key_id: api_key_id.trim().to_string(),
            is_standalone,
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.api_key_id.is_empty() || (!self.is_standalone && self.user_id.is_empty())
    }
}

impl CacheWeight for AuthApiKeyFeatureCacheKey {
    fn cache_weight(&self) -> usize {
        value_weight::<Self>(self.user_id.capacity() + self.api_key_id.capacity())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum AuthSnapshotCacheKey {
    UserApiKeyIds(AuthApiKeyIdentityCacheKey),
}

impl AuthSnapshotCacheKey {
    pub(crate) fn user_api_key_ids(user_id: &str, api_key_id: &str) -> Self {
        Self::UserApiKeyIds(AuthApiKeyIdentityCacheKey::new(user_id, api_key_id))
    }

    pub(crate) fn is_empty(&self) -> bool {
        let Self::UserApiKeyIds(key) = self;
        key.is_empty()
    }
}

impl CacheWeight for AuthSnapshotCacheKey {
    fn cache_weight(&self) -> usize {
        let Self::UserApiKeyIds(key) = self;
        value_weight::<Self>(key.cache_weight())
    }
}

/// Resolved API-key snapshots, keyed by the `(user, key)` pair.
///
/// It is a `ValueCache` like every other runtime lookup; the alias stays so the
/// call sites read as "the auth snapshot cache".
pub(crate) type AuthSnapshotCache =
    ValueCache<AuthSnapshotCacheKey, Arc<GatewayAuthApiKeySnapshot>>;

/// Arbitrary JSON keyed by a typed key, for per-user settings blobs.
///
/// The value shape is the only thing `JsonValueCache` used to add; keeping it as
/// an alias means one cache type, one error mapping and one budget path.
pub(crate) type JsonValueCache<K> = ValueCache<K, Value>;

/// Typed values keyed by a typed key, for the per-user runtime lookups that
/// are too irregular to share a shape.
#[derive(Debug)]
pub(crate) struct ValueCache<K, V> {
    entries: OnceMap<K, Option<V>, crate::GatewayError>,
}

impl<K, V> Default for ValueCache<K, V>
where
    K: Clone + Eq + Hash + CacheWeight + Send + Sync + 'static,
    V: Clone + CacheWeight + Send + Sync + 'static,
{
    fn default() -> Self {
        Self::with_observer(CacheObserver::default())
    }
}

impl<K, V> ValueCache<K, V>
where
    K: Clone + Eq + Hash + CacheWeight + Send + Sync + 'static,
    V: Clone + CacheWeight + Send + Sync + 'static,
{
    pub(crate) fn with_observer(observer: CacheObserver) -> Self {
        Self {
            entries: OnceMap::builder()
                .capacity_bytes(AUTH_RUNTIME_CACHE_CAPACITY_BYTES)
                .observer(observer)
                .build(),
        }
    }
}

impl<K, V> ValueCache<K, V>
where
    K: Clone + Eq + Hash + CacheWeight + Send + Sync + 'static,
    V: Clone + CacheWeight + Send + Sync + 'static,
{
    /// Test-only view of a cached entry; production paths go through
    /// `get_or_load`, so this is not part of the runtime API.
    #[cfg(test)]
    pub(crate) fn get(&self, key: &K, ttl: Duration) -> Option<Option<V>> {
        self.entries
            .peek_within(key, ttl)
            .map(|value| (*value).clone())
    }

    pub(crate) fn insert(&self, key: K, value: Option<V>, window: Duration) {
        self.entries.publish(key, value, window);
    }

    pub(crate) async fn get_or_load<F, Fut>(
        &self,
        key: K,
        ttl: Duration,
        load: F,
    ) -> Result<Option<V>, crate::GatewayError>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<Option<V>, crate::GatewayError>>,
    {
        self.entries
            .get_or_load(key, ttl, load)
            .await
            .map(|value| (*value).clone())
            .map_err(|error| error.or_else_load(crate::GatewayError::Internal))
    }

    pub(crate) async fn get_or_load_revalidating<C, CF, B, BF>(
        &self,
        key: K,
        ttl: Duration,
        stale_ttl: Duration,
        cold_load: C,
        background_refresh: B,
    ) -> Result<Option<V>, crate::GatewayError>
    where
        C: FnOnce() -> CF,
        CF: Future<Output = Result<Option<V>, crate::GatewayError>>,
        B: FnOnce() -> BF,
        BF: Future<Output = Result<Option<V>, crate::GatewayError>> + Send + 'static,
    {
        self.entries
            .get_or_load_revalidating(key, ttl, stale_ttl.max(ttl), cold_load, background_refresh)
            .await
            .map(|value| (*value).clone())
            .map_err(|error| error.or_else_load(crate::GatewayError::Internal))
    }

    pub(crate) fn clear(&self) {
        self.entries.invalidate_all();
    }
}

#[cfg(test)]
mod tests {
    use super::{AuthSnapshotCache, AuthSnapshotCacheKey, JsonValueCache, ValueCache};
    use aether_cache::CacheObserver;
    use serde_json::json;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    #[tokio::test]
    async fn a_snapshot_is_served_from_the_cache() {
        let cache = AuthSnapshotCache::default();
        let key = AuthSnapshotCacheKey::user_api_key_ids("user-a", "key-a");
        let loads = Arc::new(AtomicUsize::new(0));

        for _ in 0..3 {
            let loads = Arc::clone(&loads);
            cache
                .get_or_load(key.clone(), Duration::from_secs(60), || async move {
                    loads.fetch_add(1, Ordering::SeqCst);
                    Ok(None)
                })
                .await
                .expect("read should succeed");
        }

        assert_eq!(loads.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn concurrent_snapshot_reads_load_once() {
        let cache = Arc::new(AuthSnapshotCache::default());
        let loads = Arc::new(AtomicUsize::new(0));

        let mut tasks = Vec::new();
        for _ in 0..16 {
            let cache = Arc::clone(&cache);
            let loads = Arc::clone(&loads);
            tasks.push(tokio::spawn(async move {
                cache
                    .get_or_load(
                        AuthSnapshotCacheKey::user_api_key_ids("user-a", "key-a"),
                        Duration::from_secs(60),
                        || async move {
                            loads.fetch_add(1, Ordering::SeqCst);
                            Ok(None)
                        },
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
    async fn json_values_are_served_from_the_cache() {
        let cache = JsonValueCache::<String>::default();
        let loads = Arc::new(AtomicUsize::new(0));

        for _ in 0..3 {
            let loads = Arc::clone(&loads);
            cache
                .get_or_load(
                    "user-a".to_string(),
                    Duration::from_secs(60),
                    || async move {
                        loads.fetch_add(1, Ordering::SeqCst);
                        Ok(Some(json!({"capability": true})))
                    },
                )
                .await
                .expect("read should succeed");
        }

        assert_eq!(loads.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn a_value_write_is_visible_immediately() {
        let cache = ValueCache::<String, f64>::default();
        cache
            .get_or_load("user-a".to_string(), Duration::from_secs(60), || async {
                Ok(Some(1.0))
            })
            .await
            .expect("initial read should succeed");

        cache.insert("user-a".to_string(), Some(2.0), Duration::from_secs(60));

        let value = cache
            .get_or_load("user-a".to_string(), Duration::from_secs(60), || async {
                unreachable!("an inserted value must be served without a load")
            })
            .await
            .expect("cached read should succeed");
        assert_eq!(value, Some(2.0));
    }

    #[tokio::test]
    async fn a_stale_value_is_served_without_running_a_load() {
        let cache = ValueCache::<String, f64>::default();
        let loads = Arc::new(AtomicUsize::new(0));
        cache.insert("user-a".to_string(), Some(1.0), Duration::from_secs(60));

        let loads_ref = Arc::clone(&loads);
        let value = cache
            .get_or_load(
                "user-a".to_string(),
                Duration::from_secs(60),
                || async move {
                    loads_ref.fetch_add(1, Ordering::SeqCst);
                    Ok(Some(2.0))
                },
            )
            .await
            .expect("stale read should succeed");

        assert_eq!(value, Some(1.0));
        assert_eq!(loads.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn a_revalidating_read_refreshes_behind_the_stale_value() {
        let cache = ValueCache::<String, f64>::default();
        cache.insert("user-a".to_string(), Some(1.0), Duration::from_secs(60));

        let value = cache
            .get_or_load_revalidating(
                "user-a".to_string(),
                Duration::ZERO,
                Duration::from_secs(60),
                || async { Ok(Some(2.0)) },
                || async { Ok(Some(3.0)) },
            )
            .await
            .expect("stale read should succeed");
        assert_eq!(value, Some(1.0));

        let refreshed = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let current = cache
                    .get_or_load("user-a".to_string(), Duration::from_secs(60), || async {
                        unreachable!("a published value must be served without a load")
                    })
                    .await
                    .expect("polled read should succeed");
                if current == Some(3.0) {
                    break Some(3.0);
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the background refresh should publish");
        assert_eq!(refreshed, Some(3.0));
    }

    #[tokio::test]
    async fn a_bound_observer_sees_cache_traffic() {
        static HITS: AtomicUsize = AtomicUsize::new(0);
        fn record_hit() {
            HITS.fetch_add(1, Ordering::SeqCst);
        }

        HITS.store(0, Ordering::SeqCst);
        let cache =
            ValueCache::<String, f64>::with_observer(CacheObserver::new().on_hit(record_hit));
        cache.insert("user-a".to_string(), Some(1.0), Duration::from_secs(60));

        cache
            .get_or_load("user-a".to_string(), Duration::from_secs(60), || async {
                Ok(Some(2.0))
            })
            .await
            .expect("read should succeed");

        assert_eq!(HITS.load(Ordering::SeqCst), 1);
    }
}
