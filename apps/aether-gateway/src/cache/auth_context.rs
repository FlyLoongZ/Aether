use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use aether_cache::{optional_strings_weight, value_weight, CacheWeight, OnceMap, OnceMapError};

use crate::{control::GatewayControlAuthContext, GatewayError};

pub(crate) const AUTH_CONTEXT_CACHE_TTL: Duration = Duration::from_secs(60);
const AUTH_CONTEXT_CACHE_CAPACITY_BYTES: usize = 8 * 1024 * 1024;
const AUTH_CONTEXT_CACHE_MAX_INFLIGHT: usize = 1024;

/// A resolution that found a principal and one that found none lead different
/// lives, so they get different keys instead of one string with a prefix.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum AuthContextCacheKey {
    Resolved(String),
    Negative(String),
}

impl AuthContextCacheKey {
    pub(crate) fn resolved(cache_key: &str) -> Self {
        Self::Resolved(cache_key.to_string())
    }

    pub(crate) fn negative(cache_key: &str) -> Self {
        Self::Negative(cache_key.to_string())
    }
}

impl CacheWeight for AuthContextCacheKey {
    fn cache_weight(&self) -> usize {
        let key = match self {
            Self::Resolved(key) | Self::Negative(key) => key,
        };
        value_weight::<Self>(key.capacity())
    }
}

impl CacheWeight for GatewayControlAuthContext {
    fn cache_weight(&self) -> usize {
        let strings = self.user_id.capacity()
            + self.api_key_id.capacity()
            + self.username.as_ref().map_or(0, String::capacity)
            + self.api_key_name.as_ref().map_or(0, String::capacity)
            + self
                .verified_api_key_hash
                .as_ref()
                .map_or(0, CacheWeight::cache_weight);
        let lists = optional_strings_weight(self.allowed_models.as_ref())
            + optional_strings_weight(self.ip_rules.as_ref());
        value_weight::<Self>(strings + lists)
    }
}

/// Resolved authorization contexts, keyed by request credentials.
///
/// The value is `Option` because a resolution that legitimately finds nothing
/// is worth not repeating, but callers treat it as a miss.
#[derive(Debug)]
pub(crate) struct AuthContextCache {
    entries: OnceMap<AuthContextCacheKey, Option<GatewayControlAuthContext>, GatewayError>,
    #[cfg(test)]
    refresh_interval_override_millis: AtomicU64,
}

impl Default for AuthContextCache {
    fn default() -> Self {
        Self {
            entries: OnceMap::builder()
                .capacity_bytes(AUTH_CONTEXT_CACHE_CAPACITY_BYTES)
                .max_inflight(AUTH_CONTEXT_CACHE_MAX_INFLIGHT)
                .build(),
            #[cfg(test)]
            refresh_interval_override_millis: AtomicU64::new(0),
        }
    }
}

impl AuthContextCache {
    /// A cached context younger than `max_age`, ignoring entries that resolved
    /// to nothing.
    pub(crate) fn peek_within(
        &self,
        cache_key: AuthContextCacheKey,
        max_age: Duration,
    ) -> Option<(GatewayControlAuthContext, Duration)> {
        let (value, age) = self.entries.peek_within_age(&cache_key, max_age)?;
        let context = value.as_ref().as_ref()?.clone();
        Some((context, age))
    }

    pub(crate) fn publish(
        &self,
        cache_key: AuthContextCacheKey,
        auth_context: GatewayControlAuthContext,
        ttl: Duration,
    ) {
        self.entries.publish(cache_key, Some(auth_context), ttl);
    }

    /// Resolves through the cache, running the loader once per key.
    pub(crate) async fn get_or_load<F, Fut>(
        &self,
        cache_key: AuthContextCacheKey,
        load: F,
    ) -> Result<Option<GatewayControlAuthContext>, GatewayError>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<Option<GatewayControlAuthContext>, GatewayError>>,
    {
        self.entries
            .get_or_load(cache_key, AUTH_CONTEXT_CACHE_TTL, load)
            .await
            .map(|value| (*value).clone())
            .map_err(|error| error.or_else_load(GatewayError::Internal))
    }

    /// Re-resolves in the foreground even when a value is cached, running the
    /// loader once per key. Authorization staleness is not negotiable, but the
    /// load behind it still is.
    ///
    /// The cache failure stays in the error so callers can tell a failed
    /// resolution from one that never ran.
    pub(crate) async fn reload<F, Fut>(
        &self,
        cache_key: AuthContextCacheKey,
        load: F,
    ) -> Result<GatewayControlAuthContext, OnceMapError<GatewayError>>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<GatewayControlAuthContext, GatewayError>>,
    {
        self.entries
            .reload(cache_key, AUTH_CONTEXT_CACHE_TTL, || async move {
                load().await.map(Some)
            })
            .await
            .map(|value| {
                value
                    .as_ref()
                    .clone()
                    .expect("a reload always publishes the context it loaded")
            })
    }

    /// Drops both variants for one request's credentials.
    pub(crate) fn invalidate(&self, cache_key: &str) {
        self.entries
            .invalidate(&AuthContextCacheKey::resolved(cache_key));
        self.entries
            .invalidate(&AuthContextCacheKey::negative(cache_key));
    }

    pub(crate) fn invalidate_all(&self) {
        self.entries.invalidate_all();
    }

    #[cfg(test)]
    pub(crate) fn set_refresh_interval_for_tests(&self, interval: Duration) {
        let millis = u64::try_from(interval.as_millis()).unwrap_or(u64::MAX);
        self.refresh_interval_override_millis
            .store(millis.max(1), Ordering::Release);
    }

    #[cfg(test)]
    pub(crate) fn refresh_interval_for_tests(&self) -> Option<Duration> {
        let millis = self
            .refresh_interval_override_millis
            .load(Ordering::Acquire);
        (millis > 0).then(|| Duration::from_millis(millis))
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.entries.len()
    }
}

#[cfg(test)]
mod tests {
    use super::{AuthContextCache, AuthContextCacheKey};
    use crate::{control::GatewayControlAuthContext, GatewayError};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    fn context(api_key_id: &str) -> GatewayControlAuthContext {
        GatewayControlAuthContext {
            user_id: "user-1".to_string(),
            api_key_id: api_key_id.to_string(),
            username: None,
            api_key_name: None,
            balance_remaining: None,
            access_allowed: true,
            user_rate_limit: None,
            api_key_rate_limit: None,
            api_key_is_standalone: false,
            admin_bypass_limits: false,
            local_rejection: None,
            allowed_models: None,
            ip_rules: None,
            verified_api_key_hash: None,
        }
    }

    fn key() -> AuthContextCacheKey {
        AuthContextCacheKey::resolved("key-a")
    }

    #[tokio::test]
    async fn a_resolved_context_is_served_from_the_cache() {
        let cache = AuthContextCache::default();
        let loads = Arc::new(AtomicUsize::new(0));

        for _ in 0..4 {
            let loads = Arc::clone(&loads);
            cache
                .get_or_load(key(), || async move {
                    loads.fetch_add(1, Ordering::SeqCst);
                    Ok(Some(context("api-key-a")))
                })
                .await
                .expect("resolution should succeed");
        }

        assert_eq!(loads.load(Ordering::SeqCst), 1);
        assert_eq!(
            cache
                .peek_within(key(), Duration::from_secs(60))
                .expect("the cached context should be readable")
                .0
                .api_key_id,
            "api-key-a"
        );
    }

    #[tokio::test]
    async fn concurrent_resolutions_of_one_key_run_once() {
        let cache = Arc::new(AuthContextCache::default());
        let loads = Arc::new(AtomicUsize::new(0));

        let mut tasks = Vec::new();
        for _ in 0..16 {
            let cache = Arc::clone(&cache);
            let loads = Arc::clone(&loads);
            tasks.push(tokio::spawn(async move {
                cache
                    .get_or_load(key(), || async move {
                        loads.fetch_add(1, Ordering::SeqCst);
                        Ok(Some(context("api-key-a")))
                    })
                    .await
                    .expect("resolution should succeed")
            }));
        }
        for task in tasks {
            task.await.expect("task should join");
        }

        assert_eq!(loads.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn a_resolution_that_finds_nothing_is_not_readable() {
        let cache = AuthContextCache::default();
        cache.entries.publish(key(), None, Duration::from_secs(60));

        assert!(cache.peek_within(key(), Duration::from_secs(60)).is_none());
    }

    #[test]
    fn invalidate_drops_both_variants_for_one_key_only() {
        let cache = AuthContextCache::default();
        cache.publish(key(), context("api-key-a"), Duration::from_secs(60));
        cache.publish(
            AuthContextCacheKey::negative("key-a"),
            context("negative-api-key-a"),
            Duration::from_secs(60),
        );
        cache.publish(
            AuthContextCacheKey::resolved("key-b"),
            context("api-key-b"),
            Duration::from_secs(60),
        );

        cache.invalidate("key-a");

        assert!(cache.peek_within(key(), Duration::from_secs(60)).is_none());
        assert!(cache
            .peek_within(
                AuthContextCacheKey::negative("key-a"),
                Duration::from_secs(60)
            )
            .is_none());
        assert_eq!(
            cache
                .peek_within(
                    AuthContextCacheKey::resolved("key-b"),
                    Duration::from_secs(60)
                )
                .expect("an unrelated key should survive")
                .0
                .api_key_id,
            "api-key-b"
        );
    }

    #[test]
    fn invalidate_all_drops_every_key() {
        let cache = AuthContextCache::default();
        cache.publish(key(), context("api-key-a"), Duration::from_secs(60));
        cache.publish(
            AuthContextCacheKey::resolved("key-b"),
            context("api-key-b"),
            Duration::from_secs(60),
        );

        cache.invalidate_all();

        assert_eq!(cache.len(), 0);
    }

    #[tokio::test]
    async fn reload_runs_the_loader_even_when_a_context_is_cached() {
        let cache = AuthContextCache::default();
        cache.publish(key(), context("stale"), Duration::from_secs(60));

        let refreshed = cache
            .reload(key(), || async { Ok(context("fresh")) })
            .await
            .expect("reload should succeed");

        assert_eq!(refreshed.api_key_id, "fresh");
        assert_eq!(
            cache
                .peek_within(key(), Duration::from_secs(60))
                .expect("reload should publish its result")
                .0
                .api_key_id,
            "fresh"
        );
    }

    #[tokio::test]
    async fn a_failed_load_publishes_nothing() {
        let cache = AuthContextCache::default();

        let error = cache
            .get_or_load(key(), || async {
                Err(GatewayError::Internal("forced failure".to_string()))
            })
            .await
            .expect_err("the failure should surface");

        assert_eq!(error.into_message(), "forced failure");
        assert!(cache.peek_within(key(), Duration::from_secs(60)).is_none());
    }
}
