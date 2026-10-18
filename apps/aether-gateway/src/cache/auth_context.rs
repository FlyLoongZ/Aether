use std::future::Future;
#[cfg(test)]
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use aether_cache::{optional_strings_weight, value_weight, CacheWeight, OnceMap, OnceMapError};

use crate::control::{GatewayControlAuthContext, GatewayLocalAuthRejection};
use crate::GatewayError;

pub(crate) const AUTH_CONTEXT_CACHE_TTL: Duration = Duration::from_secs(60);
const AUTH_CONTEXT_CACHE_CAPACITY_BYTES: usize = 8 * 1024 * 1024;
const AUTH_CONTEXT_CACHE_MAX_INFLIGHT: usize = 1024;

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

/// Whether `auth_context` is a rejection rather than a usable principal.
///
/// A rejection is worth remembering only long enough to keep one bad credential
/// from hammering the database, so it lives inside a much shorter window than a
/// resolved context. The cache is the only place that distinction survives, so
/// the predicate lives here with it.
pub(crate) fn is_negative_auth_context(auth_context: &GatewayControlAuthContext) -> bool {
    auth_context.user_id.is_empty()
        || auth_context.api_key_id.is_empty()
        || matches!(
            auth_context.local_rejection,
            Some(GatewayLocalAuthRejection::InvalidApiKey)
        )
}

/// Authorization contexts keyed by the request's credentials.
///
/// One entry per credential, whatever the answer turned out to be: a resolved
/// principal and the rejection that proves the credential wrong are two answers
/// to the same question, so they share one slot — and therefore one in-flight
/// load. Only the read window differs; see [`AuthContextCache::peek_within`].
///
/// The value is `Option` because a resolution can legitimately find no principal
/// at all, which callers treat as a miss.
#[derive(Debug)]
pub(crate) struct AuthContextCache {
    entries: OnceMap<String, Option<GatewayControlAuthContext>, GatewayError>,
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
    /// A cached answer for `cache_key` still inside the window its own kind
    /// allows, together with the age it was published at.
    ///
    /// A resolved context lives for `resolved_max_age`; a rejection lives for
    /// `negative_max_age`, which an operator may set to zero to disable negative
    /// caching. Entries are retained for the longer of the two so that widening
    /// the negative window does not require waiting for a miss.
    ///
    /// Freshness is decided here rather than by the entry's own window, because
    /// both kinds share one slot: an entry stored with the longer window would
    /// otherwise keep a rejection readable for the resolved window's lifetime.
    pub(crate) fn peek_within(
        &self,
        cache_key: &str,
        resolved_max_age: Duration,
        negative_max_age: Duration,
    ) -> Option<(GatewayControlAuthContext, Duration)> {
        let retained_for = resolved_max_age.max(negative_max_age);
        let (value, age) = self.entries.peek_within_age(cache_key, retained_for)?;
        let context = value.as_ref().as_ref()?;
        let allowed_for = if is_negative_auth_context(context) {
            negative_max_age
        } else {
            resolved_max_age
        };
        (!allowed_for.is_zero() && age <= allowed_for).then(|| (context.clone(), age))
    }

    /// Stores `auth_context` and drops any load in flight for the key, so a
    /// slower read cannot overwrite what the application just decided.
    pub(crate) fn publish(
        &self,
        cache_key: &str,
        auth_context: GatewayControlAuthContext,
        window: Duration,
    ) {
        self.entries
            .publish(cache_key.to_string(), Some(auth_context), window);
    }

    /// Resolves through the cache, running the loader once per credential.
    pub(crate) async fn get_or_load<F, Fut>(
        &self,
        cache_key: &str,
        resolved_ttl: Duration,
        negative_ttl: Duration,
        load: F,
    ) -> Result<Option<GatewayControlAuthContext>, GatewayError>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<Option<GatewayControlAuthContext>, GatewayError>>,
    {
        if let Some((context, _)) = self.peek_within(cache_key, resolved_ttl, negative_ttl) {
            return Ok(Some(context));
        }
        // The kind-aware window above already decided this is a miss, so the
        // load must not re-read the store through the plain per-entry window.
        self.entries
            .reload(cache_key.to_string(), resolved_ttl.max(negative_ttl), load)
            .await
            .map(|value| (*value).clone())
            .map_err(|error| error.or_else_load(GatewayError::Internal))
    }

    /// Re-resolves in the foreground even when an answer is cached, running the
    /// loader once per credential. Authorization staleness is not negotiable,
    /// but the load behind it still is.
    ///
    /// The cache failure stays in the error so callers can tell a failed
    /// resolution from one that never ran.
    pub(crate) async fn reload<F, Fut>(
        &self,
        cache_key: &str,
        resolved_ttl: Duration,
        negative_ttl: Duration,
        load: F,
    ) -> Result<GatewayControlAuthContext, OnceMapError<GatewayError>>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<GatewayControlAuthContext, GatewayError>>,
    {
        let value = self
            .entries
            .reload(
                cache_key.to_string(),
                resolved_ttl.max(negative_ttl),
                || async move { load().await.map(Some) },
            )
            .await?;
        // A reload only ever coalesces with another reload, and every reload
        // loader wraps its answer in `Some`, so this is unreachable. It is an
        // error rather than an `expect` because a panic here would happen on
        // the authorization path, where a failed refresh already has a
        // handling story.
        value.as_ref().clone().ok_or_else(|| {
            OnceMapError::Load(GatewayError::Internal(
                "cache reload returned no auth context".to_string(),
            ))
        })
    }

    /// Drops the cached answer for one credential.
    pub(crate) fn invalidate(&self, cache_key: &str) {
        self.entries.invalidate(cache_key);
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
    use super::{is_negative_auth_context, AuthContextCache, AUTH_CONTEXT_CACHE_TTL};
    use crate::control::{GatewayControlAuthContext, GatewayLocalAuthRejection};
    use crate::GatewayError;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    const NEGATIVE_TTL: Duration = Duration::from_secs(10);

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

    fn rejection() -> GatewayControlAuthContext {
        GatewayControlAuthContext {
            user_id: String::new(),
            api_key_id: String::new(),
            username: None,
            api_key_name: None,
            balance_remaining: None,
            access_allowed: false,
            user_rate_limit: None,
            api_key_rate_limit: None,
            api_key_is_standalone: false,
            admin_bypass_limits: false,
            local_rejection: Some(GatewayLocalAuthRejection::InvalidApiKey),
            allowed_models: None,
            ip_rules: None,
            verified_api_key_hash: None,
        }
    }

    fn peek(cache: &AuthContextCache) -> Option<(GatewayControlAuthContext, Duration)> {
        cache.peek_within("key-a", AUTH_CONTEXT_CACHE_TTL, NEGATIVE_TTL)
    }

    #[tokio::test]
    async fn a_resolved_context_is_served_from_the_cache() {
        let cache = AuthContextCache::default();
        let loads = Arc::new(AtomicUsize::new(0));

        for _ in 0..4 {
            let loads = Arc::clone(&loads);
            cache
                .get_or_load(
                    "key-a",
                    AUTH_CONTEXT_CACHE_TTL,
                    NEGATIVE_TTL,
                    || async move {
                        loads.fetch_add(1, Ordering::SeqCst);
                        Ok(Some(context("api-key-a")))
                    },
                )
                .await
                .expect("resolution should succeed");
        }

        assert_eq!(loads.load(Ordering::SeqCst), 1);
        assert_eq!(
            peek(&cache)
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
                    .get_or_load(
                        "key-a",
                        AUTH_CONTEXT_CACHE_TTL,
                        NEGATIVE_TTL,
                        || async move {
                            loads.fetch_add(1, Ordering::SeqCst);
                            Ok(Some(context("api-key-a")))
                        },
                    )
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
        cache.entries.publish(
            "key-a".to_string(),
            None,
            AUTH_CONTEXT_CACHE_TTL.max(NEGATIVE_TTL),
        );

        assert!(peek(&cache).is_none());
    }

    /// The whole point of one slot per credential: a rejection must not inherit
    /// the resolved window just because it shares the entry.
    #[test]
    fn a_rejection_is_not_readable_past_the_negative_window() {
        let cache = AuthContextCache::default();
        cache.publish(
            "key-a",
            rejection(),
            AUTH_CONTEXT_CACHE_TTL.max(NEGATIVE_TTL),
        );
        assert!(peek(&cache).is_some(), "a fresh rejection is readable");

        std::thread::sleep(NEGATIVE_TTL + Duration::from_millis(20));

        assert!(
            peek(&cache).is_none(),
            "a rejection must expire with the negative window, not the resolved one"
        );
    }

    /// A disabled negative cache must not fall back to reading the entry.
    #[test]
    fn a_rejection_is_never_readable_with_a_zero_negative_window() {
        let cache = AuthContextCache::default();
        cache.publish("key-a", rejection(), AUTH_CONTEXT_CACHE_TTL);

        assert!(cache
            .peek_within("key-a", AUTH_CONTEXT_CACHE_TTL, Duration::ZERO)
            .is_none());
        assert!(
            cache
                .peek_within("key-a", AUTH_CONTEXT_CACHE_TTL, NEGATIVE_TTL)
                .is_some(),
            "the entry is still stored; only its read window is disabled"
        );
    }

    /// A negative answer must not suppress the next lookup once its window is
    /// over, even though the entry is retained for longer.
    #[tokio::test]
    async fn an_expired_rejection_triggers_a_new_load() {
        let cache = AuthContextCache::default();
        let loads = Arc::new(AtomicUsize::new(0));
        let negative_ttl = Duration::from_millis(20);

        for _ in 0..2 {
            let loads = Arc::clone(&loads);
            cache
                .get_or_load(
                    "key-a",
                    AUTH_CONTEXT_CACHE_TTL,
                    negative_ttl,
                    || async move {
                        loads.fetch_add(1, Ordering::SeqCst);
                        Ok(Some(rejection()))
                    },
                )
                .await
                .expect("resolution should succeed");
            tokio::time::sleep(negative_ttl + Duration::from_millis(20)).await;
        }

        assert_eq!(loads.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn invalidate_drops_only_one_key() {
        let cache = AuthContextCache::default();
        cache.publish("key-a", context("api-key-a"), AUTH_CONTEXT_CACHE_TTL);
        cache.publish("key-a-neg", rejection(), AUTH_CONTEXT_CACHE_TTL);
        cache.publish("key-b", context("api-key-b"), AUTH_CONTEXT_CACHE_TTL);

        cache.invalidate("key-a");

        assert!(peek(&cache).is_none());
        assert_eq!(
            cache
                .peek_within("key-b", AUTH_CONTEXT_CACHE_TTL, NEGATIVE_TTL)
                .expect("an unrelated key should survive")
                .0
                .api_key_id,
            "api-key-b"
        );
    }

    #[test]
    fn invalidate_all_drops_every_key() {
        let cache = AuthContextCache::default();
        cache.publish("key-a", context("api-key-a"), AUTH_CONTEXT_CACHE_TTL);
        cache.publish("key-b", context("api-key-b"), AUTH_CONTEXT_CACHE_TTL);

        cache.invalidate_all();

        assert_eq!(cache.len(), 0);
    }

    #[tokio::test]
    async fn reload_runs_the_loader_even_when_a_context_is_cached() {
        let cache = AuthContextCache::default();
        cache.publish("key-a", context("stale"), AUTH_CONTEXT_CACHE_TTL);

        let refreshed = cache
            .reload("key-a", AUTH_CONTEXT_CACHE_TTL, NEGATIVE_TTL, || async {
                Ok(context("fresh"))
            })
            .await
            .expect("reload should succeed");

        assert_eq!(refreshed.api_key_id, "fresh");
        assert_eq!(
            peek(&cache)
                .expect("reload should publish its result")
                .0
                .api_key_id,
            "fresh"
        );
    }

    /// A security refresh that discovers the credential is gone must replace the
    /// allow it was revalidating — and, being a rejection, expire on the short
    /// window rather than live on the resolved one.
    #[tokio::test]
    async fn a_reload_that_denies_replaces_the_allow_it_revalidated() {
        let cache = AuthContextCache::default();
        let negative_ttl = Duration::from_millis(20);
        cache.publish("key-a", context("api-key-a"), AUTH_CONTEXT_CACHE_TTL);
        assert!(cache
            .peek_within("key-a", AUTH_CONTEXT_CACHE_TTL, negative_ttl)
            .is_some());

        let refreshed = cache
            .reload("key-a", AUTH_CONTEXT_CACHE_TTL, negative_ttl, || async {
                Ok(rejection())
            })
            .await
            .expect("reload should succeed");
        assert!(is_negative_auth_context(&refreshed));
        assert!(
            !cache
                .peek_within("key-a", AUTH_CONTEXT_CACHE_TTL, negative_ttl)
                .expect("the rejection is readable for its own window")
                .0
                .access_allowed,
            "the allow the refresh was revalidating must be gone"
        );

        tokio::time::sleep(negative_ttl + Duration::from_millis(20)).await;
        assert!(
            cache
                .peek_within("key-a", AUTH_CONTEXT_CACHE_TTL, negative_ttl)
                .is_none(),
            "a rejection must not survive past the negative window"
        );
    }

    #[tokio::test]
    async fn a_failed_load_publishes_nothing() {
        let cache = AuthContextCache::default();

        let error = cache
            .get_or_load("key-a", AUTH_CONTEXT_CACHE_TTL, NEGATIVE_TTL, || async {
                Err(GatewayError::Internal("forced failure".to_string()))
            })
            .await
            .expect_err("the failure should surface");

        assert_eq!(error.into_message(), "forced failure");
        assert!(peek(&cache).is_none());
    }
}
