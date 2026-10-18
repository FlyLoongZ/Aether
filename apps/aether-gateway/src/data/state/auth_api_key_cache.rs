use std::sync::Arc;
use std::time::Duration;

use aether_cache::{optional_strings_weight, value_weight, CacheWeight, OnceMap};
use aether_data::repository::auth::*;
use aether_data::DataLayerError;
use async_trait::async_trait;

tokio::task_local! {
    static AUTH_API_KEY_READ_CACHE_BYPASS: ();
}

/// Auth API key snapshots, cached with request coalescing.
///
/// A snapshot carries six optional string lists plus a dozen scalars, so the
/// budget is expressed in bytes; an entry count says nothing about its cost.
pub(super) struct CachedAuthApiKeyReadRepository {
    inner: Arc<dyn AuthApiKeyReadRepository>,
    snapshots: OnceMap<AuthApiKeySnapshotCacheKey, CachedAuthApiKeySnapshot, DataLayerError>,
}

/// Wrapper so the weight can live next to the cache that stores it.
#[derive(Debug, Clone)]
struct CachedAuthApiKeySnapshot(Option<StoredAuthApiKeySnapshot>);

impl CacheWeight for CachedAuthApiKeySnapshot {
    fn cache_weight(&self) -> usize {
        let Some(snapshot) = self.0.as_ref() else {
            return 0;
        };
        value_weight::<StoredAuthApiKeySnapshot>(
            snapshot.user_id.capacity()
                + snapshot.username.capacity()
                + snapshot.email.as_ref().map_or(0, String::capacity)
                + snapshot.user_role.capacity()
                + snapshot.user_auth_source.capacity()
                + optional_strings_weight(snapshot.user_allowed_providers.as_ref())
                + optional_strings_weight(snapshot.user_allowed_api_formats.as_ref())
                + optional_strings_weight(snapshot.user_allowed_models.as_ref())
                + snapshot.api_key_id.capacity()
                + snapshot.api_key_name.as_ref().map_or(0, String::capacity)
                + optional_strings_weight(snapshot.api_key_allowed_providers.as_ref())
                + optional_strings_weight(snapshot.api_key_allowed_api_formats.as_ref())
                + optional_strings_weight(snapshot.api_key_allowed_models.as_ref())
                + optional_strings_weight(snapshot.api_key_ip_rules.as_ref()),
        )
    }
}

/// Snapshot answers go stale quickly: a rotated or locked key must take effect.
const AUTH_API_KEY_SNAPSHOT_CACHE_TTL: Duration = Duration::from_secs(30);
const AUTH_API_KEY_SNAPSHOT_CACHE_CAPACITY_BYTES: usize = 4 * 1024 * 1024;
const AUTH_API_KEY_SNAPSHOT_CACHE_MAX_INFLIGHT: usize = 256;

impl CachedAuthApiKeyReadRepository {
    pub(super) fn new(inner: Arc<dyn AuthApiKeyReadRepository>) -> Self {
        Self {
            inner,
            snapshots: OnceMap::builder()
                .capacity_bytes(AUTH_API_KEY_SNAPSHOT_CACHE_CAPACITY_BYTES)
                .max_inflight(AUTH_API_KEY_SNAPSHOT_CACHE_MAX_INFLIGHT)
                .build(),
        }
    }

    pub(super) fn clear_cache(&self) {
        self.snapshots.invalidate_all();
    }

    fn cache_key(key: AuthApiKeyLookupKey<'_>) -> AuthApiKeySnapshotCacheKey {
        match key {
            AuthApiKeyLookupKey::KeyHash(value) => {
                AuthApiKeySnapshotCacheKey::KeyHash(value.to_string())
            }
            AuthApiKeyLookupKey::ApiKeyId(value) => {
                AuthApiKeySnapshotCacheKey::ApiKeyId(value.to_string())
            }
            AuthApiKeyLookupKey::UserApiKeyIds {
                user_id,
                api_key_id,
            } => AuthApiKeySnapshotCacheKey::UserApiKeyIds {
                user_id: user_id.to_string(),
                api_key_id: api_key_id.to_string(),
            },
        }
    }
}

impl super::GatewayDataState {
    pub(crate) fn clear_auth_api_key_read_cache(&self) {
        if let Some(repository) = self.auth_api_key_reader.as_ref() {
            repository.clear_cache();
        }
    }

    #[cfg(test)]
    pub(crate) fn with_cached_auth_api_key_repository_for_tests<T>(repository: Arc<T>) -> Self
    where
        T: AuthRepository + 'static,
    {
        let inner: Arc<dyn AuthApiKeyReadRepository> = repository.clone();
        let cached: Arc<dyn AuthApiKeyReadRepository> =
            Arc::new(CachedAuthApiKeyReadRepository::new(inner));
        let mut state = Self::with_auth_api_key_repository_for_tests(repository);
        state.auth_api_key_reader = Some(cached);
        state
    }

    pub(crate) async fn read_auth_api_key_snapshot_strong(
        &self,
        user_id: &str,
        api_key_id: &str,
        now_unix_secs: u64,
    ) -> Result<Option<crate::data::auth::GatewayAuthApiKeySnapshot>, DataLayerError> {
        AUTH_API_KEY_READ_CACHE_BYPASS
            .scope(
                (),
                self.read_auth_api_key_snapshot(user_id, api_key_id, now_unix_secs),
            )
            .await
    }

    pub(crate) async fn read_auth_api_key_snapshot_by_key_hash_strong(
        &self,
        key_hash: &str,
        now_unix_secs: u64,
    ) -> Result<Option<crate::data::auth::GatewayAuthApiKeySnapshot>, DataLayerError> {
        AUTH_API_KEY_READ_CACHE_BYPASS
            .scope(
                (),
                self.read_auth_api_key_snapshot_by_key_hash(key_hash, now_unix_secs),
            )
            .await
    }
}

impl CacheWeight for AuthApiKeySnapshotCacheKey {
    fn cache_weight(&self) -> usize {
        match self {
            Self::KeyHash(value) | Self::ApiKeyId(value) => value_weight::<Self>(value.capacity()),
            Self::UserApiKeyIds {
                user_id,
                api_key_id,
            } => value_weight::<Self>(user_id.capacity() + api_key_id.capacity()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum AuthApiKeySnapshotCacheKey {
    KeyHash(String),
    ApiKeyId(String),
    UserApiKeyIds { user_id: String, api_key_id: String },
}

#[async_trait]
impl AuthApiKeyReadRepository for CachedAuthApiKeyReadRepository {
    async fn find_api_key_snapshot(
        &self,
        key: AuthApiKeyLookupKey<'_>,
    ) -> Result<Option<StoredAuthApiKeySnapshot>, DataLayerError> {
        if AUTH_API_KEY_READ_CACHE_BYPASS
            .try_with(|_| true)
            .unwrap_or(false)
        {
            return self.inner.find_api_key_snapshot(key).await;
        }

        let cache_key = Self::cache_key(key);
        self.snapshots
            .get_or_load(cache_key, AUTH_API_KEY_SNAPSHOT_CACHE_TTL, || async {
                self.inner
                    .find_api_key_snapshot(key)
                    .await
                    .map(CachedAuthApiKeySnapshot)
            })
            .await
            .map(|cached| cached.0.clone())
            .map_err(|error| error.or_else_load(DataLayerError::TimedOut))
    }

    async fn list_api_key_snapshots_by_ids(
        &self,
        api_key_ids: &[String],
    ) -> Result<Vec<StoredAuthApiKeySnapshot>, DataLayerError> {
        let mut snapshots = Vec::with_capacity(api_key_ids.len());
        for api_key_id in api_key_ids {
            if let Some(snapshot) = self
                .find_api_key_snapshot(AuthApiKeyLookupKey::ApiKeyId(api_key_id))
                .await?
            {
                snapshots.push(snapshot);
            }
        }
        Ok(snapshots)
    }

    fn clear_cache(&self) {
        CachedAuthApiKeyReadRepository::clear_cache(self);
    }

    async fn list_export_api_keys_by_user_ids(
        &self,
        user_ids: &[String],
    ) -> Result<Vec<StoredAuthApiKeyExportRecord>, DataLayerError> {
        self.inner.list_export_api_keys_by_user_ids(user_ids).await
    }

    async fn list_export_api_keys_by_ids(
        &self,
        api_key_ids: &[String],
    ) -> Result<Vec<StoredAuthApiKeyExportRecord>, DataLayerError> {
        self.inner.list_export_api_keys_by_ids(api_key_ids).await
    }

    async fn list_export_api_keys_by_name_search(
        &self,
        name_search: &str,
    ) -> Result<Vec<StoredAuthApiKeyExportRecord>, DataLayerError> {
        self.inner
            .list_export_api_keys_by_name_search(name_search)
            .await
    }

    async fn list_export_standalone_api_keys_page(
        &self,
        query: &StandaloneApiKeyExportListQuery,
    ) -> Result<Vec<StoredAuthApiKeyExportRecord>, DataLayerError> {
        self.inner.list_export_standalone_api_keys_page(query).await
    }

    async fn count_export_standalone_api_keys(
        &self,
        is_active: Option<bool>,
    ) -> Result<u64, DataLayerError> {
        self.inner.count_export_standalone_api_keys(is_active).await
    }

    async fn summarize_export_api_keys_by_user_ids(
        &self,
        user_ids: &[String],
        now_unix_secs: u64,
    ) -> Result<AuthApiKeyExportSummary, DataLayerError> {
        self.inner
            .summarize_export_api_keys_by_user_ids(user_ids, now_unix_secs)
            .await
    }

    async fn summarize_export_non_standalone_api_keys(
        &self,
        now_unix_secs: u64,
    ) -> Result<AuthApiKeyExportSummary, DataLayerError> {
        self.inner
            .summarize_export_non_standalone_api_keys(now_unix_secs)
            .await
    }

    async fn summarize_export_standalone_api_keys(
        &self,
        now_unix_secs: u64,
    ) -> Result<AuthApiKeyExportSummary, DataLayerError> {
        self.inner
            .summarize_export_standalone_api_keys(now_unix_secs)
            .await
    }

    async fn find_export_standalone_api_key_by_id(
        &self,
        api_key_id: &str,
    ) -> Result<Option<StoredAuthApiKeyExportRecord>, DataLayerError> {
        self.inner
            .find_export_standalone_api_key_by_id(api_key_id)
            .await
    }

    async fn list_export_standalone_api_keys(
        &self,
    ) -> Result<Vec<StoredAuthApiKeyExportRecord>, DataLayerError> {
        self.inner.list_export_standalone_api_keys().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aether_data::repository::auth::InMemoryAuthApiKeySnapshotRepository;

    fn sample_snapshot(api_key_id: &str, user_id: &str) -> StoredAuthApiKeySnapshot {
        StoredAuthApiKeySnapshot::new(
            user_id.to_string(),
            "alice".to_string(),
            Some("alice@example.com".to_string()),
            "user".to_string(),
            "local".to_string(),
            true,
            false,
            Some(serde_json::json!(["openai"])),
            Some(serde_json::json!(["openai:chat"])),
            Some(serde_json::json!(["gpt-4.1"])),
            api_key_id.to_string(),
            Some("default".to_string()),
            true,
            false,
            false,
            Some(60),
            Some(5),
            Some(200),
            Some(serde_json::json!(["openai"])),
            Some(serde_json::json!(["openai:chat"])),
            Some(serde_json::json!(["gpt-4.1"])),
        )
        .expect("snapshot should build")
    }

    #[tokio::test]
    async fn concurrent_same_key_loads_once_and_reuses_cached_snapshot() {
        let inner = Arc::new(
            InMemoryAuthApiKeySnapshotRepository::seed([(
                None,
                sample_snapshot("key-a", "user-a"),
            )])
            .with_lookup_delay_for_tests(Duration::from_millis(25)),
        );
        let repository = Arc::new(CachedAuthApiKeyReadRepository::new(inner.clone()));
        let mut tasks = Vec::new();

        for _ in 0..32 {
            let repository = Arc::clone(&repository);
            tasks.push(tokio::spawn(async move {
                repository
                    .find_api_key_snapshot(AuthApiKeyLookupKey::ApiKeyId("key-a"))
                    .await
                    .expect("cached lookup should succeed")
                    .expect("snapshot should exist")
            }));
        }

        for task in tasks {
            assert_eq!(
                task.await.expect("lookup task should join").api_key_id,
                "key-a"
            );
        }
        assert_eq!(inner.snapshot_lookup_count("key-a"), 1);
    }

    #[tokio::test]
    async fn strong_read_bypasses_a_fresh_cached_allow_snapshot() {
        let inner = Arc::new(InMemoryAuthApiKeySnapshotRepository::seed([(
            None,
            sample_snapshot("key-a", "user-a"),
        )]));
        let repository = CachedAuthApiKeyReadRepository::new(inner.clone());
        let lookup = AuthApiKeyLookupKey::UserApiKeyIds {
            user_id: "user-a",
            api_key_id: "key-a",
        };

        let cached = repository
            .find_api_key_snapshot(lookup)
            .await
            .expect("initial lookup should succeed")
            .expect("snapshot should exist");
        assert!(!cached.api_key_is_locked);
        assert!(inner
            .set_user_api_key_locked("user-a", "key-a", true)
            .await
            .expect("cross-node lock should succeed"));

        let still_cached = repository
            .find_api_key_snapshot(lookup)
            .await
            .expect("cached lookup should succeed")
            .expect("snapshot should exist");
        assert!(!still_cached.api_key_is_locked);

        let strong = AUTH_API_KEY_READ_CACHE_BYPASS
            .scope((), repository.find_api_key_snapshot(lookup))
            .await
            .expect("strong lookup should succeed")
            .expect("snapshot should exist");
        assert!(strong.api_key_is_locked);
        assert_eq!(inner.snapshot_lookup_count("key-a"), 2);
    }
}
