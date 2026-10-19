use std::sync::Arc;
use std::time::Duration;

use aether_cache::Snapshot;
use aether_data::DataLayerError;
use aether_data_contracts::repository::provider_catalog::{
    ProviderCatalogKeyListQuery, ProviderCatalogReadRepository, StoredProviderCatalogEndpoint,
    StoredProviderCatalogKey, StoredProviderCatalogKeyMaintenanceSummary,
    StoredProviderCatalogKeyPage, StoredProviderCatalogKeyStats, StoredProviderCatalogProvider,
};
use async_trait::async_trait;

// A missed cross-node invalidation may not keep routing on a catalog older than
// this; the local write path drops the version explicitly.
const PROVIDER_CATALOG_SNAPSHOT_TTL: Duration = Duration::from_secs(5);

/// The whole provider catalog, loaded as one version.
///
/// The catalog is administrator-written and read on the request path. Caching it
/// per query shape meant the same tables were cut into nine entries with nine
/// independent freshness windows and nine ways to reload them; here one load
/// produces one immutable version and every read answers from it.
pub(super) struct CachedProviderCatalogReadRepository {
    inner: Arc<dyn ProviderCatalogReadRepository>,
    catalog: Snapshot<ProviderCatalogSnapshot>,
}

struct ProviderCatalogSnapshot {
    /// `ORDER BY provider_priority ASC, name ASC`, as `list_providers(false)`.
    providers: Vec<StoredProviderCatalogProvider>,
    /// `ORDER BY provider_id ASC, api_format ASC, id ASC`.
    endpoints: Vec<StoredProviderCatalogEndpoint>,
    /// `ORDER BY provider_id ASC, name ASC, id ASC`.
    keys: Vec<StoredProviderCatalogKey>,
}

impl CachedProviderCatalogReadRepository {
    pub(super) fn new(inner: Arc<dyn ProviderCatalogReadRepository>) -> Self {
        Self {
            inner,
            catalog: Snapshot::new(),
        }
    }

    async fn catalog(&self) -> Result<Arc<ProviderCatalogSnapshot>, DataLayerError> {
        self.catalog
            .get_or_load(PROVIDER_CATALOG_SNAPSHOT_TTL, || self.load_catalog())
            .await
    }

    /// Reads that the version cannot answer go to the repository: a key or
    /// endpoint whose provider was not loaded is invisible in the snapshot.
    async fn covers_providers(&self, provider_ids: &[String]) -> Result<bool, DataLayerError> {
        Ok(self.catalog().await?.covers_providers(provider_ids))
    }

    async fn load_catalog(&self) -> Result<ProviderCatalogSnapshot, DataLayerError> {
        let providers = self.inner.list_providers(false).await?;
        let provider_ids = providers
            .iter()
            .map(|provider| provider.id.clone())
            .collect::<Vec<_>>();
        if provider_ids.is_empty() {
            return Ok(ProviderCatalogSnapshot {
                providers,
                endpoints: Vec::new(),
                keys: Vec::new(),
            });
        }
        let endpoints = self
            .inner
            .list_endpoints_by_provider_ids(&provider_ids)
            .await?;
        let keys = self.inner.list_keys_by_provider_ids(&provider_ids).await?;
        Ok(ProviderCatalogSnapshot {
            providers,
            endpoints,
            keys,
        })
    }
}

fn contains_id(ids: &[String], id: &str) -> bool {
    ids.iter().any(|candidate| candidate == id)
}

impl ProviderCatalogSnapshot {
    /// Whether every requested provider was part of the load. A provider that
    /// was never loaded (deleted, or seeded without a provider row) cannot be
    /// answered from the version, so those reads go to the repository.
    fn covers_providers(&self, provider_ids: &[String]) -> bool {
        provider_ids
            .iter()
            .all(|id| self.providers.iter().any(|provider| &provider.id == id))
    }

    fn covers_endpoint_ids(&self, endpoint_ids: &[String]) -> bool {
        endpoint_ids
            .iter()
            .all(|id| self.endpoints.iter().any(|endpoint| &endpoint.id == id))
    }

    fn covers_key_ids(&self, key_ids: &[String]) -> bool {
        key_ids
            .iter()
            .all(|id| self.keys.iter().any(|key| &key.id == id))
    }

    /// `list_providers(false)` order, filtered.
    fn providers(&self, active_only: bool) -> Vec<StoredProviderCatalogProvider> {
        self.providers
            .iter()
            .filter(|provider| !active_only || provider.is_active)
            .cloned()
            .collect()
    }

    /// `ORDER BY name ASC`, as `list_providers_by_ids`.
    fn providers_by_ids(&self, provider_ids: &[String]) -> Vec<StoredProviderCatalogProvider> {
        let mut rows = self
            .providers
            .iter()
            .filter(|provider| contains_id(provider_ids, &provider.id))
            .cloned()
            .collect::<Vec<_>>();
        rows.sort_by(|left, right| left.name.cmp(&right.name));
        rows
    }

    /// `ORDER BY api_format ASC, id ASC`, as `list_endpoints_by_ids`.
    fn endpoints_by_ids(&self, endpoint_ids: &[String]) -> Vec<StoredProviderCatalogEndpoint> {
        let mut rows = self
            .endpoints
            .iter()
            .filter(|endpoint| contains_id(endpoint_ids, &endpoint.id))
            .cloned()
            .collect::<Vec<_>>();
        rows.sort_by(|left, right| {
            left.api_format
                .cmp(&right.api_format)
                .then_with(|| left.id.cmp(&right.id))
        });
        rows
    }

    /// `ORDER BY provider_id ASC, api_format ASC, id ASC`.
    fn endpoints_by_provider_ids(
        &self,
        provider_ids: &[String],
    ) -> Vec<StoredProviderCatalogEndpoint> {
        let mut rows = self
            .endpoints
            .iter()
            .filter(|endpoint| contains_id(provider_ids, &endpoint.provider_id))
            .cloned()
            .collect::<Vec<_>>();
        rows.sort_by(|left, right| {
            left.provider_id
                .cmp(&right.provider_id)
                .then_with(|| left.api_format.cmp(&right.api_format))
                .then_with(|| left.id.cmp(&right.id))
        });
        rows
    }

    /// `ORDER BY name ASC, id ASC`, as `list_keys_by_ids`.
    fn keys_by_ids(&self, key_ids: &[String]) -> Vec<StoredProviderCatalogKey> {
        let mut rows = self
            .keys
            .iter()
            .filter(|key| contains_id(key_ids, &key.id))
            .cloned()
            .collect::<Vec<_>>();
        rows.sort_by(|left, right| {
            left.name
                .cmp(&right.name)
                .then_with(|| left.id.cmp(&right.id))
        });
        rows
    }

    /// `ORDER BY provider_id ASC, name ASC, id ASC`.
    fn keys_by_provider_ids(&self, provider_ids: &[String]) -> Vec<StoredProviderCatalogKey> {
        let mut rows = self
            .keys
            .iter()
            .filter(|key| contains_id(provider_ids, &key.provider_id))
            .cloned()
            .collect::<Vec<_>>();
        rows.sort_by(|left, right| {
            left.provider_id
                .cmp(&right.provider_id)
                .then_with(|| left.name.cmp(&right.name))
                .then_with(|| left.id.cmp(&right.id))
        });
        rows
    }

    /// `ORDER BY provider_id ASC, id ASC` over the masked projection.
    fn key_summaries(&self, provider_ids: &[String]) -> Vec<StoredProviderCatalogKey> {
        let mut rows = self
            .keys
            .iter()
            .filter(|key| contains_id(provider_ids, &key.provider_id))
            .collect::<Vec<_>>();
        rows.sort_by(|left, right| {
            left.provider_id
                .cmp(&right.provider_id)
                .then_with(|| left.id.cmp(&right.id))
        });
        rows.into_iter().map(mask_key_summary).collect()
    }

    /// `ORDER BY provider_id ASC, id ASC` over the four-column projection.
    fn key_maintenance_summaries(
        &self,
        provider_ids: &[String],
    ) -> Vec<StoredProviderCatalogKeyMaintenanceSummary> {
        let mut rows = self
            .keys
            .iter()
            .filter(|key| contains_id(provider_ids, &key.provider_id))
            .collect::<Vec<_>>();
        rows.sort_by(|left, right| {
            left.provider_id
                .cmp(&right.provider_id)
                .then_with(|| left.id.cmp(&right.id))
        });
        rows.into_iter()
            .map(|key| StoredProviderCatalogKeyMaintenanceSummary {
                id: key.id.clone(),
                provider_id: key.provider_id.clone(),
                is_active: key.is_active,
                upstream_metadata: key.upstream_metadata.clone(),
            })
            .collect()
    }

    /// `GROUP BY provider_id ORDER BY provider_id ASC`, from the same rows.
    fn key_stats(&self, provider_ids: &[String]) -> Vec<StoredProviderCatalogKeyStats> {
        let mut totals = std::collections::BTreeMap::<&str, (u64, u64)>::new();
        for key in self
            .keys
            .iter()
            .filter(|key| contains_id(provider_ids, &key.provider_id))
        {
            let entry = totals.entry(key.provider_id.as_str()).or_default();
            entry.0 += 1;
            if key.is_active {
                entry.1 += 1;
            }
        }
        totals
            .into_iter()
            .map(
                |(provider_id, (total_keys, active_keys))| StoredProviderCatalogKeyStats {
                    provider_id: provider_id.to_string(),
                    total_keys,
                    active_keys,
                },
            )
            .collect()
    }
}

/// The masked projection `list_key_summaries_by_provider_ids` selects.
///
/// Mirrors `LIST_KEY_SUMMARIES_BY_PROVIDER_IDS_PREFIX`: the summary surface must
/// never see `api_key`, `auth_config`, capabilities, limits or usage counters.
/// Keep this aligned with that query column by column — the row mapper's
/// defaults for the NULLed integers are 50 / 5 / 32.
fn mask_key_summary(key: &StoredProviderCatalogKey) -> StoredProviderCatalogKey {
    StoredProviderCatalogKey {
        id: key.id.clone(),
        provider_id: key.provider_id.clone(),
        name: if key.name.is_empty() {
            key.id.clone()
        } else {
            key.name.clone()
        },
        auth_type: if key.auth_type.is_empty() {
            "summary".to_string()
        } else {
            key.auth_type.clone()
        },
        capabilities: None,
        is_active: key.is_active,
        api_formats: key.api_formats.clone(),
        auth_type_by_format: None,
        allow_auth_channel_mismatch_formats: None,
        encrypted_api_key: Some("summary".to_string()),
        encrypted_auth_config: key.encrypted_auth_config.as_ref().map(|_| "{}".to_string()),
        note: None,
        internal_priority: 50,
        rate_multipliers: None,
        global_priority_by_format: None,
        allowed_models: None,
        expires_at_unix_secs: None,
        cache_ttl_minutes: 5,
        max_probe_interval_minutes: 32,
        proxy: None,
        fingerprint: None,
        rpm_limit: None,
        concurrent_limit: None,
        learned_rpm_limit: None,
        concurrent_429_count: None,
        rpm_429_count: None,
        last_429_at_unix_secs: None,
        last_429_type: None,
        adjustment_history: None,
        utilization_samples: None,
        last_probe_increase_at_unix_secs: None,
        last_rpm_peak: None,
        request_count: None,
        total_tokens: 0,
        total_cost_usd: 0.0,
        success_count: None,
        error_count: None,
        total_response_time_ms: None,
        last_used_at_unix_secs: None,
        auto_fetch_models: false,
        last_models_fetch_at_unix_secs: None,
        last_models_fetch_error: None,
        locked_models: None,
        model_include_patterns: None,
        model_exclude_patterns: None,
        upstream_metadata: None,
        oauth_invalid_at_unix_secs: None,
        oauth_invalid_reason: None,
        status_snapshot: None,
        created_at_unix_ms: None,
        updated_at_unix_secs: None,
        health_by_format: key.health_by_format.clone(),
        circuit_breaker_by_format: None,
    }
}

#[async_trait]
impl ProviderCatalogReadRepository for CachedProviderCatalogReadRepository {
    fn clear_local_cache(&self) {
        self.catalog.invalidate();
        self.inner.clear_local_cache();
    }

    async fn list_providers(
        &self,
        active_only: bool,
    ) -> Result<Vec<StoredProviderCatalogProvider>, DataLayerError> {
        Ok(self.catalog().await?.providers(active_only))
    }

    async fn list_providers_by_ids(
        &self,
        provider_ids: &[String],
    ) -> Result<Vec<StoredProviderCatalogProvider>, DataLayerError> {
        if !self.covers_providers(provider_ids).await? {
            return self.inner.list_providers_by_ids(provider_ids).await;
        }
        Ok(self.catalog().await?.providers_by_ids(provider_ids))
    }

    async fn list_endpoints_by_ids(
        &self,
        endpoint_ids: &[String],
    ) -> Result<Vec<StoredProviderCatalogEndpoint>, DataLayerError> {
        if !self.catalog().await?.covers_endpoint_ids(endpoint_ids) {
            return self.inner.list_endpoints_by_ids(endpoint_ids).await;
        }
        Ok(self.catalog().await?.endpoints_by_ids(endpoint_ids))
    }

    async fn list_endpoints_by_provider_ids(
        &self,
        provider_ids: &[String],
    ) -> Result<Vec<StoredProviderCatalogEndpoint>, DataLayerError> {
        if !self.covers_providers(provider_ids).await? {
            return self
                .inner
                .list_endpoints_by_provider_ids(provider_ids)
                .await;
        }
        Ok(self
            .catalog()
            .await?
            .endpoints_by_provider_ids(provider_ids))
    }

    async fn list_keys_by_ids(
        &self,
        key_ids: &[String],
    ) -> Result<Vec<StoredProviderCatalogKey>, DataLayerError> {
        if !self.catalog().await?.covers_key_ids(key_ids) {
            return self.inner.list_keys_by_ids(key_ids).await;
        }
        Ok(self.catalog().await?.keys_by_ids(key_ids))
    }

    async fn list_keys_by_ids_strong(
        &self,
        key_ids: &[String],
    ) -> Result<Vec<StoredProviderCatalogKey>, DataLayerError> {
        // A credential fence must not read whatever version happens to be held.
        self.inner.list_keys_by_ids_strong(key_ids).await
    }

    async fn list_keys_by_provider_ids(
        &self,
        provider_ids: &[String],
    ) -> Result<Vec<StoredProviderCatalogKey>, DataLayerError> {
        if !self.covers_providers(provider_ids).await? {
            return self.inner.list_keys_by_provider_ids(provider_ids).await;
        }
        Ok(self.catalog().await?.keys_by_provider_ids(provider_ids))
    }

    async fn list_key_summaries_by_provider_ids(
        &self,
        provider_ids: &[String],
    ) -> Result<Vec<StoredProviderCatalogKey>, DataLayerError> {
        if !self.covers_providers(provider_ids).await? {
            return self
                .inner
                .list_key_summaries_by_provider_ids(provider_ids)
                .await;
        }
        Ok(self.catalog().await?.key_summaries(provider_ids))
    }

    async fn list_key_maintenance_summaries_by_provider_ids(
        &self,
        provider_ids: &[String],
    ) -> Result<Vec<StoredProviderCatalogKeyMaintenanceSummary>, DataLayerError> {
        if !self.covers_providers(provider_ids).await? {
            return self
                .inner
                .list_key_maintenance_summaries_by_provider_ids(provider_ids)
                .await;
        }
        Ok(self
            .catalog()
            .await?
            .key_maintenance_summaries(provider_ids))
    }

    /// Paged reads carry their own ordering and limit; they are not part of the
    /// whole-table version.
    async fn list_keys_page(
        &self,
        query: &ProviderCatalogKeyListQuery,
    ) -> Result<StoredProviderCatalogKeyPage, DataLayerError> {
        self.inner.list_keys_page(query).await
    }

    async fn list_key_stats_by_provider_ids(
        &self,
        provider_ids: &[String],
    ) -> Result<Vec<StoredProviderCatalogKeyStats>, DataLayerError> {
        if !self.covers_providers(provider_ids).await? {
            return self
                .inner
                .list_key_stats_by_provider_ids(provider_ids)
                .await;
        }
        Ok(self.catalog().await?.key_stats(provider_ids))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    #[derive(Default)]
    struct CountingCatalogRepository {
        provider_loads: AtomicUsize,
        endpoint_loads: AtomicUsize,
        key_loads: AtomicUsize,
        strong_loads: AtomicUsize,
        providers: Vec<StoredProviderCatalogProvider>,
        endpoints: Vec<StoredProviderCatalogEndpoint>,
        keys: Vec<StoredProviderCatalogKey>,
    }

    #[async_trait]
    impl ProviderCatalogReadRepository for CountingCatalogRepository {
        async fn list_providers(
            &self,
            _active_only: bool,
        ) -> Result<Vec<StoredProviderCatalogProvider>, DataLayerError> {
            self.provider_loads.fetch_add(1, Ordering::AcqRel);
            tokio::time::sleep(Duration::from_millis(10)).await;
            Ok(self.providers.clone())
        }

        async fn list_providers_by_ids(
            &self,
            _provider_ids: &[String],
        ) -> Result<Vec<StoredProviderCatalogProvider>, DataLayerError> {
            panic!("by-id reads must be answered from the version");
        }

        async fn list_endpoints_by_ids(
            &self,
            _endpoint_ids: &[String],
        ) -> Result<Vec<StoredProviderCatalogEndpoint>, DataLayerError> {
            panic!("by-id reads must be answered from the version");
        }

        async fn list_endpoints_by_provider_ids(
            &self,
            _provider_ids: &[String],
        ) -> Result<Vec<StoredProviderCatalogEndpoint>, DataLayerError> {
            self.endpoint_loads.fetch_add(1, Ordering::AcqRel);
            Ok(self.endpoints.clone())
        }

        async fn list_keys_by_ids(
            &self,
            _key_ids: &[String],
        ) -> Result<Vec<StoredProviderCatalogKey>, DataLayerError> {
            panic!("by-id reads must be answered from the version");
        }

        async fn list_keys_by_ids_strong(
            &self,
            _key_ids: &[String],
        ) -> Result<Vec<StoredProviderCatalogKey>, DataLayerError> {
            self.strong_loads.fetch_add(1, Ordering::AcqRel);
            Ok(self.keys.clone())
        }

        async fn list_keys_by_provider_ids(
            &self,
            _provider_ids: &[String],
        ) -> Result<Vec<StoredProviderCatalogKey>, DataLayerError> {
            self.key_loads.fetch_add(1, Ordering::AcqRel);
            Ok(self.keys.clone())
        }

        async fn list_key_summaries_by_provider_ids(
            &self,
            provider_ids: &[String],
        ) -> Result<Vec<StoredProviderCatalogKey>, DataLayerError> {
            // The reference implementation of the summary projection.
            let mut rows = self
                .keys
                .iter()
                .filter(|key| contains_id(provider_ids, &key.provider_id))
                .collect::<Vec<_>>();
            rows.sort_by(|left, right| {
                left.provider_id
                    .cmp(&right.provider_id)
                    .then_with(|| left.id.cmp(&right.id))
            });
            Ok(rows.into_iter().map(mask_key_summary).collect())
        }

        async fn list_key_maintenance_summaries_by_provider_ids(
            &self,
            provider_ids: &[String],
        ) -> Result<Vec<StoredProviderCatalogKeyMaintenanceSummary>, DataLayerError> {
            let mut rows = self
                .keys
                .iter()
                .filter(|key| contains_id(provider_ids, &key.provider_id))
                .collect::<Vec<_>>();
            rows.sort_by(|left, right| {
                left.provider_id
                    .cmp(&right.provider_id)
                    .then_with(|| left.id.cmp(&right.id))
            });
            Ok(rows
                .into_iter()
                .map(|key| StoredProviderCatalogKeyMaintenanceSummary {
                    id: key.id.clone(),
                    provider_id: key.provider_id.clone(),
                    is_active: key.is_active,
                    upstream_metadata: key.upstream_metadata.clone(),
                })
                .collect())
        }

        async fn list_keys_page(
            &self,
            _query: &ProviderCatalogKeyListQuery,
        ) -> Result<StoredProviderCatalogKeyPage, DataLayerError> {
            panic!("paged reads go straight through");
        }

        async fn list_key_stats_by_provider_ids(
            &self,
            provider_ids: &[String],
        ) -> Result<Vec<StoredProviderCatalogKeyStats>, DataLayerError> {
            let ordered = ProviderCatalogSnapshot {
                providers: Vec::new(),
                endpoints: Vec::new(),
                keys: self.keys.clone(),
            };
            Ok(ordered.key_stats(provider_ids))
        }
    }

    fn provider(
        id: &str,
        name: &str,
        is_active: bool,
        provider_priority: i32,
    ) -> StoredProviderCatalogProvider {
        StoredProviderCatalogProvider {
            id: id.to_string(),
            name: name.to_string(),
            description: None,
            website: None,
            provider_type: "custom".to_string(),
            billing_type: None,
            monthly_quota_usd: None,
            monthly_used_usd: None,
            quota_reset_day: None,
            quota_last_reset_at_unix_secs: None,
            quota_expires_at_unix_secs: None,
            provider_priority,
            is_active,
            keep_priority_on_conversion: false,
            enable_format_conversion: false,
            concurrent_limit: None,
            max_retries: None,
            proxy: None,
            request_timeout_secs: None,
            stream_first_byte_timeout_secs: None,
            config: None,
            created_at_unix_ms: None,
            updated_at_unix_secs: None,
        }
    }

    fn endpoint(id: &str, provider_id: &str, api_format: &str) -> StoredProviderCatalogEndpoint {
        StoredProviderCatalogEndpoint {
            id: id.to_string(),
            provider_id: provider_id.to_string(),
            api_format: api_format.to_string(),
            api_family: None,
            endpoint_kind: None,
            is_active: true,
            health_score: 1.0,
            base_url: "https://example.test".to_string(),
            header_rules: None,
            body_rules: None,
            max_retries: None,
            custom_path: None,
            config: None,
            format_acceptance_config: None,
            proxy: None,
            created_at_unix_ms: None,
            updated_at_unix_secs: None,
        }
    }

    fn key(id: &str, provider_id: &str, is_active: bool) -> StoredProviderCatalogKey {
        StoredProviderCatalogKey {
            id: id.to_string(),
            provider_id: provider_id.to_string(),
            name: format!("key-{id}"),
            auth_type: "api_key".to_string(),
            capabilities: Some(serde_json::json!({"vision": true})),
            is_active,
            api_formats: Some(serde_json::json!(["openai:chat"])),
            auth_type_by_format: Some(serde_json::json!({"openai:chat": "api_key"})),
            allow_auth_channel_mismatch_formats: None,
            encrypted_api_key: Some("sk-secret".to_string()),
            encrypted_auth_config: Some("ciphertext".to_string()),
            note: Some("note".to_string()),
            internal_priority: 7,
            rate_multipliers: Some(serde_json::json!({"openai:chat": 1.5})),
            global_priority_by_format: None,
            allowed_models: Some(serde_json::json!(["gpt-5"])),
            expires_at_unix_secs: Some(1_700_000_000),
            cache_ttl_minutes: 9,
            max_probe_interval_minutes: 11,
            proxy: None,
            fingerprint: None,
            rpm_limit: Some(60),
            concurrent_limit: Some(3),
            learned_rpm_limit: None,
            concurrent_429_count: None,
            rpm_429_count: None,
            last_429_at_unix_secs: None,
            last_429_type: None,
            adjustment_history: None,
            utilization_samples: None,
            last_probe_increase_at_unix_secs: None,
            last_rpm_peak: None,
            request_count: Some(42),
            total_tokens: 1234,
            total_cost_usd: 1.5,
            success_count: Some(40),
            error_count: Some(2),
            total_response_time_ms: Some(100),
            last_used_at_unix_secs: None,
            auto_fetch_models: true,
            last_models_fetch_at_unix_secs: None,
            last_models_fetch_error: None,
            locked_models: None,
            model_include_patterns: None,
            model_exclude_patterns: None,
            upstream_metadata: Some(serde_json::json!({"region": "us"})),
            oauth_invalid_at_unix_secs: None,
            oauth_invalid_reason: None,
            status_snapshot: None,
            created_at_unix_ms: Some(1),
            updated_at_unix_secs: Some(2),
            health_by_format: Some(serde_json::json!({"openai:chat": "ok"})),
            circuit_breaker_by_format: None,
        }
    }

    fn repository() -> (
        Arc<CountingCatalogRepository>,
        CachedProviderCatalogReadRepository,
    ) {
        let inner = Arc::new(CountingCatalogRepository {
            providers: vec![
                provider("p-2", "zeta", true, 2),
                provider("p-1", "alpha", false, 1),
            ],
            endpoints: vec![
                endpoint("e-2", "p-1", "openai:responses"),
                endpoint("e-1", "p-1", "openai:chat"),
            ],
            keys: vec![key("k-2", "p-1", true), key("k-1", "p-1", false)],
            ..Default::default()
        });
        let cached = CachedProviderCatalogReadRepository::new(inner.clone());
        (inner, cached)
    }

    #[tokio::test]
    async fn one_version_serves_every_shape() {
        let (inner, repository) = repository();
        let all = ["p-1".to_string(), "p-2".to_string()];
        let key_ids = ["k-1".to_string(), "k-2".to_string()];
        let endpoint_ids = ["e-1".to_string(), "e-2".to_string()];

        for _ in 0..2 {
            assert_eq!(2, repository.list_providers(false).await.unwrap().len());
            assert_eq!(1, repository.list_providers(true).await.unwrap().len());
            assert_eq!(
                2,
                repository.list_providers_by_ids(&all).await.unwrap().len()
            );
            assert_eq!(
                2,
                repository
                    .list_endpoints_by_ids(&endpoint_ids)
                    .await
                    .unwrap()
                    .len()
            );
            assert_eq!(
                2,
                repository
                    .list_endpoints_by_provider_ids(&all)
                    .await
                    .unwrap()
                    .len()
            );
            assert_eq!(
                2,
                repository.list_keys_by_ids(&key_ids).await.unwrap().len()
            );
            assert_eq!(
                2,
                repository
                    .list_keys_by_provider_ids(&all)
                    .await
                    .unwrap()
                    .len()
            );
            repository
                .list_key_summaries_by_provider_ids(&all)
                .await
                .unwrap();
            repository
                .list_key_maintenance_summaries_by_provider_ids(&all)
                .await
                .unwrap();
            repository
                .list_key_stats_by_provider_ids(&all)
                .await
                .unwrap();
        }

        assert_eq!(1, inner.provider_loads.load(Ordering::Acquire));
        assert_eq!(1, inner.endpoint_loads.load(Ordering::Acquire));
        assert_eq!(1, inner.key_loads.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn concurrent_cold_reads_load_one_version() {
        let (inner, repository) = repository();
        let repository = Arc::new(repository);

        let mut tasks = Vec::new();
        for _ in 0..16 {
            let repository = Arc::clone(&repository);
            tasks.push(tokio::spawn(async move {
                repository.list_providers(false).await.unwrap();
            }));
        }
        for task in tasks {
            task.await.unwrap();
        }

        assert_eq!(1, inner.provider_loads.load(Ordering::Acquire));
        assert_eq!(1, inner.endpoint_loads.load(Ordering::Acquire));
        assert_eq!(1, inner.key_loads.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn the_summary_view_masks_credentials_and_counters() {
        let (_inner, repository) = repository();
        let summaries = repository
            .list_key_summaries_by_provider_ids(&["p-1".to_string()])
            .await
            .unwrap();

        assert_eq!(2, summaries.len());
        assert_eq!("k-1", summaries[0].id, "ordered by id");
        for summary in &summaries {
            assert_eq!(Some("summary"), summary.encrypted_api_key.as_deref());
            assert_eq!(Some("{}"), summary.encrypted_auth_config.as_deref());
            assert!(summary.capabilities.is_none());
            assert!(summary.rate_multipliers.is_none());
            assert!(summary.allowed_models.is_none());
            assert_eq!(50, summary.internal_priority);
            assert_eq!(5, summary.cache_ttl_minutes);
            assert_eq!(32, summary.max_probe_interval_minutes);
            assert_eq!(0, summary.total_tokens);
            assert_eq!(0.0, summary.total_cost_usd);
            assert!(!summary.auto_fetch_models);
            assert!(summary.request_count.is_none());
            // Still real: the summary surface shows these.
            assert!(summary.api_formats.is_some());
            assert!(summary.health_by_format.is_some());
        }

        // The full-record read keeps the credentials.
        let keys = repository
            .list_keys_by_ids(&["k-1".to_string()])
            .await
            .unwrap();
        assert_eq!(Some("sk-secret"), keys[0].encrypted_api_key.as_deref());
        assert_eq!(Some("ciphertext"), keys[0].encrypted_auth_config.as_deref());
    }

    #[tokio::test]
    async fn stats_are_grouped_from_the_same_rows() {
        let (_inner, repository) = repository();
        let stats = repository
            .list_key_stats_by_provider_ids(&["p-1".to_string(), "p-2".to_string()])
            .await
            .unwrap();

        assert_eq!(1, stats.len(), "providers without keys produce no row");
        assert_eq!("p-1", stats[0].provider_id);
        assert_eq!(2, stats[0].total_keys);
        assert_eq!(1, stats[0].active_keys);
    }

    #[tokio::test]
    async fn ordering_matches_the_per_shape_queries() {
        let (_inner, repository) = repository();

        let providers = repository
            .list_providers_by_ids(&["p-1".to_string(), "p-2".to_string()])
            .await
            .unwrap();
        assert_eq!(
            vec!["alpha", "zeta"],
            providers
                .iter()
                .map(|p| p.name.as_str())
                .collect::<Vec<_>>()
        );

        let endpoints = repository
            .list_endpoints_by_ids(&["e-1".to_string(), "e-2".to_string()])
            .await
            .unwrap();
        assert_eq!(
            vec!["e-1", "e-2"],
            endpoints.iter().map(|e| e.id.as_str()).collect::<Vec<_>>()
        );

        let keys = repository
            .list_keys_by_ids(&["k-1".to_string(), "k-2".to_string()])
            .await
            .unwrap();
        assert_eq!(
            vec!["k-1", "k-2"],
            keys.iter().map(|k| k.id.as_str()).collect::<Vec<_>>()
        );
    }

    #[tokio::test]
    async fn the_strong_read_bypasses_the_version() {
        let (inner, repository) = repository();

        for _ in 0..2 {
            repository
                .list_keys_by_ids_strong(&["k-1".to_string()])
                .await
                .unwrap();
        }

        assert_eq!(2, inner.strong_loads.load(Ordering::Acquire));
        assert_eq!(0, inner.provider_loads.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn clearing_the_version_forces_a_reload() {
        let (inner, repository) = repository();

        repository.list_providers(false).await.unwrap();
        repository.clear_local_cache();
        repository.list_providers(false).await.unwrap();

        assert_eq!(2, inner.provider_loads.load(Ordering::Acquire));
    }
}
