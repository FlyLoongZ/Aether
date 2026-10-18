use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use aether_cache::{optional_strings_weight, value_weight, CacheWeight, OnceMap};
use aether_data::DataLayerError;
use aether_data_contracts::repository::candidate_selection::{
    MinimalCandidateSelectionReadRepository, StoredApiFormatCandidateRowsQuery,
    StoredMinimalCandidateSelectionRow, StoredPoolKeyCandidateOrder,
    StoredPoolKeyCandidateRowsByKeyIdsQuery, StoredPoolKeyCandidateRowsQuery,
    StoredProviderModelMapping, StoredRequestedModelCandidateRowsQuery,
};
use async_trait::async_trait;
use tokio::time::timeout;
use tracing::warn;

const CANDIDATE_SELECTION_CACHE_TTL: Duration = Duration::from_secs(5);
const CANDIDATE_SELECTION_CACHE_CAPACITY_BYTES: usize = 16 * 1024 * 1024;
const CANDIDATE_SELECTION_CACHE_MAX_INFLIGHT: usize = 4096;
#[cfg(not(test))]
const CANDIDATE_SELECTION_CACHE_LOAD_TIMEOUT: Duration = Duration::from_secs(10);
#[cfg(test)]
const CANDIDATE_SELECTION_CACHE_LOAD_TIMEOUT: Duration = Duration::from_millis(50);

/// A cached candidate page.
///
/// One row owns a dozen strings plus several JSON values, so its weight has to
/// walk them. Only this crate caches pages of rows, so the impl lives here
/// rather than on the contract type.
#[derive(Debug, Clone)]
struct CandidateRows(Vec<StoredMinimalCandidateSelectionRow>);

impl CacheWeight for CandidateRows {
    fn cache_weight(&self) -> usize {
        value_weight::<Self>(
            self.0.capacity() * std::mem::size_of::<StoredMinimalCandidateSelectionRow>(),
        )
        .saturating_add(self.0.iter().map(candidate_row_weight).sum())
    }
}

fn candidate_row_weight(row: &StoredMinimalCandidateSelectionRow) -> usize {
    let strings = row.provider_id.capacity()
        + row.provider_name.capacity()
        + row.provider_type.capacity()
        + row.endpoint_id.capacity()
        + row.endpoint_api_format.capacity()
        + row.key_id.capacity()
        + row.key_name.capacity()
        + row.key_auth_type.capacity()
        + row.model_id.capacity()
        + row.global_model_id.capacity()
        + row.global_model_name.capacity()
        + row.model_provider_model_name.capacity()
        + row.endpoint_api_family.as_ref().map_or(0, String::capacity)
        + row.endpoint_kind.as_ref().map_or(0, String::capacity)
        + optional_strings_weight(row.key_api_formats.as_ref())
        + optional_strings_weight(row.key_allowed_models.as_ref())
        + optional_strings_weight(row.global_model_mappings.as_ref())
        + row
            .key_capabilities
            .as_ref()
            .map_or(0, |value| value.cache_weight())
        + row
            .key_global_priority_by_format
            .as_ref()
            .map_or(0, |value| value.cache_weight())
        + row
            .model_provider_model_mappings
            .as_ref()
            .map_or(0, |mappings| {
                mappings.capacity() * std::mem::size_of::<StoredProviderModelMapping>()
                    + mappings
                        .iter()
                        .map(|mapping| {
                            mapping.name.capacity()
                                + optional_strings_weight(mapping.api_formats.as_ref())
                                + optional_strings_weight(mapping.endpoint_ids.as_ref())
                                + optional_strings_weight(mapping.operations.as_ref())
                        })
                        .sum::<usize>()
            });

    value_weight::<StoredMinimalCandidateSelectionRow>(strings)
}

pub(super) struct CachedMinimalCandidateSelectionReadRepository {
    inner: Arc<dyn MinimalCandidateSelectionReadRepository>,
    cache: OnceMap<CandidateSelectionCacheKey, CandidateRows, DataLayerError>,
}

impl CachedMinimalCandidateSelectionReadRepository {
    pub(super) fn new(inner: Arc<dyn MinimalCandidateSelectionReadRepository>) -> Self {
        Self {
            inner,
            cache: OnceMap::builder()
                .capacity_bytes(CANDIDATE_SELECTION_CACHE_CAPACITY_BYTES)
                .max_inflight(CANDIDATE_SELECTION_CACHE_MAX_INFLIGHT)
                .build(),
        }
    }
    async fn get_or_load<F, Fut>(
        &self,
        key: CandidateSelectionCacheKey,
        load: F,
    ) -> Result<Vec<StoredMinimalCandidateSelectionRow>, DataLayerError>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<Vec<StoredMinimalCandidateSelectionRow>, DataLayerError>>,
    {
        // The loader keeps its own deadline so a timeout is still logged.
        let load_key = key.clone();
        let rows = self
            .cache
            .get_or_load(key, CANDIDATE_SELECTION_CACHE_TTL, || async move {
                load_candidate_selection_rows_with_timeout(&load_key, load())
                    .await
                    .map(CandidateRows)
            })
            .await
            .map_err(|error| error.or_else_load(DataLayerError::TimedOut))?;
        // The repository trait hands out owned rows; the cache keeps one shared
        // page so concurrent readers never build it twice.
        Ok(rows.0.clone())
    }
}

async fn load_candidate_selection_rows_with_timeout<Fut>(
    key: &CandidateSelectionCacheKey,
    load: Fut,
) -> Result<Vec<StoredMinimalCandidateSelectionRow>, DataLayerError>
where
    Fut: Future<Output = Result<Vec<StoredMinimalCandidateSelectionRow>, DataLayerError>>,
{
    match timeout(CANDIDATE_SELECTION_CACHE_LOAD_TIMEOUT, load).await {
        Ok(result) => result,
        Err(_) => {
            warn!(
                event_name = "candidate_selection_cache_load_timeout",
                log_type = "ops",
                cache_key = ?key,
                timeout_ms = CANDIDATE_SELECTION_CACHE_LOAD_TIMEOUT.as_millis() as u64,
                "gateway candidate selection cache load timed out"
            );
            Err(DataLayerError::TimedOut(format!(
                "candidate selection cache load exceeded {}ms for {key:?}",
                CANDIDATE_SELECTION_CACHE_LOAD_TIMEOUT.as_millis()
            )))
        }
    }
}

#[async_trait]
impl MinimalCandidateSelectionReadRepository for CachedMinimalCandidateSelectionReadRepository {
    fn clear_local_cache(&self) {
        self.cache.invalidate_all();
        self.inner.clear_local_cache();
    }

    async fn list_for_exact_api_format(
        &self,
        api_format: &str,
    ) -> Result<Vec<StoredMinimalCandidateSelectionRow>, DataLayerError> {
        let key = CandidateSelectionCacheKey::ApiFormat {
            api_format: normalize_api_format_key(api_format),
        };
        self.get_or_load(key, || self.inner.list_for_exact_api_format(api_format))
            .await
    }

    async fn list_for_exact_api_format_page(
        &self,
        query: &StoredApiFormatCandidateRowsQuery,
    ) -> Result<Vec<StoredMinimalCandidateSelectionRow>, DataLayerError> {
        let key = CandidateSelectionCacheKey::ApiFormatPage {
            api_format: normalize_api_format_key(&query.api_format),
            offset: query.offset,
            limit: query.limit,
        };
        self.get_or_load(key, || self.inner.list_for_exact_api_format_page(query))
            .await
    }

    async fn list_for_exact_api_format_and_global_model(
        &self,
        api_format: &str,
        global_model_name: &str,
    ) -> Result<Vec<StoredMinimalCandidateSelectionRow>, DataLayerError> {
        let key = CandidateSelectionCacheKey::ApiFormatAndGlobalModel {
            api_format: normalize_api_format_key(api_format),
            global_model_name: global_model_name.to_string(),
        };
        self.get_or_load(key, || {
            self.inner
                .list_for_exact_api_format_and_global_model(api_format, global_model_name)
        })
        .await
    }

    async fn list_for_exact_api_format_and_requested_model(
        &self,
        api_format: &str,
        requested_model_name: &str,
    ) -> Result<Vec<StoredMinimalCandidateSelectionRow>, DataLayerError> {
        let key = CandidateSelectionCacheKey::ApiFormatAndRequestedModel {
            api_format: normalize_api_format_key(api_format),
            requested_model_name: requested_model_name.to_string(),
        };
        self.get_or_load(key, || {
            self.inner
                .list_for_exact_api_format_and_requested_model(api_format, requested_model_name)
        })
        .await
    }

    async fn list_for_exact_api_format_and_requested_model_page(
        &self,
        query: &StoredRequestedModelCandidateRowsQuery,
    ) -> Result<Vec<StoredMinimalCandidateSelectionRow>, DataLayerError> {
        let key = CandidateSelectionCacheKey::RequestedModelPage {
            api_format: normalize_api_format_key(&query.api_format),
            requested_model_name: query.requested_model_name.clone(),
            offset: query.offset,
            limit: query.limit,
        };
        self.get_or_load(key, || {
            self.inner
                .list_for_exact_api_format_and_requested_model_page(query)
        })
        .await
    }

    async fn list_pool_key_rows_for_group(
        &self,
        query: &StoredPoolKeyCandidateRowsQuery,
    ) -> Result<Vec<StoredMinimalCandidateSelectionRow>, DataLayerError> {
        let key = CandidateSelectionCacheKey::PoolKeyRowsForGroup {
            api_format: normalize_api_format_key(&query.api_format),
            provider_id: query.provider_id.clone(),
            endpoint_id: query.endpoint_id.clone(),
            model_id: query.model_id.clone(),
            selected_provider_model_name: query.selected_provider_model_name.clone(),
            order: CandidateSelectionPoolOrderKey::from(&query.order),
            offset: query.offset,
            limit: query.limit,
        };
        self.get_or_load(key, || self.inner.list_pool_key_rows_for_group(query))
            .await
    }

    async fn list_pool_key_rows_for_group_key_ids(
        &self,
        query: &StoredPoolKeyCandidateRowsByKeyIdsQuery,
    ) -> Result<Vec<StoredMinimalCandidateSelectionRow>, DataLayerError> {
        let key = CandidateSelectionCacheKey::PoolKeyRowsForGroupKeyIds {
            api_format: normalize_api_format_key(&query.api_format),
            provider_id: query.provider_id.clone(),
            endpoint_id: query.endpoint_id.clone(),
            model_id: query.model_id.clone(),
            selected_provider_model_name: query.selected_provider_model_name.clone(),
            key_ids: query.key_ids.clone(),
        };
        self.get_or_load(key, || {
            self.inner.list_pool_key_rows_for_group_key_ids(query)
        })
        .await
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum CandidateSelectionCacheKey {
    ApiFormat {
        api_format: String,
    },
    ApiFormatPage {
        api_format: String,
        offset: u32,
        limit: u32,
    },
    ApiFormatAndGlobalModel {
        api_format: String,
        global_model_name: String,
    },
    ApiFormatAndRequestedModel {
        api_format: String,
        requested_model_name: String,
    },
    RequestedModelPage {
        api_format: String,
        requested_model_name: String,
        offset: u32,
        limit: u32,
    },
    PoolKeyRowsForGroup {
        api_format: String,
        provider_id: String,
        endpoint_id: String,
        model_id: String,
        selected_provider_model_name: String,
        order: CandidateSelectionPoolOrderKey,
        offset: u32,
        limit: u32,
    },
    PoolKeyRowsForGroupKeyIds {
        api_format: String,
        provider_id: String,
        endpoint_id: String,
        model_id: String,
        selected_provider_model_name: String,
        key_ids: Vec<String>,
    },
}

impl CacheWeight for CandidateSelectionCacheKey {
    fn cache_weight(&self) -> usize {
        let owned = match self {
            Self::ApiFormat { api_format } => api_format.capacity(),
            Self::ApiFormatPage { api_format, .. } => api_format.capacity(),
            Self::ApiFormatAndGlobalModel {
                api_format,
                global_model_name,
            } => api_format.capacity() + global_model_name.capacity(),
            Self::ApiFormatAndRequestedModel {
                api_format,
                requested_model_name,
            } => api_format.capacity() + requested_model_name.capacity(),
            Self::RequestedModelPage {
                api_format,
                requested_model_name,
                ..
            } => api_format.capacity() + requested_model_name.capacity(),
            Self::PoolKeyRowsForGroup {
                api_format,
                provider_id,
                endpoint_id,
                model_id,
                selected_provider_model_name,
                order,
                ..
            } => {
                api_format.capacity()
                    + provider_id.capacity()
                    + endpoint_id.capacity()
                    + model_id.capacity()
                    + selected_provider_model_name.capacity()
                    + order.cache_weight()
            }
            Self::PoolKeyRowsForGroupKeyIds {
                api_format,
                provider_id,
                endpoint_id,
                model_id,
                selected_provider_model_name,
                key_ids,
            } => {
                api_format.capacity()
                    + provider_id.capacity()
                    + endpoint_id.capacity()
                    + model_id.capacity()
                    + selected_provider_model_name.capacity()
                    + value_weight::<Vec<String>>(
                        key_ids.capacity() * std::mem::size_of::<String>(),
                    )
                    + key_ids.iter().map(|id| id.capacity()).sum::<usize>()
            }
        };
        value_weight::<Self>(owned)
    }
}

impl CacheWeight for CandidateSelectionPoolOrderKey {
    fn cache_weight(&self) -> usize {
        match self {
            Self::LoadBalance { seed } => seed.capacity(),
            Self::InternalPriority | Self::Lru | Self::CacheAffinity | Self::SingleAccount => 0,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum CandidateSelectionPoolOrderKey {
    InternalPriority,
    Lru,
    CacheAffinity,
    SingleAccount,
    LoadBalance { seed: String },
}

impl From<&StoredPoolKeyCandidateOrder> for CandidateSelectionPoolOrderKey {
    fn from(order: &StoredPoolKeyCandidateOrder) -> Self {
        match order {
            StoredPoolKeyCandidateOrder::InternalPriority => Self::InternalPriority,
            StoredPoolKeyCandidateOrder::Lru => Self::Lru,
            StoredPoolKeyCandidateOrder::CacheAffinity => Self::CacheAffinity,
            StoredPoolKeyCandidateOrder::SingleAccount => Self::SingleAccount,
            StoredPoolKeyCandidateOrder::LoadBalance { seed } => {
                Self::LoadBalance { seed: seed.clone() }
            }
        }
    }
}

fn normalize_api_format_key(api_format: &str) -> String {
    crate::ai_serving::normalize_api_format_alias(api_format.trim())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::future::pending;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct StubCandidateSelectionRepository {
        calls: AtomicUsize,
        delay: Duration,
        rows: Vec<StoredMinimalCandidateSelectionRow>,
    }

    impl StubCandidateSelectionRepository {
        fn new(delay: Duration) -> Self {
            Self {
                calls: AtomicUsize::new(0),
                delay,
                rows: Vec::new(),
            }
        }

        fn calls(&self) -> usize {
            self.calls.load(Ordering::SeqCst)
        }

        async fn load(&self) -> Result<Vec<StoredMinimalCandidateSelectionRow>, DataLayerError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if !self.delay.is_zero() {
                tokio::time::sleep(self.delay).await;
            }
            Ok(self.rows.clone())
        }
    }

    #[async_trait]
    impl MinimalCandidateSelectionReadRepository for StubCandidateSelectionRepository {
        async fn list_for_exact_api_format(
            &self,
            _api_format: &str,
        ) -> Result<Vec<StoredMinimalCandidateSelectionRow>, DataLayerError> {
            self.load().await
        }

        async fn list_for_exact_api_format_and_global_model(
            &self,
            _api_format: &str,
            _global_model_name: &str,
        ) -> Result<Vec<StoredMinimalCandidateSelectionRow>, DataLayerError> {
            self.load().await
        }

        async fn list_for_exact_api_format_and_requested_model(
            &self,
            _api_format: &str,
            _requested_model_name: &str,
        ) -> Result<Vec<StoredMinimalCandidateSelectionRow>, DataLayerError> {
            self.load().await
        }

        async fn list_for_exact_api_format_and_requested_model_page(
            &self,
            _query: &StoredRequestedModelCandidateRowsQuery,
        ) -> Result<Vec<StoredMinimalCandidateSelectionRow>, DataLayerError> {
            self.load().await
        }

        async fn list_pool_key_rows_for_group(
            &self,
            _query: &StoredPoolKeyCandidateRowsQuery,
        ) -> Result<Vec<StoredMinimalCandidateSelectionRow>, DataLayerError> {
            self.load().await
        }

        async fn list_pool_key_rows_for_group_key_ids(
            &self,
            _query: &StoredPoolKeyCandidateRowsByKeyIdsQuery,
        ) -> Result<Vec<StoredMinimalCandidateSelectionRow>, DataLayerError> {
            self.load().await
        }
    }

    /// The first load never answers; later loads return immediately.
    struct FirstLoadPendingThenFastRepository {
        calls: AtomicUsize,
    }

    impl FirstLoadPendingThenFastRepository {
        fn new() -> Self {
            Self {
                calls: AtomicUsize::new(0),
            }
        }

        fn calls(&self) -> usize {
            self.calls.load(Ordering::SeqCst)
        }

        async fn load(&self) -> Result<Vec<StoredMinimalCandidateSelectionRow>, DataLayerError> {
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            if call == 0 {
                pending::<()>().await;
            }
            Ok(Vec::new())
        }
    }

    #[async_trait]
    impl MinimalCandidateSelectionReadRepository for FirstLoadPendingThenFastRepository {
        async fn list_for_exact_api_format(
            &self,
            _api_format: &str,
        ) -> Result<Vec<StoredMinimalCandidateSelectionRow>, DataLayerError> {
            self.load().await
        }

        async fn list_for_exact_api_format_and_global_model(
            &self,
            _api_format: &str,
            _global_model_name: &str,
        ) -> Result<Vec<StoredMinimalCandidateSelectionRow>, DataLayerError> {
            self.load().await
        }

        async fn list_for_exact_api_format_and_requested_model(
            &self,
            _api_format: &str,
            _requested_model_name: &str,
        ) -> Result<Vec<StoredMinimalCandidateSelectionRow>, DataLayerError> {
            self.load().await
        }

        async fn list_for_exact_api_format_and_requested_model_page(
            &self,
            _query: &StoredRequestedModelCandidateRowsQuery,
        ) -> Result<Vec<StoredMinimalCandidateSelectionRow>, DataLayerError> {
            self.load().await
        }

        async fn list_pool_key_rows_for_group(
            &self,
            _query: &StoredPoolKeyCandidateRowsQuery,
        ) -> Result<Vec<StoredMinimalCandidateSelectionRow>, DataLayerError> {
            self.load().await
        }

        async fn list_pool_key_rows_for_group_key_ids(
            &self,
            _query: &StoredPoolKeyCandidateRowsByKeyIdsQuery,
        ) -> Result<Vec<StoredMinimalCandidateSelectionRow>, DataLayerError> {
            self.load().await
        }
    }

    fn requested_model_page(offset: u32, limit: u32) -> StoredRequestedModelCandidateRowsQuery {
        StoredRequestedModelCandidateRowsQuery {
            api_format: "openai:chat".to_string(),
            requested_model_name: "gpt-5.5".to_string(),
            offset,
            limit,
        }
    }

    #[tokio::test]
    async fn concurrent_misses_load_once() {
        let inner = Arc::new(StubCandidateSelectionRepository::new(
            Duration::from_millis(25),
        ));
        let cache = Arc::new(CachedMinimalCandidateSelectionReadRepository::new(
            inner.clone(),
        ));

        let mut tasks = Vec::new();
        for _ in 0..16 {
            let cache = cache.clone();
            tasks.push(tokio::spawn(async move {
                cache.list_for_exact_api_format("openai").await.unwrap();
            }));
        }
        for task in tasks {
            task.await.unwrap();
        }

        assert_eq!(inner.calls(), 1);
    }

    #[tokio::test]
    async fn a_repeat_read_is_served_from_the_cache() {
        let inner = Arc::new(StubCandidateSelectionRepository::new(Duration::ZERO));
        let cache = CachedMinimalCandidateSelectionReadRepository::new(inner.clone());

        cache.list_for_exact_api_format("openai").await.unwrap();
        cache.list_for_exact_api_format("openai").await.unwrap();

        assert_eq!(inner.calls(), 1);
    }

    #[tokio::test]
    async fn pages_of_the_same_query_do_not_share_an_entry() {
        let inner = Arc::new(StubCandidateSelectionRepository::new(Duration::ZERO));
        let cache = CachedMinimalCandidateSelectionReadRepository::new(inner.clone());

        cache
            .list_for_exact_api_format_page(&StoredApiFormatCandidateRowsQuery {
                api_format: "openai:chat".to_string(),
                offset: 0,
                limit: 256,
            })
            .await
            .unwrap();
        cache
            .list_for_exact_api_format_page(&StoredApiFormatCandidateRowsQuery {
                api_format: "openai:chat".to_string(),
                offset: 256,
                limit: 256,
            })
            .await
            .unwrap();

        assert_eq!(inner.calls(), 2);
    }

    #[tokio::test]
    async fn load_balance_seeds_get_their_own_entries() {
        let inner = Arc::new(StubCandidateSelectionRepository {
            calls: AtomicUsize::new(0),
            delay: Duration::ZERO,
            rows: vec![
                sample_row("key-a", 1),
                sample_row("key-b", 2),
                sample_row("key-c", 3),
            ],
        });
        let cache = CachedMinimalCandidateSelectionReadRepository::new(inner.clone());
        let query = |seed: &str| StoredPoolKeyCandidateRowsQuery {
            api_format: "openai:chat".to_string(),
            provider_id: "provider-1".to_string(),
            endpoint_id: "endpoint-1".to_string(),
            model_id: "model-1".to_string(),
            selected_provider_model_name: "mock-model".to_string(),
            order: StoredPoolKeyCandidateOrder::LoadBalance {
                seed: seed.to_string(),
            },
            offset: 0,
            limit: 2,
        };

        cache
            .list_pool_key_rows_for_group(&query("seed-a"))
            .await
            .unwrap();
        cache
            .list_pool_key_rows_for_group(&query("seed-b"))
            .await
            .unwrap();
        assert_eq!(inner.calls(), 2);

        let repeated = cache
            .list_pool_key_rows_for_group(&query("seed-a"))
            .await
            .unwrap();
        assert_eq!(inner.calls(), 2);
        assert_eq!(repeated.len(), 3);
    }

    #[tokio::test]
    async fn clearing_the_cache_forces_a_reload() {
        let inner = Arc::new(StubCandidateSelectionRepository::new(Duration::ZERO));
        let cache = CachedMinimalCandidateSelectionReadRepository::new(inner.clone());

        cache.list_for_exact_api_format("openai").await.unwrap();
        cache.list_for_exact_api_format("openai").await.unwrap();
        assert_eq!(inner.calls(), 1);

        cache.clear_local_cache();
        cache.list_for_exact_api_format("openai").await.unwrap();

        assert_eq!(inner.calls(), 2);
    }

    #[tokio::test]
    async fn a_leader_that_never_answers_times_out_and_the_next_read_succeeds() {
        let inner = Arc::new(FirstLoadPendingThenFastRepository::new());
        let cache = CachedMinimalCandidateSelectionReadRepository::new(inner.clone());

        let error = cache
            .list_for_exact_api_format_and_requested_model_page(&requested_model_page(0, 64))
            .await
            .unwrap_err();
        assert!(matches!(error, DataLayerError::TimedOut(_)));

        // The timed-out flight must not keep the key pinned.
        cache
            .list_for_exact_api_format_and_requested_model_page(&requested_model_page(0, 64))
            .await
            .unwrap();

        assert_eq!(inner.calls(), 2);
    }

    fn sample_row(key_id: &str, key_internal_priority: i32) -> StoredMinimalCandidateSelectionRow {
        StoredMinimalCandidateSelectionRow {
            provider_id: "provider-1".to_string(),
            provider_name: "provider".to_string(),
            provider_type: "custom".to_string(),
            provider_priority: 1,
            provider_is_active: true,
            endpoint_id: "endpoint-1".to_string(),
            endpoint_api_format: "openai:chat".to_string(),
            endpoint_api_family: Some("openai".to_string()),
            endpoint_kind: Some("chat".to_string()),
            endpoint_is_active: true,
            key_id: key_id.to_string(),
            key_name: key_id.to_string(),
            key_auth_type: "api_key".to_string(),
            key_is_active: true,
            key_api_formats: Some(vec!["openai:chat".to_string()]),
            key_allowed_models: None,
            key_capabilities: None,
            key_internal_priority,
            key_global_priority_by_format: None,
            model_id: "model-1".to_string(),
            global_model_id: "global-model-1".to_string(),
            global_model_name: "mock-model".to_string(),
            global_model_mappings: None,
            global_model_supports_streaming: Some(true),
            model_provider_model_name: "mock-model".to_string(),
            model_provider_model_mappings: None,
            model_supports_streaming: Some(true),
            model_is_active: true,
            model_is_available: true,
        }
    }
}
