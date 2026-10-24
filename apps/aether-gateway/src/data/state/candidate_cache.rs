use std::collections::HashSet;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use aether_cache::{optional_strings_weight, value_weight, CacheBudget, CacheWeight, OnceMap};
use aether_data::DataLayerError;
use aether_data_contracts::repository::candidate_selection::{
    MinimalCandidateSelectionReadRepository, StoredApiFormatCandidateRowsQuery,
    StoredMinimalCandidateSelectionRow, StoredPoolKeyCandidateOrder,
    StoredPoolKeyCandidateRowsByKeyIdsQuery, StoredPoolKeyCandidateRowsQuery,
    StoredProviderModelMapping, StoredRequestedModelCandidateRowsQuery,
};
use aether_scheduler_core::row_supports_requested_model_with_model_directives;
use async_trait::async_trait;
use tokio::time::timeout;
use tracing::warn;

const CANDIDATE_CACHE_TTL: Duration = Duration::from_secs(5);
/// Pool windows are small, keyed by the pool member triple, and cannot be
/// derived from the format snapshot (see [`CachedMinimalCandidateSelectionReadRepository`]).
const CANDIDATE_POOL_ROWS_CAPACITY_BYTES: usize = 4 * 1024 * 1024;
const CANDIDATE_CACHE_MAX_INFLIGHT: usize = 4096;
#[cfg(not(test))]
const CANDIDATE_CACHE_LOAD_TIMEOUT: Duration = Duration::from_secs(10);
#[cfg(test)]
const CANDIDATE_CACHE_LOAD_TIMEOUT: Duration = Duration::from_millis(50);

/// Every candidate row one api format can serve, as one immutable version.
///
/// The planner asks the same table five different ways — all rows, one page of
/// them, one global model, one requested model, one page of that — and each
/// shape used to be its own cache entry with its own five-second window. Two
/// consequences: a first read paid one query per shape (a single request could
/// do three), and after one configuration change the shapes could disagree
/// about which version they were serving.
///
/// One load now produces one version, and every shape is answered from it in
/// memory. The rows arrive already filtered by the query's own policy — which
/// providers, endpoints and credentials may serve this format is decided by the
/// repository, not here — so this cache only re-arranges them.
///
/// Order is the same one `list_for_exact_api_format_page` used to ask the
/// database for, applied once at load time instead of once per page.
struct CandidateFormatRows {
    rows: Arc<Vec<StoredMinimalCandidateSelectionRow>>,
}

impl CandidateFormatRows {
    fn new(rows: Vec<StoredMinimalCandidateSelectionRow>) -> Self {
        Self {
            rows: Arc::new(normalize_rows(rows)),
        }
    }

    fn all(&self) -> Vec<StoredMinimalCandidateSelectionRow> {
        self.rows.as_ref().clone()
    }

    /// The version itself, so a caller that pages can hold it.
    fn pinned(&self) -> Arc<Vec<StoredMinimalCandidateSelectionRow>> {
        Arc::clone(&self.rows)
    }

    /// Rows `[offset, offset + limit)`, clamped to what the version holds.
    fn window(&self, offset: u32, limit: u32) -> Vec<StoredMinimalCandidateSelectionRow> {
        if limit == 0 {
            return Vec::new();
        }
        let start = offset as usize;
        if start >= self.rows.len() {
            return Vec::new();
        }
        let end = start.saturating_add(limit as usize).min(self.rows.len());
        self.rows[start..end].to_vec()
    }

    fn by_global_model(&self, global_model_name: &str) -> Vec<StoredMinimalCandidateSelectionRow> {
        self.rows
            .iter()
            .filter(|row| row.global_model_name == global_model_name)
            .cloned()
            .collect()
    }

    /// The rows a request naming `requested_model_name` may consider.
    ///
    /// This is the same predicate the planner applies to the rows it receives,
    /// so answering here changes nothing about the result — but a page of this
    /// shape has to be cut from the filtered stream, or the caller's offsets
    /// would address rows it never sees.
    fn by_requested_model(
        &self,
        requested_model_name: &str,
        api_format: &str,
    ) -> Vec<StoredMinimalCandidateSelectionRow> {
        self.rows
            .iter()
            .filter(|row| {
                row_supports_requested_model_with_model_directives(
                    row,
                    requested_model_name,
                    api_format,
                    false,
                )
            })
            .cloned()
            .collect()
    }
}

impl CacheWeight for CandidateFormatRows {
    fn cache_weight(&self) -> usize {
        value_weight::<Self>(
            self.rows.capacity() * std::mem::size_of::<StoredMinimalCandidateSelectionRow>(),
        )
        .saturating_add(self.rows.iter().map(candidate_row_weight).sum())
    }
}

/// One window of pool-key rows.
///
/// These stay on a keyed cache: their order comes from `pak.last_used_at` and
/// from `md5(seed || key id)` on the database side, and
/// `StoredMinimalCandidateSelectionRow` carries neither, so the format snapshot
/// cannot reproduce them.
#[derive(Debug, Clone)]
struct CandidatePoolRows(Vec<StoredMinimalCandidateSelectionRow>);

impl CacheWeight for CandidatePoolRows {
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
    /// A whole-format version can exceed the old 16 MiB query-page cap. Charge
    /// it to the shared process ledger instead: retention stays bounded without
    /// silently forcing large formats to relist on every request.
    formats: OnceMap<String, CandidateFormatRows, DataLayerError>,
    pools: OnceMap<CandidatePoolCacheKey, CandidatePoolRows, DataLayerError>,
}

impl CachedMinimalCandidateSelectionReadRepository {
    pub(super) fn new(inner: Arc<dyn MinimalCandidateSelectionReadRepository>) -> Self {
        Self::with_budget(inner, CacheBudget::process())
    }

    fn with_budget(
        inner: Arc<dyn MinimalCandidateSelectionReadRepository>,
        budget: Arc<CacheBudget>,
    ) -> Self {
        Self {
            inner,
            formats: OnceMap::builder()
                .budget(Arc::clone(&budget))
                .max_inflight(CANDIDATE_CACHE_MAX_INFLIGHT)
                .build(),
            pools: OnceMap::builder()
                .capacity_bytes(CANDIDATE_POOL_ROWS_CAPACITY_BYTES)
                .budget(budget)
                .max_inflight(CANDIDATE_CACHE_MAX_INFLIGHT)
                .build(),
        }
    }

    async fn format_rows(
        &self,
        api_format: &str,
    ) -> Result<Arc<CandidateFormatRows>, DataLayerError> {
        let cache_key = normalize_api_format_key(api_format);
        self.formats
            .get_or_load(cache_key.clone(), CANDIDATE_CACHE_TTL, || async move {
                let rows =
                    load_with_timeout(&cache_key, self.inner.list_for_exact_api_format(api_format))
                        .await?;
                Ok(CandidateFormatRows::new(rows))
            })
            .await
            .map_err(|error| error.or_else_load(DataLayerError::TimedOut))
    }

    async fn pool_rows<F, Fut>(
        &self,
        key: CandidatePoolCacheKey,
        load: F,
    ) -> Result<Vec<StoredMinimalCandidateSelectionRow>, DataLayerError>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<Vec<StoredMinimalCandidateSelectionRow>, DataLayerError>>,
    {
        let load_key = key.clone();
        let rows = self
            .pools
            .get_or_load(key, CANDIDATE_CACHE_TTL, || async move {
                load_with_timeout(&load_key, load())
                    .await
                    .map(CandidatePoolRows)
            })
            .await
            .map_err(|error| error.or_else_load(DataLayerError::TimedOut))?;
        Ok(rows.0.clone())
    }
}

/// Dedupes and orders one format's rows.
///
/// The repository concatenates one query per storage alias, so the same
/// `(endpoint, key, model)` triple can arrive more than once; the first
/// occurrence wins, exactly as the per-page query did. The order is the one the
/// page query asked the database for, so a page cut here matches a page cut
/// there.
fn normalize_rows(
    rows: Vec<StoredMinimalCandidateSelectionRow>,
) -> Vec<StoredMinimalCandidateSelectionRow> {
    let mut seen = HashSet::with_capacity(rows.len());
    let mut rows = rows
        .into_iter()
        .filter(|row| {
            seen.insert((
                row.endpoint_id.clone(),
                row.key_id.clone(),
                row.model_id.clone(),
            ))
        })
        .collect::<Vec<_>>();
    rows.sort_by(|left, right| {
        left.global_model_name
            .cmp(&right.global_model_name)
            .then(left.provider_priority.cmp(&right.provider_priority))
            .then(left.key_internal_priority.cmp(&right.key_internal_priority))
            .then(left.provider_id.cmp(&right.provider_id))
            .then(left.endpoint_id.cmp(&right.endpoint_id))
            .then(left.key_id.cmp(&right.key_id))
            .then(left.model_id.cmp(&right.model_id))
    });
    rows
}

async fn load_with_timeout<Fut, T, L>(label: &L, load: Fut) -> Result<T, DataLayerError>
where
    Fut: Future<Output = Result<T, DataLayerError>>,
    L: std::fmt::Debug + ?Sized,
{
    match timeout(CANDIDATE_CACHE_LOAD_TIMEOUT, load).await {
        Ok(result) => result,
        Err(_) => {
            warn!(
                event_name = "candidate_selection_cache_load_timeout",
                log_type = "ops",
                cache_key = ?label,
                timeout_ms = CANDIDATE_CACHE_LOAD_TIMEOUT.as_millis() as u64,
                "gateway candidate selection cache load timed out"
            );
            Err(DataLayerError::TimedOut(format!(
                "candidate selection cache load exceeded {}ms for {label:?}",
                CANDIDATE_CACHE_LOAD_TIMEOUT.as_millis()
            )))
        }
    }
}

#[async_trait]
impl MinimalCandidateSelectionReadRepository for CachedMinimalCandidateSelectionReadRepository {
    fn clear_local_cache(&self) {
        self.formats.invalidate_all();
        self.pools.invalidate_all();
        self.inner.clear_local_cache();
    }

    async fn list_for_exact_api_format(
        &self,
        api_format: &str,
    ) -> Result<Vec<StoredMinimalCandidateSelectionRow>, DataLayerError> {
        Ok(self.format_rows(api_format).await?.all())
    }

    /// The cache already holds exactly one version per format, so pinning is a
    /// refcount bump. The caller keeps being answered from the rows it started
    /// with even if the entry is invalidated between its pages.
    async fn pin_api_format_rows(
        &self,
        api_format: &str,
    ) -> Result<Arc<Vec<StoredMinimalCandidateSelectionRow>>, DataLayerError> {
        Ok(self.format_rows(api_format).await?.pinned())
    }

    async fn list_for_exact_api_format_page(
        &self,
        query: &StoredApiFormatCandidateRowsQuery,
    ) -> Result<Vec<StoredMinimalCandidateSelectionRow>, DataLayerError> {
        Ok(self
            .format_rows(&query.api_format)
            .await?
            .window(query.offset, query.limit))
    }

    async fn list_for_exact_api_format_and_global_model(
        &self,
        api_format: &str,
        global_model_name: &str,
    ) -> Result<Vec<StoredMinimalCandidateSelectionRow>, DataLayerError> {
        Ok(self
            .format_rows(api_format)
            .await?
            .by_global_model(global_model_name))
    }

    async fn list_for_exact_api_format_and_requested_model(
        &self,
        api_format: &str,
        requested_model_name: &str,
    ) -> Result<Vec<StoredMinimalCandidateSelectionRow>, DataLayerError> {
        Ok(self
            .format_rows(api_format)
            .await?
            .by_requested_model(requested_model_name, api_format))
    }

    async fn list_for_exact_api_format_and_requested_model_page(
        &self,
        query: &StoredRequestedModelCandidateRowsQuery,
    ) -> Result<Vec<StoredMinimalCandidateSelectionRow>, DataLayerError> {
        let rows = self.format_rows(&query.api_format).await?;
        let matched = rows.by_requested_model(&query.requested_model_name, &query.api_format);
        if query.limit == 0 {
            return Ok(Vec::new());
        }
        let start = query.offset as usize;
        if start >= matched.len() {
            return Ok(Vec::new());
        }
        let end = start
            .saturating_add(query.limit as usize)
            .min(matched.len());
        Ok(matched[start..end].to_vec())
    }

    async fn list_pool_key_rows_for_group(
        &self,
        query: &StoredPoolKeyCandidateRowsQuery,
    ) -> Result<Vec<StoredMinimalCandidateSelectionRow>, DataLayerError> {
        let key = CandidatePoolCacheKey::Group {
            api_format: normalize_api_format_key(&query.api_format),
            provider_id: query.provider_id.clone(),
            endpoint_id: query.endpoint_id.clone(),
            model_id: query.model_id.clone(),
            selected_provider_model_name: query.selected_provider_model_name.clone(),
            order: CandidatePoolOrderKey::from(&query.order),
            offset: query.offset,
            limit: query.limit,
        };
        self.pool_rows(key, || self.inner.list_pool_key_rows_for_group(query))
            .await
    }

    async fn list_pool_key_rows_for_group_key_ids(
        &self,
        query: &StoredPoolKeyCandidateRowsByKeyIdsQuery,
    ) -> Result<Vec<StoredMinimalCandidateSelectionRow>, DataLayerError> {
        let key = CandidatePoolCacheKey::GroupKeyIds {
            api_format: normalize_api_format_key(&query.api_format),
            provider_id: query.provider_id.clone(),
            endpoint_id: query.endpoint_id.clone(),
            model_id: query.model_id.clone(),
            selected_provider_model_name: query.selected_provider_model_name.clone(),
            key_ids: query.key_ids.clone(),
        };
        self.pool_rows(key, || {
            self.inner.list_pool_key_rows_for_group_key_ids(query)
        })
        .await
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum CandidatePoolCacheKey {
    Group {
        api_format: String,
        provider_id: String,
        endpoint_id: String,
        model_id: String,
        selected_provider_model_name: String,
        order: CandidatePoolOrderKey,
        offset: u32,
        limit: u32,
    },
    GroupKeyIds {
        api_format: String,
        provider_id: String,
        endpoint_id: String,
        model_id: String,
        selected_provider_model_name: String,
        key_ids: Vec<String>,
    },
}

impl CacheWeight for CandidatePoolCacheKey {
    fn cache_weight(&self) -> usize {
        let owned = match self {
            Self::Group {
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
            Self::GroupKeyIds {
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

impl CacheWeight for CandidatePoolOrderKey {
    fn cache_weight(&self) -> usize {
        match self {
            Self::LoadBalance { seed } => seed.capacity(),
            Self::InternalPriority | Self::Lru | Self::CacheAffinity | Self::SingleAccount => 0,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum CandidatePoolOrderKey {
    InternalPriority,
    Lru,
    CacheAffinity,
    SingleAccount,
    LoadBalance { seed: String },
}

impl From<&StoredPoolKeyCandidateOrder> for CandidatePoolOrderKey {
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

        fn with_rows(delay: Duration, rows: Vec<StoredMinimalCandidateSelectionRow>) -> Self {
            Self {
                calls: AtomicUsize::new(0),
                delay,
                rows,
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

    fn format_page(api_format: &str, offset: u32, limit: u32) -> StoredApiFormatCandidateRowsQuery {
        StoredApiFormatCandidateRowsQuery {
            api_format: api_format.to_string(),
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
    async fn a_format_larger_than_the_old_page_cap_is_retained_within_the_shared_budget() {
        const OLD_PAGE_CAP: usize = 16 * 1024 * 1024;
        let mut row = sample_row_for("key-large", 1, "model-large");
        row.provider_name = "p".repeat(OLD_PAGE_CAP);
        let inner = Arc::new(StubCandidateSelectionRepository::with_rows(
            Duration::ZERO,
            vec![row],
        ));
        let budget = Arc::new(CacheBudget::new(64 * 1024 * 1024));
        let cache = CachedMinimalCandidateSelectionReadRepository::with_budget(
            inner.clone(),
            Arc::clone(&budget),
        );

        let first = cache.format_rows("openai:chat").await.unwrap();
        let second = cache.format_rows("openai:chat").await.unwrap();
        assert!(Arc::ptr_eq(&first, &second));
        assert_eq!(
            inner.calls(),
            1,
            "large formats must not be relisted per request"
        );
        assert!(budget.used() > OLD_PAGE_CAP);
        assert!(budget.used() <= budget.limit());
        assert_eq!(budget.rejections(), 0);

        cache.clear_local_cache();
        assert_eq!(budget.used(), 0);
        let fresh = cache.format_rows("openai:chat").await.unwrap();
        assert!(!Arc::ptr_eq(&first, &fresh));
        assert_eq!(inner.calls(), 2);
    }

    /// The whole point of the version: five query shapes, one load.
    #[tokio::test]
    async fn every_shape_of_one_format_is_answered_from_one_load() {
        let inner = Arc::new(StubCandidateSelectionRepository::with_rows(
            Duration::ZERO,
            vec![sample_row_for("key-a", 1, "mock-model")],
        ));
        let cache = CachedMinimalCandidateSelectionReadRepository::new(inner.clone());

        cache
            .list_for_exact_api_format("openai:chat")
            .await
            .unwrap();
        cache
            .list_for_exact_api_format_page(&format_page("openai:chat", 0, 256))
            .await
            .unwrap();
        cache
            .list_for_exact_api_format_and_global_model("openai:chat", "mock-model")
            .await
            .unwrap();
        cache
            .list_for_exact_api_format_and_requested_model("openai:chat", "mock-model")
            .await
            .unwrap();
        cache
            .list_for_exact_api_format_and_requested_model_page(
                &StoredRequestedModelCandidateRowsQuery {
                    api_format: "openai:chat".to_string(),
                    requested_model_name: "mock-model".to_string(),
                    offset: 0,
                    limit: 256,
                },
            )
            .await
            .unwrap();

        assert_eq!(inner.calls(), 1);
    }

    #[tokio::test]
    async fn pages_of_one_format_share_the_version() {
        let inner = Arc::new(StubCandidateSelectionRepository::new(Duration::ZERO));
        let cache = CachedMinimalCandidateSelectionReadRepository::new(inner.clone());

        cache
            .list_for_exact_api_format_page(&format_page("openai:chat", 0, 256))
            .await
            .unwrap();
        cache
            .list_for_exact_api_format_page(&format_page("openai:chat", 256, 256))
            .await
            .unwrap();

        assert_eq!(inner.calls(), 1);
    }

    /// A page is a window of the version, so its bounds are the version's, not
    /// a second query's.
    #[tokio::test]
    async fn pages_are_windows_of_the_version_in_query_order() {
        let rows = vec![
            sample_row_for("key-c", 3, "model-b"),
            sample_row_for("key-a", 1, "model-a"),
            sample_row_for("key-b", 2, "model-a"),
        ];
        let inner = Arc::new(StubCandidateSelectionRepository::with_rows(
            Duration::ZERO,
            rows,
        ));
        let cache = CachedMinimalCandidateSelectionReadRepository::new(inner.clone());

        let first = cache
            .list_for_exact_api_format_page(&format_page("openai:chat", 0, 2))
            .await
            .unwrap();
        assert_eq!(
            vec!["model-a", "model-a"],
            first
                .iter()
                .map(|row| row.global_model_name.as_str())
                .collect::<Vec<_>>()
        );
        assert_eq!(
            vec!["key-a", "key-b"],
            first
                .iter()
                .map(|row| row.key_id.as_str())
                .collect::<Vec<_>>()
        );

        let second = cache
            .list_for_exact_api_format_page(&format_page("openai:chat", 2, 2))
            .await
            .unwrap();
        assert_eq!(1, second.len());
        assert_eq!("key-c", second[0].key_id);

        let past_the_end = cache
            .list_for_exact_api_format_page(&format_page("openai:chat", 9, 2))
            .await
            .unwrap();
        assert!(past_the_end.is_empty());

        assert_eq!(inner.calls(), 1);
    }

    #[tokio::test]
    async fn a_duplicate_row_is_only_cached_once() {
        let rows = vec![
            sample_row_for("key-a", 1, "model-a"),
            sample_row_for("key-a", 1, "model-a"),
        ];
        let inner = Arc::new(StubCandidateSelectionRepository::with_rows(
            Duration::ZERO,
            rows,
        ));
        let cache = CachedMinimalCandidateSelectionReadRepository::new(inner);

        let all = cache
            .list_for_exact_api_format("openai:chat")
            .await
            .unwrap();

        assert_eq!(1, all.len());
    }

    /// A requested-model page is cut from the filtered stream, so its offsets
    /// address rows the caller will actually see.
    #[tokio::test]
    async fn a_requested_model_page_is_cut_from_the_filtered_rows() {
        let rows = vec![
            sample_row_for("key-a", 1, "wanted"),
            sample_row_for("key-b", 2, "wanted"),
            sample_row_for("key-c", 3, "unrelated"),
        ];
        let inner = Arc::new(StubCandidateSelectionRepository::with_rows(
            Duration::ZERO,
            rows,
        ));
        let cache = CachedMinimalCandidateSelectionReadRepository::new(inner);

        let all = cache
            .list_for_exact_api_format_and_requested_model("openai:chat", "wanted")
            .await
            .unwrap();
        assert_eq!(
            vec!["key-a", "key-b"],
            all.iter()
                .map(|row| row.key_id.as_str())
                .collect::<Vec<_>>()
        );

        let page = cache
            .list_for_exact_api_format_and_requested_model_page(
                &StoredRequestedModelCandidateRowsQuery {
                    api_format: "openai:chat".to_string(),
                    requested_model_name: "wanted".to_string(),
                    offset: 1,
                    limit: 1,
                },
            )
            .await
            .unwrap();
        assert_eq!(1, page.len());
        assert_eq!("key-b", page[0].key_id);
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

    /// A caller that paged through a version keeps reading that version even
    /// after the entry is invalidated: its offsets address the rows it started
    /// with, and no second read can move them.
    #[tokio::test]
    async fn a_pinned_version_outlives_an_invalidation() {
        let inner = Arc::new(StubCandidateSelectionRepository::with_rows(
            Duration::ZERO,
            vec![
                sample_row_for("key-a", 1, "model-a"),
                sample_row_for("key-b", 2, "model-a"),
            ],
        ));
        let cache = CachedMinimalCandidateSelectionReadRepository::new(inner.clone());

        let pinned = cache.pin_api_format_rows("openai:chat").await.unwrap();
        assert_eq!(pinned.len(), 2);

        cache.clear_local_cache();
        assert_eq!(
            pinned.len(),
            2,
            "the handle the caller holds is the version, not a lookup"
        );

        // A reader that arrives after the invalidation gets a fresh version,
        // and that costs exactly one load.
        let reloaded = cache.pin_api_format_rows("openai:chat").await.unwrap();
        assert_eq!(reloaded.len(), 2);
        assert_eq!(inner.calls(), 2);
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
        sample_row_for(key_id, key_internal_priority, "mock-model")
    }

    fn sample_row_for(
        key_id: &str,
        key_internal_priority: i32,
        global_model_name: &str,
    ) -> StoredMinimalCandidateSelectionRow {
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
            global_model_name: global_model_name.to_string(),
            global_model_mappings: None,
            global_model_supports_streaming: Some(true),
            model_provider_model_name: global_model_name.to_string(),
            model_provider_model_mappings: None,
            model_supports_streaming: Some(true),
            model_is_active: true,
            model_is_available: true,
        }
    }
}
