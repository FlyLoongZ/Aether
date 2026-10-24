use aether_data::DataLayerError;
use aether_data_contracts::repository::candidate_selection::StoredMinimalCandidateSelectionRow;
use aether_scheduler_core::{
    auth_constraints_allow_api_format, collect_global_model_names_for_required_capability,
    enumerate_minimal_candidate_selection_with_model_directives, normalize_api_format,
    resolve_requested_global_model_name_with_reserved_global_model,
    row_supports_requested_model_with_model_directives, EnumerateMinimalCandidateSelectionInput,
    SchedulerAuthConstraints, SchedulerMinimalCandidateSelectionCandidate,
};
use async_trait::async_trait;
use std::collections::BTreeSet;
use std::sync::Arc;

use super::auth::GatewayAuthApiKeySnapshot;

#[async_trait]
pub(crate) trait MinimalCandidateSelectionRowSource {
    async fn read_minimal_candidate_selection_rows_for_api_format(
        &self,
        api_format: &str,
    ) -> Result<Vec<StoredMinimalCandidateSelectionRow>, DataLayerError>;

    /// One immutable version of a format's rows, for a caller that pages
    /// through it. See
    /// [`aether_data_contracts::repository::candidate_selection::MinimalCandidateSelectionReadRepository::pin_api_format_rows`]
    /// for why paging needs it; the sources without a snapshot answer by
    /// reading the format once.
    async fn pin_minimal_candidate_selection_rows_for_api_format(
        &self,
        api_format: &str,
    ) -> Result<Arc<Vec<StoredMinimalCandidateSelectionRow>>, DataLayerError> {
        Ok(Arc::new(
            self.read_minimal_candidate_selection_rows_for_api_format(api_format)
                .await?,
        ))
    }

    /// Returns the canonical global model name when `model_name` is one, so the
    /// caller can keep provider-side aliases out of a request that names a
    /// global model. Sources without a global model reader answer `None`, which
    /// leaves matching unrestricted.
    async fn read_reserved_global_model_name(
        &self,
        _model_name: &str,
    ) -> Result<Option<String>, DataLayerError> {
        Ok(None)
    }
}

/// Resolves the reserved global model name for `routing_model`.
///
/// Rows already in hand answer the question for free whenever one of them is
/// bound to a global model of that exact name; only a request that no local row
/// claims as a global model needs the lookup, which keeps the extra read off the
/// path every ordinary request takes.
pub(crate) async fn resolve_reserved_global_model_name(
    source: &(impl MinimalCandidateSelectionRowSource + Sync),
    rows: &[StoredMinimalCandidateSelectionRow],
    routing_model: &str,
) -> Result<Option<String>, DataLayerError> {
    if rows
        .iter()
        .any(|row| row.global_model_name == routing_model)
    {
        return Ok(Some(routing_model.to_string()));
    }
    source.read_reserved_global_model_name(routing_model).await
}

pub(crate) const REQUESTED_MODEL_CANDIDATE_PAGE_SIZE: u32 = 256;
/// How many rows one api format may collect before the search for a requested
/// name gives up.
///
/// The unit is deliberately the rows the plan collected — matching the query
/// shape's own predicate, after the alias is known — and not the rows the
/// storage scanned. A storage-side "rows examined" counter cannot be
/// compared across backends (the in-memory repository examines every row, the
/// SQL one examines whatever the index chose), so a budget denominated in it
/// would mean something different depending on which backend answered.
///
/// Elasticsearch's `terminate_after` draws the line in the same place: it
/// counts collected documents, before sorting, and documents that the count is
/// a per-shard collection budget rather than a scan estimate.
pub(crate) const REQUESTED_MODEL_MAX_COLLECTED_ROWS: u32 = 2048;

/// One window of candidate rows read from the data layer.
///
/// The three fields answer three different questions and must never be derived
/// from one another:
///
/// - `rows` — the rows this request may consider, after per-request filtering;
/// - `scanned_rows` — how much of the query shape's row set this window has
///   consumed, which is what a caller charges against its scan budget;
/// - `scan_exhausted` — whether the query shape had nothing left after it.
///
/// A short window usually means the shape is exhausted, but not always: a
/// window can come back full while the shape still has nothing more that
/// matches. Treating "page is not full" as "stop looking" is how a bounded scan
/// silently turns into a partial answer.
#[derive(Debug, Clone)]
pub(crate) struct RequestedModelCandidateRowsPage {
    pub(crate) rows: Vec<StoredMinimalCandidateSelectionRow>,
    pub(crate) scanned_rows: u32,
    pub(crate) scan_exhausted: bool,
}

/// How much of one API format's row set a single request may walk.
///
/// The budget is the *cost* bound of a plan, not the bound of its result: when
/// it runs out the scan stops even though rows remain, and callers must report
/// that as "the budget ended" rather than as "there is nothing else".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CandidateScanBudget {
    used: u32,
    limit: u32,
}

impl CandidateScanBudget {
    pub(crate) const fn new(limit: u32) -> Self {
        Self { used: 0, limit }
    }

    pub(crate) const fn remaining(&self) -> u32 {
        self.limit.saturating_sub(self.used)
    }

    pub(crate) const fn is_exhausted(&self) -> bool {
        self.remaining() == 0
    }

    pub(crate) fn consume(&mut self, rows: u32) {
        self.used = self.used.saturating_add(rows).min(self.limit);
    }
}

pub(crate) async fn read_requested_model_rows(
    state: &(impl MinimalCandidateSelectionRowSource + Sync),
    api_format: &str,
    requested_model_name: &str,
    enable_model_directives: bool,
) -> Result<Option<(String, Vec<StoredMinimalCandidateSelectionRow>)>, DataLayerError> {
    let fast_rows = read_requested_model_rows_fast_path(
        state,
        api_format,
        requested_model_name,
        enable_model_directives,
    )
    .await?;
    let mut rows = filter_rows_for_requested_model(
        fast_rows,
        requested_model_name,
        api_format,
        enable_model_directives,
    );
    if rows.is_empty() {
        let fallback_rows = state
            .read_minimal_candidate_selection_rows_for_api_format(api_format)
            .await?;
        rows = filter_rows_for_requested_model(
            fallback_rows,
            requested_model_name,
            api_format,
            enable_model_directives,
        );
    }
    if rows.is_empty() {
        return Ok(None);
    }

    let reserved_global_model_name =
        resolve_reserved_global_model_name(state, &rows, requested_model_name).await?;
    let Some(resolved_global_model_name) =
        resolve_requested_global_model_name_with_reserved_global_model(
            &rows,
            requested_model_name,
            api_format,
            enable_model_directives,
            None,
            reserved_global_model_name.as_deref(),
        )
    else {
        return Ok(None);
    };

    let resolved_rows = rows
        .into_iter()
        .filter(|row| row.global_model_name == resolved_global_model_name)
        .collect();

    Ok(Some((resolved_global_model_name, resolved_rows)))
}

fn filter_rows_for_requested_model(
    rows: Vec<StoredMinimalCandidateSelectionRow>,
    requested_model_name: &str,
    api_format: &str,
    enable_model_directives: bool,
) -> Vec<StoredMinimalCandidateSelectionRow> {
    rows.into_iter()
        .filter(|row| {
            row_supports_requested_model_with_model_directives(
                row,
                requested_model_name,
                api_format,
                enable_model_directives,
            )
        })
        .collect()
}

async fn read_requested_model_rows_fast_path(
    state: &(impl MinimalCandidateSelectionRowSource + Sync),
    api_format: &str,
    requested_model_name: &str,
    enable_model_directives: bool,
) -> Result<Vec<StoredMinimalCandidateSelectionRow>, DataLayerError> {
    let requested_names =
        requested_model_candidate_names(requested_model_name, enable_model_directives);

    // One version for the whole walk. Every offset below is measured against
    // these rows, so a concurrent configuration change cannot make one page of
    // this scan address a set the previous page never saw.
    let all_rows = state
        .pin_minimal_candidate_selection_rows_for_api_format(api_format)
        .await?;

    let mut rows = Vec::new();
    let mut seen = BTreeSet::new();
    for requested_name in requested_names {
        if requested_name.is_empty() {
            continue;
        }
        let mut offset = 0;
        let mut scanned = 0;
        while scanned < REQUESTED_MODEL_MAX_COLLECTED_ROWS {
            let limit = REQUESTED_MODEL_CANDIDATE_PAGE_SIZE
                .min(REQUESTED_MODEL_MAX_COLLECTED_ROWS - scanned);
            let page = requested_name_window_from_rows(
                all_rows.as_slice(),
                api_format,
                &requested_name,
                offset,
                limit,
            );
            if page.is_empty() {
                break;
            }
            let page_len = page.len() as u32;
            for row in page {
                if seen.insert((
                    row.endpoint_id.clone(),
                    row.key_id.clone(),
                    row.model_id.clone(),
                )) {
                    rows.push(row);
                }
            }
            scanned = scanned.saturating_add(page_len);
            if page_len < limit {
                break;
            }
            offset = offset.saturating_add(limit);
        }
    }
    Ok(rows)
}

pub(crate) fn requested_model_candidate_names(
    requested_model_name: &str,
    enable_model_directives: bool,
) -> Vec<String> {
    let mut requested_names = vec![requested_model_name.trim().to_string()];
    if enable_model_directives {
        if let Some(base_model) =
            crate::ai_serving::model_directive_base_model(requested_model_name)
        {
            if !requested_names.iter().any(|value| value == &base_model) {
                requested_names.push(base_model);
            }
        }
    }
    requested_names
}

/// One requested-model window, cut from a pinned version.
///
/// The predicate is the one the query shape uses — it filters on the alias
/// being tried, before the request-level filter runs — so the offsets here
/// address exactly the rows the data layer would have returned for the same
/// window.
#[allow(clippy::too_many_arguments)]
pub(crate) fn requested_model_page_from_rows(
    all_rows: &[StoredMinimalCandidateSelectionRow],
    api_format: &str,
    requested_model_name: &str,
    requested_name: &str,
    offset: u32,
    limit: u32,
    enable_model_directives: bool,
) -> RequestedModelCandidateRowsPage {
    let limit = limit.max(1);
    let window =
        requested_name_window_from_rows(all_rows, api_format, requested_name, offset, limit);
    finish_requested_model_window(
        window,
        api_format,
        requested_model_name,
        limit,
        enable_model_directives,
    )
}

/// The rows the query shape matches for one alias, inside
/// `[offset, offset + limit)`.
///
/// This is the window an offset is measured against, which is why it is a
/// separate step from the request-level filter: cutting the window after the
/// request filter would make offsets address rows the caller never sees, and
/// cutting it before the alias filter would make them address rows the shape
/// never returned.
pub(crate) fn requested_name_window_from_rows(
    all_rows: &[StoredMinimalCandidateSelectionRow],
    api_format: &str,
    requested_name: &str,
    offset: u32,
    limit: u32,
) -> Vec<StoredMinimalCandidateSelectionRow> {
    let limit = limit.max(1);
    all_rows
        .iter()
        .filter(|row| {
            row_supports_requested_model_with_model_directives(
                row,
                requested_name,
                api_format,
                false,
            )
        })
        .skip(offset as usize)
        .take(limit as usize)
        .cloned()
        .collect()
}

fn finish_requested_model_window(
    window: Vec<StoredMinimalCandidateSelectionRow>,
    api_format: &str,
    requested_model_name: &str,
    limit: u32,
    enable_model_directives: bool,
) -> RequestedModelCandidateRowsPage {
    let scanned_rows = window.len() as u32;
    let scan_exhausted = scanned_rows < limit;
    let rows = filter_rows_for_requested_model(
        window,
        requested_model_name,
        api_format,
        enable_model_directives,
    );
    RequestedModelCandidateRowsPage {
        rows,
        scanned_rows,
        scan_exhausted,
    }
}

/// One api-format window, cut from a pinned version.
pub(crate) fn api_format_page_from_rows(
    all_rows: &[StoredMinimalCandidateSelectionRow],
    offset: u32,
    limit: u32,
) -> RequestedModelCandidateRowsPage {
    let limit = limit.max(1);
    let window = all_rows
        .iter()
        .skip(offset as usize)
        .take(limit as usize)
        .cloned()
        .collect::<Vec<_>>();
    finish_api_format_window(window, limit)
}

fn finish_api_format_window(
    window: Vec<StoredMinimalCandidateSelectionRow>,
    limit: u32,
) -> RequestedModelCandidateRowsPage {
    let scanned_rows = window.len() as u32;
    RequestedModelCandidateRowsPage {
        rows: window,
        scanned_rows,
        scan_exhausted: scanned_rows < limit,
    }
}

#[cfg(test)]
pub(crate) async fn enumerate_minimal_candidate_selection_with_required_capabilities(
    state: &(impl MinimalCandidateSelectionRowSource + Sync),
    api_format: &str,
    requested_model_name: &str,
    require_streaming: bool,
    auth_snapshot: Option<&GatewayAuthApiKeySnapshot>,
    required_capabilities: Option<&serde_json::Value>,
    enable_model_directives: bool,
) -> Result<Vec<SchedulerMinimalCandidateSelectionCandidate>, DataLayerError> {
    enumerate_minimal_candidate_selection_with_required_capabilities_for_request_operation(
        state,
        api_format,
        requested_model_name,
        require_streaming,
        auth_snapshot,
        required_capabilities,
        enable_model_directives,
        None,
    )
    .await
}

pub(crate) async fn enumerate_minimal_candidate_selection_with_required_capabilities_for_request_operation(
    state: &(impl MinimalCandidateSelectionRowSource + Sync),
    api_format: &str,
    requested_model_name: &str,
    require_streaming: bool,
    auth_snapshot: Option<&GatewayAuthApiKeySnapshot>,
    required_capabilities: Option<&serde_json::Value>,
    enable_model_directives: bool,
    request_operation: Option<&str>,
) -> Result<Vec<SchedulerMinimalCandidateSelectionCandidate>, DataLayerError> {
    let normalized_api_format = normalize_api_format(api_format);
    if normalized_api_format.is_empty() {
        return Ok(Vec::new());
    }

    if !auth_constraints_allow_api_format(
        auth_snapshot.map(auth_snapshot_constraints).as_ref(),
        &normalized_api_format,
    ) {
        return Ok(Vec::new());
    }

    let Some((resolved_global_model_name, rows)) = read_requested_model_rows(
        state,
        &normalized_api_format,
        requested_model_name,
        enable_model_directives,
    )
    .await?
    else {
        return Ok(Vec::new());
    };
    let auth_constraints = auth_snapshot.map(auth_snapshot_constraints);
    enumerate_minimal_candidate_selection_with_model_directives(
        EnumerateMinimalCandidateSelectionInput {
            rows,
            normalized_api_format: &normalized_api_format,
            request_operation,
            requested_model_name,
            resolved_global_model_name: resolved_global_model_name.as_str(),
            require_streaming,
            required_capabilities,
            auth_constraints: auth_constraints.as_ref(),
        },
        enable_model_directives,
    )
}

pub(crate) async fn read_global_model_names_for_required_capability(
    state: &(impl MinimalCandidateSelectionRowSource + Sync),
    api_format: &str,
    required_capability: &str,
    require_streaming: bool,
    auth_snapshot: Option<&GatewayAuthApiKeySnapshot>,
) -> Result<Vec<String>, DataLayerError> {
    let normalized_api_format = normalize_api_format(api_format);
    let required_capability = required_capability.trim();
    if normalized_api_format.is_empty() || required_capability.is_empty() {
        return Ok(Vec::new());
    }

    if !auth_constraints_allow_api_format(
        auth_snapshot.map(auth_snapshot_constraints).as_ref(),
        &normalized_api_format,
    ) {
        return Ok(Vec::new());
    }

    let rows = state
        .read_minimal_candidate_selection_rows_for_api_format(&normalized_api_format)
        .await?;
    let auth_constraints = auth_snapshot.map(auth_snapshot_constraints);
    Ok(collect_global_model_names_for_required_capability(
        rows,
        &normalized_api_format,
        required_capability,
        require_streaming,
        auth_constraints.as_ref(),
    ))
}

pub(crate) async fn read_global_model_names_for_api_format(
    state: &(impl MinimalCandidateSelectionRowSource + Sync),
    api_format: &str,
    require_streaming: bool,
    auth_snapshot: Option<&GatewayAuthApiKeySnapshot>,
) -> Result<Vec<String>, DataLayerError> {
    let normalized_api_format = normalize_api_format(api_format);
    if normalized_api_format.is_empty() {
        return Ok(Vec::new());
    }

    if !auth_constraints_allow_api_format(
        auth_snapshot.map(auth_snapshot_constraints).as_ref(),
        &normalized_api_format,
    ) {
        return Ok(Vec::new());
    }

    let rows = state
        .read_minimal_candidate_selection_rows_for_api_format(&normalized_api_format)
        .await?;
    let auth_constraints = auth_snapshot.map(auth_snapshot_constraints);
    let mut model_names = BTreeSet::new();

    for row in rows {
        if require_streaming && !row.supports_streaming() {
            continue;
        }
        if !aether_scheduler_core::auth_constraints_allow_provider(
            auth_constraints.as_ref(),
            &row.provider_id,
            &row.provider_name,
            &row.provider_type,
        ) {
            continue;
        }
        if !aether_scheduler_core::auth_constraints_allow_model(
            auth_constraints.as_ref(),
            &row.global_model_name,
            &row.global_model_name,
        ) {
            continue;
        }
        model_names.insert(row.global_model_name);
    }

    Ok(model_names.into_iter().collect())
}

pub(crate) fn auth_snapshot_constraints(
    snapshot: &GatewayAuthApiKeySnapshot,
) -> SchedulerAuthConstraints {
    SchedulerAuthConstraints {
        allowed_providers: snapshot
            .effective_allowed_providers()
            .map(|items| items.to_vec()),
        allowed_api_formats: snapshot
            .effective_allowed_api_formats()
            .map(|items| items.to_vec()),
        allowed_models: snapshot
            .effective_allowed_models()
            .map(|items| items.to_vec()),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        read_requested_model_rows, requested_model_page_from_rows,
        MinimalCandidateSelectionRowSource, StoredMinimalCandidateSelectionRow,
    };
    use aether_data::DataLayerError;
    use async_trait::async_trait;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    /// Counts how many times the version behind a format is read. Every window
    /// a caller pages through is cut from one of these reads.
    struct CountingSelectionSource {
        format_rows: Vec<StoredMinimalCandidateSelectionRow>,
        pins: AtomicUsize,
        format_reads: AtomicUsize,
    }

    impl CountingSelectionSource {
        fn new(format_rows: Vec<StoredMinimalCandidateSelectionRow>) -> Self {
            Self {
                format_rows,
                pins: AtomicUsize::new(0),
                format_reads: AtomicUsize::new(0),
            }
        }

        fn pins(&self) -> usize {
            self.pins.load(Ordering::SeqCst)
        }

        fn format_reads(&self) -> usize {
            self.format_reads.load(Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl MinimalCandidateSelectionRowSource for CountingSelectionSource {
        async fn read_minimal_candidate_selection_rows_for_api_format(
            &self,
            _api_format: &str,
        ) -> Result<Vec<StoredMinimalCandidateSelectionRow>, DataLayerError> {
            self.format_reads.fetch_add(1, Ordering::SeqCst);
            Ok(self.format_rows.clone())
        }

        async fn pin_minimal_candidate_selection_rows_for_api_format(
            &self,
            _api_format: &str,
        ) -> Result<Arc<Vec<StoredMinimalCandidateSelectionRow>>, DataLayerError> {
            self.pins.fetch_add(1, Ordering::SeqCst);
            Ok(Arc::new(self.format_rows.clone()))
        }
    }

    fn sample_row(global_model_name: &str) -> StoredMinimalCandidateSelectionRow {
        StoredMinimalCandidateSelectionRow {
            provider_id: "provider-1".to_string(),
            provider_name: "provider".to_string(),
            provider_type: "custom".to_string(),
            provider_priority: 10,
            provider_is_active: true,
            endpoint_id: "endpoint-1".to_string(),
            endpoint_api_format: "openai:chat".to_string(),
            endpoint_api_family: Some("openai".to_string()),
            endpoint_kind: Some("chat".to_string()),
            endpoint_is_active: true,
            key_id: "key-1".to_string(),
            key_name: "key".to_string(),
            key_auth_type: "api_key".to_string(),
            key_is_active: true,
            key_api_formats: Some(vec!["openai:chat".to_string()]),
            key_allowed_models: None,
            key_capabilities: None,
            key_internal_priority: 10,
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

    #[tokio::test]
    async fn requested_model_rows_are_filtered_from_one_pinned_version() {
        let source = CountingSelectionSource::new(vec![sample_row("gpt-5")]);

        let result = read_requested_model_rows(&source, "openai:chat", "gpt-5", false)
            .await
            .expect("read should succeed")
            .expect("rows should resolve");

        assert_eq!(result.0, "gpt-5");
        assert_eq!(result.1.len(), 1);
        // One version read answers the whole walk, however many windows it
        // needs: the windows are slices, not queries.
        assert_eq!(source.pins(), 1);
        assert_eq!(source.format_reads(), 0);
    }

    #[tokio::test]
    async fn requested_model_rows_walk_stops_at_the_collected_row_limit() {
        let mut rows = Vec::new();
        for index in 0..(super::REQUESTED_MODEL_MAX_COLLECTED_ROWS + 5) {
            let mut row = sample_row("gpt-5");
            row.provider_id = format!("provider-{index}");
            row.endpoint_id = format!("endpoint-{index}");
            row.key_id = format!("key-{index}");
            row.model_id = format!("model-{index}");
            rows.push(row);
        }
        let source = CountingSelectionSource::new(rows);

        let result = read_requested_model_rows(&source, "openai:chat", "gpt-5", false)
            .await
            .expect("read should succeed")
            .expect("rows should resolve");

        assert_eq!(
            result.1.len(),
            super::REQUESTED_MODEL_MAX_COLLECTED_ROWS as usize
        );
        assert_eq!(source.pins(), 1);
        assert_eq!(source.format_reads(), 0);
    }

    #[tokio::test]
    async fn requested_model_window_reads_only_the_requested_slice() {
        let mut rows = Vec::new();
        for index in 0..10 {
            let mut row = sample_row("gpt-5");
            row.key_id = format!("key-{index}");
            rows.push(row);
        }

        let page =
            requested_model_page_from_rows(&rows, "openai:chat", "gpt-5", "gpt-5", 4, 3, false);

        assert_eq!(page.scanned_rows, 3);
        assert!(!page.scan_exhausted);
        assert_eq!(
            page.rows
                .iter()
                .map(|row| row.key_id.as_str())
                .collect::<Vec<_>>(),
            vec!["key-4", "key-5", "key-6"]
        );
    }

    #[tokio::test]
    async fn requested_model_window_reports_an_exhausted_scan() {
        let rows = vec![sample_row("gpt-5")];

        let page =
            requested_model_page_from_rows(&rows, "openai:chat", "gpt-5", "gpt-5", 0, 3, false);

        assert_eq!(page.scanned_rows, 1);
        assert!(page.scan_exhausted);
        assert_eq!(page.rows.len(), 1);
    }
}
