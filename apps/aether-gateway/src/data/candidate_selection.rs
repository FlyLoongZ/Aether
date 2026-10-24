use aether_data::DataLayerError;
use aether_data_contracts::repository::candidate_selection::StoredMinimalCandidateSelectionRow;
use aether_scheduler_core::{
    auth_constraints_allow_api_format, collect_global_model_names_for_required_capability,
    enumerate_minimal_candidate_selection_with_model_directives, normalize_api_format,
    resolve_requested_global_model_name_with_reserved_global_model,
    row_supports_requested_model_with_model_directives_and_request_operation,
    EnumerateMinimalCandidateSelectionInput, SchedulerAuthConstraints,
    SchedulerMinimalCandidateSelectionCandidate,
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
    request_operation: Option<&str>,
) -> Result<Option<(String, Vec<StoredMinimalCandidateSelectionRow>)>, DataLayerError> {
    // Resolve against the complete pinned version before collection: broad
    // global-model mappings must not fill the budget ahead of a higher-rank
    // exact match or rows belonging to the reserved global-model namespace.
    let all_rows = state
        .pin_minimal_candidate_selection_rows_for_api_format(api_format)
        .await?;
    let reserved_global_model_name =
        resolve_reserved_global_model_name(state, &all_rows, requested_model_name).await?;
    let Some(resolved_global_model_name) =
        resolve_requested_global_model_name_with_reserved_global_model(
            &all_rows,
            requested_model_name,
            api_format,
            enable_model_directives,
            request_operation,
            reserved_global_model_name.as_deref(),
        )
    else {
        return Ok(None);
    };

    let mut seen = BTreeSet::new();
    let rows = all_rows
        .iter()
        .filter(|row| row.global_model_name == resolved_global_model_name)
        .filter(|row| {
            row_supports_requested_model_with_model_directives_and_request_operation(
                row,
                requested_model_name,
                api_format,
                enable_model_directives,
                request_operation,
            )
        })
        .take(REQUESTED_MODEL_MAX_COLLECTED_ROWS as usize)
        .filter(|row| {
            seen.insert((
                row.endpoint_id.clone(),
                row.key_id.clone(),
                row.model_id.clone(),
            ))
        })
        .cloned()
        .collect();

    Ok(Some((resolved_global_model_name, rows)))
}

/// The request-level filter, including the request operation.
///
/// A model mapping may be scoped to one operation (`compact` is the live
/// example). Leaving the operation out of the predicate does not mean "any
/// operation": `mapping_scope_matches` only accepts an operation-scoped mapping
/// when the request names a matching operation, so an unscoped call quietly
/// drops those rows. Every window therefore carries the operation the request
/// asked for.
fn filter_rows_for_requested_model_for_operation(
    rows: Vec<StoredMinimalCandidateSelectionRow>,
    requested_model_name: &str,
    api_format: &str,
    enable_model_directives: bool,
    request_operation: Option<&str>,
) -> Vec<StoredMinimalCandidateSelectionRow> {
    rows.into_iter()
        .filter(|row| {
            row_supports_requested_model_with_model_directives_and_request_operation(
                row,
                requested_model_name,
                api_format,
                enable_model_directives,
                request_operation,
            )
        })
        .collect()
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
    resolved_global_model_name: &str,
    api_format: &str,
    requested_model_name: &str,
    requested_name: &str,
    offset: u32,
    limit: u32,
    enable_model_directives: bool,
    request_operation: Option<&str>,
) -> RequestedModelCandidateRowsPage {
    let limit = limit.max(1);
    let window = requested_name_window_from_rows(
        all_rows,
        resolved_global_model_name,
        api_format,
        requested_name,
        offset,
        limit,
        request_operation,
    );
    finish_requested_model_window(
        window,
        api_format,
        requested_model_name,
        limit,
        enable_model_directives,
        request_operation,
    )
}

/// The target global model is resolved from the entire pinned version first;
/// only its matching rows belong to this window and its collection budget.
/// Other global models' broad aliases cannot shift these offsets.
pub(crate) fn requested_name_window_from_rows(
    all_rows: &[StoredMinimalCandidateSelectionRow],
    resolved_global_model_name: &str,
    api_format: &str,
    requested_name: &str,
    offset: u32,
    limit: u32,
    request_operation: Option<&str>,
) -> Vec<StoredMinimalCandidateSelectionRow> {
    let limit = limit.max(1);
    all_rows
        .iter()
        .filter(|row| row.global_model_name == resolved_global_model_name)
        .filter(|row| {
            row_supports_requested_model_with_model_directives_and_request_operation(
                row,
                requested_name,
                api_format,
                false,
                request_operation,
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
    request_operation: Option<&str>,
) -> RequestedModelCandidateRowsPage {
    let scanned_rows = window.len() as u32;
    let scan_exhausted = scanned_rows < limit;
    let rows = filter_rows_for_requested_model_for_operation(
        window,
        requested_model_name,
        api_format,
        enable_model_directives,
        request_operation,
    );
    RequestedModelCandidateRowsPage {
        rows,
        scanned_rows,
        scan_exhausted,
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
        request_operation,
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

        let result = read_requested_model_rows(&source, "openai:chat", "gpt-5", false, None)
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

        let result = read_requested_model_rows(&source, "openai:chat", "gpt-5", false, None)
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
    async fn exact_alias_after_the_collection_budget_beats_broad_mappings() {
        for requested in ["requested-alias", "zzz-target"] {
            let mut rows = (0..super::REQUESTED_MODEL_MAX_COLLECTED_ROWS)
                .map(|index| {
                    let mut row = sample_row("aaa-general");
                    row.endpoint_id = format!("endpoint-{index}");
                    row.global_model_mappings = Some(vec![requested.to_string()]);
                    row
                })
                .collect::<Vec<_>>();
            let mut exact = sample_row("zzz-target");
            exact.model_provider_model_name = requested.to_string();
            rows.push(exact);
            let source = CountingSelectionSource::new(rows);

            let (global, rows) =
                read_requested_model_rows(&source, "openai:chat", requested, false, None)
                    .await
                    .unwrap()
                    .expect("exact match must remain reachable");
            assert_eq!(global, "zzz-target");
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].model_provider_model_name, requested);
            assert_eq!(source.pins(), 1);
        }
    }

    #[tokio::test]
    async fn operation_scoped_alias_is_used_for_global_resolution_too() {
        let mut row = sample_row("global-compact");
        row.model_provider_model_mappings = Some(vec![
            aether_data_contracts::repository::candidate_selection::StoredProviderModelMapping {
                name: "compact-alias".to_string(),
                priority: 0,
                api_formats: None,
                endpoint_ids: None,
                operations: Some(vec!["compact".to_string()]),
            },
        ]);
        let source = CountingSelectionSource::new(vec![row]);
        let result = read_requested_model_rows(
            &source,
            "openai:chat",
            "compact-alias",
            false,
            Some("compact"),
        )
        .await
        .unwrap()
        .expect("operation must reach the resolver");
        assert_eq!(result.0, "global-compact");
        assert_eq!(result.1.len(), 1);
        assert!(
            read_requested_model_rows(&source, "openai:chat", "compact-alias", false, None,)
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn requested_model_window_reads_only_the_requested_slice() {
        let mut rows = Vec::new();
        for index in 0..10 {
            let mut row = sample_row("gpt-5");
            row.key_id = format!("key-{index}");
            rows.push(row);
        }

        let page = requested_model_page_from_rows(
            &rows,
            "gpt-5",
            "openai:chat",
            "gpt-5",
            "gpt-5",
            4,
            3,
            false,
            None,
        );

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

    /// A mapping scoped to one request operation is only offered to requests
    /// that name it. The window has to apply the same predicate the request
    /// does: an operation-scoped mapping does not mean "any operation".
    #[tokio::test]
    async fn a_window_applies_the_request_operation_scope() {
        let mut row = sample_row("compacted");
        row.model_provider_model_name = "gpt-5-compact".to_string();
        row.model_provider_model_mappings = Some(vec![
            aether_data_contracts::repository::candidate_selection::StoredProviderModelMapping {
                name: "gpt-5-compact".to_string(),
                priority: 0,
                api_formats: None,
                endpoint_ids: None,
                operations: Some(vec!["compact".to_string()]),
            },
        ]);
        let rows = vec![row];

        let compact = requested_model_page_from_rows(
            &rows,
            "compacted",
            "openai:chat",
            "gpt-5-compact",
            "gpt-5-compact",
            0,
            8,
            false,
            Some("compact"),
        );
        assert_eq!(compact.rows.len(), 1);
        assert_eq!(compact.scanned_rows, 1);

        let unscoped = requested_model_page_from_rows(
            &rows,
            "compacted",
            "openai:chat",
            "gpt-5-compact",
            "gpt-5-compact",
            0,
            8,
            false,
            None,
        );
        assert!(unscoped.rows.is_empty());
        assert_eq!(unscoped.scanned_rows, 0);
    }

    #[tokio::test]
    async fn requested_model_window_reports_an_exhausted_scan() {
        let rows = vec![sample_row("gpt-5")];

        let page = requested_model_page_from_rows(
            &rows,
            "gpt-5",
            "openai:chat",
            "gpt-5",
            "gpt-5",
            0,
            3,
            false,
            None,
        );

        assert_eq!(page.scanned_rows, 1);
        assert!(page.scan_exhausted);
        assert_eq!(page.rows.len(), 1);
    }
}
