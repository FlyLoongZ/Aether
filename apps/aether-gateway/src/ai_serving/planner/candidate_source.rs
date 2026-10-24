use aether_ai_serving::{
    run_ai_candidate_preselection, AiCandidatePreselectionOutcome, AiCandidatePreselectionPort,
};
use aether_data_contracts::repository::candidate_selection::StoredMinimalCandidateSelectionRow;
use aether_routing_core::ResolvedRoutingPolicy;
use aether_runtime::ConcurrencyPermit;
use aether_scheduler_core::{
    enumerate_minimal_candidate_selection_with_model_directives, normalize_api_format,
    resolve_requested_global_model_name_with_reserved_global_model,
    row_supports_requested_model_with_model_directives_and_request_operation,
    ClientSessionAffinity, EnumerateMinimalCandidateSelectionInput,
    SchedulerMinimalCandidateSelectionCandidate,
};
use async_trait::async_trait;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::Arc;

use crate::ai_serving::planner::candidate_affinity_cache::has_explicit_session_affinity;
use crate::ai_serving::planner::candidate_resolution::SkippedLocalExecutionCandidate;
use crate::ai_serving::{GatewayAuthApiKeySnapshot, PlannerAppState};
use crate::cache::{
    candidate_page_cache_stale_ttl, candidate_page_cache_ttl_from_env,
    record_candidate_page_cache_none, CandidatePageCacheKey, CandidatePageSnapshot,
};
use crate::clock::request_distribution_seed;
use crate::data::candidate_selection::{
    read_api_format_rows_fallback_page, read_requested_model_rows_fast_path_page,
    requested_model_candidate_names, CandidateScanBudget, RequestedModelCandidateRowsPage,
    REQUESTED_MODEL_CANDIDATE_PAGE_SIZE, REQUESTED_MODEL_MAX_COLLECTED_ROWS,
};
use crate::scheduler::candidate::SchedulerSkippedCandidate;
use crate::scheduler::config::{SchedulerOrderingConfig, SchedulerSchedulingMode};
use crate::stage_metrics::observe_gateway_stage_ms;
use crate::GatewayError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LocalCandidatePreselectionKeyMode {
    ProviderEndpointKeyModel,
    ProviderEndpointKeyModelAndApiFormat,
}

impl LocalCandidatePreselectionKeyMode {
    pub(crate) fn cache_key_name(self) -> &'static str {
        match self {
            Self::ProviderEndpointKeyModel => "provider_endpoint_key_model",
            Self::ProviderEndpointKeyModelAndApiFormat => "provider_endpoint_key_model_api_format",
        }
    }
}

struct GatewayLocalCandidatePreselectionPort<'a> {
    state: PlannerAppState<'a>,
    client_api_format: &'a str,
    requested_model: &'a str,
    request_operation: Option<&'a str>,
    require_streaming: bool,
    required_capabilities: Option<&'a serde_json::Value>,
    auth_snapshot: &'a GatewayAuthApiKeySnapshot,
    routing_policy: Option<&'a ResolvedRoutingPolicy>,
    client_session_affinity: Option<&'a ClientSessionAffinity>,
    use_api_format_alias_match: bool,
    key_mode: LocalCandidatePreselectionKeyMode,
    candidate_api_formats: Vec<String>,
    model_directive_routing_models: BTreeMap<String, String>,
    ranking_seed: u64,
}

impl GatewayLocalCandidatePreselectionPort<'_> {
    fn model_directive_base_model(&self, candidate_api_format: &str) -> Option<&str> {
        self.model_directive_routing_models
            .get(&crate::ai_serving::normalize_api_format_alias(
                candidate_api_format,
            ))
            .map(String::as_str)
    }

    fn routing_model(&self, candidate_api_format: &str) -> &str {
        self.model_directive_base_model(candidate_api_format)
            .unwrap_or(self.requested_model)
    }
}

/// A Responses compaction request carries the OpenAI-only `compaction_trigger`
/// control item.  It must stay on an OpenAI Responses endpoint: treating it as
/// an ordinary cross-format request would make Gemini/Claude candidates look
/// eligible and defer the inevitable lossy-conversion failure until payload
/// construction.
fn request_candidate_api_formats_for_operation(
    client_api_format: &str,
    require_streaming: bool,
    request_operation: Option<&str>,
) -> Vec<String> {
    let candidate_api_formats =
        crate::ai_serving::request_candidate_api_formats(client_api_format, require_streaming)
            .into_iter()
            .map(str::to_string)
            .collect::<Vec<_>>();

    restrict_candidate_api_formats_for_operation(
        client_api_format,
        request_operation,
        candidate_api_formats,
    )
}

fn restrict_candidate_api_formats_for_operation(
    client_api_format: &str,
    request_operation: Option<&str>,
    candidate_api_formats: Vec<String>,
) -> Vec<String> {
    let is_responses_compaction = request_operation.is_some_and(|operation| {
        operation.eq_ignore_ascii_case(crate::ai_serving::OPENAI_RESPONSES_OPERATION_COMPACT)
    });
    let is_standard_responses_client =
        crate::ai_serving::normalize_api_format_alias(client_api_format) == "openai:responses";
    if !(is_responses_compaction && is_standard_responses_client) {
        return candidate_api_formats;
    }

    candidate_api_formats
        .into_iter()
        .filter(|candidate_api_format| {
            crate::ai_serving::normalize_api_format_alias(candidate_api_format)
                == "openai:responses"
        })
        .collect()
}

#[async_trait]
impl AiCandidatePreselectionPort for GatewayLocalCandidatePreselectionPort<'_> {
    type Candidate = SchedulerMinimalCandidateSelectionCandidate;
    type Skipped = SkippedLocalExecutionCandidate;
    type Error = GatewayError;

    fn candidate_api_formats(&self) -> Vec<String> {
        self.candidate_api_formats.clone()
    }

    fn candidate_api_format_matches_client(&self, candidate_api_format: &str) -> bool {
        if self.use_api_format_alias_match {
            crate::ai_serving::api_format_alias_matches(
                candidate_api_format,
                self.client_api_format,
            )
        } else {
            candidate_api_format == self.client_api_format
        }
    }

    async fn list_candidates_for_api_format(
        &self,
        candidate_api_format: &str,
        matches_client_format: bool,
    ) -> Result<(Vec<Self::Candidate>, Vec<Self::Skipped>), Self::Error> {
        let auth_snapshot = matches_client_format.then_some(self.auth_snapshot);
        let (candidates, skipped_candidates) = self
            .state
            .list_selectable_candidates_with_skip_reasons_for_request_operation(
                candidate_api_format,
                self.routing_model(candidate_api_format),
                self.require_streaming,
                self.required_capabilities,
                auth_snapshot,
                self.routing_policy
                    .is_none()
                    .then_some(self.client_session_affinity)
                    .flatten(),
                self.ranking_seed,
                false,
                self.request_operation,
                super::candidate_ranking::scheduler_ordering_config_for_routing_policy(
                    self.routing_policy,
                ),
            )
            .await?;

        Ok((
            candidates,
            skipped_candidates
                .into_iter()
                .map(skipped_local_execution_candidate_from_scheduler_skip)
                .collect(),
        ))
    }

    fn candidate_allowed(
        &self,
        candidate: &Self::Candidate,
        candidate_api_format: &str,
        matches_client_format: bool,
    ) -> bool {
        routing_policy_allows_provider(self.routing_policy, candidate)
            && (matches_client_format
                || auth_snapshot_allows_cross_format_candidate(
                    self.auth_snapshot,
                    self.requested_model,
                    self.model_directive_base_model(candidate_api_format),
                    candidate,
                ))
    }

    fn skipped_candidate_allowed(
        &self,
        skipped_candidate: &Self::Skipped,
        candidate_api_format: &str,
        matches_client_format: bool,
    ) -> bool {
        routing_policy_allows_provider(self.routing_policy, &skipped_candidate.candidate)
            && (matches_client_format
                || auth_snapshot_allows_cross_format_candidate(
                    self.auth_snapshot,
                    self.requested_model,
                    self.model_directive_base_model(candidate_api_format),
                    &skipped_candidate.candidate,
                ))
    }

    fn candidate_key(&self, candidate: &Self::Candidate) -> String {
        local_candidate_preselection_key(candidate, self.key_mode)
    }

    fn skipped_candidate_key(&self, skipped_candidate: &Self::Skipped) -> String {
        local_candidate_preselection_key(&skipped_candidate.candidate, self.key_mode)
    }
}

fn resolve_model_directive_routing_models(
    policy: &crate::system_features::ModelDirectivePolicySnapshot,
    candidate_api_formats: &[String],
    requested_model: &str,
) -> BTreeMap<String, String> {
    candidate_api_formats
        .iter()
        .filter_map(|api_format| {
            let api_format = crate::ai_serving::normalize_api_format_alias(api_format);
            let resolution = policy.resolve_reasoning(&api_format, Some(requested_model));
            resolution
                .base_model()
                .map(|base_model| (api_format, base_model.to_string()))
        })
        .collect()
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn preselect_local_execution_candidates_with_serving(
    state: PlannerAppState<'_>,
    model_directive_policy: &crate::system_features::ModelDirectivePolicySnapshot,
    client_api_format: &str,
    requested_model: &str,
    request_operation: Option<&str>,
    require_streaming: bool,
    required_capabilities: Option<&serde_json::Value>,
    auth_snapshot: &GatewayAuthApiKeySnapshot,
    routing_policy: Option<&ResolvedRoutingPolicy>,
    client_session_affinity: Option<&ClientSessionAffinity>,
    use_api_format_alias_match: bool,
    key_mode: LocalCandidatePreselectionKeyMode,
) -> Result<
    AiCandidatePreselectionOutcome<
        SchedulerMinimalCandidateSelectionCandidate,
        SkippedLocalExecutionCandidate,
    >,
    GatewayError,
> {
    let candidate_api_formats = request_candidate_api_formats_for_operation(
        client_api_format,
        require_streaming,
        request_operation,
    );
    preselect_local_execution_candidates_for_api_formats_with_serving(
        state,
        model_directive_policy,
        client_api_format,
        requested_model,
        request_operation,
        require_streaming,
        required_capabilities,
        auth_snapshot,
        routing_policy,
        client_session_affinity,
        use_api_format_alias_match,
        key_mode,
        candidate_api_formats,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn preselect_local_execution_candidates_for_api_formats_with_serving(
    state: PlannerAppState<'_>,
    model_directive_policy: &crate::system_features::ModelDirectivePolicySnapshot,
    client_api_format: &str,
    requested_model: &str,
    request_operation: Option<&str>,
    require_streaming: bool,
    required_capabilities: Option<&serde_json::Value>,
    auth_snapshot: &GatewayAuthApiKeySnapshot,
    routing_policy: Option<&ResolvedRoutingPolicy>,
    client_session_affinity: Option<&ClientSessionAffinity>,
    use_api_format_alias_match: bool,
    key_mode: LocalCandidatePreselectionKeyMode,
    candidate_api_formats: Vec<String>,
) -> Result<
    AiCandidatePreselectionOutcome<
        SchedulerMinimalCandidateSelectionCandidate,
        SkippedLocalExecutionCandidate,
    >,
    GatewayError,
> {
    let candidate_api_formats = restrict_candidate_api_formats_for_operation(
        client_api_format,
        request_operation,
        candidate_api_formats,
    );
    let model_directive_routing_models = resolve_model_directive_routing_models(
        model_directive_policy,
        &candidate_api_formats,
        requested_model,
    );
    let port = GatewayLocalCandidatePreselectionPort {
        state,
        client_api_format,
        requested_model,
        request_operation,
        require_streaming,
        required_capabilities,
        auth_snapshot,
        routing_policy,
        client_session_affinity,
        use_api_format_alias_match,
        key_mode,
        candidate_api_formats,
        model_directive_routing_models,
        ranking_seed: request_distribution_seed(),
    };

    run_ai_candidate_preselection(&port).await
}

pub(crate) struct LocalCandidatePreselectionPageCursor<'a> {
    state: PlannerAppState<'a>,
    trace_id: String,
    client_api_format: String,
    requested_model: String,
    request_operation: Option<String>,
    require_streaming: bool,
    required_capabilities: Option<serde_json::Value>,
    auth_snapshot: GatewayAuthApiKeySnapshot,
    routing_policy: Option<ResolvedRoutingPolicy>,
    client_session_affinity: Option<ClientSessionAffinity>,
    request_auth_channel: Option<String>,
    use_api_format_alias_match: bool,
    key_mode: LocalCandidatePreselectionKeyMode,
    allow_priority_page_cache: bool,
    candidate_api_formats: Vec<String>,
    model_directive_routing_models: BTreeMap<String, String>,
    model_directive_policy_cache_key: String,
    ordering_config: SchedulerOrderingConfig,
    ranking_seed: u64,
    priority_page_emitted: bool,
    deferred_pages_by_format: BTreeMap<
        String,
        VecDeque<
            AiCandidatePreselectionOutcome<
                SchedulerMinimalCandidateSelectionCandidate,
                SkippedLocalExecutionCandidate,
            >,
        >,
    >,
    format_index: usize,
    requested_name_indexes: BTreeMap<String, usize>,
    requested_name_offsets: BTreeMap<String, u32>,
    scan_budget_by_format: BTreeMap<String, CandidateScanBudget>,
    resolved_global_model_names: BTreeMap<String, String>,
    reserved_global_model_names: BTreeMap<String, Option<String>>,
    fallback_offsets: BTreeMap<String, u32>,
    fallback_scan_epoch: u32,
    exhausted_api_formats: BTreeSet<String>,
    seen_candidate_keys: BTreeSet<String>,
}

impl<'a> LocalCandidatePreselectionPageCursor<'a> {
    fn model_directive_base_model(&self, candidate_api_format: &str) -> Option<&str> {
        self.model_directive_routing_models
            .get(&crate::ai_serving::normalize_api_format_alias(
                candidate_api_format,
            ))
            .map(String::as_str)
    }

    fn routing_model(&self, candidate_api_format: &str) -> &str {
        self.model_directive_base_model(candidate_api_format)
            .unwrap_or(&self.requested_model)
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn new(
        state: PlannerAppState<'a>,
        model_directive_policy: &crate::system_features::ModelDirectivePolicySnapshot,
        client_api_format: &str,
        requested_model: &str,
        request_operation: Option<&str>,
        require_streaming: bool,
        required_capabilities: Option<&serde_json::Value>,
        auth_snapshot: &GatewayAuthApiKeySnapshot,
        routing_policy: Option<&ResolvedRoutingPolicy>,
        client_session_affinity: Option<&ClientSessionAffinity>,
        request_auth_channel: Option<&str>,
        use_api_format_alias_match: bool,
        key_mode: LocalCandidatePreselectionKeyMode,
        allow_priority_page_cache: bool,
        trace_id: Option<&str>,
    ) -> Self {
        let candidate_api_formats = request_candidate_api_formats_for_operation(
            client_api_format,
            require_streaming,
            request_operation,
        );
        let model_directive_routing_models = resolve_model_directive_routing_models(
            model_directive_policy,
            &candidate_api_formats,
            requested_model,
        );

        let ordering_config =
            super::candidate_ranking::scheduler_ordering_config_for_routing_policy(routing_policy);

        Self {
            state,
            trace_id: trace_id.unwrap_or_default().to_string(),
            client_api_format: client_api_format.to_string(),
            requested_model: requested_model.to_string(),
            request_operation: request_operation.map(str::to_string),
            require_streaming,
            required_capabilities: required_capabilities.cloned(),
            auth_snapshot: auth_snapshot.clone(),
            routing_policy: routing_policy.cloned(),
            client_session_affinity: client_session_affinity.cloned(),
            request_auth_channel: request_auth_channel.map(str::to_string),
            use_api_format_alias_match,
            key_mode,
            allow_priority_page_cache,
            candidate_api_formats,
            model_directive_routing_models,
            model_directive_policy_cache_key: model_directive_policy.cache_key().to_string(),
            ordering_config,
            ranking_seed: request_distribution_seed(),
            priority_page_emitted: false,
            deferred_pages_by_format: BTreeMap::new(),
            format_index: 0,
            requested_name_indexes: BTreeMap::new(),
            requested_name_offsets: BTreeMap::new(),
            scan_budget_by_format: BTreeMap::new(),
            resolved_global_model_names: BTreeMap::new(),
            reserved_global_model_names: BTreeMap::new(),
            fallback_offsets: BTreeMap::new(),
            fallback_scan_epoch: 0,
            exhausted_api_formats: BTreeSet::new(),
            seen_candidate_keys: BTreeSet::new(),
        }
    }

    pub(crate) async fn next_page(
        &mut self,
    ) -> Result<
        Option<
            AiCandidatePreselectionOutcome<
                SchedulerMinimalCandidateSelectionCandidate,
                SkippedLocalExecutionCandidate,
            >,
        >,
        GatewayError,
    > {
        if !self.priority_page_emitted {
            self.priority_page_emitted = true;
            let mut priority_page = self.cached_next_priority_page().await?;
            if self.routing_policy.is_some() {
                while let Some(mut page) = self.next_page_after_priority().await? {
                    priority_page.candidates.append(&mut page.candidates);
                    priority_page
                        .skipped_candidates
                        .append(&mut page.skipped_candidates);
                }
            }
            if !priority_page.candidates.is_empty() || !priority_page.skipped_candidates.is_empty()
            {
                return Ok(Some(priority_page));
            }
        }

        self.next_page_after_priority().await
    }

    async fn next_page_after_priority(
        &mut self,
    ) -> Result<
        Option<
            AiCandidatePreselectionOutcome<
                SchedulerMinimalCandidateSelectionCandidate,
                SkippedLocalExecutionCandidate,
            >,
        >,
        GatewayError,
    > {
        // Deferred pages and formats already proven exhausted require no planning
        // permit. This is the common second-target path for a single-candidate
        // model, so keep it entirely in memory before joining the shared gate.
        while self.format_index < self.candidate_api_formats.len() {
            let candidate_api_format = self.candidate_api_formats[self.format_index].clone();
            if let Some(outcome) = self.pop_deferred_page(&candidate_api_format) {
                return Ok(Some(outcome));
            }
            if self.api_format_is_exhausted(&candidate_api_format) {
                self.format_index += 1;
                continue;
            }
            break;
        }
        if self.format_index >= self.candidate_api_formats.len() {
            return Ok(None);
        }

        // One next_page call may need to confirm exhaustion across several API
        // formats. Hold one permit for that scan instead of rejoining the gate
        // once per format.
        let _permit = acquire_candidate_planning_gate(self.state, &self.trace_id).await?;
        while self.format_index < self.candidate_api_formats.len() {
            let candidate_api_format = self.candidate_api_formats[self.format_index].clone();
            if let Some(outcome) = self.pop_deferred_page(&candidate_api_format) {
                return Ok(Some(outcome));
            }
            if self.api_format_is_exhausted(&candidate_api_format) {
                self.format_index += 1;
                continue;
            }
            let Some(outcome) = self.next_page_for_api_format(&candidate_api_format).await? else {
                self.format_index += 1;
                continue;
            };
            if outcome.candidates.is_empty() && outcome.skipped_candidates.is_empty() {
                continue;
            }
            return Ok(Some(outcome));
        }
        Ok(None)
    }

    pub(crate) fn restart_scan(&mut self) {
        self.format_index = 0;
        self.requested_name_indexes.clear();
        self.requested_name_offsets.clear();
        self.scan_budget_by_format.clear();
        self.resolved_global_model_names.clear();
        self.reserved_global_model_names.clear();
        self.fallback_offsets.clear();
        self.fallback_scan_epoch = self.fallback_scan_epoch.wrapping_add(1);
        self.exhausted_api_formats.clear();
        self.seen_candidate_keys.clear();
        self.priority_page_emitted = false;
        self.deferred_pages_by_format.clear();
    }

    /// The scan budget for one API format, created on first use.
    fn scan_budget(&self, normalized_api_format: &str) -> CandidateScanBudget {
        self.scan_budget_by_format
            .get(normalized_api_format)
            .copied()
            .unwrap_or_else(|| CandidateScanBudget::new(REQUESTED_MODEL_MAX_COLLECTED_ROWS))
    }

    fn charge_scan_budget(&mut self, normalized_api_format: &str, rows: u32) {
        let mut budget = self.scan_budget(normalized_api_format);
        budget.consume(rows);
        self.scan_budget_by_format
            .insert(normalized_api_format.to_string(), budget);
    }

    pub(crate) fn resolved_page_cache_preselection_mode(&self) -> &'static str {
        self.key_mode.cache_key_name()
    }

    pub(crate) fn resolved_page_cache_request_operation(&self) -> Option<&str> {
        self.request_operation.as_deref()
    }

    pub(crate) fn resolved_page_cache_use_api_format_alias_match(&self) -> bool {
        self.use_api_format_alias_match
    }

    pub(crate) fn resolved_page_cache_model_directive_policy_hash(&self) -> &str {
        &self.model_directive_policy_cache_key
    }

    pub(crate) fn should_cache_current_priority_resolved_page(&self) -> bool {
        if !(self.priority_page_emitted
            && self.format_index == 0
            && self.deferred_pages_by_format.is_empty())
        {
            return false;
        }

        match self.ordering_config.scheduling_mode {
            SchedulerSchedulingMode::FixedOrder => true,
            SchedulerSchedulingMode::CacheAffinity => {
                has_explicit_session_affinity(self.client_session_affinity.as_ref())
            }
            SchedulerSchedulingMode::LoadBalance => false,
        }
    }

    fn should_cache_current_priority_page(&self) -> bool {
        self.allow_priority_page_cache && self.should_cache_current_priority_resolved_page()
    }

    #[cfg(test)]
    pub(crate) fn mark_priority_page_emitted_for_tests(&mut self) {
        self.priority_page_emitted = true;
    }

    async fn cached_next_priority_page(
        &mut self,
    ) -> Result<
        AiCandidatePreselectionOutcome<
            SchedulerMinimalCandidateSelectionCandidate,
            SkippedLocalExecutionCandidate,
        >,
        GatewayError,
    > {
        let page = if self.should_cache_current_priority_page() {
            self.cached_next_priority_page_snapshot().await?
        } else {
            self.next_priority_page_with_planning_gate().await?
        };
        self.remember_seen_candidates_from_page(&page);
        Ok(page)
    }

    async fn cached_next_priority_page_snapshot(
        &mut self,
    ) -> Result<
        AiCandidatePreselectionOutcome<
            SchedulerMinimalCandidateSelectionCandidate,
            SkippedLocalExecutionCandidate,
        >,
        GatewayError,
    > {
        let key = CandidatePageCacheKey::new(
            &self.requested_model,
            self.request_operation.as_deref(),
            &self.client_api_format,
            self.require_streaming,
            &self.auth_snapshot,
            self.required_capabilities.as_ref(),
            self.routing_policy.as_ref(),
            self.request_auth_channel.as_deref(),
            self.state.app().scheduler_affinity_epoch(),
            self.key_mode.cache_key_name(),
            self.use_api_format_alias_match,
            self.client_session_affinity.as_ref(),
            &self.model_directive_policy_cache_key,
        );
        let cache = self.state.app().candidate_page_cache.clone();
        // This page has no background refresh: the loader advances this
        // request's own cursor, so it can neither be borrowed by a spawned task
        // nor replayed out of order. Freshness is therefore write-driven — a
        // catalog, routing or health change clears the cache — and the window
        // below is only the fallback for a missed cross-node invalidation.
        let window = candidate_page_cache_stale_ttl(candidate_page_cache_ttl_from_env());
        let cached = cache
            .get_or_load(key, window, || async {
                let page = self.next_priority_page_with_planning_gate().await?;
                Ok::<_, GatewayError>(Some(Arc::new(page) as Arc<CandidatePageSnapshot>))
            })
            .await?;

        match cached {
            Some(snapshot) => {
                let page = snapshot.as_ref().clone();
                if page.candidates.is_empty() && page.skipped_candidates.is_empty() {
                    record_candidate_page_cache_none();
                }
                Ok(page)
            }
            None => {
                record_candidate_page_cache_none();
                Ok(AiCandidatePreselectionOutcome {
                    candidates: Vec::new(),
                    skipped_candidates: Vec::new(),
                })
            }
        }
    }

    fn remember_seen_candidates_from_page(
        &mut self,
        page: &AiCandidatePreselectionOutcome<
            SchedulerMinimalCandidateSelectionCandidate,
            SkippedLocalExecutionCandidate,
        >,
    ) {
        for candidate in &page.candidates {
            self.seen_candidate_keys
                .insert(local_candidate_preselection_key(candidate, self.key_mode));
        }
        for skipped_candidate in &page.skipped_candidates {
            self.seen_candidate_keys
                .insert(local_candidate_preselection_key(
                    &skipped_candidate.candidate,
                    self.key_mode,
                ));
        }
    }

    async fn next_priority_page(
        &mut self,
    ) -> Result<
        AiCandidatePreselectionOutcome<
            SchedulerMinimalCandidateSelectionCandidate,
            SkippedLocalExecutionCandidate,
        >,
        GatewayError,
    > {
        let mut priority_page = AiCandidatePreselectionOutcome {
            candidates: Vec::new(),
            skipped_candidates: Vec::new(),
        };

        for candidate_api_format in self.candidate_api_formats.clone() {
            let Some(outcome) = self.next_page_for_api_format(&candidate_api_format).await? else {
                continue;
            };
            if matches_client_api_format(
                self.use_api_format_alias_match,
                &candidate_api_format,
                &self.client_api_format,
            ) {
                priority_page.candidates.extend(outcome.candidates);
                priority_page
                    .skipped_candidates
                    .extend(outcome.skipped_candidates);
                continue;
            }

            let (promoted, deferred) = self
                .split_priority_conversion_page(&candidate_api_format, outcome)
                .await;
            priority_page.candidates.extend(promoted.candidates);
            priority_page
                .skipped_candidates
                .extend(promoted.skipped_candidates);
            self.defer_page(candidate_api_format, deferred);
        }

        Ok(priority_page)
    }

    async fn next_priority_page_with_planning_gate(
        &mut self,
    ) -> Result<
        AiCandidatePreselectionOutcome<
            SchedulerMinimalCandidateSelectionCandidate,
            SkippedLocalExecutionCandidate,
        >,
        GatewayError,
    > {
        let _permit = acquire_candidate_planning_gate(self.state, &self.trace_id).await?;
        self.next_priority_page().await
    }

    async fn split_priority_conversion_page(
        &self,
        candidate_api_format: &str,
        outcome: AiCandidatePreselectionOutcome<
            SchedulerMinimalCandidateSelectionCandidate,
            SkippedLocalExecutionCandidate,
        >,
    ) -> (
        AiCandidatePreselectionOutcome<
            SchedulerMinimalCandidateSelectionCandidate,
            SkippedLocalExecutionCandidate,
        >,
        AiCandidatePreselectionOutcome<
            SchedulerMinimalCandidateSelectionCandidate,
            SkippedLocalExecutionCandidate,
        >,
    ) {
        let mut promoted = AiCandidatePreselectionOutcome {
            candidates: Vec::new(),
            skipped_candidates: Vec::new(),
        };
        let mut deferred = AiCandidatePreselectionOutcome {
            candidates: Vec::new(),
            skipped_candidates: Vec::new(),
        };

        for candidate in outcome.candidates {
            if self
                .cross_format_candidate_keeps_priority(&candidate, candidate_api_format)
                .await
            {
                promoted.candidates.push(candidate);
            } else {
                deferred.candidates.push(candidate);
            }
        }

        for skipped_candidate in outcome.skipped_candidates {
            if self
                .cross_format_candidate_keeps_priority(
                    &skipped_candidate.candidate,
                    candidate_api_format,
                )
                .await
            {
                promoted.skipped_candidates.push(skipped_candidate);
            } else {
                deferred.skipped_candidates.push(skipped_candidate);
            }
        }

        (promoted, deferred)
    }

    async fn cross_format_candidate_keeps_priority(
        &self,
        candidate: &SchedulerMinimalCandidateSelectionCandidate,
        candidate_api_format: &str,
    ) -> bool {
        if matches_client_api_format(
            self.use_api_format_alias_match,
            candidate_api_format,
            &self.client_api_format,
        ) {
            return false;
        }
        super::candidate_transport_ranking_facts::candidate_keeps_priority_on_conversion(
            self.state,
            candidate,
            self.ordering_config,
        )
        .await
    }

    fn defer_page(
        &mut self,
        candidate_api_format: String,
        outcome: AiCandidatePreselectionOutcome<
            SchedulerMinimalCandidateSelectionCandidate,
            SkippedLocalExecutionCandidate,
        >,
    ) {
        if outcome.candidates.is_empty() && outcome.skipped_candidates.is_empty() {
            return;
        }
        self.deferred_pages_by_format
            .entry(candidate_api_format)
            .or_default()
            .push_back(outcome);
    }

    fn pop_deferred_page(
        &mut self,
        candidate_api_format: &str,
    ) -> Option<
        AiCandidatePreselectionOutcome<
            SchedulerMinimalCandidateSelectionCandidate,
            SkippedLocalExecutionCandidate,
        >,
    > {
        loop {
            let pages = self
                .deferred_pages_by_format
                .get_mut(candidate_api_format)?;
            let outcome = pages.pop_front()?;
            if pages.is_empty() {
                self.deferred_pages_by_format.remove(candidate_api_format);
            }
            if !outcome.candidates.is_empty() || !outcome.skipped_candidates.is_empty() {
                return Some(outcome);
            }
        }
    }

    async fn next_page_for_api_format(
        &mut self,
        candidate_api_format: &str,
    ) -> Result<
        Option<
            AiCandidatePreselectionOutcome<
                SchedulerMinimalCandidateSelectionCandidate,
                SkippedLocalExecutionCandidate,
            >,
        >,
        GatewayError,
    > {
        let normalized_api_format = normalize_api_format(candidate_api_format);
        if normalized_api_format.is_empty() {
            return Ok(None);
        }
        if self.exhausted_api_formats.contains(&normalized_api_format) {
            return Ok(None);
        }
        let routing_model = self.routing_model(candidate_api_format).to_string();
        let requested_names = requested_model_candidate_names(&routing_model, false);
        if self.scan_budget(&normalized_api_format).is_exhausted() {
            self.exhausted_api_formats
                .insert(normalized_api_format.clone());
            return Ok(None);
        }

        loop {
            let requested_name_index = *self
                .requested_name_indexes
                .entry(normalized_api_format.clone())
                .or_insert(0);
            let Some(requested_name) = requested_names.get(requested_name_index) else {
                return self
                    .next_fallback_page_for_api_format(candidate_api_format, &normalized_api_format)
                    .await;
            };
            if requested_name.trim().is_empty() {
                self.requested_name_indexes
                    .insert(normalized_api_format.clone(), requested_name_index + 1);
                continue;
            }

            let offset_key = format!("{normalized_api_format}:{requested_name_index}");
            let offset = *self
                .requested_name_offsets
                .entry(offset_key.clone())
                .or_insert(0);
            let budget = self.scan_budget(&normalized_api_format);
            if budget.is_exhausted() {
                self.exhausted_api_formats
                    .insert(normalized_api_format.clone());
                return Ok(None);
            }
            let limit = REQUESTED_MODEL_CANDIDATE_PAGE_SIZE.min(budget.remaining());
            let page = self
                .read_requested_model_rows_fast_path_page_cached(
                    &normalized_api_format,
                    requested_name,
                    &routing_model,
                    offset,
                    limit,
                )
                .await?;
            self.charge_scan_budget(&normalized_api_format, page.scanned_rows);
            self.requested_name_offsets
                .insert(offset_key, offset.saturating_add(limit));
            // The name is done when the query shape had nothing after this
            // window. Budget exhaustion is a different outcome: it ends the
            // whole format, not this one name.
            if page.scan_exhausted {
                self.requested_name_indexes
                    .insert(normalized_api_format.clone(), requested_name_index + 1);
            }
            if page.scanned_rows == 0 {
                if requested_name_index + 1 >= requested_names.len() {
                    return self
                        .next_fallback_page_for_api_format(
                            candidate_api_format,
                            &normalized_api_format,
                        )
                        .await;
                }
                continue;
            }

            if let Some(outcome) = self
                .build_page_outcome_from_rows(
                    candidate_api_format,
                    &normalized_api_format,
                    page.rows,
                )
                .await?
            {
                return Ok(Some(outcome));
            }
        }
    }

    /// The candidate rows behind one requested-model page.
    ///
    /// The data layer already caches this page (five seconds, per query
    /// shape), so a second application-level page cache here only widened
    /// staleness without saving a query: it was keyed by the same fields and
    /// served rows up to its own fallback window while the data layer had
    /// already refreshed them.
    async fn read_requested_model_rows_fast_path_page_cached(
        &self,
        normalized_api_format: &str,
        requested_name: &str,
        routing_model: &str,
        offset: u32,
        limit: u32,
    ) -> Result<RequestedModelCandidateRowsPage, GatewayError> {
        read_requested_model_rows_fast_path_page(
            self.state.app().data.as_ref(),
            normalized_api_format,
            routing_model,
            requested_name,
            offset,
            limit,
            false,
        )
        .await
        .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    async fn read_api_format_rows_fallback_page_cached(
        &self,
        normalized_api_format: &str,
        offset: u32,
        limit: u32,
    ) -> Result<RequestedModelCandidateRowsPage, GatewayError> {
        read_api_format_rows_fallback_page(
            self.state.app().data.as_ref(),
            normalized_api_format,
            offset,
            limit,
        )
        .await
        .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    async fn next_fallback_page_for_api_format(
        &mut self,
        candidate_api_format: &str,
        normalized_api_format: &str,
    ) -> Result<
        Option<
            AiCandidatePreselectionOutcome<
                SchedulerMinimalCandidateSelectionCandidate,
                SkippedLocalExecutionCandidate,
            >,
        >,
        GatewayError,
    > {
        let routing_model = self.routing_model(candidate_api_format).to_string();
        loop {
            let budget = self.scan_budget(normalized_api_format);
            if budget.is_exhausted() {
                self.exhausted_api_formats
                    .insert(normalized_api_format.to_string());
                return Ok(None);
            }
            let limit = REQUESTED_MODEL_CANDIDATE_PAGE_SIZE.min(budget.remaining());
            let offset = *self
                .fallback_offsets
                .get(normalized_api_format)
                .unwrap_or(&0);
            let page = self
                .read_api_format_rows_fallback_page_cached(normalized_api_format, offset, limit)
                .await?;
            let page_scanned = page.scanned_rows.min(limit);
            // Two independent stop conditions: the shape has nothing left, or
            // this request has walked as far as its budget allows.
            let scan_ended = page.scan_exhausted || page_scanned < limit;
            self.fallback_offsets.insert(
                normalized_api_format.to_string(),
                offset.saturating_add(page_scanned),
            );
            self.charge_scan_budget(normalized_api_format, page_scanned);
            if scan_ended || self.scan_budget(normalized_api_format).is_exhausted() {
                self.exhausted_api_formats
                    .insert(normalized_api_format.to_string());
            }
            if page_scanned == 0 {
                return Ok(None);
            }

            let rows = page
                .rows
                .into_iter()
                .take(page_scanned as usize)
                .filter(|row| {
                    row_supports_requested_model_with_model_directives_and_request_operation(
                        row,
                        &routing_model,
                        normalized_api_format,
                        false,
                        self.request_operation.as_deref(),
                    )
                })
                .collect::<Vec<_>>();
            if let Some(outcome) = self
                .build_page_outcome_from_rows(candidate_api_format, normalized_api_format, rows)
                .await?
            {
                return Ok(Some(outcome));
            }
            if self.exhausted_api_formats.contains(normalized_api_format) {
                return Ok(None);
            }
        }
    }

    fn api_format_is_exhausted(&self, candidate_api_format: &str) -> bool {
        let normalized_api_format = normalize_api_format(candidate_api_format);
        normalized_api_format.is_empty()
            || self.exhausted_api_formats.contains(&normalized_api_format)
    }

    /// Global model names are a reserved routing namespace, so a request that
    /// names one must not be answered by a provider whose own model merely
    /// carries that name as an upstream alias. Cached per routing model: the
    /// answer does not change between pages or API formats.
    async fn reserved_global_model_name(
        &mut self,
        rows: &[StoredMinimalCandidateSelectionRow],
        routing_model: &str,
    ) -> Result<Option<String>, GatewayError> {
        if let Some(cached) = self.reserved_global_model_names.get(routing_model) {
            return Ok(cached.clone());
        }
        let state = self.state;
        let reserved_global_model_name =
            crate::data::candidate_selection::resolve_reserved_global_model_name(
                state.app().data.as_ref(),
                rows,
                routing_model,
            )
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))?;
        self.reserved_global_model_names.insert(
            routing_model.to_string(),
            reserved_global_model_name.clone(),
        );
        Ok(reserved_global_model_name)
    }

    async fn build_page_outcome_from_rows(
        &mut self,
        candidate_api_format: &str,
        normalized_api_format: &str,
        rows: Vec<StoredMinimalCandidateSelectionRow>,
    ) -> Result<
        Option<
            AiCandidatePreselectionOutcome<
                SchedulerMinimalCandidateSelectionCandidate,
                SkippedLocalExecutionCandidate,
            >,
        >,
        GatewayError,
    > {
        let mut rows = rows
            .into_iter()
            .filter(|row| {
                self.seen_candidate_keys.insert(format!(
                    "{}:{}:{}:{}",
                    row.endpoint_id, row.key_id, row.model_id, row.endpoint_api_format
                ))
            })
            .collect::<Vec<_>>();
        if rows.is_empty() {
            return Ok(None);
        }
        let routing_model = self.routing_model(candidate_api_format).to_string();
        let resolved_global_model_name =
            if let Some(value) = self.resolved_global_model_names.get(normalized_api_format) {
                value.clone()
            } else {
                let reserved_global_model_name = self
                    .reserved_global_model_name(&rows, &routing_model)
                    .await?;
                let Some(value) = resolve_requested_global_model_name_with_reserved_global_model(
                    &rows,
                    &routing_model,
                    normalized_api_format,
                    false,
                    self.request_operation.as_deref(),
                    reserved_global_model_name.as_deref(),
                ) else {
                    return Ok(None);
                };
                self.resolved_global_model_names
                    .insert(normalized_api_format.to_string(), value.clone());
                value
            };
        rows.retain(|row| row.global_model_name == resolved_global_model_name);
        if rows.is_empty() {
            return Ok(None);
        }

        let auth_constraints = matches_client_api_format(
            self.use_api_format_alias_match,
            candidate_api_format,
            &self.client_api_format,
        )
        .then_some(&self.auth_snapshot)
        .map(crate::data::candidate_selection::auth_snapshot_constraints);
        let enumerated_candidates = enumerate_minimal_candidate_selection_with_model_directives(
            EnumerateMinimalCandidateSelectionInput {
                rows,
                normalized_api_format,
                request_operation: self.request_operation.as_deref(),
                requested_model_name: &routing_model,
                resolved_global_model_name: resolved_global_model_name.as_str(),
                require_streaming: self.require_streaming,
                required_capabilities: self.required_capabilities.as_ref(),
                auth_constraints: auth_constraints.as_ref(),
            },
            false,
        )
        .map_err(|err| GatewayError::Internal(err.to_string()))?;
        let mut candidates = Vec::new();
        for candidate in enumerated_candidates {
            if !self.candidate_allowed_for_page(&candidate, candidate_api_format) {
                continue;
            }
            if !self
                .seen_candidate_keys
                .insert(local_candidate_preselection_key(&candidate, self.key_mode))
            {
                continue;
            }
            candidates.push(candidate);
        }

        let matches_client_format = matches_client_api_format(
            self.use_api_format_alias_match,
            candidate_api_format,
            &self.client_api_format,
        );
        let auth_snapshot = matches_client_format.then_some(&self.auth_snapshot);
        let (candidates, skipped_candidates) = self
            .state
            .list_selectable_enumerated_candidates_with_skip_reasons(
                candidate_api_format,
                &resolved_global_model_name,
                candidates,
                self.required_capabilities.as_ref(),
                auth_snapshot,
                self.routing_policy
                    .is_none()
                    .then_some(self.client_session_affinity.as_ref())
                    .flatten(),
                self.ranking_seed,
                self.ordering_config,
            )
            .await?;
        let skipped_candidates = skipped_candidates
            .into_iter()
            .map(skipped_local_execution_candidate_from_scheduler_skip)
            .filter(|skipped_candidate| {
                self.skipped_candidate_allowed_for_page(skipped_candidate, candidate_api_format)
            })
            .collect::<Vec<_>>();

        Ok(Some(AiCandidatePreselectionOutcome {
            candidates,
            skipped_candidates,
        }))
    }

    fn candidate_allowed_for_page(
        &self,
        candidate: &SchedulerMinimalCandidateSelectionCandidate,
        candidate_api_format: &str,
    ) -> bool {
        routing_policy_allows_provider(self.routing_policy.as_ref(), candidate)
            && (matches_client_api_format(
                self.use_api_format_alias_match,
                candidate_api_format,
                &self.client_api_format,
            ) || auth_snapshot_allows_cross_format_candidate(
                &self.auth_snapshot,
                &self.requested_model,
                self.model_directive_base_model(candidate_api_format),
                candidate,
            ))
    }

    fn skipped_candidate_allowed_for_page(
        &self,
        skipped_candidate: &SkippedLocalExecutionCandidate,
        candidate_api_format: &str,
    ) -> bool {
        routing_policy_allows_provider(self.routing_policy.as_ref(), &skipped_candidate.candidate)
            && (matches_client_api_format(
                self.use_api_format_alias_match,
                candidate_api_format,
                &self.client_api_format,
            ) || auth_snapshot_allows_cross_format_candidate(
                &self.auth_snapshot,
                &self.requested_model,
                self.model_directive_base_model(candidate_api_format),
                &skipped_candidate.candidate,
            ))
    }
}

fn skipped_local_execution_candidate_from_scheduler_skip(
    skipped_candidate: SchedulerSkippedCandidate,
) -> SkippedLocalExecutionCandidate {
    SkippedLocalExecutionCandidate {
        candidate: skipped_candidate.candidate,
        skip_reason: skipped_candidate.skip_reason,
        transport: None,
        ranking: None,
        extra_data: None,
    }
}

fn local_candidate_preselection_key(
    candidate: &SchedulerMinimalCandidateSelectionCandidate,
    mode: LocalCandidatePreselectionKeyMode,
) -> String {
    match mode {
        LocalCandidatePreselectionKeyMode::ProviderEndpointKeyModel => format!(
            "{}:{}:{}:{}:{}",
            candidate.provider_id,
            candidate.endpoint_id,
            candidate.key_id,
            candidate.model_id,
            candidate.selected_provider_model_name,
        ),
        LocalCandidatePreselectionKeyMode::ProviderEndpointKeyModelAndApiFormat => format!(
            "{}:{}:{}:{}:{}:{}",
            candidate.provider_id,
            candidate.endpoint_id,
            candidate.key_id,
            candidate.model_id,
            candidate.selected_provider_model_name,
            candidate.endpoint_api_format,
        ),
    }
}

async fn acquire_candidate_planning_gate(
    state: PlannerAppState<'_>,
    trace_id: &str,
) -> Result<Option<ConcurrencyPermit>, GatewayError> {
    let Some(gate) = state.app().candidate_planning_gate.as_ref() else {
        return Ok(None);
    };
    let budget = state
        .app()
        .frontdoor_runtime_guards
        .internal_gate_queue_budget;
    let gate_wait_started_at = std::time::Instant::now();
    match tokio::time::timeout(budget, gate.acquire()).await {
        Ok(Ok(permit)) => {
            observe_gateway_stage_ms(
                "candidate_planning_gate_wait",
                gate_wait_started_at.elapsed().as_millis() as u64,
            );
            Ok(Some(permit))
        }
        Ok(Err(err)) => Err(GatewayError::Internal(err.to_string())),
        Err(_) => Err(GatewayError::AdmissionTimeout {
            trace_id: trace_id.to_string(),
            gate: "gateway_candidate_planning",
            queue_budget_ms: budget.as_millis() as u64,
        }),
    }
}

fn matches_client_api_format(
    use_api_format_alias_match: bool,
    candidate_api_format: &str,
    client_api_format: &str,
) -> bool {
    if use_api_format_alias_match {
        crate::ai_serving::api_format_alias_matches(candidate_api_format, client_api_format)
    } else {
        candidate_api_format == client_api_format
    }
}

pub(crate) fn auth_snapshot_allows_cross_format_candidate(
    auth_snapshot: &GatewayAuthApiKeySnapshot,
    requested_model: &str,
    requested_base_model: Option<&str>,
    candidate: &SchedulerMinimalCandidateSelectionCandidate,
) -> bool {
    if let Some(allowed_providers) = auth_snapshot.effective_allowed_providers() {
        let provider_allowed = allowed_providers.iter().any(|value| {
            aether_scheduler_core::provider_matches_allowed_value(
                value,
                &candidate.provider_id,
                &candidate.provider_name,
                &candidate.provider_type,
            )
        });
        if !provider_allowed {
            return false;
        }
    }

    if let Some(allowed_models) = auth_snapshot.effective_allowed_models() {
        let model_allowed = allowed_models.iter().any(|value| {
            value == requested_model
                || value == &candidate.global_model_name
                || requested_base_model.is_some_and(|base_model| value == base_model)
        });
        if !model_allowed {
            return false;
        }
    }

    true
}

fn routing_policy_allows_provider(
    routing_policy: Option<&ResolvedRoutingPolicy>,
    candidate: &SchedulerMinimalCandidateSelectionCandidate,
) -> bool {
    match routing_policy {
        Some(policy) => policy
            .ranking_overlay
            .provider_allowed(candidate.provider_id.as_str()),
        None => true,
    }
}

#[cfg(test)]
#[path = "candidate_source_tests.rs"]
mod tests;
