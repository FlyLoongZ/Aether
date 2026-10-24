use std::collections::{btree_map::Entry, BTreeMap, BTreeSet, VecDeque};
use std::sync::{
    atomic::{AtomicU64, Ordering as AtomicOrdering},
    Arc, LazyLock,
};

use aether_admin::provider::{
    pool as admin_provider_pool_pure, status as admin_provider_status_pure,
};
use aether_data_contracts::repository::candidate_selection::{
    StoredMinimalCandidateSelectionRow, StoredPoolKeyCandidateOrder,
    StoredPoolKeyCandidateRowsByKeyIdsQuery, StoredPoolKeyCandidateRowsQuery,
};
use aether_data_contracts::repository::pool_scores::{
    ListRankedPoolMembersQuery, PoolMemberHardState, PoolMemberIdentity,
    PoolMemberScheduleFeedback, PoolScoreScope, StoredPoolMemberScore, POOL_KIND_PROVIDER_KEY_POOL,
};
use aether_data_contracts::repository::provider_catalog::StoredProviderCatalogKey;
use aether_pool_core::{
    run_pool_scheduler, PoolCandidateFacts, PoolCandidateInput, PoolCandidateOrchestration,
    PoolMemberSignals, PoolRuntimeState, PoolSchedulingConfig, PoolSchedulingPreset,
    POOL_ACCOUNT_BLOCKED_SKIP_REASON, POOL_ACCOUNT_EXHAUSTED_SKIP_REASON,
    POOL_COOLDOWN_SKIP_REASON, POOL_COST_LIMIT_REACHED_SKIP_REASON,
};
use aether_provider_pool::ProviderPoolService;
use aether_routing_core::{RankingOverlay, ResolvedRoutingPolicy};
use tokio::sync::Semaphore;
use tracing::{debug, warn};

use crate::ai_serving::{
    candidate_auth_channel_skip_reason, candidate_common_transport_skip_reason,
    provider_key_pool_score_scope, read_candidate_transport_snapshot,
    record_local_runtime_candidate_skip_reason, CandidateTransportPolicyFacts,
    EligibleLocalExecutionCandidate, LocalExecutionCandidateKind, PlannerAppState,
    SkippedLocalExecutionCandidate,
};
use crate::clock::current_unix_ms;
use crate::handlers::shared::provider_pool::{
    admin_provider_pool_cache_affinity_enabled, admin_provider_pool_config_from_config_value,
};
use crate::handlers::shared::provider_pool::{
    admin_provider_pool_quota_probe_active_members_key,
    read_admin_provider_pool_key_cooldown_reason, AdminProviderPoolConfig,
    AdminProviderPoolRuntimeState, AdminProviderPoolSchedulingPreset,
};
use crate::handlers::shared::provider_pool::{
    read_provider_pool_scheduling_runtime_state, read_provider_pool_sticky_bound_key_id,
};
use crate::handlers::shared::{parse_catalog_auth_config_json, provider_key_health_summary};
use crate::maintenance::spawn_pool_quota_probe_replenish_for_request;
use crate::orchestration::LocalExecutionCandidateMetadata;
use crate::stage_metrics::observe_gateway_stage_ms;

static LOAD_BALANCE_SEQUENCE: AtomicU64 = AtomicU64::new(0);
static POOL_SCORE_SCHEDULE_INTEREST_SEMAPHORE: LazyLock<Arc<Semaphore>> =
    LazyLock::new(|| Arc::new(Semaphore::new(POOL_SCORE_SCHEDULE_INTEREST_CONCURRENCY)));
const POOL_ACTIVE_PROBE_SEALED_SKIP_REASON: &str = "pool_active_probe_sealed";
const ROUTING_PROFILE_DISALLOWED_KEY_SKIP_REASON: &str = "routing_profile_disallowed_key";
const POOL_SCORE_SCHEDULE_INTEREST_CONCURRENCY: usize = 4;
const POOL_SCORE_SCHEDULE_INTEREST_MAX_PER_BATCH: usize = 16;
const POOL_SCORE_SCHEDULE_INTEREST_MIN_INTERVAL_SECS: u64 = 60;

type PoolCatalogKeyContext = PoolMemberSignals;

#[cfg(test)]
pub(crate) async fn apply_local_execution_pool_scheduler(
    state: PlannerAppState<'_>,
    candidates: Vec<EligibleLocalExecutionCandidate>,
    sticky_session_token: Option<&str>,
    requested_model: Option<&str>,
    request_auth_channel: Option<&str>,
) -> (
    Vec<EligibleLocalExecutionCandidate>,
    Vec<SkippedLocalExecutionCandidate>,
) {
    if candidates.is_empty() {
        return (Vec::new(), Vec::new());
    }

    let sticky_session_token = sticky_session_token
        .map(str::trim)
        .filter(|value| !value.is_empty());

    let mut scheduled = Vec::new();
    let mut skipped = Vec::new();
    for candidate in candidates {
        if candidate.kind == LocalExecutionCandidateKind::PoolGroup {
            let mut expanded = expand_pool_group_candidate(
                state,
                candidate,
                sticky_session_token,
                requested_model,
                request_auth_channel,
            )
            .await;
            scheduled.append(&mut expanded.0);
            skipped.append(&mut expanded.1);
        } else {
            scheduled.push(candidate);
        }
    }

    (scheduled, skipped)
}

async fn schedule_pool_page_candidates(
    state: PlannerAppState<'_>,
    candidates: Vec<EligibleLocalExecutionCandidate>,
    sticky_session_token: Option<&str>,
    effective_pool_config: Option<&AdminProviderPoolConfig>,
    provider_model_name: Option<&str>,
) -> (
    Vec<EligibleLocalExecutionCandidate>,
    Vec<SkippedLocalExecutionCandidate>,
) {
    if candidates.is_empty() {
        return (Vec::new(), Vec::new());
    }

    let mut provider_runtime_requirements =
        BTreeMap::<String, (AdminProviderPoolConfig, BTreeSet<String>)>::new();
    for candidate in &candidates {
        let Some(pool_config) = effective_pool_config
            .cloned()
            .or_else(|| pool_config_for_candidate(candidate))
        else {
            continue;
        };
        let entry = provider_runtime_requirements
            .entry(candidate.candidate.provider_id.clone())
            .or_insert_with(|| (pool_config.clone(), BTreeSet::new()));
        entry.1.insert(candidate.candidate.key_id.clone());
    }

    let key_context_by_id = read_pool_catalog_key_contexts_by_id(
        state,
        &candidates,
        provider_model_name,
        effective_pool_config,
    )
    .await;

    let mut runtime_by_provider = BTreeMap::new();
    let mut pool_config_by_provider = BTreeMap::new();
    let mut burst_provider_ids = BTreeSet::<String>::new();
    for (provider_id, (pool_config, key_ids)) in provider_runtime_requirements {
        let key_ids = key_ids.into_iter().collect::<Vec<_>>();
        let runtime = if key_ids.is_empty() {
            AdminProviderPoolRuntimeState::default()
        } else {
            let runtime_started_at = std::time::Instant::now();
            let runtime = read_provider_pool_scheduling_runtime_state(
                state.app().runtime_state.as_ref(),
                provider_id.as_str(),
                &key_ids,
                &pool_config,
                sticky_session_token,
            )
            .await;
            observe_gateway_stage_ms(
                "pool_runtime_state",
                runtime_started_at.elapsed().as_millis() as u64,
            );
            runtime
        };
        pool_config_by_provider.insert(provider_id.clone(), pool_config);
        runtime_by_provider.insert(provider_id, runtime);
    }

    let preflight_evictions = prune_unschedulable_active_probe_members_for_request(
        &mut runtime_by_provider,
        &candidates,
        &key_context_by_id,
    );
    spawn_active_probe_member_evictions_for_request(state, &preflight_evictions);
    burst_provider_ids.extend(preflight_evictions.keys().cloned());

    for (provider_id, pool_config) in &pool_config_by_provider {
        let Some(runtime) = runtime_by_provider.get(provider_id) else {
            continue;
        };
        if should_trigger_active_probe_burst_for_request(pool_config, runtime) {
            burst_provider_ids.insert(provider_id.clone());
        }
    }

    let effective_pool_config_by_provider = effective_pool_config
        .map(|config| {
            BTreeMap::from([(candidates[0].candidate.provider_id.clone(), config.clone())])
        })
        .unwrap_or_default();
    let outcome = apply_local_execution_pool_scheduler_with_runtime_map_outcome_and_configs(
        candidates,
        &runtime_by_provider,
        &key_context_by_id,
        &effective_pool_config_by_provider,
    );
    let scheduled = outcome.candidates;
    let skipped = outcome.skipped;
    burst_provider_ids.extend(outcome.active_probe_seal_fallback_provider_ids);
    spawn_active_probe_member_evictions_for_request(
        state,
        &outcome.active_probe_evicted_members_by_provider,
    );
    burst_provider_ids.extend(
        outcome
            .active_probe_evicted_members_by_provider
            .keys()
            .cloned(),
    );

    for skipped_candidate in &skipped {
        if skipped_candidate.skip_reason == POOL_ACTIVE_PROBE_SEALED_SKIP_REASON {
            burst_provider_ids.insert(skipped_candidate.candidate.provider_id.clone());
        }
    }

    for provider_id in burst_provider_ids {
        let _ = spawn_pool_quota_probe_replenish_for_request(state.app().clone(), provider_id);
    }

    (scheduled, skipped)
}

#[cfg(test)]
async fn remove_active_probe_members_for_request(
    state: PlannerAppState<'_>,
    evicted_members_by_provider: &BTreeMap<String, BTreeSet<String>>,
) {
    remove_active_probe_members(state.app().clone(), evicted_members_by_provider).await;
}

fn spawn_active_probe_member_evictions_for_request(
    state: PlannerAppState<'_>,
    evicted_members_by_provider: &BTreeMap<String, BTreeSet<String>>,
) {
    if evicted_members_by_provider.is_empty() {
        return;
    }
    let app = state.app().clone();
    let evicted_members_by_provider = evicted_members_by_provider.clone();
    tokio::spawn(async move {
        remove_active_probe_members(app, &evicted_members_by_provider).await;
    });
}

async fn remove_active_probe_members(
    app: crate::AppState,
    evicted_members_by_provider: &BTreeMap<String, BTreeSet<String>>,
) {
    for (provider_id, key_ids) in evicted_members_by_provider {
        let set_key = admin_provider_pool_quota_probe_active_members_key(provider_id);
        for key_id in key_ids {
            if let Err(err) = app
                .runtime_state
                .as_ref()
                .set_remove(&set_key, key_id)
                .await
            {
                warn!(
                    event_name = "pool_active_probe_member_evict_failed",
                    log_type = "event",
                    provider_id,
                    key_id,
                    error = ?err,
                    "gateway pool scheduler failed to evict unschedulable active probe member"
                );
            }
        }
    }
}

fn prune_unschedulable_active_probe_members_for_request(
    runtime_by_provider: &mut BTreeMap<String, AdminProviderPoolRuntimeState>,
    candidates: &[EligibleLocalExecutionCandidate],
    key_context_by_id: &BTreeMap<String, PoolCatalogKeyContext>,
) -> BTreeMap<String, BTreeSet<String>> {
    let mut evicted = BTreeMap::<String, BTreeSet<String>>::new();
    for candidate in candidates {
        let Some(pool_config) = pool_config_for_candidate(candidate) else {
            continue;
        };
        if !should_enforce_active_probe_sealed_pool(&pool_config) {
            continue;
        }
        let provider_id = candidate.candidate.provider_id.as_str();
        let key_id = candidate.candidate.key_id.as_str();
        let Some(runtime) = runtime_by_provider.get_mut(provider_id) else {
            continue;
        };
        if !runtime.active_probe_member_ids.contains(key_id) {
            continue;
        }
        if !active_probe_member_is_unschedulable_for_request(
            &pool_config,
            runtime,
            key_id,
            key_context_by_id.get(key_id),
        ) {
            continue;
        }
        runtime.active_probe_member_ids.remove(key_id);
        evicted
            .entry(provider_id.to_string())
            .or_default()
            .insert(key_id.to_string());
    }
    evicted
}

fn active_probe_member_is_unschedulable_for_request(
    pool_config: &AdminProviderPoolConfig,
    runtime: &AdminProviderPoolRuntimeState,
    key_id: &str,
    key_context: Option<&PoolCatalogKeyContext>,
) -> bool {
    if runtime.cooldown_reason_by_key.contains_key(key_id) {
        return true;
    }
    if pool_config.cost_limit_per_key_tokens.is_some_and(|limit| {
        runtime
            .cost_window_usage_by_key
            .get(key_id)
            .copied()
            .unwrap_or(0)
            >= limit
    }) {
        return true;
    }
    key_context.is_some_and(|context| {
        context.account_blocked
            || (pool_config.skip_exhausted_accounts && context.quota_exhausted)
            || context.quota_hard_blocked
    })
}

#[cfg(test)]
async fn expand_pool_group_candidate(
    state: PlannerAppState<'_>,
    group: EligibleLocalExecutionCandidate,
    sticky_session_token: Option<&str>,
    requested_model: Option<&str>,
    request_auth_channel: Option<&str>,
) -> (
    Vec<EligibleLocalExecutionCandidate>,
    Vec<SkippedLocalExecutionCandidate>,
) {
    let mut cursor = PoolKeyCursor::new(
        state,
        group,
        sticky_session_token,
        requested_model,
        request_auth_channel,
    );
    let mut scheduled = Vec::new();
    let mut skipped = Vec::new();

    while let Some(candidate) = cursor.next_key().await {
        scheduled.push(candidate);
    }
    skipped.append(&mut cursor.take_skipped_candidates());

    if scheduled.is_empty() {
        cursor.log_exhausted();
    }
    (scheduled, skipped)
}

pub(crate) struct PoolKeyCursor<'a> {
    state: PlannerAppState<'a>,
    group: EligibleLocalExecutionCandidate,
    sticky_session_token: Option<String>,
    requested_model: Option<String>,
    request_auth_channel: Option<String>,
    routing_overlay: Option<RankingOverlay>,
    routing_allowed_key_ids: Option<Vec<String>>,
    effective_pool_config: Option<AdminProviderPoolConfig>,
    runtime_miss_trace_id: Option<String>,
    record_runtime_miss_diagnostic: bool,
    pool_key_order: StoredPoolKeyCandidateOrder,
    next_offset: u32,
    scanned_keys: u32,
    budget_scanned_keys: u32,
    window_size: u32,
    page_size: u32,
    max_scanned_keys: u32,
    absolute_max_scanned_keys: u32,
    score_top_n: u32,
    score_next_offset: u32,
    score_phase_exhausted: bool,
    score_schedule_interest_count: usize,
    routing_allowed_key_offset: usize,
    routing_allowed_rows: Option<VecDeque<StoredMinimalCandidateSelectionRow>>,
    skip_reason_counts: BTreeMap<&'static str, u32>,
    next_pool_key_index: u32,
    sticky_candidate_loaded: bool,
    seen_key_ids: BTreeSet<String>,
    queued_candidates: VecDeque<EligibleLocalExecutionCandidate>,
    skipped_candidates: Vec<SkippedLocalExecutionCandidate>,
    exhausted_logged: bool,
    returned_key_count: u32,
    exhaustion_skip_recorded: bool,
}

impl<'a> PoolKeyCursor<'a> {
    pub(crate) fn provider_id(&self) -> &str {
        self.group.candidate.provider_id.as_str()
    }

    pub(crate) fn endpoint_id(&self) -> &str {
        self.group.candidate.endpoint_id.as_str()
    }

    #[cfg(test)]
    pub(crate) fn new(
        state: PlannerAppState<'a>,
        group: EligibleLocalExecutionCandidate,
        sticky_session_token: Option<&str>,
        requested_model: Option<&str>,
        request_auth_channel: Option<&str>,
    ) -> Self {
        Self::new_with_routing_policy(
            state,
            group,
            sticky_session_token,
            requested_model,
            request_auth_channel,
            None,
        )
    }

    pub(crate) fn new_with_routing_policy(
        state: PlannerAppState<'a>,
        group: EligibleLocalExecutionCandidate,
        sticky_session_token: Option<&str>,
        requested_model: Option<&str>,
        request_auth_channel: Option<&str>,
        routing_policy: Option<&ResolvedRoutingPolicy>,
    ) -> Self {
        let effective_pool_config = effective_pool_config_for_group(&group, routing_policy);
        let pool_key_order =
            pool_key_candidate_order_for_group(&group, effective_pool_config.as_ref());
        let routing_overlay = routing_policy.map(|policy| policy.ranking_overlay.clone());
        let routing_allowed_key_ids = routing_policy
            .map(|policy| &policy.ranking_overlay.allowed_keys)
            .filter(|key_ids| !key_ids.is_empty())
            .map(|key_ids| {
                let mut seen = BTreeSet::new();
                key_ids
                    .iter()
                    .filter(|key_id| seen.insert((*key_id).clone()))
                    .cloned()
                    .collect::<Vec<_>>()
            });
        let score_top_n = effective_pool_config
            .as_ref()
            .map(|config| config.score_top_n)
            .unwrap_or(u64::from(aether_dispatch_core::DEFAULT_POOL_PAGE_SIZE))
            .clamp(1, u64::from(u32::MAX)) as u32;
        let configured_max_scanned_keys = effective_pool_config
            .as_ref()
            .map(|config| config.score_fallback_scan_limit)
            .unwrap_or(u64::from(aether_dispatch_core::DEFAULT_POOL_MAX_SCAN))
            .clamp(1, u64::from(u32::MAX)) as u32;
        let window_config = crate::dispatch::pool::default_pool_window_config().normalized();
        let max_scanned_keys = configured_max_scanned_keys.min(window_config.max_scan);
        let absolute_max_scanned_keys = configured_max_scanned_keys.max(max_scanned_keys);
        Self {
            state,
            group,
            sticky_session_token: sticky_session_token.map(str::to_string),
            requested_model: requested_model.map(str::to_string),
            request_auth_channel: request_auth_channel.map(str::to_string),
            routing_overlay,
            routing_allowed_key_ids,
            effective_pool_config,
            runtime_miss_trace_id: None,
            record_runtime_miss_diagnostic: false,
            pool_key_order,
            next_offset: 0,
            scanned_keys: 0,
            budget_scanned_keys: 0,
            window_size: window_config.window_size,
            page_size: window_config.page_size,
            max_scanned_keys: max_scanned_keys.max(window_config.window_size),
            absolute_max_scanned_keys: absolute_max_scanned_keys.max(window_config.window_size),
            score_top_n,
            score_next_offset: 0,
            score_phase_exhausted: false,
            score_schedule_interest_count: 0,
            routing_allowed_key_offset: 0,
            routing_allowed_rows: None,
            skip_reason_counts: BTreeMap::new(),
            next_pool_key_index: 0,
            sticky_candidate_loaded: false,
            seen_key_ids: BTreeSet::new(),
            queued_candidates: VecDeque::new(),
            skipped_candidates: Vec::new(),
            exhausted_logged: false,
            returned_key_count: 0,
            exhaustion_skip_recorded: false,
        }
    }

    pub(crate) fn with_runtime_miss_diagnostic(
        mut self,
        trace_id: &str,
        record_runtime_miss_diagnostic: bool,
    ) -> Self {
        if record_runtime_miss_diagnostic {
            self.runtime_miss_trace_id = Some(trace_id.to_string());
            self.record_runtime_miss_diagnostic = true;
        }
        self
    }

    pub(crate) async fn next_key(&mut self) -> Option<EligibleLocalExecutionCandidate> {
        let started_at = std::time::Instant::now();
        loop {
            if let Some(candidate) = self.next_queued_candidate().await {
                self.returned_key_count = self.returned_key_count.saturating_add(1);
                observe_gateway_stage_ms(
                    "pool_cursor_next_key",
                    started_at.elapsed().as_millis() as u64,
                );
                return Some(candidate);
            }

            if !self.sticky_candidate_loaded {
                self.sticky_candidate_loaded = true;
                if let Some(candidate) = self.sticky_candidate().await {
                    self.queued_candidates.push_back(candidate);
                    continue;
                }
            }

            if !self.refill_queued_candidates().await {
                observe_gateway_stage_ms(
                    "pool_cursor_next_key",
                    started_at.elapsed().as_millis() as u64,
                );
                return None;
            }
        }
    }

    pub(crate) fn take_skipped_candidates(&mut self) -> Vec<SkippedLocalExecutionCandidate> {
        std::mem::take(&mut self.skipped_candidates)
    }

    pub(crate) fn exhausted_group_skipped_candidate(
        &self,
    ) -> Option<SkippedLocalExecutionCandidate> {
        if self.returned_key_count > 0 {
            return None;
        }

        let skip_reason_counts = self
            .skip_reason_counts
            .iter()
            .map(|(reason, count)| ((*reason).to_string(), serde_json::json!(count)))
            .collect::<serde_json::Map<String, serde_json::Value>>();
        Some(SkippedLocalExecutionCandidate {
            candidate: self.group.candidate.clone(),
            skip_reason: self.runtime_miss_pool_exhaustion_skip_reason(),
            transport: Some(self.group.transport.clone()),
            ranking: self.group.ranking.clone(),
            extra_data: Some(serde_json::json!({
                "pool_group_exhaustion": {
                    "scanned_keys": self.scanned_keys,
                    "budget_scanned_keys": self.budget_scanned_keys,
                    "skip_reason_counts": skip_reason_counts,
                }
            })),
        })
    }

    pub(crate) fn log_exhausted(&mut self) {
        if self.exhausted_logged {
            return;
        }
        self.exhausted_logged = true;
        warn!(
            event_name = "pool_group_exhausted",
            log_type = "event",
            provider_id = %self.group.candidate.provider_id,
            endpoint_id = %self.group.candidate.endpoint_id,
            model_id = %self.group.candidate.model_id,
            scanned_keys = self.scanned_keys,
            budget_scanned_keys = self.budget_scanned_keys,
            max_scanned_keys = self.max_scanned_keys,
            absolute_max_scanned_keys = self.absolute_max_scanned_keys,
            skip_reason_counts = ?self.skip_reason_counts,
            "gateway pool scheduler exhausted pool group without a schedulable key"
        );
        self.record_runtime_miss_pool_exhaustion_skip_reason();
    }

    fn record_runtime_miss_pool_exhaustion_skip_reason(&mut self) {
        if self.exhaustion_skip_recorded
            || !self.record_runtime_miss_diagnostic
            || self.returned_key_count > 0
        {
            return;
        }
        let Some(trace_id) = self.runtime_miss_trace_id.as_deref() else {
            return;
        };
        self.exhaustion_skip_recorded = true;
        if self.skip_reason_counts.is_empty() {
            record_local_runtime_candidate_skip_reason(
                self.state.app(),
                trace_id,
                "pool_group_exhausted",
            );
            return;
        }
        for reason in self.skip_reason_counts.keys() {
            record_local_runtime_candidate_skip_reason(self.state.app(), trace_id, reason);
        }
    }

    fn runtime_miss_pool_exhaustion_skip_reason(&self) -> &'static str {
        let mut selected_reason = "pool_group_exhausted";
        let mut selected_count = 0;
        for (reason, count) in &self.skip_reason_counts {
            if *count > selected_count {
                selected_reason = *reason;
                selected_count = *count;
            }
        }
        selected_reason
    }

    async fn next_page_candidates(&mut self) -> Option<Vec<EligibleLocalExecutionCandidate>> {
        if self.routing_allowed_key_ids.is_some() {
            return self.next_routing_allowed_candidates().await;
        }

        if !self.score_phase_exhausted {
            if let Some(score_candidates) = self.next_score_candidates().await {
                if !score_candidates.is_empty() {
                    return Some(score_candidates);
                }
            }
        }

        if self.budget_scanned_keys >= self.max_scanned_keys
            || self.scanned_keys >= self.absolute_max_scanned_keys
        {
            return None;
        }

        let limit = self
            .page_size
            .min(self.max_scanned_keys - self.budget_scanned_keys)
            .min(self.absolute_max_scanned_keys - self.scanned_keys);
        let query = StoredPoolKeyCandidateRowsQuery {
            api_format: self.group.candidate.endpoint_api_format.clone(),
            provider_id: self.group.candidate.provider_id.clone(),
            endpoint_id: self.group.candidate.endpoint_id.clone(),
            model_id: self.group.candidate.model_id.clone(),
            selected_provider_model_name: self.group.candidate.selected_provider_model_name.clone(),
            order: self.pool_key_order.clone(),
            offset: self.next_offset,
            limit,
        };
        let rows = match self
            .state
            .app()
            .list_pool_key_candidate_rows_for_group(&query)
            .await
        {
            Ok(rows) => rows,
            Err(err) => {
                warn!(
                    event_name = "pool_group_key_page_load_failed",
                    log_type = "event",
                    provider_id = %self.group.candidate.provider_id,
                    endpoint_id = %self.group.candidate.endpoint_id,
                    model_id = %self.group.candidate.model_id,
                    selected_provider_model_name = %self.group.candidate.selected_provider_model_name,
                    offset = self.next_offset,
                    limit,
                    error = ?err,
                    "gateway pool scheduler failed to read pool key page"
                );
                return None;
            }
        };
        if rows.is_empty() {
            return None;
        }

        let row_count = u32::try_from(rows.len()).unwrap_or(u32::MAX);
        self.next_offset = self.next_offset.saturating_add(row_count);
        let seen_count_before = self.seen_key_ids.len();
        let candidates = self.build_page_eligible_candidates(rows).await;
        let distinct_row_count = self.seen_key_ids.len().saturating_sub(seen_count_before);
        self.scanned_keys = self.scanned_keys.saturating_add(row_count);
        self.budget_scanned_keys = self
            .budget_scanned_keys
            .saturating_add(u32::try_from(distinct_row_count).unwrap_or(u32::MAX));
        Some(candidates)
    }

    async fn next_routing_allowed_candidates(
        &mut self,
    ) -> Option<Vec<EligibleLocalExecutionCandidate>> {
        loop {
            if self.budget_scanned_keys >= self.max_scanned_keys
                || self.scanned_keys >= self.absolute_max_scanned_keys
            {
                return None;
            }

            let key_ids = self.routing_allowed_key_ids.as_ref()?;
            let has_buffered_rows = self
                .routing_allowed_rows
                .as_ref()
                .is_some_and(|rows| !rows.is_empty());
            if !has_buffered_rows && self.routing_allowed_key_offset >= key_ids.len() {
                return None;
            }
            let limit = self
                .page_size
                .min(self.max_scanned_keys - self.budget_scanned_keys)
                .min(self.absolute_max_scanned_keys - self.scanned_keys)
                as usize;
            if matches!(
                self.pool_key_order,
                StoredPoolKeyCandidateOrder::InternalPriority
            ) && self.routing_allowed_rows.is_none()
            {
                let query = StoredPoolKeyCandidateRowsByKeyIdsQuery {
                    api_format: self.group.candidate.endpoint_api_format.clone(),
                    provider_id: self.group.candidate.provider_id.clone(),
                    endpoint_id: self.group.candidate.endpoint_id.clone(),
                    model_id: self.group.candidate.model_id.clone(),
                    selected_provider_model_name: self
                        .group
                        .candidate
                        .selected_provider_model_name
                        .clone(),
                    key_ids: key_ids.clone(),
                };
                let mut rows = match self
                    .state
                    .app()
                    .list_pool_key_candidate_rows_for_group_key_ids(&query)
                    .await
                {
                    Ok(rows) => rows,
                    Err(err) => {
                        warn!(
                            event_name = "pool_group_routing_key_load_failed",
                            log_type = "event",
                            provider_id = %self.group.candidate.provider_id,
                            endpoint_id = %self.group.candidate.endpoint_id,
                            model_id = %self.group.candidate.model_id,
                            allowed_key_count = query.key_ids.len(),
                            error = ?err,
                            "gateway pool scheduler failed to materialize routing-allowed pool keys"
                        );
                        return None;
                    }
                };
                let api_format = self.group.candidate.endpoint_api_format.as_str();
                rows.sort_by(|left, right| {
                    let left_priority = self.routing_overlay.as_ref().map_or(
                        left.key_internal_priority,
                        |overlay| {
                            overlay.key_priority_for_format(
                                &left.key_id,
                                api_format,
                                left.key_internal_priority,
                            )
                        },
                    );
                    let right_priority = self.routing_overlay.as_ref().map_or(
                        right.key_internal_priority,
                        |overlay| {
                            overlay.key_priority_for_format(
                                &right.key_id,
                                api_format,
                                right.key_internal_priority,
                            )
                        },
                    );
                    left_priority
                        .cmp(&right_priority)
                        .then(left.key_id.cmp(&right.key_id))
                });
                self.routing_allowed_key_offset = key_ids.len();
                self.routing_allowed_rows = Some(rows.into());
            }

            let rows = if let Some(rows) = self.routing_allowed_rows.as_mut() {
                let page_len = limit.min(rows.len());
                rows.drain(..page_len).collect::<Vec<_>>()
            } else {
                let end = self
                    .routing_allowed_key_offset
                    .saturating_add(limit)
                    .min(key_ids.len());
                let page_key_ids = key_ids[self.routing_allowed_key_offset..end].to_vec();
                self.routing_allowed_key_offset = end;
                let query = StoredPoolKeyCandidateRowsByKeyIdsQuery {
                    api_format: self.group.candidate.endpoint_api_format.clone(),
                    provider_id: self.group.candidate.provider_id.clone(),
                    endpoint_id: self.group.candidate.endpoint_id.clone(),
                    model_id: self.group.candidate.model_id.clone(),
                    selected_provider_model_name: self
                        .group
                        .candidate
                        .selected_provider_model_name
                        .clone(),
                    key_ids: page_key_ids,
                };
                match self
                    .state
                    .app()
                    .list_pool_key_candidate_rows_for_group_key_ids(&query)
                    .await
                {
                    Ok(rows) => rows,
                    Err(err) => {
                        warn!(
                            event_name = "pool_group_routing_key_load_failed",
                            log_type = "event",
                            provider_id = %self.group.candidate.provider_id,
                            endpoint_id = %self.group.candidate.endpoint_id,
                            model_id = %self.group.candidate.model_id,
                            allowed_key_count = query.key_ids.len(),
                            error = ?err,
                            "gateway pool scheduler failed to materialize routing-allowed pool keys"
                        );
                        return None;
                    }
                }
            };
            if rows.is_empty() {
                if self.routing_allowed_rows.is_some() {
                    return None;
                }
                continue;
            }

            let row_count = u32::try_from(rows.len()).unwrap_or(u32::MAX);
            let seen_count_before = self.seen_key_ids.len();
            let candidates = self.build_page_eligible_candidates(rows).await;
            let distinct_row_count = self.seen_key_ids.len().saturating_sub(seen_count_before);
            self.scanned_keys = self.scanned_keys.saturating_add(row_count);
            self.budget_scanned_keys = self
                .budget_scanned_keys
                .saturating_add(u32::try_from(distinct_row_count).unwrap_or(u32::MAX));
            return Some(candidates);
        }
    }

    async fn next_score_candidates(&mut self) -> Option<Vec<EligibleLocalExecutionCandidate>> {
        if self.score_phase_exhausted
            || self.score_next_offset >= self.score_top_n
            || self.scanned_keys >= self.absolute_max_scanned_keys
        {
            self.score_phase_exhausted = true;
            return None;
        }
        let limit = self
            .page_size
            .min(self.score_top_n - self.score_next_offset)
            .min(self.absolute_max_scanned_keys - self.scanned_keys);
        if limit == 0 {
            self.score_phase_exhausted = true;
            return None;
        }
        let scope = provider_key_pool_score_scope();
        let query = ListRankedPoolMembersQuery {
            pool_kind: POOL_KIND_PROVIDER_KEY_POOL.to_string(),
            pool_id: self.group.candidate.provider_id.clone(),
            capability: scope.capability.clone(),
            scope_kind: scope.scope_kind.clone(),
            scope_id: scope.scope_id.clone(),
            hard_states: vec![PoolMemberHardState::Available, PoolMemberHardState::Unknown],
            probe_statuses: None,
            offset: self.score_next_offset as usize,
            limit: limit as usize,
        };
        let score_started_at = std::time::Instant::now();
        let scores = match self.state.app().data.list_ranked_pool_members(&query).await {
            Ok(scores) => scores,
            Err(err) => {
                observe_gateway_stage_ms(
                    "pool_score_load",
                    score_started_at.elapsed().as_millis() as u64,
                );
                warn!(
                    event_name = "pool_group_score_load_failed",
                    log_type = "event",
                    provider_id = %self.group.candidate.provider_id,
                    endpoint_id = %self.group.candidate.endpoint_id,
                    model_id = %self.group.candidate.model_id,
                    selected_provider_model_name = %self.group.candidate.selected_provider_model_name,
                    error = ?err,
                    "gateway pool scheduler failed to read ranked pool member scores"
                );
                self.score_phase_exhausted = true;
                return None;
            }
        };
        observe_gateway_stage_ms(
            "pool_score_load",
            score_started_at.elapsed().as_millis() as u64,
        );
        if scores.is_empty() {
            self.score_phase_exhausted = true;
            return None;
        }
        let score_count = u32::try_from(scores.len()).unwrap_or(u32::MAX);
        self.score_next_offset = self.score_next_offset.saturating_add(score_count);
        if score_count < limit || self.score_next_offset >= self.score_top_n {
            self.score_phase_exhausted = true;
        }

        self.spawn_score_schedule_interest_recording(&scores);

        let key_ids = scores
            .iter()
            .map(|score| score.member_id.clone())
            .collect::<Vec<_>>();
        let rows_query = StoredPoolKeyCandidateRowsByKeyIdsQuery {
            api_format: self.group.candidate.endpoint_api_format.clone(),
            provider_id: self.group.candidate.provider_id.clone(),
            endpoint_id: self.group.candidate.endpoint_id.clone(),
            model_id: self.group.candidate.model_id.clone(),
            selected_provider_model_name: self.group.candidate.selected_provider_model_name.clone(),
            key_ids,
        };
        let rows_started_at = std::time::Instant::now();
        let rows = match self
            .state
            .app()
            .list_pool_key_candidate_rows_for_group_key_ids(&rows_query)
            .await
        {
            Ok(rows) => rows,
            Err(err) => {
                observe_gateway_stage_ms(
                    "pool_score_key_rows",
                    rows_started_at.elapsed().as_millis() as u64,
                );
                warn!(
                    event_name = "pool_group_score_key_load_failed",
                    log_type = "event",
                    provider_id = %self.group.candidate.provider_id,
                    endpoint_id = %self.group.candidate.endpoint_id,
                    model_id = %self.group.candidate.model_id,
                    selected_provider_model_name = %self.group.candidate.selected_provider_model_name,
                    score_count = scores.len(),
                    error = ?err,
                    "gateway pool scheduler failed to materialize ranked pool keys"
                );
                self.score_phase_exhausted = true;
                return None;
            }
        };
        observe_gateway_stage_ms(
            "pool_score_key_rows",
            rows_started_at.elapsed().as_millis() as u64,
        );
        let materialized_row_count = u32::try_from(rows.len()).unwrap_or(u32::MAX);
        let missing_score_count = scores.len().saturating_sub(rows.len());
        if missing_score_count > 0 {
            *self
                .skip_reason_counts
                .entry("pool_score_member_missing")
                .or_insert(0) += u32::try_from(missing_score_count).unwrap_or(u32::MAX);
        }
        let seen_count_before = self.seen_key_ids.len();
        let candidates = self.build_page_eligible_candidates(rows).await;
        let distinct_row_count = self.seen_key_ids.len().saturating_sub(seen_count_before);
        self.scanned_keys = self.scanned_keys.saturating_add(materialized_row_count);
        self.budget_scanned_keys = self
            .budget_scanned_keys
            .saturating_add(u32::try_from(distinct_row_count).unwrap_or(u32::MAX));
        Some(candidates)
    }

    async fn sticky_candidate(&mut self) -> Option<EligibleLocalExecutionCandidate> {
        let pool_config = self.effective_pool_config.as_ref()?.clone();
        if !admin_provider_pool_cache_affinity_enabled(&pool_config) {
            return None;
        }
        let sticky_key_id = read_provider_pool_sticky_bound_key_id(
            self.state.app().runtime_state.as_ref(),
            self.group.candidate.provider_id.as_str(),
            &pool_config,
            self.sticky_session_token.as_deref(),
        )
        .await?;
        if self
            .routing_overlay
            .as_ref()
            .is_some_and(|overlay| !overlay.key_allowed(sticky_key_id.as_str()))
        {
            return None;
        }
        if self.seen_key_ids.contains(&sticky_key_id) {
            return None;
        }

        let key = match self
            .state
            .app()
            .read_provider_catalog_keys_by_ids(std::slice::from_ref(&sticky_key_id))
            .await
        {
            Ok(mut keys) => keys.pop()?,
            Err(err) => {
                warn!(
                    event_name = "pool_group_sticky_key_load_failed",
                    log_type = "event",
                    provider_id = %self.group.candidate.provider_id,
                    endpoint_id = %self.group.candidate.endpoint_id,
                    model_id = %self.group.candidate.model_id,
                    key_id = %sticky_key_id,
                    error = ?err,
                    "gateway pool scheduler failed to read sticky pool key"
                );
                return None;
            }
        };
        if key.provider_id != self.group.candidate.provider_id {
            return None;
        }

        if pool_config.reserve_minimum_quota
            && admin_provider_pool_pure::admin_pool_key_minimum_quota_reached(
                &key,
                self.group.candidate.provider_type.as_str(),
                Some(self.group.candidate.selected_provider_model_name.as_str()),
            )
        {
            self.seen_key_ids.insert(key.id.clone());
            self.record_skip_reason(POOL_ACCOUNT_EXHAUSTED_SKIP_REASON);
            self.skipped_candidates
                .push(SkippedLocalExecutionCandidate {
                    candidate: pool_candidate_from_catalog_key(&self.group, key),
                    skip_reason: POOL_ACCOUNT_EXHAUSTED_SKIP_REASON,
                    transport: None,
                    ranking: self.group.ranking.clone(),
                    extra_data: None,
                });
            return None;
        }

        let candidate = pool_candidate_from_catalog_key(&self.group, key);
        self.build_eligible_candidate(candidate).await
    }

    async fn refill_queued_candidates(&mut self) -> bool {
        let refill_target = self.window_size.max(1) as usize;

        loop {
            let mut candidates = Vec::new();
            // Keep pool expansion bounded; the cursor freezes one small window at a time.
            while candidates.len() < refill_target {
                let Some(mut page_candidates) = self.next_page_candidates().await else {
                    break;
                };
                candidates.append(&mut page_candidates);
            }

            if candidates.is_empty() {
                return false;
            }

            let (mut scheduled, mut skipped) = schedule_pool_page_candidates(
                self.state,
                candidates,
                self.sticky_session_token.as_deref(),
                self.effective_pool_config.as_ref(),
                Some(self.group.candidate.selected_provider_model_name.as_str()),
            )
            .await;
            self.record_skipped_candidates(&skipped);
            self.skipped_candidates.append(&mut skipped);

            if scheduled.is_empty() {
                continue;
            }

            scheduled.truncate(refill_target);
            self.queued_candidates.extend(scheduled.drain(..));
            return true;
        }
    }

    async fn next_queued_candidate(&mut self) -> Option<EligibleLocalExecutionCandidate> {
        while let Some(candidate) = self.queued_candidates.pop_front() {
            let mut candidate = candidate;
            if self.skip_candidate_if_routing_profile_disallowed(&candidate) {
                continue;
            }
            if self.skip_candidate_if_runtime_cooldown(&candidate).await {
                continue;
            }
            if candidate.orchestration.candidate_group_id.is_none() {
                candidate.orchestration.candidate_group_id =
                    Some(pool_cursor_candidate_group_id(&self.group));
            }
            candidate.orchestration.pool_key_index = Some(self.next_pool_key_index);
            self.next_pool_key_index = self.next_pool_key_index.saturating_add(1);
            return Some(candidate);
        }

        None
    }

    fn skip_candidate_if_routing_profile_disallowed(
        &mut self,
        candidate: &EligibleLocalExecutionCandidate,
    ) -> bool {
        let Some(overlay) = self.routing_overlay.as_ref() else {
            return false;
        };
        if overlay.key_allowed(candidate.candidate.key_id.as_str()) {
            return false;
        }
        self.record_skip_reason(ROUTING_PROFILE_DISALLOWED_KEY_SKIP_REASON);
        self.skipped_candidates
            .push(SkippedLocalExecutionCandidate {
                candidate: candidate.candidate.clone(),
                skip_reason: ROUTING_PROFILE_DISALLOWED_KEY_SKIP_REASON,
                transport: Some(candidate.transport.clone()),
                ranking: candidate.ranking.clone(),
                extra_data: None,
            });
        true
    }

    async fn skip_candidate_if_runtime_cooldown(
        &mut self,
        candidate: &EligibleLocalExecutionCandidate,
    ) -> bool {
        match read_admin_provider_pool_key_cooldown_reason(
            self.state.app().runtime_state.as_ref(),
            candidate.candidate.provider_id.as_str(),
            candidate.candidate.key_id.as_str(),
        )
        .await
        {
            Ok(Some(_)) => {
                self.record_skip_reason("pool_cooldown");
                self.skipped_candidates
                    .push(SkippedLocalExecutionCandidate {
                        candidate: candidate.candidate.clone(),
                        skip_reason: "pool_cooldown",
                        transport: Some(candidate.transport.clone()),
                        ranking: candidate.ranking.clone(),
                        extra_data: None,
                    });
                self.spawn_active_probe_member_eviction_and_replenish(candidate);
                true
            }
            Ok(None) => false,
            Err(err) => {
                warn!(
                    event_name = "pool_key_cooldown_check_failed",
                    log_type = "event",
                    provider_id = %candidate.candidate.provider_id,
                    endpoint_id = %candidate.candidate.endpoint_id,
                    model_id = %candidate.candidate.model_id,
                    key_id = %candidate.candidate.key_id,
                    error = ?err,
                    "gateway pool scheduler failed to read pool key cooldown; scheduling key"
                );
                false
            }
        }
    }

    fn spawn_active_probe_member_eviction_and_replenish(
        &self,
        candidate: &EligibleLocalExecutionCandidate,
    ) {
        let Some(config) = pool_config_for_candidate(candidate) else {
            return;
        };
        if !should_enforce_active_probe_sealed_pool(&config) {
            return;
        }
        let provider_id = candidate.candidate.provider_id.as_str();
        let key_id = candidate.candidate.key_id.as_str();
        spawn_active_probe_member_evictions_for_request(
            self.state,
            &BTreeMap::from([(
                provider_id.to_string(),
                BTreeSet::from([key_id.to_string()]),
            )]),
        );
        let _ = spawn_pool_quota_probe_replenish_for_request(
            self.state.app().clone(),
            provider_id.to_string(),
        );
    }

    fn spawn_score_schedule_interest_recording(&mut self, scores: &[StoredPoolMemberScore]) {
        if scores.is_empty()
            || self.score_schedule_interest_count >= POOL_SCORE_SCHEDULE_INTEREST_MAX_PER_BATCH
            || !self.state.app().data.has_pool_score_writer()
        {
            return;
        }

        let remaining_interest_budget = POOL_SCORE_SCHEDULE_INTEREST_MAX_PER_BATCH
            .saturating_sub(self.score_schedule_interest_count);
        let scheduled_at = current_unix_ms() / 1000;
        let provider_id = self.group.candidate.provider_id.clone();
        let endpoint_id = self.group.candidate.endpoint_id.clone();
        let model_id = self.group.candidate.model_id.clone();
        let feedback = scores
            .iter()
            .filter(|score| {
                score.last_scheduled_at.is_none_or(|last_scheduled_at| {
                    scheduled_at.saturating_sub(last_scheduled_at)
                        >= POOL_SCORE_SCHEDULE_INTEREST_MIN_INTERVAL_SECS
                })
            })
            .take(remaining_interest_budget)
            .map(|score| PoolMemberScheduleFeedback {
                identity: PoolMemberIdentity {
                    pool_kind: score.pool_kind.clone(),
                    pool_id: score.pool_id.clone(),
                    member_kind: score.member_kind.clone(),
                    member_id: score.member_id.clone(),
                },
                scope: Some(PoolScoreScope {
                    capability: score.capability.clone(),
                    scope_kind: score.scope_kind.clone(),
                    scope_id: score.scope_id.clone(),
                }),
                scheduled_at,
                succeeded: None,
                hard_state: None,
                score_delta: None,
                score_reason_patch: Some(serde_json::json!({
                    "last_schedule_interest": {
                        "provider_id": provider_id.as_str(),
                        "endpoint_id": endpoint_id.as_str(),
                        "model_id": model_id.as_str()
                    }
                })),
            })
            .collect::<Vec<_>>();
        let score_count = feedback.len();
        if feedback.is_empty() {
            return;
        }

        let Ok(permit) = POOL_SCORE_SCHEDULE_INTEREST_SEMAPHORE
            .clone()
            .try_acquire_owned()
        else {
            debug!(
                event_name = "pool_group_score_interest_dropped",
                log_type = "event",
                provider_id = %self.group.candidate.provider_id,
                endpoint_id = %self.group.candidate.endpoint_id,
                model_id = %self.group.candidate.model_id,
                score_count = scores.len(),
                "gateway pool scheduler dropped score schedule interest because the background writer is saturated"
            );
            return;
        };
        self.score_schedule_interest_count = self
            .score_schedule_interest_count
            .saturating_add(score_count);

        let app = self.state.app().clone();

        tokio::spawn(async move {
            let _permit = permit;
            let mut failed = 0usize;
            for feedback in feedback {
                let result = app
                    .data
                    .record_pool_member_schedule_feedback(feedback)
                    .await;
                if result.is_err() {
                    failed += 1;
                }
            }
            if failed > 0 {
                warn!(
                    event_name = "pool_group_score_interest_update_failed",
                    log_type = "event",
                    provider_id = %provider_id,
                    endpoint_id = %endpoint_id,
                    model_id = %model_id,
                    failed_count = failed,
                    score_count,
                    "gateway pool scheduler failed to record some pool score schedule interests"
                );
            }
        });
    }

    async fn build_page_eligible_candidates(
        &mut self,
        rows: Vec<StoredMinimalCandidateSelectionRow>,
    ) -> Vec<EligibleLocalExecutionCandidate> {
        let mut candidates = Vec::with_capacity(rows.len());
        for row in rows {
            let candidate = pool_candidate_from_row(&self.group, row);
            if let Some(candidate) = self.build_eligible_candidate(candidate).await {
                candidates.push(candidate);
            }
        }
        candidates
    }

    async fn build_eligible_candidate(
        &mut self,
        candidate: aether_scheduler_core::SchedulerMinimalCandidateSelectionCandidate,
    ) -> Option<EligibleLocalExecutionCandidate> {
        if self
            .routing_overlay
            .as_ref()
            .is_some_and(|overlay| !overlay.key_allowed(candidate.key_id.as_str()))
        {
            self.record_skip_reason(ROUTING_PROFILE_DISALLOWED_KEY_SKIP_REASON);
            return None;
        }
        if !self.seen_key_ids.insert(candidate.key_id.clone()) {
            return None;
        }

        let transport_started_at = std::time::Instant::now();
        let Some(transport) = read_candidate_transport_snapshot(self.state, &candidate).await
        else {
            observe_gateway_stage_ms(
                "candidate_transport_snapshot",
                transport_started_at.elapsed().as_millis() as u64,
            );
            self.record_skip_reason("transport_snapshot_missing");
            return None;
        };
        observe_gateway_stage_ms(
            "candidate_transport_snapshot",
            transport_started_at.elapsed().as_millis() as u64,
        );
        if let Some(skip_reason) =
            candidate_auth_channel_skip_reason(&transport, self.request_auth_channel.as_deref())
        {
            self.record_skip_reason(skip_reason);
            return None;
        }
        if let Some(skip_reason) = candidate_common_transport_skip_reason(
            &transport,
            pool_candidate_transport_policy_facts(&candidate),
            self.requested_model.as_deref(),
        ) {
            self.record_skip_reason(skip_reason);
            return None;
        }
        Some(EligibleLocalExecutionCandidate {
            kind: LocalExecutionCandidateKind::SingleKey,
            candidate,
            provider_api_format: transport.endpoint.api_format.trim().to_ascii_lowercase(),
            transport: std::sync::Arc::new(transport),
            orchestration: LocalExecutionCandidateMetadata::default(),
            ranking: self.group.ranking.clone(),
        })
    }

    fn record_skip_reason(&mut self, reason: &'static str) {
        *self.skip_reason_counts.entry(reason).or_insert(0) += 1;
    }

    fn record_skipped_candidates(&mut self, skipped_candidates: &[SkippedLocalExecutionCandidate]) {
        for skipped_candidate in skipped_candidates {
            self.record_skip_reason(skipped_candidate.skip_reason);
        }
        let prefiltered_count = skipped_candidates
            .iter()
            .filter(|candidate| pool_skip_reason_releases_scan_budget(candidate.skip_reason))
            .count();
        self.budget_scanned_keys = self
            .budget_scanned_keys
            .saturating_sub(u32::try_from(prefiltered_count).unwrap_or(u32::MAX));
    }
}

fn pool_skip_reason_releases_scan_budget(skip_reason: &str) -> bool {
    matches!(
        skip_reason,
        POOL_ACCOUNT_EXHAUSTED_SKIP_REASON | POOL_ACCOUNT_BLOCKED_SKIP_REASON
    )
}

fn pool_candidate_transport_policy_facts(
    candidate: &aether_scheduler_core::SchedulerMinimalCandidateSelectionCandidate,
) -> CandidateTransportPolicyFacts<'_> {
    CandidateTransportPolicyFacts {
        endpoint_api_format: candidate.endpoint_api_format.as_str(),
        global_model_name: candidate.global_model_name.as_str(),
        selected_provider_model_name: candidate.selected_provider_model_name.as_str(),
        mapping_matched_model: candidate.mapping_matched_model.as_deref(),
    }
}

fn pool_candidate_from_row(
    group: &EligibleLocalExecutionCandidate,
    row: StoredMinimalCandidateSelectionRow,
) -> aether_scheduler_core::SchedulerMinimalCandidateSelectionCandidate {
    let mut candidate = group.candidate.clone();
    candidate.key_id = row.key_id;
    candidate.key_name = row.key_name;
    candidate.key_auth_type = row.key_auth_type;
    candidate.key_internal_priority = row.key_internal_priority;
    candidate.key_global_priority_for_format =
        aether_scheduler_core::extract_global_priority_for_format(
            row.key_global_priority_by_format.as_ref(),
            group.candidate.endpoint_api_format.as_str(),
        )
        .ok()
        .flatten();
    candidate.key_capabilities = row.key_capabilities;
    candidate
}

fn pool_candidate_from_catalog_key(
    group: &EligibleLocalExecutionCandidate,
    key: StoredProviderCatalogKey,
) -> aether_scheduler_core::SchedulerMinimalCandidateSelectionCandidate {
    let mut candidate = group.candidate.clone();
    candidate.key_id = key.id;
    candidate.key_name = key.name;
    candidate.key_auth_type = key.auth_type;
    candidate.key_internal_priority = key.internal_priority;
    candidate.key_global_priority_for_format =
        aether_scheduler_core::extract_global_priority_for_format(
            key.global_priority_by_format.as_ref(),
            group.candidate.endpoint_api_format.as_str(),
        )
        .ok()
        .flatten();
    candidate.key_capabilities = key.capabilities;
    candidate
}

async fn read_pool_catalog_key_contexts_by_id(
    state: PlannerAppState<'_>,
    candidates: &[EligibleLocalExecutionCandidate],
    provider_model_name: Option<&str>,
    effective_pool_config: Option<&AdminProviderPoolConfig>,
) -> BTreeMap<String, PoolCatalogKeyContext> {
    let mut key_ids = Vec::new();
    let mut provider_type_by_key_id = BTreeMap::<String, String>::new();
    let mut reserve_minimum_quota_key_ids = BTreeSet::new();

    for candidate in candidates {
        let Some(pool_config) = effective_pool_config
            .cloned()
            .or_else(|| pool_config_for_candidate(candidate))
        else {
            continue;
        };
        let key_id = candidate.candidate.key_id.clone();
        if pool_config.reserve_minimum_quota {
            reserve_minimum_quota_key_ids.insert(key_id.clone());
        }
        if let Entry::Vacant(entry) = provider_type_by_key_id.entry(key_id.clone()) {
            entry.insert(candidate.transport.provider.provider_type.clone());
            key_ids.push(key_id);
        }
    }

    if key_ids.is_empty() {
        return BTreeMap::new();
    }

    let keys = match state
        .app()
        .read_provider_catalog_keys_by_ids(&key_ids)
        .await
    {
        Ok(keys) => keys,
        Err(err) => {
            warn!(
                error = ?err,
                key_count = key_ids.len(),
                "gateway pool scheduler: failed to read catalog key metadata"
            );
            // Do not fail open when the quota metadata read is unavailable. A
            // missing context must never turn an exhausted account into an
            // eligible candidate and produce another upstream 429. The caller
            // treats this marker as a pool quota skip and the next request will
            // retry the metadata read.
            return key_ids
                .into_iter()
                .map(|key_id| {
                    (
                        key_id,
                        PoolCatalogKeyContext {
                            quota_hard_blocked: true,
                            ..PoolCatalogKeyContext::default()
                        },
                    )
                })
                .collect();
        }
    };

    let provider_pool_service = ProviderPoolService::with_builtin_adapters();

    let mut contexts = keys
        .into_iter()
        .map(|key| {
            let provider_type = provider_type_by_key_id
                .get(&key.id)
                .map(String::as_str)
                .unwrap_or_default();
            let mut context = build_pool_catalog_key_context(
                state,
                &provider_pool_service,
                &key,
                provider_type,
                provider_model_name,
            );
            let reserve_reached = reserve_minimum_quota_key_ids.contains(&key.id)
                && admin_provider_pool_pure::admin_pool_key_minimum_quota_reached(
                    &key,
                    provider_type,
                    provider_model_name,
                );
            context.quota_exhausted |= reserve_reached;
            context.quota_hard_blocked |= reserve_reached;
            (key.id.clone(), context)
        })
        .collect::<BTreeMap<_, _>>();
    // A key can disappear between the candidate-row and catalog reads. Keep
    // the snapshot non-empty and fail closed for those IDs so the caller does
    // not interpret an incomplete read as "all accounts are healthy".
    for key_id in key_ids {
        contexts
            .entry(key_id)
            .or_insert_with(|| PoolCatalogKeyContext {
                quota_hard_blocked: true,
                ..PoolCatalogKeyContext::default()
            });
    }
    contexts
}

fn build_pool_catalog_key_context(
    state: PlannerAppState<'_>,
    provider_pool_service: &ProviderPoolService,
    key: &StoredProviderCatalogKey,
    provider_type: &str,
    provider_model_name: Option<&str>,
) -> PoolCatalogKeyContext {
    let (health_score, _, _, _, _) = provider_key_health_summary(key);
    let health_score = key
        .health_by_format
        .as_ref()
        .and_then(serde_json::Value::as_object)
        .filter(|payload| !payload.is_empty())
        .map(|_| health_score);
    let latency_avg_ms = key
        .success_count
        .filter(|count| *count > 0)
        .zip(key.total_response_time_ms)
        .map(|(success_count, total_response_time_ms)| {
            total_response_time_ms as f64 / f64::from(success_count)
        })
        .filter(|value| value.is_finite() && *value >= 0.0);

    let auth_config = parse_catalog_auth_config_json(state.app(), key);
    let mut signals = provider_pool_service.member_signals(
        provider_type,
        key,
        auth_config.as_ref(),
        provider_model_name,
    );
    signals.account_blocked |= admin_provider_pool_pure::admin_pool_key_is_known_banned(key);
    signals.account_blocked |=
        pool_key_requires_reauth_for_scheduling(key, current_unix_ms().saturating_div(1000));
    signals.health_score = health_score;
    signals.latency_avg_ms = latency_avg_ms;
    signals.catalog_lru_score = Some(key.last_used_at_unix_secs.unwrap_or(0) as f64);
    signals
}

fn pool_key_requires_reauth_for_scheduling(
    key: &StoredProviderCatalogKey,
    now_unix_secs: u64,
) -> bool {
    if !key.auth_type.trim().eq_ignore_ascii_case("oauth") {
        return false;
    }

    let invalid_reason = key
        .oauth_invalid_reason
        .as_deref()
        .map(str::trim)
        .unwrap_or_default();
    if !invalid_reason.is_empty() {
        let account_state = admin_provider_status_pure::resolve_pool_account_state(
            None,
            key.upstream_metadata.as_ref(),
            Some(invalid_reason),
        );
        if account_state.blocked && !account_state.recoverable {
            return true;
        }
        if pool_oauth_reason_has_tag(invalid_reason, "[ACCOUNT_BLOCK]") {
            return true;
        }
        if pool_oauth_reason_has_tag(invalid_reason, "[REQUEST_FAILED]") {
            return false;
        }
        if pool_oauth_reason_has_tag(invalid_reason, "[REFRESH_FAILED]") {
            return key
                .expires_at_unix_secs
                .is_none_or(|expires_at| expires_at == 0 || expires_at <= now_unix_secs);
        }
        if pool_oauth_reason_has_tag(invalid_reason, "[OAUTH_EXPIRED]") {
            return false;
        }
        return true;
    }

    key.oauth_invalid_at_unix_secs.is_some()
}

fn pool_oauth_reason_has_tag(reason: &str, tag: &str) -> bool {
    reason
        .lines()
        .map(str::trim)
        .any(|line| line.starts_with(tag))
}

#[cfg(test)]
fn apply_local_execution_pool_scheduler_with_runtime_map(
    candidates: Vec<EligibleLocalExecutionCandidate>,
    runtime_by_provider: &BTreeMap<String, AdminProviderPoolRuntimeState>,
    key_context_by_id: &BTreeMap<String, PoolCatalogKeyContext>,
) -> (
    Vec<EligibleLocalExecutionCandidate>,
    Vec<SkippedLocalExecutionCandidate>,
) {
    let outcome = apply_local_execution_pool_scheduler_with_runtime_map_outcome(
        candidates,
        runtime_by_provider,
        key_context_by_id,
    );
    (outcome.candidates, outcome.skipped)
}

struct PoolSchedulerApplyOutcome {
    candidates: Vec<EligibleLocalExecutionCandidate>,
    skipped: Vec<SkippedLocalExecutionCandidate>,
    active_probe_seal_fallback_provider_ids: BTreeSet<String>,
    active_probe_evicted_members_by_provider: BTreeMap<String, BTreeSet<String>>,
}

#[cfg(test)]
fn apply_local_execution_pool_scheduler_with_runtime_map_outcome(
    candidates: Vec<EligibleLocalExecutionCandidate>,
    runtime_by_provider: &BTreeMap<String, AdminProviderPoolRuntimeState>,
    key_context_by_id: &BTreeMap<String, PoolCatalogKeyContext>,
) -> PoolSchedulerApplyOutcome {
    apply_local_execution_pool_scheduler_with_runtime_map_outcome_and_configs(
        candidates,
        runtime_by_provider,
        key_context_by_id,
        &BTreeMap::new(),
    )
}

fn apply_local_execution_pool_scheduler_with_runtime_map_outcome_and_configs(
    candidates: Vec<EligibleLocalExecutionCandidate>,
    runtime_by_provider: &BTreeMap<String, AdminProviderPoolRuntimeState>,
    key_context_by_id: &BTreeMap<String, PoolCatalogKeyContext>,
    effective_pool_config_by_provider: &BTreeMap<String, AdminProviderPoolConfig>,
) -> PoolSchedulerApplyOutcome {
    let (scheduled, skipped) = run_local_execution_pool_scheduler_with_runtime_map(
        candidates.clone(),
        runtime_by_provider,
        key_context_by_id,
        effective_pool_config_by_provider,
        true,
    );
    let mut active_probe_evicted_members_by_provider =
        active_probe_evicted_members_from_skipped(&skipped, runtime_by_provider);
    let active_probe_seal_fallback_provider_ids = if scheduled.is_empty() {
        skipped
            .iter()
            .filter(|skipped| skipped.skip_reason == POOL_ACTIVE_PROBE_SEALED_SKIP_REASON)
            .map(|skipped| skipped.candidate.provider_id.clone())
            .collect::<BTreeSet<_>>()
    } else {
        BTreeSet::new()
    };

    if active_probe_seal_fallback_provider_ids.is_empty() {
        return PoolSchedulerApplyOutcome {
            candidates: scheduled,
            skipped,
            active_probe_seal_fallback_provider_ids,
            active_probe_evicted_members_by_provider,
        };
    }

    let (scheduled, skipped) = run_local_execution_pool_scheduler_with_runtime_map(
        candidates,
        runtime_by_provider,
        key_context_by_id,
        effective_pool_config_by_provider,
        false,
    );
    merge_active_probe_evictions(
        &mut active_probe_evicted_members_by_provider,
        active_probe_evicted_members_from_skipped(&skipped, runtime_by_provider),
    );
    PoolSchedulerApplyOutcome {
        candidates: scheduled,
        skipped,
        active_probe_seal_fallback_provider_ids,
        active_probe_evicted_members_by_provider,
    }
}

fn merge_active_probe_evictions(
    target: &mut BTreeMap<String, BTreeSet<String>>,
    source: BTreeMap<String, BTreeSet<String>>,
) {
    for (provider_id, key_ids) in source {
        target.entry(provider_id).or_default().extend(key_ids);
    }
}

fn active_probe_evicted_members_from_skipped(
    skipped: &[SkippedLocalExecutionCandidate],
    runtime_by_provider: &BTreeMap<String, AdminProviderPoolRuntimeState>,
) -> BTreeMap<String, BTreeSet<String>> {
    if skipped.is_empty() {
        return BTreeMap::new();
    }

    let mut evicted = BTreeMap::<String, BTreeSet<String>>::new();
    for skipped_candidate in skipped {
        if !matches!(
            skipped_candidate.skip_reason,
            POOL_ACCOUNT_BLOCKED_SKIP_REASON
                | POOL_ACCOUNT_EXHAUSTED_SKIP_REASON
                | POOL_COOLDOWN_SKIP_REASON
                | POOL_COST_LIMIT_REACHED_SKIP_REASON
        ) {
            continue;
        }
        let Some(runtime) = runtime_by_provider.get(&skipped_candidate.candidate.provider_id)
        else {
            continue;
        };
        if runtime
            .active_probe_member_ids
            .contains(&skipped_candidate.candidate.key_id)
        {
            evicted
                .entry(skipped_candidate.candidate.provider_id.clone())
                .or_default()
                .insert(skipped_candidate.candidate.key_id.clone());
        }
    }
    evicted
}

fn run_local_execution_pool_scheduler_with_runtime_map(
    candidates: Vec<EligibleLocalExecutionCandidate>,
    runtime_by_provider: &BTreeMap<String, AdminProviderPoolRuntimeState>,
    key_context_by_id: &BTreeMap<String, PoolCatalogKeyContext>,
    effective_pool_config_by_provider: &BTreeMap<String, AdminProviderPoolConfig>,
    enforce_active_probe_seal: bool,
) -> (
    Vec<EligibleLocalExecutionCandidate>,
    Vec<SkippedLocalExecutionCandidate>,
) {
    let scheduler_runtime_by_provider = runtime_by_provider
        .iter()
        .map(|(provider_id, runtime)| (provider_id.clone(), pool_runtime_state(runtime)))
        .collect::<BTreeMap<_, _>>();
    let mut inputs = Vec::new();
    let mut skipped_candidates = Vec::new();
    for candidate in candidates {
        let key_context = key_context_by_id
            .get(&candidate.candidate.key_id)
            .cloned()
            .unwrap_or_else(|| {
                // An explicitly non-empty metadata snapshot should contain
                // every catalog key in this page. If one disappeared between
                // reads, fail closed for that key instead of sending traffic
                // with an unknown quota state. Empty maps are retained for
                // callers/tests that intentionally provide no runtime context.
                if key_context_by_id.is_empty() {
                    PoolCatalogKeyContext::default()
                } else {
                    PoolCatalogKeyContext {
                        quota_hard_blocked: true,
                        ..PoolCatalogKeyContext::default()
                    }
                }
            });
        let admin_pool_config = effective_pool_config_by_provider
            .get(&candidate.candidate.provider_id)
            .cloned()
            .or_else(|| pool_config_for_candidate(&candidate));

        if let Some(config) = admin_pool_config.as_ref() {
            if enforce_active_probe_seal && should_enforce_active_probe_sealed_pool(config) {
                let active_member_ids = runtime_by_provider
                    .get(&candidate.candidate.provider_id)
                    .map(|runtime| &runtime.active_probe_member_ids);
                let should_seal_cold_member = active_member_ids.is_some_and(|members| {
                    !members.is_empty() && !members.contains(&candidate.candidate.key_id)
                });
                if should_seal_cold_member {
                    skipped_candidates.push(SkippedLocalExecutionCandidate {
                        candidate: candidate.candidate.clone(),
                        skip_reason: POOL_ACTIVE_PROBE_SEALED_SKIP_REASON,
                        transport: Some(candidate.transport.clone()),
                        ranking: candidate.ranking.clone(),
                        extra_data: None,
                    });
                    continue;
                }
            }
        }

        let pool_config = admin_pool_config.map(|config| {
            pool_scheduling_config(config, candidate.transport.provider.provider_type.as_str())
        });
        inputs.push(PoolCandidateInput {
            facts: pool_candidate_facts(&candidate),
            pool_config,
            key_context,
            candidate,
        });
    }
    let outcome = run_pool_scheduler(
        inputs,
        &scheduler_runtime_by_provider,
        pool_sort_seed().as_str(),
    );

    let candidates = outcome
        .candidates
        .into_iter()
        .map(|scheduled| apply_pool_orchestration(scheduled.candidate, scheduled.orchestration))
        .collect::<Vec<_>>();
    skipped_candidates.extend(outcome.skipped_candidates.into_iter().map(|skipped| {
        SkippedLocalExecutionCandidate {
            candidate: skipped.candidate.candidate,
            skip_reason: skipped.skip_reason,
            transport: Some(skipped.candidate.transport),
            ranking: skipped.candidate.ranking,
            extra_data: None,
        }
    }));

    (candidates, skipped_candidates)
}

fn pool_config_for_candidate(
    candidate: &EligibleLocalExecutionCandidate,
) -> Option<AdminProviderPoolConfig> {
    admin_provider_pool_config_from_config_value(candidate.transport.provider.config.as_ref())
}

fn effective_pool_config_for_group(
    group: &EligibleLocalExecutionCandidate,
    routing_policy: Option<&ResolvedRoutingPolicy>,
) -> Option<AdminProviderPoolConfig> {
    let mut pool_config = pool_config_for_candidate(group)?;
    let override_policy = routing_policy
        .and_then(|policy| {
            policy
                .pool_policy_overrides
                .get(group.candidate.provider_id.as_str())
        })
        .filter(|override_policy| !override_policy.scheduling_presets.is_empty());
    if let Some(override_policy) = override_policy {
        let scheduling_presets = override_policy
            .scheduling_presets
            .iter()
            .map(|preset| AdminProviderPoolSchedulingPreset {
                preset: preset.preset.clone(),
                enabled: preset.enabled,
                mode: preset.mode.clone(),
            })
            .collect::<Vec<_>>();
        pool_config.lru_enabled = scheduling_presets
            .iter()
            .any(|preset| preset.enabled && preset.preset.eq_ignore_ascii_case("lru"));
        pool_config.scheduling_presets = scheduling_presets;
    }
    Some(pool_config)
}

fn should_enforce_active_probe_sealed_pool(pool_config: &AdminProviderPoolConfig) -> bool {
    pool_config.probing_enabled
}

fn should_trigger_active_probe_burst_for_request(
    pool_config: &AdminProviderPoolConfig,
    runtime: &AdminProviderPoolRuntimeState,
) -> bool {
    if !should_enforce_active_probe_sealed_pool(pool_config) {
        return false;
    }
    if runtime.provider_burst_pending {
        return false;
    }
    let active_count = runtime.active_probe_member_ids.len();
    runtime.provider_desired_hot > 0 && active_count < runtime.provider_desired_hot
}

fn pool_key_candidate_order_for_group(
    group: &EligibleLocalExecutionCandidate,
    pool_config: Option<&AdminProviderPoolConfig>,
) -> StoredPoolKeyCandidateOrder {
    let Some(pool_config) = pool_config else {
        return StoredPoolKeyCandidateOrder::InternalPriority;
    };
    let presets = pool_config
        .scheduling_presets
        .iter()
        .map(|preset| PoolSchedulingPreset {
            preset: preset.preset.clone(),
            enabled: preset.enabled,
            mode: preset.mode.clone(),
        })
        .collect::<Vec<_>>();
    let active_presets = ProviderPoolService::with_builtin_adapters()
        .normalize_scheduling_presets(group.transport.provider.provider_type.as_str(), &presets);
    if let Some(distribution_mode) = active_presets
        .iter()
        .find(|preset| pool_distribution_mode_preset(preset.preset.as_str()))
    {
        return match distribution_mode.preset.as_str() {
            "cache_affinity" => match distribution_mode.mode.as_deref() {
                Some("lru") => StoredPoolKeyCandidateOrder::Lru,
                Some("single_account") => StoredPoolKeyCandidateOrder::SingleAccount,
                _ => StoredPoolKeyCandidateOrder::CacheAffinity,
            },
            "load_balance" => StoredPoolKeyCandidateOrder::LoadBalance {
                seed: pool_sort_seed(),
            },
            "single_account" => StoredPoolKeyCandidateOrder::SingleAccount,
            _ => StoredPoolKeyCandidateOrder::InternalPriority,
        };
    }
    if pool_config.lru_enabled {
        return StoredPoolKeyCandidateOrder::Lru;
    }
    StoredPoolKeyCandidateOrder::InternalPriority
}

fn pool_distribution_mode_preset(preset: &str) -> bool {
    matches!(preset, "cache_affinity" | "load_balance" | "single_account")
}

fn pool_sort_seed() -> String {
    let now_ms = current_unix_ms();
    let sequence = LOAD_BALANCE_SEQUENCE.fetch_add(1, AtomicOrdering::Relaxed);
    format!("{now_ms}:{sequence}")
}

fn pool_candidate_facts(candidate: &EligibleLocalExecutionCandidate) -> PoolCandidateFacts {
    PoolCandidateFacts {
        provider_id: candidate.candidate.provider_id.clone(),
        endpoint_id: candidate.candidate.endpoint_id.clone(),
        model_id: candidate.candidate.model_id.clone(),
        selected_provider_model_name: candidate.candidate.selected_provider_model_name.clone(),
        provider_api_format: candidate.provider_api_format.clone(),
        key_id: candidate.candidate.key_id.clone(),
        key_internal_priority: candidate.candidate.key_internal_priority,
    }
}

fn pool_cursor_candidate_group_id(group: &EligibleLocalExecutionCandidate) -> String {
    format!(
        "provider={}|endpoint={}|model={}|selected_model={}|api_format={}|singleton_key=*",
        group.candidate.provider_id,
        group.candidate.endpoint_id,
        group.candidate.model_id,
        group.candidate.selected_provider_model_name,
        group.provider_api_format,
    )
}

fn pool_scheduling_config(
    config: AdminProviderPoolConfig,
    provider_type: &str,
) -> PoolSchedulingConfig {
    let service = ProviderPoolService::with_builtin_adapters();
    let scheduling_presets = config
        .scheduling_presets
        .into_iter()
        .map(|preset| PoolSchedulingPreset {
            preset: preset.preset,
            enabled: preset.enabled,
            mode: preset.mode,
        })
        .collect::<Vec<_>>();
    PoolSchedulingConfig {
        scheduling_presets: service
            .normalize_scheduling_presets(provider_type, &scheduling_presets),
        lru_enabled: config.lru_enabled,
        skip_exhausted_accounts: config.skip_exhausted_accounts,
        cost_limit_per_key_tokens: config.cost_limit_per_key_tokens,
    }
}

fn pool_runtime_state(runtime: &AdminProviderPoolRuntimeState) -> PoolRuntimeState {
    PoolRuntimeState {
        sticky_bound_key_id: runtime.sticky_bound_key_id.clone(),
        cooldown_reason_by_key: runtime.cooldown_reason_by_key.clone(),
        cost_window_usage_by_key: runtime.cost_window_usage_by_key.clone(),
        latency_avg_ms_by_key: runtime.latency_avg_ms_by_key.clone(),
        lru_score_by_key: runtime.lru_score_by_key.clone(),
    }
}

fn apply_pool_orchestration(
    mut candidate: EligibleLocalExecutionCandidate,
    orchestration: PoolCandidateOrchestration,
) -> EligibleLocalExecutionCandidate {
    let scheduler_affinity_epoch = candidate.orchestration.scheduler_affinity_epoch;
    let sticky_key_attempts = candidate.orchestration.sticky_key_attempts;
    candidate.orchestration = LocalExecutionCandidateMetadata {
        candidate_group_id: orchestration.candidate_group_id,
        pool_key_index: orchestration.pool_key_index,
        pool_key_lease: None,
        scheduler_affinity_epoch,
        sticky_key_attempts,
    };
    candidate
}

#[cfg(test)]
#[path = "pool_scheduler/tests.rs"]
mod tests;
