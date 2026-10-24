use aether_ai_serving::{
    ai_ranking_context, build_ai_rankable_candidate, run_ai_candidate_ranking,
    AiCandidateRankingPort, AiRankableCandidateParts, AiRankingContextConfig,
    AiRankingSchedulingMode,
};
use aether_routing_core::ResolvedRoutingPolicy;
use async_trait::async_trait;
use tokio::sync::Mutex;

use crate::ai_serving::{GatewayAuthApiKeySnapshot, PlannerAppState};
use crate::clock::current_unix_ms;
use crate::handlers::shared::provider_pool::admin_provider_pool_config_from_config_value;
use crate::scheduler::config::{SchedulerOrderingConfig, SchedulerSchedulingMode};
use aether_scheduler_core::{
    matches_affinity_target, ClientSessionAffinity, SchedulerAffinityTarget,
    SchedulerMinimalCandidateSelectionCandidate, SchedulerRankableCandidate,
    SchedulerRankingContext, SchedulerRankingOutcome,
};

use super::candidate_affinity_cache::read_cached_scheduler_affinity_target;
use super::candidate_resolution::{EligibleLocalExecutionCandidate, LocalExecutionCandidateKind};
use super::candidate_transport_ranking_facts::{
    resolve_cached_transport_ranking_facts, CandidateTransportRankingFactsCache,
};

struct GatewayLocalCandidateRankingPort<'a> {
    state: PlannerAppState<'a>,
    requested_model: Option<&'a str>,
    auth_snapshot: Option<&'a GatewayAuthApiKeySnapshot>,
    client_session_affinity: Option<&'a ClientSessionAffinity>,
    required_capabilities: Option<&'a serde_json::Value>,
    ordering_config: SchedulerOrderingConfig,
    routing_policy: Option<&'a ResolvedRoutingPolicy>,
    transport_ranking_facts_cache: Mutex<CandidateTransportRankingFactsCache>,
}

#[async_trait]
impl AiCandidateRankingPort for GatewayLocalCandidateRankingPort<'_> {
    type Candidate = EligibleLocalExecutionCandidate;
    type AffinityTarget = SchedulerAffinityTarget;
    type Error = std::convert::Infallible;

    fn affinity_requested_model(&self, candidates: &[Self::Candidate]) -> Option<String> {
        self.requested_model
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string)
            .or_else(|| {
                candidates
                    .first()
                    .map(|candidate| candidate.candidate.global_model_name.clone())
            })
    }

    async fn read_cached_affinity_target(
        &self,
        normalized_client_api_format: &str,
        affinity_requested_model: Option<&str>,
    ) -> Result<Option<Self::AffinityTarget>, Self::Error> {
        Ok(read_cached_scheduler_affinity_target(
            self.state,
            self.auth_snapshot,
            self.client_session_affinity,
            normalized_client_api_format,
            affinity_requested_model,
            self.routing_policy,
        ))
    }

    fn cached_affinity_matches(
        &self,
        candidate: &Self::Candidate,
        target: &Self::AffinityTarget,
    ) -> bool {
        cached_affinity_matches_local_execution_scope(candidate, target)
    }

    async fn build_rankable_candidate(
        &self,
        candidate: &Self::Candidate,
        original_index: usize,
        normalized_client_api_format: &str,
        cached_affinity_match: bool,
    ) -> Result<SchedulerRankableCandidate, Self::Error> {
        let ranking_facts = {
            let mut cache = self.transport_ranking_facts_cache.lock().await;
            resolve_cached_transport_ranking_facts(
                self.state,
                &mut cache,
                &candidate.candidate,
                candidate.transport.as_ref(),
                self.ordering_config,
            )
            .await
        };
        let routing_overlaid_candidate =
            routing_overlaid_candidate(self.routing_policy, candidate.kind, &candidate.candidate);
        Ok(build_ai_rankable_candidate(AiRankableCandidateParts {
            candidate: &routing_overlaid_candidate,
            original_index,
            normalized_client_api_format,
            provider_api_format: candidate.provider_api_format.as_str(),
            required_capabilities: self.required_capabilities,
            cached_affinity_match,
            keep_priority_on_conversion: ranking_facts.keep_priority_on_conversion,
        }))
    }

    fn ranking_context(&self) -> SchedulerRankingContext {
        ai_ranking_context(ai_ranking_context_config(self.ordering_config))
    }

    fn apply_ranking_outcome(
        &self,
        candidate: &mut Self::Candidate,
        outcome: SchedulerRankingOutcome,
    ) {
        candidate.ranking = Some(outcome);
    }
}

pub(crate) async fn rank_eligible_local_execution_candidates(
    state: PlannerAppState<'_>,
    candidates: Vec<EligibleLocalExecutionCandidate>,
    normalized_client_api_format: &str,
    requested_model: Option<&str>,
    auth_snapshot: Option<&GatewayAuthApiKeySnapshot>,
    client_session_affinity: Option<&ClientSessionAffinity>,
    required_capabilities: Option<&serde_json::Value>,
    routing_policy: Option<&ResolvedRoutingPolicy>,
) -> Vec<EligibleLocalExecutionCandidate> {
    let ordering_config = scheduler_ordering_config_for_routing_policy(routing_policy);
    let port = GatewayLocalCandidateRankingPort {
        state,
        requested_model,
        auth_snapshot,
        client_session_affinity,
        required_capabilities,
        ordering_config,
        routing_policy,
        transport_ranking_facts_cache: Mutex::new(CandidateTransportRankingFactsCache::default()),
    };

    match run_ai_candidate_ranking(&port, candidates, normalized_client_api_format).await {
        Ok(candidates) => candidates,
        Err(error) => match error {},
    }
}

fn cached_affinity_matches_local_execution_scope(
    eligible: &EligibleLocalExecutionCandidate,
    target: &SchedulerAffinityTarget,
) -> bool {
    if local_execution_candidate_uses_pool(eligible) {
        return eligible.candidate.provider_id == target.provider_id
            && eligible.candidate.endpoint_id == target.endpoint_id;
    }

    matches_affinity_target(&eligible.candidate, target)
}

fn local_execution_candidate_uses_pool(eligible: &EligibleLocalExecutionCandidate) -> bool {
    admin_provider_pool_config_from_config_value(eligible.transport.provider.config.as_ref())
        .is_some()
}

fn ai_ranking_context_config(ordering_config: SchedulerOrderingConfig) -> AiRankingContextConfig {
    AiRankingContextConfig {
        priority_mode: ordering_config.priority_mode,
        scheduling_mode: ai_ranking_scheduling_mode(ordering_config.scheduling_mode),
        load_balance_seed: current_unix_ms(),
    }
}

fn ai_ranking_scheduling_mode(mode: SchedulerSchedulingMode) -> AiRankingSchedulingMode {
    match mode {
        SchedulerSchedulingMode::FixedOrder => AiRankingSchedulingMode::FixedOrder,
        SchedulerSchedulingMode::CacheAffinity => AiRankingSchedulingMode::CacheAffinity,
        SchedulerSchedulingMode::LoadBalance => AiRankingSchedulingMode::LoadBalance,
    }
}

/// Return the immutable scheduler snapshot carried by a resolved routing
/// policy. A missing policy is a programming error in production request
/// paths; unit tests may use the scheduler default for isolated ranking tests.
pub(crate) fn scheduler_ordering_config_for_routing_policy(
    routing_policy: Option<&ResolvedRoutingPolicy>,
) -> SchedulerOrderingConfig {
    match routing_policy {
        Some(policy) => SchedulerOrderingConfig::from_routing_policy(policy),
        None => {
            #[cfg(test)]
            {
                SchedulerOrderingConfig::default()
            }
            #[cfg(not(test))]
            {
                panic!("resolved routing policy is required before candidate scheduling")
            }
        }
    }
}

fn routing_overlaid_candidate(
    routing_policy: Option<&ResolvedRoutingPolicy>,
    kind: LocalExecutionCandidateKind,
    candidate: &SchedulerMinimalCandidateSelectionCandidate,
) -> SchedulerMinimalCandidateSelectionCandidate {
    let Some(policy) = routing_policy else {
        return candidate.clone();
    };
    let mut overlaid = candidate.clone();
    overlaid.provider_priority = policy
        .ranking_overlay
        .provider_priority(candidate.provider_id.as_str(), candidate.provider_priority);
    let overlaid_key_priority = match kind {
        LocalExecutionCandidateKind::SingleKey => policy
            .ranking_overlay
            .key_priority_override_matching_format(candidate.key_id.as_str(), |format| {
                crate::ai_serving::api_format_alias_matches(
                    format,
                    candidate.endpoint_api_format.as_str(),
                )
            })
            .or_else(|| {
                policy
                    .ranking_overlay
                    .key_priority_overrides
                    .get(candidate.key_id.as_str())
                    .copied()
            }),
        LocalExecutionCandidateKind::PoolGroup => policy
            .ranking_overlay
            .pool_priority_overrides
            .get(candidate.provider_id.as_str())
            .copied(),
    };
    if let Some(overlaid_key_priority) = overlaid_key_priority {
        overlaid.key_internal_priority = overlaid_key_priority;
        overlaid.key_global_priority_for_format = Some(overlaid_key_priority);
    }
    overlaid
}

#[cfg(test)]
#[path = "candidate_ranking/tests.rs"]
mod tests;
