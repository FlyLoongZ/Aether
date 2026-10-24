use std::collections::BTreeMap;

use aether_scheduler_core::SchedulerMinimalCandidateSelectionCandidate;

use crate::ai_serving::{GatewayProviderTransportSnapshot, PlannerAppState};
use crate::scheduler::config::SchedulerOrderingConfig;

use super::candidate_resolution::read_candidate_transport_snapshot;

pub(super) type CandidateTransportIdentity = (String, String, String);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct CandidateTransportRankingFacts {
    pub(super) keep_priority_on_conversion: bool,
}

#[derive(Debug, Default)]
pub(super) struct CandidateTransportRankingFactsCache {
    candidate_facts: BTreeMap<CandidateTransportIdentity, CandidateTransportRankingFacts>,
}

#[cfg(test)]
pub(super) async fn resolve_cached_candidate_transport_ranking_facts(
    state: PlannerAppState<'_>,
    cache: &mut CandidateTransportRankingFactsCache,
    candidate: &SchedulerMinimalCandidateSelectionCandidate,
    ordering_config: SchedulerOrderingConfig,
) -> CandidateTransportRankingFacts {
    let identity = candidate_transport_identity(candidate);
    if let Some(facts) = cache.candidate_facts.get(&identity).copied() {
        return facts;
    }

    let facts =
        resolve_candidate_transport_ranking_facts(state, cache, candidate, ordering_config).await;
    cache.candidate_facts.insert(identity, facts);
    facts
}

pub(super) async fn resolve_cached_transport_ranking_facts(
    _statee: PlannerAppState<'_>,
    cache: &mut CandidateTransportRankingFactsCache,
    candidate: &SchedulerMinimalCandidateSelectionCandidate,
    transport: &GatewayProviderTransportSnapshot,
    ordering_config: SchedulerOrderingConfig,
) -> CandidateTransportRankingFacts {
    let identity = candidate_transport_identity(candidate);
    if let Some(facts) = cache.candidate_facts.get(&identity).copied() {
        return facts;
    }

    let facts =
        resolve_candidate_transport_ranking_facts_from_transport(transport, ordering_config).await;
    cache.candidate_facts.insert(identity, facts);
    facts
}

pub(super) async fn candidate_keeps_priority_on_conversion(
    state: PlannerAppState<'_>,
    candidate: &SchedulerMinimalCandidateSelectionCandidate,
    ordering_config: SchedulerOrderingConfig,
) -> bool {
    let mut cache = CandidateTransportRankingFactsCache::default();
    resolve_candidate_transport_ranking_facts(state, &mut cache, candidate, ordering_config)
        .await
        .keep_priority_on_conversion
}

async fn resolve_candidate_transport_ranking_facts(
    state: PlannerAppState<'_>,
    _cache: &mut CandidateTransportRankingFactsCache,
    candidate: &SchedulerMinimalCandidateSelectionCandidate,
    ordering_config: SchedulerOrderingConfig,
) -> CandidateTransportRankingFacts {
    let Some(transport) = read_candidate_transport_snapshot(state, candidate).await else {
        return CandidateTransportRankingFacts {
            keep_priority_on_conversion: ordering_config.keep_priority_on_conversion,
        };
    };

    resolve_candidate_transport_ranking_facts_from_transport(&transport, ordering_config).await
}

async fn resolve_candidate_transport_ranking_facts_from_transport(
    transport: &GatewayProviderTransportSnapshot,
    ordering_config: SchedulerOrderingConfig,
) -> CandidateTransportRankingFacts {
    CandidateTransportRankingFacts {
        keep_priority_on_conversion: ordering_config.keep_priority_on_conversion
            || transport.provider.keep_priority_on_conversion,
    }
}

fn candidate_transport_identity(
    candidate: &SchedulerMinimalCandidateSelectionCandidate,
) -> CandidateTransportIdentity {
    (
        candidate.provider_id.clone(),
        candidate.endpoint_id.clone(),
        candidate.key_id.clone(),
    )
}
