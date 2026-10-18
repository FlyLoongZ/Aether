use aether_scheduler_core::{
    build_scheduler_affinity_cache_key_for_api_key_id_with_client_session, candidate_affinity_hash,
    matches_affinity_target, ClientSessionAffinity, SchedulerAffinityTarget,
};

use super::SchedulerMinimalCandidateSelectionCandidate;
use crate::data::auth::GatewayAuthApiKeySnapshot;

pub(super) fn build_scheduler_affinity_cache_key(
    auth_snapshot: Option<&GatewayAuthApiKeySnapshot>,
    api_format: &str,
    global_model_name: &str,
    client_session_affinity: Option<&ClientSessionAffinity>,
) -> Option<String> {
    let api_key_id = auth_snapshot
        .map(|snapshot| snapshot.api_key_id.trim())
        .filter(|value| !value.is_empty())?;
    build_scheduler_affinity_cache_key_for_api_key_id_with_client_session(
        api_key_id,
        api_format,
        global_model_name,
        client_session_affinity,
    )
}

pub(super) fn has_explicit_session_affinity(
    client_session_affinity: Option<&ClientSessionAffinity>,
) -> bool {
    client_session_affinity.is_some_and(ClientSessionAffinity::has_session_key)
}

pub(super) fn scheduler_candidate_affinity_hash(
    affinity_key: &str,
    candidate: &SchedulerMinimalCandidateSelectionCandidate,
) -> u64 {
    candidate_affinity_hash(affinity_key, candidate)
}

pub(super) fn scheduler_candidate_matches_affinity_target(
    candidate: &SchedulerMinimalCandidateSelectionCandidate,
    target: &SchedulerAffinityTarget,
) -> bool {
    matches_affinity_target(candidate, target)
}
