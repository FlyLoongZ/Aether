use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex as StdMutex, Weak};
use std::time::Duration;

use aether_admin::provider::quota as admin_provider_quota_pure;
use aether_cache::Gate;
use aether_contracts::{ExecutionPlan, ExecutionTelemetry};
use aether_data_contracts::repository::pool_scores::{
    PoolMemberHardState, PoolMemberIdentity, PoolMemberScheduleFeedback,
};
use aether_data_contracts::repository::provider_catalog::{
    ProviderCatalogKeyAdaptiveState, ProviderCatalogKeyAdaptiveStateUpdate,
    ProviderCatalogKeyHealthStateUpdate,
};
use aether_routing_core::RoutingPoolPolicyOverride;
use aether_scheduler_core::{
    build_scheduler_affinity_cache_key_for_api_key_id_with_client_session_and_scope,
    count_recent_rpm_requests_for_provider_key, ClientSessionAffinity, SchedulerAffinityTarget,
};
use aether_usage_runtime::{
    build_stream_terminal_usage_outcome, build_sync_terminal_usage_outcome,
    GatewayStreamReportRequest, GatewaySyncReportRequest, TerminalUsageOutcome,
};
use serde_json::Value;
use tokio::sync::Mutex as TokioMutex;
use tracing::warn;

use super::{
    classify_failure_disposition, local_failover_error_message, project_local_adaptive_rate_limit,
    project_local_adaptive_success, project_local_failure_health, project_local_key_circuit_closed,
    project_local_key_circuit_failure, project_local_success_health,
    resolve_local_failover_analysis_for_attempt, FailureScope, LocalFailoverAnalysis,
    LocalFailoverClassification,
};
use crate::ai_serving::extract_pool_sticky_session_token;
use crate::client_session_affinity::{
    client_session_affinity_from_report_context_value, CLIENT_SESSION_AFFINITY_REPORT_CONTEXT_FIELD,
};
use crate::clock::current_unix_secs;
use crate::handlers::shared::provider_pool::admin_provider_pool_config_from_config_value;
use crate::handlers::shared::provider_pool::{
    admin_provider_pool_key_terminal_error_reason, record_admin_provider_pool_error,
    record_admin_provider_pool_stream_timeout, record_admin_provider_pool_success,
    release_admin_provider_pool_key_lease, AdminProviderPoolConfig,
    AdminProviderPoolSchedulingPreset,
};
use crate::orchestration::{
    local_execution_candidate_metadata_from_report_context,
    ROUTING_POOL_POLICY_OVERRIDE_REPORT_FIELD,
};
use crate::scheduler::affinity::{
    scheduler_affinity_policy_context_from_report_context, SCHEDULER_AFFINITY_POLICY_REPORT_FIELD,
    SCHEDULER_AFFINITY_TTL,
};
use crate::AppState;

const POOL_SCORE_SUCCESS_FEEDBACK_MIN_INTERVAL_ENV: &str =
    "AETHER_GATEWAY_POOL_SCORE_SUCCESS_FEEDBACK_MIN_INTERVAL_SECS";
const POOL_SCORE_FAILURE_FEEDBACK_MIN_INTERVAL_ENV: &str =
    "AETHER_GATEWAY_POOL_SCORE_FAILURE_FEEDBACK_MIN_INTERVAL_SECS";
const HEALTH_SUCCESS_PERSIST_MIN_INTERVAL_ENV: &str =
    "AETHER_GATEWAY_PROVIDER_KEY_HEALTH_SUCCESS_PERSIST_MIN_INTERVAL_SECS";
const ADAPTIVE_SUCCESS_PERSIST_MIN_INTERVAL_ENV: &str =
    "AETHER_GATEWAY_PROVIDER_KEY_ADAPTIVE_SUCCESS_PERSIST_MIN_INTERVAL_SECS";
const DEFAULT_POOL_SCORE_SUCCESS_FEEDBACK_MIN_INTERVAL_SECS: u64 = 5;
const DEFAULT_POOL_SCORE_FAILURE_FEEDBACK_MIN_INTERVAL_SECS: u64 = 1;
const DEFAULT_HEALTH_SUCCESS_PERSIST_MIN_INTERVAL_SECS: u64 = 5;
const DEFAULT_ADAPTIVE_SUCCESS_PERSIST_MIN_INTERVAL_SECS: u64 = 5;
const MAX_POOL_SCORE_FEEDBACK_MIN_INTERVAL_SECS: u64 = 300;
const PROVIDER_KEY_EFFECT_LOCK_PRUNE_THRESHOLD: usize = 8_192;
// Same-process writers are serialized by the per-key lock. Keep remote-writer
// retries bounded so request/report completion cannot accumulate a long DB tail.
const PROVIDER_KEY_STATE_CAS_MAX_ATTEMPTS: usize = 4;

#[derive(Debug)]
struct ProviderKeyEffectLockPoolState {
    entries: HashMap<String, Weak<TokioMutex<()>>>,
    accesses_since_prune: usize,
    next_growth_prune_at: usize,
    #[cfg(test)]
    prune_count: usize,
}

impl ProviderKeyEffectLockPoolState {
    fn new(min_prune_threshold: usize) -> Self {
        Self {
            entries: HashMap::new(),
            accesses_since_prune: 0,
            next_growth_prune_at: min_prune_threshold,
            #[cfg(test)]
            prune_count: 0,
        }
    }
}

#[derive(Debug)]
struct ProviderKeyEffectLockPool {
    state: StdMutex<ProviderKeyEffectLockPoolState>,
    min_prune_threshold: usize,
}

impl Default for ProviderKeyEffectLockPool {
    fn default() -> Self {
        Self::new(PROVIDER_KEY_EFFECT_LOCK_PRUNE_THRESHOLD)
    }
}

impl ProviderKeyEffectLockPool {
    fn new(min_prune_threshold: usize) -> Self {
        let min_prune_threshold = min_prune_threshold.max(1);
        Self {
            state: StdMutex::new(ProviderKeyEffectLockPoolState::new(min_prune_threshold)),
            min_prune_threshold,
        }
    }

    fn lock_for(&self, key_id: &str) -> Arc<TokioMutex<()>> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.accesses_since_prune = state.accesses_since_prune.saturating_add(1);
        let entry_count = state.entries.len();
        let growth_prune_due = entry_count >= state.next_growth_prune_at;
        let maintenance_prune_due = entry_count >= self.min_prune_threshold
            && state.accesses_since_prune >= entry_count.max(self.min_prune_threshold);
        if growth_prune_due || maintenance_prune_due {
            self.prune_inactive_locks(&mut state);
        }

        if let Some(existing) = state.entries.get(key_id).and_then(Weak::upgrade) {
            return existing;
        }
        let lock = Arc::new(TokioMutex::new(()));
        state
            .entries
            .insert(key_id.to_string(), Arc::downgrade(&lock));
        lock
    }

    fn prune_inactive_locks(&self, state: &mut ProviderKeyEffectLockPoolState) {
        state.entries.retain(|_, lock| lock.strong_count() > 0);
        let active_entries = state.entries.len();
        state.next_growth_prune_at = if active_entries < self.min_prune_threshold {
            self.min_prune_threshold
        } else {
            active_entries.saturating_mul(2)
        };
        state.accesses_since_prune = 0;
        #[cfg(test)]
        {
            state.prune_count = state.prune_count.saturating_add(1);
        }
    }
}

static POOL_SCORE_FEEDBACK_GATE: LazyLock<Gate<String>> = LazyLock::new(Gate::new);
static HEALTH_SUCCESS_PERSIST_GATE: LazyLock<Gate<String>> = LazyLock::new(Gate::new);
static ADAPTIVE_SUCCESS_PERSIST_GATE: LazyLock<Gate<String, u64>> = LazyLock::new(Gate::new);
static ADAPTIVE_SUCCESS_PERSIST_GATE_NEXT_TOKEN: AtomicU64 = AtomicU64::new(1);
static PROVIDER_KEY_EFFECT_LOCKS: LazyLock<ProviderKeyEffectLockPool> =
    LazyLock::new(ProviderKeyEffectLockPool::default);
static POOL_SCORE_SUCCESS_FEEDBACK_MIN_INTERVAL: LazyLock<Duration> = LazyLock::new(|| {
    pool_score_feedback_interval_from_env(
        POOL_SCORE_SUCCESS_FEEDBACK_MIN_INTERVAL_ENV,
        DEFAULT_POOL_SCORE_SUCCESS_FEEDBACK_MIN_INTERVAL_SECS,
    )
});
static POOL_SCORE_FAILURE_FEEDBACK_MIN_INTERVAL: LazyLock<Duration> = LazyLock::new(|| {
    pool_score_feedback_interval_from_env(
        POOL_SCORE_FAILURE_FEEDBACK_MIN_INTERVAL_ENV,
        DEFAULT_POOL_SCORE_FAILURE_FEEDBACK_MIN_INTERVAL_SECS,
    )
});
static HEALTH_SUCCESS_PERSIST_MIN_INTERVAL: LazyLock<Duration> = LazyLock::new(|| {
    pool_score_feedback_interval_from_env(
        HEALTH_SUCCESS_PERSIST_MIN_INTERVAL_ENV,
        DEFAULT_HEALTH_SUCCESS_PERSIST_MIN_INTERVAL_SECS,
    )
});
static ADAPTIVE_SUCCESS_PERSIST_MIN_INTERVAL: LazyLock<Duration> = LazyLock::new(|| {
    pool_score_feedback_interval_from_env(
        ADAPTIVE_SUCCESS_PERSIST_MIN_INTERVAL_ENV,
        DEFAULT_ADAPTIVE_SUCCESS_PERSIST_MIN_INTERVAL_SECS,
    )
});

#[derive(Debug, Clone, Copy)]
pub(crate) struct LocalExecutionEffectContext<'a> {
    pub(crate) plan: &'a ExecutionPlan,
    pub(crate) report_context: Option<&'a Value>,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct LocalPoolErrorEffect<'a> {
    pub(crate) status_code: u16,
    pub(crate) classification: LocalFailoverClassification,
    pub(crate) headers: &'a BTreeMap<String, String>,
    pub(crate) error_body: Option<&'a str>,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct LocalAttemptFailureEffect {
    pub(crate) status_code: u16,
    pub(crate) classification: LocalFailoverClassification,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct LocalAdaptiveRateLimitEffect<'a> {
    pub(crate) status_code: u16,
    pub(crate) classification: LocalFailoverClassification,
    pub(crate) headers: Option<&'a BTreeMap<String, String>>,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct LocalHealthFailureEffect {
    pub(crate) status_code: u16,
    pub(crate) classification: LocalFailoverClassification,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct LocalHealthSuccessEffect;

#[derive(Debug, Clone, Copy)]
pub(crate) struct LocalAdaptiveSuccessEffect;

#[derive(Debug, Clone, Copy)]
pub(crate) struct LocalOAuthInvalidationEffect<'a> {
    pub(crate) status_code: u16,
    pub(crate) response_text: Option<&'a str>,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct LocalOAuthSuccessEffect<'a> {
    pub(crate) status_code: u16,
    pub(crate) request_started_at_unix_ms: Option<u64>,
    pub(crate) request_order_id: Option<&'a str>,
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum LocalExecutionEffect<'a> {
    AttemptFailure(LocalAttemptFailureEffect),
    AdaptiveRateLimit(LocalAdaptiveRateLimitEffect<'a>),
    HealthFailure(LocalHealthFailureEffect),
    HealthSuccess(LocalHealthSuccessEffect),
    AdaptiveSuccess(LocalAdaptiveSuccessEffect),
    OauthInvalidation(LocalOAuthInvalidationEffect<'a>),
    #[cfg_attr(not(test), allow(dead_code))]
    OauthSuccess(LocalOAuthSuccessEffect<'a>),
    PoolSuccessSync {
        payload: &'a GatewaySyncReportRequest,
    },
    PoolSuccessStream {
        payload: &'a GatewayStreamReportRequest,
    },
    PoolError(LocalPoolErrorEffect<'a>),
    PoolStreamTimeout,
}

#[derive(Debug)]
struct OwnedLocalOAuthSuccessEffect {
    status_code: u16,
    provider_id: String,
    endpoint_id: String,
    key_id: String,
    authorization: String,
    request_started_at_unix_ms: u64,
    request_order_id: String,
    observed_credential_generation: Option<String>,
}

fn owned_local_oauth_success_effect(
    plan: &ExecutionPlan,
    report_context: Option<&Value>,
    effect: LocalOAuthSuccessEffect<'_>,
) -> Option<OwnedLocalOAuthSuccessEffect> {
    if !(200..300).contains(&effect.status_code) {
        return None;
    }
    let request_started_at_unix_ms = effect.request_started_at_unix_ms?;
    let request_order_id = effect
        .request_order_id
        .map(str::trim)
        .filter(|value| !value.is_empty())?;
    let authorization = execution_plan_authorization(plan)?.trim().to_string();
    if authorization.is_empty() || bearer_access_token(&authorization).is_none() {
        return None;
    }
    Some(OwnedLocalOAuthSuccessEffect {
        status_code: effect.status_code,
        provider_id: plan.provider_id.clone(),
        endpoint_id: plan.endpoint_id.clone(),
        key_id: plan.key_id.clone(),
        authorization,
        request_started_at_unix_ms,
        request_order_id: request_order_id.to_string(),
        observed_credential_generation: report_context_string_field(
            report_context,
            "codex_credential_generation",
        )
        .map(ToOwned::to_owned),
    })
}

/// Schedule a fenced Codex OAuth-success observation after provider headers are available.
/// Only the small identity/observation tuple is moved into the task; request bodies and plans
/// remain owned by the caller.
pub(crate) fn spawn_local_oauth_success_effect(
    state: AppState,
    plan: &ExecutionPlan,
    report_context: Option<&Value>,
    effect: LocalOAuthSuccessEffect<'_>,
) {
    let Some(effect) = owned_local_oauth_success_effect(plan, report_context, effect) else {
        return;
    };
    tokio::spawn(async move {
        record_oauth_success_effect_owned(&state, effect).await;
    });
}

/// Inputs for the terminal effects of a failed streaming attempt.
///
/// The status/body are deliberately supplied by the transport-specific caller:
/// a WebSocket terminal event may carry its own status and error body, while a
/// normal stream failure gets them from the HTTP response. Keeping this type at
/// the orchestration boundary prevents each transport from rebuilding the
/// health, adaptive, OAuth, and pool effect sequence independently.
#[derive(Debug, Clone, Copy)]
pub(crate) struct LocalStreamFailureEffect<'a> {
    pub(crate) status_code: u16,
    pub(crate) headers: &'a BTreeMap<String, String>,
    pub(crate) response_text: Option<&'a str>,
    pub(crate) stream_timeout: bool,
}

impl<'a> LocalStreamFailureEffect<'a> {
    pub(crate) const fn new(
        status_code: u16,
        headers: &'a BTreeMap<String, String>,
        response_text: Option<&'a str>,
    ) -> Self {
        Self {
            status_code,
            headers,
            response_text,
            stream_timeout: false,
        }
    }

    pub(crate) const fn with_stream_timeout(mut self) -> Self {
        self.stream_timeout = true;
        self
    }
}

struct PoolFeedbackContext {
    pool_config: AdminProviderPoolConfig,
    sticky_session_token: Option<String>,
}

#[derive(Debug, Clone)]
enum LocalExecutionAuthConfigFence {
    Unfenced,
    Fenced(String),
}

impl LocalExecutionAuthConfigFence {
    fn encrypted_auth_config(&self) -> Option<&str> {
        match self {
            Self::Unfenced => None,
            Self::Fenced(ciphertext) => Some(ciphertext),
        }
    }
}

const ADAPTIVE_RPM_RECENT_CANDIDATE_LIMIT: usize = 512;
const LOCAL_EXECUTION_SCHEDULER_AFFINITY_MAX_ENTRIES: usize = 10_000;

pub(crate) async fn apply_local_execution_effect(
    state: &AppState,
    context: LocalExecutionEffectContext<'_>,
    effect: LocalExecutionEffect<'_>,
) {
    match effect {
        LocalExecutionEffect::AttemptFailure(effect) => {
            record_attempt_failure_effect(state, context, effect).await;
        }
        LocalExecutionEffect::AdaptiveRateLimit(effect) => {
            record_adaptive_rate_limit_effect(state, context, effect).await;
        }
        LocalExecutionEffect::HealthFailure(effect) => {
            record_health_failure_effect(state, context, effect).await;
        }
        LocalExecutionEffect::HealthSuccess(effect) => {
            record_health_success_effect(state, context, effect).await;
        }
        LocalExecutionEffect::AdaptiveSuccess(effect) => {
            record_adaptive_success_effect(state, context, effect).await;
        }
        LocalExecutionEffect::OauthInvalidation(effect) => {
            record_oauth_invalidation_effect(state, context, effect).await;
        }
        LocalExecutionEffect::OauthSuccess(effect) => {
            record_oauth_success_effect(state, context, effect).await;
        }
        LocalExecutionEffect::PoolSuccessSync { payload } => {
            record_sync_pool_success_effect(state, context, payload).await;
            release_local_pool_key_lease(state, context).await;
        }
        LocalExecutionEffect::PoolSuccessStream { payload } => {
            record_stream_pool_success_effect(state, context, payload).await;
            release_local_pool_key_lease(state, context).await;
        }
        LocalExecutionEffect::PoolError(effect) => {
            record_pool_error_effect(state, context, effect).await;
            release_local_pool_key_lease(state, context).await;
        }
        LocalExecutionEffect::PoolStreamTimeout => {
            record_pool_stream_timeout_effect(state, context).await;
            release_local_pool_key_lease(state, context).await;
        }
    }
}

/// Apply the provider/key effects shared by every successful streaming
/// transport. Usage persistence and request-candidate terminal status remain
/// owned by the report layer; this helper only projects execution health,
/// adaptive state, and pool feedback.
pub(crate) async fn apply_local_stream_success_effects(
    state: &AppState,
    context: LocalExecutionEffectContext<'_>,
    payload: &GatewayStreamReportRequest,
) {
    apply_local_execution_effect(
        state,
        context,
        LocalExecutionEffect::HealthSuccess(LocalHealthSuccessEffect),
    )
    .await;
    apply_local_execution_effect(
        state,
        context,
        LocalExecutionEffect::AdaptiveSuccess(LocalAdaptiveSuccessEffect),
    )
    .await;
    apply_local_execution_effect(
        state,
        context,
        LocalExecutionEffect::PoolSuccessStream { payload },
    )
    .await;
}

/// Apply the provider/key effects shared by every failed streaming attempt.
/// The returned analysis is the same failover classification used by the
/// normal stream runtime, allowing the caller to make a transport-specific
/// retry/close decision without re-running policy evaluation.
pub(crate) async fn apply_local_stream_failure_effects(
    state: &AppState,
    context: LocalExecutionEffectContext<'_>,
    effect: LocalStreamFailureEffect<'_>,
) -> LocalFailoverAnalysis {
    let analysis = resolve_local_failover_analysis_for_attempt(
        state,
        context.plan,
        context.report_context,
        effect.status_code,
        effect.response_text,
    )
    .await;

    if effect.stream_timeout {
        apply_local_execution_effect(state, context, LocalExecutionEffect::PoolStreamTimeout).await;
    }
    apply_local_execution_effect(
        state,
        context,
        LocalExecutionEffect::AttemptFailure(LocalAttemptFailureEffect {
            status_code: effect.status_code,
            classification: analysis.classification,
        }),
    )
    .await;
    apply_local_execution_effect(
        state,
        context,
        LocalExecutionEffect::AdaptiveRateLimit(LocalAdaptiveRateLimitEffect {
            status_code: effect.status_code,
            classification: analysis.classification,
            headers: Some(effect.headers),
        }),
    )
    .await;
    apply_local_execution_effect(
        state,
        context,
        LocalExecutionEffect::HealthFailure(LocalHealthFailureEffect {
            status_code: effect.status_code,
            classification: analysis.classification,
        }),
    )
    .await;
    apply_local_execution_effect(
        state,
        context,
        LocalExecutionEffect::OauthInvalidation(LocalOAuthInvalidationEffect {
            status_code: effect.status_code,
            response_text: effect.response_text,
        }),
    )
    .await;
    apply_local_execution_effect(
        state,
        context,
        LocalExecutionEffect::PoolError(LocalPoolErrorEffect {
            status_code: effect.status_code,
            classification: analysis.classification,
            headers: effect.headers,
            error_body: effect.response_text,
        }),
    )
    .await;

    analysis
}

pub(crate) async fn release_local_pool_key_lease(
    state: &AppState,
    context: LocalExecutionEffectContext<'_>,
) {
    let metadata = local_execution_candidate_metadata_from_report_context(context.report_context);
    let Some(lease) = metadata.pool_key_lease else {
        return;
    };
    release_pool_key_lease(state, &lease).await;
}

/// Releases a lease carried by a planned-but-not-started report context. This
/// path has no execution plan yet, so it intentionally omits candidate health
/// logging and only performs the distributed lock cleanup.
pub(crate) async fn release_pool_key_lease_from_report_context(
    state: &AppState,
    report_context: Option<&Value>,
) {
    let metadata = local_execution_candidate_metadata_from_report_context(report_context);
    let Some(lease) = metadata.pool_key_lease else {
        return;
    };
    release_pool_key_lease(state, &lease).await;
}

async fn release_pool_key_lease(state: &AppState, lease: &aether_runtime_state::RuntimeLockLease) {
    if let Err(err) =
        release_admin_provider_pool_key_lease(state.runtime_state.as_ref(), lease).await
    {
        warn!(
            error = ?err,
            "gateway orchestration effects: failed to release a planned pool key lease"
        );
    }
}

fn report_context_string_field<'a>(
    report_context: Option<&'a Value>,
    field: &str,
) -> Option<&'a str> {
    report_context
        .and_then(|context| context.get(field))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

fn report_context_u64_field(report_context: Option<&Value>, field: &str) -> Option<u64> {
    report_context
        .and_then(|context| context.get(field))
        .and_then(|value| {
            value
                .as_u64()
                .or_else(|| value.as_str().and_then(|value| value.parse::<u64>().ok()))
        })
}

fn local_scheduler_affinity_cache_key(report_context: Option<&Value>) -> Option<String> {
    let client_session_affinity = local_client_session_affinity(report_context);
    let policy_context = scheduler_affinity_policy_context_from_report_context(report_context);
    if report_context
        .and_then(|context| context.get(SCHEDULER_AFFINITY_POLICY_REPORT_FIELD))
        .is_some()
        && policy_context.is_none()
    {
        return None;
    }
    build_scheduler_affinity_cache_key_for_api_key_id_with_client_session_and_scope(
        report_context_string_field(report_context, "api_key_id")?,
        report_context_string_field(report_context, "client_api_format")?,
        report_context_string_field(report_context, "model")?,
        client_session_affinity.as_ref(),
        policy_context
            .as_ref()
            .and_then(|context| context.scope.as_ref()),
    )
}

fn local_client_session_affinity(report_context: Option<&Value>) -> Option<ClientSessionAffinity> {
    let report_context = report_context?;
    if let Some(affinity) = client_session_affinity_from_report_context_value(
        report_context.get(CLIENT_SESSION_AFFINITY_REPORT_CONTEXT_FIELD),
    ) {
        return Some(affinity);
    }

    let headers = header_map_from_report_context(report_context.get("original_headers"));
    let body_json = report_context
        .get("original_request_body")
        .filter(|value| !value.is_null());

    crate::client_session_affinity::client_session_affinity_from_api_request(
        report_context_string_field(Some(report_context), "client_api_format").unwrap_or_default(),
        &headers,
        body_json,
    )
}

fn header_map_from_report_context(headers: Option<&Value>) -> http::HeaderMap {
    let mut header_map = http::HeaderMap::new();
    let Some(headers) = headers.and_then(Value::as_object) else {
        return header_map;
    };

    for (name, value) in headers {
        let Some(value) = value.as_str() else {
            continue;
        };
        let Ok(name) = http::header::HeaderName::from_bytes(name.as_bytes()) else {
            continue;
        };
        let Ok(value) = http::HeaderValue::from_str(value) else {
            continue;
        };
        header_map.insert(name, value);
    }

    header_map
}

fn local_scheduler_affinity_target(plan: &ExecutionPlan) -> Option<SchedulerAffinityTarget> {
    let provider_id = plan.provider_id.trim();
    let endpoint_id = plan.endpoint_id.trim();
    let key_id = plan.key_id.trim();
    if provider_id.is_empty() || endpoint_id.is_empty() || key_id.is_empty() {
        return None;
    }

    Some(SchedulerAffinityTarget {
        provider_id: provider_id.to_string(),
        endpoint_id: endpoint_id.to_string(),
        key_id: key_id.to_string(),
    })
}

async fn local_execution_plan_uses_pool(state: &AppState, plan: &ExecutionPlan) -> bool {
    let Ok(Some(transport)) = state
        .read_provider_transport_snapshot(&plan.provider_id, &plan.endpoint_id, &plan.key_id)
        .await
    else {
        return false;
    };

    admin_provider_pool_config_from_config_value(transport.provider.config.as_ref()).is_some()
}

async fn capture_local_execution_auth_config_fence(
    state: &AppState,
    plan: &ExecutionPlan,
) -> Option<LocalExecutionAuthConfigFence> {
    let transport = match state
        .read_provider_transport_snapshot(&plan.provider_id, &plan.endpoint_id, &plan.key_id)
        .await
    {
        Ok(Some(transport)) => transport,
        Ok(None) => return None,
        Err(err) => {
            warn!(
                provider_id = %plan.provider_id,
                endpoint_id = %plan.endpoint_id,
                key_id = %plan.key_id,
                error = ?err,
                "gateway orchestration effects: failed to read transport for credential fencing"
            );
            return None;
        }
    };
    if !transport
        .provider
        .provider_type
        .trim()
        .eq_ignore_ascii_case("codex")
        || !transport.key.auth_type.trim().eq_ignore_ascii_case("oauth")
    {
        return Some(LocalExecutionAuthConfigFence::Unfenced);
    }

    let authorization = execution_plan_authorization(plan)?;
    let current_uses_agent_identity =
        crate::provider_transport::is_codex_agent_identity_transport(&transport);
    let authorization_matches = if current_uses_agent_identity {
        crate::provider_transport::codex_agent_identity_authorization_matches_transport(
            &transport,
            authorization,
        )
    } else if crate::provider_transport::is_codex_agent_identity_authorization(authorization) {
        false
    } else {
        execution_plan_bearer_matches_transport(plan, &transport)
    };
    if !authorization_matches {
        return None;
    }

    match state
        .capture_provider_transport_auth_config_fence(&transport)
        .await
    {
        Ok(Some(ciphertext)) => Some(LocalExecutionAuthConfigFence::Fenced(ciphertext)),
        Ok(None) => None,
        Err(err) => {
            warn!(
                provider_id = %plan.provider_id,
                endpoint_id = %plan.endpoint_id,
                key_id = %plan.key_id,
                error = ?err,
                "gateway orchestration effects: failed to capture credential fence"
            );
            None
        }
    }
}

async fn local_scheduler_affinity_matches_failed_target(
    state: &AppState,
    plan: &ExecutionPlan,
    cached_target: &SchedulerAffinityTarget,
    failed_target: &SchedulerAffinityTarget,
) -> bool {
    if cached_target == failed_target {
        return true;
    }
    if cached_target.provider_id != failed_target.provider_id
        || cached_target.endpoint_id != failed_target.endpoint_id
    {
        return false;
    }

    local_execution_plan_uses_pool(state, plan).await
}

fn scheduler_cache_affinity_enabled(report_context: Option<&Value>) -> bool {
    report_context
        .and_then(|context| context.get(SCHEDULER_AFFINITY_POLICY_REPORT_FIELD))
        .is_some()
        && scheduler_affinity_policy_context_from_report_context(report_context)
            .is_some_and(|context| context.cache_affinity_enabled())
}

async fn remember_successful_local_scheduler_affinity(
    state: &AppState,
    context: LocalExecutionEffectContext<'_>,
) {
    if !scheduler_cache_affinity_enabled(context.report_context) {
        return;
    }
    let Some(cache_key) = local_scheduler_affinity_cache_key(context.report_context) else {
        return;
    };
    let Some(target) = local_scheduler_affinity_target(context.plan) else {
        return;
    };
    let expected_epoch =
        local_execution_candidate_metadata_from_report_context(context.report_context)
            .scheduler_affinity_epoch;

    let _ = state.remember_scheduler_affinity_target_for_epoch(
        &cache_key,
        target,
        SCHEDULER_AFFINITY_TTL,
        LOCAL_EXECUTION_SCHEDULER_AFFINITY_MAX_ENTRIES,
        expected_epoch,
    );
}

fn pool_feedback_request_body<'a>(
    plan: &'a ExecutionPlan,
    report_context: Option<&'a Value>,
) -> Option<&'a Value> {
    report_context
        .and_then(Value::as_object)
        .and_then(|object| object.get("original_request_body"))
        .filter(|value| !value.is_null())
        .or(plan.body.json_body.as_ref())
}

async fn resolve_pool_feedback_context(
    state: &AppState,
    context: LocalExecutionEffectContext<'_>,
) -> Option<PoolFeedbackContext> {
    let plan = context.plan;
    capture_local_execution_auth_config_fence(state, plan).await?;
    let transport = match state
        .read_provider_transport_snapshot(&plan.provider_id, &plan.endpoint_id, &plan.key_id)
        .await
    {
        Ok(Some(transport)) => transport,
        Ok(None) => return None,
        Err(err) => {
            warn!(
                "gateway orchestration effects: failed to read transport snapshot for provider {} endpoint {} key {}: {:?}",
                plan.provider_id, plan.endpoint_id, plan.key_id, err
            );
            return None;
        }
    };

    let Some(mut pool_config) =
        admin_provider_pool_config_from_config_value(transport.provider.config.as_ref())
    else {
        return None;
    };

    if let Some(override_policy) = context
        .report_context
        .and_then(|report_context| report_context.get(ROUTING_POOL_POLICY_OVERRIDE_REPORT_FIELD))
        .and_then(|value| serde_json::from_value::<RoutingPoolPolicyOverride>(value.clone()).ok())
        .filter(|override_policy| !override_policy.scheduling_presets.is_empty())
    {
        let scheduling_presets = override_policy
            .scheduling_presets
            .into_iter()
            .map(|preset| AdminProviderPoolSchedulingPreset {
                preset: preset.preset,
                enabled: preset.enabled,
                mode: preset.mode,
            })
            .collect::<Vec<_>>();
        pool_config.lru_enabled = scheduling_presets
            .iter()
            .any(|preset| preset.enabled && preset.preset.eq_ignore_ascii_case("lru"));
        pool_config.scheduling_presets = scheduling_presets;
    }

    let sticky_session_token = pool_feedback_request_body(plan, context.report_context)
        .and_then(extract_pool_sticky_session_token);

    Some(PoolFeedbackContext {
        pool_config,
        sticky_session_token,
    })
}

fn total_tokens_used(outcome: &TerminalUsageOutcome) -> u64 {
    outcome
        .standardized_usage
        .as_ref()
        .map(|usage| {
            usage
                .input_tokens
                .saturating_add(usage.output_tokens)
                .max(0) as u64
        })
        .unwrap_or(0)
}

fn resolve_ttfb_ms(telemetry: Option<&ExecutionTelemetry>) -> Option<u64> {
    telemetry.and_then(|telemetry| telemetry.ttfb_ms.or(telemetry.elapsed_ms))
}

async fn record_attempt_failure_effect(
    state: &AppState,
    context: LocalExecutionEffectContext<'_>,
    effect: LocalAttemptFailureEffect,
) {
    if !local_candidate_failure_should_invalidate_affinity_for_provider(
        &context.plan.provider_api_format,
        effect.classification,
        effect.status_code,
    ) {
        return;
    }

    if let Some(cache_key) = local_scheduler_affinity_cache_key(context.report_context) {
        let Some(failed_target) = local_scheduler_affinity_target(context.plan) else {
            return;
        };
        let Some(cached_target) =
            state.read_scheduler_affinity_target(&cache_key, SCHEDULER_AFFINITY_TTL)
        else {
            return;
        };
        if local_scheduler_affinity_matches_failed_target(
            state,
            context.plan,
            &cached_target,
            &failed_target,
        )
        .await
        {
            let _ = state.remove_scheduler_affinity_cache_entry(&cache_key);
        }
    }
}

async fn record_sync_pool_success_effect(
    state: &AppState,
    context: LocalExecutionEffectContext<'_>,
    payload: &GatewaySyncReportRequest,
) {
    let Some(pool_context) = resolve_pool_feedback_context(state, context).await else {
        return;
    };

    let usage_outcome =
        build_sync_terminal_usage_outcome(context.plan, context.report_context, payload);
    record_admin_provider_pool_success(
        state.runtime_state.as_ref(),
        &context.plan.provider_id,
        &context.plan.key_id,
        &pool_context.pool_config,
        pool_context.sticky_session_token.as_deref(),
        total_tokens_used(&usage_outcome),
        resolve_ttfb_ms(payload.telemetry.as_ref()),
    )
    .await;
    record_pool_score_schedule_feedback(
        state,
        context,
        Some(true),
        Some(PoolMemberHardState::Available),
        Some(50),
        serde_json::json!({
            "last_request_feedback": {
                "source": "sync_success"
            }
        }),
    )
    .await;
}

async fn record_adaptive_rate_limit_effect(
    state: &AppState,
    context: LocalExecutionEffectContext<'_>,
    effect: LocalAdaptiveRateLimitEffect<'_>,
) {
    if !local_candidate_failure_should_apply_key_effects(
        &context.plan.provider_api_format,
        effect.classification,
        effect.status_code,
    ) {
        return;
    }
    let Some(auth_config_fence) =
        capture_local_execution_auth_config_fence(state, context.plan).await
    else {
        return;
    };
    let effect_lock = PROVIDER_KEY_EFFECT_LOCKS.lock_for(&context.plan.key_id);
    let _effect_guard = effect_lock.lock().await;
    let observed_at_unix_secs = current_unix_secs();
    let current_rpm = state
        .read_recent_runtime_request_candidates(ADAPTIVE_RPM_RECENT_CANDIDATE_LIMIT)
        .await
        .ok()
        .map(|recent_candidates| {
            count_recent_rpm_requests_for_provider_key(
                &recent_candidates,
                &context.plan.key_id,
                observed_at_unix_secs,
            ) as u32
        });

    for _ in 0..PROVIDER_KEY_STATE_CAS_MAX_ATTEMPTS {
        let Some(current_key) = state
            .read_provider_catalog_keys_by_ids(std::slice::from_ref(&context.plan.key_id))
            .await
            .ok()
            .and_then(|mut keys| keys.drain(..).next())
        else {
            return;
        };
        if auth_config_fence
            .encrypted_auth_config()
            .is_some_and(|expected| current_key.encrypted_auth_config.as_deref() != Some(expected))
        {
            return;
        }
        let Some(projection) = project_local_adaptive_rate_limit(
            &current_key,
            effect.classification,
            effect.status_code,
            current_rpm,
            effect.headers,
            observed_at_unix_secs,
        ) else {
            return;
        };
        let expected = ProviderCatalogKeyAdaptiveState::from(&current_key);
        let mut next = expected.clone();
        next.rpm_429_count = Some(projection.rpm_429_count);
        next.learned_rpm_limit = projection.learned_rpm_limit;
        next.last_429_at_unix_secs = Some(projection.last_429_at_unix_secs);
        next.last_429_type = Some(projection.last_429_type);
        next.adjustment_history = projection.adjustment_history;
        next.utilization_samples = projection.utilization_samples;
        next.last_probe_increase_at_unix_secs = projection.last_probe_increase_at_unix_secs;
        next.last_rpm_peak = projection.last_rpm_peak;
        let update = ProviderCatalogKeyAdaptiveStateUpdate {
            key_id: context.plan.key_id.clone(),
            expected_encrypted_auth_config: auth_config_fence
                .encrypted_auth_config()
                .map(ToOwned::to_owned),
            expected,
            next,
            status_snapshot_patch: adaptive_status_snapshot_patch(&projection.status_snapshot),
            updated_at_unix_secs: Some(observed_at_unix_secs),
        };
        provider_key_adaptive_success_persist_gate_reset(&context.plan.key_id);
        match state
            .compare_and_update_provider_catalog_key_adaptive_state(&update)
            .await
        {
            Ok(true) => return,
            Ok(false) => tokio::task::yield_now().await,
            Err(err) => {
                warn!(
                    "gateway orchestration effects: failed to persist adaptive rate-limit projection for provider {} endpoint {} key {}: {:?}",
                    context.plan.provider_id, context.plan.endpoint_id, context.plan.key_id, err
                );
                return;
            }
        }
    }
    warn!(
        "gateway orchestration effects: adaptive rate-limit CAS retries exhausted for provider {} endpoint {} key {}",
        context.plan.provider_id, context.plan.endpoint_id, context.plan.key_id
    );
}

fn adaptive_status_snapshot_patch(status_snapshot: &Value) -> Value {
    const OWNED_FIELDS: [&str; 6] = [
        "observation_count",
        "header_observation_count",
        "latest_upstream_limit",
        "learning_confidence",
        "enforcement_active",
        "known_boundary",
    ];
    let Some(snapshot) = status_snapshot.as_object() else {
        return serde_json::json!({});
    };
    Value::Object(
        OWNED_FIELDS
            .into_iter()
            .filter_map(|field| {
                snapshot
                    .get(field)
                    .cloned()
                    .map(|value| (field.to_string(), value))
            })
            .collect(),
    )
}

async fn record_adaptive_success_effect(
    state: &AppState,
    context: LocalExecutionEffectContext<'_>,
    _effect: LocalAdaptiveSuccessEffect,
) {
    let Some(auth_config_fence) =
        capture_local_execution_auth_config_fence(state, context.plan).await
    else {
        return;
    };
    let observed_at_unix_secs = current_unix_secs();
    let Some(current_key) = state
        .read_provider_catalog_keys_by_ids(std::slice::from_ref(&context.plan.key_id))
        .await
        .ok()
        .and_then(|mut keys| keys.drain(..).next())
    else {
        return;
    };
    if auth_config_fence
        .encrypted_auth_config()
        .is_some_and(|expected| current_key.encrypted_auth_config.as_deref() != Some(expected))
    {
        return;
    }
    if current_key.rpm_limit.is_some()
        || current_key
            .learned_rpm_limit
            .filter(|value| *value > 0)
            .is_none()
    {
        return;
    }
    let Some(gate_token) = provider_key_adaptive_success_persist_gate_admit(&context.plan.key_id)
    else {
        return;
    };

    let effect_lock = PROVIDER_KEY_EFFECT_LOCKS.lock_for(&context.plan.key_id);
    let _effect_guard = effect_lock.lock().await;
    if !provider_key_adaptive_success_persist_gate_admission_is_current(
        &context.plan.key_id,
        gate_token,
    ) {
        return;
    }
    let Some(recent_candidates) = state
        .read_recent_runtime_request_candidates(ADAPTIVE_RPM_RECENT_CANDIDATE_LIMIT)
        .await
        .ok()
    else {
        return;
    };
    let current_rpm = count_recent_rpm_requests_for_provider_key(
        &recent_candidates,
        &context.plan.key_id,
        observed_at_unix_secs,
    ) as u32;

    for _ in 0..PROVIDER_KEY_STATE_CAS_MAX_ATTEMPTS {
        let Some(current_key) = state
            .read_provider_catalog_keys_by_ids(std::slice::from_ref(&context.plan.key_id))
            .await
            .ok()
            .and_then(|mut keys| keys.drain(..).next())
        else {
            return;
        };
        if auth_config_fence
            .encrypted_auth_config()
            .is_some_and(|expected| current_key.encrypted_auth_config.as_deref() != Some(expected))
        {
            return;
        }
        if current_key.rpm_limit.is_some()
            || current_key
                .learned_rpm_limit
                .filter(|value| *value > 0)
                .is_none()
        {
            return;
        }
        let Some(projection) =
            project_local_adaptive_success(&current_key, current_rpm, observed_at_unix_secs)
        else {
            return;
        };
        let expected = ProviderCatalogKeyAdaptiveState::from(&current_key);
        let mut next = expected.clone();
        next.learned_rpm_limit = projection.learned_rpm_limit;
        next.adjustment_history = projection.adjustment_history;
        next.utilization_samples = projection.utilization_samples;
        next.last_probe_increase_at_unix_secs = projection.last_probe_increase_at_unix_secs;
        let update = ProviderCatalogKeyAdaptiveStateUpdate {
            key_id: context.plan.key_id.clone(),
            expected_encrypted_auth_config: auth_config_fence
                .encrypted_auth_config()
                .map(ToOwned::to_owned),
            expected,
            next,
            status_snapshot_patch: adaptive_status_snapshot_patch(&projection.status_snapshot),
            updated_at_unix_secs: Some(observed_at_unix_secs),
        };
        match state
            .compare_and_update_provider_catalog_key_adaptive_state(&update)
            .await
        {
            Ok(true) => return,
            Ok(false) => tokio::task::yield_now().await,
            Err(err) => {
                warn!(
                    "gateway orchestration effects: failed to persist adaptive success projection for provider {} endpoint {} key {}: {:?}",
                    context.plan.provider_id, context.plan.endpoint_id, context.plan.key_id, err
                );
                return;
            }
        }
    }
    warn!(
        "gateway orchestration effects: adaptive success CAS retries exhausted for provider {} endpoint {} key {}",
        context.plan.provider_id, context.plan.endpoint_id, context.plan.key_id
    );
}

fn provider_key_adaptive_success_persist_gate_admit(key_id: &str) -> Option<u64> {
    if cfg!(test) {
        return Some(0);
    }
    let interval = *ADAPTIVE_SUCCESS_PERSIST_MIN_INTERVAL;
    if interval.is_zero() {
        return Some(0);
    }
    let token = ADAPTIVE_SUCCESS_PERSIST_GATE_NEXT_TOKEN.fetch_add(1, Ordering::Relaxed);
    ADAPTIVE_SUCCESS_PERSIST_GATE
        .try_enter(
            provider_key_adaptive_success_persist_gate_key(key_id),
            interval,
            token,
        )
        .then_some(token)
}

fn provider_key_adaptive_success_persist_gate_admission_is_current(
    key_id: &str,
    token: u64,
) -> bool {
    if cfg!(test) || ADAPTIVE_SUCCESS_PERSIST_MIN_INTERVAL.is_zero() {
        return true;
    }
    ADAPTIVE_SUCCESS_PERSIST_GATE.peek(
        &provider_key_adaptive_success_persist_gate_key(key_id),
        *ADAPTIVE_SUCCESS_PERSIST_MIN_INTERVAL,
    ) == Some(token)
}

fn provider_key_adaptive_success_persist_gate_reset(key_id: &str) {
    ADAPTIVE_SUCCESS_PERSIST_GATE.release(&provider_key_adaptive_success_persist_gate_key(key_id));
}

fn provider_key_adaptive_success_persist_gate_key(key_id: &str) -> String {
    format!("adaptive-success:{key_id}")
}

async fn record_health_failure_effect(
    state: &AppState,
    context: LocalExecutionEffectContext<'_>,
    effect: LocalHealthFailureEffect,
) {
    if !local_candidate_failure_should_apply_key_effects(
        &context.plan.provider_api_format,
        effect.classification,
        effect.status_code,
    ) {
        return;
    }
    let api_format = context.plan.provider_api_format.trim();
    if api_format.is_empty() {
        return;
    }
    let Some(auth_config_fence) =
        capture_local_execution_auth_config_fence(state, context.plan).await
    else {
        return;
    };

    let effect_lock = PROVIDER_KEY_EFFECT_LOCKS.lock_for(&context.plan.key_id);
    let _effect_guard = effect_lock.lock().await;
    let is_pool_provider = local_execution_plan_uses_pool(state, context.plan).await;
    let observed_at_unix_secs = current_unix_secs();
    provider_key_health_success_persist_gate_reset(&context.plan.key_id, api_format);

    for _ in 0..PROVIDER_KEY_STATE_CAS_MAX_ATTEMPTS {
        let Some(current_key) = state
            .read_provider_catalog_keys_by_ids(std::slice::from_ref(&context.plan.key_id))
            .await
            .ok()
            .and_then(|mut keys| keys.drain(..).next())
        else {
            return;
        };
        if auth_config_fence
            .encrypted_auth_config()
            .is_some_and(|expected| current_key.encrypted_auth_config.as_deref() != Some(expected))
        {
            return;
        }
        let Some(health_by_format) = project_local_failure_health(
            current_key.health_by_format.as_ref(),
            api_format,
            effect.classification,
            effect.status_code,
            observed_at_unix_secs,
        ) else {
            return;
        };
        let consecutive_failures = health_by_format
            .get(api_format)
            .and_then(|value| value.get("consecutive_failures"))
            .and_then(Value::as_u64)
            .unwrap_or(0);
        let circuit_breaker_by_format = if is_pool_provider {
            None
        } else {
            project_local_key_circuit_failure(
                current_key.circuit_breaker_by_format.as_ref(),
                api_format,
                observed_at_unix_secs,
                consecutive_failures,
                current_key.max_probe_interval_minutes,
            )
            .or_else(|| current_key.circuit_breaker_by_format.clone())
        };
        let update = ProviderCatalogKeyHealthStateUpdate {
            key_id: context.plan.key_id.clone(),
            expected_encrypted_auth_config: auth_config_fence
                .encrypted_auth_config()
                .map(ToOwned::to_owned),
            expected_health_by_format: current_key.health_by_format,
            expected_circuit_breaker_by_format: current_key.circuit_breaker_by_format,
            health_by_format: Some(health_by_format),
            circuit_breaker_by_format,
        };
        match state
            .compare_and_update_provider_catalog_key_health_state(&update)
            .await
        {
            Ok(true) => return,
            Ok(false) => tokio::task::yield_now().await,
            Err(err) => {
                warn!(
                    "gateway orchestration effects: failed to persist health failure projection for provider {} endpoint {} key {}: {:?}",
                    context.plan.provider_id, context.plan.endpoint_id, context.plan.key_id, err
                );
                return;
            }
        }
    }
    warn!(
        "gateway orchestration effects: health failure CAS retries exhausted for provider {} endpoint {} key {}",
        context.plan.provider_id, context.plan.endpoint_id, context.plan.key_id
    );
}

async fn record_health_success_effect(
    state: &AppState,
    context: LocalExecutionEffectContext<'_>,
    _effect: LocalHealthSuccessEffect,
) {
    remember_successful_local_scheduler_affinity(state, context).await;

    let api_format = context.plan.provider_api_format.trim();
    if api_format.is_empty() {
        return;
    }
    let Some(auth_config_fence) =
        capture_local_execution_auth_config_fence(state, context.plan).await
    else {
        return;
    };

    // Health updates replace both JSON snapshots in one write. Serialize the success
    // read/project/write with failure and circuit-clear effects for this provider key so a
    // stale success snapshot cannot overwrite a newer failure counter or open circuit.
    let effect_lock = PROVIDER_KEY_EFFECT_LOCKS.lock_for(&context.plan.key_id);
    let _effect_guard = effect_lock.lock().await;

    let is_pool_provider = local_execution_plan_uses_pool(state, context.plan).await;
    let mut persist_gate_checked = false;

    for _ in 0..PROVIDER_KEY_STATE_CAS_MAX_ATTEMPTS {
        let Some(current_key) = state
            .read_provider_catalog_keys_by_ids(std::slice::from_ref(&context.plan.key_id))
            .await
            .ok()
            .and_then(|mut keys| keys.drain(..).next())
        else {
            return;
        };
        if auth_config_fence
            .encrypted_auth_config()
            .is_some_and(|expected| current_key.encrypted_auth_config.as_deref() != Some(expected))
        {
            return;
        }
        let Some(health_by_format) =
            project_local_success_health(current_key.health_by_format.as_ref(), api_format)
        else {
            return;
        };
        let circuit_breaker_update_owned = if is_pool_provider {
            None
        } else {
            current_key
                .circuit_breaker_by_format
                .as_ref()
                .and_then(|current| project_local_key_circuit_closed(Some(current), api_format))
        };
        if current_key.health_by_format.as_ref() == Some(&health_by_format)
            && ((is_pool_provider && current_key.circuit_breaker_by_format.is_none())
                || (!is_pool_provider
                    && circuit_breaker_update_owned.as_ref()
                        == current_key.circuit_breaker_by_format.as_ref()))
        {
            return;
        }
        if !persist_gate_checked {
            if !provider_key_health_success_persist_gate_allows(
                &context.plan.key_id,
                api_format,
                circuit_breaker_update_owned.is_some(),
            ) {
                return;
            }
            persist_gate_checked = true;
        }
        let circuit_breaker_by_format = if is_pool_provider {
            None
        } else {
            circuit_breaker_update_owned.or_else(|| current_key.circuit_breaker_by_format.clone())
        };
        let update = ProviderCatalogKeyHealthStateUpdate {
            key_id: context.plan.key_id.clone(),
            expected_encrypted_auth_config: auth_config_fence
                .encrypted_auth_config()
                .map(ToOwned::to_owned),
            expected_health_by_format: current_key.health_by_format,
            expected_circuit_breaker_by_format: current_key.circuit_breaker_by_format,
            health_by_format: Some(health_by_format),
            circuit_breaker_by_format,
        };
        match state
            .compare_and_update_provider_catalog_key_health_state(&update)
            .await
        {
            Ok(true) => return,
            Ok(false) => tokio::task::yield_now().await,
            Err(err) => {
                warn!(
                    "gateway orchestration effects: failed to persist health success projection for provider {} endpoint {} key {}: {:?}",
                    context.plan.provider_id, context.plan.endpoint_id, context.plan.key_id, err
                );
                return;
            }
        }
    }
    warn!(
        "gateway orchestration effects: health success CAS retries exhausted for provider {} endpoint {} key {}",
        context.plan.provider_id, context.plan.endpoint_id, context.plan.key_id
    );
}

fn provider_key_health_success_persist_gate_allows(
    key_id: &str,
    api_format: &str,
    closes_circuit: bool,
) -> bool {
    if closes_circuit {
        return true;
    }
    let min_interval = *HEALTH_SUCCESS_PERSIST_MIN_INTERVAL;
    if min_interval.is_zero() {
        return true;
    }
    let key = provider_key_health_success_persist_gate_key(key_id, api_format);
    HEALTH_SUCCESS_PERSIST_GATE.enter(key, min_interval)
}

fn provider_key_health_success_persist_gate_reset(key_id: &str, api_format: &str) {
    let key = provider_key_health_success_persist_gate_key(key_id, api_format);
    HEALTH_SUCCESS_PERSIST_GATE.release(&key);
}

fn provider_key_health_success_persist_gate_key(key_id: &str, api_format: &str) -> String {
    format!("success:{key_id}:{api_format}")
}

async fn record_stream_pool_success_effect(
    state: &AppState,
    context: LocalExecutionEffectContext<'_>,
    payload: &GatewayStreamReportRequest,
) {
    let Some(pool_context) = resolve_pool_feedback_context(state, context).await else {
        return;
    };

    let usage_outcome =
        build_stream_terminal_usage_outcome(context.plan, context.report_context, payload);
    record_admin_provider_pool_success(
        state.runtime_state.as_ref(),
        &context.plan.provider_id,
        &context.plan.key_id,
        &pool_context.pool_config,
        pool_context.sticky_session_token.as_deref(),
        total_tokens_used(&usage_outcome),
        resolve_ttfb_ms(payload.telemetry.as_ref()),
    )
    .await;
    record_pool_score_schedule_feedback(
        state,
        context,
        Some(true),
        Some(PoolMemberHardState::Available),
        Some(50),
        serde_json::json!({
            "last_request_feedback": {
                "source": "stream_success"
            }
        }),
    )
    .await;
}

async fn record_pool_error_effect(
    state: &AppState,
    context: LocalExecutionEffectContext<'_>,
    effect: LocalPoolErrorEffect<'_>,
) {
    if !local_candidate_failure_should_apply_key_effects(
        &context.plan.provider_api_format,
        effect.classification,
        effect.status_code,
    ) {
        return;
    }
    let terminal_error_reason =
        admin_provider_pool_key_terminal_error_reason(effect.status_code, effect.error_body);
    if terminal_error_reason.is_none()
        && !local_candidate_failure_should_record_pool_error(
            effect.classification,
            effect.status_code,
        )
    {
        return;
    }

    let Some(pool_context) = resolve_pool_feedback_context(state, context).await else {
        return;
    };

    clear_pool_key_circuit_breaker(state, context).await;
    if capture_local_execution_auth_config_fence(state, context.plan)
        .await
        .is_none()
    {
        return;
    }
    record_admin_provider_pool_error(
        state.runtime_state.as_ref(),
        &context.plan.provider_id,
        &context.plan.key_id,
        &pool_context.pool_config,
        effect.status_code,
        effect.error_body,
        Some(effect.headers),
    )
    .await;
    record_pool_score_schedule_feedback(
        state,
        context,
        Some(false),
        pool_score_hard_state_for_status(effect.status_code, effect.error_body),
        Some(pool_score_delta_for_status(effect.status_code)),
        serde_json::json!({
            "last_request_feedback": {
                "source": "pool_error",
                "status_code": effect.status_code,
                "classification": format!("{:?}", effect.classification)
            }
        }),
    )
    .await;
}

async fn clear_pool_key_circuit_breaker(
    state: &AppState,
    context: LocalExecutionEffectContext<'_>,
) {
    let Some(auth_config_fence) =
        capture_local_execution_auth_config_fence(state, context.plan).await
    else {
        return;
    };
    let effect_lock = PROVIDER_KEY_EFFECT_LOCKS.lock_for(&context.plan.key_id);
    let _effect_guard = effect_lock.lock().await;

    for _ in 0..PROVIDER_KEY_STATE_CAS_MAX_ATTEMPTS {
        let Some(current_key) = state
            .read_provider_catalog_keys_by_ids(std::slice::from_ref(&context.plan.key_id))
            .await
            .ok()
            .and_then(|mut keys| keys.drain(..).next())
        else {
            return;
        };
        if auth_config_fence
            .encrypted_auth_config()
            .is_some_and(|expected| current_key.encrypted_auth_config.as_deref() != Some(expected))
        {
            return;
        }
        if current_key.circuit_breaker_by_format.is_none() {
            return;
        }
        let update = ProviderCatalogKeyHealthStateUpdate {
            key_id: context.plan.key_id.clone(),
            expected_encrypted_auth_config: auth_config_fence
                .encrypted_auth_config()
                .map(ToOwned::to_owned),
            expected_health_by_format: current_key.health_by_format.clone(),
            expected_circuit_breaker_by_format: current_key.circuit_breaker_by_format,
            health_by_format: current_key.health_by_format,
            circuit_breaker_by_format: None,
        };
        match state
            .compare_and_update_provider_catalog_key_health_state(&update)
            .await
        {
            Ok(true) => return,
            Ok(false) => tokio::task::yield_now().await,
            Err(err) => {
                warn!(
                    "gateway orchestration effects: failed to clear pool key circuit for provider {} endpoint {} key {}: {:?}",
                    context.plan.provider_id, context.plan.endpoint_id, context.plan.key_id, err
                );
                return;
            }
        }
    }
    warn!(
        "gateway orchestration effects: clear pool key circuit CAS retries exhausted for provider {} endpoint {} key {}",
        context.plan.provider_id, context.plan.endpoint_id, context.plan.key_id
    );
}

async fn record_oauth_invalidation_effect(
    state: &AppState,
    context: LocalExecutionEffectContext<'_>,
    effect: LocalOAuthInvalidationEffect<'_>,
) {
    if effect.status_code < 400 {
        return;
    }

    let plan = context.plan;
    // Agent assertions are long-lived credential requests whose task can rotate
    // while the response is in flight. Runtime 401/403 handling must not project
    // that response onto whichever credential generation is stored later.
    if execution_plan_uses_codex_agent_identity(plan) {
        return;
    }
    let transport = match state
        .read_provider_transport_snapshot(&plan.provider_id, &plan.endpoint_id, &plan.key_id)
        .await
    {
        Ok(Some(transport)) => transport,
        Ok(None) => return,
        Err(err) => {
            warn!(
                "gateway orchestration effects: failed to read transport snapshot for oauth invalidation provider {} endpoint {} key {}: {:?}",
                plan.provider_id, plan.endpoint_id, plan.key_id, err
            );
            return;
        }
    };
    // The inverse replacement is equally unsafe: a response sent with an old
    // bearer token must not invalidate a newly installed Agent Identity.
    if crate::provider_transport::is_codex_agent_identity_transport(&transport) {
        return;
    }
    if !transport.key.auth_type.trim().eq_ignore_ascii_case("oauth") {
        return;
    }
    if !execution_plan_bearer_matches_transport(plan, &transport) {
        return;
    }

    let Some(invalid_reason) = resolve_local_oauth_invalid_reason(
        transport.provider.provider_type.as_str(),
        effect.status_code,
        effect.response_text,
    ) else {
        return;
    };
    let request_started_at_unix_ms = report_context_u64_field(
        context.report_context,
        "provider_request_started_at_unix_ms",
    );
    let request_order_id =
        report_context_string_field(context.report_context, "provider_request_order_id");
    let observed_credential_generation =
        report_context_string_field(context.report_context, "codex_credential_generation");

    if let Err(err) = state
        .mark_provider_transport_oauth_invalid_fenced(
            &transport,
            invalid_reason.as_str(),
            request_started_at_unix_ms,
            request_order_id,
            observed_credential_generation,
        )
        .await
    {
        warn!(
            "gateway orchestration effects: failed to persist oauth invalidation for provider {} endpoint {} key {}: {:?}",
            plan.provider_id, plan.endpoint_id, plan.key_id, err
        );
    }
}

async fn record_oauth_success_effect(
    state: &AppState,
    context: LocalExecutionEffectContext<'_>,
    effect: LocalOAuthSuccessEffect<'_>,
) {
    let Some(effect) =
        owned_local_oauth_success_effect(context.plan, context.report_context, effect)
    else {
        return;
    };
    record_oauth_success_effect_owned(state, effect).await;
}

async fn record_oauth_success_effect_owned(state: &AppState, effect: OwnedLocalOAuthSuccessEffect) {
    if !(200..300).contains(&effect.status_code) {
        return;
    }
    let transport = match state
        .read_provider_transport_snapshot(&effect.provider_id, &effect.endpoint_id, &effect.key_id)
        .await
    {
        Ok(Some(transport)) => transport,
        Ok(None) => return,
        Err(err) => {
            warn!(
                "gateway orchestration effects: failed to read transport snapshot for oauth success provider {} endpoint {} key {}: {:?}",
                effect.provider_id, effect.endpoint_id, effect.key_id, err
            );
            return;
        }
    };
    if !transport
        .provider
        .provider_type
        .trim()
        .eq_ignore_ascii_case("codex")
        || crate::provider_transport::is_codex_agent_identity_transport(&transport)
        || !transport.key.auth_type.trim().eq_ignore_ascii_case("oauth")
        || crate::provider_transport::resolve_local_generic_oauth_transport_authorization(
            &transport,
        )
        .as_deref()
        .and_then(bearer_access_token)
            != bearer_access_token(effect.authorization.as_str())
    {
        return;
    }

    if let Err(err) = state
        .mark_provider_transport_oauth_success_fenced(
            &transport,
            Some(effect.request_started_at_unix_ms),
            Some(effect.request_order_id.as_str()),
            effect.observed_credential_generation.as_deref(),
        )
        .await
    {
        warn!(
            "gateway orchestration effects: failed to persist oauth success for provider {} endpoint {} key {}: {:?}",
            effect.provider_id, effect.endpoint_id, effect.key_id, err
        );
    }
}

fn execution_plan_uses_codex_agent_identity(plan: &ExecutionPlan) -> bool {
    execution_plan_authorization(plan)
        .is_some_and(crate::provider_transport::is_codex_agent_identity_authorization)
}

fn execution_plan_authorization(plan: &ExecutionPlan) -> Option<&str> {
    plan.headers
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case("authorization"))
        .map(|(_, value)| value.as_str())
}

fn execution_plan_bearer_matches_transport(
    plan: &ExecutionPlan,
    transport: &crate::provider_transport::GatewayProviderTransportSnapshot,
) -> bool {
    let Some(plan_token) = execution_plan_authorization(plan).and_then(bearer_access_token) else {
        return false;
    };
    crate::provider_transport::resolve_local_generic_oauth_transport_authorization(transport)
        .as_deref()
        .and_then(bearer_access_token)
        .is_some_and(|current_token| current_token == plan_token)
}

fn bearer_access_token(authorization: &str) -> Option<&str> {
    let mut parts = authorization.split_ascii_whitespace();
    let scheme = parts.next()?;
    let token = parts.next()?;
    (scheme.eq_ignore_ascii_case("bearer") && parts.next().is_none()).then_some(token)
}

fn resolve_local_oauth_invalid_reason(
    provider_type: &str,
    status_code: u16,
    response_text: Option<&str>,
) -> Option<String> {
    let upstream_message = local_failover_error_message(response_text);
    match provider_type.trim().to_ascii_lowercase().as_str() {
        "codex" => admin_provider_quota_pure::codex_runtime_invalid_reason(
            status_code,
            upstream_message.as_deref(),
        ),
        _ if super::oauth_status_may_be_invalid(status_code, response_text) => Some(format!(
            "[OAUTH_EXPIRED] {}",
            upstream_message
                .as_deref()
                .unwrap_or("OAuth access token was rejected")
        )),
        _ => None,
    }
}

fn local_candidate_failure_should_invalidate_affinity(
    classification: LocalFailoverClassification,
    status_code: u16,
) -> bool {
    if status_code < 400 {
        return false;
    }

    match classification {
        LocalFailoverClassification::RetrySuccessPattern
        | LocalFailoverClassification::RetryStatusCode
        | LocalFailoverClassification::RetryUpstreamFailure => true,
        LocalFailoverClassification::UseDefault | LocalFailoverClassification::StopStatusCode => {
            status_code >= 500
        }
        LocalFailoverClassification::StopErrorPattern
        | LocalFailoverClassification::StopExecutionError
        | LocalFailoverClassification::StopCyberPolicy => false,
    }
}

fn local_candidate_failure_should_invalidate_affinity_for_provider(
    provider_api_format: &str,
    classification: LocalFailoverClassification,
    status_code: u16,
) -> bool {
    if !local_candidate_failure_should_invalidate_affinity(classification, status_code) {
        return false;
    }
    if !provider_api_format
        .trim()
        .eq_ignore_ascii_case("claude:messages")
    {
        return true;
    }

    // Only `retry_action` and `failure_scope` are read here, so the exhaustion
    // switch is irrelevant to this decision.
    let disposition =
        classify_failure_disposition(provider_api_format, classification, status_code, true);
    !(disposition.retry_action == crate::orchestration::FailureRetryAction::Stop
        && disposition.failure_scope == FailureScope::None)
}

fn local_candidate_failure_should_apply_key_effects(
    provider_api_format: &str,
    classification: LocalFailoverClassification,
    status_code: u16,
) -> bool {
    if !provider_api_format
        .trim()
        .eq_ignore_ascii_case("claude:messages")
    {
        return true;
    }

    matches!(
        classify_failure_disposition(provider_api_format, classification, status_code, true)
            .failure_scope,
        FailureScope::Credential
    )
}

fn local_candidate_failure_should_record_pool_error(
    classification: LocalFailoverClassification,
    status_code: u16,
) -> bool {
    if status_code == 400 {
        return false;
    }

    local_candidate_failure_should_invalidate_affinity(classification, status_code)
}

async fn record_pool_stream_timeout_effect(
    state: &AppState,
    context: LocalExecutionEffectContext<'_>,
) {
    let Some(pool_context) = resolve_pool_feedback_context(state, context).await else {
        return;
    };

    record_admin_provider_pool_stream_timeout(
        state.runtime_state.as_ref(),
        &context.plan.provider_id,
        &context.plan.key_id,
        &pool_context.pool_config,
    )
    .await;
    record_pool_score_schedule_feedback(
        state,
        context,
        Some(false),
        Some(PoolMemberHardState::Cooldown),
        Some(-250),
        serde_json::json!({
            "last_request_feedback": {
                "source": "stream_timeout"
            }
        }),
    )
    .await;
}

async fn record_pool_score_schedule_feedback(
    state: &AppState,
    context: LocalExecutionEffectContext<'_>,
    succeeded: Option<bool>,
    hard_state: Option<PoolMemberHardState>,
    score_delta: Option<i32>,
    score_reason_patch: Value,
) {
    if context.plan.provider_id.trim().is_empty() || context.plan.key_id.trim().is_empty() {
        return;
    }
    if capture_local_execution_auth_config_fence(state, context.plan)
        .await
        .is_none()
    {
        return;
    }
    if !pool_score_feedback_gate_allows(context.plan, succeeded, hard_state, score_delta) {
        return;
    }
    let feedback = PoolMemberScheduleFeedback {
        identity: PoolMemberIdentity::provider_api_key(
            context.plan.provider_id.clone(),
            context.plan.key_id.clone(),
        ),
        scope: None,
        scheduled_at: current_unix_secs(),
        succeeded,
        hard_state,
        score_delta,
        score_reason_patch: Some(score_reason_patch),
    };
    if let Err(err) = state
        .data
        .record_pool_member_schedule_feedback(feedback)
        .await
    {
        warn!(
            provider_id = %context.plan.provider_id,
            key_id = %context.plan.key_id,
            error = ?err,
            "gateway orchestration effects: failed to record pool score schedule feedback"
        );
    }
}

fn pool_score_feedback_interval_from_env(key: &str, default_secs: u64) -> Duration {
    let secs = std::env::var(key)
        .ok()
        .and_then(|value| value.trim().parse::<u64>().ok())
        .unwrap_or(default_secs)
        .min(MAX_POOL_SCORE_FEEDBACK_MIN_INTERVAL_SECS);
    Duration::from_secs(secs)
}

fn pool_score_feedback_min_interval(succeeded: Option<bool>) -> Duration {
    match succeeded {
        Some(false) => *POOL_SCORE_FAILURE_FEEDBACK_MIN_INTERVAL,
        _ => *POOL_SCORE_SUCCESS_FEEDBACK_MIN_INTERVAL,
    }
}

fn pool_score_feedback_gate_key(
    plan: &ExecutionPlan,
    succeeded: Option<bool>,
    hard_state: Option<PoolMemberHardState>,
    score_delta: Option<i32>,
) -> String {
    let succeeded = match succeeded {
        Some(true) => "success",
        Some(false) => "failure",
        None => "neutral",
    };
    let hard_state = hard_state
        .map(PoolMemberHardState::as_database)
        .unwrap_or("none");
    format!(
        "provider:{}:key:{}:result:{}:state:{}:delta:{}",
        plan.provider_id,
        plan.key_id,
        succeeded,
        hard_state,
        score_delta.unwrap_or_default()
    )
}

fn pool_score_feedback_gate_allows(
    plan: &ExecutionPlan,
    succeeded: Option<bool>,
    hard_state: Option<PoolMemberHardState>,
    score_delta: Option<i32>,
) -> bool {
    let min_interval = pool_score_feedback_min_interval(succeeded);
    if min_interval.is_zero() {
        return true;
    }
    let key = pool_score_feedback_gate_key(plan, succeeded, hard_state, score_delta);
    POOL_SCORE_FEEDBACK_GATE.enter(key, min_interval)
}

fn pool_score_hard_state_for_status(
    status_code: u16,
    error_body: Option<&str>,
) -> Option<PoolMemberHardState> {
    if let Some(reason) = admin_provider_pool_key_terminal_error_reason(status_code, error_body) {
        return Some(pool_score_hard_state_for_terminal_error_reason(&reason));
    }

    // A number of providers report account quota exhaustion as HTTP 429 rather
    // than 402. Keep those members out of the score-based pool fallback until
    // the provider's quota probe observes a reset; treating every 429 as a
    // generic cooldown otherwise lets the member re-enter as soon as the short
    // transient cooldown expires.
    if status_code == 429 && error_body_indicates_quota_exhaustion(error_body) {
        return Some(PoolMemberHardState::QuotaExhausted);
    }

    match status_code {
        401 | 403 => Some(PoolMemberHardState::AuthInvalid),
        402 => Some(PoolMemberHardState::QuotaExhausted),
        429 | 500..=599 => Some(PoolMemberHardState::Cooldown),
        _ => {
            let body = error_body.unwrap_or_default().to_ascii_lowercase();
            if body.contains("quota") && body.contains("exceed") {
                Some(PoolMemberHardState::QuotaExhausted)
            } else if body.contains("invalid") && body.contains("token") {
                Some(PoolMemberHardState::AuthInvalid)
            } else if body.contains("banned")
                || body.contains("suspended")
                || body.contains("blocked")
            {
                Some(PoolMemberHardState::Banned)
            } else {
                None
            }
        }
    }
}

fn error_body_indicates_quota_exhaustion(error_body: Option<&str>) -> bool {
    let body = error_body.unwrap_or_default().to_ascii_lowercase();
    [
        "quota exhausted",
        "quota_exhausted",
        "quota exceeded",
        "quota_exceeded",
        "insufficient_quota",
        "resource exhausted",
        "resource has been exhausted",
        "resource_exhausted",
        "usage_limit_reached",
        "limit_reached",
        "quota limit reached",
        "credits exhausted",
        "insufficient credits",
    ]
    .iter()
    .any(|marker| body.contains(marker))
}

fn pool_score_hard_state_for_terminal_error_reason(reason: &str) -> PoolMemberHardState {
    if reason.starts_with("payment_required_") {
        PoolMemberHardState::QuotaExhausted
    } else if reason.starts_with("forbidden_") {
        PoolMemberHardState::AuthInvalid
    } else {
        PoolMemberHardState::Banned
    }
}

fn pool_score_delta_for_status(status_code: u16) -> i32 {
    match status_code {
        401 | 403 => -2_000,
        402 => -1_000,
        429 => -500,
        500..=599 => -300,
        _ => -100,
    }
}

#[cfg(test)]
#[path = "effects/tests.rs"]
mod tests;
