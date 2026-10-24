use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::Duration;

use aether_ai_serving::AiCandidatePreselectionOutcome;
use aether_ai_serving::AiCandidateResolutionMode;
use aether_routing_core::ResolvedRoutingPolicy;
use aether_runtime::{MetricKind, MetricSample};
use aether_scheduler_core::{
    normalize_api_format, ClientSessionAffinity, SchedulerMinimalCandidateSelectionCandidate,
};
use serde_json::Value;
use sha2::Digest as _;

use crate::ai_serving::{
    EligibleLocalExecutionCandidate, GatewayAuthApiKeySnapshot, SkippedLocalExecutionCandidate,
};
const DEFAULT_CANDIDATE_PAGE_CACHE_TTL_MS: u64 = 250;
const MIN_CANDIDATE_PAGE_CACHE_TTL_MS: u64 = 50;
const MAX_CANDIDATE_PAGE_CACHE_TTL_MS: u64 = 1_000;
const CANDIDATE_PAGE_CACHE_TTL_ENV: &str = "AETHER_GATEWAY_CANDIDATE_PAGE_CACHE_TTL_MS";
const DEFAULT_CANDIDATE_PAGE_CACHE_STALE_TTL_MS: u64 = 300_000;
const MIN_CANDIDATE_PAGE_CACHE_STALE_TTL_MS: u64 = 1_000;
const MAX_CANDIDATE_PAGE_CACHE_STALE_TTL_MS: u64 = 3_600_000;
const CANDIDATE_PAGE_CACHE_STALE_TTL_ENV: &str = "AETHER_GATEWAY_CANDIDATE_PAGE_CACHE_STALE_TTL_MS";

pub(crate) type CandidatePageSnapshot = AiCandidatePreselectionOutcome<
    SchedulerMinimalCandidateSelectionCandidate,
    SkippedLocalExecutionCandidate,
>;

pub(crate) type CandidatePageCache =
    super::ValueCache<CandidatePageCacheKey, Arc<CandidatePageSnapshot>>;

#[derive(Debug, Clone)]
pub(crate) struct CandidateResolvedPageSnapshot {
    pub(crate) candidates: Vec<EligibleLocalExecutionCandidate>,
    pub(crate) resolved_skipped: Vec<SkippedLocalExecutionCandidate>,
}

pub(crate) type CandidateResolvedPageCache =
    super::ValueCache<CandidateResolvedPageCacheKey, Arc<CandidateResolvedPageSnapshot>>;

static CANDIDATE_PAGE_CACHE_METRICS: LazyLock<CandidatePageCacheMetrics> =
    LazyLock::new(CandidatePageCacheMetrics::default);

#[derive(Debug, Default)]
struct CandidatePageCacheMetrics {
    hit_total: AtomicU64,
    load_total: AtomicU64,
    follower_wait_total: AtomicU64,
    miss_total: AtomicU64,
    none_total: AtomicU64,
    resolve_hit_total: AtomicU64,
    resolve_load_total: AtomicU64,
    resolve_follower_wait_total: AtomicU64,
    resolve_miss_total: AtomicU64,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct CandidatePageCacheKey {
    requested_model: String,
    request_operation: String,
    client_api_format: String,
    auth_identity: CandidatePageAuthIdentity,
    require_streaming: bool,
    required_capabilities_hash: String,
    routing_policy_hash: String,
    request_auth_channel: String,
    scheduler_affinity_epoch: u64,
    preselection_mode: &'static str,
    use_api_format_alias_match: bool,
    client_session_affinity_hash: String,
    model_directive_policy_hash: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum CandidatePageAuthIdentity {
    Standalone { api_key_id: String },
    UserApiKey { user_id: String, api_key_id: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct CandidateResolvedPageCacheKey {
    page_key: CandidatePageCacheKey,
    resolution_mode: &'static str,
}

impl aether_cache::CacheWeight for CandidatePageCacheKey {
    fn cache_weight(&self) -> usize {
        aether_cache::value_weight::<Self>(
            self.requested_model.capacity()
                + self.request_operation.capacity()
                + self.client_api_format.capacity()
                + self.auth_identity.cache_weight()
                + self.required_capabilities_hash.capacity()
                + self.routing_policy_hash.capacity()
                + self.request_auth_channel.capacity()
                + self.client_session_affinity_hash.capacity()
                + self.model_directive_policy_hash.capacity(),
        )
    }
}

impl aether_cache::CacheWeight for CandidatePageAuthIdentity {
    fn cache_weight(&self) -> usize {
        match self {
            Self::Standalone { api_key_id } => {
                aether_cache::value_weight::<Self>(api_key_id.capacity())
            }
            Self::UserApiKey {
                user_id,
                api_key_id,
            } => aether_cache::value_weight::<Self>(user_id.capacity() + api_key_id.capacity()),
        }
    }
}

impl aether_cache::CacheWeight for CandidateResolvedPageCacheKey {
    fn cache_weight(&self) -> usize {
        aether_cache::value_weight::<Self>(self.page_key.cache_weight())
    }
}

impl aether_cache::CacheWeight for CandidateResolvedPageSnapshot {
    fn cache_weight(&self) -> usize {
        // Each record owns its strings and its orchestration metadata; the
        // transport snapshot behind its Arc belongs to another cache.
        aether_cache::vec_weight(&self.candidates, aether_cache::CacheWeight::cache_weight)
            + aether_cache::vec_weight(
                &self.resolved_skipped,
                aether_cache::CacheWeight::cache_weight,
            )
    }
}

impl CandidatePageCacheKey {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        requested_model: &str,
        request_operation: Option<&str>,
        client_api_format: &str,
        require_streaming: bool,
        auth_snapshot: &GatewayAuthApiKeySnapshot,
        required_capabilities: Option<&Value>,
        routing_policy: Option<&ResolvedRoutingPolicy>,
        request_auth_channel: Option<&str>,
        scheduler_affinity_epoch: u64,
        preselection_mode: &'static str,
        use_api_format_alias_match: bool,
        client_session_affinity: Option<&ClientSessionAffinity>,
        model_directive_policy_hash: &str,
    ) -> Self {
        Self {
            requested_model: normalize_text_key(requested_model),
            request_operation: normalize_text_key(request_operation.unwrap_or_default()),
            client_api_format: normalize_api_format(client_api_format),
            auth_identity: CandidatePageAuthIdentity::from_auth_snapshot(auth_snapshot),
            require_streaming,
            required_capabilities_hash: stable_json_hash(required_capabilities),
            routing_policy_hash: stable_json_hash(routing_policy),
            request_auth_channel: normalize_text_key(request_auth_channel.unwrap_or_default()),
            scheduler_affinity_epoch,
            preselection_mode,
            use_api_format_alias_match,
            client_session_affinity_hash: client_session_affinity_key(client_session_affinity),
            model_directive_policy_hash: normalize_text_key(model_directive_policy_hash),
        }
    }
}

impl CandidateResolvedPageCacheKey {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        requested_model: &str,
        request_operation: Option<&str>,
        client_api_format: &str,
        require_streaming: bool,
        auth_snapshot: &GatewayAuthApiKeySnapshot,
        required_capabilities: Option<&Value>,
        routing_policy: Option<&ResolvedRoutingPolicy>,
        request_auth_channel: Option<&str>,
        scheduler_affinity_epoch: u64,
        preselection_mode: &'static str,
        use_api_format_alias_match: bool,
        client_session_affinity: Option<&ClientSessionAffinity>,
        model_directive_policy_hash: &str,
        resolution_mode: AiCandidateResolutionMode,
    ) -> Self {
        Self {
            page_key: CandidatePageCacheKey::new(
                requested_model,
                request_operation,
                client_api_format,
                require_streaming,
                auth_snapshot,
                required_capabilities,
                routing_policy,
                request_auth_channel,
                scheduler_affinity_epoch,
                preselection_mode,
                use_api_format_alias_match,
                client_session_affinity,
                model_directive_policy_hash,
            ),
            resolution_mode: resolution_mode_name(resolution_mode),
        }
    }
}

impl CandidatePageAuthIdentity {
    fn from_auth_snapshot(auth_snapshot: &GatewayAuthApiKeySnapshot) -> Self {
        let api_key_id = normalize_text_key(&auth_snapshot.api_key_id);
        if auth_snapshot.api_key_is_standalone {
            Self::Standalone { api_key_id }
        } else {
            Self::UserApiKey {
                user_id: normalize_text_key(&auth_snapshot.user_id),
                api_key_id,
            }
        }
    }
}

#[cfg(not(test))]
static CANDIDATE_PAGE_CACHE_TTL: std::sync::OnceLock<Duration> = std::sync::OnceLock::new();
#[cfg(not(test))]
static CANDIDATE_PAGE_CACHE_STALE_TTL: std::sync::OnceLock<Duration> = std::sync::OnceLock::new();

/// Resolved once per process: the planner asks for these windows on every
/// request, and parsing the environment there is pure overhead.
pub(crate) fn candidate_page_cache_ttl_from_env() -> Duration {
    #[cfg(test)]
    {
        resolve_candidate_page_cache_ttl()
    }
    #[cfg(not(test))]
    {
        *CANDIDATE_PAGE_CACHE_TTL.get_or_init(resolve_candidate_page_cache_ttl)
    }
}

fn resolve_candidate_page_cache_ttl() -> Duration {
    let ttl_ms = std::env::var(CANDIDATE_PAGE_CACHE_TTL_ENV)
        .ok()
        .and_then(|value| value.trim().parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_CANDIDATE_PAGE_CACHE_TTL_MS)
        .clamp(
            MIN_CANDIDATE_PAGE_CACHE_TTL_MS,
            MAX_CANDIDATE_PAGE_CACHE_TTL_MS,
        );
    Duration::from_millis(ttl_ms)
}

/// The oldest a candidate page may ever be served, whatever its entry says.
///
/// Two different mechanisms end up here, and they are not the same thing:
///
/// - a **resync**: the entry is still fresh enough to answer with, and one
///   task re-derives it in the background (`get_or_load_revalidating`); the
///   page a request sees is at most one refresh cadence old;
/// - a **relist-on-miss**: nobody re-derives anything, the entry is simply kept
///   until this age because every node that could have invalidated it might
///   have missed the signal. Serving it is cheaper than a relist and strictly
///   more expensive than a resync — it is the case where the answer is wrong
///   for up to this long.
///
/// The name is deliberate: this bounds staleness, it does not promise
/// freshness. A plain `get_or_load` that is handed this value is choosing the
/// second mechanism.
///
/// The environment knob keeps its historical `..._STALE_TTL_MS` name.
pub(crate) fn candidate_page_cache_max_stale_age(ttl: Duration) -> Duration {
    #[cfg(test)]
    {
        resolve_candidate_page_cache_max_stale_age().max(ttl)
    }
    #[cfg(not(test))]
    {
        (*CANDIDATE_PAGE_CACHE_STALE_TTL.get_or_init(resolve_candidate_page_cache_max_stale_age))
            .max(ttl)
    }
}

fn resolve_candidate_page_cache_max_stale_age() -> Duration {
    let stale_ttl_ms = std::env::var(CANDIDATE_PAGE_CACHE_STALE_TTL_ENV)
        .ok()
        .and_then(|value| value.trim().parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_CANDIDATE_PAGE_CACHE_STALE_TTL_MS)
        .clamp(
            MIN_CANDIDATE_PAGE_CACHE_STALE_TTL_MS,
            MAX_CANDIDATE_PAGE_CACHE_STALE_TTL_MS,
        );
    Duration::from_millis(stale_ttl_ms)
}

pub(crate) fn record_candidate_page_cache_hit() {
    CANDIDATE_PAGE_CACHE_METRICS
        .hit_total
        .fetch_add(1, Ordering::Relaxed);
}

pub(crate) fn record_candidate_page_cache_miss() {
    CANDIDATE_PAGE_CACHE_METRICS
        .miss_total
        .fetch_add(1, Ordering::Relaxed);
}

pub(crate) fn record_candidate_page_cache_load() {
    CANDIDATE_PAGE_CACHE_METRICS
        .load_total
        .fetch_add(1, Ordering::Relaxed);
}

pub(crate) fn record_candidate_page_cache_follower_wait() {
    CANDIDATE_PAGE_CACHE_METRICS
        .follower_wait_total
        .fetch_add(1, Ordering::Relaxed);
}

pub(crate) fn record_candidate_page_cache_none() {
    CANDIDATE_PAGE_CACHE_METRICS
        .none_total
        .fetch_add(1, Ordering::Relaxed);
}

pub(crate) fn record_candidate_page_resolve_cache_hit() {
    CANDIDATE_PAGE_CACHE_METRICS
        .resolve_hit_total
        .fetch_add(1, Ordering::Relaxed);
}

pub(crate) fn record_candidate_page_resolve_cache_miss() {
    CANDIDATE_PAGE_CACHE_METRICS
        .resolve_miss_total
        .fetch_add(1, Ordering::Relaxed);
}

pub(crate) fn record_candidate_page_resolve_cache_load() {
    CANDIDATE_PAGE_CACHE_METRICS
        .resolve_load_total
        .fetch_add(1, Ordering::Relaxed);
}

pub(crate) fn record_candidate_page_resolve_cache_follower_wait() {
    CANDIDATE_PAGE_CACHE_METRICS
        .resolve_follower_wait_total
        .fetch_add(1, Ordering::Relaxed);
}

pub(crate) fn candidate_page_cache_metric_samples() -> Vec<MetricSample> {
    vec![
        MetricSample::new(
            "candidate_page_cache_hit_total",
            "Total candidate page cache hits.",
            MetricKind::Counter,
            CANDIDATE_PAGE_CACHE_METRICS
                .hit_total
                .load(Ordering::Relaxed),
        ),
        MetricSample::new(
            "candidate_page_cache_miss_total",
            "Total candidate page cache misses before singleflight registration.",
            MetricKind::Counter,
            CANDIDATE_PAGE_CACHE_METRICS
                .miss_total
                .load(Ordering::Relaxed),
        ),
        MetricSample::new(
            "candidate_page_cache_load_total",
            "Total candidate page cache loader executions.",
            MetricKind::Counter,
            CANDIDATE_PAGE_CACHE_METRICS
                .load_total
                .load(Ordering::Relaxed),
        ),
        MetricSample::new(
            "candidate_page_cache_follower_wait_total",
            "Total candidate page cache requests that waited for another loader.",
            MetricKind::Counter,
            CANDIDATE_PAGE_CACHE_METRICS
                .follower_wait_total
                .load(Ordering::Relaxed),
        ),
        MetricSample::new(
            "candidate_page_cache_none_total",
            "Total candidate page cache lookups that resolved to no page.",
            MetricKind::Counter,
            CANDIDATE_PAGE_CACHE_METRICS
                .none_total
                .load(Ordering::Relaxed),
        ),
        MetricSample::new(
            "candidate_page_resolve_cache_hit_total",
            "Total resolved candidate page cache hits.",
            MetricKind::Counter,
            CANDIDATE_PAGE_CACHE_METRICS
                .resolve_hit_total
                .load(Ordering::Relaxed),
        ),
        MetricSample::new(
            "candidate_page_resolve_cache_miss_total",
            "Total resolved candidate page cache misses before singleflight registration.",
            MetricKind::Counter,
            CANDIDATE_PAGE_CACHE_METRICS
                .resolve_miss_total
                .load(Ordering::Relaxed),
        ),
        MetricSample::new(
            "candidate_page_resolve_cache_load_total",
            "Total resolved candidate page cache loader executions.",
            MetricKind::Counter,
            CANDIDATE_PAGE_CACHE_METRICS
                .resolve_load_total
                .load(Ordering::Relaxed),
        ),
        MetricSample::new(
            "candidate_page_resolve_cache_follower_wait_total",
            "Total resolved candidate page cache requests that waited for another loader.",
            MetricKind::Counter,
            CANDIDATE_PAGE_CACHE_METRICS
                .resolve_follower_wait_total
                .load(Ordering::Relaxed),
        ),
    ]
}

fn normalize_text_key(value: &str) -> String {
    value.trim().to_string()
}

fn client_session_affinity_key(affinity: Option<&ClientSessionAffinity>) -> String {
    let Some(affinity) = affinity else {
        return String::new();
    };
    let family = affinity
        .client_family
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_ascii_lowercase)
        .unwrap_or_default();
    let session = affinity
        .session_key
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(sha256_hex)
        .unwrap_or_default();
    format!("{family}:{session}")
}

fn stable_json_hash<T>(value: Option<&T>) -> String
where
    T: serde::Serialize,
{
    let Some(value) = value else {
        return String::new();
    };
    match serde_json::to_vec(value) {
        Ok(serialized) => sha256_hex(&serialized),
        Err(_) => {
            let mut hasher = DefaultHasher::new();
            std::any::type_name::<T>().hash(&mut hasher);
            format!("fallback:{:016x}", hasher.finish())
        }
    }
}

fn sha256_hex(value: impl AsRef<[u8]>) -> String {
    let digest = sha2::Sha256::digest(value.as_ref());
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn resolution_mode_name(mode: AiCandidateResolutionMode) -> &'static str {
    match mode {
        AiCandidateResolutionMode::Standard => "standard",
        AiCandidateResolutionMode::WithoutTransportPairGate => "without_transport_pair_gate",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aether_data::repository::auth::ResolvedAuthApiKeySnapshot;
    use serde_json::json;
    use std::sync::Mutex;

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    fn auth_snapshot(user_id: &str, api_key_id: &str) -> ResolvedAuthApiKeySnapshot {
        ResolvedAuthApiKeySnapshot {
            user_id: user_id.to_string(),
            username: "user".to_string(),
            email: None,
            user_role: "user".to_string(),
            user_auth_source: "local".to_string(),
            user_is_active: true,
            user_is_deleted: false,
            user_rate_limit: None,
            user_allowed_providers: None,
            user_allowed_api_formats: None,
            user_allowed_models: None,
            api_key_id: api_key_id.to_string(),
            api_key_name: None,
            api_key_is_active: true,
            api_key_is_locked: false,
            api_key_is_standalone: false,
            api_key_rate_limit: None,
            api_key_concurrent_limit: None,
            api_key_expires_at_unix_secs: None,
            api_key_allowed_providers: None,
            api_key_allowed_api_formats: None,
            api_key_allowed_models: None,
            api_key_ip_rules: None,
            currently_usable: true,
        }
    }

    #[test]
    fn candidate_page_cache_max_stale_age_defaults_longer_than_burst_gap() {
        let _guard = ENV_LOCK.lock().expect("env lock poisoned");
        std::env::remove_var(CANDIDATE_PAGE_CACHE_STALE_TTL_ENV);

        assert_eq!(
            candidate_page_cache_max_stale_age(Duration::from_millis(250)),
            Duration::from_millis(DEFAULT_CANDIDATE_PAGE_CACHE_STALE_TTL_MS)
        );
    }

    #[test]
    fn candidate_page_cache_max_stale_age_respects_env_and_never_under_fresh_ttl() {
        let _guard = ENV_LOCK.lock().expect("env lock poisoned");
        std::env::set_var(CANDIDATE_PAGE_CACHE_STALE_TTL_ENV, "100");
        assert_eq!(
            candidate_page_cache_max_stale_age(Duration::from_millis(2_500)),
            Duration::from_millis(2_500)
        );

        std::env::set_var(CANDIDATE_PAGE_CACHE_STALE_TTL_ENV, "60000");
        assert_eq!(
            candidate_page_cache_max_stale_age(Duration::from_millis(250)),
            Duration::from_secs(60)
        );
        std::env::remove_var(CANDIDATE_PAGE_CACHE_STALE_TTL_ENV);
    }

    #[test]
    fn candidate_page_cache_key_isolates_auth_model_format_and_capabilities() {
        let auth_a = auth_snapshot("user-a", "key-a");
        let auth_b = auth_snapshot("user-b", "key-a");
        let base = CandidatePageCacheKey::new(
            "gpt-4o",
            None,
            "openai:chat",
            true,
            &auth_a,
            Some(&json!({"vision": true})),
            None,
            Some("bearer"),
            7,
            "provider_endpoint_key_model",
            true,
            None,
            "policy-a",
        );
        let different_user = CandidatePageCacheKey::new(
            "gpt-4o",
            None,
            "openai:chat",
            true,
            &auth_b,
            Some(&json!({"vision": true})),
            None,
            Some("bearer"),
            7,
            "provider_endpoint_key_model",
            true,
            None,
            "policy-a",
        );
        let different_model = CandidatePageCacheKey::new(
            "gpt-4.1",
            None,
            "openai:chat",
            true,
            &auth_a,
            Some(&json!({"vision": true})),
            None,
            Some("bearer"),
            7,
            "provider_endpoint_key_model",
            true,
            None,
            "policy-a",
        );
        let different_operation = CandidatePageCacheKey::new(
            "gpt-4o",
            Some("compact"),
            "openai:chat",
            true,
            &auth_a,
            Some(&json!({"vision": true})),
            None,
            Some("bearer"),
            7,
            "provider_endpoint_key_model",
            true,
            None,
            "policy-a",
        );
        let different_format = CandidatePageCacheKey::new(
            "gpt-4o",
            None,
            "openai:responses",
            true,
            &auth_a,
            Some(&json!({"vision": true})),
            None,
            Some("bearer"),
            7,
            "provider_endpoint_key_model",
            true,
            None,
            "policy-a",
        );
        let different_capabilities = CandidatePageCacheKey::new(
            "gpt-4o",
            None,
            "openai:chat",
            true,
            &auth_a,
            Some(&json!({"vision": false})),
            None,
            Some("bearer"),
            7,
            "provider_endpoint_key_model",
            true,
            None,
            "policy-a",
        );
        let same_policy = CandidatePageCacheKey::new(
            "gpt-4o",
            None,
            "openai:chat",
            true,
            &auth_a,
            Some(&json!({"vision": true})),
            None,
            Some("bearer"),
            7,
            "provider_endpoint_key_model",
            true,
            None,
            "policy-a",
        );
        let different_policy = CandidatePageCacheKey::new(
            "gpt-4o",
            None,
            "openai:chat",
            true,
            &auth_a,
            Some(&json!({"vision": true})),
            None,
            Some("bearer"),
            7,
            "provider_endpoint_key_model",
            true,
            None,
            "policy-b",
        );

        assert_eq!(base, same_policy);
        assert_ne!(base, different_user);
        assert_ne!(base, different_model);
        assert_ne!(base, different_operation);
        assert_ne!(base, different_format);
        assert_ne!(base, different_capabilities);
        assert_ne!(base, different_policy);

        let resolved_base = CandidateResolvedPageCacheKey::new(
            "gpt-4o",
            None,
            "openai:chat",
            true,
            &auth_a,
            Some(&json!({"vision": true})),
            None,
            Some("bearer"),
            7,
            "provider_endpoint_key_model",
            true,
            None,
            "policy-a",
            AiCandidateResolutionMode::Standard,
        );
        let resolved_same_policy = CandidateResolvedPageCacheKey::new(
            "gpt-4o",
            None,
            "openai:chat",
            true,
            &auth_a,
            Some(&json!({"vision": true})),
            None,
            Some("bearer"),
            7,
            "provider_endpoint_key_model",
            true,
            None,
            "policy-a",
            AiCandidateResolutionMode::Standard,
        );
        let resolved_different_policy = CandidateResolvedPageCacheKey::new(
            "gpt-4o",
            None,
            "openai:chat",
            true,
            &auth_a,
            Some(&json!({"vision": true})),
            None,
            Some("bearer"),
            7,
            "provider_endpoint_key_model",
            true,
            None,
            "policy-b",
            AiCandidateResolutionMode::Standard,
        );
        assert_eq!(resolved_base, resolved_same_policy);
        assert_ne!(resolved_base, resolved_different_policy);
    }
}
