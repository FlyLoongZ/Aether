use std::collections::BTreeMap;
use std::sync::Arc;

use aether_contracts::{ExecutionPlan, RequestBody};
use aether_crypto::{encrypt_python_fernet_plaintext, DEVELOPMENT_ENCRYPTION_KEY};
use aether_data::repository::candidates::InMemoryRequestCandidateRepository;
use aether_data::repository::provider_catalog::InMemoryProviderCatalogReadRepository;
use aether_data_contracts::repository::candidates::{
    RequestCandidateStatus, StoredRequestCandidate,
};
use aether_data_contracts::repository::pool_scores::PoolMemberHardState;
use aether_data_contracts::repository::provider_catalog::{
    ProviderCatalogKeyAdaptiveState, StoredProviderCatalogEndpoint, StoredProviderCatalogKey,
    StoredProviderCatalogProvider,
};
use aether_test_support::ManagedRedisServer;
use aether_usage_runtime::GatewaySyncReportRequest;
use serde_json::{json, Value};

use super::{
    apply_local_execution_effect, apply_local_stream_failure_effects,
    apply_local_stream_success_effects, execution_plan_bearer_matches_transport,
    local_candidate_failure_should_apply_key_effects,
    local_candidate_failure_should_record_pool_error, pool_score_feedback_gate_allows,
    pool_score_hard_state_for_status, resolve_pool_feedback_context, LocalAdaptiveRateLimitEffect,
    LocalAdaptiveSuccessEffect, LocalAttemptFailureEffect, LocalExecutionEffect,
    LocalExecutionEffectContext, LocalHealthFailureEffect, LocalHealthSuccessEffect,
    LocalOAuthInvalidationEffect, LocalOAuthSuccessEffect, LocalPoolErrorEffect,
    LocalStreamFailureEffect, ProviderKeyEffectLockPool,
};
use crate::data::{GatewayDataConfig, GatewayDataState};
use crate::orchestration::{
    apply_local_report_effect, LocalFailoverClassification, LocalReportEffect,
};
use crate::scheduler::affinity::SCHEDULER_AFFINITY_TTL;
use crate::usage::GatewayStreamReportRequest;
use crate::AppState;
use aether_scheduler_core::{
    build_scheduler_affinity_cache_key_for_api_key_id,
    build_scheduler_affinity_cache_key_for_api_key_id_with_client_session,
    build_scheduler_affinity_cache_key_for_api_key_id_with_client_session_and_scope,
    ClientSessionAffinity, SchedulerAffinityScope, SchedulerAffinityTarget,
};

async fn start_managed_redis_or_skip() -> Option<ManagedRedisServer> {
    match ManagedRedisServer::start().await {
        Ok(server) => Some(server),
        Err(err) if err.to_string().contains("No such file or directory") => {
            eprintln!("skipping redis-backed orchestration effect test: {err}");
            None
        }
        Err(err) => panic!("redis server should start: {err}"),
    }
}

fn sample_plan() -> ExecutionPlan {
    ExecutionPlan {
        request_id: "req-1".to_string(),
        candidate_id: Some("cand-1".to_string()),
        provider_name: Some("openai".to_string()),
        provider_id: "prov-1".to_string(),
        endpoint_id: "ep-1".to_string(),
        key_id: "key-1".to_string(),
        method: "POST".to_string(),
        url: "https://example.com/v1/chat/completions".to_string(),
        headers: BTreeMap::new(),
        content_type: Some("application/json".to_string()),
        content_encoding: None,
        body: RequestBody::from_json(json!({"model":"gpt-5"})),
        stream: false,
        client_api_format: "openai:chat".to_string(),
        provider_api_format: "openai:chat".to_string(),
        model_name: Some("gpt-5".to_string()),
        proxy: None,
        transport_profile: None,
        timeouts: None,
    }
}

fn sample_claude_plan() -> ExecutionPlan {
    let mut plan = sample_plan();
    plan.provider_name = Some("anthropic".to_string());
    plan.provider_api_format = "claude:messages".to_string();
    plan
}

fn sample_stream_report() -> GatewayStreamReportRequest {
    GatewayStreamReportRequest {
        trace_id: "trace-stream-effects".to_string(),
        report_kind: "openai_chat_stream_success".to_string(),
        report_context: None,
        status_code: 200,
        headers: BTreeMap::new(),
        provider_body_base64: None,
        provider_body_state: None,
        client_body_base64: None,
        client_body_state: None,
        terminal_summary: None,
        telemetry: None,
    }
}

#[test]
fn pool_score_feedback_gate_suppresses_repeated_success_writes() {
    super::POOL_SCORE_FEEDBACK_GATE.clear();
    let plan = sample_plan();

    assert!(pool_score_feedback_gate_allows(
        &plan,
        Some(true),
        Some(PoolMemberHardState::Available),
        Some(50),
    ));
    assert!(!pool_score_feedback_gate_allows(
        &plan,
        Some(true),
        Some(PoolMemberHardState::Available),
        Some(50),
    ));
    assert!(pool_score_feedback_gate_allows(
        &plan,
        Some(false),
        Some(PoolMemberHardState::Cooldown),
        Some(-500),
    ));
}

#[test]
fn provider_key_effect_lock_pool_prunes_geometrically_and_keeps_active_locks() {
    let pool = ProviderKeyEffectLockPool::new(4);
    let locks = (0..10)
        .map(|index| pool.lock_for(&format!("key-{index}")))
        .collect::<Vec<_>>();

    {
        let state = pool.state.lock().expect("effect lock pool should lock");
        assert_eq!(state.entries.len(), 10);
        assert_eq!(state.prune_count, 2);
        assert_eq!(state.next_growth_prune_at, 16);
    }

    for (index, expected) in locks.iter().enumerate() {
        let existing = pool.lock_for(&format!("key-{index}"));
        assert!(Arc::ptr_eq(expected, &existing));
    }

    let hot_lock = Arc::clone(&locks[0]);
    drop(locks);
    for _ in 0..32 {
        let existing = pool.lock_for("key-0");
        assert!(Arc::ptr_eq(&hot_lock, &existing));
    }

    let state = pool.state.lock().expect("effect lock pool should lock");
    assert_eq!(state.entries.len(), 1);
    assert!(state.prune_count >= 3);
}

fn session_affinity() -> ClientSessionAffinity {
    ClientSessionAffinity::new(
        Some("generic".to_string()),
        Some("session=session-1;agent=coder".to_string()),
    )
}

fn session_report_context() -> Value {
    json!({
        "api_key_id": "api-key-1",
        "client_api_format": "openai:chat",
        "model": "gpt-5",
        "scheduler_affinity_policy": {
            "scheduling_mode": "cache_affinity"
        },
        "client_session_affinity": {
            "client_family": "generic",
            "session_key": "session=session-1;agent=coder"
        },
        "original_headers": {
            "x-aether-session-id": "raw-session",
            "x-aether-agent-id": "raw-agent"
        },
        "original_request_body": {
            "model": "gpt-5"
        }
    })
}

fn cache_affinity_report_context() -> Value {
    json!({
        "api_key_id": "api-key-1",
        "client_api_format": "openai:chat",
        "model": "gpt-5",
        "scheduler_affinity_policy": {
            "scheduling_mode": "cache_affinity"
        }
    })
}

fn session_scheduler_affinity_cache_key() -> String {
    build_scheduler_affinity_cache_key_for_api_key_id_with_client_session(
        "api-key-1",
        "openai:chat",
        "gpt-5",
        Some(&session_affinity()),
    )
    .expect("session scheduler affinity cache key should build")
}

fn sample_codex_plan() -> ExecutionPlan {
    ExecutionPlan {
        request_id: "req-codex-1".to_string(),
        candidate_id: Some("cand-codex-1".to_string()),
        provider_name: Some("codex".to_string()),
        provider_id: "provider-codex-cli-local-1".to_string(),
        endpoint_id: "endpoint-codex-cli-local-1".to_string(),
        key_id: "key-codex-cli-local-1".to_string(),
        method: "POST".to_string(),
        url: "https://chatgpt.com/backend-api/codex".to_string(),
        headers: BTreeMap::from([(
            "authorization".to_string(),
            "Bearer codex-access-token".to_string(),
        )]),
        content_type: Some("application/json".to_string()),
        content_encoding: None,
        body: RequestBody::from_json(json!({"model":"gpt-5.4"})),
        stream: false,
        client_api_format: "openai:responses".to_string(),
        provider_api_format: "openai:responses".to_string(),
        model_name: Some("gpt-5.4".to_string()),
        proxy: None,
        transport_profile: None,
        timeouts: None,
    }
}

fn sample_codex_agent_identity_plan() -> ExecutionPlan {
    let mut plan = sample_codex_plan();
    plan.headers.insert(
        "Authorization".to_string(),
        "AgentAssertion in-flight-assertion".to_string(),
    );
    plan
}

fn sample_codex_provider() -> StoredProviderCatalogProvider {
    StoredProviderCatalogProvider::new(
        "provider-codex-cli-local-1".to_string(),
        "codex".to_string(),
        Some("https://chatgpt.com".to_string()),
        "codex".to_string(),
    )
    .expect("provider should build")
    .with_transport_fields(
        true,
        false,
        false,
        None,
        Some(2),
        None,
        Some(20.0),
        None,
        Some(json!({"pool_advanced": {}})),
    )
}

fn sample_codex_provider_with_auto_remove() -> StoredProviderCatalogProvider {
    let mut provider = sample_codex_provider();
    provider.config = Some(json!({
        "pool_advanced": {
            "auto_remove_banned_keys": true
        }
    }));
    provider
}

fn sample_codex_endpoint() -> StoredProviderCatalogEndpoint {
    StoredProviderCatalogEndpoint::new(
        "endpoint-codex-cli-local-1".to_string(),
        "provider-codex-cli-local-1".to_string(),
        "openai:responses".to_string(),
        Some("openai".to_string()),
        Some("cli".to_string()),
        true,
    )
    .expect("endpoint should build")
    .with_transport_fields(
        "https://chatgpt.com/backend-api/codex".to_string(),
        None,
        None,
        Some(2),
        None,
        None,
        None,
        None,
    )
    .expect("endpoint transport should build")
}

fn sample_codex_key() -> StoredProviderCatalogKey {
    let provider_id = "provider-codex-cli-local-1";
    let key_id = "key-codex-cli-local-1";
    let credential_state = AppState::new()
        .expect("credential state should build")
        .with_data_state_for_tests(
            GatewayDataState::disabled().with_encryption_key_for_tests(DEVELOPMENT_ENCRYPTION_KEY),
        );
    let encrypted_api_key = credential_state
        .seal_provider_catalog_key_api_key(provider_id, key_id, "codex-access-token")
        .expect("access token should encrypt");
    let encrypted_auth_config = credential_state
        .seal_provider_catalog_key_auth_config(
            provider_id,
            key_id,
            r#"{"provider_type":"codex","refresh_token":"rt-codex-local-123"}"#,
        )
        .expect("auth config should encrypt");
    StoredProviderCatalogKey::new(
        key_id.to_string(),
        provider_id.to_string(),
        "oauth".to_string(),
        "oauth".to_string(),
        None,
        true,
    )
    .expect("key should build")
    .with_transport_fields(
        Some(serde_json::json!(["openai:responses"])),
        encrypted_api_key,
        Some(encrypted_auth_config),
        None,
        Some(serde_json::json!({"openai:responses": 1})),
        None,
        None,
        None,
        None,
    )
    .expect("key transport should build")
}

fn sample_codex_agent_identity_key() -> StoredProviderCatalogKey {
    let mut key = sample_codex_key();
    key.name = "Agent Identity".to_string();
    key.encrypted_api_key = Some(
        encrypt_python_fernet_plaintext(DEVELOPMENT_ENCRYPTION_KEY, "__placeholder__")
            .expect("placeholder api key should encrypt"),
    );
    key.encrypted_auth_config = Some(
        encrypt_python_fernet_plaintext(
            DEVELOPMENT_ENCRYPTION_KEY,
            r#"{"provider_type":"codex","auth_mode":"agentIdentity","agent_runtime_id":"runtime-current","agent_private_key":"MC4CAQAwBQYDK2VwBCIEIAcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcH","task_id":"task-current"}"#,
        )
        .expect("Agent Identity auth config should encrypt"),
    );
    key
}

fn codex_state() -> AppState {
    codex_state_with_provider(sample_codex_provider())
}

fn codex_state_with_auto_remove() -> AppState {
    codex_state_with_provider(sample_codex_provider_with_auto_remove())
}

fn codex_state_with_provider(provider: StoredProviderCatalogProvider) -> AppState {
    codex_state_with_provider_and_key(provider, sample_codex_key())
}

fn codex_state_with_provider_and_key(
    provider: StoredProviderCatalogProvider,
    key: StoredProviderCatalogKey,
) -> AppState {
    let repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![provider],
        vec![sample_codex_endpoint()],
        vec![key],
    ));
    AppState::new()
        .expect("gateway state should build")
        .with_data_state_for_tests(
            GatewayDataState::with_provider_catalog_repository_for_tests(repository)
                .with_encryption_key_for_tests(DEVELOPMENT_ENCRYPTION_KEY),
        )
}

fn claude_code_oauth_state() -> AppState {
    let mut provider = sample_codex_provider();
    provider.name = "claude_code".to_string();
    provider.provider_type = "claude_code".to_string();
    let mut endpoint = sample_codex_endpoint();
    endpoint.api_format = "claude:messages".to_string();
    endpoint.api_family = Some("claude".to_string());
    endpoint.base_url = "https://api.anthropic.com".to_string();
    let mut key = sample_codex_key();
    key.api_formats = Some(json!(["claude:messages"]));
    key.encrypted_auth_config = Some(
        encrypt_python_fernet_plaintext(
            DEVELOPMENT_ENCRYPTION_KEY,
            r#"{"provider_type":"claude_code","refresh_token":"rt-claude-local-123"}"#,
        )
        .expect("Claude Code auth config should encrypt"),
    );
    let repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![provider],
        vec![endpoint],
        vec![key],
    ));
    AppState::new()
        .expect("gateway state should build")
        .with_data_state_for_tests(
            GatewayDataState::with_provider_catalog_repository_for_tests(repository)
                .with_encryption_key_for_tests(DEVELOPMENT_ENCRYPTION_KEY),
        )
}

fn codex_state_with_redis(redis_url: &str, redis_key_prefix: &str) -> AppState {
    let repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![sample_codex_provider()],
        vec![sample_codex_endpoint()],
        vec![sample_codex_key()],
    ));
    let data_state = GatewayDataState::from_config(
        GatewayDataConfig::disabled()
            .with_redis_url(redis_url, Some(redis_key_prefix))
            .with_encryption_key(DEVELOPMENT_ENCRYPTION_KEY),
    )
    .expect("data state should build")
    .attach_provider_catalog_repository_for_tests(repository);
    AppState::new()
        .expect("gateway state should build")
        .with_data_state_for_tests(data_state)
}

fn sample_health_provider() -> StoredProviderCatalogProvider {
    StoredProviderCatalogProvider::new(
        "prov-1".to_string(),
        "openai".to_string(),
        Some("https://example.com".to_string()),
        "custom".to_string(),
    )
    .expect("provider should build")
}

fn sample_pool_health_provider() -> StoredProviderCatalogProvider {
    sample_health_provider().with_transport_fields(
        true,
        false,
        false,
        None,
        None,
        None,
        None,
        None,
        Some(json!({"pool_advanced": {}})),
    )
}

fn sample_health_endpoint() -> StoredProviderCatalogEndpoint {
    StoredProviderCatalogEndpoint::new(
        "ep-1".to_string(),
        "prov-1".to_string(),
        "openai:chat".to_string(),
        Some("openai".to_string()),
        Some("chat".to_string()),
        true,
    )
    .expect("endpoint should build")
    .with_transport_fields(
        "https://example.com/v1/chat/completions".to_string(),
        None,
        None,
        Some(2),
        None,
        None,
        None,
        None,
    )
    .expect("endpoint transport should build")
}

fn sample_health_key() -> StoredProviderCatalogKey {
    StoredProviderCatalogKey::new(
        "key-1".to_string(),
        "prov-1".to_string(),
        "prod".to_string(),
        "api_key".to_string(),
        None,
        true,
    )
    .expect("key should build")
    .with_transport_fields(
        Some(serde_json::json!(["openai:chat"])),
        encrypt_python_fernet_plaintext(DEVELOPMENT_ENCRYPTION_KEY, "sk-test")
            .expect("api key should encrypt"),
        None,
        None,
        Some(serde_json::json!({"openai:chat": 1})),
        None,
        None,
        None,
        None,
    )
    .expect("key transport should build")
}

fn health_state() -> AppState {
    let repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![sample_health_provider()],
        vec![sample_health_endpoint()],
        vec![sample_health_key()],
    ));
    AppState::new()
        .expect("gateway state should build")
        .with_data_state_for_tests(
            GatewayDataState::with_provider_catalog_repository_for_tests(repository)
                .with_encryption_key_for_tests(DEVELOPMENT_ENCRYPTION_KEY),
        )
}

fn pool_health_state() -> AppState {
    let repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![sample_pool_health_provider()],
        vec![sample_health_endpoint()],
        vec![sample_health_key()],
    ));
    AppState::new()
        .expect("gateway state should build")
        .with_data_state_for_tests(
            GatewayDataState::with_provider_catalog_repository_for_tests(repository)
                .with_encryption_key_for_tests(DEVELOPMENT_ENCRYPTION_KEY),
        )
}

#[tokio::test]
async fn pool_feedback_uses_routing_profile_scheduling_override() {
    let mut provider = sample_pool_health_provider();
    provider.config = Some(json!({
        "pool_advanced": {
            "scheduling_presets": [{
                "preset": "lru",
                "enabled": true
            }]
        }
    }));
    let repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![provider],
        vec![sample_health_endpoint()],
        vec![sample_health_key()],
    ));
    let state = AppState::new()
        .expect("gateway state should build")
        .with_data_state_for_tests(
            GatewayDataState::with_provider_catalog_repository_for_tests(repository)
                .with_encryption_key_for_tests(DEVELOPMENT_ENCRYPTION_KEY),
        );
    let plan = sample_plan();
    let report_context = json!({
        "routing_pool_policy_override": {
            "scheduling_presets": [{
                "preset": "cache_affinity",
                "enabled": true
            }]
        }
    });

    let feedback = resolve_pool_feedback_context(
        &state,
        LocalExecutionEffectContext {
            plan: &plan,
            report_context: Some(&report_context),
        },
    )
    .await
    .expect("pool feedback context should resolve");

    assert!(
        crate::handlers::shared::provider_pool::admin_provider_pool_cache_affinity_enabled(
            &feedback.pool_config
        )
    );
    assert!(!feedback.pool_config.lru_enabled);
    assert_eq!(feedback.pool_config.scheduling_presets.len(), 1);
    assert_eq!(
        feedback.pool_config.scheduling_presets[0].preset,
        "cache_affinity"
    );
}

fn health_state_with_key(key: StoredProviderCatalogKey) -> AppState {
    let repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![sample_health_provider()],
        vec![sample_health_endpoint()],
        vec![key],
    ));
    AppState::new()
        .expect("gateway state should build")
        .with_data_state_for_tests(
            GatewayDataState::with_provider_catalog_repository_for_tests(repository)
                .with_encryption_key_for_tests(DEVELOPMENT_ENCRYPTION_KEY),
        )
}

fn sample_adaptive_key() -> StoredProviderCatalogKey {
    let mut key = sample_health_key();
    key.name = "adaptive".to_string();
    key.rpm_limit = None;
    key.learned_rpm_limit = Some(12);
    key.rpm_429_count = Some(1);
    key
}

fn adaptive_state() -> AppState {
    let repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![sample_health_provider()],
        vec![sample_health_endpoint()],
        vec![sample_adaptive_key()],
    ));
    AppState::new()
        .expect("gateway state should build")
        .with_data_state_for_tests(
            GatewayDataState::with_provider_catalog_repository_for_tests(repository)
                .with_encryption_key_for_tests(DEVELOPMENT_ENCRYPTION_KEY),
        )
}

fn adaptive_state_with_request_candidates(
    key: StoredProviderCatalogKey,
    request_candidates: Vec<StoredRequestCandidate>,
) -> AppState {
    let provider_catalog = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![sample_health_provider()],
        vec![sample_health_endpoint()],
        vec![key],
    ));
    let request_candidates = Arc::new(InMemoryRequestCandidateRepository::seed(request_candidates));
    AppState::new()
        .expect("gateway state should build")
        .with_data_state_for_tests(
            GatewayDataState::with_provider_catalog_repository_for_tests(provider_catalog)
                .with_request_candidate_reader(request_candidates)
                .with_encryption_key_for_tests(DEVELOPMENT_ENCRYPTION_KEY),
        )
}

fn fixed_limit_state() -> AppState {
    let mut key = sample_health_key();
    key.rpm_limit = Some(24);

    let repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![sample_health_provider()],
        vec![sample_health_endpoint()],
        vec![key],
    ));
    AppState::new()
        .expect("gateway state should build")
        .with_data_state_for_tests(
            GatewayDataState::with_provider_catalog_repository_for_tests(repository)
                .with_encryption_key_for_tests(DEVELOPMENT_ENCRYPTION_KEY),
        )
}

#[tokio::test]
async fn attempt_failure_invalidates_scheduler_affinity_cache() {
    let state = AppState::new().expect("gateway state should build");
    let plan = sample_plan();
    let report_context = json!({
        "api_key_id": "api-key-1",
        "client_api_format": "openai:chat",
        "model": "gpt-5",
    });
    let cache_key =
        build_scheduler_affinity_cache_key_for_api_key_id("api-key-1", "openai:chat", "gpt-5")
            .expect("scheduler affinity cache key should build");

    state.remember_scheduler_affinity_target(
        &cache_key,
        SchedulerAffinityTarget {
            provider_id: "prov-1".to_string(),
            endpoint_id: "ep-1".to_string(),
            key_id: "key-1".to_string(),
        },
        SCHEDULER_AFFINITY_TTL,
        16,
    );
    assert!(state
        .read_scheduler_affinity_target(cache_key.as_str(), SCHEDULER_AFFINITY_TTL)
        .is_some());

    apply_local_execution_effect(
        &state,
        LocalExecutionEffectContext {
            plan: &plan,
            report_context: Some(&report_context),
        },
        LocalExecutionEffect::AttemptFailure(LocalAttemptFailureEffect {
            status_code: 429,
            classification: LocalFailoverClassification::RetryUpstreamFailure,
        }),
    )
    .await;

    assert!(state
        .read_scheduler_affinity_target(cache_key.as_str(), SCHEDULER_AFFINITY_TTL)
        .is_none());
}

#[tokio::test]
async fn attempt_failure_invalidates_session_scoped_scheduler_affinity_cache() {
    let state = AppState::new().expect("gateway state should build");
    let plan = sample_plan();
    let report_context = session_report_context();
    let session_cache_key = session_scheduler_affinity_cache_key();
    let legacy_cache_key =
        build_scheduler_affinity_cache_key_for_api_key_id("api-key-1", "openai:chat", "gpt-5")
            .expect("legacy scheduler affinity cache key should build");

    for cache_key in [&session_cache_key, &legacy_cache_key] {
        state.remember_scheduler_affinity_target(
            cache_key.as_str(),
            SchedulerAffinityTarget {
                provider_id: "prov-1".to_string(),
                endpoint_id: "ep-1".to_string(),
                key_id: "key-1".to_string(),
            },
            SCHEDULER_AFFINITY_TTL,
            16,
        );
    }

    apply_local_execution_effect(
        &state,
        LocalExecutionEffectContext {
            plan: &plan,
            report_context: Some(&report_context),
        },
        LocalExecutionEffect::AttemptFailure(LocalAttemptFailureEffect {
            status_code: 429,
            classification: LocalFailoverClassification::RetryUpstreamFailure,
        }),
    )
    .await;

    assert!(state
        .read_scheduler_affinity_target(session_cache_key.as_str(), SCHEDULER_AFFINITY_TTL)
        .is_none());
    assert!(state
        .read_scheduler_affinity_target(legacy_cache_key.as_str(), SCHEDULER_AFFINITY_TTL)
        .is_some());
}

#[tokio::test]
async fn attempt_failure_keeps_scheduler_affinity_for_non_affinity_candidate() {
    let state = AppState::new().expect("gateway state should build");
    let plan = sample_plan();
    let report_context = json!({
        "api_key_id": "api-key-1",
        "client_api_format": "openai:chat",
        "model": "gpt-5",
    });
    let cache_key =
        build_scheduler_affinity_cache_key_for_api_key_id("api-key-1", "openai:chat", "gpt-5")
            .expect("scheduler affinity cache key should build");
    let affinity_target = SchedulerAffinityTarget {
        provider_id: "prov-2".to_string(),
        endpoint_id: "ep-2".to_string(),
        key_id: "key-2".to_string(),
    };

    state.remember_scheduler_affinity_target(
        &cache_key,
        affinity_target.clone(),
        SCHEDULER_AFFINITY_TTL,
        16,
    );

    apply_local_execution_effect(
        &state,
        LocalExecutionEffectContext {
            plan: &plan,
            report_context: Some(&report_context),
        },
        LocalExecutionEffect::AttemptFailure(LocalAttemptFailureEffect {
            status_code: 524,
            classification: LocalFailoverClassification::RetryUpstreamFailure,
        }),
    )
    .await;

    assert_eq!(
        state.read_scheduler_affinity_target(cache_key.as_str(), SCHEDULER_AFFINITY_TTL),
        Some(affinity_target)
    );
}

#[tokio::test]
async fn attempt_failure_keeps_scheduler_affinity_for_non_pool_sibling_key() {
    let state = health_state();
    let plan = sample_plan();
    let report_context = json!({
        "api_key_id": "api-key-1",
        "client_api_format": "openai:chat",
        "model": "gpt-5",
    });
    let cache_key =
        build_scheduler_affinity_cache_key_for_api_key_id("api-key-1", "openai:chat", "gpt-5")
            .expect("scheduler affinity cache key should build");
    let affinity_target = SchedulerAffinityTarget {
        provider_id: "prov-1".to_string(),
        endpoint_id: "ep-1".to_string(),
        key_id: "key-2".to_string(),
    };

    state.remember_scheduler_affinity_target(
        &cache_key,
        affinity_target.clone(),
        SCHEDULER_AFFINITY_TTL,
        16,
    );

    apply_local_execution_effect(
        &state,
        LocalExecutionEffectContext {
            plan: &plan,
            report_context: Some(&report_context),
        },
        LocalExecutionEffect::AttemptFailure(LocalAttemptFailureEffect {
            status_code: 524,
            classification: LocalFailoverClassification::RetryUpstreamFailure,
        }),
    )
    .await;

    assert_eq!(
        state.read_scheduler_affinity_target(cache_key.as_str(), SCHEDULER_AFFINITY_TTL),
        Some(affinity_target)
    );
}

#[tokio::test]
async fn attempt_failure_invalidates_scheduler_affinity_for_same_pool_candidate() {
    let state = pool_health_state();
    let plan = sample_plan();
    let report_context = json!({
        "api_key_id": "api-key-1",
        "client_api_format": "openai:chat",
        "model": "gpt-5",
    });
    let cache_key =
        build_scheduler_affinity_cache_key_for_api_key_id("api-key-1", "openai:chat", "gpt-5")
            .expect("scheduler affinity cache key should build");

    state.remember_scheduler_affinity_target(
        &cache_key,
        SchedulerAffinityTarget {
            provider_id: "prov-1".to_string(),
            endpoint_id: "ep-1".to_string(),
            key_id: "key-2".to_string(),
        },
        SCHEDULER_AFFINITY_TTL,
        16,
    );

    apply_local_execution_effect(
        &state,
        LocalExecutionEffectContext {
            plan: &plan,
            report_context: Some(&report_context),
        },
        LocalExecutionEffect::AttemptFailure(LocalAttemptFailureEffect {
            status_code: 524,
            classification: LocalFailoverClassification::RetryUpstreamFailure,
        }),
    )
    .await;

    assert_eq!(
        state.read_scheduler_affinity_target(cache_key.as_str(), SCHEDULER_AFFINITY_TTL),
        None
    );
}

#[tokio::test]
async fn attempt_failure_keeps_scheduler_affinity_for_non_failure_status() {
    let state = AppState::new().expect("gateway state should build");
    let plan = sample_plan();
    let report_context = json!({
        "api_key_id": "api-key-1",
        "client_api_format": "openai:chat",
        "model": "gpt-5",
    });
    let cache_key =
        build_scheduler_affinity_cache_key_for_api_key_id("api-key-1", "openai:chat", "gpt-5")
            .expect("scheduler affinity cache key should build");

    state.remember_scheduler_affinity_target(
        &cache_key,
        SchedulerAffinityTarget {
            provider_id: "prov-1".to_string(),
            endpoint_id: "ep-1".to_string(),
            key_id: "key-1".to_string(),
        },
        SCHEDULER_AFFINITY_TTL,
        16,
    );

    apply_local_execution_effect(
        &state,
        LocalExecutionEffectContext {
            plan: &plan,
            report_context: Some(&report_context),
        },
        LocalExecutionEffect::AttemptFailure(LocalAttemptFailureEffect {
            status_code: 200,
            classification: LocalFailoverClassification::UseDefault,
        }),
    )
    .await;

    assert!(state
        .read_scheduler_affinity_target(cache_key.as_str(), SCHEDULER_AFFINITY_TTL)
        .is_some());
}

#[tokio::test]
async fn stream_success_effect_helper_projects_health_and_scheduler_affinity() {
    let state = AppState::new().expect("gateway state should build");
    let plan = sample_plan();
    let report_context = cache_affinity_report_context();
    let cache_key =
        build_scheduler_affinity_cache_key_for_api_key_id("api-key-1", "openai:chat", "gpt-5")
            .expect("scheduler affinity cache key should build");
    let payload = sample_stream_report();

    apply_local_stream_success_effects(
        &state,
        LocalExecutionEffectContext {
            plan: &plan,
            report_context: Some(&report_context),
        },
        &payload,
    )
    .await;

    assert_eq!(
        state.read_scheduler_affinity_target(cache_key.as_str(), SCHEDULER_AFFINITY_TTL),
        Some(SchedulerAffinityTarget {
            provider_id: "prov-1".to_string(),
            endpoint_id: "ep-1".to_string(),
            key_id: "key-1".to_string(),
        })
    );
}

#[tokio::test]
async fn stream_failure_effect_helper_returns_analysis_and_projects_health() {
    let state = health_state();
    let plan = sample_plan();
    let headers = BTreeMap::new();
    let analysis = apply_local_stream_failure_effects(
        &state,
        LocalExecutionEffectContext {
            plan: &plan,
            report_context: None,
        },
        LocalStreamFailureEffect::new(503, &headers, Some("upstream unavailable"))
            .with_stream_timeout(),
    )
    .await;

    assert_eq!(
        analysis.classification,
        LocalFailoverClassification::UseDefault
    );
    assert_eq!(analysis.decision.as_str(), "use_default");
    let stored_key = state
        .read_provider_catalog_keys_by_ids(std::slice::from_ref(&plan.key_id))
        .await
        .expect("provider catalog keys should load")
        .into_iter()
        .next()
        .expect("stored key should exist");
    assert_eq!(
        stored_key
            .health_by_format
            .as_ref()
            .and_then(|value| value.get("openai:chat"))
            .and_then(|value| value.get("consecutive_failures"))
            .and_then(Value::as_u64),
        Some(1)
    );
}

#[tokio::test]
async fn configured_stop_pattern_keeps_scheduler_affinity_cache() {
    let state = AppState::new().expect("gateway state should build");
    let plan = sample_plan();
    let report_context = json!({
        "api_key_id": "api-key-1",
        "client_api_format": "openai:chat",
        "model": "gpt-5",
    });
    let cache_key =
        build_scheduler_affinity_cache_key_for_api_key_id("api-key-1", "openai:chat", "gpt-5")
            .expect("scheduler affinity cache key should build");

    state.remember_scheduler_affinity_target(
        &cache_key,
        SchedulerAffinityTarget {
            provider_id: "prov-1".to_string(),
            endpoint_id: "ep-1".to_string(),
            key_id: "key-1".to_string(),
        },
        SCHEDULER_AFFINITY_TTL,
        16,
    );

    apply_local_execution_effect(
        &state,
        LocalExecutionEffectContext {
            plan: &plan,
            report_context: Some(&report_context),
        },
        LocalExecutionEffect::AttemptFailure(LocalAttemptFailureEffect {
            status_code: 400,
            classification: LocalFailoverClassification::StopErrorPattern,
        }),
    )
    .await;

    assert!(state
        .read_scheduler_affinity_target(cache_key.as_str(), SCHEDULER_AFFINITY_TTL)
        .is_some());
}

#[tokio::test]
async fn success_remembers_scheduler_affinity_cache_for_final_candidate() {
    let state = AppState::new().expect("gateway state should build");
    let plan = sample_plan();
    let report_context = cache_affinity_report_context();
    let cache_key =
        build_scheduler_affinity_cache_key_for_api_key_id("api-key-1", "openai:chat", "gpt-5")
            .expect("scheduler affinity cache key should build");

    apply_local_execution_effect(
        &state,
        LocalExecutionEffectContext {
            plan: &plan,
            report_context: Some(&report_context),
        },
        LocalExecutionEffect::HealthSuccess(LocalHealthSuccessEffect),
    )
    .await;

    assert_eq!(
        state.read_scheduler_affinity_target(cache_key.as_str(), SCHEDULER_AFFINITY_TTL),
        Some(SchedulerAffinityTarget {
            provider_id: "prov-1".to_string(),
            endpoint_id: "ep-1".to_string(),
            key_id: "key-1".to_string(),
        })
    );
}

#[tokio::test]
async fn routing_profile_cache_affinity_overrides_legacy_fixed_mode_on_success() {
    let state = AppState::new()
        .expect("gateway state should build")
        .with_data_state_for_tests(
            GatewayDataState::disabled().with_system_config_values_for_tests(vec![(
                "scheduling_mode".to_string(),
                json!("fixed_order"),
            )]),
        );
    let plan = sample_plan();
    let affinity = session_affinity();
    let scope = SchedulerAffinityScope::new("routing-group-1", Some(7));
    let report_context = json!({
        "api_key_id": "api-key-1",
        "client_api_format": "openai:chat",
        "model": "gpt-5",
        "client_session_affinity": {
            "client_family": "generic",
            "session_key": "session=session-1;agent=coder"
        },
        "scheduler_affinity_policy": {
            "scheduling_mode": "cache_affinity",
            "scope": {
                "routing_group_id": "routing-group-1",
                "routing_group_version": 7
            }
        }
    });
    let scoped_cache_key =
        build_scheduler_affinity_cache_key_for_api_key_id_with_client_session_and_scope(
            "api-key-1",
            "openai:chat",
            "gpt-5",
            Some(&affinity),
            Some(&scope),
        )
        .expect("scoped scheduler affinity cache key should build");

    apply_local_execution_effect(
        &state,
        LocalExecutionEffectContext {
            plan: &plan,
            report_context: Some(&report_context),
        },
        LocalExecutionEffect::HealthSuccess(LocalHealthSuccessEffect),
    )
    .await;

    assert_eq!(
        state.read_scheduler_affinity_target(scoped_cache_key.as_str(), SCHEDULER_AFFINITY_TTL),
        Some(SchedulerAffinityTarget {
            provider_id: "prov-1".to_string(),
            endpoint_id: "ep-1".to_string(),
            key_id: "key-1".to_string(),
        })
    );
    assert!(state
        .read_scheduler_affinity_target(
            session_scheduler_affinity_cache_key().as_str(),
            SCHEDULER_AFFINITY_TTL
        )
        .is_none());
}

#[tokio::test]
async fn routing_profile_fixed_mode_overrides_legacy_cache_affinity_on_success() {
    let state = AppState::new().expect("gateway state should build");
    let plan = sample_plan();
    let report_context = json!({
        "api_key_id": "api-key-1",
        "client_api_format": "openai:chat",
        "model": "gpt-5",
        "scheduler_affinity_policy": {
            "scheduling_mode": "fixed_order",
            "scope": {
                "routing_group_id": "routing-group-1",
                "routing_group_version": 7
            }
        }
    });
    let scope = SchedulerAffinityScope::new("routing-group-1", Some(7));
    let scoped_cache_key =
        build_scheduler_affinity_cache_key_for_api_key_id_with_client_session_and_scope(
            "api-key-1",
            "openai:chat",
            "gpt-5",
            None,
            Some(&scope),
        )
        .expect("scoped scheduler affinity cache key should build");

    apply_local_execution_effect(
        &state,
        LocalExecutionEffectContext {
            plan: &plan,
            report_context: Some(&report_context),
        },
        LocalExecutionEffect::HealthSuccess(LocalHealthSuccessEffect),
    )
    .await;

    assert!(state
        .read_scheduler_affinity_target(scoped_cache_key.as_str(), SCHEDULER_AFFINITY_TTL)
        .is_none());
}

#[tokio::test]
async fn malformed_routing_affinity_context_does_not_fall_back_to_legacy_mode() {
    let state = AppState::new().expect("gateway state should build");
    let plan = sample_plan();
    let report_context = json!({
        "api_key_id": "api-key-1",
        "client_api_format": "openai:chat",
        "model": "gpt-5",
        "scheduler_affinity_policy": {
            "scheduling_mode": "unknown"
        }
    });
    let legacy_cache_key =
        build_scheduler_affinity_cache_key_for_api_key_id("api-key-1", "openai:chat", "gpt-5")
            .expect("legacy scheduler affinity cache key should build");

    apply_local_execution_effect(
        &state,
        LocalExecutionEffectContext {
            plan: &plan,
            report_context: Some(&report_context),
        },
        LocalExecutionEffect::HealthSuccess(LocalHealthSuccessEffect),
    )
    .await;

    assert!(state
        .read_scheduler_affinity_target(legacy_cache_key.as_str(), SCHEDULER_AFFINITY_TTL)
        .is_none());
}

#[tokio::test]
async fn health_success_keeps_scheduler_affinity_after_health_state_update() {
    let state = health_state();
    let plan = sample_plan();
    let report_context = cache_affinity_report_context();
    let cache_key =
        build_scheduler_affinity_cache_key_for_api_key_id("api-key-1", "openai:chat", "gpt-5")
            .expect("scheduler affinity cache key should build");

    apply_local_execution_effect(
        &state,
        LocalExecutionEffectContext {
            plan: &plan,
            report_context: Some(&report_context),
        },
        LocalExecutionEffect::HealthSuccess(LocalHealthSuccessEffect),
    )
    .await;

    assert_eq!(
        state.read_scheduler_affinity_target(cache_key.as_str(), SCHEDULER_AFFINITY_TTL),
        Some(SchedulerAffinityTarget {
            provider_id: "prov-1".to_string(),
            endpoint_id: "ep-1".to_string(),
            key_id: "key-1".to_string(),
        })
    );
}

#[tokio::test]
async fn load_balance_success_does_not_remember_scheduler_affinity_cache() {
    let state = AppState::new().expect("gateway state should build");
    let plan = sample_plan();
    let report_context = json!({
        "api_key_id": "api-key-1",
        "client_api_format": "openai:chat",
        "model": "gpt-5",
        "scheduler_affinity_policy": {
            "scheduling_mode": "load_balance"
        }
    });
    let cache_key =
        build_scheduler_affinity_cache_key_for_api_key_id("api-key-1", "openai:chat", "gpt-5")
            .expect("scheduler affinity cache key should build");

    apply_local_execution_effect(
        &state,
        LocalExecutionEffectContext {
            plan: &plan,
            report_context: Some(&report_context),
        },
        LocalExecutionEffect::HealthSuccess(LocalHealthSuccessEffect),
    )
    .await;

    assert!(state
        .read_scheduler_affinity_target(cache_key.as_str(), SCHEDULER_AFFINITY_TTL)
        .is_none());
}

#[tokio::test]
async fn success_remembers_session_scoped_scheduler_affinity_cache() {
    let state = AppState::new().expect("gateway state should build");
    let plan = sample_plan();
    let report_context = session_report_context();
    let session_cache_key = session_scheduler_affinity_cache_key();
    let legacy_cache_key =
        build_scheduler_affinity_cache_key_for_api_key_id("api-key-1", "openai:chat", "gpt-5")
            .expect("legacy scheduler affinity cache key should build");

    apply_local_execution_effect(
        &state,
        LocalExecutionEffectContext {
            plan: &plan,
            report_context: Some(&report_context),
        },
        LocalExecutionEffect::HealthSuccess(LocalHealthSuccessEffect),
    )
    .await;

    assert_eq!(
        state.read_scheduler_affinity_target(session_cache_key.as_str(), SCHEDULER_AFFINITY_TTL),
        Some(SchedulerAffinityTarget {
            provider_id: "prov-1".to_string(),
            endpoint_id: "ep-1".to_string(),
            key_id: "key-1".to_string(),
        })
    );
    assert!(state
        .read_scheduler_affinity_target(legacy_cache_key.as_str(), SCHEDULER_AFFINITY_TTL)
        .is_none());
}

#[tokio::test]
async fn fallback_success_rewarms_scheduler_affinity_after_failed_candidate_invalidates() {
    let state = AppState::new().expect("gateway state should build");
    let failed_plan = sample_plan();
    let mut success_plan = sample_plan();
    success_plan.provider_id = "prov-2".to_string();
    success_plan.endpoint_id = "ep-2".to_string();
    success_plan.key_id = "key-2".to_string();
    let report_context = cache_affinity_report_context();
    let cache_key =
        build_scheduler_affinity_cache_key_for_api_key_id("api-key-1", "openai:chat", "gpt-5")
            .expect("scheduler affinity cache key should build");

    state.remember_scheduler_affinity_target(
        &cache_key,
        SchedulerAffinityTarget {
            provider_id: "prov-1".to_string(),
            endpoint_id: "ep-1".to_string(),
            key_id: "key-1".to_string(),
        },
        SCHEDULER_AFFINITY_TTL,
        16,
    );
    apply_local_execution_effect(
        &state,
        LocalExecutionEffectContext {
            plan: &failed_plan,
            report_context: Some(&report_context),
        },
        LocalExecutionEffect::AttemptFailure(LocalAttemptFailureEffect {
            status_code: 429,
            classification: LocalFailoverClassification::RetryUpstreamFailure,
        }),
    )
    .await;
    assert!(state
        .read_scheduler_affinity_target(cache_key.as_str(), SCHEDULER_AFFINITY_TTL)
        .is_none());

    apply_local_execution_effect(
        &state,
        LocalExecutionEffectContext {
            plan: &success_plan,
            report_context: Some(&report_context),
        },
        LocalExecutionEffect::HealthSuccess(LocalHealthSuccessEffect),
    )
    .await;

    assert_eq!(
        state.read_scheduler_affinity_target(cache_key.as_str(), SCHEDULER_AFFINITY_TTL),
        Some(SchedulerAffinityTarget {
            provider_id: "prov-2".to_string(),
            endpoint_id: "ep-2".to_string(),
            key_id: "key-2".to_string(),
        })
    );
}

#[test]
fn configured_stop_pattern_does_not_penalize_pool_feedback() {
    assert!(!local_candidate_failure_should_record_pool_error(
        LocalFailoverClassification::StopErrorPattern,
        400,
    ));
    assert!(!local_candidate_failure_should_record_pool_error(
        LocalFailoverClassification::RetryUpstreamFailure,
        400,
    ));
    assert!(local_candidate_failure_should_record_pool_error(
        LocalFailoverClassification::RetryUpstreamFailure,
        429,
    ));
}

#[test]
fn anthropic_non_credential_failures_do_not_apply_key_wide_effects() {
    assert!(!local_candidate_failure_should_apply_key_effects(
        "claude:messages",
        LocalFailoverClassification::RetryUpstreamFailure,
        529,
    ));
    assert!(!local_candidate_failure_should_apply_key_effects(
        "claude:messages",
        LocalFailoverClassification::RetryUpstreamFailure,
        429,
    ));
    assert!(!local_candidate_failure_should_apply_key_effects(
        "claude:messages",
        LocalFailoverClassification::RetryUpstreamFailure,
        503,
    ));
    assert!(local_candidate_failure_should_apply_key_effects(
        "claude:messages",
        LocalFailoverClassification::RetryUpstreamFailure,
        401,
    ));
    assert!(local_candidate_failure_should_apply_key_effects(
        "claude:messages",
        LocalFailoverClassification::RetryUpstreamFailure,
        403,
    ));
    assert!(!local_candidate_failure_should_apply_key_effects(
        "claude:messages",
        LocalFailoverClassification::RetryUpstreamFailure,
        400,
    ));
    assert!(local_candidate_failure_should_apply_key_effects(
        "openai:chat",
        LocalFailoverClassification::RetryUpstreamFailure,
        529,
    ));
    assert!(local_candidate_failure_should_apply_key_effects(
        "openai:chat",
        LocalFailoverClassification::RetryUpstreamFailure,
        429,
    ));
    assert!(local_candidate_failure_should_apply_key_effects(
        "openai:chat",
        LocalFailoverClassification::RetryUpstreamFailure,
        503,
    ));
}

#[tokio::test]
async fn anthropic_non_credential_failures_preserve_key_wide_state() {
    for status_code in [400, 429, 503, 529] {
        let mut key = sample_adaptive_key();
        let circuit = json!({
            "openai:chat": {
                "open": true,
                "reason": "existing-state"
            }
        });
        key.circuit_breaker_by_format = Some(circuit.clone());
        let expected_adaptive_state = ProviderCatalogKeyAdaptiveState::from(&key);
        let expected_health = key.health_by_format.clone();
        let repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
            vec![sample_pool_health_provider()],
            vec![sample_health_endpoint()],
            vec![key],
        ));
        let state = AppState::new()
            .expect("gateway state should build")
            .with_data_state_for_tests(
                GatewayDataState::with_provider_catalog_repository_for_tests(repository)
                    .with_encryption_key_for_tests(DEVELOPMENT_ENCRYPTION_KEY),
            );
        let plan = sample_claude_plan();
        let report_context = json!({
            "api_key_id": "api-key-1",
            "client_api_format": "claude:messages",
            "model": "claude-sonnet-4-5",
        });
        let cache_key = build_scheduler_affinity_cache_key_for_api_key_id(
            "api-key-1",
            "claude:messages",
            "claude-sonnet-4-5",
        )
        .expect("scheduler affinity cache key should build");
        let target = SchedulerAffinityTarget {
            provider_id: plan.provider_id.clone(),
            endpoint_id: plan.endpoint_id.clone(),
            key_id: plan.key_id.clone(),
        };
        state.remember_scheduler_affinity_target(
            &cache_key,
            target.clone(),
            SCHEDULER_AFFINITY_TTL,
            16,
        );
        let headers = BTreeMap::from([("Retry-After".to_string(), "120".to_string())]);
        let context = LocalExecutionEffectContext {
            plan: &plan,
            report_context: Some(&report_context),
        };
        let classification = LocalFailoverClassification::RetryUpstreamFailure;

        apply_local_execution_effect(
            &state,
            context,
            LocalExecutionEffect::AttemptFailure(LocalAttemptFailureEffect {
                status_code,
                classification,
            }),
        )
        .await;
        apply_local_execution_effect(
            &state,
            context,
            LocalExecutionEffect::AdaptiveRateLimit(LocalAdaptiveRateLimitEffect {
                status_code,
                classification,
                headers: Some(&headers),
            }),
        )
        .await;
        apply_local_execution_effect(
            &state,
            context,
            LocalExecutionEffect::HealthFailure(LocalHealthFailureEffect {
                status_code,
                classification,
            }),
        )
        .await;
        apply_local_execution_effect(
            &state,
            context,
            LocalExecutionEffect::PoolError(LocalPoolErrorEffect {
                status_code,
                classification,
                headers: &headers,
                error_body: Some(r#"{"error":{"message":"temporarily unavailable"}}"#),
            }),
        )
        .await;

        let stored_key = state
            .read_provider_catalog_keys_by_ids(std::slice::from_ref(&plan.key_id))
            .await
            .expect("provider catalog keys should load")
            .into_iter()
            .next()
            .expect("stored key should exist");
        assert_eq!(
            ProviderCatalogKeyAdaptiveState::from(&stored_key),
            expected_adaptive_state,
            "Anthropic status {status_code} must not update key-wide adaptive state"
        );
        assert_eq!(
            stored_key.health_by_format, expected_health,
            "Anthropic status {status_code} must not update key-wide health"
        );
        assert_eq!(
            stored_key.circuit_breaker_by_format,
            Some(circuit),
            "Anthropic status {status_code} must not clear pool key state"
        );
        let expected_affinity = (status_code == 400).then_some(target);
        assert_eq!(
            state.read_scheduler_affinity_target(&cache_key, SCHEDULER_AFFINITY_TTL),
            expected_affinity,
            "Anthropic status {status_code} must invalidate only retryable target affinity"
        );
    }
}

#[test]
fn terminal_pool_account_errors_project_pool_hard_state() {
    assert_eq!(
        pool_score_hard_state_for_status(
            400,
            Some(r#"{"error":{"message":"deactivated_workspace"}}"#),
        ),
        Some(PoolMemberHardState::Banned)
    );
    assert_eq!(
        pool_score_hard_state_for_status(402, Some(r#"{"error":{"message":"payment required"}}"#),),
        Some(PoolMemberHardState::QuotaExhausted)
    );
    assert_eq!(
        pool_score_hard_state_for_status(
            429,
            Some(r#"{"error":{"status":"RESOURCE_EXHAUSTED","message":"quota exhausted"}}"#),
        ),
        Some(PoolMemberHardState::QuotaExhausted)
    );
}

#[tokio::test]
async fn pool_account_error_does_not_open_key_circuit() {
    let Some(redis) = start_managed_redis_or_skip().await else {
        return;
    };
    let state = codex_state_with_redis(redis.redis_url(), "orchestration_pool_circuit");
    let plan = sample_codex_plan();
    let legacy_circuit = json!({
        "openai:responses": {
            "open": true,
            "reason": "legacy"
        }
    });
    state
        .update_provider_catalog_key_health_state(&plan.key_id, true, None, Some(&legacy_circuit))
        .await
        .expect("legacy circuit should seed");

    apply_local_execution_effect(
        &state,
        LocalExecutionEffectContext {
            plan: &plan,
            report_context: None,
        },
        LocalExecutionEffect::PoolError(LocalPoolErrorEffect {
            status_code: 401,
            classification: LocalFailoverClassification::StopErrorPattern,
            headers: &BTreeMap::new(),
            error_body: Some(r#"{"error":{"message":"account has been deactivated"}}"#),
        }),
    )
    .await;

    let stored_key = state
        .read_provider_catalog_keys_by_ids(std::slice::from_ref(&plan.key_id))
        .await
        .expect("provider catalog keys should load")
        .into_iter()
        .next()
        .expect("stored key should exist");
    assert_eq!(stored_key.circuit_breaker_by_format, None);
}

#[tokio::test]
async fn oauth_bearer_generation_match_supports_generic_auth_config_token() {
    let state = codex_state();
    let mut transport = state
        .read_provider_transport_snapshot(
            "provider-codex-cli-local-1",
            "endpoint-codex-cli-local-1",
            "key-codex-cli-local-1",
        )
        .await
        .expect("transport should load")
        .expect("transport should exist");
    transport.provider.provider_type = "claude_code".to_string();
    transport.key.decrypted_api_key = "__placeholder__".to_string();
    transport.key.decrypted_auth_config =
        Some(json!({"accessToken": "current-access-token"}).to_string());
    let mut plan = sample_codex_plan();
    plan.headers.insert(
        "authorization".to_string(),
        "Bearer current-access-token".to_string(),
    );
    assert!(execution_plan_bearer_matches_transport(&plan, &transport));

    plan.headers.insert(
        "authorization".to_string(),
        "Bearer stale-access-token".to_string(),
    );
    assert!(!execution_plan_bearer_matches_transport(&plan, &transport));

    transport.key.decrypted_api_key = "replacement-access-token".to_string();
    plan.headers.insert(
        "authorization".to_string(),
        "Bearer current-access-token".to_string(),
    );
    assert!(!execution_plan_bearer_matches_transport(&plan, &transport));
    plan.headers.insert(
        "authorization".to_string(),
        "Bearer replacement-access-token".to_string(),
    );
    assert!(execution_plan_bearer_matches_transport(&plan, &transport));

    transport.key.decrypted_auth_config = Some(
        json!({
            "accessToken": "current-access-token",
            "request": {
                "extraHeaders": {
                    "Authorization": "Bearer nested-override-token"
                }
            }
        })
        .to_string(),
    );
    plan.headers.insert(
        "authorization".to_string(),
        "Bearer replacement-access-token".to_string(),
    );
    assert!(!execution_plan_bearer_matches_transport(&plan, &transport));
    plan.headers.insert(
        "authorization".to_string(),
        "Bearer nested-override-token".to_string(),
    );
    assert!(execution_plan_bearer_matches_transport(&plan, &transport));
}

#[tokio::test]
async fn oauth_invalidation_marks_claude_code_authentication_failures_only() {
    let state = claude_code_oauth_state();
    let mut plan = sample_codex_plan();
    plan.provider_name = Some("claude_code".to_string());
    plan.provider_api_format = "claude:messages".to_string();

    apply_local_execution_effect(
        &state,
        LocalExecutionEffectContext {
            plan: &plan,
            report_context: None,
        },
        LocalExecutionEffect::OauthInvalidation(LocalOAuthInvalidationEffect {
            status_code: 403,
            response_text: Some(
                r#"{"type":"error","error":{"type":"permission_error","message":"insufficient scope"}}"#,
            ),
        }),
    )
    .await;
    let unmarked = state
        .read_provider_catalog_keys_by_ids(std::slice::from_ref(&plan.key_id))
        .await
        .expect("provider catalog keys should load")
        .into_iter()
        .next()
        .expect("stored key should exist");
    assert!(unmarked.oauth_invalid_at_unix_secs.is_none());

    apply_local_execution_effect(
        &state,
        LocalExecutionEffectContext {
            plan: &plan,
            report_context: None,
        },
        LocalExecutionEffect::OauthInvalidation(LocalOAuthInvalidationEffect {
            status_code: 403,
            response_text: Some(
                r#"{"type":"error","error":{"type":"authentication_error","message":"invalid access token"}}"#,
            ),
        }),
    )
    .await;
    let marked = state
        .read_provider_catalog_keys_by_ids(std::slice::from_ref(&plan.key_id))
        .await
        .expect("provider catalog keys should load")
        .into_iter()
        .next()
        .expect("stored key should exist");
    assert!(marked.oauth_invalid_at_unix_secs.is_some());
    assert_eq!(
        marked.oauth_invalid_reason.as_deref(),
        Some("[OAUTH_EXPIRED] invalid access token")
    );
}

#[tokio::test]
async fn oauth_invalidation_marks_claude_code_unauthorized_without_body() {
    let state = claude_code_oauth_state();
    let mut plan = sample_codex_plan();
    plan.provider_name = Some("claude_code".to_string());
    plan.provider_api_format = "claude:messages".to_string();

    apply_local_execution_effect(
        &state,
        LocalExecutionEffectContext {
            plan: &plan,
            report_context: None,
        },
        LocalExecutionEffect::OauthInvalidation(LocalOAuthInvalidationEffect {
            status_code: 401,
            response_text: None,
        }),
    )
    .await;

    let stored_key = state
        .read_provider_catalog_keys_by_ids(std::slice::from_ref(&plan.key_id))
        .await
        .expect("provider catalog keys should load")
        .into_iter()
        .next()
        .expect("stored key should exist");
    assert!(stored_key.oauth_invalid_at_unix_secs.is_some());
    assert_eq!(
        stored_key.oauth_invalid_reason.as_deref(),
        Some("[OAUTH_EXPIRED] OAuth access token was rejected")
    );
}

#[tokio::test]
async fn oauth_invalidation_marks_codex_key_expired() {
    let state = codex_state();
    let plan = sample_codex_plan();

    apply_local_execution_effect(
        &state,
        LocalExecutionEffectContext {
            plan: &plan,
            report_context: None,
        },
        LocalExecutionEffect::OauthInvalidation(LocalOAuthInvalidationEffect {
            status_code: 401,
            response_text: Some(
                r#"{"error":{"message":"session expired","type":"invalid_request_error"}}"#,
            ),
        }),
    )
    .await;

    let stored_key = state
        .read_provider_catalog_keys_by_ids(std::slice::from_ref(&plan.key_id))
        .await
        .expect("provider catalog keys should load")
        .into_iter()
        .next()
        .expect("stored key should exist");
    assert!(stored_key.oauth_invalid_at_unix_secs.is_some());
    assert_eq!(
        stored_key.oauth_invalid_reason.as_deref(),
        Some("[OAUTH_EXPIRED] Codex Token 已过期")
    );
    assert_eq!(
        stored_key
            .status_snapshot
            .as_ref()
            .and_then(|value| value.get("oauth"))
            .and_then(|value| value.get("code"))
            .and_then(Value::as_str),
        Some("expired")
    );
}

#[tokio::test]
async fn oauth_success_clears_recoverable_codex_invalid_state() {
    let mut key = sample_codex_key();
    key.oauth_invalid_at_unix_secs = Some(100);
    key.oauth_invalid_reason = Some("[OAUTH_EXPIRED] session expired".to_string());
    key.upstream_metadata = Some(json!({
        "codex": {
            "credential_generation": "credential-generation-current",
            "oauth_state_request_started_at_unix_ms": 100_000u64,
            "oauth_state_request_id": "00000001-86a0-7000-8000-000000000001"
        }
    }));
    let state = codex_state_with_provider_and_key(sample_codex_provider(), key);
    let plan = sample_codex_plan();
    let report_context = json!({
        "codex_credential_generation": "credential-generation-current",
        "provider_request_started_at_unix_ms": 200_000u64,
        "provider_request_order_id": "00000003-0d40-7000-8000-000000000001"
    });

    apply_local_execution_effect(
        &state,
        LocalExecutionEffectContext {
            plan: &plan,
            report_context: Some(&report_context),
        },
        LocalExecutionEffect::OauthSuccess(LocalOAuthSuccessEffect {
            status_code: 200,
            request_started_at_unix_ms: Some(200_000),
            request_order_id: Some("00000003-0d40-7000-8000-000000000001"),
        }),
    )
    .await;

    let stored_key = state
        .read_provider_catalog_keys_by_ids(std::slice::from_ref(&plan.key_id))
        .await
        .expect("provider catalog keys should load")
        .into_iter()
        .next()
        .expect("stored key should exist");
    assert_eq!(stored_key.oauth_invalid_at_unix_secs, None);
    assert_eq!(stored_key.oauth_invalid_reason, None);
    assert_eq!(
        stored_key
            .upstream_metadata
            .as_ref()
            .and_then(|metadata| metadata.pointer("/codex/oauth_state_request_started_at_unix_ms")),
        Some(&json!(200_000u64))
    );
    assert_eq!(
        stored_key
            .upstream_metadata
            .as_ref()
            .and_then(|metadata| metadata.pointer("/codex/oauth_state_request_id")),
        Some(&json!("00000003-0d40-7000-8000-000000000001"))
    );
}

#[tokio::test]
async fn same_millisecond_older_oauth_success_does_not_clear_newer_codex_invalidation() {
    let mut key = sample_codex_key();
    key.oauth_invalid_at_unix_secs = Some(300);
    key.oauth_invalid_reason = Some("[OAUTH_EXPIRED] session expired".to_string());
    key.upstream_metadata = Some(json!({
        "codex": {
            "credential_generation": "credential-generation-current",
            "oauth_state_request_started_at_unix_ms": 300_000u64,
            "oauth_state_request_id": "00000004-93e0-7000-8000-000000000002"
        }
    }));
    let state = codex_state_with_provider_and_key(sample_codex_provider(), key);
    let plan = sample_codex_plan();
    let report_context = json!({
        "codex_credential_generation": "credential-generation-current",
        "provider_request_started_at_unix_ms": 300_000u64,
        "provider_request_order_id": "00000004-93e0-7000-8000-000000000001"
    });

    apply_local_execution_effect(
        &state,
        LocalExecutionEffectContext {
            plan: &plan,
            report_context: Some(&report_context),
        },
        LocalExecutionEffect::OauthSuccess(LocalOAuthSuccessEffect {
            status_code: 200,
            request_started_at_unix_ms: Some(300_000),
            request_order_id: Some("00000004-93e0-7000-8000-000000000001"),
        }),
    )
    .await;

    let stored_key = state
        .read_provider_catalog_keys_by_ids(std::slice::from_ref(&plan.key_id))
        .await
        .expect("provider catalog keys should load")
        .into_iter()
        .next()
        .expect("stored key should exist");
    assert_eq!(stored_key.oauth_invalid_at_unix_secs, Some(300));
    assert_eq!(
        stored_key.oauth_invalid_reason.as_deref(),
        Some("[OAUTH_EXPIRED] session expired")
    );
    assert_eq!(
        stored_key
            .upstream_metadata
            .as_ref()
            .and_then(|metadata| metadata.pointer("/codex/oauth_state_request_id")),
        Some(&json!("00000004-93e0-7000-8000-000000000002"))
    );
}

#[tokio::test]
async fn oauth_success_preserves_codex_account_block() {
    let mut key = sample_codex_key();
    key.oauth_invalid_at_unix_secs = Some(100);
    key.oauth_invalid_reason = Some("[ACCOUNT_BLOCK] account deactivated".to_string());
    key.upstream_metadata = Some(json!({
        "codex": {
            "credential_generation": "credential-generation-current"
        }
    }));
    let state = codex_state_with_provider_and_key(sample_codex_provider(), key);
    let plan = sample_codex_plan();
    let report_context = json!({
        "codex_credential_generation": "credential-generation-current",
        "provider_request_started_at_unix_ms": 200_000u64,
        "provider_request_order_id": "00000003-0d40-7000-8000-000000000001"
    });

    apply_local_execution_effect(
        &state,
        LocalExecutionEffectContext {
            plan: &plan,
            report_context: Some(&report_context),
        },
        LocalExecutionEffect::OauthSuccess(LocalOAuthSuccessEffect {
            status_code: 200,
            request_started_at_unix_ms: Some(200_000),
            request_order_id: Some("00000003-0d40-7000-8000-000000000001"),
        }),
    )
    .await;

    let stored_key = state
        .read_provider_catalog_keys_by_ids(std::slice::from_ref(&plan.key_id))
        .await
        .expect("provider catalog keys should load")
        .into_iter()
        .next()
        .expect("stored key should exist");
    assert_eq!(stored_key.oauth_invalid_at_unix_secs, Some(100));
    assert_eq!(
        stored_key.oauth_invalid_reason.as_deref(),
        Some("[ACCOUNT_BLOCK] account deactivated")
    );
    assert_eq!(
        stored_key
            .upstream_metadata
            .as_ref()
            .and_then(|metadata| metadata.pointer("/codex/oauth_state_request_started_at_unix_ms")),
        Some(&json!(200_000u64))
    );
}

#[tokio::test]
async fn oauth_success_does_not_clear_codex_invalid_state_for_replaced_bearer() {
    let mut key = sample_codex_key();
    key.oauth_invalid_at_unix_secs = Some(100);
    key.oauth_invalid_reason = Some("[OAUTH_EXPIRED] session expired".to_string());
    key.upstream_metadata = Some(json!({
        "codex": {
            "credential_generation": "credential-generation-current"
        }
    }));
    let state = codex_state_with_provider_and_key(sample_codex_provider(), key);
    let mut plan = sample_codex_plan();
    plan.headers.insert(
        "authorization".to_string(),
        "Bearer replaced-access-token".to_string(),
    );
    let report_context = json!({
        "codex_credential_generation": "credential-generation-current",
        "provider_request_started_at_unix_ms": 200_000u64,
        "provider_request_order_id": "00000003-0d40-7000-8000-000000000001"
    });

    apply_local_execution_effect(
        &state,
        LocalExecutionEffectContext {
            plan: &plan,
            report_context: Some(&report_context),
        },
        LocalExecutionEffect::OauthSuccess(LocalOAuthSuccessEffect {
            status_code: 200,
            request_started_at_unix_ms: Some(200_000),
            request_order_id: Some("00000003-0d40-7000-8000-000000000001"),
        }),
    )
    .await;

    let stored_key = state
        .read_provider_catalog_keys_by_ids(std::slice::from_ref(&plan.key_id))
        .await
        .expect("provider catalog keys should load")
        .into_iter()
        .next()
        .expect("stored key should exist");
    assert_eq!(stored_key.oauth_invalid_at_unix_secs, Some(100));
    assert_eq!(
        stored_key.oauth_invalid_reason.as_deref(),
        Some("[OAUTH_EXPIRED] session expired")
    );
    assert!(stored_key
        .upstream_metadata
        .as_ref()
        .and_then(|metadata| metadata.pointer("/codex/oauth_state_request_started_at_unix_ms"))
        .is_none());
}

#[tokio::test]
async fn oauth_success_effect_ignores_non_success_status() {
    let mut key = sample_codex_key();
    key.oauth_invalid_at_unix_secs = Some(100);
    key.oauth_invalid_reason = Some("[OAUTH_EXPIRED] session expired".to_string());
    key.upstream_metadata = Some(json!({
        "codex": {
            "credential_generation": "credential-generation-current"
        }
    }));
    let state = codex_state_with_provider_and_key(sample_codex_provider(), key);
    let plan = sample_codex_plan();
    let report_context = json!({
        "codex_credential_generation": "credential-generation-current",
        "provider_request_started_at_unix_ms": 200_000u64,
        "provider_request_order_id": "00000003-0d40-7000-8000-000000000001"
    });

    apply_local_execution_effect(
        &state,
        LocalExecutionEffectContext {
            plan: &plan,
            report_context: Some(&report_context),
        },
        LocalExecutionEffect::OauthSuccess(LocalOAuthSuccessEffect {
            status_code: 304,
            request_started_at_unix_ms: Some(200_000),
            request_order_id: Some("00000003-0d40-7000-8000-000000000001"),
        }),
    )
    .await;

    let stored_key = state
        .read_provider_catalog_keys_by_ids(std::slice::from_ref(&plan.key_id))
        .await
        .expect("provider catalog keys should load")
        .into_iter()
        .next()
        .expect("stored key should exist");
    assert_eq!(stored_key.oauth_invalid_at_unix_secs, Some(100));
    assert_eq!(
        stored_key.oauth_invalid_reason.as_deref(),
        Some("[OAUTH_EXPIRED] session expired")
    );
    assert!(stored_key
        .upstream_metadata
        .as_ref()
        .and_then(|metadata| metadata.pointer("/codex/oauth_state_request_started_at_unix_ms"))
        .is_none());
}

#[tokio::test]
async fn oauth_invalidation_marks_generic_codex_403_as_token_invalid() {
    let state = codex_state();
    let plan = sample_codex_plan();

    apply_local_execution_effect(
        &state,
        LocalExecutionEffectContext {
            plan: &plan,
            report_context: None,
        },
        LocalExecutionEffect::OauthInvalidation(LocalOAuthInvalidationEffect {
            status_code: 403,
            response_text: Some(r#"{"error":{"message":"forbidden"}}"#),
        }),
    )
    .await;

    let stored_key = state
        .read_provider_catalog_keys_by_ids(std::slice::from_ref(&plan.key_id))
        .await
        .expect("provider catalog keys should load")
        .into_iter()
        .next()
        .expect("stored key should exist");
    assert!(stored_key.oauth_invalid_at_unix_secs.is_some());
    assert_eq!(
        stored_key.oauth_invalid_reason.as_deref(),
        Some("[OAUTH_EXPIRED] Codex Token 已失效 (403)")
    );
    assert_eq!(
        stored_key
            .status_snapshot
            .as_ref()
            .and_then(|value| value.get("oauth"))
            .and_then(|value| value.get("code"))
            .and_then(Value::as_str),
        Some("invalid")
    );
}

#[tokio::test]
async fn oauth_invalidation_marks_codex_inactive_pat_owner_403_as_token_invalid() {
    let state = codex_state();
    let plan = sample_codex_plan();

    apply_local_execution_effect(
        &state,
        LocalExecutionEffectContext {
            plan: &plan,
            report_context: None,
        },
        LocalExecutionEffect::OauthInvalidation(LocalOAuthInvalidationEffect {
            status_code: 403,
            response_text: Some(
                r#"{"error":{"code":"biscuit_baker_service_auth_credential_error_status","message":"Personal access token owner is inactive."},"status":403}"#,
            ),
        }),
    )
    .await;

    let stored_key = state
        .read_provider_catalog_keys_by_ids(std::slice::from_ref(&plan.key_id))
        .await
        .expect("provider catalog keys should load")
        .into_iter()
        .next()
        .expect("stored key should exist");
    assert!(stored_key.oauth_invalid_at_unix_secs.is_some());
    assert_eq!(
        stored_key.oauth_invalid_reason.as_deref(),
        Some("[OAUTH_EXPIRED] Codex Token 已失效")
    );
    assert_eq!(
        stored_key
            .status_snapshot
            .as_ref()
            .and_then(|value| value.get("oauth"))
            .and_then(|value| value.get("code"))
            .and_then(Value::as_str),
        Some("invalid")
    );
}

#[tokio::test]
async fn oauth_invalidation_auto_remove_keeps_inactive_pat_owner() {
    let state = codex_state_with_auto_remove();
    let plan = sample_codex_plan();

    apply_local_execution_effect(
        &state,
        LocalExecutionEffectContext {
            plan: &plan,
            report_context: None,
        },
        LocalExecutionEffect::OauthInvalidation(LocalOAuthInvalidationEffect {
            status_code: 403,
            response_text: Some(
                r#"{"error":{"code":"biscuit_baker_service_auth_credential_error_status","message":"Personal access token owner is inactive."},"status":403}"#,
            ),
        }),
    )
    .await;

    let stored_key = state
        .read_provider_catalog_keys_by_ids(std::slice::from_ref(&plan.key_id))
        .await
        .expect("provider catalog keys should load")
        .into_iter()
        .next()
        .expect("recoverable token invalidation should retain the key");
    assert_eq!(
        stored_key.oauth_invalid_reason.as_deref(),
        Some("[OAUTH_EXPIRED] Codex Token 已失效")
    );
}

#[tokio::test]
async fn oauth_invalidation_auto_removes_account_block() {
    let state = codex_state_with_auto_remove();
    let plan = sample_codex_plan();

    apply_local_execution_effect(
        &state,
        LocalExecutionEffectContext {
            plan: &plan,
            report_context: None,
        },
        LocalExecutionEffect::OauthInvalidation(LocalOAuthInvalidationEffect {
            status_code: 403,
            response_text: Some(
                r#"{"error":{"message":"account has been deactivated"},"status":403}"#,
            ),
        }),
    )
    .await;

    let stored_keys = state
        .read_provider_catalog_keys_by_ids(std::slice::from_ref(&plan.key_id))
        .await
        .expect("provider catalog keys should load");
    assert!(stored_keys.is_empty());
}

#[tokio::test]
async fn oauth_invalidation_auto_remove_keeps_recoverable_expired_token() {
    let state = codex_state_with_auto_remove();
    let plan = sample_codex_plan();

    apply_local_execution_effect(
        &state,
        LocalExecutionEffectContext {
            plan: &plan,
            report_context: None,
        },
        LocalExecutionEffect::OauthInvalidation(LocalOAuthInvalidationEffect {
            status_code: 401,
            response_text: Some(
                r#"{"error":{"message":"session expired","type":"invalid_request_error"}}"#,
            ),
        }),
    )
    .await;

    let stored_key = state
        .read_provider_catalog_keys_by_ids(std::slice::from_ref(&plan.key_id))
        .await
        .expect("provider catalog keys should load")
        .into_iter()
        .next()
        .expect("recoverable expired token should be retained");
    assert_eq!(
        stored_key.oauth_invalid_reason.as_deref(),
        Some("[OAUTH_EXPIRED] Codex Token 已过期")
    );
}

#[tokio::test]
async fn oauth_invalidation_does_not_mutate_replacement_after_agent_request() {
    let state = codex_state_with_auto_remove();
    let plan = sample_codex_agent_identity_plan();

    apply_local_execution_effect(
        &state,
        LocalExecutionEffectContext {
            plan: &plan,
            report_context: None,
        },
        LocalExecutionEffect::OauthInvalidation(LocalOAuthInvalidationEffect {
            status_code: 403,
            response_text: Some(
                r#"{"error":{"code":"biscuit_baker_service_auth_credential_error_status","message":"Personal access token owner is inactive."},"status":403}"#,
            ),
        }),
    )
    .await;

    let stored_key = state
        .read_provider_catalog_keys_by_ids(std::slice::from_ref(&plan.key_id))
        .await
        .expect("provider catalog keys should load")
        .into_iter()
        .next()
        .expect("replacement OAuth key should not be removed");
    assert_eq!(stored_key.oauth_invalid_at_unix_secs, None);
    assert_eq!(stored_key.oauth_invalid_reason, None);
}

#[tokio::test]
async fn oauth_invalidation_does_not_mutate_agent_replacement_after_bearer_request() {
    let state = codex_state_with_provider_and_key(
        sample_codex_provider_with_auto_remove(),
        sample_codex_agent_identity_key(),
    );
    let mut plan = sample_codex_plan();
    plan.headers.insert(
        "authorization".to_string(),
        "Bearer old-access-token".to_string(),
    );

    apply_local_execution_effect(
        &state,
        LocalExecutionEffectContext {
            plan: &plan,
            report_context: None,
        },
        LocalExecutionEffect::OauthInvalidation(LocalOAuthInvalidationEffect {
            status_code: 403,
            response_text: Some(
                r#"{"error":{"code":"biscuit_baker_service_auth_credential_error_status","message":"Personal access token owner is inactive."},"status":403}"#,
            ),
        }),
    )
    .await;

    let stored_key = state
        .read_provider_catalog_keys_by_ids(std::slice::from_ref(&plan.key_id))
        .await
        .expect("provider catalog keys should load")
        .into_iter()
        .next()
        .expect("Agent Identity replacement should not be removed");
    assert_eq!(stored_key.oauth_invalid_at_unix_secs, None);
    assert_eq!(stored_key.oauth_invalid_reason, None);
}

#[tokio::test]
async fn oauth_invalidation_rejects_stale_codex_credential_generation() {
    let mut key = sample_codex_key();
    key.upstream_metadata = Some(json!({
        "codex": {
            "credential_generation": "credential-generation-current"
        }
    }));
    let state = codex_state_with_provider_and_key(sample_codex_provider(), key);
    let plan = sample_codex_plan();
    let report_context = json!({
        "codex_credential_generation": "credential-generation-stale",
        "provider_request_started_at_unix_ms": 100_000u64,
        "provider_request_order_id": "00000001-86a0-7000-8000-000000000001"
    });

    apply_local_execution_effect(
        &state,
        LocalExecutionEffectContext {
            plan: &plan,
            report_context: Some(&report_context),
        },
        LocalExecutionEffect::OauthInvalidation(LocalOAuthInvalidationEffect {
            status_code: 401,
            response_text: Some(r#"{"error":{"message":"session expired"}}"#),
        }),
    )
    .await;

    let stored_key = state
        .read_provider_catalog_keys_by_ids(std::slice::from_ref(&plan.key_id))
        .await
        .expect("provider catalog keys should load")
        .into_iter()
        .next()
        .expect("stored key should exist");
    assert_eq!(stored_key.oauth_invalid_at_unix_secs, None);
    assert_eq!(stored_key.oauth_invalid_reason, None);
    assert!(stored_key
        .upstream_metadata
        .as_ref()
        .and_then(|metadata| metadata.pointer("/codex/oauth_state_request_started_at_unix_ms"))
        .is_none());
}

#[tokio::test]
async fn delayed_codex_quota_report_does_not_clear_newer_oauth_invalidation() {
    let mut key = sample_codex_key();
    key.upstream_metadata = Some(json!({
        "codex": {
            "credential_generation": "credential-generation-current",
            "account_quota_reset_generation": 0
        }
    }));
    let state = codex_state_with_provider_and_key(sample_codex_provider(), key);
    let plan = sample_codex_plan();
    let older_request_id = "00000001-86a0-7000-8000-000000000001";
    let newer_request_id = "00000001-86a0-7000-8000-000000000002";
    let older_uuid = uuid::Uuid::parse_str(older_request_id).expect("older id should parse");
    let newer_uuid = uuid::Uuid::parse_str(newer_request_id).expect("newer id should parse");
    assert_eq!(older_uuid.get_version_num(), 7);
    assert_eq!(newer_uuid.get_version_num(), 7);
    assert!(older_uuid < newer_uuid);

    let invalidation_context = json!({
        "codex_credential_generation": "credential-generation-current",
        "provider_request_started_at_unix_ms": 100_000u64,
        "provider_request_order_id": newer_request_id
    });
    apply_local_execution_effect(
        &state,
        LocalExecutionEffectContext {
            plan: &plan,
            report_context: Some(&invalidation_context),
        },
        LocalExecutionEffect::OauthInvalidation(LocalOAuthInvalidationEffect {
            status_code: 401,
            response_text: Some(r#"{"error":{"message":"session expired"}}"#),
        }),
    )
    .await;

    let delayed_report_context = json!({
        "key_id": plan.key_id,
        "codex_credential_generation": "credential-generation-current",
        "codex_quota_reset_generation": 0,
        "provider_request_started_at_unix_ms": 100_000u64,
        "provider_response_headers_observed_at_unix_ms": 110_000u64,
        "provider_request_order_id": older_request_id
    });
    let delayed_headers = BTreeMap::from([
        ("x-codex-plan-type".to_string(), "free".to_string()),
        ("x-codex-primary-used-percent".to_string(), "25".to_string()),
        (
            "x-codex-primary-reset-at".to_string(),
            "2000000000".to_string(),
        ),
        (
            "x-codex-primary-window-minutes".to_string(),
            "300".to_string(),
        ),
    ]);
    let delayed_report = GatewaySyncReportRequest {
        trace_id: "trace-delayed-codex-quota".to_string(),
        report_kind: "openai_responses_sync_success".to_string(),
        report_context: Some(delayed_report_context),
        status_code: 200,
        headers: delayed_headers,
        body_json: None,
        client_body_json: None,
        body_base64: None,
        telemetry: None,
    };
    apply_local_report_effect(
        &state,
        LocalReportEffect::Sync {
            payload: &delayed_report,
        },
    )
    .await;

    let stored_key = state
        .read_provider_catalog_keys_by_ids(std::slice::from_ref(&plan.key_id))
        .await
        .expect("provider catalog keys should load")
        .into_iter()
        .next()
        .expect("stored key should exist");
    assert_eq!(
        stored_key.oauth_invalid_reason.as_deref(),
        Some("[OAUTH_EXPIRED] Codex Token 已过期")
    );
    assert!(stored_key.oauth_invalid_at_unix_secs.is_some());
    assert_eq!(
        stored_key
            .upstream_metadata
            .as_ref()
            .and_then(|metadata| metadata.pointer("/codex/oauth_state_request_id")),
        Some(&json!(newer_request_id))
    );
    assert_eq!(
        stored_key
            .status_snapshot
            .as_ref()
            .and_then(|snapshot| snapshot.pointer("/oauth/code")),
        Some(&json!("expired"))
    );
}

#[tokio::test]
async fn health_failure_updates_codex_key_for_current_bearer_request() {
    let state = codex_state();
    let plan = sample_codex_plan();

    apply_local_execution_effect(
        &state,
        LocalExecutionEffectContext {
            plan: &plan,
            report_context: None,
        },
        LocalExecutionEffect::HealthFailure(LocalHealthFailureEffect {
            status_code: 503,
            classification: LocalFailoverClassification::RetryUpstreamFailure,
        }),
    )
    .await;

    let stored_key = state
        .read_provider_catalog_keys_by_ids(std::slice::from_ref(&plan.key_id))
        .await
        .expect("provider catalog keys should load")
        .into_iter()
        .next()
        .expect("stored key should exist");
    assert_eq!(
        stored_key
            .health_by_format
            .as_ref()
            .and_then(|value| value.get("openai:responses"))
            .and_then(|value| value.get("consecutive_failures"))
            .and_then(Value::as_u64),
        Some(1)
    );
}

#[tokio::test]
async fn health_failure_does_not_mutate_codex_bearer_replacement() {
    let mut replacement = sample_codex_key();
    replacement.encrypted_api_key = Some(
        encrypt_python_fernet_plaintext(DEVELOPMENT_ENCRYPTION_KEY, "replacement-token")
            .expect("replacement token should encrypt"),
    );
    replacement.encrypted_auth_config = Some(
        encrypt_python_fernet_plaintext(
            DEVELOPMENT_ENCRYPTION_KEY,
            r#"{"provider_type":"codex","refresh_token":"replacement-refresh-token"}"#,
        )
        .expect("replacement auth config should encrypt"),
    );
    let expected_health = replacement.health_by_format.clone();
    let expected_circuit = replacement.circuit_breaker_by_format.clone();
    let state = codex_state_with_provider_and_key(sample_codex_provider(), replacement);
    let plan = sample_codex_plan();

    apply_local_execution_effect(
        &state,
        LocalExecutionEffectContext {
            plan: &plan,
            report_context: None,
        },
        LocalExecutionEffect::HealthFailure(LocalHealthFailureEffect {
            status_code: 503,
            classification: LocalFailoverClassification::RetryUpstreamFailure,
        }),
    )
    .await;

    let stored_key = state
        .read_provider_catalog_keys_by_ids(std::slice::from_ref(&plan.key_id))
        .await
        .expect("provider catalog keys should load")
        .into_iter()
        .next()
        .expect("replacement key should exist");
    assert_eq!(stored_key.health_by_format, expected_health);
    assert_eq!(stored_key.circuit_breaker_by_format, expected_circuit);
}

#[tokio::test]
async fn adaptive_rate_limit_does_not_mutate_codex_bearer_replacement() {
    let mut replacement = sample_codex_key();
    replacement.encrypted_api_key = Some(
        encrypt_python_fernet_plaintext(DEVELOPMENT_ENCRYPTION_KEY, "replacement-token")
            .expect("replacement token should encrypt"),
    );
    replacement.encrypted_auth_config = Some(
        encrypt_python_fernet_plaintext(
            DEVELOPMENT_ENCRYPTION_KEY,
            r#"{"provider_type":"codex","refresh_token":"replacement-refresh-token"}"#,
        )
        .expect("replacement auth config should encrypt"),
    );
    replacement.learned_rpm_limit = Some(12);
    replacement.rpm_429_count = Some(1);
    let expected_adaptive_state = ProviderCatalogKeyAdaptiveState::from(&replacement);
    let state = codex_state_with_provider_and_key(sample_codex_provider(), replacement);
    let plan = sample_codex_plan();

    apply_local_execution_effect(
        &state,
        LocalExecutionEffectContext {
            plan: &plan,
            report_context: None,
        },
        LocalExecutionEffect::AdaptiveRateLimit(LocalAdaptiveRateLimitEffect {
            status_code: 429,
            classification: LocalFailoverClassification::RetryUpstreamFailure,
            headers: Some(&BTreeMap::from([(
                "x-ratelimit-limit-requests".to_string(),
                "42".to_string(),
            )])),
        }),
    )
    .await;

    let stored_key = state
        .read_provider_catalog_keys_by_ids(std::slice::from_ref(&plan.key_id))
        .await
        .expect("provider catalog keys should load")
        .into_iter()
        .next()
        .expect("replacement key should exist");
    assert_eq!(
        ProviderCatalogKeyAdaptiveState::from(&stored_key),
        expected_adaptive_state
    );
}

#[tokio::test]
async fn pool_error_does_not_clear_codex_bearer_replacement_circuit() {
    let legacy_circuit = json!({
        "openai:responses": {
            "open": true,
            "reason": "replacement-state"
        }
    });
    let mut replacement = sample_codex_key();
    replacement.encrypted_api_key = Some(
        encrypt_python_fernet_plaintext(DEVELOPMENT_ENCRYPTION_KEY, "replacement-token")
            .expect("replacement token should encrypt"),
    );
    replacement.encrypted_auth_config = Some(
        encrypt_python_fernet_plaintext(
            DEVELOPMENT_ENCRYPTION_KEY,
            r#"{"provider_type":"codex","refresh_token":"replacement-refresh-token"}"#,
        )
        .expect("replacement auth config should encrypt"),
    );
    replacement.circuit_breaker_by_format = Some(legacy_circuit.clone());
    let state = codex_state_with_provider_and_key(sample_codex_provider(), replacement);
    let plan = sample_codex_plan();

    apply_local_execution_effect(
        &state,
        LocalExecutionEffectContext {
            plan: &plan,
            report_context: None,
        },
        LocalExecutionEffect::PoolError(LocalPoolErrorEffect {
            status_code: 401,
            classification: LocalFailoverClassification::StopErrorPattern,
            headers: &BTreeMap::new(),
            error_body: Some(r#"{"error":{"message":"account has been deactivated"}}"#),
        }),
    )
    .await;

    let stored_key = state
        .read_provider_catalog_keys_by_ids(std::slice::from_ref(&plan.key_id))
        .await
        .expect("provider catalog keys should load")
        .into_iter()
        .next()
        .expect("replacement key should exist");
    assert_eq!(stored_key.circuit_breaker_by_format, Some(legacy_circuit));
}

#[tokio::test]
async fn health_failure_projection_updates_key_health_for_format() {
    let state = health_state();
    let plan = sample_plan();

    apply_local_execution_effect(
        &state,
        LocalExecutionEffectContext {
            plan: &plan,
            report_context: None,
        },
        LocalExecutionEffect::HealthFailure(LocalHealthFailureEffect {
            status_code: 503,
            classification: LocalFailoverClassification::RetryUpstreamFailure,
        }),
    )
    .await;

    let stored_key = state
        .read_provider_catalog_keys_by_ids(std::slice::from_ref(&plan.key_id))
        .await
        .expect("provider catalog keys should load")
        .into_iter()
        .next()
        .expect("stored key should exist");
    assert_eq!(
        stored_key.health_by_format,
        Some(json!({
            "openai:chat": {
                "health_score": 0.6,
                "consecutive_failures": 1,
                "last_failure_at": stored_key
                    .health_by_format
                    .as_ref()
                    .and_then(|value| value.get("openai:chat"))
                    .and_then(|value| value.get("last_failure_at"))
                    .cloned()
                    .unwrap_or(Value::Null)
            }
        }))
    );
}

#[tokio::test]
async fn runtime_health_failure_does_not_reactivate_admin_disabled_key() {
    let mut key = sample_health_key();
    key.is_active = false;
    let state = health_state_with_key(key);
    let plan = sample_plan();

    apply_local_execution_effect(
        &state,
        LocalExecutionEffectContext {
            plan: &plan,
            report_context: None,
        },
        LocalExecutionEffect::HealthFailure(LocalHealthFailureEffect {
            status_code: 503,
            classification: LocalFailoverClassification::RetryUpstreamFailure,
        }),
    )
    .await;

    let stored_key = state
        .read_provider_catalog_keys_by_ids(std::slice::from_ref(&plan.key_id))
        .await
        .expect("provider catalog keys should load")
        .into_iter()
        .next()
        .expect("stored key should exist");
    assert!(!stored_key.is_active);
    assert_eq!(
        stored_key
            .health_by_format
            .as_ref()
            .and_then(|value| value.get("openai:chat"))
            .and_then(|value| value.get("consecutive_failures"))
            .and_then(Value::as_u64),
        Some(1)
    );
}

#[tokio::test]
async fn health_failure_opens_circuit_after_eight_consecutive_failures() {
    let state = health_state();
    let plan = sample_plan();

    for _ in 0..8 {
        apply_local_execution_effect(
            &state,
            LocalExecutionEffectContext {
                plan: &plan,
                report_context: None,
            },
            LocalExecutionEffect::HealthFailure(LocalHealthFailureEffect {
                status_code: 503,
                classification: LocalFailoverClassification::RetryUpstreamFailure,
            }),
        )
        .await;
    }

    let stored_key = state
        .read_provider_catalog_keys_by_ids(std::slice::from_ref(&plan.key_id))
        .await
        .expect("provider catalog keys should load")
        .into_iter()
        .next()
        .expect("stored key should exist");
    let circuit = stored_key
        .circuit_breaker_by_format
        .as_ref()
        .and_then(|value| value.get("openai:chat"))
        .expect("format circuit should be stored");
    assert_eq!(circuit["open"], json!(true));
    assert_eq!(circuit["reason"], json!("consecutive_failures_8"));
    assert_eq!(circuit["probe_interval_minutes"], json!(1));
    assert!(circuit["next_probe_at_unix_secs"].as_u64().is_some());
    assert_eq!(
        circuit["request_results_window"]
            .as_array()
            .map(Vec::len)
            .unwrap_or_default(),
        8
    );
}

#[tokio::test]
async fn concurrent_health_failures_for_one_key_do_not_lose_updates() {
    let state = health_state();
    let plan = sample_plan();
    let mut tasks = tokio::task::JoinSet::new();

    for _ in 0..8 {
        let state = state.clone();
        let plan = plan.clone();
        tasks.spawn(async move {
            apply_local_execution_effect(
                &state,
                LocalExecutionEffectContext {
                    plan: &plan,
                    report_context: None,
                },
                LocalExecutionEffect::HealthFailure(LocalHealthFailureEffect {
                    status_code: 503,
                    classification: LocalFailoverClassification::RetryUpstreamFailure,
                }),
            )
            .await;
        });
    }
    while let Some(result) = tasks.join_next().await {
        result.expect("health failure task should complete");
    }

    let stored_key = state
        .read_provider_catalog_keys_by_ids(std::slice::from_ref(&plan.key_id))
        .await
        .expect("provider catalog keys should load")
        .into_iter()
        .next()
        .expect("stored key should exist");
    let circuit = stored_key
        .circuit_breaker_by_format
        .as_ref()
        .and_then(|value| value.get("openai:chat"))
        .expect("format circuit should be stored");
    assert_eq!(
        stored_key
            .health_by_format
            .as_ref()
            .and_then(|value| value.get("openai:chat"))
            .and_then(|value| value.get("consecutive_failures"))
            .and_then(Value::as_u64),
        Some(8)
    );
    assert_eq!(circuit["open"], json!(true));
    assert_eq!(circuit["reason"], json!("consecutive_failures_8"));
}

#[tokio::test]
async fn pool_health_failure_does_not_open_key_circuit_after_eight_consecutive_failures() {
    let state = pool_health_state();
    let plan = sample_plan();

    for _ in 0..8 {
        apply_local_execution_effect(
            &state,
            LocalExecutionEffectContext {
                plan: &plan,
                report_context: None,
            },
            LocalExecutionEffect::HealthFailure(LocalHealthFailureEffect {
                status_code: 503,
                classification: LocalFailoverClassification::RetryUpstreamFailure,
            }),
        )
        .await;
    }

    let stored_key = state
        .read_provider_catalog_keys_by_ids(std::slice::from_ref(&plan.key_id))
        .await
        .expect("provider catalog keys should load")
        .into_iter()
        .next()
        .expect("stored key should exist");
    assert_eq!(stored_key.circuit_breaker_by_format, None);
    assert_eq!(
        stored_key
            .health_by_format
            .as_ref()
            .and_then(|value| value.get("openai:chat"))
            .and_then(|value| value.get("consecutive_failures"))
            .and_then(Value::as_u64),
        Some(8)
    );
}

#[tokio::test]
async fn health_success_projection_resets_key_health_for_format() {
    let state = health_state();
    let plan = sample_plan();

    apply_local_execution_effect(
        &state,
        LocalExecutionEffectContext {
            plan: &plan,
            report_context: None,
        },
        LocalExecutionEffect::HealthFailure(LocalHealthFailureEffect {
            status_code: 503,
            classification: LocalFailoverClassification::RetryUpstreamFailure,
        }),
    )
    .await;
    apply_local_execution_effect(
        &state,
        LocalExecutionEffectContext {
            plan: &plan,
            report_context: None,
        },
        LocalExecutionEffect::HealthSuccess(LocalHealthSuccessEffect),
    )
    .await;

    let stored_key = state
        .read_provider_catalog_keys_by_ids(std::slice::from_ref(&plan.key_id))
        .await
        .expect("provider catalog keys should load")
        .into_iter()
        .next()
        .expect("stored key should exist");
    assert_eq!(
        stored_key.health_by_format,
        Some(json!({
            "openai:chat": {
                "health_score": 1.0,
                "consecutive_failures": 0,
                "last_failure_at": Value::Null
            }
        }))
    );
}

#[tokio::test]
async fn health_success_projection_is_rate_limited_until_failure_resets_gate() {
    // Keep this test's process-wide persistence gate isolated from the other
    // effect tests, which intentionally exercise the same health key in parallel.
    let mut plan = sample_plan();
    plan.key_id = format!("health-success-rate-limit-{}", uuid::Uuid::new_v4());
    let mut key = sample_health_key();
    key.id = plan.key_id.clone();
    let state = health_state_with_key(key);

    apply_local_execution_effect(
        &state,
        LocalExecutionEffectContext {
            plan: &plan,
            report_context: None,
        },
        LocalExecutionEffect::HealthSuccess(LocalHealthSuccessEffect),
    )
    .await;
    let first_updated_at = state
        .read_provider_catalog_keys_by_ids(std::slice::from_ref(&plan.key_id))
        .await
        .expect("provider catalog keys should load")
        .into_iter()
        .next()
        .expect("stored key should exist")
        .updated_at_unix_secs;

    apply_local_execution_effect(
        &state,
        LocalExecutionEffectContext {
            plan: &plan,
            report_context: None,
        },
        LocalExecutionEffect::HealthSuccess(LocalHealthSuccessEffect),
    )
    .await;
    let second_updated_at = state
        .read_provider_catalog_keys_by_ids(std::slice::from_ref(&plan.key_id))
        .await
        .expect("provider catalog keys should load")
        .into_iter()
        .next()
        .expect("stored key should exist")
        .updated_at_unix_secs;
    assert_eq!(second_updated_at, first_updated_at);

    apply_local_execution_effect(
        &state,
        LocalExecutionEffectContext {
            plan: &plan,
            report_context: None,
        },
        LocalExecutionEffect::HealthFailure(LocalHealthFailureEffect {
            status_code: 503,
            classification: LocalFailoverClassification::RetryUpstreamFailure,
        }),
    )
    .await;
    apply_local_execution_effect(
        &state,
        LocalExecutionEffectContext {
            plan: &plan,
            report_context: None,
        },
        LocalExecutionEffect::HealthSuccess(LocalHealthSuccessEffect),
    )
    .await;

    let stored_key = state
        .read_provider_catalog_keys_by_ids(std::slice::from_ref(&plan.key_id))
        .await
        .expect("provider catalog keys should load")
        .into_iter()
        .next()
        .expect("stored key should exist");
    assert_eq!(
        stored_key
            .health_by_format
            .as_ref()
            .and_then(|value| value.get("openai:chat"))
            .and_then(|value| value.get("consecutive_failures"))
            .and_then(Value::as_u64),
        Some(0)
    );
}

#[tokio::test]
async fn health_success_projection_closes_key_circuit_for_format() {
    let mut key = sample_health_key();
    key.circuit_breaker_by_format = Some(json!({
        "openai:chat": {
            "open": true,
            "reason": "account_deactivated_401",
            "next_probe_at_unix_secs": 1_760_001_920u64
        }
    }));
    let state = health_state_with_key(key);
    let plan = sample_plan();

    apply_local_execution_effect(
        &state,
        LocalExecutionEffectContext {
            plan: &plan,
            report_context: None,
        },
        LocalExecutionEffect::HealthSuccess(LocalHealthSuccessEffect),
    )
    .await;

    let stored_key = state
        .read_provider_catalog_keys_by_ids(std::slice::from_ref(&plan.key_id))
        .await
        .expect("provider catalog keys should load")
        .into_iter()
        .next()
        .expect("stored key should exist");
    let circuit = stored_key
        .circuit_breaker_by_format
        .as_ref()
        .and_then(|value| value.get("openai:chat"))
        .expect("format circuit should be stored");
    assert_eq!(circuit["open"], json!(false));
    assert_eq!(circuit["reason"], Value::Null);
    assert_eq!(circuit["next_probe_at_unix_secs"], Value::Null);
}

#[tokio::test]
async fn adaptive_rate_limit_effect_updates_adaptive_key_observation() {
    let state = adaptive_state();
    let plan = sample_plan();
    let cache_key =
        build_scheduler_affinity_cache_key_for_api_key_id("api-key-1", "openai:chat", "gpt-5")
            .expect("scheduler affinity cache key should build");
    let target = SchedulerAffinityTarget {
        provider_id: plan.provider_id.clone(),
        endpoint_id: plan.endpoint_id.clone(),
        key_id: plan.key_id.clone(),
    };
    state.remember_scheduler_affinity_target(
        &cache_key,
        target.clone(),
        SCHEDULER_AFFINITY_TTL,
        16,
    );
    let initial_epoch = state.scheduler_affinity_epoch();

    apply_local_execution_effect(
        &state,
        LocalExecutionEffectContext {
            plan: &plan,
            report_context: None,
        },
        LocalExecutionEffect::AdaptiveRateLimit(LocalAdaptiveRateLimitEffect {
            status_code: 429,
            classification: LocalFailoverClassification::RetryUpstreamFailure,
            headers: Some(&BTreeMap::from([(
                "x-ratelimit-limit-requests".to_string(),
                "42".to_string(),
            )])),
        }),
    )
    .await;

    let stored_key = state
        .read_provider_catalog_keys_by_ids(std::slice::from_ref(&plan.key_id))
        .await
        .expect("provider catalog keys should load")
        .into_iter()
        .next()
        .expect("stored key should exist");
    assert_eq!(stored_key.rpm_429_count, Some(2));
    assert_eq!(stored_key.last_429_type.as_deref(), Some("rpm"));
    assert!(stored_key.last_429_at_unix_secs.is_some());
    assert_eq!(
        stored_key
            .status_snapshot
            .as_ref()
            .and_then(|value| value.get("observation_count")),
        Some(&json!(1))
    );
    assert_eq!(
        stored_key
            .status_snapshot
            .as_ref()
            .and_then(|value| value.get("header_observation_count")),
        Some(&json!(1))
    );
    assert_eq!(
        stored_key
            .status_snapshot
            .as_ref()
            .and_then(|value| value.get("latest_upstream_limit")),
        Some(&json!(42))
    );
    assert_eq!(
        stored_key
            .status_snapshot
            .as_ref()
            .and_then(|value| value.get("learning_confidence")),
        Some(&json!(0.3))
    );
    assert_eq!(
        stored_key
            .status_snapshot
            .as_ref()
            .and_then(|value| value.get("enforcement_active")),
        Some(&json!(false))
    );
    assert_eq!(state.scheduler_affinity_epoch(), initial_epoch);
    assert_eq!(
        state.read_scheduler_affinity_target(cache_key.as_str(), SCHEDULER_AFFINITY_TTL),
        Some(target)
    );
}

#[tokio::test]
async fn adaptive_rate_limit_effect_preserves_quota_status_owned_by_reports() {
    let mut key = sample_adaptive_key();
    key.status_snapshot = Some(json!({
        "quota": {"remaining": 9},
        "oauth": {"invalid": false},
        "observation_count": 0
    }));
    let state = health_state_with_key(key);
    let plan = sample_plan();

    apply_local_execution_effect(
        &state,
        LocalExecutionEffectContext {
            plan: &plan,
            report_context: None,
        },
        LocalExecutionEffect::AdaptiveRateLimit(LocalAdaptiveRateLimitEffect {
            status_code: 429,
            classification: LocalFailoverClassification::RetryUpstreamFailure,
            headers: None,
        }),
    )
    .await;

    let stored_key = state
        .read_provider_catalog_keys_by_ids(std::slice::from_ref(&plan.key_id))
        .await
        .expect("provider catalog keys should load")
        .into_iter()
        .next()
        .expect("stored key should exist");
    let status = stored_key.status_snapshot.expect("status should exist");
    assert_eq!(status["quota"], json!({"remaining":9}));
    assert_eq!(status["oauth"], json!({"invalid":false}));
    assert_eq!(status["observation_count"], json!(1));
}

#[tokio::test]
async fn adaptive_rate_limit_effect_ignores_fixed_limit_key() {
    let state = fixed_limit_state();
    let plan = sample_plan();

    apply_local_execution_effect(
        &state,
        LocalExecutionEffectContext {
            plan: &plan,
            report_context: None,
        },
        LocalExecutionEffect::AdaptiveRateLimit(LocalAdaptiveRateLimitEffect {
            status_code: 429,
            classification: LocalFailoverClassification::RetryUpstreamFailure,
            headers: Some(&BTreeMap::from([(
                "x-ratelimit-limit-requests".to_string(),
                "42".to_string(),
            )])),
        }),
    )
    .await;

    let stored_key = state
        .read_provider_catalog_keys_by_ids(std::slice::from_ref(&plan.key_id))
        .await
        .expect("provider catalog keys should load")
        .into_iter()
        .next()
        .expect("stored key should exist");
    assert_eq!(stored_key.rpm_429_count, None);
    assert_eq!(stored_key.last_429_at_unix_secs, None);
    assert_eq!(stored_key.last_429_type, None);
}

#[tokio::test]
async fn adaptive_rate_limit_effect_records_429_as_rpm_observation() {
    let mut key = sample_health_key();
    key.rpm_limit = None;
    key.learned_rpm_limit = Some(20);
    let state = adaptive_state_with_request_candidates(key, Vec::new());
    let plan = sample_plan();

    apply_local_execution_effect(
        &state,
        LocalExecutionEffectContext {
            plan: &plan,
            report_context: None,
        },
        LocalExecutionEffect::AdaptiveRateLimit(LocalAdaptiveRateLimitEffect {
            status_code: 429,
            classification: LocalFailoverClassification::RetryStatusCode,
            headers: None,
        }),
    )
    .await;

    let stored_key = state
        .read_provider_catalog_keys_by_ids(std::slice::from_ref(&plan.key_id))
        .await
        .expect("provider catalog keys should load")
        .into_iter()
        .next()
        .expect("stored key should exist");
    assert_eq!(stored_key.rpm_429_count, Some(1));
    assert_eq!(stored_key.learned_rpm_limit, Some(20));
    assert_eq!(stored_key.last_429_type.as_deref(), Some("rpm"));
}

#[tokio::test]
async fn adaptive_success_effect_expands_limit_from_recent_rpm_usage() {
    let now_unix_secs = chrono::Utc::now().timestamp().max(0) as u64;
    let mut key = sample_adaptive_key();
    key.learned_rpm_limit = Some(20);
    key.last_rpm_peak = Some(25);
    key.last_429_at_unix_secs = Some(now_unix_secs.saturating_sub(600));
    key.adjustment_history = Some(json!([
        {
            "timestamp": "2026-04-19T00:00:00Z",
            "old_limit": 0,
            "new_limit": 20,
            "reason": "rpm_429",
            "confidence": 0.8
        }
    ]));
    key.utilization_samples = Some(json!([
        {"ts": now_unix_secs.saturating_sub(40), "util": 0.90},
        {"ts": now_unix_secs.saturating_sub(30), "util": 0.95},
        {"ts": now_unix_secs.saturating_sub(20), "util": 0.85},
        {"ts": now_unix_secs.saturating_sub(10), "util": 0.80}
    ]));
    let state = adaptive_state_with_request_candidates(
        key,
        vec![StoredRequestCandidate::new(
            "candidate-1".to_string(),
            "req-1".to_string(),
            None,
            None,
            None,
            None,
            0,
            0,
            Some("prov-1".to_string()),
            Some("ep-1".to_string()),
            Some("key-1".to_string()),
            RequestCandidateStatus::Success,
            None,
            false,
            Some(200),
            None,
            None,
            Some(10),
            Some(19),
            None,
            None,
            i64::try_from(now_unix_secs.saturating_sub(30) * 1000)
                .expect("candidate created_at should fit i64"),
            Some(
                i64::try_from(now_unix_secs.saturating_sub(30) * 1000)
                    .expect("candidate started_at should fit i64"),
            ),
            Some(
                i64::try_from(now_unix_secs.saturating_sub(29) * 1000)
                    .expect("candidate finished_at should fit i64"),
            ),
        )
        .expect("request candidate should build")],
    );
    let plan = sample_plan();
    let cache_key =
        build_scheduler_affinity_cache_key_for_api_key_id("api-key-1", "openai:chat", "gpt-5")
            .expect("scheduler affinity cache key should build");
    let target = SchedulerAffinityTarget {
        provider_id: plan.provider_id.clone(),
        endpoint_id: plan.endpoint_id.clone(),
        key_id: plan.key_id.clone(),
    };
    state.remember_scheduler_affinity_target(
        &cache_key,
        target.clone(),
        SCHEDULER_AFFINITY_TTL,
        16,
    );
    let initial_epoch = state.scheduler_affinity_epoch();

    apply_local_execution_effect(
        &state,
        LocalExecutionEffectContext {
            plan: &plan,
            report_context: None,
        },
        LocalExecutionEffect::AdaptiveSuccess(LocalAdaptiveSuccessEffect),
    )
    .await;

    let stored_key = state
        .read_provider_catalog_keys_by_ids(std::slice::from_ref(&plan.key_id))
        .await
        .expect("provider catalog keys should load")
        .into_iter()
        .next()
        .expect("stored key should exist");
    assert_eq!(stored_key.learned_rpm_limit, Some(25));
    assert_eq!(stored_key.utilization_samples, Some(json!([])));
    assert_eq!(
        stored_key
            .adjustment_history
            .as_ref()
            .and_then(Value::as_array)
            .and_then(|items| items.last())
            .and_then(Value::as_object)
            .and_then(|record| record.get("reason"))
            .and_then(Value::as_str),
        Some("high_utilization")
    );
    assert_eq!(state.scheduler_affinity_epoch(), initial_epoch);
    assert_eq!(
        state.read_scheduler_affinity_target(cache_key.as_str(), SCHEDULER_AFFINITY_TTL),
        Some(target)
    );
}
