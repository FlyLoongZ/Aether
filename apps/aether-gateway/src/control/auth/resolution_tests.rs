use std::sync::Arc;
use std::time::Duration;

use aether_data::repository::auth::{
    AuthApiKeyWriteRepository, CreateUserApiKeyRecord, InMemoryAuthApiKeySnapshotRepository,
    StoredAuthApiKeySnapshot,
};
use aether_data::repository::provider_catalog::InMemoryProviderCatalogReadRepository;
use aether_data::repository::wallet::{
    InMemoryWalletRepository, StoredWalletSnapshot, WalletReadRepository,
};
use aether_data::{DatabaseDriver, SqlDatabaseConfig, SqlPoolConfig};
use aether_data_contracts::repository::provider_catalog::{
    StoredProviderCatalogEndpoint, StoredProviderCatalogProvider,
};
use aether_runtime::ConcurrencyGate;
use axum::http::{HeaderMap, Uri};
use futures_util::future::join_all;

use super::{
    api_key_is_expired, get_cached_auth_context,
    refresh_execution_runtime_auth_context_with_snapshot, resolve_control_decision_auth,
    resolve_data_backed_auth_context, resolve_execution_runtime_auth_context,
    ControlDecisionAuthResolution, GatewayLocalAuthRejection,
};
use crate::control::auth::credentials::{build_auth_context_cache_key, hash_api_key};
use crate::control::GatewayControlDecision;
use crate::data::{GatewayDataConfig, GatewayDataState};
use crate::AppState;

fn sample_snapshot(api_key_id: &str, user_id: &str) -> StoredAuthApiKeySnapshot {
    StoredAuthApiKeySnapshot::new(
        user_id.to_string(),
        "alice".to_string(),
        Some("alice@example.com".to_string()),
        "user".to_string(),
        "local".to_string(),
        true,
        false,
        Some(serde_json::json!(["openai"])),
        Some(serde_json::json!(["openai:chat"])),
        Some(serde_json::json!(["gpt-4.1"])),
        api_key_id.to_string(),
        Some("default".to_string()),
        true,
        false,
        false,
        Some(60),
        Some(5),
        Some(4_102_444_800),
        Some(serde_json::json!(["openai"])),
        Some(serde_json::json!(["openai:chat"])),
        Some(serde_json::json!(["gpt-4.1"])),
    )
    .expect("snapshot should build")
}

fn uri(path: &str) -> Uri {
    path.parse().expect("uri should parse")
}

#[test]
fn api_key_expiry_is_inclusive_at_the_declared_second() {
    assert!(!api_key_is_expired(None, 100));
    assert!(!api_key_is_expired(Some(101), 100));
    assert!(api_key_is_expired(Some(100), 100));
    assert!(api_key_is_expired(Some(99), 100));
}

fn sample_provider(id: &str, name: &str, provider_type: &str) -> StoredProviderCatalogProvider {
    StoredProviderCatalogProvider::new(
        id.to_string(),
        name.to_string(),
        None,
        provider_type.to_string(),
    )
    .expect("provider should build")
}

fn sample_endpoint(id: &str, provider_id: &str, api_format: &str) -> StoredProviderCatalogEndpoint {
    StoredProviderCatalogEndpoint::new(
        id.to_string(),
        provider_id.to_string(),
        api_format.to_string(),
        None,
        None,
        true,
    )
    .expect("endpoint should build")
}

struct PostgresAuthConfigNodes {
    first: AppState,
    second: AppState,
    _database: aether_testkit::ManagedPostgresServer,
}

async fn postgres_auth_config_nodes(
    auth_repository: Arc<InMemoryAuthApiKeySnapshotRepository>,
) -> PostgresAuthConfigNodes {
    let server = aether_testkit::ManagedPostgresServer::start()
        .await
        .expect("temporary PostgreSQL should start");
    let mut pool = SqlPoolConfig::default();
    pool.min_connections = 0;
    pool.max_connections = 4;
    let database = SqlDatabaseConfig::new(DatabaseDriver::Postgres, server.database_url(), pool)
        .expect("postgres config should build");
    let config = GatewayDataConfig::from_database_config(database);
    let first_data = GatewayDataState::from_config(config.clone())
        .expect("first data state should build")
        .with_auth_api_key_reader(auth_repository.clone())
        .without_wallet_reader_for_tests();
    assert!(first_data
        .run_database_migrations()
        .await
        .expect("postgres migrations should run"));
    let second_data = GatewayDataState::from_config(config)
        .expect("second data state should build")
        .with_auth_api_key_reader(auth_repository)
        .without_wallet_reader_for_tests();

    PostgresAuthConfigNodes {
        first: AppState::new()
            .expect("first app state should build")
            .with_data_state_for_tests(first_data),
        second: AppState::new()
            .expect("second app state should build")
            .with_data_state_for_tests(second_data),
        _database: server,
    }
}

#[tokio::test]
async fn strong_system_config_read_bypasses_the_app_cache() {
    let nodes =
        postgres_auth_config_nodes(Arc::new(InMemoryAuthApiKeySnapshotRepository::seed([]))).await;
    let key = format!("test.auth.strong-read.{}", uuid::Uuid::new_v4());
    let old_value = serde_json::json!({"version": "old"});
    let new_value = serde_json::json!({"version": "new"});

    nodes
        .first
        .data
        .upsert_system_config_value(&key, &old_value, None)
        .await
        .expect("initial config should write");
    assert_eq!(
        nodes
            .first
            .read_system_config_json_value(&key)
            .await
            .expect("initial app config read should succeed"),
        Some(old_value.clone())
    );
    nodes
        .second
        .data
        .upsert_system_config_value(&key, &new_value, None)
        .await
        .expect("cross-node config update should write");

    assert_eq!(
        nodes
            .first
            .read_system_config_json_value(&key)
            .await
            .expect("stale app config read should succeed"),
        Some(old_value.clone()),
        "node-local AppState cache should still contain the old value"
    );
    assert_eq!(
        nodes
            .first
            .data
            .find_system_config_value_strong(&key)
            .await
            .expect("strong config read should succeed"),
        Some(new_value),
        "strong reads must reach the shared repository"
    );
}

#[tokio::test]
async fn control_auth_caches_invalid_api_key_rejections() {
    let repository = Arc::new(InMemoryAuthApiKeySnapshotRepository::seed([]));
    let data = GatewayDataState::with_auth_api_key_repository_for_tests(repository);
    let state = AppState::new()
        .expect("state should build")
        .with_data_state_for_tests(data);
    let mut headers = HeaderMap::new();
    headers.insert(
        http::header::AUTHORIZATION,
        "Bearer sk-missing-for-negative-cache".parse().unwrap(),
    );
    let request_uri = uri("/v1/chat/completions");
    let decision = GatewayControlDecision::synthetic(
        "/v1/chat/completions",
        Some("ai_public".to_string()),
        Some("openai".to_string()),
        Some("chat".to_string()),
        Some("openai:chat".to_string()),
    );

    let ControlDecisionAuthResolution::Resolved(first) = resolve_control_decision_auth(
        &state,
        &headers,
        &request_uri,
        "trace-invalid-auth-cache",
        decision,
    )
    .await
    .expect("auth resolution should succeed");

    assert_eq!(
        first.local_auth_rejection,
        Some(GatewayLocalAuthRejection::InvalidApiKey)
    );
    let cache_key = build_auth_context_cache_key(&headers, &request_uri, "openai:chat")
        .expect("cache key should exist");
    let cached = get_cached_auth_context(&state, &cache_key)
        .expect("invalid API key rejection should be cached");
    assert_eq!(
        cached.local_rejection,
        Some(GatewayLocalAuthRejection::InvalidApiKey)
    );
    assert!(cached.user_id.is_empty());
    assert!(cached.api_key_id.is_empty());
}

#[tokio::test]
async fn uncached_invalid_api_key_lookup_waits_for_auth_snapshot_gate() {
    let repository = Arc::new(InMemoryAuthApiKeySnapshotRepository::seed([]));
    let data = GatewayDataState::with_auth_api_key_repository_for_tests(repository.clone());
    let mut state = AppState::new()
        .expect("state should build")
        .with_data_state_for_tests(data);
    state.auth_snapshot_load_gate = Some(Arc::new(ConcurrencyGate::new(
        "test_invalid_auth_snapshot_lookup",
        1,
    )));
    let held = state
        .acquire_auth_snapshot_load_gate()
        .await
        .expect("auth gate acquisition should succeed")
        .expect("auth gate should be configured");
    let api_key = "sk-missing-auth-gate";
    let mut headers = HeaderMap::new();
    headers.insert(
        http::header::AUTHORIZATION,
        format!("Bearer {api_key}").parse().unwrap(),
    );
    let request_uri = uri("/v1/chat/completions");

    let blocked = tokio::time::timeout(
        Duration::from_millis(25),
        resolve_data_backed_auth_context(&state, &headers, &request_uri, Some("openai:chat")),
    )
    .await;
    assert!(
        blocked.is_err(),
        "an uncached invalid-key strong read must wait for auth DB admission"
    );
    assert_eq!(repository.key_hash_lookup_count(&hash_api_key(api_key)), 0);

    drop(held);
    let resolved = tokio::time::timeout(
        Duration::from_secs(1),
        resolve_data_backed_auth_context(&state, &headers, &request_uri, Some("openai:chat")),
    )
    .await
    .expect("auth lookup should resume after releasing the gate")
    .expect("auth resolution should succeed")
    .expect("invalid API key should resolve to a rejection context");
    assert_eq!(
        resolved.local_rejection,
        Some(GatewayLocalAuthRejection::InvalidApiKey)
    );
    assert_eq!(repository.key_hash_lookup_count(&hash_api_key(api_key)), 1);
}

#[tokio::test]
async fn data_backed_api_key_auth_touches_last_used_once_per_throttle_window() {
    let api_key = "sk-test-touch";
    let repository = Arc::new(InMemoryAuthApiKeySnapshotRepository::seed(vec![(
        Some(hash_api_key(api_key)),
        sample_snapshot("key-1", "user-1"),
    )]));
    let data = GatewayDataState::with_auth_api_key_repository_for_tests(repository.clone());
    let state = AppState::new()
        .expect("state should build")
        .with_data_state_for_tests(data);

    let mut headers = HeaderMap::new();
    headers.insert(
        http::header::AUTHORIZATION,
        format!("Bearer {api_key}").parse().unwrap(),
    );

    let first = resolve_data_backed_auth_context(
        &state,
        &headers,
        &uri("/v1/chat/completions"),
        Some("openai:chat"),
    )
    .await
    .expect("resolution should succeed")
    .expect("auth context should exist");
    assert_eq!(first.user_id, "user-1");
    assert_eq!(first.api_key_id, "key-1");
    assert_eq!(repository.touch_count("key-1"), 1);

    let second = resolve_data_backed_auth_context(
        &state,
        &headers,
        &uri("/v1/chat/completions"),
        Some("openai:chat"),
    )
    .await
    .expect("resolution should succeed")
    .expect("auth context should exist");
    assert_eq!(second.api_key_id, "key-1");
    assert_eq!(repository.touch_count("key-1"), 1);
}

#[tokio::test]
async fn long_lived_refresh_rejects_same_ids_recreated_with_a_different_credential() {
    let old_api_key = "sk-old-websocket-credential";
    let new_api_key = "sk-new-websocket-credential";
    let old_key_hash = hash_api_key(old_api_key);
    let new_key_hash = hash_api_key(new_api_key);
    let mut old_snapshot = sample_snapshot("key-stable-id", "user-stable-id");
    old_snapshot.user_allowed_api_formats = Some(vec!["openai:responses".to_string()]);
    old_snapshot.api_key_allowed_api_formats = Some(vec!["openai:responses".to_string()]);
    let repository = Arc::new(InMemoryAuthApiKeySnapshotRepository::seed(vec![(
        Some(old_key_hash.clone()),
        old_snapshot,
    )]));
    let data = GatewayDataState::with_auth_api_key_repository_for_tests(repository.clone());
    let state = AppState::new()
        .expect("state should build")
        .with_data_state_for_tests(data);
    let mut headers = HeaderMap::new();
    headers.insert(
        http::header::AUTHORIZATION,
        format!("Bearer {old_api_key}").parse().unwrap(),
    );

    let original = resolve_data_backed_auth_context(
        &state,
        &headers,
        &uri("/v1/responses"),
        Some("openai:responses"),
    )
    .await
    .expect("initial auth resolution should succeed")
    .expect("the old API key should authenticate");
    assert!(original.access_allowed);
    assert!(original.verified_api_key_hash.is_some());
    assert!(
        !format!("{original:?}").contains(&old_key_hash),
        "the credential verifier must stay redacted from Debug output"
    );

    assert!(repository
        .delete_user_api_key("user-stable-id", "key-stable-id")
        .await
        .expect("old API key deletion should succeed"));
    repository
        .create_user_api_key(CreateUserApiKeyRecord {
            user_id: "user-stable-id".to_string(),
            api_key_id: "key-stable-id".to_string(),
            key_hash: new_key_hash,
            key_encrypted: None,
            name: Some("restored-with-new-secret".to_string()),
            allowed_providers: Some(vec!["openai".to_string()]),
            allowed_api_formats: Some(vec!["openai:responses".to_string()]),
            allowed_models: Some(vec!["gpt-4.1".to_string()]),
            ip_rules: None,
            rate_limit: 60,
            concurrent_limit: Some(5),
            force_capabilities: None,
            feature_settings: None,
            is_active: true,
            expires_at_unix_secs: Some(4_102_444_800),
            auto_delete_on_expiry: false,
            total_requests: 0,
            total_tokens: 0,
            total_cost_usd: 0.0,
        })
        .await
        .expect("same-ID API key recreation should resolve")
        .expect("same-ID API key recreation should persist");

    let (refreshed, snapshot) = refresh_execution_runtime_auth_context_with_snapshot(
        &state,
        original,
        Some("openai:responses"),
    )
    .await
    .expect("long-lived auth refresh should resolve");

    assert!(!refreshed.access_allowed);
    assert_eq!(
        refreshed.local_rejection,
        Some(GatewayLocalAuthRejection::InvalidApiKey)
    );
    assert!(snapshot.is_none());
    assert_eq!(repository.key_hash_lookup_count(&old_key_hash), 1);
    assert_eq!(
        repository.snapshot_lookup_count("key-stable-id"),
        0,
        "a bound long-lived credential must not fall back to identity-only lookup"
    );
}

#[tokio::test]
async fn control_auth_context_singleflights_concurrent_cache_misses() {
    let api_key = "sk-test-concurrent-auth-miss";
    let repository = Arc::new(
        InMemoryAuthApiKeySnapshotRepository::seed(vec![(
            Some(hash_api_key(api_key)),
            sample_snapshot("key-concurrent-auth-miss", "user-concurrent-auth-miss"),
        )])
        .with_lookup_delay_for_tests(Duration::from_millis(20)),
    );
    let data = GatewayDataState::with_auth_api_key_repository_for_tests(repository.clone());
    let state = AppState::new()
        .expect("state should build")
        .with_data_state_for_tests(data);
    let mut headers = HeaderMap::new();
    headers.insert(
        http::header::AUTHORIZATION,
        format!("Bearer {api_key}").parse().unwrap(),
    );
    let request_uri = uri("/v1/chat/completions");

    let tasks = (0..32).map(|index| {
        let decision = GatewayControlDecision::synthetic(
            "/v1/chat/completions",
            Some("ai_public".to_string()),
            Some("openai".to_string()),
            Some("chat".to_string()),
            Some("openai:chat".to_string()),
        );
        let trace_id = format!("trace-concurrent-auth-miss-{index}");
        let state = &state;
        let headers = &headers;
        let request_uri = &request_uri;
        async move {
            resolve_control_decision_auth(state, headers, request_uri, &trace_id, decision).await
        }
    });

    for result in join_all(tasks).await {
        let ControlDecisionAuthResolution::Resolved(decision) =
            result.expect("auth resolution should succeed");
        let auth_context = decision
            .auth_context
            .expect("auth context should be resolved");
        assert_eq!(auth_context.user_id, "user-concurrent-auth-miss");
        assert_eq!(auth_context.api_key_id, "key-concurrent-auth-miss");
    }
    assert_eq!(
        repository.key_hash_lookup_count(&hash_api_key(api_key)),
        1,
        "concurrent cache misses for one auth context should only load one snapshot"
    );
}

#[tokio::test]
async fn data_backed_auth_context_marks_wallet_denial_as_not_allowed() {
    let api_key = "sk-test-empty-wallet";
    let auth_repository = Arc::new(InMemoryAuthApiKeySnapshotRepository::seed(vec![(
        Some(hash_api_key(api_key)),
        sample_snapshot("key-empty-wallet", "user-empty-wallet"),
    )]));
    let wallet_repository = Arc::new(InMemoryWalletRepository::seed(vec![
        StoredWalletSnapshot::new(
            "wallet-empty".to_string(),
            Some("user-empty-wallet".to_string()),
            None,
            0.0,
            0.0,
            "finite".to_string(),
            "USD".to_string(),
            "active".to_string(),
            0.0,
            0.0,
            0.0,
            0.0,
            100,
        )
        .expect("wallet should build"),
    ]));
    let data = GatewayDataState::with_auth_and_wallet_for_tests(auth_repository, wallet_repository);
    let state = AppState::new()
        .expect("state should build")
        .with_data_state_for_tests(data);

    let mut headers = HeaderMap::new();
    headers.insert(
        http::header::AUTHORIZATION,
        format!("Bearer {api_key}").parse().unwrap(),
    );

    let auth_context = resolve_data_backed_auth_context(
        &state,
        &headers,
        &uri("/v1/chat/completions"),
        Some("openai:chat"),
    )
    .await
    .expect("resolution should succeed")
    .expect("auth context should exist");

    assert_eq!(
        auth_context.local_rejection,
        Some(GatewayLocalAuthRejection::BalanceDenied {
            remaining: Some(0.0),
        })
    );
    assert!(!auth_context.access_allowed);
}

#[tokio::test]
async fn execution_runtime_auth_context_revalidates_cached_wallet_state() {
    let api_key = "sk-test-runtime-wallet-cache";
    let auth_repository = Arc::new(InMemoryAuthApiKeySnapshotRepository::seed(vec![(
        Some(hash_api_key(api_key)),
        sample_snapshot("key-runtime-wallet-cache", "user-runtime-wallet-cache"),
    )]));
    let wallet_repository = Arc::new(InMemoryWalletRepository::seed(vec![
        StoredWalletSnapshot::new(
            "wallet-runtime-cache".to_string(),
            Some("user-runtime-wallet-cache".to_string()),
            None,
            10.0,
            0.0,
            "finite".to_string(),
            "USD".to_string(),
            "active".to_string(),
            10.0,
            0.0,
            0.0,
            0.0,
            100,
        )
        .expect("wallet should build"),
    ]));
    let data = GatewayDataState::with_auth_and_wallet_for_tests(
        auth_repository.clone(),
        Arc::clone(&wallet_repository),
    );
    let state = AppState::new()
        .expect("state should build")
        .with_data_state_for_tests(data);
    let decision = GatewayControlDecision::synthetic(
        "/v1/chat/completions",
        Some("ai_public".to_string()),
        Some("openai".to_string()),
        Some("chat".to_string()),
        Some("openai:chat".to_string()),
    );
    let mut headers = HeaderMap::new();
    headers.insert("x-api-key", api_key.parse().unwrap());

    let first = resolve_execution_runtime_auth_context(
        &state,
        &decision,
        &headers,
        &uri("/v1/chat/completions"),
        "trace-runtime-wallet-cache",
    )
    .await
    .expect("resolution should succeed")
    .expect("auth context should exist");
    assert!(first.access_allowed);
    state
        .auth_context_cache
        .set_refresh_interval_for_tests(Duration::from_millis(10));

    wallet_repository
        .update_auth_user_wallet_snapshot(
            "user-runtime-wallet-cache",
            0.0,
            0.0,
            "finite",
            "USD",
            "active",
            10.0,
            10.0,
            0.0,
            0.0,
            Some(101),
        )
        .await
        .expect("wallet update should succeed")
        .expect("wallet should exist");
    tokio::time::sleep(Duration::from_millis(15)).await;

    let second = tokio::time::timeout(
        Duration::from_millis(100),
        resolve_execution_runtime_auth_context(
            &state,
            &decision,
            &headers,
            &uri("/v1/chat/completions"),
            "trace-runtime-wallet-cache",
        ),
    )
    .await
    .expect("due security refresh should complete")
    .expect("resolution should succeed")
    .expect("auth context should exist");
    assert!(
        !second.access_allowed,
        "the due request must observe denial"
    );

    let cache_key =
        build_auth_context_cache_key(&headers, &uri("/v1/chat/completions"), "openai:chat")
            .expect("cache key should exist");
    let refreshed = get_cached_auth_context(&state, &cache_key)
        .expect("synchronous refresh should publish the wallet denial");
    assert_eq!(
        refreshed.local_rejection,
        Some(GatewayLocalAuthRejection::BalanceDenied {
            remaining: Some(0.0),
        }),
        "auth refresh should publish current wallet state"
    );
    assert!(!refreshed.access_allowed);
    assert_eq!(
        auth_repository.key_hash_lookup_count(&hash_api_key(api_key)),
        2,
        "initial auth and due refresh must each validate the presented credential"
    );
    assert_eq!(
        auth_repository.snapshot_lookup_count("key-runtime-wallet-cache"),
        0,
        "due refresh must not trust the cached user/key mapping"
    );
}

#[tokio::test]
async fn due_auth_context_refresh_observes_cross_node_key_lock() {
    let api_key = "sk-test-cross-node-lock";
    let auth_repository = Arc::new(InMemoryAuthApiKeySnapshotRepository::seed(vec![(
        Some(hash_api_key(api_key)),
        sample_snapshot("key-cross-node-lock", "user-cross-node-lock"),
    )]));
    let data = GatewayDataState::with_auth_api_key_repository_for_tests(auth_repository.clone());
    let state = AppState::new()
        .expect("state should build")
        .with_data_state_for_tests(data);
    state
        .auth_context_cache
        .set_refresh_interval_for_tests(Duration::from_millis(10));
    let decision = GatewayControlDecision::synthetic(
        "/v1/chat/completions",
        Some("ai_public".to_string()),
        Some("openai".to_string()),
        Some("chat".to_string()),
        Some("openai:chat".to_string()),
    );
    let mut headers = HeaderMap::new();
    headers.insert("x-api-key", api_key.parse().unwrap());
    let request_uri = uri("/v1/chat/completions");

    let first = resolve_execution_runtime_auth_context(
        &state,
        &decision,
        &headers,
        &request_uri,
        "trace-cross-node-lock-prime",
    )
    .await
    .expect("initial resolution should succeed")
    .expect("auth context should exist");
    assert!(first.access_allowed);

    assert!(auth_repository
        .set_user_api_key_locked("user-cross-node-lock", "key-cross-node-lock", true)
        .await
        .expect("simulated cross-node lock should succeed"));
    tokio::time::sleep(Duration::from_millis(15)).await;

    let refreshed = resolve_execution_runtime_auth_context(
        &state,
        &decision,
        &headers,
        &request_uri,
        "trace-cross-node-lock-refresh",
    )
    .await
    .expect("due resolution should succeed")
    .expect("auth context should exist");
    assert!(!refreshed.access_allowed);
    assert_eq!(
        refreshed.local_rejection,
        Some(GatewayLocalAuthRejection::LockedApiKey)
    );
}

#[tokio::test]
async fn auth_context_hard_miss_bypasses_fresh_snapshot_allow_caches() {
    let api_key = "sk-test-hard-miss-cross-node-lock";
    let key_hash = hash_api_key(api_key);
    let auth_repository = Arc::new(InMemoryAuthApiKeySnapshotRepository::seed(vec![(
        Some(key_hash.clone()),
        sample_snapshot("key-hard-miss-lock", "user-hard-miss-lock"),
    )]));
    let data =
        GatewayDataState::with_cached_auth_api_key_repository_for_tests(auth_repository.clone());
    let state = AppState::new()
        .expect("state should build")
        .with_data_state_for_tests(data);

    let primed = state
        .read_cached_auth_api_key_snapshot_by_key_hash(&key_hash, super::current_unix_secs())
        .await
        .expect("snapshot cache prime should succeed")
        .expect("snapshot should exist");
    assert!(primed.currently_usable);
    assert!(auth_repository
        .set_user_api_key_locked("user-hard-miss-lock", "key-hard-miss-lock", true)
        .await
        .expect("simulated cross-node lock should succeed"));

    let decision = GatewayControlDecision::synthetic(
        "/v1/chat/completions",
        Some("ai_public".to_string()),
        Some("openai".to_string()),
        Some("chat".to_string()),
        Some("openai:chat".to_string()),
    );
    let mut headers = HeaderMap::new();
    headers.insert("x-api-key", api_key.parse().unwrap());
    let resolved = resolve_execution_runtime_auth_context(
        &state,
        &decision,
        &headers,
        &uri("/v1/chat/completions"),
        "trace-hard-miss-cross-node-lock",
    )
    .await
    .expect("hard-miss resolution should succeed")
    .expect("auth context should exist");

    assert!(!resolved.access_allowed);
    assert_eq!(
        resolved.local_rejection,
        Some(GatewayLocalAuthRejection::LockedApiKey)
    );
    assert_eq!(
        auth_repository.key_hash_lookup_count(&key_hash),
        2,
        "hard miss must bypass both fresh snapshot cache layers"
    );
}

#[tokio::test]
async fn execution_runtime_auth_context_singleflights_concurrent_cache_refreshes() {
    let api_key = "sk-test-runtime-auth-refresh";
    let auth_repository = Arc::new(
        InMemoryAuthApiKeySnapshotRepository::seed(vec![(
            Some(hash_api_key(api_key)),
            sample_snapshot("key-runtime-auth-refresh", "user-runtime-auth-refresh"),
        )])
        .with_lookup_delay_for_tests(Duration::from_millis(200)),
    );
    let data = GatewayDataState::with_auth_api_key_repository_for_tests(auth_repository.clone());
    let state = AppState::new()
        .expect("state should build")
        .with_data_state_for_tests(data);
    let decision = GatewayControlDecision::synthetic(
        "/v1/chat/completions",
        Some("ai_public".to_string()),
        Some("openai".to_string()),
        Some("chat".to_string()),
        Some("openai:chat".to_string()),
    );
    let mut headers = HeaderMap::new();
    headers.insert("x-api-key", api_key.parse().unwrap());
    let request_uri = uri("/v1/chat/completions");

    let first = resolve_execution_runtime_auth_context(
        &state,
        &decision,
        &headers,
        &request_uri,
        "trace-runtime-auth-refresh-prime",
    )
    .await
    .expect("resolution should succeed")
    .expect("auth context should exist");
    assert_eq!(first.api_key_id, "key-runtime-auth-refresh");
    assert_eq!(
        auth_repository.key_hash_lookup_count(&hash_api_key(api_key)),
        1
    );
    state
        .auth_context_cache
        .set_refresh_interval_for_tests(Duration::from_millis(100));
    tokio::time::sleep(Duration::from_millis(110)).await;

    let tasks = (0..32).map(|index| {
        let trace_id = format!("trace-runtime-auth-refresh-{index}");
        let state = &state;
        let decision = &decision;
        let headers = &headers;
        let request_uri = &request_uri;
        async move {
            resolve_execution_runtime_auth_context(state, decision, headers, request_uri, &trace_id)
                .await
        }
    });

    let results = tokio::time::timeout(Duration::from_secs(1), join_all(tasks))
        .await
        .expect("same-key security refreshes should complete through one flight");
    for result in results {
        let auth_context = result
            .expect("resolution should succeed")
            .expect("auth context should exist");
        assert_eq!(auth_context.user_id, "user-runtime-auth-refresh");
        assert_eq!(auth_context.api_key_id, "key-runtime-auth-refresh");
    }
    assert_eq!(
        auth_repository.key_hash_lookup_count(&hash_api_key(api_key)),
        2,
        "one due security refresh should revalidate the original API key hash"
    );
    assert_eq!(
        auth_repository.snapshot_lookup_count("key-runtime-auth-refresh"),
        0,
        "full credential revalidation must not trust the cached user/key mapping"
    );
}

#[tokio::test]
async fn execution_runtime_auth_context_reuses_control_resolved_context() {
    let api_key = "sk-test-control-execution-auth-reuse";
    let auth_repository = Arc::new(InMemoryAuthApiKeySnapshotRepository::seed(vec![(
        Some(hash_api_key(api_key)),
        sample_snapshot(
            "key-control-execution-auth-reuse",
            "user-control-execution-auth-reuse",
        ),
    )]));
    let data = GatewayDataState::with_auth_api_key_repository_for_tests(auth_repository.clone());
    let state = AppState::new()
        .expect("state should build")
        .with_data_state_for_tests(data);
    let mut headers = HeaderMap::new();
    headers.insert("x-api-key", api_key.parse().unwrap());
    let request_uri = uri("/v1/chat/completions");

    let decision = GatewayControlDecision::synthetic(
        "/v1/chat/completions",
        Some("ai_public".to_string()),
        Some("openai".to_string()),
        Some("chat".to_string()),
        Some("openai:chat".to_string()),
    )
    .with_execution_runtime_candidate(true);
    let ControlDecisionAuthResolution::Resolved(first) = resolve_control_decision_auth(
        &state,
        &headers,
        &request_uri,
        "trace-control-execution-auth-reuse-first",
        decision,
    )
    .await
    .expect("control auth resolution should succeed");
    assert!(first.auth_context.is_some());
    assert_eq!(
        auth_repository.snapshot_lookup_count("key-control-execution-auth-reuse"),
        0,
        "initial auth resolution should use the key-hash lookup"
    );

    let mut second = None;
    for index in 0..32 {
        let decision = GatewayControlDecision::synthetic(
            "/v1/chat/completions",
            Some("ai_public".to_string()),
            Some("openai".to_string()),
            Some("chat".to_string()),
            Some("openai:chat".to_string()),
        )
        .with_execution_runtime_candidate(true);
        let ControlDecisionAuthResolution::Resolved(resolved) = resolve_control_decision_auth(
            &state,
            &headers,
            &request_uri,
            &format!("trace-control-execution-auth-reuse-{index}"),
            decision,
        )
        .await
        .expect("control auth resolution should succeed");
        second = Some(resolved);
    }
    let second = second.expect("fresh cache hit should resolve a decision");
    assert!(second.auth_context.is_some());
    let snapshot_lookups_after_control =
        auth_repository.snapshot_lookup_count("key-control-execution-auth-reuse");
    assert_eq!(
        snapshot_lookups_after_control, 0,
        "continuous fresh hits before soft TTL must not refresh auth"
    );

    let execution = resolve_execution_runtime_auth_context(
        &state,
        &second,
        &headers,
        &request_uri,
        "trace-control-execution-auth-reuse-execution",
    )
    .await
    .expect("execution auth resolution should succeed")
    .expect("execution auth context should exist");
    assert_eq!(execution.api_key_id, "key-control-execution-auth-reuse");
    let control_context = second
        .auth_context
        .as_ref()
        .expect("control auth context should exist");
    assert_eq!(
        execution.access_allowed, control_context.access_allowed,
        "execution must preserve the control-stage access decision"
    );
    assert_eq!(
        execution.local_rejection, control_context.local_rejection,
        "execution must preserve the control-stage local rejection"
    );
    assert_eq!(
        auth_repository.snapshot_lookup_count("key-control-execution-auth-reuse"),
        snapshot_lookups_after_control,
        "execution should reuse the control-resolved auth context without another snapshot lookup"
    );
}

#[tokio::test]
async fn data_backed_auth_context_allows_provider_id_for_matching_provider_type() {
    let api_key = "sk-test-provider-id";
    let mut snapshot = sample_snapshot("key-2", "user-2");
    snapshot.user_allowed_providers = Some(vec!["provider-openai-1".to_string()]);
    snapshot.api_key_allowed_providers = Some(vec!["provider-openai-1".to_string()]);
    let repository = Arc::new(InMemoryAuthApiKeySnapshotRepository::seed(vec![(
        Some(hash_api_key(api_key)),
        snapshot,
    )]));
    let provider_catalog = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![sample_provider(
            "provider-openai-1",
            "OpenAI Pool 1",
            "openai",
        )],
        Vec::new(),
        Vec::new(),
    ));
    let data = GatewayDataState::with_auth_api_key_reader_for_tests(repository)
        .with_provider_catalog_reader(provider_catalog);
    let state = AppState::new()
        .expect("state should build")
        .with_data_state_for_tests(data);

    let mut headers = HeaderMap::new();
    headers.insert("x-api-key", api_key.parse().unwrap());

    let auth_context = resolve_data_backed_auth_context(
        &state,
        &headers,
        &uri("/v1/chat/completions"),
        Some("openai:chat"),
    )
    .await
    .expect("resolution should succeed")
    .expect("auth context should exist");

    assert_eq!(auth_context.local_rejection, None);
}

#[tokio::test]
async fn data_backed_auth_context_allows_provider_id_for_matching_endpoint_format() {
    let api_key = "sk-test-provider-endpoint";
    let mut snapshot = sample_snapshot("key-4", "user-4");
    snapshot.user_allowed_providers = Some(vec!["provider-custom-claude".to_string()]);
    snapshot.api_key_allowed_providers = Some(vec!["provider-custom-claude".to_string()]);
    snapshot.user_allowed_api_formats = None;
    snapshot.api_key_allowed_api_formats = None;
    let repository = Arc::new(InMemoryAuthApiKeySnapshotRepository::seed(vec![(
        Some(hash_api_key(api_key)),
        snapshot,
    )]));
    let provider_catalog = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![sample_provider(
            "provider-custom-claude",
            "Custom Claude Gateway",
            "custom",
        )],
        vec![sample_endpoint(
            "endpoint-custom-claude",
            "provider-custom-claude",
            "claude:messages",
        )],
        Vec::new(),
    ));
    let data = GatewayDataState::with_auth_api_key_reader_for_tests(repository)
        .with_provider_catalog_reader(provider_catalog);
    let state = AppState::new()
        .expect("state should build")
        .with_data_state_for_tests(data);

    let mut headers = HeaderMap::new();
    headers.insert("x-api-key", api_key.parse().unwrap());

    let auth_context = resolve_data_backed_auth_context(
        &state,
        &headers,
        &uri("/v1/messages"),
        Some("claude:messages"),
    )
    .await
    .expect("resolution should succeed")
    .expect("auth context should exist");

    assert_eq!(auth_context.local_rejection, None);
}

#[tokio::test]
async fn data_backed_auth_context_denies_unresolved_provider_id_without_catalog_reader() {
    let api_key = "sk-test-provider-no-catalog";
    let mut snapshot = sample_snapshot("key-no-catalog", "user-no-catalog");
    snapshot.user_allowed_providers = Some(vec!["provider-custom-claude".to_string()]);
    snapshot.api_key_allowed_providers = Some(vec!["provider-custom-claude".to_string()]);
    snapshot.user_allowed_api_formats = None;
    snapshot.api_key_allowed_api_formats = None;
    let repository = Arc::new(InMemoryAuthApiKeySnapshotRepository::seed(vec![(
        Some(hash_api_key(api_key)),
        snapshot,
    )]));
    let data = GatewayDataState::with_auth_api_key_reader_for_tests(repository);
    let state = AppState::new()
        .expect("state should build")
        .with_data_state_for_tests(data);
    let mut headers = HeaderMap::new();
    headers.insert("x-api-key", api_key.parse().unwrap());

    let auth_context = resolve_data_backed_auth_context(
        &state,
        &headers,
        &uri("/v1/messages"),
        Some("claude:messages"),
    )
    .await
    .expect("resolution should succeed")
    .expect("auth context should exist");

    assert!(!auth_context.access_allowed);
    assert_eq!(
        auth_context.local_rejection,
        Some(GatewayLocalAuthRejection::ProviderNotAllowed {
            provider: "claude".to_string(),
        })
    );
}

#[tokio::test]
async fn due_antigravity_bearer_refresh_observes_cross_node_allowlist_revocation() {
    let raw_bearer = "google-oauth-access-token-revoked-cross-node";
    let mut snapshot = sample_snapshot(
        "key-antigravity-bearer-revocation",
        "user-antigravity-bearer-revocation",
    );
    snapshot.user_allowed_providers = Some(vec!["antigravity".to_string()]);
    snapshot.api_key_allowed_providers = Some(vec!["antigravity".to_string()]);
    snapshot.user_allowed_api_formats = Some(vec!["gemini:generate_content".to_string()]);
    snapshot.api_key_allowed_api_formats = Some(vec!["gemini:generate_content".to_string()]);
    let auth_repository = Arc::new(InMemoryAuthApiKeySnapshotRepository::seed(vec![(
        None, snapshot,
    )]));
    let nodes = postgres_auth_config_nodes(auth_repository.clone()).await;
    nodes
        .first
        .auth_context_cache
        .set_refresh_interval_for_tests(Duration::from_millis(10));
    let allowed_config = serde_json::json!({
        "enabled": true,
        "auth_user_id": "user-antigravity-bearer-revocation",
        "auth_api_key_id": "key-antigravity-bearer-revocation",
        "bearer_sha256_allowlist": [hash_api_key(raw_bearer)]
    });
    nodes
        .first
        .data
        .upsert_system_config_value(
            crate::constants::ANTIGRAVITY_BEARER_BRIDGE_CONFIG_KEY,
            &allowed_config,
            None,
        )
        .await
        .expect("initial bearer bridge config should write");
    assert_eq!(
        nodes
            .first
            .read_system_config_json_value(crate::constants::ANTIGRAVITY_BEARER_BRIDGE_CONFIG_KEY,)
            .await
            .expect("initial bridge config read should succeed"),
        Some(allowed_config.clone())
    );

    let decision = GatewayControlDecision::synthetic(
        "/v1internal:streamGenerateContent",
        Some("ai_public".to_string()),
        Some("antigravity".to_string()),
        Some("v1internal".to_string()),
        Some("antigravity:v1internal".to_string()),
    );
    let mut headers = HeaderMap::new();
    headers.insert(
        http::header::AUTHORIZATION,
        format!("Bearer {raw_bearer}").parse().unwrap(),
    );
    let request_uri = uri("/v1internal:streamGenerateContent?alt=sse");
    let initial = resolve_execution_runtime_auth_context(
        &nodes.first,
        &decision,
        &headers,
        &request_uri,
        "trace-antigravity-bearer-revocation-prime",
    )
    .await
    .expect("initial bearer auth should resolve")
    .expect("initial bearer auth context should exist");
    assert!(initial.access_allowed);

    let revoked_config = serde_json::json!({
        "enabled": true,
        "auth_user_id": "user-antigravity-bearer-revocation",
        "auth_api_key_id": "key-antigravity-bearer-revocation",
        "bearer_sha256_allowlist": [hash_api_key("different-bearer")]
    });
    nodes
        .second
        .data
        .upsert_system_config_value(
            crate::constants::ANTIGRAVITY_BEARER_BRIDGE_CONFIG_KEY,
            &revoked_config,
            None,
        )
        .await
        .expect("cross-node bearer revocation should write");
    assert_eq!(
        nodes
            .first
            .read_system_config_json_value(crate::constants::ANTIGRAVITY_BEARER_BRIDGE_CONFIG_KEY,)
            .await
            .expect("stale app config read should succeed"),
        Some(allowed_config),
        "the app cache should remain stale for the regression setup"
    );
    tokio::time::sleep(Duration::from_millis(15)).await;

    let refreshed = resolve_execution_runtime_auth_context(
        &nodes.first,
        &decision,
        &headers,
        &request_uri,
        "trace-antigravity-bearer-revocation-refresh",
    )
    .await
    .expect("due bearer auth refresh should resolve")
    .expect("due bearer auth context should exist");
    assert!(!refreshed.access_allowed);
    assert_eq!(
        refreshed.local_rejection,
        Some(GatewayLocalAuthRejection::InvalidApiKey)
    );
    assert_eq!(
        auth_repository.snapshot_lookup_count("key-antigravity-bearer-revocation"),
        1,
        "a revoked bearer must be rejected before its mapped API key is reloaded"
    );
}

#[tokio::test]
async fn data_backed_auth_context_allows_antigravity_v1internal_for_gemini_generate_content_keys() {
    let api_key = "sk-test-antigravity-v1internal";
    let mut snapshot = sample_snapshot("key-ant-v1internal", "user-ant-v1internal");
    snapshot.user_allowed_providers = Some(vec!["antigravity".to_string()]);
    snapshot.api_key_allowed_providers = Some(vec!["antigravity".to_string()]);
    snapshot.user_allowed_api_formats = Some(vec!["gemini:generate_content".to_string()]);
    snapshot.api_key_allowed_api_formats = Some(vec!["gemini:generate_content".to_string()]);
    let repository = Arc::new(InMemoryAuthApiKeySnapshotRepository::seed(vec![(
        Some(hash_api_key(api_key)),
        snapshot,
    )]));
    let provider_catalog = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![sample_provider(
            "provider-antigravity-1",
            "Antigravity",
            "antigravity",
        )],
        vec![sample_endpoint(
            "endpoint-antigravity-1",
            "provider-antigravity-1",
            "gemini:generate_content",
        )],
        Vec::new(),
    ));
    let data = GatewayDataState::with_auth_api_key_reader_for_tests(repository)
        .with_provider_catalog_reader(provider_catalog);
    let state = AppState::new()
        .expect("state should build")
        .with_data_state_for_tests(data);

    let mut headers = HeaderMap::new();
    headers.insert("x-api-key", api_key.parse().unwrap());
    headers.insert(
        http::header::AUTHORIZATION,
        "Bearer google-oauth-access-token".parse().unwrap(),
    );

    let auth_context = resolve_data_backed_auth_context(
        &state,
        &headers,
        &uri("/v1internal:streamGenerateContent?alt=sse"),
        Some("antigravity:v1internal"),
    )
    .await
    .expect("resolution should succeed")
    .expect("auth context should exist");

    assert_eq!(auth_context.local_rejection, None);
}

#[tokio::test]
async fn data_backed_auth_context_allows_provider_id_for_convertible_endpoint_format() {
    let api_key = "sk-test-provider-convertible-endpoint";
    let mut snapshot = sample_snapshot("key-9", "user-9");
    snapshot.api_key_is_standalone = true;
    snapshot.user_allowed_providers = None;
    snapshot.api_key_allowed_providers = Some(vec!["provider-custom-openai".to_string()]);
    snapshot.user_allowed_api_formats = None;
    snapshot.api_key_allowed_api_formats = None;
    let repository = Arc::new(InMemoryAuthApiKeySnapshotRepository::seed(vec![(
        Some(hash_api_key(api_key)),
        snapshot,
    )]));
    let provider_catalog = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![sample_provider(
            "provider-custom-openai",
            "Custom OpenAI Responses Gateway",
            "custom",
        )],
        vec![sample_endpoint(
            "endpoint-custom-openai-responses",
            "provider-custom-openai",
            "openai:responses",
        )],
        Vec::new(),
    ));
    let data = GatewayDataState::with_auth_api_key_reader_for_tests(repository)
        .with_provider_catalog_reader(provider_catalog);
    let state = AppState::new()
        .expect("state should build")
        .with_data_state_for_tests(data);

    let mut headers = HeaderMap::new();
    headers.insert("x-api-key", api_key.parse().unwrap());

    let auth_context = resolve_data_backed_auth_context(
        &state,
        &headers,
        &uri("/v1/messages?beta=true"),
        Some("claude:messages"),
    )
    .await
    .expect("resolution should succeed")
    .expect("auth context should exist");

    assert_eq!(auth_context.local_rejection, None);
}

#[tokio::test]
async fn data_backed_auth_context_denies_retired_anthropic_provider_alias_for_claude_route() {
    let api_key = "sk-test-provider-retired-anthropic-alias";
    let mut snapshot = sample_snapshot("key-5", "user-5");
    snapshot.user_allowed_providers = Some(vec!["anthropic".to_string()]);
    snapshot.api_key_allowed_providers = Some(vec!["anthropic".to_string()]);
    snapshot.user_allowed_api_formats = None;
    snapshot.api_key_allowed_api_formats = None;
    let repository = Arc::new(InMemoryAuthApiKeySnapshotRepository::seed(vec![(
        Some(hash_api_key(api_key)),
        snapshot,
    )]));
    let provider_catalog = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![sample_provider("provider-claude", "Claude", "custom")],
        Vec::new(),
        Vec::new(),
    ));
    let data = GatewayDataState::with_auth_api_key_reader_for_tests(repository)
        .with_provider_catalog_reader(provider_catalog);
    let state = AppState::new()
        .expect("state should build")
        .with_data_state_for_tests(data);

    let mut headers = HeaderMap::new();
    headers.insert("x-api-key", api_key.parse().unwrap());

    let auth_context = resolve_data_backed_auth_context(
        &state,
        &headers,
        &uri("/v1/messages"),
        Some("claude:messages"),
    )
    .await
    .expect("resolution should succeed")
    .expect("auth context should exist");

    assert_eq!(
        auth_context.local_rejection,
        Some(GatewayLocalAuthRejection::ProviderNotAllowed {
            provider: "claude".to_string(),
        })
    );
}

#[tokio::test]
async fn data_backed_auth_context_treats_empty_allowed_lists_as_unrestricted() {
    let api_key = "sk-test-empty-restrictions";
    let mut snapshot = sample_snapshot("key-6", "user-6");
    snapshot.api_key_is_standalone = true;
    snapshot.user_allowed_providers = Some(vec!["openai".to_string()]);
    snapshot.user_allowed_api_formats = Some(vec!["openai:chat".to_string()]);
    snapshot.user_allowed_models = Some(vec!["gpt-4.1".to_string()]);
    snapshot.api_key_allowed_providers = Some(Vec::new());
    snapshot.api_key_allowed_api_formats = Some(Vec::new());
    snapshot.api_key_allowed_models = Some(Vec::new());
    let repository = Arc::new(InMemoryAuthApiKeySnapshotRepository::seed(vec![(
        Some(hash_api_key(api_key)),
        snapshot,
    )]));
    let provider_catalog = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![sample_provider("provider-claude", "Claude", "custom")],
        vec![sample_endpoint(
            "endpoint-claude",
            "provider-claude",
            "claude:messages",
        )],
        Vec::new(),
    ));
    let data = GatewayDataState::with_auth_api_key_reader_for_tests(repository)
        .with_provider_catalog_reader(provider_catalog);
    let state = AppState::new()
        .expect("state should build")
        .with_data_state_for_tests(data);

    let mut headers = HeaderMap::new();
    headers.insert("x-api-key", api_key.parse().unwrap());

    let auth_context = resolve_data_backed_auth_context(
        &state,
        &headers,
        &uri("/v1/messages"),
        Some("claude:messages"),
    )
    .await
    .expect("resolution should succeed")
    .expect("auth context should exist");

    assert_eq!(auth_context.local_rejection, None);
}

#[tokio::test]
async fn data_backed_auth_context_denies_provider_type_without_matching_allowed_provider() {
    let api_key = "sk-test-provider-miss";
    let mut snapshot = sample_snapshot("key-3", "user-3");
    snapshot.user_allowed_providers = Some(vec!["provider-claude-1".to_string()]);
    snapshot.api_key_allowed_providers = Some(vec!["provider-claude-1".to_string()]);
    let repository = Arc::new(InMemoryAuthApiKeySnapshotRepository::seed(vec![(
        Some(hash_api_key(api_key)),
        snapshot,
    )]));
    let provider_catalog = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![
            sample_provider("provider-openai-1", "OpenAI Pool 1", "openai"),
            sample_provider("provider-claude-1", "Claude Pool 1", "claude"),
        ],
        Vec::new(),
        Vec::new(),
    ));
    let data = GatewayDataState::with_auth_api_key_reader_for_tests(repository)
        .with_provider_catalog_reader(provider_catalog);
    let state = AppState::new()
        .expect("state should build")
        .with_data_state_for_tests(data);

    let mut headers = HeaderMap::new();
    headers.insert(
        http::header::AUTHORIZATION,
        format!("Bearer {api_key}").parse().unwrap(),
    );

    let auth_context = resolve_data_backed_auth_context(
        &state,
        &headers,
        &uri("/v1/chat/completions"),
        Some("openai:chat"),
    )
    .await
    .expect("resolution should succeed")
    .expect("auth context should exist");

    assert_eq!(
        auth_context.local_rejection,
        Some(GatewayLocalAuthRejection::ProviderNotAllowed {
            provider: "openai".to_string(),
        })
    );
}
