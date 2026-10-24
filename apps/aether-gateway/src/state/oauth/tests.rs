use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use aether_crypto::{encrypt_python_fernet_plaintext, DEVELOPMENT_ENCRYPTION_KEY};
use aether_data::repository::provider_catalog::InMemoryProviderCatalogReadRepository;
use aether_data_contracts::repository::provider_catalog::{
    ProviderCatalogKeyAdminCasUpdate, ProviderCatalogKeyListQuery,
    ProviderCatalogKeyOAuthCredentialFence, ProviderCatalogReadRepository,
    ProviderCatalogWriteRepository, StoredProviderCatalogEndpoint, StoredProviderCatalogKey,
    StoredProviderCatalogKeyMaintenanceSummary, StoredProviderCatalogKeyPage,
    StoredProviderCatalogKeyStats, StoredProviderCatalogProvider,
};
use aether_data_contracts::DataLayerError;
use async_trait::async_trait;
use serde_json::json;
use tokio::sync::Notify;

use super::{
    local_oauth_refresh_gateway_error, AgentIdentityAuthConfigFence, AppState,
    CodexRuntimeOAuthObservation, ProviderTransportSnapshotCacheKey,
    ProviderTransportSnapshotFlight, ProviderTransportSnapshotFlightResult,
    ProviderTransportSnapshotInflightRegistration, PROVIDER_TRANSPORT_SNAPSHOT_CACHE_STALE_TTL,
    PROVIDER_TRANSPORT_SNAPSHOT_CACHE_TTL,
};
use crate::data::GatewayDataState;

#[test]
fn oauth_refresh_diagnostic_preserves_failure_reason_and_redacts_credentials() {
    for (message, secret) in [
        (
            "connection refused refresh_token=refresh-secret",
            "refresh-secret",
        ),
        (
            "connection refused accessToken=access-secret",
            "access-secret",
        ),
        (
            "connection refused client_secret=client-secret",
            "client-secret",
        ),
        (
            "connection refused Authorization: Bearer bearer-secret",
            "bearer-secret",
        ),
        (
            "connection refused https://proxy-user:proxy-secret@proxy.example",
            "proxy-secret",
        ),
    ] {
        let error = crate::provider_transport::LocalOAuthRefreshError::TransportMessage {
            provider_type: "codex",
            message: message.to_string(),
        };
        let diagnostic = local_oauth_refresh_gateway_error(&error).into_message();
        assert!(diagnostic.contains("codex oauth refresh transport failed"));
        assert!(diagnostic.contains("connection refused"));
        assert!(!diagnostic.contains(secret), "diagnostic: {diagnostic}");
    }
}

#[test]
fn oauth_refresh_diagnostic_keeps_http_status_without_provider_body() {
    let error = crate::provider_transport::LocalOAuthRefreshError::HttpStatus {
        provider_type: "codex",
        status_code: 503,
        body_excerpt: "unstructured-provider-secret".to_string(),
    };
    let diagnostic = local_oauth_refresh_gateway_error(&error).into_message();

    assert!(diagnostic.contains("codex oauth refresh returned HTTP 503"));
    assert!(!diagnostic.contains("unstructured-provider-secret"));
}

#[tokio::test]
async fn oauth_refresh_diagnostic_is_hidden_from_client_response() {
    use axum::body::to_bytes;
    use axum::response::IntoResponse;

    let error = crate::provider_transport::LocalOAuthRefreshError::TransportMessage {
        provider_type: "codex",
        message: "connection refused".to_string(),
    };
    let response = local_oauth_refresh_gateway_error(&error).into_response();

    assert_eq!(
        response.status(),
        axum::http::StatusCode::INTERNAL_SERVER_ERROR
    );
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("error response body should read");
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&body).expect("error should be JSON"),
        json!({"error": {"message": "internal server error"}}),
    );
}

fn sample_provider() -> StoredProviderCatalogProvider {
    StoredProviderCatalogProvider::new(
        "provider-1".to_string(),
        "provider-1".to_string(),
        Some("https://provider.example".to_string()),
        "custom".to_string(),
    )
    .expect("provider should build")
    .with_transport_fields(true, false, false, None, None, None, None, None, None)
}

fn sample_endpoint() -> StoredProviderCatalogEndpoint {
    StoredProviderCatalogEndpoint::new(
        "endpoint-1".to_string(),
        "provider-1".to_string(),
        "openai:chat".to_string(),
        Some("openai".to_string()),
        Some("chat".to_string()),
        true,
    )
    .expect("endpoint should build")
    .with_transport_fields(
        "https://api.provider.example".to_string(),
        None,
        None,
        None,
        None,
        None,
        None,
        None,
    )
    .expect("endpoint transport should build")
}

fn sample_key_for_provider(provider_id: &str) -> StoredProviderCatalogKey {
    let encrypted_api_key = if provider_id == "provider-1" {
        static ENCRYPTED_API_KEY: OnceLock<String> = OnceLock::new();
        ENCRYPTED_API_KEY
            .get_or_init(|| {
                let credential_state = crate::tests::support::credential_fixture_state();
                credential_state
                    .seal_provider_catalog_key_api_key("provider-1", "key-1", "plain-upstream-key")
                    .expect("api key should encrypt")
            })
            .clone()
    } else {
        let credential_state = crate::tests::support::credential_fixture_state();
        credential_state
            .seal_provider_catalog_key_api_key(provider_id, "key-1", "plain-upstream-key")
            .expect("api key should encrypt")
    };
    StoredProviderCatalogKey::new(
        "key-1".to_string(),
        provider_id.to_string(),
        "default".to_string(),
        "api_key".to_string(),
        None,
        true,
    )
    .expect("key should build")
    .with_transport_fields(
        Some(json!(["openai:chat"])),
        encrypted_api_key,
        None,
        None,
        Some(json!({"openai:chat": 1})),
        None,
        None,
        None,
        None,
    )
    .expect("key transport should build")
}

fn sample_key() -> StoredProviderCatalogKey {
    sample_key_for_provider("provider-1")
}

fn codex_oauth_state(
    auth_config: &serde_json::Value,
    access_token: &str,
) -> (AppState, Arc<InMemoryProviderCatalogReadRepository>, String) {
    provider_oauth_state("codex", auth_config, access_token)
}

fn provider_oauth_state(
    provider_type: &str,
    auth_config: &serde_json::Value,
    access_token: &str,
) -> (AppState, Arc<InMemoryProviderCatalogReadRepository>, String) {
    let mut provider = sample_provider();
    provider.provider_type = provider_type.to_string();
    let encrypted_auth_config =
        encrypt_python_fernet_plaintext(DEVELOPMENT_ENCRYPTION_KEY, &auth_config.to_string())
            .expect("auth config should encrypt");
    let encrypted_api_key =
        encrypt_python_fernet_plaintext(DEVELOPMENT_ENCRYPTION_KEY, access_token)
            .expect("api key should encrypt");
    let key = StoredProviderCatalogKey::new(
        "key-1".to_string(),
        "provider-1".to_string(),
        format!("{provider_type} OAuth"),
        "oauth".to_string(),
        None,
        true,
    )
    .expect("key should build")
    .with_transport_fields(
        Some(json!(["openai:chat"])),
        encrypted_api_key,
        Some(encrypted_auth_config.clone()),
        None,
        Some(json!({"openai:chat": 1})),
        None,
        Some(4_102_444_800),
        None,
        None,
    )
    .expect("key transport should build");
    let repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![provider],
        vec![sample_endpoint()],
        vec![key],
    ));
    let state = AppState::new()
        .expect("state should build")
        .with_data_state_for_tests(
            GatewayDataState::with_provider_catalog_repository_for_tests(repository.clone())
                .with_encryption_key_for_tests(DEVELOPMENT_ENCRYPTION_KEY),
        );
    (state, repository, encrypted_auth_config)
}

fn vertex_service_account_state() -> (AppState, Arc<InMemoryProviderCatalogReadRepository>, String)
{
    let mut provider = sample_provider();
    provider.provider_type = "vertex_ai".to_string();
    let mut endpoint = sample_endpoint();
    endpoint.api_format = "gemini:generate_content".to_string();
    endpoint.api_family = Some("gemini".to_string());
    endpoint.base_url = "https://aiplatform.googleapis.com".to_string();
    let auth_config = json!({
        "client_email": "svc@example.iam.gserviceaccount.com",
        "private_key": "TEST-PRIVATE-KEY",
        "project_id": "demo-project"
    });
    let credential_state = crate::tests::support::credential_fixture_state();
    let encrypted_auth_config = credential_state
        .seal_provider_catalog_key_auth_config("provider-1", "key-1", &auth_config.to_string())
        .expect("Vertex auth config should encrypt");
    let encrypted_api_key = credential_state
        .seal_provider_catalog_key_api_key("provider-1", "key-1", "__placeholder__")
        .expect("Vertex placeholder should encrypt");
    let key = StoredProviderCatalogKey::new(
        "key-1".to_string(),
        "provider-1".to_string(),
        "Vertex service account".to_string(),
        "service_account".to_string(),
        None,
        true,
    )
    .expect("Vertex key should build")
    .with_transport_fields(
        Some(json!(["gemini:generate_content"])),
        encrypted_api_key,
        Some(encrypted_auth_config.clone()),
        None,
        Some(json!({"gemini:generate_content": 1})),
        None,
        None,
        None,
        None,
    )
    .expect("Vertex key transport should build");
    let repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![provider],
        vec![endpoint],
        vec![key],
    ));
    let state = AppState::new()
        .expect("state should build")
        .with_data_state_for_tests(
            GatewayDataState::with_provider_catalog_repository_for_tests(repository.clone())
                .with_encryption_key_for_tests(DEVELOPMENT_ENCRYPTION_KEY),
        );
    (state, repository, encrypted_auth_config)
}

async fn replace_key_credential_for_test(
    repository: &InMemoryProviderCatalogReadRepository,
    replacement: StoredProviderCatalogKey,
    provider_type: &str,
) {
    let current = repository
        .list_keys_by_ids(std::slice::from_ref(&replacement.id))
        .await
        .expect("current key should load before replacement")
        .into_iter()
        .next()
        .expect("current key should exist before replacement");
    let codex_rotation = provider_type.eq_ignore_ascii_case("codex").then(|| {
        json!({
            "credential_generation": "test-admin-replacement-generation"
        })
    });
    assert!(repository
        .compare_and_update_key_admin_state(&ProviderCatalogKeyAdminCasUpdate {
            expected_encrypted_auth_config: current.encrypted_auth_config.clone(),
            expected_credential: ProviderCatalogKeyOAuthCredentialFence {
                encrypted_api_key: current.encrypted_api_key.clone(),
                auth_type: current.auth_type.clone(),
                provider_id: current.provider_id.clone(),
                provider_type: provider_type.to_string(),
            },
            key: replacement,
            codex_rotation,
            reset_oauth_runtime: true,
        })
        .await
        .expect("credential replacement CAS should run"));
}

fn state_with_global_format_conversion(enabled: bool) -> AppState {
    let repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![sample_provider()],
        vec![sample_endpoint()],
        vec![sample_key()],
    ));
    let data_state = GatewayDataState::with_provider_transport_reader_for_tests(
        repository,
        DEVELOPMENT_ENCRYPTION_KEY,
    )
    .with_system_config_values_for_tests(vec![(
        "enable_format_conversion".to_string(),
        json!(enabled),
    )]);
    AppState::new()
        .expect("state should build")
        .with_data_state_for_tests(data_state)
}

struct BlockingProviderCatalogReadRepository {
    inner: Arc<InMemoryProviderCatalogReadRepository>,
    key_reads: AtomicUsize,
    block_on_key_read: usize,
    blocked_key_read_started: Notify,
    release_blocked_key_read: Notify,
}

impl BlockingProviderCatalogReadRepository {
    fn blocking_on_key_read(
        inner: Arc<InMemoryProviderCatalogReadRepository>,
        block_on_key_read: usize,
    ) -> Self {
        Self {
            inner,
            key_reads: AtomicUsize::new(0),
            block_on_key_read,
            blocked_key_read_started: Notify::new(),
            release_blocked_key_read: Notify::new(),
        }
    }
}

#[async_trait]
impl ProviderCatalogReadRepository for BlockingProviderCatalogReadRepository {
    async fn list_providers(
        &self,
        active_only: bool,
    ) -> Result<Vec<StoredProviderCatalogProvider>, DataLayerError> {
        self.inner.list_providers(active_only).await
    }

    async fn list_providers_by_ids(
        &self,
        provider_ids: &[String],
    ) -> Result<Vec<StoredProviderCatalogProvider>, DataLayerError> {
        self.inner.list_providers_by_ids(provider_ids).await
    }

    async fn list_endpoints_by_ids(
        &self,
        endpoint_ids: &[String],
    ) -> Result<Vec<StoredProviderCatalogEndpoint>, DataLayerError> {
        self.inner.list_endpoints_by_ids(endpoint_ids).await
    }

    async fn list_endpoints_by_provider_ids(
        &self,
        provider_ids: &[String],
    ) -> Result<Vec<StoredProviderCatalogEndpoint>, DataLayerError> {
        self.inner
            .list_endpoints_by_provider_ids(provider_ids)
            .await
    }

    async fn list_keys_by_ids(
        &self,
        key_ids: &[String],
    ) -> Result<Vec<StoredProviderCatalogKey>, DataLayerError> {
        let keys = self.inner.list_keys_by_ids(key_ids).await?;
        let read_number = self.key_reads.fetch_add(1, Ordering::AcqRel) + 1;
        if read_number == self.block_on_key_read {
            self.blocked_key_read_started.notify_one();
            self.release_blocked_key_read.notified().await;
        }
        Ok(keys)
    }

    async fn list_keys_by_provider_ids(
        &self,
        provider_ids: &[String],
    ) -> Result<Vec<StoredProviderCatalogKey>, DataLayerError> {
        self.inner.list_keys_by_provider_ids(provider_ids).await
    }

    async fn list_key_summaries_by_provider_ids(
        &self,
        provider_ids: &[String],
    ) -> Result<Vec<StoredProviderCatalogKey>, DataLayerError> {
        self.inner
            .list_key_summaries_by_provider_ids(provider_ids)
            .await
    }

    async fn list_key_maintenance_summaries_by_provider_ids(
        &self,
        provider_ids: &[String],
    ) -> Result<Vec<StoredProviderCatalogKeyMaintenanceSummary>, DataLayerError> {
        self.inner
            .list_key_maintenance_summaries_by_provider_ids(provider_ids)
            .await
    }

    async fn list_keys_page(
        &self,
        query: &ProviderCatalogKeyListQuery,
    ) -> Result<StoredProviderCatalogKeyPage, DataLayerError> {
        self.inner.list_keys_page(query).await
    }

    async fn list_key_stats_by_provider_ids(
        &self,
        provider_ids: &[String],
    ) -> Result<Vec<StoredProviderCatalogKeyStats>, DataLayerError> {
        self.inner
            .list_key_stats_by_provider_ids(provider_ids)
            .await
    }
}

#[tokio::test]
async fn global_format_conversion_overrides_snapshot_without_persisting_provider_value() {
    let state = state_with_global_format_conversion(false);

    let snapshot = state
        .read_provider_transport_snapshot("provider-1", "endpoint-1", "key-1")
        .await
        .expect("snapshot read should succeed")
        .expect("snapshot should exist");
    assert!(!snapshot.provider.enable_format_conversion);

    state
        .upsert_system_config_json_value("enable_format_conversion", &json!(true), None)
        .await
        .expect("global config update should succeed");
    let snapshot = state
        .read_provider_transport_snapshot("provider-1", "endpoint-1", "key-1")
        .await
        .expect("snapshot read should succeed")
        .expect("snapshot should exist");
    assert!(snapshot.provider.enable_format_conversion);

    state
        .upsert_system_config_json_value("enable_format_conversion", &json!(false), None)
        .await
        .expect("global config update should succeed");
    let snapshot = state
        .read_provider_transport_snapshot("provider-1", "endpoint-1", "key-1")
        .await
        .expect("snapshot read should succeed")
        .expect("snapshot should exist");
    assert!(!snapshot.provider.enable_format_conversion);
}

#[tokio::test]
async fn provider_transport_snapshot_inflight_entry_is_removed_after_read() {
    let state = state_with_global_format_conversion(false);

    let snapshot = state
        .read_provider_transport_snapshot_arc("provider-1", "endpoint-1", "key-1")
        .await
        .expect("snapshot read should succeed");

    assert!(snapshot.is_some());
    assert!(state.provider_transport_snapshot_inflight.is_empty());
}

#[tokio::test]
async fn finished_transport_flight_detects_generation_clear_before_return() {
    let state = state_with_global_format_conversion(false);
    let snapshot = state
        .read_provider_transport_snapshot_arc("provider-1", "endpoint-1", "key-1")
        .await
        .expect("snapshot read should succeed")
        .expect("snapshot should exist");
    state.clear_provider_transport_snapshot_cache();

    let cache_key = ProviderTransportSnapshotCacheKey::new("provider-1", "endpoint-1", "key-1")
        .expect("cache key should build");
    let generation = state
        .provider_transport_snapshot_cache_generation
        .load(Ordering::Acquire);
    let mut guard =
        match state.register_provider_transport_snapshot_inflight(&cache_key, generation) {
            ProviderTransportSnapshotInflightRegistration::Leader(guard) => guard,
            _ => panic!("empty current-generation key should register a leader"),
        };
    guard.finish(ProviderTransportSnapshotFlightResult::Published(snapshot));
    assert!(guard.generation_is_current(&state));

    state.clear_provider_transport_snapshot_cache();
    assert!(!guard.generation_is_current(&state));
}

#[tokio::test]
async fn transport_cache_clear_retries_inflight_load_before_publishing_snapshot() {
    let inner = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![sample_provider()],
        vec![sample_endpoint()],
        vec![sample_key()],
    ));
    let reader = Arc::new(BlockingProviderCatalogReadRepository::new(Arc::clone(
        &inner,
    )));
    let data_state = GatewayDataState::with_provider_transport_reader_for_tests(
        reader.clone(),
        DEVELOPMENT_ENCRYPTION_KEY,
    );
    let state = AppState::new()
        .expect("state should build")
        .with_data_state_for_tests(data_state);

    let read_state = state.clone();
    let read_task = tokio::spawn(async move {
        read_state
            .read_provider_transport_snapshot_arc("provider-1", "endpoint-1", "key-1")
            .await
            .expect("transport snapshot read should succeed")
            .expect("transport snapshot should exist")
    });
    tokio::time::timeout(
        Duration::from_secs(1),
        reader.blocked_key_read_started.notified(),
    )
    .await
    .expect("first transport load should reach the blocked key read");

    let mut inactive_key = sample_key();
    inactive_key.is_active = false;
    inner
        .update_key(&inactive_key)
        .await
        .expect("provider key should update while the old snapshot load is blocked");
    state.clear_provider_transport_snapshot_cache();
    reader.release_blocked_key_read.notify_one();

    let snapshot = tokio::time::timeout(Duration::from_secs(1), read_task)
        .await
        .expect("transport load should retry promptly after invalidation")
        .expect("transport load task should join");
    assert!(!snapshot.key.is_active);
    assert!(reader.key_reads.load(Ordering::Acquire) >= 2);

    let cached = state
        .read_provider_transport_snapshot_arc("provider-1", "endpoint-1", "key-1")
        .await
        .expect("cached transport read should succeed")
        .expect("cached transport should exist");
    assert!(!cached.key.is_active);
    assert!(Arc::ptr_eq(&snapshot, &cached));
}

fn blocking_transport_state(
    block_on_key_read: usize,
) -> (
    AppState,
    Arc<InMemoryProviderCatalogReadRepository>,
    Arc<BlockingProviderCatalogReadRepository>,
) {
    let inner = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![sample_provider()],
        vec![sample_endpoint()],
        vec![sample_key()],
    ));
    let reader = Arc::new(BlockingProviderCatalogReadRepository::blocking_on_key_read(
        Arc::clone(&inner),
        block_on_key_read,
    ));
    let data_state = GatewayDataState::with_provider_transport_reader_for_tests(
        reader.clone(),
        DEVELOPMENT_ENCRYPTION_KEY,
    );
    let state = AppState::new()
        .expect("state should build")
        .with_data_state_for_tests(data_state);
    (state, inner, reader)
}

fn age_transport_snapshot_cache(state: &AppState, age: Duration) {
    let cache_key = ProviderTransportSnapshotCacheKey::new("provider-1", "endpoint-1", "key-1")
        .expect("cache key should build");
    let mut cached = state
        .provider_transport_snapshot_cache
        .get_mut(&cache_key)
        .expect("snapshot should be cached before aging");
    cached.loaded_at = Instant::now() - age;
}

async fn wait_for_transport_refresh_to_finish(state: &AppState) {
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if state.provider_transport_snapshot_inflight.is_empty() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("background transport refresh should finish");
}

async fn wait_for_transport_flight_followers(
    state: &AppState,
    expected_strong_count: usize,
) -> Arc<ProviderTransportSnapshotFlight> {
    let cache_key = ProviderTransportSnapshotCacheKey::new("provider-1", "endpoint-1", "key-1")
        .expect("cache key should build");
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if let Some(flight) = state
                .provider_transport_snapshot_inflight
                .get(&cache_key)
                .filter(|flight| Arc::strong_count(flight.value()) >= expected_strong_count)
                .map(|flight| flight.clone())
            {
                return flight;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("all transport snapshot followers should register")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cold_transport_snapshot_broadcasts_to_twenty_thousand_followers() {
    const REQUESTS: usize = 20_000;
    let (state, _inner, reader) = blocking_transport_state(1);
    let mut reads = Vec::with_capacity(REQUESTS);
    for _ in 0..REQUESTS {
        let read_state = state.clone();
        reads.push(tokio::spawn(async move {
            read_state
                .read_provider_transport_snapshot_arc("provider-1", "endpoint-1", "key-1")
                .await
                .expect("transport snapshot read should succeed")
                .expect("transport snapshot should exist")
        }));
    }

    tokio::time::timeout(
        Duration::from_secs(2),
        reader.blocked_key_read_started.notified(),
    )
    .await
    .expect("leader should reach the database barrier");
    let _flight = wait_for_transport_flight_followers(&state, REQUESTS + 1).await;
    assert_eq!(reader.key_reads.load(Ordering::Acquire), 1);

    reader.release_blocked_key_read.notify_one();
    let snapshots = tokio::time::timeout(Duration::from_secs(5), async {
        let mut snapshots = Vec::with_capacity(REQUESTS);
        for read in reads {
            snapshots.push(read.await.expect("transport read task should join"));
        }
        snapshots
    })
    .await
    .expect("all followers should wake from one broadcast");
    assert_eq!(reader.key_reads.load(Ordering::Acquire), 1);
    let first = snapshots
        .first()
        .expect("at least one snapshot should exist");
    for snapshot in snapshots.iter().skip(1) {
        assert!(Arc::ptr_eq(first, snapshot));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn transport_cache_clear_wakes_old_followers_before_old_leader_finishes() {
    const REQUESTS: usize = 64;
    let (state, inner, reader) = blocking_transport_state(1);
    let mut reads = Vec::with_capacity(REQUESTS);
    for _ in 0..REQUESTS {
        let read_state = state.clone();
        reads.push(tokio::spawn(async move {
            read_state
                .read_provider_transport_snapshot_arc("provider-1", "endpoint-1", "key-1")
                .await
                .expect("transport snapshot read should succeed")
                .expect("transport snapshot should exist")
        }));
    }
    tokio::time::timeout(
        Duration::from_secs(1),
        reader.blocked_key_read_started.notified(),
    )
    .await
    .expect("old leader should reach the database barrier");
    let _old_flight = wait_for_transport_flight_followers(&state, REQUESTS + 1).await;

    let mut inactive_key = sample_key();
    inactive_key.is_active = false;
    inner
        .update_key(&inactive_key)
        .await
        .expect("provider key should update before invalidation");
    state.clear_provider_transport_snapshot_cache();

    // The old leader is still blocked. A follower must nevertheless claim
    // the new generation and perform the replacement read immediately.
    tokio::time::timeout(Duration::from_secs(1), async {
        while reader.key_reads.load(Ordering::Acquire) < 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("new generation follower should reload before old leader release");

    reader.release_blocked_key_read.notify_one();
    let snapshots = tokio::time::timeout(Duration::from_secs(3), async {
        let mut snapshots = Vec::with_capacity(REQUESTS);
        for read in reads {
            snapshots.push(read.await.expect("transport read task should join"));
        }
        snapshots
    })
    .await
    .expect("invalidated followers should complete");
    assert_eq!(reader.key_reads.load(Ordering::Acquire), 2);
    assert!(snapshots.iter().all(|snapshot| !snapshot.key.is_active));
}

#[tokio::test]
async fn cancelled_transport_snapshot_leader_releases_followers() {
    let (state, _inner, reader) = blocking_transport_state(1);
    let leader_state = state.clone();
    let leader = tokio::spawn(async move {
        leader_state
            .read_provider_transport_snapshot_arc("provider-1", "endpoint-1", "key-1")
            .await
    });
    tokio::time::timeout(
        Duration::from_secs(1),
        reader.blocked_key_read_started.notified(),
    )
    .await
    .expect("leader should reach the database barrier");

    let follower_state = state.clone();
    let follower = tokio::spawn(async move {
        follower_state
            .read_provider_transport_snapshot_arc("provider-1", "endpoint-1", "key-1")
            .await
            .expect("replacement transport read should succeed")
            .expect("replacement snapshot should exist")
    });
    let _old_flight = wait_for_transport_flight_followers(&state, 3).await;
    leader.abort();
    assert!(leader
        .await
        .expect_err("leader should be cancelled")
        .is_cancelled());

    let replacement = tokio::time::timeout(Duration::from_secs(2), follower)
        .await
        .expect("follower should be released after leader cancellation")
        .expect("follower task should join");
    assert_eq!(reader.key_reads.load(Ordering::Acquire), 2);
    assert!(replacement.key.is_active);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn missing_transport_snapshot_result_is_broadcast_to_all_followers() {
    const REQUESTS: usize = 64;
    let inner = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![sample_provider()],
        vec![sample_endpoint()],
        Vec::new(),
    ));
    let reader = Arc::new(BlockingProviderCatalogReadRepository::new(Arc::clone(
        &inner,
    )));
    let data_state = GatewayDataState::with_provider_transport_reader_for_tests(
        reader.clone(),
        DEVELOPMENT_ENCRYPTION_KEY,
    );
    let state = AppState::new()
        .expect("state should build")
        .with_data_state_for_tests(data_state);

    let mut reads = Vec::with_capacity(REQUESTS);
    for _ in 0..REQUESTS {
        let read_state = state.clone();
        reads.push(tokio::spawn(async move {
            read_state
                .read_provider_transport_snapshot_arc("provider-1", "endpoint-1", "key-1")
                .await
                .expect("missing transport read should not fail")
        }));
    }
    tokio::time::timeout(
        Duration::from_secs(1),
        reader.blocked_key_read_started.notified(),
    )
    .await
    .expect("missing-result leader should reach the database barrier");
    let _flight = wait_for_transport_flight_followers(&state, REQUESTS + 1).await;

    reader.release_blocked_key_read.notify_one();
    tokio::time::timeout(Duration::from_secs(2), async {
        for read in reads {
            assert!(read.await.expect("missing read task should join").is_none());
        }
    })
    .await
    .expect("missing result should wake all followers");
    assert_eq!(reader.key_reads.load(Ordering::Acquire), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn transport_snapshot_error_is_broadcast_to_all_followers() {
    const REQUESTS: usize = 64;
    let mismatched_key = sample_key_for_provider("provider-other");
    let inner = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![sample_provider()],
        vec![sample_endpoint()],
        vec![mismatched_key],
    ));
    let reader = Arc::new(BlockingProviderCatalogReadRepository::new(Arc::clone(
        &inner,
    )));
    let data_state = GatewayDataState::with_provider_transport_reader_for_tests(
        reader.clone(),
        DEVELOPMENT_ENCRYPTION_KEY,
    );
    let state = AppState::new()
        .expect("state should build")
        .with_data_state_for_tests(data_state);

    let mut reads = Vec::with_capacity(REQUESTS);
    for _ in 0..REQUESTS {
        let read_state = state.clone();
        reads.push(tokio::spawn(async move {
            read_state
                .read_provider_transport_snapshot_arc("provider-1", "endpoint-1", "key-1")
                .await
                .expect_err("provider mismatch should fail")
                .into_message()
        }));
    }
    tokio::time::timeout(
        Duration::from_secs(1),
        reader.blocked_key_read_started.notified(),
    )
    .await
    .expect("error-result leader should reach the database barrier");
    let _flight = wait_for_transport_flight_followers(&state, REQUESTS + 1).await;

    reader.release_blocked_key_read.notify_one();
    let messages = tokio::time::timeout(Duration::from_secs(2), async {
        let mut messages = Vec::with_capacity(REQUESTS);
        for read in reads {
            messages.push(read.await.expect("error read task should join"));
        }
        messages
    })
    .await
    .expect("error result should wake all followers");
    assert_eq!(reader.key_reads.load(Ordering::Acquire), 1);
    assert!(messages
        .iter()
        .all(|message| message.contains("provider_api_keys.provider_id mismatch")));
}

#[tokio::test]
async fn stale_transport_snapshot_returns_immediately_and_refreshes_once() {
    let (state, inner, reader) = blocking_transport_state(2);
    let original = state
        .read_provider_transport_snapshot_arc("provider-1", "endpoint-1", "key-1")
        .await
        .expect("initial transport read should succeed")
        .expect("initial transport snapshot should exist");
    age_transport_snapshot_cache(
        &state,
        PROVIDER_TRANSPORT_SNAPSHOT_CACHE_TTL + Duration::from_millis(10),
    );

    let mut inactive_key = sample_key();
    inactive_key.is_active = false;
    inner
        .update_key(&inactive_key)
        .await
        .expect("provider key should update before stale refresh");

    let stale = tokio::time::timeout(
        Duration::from_millis(250),
        state.read_provider_transport_snapshot_arc("provider-1", "endpoint-1", "key-1"),
    )
    .await
    .expect("stale cache hit must not wait for the database refresh")
    .expect("stale transport read should succeed")
    .expect("stale transport snapshot should exist");
    assert!(Arc::ptr_eq(&original, &stale));
    assert!(stale.key.is_active);

    tokio::time::timeout(
        Duration::from_secs(1),
        reader.blocked_key_read_started.notified(),
    )
    .await
    .expect("stale hit should start one background refresh");
    assert_eq!(reader.key_reads.load(Ordering::Acquire), 2);

    for _ in 0..32 {
        let observed = state
            .read_provider_transport_snapshot_arc("provider-1", "endpoint-1", "key-1")
            .await
            .expect("concurrent stale read should succeed")
            .expect("concurrent stale snapshot should exist");
        assert!(Arc::ptr_eq(&original, &observed));
    }
    assert_eq!(reader.key_reads.load(Ordering::Acquire), 2);

    reader.release_blocked_key_read.notify_one();
    wait_for_transport_refresh_to_finish(&state).await;
    let refreshed = state
        .read_provider_transport_snapshot_arc("provider-1", "endpoint-1", "key-1")
        .await
        .expect("refreshed transport read should succeed")
        .expect("refreshed transport snapshot should exist");
    assert!(!refreshed.key.is_active);
    assert_eq!(reader.key_reads.load(Ordering::Acquire), 2);
}

#[tokio::test]
async fn stale_transport_refresh_cannot_publish_after_generation_clear() {
    let (state, inner, reader) = blocking_transport_state(2);
    let _initial = state
        .read_provider_transport_snapshot_arc("provider-1", "endpoint-1", "key-1")
        .await
        .expect("initial transport read should succeed")
        .expect("initial transport snapshot should exist");
    age_transport_snapshot_cache(
        &state,
        PROVIDER_TRANSPORT_SNAPSHOT_CACHE_TTL + Duration::from_millis(10),
    );

    let mut inactive_key = sample_key();
    inactive_key.is_active = false;
    inner
        .update_key(&inactive_key)
        .await
        .expect("provider key should update before stale refresh");
    let stale = state
        .read_provider_transport_snapshot_arc("provider-1", "endpoint-1", "key-1")
        .await
        .expect("stale transport read should succeed")
        .expect("stale transport snapshot should exist");
    assert!(stale.key.is_active);
    tokio::time::timeout(
        Duration::from_secs(1),
        reader.blocked_key_read_started.notified(),
    )
    .await
    .expect("background refresh should reach the barrier");
    let cache_key = ProviderTransportSnapshotCacheKey::new("provider-1", "endpoint-1", "key-1")
        .expect("cache key should build");
    let old_inflight = state
        .provider_transport_snapshot_inflight
        .get(&cache_key)
        .expect("background refresh should own the inflight entry")
        .clone();

    inner
        .update_key(&sample_key())
        .await
        .expect("provider key should update for the new generation");
    state.clear_provider_transport_snapshot_cache();
    let current = state
        .read_provider_transport_snapshot_arc("provider-1", "endpoint-1", "key-1")
        .await
        .expect("new generation transport read should succeed")
        .expect("new generation snapshot should exist");
    assert!(current.key.is_active);

    reader.release_blocked_key_read.notify_one();
    tokio::time::timeout(Duration::from_secs(1), async {
        while Arc::strong_count(&old_inflight) > 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("old background refresh should finish after invalidation");
    let cached = state
        .read_provider_transport_snapshot_arc("provider-1", "endpoint-1", "key-1")
        .await
        .expect("cached new generation read should succeed")
        .expect("cached new generation snapshot should exist");
    assert!(cached.key.is_active);
    assert!(Arc::ptr_eq(&current, &cached));
}

#[tokio::test]
async fn hard_expired_transport_snapshot_uses_one_synchronous_reload() {
    let (state, inner, reader) = blocking_transport_state(2);
    let _initial = state
        .read_provider_transport_snapshot_arc("provider-1", "endpoint-1", "key-1")
        .await
        .expect("initial transport read should succeed")
        .expect("initial transport snapshot should exist");
    age_transport_snapshot_cache(
        &state,
        PROVIDER_TRANSPORT_SNAPSHOT_CACHE_STALE_TTL + Duration::from_secs(1),
    );
    let mut inactive_key = sample_key();
    inactive_key.is_active = false;
    inner
        .update_key(&inactive_key)
        .await
        .expect("provider key should update before hard-expiry read");

    let mut reads = Vec::new();
    for _ in 0..32 {
        let read_state = state.clone();
        reads.push(tokio::spawn(async move {
            read_state
                .read_provider_transport_snapshot_arc("provider-1", "endpoint-1", "key-1")
                .await
                .expect("hard-expiry transport read should succeed")
                .expect("hard-expiry snapshot should exist")
        }));
    }
    tokio::time::timeout(
        Duration::from_secs(1),
        reader.blocked_key_read_started.notified(),
    )
    .await
    .expect("one synchronous reload should reach the barrier");
    tokio::task::yield_now().await;
    assert_eq!(reader.key_reads.load(Ordering::Acquire), 2);
    assert!(reads.iter().any(|read| !read.is_finished()));

    reader.release_blocked_key_read.notify_one();
    let snapshots = tokio::time::timeout(Duration::from_secs(1), async {
        let mut snapshots = Vec::with_capacity(reads.len());
        for read in reads {
            snapshots.push(read.await.expect("transport read task should join"));
        }
        snapshots
    })
    .await
    .expect("synchronous reload waiters should complete after one reload");
    assert_eq!(reader.key_reads.load(Ordering::Acquire), 2);
    let first = snapshots
        .first()
        .expect("at least one snapshot should exist");
    assert!(!first.key.is_active);
    for snapshot in snapshots.iter().skip(1) {
        assert!(Arc::ptr_eq(first, snapshot));
    }
}

#[test]
fn normalizes_local_openai_refresh_token_expired_response() {
    let body = r#"{"error":{"message":"Could not validate your refresh token. Please try signing in again.","type":"invalid_request_error","param":null,"code":"refresh_token_expired"}}"#;

    assert_eq!(
        super::normalize_local_oauth_refresh_error_message(Some(401), Some(body)),
        "refresh_token 无效、已过期或已撤销，请重新登录授权"
    );
}

#[test]
fn local_oauth_diagnostics_do_not_reflect_unknown_response_credentials() {
    let body = r#"{"error":{"message":"authorization=Bearer upstream-secret https://user:pass@example.test?q=secret","type":"invalid_request_error","code":"unexpected"}}"#;

    let normalized = super::normalize_local_oauth_refresh_error_message(Some(502), Some(body));
    assert_eq!(normalized, "HTTP 502");
    assert_eq!(super::local_oauth_log_excerpt(body), "[redacted]");
    for secret in ["upstream-secret", "user:pass", "q=secret"] {
        assert!(!normalized.contains(secret), "leaked {secret}");
    }
}

#[test]
fn local_refresh_failure_is_appended_to_access_token_expired_marker() {
    assert_eq!(
        super::merge_local_oauth_refresh_failure_reason(
            Some("[OAUTH_EXPIRED] access token invalid"),
            "[REFRESH_FAILED] Token 续期失败 (401): refresh_token 无效",
        ),
        Some(
            "[OAUTH_EXPIRED] access token invalid\n[REFRESH_FAILED] Token 续期失败 (401): refresh_token 无效"
                .to_string()
        ),
    );
}

#[test]
fn vertex_service_account_refresh_entry_stays_memory_only() {
    let transport = crate::provider_transport::GatewayProviderTransportSnapshot {
        provider: crate::provider_transport::snapshot::GatewayProviderTransportProvider {
            id: "provider-1".to_string(),
            name: "Vertex".to_string(),
            provider_type: "vertex_ai".to_string(),
            website: None,
            is_active: true,
            keep_priority_on_conversion: false,
            enable_format_conversion: false,
            concurrent_limit: None,
            max_retries: None,
            proxy: None,
            request_timeout_secs: None,
            stream_first_byte_timeout_secs: None,
            config: None,
        },
        endpoint: crate::provider_transport::snapshot::GatewayProviderTransportEndpoint {
            id: "endpoint-1".to_string(),
            provider_id: "provider-1".to_string(),
            api_format: "gemini:generate_content".to_string(),
            api_family: Some("gemini".to_string()),
            endpoint_kind: Some("chat".to_string()),
            is_active: true,
            base_url: "https://aiplatform.googleapis.com".to_string(),
            header_rules: None,
            body_rules: None,
            max_retries: None,
            custom_path: None,
            config: None,
            format_acceptance_config: None,
            proxy: None,
        },
        key: crate::provider_transport::snapshot::GatewayProviderTransportKey {
            id: "key-1".to_string(),
            provider_id: "provider-1".to_string(),
            name: "Gemini".to_string(),
            auth_type: "service_account".to_string(),
            is_active: true,
            api_formats: Some(vec!["gemini:generate_content".to_string()]),
            auth_type_by_format: None,
            allow_auth_channel_mismatch_formats: None,
            allowed_models: None,
            capabilities: None,
            rate_multipliers: None,
            global_priority_by_format: None,
            expires_at_unix_secs: None,
            proxy: None,
            fingerprint: None,
            upstream_metadata: None,
            decrypted_api_key: "__placeholder__".to_string(),
            decrypted_auth_config: Some("{\"project_id\":\"demo\"}".to_string()),
        },
    };
    let entry = crate::provider_transport::CachedOAuthEntry {
        provider_type: "vertex_ai".to_string(),
        auth_header_name: "authorization".to_string(),
        auth_header_value: "Bearer access-token".to_string(),
        expires_at_unix_secs: Some(4_102_444_800),
        metadata: None,
        source_fingerprint: None,
    };

    assert!(super::local_oauth_refresh_entry_should_stay_memory_only(
        &transport, &entry
    ));
}

#[tokio::test]
async fn memory_only_vertex_refresh_rejects_replaced_credential_generation() {
    let (state, repository, initial_encrypted_auth_config) = vertex_service_account_state();
    let transport = state
        .read_provider_transport_snapshot("provider-1", "endpoint-1", "key-1")
        .await
        .expect("Vertex transport should load")
        .expect("Vertex transport should exist");
    let expected_credential_fence = state
        .capture_provider_transport_credential_fence(&transport)
        .await
        .expect("Vertex fence should load")
        .expect("Vertex fence should match");
    let entry = crate::provider_transport::CachedOAuthEntry {
        provider_type: "vertex_ai".to_string(),
        auth_header_name: "authorization".to_string(),
        auth_header_value: "Bearer memory-only-token".to_string(),
        expires_at_unix_secs: Some(4_102_444_800),
        metadata: None,
        source_fingerprint: Some("service-account-generation".to_string()),
    };

    state
        .persist_local_oauth_refresh_entry(&transport, &entry, Some(&expected_credential_fence))
        .await
        .expect("unchanged Vertex credential should accept memory-only token");
    let unchanged = repository
        .list_keys_by_ids(&["key-1".to_string()])
        .await
        .expect("Vertex key should load")
        .pop()
        .expect("Vertex key should exist");
    assert_eq!(
        unchanged.encrypted_auth_config.as_deref(),
        Some(initial_encrypted_auth_config.as_str())
    );

    let mut replacement = unchanged;
    replacement.encrypted_api_key = Some(
        encrypt_python_fernet_plaintext(DEVELOPMENT_ENCRYPTION_KEY, "admin-replacement")
            .expect("replacement credential should encrypt"),
    );
    replace_key_credential_for_test(repository.as_ref(), replacement, "vertex_ai").await;

    assert!(state
        .persist_local_oauth_refresh_entry(&transport, &entry, Some(&expected_credential_fence),)
        .await
        .is_err());
}

#[test]
fn failed_refresh_persistence_discards_provisional_auth_and_cache_entry() {
    let mut resolution = Some(crate::provider_transport::LocalOAuthResolution {
        auth: Some(
            crate::provider_transport::LocalResolvedOAuthRequestAuth::Header {
                name: "authorization".to_string(),
                value: "AgentAssertion provisional".to_string(),
            },
        ),
        refreshed_entry: Some(crate::provider_transport::CachedOAuthEntry {
            provider_type: "codex_agent_identity".to_string(),
            auth_header_name: "authorization".to_string(),
            auth_header_value: "AgentAssertion provisional".to_string(),
            expires_at_unix_secs: None,
            metadata: None,
            source_fingerprint: Some("credential-generation".to_string()),
        }),
        refresh_in_flight: false,
        reused_refresh: false,
        distributed_lease: None,
        local_refresh_guard: None,
    });

    super::discard_failed_local_oauth_refresh_resolution(&mut resolution);

    let resolution = resolution.expect("resolution should remain allocated for lease release");
    assert!(resolution.auth.is_none());
    assert!(resolution.refreshed_entry.is_none());
}

#[test]
fn remote_refresh_wait_outlives_upstream_http_timeout() {
    assert!(
        super::REMOTE_OAUTH_REFRESH_WAIT_TIMEOUT
            > Duration::from_millis(super::LOCAL_OAUTH_HTTP_TIMEOUT_MS)
    );
}

#[tokio::test]
async fn antigravity_refresh_entry_persists_tokens_and_expiry() {
    let initial_config = json!({
        "provider_type": "antigravity",
        "refreshToken": "legacy-refresh-token",
        "expires_at": 1,
    });
    let (state, repository, _) =
        provider_oauth_state("antigravity", &initial_config, "stale-access-token");
    let transport = state
        .read_provider_transport_snapshot("provider-1", "endpoint-1", "key-1")
        .await
        .expect("transport should load")
        .expect("transport should exist");
    let expected_credential_fence = state
        .capture_provider_transport_credential_fence(&transport)
        .await
        .expect("credential fence should load")
        .expect("credential fence should match");
    let expires_at = 4_102_555_900;
    let refreshed_entry = crate::provider_transport::CachedOAuthEntry {
        provider_type: "antigravity".to_string(),
        auth_header_name: "authorization".to_string(),
        auth_header_value: "Bearer fresh-access-token".to_string(),
        expires_at_unix_secs: Some(expires_at),
        metadata: Some(json!({
            "provider_type": "antigravity",
            "refresh_token": "legacy-refresh-token",
            "expires_at": expires_at,
        })),
        source_fingerprint: None,
    };

    state
        .persist_local_oauth_refresh_entry(
            &transport,
            &refreshed_entry,
            Some(&expected_credential_fence),
        )
        .await
        .expect("Antigravity refresh should persist");

    let stored = repository
        .list_keys_by_ids(&["key-1".to_string()])
        .await
        .expect("key should reload")
        .pop()
        .expect("key should remain");
    let access_token = state
        .decrypt_provider_catalog_key_api_key(&stored)
        .expect("access token should decrypt")
        .expect("access token should persist");
    let auth_config = state
        .decrypt_provider_catalog_key_auth_config(&stored)
        .expect("auth config should decrypt")
        .expect("auth config should persist");
    let auth_config: serde_json::Value =
        serde_json::from_str(&auth_config).expect("auth config should parse");

    assert_eq!(access_token, "fresh-access-token");
    assert_eq!(auth_config["refresh_token"], json!("legacy-refresh-token"));
    assert_eq!(stored.expires_at_unix_secs, Some(expires_at));
}

#[tokio::test]
async fn agent_auth_config_fence_rejects_metadata_only_rewrite() {
    let initial_config = json!({
        "provider_type": "codex",
        "auth_mode": "agentIdentity",
        "agent_runtime_id": "runtime-1",
        "agent_private_key": "MC4CAQAwBQYDK2VwBCIEIAcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcH",
        "task_id": "task-1",
        "email": "before@example.com"
    });
    let (state, repository, _) = codex_oauth_state(&initial_config, "__placeholder__");
    let transport = state
        .read_provider_transport_snapshot("provider-1", "endpoint-1", "key-1")
        .await
        .expect("transport should load")
        .expect("transport should exist");

    let mut replaced = repository
        .list_keys_by_ids(&["key-1".to_string()])
        .await
        .expect("key should load")
        .pop()
        .expect("key should exist");
    let mut replacement_config = initial_config;
    replacement_config["email"] = json!("after@example.com");
    replaced.encrypted_auth_config = Some(
        encrypt_python_fernet_plaintext(
            DEVELOPMENT_ENCRYPTION_KEY,
            &replacement_config.to_string(),
        )
        .expect("replacement config should encrypt"),
    );
    replace_key_credential_for_test(repository.as_ref(), replaced, "codex").await;

    assert!(matches!(
        state
            .capture_agent_identity_auth_config_fence(&transport)
            .await
            .expect("fence should resolve"),
        AgentIdentityAuthConfigFence::StaleGeneration
    ));
}

#[tokio::test]
async fn stale_bearer_refresh_cannot_overwrite_agent_replacement() {
    let initial_config = json!({
        "provider_type": "codex",
        "refresh_token": "refresh-old",
        "email": "before@example.com",
        "expires_at": 4102444800_u64
    });
    let (state, repository, _expected_auth_config) =
        codex_oauth_state(&initial_config, "access-old");
    let transport = state
        .read_provider_transport_snapshot("provider-1", "endpoint-1", "key-1")
        .await
        .expect("transport should load")
        .expect("transport should exist");
    let expected_credential_fence = state
        .capture_provider_transport_credential_fence(&transport)
        .await
        .expect("credential fence should load")
        .expect("credential fence should match the initial transport");

    let replacement_config = json!({
        "provider_type": "codex",
        "auth_mode": "agentIdentity",
        "agent_runtime_id": "runtime-new",
        "agent_private_key": "MC4CAQAwBQYDK2VwBCIEIAcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcH",
        "task_id": "task-new"
    });
    let replacement_auth_config = encrypt_python_fernet_plaintext(
        DEVELOPMENT_ENCRYPTION_KEY,
        &replacement_config.to_string(),
    )
    .expect("replacement config should encrypt");
    let replacement_api_key =
        encrypt_python_fernet_plaintext(DEVELOPMENT_ENCRYPTION_KEY, "__placeholder__")
            .expect("replacement api key should encrypt");
    let mut replaced = repository
        .list_keys_by_ids(&["key-1".to_string()])
        .await
        .expect("key should load")
        .pop()
        .expect("key should exist");
    replaced.encrypted_auth_config = Some(replacement_auth_config.clone());
    replaced.encrypted_api_key = Some(replacement_api_key.clone());
    replaced.expires_at_unix_secs = None;
    replace_key_credential_for_test(repository.as_ref(), replaced, "codex").await;

    let refreshed_entry = crate::provider_transport::CachedOAuthEntry {
        provider_type: "codex".to_string(),
        auth_header_name: "authorization".to_string(),
        auth_header_value: "Bearer access-refreshed-old".to_string(),
        expires_at_unix_secs: Some(4_102_555_900),
        metadata: Some(json!({
            "provider_type": "codex",
            "refresh_token": "refresh-rotated-old",
            "email": "before@example.com",
            "expires_at": 4102555900_u64
        })),
        source_fingerprint: None,
    };
    assert!(state
        .persist_local_oauth_refresh_entry(
            &transport,
            &refreshed_entry,
            Some(&expected_credential_fence),
        )
        .await
        .is_err());

    let stored = repository
        .list_keys_by_ids(&["key-1".to_string()])
        .await
        .expect("key should reload")
        .pop()
        .expect("replacement should remain");
    assert_eq!(
        stored.encrypted_auth_config.as_deref(),
        Some(replacement_auth_config.as_str())
    );
    assert_eq!(
        stored.encrypted_api_key.as_deref(),
        Some(replacement_api_key.as_str())
    );
    assert_eq!(stored.expires_at_unix_secs, None);
}

#[tokio::test]
async fn stale_refresh_failure_does_not_mark_access_token_only_replacement() {
    let initial_config = json!({
        "provider_type": "codex",
        "refresh_token": "refresh-stable",
        "expires_at": 4102444800_u64
    });
    let (state, repository, _) = codex_oauth_state(&initial_config, "access-old");
    let stale_transport = state
        .read_provider_transport_snapshot("provider-1", "endpoint-1", "key-1")
        .await
        .expect("transport should load")
        .expect("transport should exist");

    let replacement_api_key =
        encrypt_python_fernet_plaintext(DEVELOPMENT_ENCRYPTION_KEY, "access-admin")
            .expect("replacement api key should encrypt");
    let mut replaced = repository
        .list_keys_by_ids(&["key-1".to_string()])
        .await
        .expect("key should load")
        .pop()
        .expect("key should exist");
    replaced.encrypted_api_key = Some(replacement_api_key.clone());
    replace_key_credential_for_test(repository.as_ref(), replaced, "codex").await;

    assert!(!state
        .persist_local_oauth_refresh_failure_state(
            &stale_transport,
            401,
            r#"{"error":"invalid_token"}"#,
            true,
        )
        .await
        .expect("stale failure should be ignored"));

    let stored = repository
        .list_keys_by_ids(&["key-1".to_string()])
        .await
        .expect("key should reload")
        .pop()
        .expect("replacement should remain");
    assert_eq!(
        stored.encrypted_api_key.as_deref(),
        Some(replacement_api_key.as_str())
    );
    assert!(stored.oauth_invalid_at_unix_secs.is_none());
    assert!(stored.oauth_invalid_reason.is_none());
}

#[tokio::test]
async fn stale_request_invalidation_does_not_mark_access_token_only_replacement() {
    let initial_config = json!({
        "provider_type": "codex",
        "refresh_token": "refresh-stable",
        "expires_at": 4102444800_u64
    });
    let (state, repository, _) = codex_oauth_state(&initial_config, "access-old");
    let stale_transport = state
        .read_provider_transport_snapshot("provider-1", "endpoint-1", "key-1")
        .await
        .expect("transport should load")
        .expect("transport should exist");

    let replacement_api_key =
        encrypt_python_fernet_plaintext(DEVELOPMENT_ENCRYPTION_KEY, "access-admin")
            .expect("replacement api key should encrypt");
    let mut replaced = repository
        .list_keys_by_ids(&["key-1".to_string()])
        .await
        .expect("key should load")
        .pop()
        .expect("key should exist");
    replaced.encrypted_api_key = Some(replacement_api_key.clone());
    replace_key_credential_for_test(repository.as_ref(), replaced, "codex").await;

    assert!(!state
        .mark_provider_transport_oauth_invalid_fenced(
            &stale_transport,
            "[OAUTH_EXPIRED] stale request",
            None,
            None,
            None,
        )
        .await
        .expect("stale invalidation should be ignored"));

    let stored = repository
        .list_keys_by_ids(&["key-1".to_string()])
        .await
        .expect("key should reload")
        .pop()
        .expect("replacement should remain");
    assert_eq!(
        stored.encrypted_api_key.as_deref(),
        Some(replacement_api_key.as_str())
    );
    assert!(stored.oauth_invalid_at_unix_secs.is_none());
    assert!(stored.oauth_invalid_reason.is_none());
}

#[tokio::test]
async fn newer_runtime_oauth_success_clears_older_invalid_and_advances_watermark() {
    let initial_config = json!({
        "provider_type": "codex",
        "refresh_token": "refresh-stable",
        "expires_at": 4102444800_u64
    });
    let (state, repository, _) = codex_oauth_state(&initial_config, "access-current");
    let transport = state
        .read_provider_transport_snapshot("provider-1", "endpoint-1", "key-1")
        .await
        .expect("transport should load")
        .expect("transport should exist");

    assert!(state
        .mark_provider_transport_oauth_invalid_fenced(
            &transport,
            "[OAUTH_EXPIRED] stale request",
            Some(1_000),
            Some("01900000-0000-7000-8000-000000000010"),
            None,
        )
        .await
        .expect("older invalidation should persist"));
    assert!(state
        .mark_provider_transport_oauth_success_fenced(
            &transport,
            Some(1_001),
            Some("01900000-0000-7000-8000-000000000011"),
            None,
        )
        .await
        .expect("newer success should persist"));

    let stored = repository
        .list_keys_by_ids(&["key-1".to_string()])
        .await
        .expect("key should reload")
        .pop()
        .expect("key should remain");
    assert!(stored.oauth_invalid_at_unix_secs.is_none());
    assert!(stored.oauth_invalid_reason.is_none());
    let codex = stored
        .upstream_metadata
        .as_ref()
        .and_then(|metadata| metadata.get("codex"))
        .expect("Codex metadata should exist");
    assert_eq!(
        codex["oauth_state_request_started_at_unix_ms"],
        json!(1_001_u64)
    );
    assert_eq!(
        codex["oauth_state_request_id"],
        json!("01900000-0000-7000-8000-000000000011")
    );
    assert_eq!(
        stored
            .status_snapshot
            .as_ref()
            .and_then(|snapshot| snapshot.get("oauth"))
            .and_then(|oauth| oauth.get("requires_reauth")),
        Some(&json!(false))
    );
}

#[tokio::test]
async fn runtime_oauth_success_allows_equal_quota_watermark() {
    let initial_config = json!({
        "provider_type": "codex",
        "refresh_token": "refresh-stable",
        "expires_at": 4102444800_u64
    });
    let (state, repository, _) = codex_oauth_state(&initial_config, "access-current");
    repository
        .update_key_oauth_runtime_state(
            "key-1",
            Some(1),
            Some("[OAUTH_EXPIRED] response raced with quota persistence"),
            Some(1),
        )
        .await
        .expect("invalid state should persist");
    repository
        .upsert_key_upstream_metadata_namespace(
            "key-1",
            "codex",
            &json!({
                "account_quota_request_started_at_unix_ms": 1_000_u64,
                "account_quota_request_id": "01900000-0000-7000-8000-000000000010"
            }),
            Some(1),
        )
        .await
        .expect("quota watermark should persist");
    let transport = state
        .read_provider_transport_snapshot("provider-1", "endpoint-1", "key-1")
        .await
        .expect("transport should load")
        .expect("transport should exist");

    assert!(state
        .mark_provider_transport_oauth_success_fenced(
            &transport,
            Some(1_000),
            Some("01900000-0000-7000-8000-000000000010"),
            None,
        )
        .await
        .expect("the same response should be idempotently accepted"));

    let stored = repository
        .list_keys_by_ids(&["key-1".to_string()])
        .await
        .expect("key should reload")
        .pop()
        .expect("key should remain");
    assert!(stored.oauth_invalid_at_unix_secs.is_none());
    assert!(stored.oauth_invalid_reason.is_none());
    let codex = stored
        .upstream_metadata
        .as_ref()
        .and_then(|metadata| metadata.get("codex"))
        .expect("Codex metadata should exist");
    assert_eq!(
        codex["oauth_state_request_id"],
        json!("01900000-0000-7000-8000-000000000010")
    );
    assert_eq!(
        codex["account_quota_request_id"],
        json!("01900000-0000-7000-8000-000000000010")
    );
}

#[tokio::test]
async fn older_runtime_oauth_success_cannot_clear_newer_invalid() {
    let initial_config = json!({
        "provider_type": "codex",
        "refresh_token": "refresh-stable",
        "expires_at": 4102444800_u64
    });
    let (state, repository, _) = codex_oauth_state(&initial_config, "access-current");
    let transport = state
        .read_provider_transport_snapshot("provider-1", "endpoint-1", "key-1")
        .await
        .expect("transport should load")
        .expect("transport should exist");

    assert!(state
        .mark_provider_transport_oauth_invalid_fenced(
            &transport,
            "[OAUTH_EXPIRED] newer request",
            Some(1_001),
            Some("01900000-0000-7000-8000-000000000011"),
            None,
        )
        .await
        .expect("newer invalidation should persist"));
    assert!(!state
        .mark_provider_transport_oauth_success_fenced(
            &transport,
            Some(1_000),
            Some("01900000-0000-7000-8000-000000000010"),
            None,
        )
        .await
        .expect("older success should be ignored"));

    let stored = repository
        .list_keys_by_ids(&["key-1".to_string()])
        .await
        .expect("key should reload")
        .pop()
        .expect("key should remain");
    assert_eq!(
        stored.oauth_invalid_reason.as_deref(),
        Some("[OAUTH_EXPIRED] newer request")
    );
}

#[tokio::test]
async fn runtime_oauth_success_uses_request_id_for_same_millisecond_ordering() {
    let initial_config = json!({
        "provider_type": "codex",
        "refresh_token": "refresh-stable",
        "expires_at": 4102444800_u64
    });
    let (state, repository, _) = codex_oauth_state(&initial_config, "access-current");
    let transport = state
        .read_provider_transport_snapshot("provider-1", "endpoint-1", "key-1")
        .await
        .expect("transport should load")
        .expect("transport should exist");

    assert!(state
        .mark_provider_transport_oauth_invalid_fenced(
            &transport,
            "[OAUTH_EXPIRED] same millisecond request",
            Some(1_000),
            Some("01900000-0000-7000-8000-000000000010"),
            None,
        )
        .await
        .expect("invalidation should persist"));
    assert!(!state
        .mark_provider_transport_oauth_success_fenced(
            &transport,
            Some(1_000),
            Some("01900000-0000-7000-8000-000000000009"),
            None,
        )
        .await
        .expect("lower request id should be ignored"));
    assert!(state
        .mark_provider_transport_oauth_success_fenced(
            &transport,
            Some(1_000),
            Some("01900000-0000-7000-8000-000000000011"),
            None,
        )
        .await
        .expect("higher request id should persist"));

    let stored = repository
        .list_keys_by_ids(&["key-1".to_string()])
        .await
        .expect("key should reload")
        .pop()
        .expect("key should remain");
    assert!(stored.oauth_invalid_reason.is_none());
}

#[tokio::test]
async fn runtime_oauth_success_preserves_account_block() {
    let initial_config = json!({
        "provider_type": "codex",
        "refresh_token": "refresh-stable",
        "expires_at": 4102444800_u64
    });
    let (state, repository, _) = codex_oauth_state(&initial_config, "access-current");
    repository
        .update_key_oauth_runtime_state(
            "key-1",
            Some(1),
            Some("[ACCOUNT_BLOCK] account deactivated"),
            Some(1),
        )
        .await
        .expect("account block should persist");
    let transport = state
        .read_provider_transport_snapshot("provider-1", "endpoint-1", "key-1")
        .await
        .expect("transport should load")
        .expect("transport should exist");

    assert!(state
        .mark_provider_transport_oauth_success_fenced(
            &transport,
            Some(1_000),
            Some("01900000-0000-7000-8000-000000000010"),
            None,
        )
        .await
        .expect("success watermark should persist"));

    let stored = repository
        .list_keys_by_ids(&["key-1".to_string()])
        .await
        .expect("key should reload")
        .pop()
        .expect("key should remain");
    assert_eq!(
        stored.oauth_invalid_reason.as_deref(),
        Some("[ACCOUNT_BLOCK] account deactivated")
    );
    assert_eq!(stored.oauth_invalid_at_unix_secs, Some(1));
}

#[tokio::test]
async fn runtime_oauth_success_rejects_credential_generation_mismatch() {
    let initial_config = json!({
        "provider_type": "codex",
        "refresh_token": "refresh-stable",
        "expires_at": 4102444800_u64
    });
    let (state, repository, _) = codex_oauth_state(&initial_config, "access-current");
    repository
        .update_key_oauth_runtime_state(
            "key-1",
            Some(1),
            Some("[OAUTH_EXPIRED] current generation invalid"),
            Some(1),
        )
        .await
        .expect("invalid state should persist");
    repository
        .upsert_key_upstream_metadata_namespace(
            "key-1",
            "codex",
            &json!({"credential_generation": "credential-v2"}),
            Some(1),
        )
        .await
        .expect("credential generation should persist");
    let transport = state
        .read_provider_transport_snapshot("provider-1", "endpoint-1", "key-1")
        .await
        .expect("transport should load")
        .expect("transport should exist");

    assert!(!state
        .mark_provider_transport_oauth_success_fenced(
            &transport,
            Some(1_000),
            Some("01900000-0000-7000-8000-000000000010"),
            Some("credential-v1"),
        )
        .await
        .expect("stale generation should be ignored"));

    let stored = repository
        .list_keys_by_ids(&["key-1".to_string()])
        .await
        .expect("key should reload")
        .pop()
        .expect("key should remain");
    assert_eq!(
        stored.oauth_invalid_reason.as_deref(),
        Some("[OAUTH_EXPIRED] current generation invalid")
    );
}

#[tokio::test]
async fn runtime_oauth_success_rejects_replaced_transport_credential() {
    let initial_config = json!({
        "provider_type": "codex",
        "refresh_token": "refresh-stable",
        "expires_at": 4102444800_u64
    });
    let (state, repository, _) = codex_oauth_state(&initial_config, "access-old");
    repository
        .update_key_oauth_runtime_state(
            "key-1",
            Some(1),
            Some("[OAUTH_EXPIRED] replacement generation invalid"),
            Some(1),
        )
        .await
        .expect("invalid state should persist");
    let stale_transport = state
        .read_provider_transport_snapshot("provider-1", "endpoint-1", "key-1")
        .await
        .expect("transport should load")
        .expect("transport should exist");

    let replacement_api_key =
        encrypt_python_fernet_plaintext(DEVELOPMENT_ENCRYPTION_KEY, "access-admin")
            .expect("replacement api key should encrypt");
    let mut replaced = repository
        .list_keys_by_ids(&["key-1".to_string()])
        .await
        .expect("key should load")
        .pop()
        .expect("key should exist");
    replaced.encrypted_api_key = Some(replacement_api_key);
    replace_key_credential_for_test(repository.as_ref(), replaced, "codex").await;
    repository
        .update_key_oauth_runtime_state(
            "key-1",
            Some(1),
            Some("[OAUTH_EXPIRED] replacement generation invalid"),
            Some(1),
        )
        .await
        .expect("replacement invalid state should persist");

    assert!(!state
        .mark_provider_transport_oauth_success_fenced(
            &stale_transport,
            Some(1_000),
            Some("01900000-0000-7000-8000-000000000010"),
            None,
        )
        .await
        .expect("stale transport should be ignored"));

    let stored = repository
        .list_keys_by_ids(&["key-1".to_string()])
        .await
        .expect("key should reload")
        .pop()
        .expect("key should remain");
    assert_eq!(
        stored.oauth_invalid_reason.as_deref(),
        Some("[OAUTH_EXPIRED] replacement generation invalid")
    );
}

#[tokio::test]
async fn runtime_oauth_success_requires_complete_request_order() {
    let initial_config = json!({
        "provider_type": "codex",
        "refresh_token": "refresh-stable",
        "expires_at": 4102444800_u64
    });
    let (state, repository, _) = codex_oauth_state(&initial_config, "access-current");
    repository
        .update_key_oauth_runtime_state(
            "key-1",
            Some(1),
            Some("[OAUTH_EXPIRED] unordered response"),
            Some(1),
        )
        .await
        .expect("invalid state should persist");
    let transport = state
        .read_provider_transport_snapshot("provider-1", "endpoint-1", "key-1")
        .await
        .expect("transport should load")
        .expect("transport should exist");

    assert!(!state
        .mark_provider_transport_oauth_success_fenced(
            &transport,
            None,
            Some("01900000-0000-7000-8000-000000000010"),
            None,
        )
        .await
        .expect("missing timestamp should be ignored"));
    assert!(!state
        .mark_provider_transport_oauth_success_fenced(&transport, Some(1_000), None, None,)
        .await
        .expect("missing request id should be ignored"));

    let stored = repository
        .list_keys_by_ids(&["key-1".to_string()])
        .await
        .expect("key should reload")
        .pop()
        .expect("key should remain");
    assert_eq!(
        stored.oauth_invalid_reason.as_deref(),
        Some("[OAUTH_EXPIRED] unordered response")
    );
}

#[tokio::test]
async fn older_observed_refresh_failure_cannot_override_newer_success() {
    let initial_config = json!({
        "provider_type": "codex",
        "refresh_token": "refresh-stable",
        "expires_at": 4102444800_u64
    });
    let (state, repository, _) = codex_oauth_state(&initial_config, "access-current");
    let transport = state
        .read_provider_transport_snapshot("provider-1", "endpoint-1", "key-1")
        .await
        .expect("transport should load")
        .expect("transport should exist");

    assert!(state
        .mark_provider_transport_oauth_success_fenced(
            &transport,
            Some(1_001),
            Some("01900000-0000-7000-8000-000000000011"),
            None,
        )
        .await
        .expect("newer success should persist"));
    assert!(!state
        .persist_local_oauth_refresh_failure_state_observed(
            &transport,
            401,
            r#"{"error":"invalid_grant"}"#,
            true,
            CodexRuntimeOAuthObservation {
                request_started_at_unix_ms: 1_000,
                request_order_id: "01900000-0000-7000-8000-000000000010",
                observed_credential_generation: None,
                runtime_invalid_reason: None,
            },
        )
        .await
        .expect("older refresh failure should be ignored"));

    let stored = repository
        .list_keys_by_ids(&["key-1".to_string()])
        .await
        .expect("key should reload")
        .pop()
        .expect("key should remain");
    assert!(stored.oauth_invalid_reason.is_none());
    assert_eq!(
        stored
            .upstream_metadata
            .as_ref()
            .and_then(|metadata| metadata.get("codex"))
            .and_then(|codex| codex.get("oauth_state_request_id")),
        Some(&json!("01900000-0000-7000-8000-000000000011"))
    );
}

#[tokio::test]
async fn newer_success_clears_observed_refresh_failure() {
    let initial_config = json!({
        "provider_type": "codex",
        "refresh_token": "refresh-stable",
        "expires_at": 4102444800_u64
    });
    let (state, repository, _) = codex_oauth_state(&initial_config, "access-current");
    let transport = state
        .read_provider_transport_snapshot("provider-1", "endpoint-1", "key-1")
        .await
        .expect("transport should load")
        .expect("transport should exist");

    assert!(!state
        .persist_local_oauth_refresh_failure_state_observed(
            &transport,
            401,
            r#"{"error":"invalid_grant"}"#,
            true,
            CodexRuntimeOAuthObservation {
                request_started_at_unix_ms: 1_000,
                request_order_id: "01900000-0000-7000-8000-000000000010",
                observed_credential_generation: None,
                runtime_invalid_reason: None,
            },
        )
        .await
        .expect("refresh failure should persist without auto-removal"));
    let failed = repository
        .list_keys_by_ids(&["key-1".to_string()])
        .await
        .expect("key should reload")
        .pop()
        .expect("key should remain");
    assert!(failed
        .oauth_invalid_reason
        .as_deref()
        .is_some_and(|reason| reason.starts_with("[REFRESH_FAILED]")));

    assert!(state
        .mark_provider_transport_oauth_success_fenced(
            &transport,
            Some(1_001),
            Some("01900000-0000-7000-8000-000000000011"),
            None,
        )
        .await
        .expect("newer success should persist"));
    let stored = repository
        .list_keys_by_ids(&["key-1".to_string()])
        .await
        .expect("key should reload")
        .pop()
        .expect("key should remain");
    assert!(stored.oauth_invalid_at_unix_secs.is_none());
    assert!(stored.oauth_invalid_reason.is_none());
}

#[tokio::test]
async fn observed_refresh_failure_rejects_credential_generation_mismatch() {
    let initial_config = json!({
        "provider_type": "codex",
        "refresh_token": "refresh-stable",
        "expires_at": 4102444800_u64
    });
    let (state, repository, _) = codex_oauth_state(&initial_config, "access-current");
    repository
        .upsert_key_upstream_metadata_namespace(
            "key-1",
            "codex",
            &json!({"credential_generation": "credential-v2"}),
            Some(1),
        )
        .await
        .expect("credential generation should persist");
    let transport = state
        .read_provider_transport_snapshot("provider-1", "endpoint-1", "key-1")
        .await
        .expect("transport should load")
        .expect("transport should exist");

    assert!(!state
        .persist_local_oauth_refresh_failure_state_observed(
            &transport,
            401,
            r#"{"error":"invalid_grant"}"#,
            true,
            CodexRuntimeOAuthObservation {
                request_started_at_unix_ms: 1_000,
                request_order_id: "01900000-0000-7000-8000-000000000010",
                observed_credential_generation: Some("credential-v1"),
                runtime_invalid_reason: None,
            },
        )
        .await
        .expect("stale generation should be ignored"));

    let stored = repository
        .list_keys_by_ids(&["key-1".to_string()])
        .await
        .expect("key should reload")
        .pop()
        .expect("key should remain");
    assert!(stored.oauth_invalid_reason.is_none());
    assert!(stored
        .upstream_metadata
        .as_ref()
        .and_then(|metadata| metadata.get("codex"))
        .and_then(|codex| codex.get("oauth_state_request_id"))
        .is_none());
}

#[tokio::test]
async fn codex_runtime_invalid_is_not_auto_removed_before_newer_success() {
    let initial_config = json!({
        "provider_type": "codex",
        "refresh_token": "refresh-stable",
        "expires_at": 4102444800_u64
    });
    let (state, repository, _) = codex_oauth_state(&initial_config, "access-current");
    let mut provider = repository
        .list_providers_by_ids(&["provider-1".to_string()])
        .await
        .expect("provider should load")
        .pop()
        .expect("provider should exist");
    provider.config = Some(json!({
        "pool_advanced": {"auto_remove_banned_keys": true}
    }));
    repository
        .update_provider(&provider)
        .await
        .expect("provider config should persist");
    state.clear_provider_transport_snapshot_cache();
    let transport = state
        .read_provider_transport_snapshot("provider-1", "endpoint-1", "key-1")
        .await
        .expect("transport should load")
        .expect("transport should exist");

    assert!(state
        .mark_provider_transport_oauth_invalid_fenced(
            &transport,
            "oauth_token_invalid",
            Some(1_000),
            Some("01900000-0000-7000-8000-000000000010"),
            None,
        )
        .await
        .expect("runtime invalid should persist"));
    let invalid = repository
        .list_keys_by_ids(&["key-1".to_string()])
        .await
        .expect("key lookup should succeed")
        .into_iter()
        .next()
        .expect("recoverable runtime invalid must retain the key");
    assert_eq!(
        invalid.oauth_invalid_reason.as_deref(),
        Some("oauth_token_invalid")
    );
    assert!(invalid.oauth_invalid_at_unix_secs.is_some());
    assert_eq!(
        invalid
            .upstream_metadata
            .as_ref()
            .and_then(|metadata| metadata.get("codex"))
            .and_then(|codex| codex.get("oauth_state_request_id")),
        Some(&json!("01900000-0000-7000-8000-000000000010"))
    );
    assert!(state
        .mark_provider_transport_oauth_success_fenced(
            &transport,
            Some(1_001),
            Some("01900000-0000-7000-8000-000000000011"),
            None,
        )
        .await
        .expect("newer success should clear invalid"));
}

#[tokio::test]
async fn codex_observed_terminal_refresh_failure_is_persisted_without_auto_removal() {
    let initial_config = json!({
        "provider_type": "codex",
        "refresh_token": "refresh-stable",
        "expires_at": 4102444800_u64
    });
    let (state, repository, _) = codex_oauth_state(&initial_config, "access-current");
    let mut provider = repository
        .list_providers_by_ids(&["provider-1".to_string()])
        .await
        .expect("provider should load")
        .pop()
        .expect("provider should exist");
    provider.config = Some(json!({
        "pool_advanced": {"auto_remove_banned_keys": true}
    }));
    repository
        .update_provider(&provider)
        .await
        .expect("provider config should persist");
    state.clear_provider_transport_snapshot_cache();
    let transport = state
        .read_provider_transport_snapshot("provider-1", "endpoint-1", "key-1")
        .await
        .expect("transport should load")
        .expect("transport should exist");

    assert!(!state
        .persist_local_oauth_refresh_failure_state_observed(
            &transport,
            401,
            r#"{"error":{"code":"invalid_grant","message":"refresh token invalid"}}"#,
            true,
            CodexRuntimeOAuthObservation {
                request_started_at_unix_ms: 1_000,
                request_order_id: "01900000-0000-7000-8000-000000000010",
                observed_credential_generation: None,
                runtime_invalid_reason: Some("[OAUTH_EXPIRED] session expired"),
            },
        )
        .await
        .expect("terminal refresh failure should persist without auto-removal"));

    let stored = repository
        .list_keys_by_ids(&["key-1".to_string()])
        .await
        .expect("key lookup should succeed")
        .into_iter()
        .next()
        .expect("request-scoped terminal refresh failure must retain the key");
    let reason = stored
        .oauth_invalid_reason
        .as_deref()
        .expect("combined invalid reason should persist");
    assert!(reason.starts_with("[OAUTH_EXPIRED] session expired"));
    assert!(reason.contains("[REFRESH_FAILED]"));
    assert!(stored.oauth_invalid_at_unix_secs.is_some());
    assert_eq!(
        stored
            .upstream_metadata
            .as_ref()
            .and_then(|metadata| metadata.get("codex"))
            .and_then(|codex| codex.get("oauth_state_request_id")),
        Some(&json!("01900000-0000-7000-8000-000000000010"))
    );
}

#[tokio::test]
async fn codex_runtime_account_block_is_still_auto_removed() {
    let initial_config = json!({
        "provider_type": "codex",
        "refresh_token": "refresh-stable",
        "expires_at": 4102444800_u64
    });
    let (state, repository, _) = codex_oauth_state(&initial_config, "access-current");
    let mut provider = repository
        .list_providers_by_ids(&["provider-1".to_string()])
        .await
        .expect("provider should load")
        .pop()
        .expect("provider should exist");
    provider.config = Some(json!({
        "pool_advanced": {"auto_remove_banned_keys": true}
    }));
    repository
        .update_provider(&provider)
        .await
        .expect("provider config should persist");
    state.clear_provider_transport_snapshot_cache();
    let transport = state
        .read_provider_transport_snapshot("provider-1", "endpoint-1", "key-1")
        .await
        .expect("transport should load")
        .expect("transport should exist");

    assert!(state
        .mark_provider_transport_oauth_invalid_fenced(
            &transport,
            "[ACCOUNT_BLOCK] account deactivated",
            Some(1_000),
            Some("01900000-0000-7000-8000-000000000010"),
            None,
        )
        .await
        .expect("account block should persist and auto-remove"));
    assert!(repository
        .list_keys_by_ids(&["key-1".to_string()])
        .await
        .expect("key lookup should succeed")
        .is_empty());
}
