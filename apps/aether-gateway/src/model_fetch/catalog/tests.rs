use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use aether_contracts::{ExecutionPlan, ExecutionResult, ProxySnapshot, ResponseBody};
use aether_provider_transport::snapshot::{
    GatewayProviderTransportEndpoint, GatewayProviderTransportKey, GatewayProviderTransportProvider,
};
use aether_provider_transport::LocalResolvedOAuthRequestAuth;
use aether_runtime_state::MemoryRuntimeStateConfig;
use async_trait::async_trait;
use serde_json::json;

use super::*;

const TEST_PROVIDER_ID: &str = "provider-codex";
const TEST_ENDPOINT_ID: &str = "endpoint-responses";
const TEST_SECONDARY_ENDPOINT_ID: &str = "endpoint-responses-secondary";
const TEST_KEY_ID: &str = "key-oauth";
const TEST_OAUTH_TOKEN: &str = "oauth-super-secret-token";
const TEST_CREDENTIAL_GENERATION_A: &str = "0198-aether-catalog-generation-a";
const TEST_CREDENTIAL_GENERATION_B: &str = "0198-aether-catalog-generation-b";

struct QueuedExecution {
    delay: Duration,
    result: Result<ExecutionResult, String>,
}

impl QueuedExecution {
    fn result(result: ExecutionResult) -> Self {
        Self {
            delay: Duration::ZERO,
            result: Ok(result),
        }
    }

    fn delayed(delay: Duration, result: ExecutionResult) -> Self {
        Self {
            delay,
            result: Ok(result),
        }
    }

    fn error(error: impl Into<String>) -> Self {
        Self {
            delay: Duration::ZERO,
            result: Err(error.into()),
        }
    }
}

#[derive(Clone)]
struct TestRuntime {
    state: RuntimeState,
    transport: Arc<Mutex<GatewayProviderTransportSnapshot>>,
    strong_credential_scope: Arc<Mutex<Result<Option<String>, String>>>,
    strong_read_count: Arc<AtomicUsize>,
    rebind_after_strong_read: Arc<Mutex<Option<(usize, String)>>>,
    execution_results: Arc<Mutex<VecDeque<QueuedExecution>>>,
    executed_plans: Arc<Mutex<Vec<ExecutionPlan>>>,
    execution_count: Arc<AtomicUsize>,
    unavailable_endpoint_ids: Arc<Mutex<BTreeSet<String>>>,
    fetch_pace_ttl: Duration,
}

impl TestRuntime {
    fn new(execution_results: Vec<QueuedExecution>) -> Self {
        Self {
            state: RuntimeState::memory(MemoryRuntimeStateConfig::default()),
            transport: Arc::new(Mutex::new(sample_codex_transport())),
            strong_credential_scope: Arc::new(Mutex::new(Ok(Some(test_credential_scope(
                TEST_CREDENTIAL_GENERATION_A,
            ))))),
            strong_read_count: Arc::new(AtomicUsize::new(0)),
            rebind_after_strong_read: Arc::new(Mutex::new(None)),
            execution_results: Arc::new(Mutex::new(VecDeque::from(execution_results))),
            executed_plans: Arc::new(Mutex::new(Vec::new())),
            execution_count: Arc::new(AtomicUsize::new(0)),
            unavailable_endpoint_ids: Arc::new(Mutex::new(BTreeSet::new())),
            fetch_pace_ttl: CODEX_CATALOG_FETCH_PACE_TTL,
        }
    }

    fn with_fetch_pace_ttl(mut self, fetch_pace_ttl: Duration) -> Self {
        self.fetch_pace_ttl = fetch_pace_ttl;
        self
    }

    fn enqueue(&self, execution: QueuedExecution) {
        self.execution_results
            .lock()
            .expect("execution results mutex")
            .push_back(execution);
    }

    fn execution_count(&self) -> usize {
        self.execution_count.load(Ordering::SeqCst)
    }

    fn executed_plans(&self) -> Vec<ExecutionPlan> {
        self.executed_plans
            .lock()
            .expect("executed plans mutex")
            .clone()
    }

    fn mark_endpoint_unavailable(&self, endpoint_id: &str) {
        self.unavailable_endpoint_ids
            .lock()
            .expect("unavailable endpoint ids mutex")
            .insert(endpoint_id.to_string());
    }

    fn set_credential_generation(&self, generation: &str) {
        self.transport
            .lock()
            .expect("transport mutex")
            .key
            .upstream_metadata = Some(json!({
            "codex": {
                "account_id": format!("account-{generation}"),
                "credential_generation": generation,
            }
        }));
        *self
            .strong_credential_scope
            .lock()
            .expect("strong credential scope mutex") = Ok(Some(test_credential_scope(generation)));
    }

    fn set_strong_credential_scope(&self, scope: Result<Option<String>, String>) {
        *self
            .strong_credential_scope
            .lock()
            .expect("strong credential scope mutex") = scope;
    }

    fn refresh_transport_secrets(&self) {
        let mut transport = self.transport.lock().expect("transport mutex");
        transport.key.decrypted_api_key = "refreshed-oauth-placeholder".to_string();
        transport.key.decrypted_auth_config = Some(
            json!({"refresh_token": "refreshed-token", "access_token": "new-access"}).to_string(),
        );
    }

    fn rebind_after_strong_read(&self, read_number: usize, generation: &str) {
        *self
            .rebind_after_strong_read
            .lock()
            .expect("strong read rebind hook mutex") = Some((read_number, generation.to_string()));
    }
}

#[async_trait]
impl ModelFetchTransportRuntime for TestRuntime {
    async fn resolve_local_oauth_request_auth(
        &self,
        _transport: &GatewayProviderTransportSnapshot,
    ) -> Result<Option<LocalResolvedOAuthRequestAuth>, String> {
        Ok(Some(LocalResolvedOAuthRequestAuth::Header {
            name: "authorization".to_string(),
            value: format!("Bearer {TEST_OAUTH_TOKEN}"),
        }))
    }

    async fn resolve_model_fetch_proxy(
        &self,
        _transport: &GatewayProviderTransportSnapshot,
    ) -> Option<ProxySnapshot> {
        None
    }

    async fn execute_model_fetch_execution_plan(
        &self,
        plan: &ExecutionPlan,
    ) -> Result<ExecutionResult, String> {
        self.execution_count.fetch_add(1, Ordering::SeqCst);
        self.executed_plans
            .lock()
            .expect("executed plans mutex")
            .push(plan.clone());
        let queued = self
            .execution_results
            .lock()
            .expect("execution results mutex")
            .pop_front()
            .ok_or_else(|| "missing queued execution result".to_string())?;
        if !queued.delay.is_zero() {
            tokio::time::sleep(queued.delay).await;
        }
        queued.result.map(|mut result| {
            result.request_id.clone_from(&plan.request_id);
            result.candidate_id.clone_from(&plan.candidate_id);
            result
        })
    }
}

#[async_trait]
impl CodexCatalogRuntime for TestRuntime {
    fn codex_catalog_runtime_state(&self) -> &RuntimeState {
        &self.state
    }

    fn codex_catalog_fetch_pace_ttl(&self) -> Duration {
        self.fetch_pace_ttl
    }

    async fn read_codex_catalog_transport_snapshot(
        &self,
        provider_id: &str,
        endpoint_id: &str,
        key_id: &str,
    ) -> Result<Option<GatewayProviderTransportSnapshot>, String> {
        if self
            .unavailable_endpoint_ids
            .lock()
            .expect("unavailable endpoint ids mutex")
            .contains(endpoint_id)
        {
            return Ok(None);
        }
        if provider_id != TEST_PROVIDER_ID
            || !matches!(endpoint_id, TEST_ENDPOINT_ID | TEST_SECONDARY_ENDPOINT_ID)
        {
            return Ok(None);
        }
        let mut transport = self.transport.lock().expect("transport mutex").clone();
        transport.endpoint.id = endpoint_id.to_string();
        transport.key.id = key_id.to_string();
        Ok(Some(transport))
    }

    async fn read_codex_catalog_credential_scope_strong(
        &self,
        provider_id: &str,
        _key_id: &str,
    ) -> Result<Option<String>, String> {
        if provider_id != TEST_PROVIDER_ID {
            return Ok(None);
        }
        let scope = self
            .strong_credential_scope
            .lock()
            .expect("strong credential scope mutex")
            .clone();
        let read_number = self.strong_read_count.fetch_add(1, Ordering::SeqCst) + 1;
        let rebind_generation = {
            let mut hook = self
                .rebind_after_strong_read
                .lock()
                .expect("strong read rebind hook mutex");
            match hook.as_ref() {
                Some((trigger, _)) if *trigger == read_number => {
                    hook.take().map(|(_, generation)| generation)
                }
                _ => None,
            }
        };
        if let Some(generation) = rebind_generation {
            self.set_credential_generation(&generation);
        }
        scope
    }
}

fn sample_codex_transport() -> GatewayProviderTransportSnapshot {
    GatewayProviderTransportSnapshot {
        provider: GatewayProviderTransportProvider {
            id: TEST_PROVIDER_ID.to_string(),
            name: "Codex".to_string(),
            provider_type: "codex".to_string(),
            website: None,
            is_active: true,
            keep_priority_on_conversion: false,
            enable_format_conversion: true,
            concurrent_limit: None,
            max_retries: None,
            proxy: None,
            request_timeout_secs: None,
            stream_first_byte_timeout_secs: None,
            config: None,
        },
        endpoint: GatewayProviderTransportEndpoint {
            id: TEST_ENDPOINT_ID.to_string(),
            provider_id: TEST_PROVIDER_ID.to_string(),
            api_format: "openai:responses".to_string(),
            api_family: Some("openai".to_string()),
            endpoint_kind: Some("responses".to_string()),
            is_active: true,
            base_url: "https://chatgpt.com/backend-api/codex".to_string(),
            header_rules: None,
            body_rules: None,
            max_retries: None,
            custom_path: Some("/responses".to_string()),
            config: None,
            format_acceptance_config: None,
            proxy: None,
        },
        key: GatewayProviderTransportKey {
            id: TEST_KEY_ID.to_string(),
            provider_id: TEST_PROVIDER_ID.to_string(),
            name: "primary".to_string(),
            auth_type: "oauth".to_string(),
            is_active: true,
            api_formats: Some(vec!["openai:responses".to_string()]),
            auth_type_by_format: None,
            allow_auth_channel_mismatch_formats: None,
            allowed_models: None,
            capabilities: None,
            rate_multipliers: None,
            global_priority_by_format: None,
            expires_at_unix_secs: None,
            proxy: None,
            fingerprint: None,
            upstream_metadata: Some(json!({
                "codex": {
                    "account_id": "account-test",
                    "credential_generation": TEST_CREDENTIAL_GENERATION_A,
                }
            })),
            decrypted_api_key: "oauth-placeholder".to_string(),
            decrypted_auth_config: None,
        },
    }
}

fn target() -> CodexCatalogTarget {
    target_for_key(TEST_KEY_ID)
}

fn target_for_key(key_id: &str) -> CodexCatalogTarget {
    CodexCatalogTarget {
        identity: CodexCatalogIdentity::new(TEST_PROVIDER_ID, key_id),
        endpoint_ids: vec![TEST_ENDPOINT_ID.to_string()],
        credential_scope: Some(test_credential_scope(TEST_CREDENTIAL_GENERATION_A)),
    }
}

fn target_for_generation(generation: &str) -> CodexCatalogTarget {
    CodexCatalogTarget {
        identity: CodexCatalogIdentity::new(TEST_PROVIDER_ID, TEST_KEY_ID),
        endpoint_ids: vec![TEST_ENDPOINT_ID.to_string()],
        credential_scope: Some(test_credential_scope(generation)),
    }
}

fn test_credential_scope(generation: &str) -> String {
    credential_scope_digest("generation", generation).expect("test credential scope")
}

fn version(value: &str) -> NormalizedCodexClientVersion {
    let normalized = normalize_codex_client_version(Some(value));
    assert!(!normalized.used_fallback(), "test version must be valid");
    normalized
}

fn codex_model(slug: &str) -> Value {
    json!({
        "id": slug,
        "slug": slug,
        "display_name": format!("Dynamic {slug}"),
        "model_messages": {"instructions_template": format!("Instructions for {slug}")},
        "available_in_plans": ["plus"],
        "future_capability": {"opaque": true}
    })
}

fn execution_result(
    status_code: u16,
    json_body: Option<Value>,
    etag: Option<&str>,
) -> ExecutionResult {
    ExecutionResult {
        request_id: "queued-request".to_string(),
        candidate_id: None,
        status_code,
        headers: etag
            .map(|etag| BTreeMap::from([("ETag".to_string(), etag.to_string())]))
            .unwrap_or_default(),
        response_observation: None,
        body: Some(ResponseBody {
            json_body,
            body_bytes_b64: None,
        }),
        telemetry: None,
        error: None,
    }
}

fn successful_execution(slug: &str, etag: &str) -> QueuedExecution {
    QueuedExecution::result(execution_result(
        200,
        Some(json!({"models": [codex_model(slug)]})),
        Some(etag),
    ))
}

async fn load_one(
    runtime: &TestRuntime,
    client_version: &NormalizedCodexClientVersion,
) -> CodexCatalogLoad {
    load_codex_catalogs(runtime, &[target()], client_version).await
}

async fn seed_catalog(
    runtime: &TestRuntime,
    client_version: &NormalizedCodexClientVersion,
) -> CodexCatalogSnapshot {
    load_one(runtime, client_version)
        .await
        .snapshot(TEST_PROVIDER_ID, TEST_KEY_ID)
        .cloned()
        .expect("seed catalog snapshot")
}

async fn read_raw_snapshot(
    runtime: &TestRuntime,
    target: &CodexCatalogTarget,
    client_version: &str,
) -> Option<CodexCatalogSnapshot> {
    runtime
        .state
        .kv_get(&catalog_lkg_key(target, client_version))
        .await
        .expect("read raw catalog LKG")
        .map(|raw| serde_json::from_str(&raw).expect("decode raw catalog LKG"))
}

async fn wait_for_execution_count(runtime: &TestRuntime, expected: usize) {
    tokio::time::timeout(Duration::from_secs(1), async {
        while runtime.execution_count() < expected {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("model fetch execution should start");
}

async fn read_status(
    runtime: &TestRuntime,
    client_version: &NormalizedCodexClientVersion,
) -> (String, CodexCatalogStatus) {
    let raw = runtime
        .state
        .kv_get(&catalog_status_key(&target(), client_version.as_str()))
        .await
        .expect("read catalog status")
        .expect("catalog status should exist");
    let status = serde_json::from_str(&raw).expect("decode catalog status");
    (raw, status)
}

#[test]
fn client_version_normalization_is_strict_and_drops_suffixes() {
    let stable = normalize_codex_client_version(Some("0.145.2"));
    assert_eq!(stable.as_str(), "0.145.2");
    assert!(!stable.used_fallback());

    let prerelease = normalize_codex_client_version(Some("0.146.0-beta.2+desktop.7"));
    assert_eq!(prerelease.as_str(), "0.146.0");
    assert!(!prerelease.used_fallback());
}

#[tokio::test]
async fn management_catalog_uses_version_floor_and_newest_success_not_last_client() {
    let runtime = TestRuntime::new(vec![successful_execution("gpt-new", "etag")]);
    remember_seen_version(&runtime.state, &target(), "0.144.1").await;
    let initial = read_codex_management_catalog(&runtime, TEST_PROVIDER_ID, TEST_KEY_ID)
        .await
        .expect("management context");
    assert_eq!(initial.client_version, codex_client_version());
    assert!(initial.models.is_none());

    seed_catalog(&runtime, &version("0.200.0")).await;
    remember_seen_version(&runtime.state, &target(), "0.144.1").await;
    let current = read_codex_management_catalog(&runtime, TEST_PROVIDER_ID, TEST_KEY_ID)
        .await
        .expect("management context");
    assert_eq!(current.client_version, "0.200.0");
    assert_eq!(current.models.unwrap()[0]["slug"], "gpt-new");
    assert_eq!(
        runtime.execution_count(),
        1,
        "management read does not fetch upstream"
    );

    runtime
        .state
        .kv_delete(&catalog_fresh_key(&target(), "0.200.0"))
        .await
        .unwrap();
    let stale = read_codex_management_catalog(&runtime, TEST_PROVIDER_ID, TEST_KEY_ID)
        .await
        .unwrap();
    assert_eq!(stale.client_version, "0.200.0");
    assert!(
        stale.models.is_none(),
        "expired LKG must not become a fresh admin cache"
    );

    runtime.set_credential_generation(TEST_CREDENTIAL_GENERATION_B);
    let rebound = read_codex_management_catalog(&runtime, TEST_PROVIDER_ID, TEST_KEY_ID)
        .await
        .unwrap();
    assert_eq!(rebound.client_version, codex_client_version());
    assert!(rebound.models.is_none());
}

#[tokio::test]
async fn management_refresh_updates_shared_catalog_but_rejects_replaced_credentials() {
    let runtime = TestRuntime::new(vec![]);
    let context = read_codex_management_catalog(&runtime, TEST_PROVIDER_ID, TEST_KEY_ID)
        .await
        .unwrap();
    let transports = vec![sample_codex_transport()];
    for slug in ["gpt-old", "gpt-6-astra"] {
        store_codex_management_catalog(
            &runtime,
            &context,
            &transports,
            vec![codex_model(slug)],
            Some("etag"),
        )
        .await;
        let shared = read_lkg_snapshot(&runtime, &target(), &context.client_version)
            .await
            .unwrap();
        assert_eq!(shared.models[0]["slug"], slug);
        let admin = read_codex_management_catalog(&runtime, TEST_PROVIDER_ID, TEST_KEY_ID)
            .await
            .unwrap();
        assert_eq!(admin.models.unwrap()[0]["slug"], slug);
    }
    runtime.set_credential_generation(TEST_CREDENTIAL_GENERATION_B);
    store_codex_management_catalog(
        &runtime,
        &context,
        &transports,
        vec![codex_model("gpt-leaked")],
        None,
    )
    .await;
    let rebound = read_codex_management_catalog(&runtime, TEST_PROVIDER_ID, TEST_KEY_ID)
        .await
        .unwrap();
    assert!(rebound.models.is_none());
    let old = read_raw_snapshot(&runtime, &target(), &context.client_version)
        .await
        .unwrap();
    assert_eq!(old.models[0]["slug"], "gpt-6-astra");
}

#[test]
fn invalid_or_oversized_client_versions_use_bounded_fallback_identity() {
    for raw in [
        "not-a-version".to_string(),
        "1.2".to_string(),
        format!("1.2.3-{}", "x".repeat(CODEX_CLIENT_VERSION_MAX_LEN)),
    ] {
        let normalized = normalize_codex_client_version(Some(&raw));
        assert_eq!(normalized.as_str(), codex_client_version());
        assert!(normalized.used_fallback());
        assert!(!catalog_lkg_key(&target(), normalized.as_str()).contains(&raw));
    }
}

#[test]
fn model_validation_is_schema_opaque_but_requires_identity() {
    let models = vec![serde_json::json!({
        "slug": "gpt-future-dynamic",
        "model_messages": {"instructions_template": "Future instructions"},
        "future_capability": {"mode": "opaque"}
    })];
    assert!(validate_catalog_models(&models).is_ok());
    assert!(validate_catalog_models(&[serde_json::json!({
        "display_name": "Missing identity"
    })])
    .is_err());
    assert!(validate_catalog_models(&[serde_json::json!({
        "slug": " gpt-whitespace-identity "
    })])
    .is_err());
    assert!(validate_catalog_models(&[serde_json::json!({
        "slug": "gpt+future@dynamic"
    })])
    .is_ok());
}

#[test]
fn error_status_is_sanitized_before_persistence_or_logging() {
    let secret = "Bearer oauth-secret-token";
    let safe = safe_catalog_error(&format!("connection reset: {secret}"));
    assert_eq!(safe, "upstream catalog fetch failed");
    assert!(!safe.contains(secret));
}

#[test]
fn local_singleflight_fallback_is_keyed_by_scoped_flight_identity() {
    let account_a_key = catalog_flight_key(
        &target_for_generation(TEST_CREDENTIAL_GENERATION_A),
        "0.145.2",
    );
    let account_b_key = catalog_flight_key(
        &target_for_generation(TEST_CREDENTIAL_GENERATION_B),
        "0.145.2",
    );
    let account_a = local_catalog_fallback_flight(&account_a_key);
    let account_a_follower = local_catalog_fallback_flight(&account_a_key);
    let account_b = local_catalog_fallback_flight(&account_b_key);

    assert!(Arc::ptr_eq(&account_a, &account_a_follower));
    assert!(!Arc::ptr_eq(&account_a, &account_b));

    drop(account_a_follower);
    prune_local_catalog_fallback_flight(&account_a_key, &account_a);
    drop(account_a);
    drop(account_b);
    let cleanup_key = format!("{account_b_key}:cleanup");
    let cleanup = local_catalog_fallback_flight(&cleanup_key);
    prune_local_catalog_fallback_flight(&cleanup_key, &cleanup);
    drop(cleanup);
    let flights = CODEX_CATALOG_LOCAL_FALLBACK_FLIGHTS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    assert!(!flights.contains_key(&account_a_key));
    assert!(!flights.contains_key(&account_b_key));
    assert!(!flights.contains_key(&cleanup_key));
}

#[test]
fn upstream_etag_requires_valid_entity_tag_syntax() {
    assert_eq!(
        normalize_etag("\"catalog-v1\"").as_deref(),
        Some("\"catalog-v1\"")
    );
    assert_eq!(
        normalize_etag("W/\"catalog-v1\"").as_deref(),
        Some("W/\"catalog-v1\"")
    );
    assert_eq!(normalize_etag("catalog-v1"), None);
    assert_eq!(normalize_etag("\"bad\"quote\""), None);
}

#[tokio::test]
async fn catalogs_are_isolated_by_normalized_client_version() {
    let runtime = TestRuntime::new(vec![
        successful_execution("gpt-dynamic-v145", "\"etag-v145\""),
        successful_execution("gpt-dynamic-v146", "\"etag-v146\""),
    ]);
    let v145 = version("0.145.2");
    let v146 = version("0.146.0");

    let first = seed_catalog(&runtime, &v145).await;
    tokio::time::sleep(Duration::from_millis(25)).await;
    let second = seed_catalog(&runtime, &v146).await;
    let first_again = seed_catalog(&runtime, &v145).await;

    assert_eq!(first.client_version, "0.145.2");
    assert_eq!(second.client_version, "0.146.0");
    assert_eq!(first.models[0]["slug"], "gpt-dynamic-v145");
    assert_eq!(second.models[0]["slug"], "gpt-dynamic-v146");
    assert_eq!(first_again, first);
    assert_eq!(first.etag.as_deref(), Some("\"etag-v145\""));
    assert_eq!(second.etag.as_deref(), Some("\"etag-v146\""));
    assert_eq!(runtime.execution_count(), 2);

    let plans = runtime.executed_plans();
    assert_eq!(plans.len(), 2);
    assert!(plans[0].url.ends_with("/models?client_version=0.145.2"));
    assert!(plans[1].url.ends_with("/models?client_version=0.146.0"));
    for (plan, version) in plans.iter().zip(["0.145.2", "0.146.0"]) {
        let ua = plan.headers.get("user-agent").expect("画像应包含 UA");
        assert!(ua.starts_with(&format!("codex_cli_rs/{version} (")));
        assert!(ua.ends_with(") unknown"));
    }

    let target = target();
    assert_ne!(
        catalog_lkg_key(&target, v145.as_str()),
        catalog_lkg_key(&target, v146.as_str())
    );
}

#[tokio::test]
async fn credential_rebind_never_returns_previous_scope_lkg_on_503_or_401() {
    for status_code in [503, 401] {
        let runtime = TestRuntime::new(vec![
            successful_execution("gpt-account-a", "\"etag-account-a\""),
            QueuedExecution::result(execution_result(
                status_code,
                Some(json!({"error": {"message": "account B unavailable"}})),
                None,
            )),
        ]);
        let client_version = version("0.145.2");
        let account_a = seed_catalog(&runtime, &client_version).await;
        let target_a = target_for_generation(TEST_CREDENTIAL_GENERATION_A);

        runtime.set_credential_generation(TEST_CREDENTIAL_GENERATION_B);
        let account_b_load = load_one(&runtime, &client_version).await;
        let target_b = target_for_generation(TEST_CREDENTIAL_GENERATION_B);

        assert!(account_b_load
            .snapshot(TEST_PROVIDER_ID, TEST_KEY_ID)
            .is_none());
        assert_eq!(runtime.execution_count(), 2);
        assert_eq!(
            read_raw_snapshot(&runtime, &target_a, client_version.as_str()).await,
            Some(account_a)
        );
        assert!(
            read_raw_snapshot(&runtime, &target_b, client_version.as_str())
                .await
                .is_none()
        );
        assert_ne!(
            catalog_lkg_key(&target_a, client_version.as_str()),
            catalog_lkg_key(&target_b, client_version.as_str())
        );
    }
}

#[tokio::test]
async fn previous_scope_fresh_retry_flight_and_pace_do_not_suppress_rebound_scope() {
    let runtime = TestRuntime::new(vec![
        successful_execution("gpt-account-a", "\"etag-account-a\""),
        successful_execution("gpt-account-b", "\"etag-account-b\""),
    ])
    .with_fetch_pace_ttl(Duration::from_secs(2));
    let client_version = version("0.145.2");
    let _ = seed_catalog(&runtime, &client_version).await;
    let target_a = target_for_generation(TEST_CREDENTIAL_GENERATION_A);
    runtime
        .state
        .kv_set(
            &catalog_retry_key(&target_a, client_version.as_str()),
            "account-a-retry".to_string(),
            Some(Duration::from_secs(10)),
        )
        .await
        .expect("seed account A retry marker");
    let account_a_flight = runtime
        .state
        .lock_try_acquire(
            &catalog_flight_key(&target_a, client_version.as_str()),
            "account-a-flight-holder",
            Duration::from_secs(10),
        )
        .await
        .expect("acquire account A flight")
        .expect("account A flight should be available");

    runtime.set_credential_generation(TEST_CREDENTIAL_GENERATION_B);
    let target_b = target_for_generation(TEST_CREDENTIAL_GENERATION_B);
    let account_b_load =
        tokio::time::timeout(Duration::from_secs(1), load_one(&runtime, &client_version))
            .await
            .expect("account B fetch must not wait on account A markers");

    assert_eq!(
        account_b_load
            .snapshot(TEST_PROVIDER_ID, TEST_KEY_ID)
            .expect("account B snapshot")
            .models[0]["slug"],
        "gpt-account-b"
    );
    assert_eq!(runtime.execution_count(), 2);
    assert_ne!(
        catalog_fresh_key(&target_a, client_version.as_str()),
        catalog_fresh_key(&target_b, client_version.as_str())
    );
    assert_ne!(
        catalog_retry_key(&target_a, client_version.as_str()),
        catalog_retry_key(&target_b, client_version.as_str())
    );
    assert_ne!(
        catalog_flight_key(&target_a, client_version.as_str()),
        catalog_flight_key(&target_b, client_version.as_str())
    );
    assert_ne!(
        catalog_fetch_pace_key(&target_a),
        catalog_fetch_pace_key(&target_b)
    );
    assert!(runtime
        .state
        .lock_release(&account_a_flight)
        .await
        .expect("release account A flight"));
}

#[tokio::test]
async fn token_refresh_with_same_generation_keeps_the_same_lkg_scope() {
    let runtime = TestRuntime::new(vec![successful_execution(
        "gpt-stable-account",
        "\"etag-stable-account\"",
    )]);
    let client_version = version("0.145.2");
    let seeded = seed_catalog(&runtime, &client_version).await;
    let target_before = target_for_generation(TEST_CREDENTIAL_GENERATION_A);

    runtime.refresh_transport_secrets();
    let after_refresh = load_one(&runtime, &client_version).await;
    let target_after = target_for_generation(TEST_CREDENTIAL_GENERATION_A);

    assert_eq!(
        after_refresh.snapshot(TEST_PROVIDER_ID, TEST_KEY_ID),
        Some(&seeded)
    );
    assert_eq!(runtime.execution_count(), 1);
    assert_eq!(
        catalog_lkg_key(&target_before, client_version.as_str()),
        catalog_lkg_key(&target_after, client_version.as_str())
    );
}

#[tokio::test]
async fn rebind_between_initial_strong_bind_and_lkg_return_fails_closed() {
    let runtime = TestRuntime::new(vec![successful_execution(
        "gpt-account-a-cached",
        "\"etag-account-a-cached\"",
    )]);
    let client_version = version("0.145.2");
    let seeded = seed_catalog(&runtime, &client_version).await;
    let target_a = target_for_generation(TEST_CREDENTIAL_GENERATION_A);
    let target_b = target_for_generation(TEST_CREDENTIAL_GENERATION_B);
    runtime.rebind_after_strong_read(4, TEST_CREDENTIAL_GENERATION_B);

    let load = load_one(&runtime, &client_version).await;

    assert!(load.snapshot(TEST_PROVIDER_ID, TEST_KEY_ID).is_none());
    assert_eq!(runtime.execution_count(), 1);
    assert_eq!(
        read_raw_snapshot(&runtime, &target_a, client_version.as_str()).await,
        Some(seeded)
    );
    assert!(
        read_raw_snapshot(&runtime, &target_b, client_version.as_str())
            .await
            .is_none()
    );
}

#[tokio::test]
async fn in_flight_previous_scope_fetch_is_discarded_after_rebind() {
    let runtime = TestRuntime::new(vec![
        QueuedExecution::delayed(
            Duration::from_millis(150),
            execution_result(
                200,
                Some(json!({"models": [codex_model("gpt-delayed-account-a")]})),
                Some("\"etag-delayed-account-a\""),
            ),
        ),
        successful_execution("gpt-account-b", "\"etag-account-b\""),
    ]);
    let client_version = version("0.145.2");
    let task_runtime = runtime.clone();
    let task_version = client_version.clone();
    let account_a_fetch = tokio::spawn(async move { load_one(&task_runtime, &task_version).await });
    wait_for_execution_count(&runtime, 1).await;

    runtime.set_credential_generation(TEST_CREDENTIAL_GENERATION_B);
    let account_a_load = account_a_fetch.await.expect("join account A fetch");
    let target_a = target_for_generation(TEST_CREDENTIAL_GENERATION_A);
    let target_b = target_for_generation(TEST_CREDENTIAL_GENERATION_B);

    assert!(account_a_load
        .snapshot(TEST_PROVIDER_ID, TEST_KEY_ID)
        .is_none());
    assert!(
        read_lkg_snapshot(&runtime, &target_a, client_version.as_str())
            .await
            .is_none()
    );
    assert!(
        read_lkg_snapshot(&runtime, &target_b, client_version.as_str())
            .await
            .is_none()
    );

    let account_b_load = load_one(&runtime, &client_version).await;
    assert_eq!(
        account_b_load
            .snapshot(TEST_PROVIDER_ID, TEST_KEY_ID)
            .expect("account B snapshot")
            .models[0]["slug"],
        "gpt-account-b"
    );
    assert_eq!(runtime.execution_count(), 2);
}

#[tokio::test]
async fn scope_change_during_persistence_removes_old_scope_success() {
    let runtime = TestRuntime::new(vec![successful_execution(
        "gpt-account-a-raced-persist",
        "\"etag-account-a-raced-persist\"",
    )]);
    let client_version = version("0.145.2");
    runtime.rebind_after_strong_read(2, TEST_CREDENTIAL_GENERATION_B);

    let load = load_one(&runtime, &client_version).await;
    let target_a = target_for_generation(TEST_CREDENTIAL_GENERATION_A);
    let target_b = target_for_generation(TEST_CREDENTIAL_GENERATION_B);

    assert!(load.snapshot(TEST_PROVIDER_ID, TEST_KEY_ID).is_none());
    assert_eq!(runtime.execution_count(), 1);
    for key in [
        catalog_lkg_key(&target_a, client_version.as_str()),
        catalog_fresh_key(&target_a, client_version.as_str()),
        catalog_status_key(&target_a, client_version.as_str()),
        catalog_retry_key(&target_a, client_version.as_str()),
    ] {
        assert!(runtime
            .state
            .kv_get(&key)
            .await
            .expect("read old-scope catalog key")
            .is_none());
    }
    assert!(!runtime
        .state
        .score_range_by_min(&catalog_versions_key(&target_a.identity), 0.0)
        .await
        .expect("read catalog version index")
        .contains(&catalog_version_member(&target_a, client_version.as_str())));
    assert!(read_recent_codex_catalog_client_version(
        &runtime.state,
        TEST_PROVIDER_ID,
        TEST_KEY_ID,
        target_b.credential_scope().expect("account B scope"),
    )
    .await
    .is_none());
}

#[tokio::test]
async fn stale_transport_scope_fails_closed_before_upstream_execution() {
    let runtime = TestRuntime::new(vec![successful_execution(
        "must-not-be-fetched",
        "\"etag-must-not-be-fetched\"",
    )]);
    let client_version = version("0.145.2");
    let target_a = target_for_generation(TEST_CREDENTIAL_GENERATION_A);
    let target_b = target_for_generation(TEST_CREDENTIAL_GENERATION_B);
    runtime.set_strong_credential_scope(Ok(Some(test_credential_scope(
        TEST_CREDENTIAL_GENERATION_B,
    ))));

    let load = load_one(&runtime, &client_version).await;

    assert!(load.snapshot(TEST_PROVIDER_ID, TEST_KEY_ID).is_none());
    assert_eq!(runtime.execution_count(), 0);
    assert!(runtime
        .state
        .kv_get(&catalog_lkg_key(&target_a, client_version.as_str()))
        .await
        .expect("read account A LKG")
        .is_none());
    assert!(runtime
        .state
        .kv_get(&catalog_lkg_key(&target_b, client_version.as_str()))
        .await
        .expect("read account B LKG")
        .is_none());
}

#[tokio::test]
async fn missing_or_unverifiable_strong_scope_fails_closed_before_cache_or_fetch() {
    let runtime = TestRuntime::new(vec![successful_execution(
        "gpt-account-a",
        "\"etag-account-a\"",
    )]);
    let client_version = version("0.145.2");
    let seeded = seed_catalog(&runtime, &client_version).await;
    let target_a = target_for_generation(TEST_CREDENTIAL_GENERATION_A);

    runtime.set_strong_credential_scope(Ok(None));
    let missing = load_one(&runtime, &client_version).await;
    assert!(missing.snapshot(TEST_PROVIDER_ID, TEST_KEY_ID).is_none());
    assert_eq!(runtime.execution_count(), 1);

    runtime.set_strong_credential_scope(Err("strong repository unavailable".to_string()));
    let unverifiable = load_one(&runtime, &client_version).await;
    assert!(unverifiable
        .snapshot(TEST_PROVIDER_ID, TEST_KEY_ID)
        .is_none());
    assert_eq!(runtime.execution_count(), 1);
    assert_eq!(
        read_raw_snapshot(&runtime, &target_a, client_version.as_str()).await,
        Some(seeded)
    );
}

#[tokio::test]
async fn concurrent_cold_requests_for_one_identity_and_version_singleflight() {
    let runtime = TestRuntime::new(vec![QueuedExecution::delayed(
        Duration::from_millis(100),
        execution_result(
            200,
            Some(json!({
                "models": [codex_model("gpt-singleflight-dynamic")]
            })),
            Some("\"etag-singleflight\""),
        ),
    )]);
    let client_version = version("0.145.2");

    let (left, right) = tokio::join!(
        load_one(&runtime, &client_version),
        load_one(&runtime, &client_version)
    );

    let left = left
        .snapshot(TEST_PROVIDER_ID, TEST_KEY_ID)
        .expect("left snapshot");
    let right = right
        .snapshot(TEST_PROVIDER_ID, TEST_KEY_ID)
        .expect("right snapshot");
    assert_eq!(left, right);
    assert_eq!(left.models[0]["slug"], "gpt-singleflight-dynamic");
    assert_eq!(runtime.execution_count(), 1);
    assert_eq!(runtime.executed_plans().len(), 1);
}

#[tokio::test]
async fn expired_fresh_ttl_returns_lkg_and_marks_target_stale_without_fetching() {
    let runtime = TestRuntime::new(vec![successful_execution(
        "gpt-stale-dynamic",
        "\"etag-stale\"",
    )]);
    let client_version = version("0.145.2");
    let seeded = seed_catalog(&runtime, &client_version).await;

    tokio::time::sleep(CODEX_CATALOG_FRESH_TTL + Duration::from_millis(25)).await;
    let load = load_one(&runtime, &client_version).await;

    assert_eq!(load.snapshot(TEST_PROVIDER_ID, TEST_KEY_ID), Some(&seeded));
    assert_eq!(load.stale_targets(), &[target()]);
    assert_eq!(runtime.execution_count(), 1);
}

#[tokio::test]
async fn fresh_and_stale_lkg_reads_do_not_wait_for_the_retention_lock() {
    let runtime = TestRuntime::new(vec![successful_execution(
        "gpt-retention-lock-dynamic",
        "\"etag-retention-lock\"",
    )]);
    let client_version = version("0.145.2");
    let seeded = seed_catalog(&runtime, &client_version).await;
    let target = target();
    let lease = runtime
        .state
        .lock_try_acquire(
            &catalog_retention_lock_key(&target.identity),
            "catalog-retention-test-holder",
            CODEX_CATALOG_RETENTION_LOCK_TTL,
        )
        .await
        .expect("acquire retention lock")
        .expect("retention lock should be available");

    let fresh = tokio::time::timeout(
        Duration::from_millis(250),
        load_one(&runtime, &client_version),
    )
    .await
    .expect("fresh LKG read must not wait for retention lock");
    assert_eq!(fresh.snapshot(TEST_PROVIDER_ID, TEST_KEY_ID), Some(&seeded));
    assert!(fresh.stale_targets().is_empty());

    assert!(runtime
        .state
        .kv_delete(&catalog_fresh_key(&target, client_version.as_str()))
        .await
        .expect("delete fresh marker"));
    let stale = tokio::time::timeout(
        Duration::from_millis(250),
        load_one(&runtime, &client_version),
    )
    .await
    .expect("stale LKG read must not wait for retention lock");
    assert_eq!(stale.snapshot(TEST_PROVIDER_ID, TEST_KEY_ID), Some(&seeded));
    assert_eq!(stale.stale_targets(), std::slice::from_ref(&target));
    assert_eq!(runtime.execution_count(), 1);

    assert!(runtime
        .state
        .lock_release(&lease)
        .await
        .expect("release retention lock"));
}

#[tokio::test]
async fn deleting_fresh_marker_does_not_delete_or_hide_lkg() {
    let runtime = TestRuntime::new(vec![successful_execution(
        "gpt-marker-dynamic",
        "\"etag-marker\"",
    )]);
    let client_version = version("0.145.2");
    let seeded = seed_catalog(&runtime, &client_version).await;
    let target = target();

    assert!(runtime
        .state
        .kv_delete(&catalog_fresh_key(&target, client_version.as_str()))
        .await
        .expect("delete fresh marker"));
    let load = load_one(&runtime, &client_version).await;

    assert_eq!(load.snapshot(TEST_PROVIDER_ID, TEST_KEY_ID), Some(&seeded));
    assert_eq!(load.stale_targets(), std::slice::from_ref(&target));
    assert!(runtime
        .state
        .kv_get(&catalog_lkg_key(&target, client_version.as_str()))
        .await
        .expect("read LKG")
        .is_some());
    assert_eq!(runtime.execution_count(), 1);
}

#[tokio::test]
async fn mismatched_fresh_marker_hash_is_treated_as_stale() {
    let runtime = TestRuntime::new(vec![successful_execution(
        "gpt-marker-hash-dynamic",
        "\"etag-marker-hash\"",
    )]);
    let client_version = version("0.145.2");
    let seeded = seed_catalog(&runtime, &client_version).await;
    let target = target();
    runtime
        .state
        .kv_set(
            &catalog_fresh_key(&target, client_version.as_str()),
            "not-the-snapshot-hash".to_string(),
            Some(CODEX_CATALOG_FRESH_TTL),
        )
        .await
        .expect("replace fresh marker");

    let load = load_one(&runtime, &client_version).await;
    assert_eq!(load.snapshot(TEST_PROVIDER_ID, TEST_KEY_ID), Some(&seeded));
    assert_eq!(load.stale_targets(), std::slice::from_ref(&target));
    assert_eq!(runtime.execution_count(), 1);
}

#[tokio::test]
async fn lkg_content_hash_detects_corrupted_snapshot_payload() {
    let runtime = TestRuntime::new(vec![successful_execution(
        "gpt-hash-dynamic",
        "\"etag-hash\"",
    )]);
    let client_version = version("0.145.2");
    let _ = seed_catalog(&runtime, &client_version).await;
    let target = target();
    let key = catalog_lkg_key(&target, client_version.as_str());
    let raw = runtime
        .state
        .kv_get(&key)
        .await
        .expect("read LKG")
        .expect("LKG should exist");
    let mut corrupted: CodexCatalogSnapshot = serde_json::from_str(&raw).expect("decode LKG");
    corrupted.models[0]["future_capability"] = json!({"corrupted": true});
    runtime
        .state
        .kv_set(
            &key,
            serde_json::to_string(&corrupted).expect("serialize corrupted LKG"),
            None,
        )
        .await
        .expect("replace LKG");

    assert!(
        read_lkg_snapshot(&runtime, &target, client_version.as_str())
            .await
            .is_none()
    );
}

#[tokio::test]
async fn cold_failure_retry_marker_suppresses_immediate_followup_fetch() {
    let runtime = TestRuntime::new(vec![QueuedExecution::error("network unavailable")]);
    let client_version = version("0.145.2");

    assert!(load_one(&runtime, &client_version)
        .await
        .snapshot(TEST_PROVIDER_ID, TEST_KEY_ID)
        .is_none());
    assert_eq!(runtime.execution_count(), 1);
    assert!(load_one(&runtime, &client_version)
        .await
        .snapshot(TEST_PROVIDER_ID, TEST_KEY_ID)
        .is_none());
    assert_eq!(runtime.execution_count(), 1);
}

#[tokio::test]
async fn upstream_failures_never_replace_non_empty_lkg_and_status_is_sanitized() {
    let runtime = TestRuntime::new(vec![successful_execution(
        "gpt-lkg-dynamic",
        "\"etag-lkg\"",
    )]);
    let client_version = version("0.145.2");
    let seeded = seed_catalog(&runtime, &client_version).await;
    let target = target();

    let failures = [
        (
            "5xx",
            QueuedExecution::result(execution_result(
                503,
                Some(json!({
                    "error": {
                        "message": format!("temporarily unavailable: Bearer {TEST_OAUTH_TOKEN}")
                    }
                })),
                None,
            )),
            Some(503),
        ),
        (
            "timeout",
            QueuedExecution::delayed(
                CODEX_CATALOG_FETCH_TIMEOUT + Duration::from_millis(100),
                execution_result(
                    200,
                    Some(json!({"models": [codex_model("must-not-be-stored")]})),
                    Some("\"etag-must-not-be-stored\""),
                ),
            ),
            None,
        ),
        (
            "empty models",
            QueuedExecution::result(execution_result(
                200,
                Some(json!({"models": []})),
                Some("\"etag-empty\""),
            )),
            Some(200),
        ),
        (
            "invalid JSON payload",
            QueuedExecution::result(execution_result(200, None, Some("\"etag-invalid\""))),
            Some(200),
        ),
    ];

    for (label, failure, expected_status) in failures {
        runtime.enqueue(failure);
        tokio::time::sleep(CODEX_CATALOG_FETCH_PACE_TTL + Duration::from_millis(5)).await;
        let _ = runtime
            .state
            .kv_delete(&catalog_retry_key(&target, client_version.as_str()))
            .await;
        let _ = runtime
            .state
            .kv_delete(&catalog_fresh_key(&target, client_version.as_str()))
            .await;
        refresh_codex_catalog_target(&runtime, &target, &client_version).await;

        let persisted = read_lkg_snapshot(&runtime, &target, client_version.as_str())
            .await
            .unwrap_or_else(|| panic!("{label}: LKG should remain available"));
        assert_eq!(persisted, seeded, "{label}: LKG was overwritten");

        let (raw_status, status) = read_status(&runtime, &client_version).await;
        assert_eq!(
            status.last_http_status, expected_status,
            "{label}: upstream status"
        );
        assert_eq!(
            status.last_success_at_unix_secs,
            Some(seeded.fetched_at_unix_secs)
        );
        assert_eq!(status.model_count, Some(1));
        assert!(status.used_lkg, "{label}: status should report LKG use");
        assert!(
            status.last_error.is_some(),
            "{label}: error should be visible"
        );
        assert!(
            !raw_status.contains(TEST_OAUTH_TOKEN),
            "{label}: status leaked OAuth token"
        );
    }

    assert_eq!(runtime.execution_count(), 5);
    assert_eq!(seeded.etag.as_deref(), Some("\"etag-lkg\""));
    assert!(runtime.executed_plans().iter().all(|plan| {
        plan.headers.get("authorization").map(String::as_str)
            == Some("Bearer oauth-super-secret-token")
    }));
}

#[tokio::test]
async fn partial_multi_endpoint_success_does_not_replace_lkg() {
    let runtime = TestRuntime::new(vec![successful_execution(
        "gpt-complete-lkg",
        "\"etag-complete\"",
    )]);
    let client_version = version("0.145.2");
    let seeded = seed_catalog(&runtime, &client_version).await;
    let mut multi_endpoint_target = target();
    multi_endpoint_target
        .endpoint_ids
        .push(TEST_ENDPOINT_ID.to_string());
    runtime.enqueue(successful_execution(
        "gpt-partial-replacement",
        "\"etag-partial\"",
    ));
    runtime.enqueue(QueuedExecution::result(execution_result(
        503,
        Some(json!({"error": {"message": "temporary endpoint failure"}})),
        None,
    )));
    runtime
        .state
        .kv_delete(&catalog_fresh_key(
            &multi_endpoint_target,
            client_version.as_str(),
        ))
        .await
        .expect("delete fresh marker");

    tokio::time::sleep(CODEX_CATALOG_FETCH_PACE_TTL + Duration::from_millis(5)).await;
    refresh_codex_catalog_target(&runtime, &multi_endpoint_target, &client_version).await;
    let persisted = read_lkg_snapshot(&runtime, &multi_endpoint_target, client_version.as_str())
        .await
        .expect("complete LKG should remain");
    assert_eq!(persisted, seeded);
    assert_eq!(runtime.execution_count(), 3);
}

#[tokio::test]
async fn missing_multi_endpoint_transport_does_not_replace_lkg() {
    let runtime = TestRuntime::new(vec![
        successful_execution("gpt-primary", "\"etag-primary\""),
        successful_execution("gpt-secondary", "\"etag-secondary\""),
    ]);
    let client_version = version("0.145.2");
    let mut multi_endpoint_target = target();
    multi_endpoint_target
        .endpoint_ids
        .push(TEST_SECONDARY_ENDPOINT_ID.to_string());
    let seeded = load_codex_catalogs(&runtime, &[multi_endpoint_target.clone()], &client_version)
        .await
        .snapshot(TEST_PROVIDER_ID, TEST_KEY_ID)
        .cloned()
        .expect("seed complete multi-endpoint LKG");
    runtime.mark_endpoint_unavailable(TEST_SECONDARY_ENDPOINT_ID);
    runtime
        .state
        .kv_delete(&catalog_fresh_key(
            &multi_endpoint_target,
            client_version.as_str(),
        ))
        .await
        .expect("delete fresh marker");

    tokio::time::sleep(CODEX_CATALOG_FETCH_PACE_TTL + Duration::from_millis(5)).await;
    refresh_codex_catalog_target(&runtime, &multi_endpoint_target, &client_version).await;
    let persisted = read_lkg_snapshot(&runtime, &multi_endpoint_target, client_version.as_str())
        .await
        .expect("complete LKG should remain");
    assert_eq!(persisted, seeded);
    assert_eq!(runtime.execution_count(), 2);
    let (_, status) = read_status(&runtime, &client_version).await;
    assert!(status.used_lkg);
    assert!(status.last_error.is_some());
}

#[tokio::test]
async fn catalog_status_sanitizes_transport_errors_containing_credentials() {
    let runtime = TestRuntime::new(vec![QueuedExecution::error(format!(
        "401 Unauthorized: Bearer {TEST_OAUTH_TOKEN}"
    ))]);
    let client_version = version("0.145.2");

    let load = load_one(&runtime, &client_version).await;
    assert!(load.snapshot(TEST_PROVIDER_ID, TEST_KEY_ID).is_none());

    let (raw_status, status) = read_status(&runtime, &client_version).await;
    assert_eq!(
        status.last_error.as_deref(),
        Some("upstream catalog authentication failed (status 401)")
    );
    assert!(!status.used_lkg);
    assert!(!raw_status.contains(TEST_OAUTH_TOKEN));
    assert!(!raw_status.to_ascii_lowercase().contains("bearer"));
}

#[tokio::test]
async fn version_index_retains_only_eight_most_recent_catalogs() {
    let executions = (0..10)
        .map(|index| {
            successful_execution(
                &format!("gpt-version-{index}"),
                &format!("\"etag-{index}\""),
            )
        })
        .collect();
    let runtime = TestRuntime::new(executions);
    let target = target();

    for index in 0..10 {
        let client_version = version(&format!("0.200.{index}"));
        let snapshot = seed_catalog(&runtime, &client_version).await;
        assert_eq!(snapshot.models[0]["slug"], format!("gpt-version-{index}"));
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    let versions = runtime
        .state
        .score_range_by_min(&catalog_versions_key(&target.identity), 0.0)
        .await
        .expect("read catalog versions");
    assert_eq!(versions.len(), CODEX_CATALOG_MAX_VERSIONS_PER_KEY);
    assert!(!versions.contains(&catalog_version_member(&target, "0.200.0")));
    assert!(!versions.contains(&catalog_version_member(&target, "0.200.1")));
    for index in 2..10 {
        let client_version = format!("0.200.{index}");
        assert!(versions.contains(&catalog_version_member(&target, &client_version)));
        assert!(runtime
            .state
            .kv_get(&catalog_lkg_key(&target, &client_version))
            .await
            .expect("read retained LKG")
            .is_some());
    }
    for trimmed in ["0.200.0", "0.200.1"] {
        assert!(runtime
            .state
            .kv_get(&catalog_lkg_key(&target, trimmed))
            .await
            .expect("read trimmed LKG")
            .is_none());
        assert!(runtime
            .state
            .kv_get(&catalog_status_key(&target, trimmed))
            .await
            .expect("read trimmed status")
            .is_none());
    }
    assert_eq!(runtime.execution_count(), 10);
    assert_eq!(
        read_recent_codex_catalog_client_version(
            &runtime.state,
            TEST_PROVIDER_ID,
            TEST_KEY_ID,
            target.credential_scope().expect("test credential scope"),
        )
        .await
        .as_deref(),
        Some("0.200.9")
    );
}

#[tokio::test]
async fn version_retention_is_bounded_across_all_credential_scopes_for_one_key() {
    let runtime = TestRuntime::new(
        (0..10)
            .map(|index| {
                successful_execution(
                    &format!("gpt-scope-version-{index}"),
                    &format!("\"etag-scope-version-{index}\""),
                )
            })
            .collect(),
    );
    let target_a = target_for_generation(TEST_CREDENTIAL_GENERATION_A);
    let target_b = target_for_generation(TEST_CREDENTIAL_GENERATION_B);

    for index in 0..5 {
        seed_catalog(&runtime, &version(&format!("0.300.{index}"))).await;
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    runtime.set_credential_generation(TEST_CREDENTIAL_GENERATION_B);
    for index in 5..10 {
        seed_catalog(&runtime, &version(&format!("0.300.{index}"))).await;
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    let retained = runtime
        .state
        .score_range_by_min(&catalog_versions_key(&target_a.identity), 0.0)
        .await
        .expect("read cross-scope catalog versions");
    assert_eq!(retained.len(), CODEX_CATALOG_MAX_VERSIONS_PER_KEY);
    for trimmed in ["0.300.0", "0.300.1"] {
        assert!(!retained.contains(&catalog_version_member(&target_a, trimmed)));
        assert!(runtime
            .state
            .kv_get(&catalog_lkg_key(&target_a, trimmed))
            .await
            .expect("read trimmed account A LKG")
            .is_none());
    }
    for retained_a in ["0.300.2", "0.300.3", "0.300.4"] {
        assert!(retained.contains(&catalog_version_member(&target_a, retained_a)));
        assert!(runtime
            .state
            .kv_get(&catalog_lkg_key(&target_a, retained_a))
            .await
            .expect("read retained account A LKG")
            .is_some());
    }
    for index in 5..10 {
        let retained_b = format!("0.300.{index}");
        assert!(retained.contains(&catalog_version_member(&target_b, &retained_b)));
        assert!(runtime
            .state
            .kv_get(&catalog_lkg_key(&target_b, &retained_b))
            .await
            .expect("read retained account B LKG")
            .is_some());
    }
    assert!(read_recent_codex_catalog_client_version(
        &runtime.state,
        TEST_PROVIDER_ID,
        TEST_KEY_ID,
        target_a.credential_scope().expect("account A scope"),
    )
    .await
    .is_none());
    assert_eq!(
        read_recent_codex_catalog_client_version(
            &runtime.state,
            TEST_PROVIDER_ID,
            TEST_KEY_ID,
            target_b.credential_scope().expect("account B scope"),
        )
        .await
        .as_deref(),
        Some("0.300.9")
    );
}

#[tokio::test]
async fn failed_client_versions_are_also_bounded_by_version_retention() {
    let runtime = TestRuntime::new(
        (0..10)
            .map(|_| QueuedExecution::error("network unavailable"))
            .collect(),
    );
    let target = target();

    for index in 0..10 {
        let client_version = version(&format!("0.220.{index}"));
        assert!(load_one(&runtime, &client_version)
            .await
            .snapshot(TEST_PROVIDER_ID, TEST_KEY_ID)
            .is_none());
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    let versions = runtime
        .state
        .score_range_by_min(&catalog_failed_versions_key(&target.identity), 0.0)
        .await
        .expect("read failed catalog versions");
    assert_eq!(versions.len(), CODEX_CATALOG_MAX_FAILED_VERSIONS_PER_KEY);
    for trimmed in ["0.220.0", "0.220.1"] {
        assert!(runtime
            .state
            .kv_get(&catalog_status_key(&target, trimmed))
            .await
            .expect("read trimmed failure status")
            .is_none());
    }
    assert_eq!(runtime.execution_count(), 10);
}

#[tokio::test]
async fn failed_version_churn_does_not_evict_non_empty_lkg_snapshots() {
    let mut executions = (0..CODEX_CATALOG_MAX_VERSIONS_PER_KEY)
        .map(|index| {
            successful_execution(
                &format!("gpt-stable-{index}"),
                &format!("\"stable-etag-{index}\""),
            )
        })
        .collect::<Vec<_>>();
    executions.extend((0..10).map(|_| QueuedExecution::error("network unavailable during churn")));
    let runtime = TestRuntime::new(executions);
    let target = target();

    for index in 0..CODEX_CATALOG_MAX_VERSIONS_PER_KEY {
        seed_catalog(&runtime, &version(&format!("0.240.{index}"))).await;
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    for index in 0..10 {
        assert!(load_one(&runtime, &version(&format!("0.241.{index}")))
            .await
            .snapshot(TEST_PROVIDER_ID, TEST_KEY_ID)
            .is_none());
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    let retained_lkg_versions = runtime
        .state
        .score_range_by_min(&catalog_versions_key(&target.identity), 0.0)
        .await
        .expect("read retained LKG versions");
    assert_eq!(
        retained_lkg_versions.len(),
        CODEX_CATALOG_MAX_VERSIONS_PER_KEY
    );
    for index in 0..CODEX_CATALOG_MAX_VERSIONS_PER_KEY {
        let client_version = format!("0.240.{index}");
        assert!(retained_lkg_versions.contains(&catalog_version_member(&target, &client_version)));
        assert!(runtime
            .state
            .kv_get(&catalog_lkg_key(&target, &client_version))
            .await
            .expect("read stable LKG")
            .is_some());
    }
    assert_eq!(
        runtime
            .state
            .score_range_by_min(&catalog_failed_versions_key(&target.identity), 0.0)
            .await
            .expect("read retained failed versions")
            .len(),
        CODEX_CATALOG_MAX_FAILED_VERSIONS_PER_KEY
    );
    assert_eq!(
        runtime.execution_count(),
        CODEX_CATALOG_MAX_VERSIONS_PER_KEY + 10
    );
}

#[tokio::test]
async fn concurrent_cold_versions_are_paced_per_provider_key() {
    let mut executions = vec![QueuedExecution::delayed(
        CODEX_CATALOG_FETCH_PACE_TTL + Duration::from_millis(30),
        execution_result(
            503,
            Some(json!({"error": {"message": "network unavailable"}})),
            None,
        ),
    )];
    executions.extend((1..12).map(|_| QueuedExecution::error("network unavailable")));
    let runtime = TestRuntime::new(executions).with_fetch_pace_ttl(Duration::from_secs(2));
    let versions = (0..12)
        .map(|index| version(&format!("0.230.{index}")))
        .collect::<Vec<_>>();
    futures_util::future::join_all(
        versions
            .iter()
            .map(|client_version| load_one(&runtime, client_version)),
    )
    .await;

    assert_eq!(runtime.execution_count(), 1);
    let indexed_failures = runtime
        .state
        .score_range_by_min(&catalog_failed_versions_key(&target().identity), 0.0)
        .await
        .expect("read paced failure versions");
    assert!(indexed_failures.len() <= 1);
}

#[tokio::test]
async fn cold_fetch_budget_never_returns_a_partial_catalog_as_complete() {
    let target_count = CODEX_CATALOG_MAX_COLD_FETCHES_PER_LOAD + 1;
    let runtime = TestRuntime::new(
        (0..target_count)
            .map(|index| {
                successful_execution(
                    &format!("gpt-budget-{index}"),
                    &format!("\"etag-budget-{index}\""),
                )
            })
            .collect(),
    );
    let targets = (0..target_count)
        .map(|index| target_for_key(&format!("key-budget-{index}")))
        .collect::<Vec<_>>();
    let client_version = version("0.231.0");

    let first = load_codex_catalogs(&runtime, &targets, &client_version).await;
    assert!(!first.is_complete());
    assert_eq!(
        first.snapshots.len(),
        CODEX_CATALOG_MAX_COLD_FETCHES_PER_LOAD
    );
    assert_eq!(
        runtime.execution_count(),
        CODEX_CATALOG_MAX_COLD_FETCHES_PER_LOAD
    );

    let second = load_codex_catalogs(&runtime, &targets, &client_version).await;
    assert!(second.is_complete());
    assert_eq!(second.snapshots.len(), target_count);
    assert_eq!(runtime.execution_count(), target_count);
    for target in &targets {
        assert!(second
            .snapshot(&target.identity.provider_id, &target.identity.key_id)
            .is_some());
    }
}

#[tokio::test]
async fn multiple_large_source_catalogs_cannot_exceed_the_aggregate_body_budget() {
    let oversized_pair_payload = "x".repeat(CODEX_CATALOG_MAX_AGGREGATE_BODY_BYTES / 2);
    let large_execution = |slug: &str, etag: &str| {
        let mut model = codex_model(slug);
        model["future_capability"]["large_opaque_payload"] =
            Value::String(oversized_pair_payload.clone());
        QueuedExecution::result(execution_result(
            200,
            Some(json!({"models": [model]})),
            Some(etag),
        ))
    };
    let runtime = TestRuntime::new(vec![
        large_execution("gpt-large-source-a", "\"etag-large-a\""),
        large_execution("gpt-large-source-b", "\"etag-large-b\""),
    ]);
    let targets = vec![target_for_key("key-large-a"), target_for_key("key-large-b")];

    let load = load_codex_catalogs(&runtime, &targets, &version("0.232.0")).await;

    assert!(!load.is_complete());
    assert!(load.snapshots.is_empty());
    assert!(load.stale_targets().is_empty());
    assert_eq!(runtime.execution_count(), targets.len());
}

#[tokio::test]
async fn legacy_unversioned_cache_is_never_used_as_a_versioned_catalog() {
    let runtime = TestRuntime::new(vec![QueuedExecution::error("network unavailable")]);
    runtime
        .state
        .kv_set(
            &format!("upstream_models:{TEST_PROVIDER_ID}:{TEST_KEY_ID}"),
            serde_json::to_string(&vec![codex_model("gpt-legacy-cache")])
                .expect("serialize legacy cache"),
            None,
        )
        .await
        .expect("seed legacy cache");

    let load = load_one(&runtime, &version(&codex_client_version())).await;
    assert!(load.snapshot(TEST_PROVIDER_ID, TEST_KEY_ID).is_none());
    assert_eq!(runtime.execution_count(), 1);
}

#[tokio::test]
async fn recent_version_tracks_normalized_last_seen_version_even_after_cold_failure() {
    let runtime = TestRuntime::new(vec![QueuedExecution::error("network unavailable")]);
    let client_version = version("0.211.0-beta.4+desktop.9");

    let load = load_one(&runtime, &client_version).await;
    assert!(load.snapshot(TEST_PROVIDER_ID, TEST_KEY_ID).is_none());
    assert_eq!(client_version.as_str(), "0.211.0");
    assert_eq!(
        read_recent_codex_catalog_client_version(
            &runtime.state,
            TEST_PROVIDER_ID,
            TEST_KEY_ID,
            target().credential_scope().expect("test credential scope"),
        )
        .await
        .as_deref(),
        Some("0.211.0")
    );
}

#[tokio::test]
async fn invalid_version_fallback_does_not_overwrite_recent_real_client_version() {
    let runtime = TestRuntime::new(vec![
        successful_execution("gpt-real-version", "\"etag-real\""),
        successful_execution("gpt-fallback-version", "\"etag-fallback\""),
    ]);
    seed_catalog(&runtime, &version("0.212.3")).await;
    tokio::time::sleep(Duration::from_millis(25)).await;

    let fallback = normalize_codex_client_version(Some("malicious-not-semver"));
    assert!(fallback.used_fallback());
    seed_catalog(&runtime, &fallback).await;

    assert_eq!(
        read_recent_codex_catalog_client_version(
            &runtime.state,
            TEST_PROVIDER_ID,
            TEST_KEY_ID,
            target().credential_scope().expect("test credential scope"),
        )
        .await
        .as_deref(),
        Some("0.212.3")
    );
}
