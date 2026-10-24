use std::time::{Duration, SystemTime, UNIX_EPOCH};

use super::{
    hash_api_key, sample_endpoint, sample_key, sample_models_candidate_row, sample_provider,
    sample_public_catalog_model, sample_public_global_model,
    sample_public_global_model_with_capabilities, sample_request_candidate,
    InMemoryAnnouncementReadRepository, InMemoryGlobalModelReadRepository,
    InMemoryMinimalCandidateSelectionReadRepository, InMemoryProviderCatalogReadRepository,
    InMemoryRequestCandidateRepository, RequestCandidateStatus, StoredAnnouncement,
    StoredPublicGlobalModel,
};
use crate::data::GatewayDataState;
use crate::tests::{
    any, build_router, build_router_with_state, json, start_server, to_bytes, AppState, Arc, Body,
    Json, Mutex, Request, Router, StatusCode, CONTROL_ROUTE_FAMILY_HEADER,
    CONTROL_ROUTE_KIND_HEADER, TRUSTED_ADMIN_SESSION_ID_HEADER, TRUSTED_ADMIN_USER_ID_HEADER,
    TRUSTED_ADMIN_USER_ROLE_HEADER,
};
use aether_crypto::{encrypt_python_fernet_plaintext, DEVELOPMENT_ENCRYPTION_KEY};
use aether_data::repository::announcements::{AnnouncementListQuery, AnnouncementReadRepository};
use aether_data::repository::auth::{
    InMemoryAuthApiKeySnapshotRepository, StoredAuthApiKeyExportRecord, StoredAuthApiKeySnapshot,
};
use aether_data::repository::auth_modules::{
    InMemoryAuthModuleReadRepository, StoredLdapModuleConfig, StoredOAuthProviderModuleConfig,
};
use aether_data::repository::billing::InMemoryBillingReadRepository;
use aether_data::repository::management_tokens::{
    InMemoryManagementTokenRepository, ManagementTokenReadRepository, StoredManagementToken,
    StoredManagementTokenUserSummary, StoredManagementTokenWithUser,
};
use aether_data::repository::usage::InMemoryUsageReadRepository;
use aether_data::repository::users::{
    InMemoryUserReadRepository, StoredUserAuthRecord, StoredUserExportRow, UpsertUserGroupRecord,
    UserReadRepository,
};
use aether_data::repository::wallet::{
    InMemoryWalletRepository, StoredWalletSnapshot, WalletWriteRepository,
};
use aether_data_contracts::repository::billing::{
    AdminBillingMutationOutcome, BillingPlanWriteInput, BillingReadRepository,
    PaymentGatewayConfigWriteInput,
};
use aether_data_contracts::repository::global_models::StoredProviderActiveGlobalModel;
use aether_data_contracts::repository::provider_catalog::ProviderCatalogReadRepository;
use aether_data_contracts::repository::usage::{StoredRequestUsageAudit, UsageRepository};
use axum::response::IntoResponse;
use chrono::{TimeZone, Utc};

const TEST_EMAIL_VERIFICATION_TOKEN: &str =
    "test-email-verification-token-00000000000000000000000000000000";

mod announcement_user_list;
mod announcements;
mod api_key_routing;
mod auth;
mod auth_cookie;
mod catalog;
mod dashboard;
mod management_tokens;
mod misc;
mod monitoring;
mod routing_groups;
mod wallet;

async fn assert_local_route_not_found_response(response: reqwest::Response) {
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["error"]["type"], "http_error");
    assert_eq!(payload["error"]["message"], "Route not found");
}

fn test_auth_secret() -> String {
    std::env::var("JWT_SECRET_KEY")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "aether-rust-test-jwt-secret-32-bytes-minimum".to_string())
}

fn test_base64url_encode(bytes: &[u8]) -> String {
    use base64::Engine as _;

    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
}

fn build_test_auth_token(
    token_type: &str,
    mut payload: serde_json::Map<String, serde_json::Value>,
    expires_at: chrono::DateTime<chrono::Utc>,
) -> String {
    use hmac::Mac;

    let header = json!({ "alg": "HS256", "typ": "JWT" });
    payload.insert("exp".to_string(), json!(expires_at.timestamp()));
    payload.insert("type".to_string(), json!(token_type));
    let header_segment = test_base64url_encode(
        serde_json::to_vec(&header)
            .expect("jwt header should serialize")
            .as_slice(),
    );
    let payload_segment = test_base64url_encode(
        serde_json::to_vec(&payload)
            .expect("jwt payload should serialize")
            .as_slice(),
    );
    let signing_input = format!("{header_segment}.{payload_segment}");
    let mut mac = hmac::Hmac::<sha2::Sha256>::new_from_slice(test_auth_secret().as_bytes())
        .expect("jwt secret should build");
    mac.update(signing_input.as_bytes());
    let signature = mac.finalize().into_bytes();
    format!(
        "{header_segment}.{payload_segment}.{}",
        test_base64url_encode(signature.as_slice())
    )
}

struct TestEnvVarGuard {
    key: &'static str,
    previous: Option<String>,
}

impl Drop for TestEnvVarGuard {
    fn drop(&mut self) {
        if let Some(previous) = self.previous.as_deref() {
            std::env::set_var(self.key, previous);
        } else {
            std::env::remove_var(self.key);
        }
    }
}

fn set_test_env_var(key: &'static str, value: &str) -> TestEnvVarGuard {
    let previous = std::env::var(key).ok();
    std::env::set_var(key, value);
    TestEnvVarGuard { key, previous }
}

#[cfg(test)]
fn payment_callback_env_lock() -> &'static tokio::sync::Mutex<()> {
    static LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
    &LOCK
}

const TEST_PAYMENT_CALLBACK_SECRET: &str = "test-callback-secret-0123456789abcdef";

fn canonicalize_test_json(value: &serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Object(map) => {
            let mut entries = map.iter().collect::<Vec<_>>();
            entries.sort_by(|left, right| left.0.cmp(right.0));
            let mut object = serde_json::Map::new();
            for (key, value) in entries {
                object.insert(key.clone(), canonicalize_test_json(value));
            }
            serde_json::Value::Object(object)
        }
        serde_json::Value::Array(items) => {
            serde_json::Value::Array(items.iter().map(canonicalize_test_json).collect())
        }
        _ => value.clone(),
    }
}

fn build_test_payment_callback_signature(payload: &serde_json::Value, secret: &str) -> String {
    use hmac::Mac;

    let canonical = serde_json::to_string(&canonicalize_test_json(payload))
        .expect("payment callback payload should serialize");
    let mut mac = hmac::Hmac::<sha2::Sha256>::new_from_slice(secret.as_bytes())
        .expect("payment callback secret should build");
    mac.update(canonical.as_bytes());
    let signature = mac.finalize().into_bytes();
    signature.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn sample_auth_user(now: chrono::DateTime<chrono::Utc>) -> StoredUserAuthRecord {
    StoredUserAuthRecord::new(
        "user-auth-1".to_string(),
        Some("alice@example.com".to_string()),
        true,
        "alice".to_string(),
        Some("$2y$10$.OBQfixAECpsb8V/VS3csOMf00x2E/jD/gnud20t6RG0yiQosyOZ2".to_string()),
        "user".to_string(),
        "local".to_string(),
        Some(json!(["openai"])),
        Some(json!(["openai:chat"])),
        Some(json!(["gpt-5"])),
        true,
        false,
        Some(now),
        Some(now),
    )
    .expect("auth user should build")
}

fn sample_users_me_management_token(
    token_id: &str,
    user_id: &str,
    username: &str,
    is_active: bool,
) -> StoredManagementTokenWithUser {
    let token = StoredManagementToken::new(
        token_id.to_string(),
        user_id.to_string(),
        format!("{username}-token"),
    )
    .expect("management token should build")
    .with_display_fields(
        Some(format!("{username} token")),
        Some("ae_test".to_string()),
        Some(json!(["127.0.0.1"])),
    )
    .with_runtime_fields(
        Some(4_102_444_800),
        Some(1_711_000_000),
        Some("127.0.0.1".to_string()),
        7,
        is_active,
    )
    .with_timestamps(Some(1_710_000_000), Some(1_711_000_100));
    let user = StoredManagementTokenUserSummary::new(
        user_id.to_string(),
        Some(format!("{username}@example.com")),
        username.to_string(),
        "user".to_string(),
    )
    .expect("management token user should build");
    StoredManagementTokenWithUser::new(token, user)
}

fn sample_auth_wallet(user_id: &str, now: chrono::DateTime<chrono::Utc>) -> StoredWalletSnapshot {
    StoredWalletSnapshot::new(
        "wallet-auth-1".to_string(),
        Some(user_id.to_string()),
        None,
        12.5,
        3.0,
        "finite".to_string(),
        "USD".to_string(),
        "active".to_string(),
        20.0,
        4.5,
        0.0,
        0.0,
        now.timestamp(),
    )
    .expect("wallet should build")
}

fn sample_standalone_auth_wallet(
    api_key_id: &str,
    now: chrono::DateTime<chrono::Utc>,
) -> StoredWalletSnapshot {
    StoredWalletSnapshot::new(
        "wallet-standalone-1".to_string(),
        None,
        Some(api_key_id.to_string()),
        2.0,
        0.5,
        "finite".to_string(),
        "USD".to_string(),
        "active".to_string(),
        5.0,
        2.5,
        0.0,
        0.0,
        now.timestamp(),
    )
    .expect("standalone wallet should build")
}

fn wallet_today_usage_test_time() -> chrono::DateTime<chrono::Utc> {
    let offset =
        chrono::FixedOffset::east_opt(8 * 3600).expect("Asia/Shanghai test offset should be valid");
    let local_today = Utc::now().with_timezone(&offset).date_naive();
    let local_noon = local_today
        .and_hms_opt(12, 0, 0)
        .expect("wallet today test noon should be valid");
    offset
        .from_local_datetime(&local_noon)
        .single()
        .expect("fixed offset local noon should be unambiguous")
        .with_timezone(&Utc)
}

fn sample_auth_session(
    user_id: &str,
    session_id: &str,
    client_device_id: &str,
    refresh_token: &str,
    now: chrono::DateTime<chrono::Utc>,
) -> crate::data::state::StoredUserSessionRecord {
    crate::data::state::StoredUserSessionRecord::new(
        session_id.to_string(),
        user_id.to_string(),
        client_device_id.to_string(),
        None,
        crate::data::state::StoredUserSessionRecord::hash_refresh_token(refresh_token),
        None,
        None,
        Some(now),
        Some(now + chrono::Duration::days(7)),
        None,
        None,
        Some("127.0.0.1".to_string()),
        Some("AetherTest/1.0".to_string()),
        Some(now),
        Some(now),
    )
    .expect("auth session should build")
}

fn sample_usage_auth_snapshot(
    api_key_id: &str,
    user_id: &str,
    api_key_name: &str,
) -> StoredAuthApiKeySnapshot {
    StoredAuthApiKeySnapshot::new(
        user_id.to_string(),
        "alice".to_string(),
        Some("alice@example.com".to_string()),
        "user".to_string(),
        "local".to_string(),
        true,
        false,
        Some(json!(["openai"])),
        Some(json!(["openai:chat"])),
        Some(json!(["gpt-5"])),
        api_key_id.to_string(),
        Some(api_key_name.to_string()),
        true,
        false,
        false,
        Some(60),
        Some(5),
        None,
        Some(json!(["openai"])),
        Some(json!(["openai:chat"])),
        Some(json!(["gpt-5"])),
    )
    .expect("auth api key snapshot should build")
}

async fn start_auth_gateway_with_state(
    user: StoredUserAuthRecord,
    wallet: StoredWalletSnapshot,
    sessions: impl IntoIterator<Item = crate::data::state::StoredUserSessionRecord>,
) -> (
    String,
    Arc<Mutex<usize>>,
    tokio::task::JoinHandle<()>,
    tokio::task::JoinHandle<()>,
) {
    let upstream_hits = Arc::new(Mutex::new(0usize));
    let upstream_hits_clone = Arc::clone(&upstream_hits);
    let upstream = Router::new().route(
        "/{*path}",
        any(move |_request: Request| {
            let upstream_hits_inner = Arc::clone(&upstream_hits_clone);
            async move {
                *upstream_hits_inner.lock().expect("mutex should lock") += 1;
                (StatusCode::OK, Body::from("proxied"))
            }
        }),
    );

    let (_upstream_url, upstream_handle) = start_server(upstream).await;
    let user_repository = Arc::new(InMemoryUserReadRepository::seed_auth_users(vec![user]));
    let wallet_repository = Arc::new(InMemoryWalletRepository::seed(vec![wallet]));
    let state = AppState::new()
        .expect("gateway should build")
        .with_data_state_for_tests(
            crate::data::GatewayDataState::with_user_and_wallet_for_tests(
                user_repository,
                wallet_repository,
            ),
        )
        .with_auth_sessions_for_tests(sessions);
    let gateway = build_router_with_state(state);
    let (gateway_url, gateway_handle) = start_server(gateway).await;
    (gateway_url, upstream_hits, gateway_handle, upstream_handle)
}

async fn start_auth_gateway_with_usage_state<T>(
    user: StoredUserAuthRecord,
    wallet: StoredWalletSnapshot,
    sessions: impl IntoIterator<Item = crate::data::state::StoredUserSessionRecord>,
    usage_repository: Arc<T>,
) -> (
    String,
    Arc<Mutex<usize>>,
    tokio::task::JoinHandle<()>,
    tokio::task::JoinHandle<()>,
)
where
    T: UsageRepository + 'static,
{
    let upstream_hits = Arc::new(Mutex::new(0usize));
    let upstream_hits_clone = Arc::clone(&upstream_hits);
    let upstream = Router::new().route(
        "/{*path}",
        any(move |_request: Request| {
            let upstream_hits_inner = Arc::clone(&upstream_hits_clone);
            async move {
                *upstream_hits_inner.lock().expect("mutex should lock") += 1;
                (StatusCode::OK, Body::from("proxied"))
            }
        }),
    );

    let (_upstream_url, upstream_handle) = start_server(upstream).await;
    let user_repository = Arc::new(InMemoryUserReadRepository::seed_auth_users(vec![user]));
    let wallet_repository = Arc::new(InMemoryWalletRepository::seed(vec![wallet]));
    let state = AppState::new()
        .expect("gateway should build")
        .with_data_state_for_tests(
            crate::data::GatewayDataState::with_user_wallet_and_usage_for_tests(
                user_repository,
                wallet_repository,
                usage_repository,
            ),
        )
        .with_auth_sessions_for_tests(sessions);
    let gateway = build_router_with_state(state);
    let (gateway_url, gateway_handle) = start_server(gateway).await;
    (gateway_url, upstream_hits, gateway_handle, upstream_handle)
}

async fn start_auth_dashboard_gateway_with_state<TUsage, TCatalog>(
    user: StoredUserAuthRecord,
    wallet: StoredWalletSnapshot,
    sessions: impl IntoIterator<Item = crate::data::state::StoredUserSessionRecord>,
    usage_repository: Arc<TUsage>,
    provider_catalog_repository: Arc<TCatalog>,
) -> (
    String,
    Arc<Mutex<usize>>,
    tokio::task::JoinHandle<()>,
    tokio::task::JoinHandle<()>,
)
where
    TUsage: UsageRepository + 'static,
    TCatalog: ProviderCatalogReadRepository + 'static,
{
    let upstream_hits = Arc::new(Mutex::new(0usize));
    let upstream_hits_clone = Arc::clone(&upstream_hits);
    let upstream = Router::new().route(
        "/{*path}",
        any(move |_request: Request| {
            let upstream_hits_inner = Arc::clone(&upstream_hits_clone);
            async move {
                *upstream_hits_inner.lock().expect("mutex should lock") += 1;
                (StatusCode::OK, Body::from("proxied"))
            }
        }),
    );

    let (_upstream_url, upstream_handle) = start_server(upstream).await;
    let user_repository = Arc::new(InMemoryUserReadRepository::seed_auth_users(vec![user]));
    let wallet_repository = Arc::new(InMemoryWalletRepository::seed(vec![wallet]));
    let data_state = crate::data::GatewayDataState::with_user_wallet_and_usage_for_tests(
        user_repository,
        wallet_repository,
        usage_repository,
    )
    .with_provider_catalog_reader(provider_catalog_repository);
    let state = AppState::new()
        .expect("gateway should build")
        .with_data_state_for_tests(data_state)
        .with_auth_sessions_for_tests(sessions);
    let gateway = build_router_with_state(state);
    let (gateway_url, gateway_handle) = start_server(gateway).await;
    (gateway_url, upstream_hits, gateway_handle, upstream_handle)
}

async fn start_auth_gateway_with_preferences_state(
    user: StoredUserAuthRecord,
    wallet: StoredWalletSnapshot,
    sessions: impl IntoIterator<Item = crate::data::state::StoredUserSessionRecord>,
    preferences: impl IntoIterator<Item = crate::data::state::StoredUserPreferenceRecord>,
) -> (
    String,
    Arc<Mutex<usize>>,
    tokio::task::JoinHandle<()>,
    tokio::task::JoinHandle<()>,
) {
    let upstream_hits = Arc::new(Mutex::new(0usize));
    let upstream_hits_clone = Arc::clone(&upstream_hits);
    let upstream = Router::new().route(
        "/{*path}",
        any(move |_request: Request| {
            let upstream_hits_inner = Arc::clone(&upstream_hits_clone);
            async move {
                *upstream_hits_inner.lock().expect("mutex should lock") += 1;
                (StatusCode::OK, Body::from("proxied"))
            }
        }),
    );

    let (_upstream_url, upstream_handle) = start_server(upstream).await;
    let user_repository = Arc::new(InMemoryUserReadRepository::seed_auth_users(vec![user]));
    let wallet_repository = Arc::new(InMemoryWalletRepository::seed(vec![wallet]));
    let state = AppState::new()
        .expect("gateway should build")
        .with_data_state_for_tests(
            crate::data::GatewayDataState::with_user_and_wallet_for_tests(
                user_repository,
                wallet_repository,
            )
            .with_user_preferences_for_tests(preferences),
        )
        .with_auth_sessions_for_tests(sessions);
    let gateway = build_router_with_state(state);
    let (gateway_url, gateway_handle) = start_server(gateway).await;
    (gateway_url, upstream_hits, gateway_handle, upstream_handle)
}

fn sample_user_usage_audit(
    id: &str,
    request_id: &str,
    user_id: &str,
    model: &str,
    provider_name: &str,
    status: &str,
    created_at: chrono::DateTime<chrono::Utc>,
) -> StoredRequestUsageAudit {
    let mut usage = StoredRequestUsageAudit::new(
        id.to_string(),
        request_id.to_string(),
        Some(user_id.to_string()),
        Some("api-key-user-1".to_string()),
        Some("alice".to_string()),
        Some("default".to_string()),
        provider_name.to_string(),
        model.to_string(),
        Some(format!("{model}-target")),
        Some("provider-1".to_string()),
        Some("endpoint-1".to_string()),
        Some("provider-key-1".to_string()),
        Some("chat".to_string()),
        Some("openai:chat".to_string()),
        Some("openai".to_string()),
        Some("chat".to_string()),
        Some("openai:chat".to_string()),
        Some("openai".to_string()),
        Some("chat".to_string()),
        false,
        status == "streaming",
        120,
        30,
        150,
        1.25,
        1.25,
        Some(if status == "failed" { 500 } else { 200 }),
        if status == "failed" {
            Some("upstream failed".to_string())
        } else {
            None
        },
        None,
        Some(420),
        Some(120),
        status.to_string(),
        if matches!(status, "completed" | "failed" | "cancelled") {
            "settled".to_string()
        } else {
            "pending".to_string()
        },
        created_at.timestamp(),
        created_at.timestamp() + 1,
        Some(created_at.timestamp() + 2),
    )
    .expect("usage should build");
    usage.cache_creation_input_tokens = 10;
    usage.cache_creation_ephemeral_5m_input_tokens = 4;
    usage.cache_creation_ephemeral_1h_input_tokens = 6;
    usage.cache_read_input_tokens = 15;
    usage.cache_creation_cost_usd = 0.05;
    usage.cache_read_cost_usd = 0.02;
    usage
}

async fn start_auth_announcement_gateway_with_state<T>(
    user: StoredUserAuthRecord,
    wallet: StoredWalletSnapshot,
    sessions: impl IntoIterator<Item = crate::data::state::StoredUserSessionRecord>,
    announcement_repository: Arc<T>,
) -> (
    String,
    Arc<Mutex<usize>>,
    tokio::task::JoinHandle<()>,
    tokio::task::JoinHandle<()>,
)
where
    T: AnnouncementReadRepository
        + aether_data::repository::announcements::AnnouncementWriteRepository
        + 'static,
{
    let upstream_hits = Arc::new(Mutex::new(0usize));
    let upstream_hits_clone = Arc::clone(&upstream_hits);
    let upstream = Router::new().route(
        "/{*path}",
        any(move |_request: Request| {
            let upstream_hits_inner = Arc::clone(&upstream_hits_clone);
            async move {
                *upstream_hits_inner.lock().expect("mutex should lock") += 1;
                (StatusCode::OK, Body::from("proxied"))
            }
        }),
    );

    let (_upstream_url, upstream_handle) = start_server(upstream).await;
    let user_repository = Arc::new(InMemoryUserReadRepository::seed_auth_users(vec![user]));
    let wallet_repository = Arc::new(InMemoryWalletRepository::seed(vec![wallet]));
    let state = AppState::new()
        .expect("gateway should build")
        .with_data_state_for_tests(
            crate::data::GatewayDataState::with_announcement_user_and_wallet_for_tests(
                announcement_repository,
                user_repository,
                wallet_repository,
            ),
        )
        .with_auth_sessions_for_tests(sessions);
    let gateway = build_router_with_state(state);
    let (gateway_url, gateway_handle) = start_server(gateway).await;
    (gateway_url, upstream_hits, gateway_handle, upstream_handle)
}

async fn start_auth_gateway_with_builder<F>(
    build_state: F,
) -> (
    String,
    Arc<Mutex<usize>>,
    tokio::task::JoinHandle<()>,
    tokio::task::JoinHandle<()>,
)
where
    F: FnOnce() -> AppState,
{
    let upstream_hits = Arc::new(Mutex::new(0usize));
    let upstream_hits_clone = Arc::clone(&upstream_hits);
    let upstream = Router::new().route(
        "/{*path}",
        any(move |_request: Request| {
            let upstream_hits_inner = Arc::clone(&upstream_hits_clone);
            async move {
                *upstream_hits_inner.lock().expect("mutex should lock") += 1;
                (StatusCode::OK, Body::from("proxied"))
            }
        }),
    );
    let (_unused_fallback_probe_url, upstream_handle) = start_server(upstream).await;
    let gateway = build_router_with_state(build_state());
    let (gateway_url, gateway_handle) = start_server(gateway).await;
    (gateway_url, upstream_hits, gateway_handle, upstream_handle)
}

async fn start_turnstile_siteverify_server(
    response_payload: serde_json::Value,
    status: StatusCode,
) -> (
    String,
    Arc<Mutex<Vec<std::collections::HashMap<String, String>>>>,
    tokio::task::JoinHandle<()>,
) {
    start_turnstile_siteverify_server_with_delay(response_payload, status, None).await
}

async fn start_turnstile_siteverify_server_with_delay(
    response_payload: serde_json::Value,
    status: StatusCode,
    delay: Option<Duration>,
) -> (
    String,
    Arc<Mutex<Vec<std::collections::HashMap<String, String>>>>,
    tokio::task::JoinHandle<()>,
) {
    let requests = Arc::new(Mutex::new(Vec::new()));
    let requests_clone = Arc::clone(&requests);
    let upstream = Router::new().route(
        "/turnstile/siteverify",
        any(
            move |axum::extract::Form(form): axum::extract::Form<
                std::collections::HashMap<String, String>,
            >| {
                let requests_inner = Arc::clone(&requests_clone);
                let response_payload = response_payload.clone();
                async move {
                    if let Some(delay) = delay {
                        tokio::time::sleep(delay).await;
                    }
                    requests_inner
                        .lock()
                        .expect("turnstile requests should lock")
                        .push(form);
                    (status, Json(response_payload))
                }
            },
        ),
    );
    let (url, handle) = start_server(upstream).await;
    (format!("{url}/turnstile/siteverify"), requests, handle)
}

fn turnstile_enabled_data_state() -> crate::data::GatewayDataState {
    crate::data::GatewayDataState::disabled().with_system_config_values_for_tests(vec![
        ("enable_registration".to_string(), json!(true)),
        ("require_email_verification".to_string(), json!(true)),
        ("smtp_host".to_string(), json!("smtp.example.com")),
        ("smtp_from_email".to_string(), json!("ops@example.com")),
        ("default_user_initial_gift_usd".to_string(), json!(12.5)),
        ("turnstile_enabled".to_string(), json!(true)),
        ("turnstile_site_key".to_string(), json!("site-public-key")),
        (
            "turnstile_secret_key".to_string(),
            json!("secret-private-key"),
        ),
        (
            "turnstile_allowed_hostnames".to_string(),
            json!(["gateway.example.com"]),
        ),
    ])
}
