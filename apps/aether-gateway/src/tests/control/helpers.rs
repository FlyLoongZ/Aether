use aether_crypto::encrypt_python_fernet_plaintext;
use serde_json::{json, Value};

use super::{
    RequestCandidateStatus, StoredAdminGlobalModel, StoredAdminProviderModel,
    StoredLdapModuleConfig, StoredManagementToken, StoredManagementTokenUserSummary,
    StoredManagementTokenWithUser, StoredOAuthProviderConfig, StoredOAuthProviderModuleConfig,
    StoredProviderActiveGlobalModel, StoredProviderCatalogEndpoint, StoredProviderCatalogKey,
    StoredProviderModelStats, StoredProviderQuotaSnapshot, StoredProxyNode,
    StoredPublicGlobalModel, StoredRequestCandidate,
};
use crate::tests::DEV_FERNET_KEY;
use crate::AppState;
use std::sync::{Arc, Mutex};

pub(super) const PROXY_NODE_TEST_GENERATION: &str = "test-generation-1";

pub(super) use crate::tests::support::{
    build_test_auth_token, sample_key, sample_provider, sample_request_candidate,
};

pub(super) use crate::tests::hash_api_key;
pub(super) use crate::tests::support::sample_currently_usable_auth_snapshot;

pub(super) fn hash_management_token(value: &str) -> String {
    hash_api_key(value)
}

pub(in crate::tests) async fn issue_test_admin_access_token(
    state: &AppState,
    client_device_id: &str,
) -> String {
    let user = state
        .create_local_auth_user_with_settings(
            Some("admin@example.com".to_string()),
            true,
            "admin".to_string(),
            "hash".to_string(),
            "admin".to_string(),
            None,
            None,
            None,
            None,
        )
        .await
        .expect("admin user should be created")
        .expect("admin user should exist");
    let now = chrono::Utc::now();
    let session_id = "session-admin-token".to_string();
    let refresh_token = "refresh-session-admin-token".to_string();
    let session = crate::data::state::StoredUserSessionRecord::new(
        session_id.clone(),
        user.id.clone(),
        client_device_id.to_string(),
        None,
        crate::data::state::StoredUserSessionRecord::hash_refresh_token(&refresh_token),
        None,
        None,
        Some(now),
        Some(now + chrono::Duration::days(7)),
        None,
        None,
        Some("127.0.0.1".to_string()),
        Some("admin-test".to_string()),
        Some(now),
        Some(now),
    )
    .expect("session should build");
    state
        .create_user_session(session)
        .await
        .expect("session should persist")
        .expect("session should exist");
    build_test_auth_token(
        "access",
        serde_json::Map::from_iter([
            ("user_id".to_string(), json!(user.id)),
            ("role".to_string(), json!("admin")),
            (
                "created_at".to_string(),
                json!(user.created_at.map(|value| value.to_rfc3339())),
            ),
            ("session_id".to_string(), json!(session_id)),
        ]),
        now + chrono::Duration::hours(12),
    )
}

pub(super) fn sample_proxy_node(node_id: &str) -> StoredProxyNode {
    StoredProxyNode::new(
        node_id.to_string(),
        "proxy-node-1".to_string(),
        "127.0.0.1".to_string(),
        0,
        "online".to_string(),
    )
    .expect("proxy node should build")
    .with_manual_proxy_fields(Some("http://127.0.0.1:1".to_string()), None, None)
    .with_runtime_fields(
        Some("test".to_string()),
        Some("system".to_string()),
        Some(1_710_000_000),
        None,
        Some(1_709_000_000),
        Some(1_710_000_100),
    )
    .with_node_generation(PROXY_NODE_TEST_GENERATION.to_string())
}

pub(super) fn sample_provider_quota(provider_id: &str) -> StoredProviderQuotaSnapshot {
    StoredProviderQuotaSnapshot::new(
        provider_id.to_string(),
        "monthly_quota".to_string(),
        Some(100.0),
        12.5,
        Some(30),
        Some(1_711_000_000),
        Some(1_711_000_000 + 30 * 24 * 60 * 60),
        true,
    )
    .expect("provider quota should build")
}

pub(super) fn sample_provider_model_stats(
    provider_id: &str,
    total_models: i64,
    active_models: i64,
) -> StoredProviderModelStats {
    StoredProviderModelStats::new(provider_id.to_string(), total_models, active_models)
        .expect("provider model stats should build")
}

pub(super) fn sample_provider_active_global_model(
    provider_id: &str,
    global_model_id: &str,
) -> StoredProviderActiveGlobalModel {
    StoredProviderActiveGlobalModel::new(provider_id.to_string(), global_model_id.to_string())
        .expect("provider active global model should build")
}

pub(super) fn sample_management_token(
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
        "admin".to_string(),
    )
    .expect("management token user should build");
    StoredManagementTokenWithUser::new(token, user)
}

pub(super) fn sample_admin_provider_model(
    id: &str,
    provider_id: &str,
    global_model_id: &str,
    provider_model_name: &str,
) -> StoredAdminProviderModel {
    StoredAdminProviderModel::new(
        id.to_string(),
        provider_id.to_string(),
        global_model_id.to_string(),
        provider_model_name.to_string(),
        Some(json!([{"name": format!("{provider_model_name}-alias"), "priority": 1}])),
        Some(0.02),
        Some(json!({
            "tiers": [{
                "up_to": null,
                "input_price_per_1m": 3.0,
                "output_price_per_1m": 15.0,
            }]
        })),
        Some(true),
        Some(true),
        None,
        Some(false),
        Some(false),
        true,
        true,
        Some(json!({"billing": {"mode": "local"}, "provider_hint": provider_model_name})),
        Some(1_711_000_000),
        Some(1_711_000_100),
        Some("gpt-5".to_string()),
        Some("GPT 5".to_string()),
        Some(0.03),
        Some(json!({
            "tiers": [{
                "up_to": null,
                "input_price_per_1m": 4.0,
                "output_price_per_1m": 20.0,
            }]
        })),
        Some(json!(["streaming", "vision"])),
        Some(json!({"streaming": true, "vision": false, "billing": {"currency": "USD"}})),
    )
    .expect("admin provider model should build")
}

pub(super) fn sample_admin_global_model(
    id: &str,
    name: &str,
    display_name: &str,
) -> StoredAdminGlobalModel {
    StoredAdminGlobalModel::new(
        id.to_string(),
        name.to_string(),
        display_name.to_string(),
        true,
        Some(0.03),
        Some(json!({
            "tiers": [{
                "up_to": null,
                "input_price_per_1m": 4.0,
                "output_price_per_1m": 20.0,
            }]
        })),
        Some(json!(["streaming", "vision"])),
        Some(json!({"streaming": true, "vision": false, "billing": {"currency": "USD"}})),
        0,
        0,
        0,
        Some(1_711_000_000),
        Some(1_711_000_100),
    )
    .expect("admin global model should build")
}

pub(super) fn sample_public_global_model_with_mappings(
    id: &str,
    name: &str,
    display_name: &str,
    mappings: &[&str],
) -> StoredPublicGlobalModel {
    StoredPublicGlobalModel::new(
        id.to_string(),
        name.to_string(),
        Some(display_name.to_string()),
        true,
        None,
        None,
        None,
        Some(json!({ "model_mappings": mappings })),
        0,
    )
    .expect("public global model should build")
}

pub(super) fn sample_oauth_module_provider(
    provider_type: &str,
    display_name: &str,
) -> StoredOAuthProviderModuleConfig {
    StoredOAuthProviderModuleConfig::new(
        provider_type.to_string(),
        display_name.to_string(),
        "client-id".to_string(),
        Some("encrypted-secret".to_string()),
        "https://example.com/oauth/callback".to_string(),
    )
    .expect("oauth module provider should build")
}

pub(super) fn sample_ldap_module_config() -> StoredLdapModuleConfig {
    StoredLdapModuleConfig {
        server_url: "ldaps://ldap.example.com".to_string(),
        bind_dn: "cn=admin,dc=example,dc=com".to_string(),
        bind_password_encrypted: Some("encrypted-password".to_string()),
        base_dn: "dc=example,dc=com".to_string(),
        user_search_filter: Some("(uid={username})".to_string()),
        username_attr: Some("uid".to_string()),
        email_attr: Some("mail".to_string()),
        display_name_attr: Some("displayName".to_string()),
        is_enabled: true,
        is_exclusive: false,
        use_starttls: true,
        connect_timeout: Some(10),
    }
}

pub(super) fn sample_oauth_provider_config(provider_type: &str) -> StoredOAuthProviderConfig {
    StoredOAuthProviderConfig::new(
        provider_type.to_string(),
        "Linux Do".to_string(),
        "client-id".to_string(),
        "https://backend.example.com/oauth/callback".to_string(),
        "https://frontend.example.com/auth/callback".to_string(),
    )
    .expect("oauth provider config should build")
    .with_config_fields(
        Some(
            encrypt_python_fernet_plaintext(DEV_FERNET_KEY, "secret-value")
                .expect("secret should encrypt"),
        ),
        Some("https://connect.linux.do/oauth2/authorize".to_string()),
        Some("https://connect.linux.do/oauth2/token".to_string()),
        Some("https://connect.linux.do/api/user".to_string()),
        Some(vec!["openid".to_string()]),
        Some(json!({"email": "email"})),
        Some(json!({"team": true})),
        None,
        true,
    )
}

pub(super) fn sample_endpoint(
    id: &str,
    provider_id: &str,
    api_format: &str,
    base_url: &str,
) -> StoredProviderCatalogEndpoint {
    crate::tests::support::sample_endpoint_with_health_score(
        id,
        provider_id,
        api_format,
        base_url,
        Some(0.9),
    )
}

/// Build a provider catalog key using the same provider/key-bound v2 envelope
/// that production writes use.  Most control-plane tests intentionally mount
/// a read-only catalog repository; using a legacy Fernet fixture there would
/// force the reader to migrate the credential and fail closed when no writer
/// is available.  Keep `sample_key` for tests that explicitly exercise the
/// legacy migration path, and use this helper for ordinary catalog fixtures.
pub(super) fn sample_bound_key(
    id: &str,
    provider_id: &str,
    api_format: &str,
    secret: &str,
) -> StoredProviderCatalogKey {
    let bootstrap = crate::tests::support::credential_fixture_state();
    let mut key = sample_key(id, provider_id, api_format, secret);
    key.encrypted_api_key = Some(
        bootstrap
            .seal_provider_catalog_key_api_key(provider_id, id, secret)
            .expect("bound provider api key ciphertext should build"),
    );
    key
}

/// Build the provider-scoped proxy representation used by the catalog reader.
/// Stored proxy credentials are record-bound before they are accepted by
/// read-only repositories, so fixtures must use the same envelope.
pub(super) fn sample_bound_provider_proxy(provider_id: &str, host: &str, password: &str) -> Value {
    let bootstrap = crate::tests::support::credential_fixture_state();
    let purpose = format!(
        "provider-catalog-proxy-credential-v2\0scope=provider\0field=password\0record-id-bytes={}\0{provider_id}",
        provider_id.len(),
    );
    let sealed =
        crate::handlers::shared::seal_runtime_secret_payload(&bootstrap, &purpose, password)
            .expect("provider proxy password should seal");
    json!({
        "host": host,
        "password": format!("aether-provider-catalog-proxy-secret-v2:{sealed}"),
    })
}

/// Seal an auth-config fixture with the provider/key-bound v2 envelope.
/// Ordinary read-only catalog tests must not rely on the migration writer.
pub(super) fn sample_bound_auth_config(provider_id: &str, key_id: &str, plaintext: &str) -> String {
    let bootstrap = crate::tests::support::credential_fixture_state();
    bootstrap
        .seal_provider_catalog_key_auth_config(provider_id, key_id, plaintext)
        .expect("bound provider auth config ciphertext should build")
}

pub(super) fn sample_recent_key_rpm_candidate(
    id: &str,
    request_id: &str,
    endpoint_id: &str,
    key_id: &str,
    now_unix_secs: i64,
    concurrent_requests: i32,
) -> StoredRequestCandidate {
    StoredRequestCandidate::new(
        id.to_string(),
        request_id.to_string(),
        Some("user-1".to_string()),
        Some("api-key-1".to_string()),
        Some("alice".to_string()),
        Some("default".to_string()),
        0,
        0,
        Some("provider-1".to_string()),
        Some(endpoint_id.to_string()),
        Some(key_id.to_string()),
        RequestCandidateStatus::Success,
        None,
        false,
        Some(200),
        None,
        None,
        Some(120),
        Some(concurrent_requests),
        None,
        None,
        (now_unix_secs - 10) * 1_000,
        Some((now_unix_secs - 10) * 1_000),
        Some((now_unix_secs - 8) * 1_000),
    )
    .expect("request candidate should build")
}

/// Trusted-admin request headers shared by every local admin-route test.
pub(super) fn trusted_admin_headers() -> http::HeaderMap {
    let mut headers = http::HeaderMap::new();
    for (name, value) in [
        (crate::constants::GATEWAY_HEADER, "rust-phase3b"),
        (
            crate::constants::TRUSTED_ADMIN_USER_ID_HEADER,
            "admin-user-123",
        ),
        (crate::constants::TRUSTED_ADMIN_USER_ROLE_HEADER, "admin"),
        (
            crate::constants::TRUSTED_ADMIN_SESSION_ID_HEADER,
            "session-123",
        ),
        (
            crate::constants::TRUSTED_ADMIN_MANAGEMENT_TOKEN_ID_HEADER,
            "management-token-123",
        ),
    ] {
        headers.insert(name, http::HeaderValue::from_static(value));
    }
    headers
}

/// Attach the same trusted-admin headers to an outgoing reqwest request.
pub(super) fn admin_request(builder: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
    builder
        .header(crate::constants::GATEWAY_HEADER, "rust-phase3b")
        .header(
            crate::constants::TRUSTED_ADMIN_USER_ID_HEADER,
            "admin-user-123",
        )
        .header(crate::constants::TRUSTED_ADMIN_USER_ROLE_HEADER, "admin")
        .header(
            crate::constants::TRUSTED_ADMIN_SESSION_ID_HEADER,
            "session-123",
        )
}

/// Spin up an upstream that must never be reached; the counter proves it.
pub(super) async fn start_unreachable_upstream(
    path: &'static str,
) -> (String, Arc<Mutex<usize>>, tokio::task::JoinHandle<()>) {
    let upstream_hits = Arc::new(Mutex::new(0usize));
    let upstream_hits_clone = Arc::clone(&upstream_hits);
    let upstream = axum::Router::new().route(
        path,
        axum::routing::any(move |_request: axum::extract::Request| {
            let upstream_hits_inner = Arc::clone(&upstream_hits_clone);
            async move {
                *upstream_hits_inner.lock().expect("mutex should lock") += 1;
                (
                    http::StatusCode::OK,
                    axum::body::Body::from("unexpected upstream hit"),
                )
            }
        }),
    );
    let (upstream_url, upstream_handle) = super::start_server(upstream).await;
    (upstream_url, upstream_hits, upstream_handle)
}
