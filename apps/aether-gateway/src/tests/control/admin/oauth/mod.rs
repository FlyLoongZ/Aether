use std::sync::{Arc, Mutex};

use aether_contracts::{ExecutionPlan, EXECUTION_REQUEST_FOLLOW_REDIRECTS_HEADER};
use aether_crypto::{encrypt_python_fernet_plaintext, DEVELOPMENT_ENCRYPTION_KEY};
use aether_data::repository::background_tasks::InMemoryBackgroundTaskRepository;
use aether_data::repository::management_tokens::{
    InMemoryManagementTokenRepository, ManagementTokenReadRepository,
};
use aether_data::repository::oauth_providers::{
    InMemoryOAuthProviderRepository, OAuthProviderReadRepository,
};
use aether_data::repository::pool_scores::InMemoryPoolMemberScoreRepository;
use aether_data::repository::provider_catalog::InMemoryProviderCatalogReadRepository;
use aether_data::repository::proxy_nodes::InMemoryProxyNodeRepository;
use aether_data::repository::users::{InMemoryUserReadRepository, UserReadRepository};
use aether_data_contracts::repository::background_tasks::BackgroundTaskReadRepository;
use aether_data_contracts::repository::pool_scores::{
    GetPoolMemberScoresByIdsQuery, PoolMemberHardState, PoolMemberIdentity, PoolScoreReadRepository,
};
use aether_data_contracts::repository::provider_catalog::{
    ProviderCatalogKeyCredentialsCasUpdate, ProviderCatalogReadRepository,
    ProviderCatalogWriteRepository, StoredProviderCatalogEndpoint, StoredProviderCatalogKey,
};
use axum::body::{to_bytes, Body, Bytes};
use axum::response::{IntoResponse, Response};
use axum::routing::{any, get, post};
use axum::{extract::Request, Json, Router};
use http::{HeaderMap, HeaderValue, StatusCode};
use serde_json::{json, Value};

use super::super::{
    build_router_with_state, build_state_with_execution_runtime_override, hash_management_token,
    sample_bound_key, sample_endpoint, sample_key, sample_management_token,
    sample_oauth_provider_config, sample_provider, sample_proxy_node, start_server, AppState,
};
use crate::ai_serving::{
    build_provider_key_pool_score_upsert, provider_key_pool_score_id, provider_key_pool_score_scope,
};
use crate::audit::AdminAuditEvent;
use crate::constants::{
    GATEWAY_HEADER, TRUSTED_ADMIN_MANAGEMENT_TOKEN_ID_HEADER, TRUSTED_ADMIN_SESSION_ID_HEADER,
    TRUSTED_ADMIN_USER_ID_HEADER, TRUSTED_ADMIN_USER_ROLE_HEADER,
};
use crate::control::resolve_public_request_context;
use crate::data::GatewayDataState;
use crate::handlers::admin::provider::oauth::dispatch::maybe_build_local_admin_provider_oauth_response;
use crate::handlers::admin::{AdminAppState, AdminRequestContext};

const ADMIN_OAUTH_TEST_STACK_BYTES: usize = 16 * 1024 * 1024;

mod device;
mod import;
mod management_tokens;
mod misc;
mod providers;
mod refresh;

fn run_admin_oauth_test<F, Fut>(test_name: &'static str, make_future: F)
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: std::future::Future<Output = ()> + 'static,
{
    crate::tests::run_async_test_on_large_stack(
        test_name,
        ADMIN_OAUTH_TEST_STACK_BYTES,
        make_future,
    );
}

fn decrypt_persisted_provider_api_key(key: &StoredProviderCatalogKey) -> String {
    AppState::new()
        .expect("credential test state should build")
        .with_data_state_for_tests(
            GatewayDataState::disabled().with_encryption_key_for_tests(DEVELOPMENT_ENCRYPTION_KEY),
        )
        .decrypt_provider_catalog_key_api_key(key)
        .expect("persisted api key should decrypt")
        .expect("persisted api key should exist")
}

fn decrypt_persisted_provider_auth_config(key: &StoredProviderCatalogKey) -> String {
    AppState::new()
        .expect("credential test state should build")
        .with_data_state_for_tests(
            GatewayDataState::disabled().with_encryption_key_for_tests(DEVELOPMENT_ENCRYPTION_KEY),
        )
        .decrypt_provider_catalog_key_auth_config(key)
        .expect("persisted auth config should decrypt")
        .expect("persisted auth config should exist")
}

fn trusted_admin_headers() -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(GATEWAY_HEADER, HeaderValue::from_static("rust-phase3b"));
    headers.insert(
        TRUSTED_ADMIN_USER_ID_HEADER,
        HeaderValue::from_static("admin-user-123"),
    );
    headers.insert(
        TRUSTED_ADMIN_USER_ROLE_HEADER,
        HeaderValue::from_static("admin"),
    );
    headers.insert(
        TRUSTED_ADMIN_SESSION_ID_HEADER,
        HeaderValue::from_static("session-123"),
    );
    headers.insert(
        TRUSTED_ADMIN_MANAGEMENT_TOKEN_ID_HEADER,
        HeaderValue::from_static("management-token-123"),
    );
    headers
}

async fn local_admin_provider_oauth_response(
    state: &AppState,
    method: http::Method,
    uri: &str,
    body: Option<serde_json::Value>,
) -> Response<Body> {
    let headers = trusted_admin_headers();
    let request_context = resolve_public_request_context(
        state,
        &method,
        &uri.parse().expect("uri should parse"),
        &headers,
        "trace-123",
    )
    .await
    .expect("request context should resolve");
    let body_bytes = body.map(|value| Bytes::from(value.to_string()));
    maybe_build_local_admin_provider_oauth_response(
        &AdminAppState::new(state),
        &AdminRequestContext::new(&request_context),
        body_bytes.as_ref(),
    )
    .await
    .expect("local provider oauth response should build")
    .expect("provider oauth route should resolve locally")
}

fn sample_kiro_device_access_token(email: &str) -> String {
    use base64::Engine as _;

    let header =
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(r#"{"alg":"none","typ":"JWT"}"#);
    let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(
        json!({
            "email": email,
            "sub": "kiro-user-123",
        })
        .to_string(),
    );
    format!("{header}.{payload}.sig")
}

fn sample_kiro_device_access_token_without_email() -> String {
    use base64::Engine as _;

    let header =
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(r#"{"alg":"none","typ":"JWT"}"#);
    let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(
        json!({
            "sub": "kiro-user-123",
        })
        .to_string(),
    );
    format!("{header}.{payload}.sig")
}

fn sample_codex_access_token_with_profile_email(email: &str, account_id: &str) -> String {
    sample_codex_access_token_with_profile_email_and_expiry(email, account_id, Some(2_000_000_000))
}

fn sample_codex_access_token_with_profile_email_and_expiry(
    email: &str,
    account_id: &str,
    expires_at: Option<u64>,
) -> String {
    use base64::Engine as _;

    let header =
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(r#"{"alg":"none","typ":"JWT"}"#);
    let mut claims = json!({
        "iss": "https://auth.openai.com",
        "aud": ["https://api.openai.com/v1"],
        "https://api.openai.com/profile": {
            "email": email,
            "email_verified": true,
        },
        "https://api.openai.com/auth": {
            "chatgpt_account_id": account_id,
        },
    });
    if let Some(expires_at) = expires_at {
        claims["exp"] = json!(expires_at);
    }
    let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(claims.to_string());
    format!("{header}.{payload}.sig")
}

fn codex_import_token_execution_result(request_id: &str) -> serde_json::Value {
    json!({
        "request_id": request_id,
        "status_code": 200,
        "headers": {
            "content-type": "application/json"
        },
        "body": {
            "json_body": {
                "access_token": "imported-codex-access-token",
                "refresh_token": "imported-codex-refresh-token",
                "token_type": "Bearer",
                "expires_in": 1800,
                "scope": "openid email profile offline_access",
                "email": "alice@example.com",
                "account_id": "acct-codex-123",
                "plan_type": "plus"
            }
        }
    })
}

fn codex_quota_execution_result(request_id: &str) -> serde_json::Value {
    json!({
        "request_id": request_id,
        "status_code": 200,
        "headers": {},
        "body": {
            "json_body": {
                "user": {
                    "id": "user-codex-123",
                    "email": "alice@example.com"
                },
                "account": {
                    "id": "acct-codex-123",
                    "plan_type": "plus"
                },
                "plan": {
                    "type": "Plus",
                    "title": "ChatGPT Plus"
                }
            }
        }
    })
}

fn windsurf_register_user_execution_result(request_id: &str) -> serde_json::Value {
    json!({
        "request_id": request_id,
        "status_code": 200,
        "headers": {
            "content-type": "application/json"
        },
        "body": {
            "json_body": {
                "sessionToken": "devin-session-token$registered",
                "name": "Windsurf User",
                "apiServerUrl": "https://server.codeium.com"
            }
        }
    })
}

fn assert_single_provider_oauth_refresh_token_plan<'a>(
    plans: &'a [ExecutionPlan],
) -> &'a ExecutionPlan {
    let token_plans = plans
        .iter()
        .filter(|plan| plan.request_id == "provider-oauth:refresh-token")
        .collect::<Vec<_>>();
    assert_eq!(
        token_plans.len(),
        1,
        "expected exactly one provider-oauth refresh-token plan, got {:?}",
        plans
            .iter()
            .map(|plan| plan.request_id.as_str())
            .collect::<Vec<_>>()
    );
    assert!(
        plans.iter().all(|plan| {
            plan.request_id == "provider-oauth:refresh-token"
                || plan.request_id.starts_with("codex-quota:")
        }),
        "unexpected execution plans: {:?}",
        plans
            .iter()
            .map(|plan| plan.request_id.as_str())
            .collect::<Vec<_>>()
    );
    token_plans[0]
}

async fn assert_antigravity_oauth_account_uses_google_userinfo_email(
    operation: &str,
    request_body: Value,
) {
    let upstream_hits = Arc::new(Mutex::new(0usize));
    let upstream_hits_clone = Arc::clone(&upstream_hits);
    let upstream = Router::new().fallback(any(move |_request: Request| {
        let upstream_hits_inner = Arc::clone(&upstream_hits_clone);
        async move {
            *upstream_hits_inner.lock().expect("mutex should lock") += 1;
            (StatusCode::OK, Body::from("unexpected upstream hit"))
        }
    }));

    let token_hits = Arc::new(Mutex::new(0usize));
    let token_hits_clone = Arc::clone(&token_hits);
    let user_info_hits = Arc::new(Mutex::new(0usize));
    let user_info_hits_clone = Arc::clone(&user_info_hits);
    let seen_user_info_authorization = Arc::new(Mutex::new(None::<String>));
    let seen_user_info_authorization_clone = Arc::clone(&seen_user_info_authorization);
    let google_server = Router::new()
        .route(
            "/oauth/token",
            post(move || {
                let token_hits_inner = Arc::clone(&token_hits_clone);
                async move {
                    *token_hits_inner.lock().expect("mutex should lock") += 1;
                    Json(json!({
                        "access_token": "antigravity-access-token",
                        "refresh_token": "antigravity-refresh-token",
                        "token_type": "Bearer",
                        "expires_in": 3600,
                        "scope": "https://www.googleapis.com/auth/userinfo.email"
                    }))
                }
            }),
        )
        .route(
            "/oauth/userinfo",
            get(move |headers: HeaderMap| {
                let user_info_hits_inner = Arc::clone(&user_info_hits_clone);
                let seen_authorization_inner = Arc::clone(&seen_user_info_authorization_clone);
                async move {
                    *user_info_hits_inner.lock().expect("mutex should lock") += 1;
                    *seen_authorization_inner.lock().expect("mutex should lock") = headers
                        .get(http::header::AUTHORIZATION)
                        .and_then(|value| value.to_str().ok())
                        .map(ToOwned::to_owned);
                    Json(json!({
                        "email": "new-antigravity@example.com",
                        "verified_email": true,
                        "name": "Antigravity User"
                    }))
                }
            }),
        );

    let mut provider = sample_provider("provider-antigravity", "antigravity", 10);
    provider.provider_type = "antigravity".to_string();
    let endpoint = sample_endpoint(
        "endpoint-antigravity",
        "provider-antigravity",
        "gemini:generate_content",
        "https://daily-cloudcode-pa.googleapis.com",
    );
    let provider_catalog_repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![provider],
        vec![endpoint],
        vec![],
    ));

    let (upstream_url, upstream_handle) = start_server(upstream).await;
    let (google_url, google_handle) = start_server(google_server).await;
    let gateway = build_router_with_state(
        AppState::new()
            .expect("gateway should build")
            .with_data_state_for_tests(
                GatewayDataState::with_provider_catalog_repository_for_tests(
                    provider_catalog_repository.clone(),
                )
                .with_encryption_key_for_tests(DEVELOPMENT_ENCRYPTION_KEY),
            )
            .with_provider_oauth_state_entry_for_tests(
                "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
                json!({
                    "nonce": "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
                    "key_id": "",
                    "provider_id": "provider-antigravity",
                    "provider_type": "antigravity",
                    "pkce_verifier": "verifier-antigravity-123",
                    "initiated_by_user_id": "admin-user-123",
                    "initiated_by_session_id": "session-123",
                    "created_at": aether_admin::provider::state::current_unix_secs(),
                }),
            )
            .with_provider_oauth_token_url_for_tests(
                "antigravity",
                format!("{google_url}/oauth/token"),
            )
            .with_provider_oauth_token_url_for_tests(
                "antigravity_user_info",
                format!("{google_url}/oauth/userinfo"),
            ),
    );
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .post(format!(
            "{gateway_url}/api/admin/provider-oauth/providers/provider-antigravity/{operation}"
        ))
        .header(crate::constants::GATEWAY_HEADER, "rust-phase3b")
        .header(TRUSTED_ADMIN_USER_ID_HEADER, "admin-user-123")
        .header(TRUSTED_ADMIN_USER_ROLE_HEADER, "admin")
        .header(TRUSTED_ADMIN_SESSION_ID_HEADER, "session-123")
        .json(&request_body)
        .send()
        .await
        .expect("request should succeed");

    let status = response.status();
    let payload: Value = response.json().await.expect("json body should parse");
    assert_eq!(status, StatusCode::OK, "payload={payload}");
    let account_result = if operation == "batch-import" {
        assert_eq!(payload["total"], 1);
        assert_eq!(payload["success"], 1, "payload={payload}");
        assert_eq!(payload["failed"], 0);
        assert_eq!(payload["results"][0]["status"], "success");
        assert_eq!(
            payload["results"][0]["key_name"],
            "new-antigravity@example.com"
        );
        &payload["results"][0]
    } else {
        assert_eq!(payload["provider_type"], "antigravity");
        assert_eq!(payload["email"], "new-antigravity@example.com");
        &payload
    };
    assert_eq!(account_result["replaced"], false);
    assert_eq!(*token_hits.lock().expect("mutex should lock"), 1);
    assert_eq!(*user_info_hits.lock().expect("mutex should lock"), 1);
    assert_eq!(
        seen_user_info_authorization
            .lock()
            .expect("mutex should lock")
            .as_deref(),
        Some("Bearer antigravity-access-token")
    );
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    let key_id = account_result["key_id"]
        .as_str()
        .expect("created key id should be returned")
        .to_string();
    let persisted_keys = provider_catalog_repository
        .list_keys_by_ids(std::slice::from_ref(&key_id))
        .await
        .expect("created key should load");
    let persisted = persisted_keys.first().expect("created key should exist");
    assert_eq!(persisted.name, "new-antigravity@example.com");
    let decrypted_auth_config = decrypt_persisted_provider_auth_config(persisted);
    let auth_config: Value =
        serde_json::from_str(&decrypted_auth_config).expect("auth config json should parse");
    assert_eq!(auth_config["email"], "new-antigravity@example.com");
    assert_eq!(auth_config["refresh_token"], "antigravity-refresh-token");

    gateway_handle.abort();
    google_handle.abort();
    upstream_handle.abort();
    drop(upstream_url);
}

async fn run_gateway_manual_kiro_oauth_refresh_maintenance_endpoint_test(
    initial_endpoint: Option<StoredProviderCatalogEndpoint>,
    expected_endpoint_id: Option<&str>,
    expected_endpoint_active: bool,
) {
    let refreshed_access_token = sample_kiro_device_access_token("kiro-refresh@example.com");
    let expected_access_token = refreshed_access_token.clone();
    let refreshed_refresh_token = "s".repeat(120);
    let expected_refresh_token = refreshed_refresh_token.clone();
    let token_hits = Arc::new(Mutex::new(0usize));
    let token_hits_clone = Arc::clone(&token_hits);
    let token_server = Router::new().route(
        "/refreshToken",
        post(move |_request: Request| {
            let token_hits_inner = Arc::clone(&token_hits_clone);
            let access_token_inner = refreshed_access_token.clone();
            let refresh_token_inner = refreshed_refresh_token.clone();
            async move {
                *token_hits_inner.lock().expect("mutex should lock") += 1;
                Json(json!({
                    "accessToken": access_token_inner,
                    "refreshToken": refresh_token_inner,
                    "expiresIn": 3600,
                    "profileArn": "arn:aws:kiro:profile/manual-refresh"
                }))
            }
        }),
    );

    let seen_endpoint_id = Arc::new(Mutex::new(None::<String>));
    let seen_endpoint_id_clone = Arc::clone(&seen_endpoint_id);
    let execution_runtime = Router::new().route(
        "/v1/execute/sync",
        any(move |request: Request| {
            let seen_endpoint_id_inner = Arc::clone(&seen_endpoint_id_clone);
            async move {
                let plan: ExecutionPlan = serde_json::from_slice(
                    &to_bytes(request.into_body(), usize::MAX)
                        .await
                        .expect("body should read"),
                )
                .expect("plan should parse");
                *seen_endpoint_id_inner.lock().expect("mutex should lock") =
                    Some(plan.endpoint_id.clone());
                let result = aether_contracts::ExecutionResult {
                    request_id: plan.request_id,
                    candidate_id: None,
                    status_code: 200,
                    headers: std::collections::BTreeMap::new(),
                    response_observation: None,
                    body: Some(aether_contracts::ResponseBody {
                        json_body: Some(json!({
                            "subscriptionInfo": {
                                "subscriptionTitle": "KIRO PRO"
                            },
                            "usageBreakdownList": [{
                                "currentUsageWithPrecision": 2.0,
                                "usageLimitWithPrecision": 10.0,
                                "nextDateReset": 1_900_000_000u64
                            }],
                            "desktopUserInfo": {
                                "email": "kiro-refresh@example.com"
                            }
                        })),
                        body_bytes_b64: None,
                    }),
                    telemetry: None,
                    error: None,
                };
                (StatusCode::OK, Json(result))
            }
        }),
    );

    let mut provider = sample_provider("provider-kiro-oauth-refresh", "kiro", 10);
    provider.provider_type = "kiro".to_string();
    let mut key = sample_key(
        "key-kiro-oauth-refresh",
        "provider-kiro-oauth-refresh",
        "claude:messages",
        "stale-kiro-access-token",
    );
    key.auth_type = "oauth".to_string();
    key.encrypted_auth_config = Some(
        encrypt_python_fernet_plaintext(
            DEVELOPMENT_ENCRYPTION_KEY,
            &json!({
                "provider_type": "kiro",
                "auth_method": "social",
                "refresh_token": "r".repeat(120),
                "machine_id": "123e4567-e89b-12d3-a456-426614174000",
                "kiro_version": "1.2.3",
                "expires_at": 1u64
            })
            .to_string(),
        )
        .expect("auth config ciphertext should build"),
    );

    let provider_catalog_repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![provider],
        initial_endpoint.into_iter().collect(),
        vec![key],
    ));

    let (token_url, token_handle) = start_server(token_server).await;
    let (execution_runtime_url, execution_runtime_handle) = start_server(execution_runtime).await;
    let oauth_refresh =
        crate::provider_transport::LocalOAuthRefreshCoordinator::with_adapters_for_tests(vec![
            Arc::new(
                crate::provider_transport::kiro::KiroOAuthRefreshAdapter::default()
                    .with_refresh_base_urls(Some(token_url), None),
            )
                as Arc<dyn crate::provider_transport::oauth_refresh::LocalOAuthRefreshAdapter>,
        ]);
    let gateway = build_router_with_state(
        build_state_with_execution_runtime_override(execution_runtime_url)
            .with_data_state_for_tests(
                GatewayDataState::with_provider_catalog_repository_for_tests(
                    provider_catalog_repository.clone(),
                )
                .with_encryption_key_for_tests(DEVELOPMENT_ENCRYPTION_KEY),
            )
            .with_oauth_refresh_coordinator_for_tests(oauth_refresh),
    );
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .post(format!(
            "{gateway_url}/api/admin/provider-oauth/keys/key-kiro-oauth-refresh/refresh"
        ))
        .header(crate::constants::GATEWAY_HEADER, "rust-phase3b")
        .header(TRUSTED_ADMIN_USER_ID_HEADER, "admin-user-123")
        .header(TRUSTED_ADMIN_USER_ROLE_HEADER, "admin")
        .header(TRUSTED_ADMIN_SESSION_ID_HEADER, "session-123")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["provider_type"], "kiro");
    assert_eq!(payload["has_refresh_token"], true);
    assert_eq!(payload["account_state_recheck_attempted"], true);
    assert_eq!(
        payload["account_state_recheck_error"],
        serde_json::Value::Null
    );
    assert!(
        *token_hits.lock().expect("mutex should lock") >= 1,
        "Kiro refresh endpoint should be called"
    );

    let endpoints = provider_catalog_repository
        .list_endpoints_by_provider_ids(&["provider-kiro-oauth-refresh".to_string()])
        .await
        .expect("endpoints should read");
    assert_eq!(endpoints.len(), 1);
    assert_eq!(endpoints[0].api_format, "claude:messages");
    assert_eq!(endpoints[0].base_url, "https://q.{region}.amazonaws.com");
    assert_eq!(endpoints[0].is_active, expected_endpoint_active);
    if let Some(expected_endpoint_id) = expected_endpoint_id {
        assert_eq!(endpoints[0].id, expected_endpoint_id);
    }
    assert_eq!(
        *seen_endpoint_id.lock().expect("mutex should lock"),
        Some(endpoints[0].id.clone())
    );

    let stored_key = provider_catalog_repository
        .list_keys_by_ids(&["key-kiro-oauth-refresh".to_string()])
        .await
        .expect("keys should load")
        .into_iter()
        .next()
        .expect("refreshed key should exist");
    let decrypted_api_key = decrypt_persisted_provider_api_key(&stored_key);
    assert_eq!(decrypted_api_key, expected_access_token);
    let decrypted_auth_config = decrypt_persisted_provider_auth_config(&stored_key);
    let auth_config: serde_json::Value =
        serde_json::from_str(&decrypted_auth_config).expect("auth config should parse");
    assert_eq!(auth_config["provider_type"], "kiro");
    assert_eq!(auth_config["refresh_token"], expected_refresh_token);

    gateway_handle.abort();
    execution_runtime_handle.abort();
    token_handle.abort();
}
