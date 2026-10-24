use super::*;

#[test]
fn gateway_handles_admin_provider_oauth_supported_types_locally_with_trusted_admin_principal() {
    run_admin_oauth_test(
        "gateway_handles_admin_provider_oauth_supported_types_locally_with_trusted_admin_principal",
        gateway_handles_admin_provider_oauth_supported_types_locally_with_trusted_admin_principal_impl,
    );
}

async fn gateway_handles_admin_provider_oauth_supported_types_locally_with_trusted_admin_principal_impl(
) {
    let upstream_hits = Arc::new(Mutex::new(0usize));
    let upstream_hits_clone = Arc::clone(&upstream_hits);
    let upstream = Router::new().route(
        "/api/admin/provider-oauth/supported-types",
        any(move |_request: Request| {
            let upstream_hits_inner = Arc::clone(&upstream_hits_clone);
            async move {
                *upstream_hits_inner.lock().expect("mutex should lock") += 1;
                (StatusCode::OK, Body::from("unexpected upstream hit"))
            }
        }),
    );

    let (_upstream_url, upstream_handle) = start_server(upstream).await;
    let gateway = build_router_with_state(AppState::new().expect("gateway should build"));
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .get(format!(
            "{gateway_url}/api/admin/provider-oauth/supported-types"
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
    let items = payload.as_array().expect("items should be array");
    assert_eq!(items.len(), 6);
    assert_eq!(items[0]["provider_type"], "claude_code");
    assert_eq!(
        items[0]["authorize_url"],
        "https://claude.ai/oauth/authorize"
    );
    assert_eq!(
        items[0]["token_url"],
        "https://platform.claude.com/v1/oauth/token"
    );
    assert_eq!(
        items[0]["redirect_uri"],
        "https://platform.claude.com/oauth/code/callback"
    );
    assert_eq!(items[0]["supports_cookie_authorization"], true);
    assert_eq!(items[1]["provider_type"], "codex");
    assert_eq!(items[2]["provider_type"], "chatgpt_web");
    assert_eq!(items[3]["provider_type"], "gemini_cli");
    assert_eq!(items[4]["provider_type"], "antigravity");
    assert_eq!(items[5]["provider_type"], "windsurf");
    assert!(items[1..]
        .iter()
        .all(|item| item["supports_cookie_authorization"] == false));
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[test]
fn gateway_handles_admin_provider_oauth_start_key_locally_with_trusted_admin_principal() {
    run_admin_oauth_test(
        "gateway_handles_admin_provider_oauth_start_key_locally_with_trusted_admin_principal",
        gateway_handles_admin_provider_oauth_start_key_locally_with_trusted_admin_principal_impl,
    );
}

async fn gateway_handles_admin_provider_oauth_start_key_locally_with_trusted_admin_principal_impl()
{
    let upstream_hits = Arc::new(Mutex::new(0usize));
    let upstream_hits_clone = Arc::clone(&upstream_hits);
    let upstream = Router::new().route(
        "/api/admin/provider-oauth/keys/key-codex-oauth/start",
        any(move |_request: Request| {
            let upstream_hits_inner = Arc::clone(&upstream_hits_clone);
            async move {
                *upstream_hits_inner.lock().expect("mutex should lock") += 1;
                (StatusCode::OK, Body::from("unexpected upstream hit"))
            }
        }),
    );

    let mut provider = sample_provider("provider-codex", "codex", 10);
    provider.provider_type = "codex".to_string();

    let mut key = sample_bound_key(
        "key-codex-oauth",
        "provider-codex",
        "openai:chat",
        "oauth-access-token",
    );
    key.auth_type = "oauth".to_string();
    key.is_active = false;
    key.error_count = Some(7);
    key.health_by_format = Some(json!({
        "openai:chat": {"consecutive_failures": 3}
    }));
    key.circuit_breaker_by_format = Some(json!({
        "openai:chat": {"state": "open"}
    }));

    let provider_catalog_repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![provider],
        vec![],
        vec![key],
    ));

    let (_upstream_url, upstream_handle) = start_server(upstream).await;
    let gateway = build_router_with_state(
        AppState::new()
            .expect("gateway should build")
            .with_data_state_for_tests(GatewayDataState::with_provider_catalog_reader_for_tests(
                provider_catalog_repository,
            )),
    );
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .post(format!(
            "{gateway_url}/api/admin/provider-oauth/keys/key-codex-oauth/start"
        ))
        .header(crate::constants::GATEWAY_HEADER, "rust-phase3b")
        .header(TRUSTED_ADMIN_USER_ID_HEADER, "admin-user-123")
        .header(TRUSTED_ADMIN_USER_ROLE_HEADER, "admin")
        .header(TRUSTED_ADMIN_SESSION_ID_HEADER, "session-123")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get(http::header::CACHE_CONTROL),
        Some(&HeaderValue::from_static("no-store"))
    );
    assert_eq!(
        response.headers().get(http::header::PRAGMA),
        Some(&HeaderValue::from_static("no-cache"))
    );
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["provider_type"], "codex");
    assert_eq!(
        payload["redirect_uri"],
        "http://localhost:1455/auth/callback"
    );
    assert!(payload["authorization_url"]
        .as_str()
        .is_some_and(|url| url.contains("state=")));
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[test]
fn gateway_handles_admin_provider_oauth_start_provider_locally_with_trusted_admin_principal() {
    run_admin_oauth_test(
        "gateway_handles_admin_provider_oauth_start_provider_locally_with_trusted_admin_principal",
        gateway_handles_admin_provider_oauth_start_provider_locally_with_trusted_admin_principal_impl,
    );
}

async fn gateway_handles_admin_provider_oauth_start_provider_locally_with_trusted_admin_principal_impl(
) {
    let upstream_hits = Arc::new(Mutex::new(0usize));
    let upstream_hits_clone = Arc::clone(&upstream_hits);
    let upstream = Router::new().route(
        "/api/admin/provider-oauth/providers/provider-codex/start",
        any(move |_request: Request| {
            let upstream_hits_inner = Arc::clone(&upstream_hits_clone);
            async move {
                *upstream_hits_inner.lock().expect("mutex should lock") += 1;
                (StatusCode::OK, Body::from("unexpected upstream hit"))
            }
        }),
    );

    let mut provider = sample_provider("provider-codex", "codex", 10);
    provider.provider_type = "codex".to_string();

    let provider_catalog_repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![provider],
        vec![],
        vec![],
    ));

    let (_upstream_url, upstream_handle) = start_server(upstream).await;
    let gateway = build_router_with_state(
        AppState::new()
            .expect("gateway should build")
            .with_data_state_for_tests(GatewayDataState::with_provider_catalog_reader_for_tests(
                provider_catalog_repository,
            )),
    );
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .post(format!(
            "{gateway_url}/api/admin/provider-oauth/providers/provider-codex/start"
        ))
        .header(crate::constants::GATEWAY_HEADER, "rust-phase3b")
        .header(TRUSTED_ADMIN_USER_ID_HEADER, "admin-user-123")
        .header(TRUSTED_ADMIN_USER_ROLE_HEADER, "admin")
        .header(TRUSTED_ADMIN_SESSION_ID_HEADER, "session-123")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get(http::header::CACHE_CONTROL),
        Some(&HeaderValue::from_static("no-store"))
    );
    assert_eq!(
        response.headers().get(http::header::PRAGMA),
        Some(&HeaderValue::from_static("no-cache"))
    );
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["provider_type"], "codex");
    assert_eq!(
        payload["redirect_uri"],
        "http://localhost:1455/auth/callback"
    );
    assert!(payload["authorization_url"]
        .as_str()
        .is_some_and(|url| url.contains("state=")));
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[test]
fn gateway_handles_admin_provider_oauth_unavailable_routes_locally_with_trusted_admin_principal() {
    run_admin_oauth_test(
        "gateway_handles_admin_provider_oauth_unavailable_routes_locally_with_trusted_admin_principal",
        gateway_handles_admin_provider_oauth_unavailable_routes_locally_with_trusted_admin_principal_impl,
    );
}

async fn gateway_handles_admin_provider_oauth_unavailable_routes_locally_with_trusted_admin_principal_impl(
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

    let (_upstream_url, upstream_handle) = start_server(upstream).await;
    let gateway = build_router_with_state(AppState::new().expect("gateway should build"));
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let client = reqwest::Client::new();
    for path in [
        "/api/admin/provider-oauth/providers/provider-123/import-refresh-token",
        "/api/admin/provider-oauth/providers/provider-123/cookie-authorize/tasks",
        "/api/admin/provider-oauth/providers/provider-123/batch-import",
        "/api/admin/provider-oauth/providers/provider-123/batch-import/tasks",
        "/api/admin/provider-oauth/providers/provider-123/device-authorize",
        "/api/admin/provider-oauth/providers/provider-123/device-poll",
    ] {
        let response = client
            .post(format!("{gateway_url}{path}"))
            .header(crate::constants::GATEWAY_HEADER, "rust-phase3b")
            .header(TRUSTED_ADMIN_USER_ID_HEADER, "admin-user-123")
            .header(TRUSTED_ADMIN_USER_ROLE_HEADER, "admin")
            .header(TRUSTED_ADMIN_SESSION_ID_HEADER, "session-123")
            .send()
            .await
            .expect("request should succeed");

        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let payload: serde_json::Value = response.json().await.expect("json body should parse");
        assert_eq!(payload["detail"], "Admin provider OAuth data unavailable");
    }

    let refresh_response = client
        .post(format!(
            "{gateway_url}/api/admin/provider-oauth/keys/key-123/refresh"
        ))
        .header(crate::constants::GATEWAY_HEADER, "rust-phase3b")
        .header(TRUSTED_ADMIN_USER_ID_HEADER, "admin-user-123")
        .header(TRUSTED_ADMIN_USER_ROLE_HEADER, "admin")
        .header(TRUSTED_ADMIN_SESSION_ID_HEADER, "session-123")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(refresh_response.status(), StatusCode::NOT_FOUND);
    let refresh_payload: serde_json::Value = refresh_response
        .json()
        .await
        .expect("json body should parse");
    assert_eq!(refresh_payload["detail"], "Key 不存在");

    let complete_response = client
        .post(format!(
            "{gateway_url}/api/admin/provider-oauth/keys/key-123/complete"
        ))
        .header(crate::constants::GATEWAY_HEADER, "rust-phase3b")
        .header(TRUSTED_ADMIN_USER_ID_HEADER, "admin-user-123")
        .header(TRUSTED_ADMIN_USER_ROLE_HEADER, "admin")
        .header(TRUSTED_ADMIN_SESSION_ID_HEADER, "session-123")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(complete_response.status(), StatusCode::BAD_REQUEST);
    let complete_payload: serde_json::Value = complete_response
        .json()
        .await
        .expect("json body should parse");
    assert_eq!(complete_payload["detail"], "请求体必须是合法的 JSON 对象");

    let provider_complete_response = client
        .post(format!(
            "{gateway_url}/api/admin/provider-oauth/providers/provider-123/complete"
        ))
        .header(crate::constants::GATEWAY_HEADER, "rust-phase3b")
        .header(TRUSTED_ADMIN_USER_ID_HEADER, "admin-user-123")
        .header(TRUSTED_ADMIN_USER_ROLE_HEADER, "admin")
        .header(TRUSTED_ADMIN_SESSION_ID_HEADER, "session-123")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(provider_complete_response.status(), StatusCode::BAD_REQUEST);
    let provider_complete_payload: serde_json::Value = provider_complete_response
        .json()
        .await
        .expect("json body should parse");
    assert_eq!(
        provider_complete_payload["detail"],
        "请求体必须是合法的 JSON 对象"
    );

    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[test]
fn gateway_handles_admin_oauth_supported_types_locally_with_trusted_admin_principal() {
    run_admin_oauth_test(
        "gateway_handles_admin_oauth_supported_types_locally_with_trusted_admin_principal",
        gateway_handles_admin_oauth_supported_types_locally_with_trusted_admin_principal_impl,
    );
}

async fn gateway_handles_admin_oauth_supported_types_locally_with_trusted_admin_principal_impl() {
    let upstream_hits = Arc::new(Mutex::new(0usize));
    let upstream_hits_clone = Arc::clone(&upstream_hits);
    let upstream = Router::new().route(
        "/api/admin/oauth/supported-types",
        any(move |_request: Request| {
            let upstream_hits_inner = Arc::clone(&upstream_hits_clone);
            async move {
                *upstream_hits_inner.lock().expect("mutex should lock") += 1;
                (StatusCode::OK, Body::from("unexpected upstream hit"))
            }
        }),
    );

    let (_upstream_url, upstream_handle) = start_server(upstream).await;
    let gateway = build_router_with_state(AppState::new().expect("gateway should build"));
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .get(format!("{gateway_url}/api/admin/oauth/supported-types"))
        .header(crate::constants::GATEWAY_HEADER, "rust-phase3b")
        .header(TRUSTED_ADMIN_USER_ID_HEADER, "admin-user-123")
        .header(TRUSTED_ADMIN_USER_ROLE_HEADER, "admin")
        .header(TRUSTED_ADMIN_SESSION_ID_HEADER, "session-123")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    let items = payload.as_array().expect("items should be array");
    assert_eq!(items.len(), 2);
    assert_eq!(items[0]["provider_type"], "linuxdo");
    assert_eq!(items[0]["display_name"], "Linux Do");
    assert_eq!(items[1]["provider_type"], "custom_oidc");
    assert_eq!(items[1]["display_name"], "Custom OIDC");
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[test]
fn gateway_handles_admin_oauth_provider_list_locally_with_trusted_admin_principal() {
    run_admin_oauth_test(
        "gateway_handles_admin_oauth_provider_list_locally_with_trusted_admin_principal",
        gateway_handles_admin_oauth_provider_list_locally_with_trusted_admin_principal_impl,
    );
}

async fn gateway_handles_admin_oauth_provider_list_locally_with_trusted_admin_principal_impl() {
    let upstream_hits = Arc::new(Mutex::new(0usize));
    let upstream_hits_clone = Arc::clone(&upstream_hits);
    let upstream = Router::new().route(
        "/api/admin/oauth/providers",
        any(move |_request: Request| {
            let upstream_hits_inner = Arc::clone(&upstream_hits_clone);
            async move {
                *upstream_hits_inner.lock().expect("mutex should lock") += 1;
                (StatusCode::OK, Body::from("unexpected upstream hit"))
            }
        }),
    );

    let repository = Arc::new(InMemoryOAuthProviderRepository::seed(vec![
        sample_oauth_provider_config("linuxdo"),
    ]));
    let (_upstream_url, upstream_handle) = start_server(upstream).await;
    let gateway = build_router_with_state(
        AppState::new()
            .expect("gateway should build")
            .with_data_state_for_tests(GatewayDataState::with_oauth_provider_repository_for_tests(
                repository,
            )),
    );
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .get(format!("{gateway_url}/api/admin/oauth/providers"))
        .header(crate::constants::GATEWAY_HEADER, "rust-phase3b")
        .header(TRUSTED_ADMIN_USER_ID_HEADER, "admin-user-123")
        .header(TRUSTED_ADMIN_USER_ROLE_HEADER, "admin")
        .header(TRUSTED_ADMIN_SESSION_ID_HEADER, "session-123")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    let items = payload.as_array().expect("items should be array");
    assert_eq!(items.len(), 1);
    assert_eq!(items[0]["provider_type"], "linuxdo");
    assert_eq!(items[0]["has_secret"], true);
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[test]
fn gateway_upserts_admin_oauth_provider_locally_with_trusted_admin_principal() {
    run_admin_oauth_test(
        "gateway_upserts_admin_oauth_provider_locally_with_trusted_admin_principal",
        gateway_upserts_admin_oauth_provider_locally_with_trusted_admin_principal_impl,
    );
}

async fn gateway_upserts_admin_oauth_provider_locally_with_trusted_admin_principal_impl() {
    let upstream_hits = Arc::new(Mutex::new(0usize));
    let upstream_hits_clone = Arc::clone(&upstream_hits);
    let upstream = Router::new().route(
        "/api/admin/oauth/providers/linuxdo",
        any(move |_request: Request| {
            let upstream_hits_inner = Arc::clone(&upstream_hits_clone);
            async move {
                *upstream_hits_inner.lock().expect("mutex should lock") += 1;
                (StatusCode::OK, Body::from("unexpected upstream hit"))
            }
        }),
    );

    let repository = Arc::new(InMemoryOAuthProviderRepository::default());
    let (_upstream_url, upstream_handle) = start_server(upstream).await;
    let gateway = build_router_with_state(
        AppState::new()
            .expect("gateway should build")
            .with_data_state_for_tests(GatewayDataState::with_oauth_provider_repository_for_tests(
                repository.clone(),
            )),
    );
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .put(format!("{gateway_url}/api/admin/oauth/providers/linuxdo"))
        .header(crate::constants::GATEWAY_HEADER, "rust-phase3b")
        .header(TRUSTED_ADMIN_USER_ID_HEADER, "admin-user-123")
        .header(TRUSTED_ADMIN_USER_ROLE_HEADER, "admin")
        .header(TRUSTED_ADMIN_SESSION_ID_HEADER, "session-123")
        .json(&json!({
            "display_name": "Linux Do",
            "client_id": "client-id",
            "client_secret": "secret-value",
            "authorization_url_override": "https://connect.linux.do/oauth2/authorize",
            "token_url_override": "https://connect.linux.do/oauth2/token",
            "userinfo_url_override": "https://connect.linux.do/api/user",
            "scopes": ["openid", "profile"],
            "redirect_uri": "https://backend.example.com/oauth/callback",
            "frontend_callback_url": "https://frontend.example.com/auth/callback",
            "attribute_mapping": {"email": "email"},
            "extra_config": {"team": true},
            "is_enabled": true,
            "force": false
        }))
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["provider_type"], "linuxdo");
    assert_eq!(payload["has_secret"], true);
    assert_eq!(payload["is_enabled"], true);
    let stored = repository
        .get_oauth_provider_config("linuxdo")
        .await
        .expect("lookup should succeed")
        .expect("provider should exist");
    assert!(stored.client_secret_encrypted.is_some());
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[test]
fn gateway_rejects_custom_oidc_without_allowed_domains() {
    run_admin_oauth_test(
        "gateway_rejects_custom_oidc_without_allowed_domains",
        gateway_rejects_custom_oidc_without_allowed_domains_impl,
    );
}

async fn gateway_rejects_custom_oidc_without_allowed_domains_impl() {
    let upstream_hits = Arc::new(Mutex::new(0usize));
    let upstream_hits_clone = Arc::clone(&upstream_hits);
    let upstream = Router::new().route(
        "/api/admin/oauth/providers/custom_oidc",
        any(move |_request: Request| {
            let upstream_hits_inner = Arc::clone(&upstream_hits_clone);
            async move {
                *upstream_hits_inner.lock().expect("mutex should lock") += 1;
                (StatusCode::OK, Body::from("unexpected upstream hit"))
            }
        }),
    );

    let repository = Arc::new(InMemoryOAuthProviderRepository::default());
    let (_upstream_url, upstream_handle) = start_server(upstream).await;
    let gateway = build_router_with_state(
        AppState::new()
            .expect("gateway should build")
            .with_data_state_for_tests(GatewayDataState::with_oauth_provider_repository_for_tests(
                repository,
            )),
    );
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .put(format!(
            "{gateway_url}/api/admin/oauth/providers/custom_oidc"
        ))
        .header(crate::constants::GATEWAY_HEADER, "rust-phase3b")
        .header(TRUSTED_ADMIN_USER_ID_HEADER, "admin-user-123")
        .header(TRUSTED_ADMIN_USER_ROLE_HEADER, "admin")
        .header(TRUSTED_ADMIN_SESSION_ID_HEADER, "session-123")
        .json(&json!({
            "display_name": "Custom OIDC",
            "client_id": "custom-client",
            "authorization_url_override": "https://idp.example.com/oauth/authorize",
            "token_url_override": "https://idp.example.com/oauth/token",
            "userinfo_url_override": "https://idp.example.com/oauth/userinfo",
            "scopes": ["openid", "profile", "email"],
            "redirect_uri": "https://backend.example.com/oauth/callback",
            "frontend_callback_url": "https://frontend.example.com/auth/callback",
            "attribute_mapping": {"sub": "sub", "email": "email"},
            "extra_config": {},
            "is_enabled": true
        }))
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(
        payload["error"]["message"],
        "custom_oidc 必须在 extra_config.allowed_domains 配置域名白名单"
    );
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[test]
fn gateway_upserts_custom_oidc_with_allowed_domains() {
    run_admin_oauth_test(
        "gateway_upserts_custom_oidc_with_allowed_domains",
        gateway_upserts_custom_oidc_with_allowed_domains_impl,
    );
}

async fn gateway_upserts_custom_oidc_with_allowed_domains_impl() {
    let upstream_hits = Arc::new(Mutex::new(0usize));
    let upstream_hits_clone = Arc::clone(&upstream_hits);
    let upstream = Router::new().route(
        "/api/admin/oauth/providers/custom_oidc",
        any(move |_request: Request| {
            let upstream_hits_inner = Arc::clone(&upstream_hits_clone);
            async move {
                *upstream_hits_inner.lock().expect("mutex should lock") += 1;
                (StatusCode::OK, Body::from("unexpected upstream hit"))
            }
        }),
    );

    let repository = Arc::new(InMemoryOAuthProviderRepository::default());
    let (_upstream_url, upstream_handle) = start_server(upstream).await;
    let gateway = build_router_with_state(
        AppState::new()
            .expect("gateway should build")
            .with_data_state_for_tests(GatewayDataState::with_oauth_provider_repository_for_tests(
                repository.clone(),
            )),
    );
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .put(format!(
            "{gateway_url}/api/admin/oauth/providers/custom_oidc"
        ))
        .header(crate::constants::GATEWAY_HEADER, "rust-phase3b")
        .header(TRUSTED_ADMIN_USER_ID_HEADER, "admin-user-123")
        .header(TRUSTED_ADMIN_USER_ROLE_HEADER, "admin")
        .header(TRUSTED_ADMIN_SESSION_ID_HEADER, "session-123")
        .json(&json!({
            "display_name": "Custom OIDC",
            "client_id": "custom-client",
            "authorization_url_override": "https://idp.example.com/oauth/authorize",
            "token_url_override": "https://idp.example.com/oauth/token",
            "userinfo_url_override": "https://idp.example.com/oauth/userinfo",
            "scopes": ["openid", "profile", "email"],
            "redirect_uri": "https://backend.example.com/oauth/callback",
            "frontend_callback_url": "https://frontend.example.com/auth/callback",
            "attribute_mapping": {"sub": "id", "email": "profile.email"},
            "extra_config": {"allowed_domains": ["idp.example.com"]},
            "is_enabled": true
        }))
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["provider_type"], "custom_oidc");
    assert_eq!(payload["is_enabled"], true);

    let stored = repository
        .get_oauth_provider_config("custom_oidc")
        .await
        .expect("lookup should succeed")
        .expect("provider should exist");
    assert_eq!(
        stored.authorization_url_override.as_deref(),
        Some("https://idp.example.com/oauth/authorize")
    );
    assert_eq!(
        stored
            .extra_config
            .as_ref()
            .and_then(|value| {
                value
                    .get("allowed_domains")
                    .and_then(serde_json::Value::as_array)
            })
            .map(Vec::len),
        Some(1)
    );
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[test]
fn gateway_upserts_multiple_custom_oidc_configs() {
    run_admin_oauth_test(
        "gateway_upserts_multiple_custom_oidc_configs",
        gateway_upserts_multiple_custom_oidc_configs_impl,
    );
}

async fn gateway_upserts_multiple_custom_oidc_configs_impl() {
    let upstream_hits = Arc::new(Mutex::new(0usize));
    let upstream_hits_clone = Arc::clone(&upstream_hits);
    let upstream = Router::new().fallback(any(move |_request: Request| {
        let upstream_hits_inner = Arc::clone(&upstream_hits_clone);
        async move {
            *upstream_hits_inner.lock().expect("mutex should lock") += 1;
            (StatusCode::OK, Body::from("unexpected upstream hit"))
        }
    }));

    let repository = Arc::new(InMemoryOAuthProviderRepository::default());
    let (_upstream_url, upstream_handle) = start_server(upstream).await;
    let gateway = build_router_with_state(
        AppState::new()
            .expect("gateway should build")
            .with_data_state_for_tests(GatewayDataState::with_oauth_provider_repository_for_tests(
                repository.clone(),
            )),
    );
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    for (provider_type, display_name, host) in [
        ("custom_oidc_work", "Work OIDC", "work-idp.example.com"),
        (
            "custom_oidc_personal",
            "Personal OIDC",
            "personal-idp.example.com",
        ),
    ] {
        let response = reqwest::Client::new()
            .put(format!(
                "{gateway_url}/api/admin/oauth/providers/{provider_type}"
            ))
            .header(crate::constants::GATEWAY_HEADER, "rust-phase3b")
            .header(TRUSTED_ADMIN_USER_ID_HEADER, "admin-user-123")
            .header(TRUSTED_ADMIN_USER_ROLE_HEADER, "admin")
            .header(TRUSTED_ADMIN_SESSION_ID_HEADER, "session-123")
            .json(&json!({
                "display_name": display_name,
                "client_id": format!("{provider_type}-client"),
                "authorization_url_override": format!("https://{host}/oauth/authorize"),
                "token_url_override": format!("https://{host}/oauth/token"),
                "userinfo_url_override": format!("https://{host}/oauth/userinfo"),
                "scopes": ["openid", "profile", "email"],
                "redirect_uri": format!("https://backend.example.com/api/oauth/{provider_type}/callback"),
                "frontend_callback_url": "https://frontend.example.com/auth/callback",
                "attribute_mapping": {"sub": "id", "email": "profile.email"},
                "extra_config": {"allowed_domains": [host]},
                "is_enabled": true
            }))
            .send()
            .await
            .expect("request should succeed");

        assert_eq!(response.status(), StatusCode::OK);
        let payload: serde_json::Value = response.json().await.expect("json body should parse");
        assert_eq!(payload["provider_type"], provider_type);
        assert_eq!(payload["display_name"], display_name);
    }

    let stored = repository
        .list_oauth_provider_configs()
        .await
        .expect("list should succeed");
    assert_eq!(stored.len(), 2);
    assert!(stored
        .iter()
        .any(|provider| provider.provider_type == "custom_oidc_work"));
    assert!(stored
        .iter()
        .any(|provider| provider.provider_type == "custom_oidc_personal"));
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[test]
fn gateway_blocks_admin_oauth_test_ssrf_to_private_endpoints() {
    run_admin_oauth_test(
        "gateway_blocks_admin_oauth_test_ssrf_to_private_endpoints",
        gateway_blocks_admin_oauth_test_ssrf_to_private_endpoints_impl,
    );
}

async fn gateway_blocks_admin_oauth_test_ssrf_to_private_endpoints_impl() {
    let upstream_hits = Arc::new(Mutex::new(0usize));
    let upstream_hits_clone = Arc::clone(&upstream_hits);
    let upstream = Router::new().route(
        "/api/admin/oauth/providers/linuxdo/test",
        any(move |_request: Request| {
            let upstream_hits_inner = Arc::clone(&upstream_hits_clone);
            async move {
                *upstream_hits_inner.lock().expect("mutex should lock") += 1;
                (StatusCode::OK, Body::from("unexpected upstream hit"))
            }
        }),
    );

    let authorization_hits = Arc::new(Mutex::new(0usize));
    let authorization_hits_clone = Arc::clone(&authorization_hits);
    let token_hits = Arc::new(Mutex::new(0usize));
    let token_hits_clone = Arc::clone(&token_hits);
    let oauth_endpoints = Router::new()
        .route(
            "/oauth2/authorize",
            any(move |_request: Request| {
                let authorization_hits_inner = Arc::clone(&authorization_hits_clone);
                async move {
                    *authorization_hits_inner.lock().expect("mutex should lock") += 1;
                    (StatusCode::BAD_REQUEST, Body::from("missing query"))
                }
            }),
        )
        .route(
            "/oauth2/token",
            any(move |_request: Request| {
                let token_hits_inner = Arc::clone(&token_hits_clone);
                async move {
                    *token_hits_inner.lock().expect("mutex should lock") += 1;
                    (
                        StatusCode::METHOD_NOT_ALLOWED,
                        Body::from("method not allowed"),
                    )
                }
            }),
        );

    let repository = Arc::new(InMemoryOAuthProviderRepository::seed(vec![
        sample_oauth_provider_config("linuxdo"),
    ]));
    let (upstream_url, upstream_handle) = start_server(upstream).await;
    let (oauth_url, oauth_handle) = start_server(oauth_endpoints).await;
    let gateway = build_router_with_state(
        AppState::new()
            .expect("gateway should build")
            .with_data_state_for_tests(GatewayDataState::with_oauth_provider_repository_for_tests(
                repository,
            )),
    );
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .post(format!(
            "{gateway_url}/api/admin/oauth/providers/linuxdo/test"
        ))
        .header(crate::constants::GATEWAY_HEADER, "rust-phase3b")
        .header(TRUSTED_ADMIN_USER_ID_HEADER, "admin-user-123")
        .header(TRUSTED_ADMIN_USER_ROLE_HEADER, "admin")
        .header(TRUSTED_ADMIN_SESSION_ID_HEADER, "session-123")
        .json(&json!({
            "client_id": "client-id",
            "authorization_url_override": format!("{oauth_url}/oauth2/authorize"),
            "token_url_override": format!("{oauth_url}/oauth2/token"),
            "redirect_uri": "http://localhost:8084/api/oauth/linuxdo/callback"
        }))
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["authorization_url_reachable"], false);
    assert_eq!(payload["token_url_reachable"], false);
    assert_eq!(payload["secret_status"], "configured");
    assert_eq!(*authorization_hits.lock().expect("mutex should lock"), 0);
    assert_eq!(*token_hits.lock().expect("mutex should lock"), 0);
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
    oauth_handle.abort();
    let _ = upstream_url;
}

#[test]
fn gateway_tests_admin_oauth_linuxdo_reports_invalid_endpoint_urls() {
    run_admin_oauth_test(
        "gateway_tests_admin_oauth_linuxdo_reports_invalid_endpoint_urls",
        gateway_tests_admin_oauth_linuxdo_reports_invalid_endpoint_urls_impl,
    );
}

async fn gateway_tests_admin_oauth_linuxdo_reports_invalid_endpoint_urls_impl() {
    let upstream_hits = Arc::new(Mutex::new(0usize));
    let upstream_hits_clone = Arc::clone(&upstream_hits);
    let upstream = Router::new().route(
        "/api/admin/oauth/providers/linuxdo/test",
        any(move |_request: Request| {
            let upstream_hits_inner = Arc::clone(&upstream_hits_clone);
            async move {
                *upstream_hits_inner.lock().expect("mutex should lock") += 1;
                (StatusCode::OK, Body::from("unexpected upstream hit"))
            }
        }),
    );

    let repository = Arc::new(InMemoryOAuthProviderRepository::default());
    let (upstream_url, upstream_handle) = start_server(upstream).await;
    let gateway = build_router_with_state(
        AppState::new()
            .expect("gateway should build")
            .with_data_state_for_tests(GatewayDataState::with_oauth_provider_repository_for_tests(
                repository,
            )),
    );
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .post(format!(
            "{gateway_url}/api/admin/oauth/providers/linuxdo/test"
        ))
        .header(crate::constants::GATEWAY_HEADER, "rust-phase3b")
        .header(TRUSTED_ADMIN_USER_ID_HEADER, "admin-user-123")
        .header(TRUSTED_ADMIN_USER_ROLE_HEADER, "admin")
        .header(TRUSTED_ADMIN_SESSION_ID_HEADER, "session-123")
        .json(&json!({
            "client_id": "client-id",
            "authorization_url_override": "not-a-url",
            "token_url_override": "not-a-url",
            "redirect_uri": "http://localhost:8084/api/oauth/linuxdo/callback"
        }))
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["authorization_url_reachable"], false);
    assert_eq!(payload["token_url_reachable"], false);
    assert_eq!(payload["secret_status"], "not_provided");
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
    let _ = upstream_url;
}

#[test]
fn gateway_deletes_admin_oauth_provider_locally_with_trusted_admin_principal() {
    run_admin_oauth_test(
        "gateway_deletes_admin_oauth_provider_locally_with_trusted_admin_principal",
        gateway_deletes_admin_oauth_provider_locally_with_trusted_admin_principal_impl,
    );
}

async fn gateway_deletes_admin_oauth_provider_locally_with_trusted_admin_principal_impl() {
    let upstream_hits = Arc::new(Mutex::new(0usize));
    let upstream_hits_clone = Arc::clone(&upstream_hits);
    let upstream = Router::new().route(
        "/api/admin/oauth/providers/linuxdo",
        any(move |_request: Request| {
            let upstream_hits_inner = Arc::clone(&upstream_hits_clone);
            async move {
                *upstream_hits_inner.lock().expect("mutex should lock") += 1;
                (StatusCode::OK, Body::from("unexpected upstream hit"))
            }
        }),
    );

    let repository = Arc::new(InMemoryOAuthProviderRepository::seed(vec![
        sample_oauth_provider_config("linuxdo"),
    ]));
    let (_upstream_url, upstream_handle) = start_server(upstream).await;
    let gateway = build_router_with_state(
        AppState::new()
            .expect("gateway should build")
            .with_data_state_for_tests(GatewayDataState::with_oauth_provider_repository_for_tests(
                repository.clone(),
            )),
    );
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .delete(format!("{gateway_url}/api/admin/oauth/providers/linuxdo"))
        .header(crate::constants::GATEWAY_HEADER, "rust-phase3b")
        .header(TRUSTED_ADMIN_USER_ID_HEADER, "admin-user-123")
        .header(TRUSTED_ADMIN_USER_ROLE_HEADER, "admin")
        .header(TRUSTED_ADMIN_SESSION_ID_HEADER, "session-123")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["message"], "删除成功");
    assert!(repository
        .get_oauth_provider_config("linuxdo")
        .await
        .expect("lookup should succeed")
        .is_none());
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[test]
fn gateway_rejects_deleting_disabled_oauth_provider_with_user_bindings() {
    run_admin_oauth_test(
        "gateway_rejects_deleting_disabled_oauth_provider_with_user_bindings",
        gateway_rejects_deleting_disabled_oauth_provider_with_user_bindings_impl,
    );
}

async fn gateway_rejects_deleting_disabled_oauth_provider_with_user_bindings_impl() {
    let upstream_hits = Arc::new(Mutex::new(0usize));
    let upstream_hits_clone = Arc::clone(&upstream_hits);
    let upstream = Router::new().route(
        "/api/admin/oauth/providers/linuxdo",
        any(move |_request: Request| {
            let upstream_hits_inner = Arc::clone(&upstream_hits_clone);
            async move {
                *upstream_hits_inner.lock().expect("mutex should lock") += 1;
                (StatusCode::OK, Body::from("unexpected upstream hit"))
            }
        }),
    );

    let mut provider = sample_oauth_provider_config("linuxdo");
    provider.is_enabled = false;
    let provider_repository = Arc::new(InMemoryOAuthProviderRepository::seed([provider]));
    let user_repository = Arc::new(InMemoryUserReadRepository::default());
    let now = chrono::Utc::now();
    let user = user_repository
        .create_oauth_auth_user(
            Some("bound@example.com".to_string()),
            false,
            "bound-user".to_string(),
            now,
        )
        .await
        .expect("OAuth user should create")
        .expect("OAuth user should exist");
    user_repository
        .bind_user_oauth_link(&user.id, "linuxdo", "bound-subject", None, None, None, now)
        .await
        .expect("OAuth link should bind");

    let data = GatewayDataState::with_oauth_provider_repository_for_tests(Arc::clone(
        &provider_repository,
    ))
    .with_user_reader(user_repository);
    let (upstream_url, upstream_handle) = start_server(upstream).await;
    let gateway = build_router_with_state(
        AppState::new()
            .expect("gateway should build")
            .with_data_state_for_tests(data),
    );
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .delete(format!("{gateway_url}/api/admin/oauth/providers/linuxdo"))
        .header(crate::constants::GATEWAY_HEADER, "rust-phase3b")
        .header(TRUSTED_ADMIN_USER_ID_HEADER, "admin-user-123")
        .header(TRUSTED_ADMIN_USER_ROLE_HEADER, "admin")
        .header(TRUSTED_ADMIN_SESSION_ID_HEADER, "session-123")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::CONFLICT);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["error"]["type"], "provider_has_bindings");
    assert!(provider_repository
        .get_oauth_provider_config("linuxdo")
        .await
        .expect("provider lookup should succeed")
        .is_some());
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
    let _ = upstream_url;
}
