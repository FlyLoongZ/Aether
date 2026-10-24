use super::*;

#[test]
fn gateway_requires_oauth_admin_management_token_to_configure_frontend_callback() {
    run_admin_oauth_test(
        "gateway_requires_oauth_admin_management_token_to_configure_frontend_callback",
        gateway_requires_oauth_admin_management_token_to_configure_frontend_callback_impl,
    );
}

async fn gateway_requires_oauth_admin_management_token_to_configure_frontend_callback_impl() {
    let state = AppState::new().expect("gateway should build");
    let admin_user = state
        .create_local_auth_user_with_settings(
            Some("oauth-callback-admin@example.com".to_string()),
            true,
            "oauth-callback-admin".to_string(),
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

    let write_raw_token = "ae-oauth-callback-write";
    let mut write_token = sample_management_token(
        "token-oauth-callback-write",
        &admin_user.id,
        "oauth-callback-write",
        true,
    );
    write_token.token.allowed_ips = None;
    write_token.token.permissions = Some(json!(["admin:oauth:write"]));

    let admin_raw_token = "ae-oauth-callback-admin";
    let mut oauth_admin_token = sample_management_token(
        "token-oauth-callback-admin",
        &admin_user.id,
        "oauth-callback-admin",
        true,
    );
    oauth_admin_token.token.allowed_ips = None;
    oauth_admin_token.token.permissions = Some(json!(["admin:oauth:admin"]));

    let management_tokens = Arc::new(InMemoryManagementTokenRepository::seed_with_hashes(
        vec![write_token, oauth_admin_token],
        vec![
            (
                hash_management_token(write_raw_token),
                "token-oauth-callback-write".to_string(),
            ),
            (
                hash_management_token(admin_raw_token),
                "token-oauth-callback-admin".to_string(),
            ),
        ],
    ));
    let oauth_providers = Arc::new(InMemoryOAuthProviderRepository::default());
    let data = GatewayDataState::with_management_token_repository_for_tests(management_tokens)
        .attach_oauth_provider_repository_for_tests(Arc::clone(&oauth_providers));
    let gateway = build_router_with_state(state.with_data_state_for_tests(data));
    let (gateway_url, gateway_handle) = start_server(gateway).await;
    let client = reqwest::Client::new();
    let provider_payload = |frontend_callback_url: &str| {
        json!({
            "display_name": "Linux Do",
            "client_id": "client-id",
            "authorization_url_override": "https://connect.linux.do/oauth2/authorize",
            "token_url_override": "https://connect.linux.do/oauth2/token",
            "userinfo_url_override": "https://connect.linux.do/api/user",
            "scopes": ["openid", "profile"],
            "redirect_uri": "https://backend.example.com/oauth/callback",
            "frontend_callback_url": frontend_callback_url,
            "attribute_mapping": {"email": "email"},
            "extra_config": {"team": true},
            "is_enabled": true,
            "force": false
        })
    };
    let provider_url = format!("{gateway_url}/api/admin/oauth/providers/linuxdo");

    let denied_create = client
        .put(&provider_url)
        .header(crate::constants::GATEWAY_HEADER, "rust-phase3b")
        .bearer_auth(write_raw_token)
        .json(&provider_payload("https://attacker.example/auth/callback"))
        .send()
        .await
        .expect("write-token request should complete");
    let denied_create_status = denied_create.status();
    let denied_create_payload: Value = denied_create.json().await.expect("denial should be JSON");
    assert_eq!(denied_create_status, StatusCode::FORBIDDEN);
    assert_eq!(
        denied_create_payload["required_permission"],
        "admin:oauth:admin"
    );
    assert!(oauth_providers
        .get_oauth_provider_config("linuxdo")
        .await
        .expect("provider lookup should succeed")
        .is_none());

    let allowed_create = client
        .put(&provider_url)
        .header(crate::constants::GATEWAY_HEADER, "rust-phase3b")
        .bearer_auth(admin_raw_token)
        .json(&provider_payload(
            "https://configured.example/auth/callback",
        ))
        .send()
        .await
        .expect("oauth-admin request should complete");
    let allowed_create_status = allowed_create.status();
    let allowed_create_body = allowed_create.text().await.expect("body should read");
    assert_eq!(
        allowed_create_status,
        StatusCode::OK,
        "body={allowed_create_body}"
    );

    let denied_change = client
        .put(&provider_url)
        .header(crate::constants::GATEWAY_HEADER, "rust-phase3b")
        .bearer_auth(write_raw_token)
        .json(&provider_payload(
            "https://second-attacker.example/auth/callback",
        ))
        .send()
        .await
        .expect("write-token change request should complete");
    assert_eq!(denied_change.status(), StatusCode::FORBIDDEN);
    assert_eq!(
        oauth_providers
            .get_oauth_provider_config("linuxdo")
            .await
            .expect("provider lookup should succeed")
            .expect("provider should exist")
            .frontend_callback_url,
        "https://configured.example/auth/callback"
    );

    let allowed_session_change = client
        .put(&provider_url)
        .header(crate::constants::GATEWAY_HEADER, "rust-phase3b")
        .header(TRUSTED_ADMIN_USER_ID_HEADER, admin_user.id.as_str())
        .header(TRUSTED_ADMIN_USER_ROLE_HEADER, "admin")
        .header(
            TRUSTED_ADMIN_SESSION_ID_HEADER,
            "session-oauth-callback-admin",
        )
        .json(&provider_payload(
            "https://frontend.example.com/auth/callback",
        ))
        .send()
        .await
        .expect("admin session request should complete");
    let allowed_session_status = allowed_session_change.status();
    let allowed_session_body = allowed_session_change
        .text()
        .await
        .expect("body should read");
    assert_eq!(
        allowed_session_status,
        StatusCode::OK,
        "body={allowed_session_body}"
    );
    assert_eq!(
        oauth_providers
            .get_oauth_provider_config("linuxdo")
            .await
            .expect("provider lookup should succeed")
            .expect("provider should exist")
            .frontend_callback_url,
        "https://frontend.example.com/auth/callback"
    );

    gateway_handle.abort();
}

#[test]
fn gateway_handles_admin_management_token_detail_locally_with_trusted_admin_principal() {
    run_admin_oauth_test(
        "gateway_handles_admin_management_token_detail_locally_with_trusted_admin_principal",
        gateway_handles_admin_management_token_detail_locally_with_trusted_admin_principal_impl,
    );
}

async fn gateway_handles_admin_management_token_detail_locally_with_trusted_admin_principal_impl() {
    let upstream_hits = Arc::new(Mutex::new(0usize));
    let upstream_hits_clone = Arc::clone(&upstream_hits);
    let upstream = Router::new().route(
        "/api/admin/management-tokens/{token_id}",
        any(move |_request: Request| {
            let upstream_hits_inner = Arc::clone(&upstream_hits_clone);
            async move {
                *upstream_hits_inner.lock().expect("mutex should lock") += 1;
                (StatusCode::OK, Body::from("unexpected upstream hit"))
            }
        }),
    );

    let repository = Arc::new(InMemoryManagementTokenRepository::seed(vec![
        sample_management_token("mt-admin-1", "user-1", "alice", true),
    ]));

    let (_upstream_url, upstream_handle) = start_server(upstream).await;
    let gateway = build_router_with_state(
        AppState::new()
            .expect("gateway should build")
            .with_data_state_for_tests(
                GatewayDataState::with_management_token_repository_for_tests(repository),
            ),
    );
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .get(format!(
            "{gateway_url}/api/admin/management-tokens/mt-admin-1"
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
    assert_eq!(payload["id"], "mt-admin-1");
    assert_eq!(payload["user"]["email"], "alice@example.com");
    assert_eq!(payload["usage_count"], 7);
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[test]
fn gateway_creates_updates_and_regenerates_admin_management_token_locally_with_permissions() {
    run_admin_oauth_test(
        "gateway_creates_updates_and_regenerates_admin_management_token_locally_with_permissions",
        gateway_creates_updates_and_regenerates_admin_management_token_locally_with_permissions_impl,
    );
}

async fn gateway_creates_updates_and_regenerates_admin_management_token_locally_with_permissions_impl(
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

    let repository = Arc::new(InMemoryManagementTokenRepository::default());
    let state = AppState::new().expect("gateway should build");
    let admin_user = state
        .create_local_auth_user_with_settings(
            Some("management-admin@example.com".to_string()),
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

    let (upstream_url, upstream_handle) = start_server(upstream).await;
    let gateway = build_router_with_state(state.with_data_state_for_tests(
        GatewayDataState::with_management_token_repository_for_tests(repository.clone()),
    ));
    let (gateway_url, gateway_handle) = start_server(gateway).await;
    let client = reqwest::Client::new();

    let create_response = client
        .post(format!("{gateway_url}/api/admin/management-tokens"))
        .header(crate::constants::GATEWAY_HEADER, "rust-phase3b")
        .header(TRUSTED_ADMIN_USER_ID_HEADER, admin_user.id.as_str())
        .header(TRUSTED_ADMIN_USER_ROLE_HEADER, "admin")
        .header(TRUSTED_ADMIN_SESSION_ID_HEADER, "session-123")
        .json(&json!({
            "name": "admin-token",
            "description": "admin token",
            "allowed_ips": ["127.0.0.1"],
            "permissions": ["admin:usage:read", "admin:pool:write"],
            "expires_at": "2099-01-01T00:00:00Z",
        }))
        .send()
        .await
        .expect("request should succeed");

    let create_status = create_response.status();
    let create_body = create_response.text().await.expect("body should read");
    assert_eq!(create_status, StatusCode::CREATED, "{create_body}");
    let create_payload: serde_json::Value =
        serde_json::from_str(&create_body).expect("json body should parse");
    let token_id = create_payload["data"]["id"]
        .as_str()
        .expect("token id should exist")
        .to_string();
    assert_eq!(create_payload["message"], "Management Token 创建成功");
    assert_eq!(create_payload["data"]["user"]["id"], json!(admin_user.id));
    assert_eq!(
        create_payload["data"]["permissions"],
        json!(["admin:pool:write", "admin:usage:read"])
    );
    let created_secret = create_payload["token"].as_str().unwrap_or_default();
    assert!(created_secret.starts_with("ae-"));

    let update_response = client
        .put(format!(
            "{gateway_url}/api/admin/management-tokens/{token_id}"
        ))
        .header(crate::constants::GATEWAY_HEADER, "rust-phase3b")
        .header(TRUSTED_ADMIN_USER_ID_HEADER, admin_user.id.as_str())
        .header(TRUSTED_ADMIN_USER_ROLE_HEADER, "admin")
        .header(TRUSTED_ADMIN_SESSION_ID_HEADER, "session-123")
        .json(&json!({
            "name": "admin-token-updated",
            "description": null,
            "allowed_ips": null,
            "permissions": ["admin:usage:read"],
            "expires_at": null,
        }))
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(update_response.status(), StatusCode::OK);
    let update_payload: serde_json::Value = update_response
        .json()
        .await
        .expect("json body should parse");
    assert_eq!(update_payload["message"], "更新成功");
    assert_eq!(update_payload["data"]["name"], "admin-token-updated");
    assert_eq!(
        update_payload["data"]["permissions"],
        json!(["admin:usage:read"])
    );
    assert_eq!(
        update_payload["data"]["allowed_ips"],
        serde_json::Value::Null
    );

    let regenerate_response = client
        .post(format!(
            "{gateway_url}/api/admin/management-tokens/{token_id}/regenerate"
        ))
        .header(crate::constants::GATEWAY_HEADER, "rust-phase3b")
        .header(TRUSTED_ADMIN_USER_ID_HEADER, admin_user.id.as_str())
        .header(TRUSTED_ADMIN_USER_ROLE_HEADER, "admin")
        .header(TRUSTED_ADMIN_SESSION_ID_HEADER, "session-123")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(regenerate_response.status(), StatusCode::OK);
    let regenerate_payload: serde_json::Value = regenerate_response
        .json()
        .await
        .expect("json body should parse");
    assert_eq!(regenerate_payload["message"], "Token 已重新生成");
    assert!(regenerate_payload["token"]
        .as_str()
        .unwrap_or_default()
        .starts_with("ae-"));
    assert_eq!(
        repository
            .get_management_token_with_user(&token_id)
            .await
            .expect("lookup should succeed")
            .expect("token should remain")
            .token
            .permissions,
        Some(json!(["admin:usage:read"]))
    );
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
    drop(upstream_url);
}

#[test]
fn gateway_allows_management_token_with_pool_write_for_provider_oauth_batch_import() {
    run_admin_oauth_test(
        "gateway_allows_management_token_with_pool_write_for_provider_oauth_batch_import",
        gateway_allows_management_token_with_pool_write_for_provider_oauth_batch_import_impl,
    );
}

async fn gateway_allows_management_token_with_pool_write_for_provider_oauth_batch_import_impl() {
    let raw_token = "ae-provider-oauth-batch-pool-write";
    let state = AppState::new().expect("gateway should build");
    let admin_user = state
        .create_local_auth_user_with_settings(
            Some("provider-oauth-pool@example.com".to_string()),
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
    let mut management_token = sample_management_token(
        "token-provider-oauth-batch-pool",
        &admin_user.id,
        "provider-oauth-pool",
        true,
    );
    management_token.token.allowed_ips = None;
    management_token.token.permissions = Some(json!([
        "admin:pool:read",
        "admin:pool:write",
        "admin:provider_oauth:admin"
    ]));
    let management_token_repository =
        Arc::new(InMemoryManagementTokenRepository::seed_with_hashes(
            vec![management_token],
            vec![(
                hash_management_token(raw_token),
                "token-provider-oauth-batch-pool".to_string(),
            )],
        ));

    let gateway = build_router_with_state(state.with_data_state_for_tests(
        GatewayDataState::with_management_token_repository_for_tests(management_token_repository),
    ));
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .post(format!(
            "{gateway_url}/api/admin/provider-oauth/providers/provider-123/batch-import"
        ))
        .header(crate::constants::GATEWAY_HEADER, "rust-phase3b")
        .bearer_auth(raw_token)
        .send()
        .await
        .expect("request should succeed");

    let status = response.status();
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{payload}");
    assert_eq!(payload["detail"], "Admin provider OAuth data unavailable");

    gateway_handle.abort();
}

#[test]
fn gateway_rejects_management_token_without_pool_write_for_provider_oauth_batch_import() {
    run_admin_oauth_test(
        "gateway_rejects_management_token_without_pool_write_for_provider_oauth_batch_import",
        gateway_rejects_management_token_without_pool_write_for_provider_oauth_batch_import_impl,
    );
}

async fn gateway_rejects_management_token_without_pool_write_for_provider_oauth_batch_import_impl()
{
    let raw_token = "ae-provider-oauth-batch-pool-denied";
    let state = AppState::new().expect("gateway should build");
    let admin_user = state
        .create_local_auth_user_with_settings(
            Some("provider-oauth-pool-denied@example.com".to_string()),
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
    let mut management_token = sample_management_token(
        "token-provider-oauth-batch-denied",
        &admin_user.id,
        "provider-oauth-denied",
        true,
    );
    management_token.token.allowed_ips = None;
    management_token.token.permissions = Some(json!(["admin:usage:read"]));
    let management_token_repository =
        Arc::new(InMemoryManagementTokenRepository::seed_with_hashes(
            vec![management_token],
            vec![(
                hash_management_token(raw_token),
                "token-provider-oauth-batch-denied".to_string(),
            )],
        ));

    let gateway = build_router_with_state(state.with_data_state_for_tests(
        GatewayDataState::with_management_token_repository_for_tests(management_token_repository),
    ));
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .post(format!(
            "{gateway_url}/api/admin/provider-oauth/providers/provider-123/batch-import"
        ))
        .header(crate::constants::GATEWAY_HEADER, "rust-phase3b")
        .bearer_auth(raw_token)
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["detail"], "management token permission denied");
    assert_eq!(payload["required_permission"], "admin:provider_oauth:admin");
    assert_eq!(payload["route_family"], "provider_oauth_manage");

    gateway_handle.abort();
}

#[test]
fn gateway_deletes_admin_management_token_locally_with_trusted_admin_principal() {
    run_admin_oauth_test(
        "gateway_deletes_admin_management_token_locally_with_trusted_admin_principal",
        gateway_deletes_admin_management_token_locally_with_trusted_admin_principal_impl,
    );
}

async fn gateway_deletes_admin_management_token_locally_with_trusted_admin_principal_impl() {
    let upstream_hits = Arc::new(Mutex::new(0usize));
    let upstream_hits_clone = Arc::clone(&upstream_hits);
    let upstream = Router::new().route(
        "/api/admin/management-tokens/{token_id}",
        any(move |_request: Request| {
            let upstream_hits_inner = Arc::clone(&upstream_hits_clone);
            async move {
                *upstream_hits_inner.lock().expect("mutex should lock") += 1;
                (StatusCode::OK, Body::from("unexpected upstream hit"))
            }
        }),
    );

    let repository = Arc::new(InMemoryManagementTokenRepository::seed(vec![
        sample_management_token("mt-admin-1", "user-1", "alice", true),
    ]));

    let (_upstream_url, upstream_handle) = start_server(upstream).await;
    let gateway = build_router_with_state(
        AppState::new()
            .expect("gateway should build")
            .with_data_state_for_tests(
                GatewayDataState::with_management_token_repository_for_tests(repository.clone()),
            ),
    );
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .delete(format!(
            "{gateway_url}/api/admin/management-tokens/mt-admin-1"
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
    assert_eq!(payload["message"], "删除成功");
    assert_eq!(
        repository
            .get_management_token_with_user("mt-admin-1")
            .await
            .expect("lookup should succeed"),
        None
    );
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[test]
fn gateway_toggles_admin_management_token_locally_with_trusted_admin_principal() {
    run_admin_oauth_test(
        "gateway_toggles_admin_management_token_locally_with_trusted_admin_principal",
        gateway_toggles_admin_management_token_locally_with_trusted_admin_principal_impl,
    );
}

async fn gateway_toggles_admin_management_token_locally_with_trusted_admin_principal_impl() {
    let upstream_hits = Arc::new(Mutex::new(0usize));
    let upstream_hits_clone = Arc::clone(&upstream_hits);
    let upstream = Router::new().route(
        "/api/admin/management-tokens/{token_id}/status",
        any(move |_request: Request| {
            let upstream_hits_inner = Arc::clone(&upstream_hits_clone);
            async move {
                *upstream_hits_inner.lock().expect("mutex should lock") += 1;
                (StatusCode::OK, Body::from("unexpected upstream hit"))
            }
        }),
    );

    let repository = Arc::new(InMemoryManagementTokenRepository::seed(vec![
        sample_management_token("mt-admin-1", "user-1", "alice", true),
    ]));

    let (_upstream_url, upstream_handle) = start_server(upstream).await;
    let gateway = build_router_with_state(
        AppState::new()
            .expect("gateway should build")
            .with_data_state_for_tests(
                GatewayDataState::with_management_token_repository_for_tests(repository.clone()),
            ),
    );
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .patch(format!(
            "{gateway_url}/api/admin/management-tokens/mt-admin-1/status"
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
    assert_eq!(payload["message"], "Token 已禁用");
    assert_eq!(payload["data"]["is_active"], false);
    assert_eq!(
        repository
            .get_management_token_with_user("mt-admin-1")
            .await
            .expect("lookup should succeed")
            .expect("token should remain")
            .token
            .is_active,
        false
    );
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[test]
fn gateway_rejects_partial_management_token_for_admin_management_token_routes() {
    run_admin_oauth_test(
        "gateway_rejects_partial_management_token_for_admin_management_token_routes",
        gateway_rejects_partial_management_token_for_admin_management_token_routes_impl,
    );
}

async fn gateway_rejects_partial_management_token_for_admin_management_token_routes_impl() {
    let upstream_hits = Arc::new(Mutex::new(0usize));
    let upstream_hits_clone = Arc::clone(&upstream_hits);
    let upstream = Router::new().route(
        "/api/admin/management-tokens",
        any(move |_request: Request| {
            let upstream_hits_inner = Arc::clone(&upstream_hits_clone);
            async move {
                *upstream_hits_inner.lock().expect("mutex should lock") += 1;
                (StatusCode::OK, Body::from("unexpected upstream hit"))
            }
        }),
    );

    let raw_token = "ae-management-partial-access";
    let state = AppState::new().expect("gateway should build");
    let admin_user = state
        .create_local_auth_user_with_settings(
            Some("management-partial@example.com".to_string()),
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
    let mut management_token = sample_management_token(
        "mt-admin-partial",
        &admin_user.id,
        "management-partial",
        true,
    );
    management_token.token.allowed_ips = None;
    management_token.token.permissions = Some(json!(["admin:usage:read"]));
    let repository = Arc::new(InMemoryManagementTokenRepository::seed_with_hashes(
        vec![management_token],
        vec![(
            hash_management_token(raw_token),
            "mt-admin-partial".to_string(),
        )],
    ));

    let (_upstream_url, upstream_handle) = start_server(upstream).await;
    let gateway = build_router_with_state(state.with_data_state_for_tests(
        GatewayDataState::with_management_token_repository_for_tests(repository),
    ));
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .get(format!("{gateway_url}/api/admin/management-tokens"))
        .header(crate::constants::GATEWAY_HEADER, "rust-phase3b")
        .bearer_auth(raw_token)
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["detail"], "management token permission denied");
    assert_eq!(
        payload["required_permission"],
        "admin:management_tokens:read"
    );
    assert_eq!(payload["route_family"], "management_tokens_manage");
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}
