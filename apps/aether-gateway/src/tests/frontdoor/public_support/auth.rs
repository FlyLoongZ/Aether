use super::*;

#[tokio::test]
async fn gateway_handles_public_auth_modules_status_without_proxying_upstream() {
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

    let auth_module_repository = Arc::new(InMemoryAuthModuleReadRepository::seed(
        vec![StoredOAuthProviderModuleConfig::new(
            "linuxdo".to_string(),
            "Linux DO".to_string(),
            "client-id".to_string(),
            Some("encrypted-secret".to_string()),
            "https://example.com/oauth/callback".to_string(),
        )
        .expect("oauth provider should build")],
        Some(StoredLdapModuleConfig {
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
        }),
    ));

    let (_upstream_url, upstream_handle) = start_server(upstream).await;
    let gateway = build_router_with_state(
        AppState::new()
            .expect("gateway should build")
            .with_data_state_for_tests(
                crate::data::GatewayDataState::with_auth_module_reader_for_tests(
                    auth_module_repository,
                ),
            ),
    );
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .get(format!("{gateway_url}/api/modules/auth-status"))
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(
        payload,
        json!([
            {
                "name": "oauth",
                "display_name": "OAuth 登录",
                "active": false
            },
            {
                "name": "ldap",
                "display_name": "LDAP 认证",
                "active": false
            }
        ])
    );
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_handles_auth_registration_settings_without_proxying_upstream() {
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

    let data_state = crate::data::GatewayDataState::with_auth_module_reader_for_tests(Arc::new(
        InMemoryAuthModuleReadRepository::seed(Vec::new(), None),
    ))
    .with_system_config_values_for_tests(vec![
        ("enable_registration".to_string(), json!(true)),
        ("require_email_verification".to_string(), json!(true)),
        ("smtp_host".to_string(), json!("smtp.example.com")),
        ("smtp_from_email".to_string(), json!("noreply@example.com")),
        ("password_policy_level".to_string(), json!("strong")),
        ("turnstile_enabled".to_string(), json!(true)),
        ("turnstile_site_key".to_string(), json!("site-public-key")),
        (
            "turnstile_secret_key".to_string(),
            json!("secret-private-key"),
        ),
        (
            "registration_privacy_policy_enabled".to_string(),
            json!(true),
        ),
        (
            "registration_privacy_policy_format".to_string(),
            json!("html"),
        ),
        (
            "registration_privacy_policy_content".to_string(),
            json!("<p>Policy</p>"),
        ),
        (
            "registration_privacy_policy_version".to_string(),
            json!("2026-05-16"),
        ),
    ]);

    let (_upstream_url, upstream_handle) = start_server(upstream).await;
    let gateway = build_router_with_state(
        AppState::new()
            .expect("gateway should build")
            .with_data_state_for_tests(data_state),
    );
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .get(format!("{gateway_url}/api/auth/registration-settings"))
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(
        payload,
        json!({
            "enable_registration": true,
            "require_email_verification": true,
            "email_configured": true,
            "password_policy_level": "strong",
            "turnstile_enabled": true,
            "turnstile_site_key": "site-public-key",
            "turnstile_required_actions": ["send_verification_code", "register"],
            "privacy_policy": {
                "enabled": true,
                "format": "html",
                "content": "<p>Policy</p>",
                "version": "2026-05-16",
            },
        })
    );
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_handles_auth_settings_without_proxying_upstream() {
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

    let auth_module_repository = Arc::new(InMemoryAuthModuleReadRepository::seed(
        Vec::new(),
        Some(StoredLdapModuleConfig {
            server_url: "ldaps://ldap.example.com".to_string(),
            bind_dn: "cn=admin,dc=example,dc=com".to_string(),
            bind_password_encrypted: Some("encrypted-password".to_string()),
            base_dn: "dc=example,dc=com".to_string(),
            user_search_filter: Some("(uid={username})".to_string()),
            username_attr: Some("uid".to_string()),
            email_attr: Some("mail".to_string()),
            display_name_attr: Some("displayName".to_string()),
            is_enabled: true,
            is_exclusive: true,
            use_starttls: true,
            connect_timeout: Some(10),
        }),
    ));
    let data_state =
        crate::data::GatewayDataState::with_auth_module_reader_for_tests(auth_module_repository)
            .with_system_config_values_for_tests(vec![(
                "module.ldap.enabled".to_string(),
                json!(true),
            )]);

    let (_upstream_url, upstream_handle) = start_server(upstream).await;
    let gateway = build_router_with_state(
        AppState::new()
            .expect("gateway should build")
            .with_data_state_for_tests(data_state),
    );
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .get(format!("{gateway_url}/api/auth/settings"))
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(
        payload,
        json!({
            "local_enabled": false,
            "ldap_enabled": true,
            "ldap_exclusive": true,
        })
    );
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_handles_authenticated_test_connection_without_hitting_fallback_probe() {
    let fallback_probe_hits = Arc::new(Mutex::new(0usize));
    let fallback_probe_hits_clone = Arc::clone(&fallback_probe_hits);
    let fallback_probe = Router::new().route(
        "/{*path}",
        any(move |_request: Request| {
            let fallback_probe_hits_inner = Arc::clone(&fallback_probe_hits_clone);
            async move {
                *fallback_probe_hits_inner.lock().expect("mutex should lock") += 1;
                (StatusCode::OK, Body::from("proxied"))
            }
        }),
    );

    let provider_hits = Arc::new(Mutex::new(0usize));
    let provider_hits_clone = Arc::clone(&provider_hits);
    let provider = Router::new().route(
        "/chat/completions",
        any(move |request: Request| {
            let provider_hits_inner = Arc::clone(&provider_hits_clone);
            async move {
                *provider_hits_inner.lock().expect("mutex should lock") += 1;
                let headers = request.headers().clone();
                let body = to_bytes(request.into_body(), usize::MAX)
                    .await
                    .expect("body should read");
                let body_json: serde_json::Value =
                    serde_json::from_slice(&body).expect("json body should parse");
                assert_eq!(
                    headers
                        .get("authorization")
                        .and_then(|value| value.to_str().ok()),
                    Some("Bearer sk-test-openai"),
                );
                assert_eq!(body_json["model"], "gpt-5");
                assert_eq!(body_json["messages"][0]["content"], "Health check");
                assert!(
                    body_json.get("max_tokens").is_none(),
                    "public OpenAI-compatible test connection must not force a tiny max_tokens value"
                );
                Json(json!({"id": "resp_local_test"})).into_response()
            }
        }),
    );

    let (provider_url, provider_handle) = start_server(provider).await;
    let provider_catalog_repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![sample_provider("provider-1", "openai", 10)],
        vec![sample_endpoint(
            "endpoint-1",
            "provider-1",
            "openai:chat",
            &provider_url,
        )],
        vec![sample_key(
            "key-1",
            "provider-1",
            "openai:chat",
            "sk-test-openai",
        )],
    ));

    let (_unused_fallback_probe_url, fallback_probe_handle) = start_server(fallback_probe).await;
    let now = Utc::now();
    let mut user = sample_auth_user(now);
    user.role = "admin".to_string();
    let access_token = build_test_auth_token(
        "access",
        serde_json::Map::from_iter([
            ("user_id".to_string(), json!(user.id)),
            ("role".to_string(), json!(user.role)),
            (
                "created_at".to_string(),
                json!(user.created_at.map(|value| value.to_rfc3339())),
            ),
            (
                "session_id".to_string(),
                json!("session-test-connection-openai"),
            ),
        ]),
        now + chrono::Duration::hours(1),
    );
    let user_repository = Arc::new(InMemoryUserReadRepository::seed_auth_users(vec![user]));
    let gateway = build_router_with_state(
        AppState::new()
            .expect("gateway should build")
            .with_data_state_for_tests(
                crate::data::GatewayDataState::with_provider_catalog_repository_for_tests(
                    provider_catalog_repository,
                )
                .with_encryption_key_for_tests(DEVELOPMENT_ENCRYPTION_KEY)
                .with_user_reader(user_repository),
            )
            .with_auth_session_for_tests(sample_auth_session(
                "user-auth-1",
                "session-test-connection-openai",
                "device-test-connection-openai",
                "refresh-token-placeholder",
                now,
            )),
    );
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .get(format!(
            "{gateway_url}/v1/test-connection?provider=provider-1&model=gpt-5&api_format=openai:chat"
        ))
        .header("authorization", format!("Bearer {access_token}"))
        .header("x-client-device-id", "device-test-connection-openai")
        .header("user-agent", "AetherTest/1.0")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["status"], "success");
    assert!(payload.get("provider").is_none());
    assert_eq!(payload["provider_id"], "provider-1");
    assert_eq!(payload["api_format"], "openai:chat");
    assert_eq!(payload["response_id"], "resp_local_test");
    assert_eq!(*provider_hits.lock().expect("mutex should lock"), 1);
    assert_eq!(*fallback_probe_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    fallback_probe_handle.abort();
    provider_handle.abort();
}

#[tokio::test]
async fn gateway_changes_users_me_password_locally_without_proxying_upstream() {
    let now = Utc::now();
    let user = sample_auth_user(now);
    let access_token = build_test_auth_token(
        "access",
        serde_json::Map::from_iter([
            ("user_id".to_string(), json!(user.id)),
            ("role".to_string(), json!(user.role)),
            (
                "created_at".to_string(),
                json!(user.created_at.map(|value| value.to_rfc3339())),
            ),
            (
                "session_id".to_string(),
                json!("session-users-me-password-current"),
            ),
        ]),
        now + chrono::Duration::hours(1),
    );
    let current = sample_auth_session(
        "user-auth-1",
        "session-users-me-password-current",
        "device-users-me-password-current",
        "refresh-token-current",
        now,
    );
    let other = sample_auth_session(
        "user-auth-1",
        "session-users-me-password-other",
        "device-users-me-password-other",
        "refresh-token-other",
        now - chrono::Duration::hours(6),
    );
    let (gateway_url, upstream_hits, gateway_handle, upstream_handle) =
        start_auth_gateway_with_state(
            user,
            sample_auth_wallet("user-auth-1", now),
            [current, other],
        )
        .await;
    let client = reqwest::Client::new();

    let update_response = client
        .patch(format!("{gateway_url}/api/users/me/password"))
        .header("authorization", format!("Bearer {access_token}"))
        .header("x-client-device-id", "device-users-me-password-current")
        .header("user-agent", "AetherTest/1.0")
        .json(&json!({
            "current_password": "secret123",
            "new_password": "Secret456!",
        }))
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(update_response.status(), StatusCode::OK);
    let clear_cookie = update_response
        .headers()
        .get(http::header::SET_COOKIE)
        .and_then(|value| value.to_str().ok())
        .expect("password change should clear the refresh cookie")
        .to_string();
    let update_payload: serde_json::Value = update_response
        .json()
        .await
        .expect("json body should parse");
    assert_eq!(update_payload["message"], "密码修改成功");
    assert!(clear_cookie.contains("aether_refresh_token="));
    assert!(clear_cookie.contains("Max-Age=0"));

    let sessions_response = client
        .get(format!("{gateway_url}/api/users/me/sessions"))
        .header("authorization", format!("Bearer {access_token}"))
        .header("x-client-device-id", "device-users-me-password-current")
        .header("user-agent", "AetherTest/1.0")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(sessions_response.status(), StatusCode::UNAUTHORIZED);
    let sessions_payload: serde_json::Value = sessions_response
        .json()
        .await
        .expect("json body should parse");
    assert_eq!(sessions_payload["detail"], "登录会话已失效，请重新登录");
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_returns_service_unavailable_for_users_me_password_change_without_credential_storage(
) {
    let now = Utc::now();
    let user = sample_auth_user(now);
    let access_token = build_test_auth_token(
        "access",
        serde_json::Map::from_iter([
            ("user_id".to_string(), json!(user.id)),
            ("role".to_string(), json!(user.role)),
            (
                "created_at".to_string(),
                json!(user.created_at.map(|value| value.to_rfc3339())),
            ),
            (
                "session_id".to_string(),
                json!("session-users-me-password-unavailable"),
            ),
        ]),
        now + chrono::Duration::hours(1),
    );
    let user_repository =
        Arc::new(InMemoryUserReadRepository::seed_auth_users(vec![user]).read_only());
    let (gateway_url, upstream_hits, gateway_handle, upstream_handle) =
        start_auth_gateway_with_builder(|| {
            let data_state =
                crate::data::GatewayDataState::with_user_reader_for_tests(user_repository);
            AppState::new()
                .expect("gateway should build")
                .with_data_state_for_tests(data_state)
                .without_auth_user_store_for_tests()
                .with_auth_sessions_for_tests([sample_auth_session(
                    "user-auth-1",
                    "session-users-me-password-unavailable",
                    "device-users-me-password-unavailable",
                    "refresh-token-placeholder",
                    now,
                )])
        })
        .await;

    let response = reqwest::Client::new()
        .patch(format!("{gateway_url}/api/users/me/password"))
        .header("authorization", format!("Bearer {access_token}"))
        .header("x-client-device-id", "device-users-me-password-unavailable")
        .header("user-agent", "AetherTest/1.0")
        .json(&json!({
            "current_password": "secret123",
            "new_password": "Secret456!",
        }))
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["detail"], "服务暂不可用，请稍后重试");
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_handles_users_me_usage_without_legacy_api_key_name_fallback_when_auth_reader_exists(
) {
    let now = Utc::now();
    let user = sample_auth_user(now);
    let access_token = build_test_auth_token(
        "access",
        serde_json::Map::from_iter([
            ("user_id".to_string(), json!(user.id)),
            ("role".to_string(), json!(user.role)),
            (
                "created_at".to_string(),
                json!(user.created_at.map(|value| value.to_rfc3339())),
            ),
            (
                "session_id".to_string(),
                json!("session-users-me-usage-no-legacy-fallback"),
            ),
        ]),
        now + chrono::Duration::hours(1),
    );
    let usage_repository = Arc::new(InMemoryUsageReadRepository::seed(vec![
        sample_user_usage_audit(
            "usage-users-me-completed-no-legacy-fallback",
            "req-users-me-completed-no-legacy-fallback",
            "user-auth-1",
            "gpt-4.1",
            "OpenAI",
            "completed",
            now - chrono::Duration::minutes(20),
        ),
    ]));
    let (gateway_url, upstream_hits, gateway_handle, upstream_handle) =
        start_auth_gateway_with_builder(|| {
            let data_state = crate::data::GatewayDataState::with_user_wallet_and_usage_for_tests(
                Arc::new(InMemoryUserReadRepository::seed_auth_users(vec![
                    user.clone()
                ])),
                Arc::new(InMemoryWalletRepository::seed(vec![sample_auth_wallet(
                    "user-auth-1",
                    now,
                )])),
                Arc::clone(&usage_repository),
            )
            .with_auth_api_key_reader(Arc::new(InMemoryAuthApiKeySnapshotRepository::seed(vec![])));
            AppState::new()
                .expect("gateway should build")
                .with_data_state_for_tests(data_state)
                .with_auth_sessions_for_tests([sample_auth_session(
                    "user-auth-1",
                    "session-users-me-usage-no-legacy-fallback",
                    "device-users-me-usage-no-legacy-fallback",
                    "refresh-token-users-me-usage-no-legacy-fallback",
                    now,
                )])
        })
        .await;

    let response = reqwest::Client::new()
        .get(format!(
            "{gateway_url}/api/users/me/usage?limit=10&offset=0&search=default"
        ))
        .header("authorization", format!("Bearer {access_token}"))
        .header(
            "x-client-device-id",
            "device-users-me-usage-no-legacy-fallback",
        )
        .header("user-agent", "AetherTest/1.0")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["pagination"]["total"], 0);
    assert_eq!(
        payload["records"].as_array().expect("records array").len(),
        0
    );
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_handles_users_me_sessions_locally_without_proxying_upstream() {
    let now = Utc::now();
    let user = sample_auth_user(now);
    let access_token = build_test_auth_token(
        "access",
        serde_json::Map::from_iter([
            ("user_id".to_string(), json!(user.id)),
            ("role".to_string(), json!(user.role)),
            (
                "created_at".to_string(),
                json!(user.created_at.map(|value| value.to_rfc3339())),
            ),
            ("session_id".to_string(), json!("session-users-me-current")),
        ]),
        now + chrono::Duration::hours(1),
    );
    let current = sample_auth_session(
        "user-auth-1",
        "session-users-me-current",
        "device-users-me-current",
        "refresh-token-current",
        now,
    );
    let mut other = sample_auth_session(
        "user-auth-1",
        "session-users-me-other",
        "device-users-me-other",
        "refresh-token-other",
        now - chrono::Duration::hours(6),
    );
    other.ip_address = Some("10.0.0.2".to_string());
    let mut revoked = sample_auth_session(
        "user-auth-1",
        "session-users-me-revoked",
        "device-users-me-revoked",
        "refresh-token-revoked",
        now - chrono::Duration::hours(12),
    );
    revoked.revoked_at = Some(now);
    let mut expired = sample_auth_session(
        "user-auth-1",
        "session-users-me-expired",
        "device-users-me-expired",
        "refresh-token-expired",
        now - chrono::Duration::hours(24),
    );
    expired.expires_at = Some(now - chrono::Duration::minutes(1));

    let (gateway_url, upstream_hits, gateway_handle, upstream_handle) =
        start_auth_gateway_with_state(
            user,
            sample_auth_wallet("user-auth-1", now),
            [current, other, revoked, expired],
        )
        .await;

    let response = reqwest::Client::new()
        .get(format!("{gateway_url}/api/users/me/sessions"))
        .header("authorization", format!("Bearer {access_token}"))
        .header("x-client-device-id", "device-users-me-current")
        .header("user-agent", "AetherTest/1.0")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get(http::header::CACHE_CONTROL)
            .and_then(|value| value.to_str().ok()),
        Some("no-store")
    );
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    let sessions = payload.as_array().expect("sessions should be array");
    assert_eq!(sessions.len(), 2);
    assert_eq!(sessions[0]["id"], "session-users-me-current");
    assert_eq!(sessions[0]["is_current"], true);
    assert_eq!(sessions[0]["device_label"], "未知设备");
    assert_eq!(sessions[1]["id"], "session-users-me-other");
    assert_eq!(sessions[1]["is_current"], false);
    assert_eq!(sessions[1]["ip_address"], "10.0.0.2");
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_revokes_other_users_me_sessions_locally_without_proxying_upstream() {
    let now = Utc::now();
    let user = sample_auth_user(now);
    let access_token = build_test_auth_token(
        "access",
        serde_json::Map::from_iter([
            ("user_id".to_string(), json!(user.id)),
            ("role".to_string(), json!(user.role)),
            (
                "created_at".to_string(),
                json!(user.created_at.map(|value| value.to_rfc3339())),
            ),
            ("session_id".to_string(), json!("session-users-me-current")),
        ]),
        now + chrono::Duration::hours(1),
    );
    let current = sample_auth_session(
        "user-auth-1",
        "session-users-me-current",
        "device-users-me-current",
        "refresh-token-current",
        now,
    );
    let other = sample_auth_session(
        "user-auth-1",
        "session-users-me-other",
        "device-users-me-other",
        "refresh-token-other",
        now - chrono::Duration::hours(6),
    );

    let (gateway_url, upstream_hits, gateway_handle, upstream_handle) =
        start_auth_gateway_with_state(
            user,
            sample_auth_wallet("user-auth-1", now),
            [current, other],
        )
        .await;

    let client = reqwest::Client::new();
    let delete_response = client
        .delete(format!("{gateway_url}/api/users/me/sessions/others"))
        .header("authorization", format!("Bearer {access_token}"))
        .header("x-client-device-id", "device-users-me-current")
        .header("user-agent", "AetherTest/1.0")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(delete_response.status(), StatusCode::OK);
    let delete_payload: serde_json::Value = delete_response
        .json()
        .await
        .expect("json body should parse");
    assert_eq!(delete_payload["message"], "其他设备已退出登录");
    assert_eq!(delete_payload["revoked_count"], 1);

    let get_response = client
        .get(format!("{gateway_url}/api/users/me/sessions"))
        .header("authorization", format!("Bearer {access_token}"))
        .header("x-client-device-id", "device-users-me-current")
        .header("user-agent", "AetherTest/1.0")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(get_response.status(), StatusCode::OK);
    let get_payload: serde_json::Value = get_response.json().await.expect("json body should parse");
    let sessions = get_payload.as_array().expect("sessions should be array");
    assert_eq!(sessions.len(), 1);
    assert_eq!(sessions[0]["id"], "session-users-me-current");
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_revokes_users_me_session_locally_without_proxying_upstream() {
    let now = Utc::now();
    let user = sample_auth_user(now);
    let access_token = build_test_auth_token(
        "access",
        serde_json::Map::from_iter([
            ("user_id".to_string(), json!(user.id)),
            ("role".to_string(), json!(user.role)),
            (
                "created_at".to_string(),
                json!(user.created_at.map(|value| value.to_rfc3339())),
            ),
            ("session_id".to_string(), json!("session-users-me-current")),
        ]),
        now + chrono::Duration::hours(1),
    );
    let current = sample_auth_session(
        "user-auth-1",
        "session-users-me-current",
        "device-users-me-current",
        "refresh-token-current",
        now,
    );
    let other = sample_auth_session(
        "user-auth-1",
        "session-users-me-other",
        "device-users-me-other",
        "refresh-token-other",
        now - chrono::Duration::hours(6),
    );

    let (gateway_url, upstream_hits, gateway_handle, upstream_handle) =
        start_auth_gateway_with_state(
            user,
            sample_auth_wallet("user-auth-1", now),
            [current, other],
        )
        .await;

    let client = reqwest::Client::new();
    let delete_response = client
        .delete(format!(
            "{gateway_url}/api/users/me/sessions/session-users-me-other"
        ))
        .header("authorization", format!("Bearer {access_token}"))
        .header("x-client-device-id", "device-users-me-current")
        .header("user-agent", "AetherTest/1.0")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(delete_response.status(), StatusCode::OK);
    let delete_payload: serde_json::Value = delete_response
        .json()
        .await
        .expect("json body should parse");
    assert_eq!(delete_payload["message"], "设备已退出登录");

    let get_response = client
        .get(format!("{gateway_url}/api/users/me/sessions"))
        .header("authorization", format!("Bearer {access_token}"))
        .header("x-client-device-id", "device-users-me-current")
        .header("user-agent", "AetherTest/1.0")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(get_response.status(), StatusCode::OK);
    let get_payload: serde_json::Value = get_response.json().await.expect("json body should parse");
    let sessions = get_payload.as_array().expect("sessions should be array");
    assert_eq!(sessions.len(), 1);
    assert_eq!(sessions[0]["id"], "session-users-me-current");
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_updates_users_me_session_label_locally_without_proxying_upstream() {
    let now = Utc::now();
    let user = sample_auth_user(now);
    let access_token = build_test_auth_token(
        "access",
        serde_json::Map::from_iter([
            ("user_id".to_string(), json!(user.id)),
            ("role".to_string(), json!(user.role)),
            (
                "created_at".to_string(),
                json!(user.created_at.map(|value| value.to_rfc3339())),
            ),
            ("session_id".to_string(), json!("session-users-me-current")),
        ]),
        now + chrono::Duration::hours(1),
    );
    let current = sample_auth_session(
        "user-auth-1",
        "session-users-me-current",
        "device-users-me-current",
        "refresh-token-current",
        now,
    );
    let (gateway_url, upstream_hits, gateway_handle, upstream_handle) =
        start_auth_gateway_with_state(user, sample_auth_wallet("user-auth-1", now), [current])
            .await;

    let response = reqwest::Client::new()
        .patch(format!(
            "{gateway_url}/api/users/me/sessions/session-users-me-current"
        ))
        .header("authorization", format!("Bearer {access_token}"))
        .header("x-client-device-id", "device-users-me-current")
        .header("user-agent", "AetherTest/1.0")
        .json(&json!({ "device_label": "我的 MacBook" }))
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["id"], "session-users-me-current");
    assert_eq!(payload["device_label"], "我的 MacBook");
    assert_eq!(payload["is_current"], true);
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    let sessions_response = reqwest::Client::new()
        .get(format!("{gateway_url}/api/users/me/sessions"))
        .header("authorization", format!("Bearer {access_token}"))
        .header("x-client-device-id", "device-users-me-current")
        .header("user-agent", "AetherTest/1.0")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(sessions_response.status(), StatusCode::OK);
    let sessions_payload: serde_json::Value = sessions_response
        .json()
        .await
        .expect("json body should parse");
    let sessions = sessions_payload
        .as_array()
        .expect("sessions should be array");
    assert_eq!(sessions[0]["device_label"], "我的 MacBook");

    gateway_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_cannot_update_or_revoke_another_users_session() {
    let now = Utc::now();
    let user = sample_auth_user(now);
    let mut foreign_user = user.clone();
    foreign_user.id = "user-auth-2".to_string();
    foreign_user.email = Some("bob@example.com".to_string());
    foreign_user.username = "bob".to_string();
    let access_token = build_test_auth_token(
        "access",
        serde_json::Map::from_iter([
            ("user_id".to_string(), json!(user.id)),
            ("role".to_string(), json!(user.role)),
            (
                "created_at".to_string(),
                json!(user.created_at.map(|value| value.to_rfc3339())),
            ),
            (
                "session_id".to_string(),
                json!("session-users-me-owner-current"),
            ),
        ]),
        now + chrono::Duration::hours(1),
    );
    let foreign_access_token = build_test_auth_token(
        "access",
        serde_json::Map::from_iter([
            ("user_id".to_string(), json!(foreign_user.id)),
            ("role".to_string(), json!(foreign_user.role)),
            (
                "created_at".to_string(),
                json!(foreign_user.created_at.map(|value| value.to_rfc3339())),
            ),
            ("session_id".to_string(), json!("session-users-me-foreign")),
        ]),
        now + chrono::Duration::hours(1),
    );
    let owner_session = sample_auth_session(
        "user-auth-1",
        "session-users-me-owner-current",
        "device-users-me-owner-current",
        "refresh-token-owner-current",
        now,
    );
    let mut foreign_session = sample_auth_session(
        "user-auth-2",
        "session-users-me-foreign",
        "device-users-me-foreign",
        "refresh-token-foreign",
        now,
    );
    foreign_session.device_label = Some("Bob phone".to_string());
    let user_repository = Arc::new(InMemoryUserReadRepository::seed_auth_users(vec![
        user,
        foreign_user,
    ]));

    let (gateway_url, upstream_hits, gateway_handle, upstream_handle) =
        start_auth_gateway_with_builder(|| {
            AppState::new()
                .expect("gateway should build")
                .with_data_state_for_tests(GatewayDataState::with_user_reader_for_tests(
                    user_repository,
                ))
                .with_auth_sessions_for_tests([owner_session, foreign_session])
        })
        .await;
    let client = reqwest::Client::new();

    let update_response = client
        .patch(format!(
            "{gateway_url}/api/users/me/sessions/session-users-me-foreign"
        ))
        .header("authorization", format!("Bearer {access_token}"))
        .header("x-client-device-id", "device-users-me-owner-current")
        .header("user-agent", "AetherTest/1.0")
        .json(&json!({ "device_label": "Compromised" }))
        .send()
        .await
        .expect("cross-user update request should complete");
    assert_eq!(update_response.status(), StatusCode::NOT_FOUND);

    let revoke_response = client
        .delete(format!(
            "{gateway_url}/api/users/me/sessions/session-users-me-foreign"
        ))
        .header("authorization", format!("Bearer {access_token}"))
        .header("x-client-device-id", "device-users-me-owner-current")
        .header("user-agent", "AetherTest/1.0")
        .send()
        .await
        .expect("cross-user revoke request should complete");
    assert_eq!(revoke_response.status(), StatusCode::NOT_FOUND);

    let foreign_sessions_response = client
        .get(format!("{gateway_url}/api/users/me/sessions"))
        .header("authorization", format!("Bearer {foreign_access_token}"))
        .header("x-client-device-id", "device-users-me-foreign")
        .header("user-agent", "AetherTest/1.0")
        .send()
        .await
        .expect("foreign owner session request should complete");
    assert_eq!(foreign_sessions_response.status(), StatusCode::OK);
    let foreign_sessions: serde_json::Value = foreign_sessions_response
        .json()
        .await
        .expect("json body should parse");
    let foreign_sessions = foreign_sessions
        .as_array()
        .expect("sessions should be an array");
    assert_eq!(foreign_sessions.len(), 1);
    assert_eq!(foreign_sessions[0]["id"], "session-users-me-foreign");
    assert_eq!(foreign_sessions[0]["device_label"], "Bob phone");
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_rejects_invalid_users_me_session_delete_path_as_local_not_found_without_hitting_upstream(
) {
    let now = Utc::now();
    let user = sample_auth_user(now);
    let access_token = build_test_auth_token(
        "access",
        serde_json::Map::from_iter([
            ("user_id".to_string(), json!(user.id)),
            ("role".to_string(), json!(user.role)),
            (
                "created_at".to_string(),
                json!(user.created_at.map(|value| value.to_rfc3339())),
            ),
            ("session_id".to_string(), json!("session-users-me-current")),
        ]),
        now + chrono::Duration::hours(1),
    );
    let current = sample_auth_session(
        "user-auth-1",
        "session-users-me-current",
        "device-users-me-current",
        "refresh-token-current",
        now,
    );

    let (gateway_url, upstream_hits, gateway_handle, upstream_handle) =
        start_auth_gateway_with_state(user, sample_auth_wallet("user-auth-1", now), [current])
            .await;

    let response = reqwest::Client::new()
        .delete(format!("{gateway_url}/api/users/me/sessions/"))
        .header("authorization", format!("Bearer {access_token}"))
        .header("x-client-device-id", "device-users-me-current")
        .header("user-agent", "AetherTest/1.0")
        .send()
        .await
        .expect("request should succeed");

    assert_local_route_not_found_response(response).await;
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_rejects_invalid_users_me_session_update_path_as_local_not_found_without_hitting_upstream(
) {
    let now = Utc::now();
    let user = sample_auth_user(now);
    let access_token = build_test_auth_token(
        "access",
        serde_json::Map::from_iter([
            ("user_id".to_string(), json!(user.id)),
            ("role".to_string(), json!(user.role)),
            (
                "created_at".to_string(),
                json!(user.created_at.map(|value| value.to_rfc3339())),
            ),
            ("session_id".to_string(), json!("session-users-me-current")),
        ]),
        now + chrono::Duration::hours(1),
    );
    let current = sample_auth_session(
        "user-auth-1",
        "session-users-me-current",
        "device-users-me-current",
        "refresh-token-current",
        now,
    );

    let (gateway_url, upstream_hits, gateway_handle, upstream_handle) =
        start_auth_gateway_with_state(user, sample_auth_wallet("user-auth-1", now), [current])
            .await;

    let response = reqwest::Client::new()
        .patch(format!("{gateway_url}/api/users/me/sessions/"))
        .header("authorization", format!("Bearer {access_token}"))
        .header("x-client-device-id", "device-users-me-current")
        .header("user-agent", "AetherTest/1.0")
        .json(&json!({ "device_label": "我的 MacBook" }))
        .send()
        .await
        .expect("request should succeed");

    assert_local_route_not_found_response(response).await;
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_handles_auth_login_locally_without_proxying_upstream() {
    let now = Utc::now();
    let user = sample_auth_user(now);
    let (gateway_url, upstream_hits, gateway_handle, upstream_handle) =
        start_auth_gateway_with_state(user, sample_auth_wallet("user-auth-1", now), []).await;

    let client = reqwest::Client::new();
    let response = client
        .post(format!("{gateway_url}/api/auth/login"))
        .header("x-client-device-id", "device-auth-login")
        .header("user-agent", "AetherTest/1.0")
        .json(&json!({
            "email": "alice@example.com",
            "password": "secret123",
            "auth_type": "local",
        }))
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get(http::header::CACHE_CONTROL)
            .and_then(|value| value.to_str().ok()),
        Some("no-store")
    );
    let set_cookie = response
        .headers()
        .get(reqwest::header::SET_COOKIE)
        .and_then(|value| value.to_str().ok())
        .map(ToOwned::to_owned)
        .expect("set-cookie should exist");
    assert!(set_cookie.contains("aether_refresh_token="));
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    let access_token = payload["access_token"]
        .as_str()
        .expect("access token should exist")
        .to_string();
    assert_eq!(payload["user_id"], "user-auth-1");
    assert_eq!(payload["email"], "alice@example.com");
    assert_eq!(payload["username"], "alice");
    assert_eq!(payload["role"], "user");

    let me_response = client
        .get(format!("{gateway_url}/api/auth/me"))
        .header("authorization", format!("Bearer {access_token}"))
        .header("x-client-device-id", "device-auth-login")
        .header("user-agent", "AetherTest/1.0")
        .send()
        .await
        .expect("me request should succeed");

    assert_eq!(me_response.status(), StatusCode::OK);
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_auth_login_rejects_query_only_device_binding_without_creating_session() {
    let now = Utc::now();
    let user = sample_auth_user(now);
    let (gateway_url, upstream_hits, gateway_handle, upstream_handle) =
        start_auth_gateway_with_state(user, sample_auth_wallet("user-auth-1", now), []).await;

    let client = reqwest::Client::new();
    let rejected = client
        .post(format!(
            "{gateway_url}/api/auth/login?client_device_id=device-auth-login-csrf"
        ))
        .header("user-agent", "AetherTest/1.0")
        .json(&json!({
            "email": "alice@example.com",
            "password": "secret123",
            "auth_type": "local",
        }))
        .send()
        .await
        .expect("query-only login request should complete");

    assert_eq!(rejected.status(), StatusCode::BAD_REQUEST);
    assert!(!rejected.headers().contains_key(http::header::SET_COOKIE));

    let legitimate = client
        .post(format!("{gateway_url}/api/auth/login"))
        .header("x-client-device-id", "device-auth-login-csrf")
        .header("user-agent", "AetherTest/1.0")
        .json(&json!({
            "email": "alice@example.com",
            "password": "secret123",
            "auth_type": "local",
        }))
        .send()
        .await
        .expect("legitimate login should complete");
    assert_eq!(legitimate.status(), StatusCode::OK);
    assert!(legitimate.headers().contains_key(http::header::SET_COOKIE));
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_rejects_unsupported_auth_login_type_locally_without_proxying_upstream() {
    let now = Utc::now();
    let user = sample_auth_user(now);
    let (gateway_url, upstream_hits, gateway_handle, upstream_handle) =
        start_auth_gateway_with_state(user, sample_auth_wallet("user-auth-1", now), []).await;

    let response = reqwest::Client::new()
        .post(format!("{gateway_url}/api/auth/login"))
        .header("x-client-device-id", "device-auth-login-unsupported")
        .header("user-agent", "AetherTest/1.0")
        .json(&json!({
            "email": "alice@example.com",
            "password": "secret123",
            "auth_type": "oauth",
        }))
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["detail"], "不支持的认证类型");
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_handles_auth_ldap_login_locally_without_proxying_upstream() {
    let encrypted_bind_password =
        encrypt_python_fernet_plaintext(DEVELOPMENT_ENCRYPTION_KEY, "bind-secret")
            .expect("bind password should encrypt");
    let (gateway_url, upstream_hits, gateway_handle, upstream_handle) =
        start_auth_gateway_with_builder(|| {
            let auth_module_repository = Arc::new(InMemoryAuthModuleReadRepository::seed(
                Vec::<StoredOAuthProviderModuleConfig>::new(),
                Some(StoredLdapModuleConfig {
                    server_url: "mockldap://ldap.example.com".to_string(),
                    bind_dn: "cn=admin,dc=example,dc=com".to_string(),
                    bind_password_encrypted: Some(encrypted_bind_password),
                    base_dn: "dc=example,dc=com".to_string(),
                    user_search_filter: Some("(uid={username})".to_string()),
                    username_attr: Some("uid".to_string()),
                    email_attr: Some("mail".to_string()),
                    display_name_attr: Some("displayName".to_string()),
                    is_enabled: true,
                    is_exclusive: false,
                    use_starttls: false,
                    connect_timeout: Some(5),
                }),
            ));
            let data_state = crate::data::GatewayDataState::disabled()
                .with_encryption_key_for_tests(DEVELOPMENT_ENCRYPTION_KEY)
                .attach_auth_module_repository_for_tests(auth_module_repository)
                .with_system_config_values_for_tests(vec![
                    ("module.ldap.enabled".to_string(), json!(true)),
                    ("default_user_initial_gift_usd".to_string(), json!(9.5)),
                ]);
            AppState::new()
                .expect("gateway should build")
                .with_data_state_for_tests(data_state)
        })
        .await;

    let client = reqwest::Client::new();
    let response = client
        .post(format!("{gateway_url}/api/auth/login"))
        .header("x-client-device-id", "device-auth-ldap")
        .header("user-agent", "AetherTest/1.0")
        .json(&json!({
            "email": "alice",
            "password": "secret123",
            "auth_type": "ldap",
        }))
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let set_cookie = response
        .headers()
        .get(reqwest::header::SET_COOKIE)
        .and_then(|value| value.to_str().ok())
        .map(ToOwned::to_owned)
        .expect("set-cookie should exist");
    assert!(set_cookie.contains("aether_refresh_token="));
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    let access_token = payload["access_token"]
        .as_str()
        .expect("access token should exist")
        .to_string();
    let user_id = payload["user_id"]
        .as_str()
        .expect("user id should exist")
        .to_string();
    assert_eq!(payload["email"], "alice@example.com");
    assert_eq!(payload["username"], "alice");
    assert_eq!(payload["role"], "user");

    let me_response = client
        .get(format!("{gateway_url}/api/auth/me"))
        .header("authorization", format!("Bearer {access_token}"))
        .header("x-client-device-id", "device-auth-ldap")
        .header("user-agent", "AetherTest/1.0")
        .send()
        .await
        .expect("me request should succeed");

    assert_eq!(me_response.status(), StatusCode::OK);
    let me_payload: serde_json::Value = me_response.json().await.expect("json body should parse");
    assert_eq!(me_payload["id"], user_id);
    assert_eq!(me_payload["email"], "alice@example.com");
    assert_eq!(me_payload["username"], "alice");
    assert_eq!(me_payload["billing"]["balance"], 9.5);
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_handles_auth_register_locally_without_proxying_upstream() {
    let (gateway_url, upstream_hits, gateway_handle, upstream_handle) =
        start_auth_gateway_with_builder(|| {
            let data_state = crate::data::GatewayDataState::disabled()
                .with_system_config_values_for_tests(vec![
                    ("enable_registration".to_string(), json!(true)),
                    ("require_email_verification".to_string(), json!(true)),
                    ("smtp_host".to_string(), json!("smtp.example.com")),
                    ("smtp_from_email".to_string(), json!("ops@example.com")),
                    ("default_user_initial_gift_usd".to_string(), json!(12.5)),
                ]);
            AppState::new()
                .expect("gateway should build")
                .with_data_state_for_tests(data_state)
                .with_auth_email_verified_for_tests(
                    "alice@example.com",
                    TEST_EMAIL_VERIFICATION_TOKEN,
                )
        })
        .await;

    let client = reqwest::Client::new();
    let register_response = client
        .post(format!("{gateway_url}/api/auth/register"))
        .json(&json!({
            "email": "alice@example.com",
            "username": "alice",
            "password": "secret123",
            "email_verification_token": TEST_EMAIL_VERIFICATION_TOKEN,
        }))
        .send()
        .await
        .expect("register request should succeed");

    assert_eq!(register_response.status(), StatusCode::OK);
    let register_payload: serde_json::Value = register_response
        .json()
        .await
        .expect("json body should parse");
    assert_eq!(register_payload["email"], "alice@example.com");
    assert_eq!(register_payload["username"], "alice");
    assert_eq!(register_payload["message"], "注册成功");

    let login_response = client
        .post(format!("{gateway_url}/api/auth/login"))
        .header("x-client-device-id", "device-auth-register")
        .header("user-agent", "AetherTest/1.0")
        .json(&json!({
            "email": "alice@example.com",
            "password": "secret123",
            "auth_type": "local",
        }))
        .send()
        .await
        .expect("login request should succeed");

    assert_eq!(login_response.status(), StatusCode::OK);
    let login_payload: serde_json::Value =
        login_response.json().await.expect("json body should parse");
    let access_token = login_payload["access_token"]
        .as_str()
        .expect("access token should exist")
        .to_string();

    let me_response = client
        .get(format!("{gateway_url}/api/auth/me"))
        .header("authorization", format!("Bearer {access_token}"))
        .header("x-client-device-id", "device-auth-register")
        .header("user-agent", "AetherTest/1.0")
        .send()
        .await
        .expect("me request should succeed");

    assert_eq!(me_response.status(), StatusCode::OK);
    let me_payload: serde_json::Value = me_response.json().await.expect("json body should parse");
    assert_eq!(me_payload["email"], "alice@example.com");
    assert_eq!(me_payload["username"], "alice");
    assert_eq!(me_payload["billing"]["gift_balance"], 12.5);
    assert_eq!(me_payload["billing"]["total_adjusted"], 12.5);
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_rejects_auth_register_without_current_privacy_policy_acceptance() {
    let (gateway_url, upstream_hits, gateway_handle, upstream_handle) =
        start_auth_gateway_with_builder(|| {
            let data_state = crate::data::GatewayDataState::disabled()
                .with_system_config_values_for_tests(vec![
                    ("enable_registration".to_string(), json!(true)),
                    ("require_email_verification".to_string(), json!(true)),
                    ("smtp_host".to_string(), json!("smtp.example.com")),
                    ("smtp_from_email".to_string(), json!("ops@example.com")),
                    (
                        "registration_privacy_policy_enabled".to_string(),
                        json!(true),
                    ),
                    (
                        "registration_privacy_policy_version".to_string(),
                        json!("2026-05-16"),
                    ),
                ]);
            AppState::new()
                .expect("gateway should build")
                .with_data_state_for_tests(data_state)
                .with_auth_email_verified_for_tests(
                    "alice@example.com",
                    TEST_EMAIL_VERIFICATION_TOKEN,
                )
        })
        .await;

    let response = reqwest::Client::new()
        .post(format!("{gateway_url}/api/auth/register"))
        .json(&json!({
            "email": "alice@example.com",
            "username": "alice",
            "password": "secret123",
            "privacy_policy_accepted": true,
            "privacy_policy_version": "old-version",
        }))
        .send()
        .await
        .expect("register request should succeed");

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["detail"], "请先阅读并同意当前版本的隐私政策");
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_rejects_auth_register_without_turnstile_token_when_enabled() {
    let (gateway_url, upstream_hits, gateway_handle, upstream_handle) =
        start_auth_gateway_with_builder(|| {
            AppState::new()
                .expect("gateway should build")
                .with_data_state_for_tests(turnstile_enabled_data_state())
                .with_auth_email_verified_for_tests(
                    "alice@example.com",
                    TEST_EMAIL_VERIFICATION_TOKEN,
                )
        })
        .await;

    let response = reqwest::Client::new()
        .post(format!("{gateway_url}/api/auth/register"))
        .json(&json!({
            "email": "alice@example.com",
            "username": "alice",
            "password": "secret123",
            "email_verification_token": TEST_EMAIL_VERIFICATION_TOKEN,
        }))
        .send()
        .await
        .expect("register request should succeed");

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["detail"], "请先完成人机验证");
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_rejects_auth_send_verification_code_without_turnstile_token_when_enabled() {
    let (gateway_url, upstream_hits, gateway_handle, upstream_handle) =
        start_auth_gateway_with_builder(|| {
            AppState::new()
                .expect("gateway should build")
                .with_data_state_for_tests(turnstile_enabled_data_state())
        })
        .await;

    let response = reqwest::Client::new()
        .post(format!("{gateway_url}/api/auth/send-verification-code"))
        .json(&json!({ "email": "alice@example.com" }))
        .send()
        .await
        .expect("send verification request should succeed");

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["detail"], "请先完成人机验证");
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_rejects_auth_register_with_oversized_turnstile_token() {
    let (gateway_url, upstream_hits, gateway_handle, upstream_handle) =
        start_auth_gateway_with_builder(|| {
            AppState::new()
                .expect("gateway should build")
                .with_data_state_for_tests(turnstile_enabled_data_state())
                .with_auth_email_verified_for_tests(
                    "alice@example.com",
                    TEST_EMAIL_VERIFICATION_TOKEN,
                )
        })
        .await;

    let response = reqwest::Client::new()
        .post(format!("{gateway_url}/api/auth/register"))
        .json(&json!({
            "email": "alice@example.com",
            "username": "alice",
            "password": "secret123",
            "turnstile_token": "x".repeat(2049),
        }))
        .send()
        .await
        .expect("register request should succeed");

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["detail"], "人机验证失败，请重试");
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_returns_service_unavailable_when_turnstile_keys_are_incomplete() {
    let (gateway_url, upstream_hits, gateway_handle, upstream_handle) =
        start_auth_gateway_with_builder(|| {
            let data_state =
                turnstile_enabled_data_state().with_system_config_values_for_tests(vec![
                    ("enable_registration".to_string(), json!(true)),
                    ("require_email_verification".to_string(), json!(true)),
                    ("smtp_host".to_string(), json!("smtp.example.com")),
                    ("smtp_from_email".to_string(), json!("ops@example.com")),
                    ("turnstile_enabled".to_string(), json!(true)),
                    ("turnstile_site_key".to_string(), json!("site-public-key")),
                    ("turnstile_secret_key".to_string(), serde_json::Value::Null),
                ]);
            AppState::new()
                .expect("gateway should build")
                .with_data_state_for_tests(data_state)
                .with_auth_email_verified_for_tests(
                    "alice@example.com",
                    TEST_EMAIL_VERIFICATION_TOKEN,
                )
        })
        .await;

    let response = reqwest::Client::new()
        .post(format!("{gateway_url}/api/auth/register"))
        .json(&json!({
            "email": "alice@example.com",
            "username": "alice",
            "password": "secret123",
            "turnstile_token": "valid-token",
        }))
        .send()
        .await
        .expect("register request should succeed");

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["detail"], "服务暂不可用，请稍后重试");
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_ignores_spoofed_cf_ip_during_successful_turnstile_verification() {
    let (siteverify_url, turnstile_requests, turnstile_handle) = start_turnstile_siteverify_server(
        json!({
            "success": true,
            "action": "register",
            "hostname": "gateway.example.com",
        }),
        StatusCode::OK,
    )
    .await;
    let (gateway_url, upstream_hits, gateway_handle, upstream_handle) =
        start_auth_gateway_with_builder({
            let siteverify_url = siteverify_url.clone();
            move || {
                AppState::new()
                    .expect("gateway should build")
                    .with_data_state_for_tests(turnstile_enabled_data_state())
                    .with_auth_email_verified_for_tests(
                        "alice@example.com",
                        TEST_EMAIL_VERIFICATION_TOKEN,
                    )
                    .with_turnstile_siteverify_url_for_tests(&siteverify_url)
            }
        })
        .await;

    let register_response = reqwest::Client::new()
        .post(format!("{gateway_url}/api/auth/register"))
        .header("cf-connecting-ip", "203.0.113.10")
        .json(&json!({
            "email": "alice@example.com",
            "username": "alice",
            "password": "secret123",
            "turnstile_token": "valid-token",
            "email_verification_token": TEST_EMAIL_VERIFICATION_TOKEN,
        }))
        .send()
        .await
        .expect("register request should succeed");

    assert_eq!(register_response.status(), StatusCode::OK);
    let register_payload: serde_json::Value = register_response
        .json()
        .await
        .expect("json body should parse");
    assert_eq!(register_payload["message"], "注册成功");

    let requests = turnstile_requests
        .lock()
        .expect("turnstile requests should lock");
    assert_eq!(requests.len(), 1);
    assert_eq!(
        requests[0].get("secret").map(String::as_str),
        Some("secret-private-key")
    );
    assert_eq!(
        requests[0].get("response").map(String::as_str),
        Some("valid-token")
    );
    assert_eq!(
        requests[0].get("remoteip").map(String::as_str),
        Some("127.0.0.1")
    );
    assert!(requests[0].contains_key("idempotency_key"));
    drop(requests);
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
    turnstile_handle.abort();
}

#[tokio::test]
async fn gateway_rejects_auth_register_when_turnstile_action_mismatches() {
    let (siteverify_url, _turnstile_requests, turnstile_handle) =
        start_turnstile_siteverify_server(
            json!({
                "success": true,
                "action": "send_verification_code",
                "hostname": "gateway.example.com",
            }),
            StatusCode::OK,
        )
        .await;
    let (gateway_url, upstream_hits, gateway_handle, upstream_handle) =
        start_auth_gateway_with_builder({
            let siteverify_url = siteverify_url.clone();
            move || {
                AppState::new()
                    .expect("gateway should build")
                    .with_data_state_for_tests(turnstile_enabled_data_state())
                    .with_auth_email_verified_for_tests(
                        "alice@example.com",
                        TEST_EMAIL_VERIFICATION_TOKEN,
                    )
                    .with_turnstile_siteverify_url_for_tests(&siteverify_url)
            }
        })
        .await;

    let response = reqwest::Client::new()
        .post(format!("{gateway_url}/api/auth/register"))
        .json(&json!({
            "email": "alice@example.com",
            "username": "alice",
            "password": "secret123",
            "turnstile_token": "valid-token",
        }))
        .send()
        .await
        .expect("register request should succeed");

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["detail"], "人机验证失败，请重试");
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
    turnstile_handle.abort();
}

#[tokio::test]
async fn gateway_rejects_auth_register_when_turnstile_siteverify_rejects_token() {
    let (siteverify_url, _turnstile_requests, turnstile_handle) =
        start_turnstile_siteverify_server(
            json!({
                "success": false,
                "error-codes": ["invalid-input-response"],
            }),
            StatusCode::OK,
        )
        .await;
    let (gateway_url, upstream_hits, gateway_handle, upstream_handle) =
        start_auth_gateway_with_builder({
            let siteverify_url = siteverify_url.clone();
            move || {
                AppState::new()
                    .expect("gateway should build")
                    .with_data_state_for_tests(turnstile_enabled_data_state())
                    .with_auth_email_verified_for_tests(
                        "alice@example.com",
                        TEST_EMAIL_VERIFICATION_TOKEN,
                    )
                    .with_turnstile_siteverify_url_for_tests(&siteverify_url)
            }
        })
        .await;

    let response = reqwest::Client::new()
        .post(format!("{gateway_url}/api/auth/register"))
        .json(&json!({
            "email": "alice@example.com",
            "username": "alice",
            "password": "secret123",
            "turnstile_token": "invalid-token",
        }))
        .send()
        .await
        .expect("register request should succeed");

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["detail"], "人机验证失败，请重试");
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
    turnstile_handle.abort();
}

#[tokio::test]
async fn gateway_returns_service_unavailable_when_turnstile_siteverify_reports_secret_error() {
    let (siteverify_url, _turnstile_requests, turnstile_handle) =
        start_turnstile_siteverify_server(
            json!({
                "success": false,
                "error-codes": ["invalid-input-secret"],
            }),
            StatusCode::OK,
        )
        .await;
    let (gateway_url, upstream_hits, gateway_handle, upstream_handle) =
        start_auth_gateway_with_builder({
            let siteverify_url = siteverify_url.clone();
            move || {
                AppState::new()
                    .expect("gateway should build")
                    .with_data_state_for_tests(turnstile_enabled_data_state())
                    .with_auth_email_verified_for_tests(
                        "alice@example.com",
                        TEST_EMAIL_VERIFICATION_TOKEN,
                    )
                    .with_turnstile_siteverify_url_for_tests(&siteverify_url)
            }
        })
        .await;

    let response = reqwest::Client::new()
        .post(format!("{gateway_url}/api/auth/register"))
        .json(&json!({
            "email": "alice@example.com",
            "username": "alice",
            "password": "secret123",
            "turnstile_token": "valid-token",
        }))
        .send()
        .await
        .expect("register request should succeed");

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["detail"], "服务暂不可用，请稍后重试");
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
    turnstile_handle.abort();
}

#[tokio::test]
async fn gateway_rejects_auth_register_when_turnstile_hostname_mismatches() {
    let (siteverify_url, _turnstile_requests, turnstile_handle) =
        start_turnstile_siteverify_server(
            json!({
                "success": true,
                "action": "register",
                "hostname": "evil.example.com",
            }),
            StatusCode::OK,
        )
        .await;
    let (gateway_url, upstream_hits, gateway_handle, upstream_handle) =
        start_auth_gateway_with_builder({
            let siteverify_url = siteverify_url.clone();
            move || {
                AppState::new()
                    .expect("gateway should build")
                    .with_data_state_for_tests(turnstile_enabled_data_state())
                    .with_auth_email_verified_for_tests(
                        "alice@example.com",
                        TEST_EMAIL_VERIFICATION_TOKEN,
                    )
                    .with_turnstile_siteverify_url_for_tests(&siteverify_url)
            }
        })
        .await;

    let response = reqwest::Client::new()
        .post(format!("{gateway_url}/api/auth/register"))
        .json(&json!({
            "email": "alice@example.com",
            "username": "alice",
            "password": "secret123",
            "turnstile_token": "valid-token",
        }))
        .send()
        .await
        .expect("register request should succeed");

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["detail"], "人机验证失败，请重试");
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
    turnstile_handle.abort();
}

#[tokio::test]
async fn gateway_returns_service_unavailable_when_turnstile_siteverify_fails() {
    let (siteverify_url, _turnstile_requests, turnstile_handle) =
        start_turnstile_siteverify_server(
            json!({ "error": "unavailable" }),
            StatusCode::BAD_GATEWAY,
        )
        .await;
    let (gateway_url, upstream_hits, gateway_handle, upstream_handle) =
        start_auth_gateway_with_builder({
            let siteverify_url = siteverify_url.clone();
            move || {
                AppState::new()
                    .expect("gateway should build")
                    .with_data_state_for_tests(turnstile_enabled_data_state())
                    .with_auth_email_verified_for_tests(
                        "alice@example.com",
                        TEST_EMAIL_VERIFICATION_TOKEN,
                    )
                    .with_turnstile_siteverify_url_for_tests(&siteverify_url)
            }
        })
        .await;

    let response = reqwest::Client::new()
        .post(format!("{gateway_url}/api/auth/register"))
        .json(&json!({
            "email": "alice@example.com",
            "username": "alice",
            "password": "secret123",
            "turnstile_token": "valid-token",
        }))
        .send()
        .await
        .expect("register request should succeed");

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["detail"], "服务暂不可用，请稍后重试");
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
    turnstile_handle.abort();
}

#[tokio::test]
async fn gateway_returns_service_unavailable_when_turnstile_siteverify_times_out() {
    let (siteverify_url, _turnstile_requests, turnstile_handle) =
        start_turnstile_siteverify_server_with_delay(
            json!({
                "success": true,
                "action": "register",
                "hostname": "gateway.example.com",
            }),
            StatusCode::OK,
            Some(Duration::from_millis(250)),
        )
        .await;
    let (gateway_url, upstream_hits, gateway_handle, upstream_handle) =
        start_auth_gateway_with_builder({
            let siteverify_url = siteverify_url.clone();
            move || {
                AppState::new()
                    .expect("gateway should build")
                    .with_data_state_for_tests(turnstile_enabled_data_state())
                    .with_auth_email_verified_for_tests(
                        "alice@example.com",
                        TEST_EMAIL_VERIFICATION_TOKEN,
                    )
                    .with_turnstile_siteverify_url_for_tests(&siteverify_url)
                    .with_turnstile_siteverify_timeout_for_tests(Duration::from_millis(20))
            }
        })
        .await;

    let response = reqwest::Client::new()
        .post(format!("{gateway_url}/api/auth/register"))
        .json(&json!({
            "email": "alice@example.com",
            "username": "alice",
            "password": "secret123",
            "turnstile_token": "valid-token",
        }))
        .send()
        .await
        .expect("register request should succeed");

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["detail"], "服务暂不可用，请稍后重试");
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
    turnstile_handle.abort();
}

#[tokio::test]
async fn gateway_returns_service_unavailable_for_auth_register_without_storage() {
    let (gateway_url, upstream_hits, gateway_handle, upstream_handle) =
        start_auth_gateway_with_builder(|| {
            let data_state = crate::data::GatewayDataState::disabled()
                .with_system_config_values_for_tests(vec![
                    ("enable_registration".to_string(), json!(true)),
                    ("require_email_verification".to_string(), json!(true)),
                    ("smtp_host".to_string(), json!("smtp.example.com")),
                    ("smtp_from_email".to_string(), json!("ops@example.com")),
                    ("default_user_initial_gift_usd".to_string(), json!(12.5)),
                ]);
            AppState::new()
                .expect("gateway should build")
                .with_data_state_for_tests(data_state)
                .with_auth_email_verified_for_tests(
                    "alice@example.com",
                    TEST_EMAIL_VERIFICATION_TOKEN,
                )
                .without_auth_user_store_for_tests()
        })
        .await;

    let response = reqwest::Client::new()
        .post(format!("{gateway_url}/api/auth/register"))
        .json(&json!({
            "email": "alice@example.com",
            "username": "alice",
            "password": "secret123",
            "email_verification_token": TEST_EMAIL_VERIFICATION_TOKEN,
        }))
        .send()
        .await
        .expect("register request should succeed");

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["detail"], "服务暂不可用，请稍后重试");
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_handles_auth_send_verification_code_locally_without_proxying_upstream() {
    let (gateway_url, upstream_hits, gateway_handle, upstream_handle) =
        start_auth_gateway_with_builder(|| {
            AppState::new()
                .expect("gateway should build")
                .with_data_state_for_tests(
                    crate::data::GatewayDataState::disabled().with_system_config_values_for_tests(
                        vec![
                            ("smtp_host".to_string(), json!("smtp.example.com")),
                            ("smtp_from_email".to_string(), json!("noreply@example.com")),
                            ("smtp_from_name".to_string(), json!("Aether Mail")),
                        ],
                    ),
                )
        })
        .await;

    let client = reqwest::Client::new();
    let send_response = client
        .post(format!("{gateway_url}/api/auth/send-verification-code"))
        .json(&json!({ "email": "alice@example.com" }))
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(send_response.status(), StatusCode::OK);
    assert_eq!(
        send_response
            .headers()
            .get(http::header::CACHE_CONTROL)
            .and_then(|value| value.to_str().ok()),
        Some("no-store")
    );
    let send_payload: serde_json::Value =
        send_response.json().await.expect("json body should parse");
    assert_eq!(send_payload["success"], true);
    assert_eq!(send_payload["expire_minutes"], 5);
    let verification_token = send_payload["verification_token"]
        .as_str()
        .expect("verification token should exist")
        .to_string();

    let status_response = client
        .post(format!("{gateway_url}/api/auth/verification-status"))
        .json(&json!({
            "email": "alice@example.com",
            "verification_token": verification_token,
        }))
        .send()
        .await
        .expect("status request should succeed");

    assert_eq!(status_response.status(), StatusCode::OK);
    let status_payload: serde_json::Value = status_response
        .json()
        .await
        .expect("json body should parse");
    assert_eq!(status_payload["has_pending_code"], true);
    assert_eq!(status_payload["is_verified"], false);
    assert!(
        status_payload["cooldown_remaining"]
            .as_i64()
            .unwrap_or_default()
            > 0
    );

    let resend_response = client
        .post(format!("{gateway_url}/api/auth/send-verification-code"))
        .json(&json!({ "email": "alice@example.com" }))
        .send()
        .await
        .expect("second request should succeed");

    assert_eq!(resend_response.status(), StatusCode::BAD_REQUEST);
    let resend_payload: serde_json::Value = resend_response
        .json()
        .await
        .expect("json body should parse");
    assert!(resend_payload["detail"]
        .as_str()
        .unwrap_or_default()
        .contains("请在"));
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_requires_turnstile_token_before_auth_send_verification_code() {
    let (gateway_url, upstream_hits, gateway_handle, upstream_handle) =
        start_auth_gateway_with_builder(|| {
            AppState::new()
                .expect("gateway should build")
                .with_data_state_for_tests(
                    crate::data::GatewayDataState::disabled().with_system_config_values_for_tests(
                        vec![
                            ("smtp_host".to_string(), json!("smtp.example.com")),
                            ("smtp_from_email".to_string(), json!("noreply@example.com")),
                            ("turnstile_enabled".to_string(), json!(true)),
                            ("turnstile_site_key".to_string(), json!("site-key-123")),
                            ("turnstile_secret_key".to_string(), json!("secret-key-123")),
                        ],
                    ),
                )
        })
        .await;

    let response = reqwest::Client::new()
        .post(format!("{gateway_url}/api/auth/send-verification-code"))
        .json(&json!({ "email": "alice@example.com" }))
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["detail"], "请先完成人机验证");
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_verifies_turnstile_token_before_auth_send_verification_code() {
    let (siteverify_url, turnstile_requests, siteverify_handle) =
        start_turnstile_siteverify_server(
            json!({
                "success": true,
                "action": "send_verification_code",
                "hostname": "localhost"
            }),
            StatusCode::OK,
        )
        .await;

    let (gateway_url, upstream_hits, gateway_handle, upstream_handle) =
        start_auth_gateway_with_builder({
            let siteverify_url = siteverify_url.clone();
            move || {
                AppState::new()
                    .expect("gateway should build")
                    .with_data_state_for_tests(
                        crate::data::GatewayDataState::disabled()
                            .with_system_config_values_for_tests(vec![
                                ("smtp_host".to_string(), json!("smtp.example.com")),
                                ("smtp_from_email".to_string(), json!("noreply@example.com")),
                                ("smtp_from_name".to_string(), json!("Aether Mail")),
                                ("turnstile_enabled".to_string(), json!(true)),
                                ("turnstile_site_key".to_string(), json!("site-key-123")),
                                ("turnstile_secret_key".to_string(), json!("secret-key-123")),
                                (
                                    "turnstile_allowed_hostnames".to_string(),
                                    json!(["localhost"]),
                                ),
                            ]),
                    )
                    .with_turnstile_siteverify_url_for_tests(&siteverify_url)
            }
        })
        .await;

    let response = reqwest::Client::new()
        .post(format!("{gateway_url}/api/auth/send-verification-code"))
        .json(&json!({
            "email": "alice@example.com",
            "turnstile_token": "turnstile-token-123"
        }))
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["success"], true);
    let requests = turnstile_requests
        .lock()
        .expect("turnstile requests should lock");
    assert_eq!(requests.len(), 1);
    assert_eq!(
        requests[0].get("secret").map(String::as_str),
        Some("secret-key-123")
    );
    assert_eq!(
        requests[0].get("response").map(String::as_str),
        Some("turnstile-token-123")
    );
    assert!(requests[0].contains_key("idempotency_key"));
    drop(requests);
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    siteverify_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_handles_auth_verification_status_locally_without_proxying_upstream() {
    let now = Utc::now() - chrono::Duration::seconds(10);
    let (gateway_url, upstream_hits, gateway_handle, upstream_handle) =
        start_auth_gateway_with_builder(|| {
            AppState::new()
                .expect("gateway should build")
                .with_auth_email_verification_pending_for_tests(
                    "alice@example.com",
                    "123456",
                    TEST_EMAIL_VERIFICATION_TOKEN,
                    now,
                )
        })
        .await;

    let response = reqwest::Client::new()
        .post(format!("{gateway_url}/api/auth/verification-status"))
        .json(&json!({
            "email": "alice@example.com",
            "verification_token": TEST_EMAIL_VERIFICATION_TOKEN,
        }))
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["email"], "alice@example.com");
    assert_eq!(payload["has_pending_code"], true);
    assert_eq!(payload["is_verified"], false);
    assert!(payload["cooldown_remaining"].as_i64().unwrap_or_default() > 0);
    assert!(payload["code_expires_in"].as_i64().unwrap_or_default() > 0);
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_handles_auth_verify_email_locally_without_proxying_upstream() {
    let now = Utc::now();
    let (gateway_url, upstream_hits, gateway_handle, upstream_handle) =
        start_auth_gateway_with_builder(|| {
            AppState::new()
                .expect("gateway should build")
                .with_auth_email_verification_pending_for_tests(
                    "alice@example.com",
                    "123456",
                    TEST_EMAIL_VERIFICATION_TOKEN,
                    now,
                )
        })
        .await;

    let client = reqwest::Client::new();
    let verify_response = client
        .post(format!("{gateway_url}/api/auth/verify-email"))
        .json(&json!({
            "email": "alice@example.com",
            "code": "123456",
            "verification_token": TEST_EMAIL_VERIFICATION_TOKEN,
        }))
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(verify_response.status(), StatusCode::OK);
    let verify_payload: serde_json::Value = verify_response
        .json()
        .await
        .expect("json body should parse");
    assert_eq!(verify_payload["success"], true);

    let status_response = client
        .post(format!("{gateway_url}/api/auth/verification-status"))
        .json(&json!({
            "email": "alice@example.com",
            "verification_token": TEST_EMAIL_VERIFICATION_TOKEN,
        }))
        .send()
        .await
        .expect("status request should succeed");

    assert_eq!(status_response.status(), StatusCode::OK);
    let status_payload: serde_json::Value = status_response
        .json()
        .await
        .expect("json body should parse");
    assert_eq!(status_payload["has_pending_code"], false);
    assert_eq!(status_payload["is_verified"], true);
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_invalidates_email_verification_after_five_incorrect_codes() {
    let now = Utc::now();
    let (gateway_url, upstream_hits, gateway_handle, upstream_handle) =
        start_auth_gateway_with_builder(|| {
            AppState::new()
                .expect("gateway should build")
                .with_auth_email_verification_pending_for_tests(
                    "locked@example.com",
                    "123456",
                    TEST_EMAIL_VERIFICATION_TOKEN,
                    now,
                )
        })
        .await;

    let client = reqwest::Client::new();
    for attempt in 1..=4 {
        let response = client
            .post(format!("{gateway_url}/api/auth/verify-email"))
            .json(&json!({
                "email": "locked@example.com",
                "code": "000000",
                "verification_token": TEST_EMAIL_VERIFICATION_TOKEN,
            }))
            .send()
            .await
            .expect("incorrect verification request should succeed");
        assert_eq!(
            response.status(),
            StatusCode::BAD_REQUEST,
            "attempt {attempt} should report an incorrect code"
        );
        let payload: serde_json::Value = response.json().await.expect("json body should parse");
        assert_eq!(payload["detail"], "验证码错误");
    }

    let exhausted = client
        .post(format!("{gateway_url}/api/auth/verify-email"))
        .json(&json!({
            "email": "locked@example.com",
            "code": "000000",
            "verification_token": TEST_EMAIL_VERIFICATION_TOKEN,
        }))
        .send()
        .await
        .expect("exhausted verification request should succeed");
    assert_eq!(exhausted.status(), StatusCode::TOO_MANY_REQUESTS);
    assert!(exhausted
        .headers()
        .contains_key(reqwest::header::RETRY_AFTER));
    let exhausted_payload: serde_json::Value =
        exhausted.json().await.expect("json body should parse");
    assert_eq!(
        exhausted_payload["detail"],
        "验证码尝试次数过多，请重新获取"
    );

    let correct_after_exhaustion = client
        .post(format!("{gateway_url}/api/auth/verify-email"))
        .json(&json!({
            "email": "locked@example.com",
            "code": "123456",
            "verification_token": TEST_EMAIL_VERIFICATION_TOKEN,
        }))
        .send()
        .await
        .expect("verification request after exhaustion should succeed");
    assert_eq!(correct_after_exhaustion.status(), StatusCode::BAD_REQUEST);
    let payload: serde_json::Value = correct_after_exhaustion
        .json()
        .await
        .expect("json body should parse");
    assert_eq!(payload["detail"], "验证会话无效或已过期");
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_handles_auth_me_locally_without_proxying_upstream() {
    let now = Utc::now();
    let user = sample_auth_user(now);
    let access_token = build_test_auth_token(
        "access",
        serde_json::Map::from_iter([
            ("user_id".to_string(), json!(user.id)),
            ("role".to_string(), json!(user.role)),
            (
                "created_at".to_string(),
                json!(user.created_at.map(|value| value.to_rfc3339())),
            ),
            ("session_id".to_string(), json!("session-auth-1")),
        ]),
        now + chrono::Duration::hours(1),
    );
    let (gateway_url, upstream_hits, gateway_handle, upstream_handle) =
        start_auth_gateway_with_state(
            user,
            sample_auth_wallet("user-auth-1", now),
            [sample_auth_session(
                "user-auth-1",
                "session-auth-1",
                "device-auth-1",
                "refresh-token-placeholder",
                now,
            )],
        )
        .await;

    let response = reqwest::Client::new()
        .get(format!("{gateway_url}/api/auth/me"))
        .header("authorization", format!("Bearer {access_token}"))
        .header("x-client-device-id", "device-auth-1")
        .header("user-agent", "AetherTest/1.0")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get(http::header::CACHE_CONTROL)
            .and_then(|value| value.to_str().ok()),
        Some("no-store")
    );
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["id"], "user-auth-1");
    assert_eq!(payload["email"], "alice@example.com");
    assert_eq!(payload["username"], "alice");
    assert_eq!(payload["billing"]["id"], "wallet-auth-1");
    assert_eq!(payload["billing"]["balance"], 15.5);
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_handles_auth_refresh_locally_without_proxying_upstream() {
    let now = Utc::now();
    let user = sample_auth_user(now);
    let refresh_token = build_test_auth_token(
        "refresh",
        serde_json::Map::from_iter([
            ("user_id".to_string(), json!(user.id)),
            (
                "created_at".to_string(),
                json!(user.created_at.map(|value| value.to_rfc3339())),
            ),
            ("session_id".to_string(), json!("session-auth-2")),
            ("jti".to_string(), json!("jti-auth-1")),
        ]),
        now + chrono::Duration::days(7),
    );
    let (gateway_url, upstream_hits, gateway_handle, upstream_handle) =
        start_auth_gateway_with_state(
            user,
            sample_auth_wallet("user-auth-1", now),
            [sample_auth_session(
                "user-auth-1",
                "session-auth-2",
                "device-auth-2",
                &refresh_token,
                now,
            )],
        )
        .await;

    let response = reqwest::Client::new()
        .post(format!("{gateway_url}/api/auth/refresh"))
        .header("cookie", format!("aether_refresh_token={refresh_token}"))
        .header("x-client-device-id", "device-auth-2")
        .header("user-agent", "AetherTest/1.0")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get(http::header::CACHE_CONTROL)
            .and_then(|value| value.to_str().ok()),
        Some("no-store")
    );
    let set_cookie = response
        .headers()
        .get(http::header::SET_COOKIE)
        .and_then(|value| value.to_str().ok())
        .expect("set-cookie should exist")
        .to_string();
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    let access_token = payload["access_token"]
        .as_str()
        .expect("access token should be string");
    assert_eq!(payload["token_type"], "bearer");
    assert!(payload["expires_in"].as_i64().unwrap_or_default() > 0);
    assert_eq!(access_token.split('.').count(), 3);
    assert!(set_cookie.contains("aether_refresh_token="));
    assert!(set_cookie.contains("HttpOnly"));
    assert!(set_cookie.contains("Path=/api/auth"));
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_auth_refresh_rejects_query_only_device_binding_without_rotating_session() {
    let now = Utc::now();
    let user = sample_auth_user(now);
    let refresh_token = build_test_auth_token(
        "refresh",
        serde_json::Map::from_iter([
            ("user_id".to_string(), json!(user.id)),
            (
                "created_at".to_string(),
                json!(user.created_at.map(|value| value.to_rfc3339())),
            ),
            ("session_id".to_string(), json!("session-auth-refresh-csrf")),
            ("jti".to_string(), json!("jti-auth-refresh-csrf")),
        ]),
        now + chrono::Duration::days(7),
    );
    let (gateway_url, upstream_hits, gateway_handle, upstream_handle) =
        start_auth_gateway_with_state(
            user,
            sample_auth_wallet("user-auth-1", now),
            [sample_auth_session(
                "user-auth-1",
                "session-auth-refresh-csrf",
                "device-auth-refresh-csrf",
                &refresh_token,
                now,
            )],
        )
        .await;

    let client = reqwest::Client::new();
    let rejected = client
        .post(format!(
            "{gateway_url}/api/auth/refresh?client_device_id=device-auth-refresh-csrf"
        ))
        .header("cookie", format!("aether_refresh_token={refresh_token}"))
        .send()
        .await
        .expect("query-only refresh request should complete");

    assert_eq!(rejected.status(), StatusCode::BAD_REQUEST);
    assert!(!rejected.headers().contains_key(http::header::SET_COOKIE));

    let legitimate = client
        .post(format!("{gateway_url}/api/auth/refresh"))
        .header("cookie", format!("aether_refresh_token={refresh_token}"))
        .header("x-client-device-id", "device-auth-refresh-csrf")
        .send()
        .await
        .expect("legitimate refresh should complete");
    assert_eq!(legitimate.status(), StatusCode::OK);
    assert!(legitimate.headers().contains_key(http::header::SET_COOKIE));
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_handles_auth_logout_locally_without_proxying_upstream() {
    let now = Utc::now();
    let user = sample_auth_user(now);
    let access_token = build_test_auth_token(
        "access",
        serde_json::Map::from_iter([
            ("user_id".to_string(), json!(user.id)),
            ("role".to_string(), json!(user.role)),
            (
                "created_at".to_string(),
                json!(user.created_at.map(|value| value.to_rfc3339())),
            ),
            ("session_id".to_string(), json!("session-auth-3")),
        ]),
        now + chrono::Duration::hours(1),
    );
    let refresh_token = build_test_auth_token(
        "refresh",
        serde_json::Map::from_iter([
            ("user_id".to_string(), json!("user-auth-1")),
            ("created_at".to_string(), json!(Some(now.to_rfc3339()))),
            ("session_id".to_string(), json!("session-auth-3")),
            ("jti".to_string(), json!("jti-auth-3")),
        ]),
        now + chrono::Duration::days(7),
    );
    let (gateway_url, upstream_hits, gateway_handle, upstream_handle) =
        start_auth_gateway_with_state(
            user,
            sample_auth_wallet("user-auth-1", now),
            [sample_auth_session(
                "user-auth-1",
                "session-auth-3",
                "device-auth-3",
                &refresh_token,
                now,
            )],
        )
        .await;

    let response = reqwest::Client::new()
        .post(format!("{gateway_url}/api/auth/logout"))
        .header("authorization", format!("Bearer {access_token}"))
        .header("x-client-device-id", "device-auth-3")
        .header("user-agent", "AetherTest/1.0")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let set_cookie = response
        .headers()
        .get(http::header::SET_COOKIE)
        .and_then(|value| value.to_str().ok())
        .expect("set-cookie should exist")
        .to_string();
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["success"], true);
    assert_eq!(payload["message"], "登出成功");
    assert!(set_cookie.contains("aether_refresh_token="));
    assert!(set_cookie.contains("Max-Age=0"));
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_logout_rejects_tokens_bound_to_a_replaced_user_identity() {
    let now = Utc::now();
    let mut user = sample_auth_user(now);
    let original_created_at = user
        .created_at
        .expect("test user should have creation time");
    let replacement_created_at = original_created_at + chrono::Duration::milliseconds(1);
    user.created_at = Some(replacement_created_at);

    let build_access_token = |created_at: chrono::DateTime<Utc>| {
        build_test_auth_token(
            "access",
            serde_json::Map::from_iter([
                ("user_id".to_string(), json!(user.id)),
                ("role".to_string(), json!(user.role)),
                ("created_at".to_string(), json!(created_at.to_rfc3339())),
                (
                    "session_id".to_string(),
                    json!("session-auth-replaced-user"),
                ),
            ]),
            now + chrono::Duration::hours(1),
        )
    };
    let stale_access_token = build_access_token(original_created_at);
    let current_access_token = build_access_token(replacement_created_at);
    let stale_refresh_token = build_test_auth_token(
        "refresh",
        serde_json::Map::from_iter([
            ("user_id".to_string(), json!(user.id)),
            (
                "created_at".to_string(),
                json!(original_created_at.to_rfc3339()),
            ),
            (
                "session_id".to_string(),
                json!("session-auth-replaced-user"),
            ),
            ("jti".to_string(), json!("jti-auth-replaced-user")),
        ]),
        now + chrono::Duration::days(7),
    );
    let (gateway_url, upstream_hits, gateway_handle, upstream_handle) =
        start_auth_gateway_with_state(
            user,
            sample_auth_wallet("user-auth-1", now),
            [sample_auth_session(
                "user-auth-1",
                "session-auth-replaced-user",
                "device-auth-replaced-user",
                &stale_refresh_token,
                now,
            )],
        )
        .await;

    let client = reqwest::Client::new();
    let stale_response = client
        .post(format!("{gateway_url}/api/auth/logout"))
        .header("authorization", format!("Bearer {stale_access_token}"))
        .header(
            "cookie",
            format!("aether_refresh_token={stale_refresh_token}"),
        )
        .header("x-client-device-id", "device-auth-replaced-user")
        .send()
        .await
        .expect("stale logout request should complete");
    assert_eq!(stale_response.status(), StatusCode::UNAUTHORIZED);

    let current_response = client
        .post(format!("{gateway_url}/api/auth/logout"))
        .header("authorization", format!("Bearer {current_access_token}"))
        .header("x-client-device-id", "device-auth-replaced-user")
        .send()
        .await
        .expect("current logout request should complete");
    assert_eq!(current_response.status(), StatusCode::OK);
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_cookie_logout_rejects_query_only_device_binding_without_revoking_session() {
    let now = Utc::now();
    let user = sample_auth_user(now);
    let refresh_token = build_test_auth_token(
        "refresh",
        serde_json::Map::from_iter([
            ("user_id".to_string(), json!(user.id)),
            (
                "created_at".to_string(),
                json!(user.created_at.map(|value| value.to_rfc3339())),
            ),
            ("session_id".to_string(), json!("session-auth-logout-csrf")),
            ("jti".to_string(), json!("jti-auth-logout-csrf")),
        ]),
        now + chrono::Duration::days(7),
    );
    let (gateway_url, upstream_hits, gateway_handle, upstream_handle) =
        start_auth_gateway_with_state(
            user,
            sample_auth_wallet("user-auth-1", now),
            [sample_auth_session(
                "user-auth-1",
                "session-auth-logout-csrf",
                "device-auth-logout-csrf",
                &refresh_token,
                now,
            )],
        )
        .await;

    let client = reqwest::Client::new();
    let rejected = client
        .post(format!(
            "{gateway_url}/api/auth/logout?client_device_id=device-auth-logout-csrf"
        ))
        .header("cookie", format!("aether_refresh_token={refresh_token}"))
        .send()
        .await
        .expect("query-only logout request should complete");

    assert_eq!(rejected.status(), StatusCode::BAD_REQUEST);
    assert!(!rejected.headers().contains_key(http::header::SET_COOKIE));

    let legitimate = client
        .post(format!("{gateway_url}/api/auth/logout"))
        .header("cookie", format!("aether_refresh_token={refresh_token}"))
        .header("x-client-device-id", "device-auth-logout-csrf")
        .send()
        .await
        .expect("legitimate logout should complete");
    assert_eq!(legitimate.status(), StatusCode::OK);
    let clear_cookie = legitimate
        .headers()
        .get(http::header::SET_COOKIE)
        .and_then(|value| value.to_str().ok())
        .expect("legitimate logout should clear refresh cookie");
    assert!(clear_cookie.contains("Max-Age=0"));
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_does_not_report_logout_success_when_session_revoke_is_rejected() {
    let now = Utc::now();
    let user = sample_auth_user(now);
    let access_token = build_test_auth_token(
        "access",
        serde_json::Map::from_iter([
            ("user_id".to_string(), json!(user.id)),
            ("role".to_string(), json!(user.role)),
            (
                "created_at".to_string(),
                json!(user.created_at.map(|value| value.to_rfc3339())),
            ),
            ("session_id".to_string(), json!("session-auth-read-only")),
        ]),
        now + chrono::Duration::hours(1),
    );
    let session = sample_auth_session(
        "user-auth-1",
        "session-auth-read-only",
        "device-auth-read-only",
        "refresh-token-read-only",
        now,
    );
    let repository = Arc::new(
        InMemoryUserReadRepository::seed_auth_users(vec![user])
            .with_user_sessions([session])
            .read_only(),
    );
    let (gateway_url, upstream_hits, gateway_handle, upstream_handle) =
        start_auth_gateway_with_builder(|| {
            AppState::new()
                .expect("gateway should build")
                .with_data_state_for_tests(GatewayDataState::with_user_reader_for_tests(repository))
                .without_auth_session_store_for_tests()
        })
        .await;

    let response = reqwest::Client::new()
        .post(format!("{gateway_url}/api/auth/logout"))
        .header("authorization", format!("Bearer {access_token}"))
        .header("x-client-device-id", "device-auth-read-only")
        .header("user-agent", "AetherTest/1.0")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["detail"], "服务暂不可用，请稍后重试");
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}
