use super::*;

#[tokio::test]
async fn gateway_handles_test_connection_alias_without_proxying_upstream() {
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
    let gateway = build_router().expect("gateway should build");
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .get(format!("{gateway_url}/test-connection"))
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::GONE);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(
        payload["detail"],
        "Deprecated endpoint. Please use /v1/test-connection."
    );
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_gemini_test_connection_does_not_force_low_max_output_tokens() {
    let provider_hits = Arc::new(Mutex::new(0usize));
    let provider_hits_clone = Arc::clone(&provider_hits);
    let provider = Router::new().route(
        "/{*path}",
        any(move |request: Request| {
            let provider_hits_inner = Arc::clone(&provider_hits_clone);
            async move {
                *provider_hits_inner.lock().expect("mutex should lock") += 1;
                let body = to_bytes(request.into_body(), usize::MAX)
                    .await
                    .expect("body should read");
                let body_json: serde_json::Value =
                    serde_json::from_slice(&body).expect("json body should parse");
                assert_eq!(body_json["contents"][0]["parts"][0]["text"], "Health check");
                assert!(
                    body_json
                        .get("generationConfig")
                        .and_then(|config| config.get("maxOutputTokens"))
                        .is_none(),
                    "Gemini test connection must not force a tiny maxOutputTokens value"
                );
                Json(json!({
                    "candidates": [{
                        "content": {
                            "role": "model",
                            "parts": [{"text": "ok"}]
                        },
                        "finishReason": "STOP"
                    }],
                    "responseId": "gemini_test_connection_ok"
                }))
                .into_response()
            }
        }),
    );

    let (provider_url, provider_handle) = start_server(provider).await;
    let provider_catalog_repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![sample_provider("provider-gemini", "google", 10)],
        vec![sample_endpoint(
            "endpoint-gemini",
            "provider-gemini",
            "gemini:generate_content",
            &provider_url,
        )],
        vec![sample_key(
            "key-gemini",
            "provider-gemini",
            "gemini:generate_content",
            "google-api-key",
        )],
    ));

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
                json!("session-test-connection-gemini"),
            ),
        ]),
        now + chrono::Duration::hours(1),
    );
    let user_repository = Arc::new(InMemoryUserReadRepository::seed_auth_users(vec![user]));
    let gateway = build_router_with_state(
        AppState::new()
            .expect("gateway should build")
            .with_data_state_for_tests(
                GatewayDataState::with_provider_catalog_repository_for_tests(
                    provider_catalog_repository,
                )
                .with_encryption_key_for_tests(DEVELOPMENT_ENCRYPTION_KEY)
                .with_user_reader(user_repository),
            )
            .with_auth_session_for_tests(sample_auth_session(
                "user-auth-1",
                "session-test-connection-gemini",
                "device-test-connection-gemini",
                "refresh-token-placeholder",
                now,
            )),
    );
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .get(format!(
            "{gateway_url}/v1/test-connection?provider=provider-gemini&model=gemini-3-flash-preview&api_format=gemini:generate_content"
        ))
        .header("authorization", format!("Bearer {access_token}"))
        .header("x-client-device-id", "device-test-connection-gemini")
        .header("user-agent", "AetherTest/1.0")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["status"], "success");
    assert_eq!(payload["provider_id"], "provider-gemini");
    assert_eq!(payload["endpoint_id"], "endpoint-gemini");
    assert_eq!(payload["api_format"], "gemini:generate_content");
    assert_eq!(*provider_hits.lock().expect("mutex should lock"), 1);

    gateway_handle.abort();
    provider_handle.abort();
}

#[tokio::test]
async fn gateway_handles_ccswitch_usage_with_api_key_without_proxying_upstream() {
    let now = wallet_today_usage_test_time();
    let user = sample_auth_user(now);
    let usage_repository = Arc::new(InMemoryUsageReadRepository::seed(vec![
        sample_user_usage_audit(
            "usage-ccswitch-1",
            "req-ccswitch-1",
            "user-auth-1",
            "gpt-5",
            "Aether",
            "completed",
            now,
        ),
    ]));
    let auth_repository = Arc::new(InMemoryAuthApiKeySnapshotRepository::seed(vec![(
        Some(hash_api_key("sk-ccswitch-usage")),
        sample_usage_auth_snapshot("api-key-user-1", "user-auth-1", "ccswitch"),
    )]));
    let (gateway_url, upstream_hits, gateway_handle, upstream_handle) =
        start_auth_gateway_with_builder(|| {
            let data_state = GatewayDataState::with_user_wallet_and_usage_for_tests(
                Arc::new(InMemoryUserReadRepository::seed_auth_users(vec![
                    user.clone()
                ])),
                Arc::new(InMemoryWalletRepository::seed(vec![sample_auth_wallet(
                    "user-auth-1",
                    now,
                )])),
                Arc::clone(&usage_repository),
            )
            .with_auth_api_key_reader(auth_repository);
            AppState::new()
                .expect("gateway should build")
                .with_data_state_for_tests(data_state)
        })
        .await;

    let response = reqwest::Client::new()
        .get(format!("{gateway_url}/api/ccswitch/usage"))
        .header("authorization", "Bearer sk-ccswitch-usage")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["is_valid"], true);
    assert_eq!(payload["plan_name"], "Aether");
    assert_eq!(payload["remaining"], 15.5);
    assert_eq!(payload["used"], 1.25);
    assert_eq!(payload["unit"], "USD");
    assert_eq!(payload["wallet"]["wallet_balance"], 15.5);
    assert_eq!(payload["today"]["total_requests"], 1);
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_handles_ccswitch_usage_for_standalone_key_without_owner_usage() {
    let now = wallet_today_usage_test_time();
    let user = sample_auth_user(now);
    let mut owner_usage = sample_user_usage_audit(
        "usage-ccswitch-owner",
        "req-ccswitch-owner",
        "user-auth-1",
        "gpt-5",
        "Aether",
        "completed",
        now,
    );
    owner_usage.api_key_id = Some("api-key-user-1".to_string());
    owner_usage.total_cost_usd = 1.25;
    owner_usage.actual_total_cost_usd = 1.25;

    let mut standalone_usage = sample_user_usage_audit(
        "usage-ccswitch-standalone",
        "req-ccswitch-standalone",
        "user-auth-1",
        "gpt-5",
        "Aether",
        "completed",
        now,
    );
    standalone_usage.api_key_id = Some("api-key-standalone-1".to_string());
    standalone_usage.api_key_name = Some("standalone".to_string());
    standalone_usage.total_cost_usd = 0.5;
    standalone_usage.actual_total_cost_usd = 0.5;

    let usage_repository = Arc::new(InMemoryUsageReadRepository::seed(vec![
        owner_usage,
        standalone_usage,
    ]));
    let mut snapshot =
        sample_usage_auth_snapshot("api-key-standalone-1", "user-auth-1", "standalone");
    snapshot.api_key_is_standalone = true;
    let auth_repository = Arc::new(InMemoryAuthApiKeySnapshotRepository::seed(vec![(
        Some(hash_api_key("sk-ccswitch-standalone")),
        snapshot,
    )]));
    let (gateway_url, upstream_hits, gateway_handle, upstream_handle) =
        start_auth_gateway_with_builder(|| {
            let data_state = GatewayDataState::with_user_wallet_and_usage_for_tests(
                Arc::new(InMemoryUserReadRepository::seed_auth_users(vec![
                    user.clone()
                ])),
                Arc::new(InMemoryWalletRepository::seed(vec![
                    sample_auth_wallet("user-auth-1", now),
                    sample_standalone_auth_wallet("api-key-standalone-1", now),
                ])),
                Arc::clone(&usage_repository),
            )
            .with_auth_api_key_reader(auth_repository);
            AppState::new()
                .expect("gateway should build")
                .with_data_state_for_tests(data_state)
        })
        .await;

    let response = reqwest::Client::new()
        .get(format!("{gateway_url}/api/ccswitch/usage"))
        .header("authorization", "Bearer sk-ccswitch-standalone")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["is_valid"], true);
    assert_eq!(payload["remaining"], 2.5);
    assert_eq!(payload["used"], 0.5);
    assert_eq!(payload["wallet"]["wallet"]["id"], "wallet-standalone-1");
    assert_eq!(payload["today"]["total_requests"], 1);
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_updates_users_me_detail_locally_without_proxying_upstream() {
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
                json!("session-users-me-update-detail"),
            ),
        ]),
        now + chrono::Duration::hours(1),
    );
    let (gateway_url, upstream_hits, gateway_handle, upstream_handle) =
        start_auth_gateway_with_state(
            user,
            sample_auth_wallet("user-auth-1", now),
            [sample_auth_session(
                "user-auth-1",
                "session-users-me-update-detail",
                "device-users-me-update-detail",
                "refresh-token-placeholder",
                now,
            )],
        )
        .await;
    let client = reqwest::Client::new();

    let unverified_response = client
        .put(format!("{gateway_url}/api/users/me"))
        .header("authorization", format!("Bearer {access_token}"))
        .header("x-client-device-id", "device-users-me-update-detail")
        .header("user-agent", "AetherTest/1.0")
        .json(&json!({
            "email": "alice+unverified@example.com",
            "username": "alice-updated"
        }))
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(unverified_response.status(), StatusCode::BAD_REQUEST);
    let unverified_payload: serde_json::Value = unverified_response
        .json()
        .await
        .expect("json body should parse");
    assert_eq!(unverified_payload["detail"], "修改邮箱前请先完成新邮箱验证");

    let update_response = client
        .put(format!("{gateway_url}/api/users/me"))
        .header("authorization", format!("Bearer {access_token}"))
        .header("x-client-device-id", "device-users-me-update-detail")
        .header("user-agent", "AetherTest/1.0")
        .json(&json!({
            "email": "alice@example.com",
            "username": "alice-updated",
            "feature_settings": {
                "chat_pii_redaction": {
                    "enabled": true
                }
            }
        }))
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(update_response.status(), StatusCode::OK);
    let update_payload: serde_json::Value = update_response
        .json()
        .await
        .expect("json body should parse");
    assert_eq!(update_payload["message"], "个人信息更新成功");

    let get_response = client
        .get(format!("{gateway_url}/api/users/me"))
        .header("authorization", format!("Bearer {access_token}"))
        .header("x-client-device-id", "device-users-me-update-detail")
        .header("user-agent", "AetherTest/1.0")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(get_response.status(), StatusCode::OK);
    let get_payload: serde_json::Value = get_response.json().await.expect("json body should parse");
    assert_eq!(get_payload["email"], "alice@example.com");
    assert_eq!(get_payload["username"], "alice-updated");
    assert_eq!(get_payload["auth_source"], "local");
    assert_eq!(get_payload["has_password"], true);
    assert_eq!(
        get_payload["feature_settings"]["chat_pii_redaction"]["enabled"],
        true
    );
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_returns_service_unavailable_for_users_me_detail_update_without_profile_storage() {
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
                json!("session-users-me-update-unavailable"),
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
                    "session-users-me-update-unavailable",
                    "device-users-me-update-unavailable",
                    "refresh-token-placeholder",
                    now,
                )])
        })
        .await;

    let response = reqwest::Client::new()
        .put(format!("{gateway_url}/api/users/me"))
        .header("authorization", format!("Bearer {access_token}"))
        .header("x-client-device-id", "device-users-me-update-unavailable")
        .header("user-agent", "AetherTest/1.0")
        .json(&json!({
            "email": "alice+updated@example.com",
            "username": "alice-updated",
        }))
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["detail"], "修改邮箱前请先完成新邮箱验证");
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_handles_users_me_endpoint_status_locally_without_proxying_upstream() {
    let now = Utc::now();
    let now_unix_secs = now.timestamp();
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
                json!("session-user-endpoint-status-1"),
            ),
        ]),
        now + chrono::Duration::hours(1),
    );
    let provider_catalog_repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![
            sample_provider("provider-openai", "openai", 10),
            sample_provider("provider-claude", "claude", 20),
        ],
        vec![
            sample_endpoint(
                "endpoint-openai",
                "provider-openai",
                "openai:chat",
                "https://api.openai.example",
            ),
            sample_endpoint(
                "endpoint-claude",
                "provider-claude",
                "claude:messages",
                "https://api.anthropic.example",
            ),
        ],
        vec![
            sample_key(
                "key-openai",
                "provider-openai",
                "openai:chat",
                "sk-openai-endpoint-status",
            ),
            sample_key(
                "key-claude",
                "provider-claude",
                "claude:messages",
                "sk-claude-endpoint-status",
            ),
        ],
    ));
    let request_candidate_repository = Arc::new(InMemoryRequestCandidateRepository::seed(vec![
        sample_request_candidate(
            "cand-openai-success",
            "req-openai-success",
            "endpoint-openai",
            RequestCandidateStatus::Success,
            now_unix_secs - 3_000,
            Some(now_unix_secs - 2_980),
        ),
        sample_request_candidate(
            "cand-openai-failed",
            "req-openai-failed",
            "endpoint-openai",
            RequestCandidateStatus::Failed,
            now_unix_secs - 2_000,
            Some(now_unix_secs - 1_980),
        ),
        sample_request_candidate(
            "cand-claude-skipped",
            "req-claude-skipped",
            "endpoint-claude",
            RequestCandidateStatus::Skipped,
            now_unix_secs - 1_500,
            Some(now_unix_secs - 1_480),
        ),
    ]));
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
    let wallet_repository = Arc::new(InMemoryWalletRepository::seed(vec![sample_auth_wallet(
        "user-auth-1",
        now,
    )]));
    let gateway = build_router_with_state(
        AppState::new()
            .expect("gateway should build")
            .with_data_state_for_tests(
                crate::data::GatewayDataState::with_user_and_wallet_for_tests(
                    user_repository,
                    wallet_repository,
                )
                .with_provider_catalog_reader(provider_catalog_repository)
                .with_request_candidate_reader(request_candidate_repository),
            )
            .with_auth_sessions_for_tests([sample_auth_session(
                "user-auth-1",
                "session-user-endpoint-status-1",
                "device-user-endpoint-status-1",
                "refresh-token-placeholder",
                now,
            )]),
    );
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .get(format!("{gateway_url}/api/users/me/endpoint-status"))
        .header("authorization", format!("Bearer {access_token}"))
        .header("x-client-device-id", "device-user-endpoint-status-1")
        .header("user-agent", "AetherTest/1.0")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    let items = payload.as_array().expect("payload should be an array");
    assert_eq!(items.len(), 2);
    assert_eq!(items[0]["api_format"], "claude:messages");
    assert_eq!(items[0]["display_name"], "Claude Messages");
    assert_eq!(items[0]["health_score"], 1.0);
    assert_eq!(items[0]["timeline"].as_array().map(Vec::len), Some(60));
    assert!(items[0].get("total_endpoints").is_none());
    assert_eq!(items[1]["api_format"], "openai:chat");
    assert_eq!(items[1]["display_name"], "OpenAI Chat");
    assert_eq!(items[1]["health_score"], 0.5);
    assert_eq!(items[1]["timeline"].as_array().map(Vec::len), Some(60));
    assert!(items[1].get("total_keys").is_none());
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_handles_users_me_preferences_locally_without_proxying_upstream() {
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
            ("session_id".to_string(), json!("session-user-pref-1")),
        ]),
        now + chrono::Duration::hours(1),
    );
    let (gateway_url, upstream_hits, gateway_handle, upstream_handle) =
        start_auth_gateway_with_preferences_state(
            user,
            sample_auth_wallet("user-auth-1", now),
            [sample_auth_session(
                "user-auth-1",
                "session-user-pref-1",
                "device-user-pref-1",
                "refresh-token-placeholder",
                now,
            )],
            {
                let mut preference =
                    crate::data::state::StoredUserPreferenceRecord::default_for_user("user-auth-1");
                preference.default_provider_id = Some("provider-openai".to_string());
                preference.default_provider_name = Some("openai".to_string());
                vec![preference]
            },
        )
        .await;

    let client = reqwest::Client::new();
    let get_response = client
        .get(format!("{gateway_url}/api/users/me/preferences"))
        .header("authorization", format!("Bearer {access_token}"))
        .header("x-client-device-id", "device-user-pref-1")
        .header("user-agent", "AetherTest/1.0")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(get_response.status(), StatusCode::OK);
    let get_payload: serde_json::Value = get_response.json().await.expect("json should parse");
    assert_eq!(get_payload["theme"], "light");
    assert_eq!(get_payload["language"], "zh-CN");
    assert_eq!(get_payload["timezone"], "Asia/Shanghai");
    assert_eq!(get_payload["allow_wallet_overage"], false);
    assert_eq!(get_payload["notifications"]["email"], true);
    assert_eq!(get_payload["default_provider_id"], "provider-openai");
    assert!(get_payload.get("default_provider").is_none());

    let put_response = client
        .put(format!("{gateway_url}/api/users/me/preferences"))
        .header("authorization", format!("Bearer {access_token}"))
        .header("x-client-device-id", "device-user-pref-1")
        .header("user-agent", "AetherTest/1.0")
        .json(&json!({
            "theme": "dark",
            "language": "en-US",
            "timezone": "UTC",
            "bio": "hello",
            "email_notifications": false,
            "usage_alerts": false,
            "announcement_notifications": true,
            "allow_wallet_overage": true,
        }))
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(put_response.status(), StatusCode::OK);
    let put_payload: serde_json::Value = put_response.json().await.expect("json should parse");
    assert_eq!(put_payload["message"], "偏好设置更新成功");

    let verify_response = client
        .get(format!("{gateway_url}/api/users/me/preferences"))
        .header("authorization", format!("Bearer {access_token}"))
        .header("x-client-device-id", "device-user-pref-1")
        .header("user-agent", "AetherTest/1.0")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(verify_response.status(), StatusCode::OK);
    let verify_payload: serde_json::Value =
        verify_response.json().await.expect("json should parse");
    assert_eq!(verify_payload["theme"], "dark");
    assert_eq!(verify_payload["language"], "en-US");
    assert_eq!(verify_payload["timezone"], "UTC");
    assert_eq!(verify_payload["bio"], "hello");
    assert_eq!(verify_payload["allow_wallet_overage"], true);
    assert_eq!(verify_payload["notifications"]["email"], false);
    assert_eq!(verify_payload["notifications"]["usage_alerts"], false);
    assert_eq!(verify_payload["notifications"]["announcements"], true);
    for (payload, expected_status, expected_overage) in [
        (
            json!({"allow_wallet_overage": "true"}),
            StatusCode::BAD_REQUEST,
            true,
        ),
        (json!({"theme": "light"}), StatusCode::OK, true),
        (
            json!({"allow_wallet_overage": false}),
            StatusCode::OK,
            false,
        ),
    ] {
        let response = client
            .put(format!("{gateway_url}/api/users/me/preferences"))
            .header("authorization", format!("Bearer {access_token}"))
            .header("x-client-device-id", "device-user-pref-1")
            .header("user-agent", "AetherTest/1.0")
            .json(&payload)
            .send()
            .await
            .expect("preference update should succeed");
        assert_eq!(response.status(), expected_status);
        let preferences: serde_json::Value = client
            .get(format!("{gateway_url}/api/users/me/preferences"))
            .header("authorization", format!("Bearer {access_token}"))
            .header("x-client-device-id", "device-user-pref-1")
            .header("user-agent", "AetherTest/1.0")
            .send()
            .await
            .expect("preference lookup should succeed")
            .json()
            .await
            .expect("preferences should parse");
        assert_eq!(preferences["allow_wallet_overage"], expected_overage);
    }
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_returns_service_unavailable_for_users_me_preferences_update_without_storage() {
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
                json!("session-user-pref-unavailable"),
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
                .with_auth_sessions_for_tests([sample_auth_session(
                    "user-auth-1",
                    "session-user-pref-unavailable",
                    "device-user-pref-unavailable",
                    "refresh-token-placeholder",
                    now,
                )])
        })
        .await;

    let response = reqwest::Client::new()
        .put(format!("{gateway_url}/api/users/me/preferences"))
        .header("authorization", format!("Bearer {access_token}"))
        .header("x-client-device-id", "device-user-pref-unavailable")
        .header("user-agent", "AetherTest/1.0")
        .json(&json!({
            "theme": "dark",
            "language": "en-US",
        }))
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let payload: serde_json::Value = response.json().await.expect("json should parse");
    assert_eq!(payload["detail"], "服务暂不可用，请稍后重试");
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_handles_users_me_usage_locally_without_proxying_upstream() {
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
            ("session_id".to_string(), json!("session-users-me-usage-1")),
        ]),
        now + chrono::Duration::hours(1),
    );
    let mut streaming_usage = sample_user_usage_audit(
        "usage-users-me-streaming-1",
        "req-users-me-streaming-1",
        "user-auth-1",
        "gpt-4.1-mini",
        "OpenAI",
        "streaming",
        now - chrono::Duration::minutes(5),
    );
    streaming_usage.candidate_index = Some(2);
    streaming_usage.request_metadata = Some(json!({
        "rate_multiplier": 0.5,
        "input_price_per_1m": 3.0,
        "output_price_per_1m": 9.0,
        "cache_creation_price_per_1m": 3.75,
        "cache_read_price_per_1m": 0.3,
    }));
    let usage_repository = Arc::new(InMemoryUsageReadRepository::seed(vec![
        sample_user_usage_audit(
            "usage-users-me-completed-1",
            "req-users-me-completed-1",
            "user-auth-1",
            "gpt-4.1",
            "OpenAI",
            "completed",
            now - chrono::Duration::minutes(20),
        ),
        sample_user_usage_audit(
            "usage-users-me-failed-1",
            "req-users-me-failed-1",
            "user-auth-1",
            "claude-3.5-sonnet",
            "Anthropic",
            "failed",
            now - chrono::Duration::minutes(10),
        ),
        streaming_usage,
    ]));
    let auth_repository = Arc::new(InMemoryAuthApiKeySnapshotRepository::seed(vec![(
        Some("hash-api-key-user-1".to_string()),
        sample_usage_auth_snapshot("api-key-user-1", "user-auth-1", "renamed-key"),
    )]));
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
            .with_auth_api_key_reader(auth_repository);
            AppState::new()
                .expect("gateway should build")
                .with_data_state_for_tests(data_state)
                .with_auth_sessions_for_tests([sample_auth_session(
                    "user-auth-1",
                    "session-users-me-usage-1",
                    "device-users-me-usage-1",
                    "refresh-token-placeholder",
                    now,
                )])
        })
        .await;

    let response = reqwest::Client::new()
        .get(format!(
            "{gateway_url}/api/users/me/usage?limit=10&offset=0&search=renamed-key"
        ))
        .header("authorization", format!("Bearer {access_token}"))
        .header("x-client-device-id", "device-users-me-usage-1")
        .header("user-agent", "AetherTest/1.0")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["total_requests"], 2);
    assert_eq!(payload["total_input_tokens"], 240);
    assert_eq!(payload["pagination"]["total"], 3);
    assert_eq!(
        payload["records"].as_array().expect("records array").len(),
        3
    );
    assert_eq!(
        payload["records"][0]["cache_creation_ephemeral_5m_input_tokens"],
        4
    );
    assert_eq!(payload["records"][0]["effective_input_tokens"], 95);
    assert_eq!(
        payload["records"][0]["cache_creation_ephemeral_1h_input_tokens"],
        6
    );
    assert_eq!(payload["records"][0]["input_price_per_1m"], 3.0);
    assert_eq!(payload["records"][0]["output_price_per_1m"], 9.0);
    assert_eq!(payload["records"][0]["cache_creation_price_per_1m"], 3.75);
    assert_eq!(payload["records"][0]["cache_read_price_per_1m"], 0.3);
    assert_eq!(payload["records"][0]["has_fallback"], true);
    assert_eq!(payload["records"][0]["api_key"]["name"], "renamed-key");
    assert_eq!(payload["records"][0]["api_key"]["display"], "renamed-key");
    assert_eq!(
        payload["summary_by_model"]
            .as_array()
            .expect("summary array")
            .len(),
        2
    );
    assert_eq!(
        payload["summary_by_model"][0]["cache_creation_ephemeral_5m_tokens"],
        4
    );
    assert_eq!(
        payload["summary_by_model"][0]["cache_creation_ephemeral_1h_tokens"],
        6
    );
    assert_eq!(payload["summary_by_model"][0]["effective_input_tokens"], 95);
    assert_eq!(payload["summary_by_model"][0]["total_input_context"], 120);
    assert!(payload.get("summary_by_provider").is_none());
    assert_eq!(payload["billing"]["id"], "wallet-auth-1");
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_handles_users_me_usage_active_locally_without_proxying_upstream() {
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
            ("session_id".to_string(), json!("session-users-me-active-1")),
        ]),
        now + chrono::Duration::hours(1),
    );
    let mut streaming_usage = sample_user_usage_audit(
        "usage-users-me-streaming-1",
        "req-users-me-streaming-1",
        "user-auth-1",
        "gpt-4.1-mini",
        "OpenAI",
        "streaming",
        now - chrono::Duration::minutes(2),
    );
    streaming_usage.request_metadata = Some(json!({
        "rate_multiplier": 0.5,
    }));
    let usage_repository = Arc::new(InMemoryUsageReadRepository::seed(vec![
        sample_user_usage_audit(
            "usage-users-me-pending-1",
            "req-users-me-pending-1",
            "user-auth-1",
            "gpt-4.1",
            "OpenAI",
            "pending",
            now - chrono::Duration::minutes(4),
        ),
        streaming_usage,
        sample_user_usage_audit(
            "usage-users-me-completed-1",
            "req-users-me-completed-1",
            "user-auth-1",
            "gpt-4.1",
            "OpenAI",
            "completed",
            now - chrono::Duration::minutes(30),
        ),
    ]));
    let (gateway_url, upstream_hits, gateway_handle, upstream_handle) =
        start_auth_gateway_with_usage_state(
            user,
            sample_auth_wallet("user-auth-1", now),
            [sample_auth_session(
                "user-auth-1",
                "session-users-me-active-1",
                "device-users-me-active-1",
                "refresh-token-placeholder",
                now,
            )],
            usage_repository,
        )
        .await;

    let response = reqwest::Client::new()
        .get(format!("{gateway_url}/api/users/me/usage/active"))
        .header("authorization", format!("Bearer {access_token}"))
        .header("x-client-device-id", "device-users-me-active-1")
        .header("user-agent", "AetherTest/1.0")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    let requests = payload["requests"].as_array().expect("requests array");
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0]["status"], "streaming");
    assert_eq!(requests[0]["rate_multiplier"], 0.5);
    assert_eq!(requests[0]["cache_creation_ephemeral_5m_input_tokens"], 4);
    assert_eq!(requests[0]["cache_creation_ephemeral_1h_input_tokens"], 6);
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_handles_users_me_usage_interval_timeline_and_heatmap_locally_without_proxying_upstream(
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
                json!("session-users-me-usage-heatmap-1"),
            ),
        ]),
        now + chrono::Duration::hours(1),
    );
    let usage_repository = Arc::new(InMemoryUsageReadRepository::seed(vec![
        sample_user_usage_audit(
            "usage-users-me-heatmap-1",
            "req-users-me-heatmap-1",
            "user-auth-1",
            "gpt-4.1",
            "OpenAI",
            "completed",
            now - chrono::Duration::minutes(25),
        ),
        sample_user_usage_audit(
            "usage-users-me-heatmap-2",
            "req-users-me-heatmap-2",
            "user-auth-1",
            "gpt-4.1",
            "OpenAI",
            "completed",
            now - chrono::Duration::minutes(15),
        ),
        sample_user_usage_audit(
            "usage-users-me-heatmap-3",
            "req-users-me-heatmap-3",
            "user-auth-1",
            "gpt-4.1",
            "OpenAI",
            "completed",
            now - chrono::Duration::days(1) - chrono::Duration::minutes(1),
        ),
    ]));
    let (gateway_url, upstream_hits, gateway_handle, upstream_handle) =
        start_auth_gateway_with_usage_state(
            user,
            sample_auth_wallet("user-auth-1", now),
            [sample_auth_session(
                "user-auth-1",
                "session-users-me-usage-heatmap-1",
                "device-users-me-usage-heatmap-1",
                "refresh-token-placeholder",
                now,
            )],
            usage_repository,
        )
        .await;

    let client = reqwest::Client::new();
    let timeline_response = client
        .get(format!(
            "{gateway_url}/api/users/me/usage/interval-timeline?hours=24&limit=100"
        ))
        .header("authorization", format!("Bearer {access_token}"))
        .header("x-client-device-id", "device-users-me-usage-heatmap-1")
        .header("user-agent", "AetherTest/1.0")
        .send()
        .await
        .expect("timeline request should succeed");
    assert_eq!(timeline_response.status(), StatusCode::OK);
    let timeline_payload: serde_json::Value = timeline_response
        .json()
        .await
        .expect("timeline json should parse");
    assert_eq!(timeline_payload["analysis_period_hours"], 24);
    assert_eq!(timeline_payload["total_points"], 1);

    let heatmap_response = client
        .get(format!("{gateway_url}/api/users/me/usage/heatmap"))
        .header("authorization", format!("Bearer {access_token}"))
        .header("x-client-device-id", "device-users-me-usage-heatmap-1")
        .header("user-agent", "AetherTest/1.0")
        .send()
        .await
        .expect("heatmap request should succeed");
    assert_eq!(heatmap_response.status(), StatusCode::OK);
    let heatmap_payload: serde_json::Value = heatmap_response
        .json()
        .await
        .expect("heatmap json should parse");
    assert!(
        heatmap_payload["days"]
            .as_array()
            .expect("days array")
            .len()
            >= 2
    );
    assert!(heatmap_payload["max_requests"].as_u64().unwrap_or(0) >= 1);
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_samples_users_me_usage_interval_timeline_across_full_window_when_over_limit() {
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
                json!("session-users-me-usage-timeline-sampling"),
            ),
        ]),
        now + chrono::Duration::hours(1),
    );
    let first_request_at = now - chrono::Duration::hours(4);
    let latest_request_at = now - chrono::Duration::minutes(1);
    let mut seeded = Vec::new();
    for index in 0..250_i64 {
        // The latest request is isolated from the early burst so losing it is visible.
        let created_at = if index == 249 {
            latest_request_at
        } else {
            first_request_at + chrono::Duration::seconds(index)
        };
        seeded.push(sample_user_usage_audit(
            &format!("usage-users-me-timeline-sampling-{index}"),
            &format!("req-users-me-timeline-sampling-{index}"),
            "user-auth-1",
            "gpt-4.1",
            "OpenAI",
            "completed",
            created_at,
        ));
    }
    let usage_repository = Arc::new(InMemoryUsageReadRepository::seed(seeded));
    let (gateway_url, upstream_hits, gateway_handle, upstream_handle) =
        start_auth_gateway_with_usage_state(
            user,
            sample_auth_wallet("user-auth-1", now),
            [sample_auth_session(
                "user-auth-1",
                "session-users-me-usage-timeline-sampling",
                "device-users-me-usage-timeline-sampling",
                "refresh-token-placeholder",
                now,
            )],
            usage_repository,
        )
        .await;

    let client = reqwest::Client::new();
    let timeline_response = client
        .get(format!(
            "{gateway_url}/api/users/me/usage/interval-timeline?hours=24&limit=100"
        ))
        .header("authorization", format!("Bearer {access_token}"))
        .header(
            "x-client-device-id",
            "device-users-me-usage-timeline-sampling",
        )
        .header("user-agent", "AetherTest/1.0")
        .send()
        .await
        .expect("timeline request should succeed");
    assert_eq!(timeline_response.status(), StatusCode::OK);
    let payload: serde_json::Value = timeline_response
        .json()
        .await
        .expect("timeline json should parse");

    // 250 条 completed 记录产生 249 个间隔点，limit=100 时应均匀抽样并标记 truncated
    assert_eq!(payload["truncated"], json!(true));
    assert_eq!(payload["total_points"], 249);
    let points = payload["points"].as_array().expect("points array");
    let parse_x = |point: &serde_json::Value| {
        chrono::DateTime::parse_from_rfc3339(point["x"].as_str().expect("x should be a string"))
            .expect("x should be rfc3339")
            .timestamp()
    };
    let newest = parse_x(points.last().expect("points should not be empty"));
    let oldest = parse_x(&points[0]);
    assert_eq!(
        newest,
        latest_request_at.timestamp(),
        "sampling should keep the most recent interval"
    );
    assert_eq!(
        oldest,
        first_request_at.timestamp() + 1,
        "sampling should keep the first interval"
    );
    assert_eq!(points.len(), 100, "sampling should fill the point budget");
    assert!(points
        .windows(2)
        .all(|pair| parse_x(&pair[0]) <= parse_x(&pair[1])));

    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_returns_service_unavailable_for_users_me_usage_routes_without_reader() {
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
                json!("session-users-me-usage-unavailable"),
            ),
        ]),
        now + chrono::Duration::hours(1),
    );
    let (gateway_url, upstream_hits, gateway_handle, upstream_handle) =
        start_auth_gateway_with_state(
            user,
            sample_auth_wallet("user-auth-1", now),
            [sample_auth_session(
                "user-auth-1",
                "session-users-me-usage-unavailable",
                "device-users-me-usage-unavailable",
                "refresh-token-placeholder",
                now,
            )],
        )
        .await;

    let client = reqwest::Client::new();
    for path in [
        "/api/users/me/usage",
        "/api/users/me/usage/active",
        "/api/users/me/usage/interval-timeline?hours=24&limit=100",
        "/api/users/me/usage/heatmap",
    ] {
        let response = client
            .get(format!("{gateway_url}{path}"))
            .header("authorization", format!("Bearer {access_token}"))
            .header("x-client-device-id", "device-users-me-usage-unavailable")
            .header("user-agent", "AetherTest/1.0")
            .send()
            .await
            .expect("request should succeed");
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE, "{path}");
        let payload: serde_json::Value = response.json().await.expect("json body should parse");
        assert_eq!(payload["detail"], "服务暂不可用，请稍后重试", "{path}");
    }
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_handles_users_me_api_keys_locally_without_proxying_upstream() {
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
            ("session_id".to_string(), json!("session-users-me-api-keys")),
        ]),
        now + chrono::Duration::hours(1),
    );
    let encrypted = encrypt_python_fernet_plaintext(DEVELOPMENT_ENCRYPTION_KEY, "sk-user-live-1")
        .expect("ciphertext should build");
    let auth_repository = Arc::new(
        InMemoryAuthApiKeySnapshotRepository::seed(vec![(
            Some("87e376524de9f1d78f2b7d95d20bcc3faed619d572ccc9a1d9a343ab83e33834".to_string()),
            StoredAuthApiKeySnapshot::new(
                "user-auth-1".to_string(),
                "alice".to_string(),
                Some("alice@example.com".to_string()),
                "user".to_string(),
                "local".to_string(),
                true,
                false,
                Some(json!(["openai"])),
                Some(json!(["openai:chat"])),
                Some(json!(["gpt-5"])),
                "user-key-1".to_string(),
                Some("primary".to_string()),
                true,
                false,
                false,
                Some(60),
                Some(5),
                Some(300),
                Some(json!(["openai"])),
                Some(json!(["openai:chat"])),
                Some(json!(["gpt-5"])),
            )
            .expect("auth api key snapshot should build"),
        )])
        .with_export_records(vec![StoredAuthApiKeyExportRecord::new(
            "user-auth-1".to_string(),
            "user-key-1".to_string(),
            "87e376524de9f1d78f2b7d95d20bcc3faed619d572ccc9a1d9a343ab83e33834".to_string(),
            Some(encrypted),
            Some("primary".to_string()),
            Some(json!(["openai"])),
            Some(json!(["openai:chat"])),
            Some(json!(["gpt-5"])),
            Some(60),
            Some(5),
            Some(json!({"cache_1h": true})),
            true,
            Some(300),
            false,
            9,
            0,
            1.5,
            false,
        )
        .expect("export record should build")
        .with_activity_timestamps(
            Some(1_711_000_102),
            Some(1_711_000_100),
            Some(1_711_000_101),
        )
        .expect("export activity timestamps should build")]),
    );
    let user_repository = Arc::new(InMemoryUserReadRepository::seed_auth_users(vec![user]));

    let (gateway_url, upstream_hits, gateway_handle, upstream_handle) =
        start_auth_gateway_with_builder(|| {
            let data_state =
                crate::data::GatewayDataState::with_user_reader_for_tests(user_repository)
                    .attach_auth_api_key_repository_for_tests(auth_repository)
                    .with_encryption_key_for_tests(DEVELOPMENT_ENCRYPTION_KEY);
            AppState::new()
                .expect("gateway should build")
                .with_data_state_for_tests(data_state)
                .with_auth_sessions_for_tests([sample_auth_session(
                    "user-auth-1",
                    "session-users-me-api-keys",
                    "device-users-me-api-keys",
                    "refresh-token-users-me-api-keys",
                    now,
                )])
        })
        .await;

    let client = reqwest::Client::new();
    let list_response = client
        .get(format!("{gateway_url}/api/users/me/api-keys"))
        .header("authorization", format!("Bearer {access_token}"))
        .header("x-client-device-id", "device-users-me-api-keys")
        .header("user-agent", "AetherTest/1.0")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(list_response.status(), StatusCode::OK);
    let list_payload: serde_json::Value =
        list_response.json().await.expect("json body should parse");
    let api_keys = list_payload.as_array().expect("api keys should be array");
    assert_eq!(api_keys.len(), 1);
    assert_eq!(api_keys[0]["id"], "user-key-1");
    assert_eq!(api_keys[0]["name"], "primary");
    assert_eq!(api_keys[0]["key_display"], "sk-u...e-1");
    assert_eq!(api_keys[0]["total_requests"], 9);
    assert_eq!(api_keys[0]["total_cost_usd"], 1.5);
    assert_eq!(api_keys[0]["created_at"], "2024-03-21T05:48:20+00:00");
    assert_eq!(api_keys[0]["last_used_at"], "2024-03-21T05:48:22+00:00");

    let detail_response = client
        .get(format!(
            "{gateway_url}/api/users/me/api-keys/user-key-1?include_key=true"
        ))
        .header("authorization", format!("Bearer {access_token}"))
        .header("x-client-device-id", "device-users-me-api-keys")
        .header("user-agent", "AetherTest/1.0")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(detail_response.status(), StatusCode::OK);
    assert_eq!(
        detail_response
            .headers()
            .get(http::header::CACHE_CONTROL)
            .and_then(|value| value.to_str().ok()),
        Some("no-store")
    );
    let detail_payload: serde_json::Value = detail_response
        .json()
        .await
        .expect("json body should parse");
    assert_eq!(detail_payload["key"], "sk-user-live-1");
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_handles_users_me_client_config_locally_without_proxying_upstream() {
    let now = Utc::now();
    let user = sample_auth_user(now);
    let _public_base_url_guard =
        set_test_env_var("AETHER_PUBLIC_BASE_URL", "https://aether.example.com/");
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
                json!("session-users-me-client-config"),
            ),
        ]),
        now + chrono::Duration::hours(1),
    );
    let user_repository = Arc::new(InMemoryUserReadRepository::seed_auth_users(vec![user]));

    let (gateway_url, upstream_hits, gateway_handle, upstream_handle) =
        start_auth_gateway_with_builder(|| {
            let data_state =
                crate::data::GatewayDataState::with_user_reader_for_tests(user_repository)
                    .with_system_config_values_for_tests(vec![(
                        "site_name".to_string(),
                        json!("Aether Local"),
                    )]);
            AppState::new()
                .expect("gateway should build")
                .with_data_state_for_tests(data_state)
                .with_auth_sessions_for_tests([sample_auth_session(
                    "user-auth-1",
                    "session-users-me-client-config",
                    "device-users-me-client-config",
                    "refresh-token-users-me-client-config",
                    now,
                )])
        })
        .await;

    let response = reqwest::Client::new()
        .get(format!("{gateway_url}/api/users/me/client-config"))
        .header("authorization", format!("Bearer {access_token}"))
        .header("x-client-device-id", "device-users-me-client-config")
        .header("user-agent", "AetherTest/1.0")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["base_url"], "https://aether.example.com");
    assert_eq!(payload["site_name"], "Aether Local");
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_rejects_invalid_users_me_api_key_detail_path_as_local_not_found_without_hitting_upstream(
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
            ("session_id".to_string(), json!("session-users-me-api-keys")),
        ]),
        now + chrono::Duration::hours(1),
    );
    let (gateway_url, upstream_hits, gateway_handle, upstream_handle) =
        start_auth_gateway_with_state(
            user,
            sample_auth_wallet("user-auth-1", now),
            [sample_auth_session(
                "user-auth-1",
                "session-users-me-api-keys",
                "device-users-me-api-keys",
                "refresh-token-users-me-api-keys",
                now,
            )],
        )
        .await;

    let response = reqwest::Client::new()
        .get(format!("{gateway_url}/api/users/me/api-keys/"))
        .header("authorization", format!("Bearer {access_token}"))
        .header("x-client-device-id", "device-users-me-api-keys")
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
async fn gateway_rejects_invalid_users_me_api_key_patch_path_as_local_not_found_without_hitting_upstream(
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
                json!("session-users-me-api-key-writes"),
            ),
        ]),
        now + chrono::Duration::hours(1),
    );
    let (gateway_url, upstream_hits, gateway_handle, upstream_handle) =
        start_auth_gateway_with_builder(|| {
            let data_state = crate::data::GatewayDataState::with_auth_api_key_repository_for_tests(
                Arc::new(InMemoryAuthApiKeySnapshotRepository::seed(vec![])),
            )
            .with_user_reader(Arc::new(InMemoryUserReadRepository::seed_auth_users(vec![
                user,
            ])));
            AppState::new()
                .expect("gateway should build")
                .with_data_state_for_tests(data_state)
                .with_auth_sessions_for_tests([sample_auth_session(
                    "user-auth-1",
                    "session-users-me-api-key-writes",
                    "device-users-me-api-key-writes",
                    "refresh-token-users-me-api-key-writes",
                    now,
                )])
        })
        .await;

    let response = reqwest::Client::new()
        .patch(format!("{gateway_url}/api/users/me/api-keys/"))
        .header("authorization", format!("Bearer {access_token}"))
        .header("x-client-device-id", "device-users-me-api-key-writes")
        .header("user-agent", "AetherTest/1.0")
        .json(&json!({ "is_active": false }))
        .send()
        .await
        .expect("request should succeed");

    assert_local_route_not_found_response(response).await;
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_rejects_users_me_api_key_nested_get_path_as_local_not_found_without_hitting_upstream(
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
            ("session_id".to_string(), json!("session-users-me-api-keys")),
        ]),
        now + chrono::Duration::hours(1),
    );
    let (gateway_url, upstream_hits, gateway_handle, upstream_handle) =
        start_auth_gateway_with_state(
            user,
            sample_auth_wallet("user-auth-1", now),
            [sample_auth_session(
                "user-auth-1",
                "session-users-me-api-keys",
                "device-users-me-api-keys",
                "refresh-token-users-me-api-keys",
                now,
            )],
        )
        .await;

    let response = reqwest::Client::new()
        .get(format!(
            "{gateway_url}/api/users/me/api-keys/key-123/providers"
        ))
        .header("authorization", format!("Bearer {access_token}"))
        .header("x-client-device-id", "device-users-me-api-keys")
        .header("user-agent", "AetherTest/1.0")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    assert_eq!(
        response
            .headers()
            .get(CONTROL_ROUTE_FAMILY_HEADER)
            .and_then(|value| value.to_str().ok()),
        None
    );
    assert_eq!(
        response
            .headers()
            .get(CONTROL_ROUTE_KIND_HEADER)
            .and_then(|value| value.to_str().ok()),
        None
    );
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["error"]["type"], "http_error");
    assert_eq!(payload["error"]["message"], "Route not found");
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_handles_users_me_api_key_writes_locally_without_proxying_upstream() {
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
                json!("session-users-me-api-key-writes"),
            ),
        ]),
        now + chrono::Duration::hours(1),
    );
    let encrypted =
        encrypt_python_fernet_plaintext(DEVELOPMENT_ENCRYPTION_KEY, "sk-user-existing-1")
            .expect("ciphertext should build");
    let auth_repository = Arc::new(
        InMemoryAuthApiKeySnapshotRepository::seed(vec![(
            Some("hash-user-existing-1".to_string()),
            StoredAuthApiKeySnapshot::new(
                "user-auth-1".to_string(),
                "alice".to_string(),
                Some("alice@example.com".to_string()),
                "user".to_string(),
                "local".to_string(),
                true,
                false,
                Some(json!(["openai"])),
                Some(json!(["openai:chat"])),
                Some(json!(["gpt-5"])),
                "user-existing-1".to_string(),
                Some("existing".to_string()),
                true,
                false,
                false,
                Some(60),
                Some(5),
                Some(300),
                Some(json!(["openai"])),
                Some(json!(["openai:chat"])),
                Some(json!(["gpt-5"])),
            )
            .expect("auth api key snapshot should build"),
        )])
        .with_export_records(vec![StoredAuthApiKeyExportRecord::new(
            "user-auth-1".to_string(),
            "user-existing-1".to_string(),
            "hash-user-existing-1".to_string(),
            Some(encrypted),
            Some("existing".to_string()),
            Some(json!(["openai"])),
            Some(json!(["openai:chat"])),
            Some(json!(["gpt-5"])),
            Some(60),
            Some(5),
            None,
            true,
            Some(300),
            false,
            3,
            0,
            0.5,
            false,
        )
        .expect("export record should build")]),
    );
    let user_repository = Arc::new(InMemoryUserReadRepository::seed_auth_users(vec![user]));
    let provider_catalog_repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![sample_provider("provider-openai", "openai", 10)],
        vec![sample_endpoint(
            "endpoint-openai-1",
            "provider-openai",
            "openai:chat",
            "https://api.openai.example",
        )],
        vec![],
    ));

    let (gateway_url, upstream_hits, gateway_handle, upstream_handle) =
        start_auth_gateway_with_builder(|| {
            let data_state = crate::data::GatewayDataState::with_auth_api_key_repository_for_tests(
                auth_repository,
            )
            .with_user_reader(user_repository)
            .with_provider_catalog_reader(provider_catalog_repository)
            .with_encryption_key_for_tests(DEVELOPMENT_ENCRYPTION_KEY);
            AppState::new()
                .expect("gateway should build")
                .with_data_state_for_tests(data_state)
                .with_auth_sessions_for_tests([sample_auth_session(
                    "user-auth-1",
                    "session-users-me-api-key-writes",
                    "device-users-me-api-key-writes",
                    "refresh-token-users-me-api-key-writes",
                    now,
                )])
        })
        .await;

    let client = reqwest::Client::new();
    let create_response = client
        .post(format!("{gateway_url}/api/users/me/api-keys"))
        .header("authorization", format!("Bearer {access_token}"))
        .header("x-client-device-id", "device-users-me-api-key-writes")
        .header("user-agent", "AetherTest/1.0")
        .json(&json!({
            "name": "writer-key",
            "rate_limit": 120,
            "feature_settings": {
                "chat_pii_redaction": {
                    "enabled": true
                }
            }
        }))
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(create_response.status(), StatusCode::OK);
    assert_eq!(
        create_response
            .headers()
            .get(http::header::CACHE_CONTROL)
            .and_then(|value| value.to_str().ok()),
        Some("no-store")
    );
    let create_payload: serde_json::Value = create_response
        .json()
        .await
        .expect("json body should parse");
    let created_id = create_payload["id"]
        .as_str()
        .expect("created id should be string")
        .to_string();
    assert_eq!(create_payload["name"], "writer-key");
    assert!(create_payload.get("credential_kind").is_none());
    assert_eq!(create_payload["rate_limit"], 120);
    assert_eq!(create_payload["concurrent_limit"], serde_json::Value::Null);
    assert_eq!(
        create_payload["feature_settings"]["chat_pii_redaction"]["enabled"],
        true
    );
    assert_eq!(create_payload["message"], "API密钥创建成功");
    let created_at = create_payload["created_at"]
        .as_str()
        .expect("created_at should be string");
    assert!(chrono::DateTime::parse_from_rfc3339(created_at).is_ok());
    assert!(!created_at.starts_with("1970-01-01"));
    assert!(create_payload["key"]
        .as_str()
        .unwrap_or_default()
        .starts_with("sk-"));

    let update_response = client
        .put(format!("{gateway_url}/api/users/me/api-keys/{created_id}"))
        .header("authorization", format!("Bearer {access_token}"))
        .header("x-client-device-id", "device-users-me-api-key-writes")
        .header("user-agent", "AetherTest/1.0")
        .json(&json!({
            "name": "writer-key-renamed",
            "rate_limit": 30,
            "concurrent_limit": 4,
            "feature_settings": {
                "chat_pii_redaction": {
                    "enabled": false
                }
            }
        }))
        .send()
        .await
        .expect("request should succeed");
    assert_eq!(update_response.status(), StatusCode::OK);
    let update_payload: serde_json::Value = update_response
        .json()
        .await
        .expect("json body should parse");
    assert_eq!(update_payload["name"], "writer-key-renamed");
    assert!(update_payload.get("credential_kind").is_none());
    assert_eq!(update_payload["rate_limit"], 30);
    assert_eq!(update_payload["concurrent_limit"], 4);
    assert_eq!(
        update_payload["feature_settings"]["chat_pii_redaction"]["enabled"],
        false
    );
    assert_eq!(update_payload["message"], "API密钥已更新");

    let toggle_response = client
        .patch(format!("{gateway_url}/api/users/me/api-keys/{created_id}"))
        .header("authorization", format!("Bearer {access_token}"))
        .header("x-client-device-id", "device-users-me-api-key-writes")
        .header("user-agent", "AetherTest/1.0")
        .json(&json!({ "is_active": false }))
        .send()
        .await
        .expect("request should succeed");
    assert_eq!(toggle_response.status(), StatusCode::OK);
    let toggle_payload: serde_json::Value = toggle_response
        .json()
        .await
        .expect("json body should parse");
    assert_eq!(toggle_payload["is_active"], false);

    let providers_response = client
        .put(format!(
            "{gateway_url}/api/users/me/api-keys/{created_id}/providers"
        ))
        .header("authorization", format!("Bearer {access_token}"))
        .header("x-client-device-id", "device-users-me-api-key-writes")
        .header("user-agent", "AetherTest/1.0")
        .json(&json!({ "providers": ["openai"] }))
        .send()
        .await
        .expect("request should succeed");
    assert_eq!(providers_response.status(), StatusCode::OK);
    let providers_payload: serde_json::Value = providers_response
        .json()
        .await
        .expect("json body should parse");
    assert_eq!(providers_payload["message"], "API密钥可用提供商已更新");
    assert_eq!(
        providers_payload["allowed_providers"],
        json!(["provider-openai"])
    );

    let capabilities_response = client
        .put(format!(
            "{gateway_url}/api/users/me/api-keys/{created_id}/capabilities"
        ))
        .header("authorization", format!("Bearer {access_token}"))
        .header("x-client-device-id", "device-users-me-api-key-writes")
        .header("user-agent", "AetherTest/1.0")
        .json(&json!({ "force_capabilities": {} }))
        .send()
        .await
        .expect("request should succeed");
    assert_eq!(capabilities_response.status(), StatusCode::OK);
    let capabilities_payload: serde_json::Value = capabilities_response
        .json()
        .await
        .expect("json body should parse");
    assert_eq!(capabilities_payload["force_capabilities"], json!({}));

    let detail_response = client
        .get(format!("{gateway_url}/api/users/me/api-keys/{created_id}"))
        .header("authorization", format!("Bearer {access_token}"))
        .header("x-client-device-id", "device-users-me-api-key-writes")
        .header("user-agent", "AetherTest/1.0")
        .send()
        .await
        .expect("request should succeed");
    assert_eq!(detail_response.status(), StatusCode::OK);
    let detail_payload: serde_json::Value = detail_response
        .json()
        .await
        .expect("json body should parse");
    assert_eq!(detail_payload["id"], created_id);
    assert_eq!(detail_payload["is_active"], false);
    assert_eq!(
        detail_payload["allowed_providers"],
        json!(["provider-openai"])
    );
    assert_eq!(detail_payload["concurrent_limit"], 4);
    assert_eq!(detail_payload["force_capabilities"], json!({}));
    assert_eq!(detail_payload["created_at"], created_at);
    assert_eq!(
        detail_payload["feature_settings"]["chat_pii_redaction"]["enabled"],
        false
    );

    let delete_response = client
        .delete(format!("{gateway_url}/api/users/me/api-keys/{created_id}"))
        .header("authorization", format!("Bearer {access_token}"))
        .header("x-client-device-id", "device-users-me-api-key-writes")
        .header("user-agent", "AetherTest/1.0")
        .send()
        .await
        .expect("request should succeed");
    assert_eq!(delete_response.status(), StatusCode::OK);
    let delete_payload: serde_json::Value = delete_response
        .json()
        .await
        .expect("json body should parse");
    assert_eq!(delete_payload["message"], "API密钥已删除");

    let missing_response = client
        .get(format!("{gateway_url}/api/users/me/api-keys/{created_id}"))
        .header("authorization", format!("Bearer {access_token}"))
        .header("x-client-device-id", "device-users-me-api-key-writes")
        .header("user-agent", "AetherTest/1.0")
        .send()
        .await
        .expect("request should succeed");
    assert_eq!(missing_response.status(), StatusCode::NOT_FOUND);
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_returns_service_unavailable_for_users_me_api_key_writes_without_writer() {
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
                json!("session-users-me-api-key-readonly"),
            ),
        ]),
        now + chrono::Duration::hours(1),
    );
    let encrypted =
        encrypt_python_fernet_plaintext(DEVELOPMENT_ENCRYPTION_KEY, "sk-user-existing-1")
            .expect("ciphertext should build");
    let auth_repository = Arc::new(
        InMemoryAuthApiKeySnapshotRepository::seed(vec![(
            Some("hash-user-existing-1".to_string()),
            StoredAuthApiKeySnapshot::new(
                "user-auth-1".to_string(),
                "alice".to_string(),
                Some("alice@example.com".to_string()),
                "user".to_string(),
                "local".to_string(),
                true,
                false,
                Some(json!(["openai"])),
                Some(json!(["openai:chat"])),
                Some(json!(["gpt-5"])),
                "user-existing-1".to_string(),
                Some("existing".to_string()),
                true,
                false,
                false,
                Some(60),
                Some(5),
                Some(300),
                Some(json!(["openai"])),
                Some(json!(["openai:chat"])),
                Some(json!(["gpt-5"])),
            )
            .expect("auth api key snapshot should build"),
        )])
        .with_export_records(vec![StoredAuthApiKeyExportRecord::new(
            "user-auth-1".to_string(),
            "user-existing-1".to_string(),
            "hash-user-existing-1".to_string(),
            Some(encrypted),
            Some("existing".to_string()),
            Some(json!(["openai"])),
            Some(json!(["openai:chat"])),
            Some(json!(["gpt-5"])),
            Some(60),
            Some(5),
            None,
            true,
            Some(300),
            false,
            3,
            0,
            0.5,
            false,
        )
        .expect("export record should build")]),
    );
    let user_repository = Arc::new(InMemoryUserReadRepository::seed_auth_users(vec![user]));
    let provider_catalog_repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![sample_provider("provider-openai", "openai", 10)],
        vec![sample_endpoint(
            "endpoint-openai-1",
            "provider-openai",
            "openai:chat",
            "https://api.openai.example",
        )],
        vec![],
    ));

    let (gateway_url, upstream_hits, gateway_handle, upstream_handle) =
        start_auth_gateway_with_builder(|| {
            let data_state =
                crate::data::GatewayDataState::with_auth_api_key_reader_for_tests(auth_repository)
                    .with_user_reader(user_repository)
                    .with_provider_catalog_reader(provider_catalog_repository);
            AppState::new()
                .expect("gateway should build")
                .with_data_state_for_tests(data_state)
                .with_auth_sessions_for_tests([sample_auth_session(
                    "user-auth-1",
                    "session-users-me-api-key-readonly",
                    "device-users-me-api-key-readonly",
                    "refresh-token-users-me-api-key-readonly",
                    now,
                )])
        })
        .await;

    let client = reqwest::Client::new();
    let create_response = client
        .post(format!("{gateway_url}/api/users/me/api-keys"))
        .header("authorization", format!("Bearer {access_token}"))
        .header("x-client-device-id", "device-users-me-api-key-readonly")
        .header("user-agent", "AetherTest/1.0")
        .json(&json!({
            "name": "readonly-key",
            "rate_limit": 120
        }))
        .send()
        .await
        .expect("request should succeed");
    assert_eq!(create_response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let create_payload: serde_json::Value = create_response
        .json()
        .await
        .expect("json body should parse");
    assert_eq!(create_payload["detail"], "服务暂不可用，请稍后重试");

    let update_response = client
        .put(format!(
            "{gateway_url}/api/users/me/api-keys/user-existing-1"
        ))
        .header("authorization", format!("Bearer {access_token}"))
        .header("x-client-device-id", "device-users-me-api-key-readonly")
        .header("user-agent", "AetherTest/1.0")
        .json(&json!({
            "name": "writer-key-renamed",
            "rate_limit": 30
        }))
        .send()
        .await
        .expect("request should succeed");
    assert_eq!(update_response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let update_payload: serde_json::Value = update_response
        .json()
        .await
        .expect("json body should parse");
    assert_eq!(update_payload["detail"], "服务暂不可用，请稍后重试");

    let toggle_response = client
        .patch(format!(
            "{gateway_url}/api/users/me/api-keys/user-existing-1"
        ))
        .header("authorization", format!("Bearer {access_token}"))
        .header("x-client-device-id", "device-users-me-api-key-readonly")
        .header("user-agent", "AetherTest/1.0")
        .json(&json!({ "is_active": false }))
        .send()
        .await
        .expect("request should succeed");
    assert_eq!(toggle_response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let toggle_payload: serde_json::Value = toggle_response
        .json()
        .await
        .expect("json body should parse");
    assert_eq!(toggle_payload["detail"], "服务暂不可用，请稍后重试");

    let providers_response = client
        .put(format!(
            "{gateway_url}/api/users/me/api-keys/user-existing-1/providers"
        ))
        .header("authorization", format!("Bearer {access_token}"))
        .header("x-client-device-id", "device-users-me-api-key-readonly")
        .header("user-agent", "AetherTest/1.0")
        .json(&json!({ "providers": ["openai"] }))
        .send()
        .await
        .expect("request should succeed");
    assert_eq!(providers_response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let providers_payload: serde_json::Value = providers_response
        .json()
        .await
        .expect("json body should parse");
    assert_eq!(providers_payload["detail"], "服务暂不可用，请稍后重试");

    let capabilities_response = client
        .put(format!(
            "{gateway_url}/api/users/me/api-keys/user-existing-1/capabilities"
        ))
        .header("authorization", format!("Bearer {access_token}"))
        .header("x-client-device-id", "device-users-me-api-key-readonly")
        .header("user-agent", "AetherTest/1.0")
        .json(&json!({ "force_capabilities": {} }))
        .send()
        .await
        .expect("request should succeed");
    assert_eq!(
        capabilities_response.status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    let capabilities_payload: serde_json::Value = capabilities_response
        .json()
        .await
        .expect("json body should parse");
    assert_eq!(capabilities_payload["detail"], "服务暂不可用，请稍后重试");

    let delete_response = client
        .delete(format!(
            "{gateway_url}/api/users/me/api-keys/user-existing-1"
        ))
        .header("authorization", format!("Bearer {access_token}"))
        .header("x-client-device-id", "device-users-me-api-key-readonly")
        .header("user-agent", "AetherTest/1.0")
        .send()
        .await
        .expect("request should succeed");
    assert_eq!(delete_response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let delete_payload: serde_json::Value = delete_response
        .json()
        .await
        .expect("json body should parse");
    assert_eq!(delete_payload["detail"], "服务暂不可用，请稍后重试");
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_handles_users_me_detail_locally_without_proxying_upstream() {
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
            ("session_id".to_string(), json!("session-users-me-detail-1")),
        ]),
        now + chrono::Duration::hours(1),
    );
    let (gateway_url, upstream_hits, gateway_handle, upstream_handle) =
        start_auth_gateway_with_state(
            user,
            sample_auth_wallet("user-auth-1", now),
            [sample_auth_session(
                "user-auth-1",
                "session-users-me-detail-1",
                "device-users-me-detail-1",
                "refresh-token-placeholder",
                now,
            )],
        )
        .await;

    let response = reqwest::Client::new()
        .get(format!("{gateway_url}/api/users/me"))
        .header("authorization", format!("Bearer {access_token}"))
        .header("x-client-device-id", "device-users-me-detail-1")
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
    assert_eq!(payload["auth_source"], "local");
    assert_eq!(payload["has_password"], true);
    assert_eq!(payload["billing"]["id"], "wallet-auth-1");
    assert_eq!(payload["billing"]["balance"], 15.5);
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_rejects_access_and_refresh_tokens_from_a_stale_security_version() {
    let now = Utc::now();
    let user = sample_auth_user(now)
        .with_security_version(1)
        .expect("auth user security version should update");
    let refresh_token = build_test_auth_token(
        "refresh",
        serde_json::Map::from_iter([
            ("user_id".to_string(), json!(user.id)),
            (
                "created_at".to_string(),
                json!(user.created_at.map(|value| value.to_rfc3339())),
            ),
            ("session_id".to_string(), json!("session-auth-stale")),
            ("jti".to_string(), json!("jti-auth-stale")),
        ]),
        now + chrono::Duration::days(7),
    );
    let access_token = build_test_auth_token(
        "access",
        serde_json::Map::from_iter([
            ("user_id".to_string(), json!(user.id)),
            ("role".to_string(), json!(user.role)),
            (
                "created_at".to_string(),
                json!(user.created_at.map(|value| value.to_rfc3339())),
            ),
            ("session_id".to_string(), json!("session-auth-stale")),
        ]),
        now + chrono::Duration::hours(1),
    );
    let stale_session = sample_auth_session(
        "user-auth-1",
        "session-auth-stale",
        "device-auth-stale",
        &refresh_token,
        now,
    );
    assert_eq!(stale_session.security_version, 0);
    let (gateway_url, upstream_hits, gateway_handle, upstream_handle) =
        start_auth_gateway_with_state(
            user,
            sample_auth_wallet("user-auth-1", now),
            [stale_session],
        )
        .await;
    let client = reqwest::Client::new();

    let me_response = client
        .get(format!("{gateway_url}/api/auth/me"))
        .header("authorization", format!("Bearer {access_token}"))
        .header("x-client-device-id", "device-auth-stale")
        .header("user-agent", "AetherTest/1.0")
        .send()
        .await
        .expect("stale access request should complete");
    assert_eq!(me_response.status(), StatusCode::UNAUTHORIZED);
    let me_payload: serde_json::Value = me_response.json().await.expect("json body should parse");
    assert_eq!(me_payload["detail"], "登录会话已失效，请重新登录");

    let refresh_response = client
        .post(format!("{gateway_url}/api/auth/refresh"))
        .header("cookie", format!("aether_refresh_token={refresh_token}"))
        .header("x-client-device-id", "device-auth-stale")
        .header("user-agent", "AetherTest/1.0")
        .send()
        .await
        .expect("stale refresh request should complete");
    assert_eq!(refresh_response.status(), StatusCode::UNAUTHORIZED);
    let refresh_payload: serde_json::Value = refresh_response
        .json()
        .await
        .expect("json body should parse");
    assert_eq!(refresh_payload["detail"], "登录会话已失效，请重新登录");
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_allows_only_one_concurrent_refresh_rotation() {
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
            ("session_id".to_string(), json!("session-auth-refresh-race")),
            ("jti".to_string(), json!("jti-auth-refresh-race")),
        ]),
        now + chrono::Duration::days(7),
    );
    let (gateway_url, upstream_hits, gateway_handle, upstream_handle) =
        start_auth_gateway_with_state(
            user,
            sample_auth_wallet("user-auth-1", now),
            [sample_auth_session(
                "user-auth-1",
                "session-auth-refresh-race",
                "device-auth-refresh-race",
                &refresh_token,
                now,
            )],
        )
        .await;

    let client = reqwest::Client::new();
    let build_request = || {
        client
            .post(format!("{gateway_url}/api/auth/refresh"))
            .header("cookie", format!("aether_refresh_token={refresh_token}"))
            .header("x-client-device-id", "device-auth-refresh-race")
            .header("user-agent", "AetherTest/1.0")
            .send()
    };
    let (first, second) = tokio::join!(build_request(), build_request());
    let responses = [
        first.expect("first refresh request should complete"),
        second.expect("second refresh request should complete"),
    ];

    let mut success_count = 0;
    let mut conflict_count = 0;
    for response in responses {
        let status = response.status();
        let has_set_cookie = response.headers().contains_key(http::header::SET_COOKIE);
        let payload: serde_json::Value = response.json().await.expect("json body should parse");
        match status {
            StatusCode::OK => {
                success_count += 1;
                assert!(has_set_cookie);
                assert!(payload["access_token"].as_str().is_some());
            }
            StatusCode::CONFLICT => {
                conflict_count += 1;
                assert!(!has_set_cookie);
                assert!(payload.get("access_token").is_none());
            }
            status => panic!("unexpected concurrent refresh status: {status}"),
        }
    }
    assert_eq!(success_count, 1);
    assert_eq!(conflict_count, 1);
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}
