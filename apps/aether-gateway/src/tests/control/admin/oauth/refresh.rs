use super::*;
use crate::tests::DEV_FERNET_KEY;

#[test]
fn gateway_revalidates_kiro_device_poll_via_idc_refresh_and_backfills_email() {
    run_admin_oauth_test(
        "gateway_revalidates_kiro_device_poll_via_idc_refresh_and_backfills_email",
        gateway_revalidates_kiro_device_poll_via_idc_refresh_and_backfills_email_impl,
    );
}

async fn gateway_revalidates_kiro_device_poll_via_idc_refresh_and_backfills_email_impl() {
    let upstream_hits = Arc::new(Mutex::new(0usize));
    let upstream_hits_clone = Arc::clone(&upstream_hits);
    let upstream = Router::new().fallback(any(move |_request: Request| {
        let upstream_hits_inner = Arc::clone(&upstream_hits_clone);
        async move {
            *upstream_hits_inner.lock().expect("mutex should lock") += 1;
            (StatusCode::OK, Body::from("unexpected upstream hit"))
        }
    }));

    let token_requests = Arc::new(Mutex::new(Vec::<String>::new()));
    let token_requests_clone = Arc::clone(&token_requests);
    let initial_access_token = sample_kiro_device_access_token_without_email();
    let refreshed_access_token = sample_kiro_device_access_token_without_email();
    let expected_refreshed_access_token = refreshed_access_token.clone();
    let token_server = Router::new().route(
        "/token",
        post(move |request: Request| {
            let token_requests_inner = Arc::clone(&token_requests_clone);
            let initial_access_token_inner = initial_access_token.clone();
            let refreshed_access_token_inner = refreshed_access_token.clone();
            async move {
                let raw_body = String::from_utf8(
                    to_bytes(request.into_body(), usize::MAX)
                        .await
                        .expect("body should read")
                        .to_vec(),
                )
                .expect("body should be utf8");
                token_requests_inner
                    .lock()
                    .expect("mutex should lock")
                    .push(raw_body.clone());
                if raw_body.contains("urn:ietf:params:oauth:grant-type:device_code") {
                    return Json(json!({
                        "accessToken": initial_access_token_inner,
                        "refreshToken": "kiro-device-refresh-token-initial",
                        "expiresIn": 1800,
                    }))
                    .into_response();
                }
                if raw_body.contains("\"grantType\":\"refresh_token\"") {
                    return Json(json!({
                        "accessToken": refreshed_access_token_inner,
                        "refreshToken": "kiro-device-refresh-token-rotated",
                        "expiresIn": 2400,
                    }))
                    .into_response();
                }
                (
                    StatusCode::BAD_REQUEST,
                    Json(json!({
                        "error": "unexpected_request",
                        "body": raw_body,
                    })),
                )
                    .into_response()
            }
        }),
    );

    let usage_hits = Arc::new(Mutex::new(0usize));
    let usage_hits_clone = Arc::clone(&usage_hits);
    let usage_server = Router::new().route(
        "/getUsageLimits",
        get(move |_request: Request| {
            let usage_hits_inner = Arc::clone(&usage_hits_clone);
            async move {
                *usage_hits_inner.lock().expect("mutex should lock") += 1;
                Json(json!({
                    "subscriptionInfo": {
                        "subscriptionTitle": "KIRO PRO+"
                    },
                    "usageBreakdownList": [{
                        "currentUsageWithPrecision": 5.0,
                        "usageLimitWithPrecision": 20.0,
                        "nextDateReset": 1_900_000_000u64
                    }],
                    "desktopUserInfo": {
                        "email": "kiro-usage@example.com"
                    }
                }))
            }
        }),
    );

    let mut provider = sample_provider("provider-kiro", "kiro", 10);
    provider.provider_type = "kiro".to_string();
    let provider_catalog_repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![provider],
        vec![],
        vec![],
    ));

    let (_upstream_url, upstream_handle) = start_server(upstream).await;
    let (token_url, token_handle) = start_server(token_server).await;
    let (usage_url, usage_handle) = start_server(usage_server).await;
    let state = AppState::new()
        .expect("gateway should build")
        .with_data_state_for_tests(
            GatewayDataState::with_provider_catalog_repository_for_tests(
                provider_catalog_repository.clone(),
            )
            .with_encryption_key_for_tests(DEV_FERNET_KEY),
        )
        .with_provider_oauth_device_session_entry_for_tests(
            "session-refresh-email",
            json!({
                "provider_id": "provider-kiro",
                "region": "us-east-1",
                "client_id": "kiro-device-client",
                "client_secret": "kiro-device-secret",
                "device_code": "device-code-123",
                "interval": 5,
                "expires_at_unix_secs": 4_102_444_800u64,
                "status": "pending",
                "proxy_node_id": "proxy-node-kiro",
                "created_at_unix_ms": 1_711_000_000u64,
                "key_id": null,
                "email": null,
                "replaced": false,
                "error_msg": null,
            }),
        )
        .with_provider_oauth_token_url_for_tests("kiro_device_poll", format!("{token_url}/token"))
        .with_provider_oauth_token_url_for_tests("kiro_idc_refresh", token_url.to_string())
        .with_provider_oauth_token_url_for_tests(
            "kiro_device_email",
            format!("{usage_url}/getUsageLimits"),
        );
    let gateway = build_router_with_state(state.clone());
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .post(format!(
            "{gateway_url}/api/admin/provider-oauth/providers/provider-kiro/device-poll"
        ))
        .bearer_auth(crate::tests::test_admin_access_token())
        .header("x-client-device-id", crate::tests::TEST_ADMIN_DEVICE_ID)
        .json(&json!({
            "session_id": "session-refresh-email"
        }))
        .send()
        .await
        .expect("request should succeed");

    let status = response.status();
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(status, StatusCode::OK, "payload={payload}");
    assert_eq!(payload["status"], "authorized");
    assert_eq!(payload["email"], "kiro-usage@example.com");
    assert_eq!(payload["replaced"], false);
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);
    assert_eq!(*usage_hits.lock().expect("mutex should lock"), 1);
    {
        let requests = token_requests.lock().expect("mutex should lock");
        assert_eq!(requests.len(), 2);
        assert!(
            requests
                .iter()
                .any(|body| body.contains("urn:ietf:params:oauth:grant-type:device_code")),
            "requests={requests:?}"
        );
        assert!(
            requests
                .iter()
                .any(|body| body.contains("\"grantType\":\"refresh_token\"")),
            "requests={requests:?}"
        );
    }

    let stored = state
        .load_provider_oauth_device_session_for_tests("device_auth_session:session-refresh-email")
        .expect("device session should persist");
    let stored: serde_json::Value =
        serde_json::from_str(&stored).expect("device session json should parse");
    assert_eq!(stored["status"], "authorized");
    assert_eq!(stored["email"], "kiro-usage@example.com");
    assert_eq!(stored["replaced"], false);
    let key_id = stored["key_id"]
        .as_str()
        .expect("key_id should be stored")
        .to_string();
    assert_eq!(payload["key_id"], key_id);

    let persisted = provider_catalog_repository
        .list_keys_by_ids(std::slice::from_ref(&key_id))
        .await
        .expect("keys should load")
        .into_iter()
        .next()
        .expect("persisted key should exist");
    assert_eq!(persisted.auth_type, "oauth");
    assert_eq!(
        persisted.proxy,
        Some(json!({"node_id": "proxy-node-kiro", "enabled": true}))
    );
    let decrypted_api_key = decrypt_persisted_provider_api_key(&persisted);
    assert_eq!(decrypted_api_key, expected_refreshed_access_token);
    let decrypted_auth_config = decrypt_persisted_provider_auth_config(&persisted);
    let auth_config: serde_json::Value =
        serde_json::from_str(&decrypted_auth_config).expect("auth config should parse");
    assert_eq!(auth_config["provider_type"], "kiro");
    assert_eq!(auth_config["auth_method"], "idc");
    assert_eq!(
        auth_config["refresh_token"],
        "kiro-device-refresh-token-rotated"
    );
    assert_eq!(auth_config["email"], "kiro-usage@example.com");
    assert_eq!(auth_config["client_id"], "kiro-device-client");
    assert_eq!(auth_config["client_secret"], "kiro-device-secret");
    assert_eq!(auth_config["region"], "us-east-1");

    gateway_handle.abort();
    usage_handle.abort();
    token_handle.abort();
    upstream_handle.abort();
}

#[test]
fn gateway_imports_admin_provider_oauth_refresh_token_locally_with_trusted_admin_principal() {
    run_admin_oauth_test(
        "gateway_imports_admin_provider_oauth_refresh_token_locally_with_trusted_admin_principal",
        gateway_imports_admin_provider_oauth_refresh_token_locally_with_trusted_admin_principal_impl,
    );
}

async fn gateway_imports_admin_provider_oauth_refresh_token_locally_with_trusted_admin_principal_impl(
) {
    #[derive(Debug, Clone)]
    struct SeenTokenRequest {
        content_type: String,
        body: String,
    }

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
    let seen_token = Arc::new(Mutex::new(None::<SeenTokenRequest>));
    let seen_token_clone = Arc::clone(&seen_token);
    let token_server = Router::new().route(
        "/oauth/token",
        post(move |headers: HeaderMap, body: Bytes| {
            let token_hits_inner = Arc::clone(&token_hits_clone);
            let seen_token_inner = Arc::clone(&seen_token_clone);
            async move {
                *token_hits_inner.lock().expect("mutex should lock") += 1;
                *seen_token_inner.lock().expect("mutex should lock") = Some(SeenTokenRequest {
                    content_type: headers
                        .get(http::header::CONTENT_TYPE)
                        .and_then(|value| value.to_str().ok())
                        .unwrap_or_default()
                        .to_string(),
                    body: String::from_utf8(body.to_vec()).unwrap_or_default(),
                });
                Json(json!({
                    "access_token": "imported-codex-access-token",
                    "refresh_token": "imported-codex-refresh-token",
                    "token_type": "Bearer",
                    "expires_in": 1800,
                    "scope": "openid email profile offline_access",
                    "email": "alice@example.com",
                    "account_id": "acct-codex-123",
                    "plan_type": "plus",
                }))
            }
        }),
    );

    let mut provider = sample_provider("provider-codex", "codex", 10);
    provider.provider_type = "codex".to_string();
    let endpoint = sample_endpoint(
        "endpoint-codex-chat",
        "provider-codex",
        "openai:chat",
        "https://chatgpt.com/backend-api/codex",
    );

    let mut existing_key = sample_key(
        "key-codex-import-duplicate",
        "provider-codex",
        "openai:chat",
        "stale-imported-access-token",
    );
    existing_key.auth_type = "oauth".to_string();
    existing_key.is_active = false;
    existing_key.encrypted_auth_config = Some(
        encrypt_python_fernet_plaintext(
            DEV_FERNET_KEY,
            r#"{"provider_type":"codex","email":"alice@example.com","account_id":"acct-codex-123","plan_type":"plus","refresh_token":"old-refresh-token"}"#,
        )
        .expect("auth config ciphertext should build"),
    );

    let provider_catalog_repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![provider],
        vec![endpoint],
        vec![existing_key],
    ));

    let (_upstream_url, upstream_handle) = start_server(upstream).await;
    let (token_url, token_handle) = start_server(token_server).await;
    let gateway = build_router_with_state(
        AppState::new()
            .expect("gateway should build")
            .with_data_state_for_tests(
                GatewayDataState::with_provider_catalog_repository_for_tests(
                    provider_catalog_repository.clone(),
                )
                .with_encryption_key_for_tests(DEV_FERNET_KEY),
            )
            .with_provider_oauth_token_url_for_tests("codex", format!("{token_url}/oauth/token")),
    );
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .post(format!(
            "{gateway_url}/api/admin/provider-oauth/providers/provider-codex/import-refresh-token"
        ))
        .bearer_auth(crate::tests::test_admin_access_token())
        .header("x-client-device-id", crate::tests::TEST_ADMIN_DEVICE_ID)
        .json(&json!({
            "refresh_token": "provider-import-refresh-token",
            "proxy_node_id": "proxy-node-codex-import",
            "name": "should-not-override-inactive-name"
        }))
        .send()
        .await
        .expect("request should succeed");

    let status = response.status();
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(status, StatusCode::OK, "payload={payload}");
    assert_eq!(payload["key_id"], "key-codex-import-duplicate");
    assert_eq!(payload["provider_type"], "codex");
    assert_eq!(payload["has_refresh_token"], true);
    assert_eq!(payload["email"], "alice@example.com");
    assert_eq!(payload["replaced"], true);
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);
    assert_eq!(*token_hits.lock().expect("mutex should lock"), 1);

    let seen_token = seen_token
        .lock()
        .expect("mutex should lock")
        .clone()
        .expect("token request should be recorded");
    assert_eq!(seen_token.content_type, "application/x-www-form-urlencoded");
    assert!(seen_token.body.contains("grant_type=refresh_token"));
    assert!(seen_token
        .body
        .contains("refresh_token=provider-import-refresh-token"));

    let reloaded = provider_catalog_repository
        .list_keys_by_ids(&["key-codex-import-duplicate".to_string()])
        .await
        .expect("keys should load");
    let persisted = reloaded.first().expect("persisted key should exist");
    assert!(persisted.is_active);
    assert_eq!(
        persisted.proxy,
        Some(json!({"node_id": "proxy-node-codex-import", "enabled": true}))
    );
    let decrypted_api_key = decrypt_persisted_provider_api_key(persisted);
    assert_eq!(decrypted_api_key, "imported-codex-access-token");
    let decrypted_auth_config = decrypt_persisted_provider_auth_config(persisted);
    let auth_config: serde_json::Value =
        serde_json::from_str(&decrypted_auth_config).expect("auth config json should parse");
    assert_eq!(auth_config["provider_type"], "codex");
    assert_eq!(auth_config["refresh_token"], "imported-codex-refresh-token");
    assert_eq!(auth_config["email"], "alice@example.com");
    assert_eq!(auth_config["account_id"], "acct-codex-123");
    assert_eq!(auth_config["plan_type"], "plus");

    gateway_handle.abort();
    token_handle.abort();
    upstream_handle.abort();
}

#[test]
fn gateway_imports_codex_access_token_without_refresh_token_as_temporary_account() {
    run_admin_oauth_test(
        "gateway_imports_codex_access_token_without_refresh_token_as_temporary_account",
        gateway_imports_codex_access_token_without_refresh_token_as_temporary_account_impl,
    );
}

async fn gateway_imports_codex_access_token_without_refresh_token_as_temporary_account_impl() {
    let token_hits = Arc::new(Mutex::new(0usize));
    let token_hits_clone = Arc::clone(&token_hits);
    let token_server = Router::new().route(
        "/oauth/token",
        post(move || {
            let token_hits_inner = Arc::clone(&token_hits_clone);
            async move {
                *token_hits_inner.lock().expect("mutex should lock") += 1;
                (
                    StatusCode::BAD_REQUEST,
                    Json(json!({"error": "unexpected refresh exchange"})),
                )
            }
        }),
    );

    let mut provider = sample_provider("provider-codex", "codex", 10);
    provider.provider_type = "codex".to_string();
    let endpoint = sample_endpoint(
        "endpoint-codex-chat",
        "provider-codex",
        "openai:chat",
        "https://chatgpt.com/backend-api/codex",
    );
    let provider_catalog_repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![provider],
        vec![endpoint],
        vec![],
    ));

    let (token_url, token_handle) = start_server(token_server).await;
    let gateway = build_router_with_state(
        AppState::new()
            .expect("gateway should build")
            .with_data_state_for_tests(
                GatewayDataState::with_provider_catalog_repository_for_tests(
                    provider_catalog_repository.clone(),
                )
                .with_encryption_key_for_tests(DEV_FERNET_KEY),
            )
            .with_provider_oauth_token_url_for_tests("codex", format!("{token_url}/oauth/token")),
    );
    let (gateway_url, gateway_handle) = start_server(gateway).await;
    let access_token =
        sample_codex_access_token_with_profile_email("profile@example.com", "acct-profile-123");

    let response = reqwest::Client::new()
        .post(format!(
            "{gateway_url}/api/admin/provider-oauth/providers/provider-codex/import-refresh-token"
        ))
        .bearer_auth(crate::tests::test_admin_access_token())
        .header("x-client-device-id", crate::tests::TEST_ADMIN_DEVICE_ID)
        .json(&json!({
            "access_token": access_token,
            "name": "temporary-codex-access-token",
        }))
        .send()
        .await
        .expect("request should succeed");

    let status = response.status();
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(status, StatusCode::OK, "payload={payload}");
    assert_eq!(payload["provider_type"], "codex");
    assert_eq!(payload["has_refresh_token"], false);
    assert_eq!(payload["temporary"], true);
    assert_eq!(payload["email"], "profile@example.com");
    assert_eq!(*token_hits.lock().expect("mutex should lock"), 0);

    let reloaded = provider_catalog_repository
        .list_keys_by_provider_ids(&["provider-codex".to_string()])
        .await
        .expect("keys should load");
    let persisted = reloaded.first().expect("persisted key should exist");
    assert_eq!(persisted.expires_at_unix_secs, Some(2_000_000_000));
    let decrypted_api_key = decrypt_persisted_provider_api_key(persisted);
    assert_eq!(decrypted_api_key, access_token);
    let decrypted_auth_config = decrypt_persisted_provider_auth_config(persisted);
    let auth_config: serde_json::Value =
        serde_json::from_str(&decrypted_auth_config).expect("auth config json should parse");
    assert_eq!(auth_config["provider_type"], "codex");
    assert_eq!(auth_config["access_token_import_temporary"], true);
    assert_eq!(auth_config["email"], "profile@example.com");
    assert_eq!(auth_config["account_id"], "acct-profile-123");
    assert!(auth_config.get("refresh_token").is_none());

    gateway_handle.abort();
    token_handle.abort();
}

#[test]
fn gateway_imports_admin_provider_oauth_refresh_token_over_active_expired_duplicate() {
    run_admin_oauth_test(
        "gateway_imports_admin_provider_oauth_refresh_token_over_active_expired_duplicate",
        gateway_imports_admin_provider_oauth_refresh_token_over_active_expired_duplicate_impl,
    );
}

async fn gateway_imports_admin_provider_oauth_refresh_token_over_active_expired_duplicate_impl() {
    #[derive(Debug, Clone)]
    struct SeenTokenRequest {
        content_type: String,
        body: String,
    }

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
    let seen_token = Arc::new(Mutex::new(None::<SeenTokenRequest>));
    let seen_token_clone = Arc::clone(&seen_token);
    let token_server = Router::new().route(
        "/oauth/token",
        post(move |headers: HeaderMap, body: Bytes| {
            let token_hits_inner = Arc::clone(&token_hits_clone);
            let seen_token_inner = Arc::clone(&seen_token_clone);
            async move {
                *token_hits_inner.lock().expect("mutex should lock") += 1;
                *seen_token_inner.lock().expect("mutex should lock") = Some(SeenTokenRequest {
                    content_type: headers
                        .get(http::header::CONTENT_TYPE)
                        .and_then(|value| value.to_str().ok())
                        .unwrap_or_default()
                        .to_string(),
                    body: String::from_utf8(body.to_vec()).unwrap_or_default(),
                });
                Json(json!({
                    "access_token": "imported-expired-codex-access-token",
                    "refresh_token": "imported-expired-codex-refresh-token",
                    "token_type": "Bearer",
                    "expires_in": 1800,
                    "scope": "openid email profile offline_access",
                    "email": "alice@example.com",
                    "account_id": "acct-codex-123",
                    "plan_type": "plus",
                }))
            }
        }),
    );

    let mut provider = sample_provider("provider-codex", "codex", 10);
    provider.provider_type = "codex".to_string();
    let endpoint = sample_endpoint(
        "endpoint-codex-chat",
        "provider-codex",
        "openai:chat",
        "https://chatgpt.com/backend-api/codex",
    );

    let mut existing_key = sample_key(
        "key-codex-import-expired-duplicate",
        "provider-codex",
        "openai:chat",
        "stale-imported-access-token",
    );
    existing_key.auth_type = "oauth".to_string();
    existing_key.is_active = true;
    existing_key.expires_at_unix_secs = Some(1);
    existing_key.oauth_invalid_at_unix_secs = Some(1_700_000_000);
    existing_key.oauth_invalid_reason =
        Some("[REFRESH_FAILED] refresh_token 无效、已过期或已撤销，请重新登录授权".to_string());
    existing_key.encrypted_auth_config = Some(
        encrypt_python_fernet_plaintext(
            DEV_FERNET_KEY,
            r#"{"provider_type":"codex","email":"alice@example.com","account_id":"acct-codex-123","plan_type":"plus","refresh_token":"old-refresh-token","expires_at":1}"#,
        )
        .expect("auth config ciphertext should build"),
    );

    let provider_catalog_repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![provider],
        vec![endpoint],
        vec![existing_key],
    ));

    let (_upstream_url, upstream_handle) = start_server(upstream).await;
    let (token_url, token_handle) = start_server(token_server).await;
    let gateway = build_router_with_state(
        AppState::new()
            .expect("gateway should build")
            .with_data_state_for_tests(
                GatewayDataState::with_provider_catalog_repository_for_tests(
                    provider_catalog_repository.clone(),
                )
                .with_encryption_key_for_tests(DEV_FERNET_KEY),
            )
            .with_provider_oauth_token_url_for_tests("codex", format!("{token_url}/oauth/token")),
    );
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .post(format!(
            "{gateway_url}/api/admin/provider-oauth/providers/provider-codex/import-refresh-token"
        ))
        .bearer_auth(crate::tests::test_admin_access_token())
        .header("x-client-device-id", crate::tests::TEST_ADMIN_DEVICE_ID)
        .json(&json!({
            "refresh_token": "provider-import-refresh-token",
            "proxy_node_id": "proxy-node-codex-import",
            "name": "should-not-override-active-expired-name"
        }))
        .send()
        .await
        .expect("request should succeed");

    let status = response.status();
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(status, StatusCode::OK, "payload={payload}");
    assert_eq!(payload["key_id"], "key-codex-import-expired-duplicate");
    assert_eq!(payload["provider_type"], "codex");
    assert_eq!(payload["has_refresh_token"], true);
    assert_eq!(payload["email"], "alice@example.com");
    assert_eq!(payload["replaced"], true);
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);
    assert_eq!(*token_hits.lock().expect("mutex should lock"), 1);

    let seen_token = seen_token
        .lock()
        .expect("mutex should lock")
        .clone()
        .expect("token request should be recorded");
    assert_eq!(seen_token.content_type, "application/x-www-form-urlencoded");
    assert!(seen_token.body.contains("grant_type=refresh_token"));
    assert!(seen_token
        .body
        .contains("refresh_token=provider-import-refresh-token"));

    let reloaded = provider_catalog_repository
        .list_keys_by_ids(&["key-codex-import-expired-duplicate".to_string()])
        .await
        .expect("keys should load");
    let persisted = reloaded.first().expect("persisted key should exist");
    assert!(persisted.is_active);
    assert_eq!(
        persisted.proxy,
        Some(json!({"node_id": "proxy-node-codex-import", "enabled": true}))
    );
    assert_eq!(persisted.oauth_invalid_at_unix_secs, None);
    assert_eq!(persisted.oauth_invalid_reason, None);
    let decrypted_api_key = decrypt_persisted_provider_api_key(persisted);
    assert_eq!(decrypted_api_key, "imported-expired-codex-access-token");
    let decrypted_auth_config = decrypt_persisted_provider_auth_config(persisted);
    let auth_config: serde_json::Value =
        serde_json::from_str(&decrypted_auth_config).expect("auth config json should parse");
    assert_eq!(auth_config["provider_type"], "codex");
    assert_eq!(
        auth_config["refresh_token"],
        "imported-expired-codex-refresh-token"
    );
    assert_eq!(auth_config["email"], "alice@example.com");
    assert_eq!(auth_config["account_id"], "acct-codex-123");
    assert_eq!(auth_config["plan_type"], "plus");

    gateway_handle.abort();
    token_handle.abort();
    upstream_handle.abort();
}

#[test]
fn gateway_rejects_kiro_single_refresh_token_import_with_clear_error() {
    run_admin_oauth_test(
        "gateway_rejects_kiro_single_refresh_token_import_with_clear_error",
        gateway_rejects_kiro_single_refresh_token_import_with_clear_error_impl,
    );
}

async fn gateway_rejects_kiro_single_refresh_token_import_with_clear_error_impl() {
    let provider_catalog_repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![{
            let mut provider = sample_provider("provider-kiro", "kiro", 10);
            provider.provider_type = "kiro".to_string();
            provider
        }],
        vec![sample_endpoint(
            "endpoint-kiro-chat",
            "provider-kiro",
            "kiro:generateAssistantResponse",
            "https://service.kiro.dev",
        )],
        Vec::new(),
    ));
    let state = AppState::new()
        .expect("gateway should build")
        .with_data_state_for_tests(GatewayDataState::with_provider_catalog_reader_for_tests(
            provider_catalog_repository,
        ));

    let response = local_admin_provider_oauth_response(
        &state,
        http::Method::POST,
        "/api/admin/provider-oauth/providers/provider-kiro/import-refresh-token",
        Some(json!({
            "refresh_token": "kiro-refresh-token"
        })),
    )
    .await;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let payload: serde_json::Value = serde_json::from_slice(
        &to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body should read"),
    )
    .expect("json body should parse");
    assert_eq!(
        payload["detail"],
        json!("Kiro 不支持单条 Refresh Token 导入，请使用批量导入或设备授权。")
    );
}

#[test]
fn gateway_imports_admin_provider_oauth_refresh_token_via_execution_runtime_proxy_node() {
    run_admin_oauth_test(
        "gateway_imports_admin_provider_oauth_refresh_token_via_execution_runtime_proxy_node",
        gateway_imports_admin_provider_oauth_refresh_token_via_execution_runtime_proxy_node_impl,
    );
}

async fn gateway_imports_admin_provider_oauth_refresh_token_via_execution_runtime_proxy_node_impl()
{
    let execution_plans = Arc::new(Mutex::new(Vec::<ExecutionPlan>::new()));
    let execution_plans_clone = Arc::clone(&execution_plans);
    let execution_runtime = Router::new().route(
        "/v1/execute/sync",
        any(move |Json(plan): Json<ExecutionPlan>| {
            let execution_plans_inner = Arc::clone(&execution_plans_clone);
            async move {
                execution_plans_inner
                    .lock()
                    .expect("mutex should lock")
                    .push(plan.clone());
                let proxy = plan.proxy.as_ref().expect("proxy snapshot should exist");
                assert_eq!(proxy.node_id.as_deref(), Some("proxy-node-codex-import"));
                let timeouts = plan.timeouts.as_ref().expect("timeouts should exist");
                assert_eq!(timeouts.connect_ms, Some(60_000));
                assert_eq!(timeouts.read_ms, Some(60_000));
                assert_eq!(timeouts.write_ms, Some(60_000));
                assert_eq!(timeouts.pool_ms, Some(60_000));
                assert_eq!(timeouts.total_ms, Some(60_000));
                if plan.request_id == "provider-oauth:refresh-token" {
                    assert_eq!(plan.method, "POST");
                    assert_eq!(plan.url, "https://oauth.example/oauth/token");
                    assert_eq!(
                        plan.headers.get("content-type").map(String::as_str),
                        Some("application/x-www-form-urlencoded")
                    );
                    assert_eq!(
                        plan.headers
                            .get(EXECUTION_REQUEST_FOLLOW_REDIRECTS_HEADER)
                            .map(String::as_str),
                        Some("true")
                    );
                    Json(codex_import_token_execution_result(&plan.request_id))
                } else {
                    assert!(
                        plan.request_id.starts_with("codex-quota:"),
                        "unexpected execution plan: {}",
                        plan.request_id
                    );
                    Json(codex_quota_execution_result(&plan.request_id))
                }
            }
        }),
    );

    let mut provider = sample_provider("provider-codex", "codex", 10);
    provider.provider_type = "codex".to_string();
    let endpoint = sample_endpoint(
        "endpoint-codex-chat",
        "provider-codex",
        "openai:chat",
        "https://chatgpt.com/backend-api/codex",
    );
    let provider_catalog_repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![provider],
        vec![endpoint],
        vec![],
    ));
    let mut manual_node = sample_proxy_node("proxy-node-codex-import");
    manual_node.status = "online".to_string();
    manual_node.proxy_url = Some("http://proxy.example:8080".to_string());
    let proxy_node_repository = Arc::new(InMemoryProxyNodeRepository::seed(vec![manual_node]));

    let (execution_runtime_url, execution_runtime_handle) = start_server(execution_runtime).await;
    let gateway = build_router_with_state(
        build_state_with_execution_runtime_override(execution_runtime_url)
            .with_data_state_for_tests(
                GatewayDataState::with_provider_catalog_repository_for_tests(
                    provider_catalog_repository.clone(),
                )
                .attach_proxy_node_repository_for_tests(proxy_node_repository)
                .with_encryption_key_for_tests(DEV_FERNET_KEY),
            )
            .with_provider_oauth_token_url_for_tests("codex", "https://oauth.example/oauth/token"),
    );
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .post(format!(
            "{gateway_url}/api/admin/provider-oauth/providers/provider-codex/import-refresh-token"
        ))
        .bearer_auth(crate::tests::test_admin_access_token())
        .header("x-client-device-id", crate::tests::TEST_ADMIN_DEVICE_ID)
        .json(&json!({
            "refresh_token": "provider-import-refresh-token",
            "proxy_node_id": "proxy-node-codex-import",
            "name": "codex-import"
        }))
        .send()
        .await
        .expect("request should succeed");

    let status = response.status();
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(status, StatusCode::OK, "payload={payload}");
    assert_eq!(payload["provider_type"], "codex");
    assert_eq!(payload["has_refresh_token"], true);
    assert_eq!(payload["email"], "alice@example.com");
    assert_eq!(payload["replaced"], false);

    let keys = provider_catalog_repository
        .list_keys_by_provider_ids(&["provider-codex".to_string()])
        .await
        .expect("keys should load");
    assert_eq!(keys.len(), 1);
    assert_eq!(
        keys[0].proxy,
        Some(json!({"node_id": "proxy-node-codex-import", "enabled": true}))
    );

    let plans = execution_plans.lock().expect("mutex should lock");
    let token_plan = assert_single_provider_oauth_refresh_token_plan(&plans);
    assert_eq!(
        token_plan
            .proxy
            .as_ref()
            .and_then(|proxy| proxy.node_id.as_deref()),
        Some("proxy-node-codex-import")
    );

    gateway_handle.abort();
    execution_runtime_handle.abort();
}

#[test]
fn gateway_imports_admin_provider_oauth_refresh_token_via_execution_runtime_provider_proxy_before_system_proxy(
) {
    run_admin_oauth_test(
        "gateway_imports_admin_provider_oauth_refresh_token_via_execution_runtime_provider_proxy_before_system_proxy",
        gateway_imports_admin_provider_oauth_refresh_token_via_execution_runtime_provider_proxy_before_system_proxy_impl,
    );
}

async fn gateway_imports_admin_provider_oauth_refresh_token_via_execution_runtime_provider_proxy_before_system_proxy_impl(
) {
    let execution_plans = Arc::new(Mutex::new(Vec::<ExecutionPlan>::new()));
    let execution_plans_clone = Arc::clone(&execution_plans);
    let execution_runtime = Router::new().route(
        "/v1/execute/sync",
        any(move |Json(plan): Json<ExecutionPlan>| {
            let execution_plans_inner = Arc::clone(&execution_plans_clone);
            async move {
                execution_plans_inner
                    .lock()
                    .expect("mutex should lock")
                    .push(plan.clone());
                let proxy = plan.proxy.as_ref().expect("proxy snapshot should exist");
                assert_eq!(proxy.node_id.as_deref(), Some("proxy-node-codex-provider"));
                if plan.request_id == "provider-oauth:refresh-token" {
                    Json(codex_import_token_execution_result(&plan.request_id))
                } else {
                    assert!(
                        plan.request_id.starts_with("codex-quota:"),
                        "unexpected execution plan: {}",
                        plan.request_id
                    );
                    Json(codex_quota_execution_result(&plan.request_id))
                }
            }
        }),
    );

    let mut provider = sample_provider("provider-codex", "codex", 10);
    provider.provider_type = "codex".to_string();
    provider.proxy = Some(json!({"node_id":"proxy-node-codex-provider","enabled":true}));
    let endpoint = sample_endpoint(
        "endpoint-codex-chat",
        "provider-codex",
        "openai:chat",
        "https://chatgpt.com/backend-api/codex",
    );
    let provider_catalog_repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![provider],
        vec![endpoint],
        vec![],
    ));
    let mut provider_node = sample_proxy_node("proxy-node-codex-provider");
    provider_node.status = "online".to_string();
    provider_node.proxy_url = Some("http://proxy-provider.example:8080".to_string());
    let mut system_node = sample_proxy_node("proxy-node-codex-system");
    system_node.status = "online".to_string();
    system_node.proxy_url = Some("http://proxy-system.example:8080".to_string());
    let proxy_node_repository = Arc::new(InMemoryProxyNodeRepository::seed(vec![
        provider_node,
        system_node,
    ]));

    let (execution_runtime_url, execution_runtime_handle) = start_server(execution_runtime).await;
    let gateway = build_router_with_state(
        build_state_with_execution_runtime_override(execution_runtime_url)
            .with_data_state_for_tests(
                GatewayDataState::with_provider_catalog_repository_for_tests(
                    provider_catalog_repository.clone(),
                )
                .attach_proxy_node_repository_for_tests(proxy_node_repository)
                .with_system_config_values_for_tests(vec![(
                    "system_proxy_node_id".to_string(),
                    json!("proxy-node-codex-system"),
                )])
                .with_encryption_key_for_tests(DEV_FERNET_KEY),
            )
            .with_provider_oauth_token_url_for_tests("codex", "https://oauth.example/oauth/token"),
    );
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .post(format!(
            "{gateway_url}/api/admin/provider-oauth/providers/provider-codex/import-refresh-token"
        ))
        .bearer_auth(crate::tests::test_admin_access_token())
        .header("x-client-device-id", crate::tests::TEST_ADMIN_DEVICE_ID)
        .json(&json!({
            "refresh_token": "provider-import-refresh-token",
            "name": "codex-import"
        }))
        .send()
        .await
        .expect("request should succeed");

    let status = response.status();
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(status, StatusCode::OK, "payload={payload}");
    assert_eq!(payload["provider_type"], "codex");

    let keys = provider_catalog_repository
        .list_keys_by_provider_ids(&["provider-codex".to_string()])
        .await
        .expect("keys should load");
    assert_eq!(keys.len(), 1);
    assert_eq!(keys[0].proxy, None);

    let plans = execution_plans.lock().expect("mutex should lock");
    let token_plan = assert_single_provider_oauth_refresh_token_plan(&plans);
    assert_eq!(
        token_plan
            .proxy
            .as_ref()
            .and_then(|proxy| proxy.node_id.as_deref()),
        Some("proxy-node-codex-provider")
    );

    gateway_handle.abort();
    execution_runtime_handle.abort();
}

#[test]
fn gateway_imports_admin_provider_oauth_refresh_token_via_execution_runtime_system_proxy() {
    run_admin_oauth_test(
        "gateway_imports_admin_provider_oauth_refresh_token_via_execution_runtime_system_proxy",
        gateway_imports_admin_provider_oauth_refresh_token_via_execution_runtime_system_proxy_impl,
    );
}

async fn gateway_imports_admin_provider_oauth_refresh_token_via_execution_runtime_system_proxy_impl(
) {
    let execution_plans = Arc::new(Mutex::new(Vec::<ExecutionPlan>::new()));
    let execution_plans_clone = Arc::clone(&execution_plans);
    let execution_runtime = Router::new().route(
        "/v1/execute/sync",
        any(move |Json(plan): Json<ExecutionPlan>| {
            let execution_plans_inner = Arc::clone(&execution_plans_clone);
            async move {
                execution_plans_inner
                    .lock()
                    .expect("mutex should lock")
                    .push(plan.clone());
                let proxy = plan.proxy.as_ref().expect("proxy snapshot should exist");
                assert_eq!(proxy.node_id.as_deref(), Some("proxy-node-codex-system"));
                if plan.request_id == "provider-oauth:refresh-token" {
                    Json(codex_import_token_execution_result(&plan.request_id))
                } else {
                    assert!(
                        plan.request_id.starts_with("codex-quota:"),
                        "unexpected execution plan: {}",
                        plan.request_id
                    );
                    Json(codex_quota_execution_result(&plan.request_id))
                }
            }
        }),
    );

    let mut provider = sample_provider("provider-codex", "codex", 10);
    provider.provider_type = "codex".to_string();
    let endpoint = sample_endpoint(
        "endpoint-codex-chat",
        "provider-codex",
        "openai:chat",
        "https://chatgpt.com/backend-api/codex",
    );
    let provider_catalog_repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![provider],
        vec![endpoint],
        vec![],
    ));
    let mut manual_node = sample_proxy_node("proxy-node-codex-system");
    manual_node.status = "online".to_string();
    manual_node.proxy_url = Some("http://proxy.example:8080".to_string());
    let proxy_node_repository = Arc::new(InMemoryProxyNodeRepository::seed(vec![manual_node]));

    let (execution_runtime_url, execution_runtime_handle) = start_server(execution_runtime).await;
    let gateway = build_router_with_state(
        build_state_with_execution_runtime_override(execution_runtime_url)
            .with_data_state_for_tests(
                GatewayDataState::with_provider_catalog_repository_for_tests(
                    provider_catalog_repository.clone(),
                )
                .attach_proxy_node_repository_for_tests(proxy_node_repository)
                .with_system_config_values_for_tests(vec![(
                    "system_proxy_node_id".to_string(),
                    json!("proxy-node-codex-system"),
                )])
                .with_encryption_key_for_tests(DEV_FERNET_KEY),
            )
            .with_provider_oauth_token_url_for_tests("codex", "https://oauth.example/oauth/token"),
    );
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .post(format!(
            "{gateway_url}/api/admin/provider-oauth/providers/provider-codex/import-refresh-token"
        ))
        .bearer_auth(crate::tests::test_admin_access_token())
        .header("x-client-device-id", crate::tests::TEST_ADMIN_DEVICE_ID)
        .json(&json!({
            "refresh_token": "provider-import-refresh-token",
            "name": "codex-import"
        }))
        .send()
        .await
        .expect("request should succeed");

    let status = response.status();
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(status, StatusCode::OK, "payload={payload}");
    assert_eq!(payload["provider_type"], "codex");

    let plans = execution_plans.lock().expect("mutex should lock");
    let token_plan = assert_single_provider_oauth_refresh_token_plan(&plans);
    assert_eq!(
        token_plan
            .proxy
            .as_ref()
            .and_then(|proxy| proxy.node_id.as_deref()),
        Some("proxy-node-codex-system")
    );

    gateway_handle.abort();
    execution_runtime_handle.abort();
}

#[test]
fn gateway_import_refresh_token_surfaces_execution_runtime_error_detail() {
    run_admin_oauth_test(
        "gateway_import_refresh_token_surfaces_execution_runtime_error_detail",
        gateway_import_refresh_token_surfaces_execution_runtime_error_detail_impl,
    );
}

async fn gateway_import_refresh_token_surfaces_execution_runtime_error_detail_impl() {
    let execution_runtime = Router::new().route(
        "/v1/execute/sync",
        any(|| async { StatusCode::INTERNAL_SERVER_ERROR }),
    );

    let mut provider = sample_provider("provider-codex", "codex", 10);
    provider.provider_type = "codex".to_string();
    let endpoint = sample_endpoint(
        "endpoint-codex-chat",
        "provider-codex",
        "openai:chat",
        "https://chatgpt.com/backend-api/codex",
    );
    let provider_catalog_repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![provider],
        vec![endpoint],
        vec![],
    ));

    let (execution_runtime_url, execution_runtime_handle) = start_server(execution_runtime).await;
    let gateway = build_router_with_state(
        build_state_with_execution_runtime_override(execution_runtime_url)
            .with_data_state_for_tests(
                GatewayDataState::with_provider_catalog_repository_for_tests(
                    provider_catalog_repository,
                )
                .with_encryption_key_for_tests(DEV_FERNET_KEY),
            )
            .with_provider_oauth_token_url_for_tests("codex", "https://oauth.example/oauth/token"),
    );
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .post(format!(
            "{gateway_url}/api/admin/provider-oauth/providers/provider-codex/import-refresh-token"
        ))
        .bearer_auth(crate::tests::test_admin_access_token())
        .header("x-client-device-id", crate::tests::TEST_ADMIN_DEVICE_ID)
        .json(&json!({
            "refresh_token": "provider-import-refresh-token",
            "name": "codex-import"
        }))
        .send()
        .await
        .expect("request should succeed");

    let status = response.status();
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(status, StatusCode::BAD_REQUEST, "payload={payload}");
    assert_eq!(
        payload["detail"], "Refresh Token 验证失败: token exchange 失败",
        "payload={payload}"
    );

    gateway_handle.abort();
    execution_runtime_handle.abort();
}

#[test]
fn gateway_marks_lazy_codex_oauth_refresh_failures_as_invalid() {
    run_admin_oauth_test(
        "gateway_marks_lazy_codex_oauth_refresh_failures_as_invalid",
        gateway_marks_lazy_codex_oauth_refresh_failures_as_invalid_impl,
    );
}

async fn gateway_marks_lazy_codex_oauth_refresh_failures_as_invalid_impl() {
    let token_hits = Arc::new(Mutex::new(0usize));
    let token_hits_clone = Arc::clone(&token_hits);
    let token_server = Router::new().route(
        "/oauth/token",
        post(move |_request: Request| {
            let token_hits_inner = Arc::clone(&token_hits_clone);
            async move {
                *token_hits_inner.lock().expect("mutex should lock") += 1;
                (
                    StatusCode::UNAUTHORIZED,
                    Json(json!({
                        "error": {
                            "message": "Your refresh token has already been used to generate a new access token. Please try signing in again.",
                            "type": "invalid_request_error",
                            "param": serde_json::Value::Null,
                            "code": "refresh_token_reused"
                        }
                    })),
                )
            }
        }),
    );

    let mut provider = sample_provider("provider-codex", "codex", 10);
    provider.provider_type = "codex".to_string();
    let endpoint = sample_endpoint(
        "endpoint-codex-cli",
        "provider-codex",
        "openai:responses",
        "https://chatgpt.com/backend-api/codex",
    );

    let mut key = sample_key(
        "key-codex-oauth-lazy",
        "provider-codex",
        "openai:responses",
        "stale-codex-access-token",
    );
    key.auth_type = "oauth".to_string();
    key.expires_at_unix_secs = Some(1);
    key.encrypted_auth_config = Some(
        encrypt_python_fernet_plaintext(
            DEV_FERNET_KEY,
            r#"{"provider_type":"codex","refresh_token":"used-refresh-token","email":"alice@example.com","account_id":"acct-codex-123","plan_type":"plus","expires_at":1}"#,
        )
        .expect("auth config ciphertext should build"),
    );

    let provider_catalog_repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![provider],
        vec![endpoint],
        vec![key],
    ));

    let (token_url, token_handle) = start_server(token_server).await;
    let oauth_refresh =
        crate::provider_transport::LocalOAuthRefreshCoordinator::with_adapters_for_tests(vec![
            Arc::new(
                crate::provider_transport::oauth_refresh::GenericOAuthRefreshAdapter::default()
                    .with_token_url_for_tests("codex", format!("{token_url}/oauth/token")),
            ),
        ]);
    let state = AppState::new()
        .expect("state should build")
        .with_data_state_for_tests(
            GatewayDataState::with_provider_catalog_repository_for_tests(
                provider_catalog_repository.clone(),
            )
            .with_encryption_key_for_tests(DEV_FERNET_KEY),
        )
        .with_oauth_refresh_coordinator_for_tests(oauth_refresh);

    let transport = state
        .read_provider_transport_snapshot(
            "provider-codex",
            "endpoint-codex-cli",
            "key-codex-oauth-lazy",
        )
        .await
        .expect("transport snapshot should load")
        .expect("transport snapshot should exist");
    let resolved = state
        .resolve_local_oauth_request_auth(&transport)
        .await
        .expect("refresh-token reuse should degrade into oauth-unavailable");

    assert_eq!(resolved, None);
    assert_eq!(*token_hits.lock().expect("mutex should lock"), 1);

    let stored_key = provider_catalog_repository
        .list_keys_by_ids(&["key-codex-oauth-lazy".to_string()])
        .await
        .expect("keys should list")
        .into_iter()
        .next()
        .expect("oauth key should exist");
    assert!(stored_key.oauth_invalid_at_unix_secs.is_some());
    assert_eq!(
        stored_key.oauth_invalid_reason.as_deref(),
        Some("[REFRESH_FAILED] Token 续期失败 (401): refresh_token 已被使用并轮换，请重新登录授权")
    );
    let oauth_snapshot = stored_key
        .status_snapshot
        .as_ref()
        .and_then(serde_json::Value::as_object)
        .and_then(|snapshot| snapshot.get("oauth"))
        .and_then(serde_json::Value::as_object)
        .expect("oauth status snapshot should exist");
    assert_eq!(
        oauth_snapshot
            .get("code")
            .and_then(serde_json::Value::as_str),
        Some("invalid")
    );
    assert_eq!(
        oauth_snapshot
            .get("source")
            .and_then(serde_json::Value::as_str),
        Some("oauth_refresh")
    );
    assert_eq!(
        oauth_snapshot
            .get("requires_reauth")
            .and_then(serde_json::Value::as_bool),
        Some(true)
    );

    token_handle.abort();
}

#[test]
fn gateway_refreshes_admin_provider_oauth_key_locally_with_trusted_admin_principal() {
    run_admin_oauth_test(
        "gateway_refreshes_admin_provider_oauth_key_locally_with_trusted_admin_principal",
        gateway_refreshes_admin_provider_oauth_key_locally_with_trusted_admin_principal_impl,
    );
}

async fn gateway_refreshes_admin_provider_oauth_key_locally_with_trusted_admin_principal_impl() {
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
    let token_server = Router::new().route(
        "/oauth/token",
        any(move |_request: Request| {
            let token_hits_inner = Arc::clone(&token_hits_clone);
            async move {
                *token_hits_inner.lock().expect("mutex should lock") += 1;
                Json(json!({
                    "access_token": "refreshed-codex-access-token",
                    "refresh_token": "refreshed-codex-refresh-token",
                    "token_type": "Bearer",
                    "expires_in": 1800,
                    "scope": "openid email profile offline_access",
                }))
            }
        }),
    );

    #[derive(Debug, Clone)]
    struct SeenExecutionRuntimeRequest {
        url: String,
        authorization: String,
    }

    let seen_execution_runtime = Arc::new(Mutex::new(None::<SeenExecutionRuntimeRequest>));
    let seen_execution_runtime_clone = Arc::clone(&seen_execution_runtime);
    let execution_runtime = Router::new().route(
        "/v1/execute/sync",
        any(move |request: Request| {
            let seen_execution_runtime_inner = Arc::clone(&seen_execution_runtime_clone);
            async move {
                let plan: aether_contracts::ExecutionPlan = serde_json::from_slice(
                    &to_bytes(request.into_body(), usize::MAX)
                        .await
                        .expect("body should read"),
                )
                .expect("plan should parse");
                *seen_execution_runtime_inner
                    .lock()
                    .expect("mutex should lock") = Some(SeenExecutionRuntimeRequest {
                    url: plan.url.clone(),
                    authorization: plan
                        .headers
                        .get("authorization")
                        .cloned()
                        .unwrap_or_default(),
                });
                let result = aether_contracts::ExecutionResult {
                    request_id: plan.request_id,
                    candidate_id: None,
                    status_code: 401,
                    headers: std::collections::BTreeMap::new(),
                    response_observation: None,
                    body: None,
                    telemetry: None,
                    error: None,
                };
                (StatusCode::OK, Json(result))
            }
        }),
    );

    let mut provider = sample_provider("provider-codex", "codex", 10);
    provider.provider_type = "codex".to_string();
    let endpoint = sample_endpoint(
        "endpoint-codex-cli",
        "provider-codex",
        "openai:responses",
        "https://chatgpt.com/backend-api/codex",
    );

    let mut key = sample_key(
        "key-codex-oauth-refresh",
        "provider-codex",
        "openai:responses",
        "stale-codex-access-token",
    );
    key.auth_type = "oauth".to_string();
    key.oauth_invalid_at_unix_secs = Some(1_700_000_000);
    key.oauth_invalid_reason = Some("[REFRESH_FAILED] stale token".to_string());
    key.status_snapshot = Some(json!({
        "oauth": {
            "code": "expired",
            "label": "已过期",
            "reason": "Access Token 已过期，等待自动续期",
            "expires_at": 1u64,
            "invalid_at": 1_700_000_000u64,
            "source": "expires_at",
            "requires_reauth": false,
            "expiring_soon": false
        },
        "account": {
            "code": "ok",
            "label": null,
            "reason": null,
            "blocked": false,
            "source": null,
            "recoverable": false
        },
        "quota": {
            "code": "unknown",
            "label": null,
            "reason": null,
            "exhausted": false,
            "usage_ratio": null,
            "updated_at": null,
            "reset_seconds": null,
            "plan_type": null
        }
    }));
    key.encrypted_auth_config = Some(
        encrypt_python_fernet_plaintext(
            DEV_FERNET_KEY,
            r#"{"provider_type":"codex","refresh_token":"old-codex-refresh-token","email":"alice@example.com","account_id":"acct-codex-123","plan_type":"plus","expires_at":1}"#,
        )
        .expect("auth config ciphertext should build"),
    );

    let provider_catalog_repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![provider],
        vec![endpoint],
        vec![key],
    ));

    let (_upstream_url, upstream_handle) = start_server(upstream).await;
    let (token_url, token_handle) = start_server(token_server).await;
    let (execution_runtime_url, execution_runtime_handle) = start_server(execution_runtime).await;
    let oauth_refresh =
        crate::provider_transport::LocalOAuthRefreshCoordinator::with_adapters_for_tests(vec![
            Arc::new(
                crate::provider_transport::oauth_refresh::GenericOAuthRefreshAdapter::default()
                    .with_token_url_for_tests("codex", format!("{token_url}/oauth/token")),
            ),
        ]);
    let gateway = build_router_with_state(
        build_state_with_execution_runtime_override(execution_runtime_url)
            .with_data_state_for_tests(
                GatewayDataState::with_provider_catalog_repository_for_tests(
                    provider_catalog_repository.clone(),
                )
                .with_encryption_key_for_tests(DEV_FERNET_KEY),
            )
            .with_oauth_refresh_coordinator_for_tests(oauth_refresh),
    );
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .post(format!(
            "{gateway_url}/api/admin/provider-oauth/keys/key-codex-oauth-refresh/refresh"
        ))
        .bearer_auth(crate::tests::test_admin_access_token())
        .header("x-client-device-id", crate::tests::TEST_ADMIN_DEVICE_ID)
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["provider_type"], "codex");
    assert_eq!(payload["has_refresh_token"], true);
    assert_eq!(payload["email"], "alice@example.com");
    let account_state_recheck_attempted = payload["account_state_recheck_attempted"]
        .as_bool()
        .expect("account_state_recheck_attempted should be bool");
    if account_state_recheck_attempted {
        let account_state_recheck_error = payload["account_state_recheck_error"]
            .as_str()
            .expect("account_state_recheck_error should be string when attempted");
        assert!(
            account_state_recheck_error == "wham/usage API 返回状态码 401"
                || account_state_recheck_error == "wham/usage API 返回状态码 403"
                || account_state_recheck_error.starts_with("wham/usage 请求执行失败:"),
            "unexpected account_state_recheck_error: {account_state_recheck_error}"
        );
    } else {
        assert_eq!(
            payload["account_state_recheck_error"],
            serde_json::Value::Null
        );
    }
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);
    assert_eq!(*token_hits.lock().expect("mutex should lock"), 1);
    let seen_execution_runtime_request = seen_execution_runtime
        .lock()
        .expect("mutex should lock")
        .clone()
        .expect("execution runtime request should be captured");
    assert_eq!(
        seen_execution_runtime_request.url,
        "https://chatgpt.com/backend-api/wham/usage"
    );
    assert_eq!(
        seen_execution_runtime_request.authorization,
        "Bearer refreshed-codex-access-token"
    );

    let stored_key = provider_catalog_repository
        .list_keys_by_ids(&["key-codex-oauth-refresh".to_string()])
        .await
        .expect("keys should list")
        .into_iter()
        .next()
        .expect("refreshed key should exist");
    let decrypted_api_key = decrypt_persisted_provider_api_key(&stored_key);
    assert_eq!(decrypted_api_key, "refreshed-codex-access-token");
    if account_state_recheck_attempted
        && payload["account_state_recheck_error"] == "wham/usage API 返回状态码 401"
    {
        assert!(stored_key.oauth_invalid_at_unix_secs.is_some());
        assert_eq!(
            stored_key.oauth_invalid_reason.as_deref(),
            Some("[OAUTH_EXPIRED] Codex Token 已过期 (401)")
        );
    } else if account_state_recheck_attempted
        && payload["account_state_recheck_error"] == "wham/usage API 返回状态码 403"
    {
        assert!(stored_key.oauth_invalid_at_unix_secs.is_some());
        assert!(stored_key
            .oauth_invalid_reason
            .as_deref()
            .is_some_and(|reason| reason.contains("(403)")));
    } else {
        assert_eq!(stored_key.oauth_invalid_at_unix_secs, None);
        assert_eq!(stored_key.oauth_invalid_reason, None);
    }

    let decrypted_auth_config = decrypt_persisted_provider_auth_config(&stored_key);
    let auth_config: serde_json::Value =
        serde_json::from_str(&decrypted_auth_config).expect("auth config should parse");
    assert_eq!(
        auth_config["refresh_token"],
        serde_json::Value::String("refreshed-codex-refresh-token".to_string())
    );
    assert_eq!(
        auth_config["email"],
        serde_json::Value::String("alice@example.com".to_string())
    );
    let status_snapshot = stored_key
        .status_snapshot
        .as_ref()
        .and_then(serde_json::Value::as_object)
        .expect("status snapshot should exist");
    let oauth_snapshot = status_snapshot
        .get("oauth")
        .and_then(serde_json::Value::as_object)
        .expect("oauth snapshot should exist");
    assert_eq!(
        oauth_snapshot.get("expires_at"),
        auth_config.get("expires_at")
    );
    if stored_key
        .oauth_invalid_reason
        .as_deref()
        .is_some_and(|reason| reason.starts_with("[OAUTH_EXPIRED]"))
    {
        assert_eq!(
            oauth_snapshot
                .get("code")
                .and_then(serde_json::Value::as_str),
            Some("expired")
        );
        assert_eq!(
            oauth_snapshot
                .get("label")
                .and_then(serde_json::Value::as_str),
            Some("已过期")
        );
        assert_eq!(
            oauth_snapshot.get("reason"),
            Some(&json!("OAuth token has expired"))
        );
        assert_eq!(
            oauth_snapshot
                .get("requires_reauth")
                .and_then(serde_json::Value::as_bool),
            Some(false)
        );
        assert_eq!(
            oauth_snapshot
                .get("expiring_soon")
                .and_then(serde_json::Value::as_bool),
            Some(false)
        );
    } else if stored_key.oauth_invalid_reason.is_some() {
        assert_eq!(
            oauth_snapshot
                .get("code")
                .and_then(serde_json::Value::as_str),
            Some("invalid")
        );
        assert_eq!(
            oauth_snapshot
                .get("label")
                .and_then(serde_json::Value::as_str),
            Some("已失效")
        );
        assert!(oauth_snapshot
            .get("reason")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|reason| !reason.trim().is_empty()));
        assert_eq!(
            oauth_snapshot
                .get("requires_reauth")
                .and_then(serde_json::Value::as_bool),
            Some(true)
        );
        assert_eq!(
            oauth_snapshot
                .get("expiring_soon")
                .and_then(serde_json::Value::as_bool),
            Some(false)
        );
    } else {
        assert_eq!(
            oauth_snapshot
                .get("code")
                .and_then(serde_json::Value::as_str),
            Some("expiring")
        );
        assert_eq!(
            oauth_snapshot
                .get("label")
                .and_then(serde_json::Value::as_str),
            Some("即将过期")
        );
        assert_eq!(oauth_snapshot.get("reason"), Some(&serde_json::Value::Null));
        assert_eq!(
            oauth_snapshot
                .get("requires_reauth")
                .and_then(serde_json::Value::as_bool),
            Some(false)
        );
        assert_eq!(
            oauth_snapshot
                .get("expiring_soon")
                .and_then(serde_json::Value::as_bool),
            Some(true)
        );
    }

    gateway_handle.abort();
    execution_runtime_handle.abort();
    token_handle.abort();
    upstream_handle.abort();
}

#[test]
fn gateway_manual_codex_oauth_refresh_reconciles_missing_fixed_endpoint() {
    run_admin_oauth_test(
        "gateway_manual_codex_oauth_refresh_reconciles_missing_fixed_endpoint",
        gateway_manual_codex_oauth_refresh_reconciles_missing_fixed_endpoint_impl,
    );
}

async fn gateway_manual_codex_oauth_refresh_reconciles_missing_fixed_endpoint_impl() {
    let token_hits = Arc::new(Mutex::new(0usize));
    let token_hits_clone = Arc::clone(&token_hits);
    let token_server = Router::new().route(
        "/oauth/token",
        post(move |_request: Request| {
            let token_hits_inner = Arc::clone(&token_hits_clone);
            async move {
                *token_hits_inner.lock().expect("mutex should lock") += 1;
                Json(json!({
                    "access_token": "refreshed-codex-access-token",
                    "refresh_token": "refreshed-codex-refresh-token",
                    "expires_in": 3600,
                    "token_type": "Bearer"
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
                            "plan_type": "plus",
                            "rate_limit": {
                                "primary_window": {
                                    "used_percent": 12.5,
                                    "window_minutes": 300
                                },
                                "secondary_window": {
                                    "used_percent": 55.0,
                                    "window_minutes": 10080
                                }
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

    let mut provider = sample_provider("provider-codex-missing-endpoint", "codex", 10);
    provider.provider_type = "codex".to_string();
    let mut key = sample_key(
        "key-codex-missing-endpoint-refresh",
        "provider-codex-missing-endpoint",
        "openai:responses",
        "stale-codex-access-token",
    );
    key.auth_type = "oauth".to_string();
    key.encrypted_auth_config = Some(
        encrypt_python_fernet_plaintext(
            DEV_FERNET_KEY,
            r#"{"provider_type":"codex","refresh_token":"old-codex-refresh-token","email":"alice@example.com","account_id":"acct-codex-123","plan_type":"plus","expires_at":1}"#,
        )
        .expect("auth config ciphertext should build"),
    );

    let provider_catalog_repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![provider],
        Vec::new(),
        vec![key],
    ));

    let (token_url, token_handle) = start_server(token_server).await;
    let (execution_runtime_url, execution_runtime_handle) = start_server(execution_runtime).await;
    let oauth_refresh =
        crate::provider_transport::LocalOAuthRefreshCoordinator::with_adapters_for_tests(vec![
            Arc::new(
                crate::provider_transport::oauth_refresh::GenericOAuthRefreshAdapter::default()
                    .with_token_url_for_tests("codex", format!("{token_url}/oauth/token")),
            ),
        ]);
    let gateway = build_router_with_state(
        build_state_with_execution_runtime_override(execution_runtime_url)
            .with_data_state_for_tests(
                GatewayDataState::with_provider_catalog_repository_for_tests(
                    provider_catalog_repository.clone(),
                )
                .with_encryption_key_for_tests(DEV_FERNET_KEY),
            )
            .with_oauth_refresh_coordinator_for_tests(oauth_refresh),
    );
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .post(format!(
            "{gateway_url}/api/admin/provider-oauth/keys/key-codex-missing-endpoint-refresh/refresh"
        ))
        .bearer_auth(crate::tests::test_admin_access_token())
        .header("x-client-device-id", crate::tests::TEST_ADMIN_DEVICE_ID)
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["provider_type"], "codex");
    assert_eq!(payload["has_refresh_token"], true);
    assert_eq!(payload["account_state_recheck_attempted"], true);
    assert_eq!(
        payload["account_state_recheck_error"],
        serde_json::Value::Null
    );
    assert_eq!(*token_hits.lock().expect("mutex should lock"), 1);

    let endpoints = provider_catalog_repository
        .list_endpoints_by_provider_ids(&["provider-codex-missing-endpoint".to_string()])
        .await
        .expect("endpoints should read");
    let responses_endpoint = endpoints
        .iter()
        .find(|endpoint| endpoint.api_format == "openai:responses")
        .expect("openai responses endpoint should be reconciled");
    assert!(endpoints
        .iter()
        .any(|endpoint| endpoint.api_format == "openai:search"));
    assert_eq!(
        responses_endpoint.base_url,
        "https://chatgpt.com/backend-api/codex"
    );
    assert_eq!(
        *seen_endpoint_id.lock().expect("mutex should lock"),
        Some(responses_endpoint.id.clone())
    );

    let stored_key = provider_catalog_repository
        .list_keys_by_ids(&["key-codex-missing-endpoint-refresh".to_string()])
        .await
        .expect("keys should load")
        .into_iter()
        .next()
        .expect("refreshed key should exist");
    let decrypted_api_key = decrypt_persisted_provider_api_key(&stored_key);
    assert_eq!(decrypted_api_key, "refreshed-codex-access-token");
    let decrypted_auth_config = decrypt_persisted_provider_auth_config(&stored_key);
    let auth_config: serde_json::Value =
        serde_json::from_str(&decrypted_auth_config).expect("auth config should parse");
    assert_eq!(auth_config["provider_type"], "codex");
    assert_eq!(
        auth_config["refresh_token"],
        "refreshed-codex-refresh-token"
    );

    gateway_handle.abort();
    execution_runtime_handle.abort();
    token_handle.abort();
}

#[test]
fn gateway_manual_kiro_oauth_refresh_reconciles_missing_fixed_endpoint() {
    run_admin_oauth_test(
        "gateway_manual_kiro_oauth_refresh_reconciles_missing_fixed_endpoint",
        gateway_manual_kiro_oauth_refresh_reconciles_missing_fixed_endpoint_impl,
    );
}

async fn gateway_manual_kiro_oauth_refresh_reconciles_missing_fixed_endpoint_impl() {
    run_gateway_manual_kiro_oauth_refresh_maintenance_endpoint_test(None, None, true).await;
}

#[test]
fn gateway_manual_kiro_oauth_refresh_uses_disabled_fixed_endpoint_for_maintenance() {
    run_admin_oauth_test(
        "gateway_manual_kiro_oauth_refresh_uses_disabled_fixed_endpoint_for_maintenance",
        gateway_manual_kiro_oauth_refresh_uses_disabled_fixed_endpoint_for_maintenance_impl,
    );
}

async fn gateway_manual_kiro_oauth_refresh_uses_disabled_fixed_endpoint_for_maintenance_impl() {
    let mut endpoint = sample_endpoint(
        "endpoint-kiro-disabled-maintenance",
        "provider-kiro-oauth-refresh",
        "claude:messages",
        "https://q.{region}.amazonaws.com",
    );
    endpoint.is_active = false;

    run_gateway_manual_kiro_oauth_refresh_maintenance_endpoint_test(
        Some(endpoint),
        Some("endpoint-kiro-disabled-maintenance"),
        false,
    )
    .await;
}

#[test]
fn gateway_marks_manual_oauth_refresh_failures_as_invalid_in_pool_payload() {
    run_admin_oauth_test(
        "gateway_marks_manual_oauth_refresh_failures_as_invalid_in_pool_payload",
        gateway_marks_manual_oauth_refresh_failures_as_invalid_in_pool_payload_impl,
    );
}

async fn gateway_marks_manual_oauth_refresh_failures_as_invalid_in_pool_payload_impl() {
    let token_hits = Arc::new(Mutex::new(0usize));
    let token_hits_clone = Arc::clone(&token_hits);
    let token_server = Router::new().route(
        "/oauth/token",
        post(move |_request: Request| {
            let token_hits_inner = Arc::clone(&token_hits_clone);
            async move {
                *token_hits_inner.lock().expect("mutex should lock") += 1;
                (
                    StatusCode::UNAUTHORIZED,
                    Json(json!({
                        "error": {
                            "message": "Your refresh token has already been used to generate a new access token. Please try signing in again.",
                            "type": "invalid_request_error",
                            "param": serde_json::Value::Null,
                            "code": "refresh_token_reused"
                        }
                    })),
                )
            }
        }),
    );

    let mut provider = sample_provider("provider-codex", "codex", 10).with_transport_fields(
        true,
        false,
        true,
        None,
        None,
        None,
        None,
        None,
        Some(json!({
            "pool_advanced": {
                "enabled": true
            }
        })),
    );
    provider.provider_type = "codex".to_string();
    let endpoint = sample_endpoint(
        "endpoint-codex-cli",
        "provider-codex",
        "openai:responses",
        "https://chatgpt.com/backend-api/codex",
    );

    let mut key = sample_key(
        "key-codex-oauth-refresh-invalid",
        "provider-codex",
        "openai:responses",
        "stale-codex-access-token",
    );
    key.auth_type = "oauth".to_string();
    key.expires_at_unix_secs = Some(4_102_444_800);
    key.status_snapshot = Some(json!({
        "oauth": {
            "code": "valid",
            "label": "有效",
            "reason": serde_json::Value::Null,
            "expires_at": 4_102_444_800u64,
            "invalid_at": serde_json::Value::Null,
            "source": "expires_at",
            "requires_reauth": false,
            "expiring_soon": false
        },
        "account": {
            "code": "ok",
            "label": serde_json::Value::Null,
            "reason": serde_json::Value::Null,
            "blocked": false,
            "source": serde_json::Value::Null,
            "recoverable": false
        },
        "quota": {
            "code": "unknown",
            "label": serde_json::Value::Null,
            "reason": serde_json::Value::Null,
            "exhausted": false,
            "usage_ratio": serde_json::Value::Null,
            "updated_at": serde_json::Value::Null,
            "reset_seconds": serde_json::Value::Null,
            "plan_type": serde_json::Value::Null
        }
    }));
    key.encrypted_auth_config = Some(
        encrypt_python_fernet_plaintext(
            DEV_FERNET_KEY,
            r#"{"provider_type":"codex","refresh_token":"used-refresh-token","email":"alice@example.com","account_id":"acct-codex-123","plan_type":"plus","expires_at":4102444800}"#,
        )
        .expect("auth config ciphertext should build"),
    );

    let provider_catalog_repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![provider],
        vec![endpoint],
        vec![key],
    ));

    let (token_url, token_handle) = start_server(token_server).await;
    let oauth_refresh =
        crate::provider_transport::LocalOAuthRefreshCoordinator::with_adapters_for_tests(vec![
            Arc::new(
                crate::provider_transport::oauth_refresh::GenericOAuthRefreshAdapter::default()
                    .with_token_url_for_tests("codex", format!("{token_url}/oauth/token")),
            ),
        ]);
    let gateway = build_router_with_state(
        AppState::new()
            .expect("gateway should build")
            .with_data_state_for_tests(
                GatewayDataState::with_provider_catalog_repository_for_tests(
                    provider_catalog_repository,
                )
                .with_encryption_key_for_tests(DEV_FERNET_KEY),
            )
            .with_oauth_refresh_coordinator_for_tests(oauth_refresh),
    );
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let client = reqwest::Client::new();
    let refresh_response = client
        .post(format!(
            "{gateway_url}/api/admin/provider-oauth/keys/key-codex-oauth-refresh-invalid/refresh"
        ))
        .bearer_auth(crate::tests::test_admin_access_token())
        .header("x-client-device-id", crate::tests::TEST_ADMIN_DEVICE_ID)
        .send()
        .await
        .expect("refresh request should succeed");

    assert_eq!(refresh_response.status(), StatusCode::BAD_REQUEST);
    let refresh_payload: serde_json::Value = refresh_response
        .json()
        .await
        .expect("refresh payload should parse");
    assert_eq!(
        refresh_payload["detail"],
        json!("Token 刷新失败：refresh_token 已被使用并轮换，请重新登录授权")
    );
    assert_eq!(*token_hits.lock().expect("mutex should lock"), 1);

    let pool_response = client
        .get(format!(
            "{gateway_url}/api/admin/pool/provider-codex/keys?page=1&page_size=50&status=all"
        ))
        .bearer_auth(crate::tests::test_admin_access_token())
        .header("x-client-device-id", crate::tests::TEST_ADMIN_DEVICE_ID)
        .send()
        .await
        .expect("pool request should succeed");

    assert_eq!(pool_response.status(), StatusCode::OK);
    let pool_payload: serde_json::Value = pool_response
        .json()
        .await
        .expect("pool payload should parse");
    let keys = pool_payload["keys"]
        .as_array()
        .expect("keys should be array");
    assert_eq!(keys.len(), 1);
    assert_eq!(
        keys[0]["oauth_invalid_reason"],
        json!("[REFRESH_FAILED] OAuth token refresh failed")
    );
    assert_eq!(
        keys[0]["status_snapshot"]["oauth"]["code"],
        json!("reauth_required")
    );
    assert_eq!(
        keys[0]["status_snapshot"]["oauth"]["reason"],
        json!("OAuth token refresh failed")
    );
    assert_eq!(
        keys[0]["status_snapshot"]["oauth"]["source"],
        json!("oauth_refresh")
    );
    assert_eq!(
        keys[0]["status_snapshot"]["oauth"]["requires_reauth"],
        json!(true)
    );
    assert_eq!(
        keys[0]["status_snapshot"]["oauth"]["usable_until_expiry"],
        json!(true)
    );

    gateway_handle.abort();
    token_handle.abort();
}

#[test]
fn gateway_auto_removes_manual_oauth_refresh_failure_after_access_token_expiry() {
    run_admin_oauth_test(
        "gateway_auto_removes_manual_oauth_refresh_failure_after_access_token_expiry",
        gateway_auto_removes_manual_oauth_refresh_failure_after_access_token_expiry_impl,
    );
}

async fn gateway_auto_removes_manual_oauth_refresh_failure_after_access_token_expiry_impl() {
    let token_hits = Arc::new(Mutex::new(0usize));
    let token_hits_clone = Arc::clone(&token_hits);
    let token_server = Router::new().route(
        "/oauth/token",
        post(move |_request: Request| {
            let token_hits_inner = Arc::clone(&token_hits_clone);
            async move {
                *token_hits_inner.lock().expect("mutex should lock") += 1;
                (
                    StatusCode::UNAUTHORIZED,
                    Json(json!({
                        "error": {
                            "message": "Could not validate your refresh token. Please try signing in again.",
                            "type": "invalid_request_error",
                            "param": serde_json::Value::Null,
                            "code": "refresh_token_expired"
                        }
                    })),
                )
            }
        }),
    );

    let mut provider = sample_provider("provider-codex", "codex", 10).with_transport_fields(
        true,
        false,
        true,
        None,
        None,
        None,
        None,
        None,
        Some(json!({
            "pool_advanced": {
                "enabled": true,
                "auto_remove_banned_keys": true
            }
        })),
    );
    provider.provider_type = "codex".to_string();
    let endpoint = sample_endpoint(
        "endpoint-codex-cli",
        "provider-codex",
        "openai:responses",
        "https://chatgpt.com/backend-api/codex",
    );
    let mut key = sample_key(
        "key-codex-oauth-refresh-expired",
        "provider-codex",
        "openai:responses",
        "expired-codex-access-token",
    );
    key.auth_type = "oauth".to_string();
    key.expires_at_unix_secs = Some(1);
    key.encrypted_auth_config = Some(
        encrypt_python_fernet_plaintext(
            DEV_FERNET_KEY,
            r#"{"provider_type":"codex","refresh_token":"expired-refresh-token","email":"alice@example.com","account_id":"acct-codex-123","plan_type":"plus","expires_at":1}"#,
        )
        .expect("auth config ciphertext should build"),
    );

    let provider_catalog_repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![provider],
        vec![endpoint],
        vec![key],
    ));
    let (token_url, token_handle) = start_server(token_server).await;
    let oauth_refresh =
        crate::provider_transport::LocalOAuthRefreshCoordinator::with_adapters_for_tests(vec![
            Arc::new(
                crate::provider_transport::oauth_refresh::GenericOAuthRefreshAdapter::default()
                    .with_token_url_for_tests("codex", format!("{token_url}/oauth/token")),
            ),
        ]);
    let gateway = build_router_with_state(
        AppState::new()
            .expect("gateway should build")
            .with_data_state_for_tests(
                GatewayDataState::with_provider_catalog_repository_for_tests(
                    provider_catalog_repository.clone(),
                )
                .with_encryption_key_for_tests(DEV_FERNET_KEY),
            )
            .with_oauth_refresh_coordinator_for_tests(oauth_refresh),
    );
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let refresh_response = reqwest::Client::new()
        .post(format!(
            "{gateway_url}/api/admin/provider-oauth/keys/key-codex-oauth-refresh-expired/refresh"
        ))
        .bearer_auth(crate::tests::test_admin_access_token())
        .header("x-client-device-id", crate::tests::TEST_ADMIN_DEVICE_ID)
        .send()
        .await
        .expect("refresh request should succeed");

    assert_eq!(refresh_response.status(), StatusCode::OK);
    let refresh_payload: serde_json::Value = refresh_response
        .json()
        .await
        .expect("refresh payload should parse");
    assert_eq!(refresh_payload["status"], json!("auto_removed"));
    assert_eq!(refresh_payload["message"], json!("已自动删除"));
    assert_eq!(*token_hits.lock().expect("mutex should lock"), 1);

    let keys = provider_catalog_repository
        .list_keys_by_ids(&["key-codex-oauth-refresh-expired".to_string()])
        .await
        .expect("keys should read");
    assert!(keys.is_empty());

    gateway_handle.abort();
    token_handle.abort();
}

#[test]
fn gateway_refreshes_admin_provider_oauth_key_locally_via_execution_runtime_provider_proxy_before_system_proxy(
) {
    run_admin_oauth_test(
        "gateway_refreshes_admin_provider_oauth_key_locally_via_execution_runtime_provider_proxy_before_system_proxy",
        gateway_refreshes_admin_provider_oauth_key_locally_via_execution_runtime_provider_proxy_before_system_proxy_impl,
    );
}

async fn gateway_refreshes_admin_provider_oauth_key_locally_via_execution_runtime_provider_proxy_before_system_proxy_impl(
) {
    let execution_plans = Arc::new(Mutex::new(Vec::<ExecutionPlan>::new()));
    let execution_plans_clone = Arc::clone(&execution_plans);
    let execution_runtime = Router::new().route(
        "/v1/execute/sync",
        any(move |Json(plan): Json<ExecutionPlan>| {
            let execution_plans_inner = Arc::clone(&execution_plans_clone);
            async move {
                execution_plans_inner
                    .lock()
                    .expect("mutex should lock")
                    .push(plan.clone());
                if plan.request_id == "provider-oauth:local-refresh-token" {
                    Json(json!({
                        "request_id": plan.request_id,
                        "status_code": 200,
                        "headers": {
                            "content-type": "application/json"
                        },
                        "body": {
                            "json_body": {
                                "access_token": "refreshed-codex-access-token",
                                "refresh_token": "refreshed-codex-refresh-token",
                                "token_type": "Bearer",
                                "expires_in": 1800,
                                "scope": "openid email profile offline_access",
                                "email": "alice@example.com",
                                "account_id": "acct-codex-123",
                                "plan_type": "plus"
                            }
                        }
                    }))
                } else {
                    Json(json!({
                        "request_id": plan.request_id,
                        "status_code": 200,
                        "headers": {
                            "content-type": "application/json"
                        },
                        "body": {
                            "json_body": {}
                        }
                    }))
                }
            }
        }),
    );

    let mut provider = sample_provider("provider-codex", "codex", 10);
    provider.provider_type = "codex".to_string();
    provider.proxy = Some(json!({"node_id":"proxy-node-provider","enabled":true}));
    let endpoint = sample_endpoint(
        "endpoint-codex-cli",
        "provider-codex",
        "openai:responses",
        "https://chatgpt.com/backend-api/codex",
    );

    let mut key = sample_key(
        "key-codex-oauth-refresh-provider",
        "provider-codex",
        "openai:responses",
        "stale-codex-access-token",
    );
    key.auth_type = "oauth".to_string();
    key.encrypted_auth_config = Some(
        encrypt_python_fernet_plaintext(
            DEV_FERNET_KEY,
            r#"{"provider_type":"codex","refresh_token":"old-codex-refresh-token","email":"alice@example.com","account_id":"acct-codex-123","plan_type":"plus","expires_at":1}"#,
        )
        .expect("auth config ciphertext should build"),
    );

    let provider_catalog_repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![provider],
        vec![endpoint],
        vec![key],
    ));
    let mut provider_node = sample_proxy_node("proxy-node-provider");
    provider_node.status = "online".to_string();
    provider_node.proxy_url = Some("http://proxy-provider.example:8080".to_string());
    let mut system_node = sample_proxy_node("proxy-node-system");
    system_node.status = "online".to_string();
    system_node.proxy_url = Some("http://proxy-system.example:8080".to_string());
    let proxy_node_repository = Arc::new(InMemoryProxyNodeRepository::seed(vec![
        provider_node,
        system_node,
    ]));

    let oauth_refresh =
        crate::provider_transport::LocalOAuthRefreshCoordinator::with_adapters_for_tests(vec![
            Arc::new(
                crate::provider_transport::oauth_refresh::GenericOAuthRefreshAdapter::default()
                    .with_token_url_for_tests("codex", "https://oauth.example/oauth/token"),
            ),
        ]);

    let (execution_runtime_url, execution_runtime_handle) = start_server(execution_runtime).await;
    let gateway = build_router_with_state(
        build_state_with_execution_runtime_override(execution_runtime_url)
            .with_data_state_for_tests(
                GatewayDataState::with_provider_catalog_repository_for_tests(
                    provider_catalog_repository.clone(),
                )
                .attach_proxy_node_repository_for_tests(proxy_node_repository)
                .with_system_config_values_for_tests(vec![(
                    "system_proxy_node_id".to_string(),
                    json!("proxy-node-system"),
                )])
                .with_encryption_key_for_tests(DEV_FERNET_KEY),
            )
            .with_oauth_refresh_coordinator_for_tests(oauth_refresh),
    );
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .post(format!(
            "{gateway_url}/api/admin/provider-oauth/keys/key-codex-oauth-refresh-provider/refresh"
        ))
        .bearer_auth(crate::tests::test_admin_access_token())
        .header("x-client-device-id", crate::tests::TEST_ADMIN_DEVICE_ID)
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);

    let plans = execution_plans.lock().expect("mutex should lock");
    let refresh_plan = plans
        .iter()
        .find(|plan| plan.request_id == "provider-oauth:local-refresh-token")
        .expect("local refresh plan should exist");
    assert_eq!(
        refresh_plan
            .proxy
            .as_ref()
            .and_then(|proxy| proxy.node_id.as_deref()),
        Some("proxy-node-provider")
    );
    assert_eq!(
        refresh_plan
            .headers
            .get(EXECUTION_REQUEST_FOLLOW_REDIRECTS_HEADER)
            .map(String::as_str),
        Some("true")
    );

    gateway_handle.abort();
    execution_runtime_handle.abort();
}

#[test]
fn gateway_refreshes_admin_provider_oauth_key_via_proxy_node_with_direct_refresh_controls() {
    run_admin_oauth_test(
        "gateway_refreshes_admin_provider_oauth_key_via_proxy_node_with_direct_refresh_controls",
        gateway_refreshes_admin_provider_oauth_key_via_proxy_node_with_direct_refresh_controls_impl,
    );
}

async fn gateway_refreshes_admin_provider_oauth_key_via_proxy_node_with_direct_refresh_controls_impl(
) {
    let execution_plans = Arc::new(Mutex::new(Vec::<ExecutionPlan>::new()));
    let execution_plans_clone = Arc::clone(&execution_plans);
    let execution_runtime = Router::new().route(
        "/v1/execute/sync",
        any(move |Json(plan): Json<ExecutionPlan>| {
            let execution_plans_inner = Arc::clone(&execution_plans_clone);
            async move {
                execution_plans_inner
                    .lock()
                    .expect("mutex should lock")
                    .push(plan.clone());
                if plan.request_id == "provider-oauth:local-refresh-token" {
                    Json(json!({
                        "request_id": plan.request_id,
                        "status_code": 200,
                        "headers": {
                            "content-type": "application/json"
                        },
                        "body": {
                            "json_body": {
                                "access_token": "refreshed-codex-access-token",
                                "refresh_token": "refreshed-codex-refresh-token",
                                "token_type": "Bearer",
                                "expires_in": 1800,
                                "scope": "openid email profile offline_access",
                                "email": "alice@example.com",
                                "account_id": "acct-codex-123",
                                "plan_type": "plus"
                            }
                        }
                    }))
                } else {
                    Json(json!({
                        "request_id": plan.request_id,
                        "status_code": 200,
                        "headers": {
                            "content-type": "application/json"
                        },
                        "body": {
                            "json_body": {}
                        }
                    }))
                }
            }
        }),
    );

    let mut provider = sample_provider("provider-codex", "codex", 10);
    provider.provider_type = "codex".to_string();
    provider.proxy = Some(json!({
        "node_id": "proxy-node-codex",
        "enabled": true,
    }));
    let endpoint = sample_endpoint(
        "endpoint-codex-cli",
        "provider-codex",
        "openai:responses",
        "https://chatgpt.com/backend-api/codex",
    );

    let mut key = sample_key(
        "key-codex-oauth-refresh-proxy-node",
        "provider-codex",
        "openai:responses",
        "stale-codex-access-token",
    );
    key.auth_type = "oauth".to_string();
    key.encrypted_auth_config = Some(
        encrypt_python_fernet_plaintext(
            DEV_FERNET_KEY,
            r#"{"provider_type":"codex","refresh_token":"old-codex-refresh-token","email":"alice@example.com","account_id":"acct-codex-123","plan_type":"plus","expires_at":1}"#,
        )
        .expect("auth config ciphertext should build"),
    );

    let provider_catalog_repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![provider],
        vec![endpoint],
        vec![key],
    ));
    let mut proxy_node = sample_proxy_node("proxy-node-codex");
    proxy_node.status = "online".to_string();
    let proxy_node_repository = Arc::new(InMemoryProxyNodeRepository::seed(vec![proxy_node]));

    let oauth_refresh =
        crate::provider_transport::LocalOAuthRefreshCoordinator::with_adapters_for_tests(vec![
            Arc::new(
                crate::provider_transport::oauth_refresh::GenericOAuthRefreshAdapter::default()
                    .with_token_url_for_tests("codex", "https://oauth.example/oauth/token"),
            ),
        ]);

    let (execution_runtime_url, execution_runtime_handle) = start_server(execution_runtime).await;
    let gateway = build_router_with_state(
        build_state_with_execution_runtime_override(execution_runtime_url)
            .with_data_state_for_tests(
                GatewayDataState::with_provider_catalog_repository_for_tests(
                    provider_catalog_repository,
                )
                .attach_proxy_node_repository_for_tests(proxy_node_repository)
                .with_encryption_key_for_tests(DEV_FERNET_KEY),
            )
            .with_oauth_refresh_coordinator_for_tests(oauth_refresh),
    );
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .post(format!(
            "{gateway_url}/api/admin/provider-oauth/keys/key-codex-oauth-refresh-proxy-node/refresh"
        ))
        .bearer_auth(crate::tests::test_admin_access_token())
        .header("x-client-device-id", crate::tests::TEST_ADMIN_DEVICE_ID)
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);

    let plans = execution_plans.lock().expect("mutex should lock");
    let refresh_plan = plans
        .iter()
        .find(|plan| plan.request_id == "provider-oauth:local-refresh-token")
        .expect("local refresh plan should exist");
    assert_eq!(
        refresh_plan
            .proxy
            .as_ref()
            .and_then(|proxy| proxy.mode.as_deref()),
        Some("http")
    );

    gateway_handle.abort();
    execution_runtime_handle.abort();
}

#[test]
fn gateway_consecutive_manual_oauth_refresh_uses_rotated_refresh_token() {
    run_admin_oauth_test(
        "gateway_consecutive_manual_oauth_refresh_uses_rotated_refresh_token",
        gateway_consecutive_manual_oauth_refresh_uses_rotated_refresh_token_impl,
    );
}

async fn gateway_consecutive_manual_oauth_refresh_uses_rotated_refresh_token_impl() {
    let refresh_request_bodies = Arc::new(Mutex::new(Vec::<String>::new()));
    let refresh_request_bodies_clone = Arc::clone(&refresh_request_bodies);
    let execution_runtime = Router::new().route(
        "/v1/execute/sync",
        any(move |Json(plan): Json<ExecutionPlan>| {
            let refresh_request_bodies_inner = Arc::clone(&refresh_request_bodies_clone);
            async move {
                if plan.request_id == "provider-oauth:local-refresh-token" {
                    use base64::Engine as _;

                    let body_text = plan
                        .body
                        .body_bytes_b64
                        .as_deref()
                        .and_then(|body| {
                            base64::engine::general_purpose::STANDARD.decode(body).ok()
                        })
                        .and_then(|body| String::from_utf8(body).ok())
                        .unwrap_or_default();
                    refresh_request_bodies_inner
                        .lock()
                        .expect("mutex should lock")
                        .push(body_text.clone());

                    let (access_token, refresh_token) =
                        if body_text.contains("refresh_token=old-codex-refresh-token") {
                            (
                                "refreshed-codex-access-token",
                                "rotated-codex-refresh-token",
                            )
                        } else if body_text.contains("refresh_token=rotated-codex-refresh-token") {
                            (
                                "refreshed-codex-access-token-2",
                                "rotated-codex-refresh-token-2",
                            )
                        } else {
                            return Json(json!({
                                "request_id": plan.request_id,
                                "status_code": 401,
                                "headers": {
                                    "content-type": "application/json"
                                },
                                "body": {
                                    "json_body": {
                                        "error": {
                                            "message": "unexpected refresh token"
                                        }
                                    }
                                }
                            }));
                        };

                    Json(json!({
                        "request_id": plan.request_id,
                        "status_code": 200,
                        "headers": {
                            "content-type": "application/json"
                        },
                        "body": {
                            "json_body": {
                                "access_token": access_token,
                                "refresh_token": refresh_token,
                                "token_type": "Bearer",
                                "expires_in": 1800,
                                "scope": "openid email profile offline_access",
                                "email": "alice@example.com",
                                "account_id": "acct-codex-123",
                                "plan_type": "plus"
                            }
                        }
                    }))
                } else {
                    Json(json!({
                        "request_id": plan.request_id,
                        "status_code": 200,
                        "headers": {
                            "content-type": "application/json"
                        },
                        "body": {
                            "json_body": {}
                        }
                    }))
                }
            }
        }),
    );

    let mut provider = sample_provider("provider-codex", "codex", 10);
    provider.provider_type = "codex".to_string();
    let endpoint = sample_endpoint(
        "endpoint-codex-cli",
        "provider-codex",
        "openai:responses",
        "https://chatgpt.com/backend-api/codex",
    );

    let mut key = sample_key(
        "key-codex-oauth-consecutive-refresh",
        "provider-codex",
        "openai:responses",
        "stale-codex-access-token",
    );
    key.auth_type = "oauth".to_string();
    key.encrypted_auth_config = Some(
        encrypt_python_fernet_plaintext(
            DEV_FERNET_KEY,
            r#"{"provider_type":"codex","refresh_token":"old-codex-refresh-token","email":"alice@example.com","account_id":"acct-codex-123","plan_type":"plus","expires_at":1}"#,
        )
        .expect("auth config ciphertext should build"),
    );

    let provider_catalog_repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![provider],
        vec![endpoint],
        vec![key],
    ));

    let oauth_refresh =
        crate::provider_transport::LocalOAuthRefreshCoordinator::with_adapters_for_tests(vec![
            Arc::new(
                crate::provider_transport::oauth_refresh::GenericOAuthRefreshAdapter::default()
                    .with_token_url_for_tests("codex", "https://oauth.example/oauth/token"),
            ),
        ]);

    let (execution_runtime_url, execution_runtime_handle) = start_server(execution_runtime).await;
    let gateway = build_router_with_state(
        build_state_with_execution_runtime_override(execution_runtime_url)
            .with_data_state_for_tests(
                GatewayDataState::with_provider_catalog_repository_for_tests(
                    provider_catalog_repository.clone(),
                )
                .with_encryption_key_for_tests(DEV_FERNET_KEY),
            )
            .with_oauth_refresh_coordinator_for_tests(oauth_refresh),
    );
    let (gateway_url, gateway_handle) = start_server(gateway).await;
    let client = reqwest::Client::new();

    for _ in 0..2 {
        let response = client
            .post(format!(
                "{gateway_url}/api/admin/provider-oauth/keys/key-codex-oauth-consecutive-refresh/refresh"
            ))
            .bearer_auth(crate::tests::test_admin_access_token())
            .header("x-client-device-id", crate::tests::TEST_ADMIN_DEVICE_ID)
            .send()
            .await
            .expect("request should succeed");
        assert_eq!(response.status(), StatusCode::OK);
    }

    let bodies = refresh_request_bodies
        .lock()
        .expect("mutex should lock")
        .clone();
    assert_eq!(bodies.len(), 2);
    assert!(
        bodies[0].contains("refresh_token=old-codex-refresh-token"),
        "unexpected first refresh body: {}",
        bodies[0]
    );
    assert!(
        bodies[1].contains("refresh_token=rotated-codex-refresh-token"),
        "unexpected second refresh body: {}",
        bodies[1]
    );

    let stored_key = provider_catalog_repository
        .list_keys_by_ids(&["key-codex-oauth-consecutive-refresh".to_string()])
        .await
        .expect("keys should list")
        .into_iter()
        .next()
        .expect("refreshed key should exist");
    let decrypted_auth_config = decrypt_persisted_provider_auth_config(&stored_key);
    let auth_config: serde_json::Value =
        serde_json::from_str(&decrypted_auth_config).expect("auth config should parse");
    assert_eq!(
        auth_config["refresh_token"],
        "rotated-codex-refresh-token-2"
    );

    gateway_handle.abort();
    execution_runtime_handle.abort();
}

#[test]
fn gateway_concurrent_manual_oauth_refresh_reuses_winner_after_lock_wait() {
    run_admin_oauth_test(
        "gateway_concurrent_manual_oauth_refresh_reuses_winner_after_lock_wait",
        gateway_concurrent_manual_oauth_refresh_reuses_winner_after_lock_wait_impl,
    );
}

async fn gateway_concurrent_manual_oauth_refresh_reuses_winner_after_lock_wait_impl() {
    let refresh_request_bodies = Arc::new(Mutex::new(Vec::<String>::new()));
    let refresh_request_bodies_clone = Arc::clone(&refresh_request_bodies);
    let execution_runtime = Router::new().route(
        "/v1/execute/sync",
        any(move |Json(plan): Json<ExecutionPlan>| {
            let refresh_request_bodies_inner = Arc::clone(&refresh_request_bodies_clone);
            async move {
                if plan.request_id == "provider-oauth:local-refresh-token" {
                    use base64::Engine as _;

                    let body_text = plan
                        .body
                        .body_bytes_b64
                        .as_deref()
                        .and_then(|body| {
                            base64::engine::general_purpose::STANDARD.decode(body).ok()
                        })
                        .and_then(|body| String::from_utf8(body).ok())
                        .unwrap_or_default();
                    refresh_request_bodies_inner
                        .lock()
                        .expect("mutex should lock")
                        .push(body_text.clone());

                    let (access_token, refresh_token, delay_ms) =
                        if body_text.contains("refresh_token=old-codex-refresh-token") {
                            (
                                "refreshed-codex-access-token",
                                "rotated-codex-refresh-token",
                                200u64,
                            )
                        } else if body_text.contains("refresh_token=rotated-codex-refresh-token") {
                            (
                                "refreshed-codex-access-token-2",
                                "rotated-codex-refresh-token-2",
                                0u64,
                            )
                        } else {
                            return Json(json!({
                                "request_id": plan.request_id,
                                "status_code": 401,
                                "headers": {
                                    "content-type": "application/json"
                                },
                                "body": {
                                    "json_body": {
                                        "error": {
                                            "message": "Could not validate your refresh token. Please try signing in again."
                                        }
                                    }
                                }
                            }));
                        };

                    if delay_ms > 0 {
                        tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
                    }

                    Json(json!({
                        "request_id": plan.request_id,
                        "status_code": 200,
                        "headers": {
                            "content-type": "application/json"
                        },
                        "body": {
                            "json_body": {
                                "access_token": access_token,
                                "refresh_token": refresh_token,
                                "token_type": "Bearer",
                                "expires_in": 1800,
                                "scope": "openid email profile offline_access",
                                "email": "alice@example.com",
                                "account_id": "acct-codex-123",
                                "plan_type": "plus"
                            }
                        }
                    }))
                } else {
                    Json(json!({
                        "request_id": plan.request_id,
                        "status_code": 200,
                        "headers": {
                            "content-type": "application/json"
                        },
                        "body": {
                            "json_body": {}
                        }
                    }))
                }
            }
        }),
    );

    let mut provider = sample_provider("provider-codex", "codex", 10);
    provider.provider_type = "codex".to_string();
    provider.proxy = Some(json!({"node_id":"proxy-node-provider","enabled":true}));
    let endpoint = sample_endpoint(
        "endpoint-codex-cli",
        "provider-codex",
        "openai:responses",
        "https://chatgpt.com/backend-api/codex",
    );

    let mut key = sample_key(
        "key-codex-oauth-concurrent-refresh",
        "provider-codex",
        "openai:responses",
        "stale-codex-access-token",
    );
    key.auth_type = "oauth".to_string();
    key.proxy = Some(json!({"node_id":"proxy-node-key","enabled":true}));
    key.encrypted_auth_config = Some(
        encrypt_python_fernet_plaintext(
            DEV_FERNET_KEY,
            r#"{"provider_type":"codex","refresh_token":"old-codex-refresh-token","email":"alice@example.com","account_id":"acct-codex-123","plan_type":"plus","expires_at":1}"#,
        )
        .expect("auth config ciphertext should build"),
    );

    let provider_catalog_repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![provider],
        vec![endpoint],
        vec![key],
    ));
    let mut key_node = sample_proxy_node("proxy-node-key");
    key_node.status = "online".to_string();
    key_node.proxy_url = Some("http://proxy-key.example:8080".to_string());
    let mut provider_node = sample_proxy_node("proxy-node-provider");
    provider_node.status = "online".to_string();
    provider_node.proxy_url = Some("http://proxy-provider.example:8080".to_string());
    let proxy_node_repository = Arc::new(InMemoryProxyNodeRepository::seed(vec![
        key_node,
        provider_node,
    ]));

    let oauth_refresh =
        crate::provider_transport::LocalOAuthRefreshCoordinator::with_adapters_for_tests(vec![
            Arc::new(
                crate::provider_transport::oauth_refresh::GenericOAuthRefreshAdapter::default()
                    .with_token_url_for_tests("codex", "https://oauth.example/oauth/token"),
            ),
        ]);

    let (execution_runtime_url, execution_runtime_handle) = start_server(execution_runtime).await;
    let gateway = build_router_with_state(
        build_state_with_execution_runtime_override(execution_runtime_url)
            .with_data_state_for_tests(
                GatewayDataState::with_provider_catalog_repository_for_tests(
                    provider_catalog_repository.clone(),
                )
                .attach_proxy_node_repository_for_tests(proxy_node_repository)
                .with_encryption_key_for_tests(DEV_FERNET_KEY),
            )
            .with_oauth_refresh_coordinator_for_tests(oauth_refresh),
    );
    let (gateway_url, gateway_handle) = start_server(gateway).await;
    let client = reqwest::Client::new();
    let refresh_url = format!(
        "{gateway_url}/api/admin/provider-oauth/keys/key-codex-oauth-concurrent-refresh/refresh"
    );

    let request_a = client
        .post(refresh_url.clone())
        .bearer_auth(crate::tests::test_admin_access_token())
        .header("x-client-device-id", crate::tests::TEST_ADMIN_DEVICE_ID)
        .send();
    let request_b = client
        .post(refresh_url)
        .bearer_auth(crate::tests::test_admin_access_token())
        .header("x-client-device-id", crate::tests::TEST_ADMIN_DEVICE_ID)
        .send();
    let (response_a, response_b) = tokio::join!(request_a, request_b);
    let response_a = response_a.expect("first request should succeed");
    let response_b = response_b.expect("second request should succeed");

    assert_eq!(response_a.status(), StatusCode::OK);
    assert_eq!(response_b.status(), StatusCode::OK);

    let bodies = refresh_request_bodies
        .lock()
        .expect("mutex should lock")
        .clone();
    assert_eq!(bodies.len(), 1);
    assert!(
        bodies[0].contains("refresh_token=old-codex-refresh-token"),
        "unexpected first refresh body: {}",
        bodies[0]
    );

    let stored_key = provider_catalog_repository
        .list_keys_by_ids(&["key-codex-oauth-concurrent-refresh".to_string()])
        .await
        .expect("keys should list")
        .into_iter()
        .next()
        .expect("refreshed key should exist");
    let decrypted_auth_config = decrypt_persisted_provider_auth_config(&stored_key);
    let auth_config: serde_json::Value =
        serde_json::from_str(&decrypted_auth_config).expect("auth config should parse");
    assert_eq!(auth_config["refresh_token"], "rotated-codex-refresh-token");

    gateway_handle.abort();
    execution_runtime_handle.abort();
}

#[test]
fn gateway_manual_oauth_refresh_prefers_fresher_transport_auth_config_over_stale_runtime_cache() {
    run_admin_oauth_test(
        "gateway_manual_oauth_refresh_prefers_fresher_transport_auth_config_over_stale_runtime_cache",
        gateway_manual_oauth_refresh_prefers_fresher_transport_auth_config_over_stale_runtime_cache_impl,
    );
}

async fn gateway_manual_oauth_refresh_prefers_fresher_transport_auth_config_over_stale_runtime_cache_impl(
) {
    let refresh_request_bodies = Arc::new(Mutex::new(Vec::<String>::new()));
    let refresh_request_bodies_clone = Arc::clone(&refresh_request_bodies);
    let execution_runtime = Router::new().route(
        "/v1/execute/sync",
        any(move |Json(plan): Json<ExecutionPlan>| {
            let refresh_request_bodies_inner = Arc::clone(&refresh_request_bodies_clone);
            async move {
                if plan.request_id == "provider-oauth:local-refresh-token" {
                    use base64::Engine as _;

                    let body_text = plan
                        .body
                        .body_bytes_b64
                        .as_deref()
                        .and_then(|body| {
                            base64::engine::general_purpose::STANDARD.decode(body).ok()
                        })
                        .and_then(|body| String::from_utf8(body).ok())
                        .unwrap_or_default();
                    refresh_request_bodies_inner
                        .lock()
                        .expect("mutex should lock")
                        .push(body_text.clone());

                    if body_text.contains("refresh_token=old-codex-refresh-token") {
                        return Json(json!({
                            "request_id": plan.request_id,
                            "status_code": 200,
                            "headers": {
                                "content-type": "application/json"
                            },
                            "body": {
                                "json_body": {
                                    "access_token": "cached-codex-access-token",
                                    "refresh_token": "cached-codex-refresh-token",
                                    "token_type": "Bearer",
                                    "expires_in": 1800,
                                    "scope": "openid email profile offline_access",
                                    "email": "alice@example.com",
                                    "account_id": "acct-codex-123",
                                    "plan_type": "plus"
                                }
                            }
                        }));
                    }

                    if body_text.contains("refresh_token=fresh-codex-refresh-token") {
                        return Json(json!({
                            "request_id": plan.request_id,
                            "status_code": 200,
                            "headers": {
                                "content-type": "application/json"
                            },
                            "body": {
                                "json_body": {
                                    "access_token": "fresh-codex-access-token-2",
                                    "refresh_token": "fresh-codex-refresh-token-2",
                                    "token_type": "Bearer",
                                    "expires_in": 1800,
                                    "scope": "openid email profile offline_access",
                                    "email": "alice@example.com",
                                    "account_id": "acct-codex-123",
                                    "plan_type": "plus"
                                }
                            }
                        }));
                    }

                    Json(json!({
                        "request_id": plan.request_id,
                        "status_code": 401,
                        "headers": {
                            "content-type": "application/json"
                        },
                        "body": {
                            "json_body": {
                                "error": {
                                    "message": "Could not validate your refresh token. Please try signing in again."
                                }
                            }
                        }
                    }))
                } else {
                    Json(json!({
                        "request_id": plan.request_id,
                        "status_code": 200,
                        "headers": {
                            "content-type": "application/json"
                        },
                        "body": {
                            "json_body": {}
                        }
                    }))
                }
            }
        }),
    );

    let mut provider = sample_provider("provider-codex", "codex", 10);
    provider.provider_type = "codex".to_string();
    let endpoint = sample_endpoint(
        "endpoint-codex-cli",
        "provider-codex",
        "openai:responses",
        "https://chatgpt.com/backend-api/codex",
    );

    let mut key = sample_key(
        "key-codex-oauth-stale-cache",
        "provider-codex",
        "openai:responses",
        "stale-codex-access-token",
    );
    key.auth_type = "oauth".to_string();
    key.encrypted_auth_config = Some(
        encrypt_python_fernet_plaintext(
            DEV_FERNET_KEY,
            r#"{"provider_type":"codex","refresh_token":"old-codex-refresh-token","email":"alice@example.com","account_id":"acct-codex-123","plan_type":"plus","expires_at":1,"updated_at":1700000001}"#,
        )
        .expect("auth config ciphertext should build"),
    );

    let provider_catalog_repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![provider],
        vec![endpoint],
        vec![key],
    ));

    let oauth_refresh =
        crate::provider_transport::LocalOAuthRefreshCoordinator::with_adapters_for_tests(vec![
            Arc::new(
                crate::provider_transport::oauth_refresh::GenericOAuthRefreshAdapter::default()
                    .with_token_url_for_tests("codex", "https://oauth.example/oauth/token"),
            ),
        ]);

    let (execution_runtime_url, execution_runtime_handle) = start_server(execution_runtime).await;
    let app_state = build_state_with_execution_runtime_override(execution_runtime_url)
        .with_data_state_for_tests(
            GatewayDataState::with_provider_catalog_repository_for_tests(
                provider_catalog_repository.clone(),
            )
            .with_encryption_key_for_tests(DEV_FERNET_KEY),
        )
        .with_oauth_refresh_coordinator_for_tests(oauth_refresh);

    let stale_transport = app_state
        .read_provider_transport_snapshot(
            "provider-codex",
            "endpoint-codex-cli",
            "key-codex-oauth-stale-cache",
        )
        .await
        .expect("transport should load")
        .expect("transport should exist");
    let cached_entry = app_state
        .force_local_oauth_refresh_entry(&stale_transport)
        .await
        .expect("initial refresh should succeed")
        .expect("initial refresh should return cached entry");
    assert_eq!(
        cached_entry.auth_header_value,
        "Bearer cached-codex-access-token"
    );

    let fresh_auth_config = app_state
        .seal_provider_catalog_key_auth_config(
            "provider-codex",
            "key-codex-oauth-stale-cache",
        r#"{"provider_type":"codex","refresh_token":"fresh-codex-refresh-token","email":"alice@example.com","account_id":"acct-codex-123","plan_type":"plus","expires_at":1,"updated_at":4102444810}"#,
        )
        .expect("updated auth config ciphertext should build");
    let remotely_updated_key = provider_catalog_repository
        .list_keys_by_ids(&["key-codex-oauth-stale-cache".to_string()])
        .await
        .expect("provider key should reload")
        .pop()
        .expect("provider key should exist");
    let expected_encrypted_api_key = remotely_updated_key.encrypted_api_key.clone();
    let expected_encrypted_auth_config = remotely_updated_key.encrypted_auth_config.clone();
    assert!(provider_catalog_repository
        .compare_and_swap_key_credentials(&ProviderCatalogKeyCredentialsCasUpdate {
            key_id: remotely_updated_key.id.clone(),
            expected_provider_id: remotely_updated_key.provider_id.clone(),
            expected_encrypted_api_key,
            expected_encrypted_auth_config,
            encrypted_api_key: remotely_updated_key.encrypted_api_key.clone(),
            encrypted_auth_config: Some(fresh_auth_config),
        })
        .await
        .expect("OAuth credential should update remotely"));

    let gateway = build_router_with_state(app_state);
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .post(format!(
            "{gateway_url}/api/admin/provider-oauth/keys/key-codex-oauth-stale-cache/refresh"
        ))
        .bearer_auth(crate::tests::test_admin_access_token())
        .header("x-client-device-id", crate::tests::TEST_ADMIN_DEVICE_ID)
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);

    let bodies = refresh_request_bodies
        .lock()
        .expect("mutex should lock")
        .clone();
    assert_eq!(bodies.len(), 2);
    assert!(
        bodies[0].contains("refresh_token=old-codex-refresh-token"),
        "unexpected first refresh body: {}",
        bodies[0]
    );
    assert!(
        bodies[1].contains("refresh_token=fresh-codex-refresh-token"),
        "unexpected second refresh body: {}",
        bodies[1]
    );

    gateway_handle.abort();
    execution_runtime_handle.abort();
}

#[test]
fn gateway_refreshes_admin_provider_oauth_key_locally_via_execution_runtime_key_proxy_before_system_proxy(
) {
    run_admin_oauth_test(
        "gateway_refreshes_admin_provider_oauth_key_locally_via_execution_runtime_key_proxy_before_system_proxy",
        gateway_refreshes_admin_provider_oauth_key_locally_via_execution_runtime_key_proxy_before_system_proxy_impl,
    );
}

async fn gateway_refreshes_admin_provider_oauth_key_locally_via_execution_runtime_key_proxy_before_system_proxy_impl(
) {
    let execution_plans = Arc::new(Mutex::new(Vec::<ExecutionPlan>::new()));
    let execution_plans_clone = Arc::clone(&execution_plans);
    let execution_runtime = Router::new().route(
        "/v1/execute/sync",
        any(move |Json(plan): Json<ExecutionPlan>| {
            let execution_plans_inner = Arc::clone(&execution_plans_clone);
            async move {
                execution_plans_inner
                    .lock()
                    .expect("mutex should lock")
                    .push(plan.clone());
                if plan.request_id == "provider-oauth:local-refresh-token" {
                    Json(json!({
                        "request_id": plan.request_id,
                        "status_code": 200,
                        "headers": {
                            "content-type": "application/json"
                        },
                        "body": {
                            "json_body": {
                                "access_token": "refreshed-codex-access-token",
                                "refresh_token": "refreshed-codex-refresh-token",
                                "token_type": "Bearer",
                                "expires_in": 1800,
                                "scope": "openid email profile offline_access",
                                "email": "alice@example.com",
                                "account_id": "acct-codex-123",
                                "plan_type": "plus"
                            }
                        }
                    }))
                } else {
                    Json(json!({
                        "request_id": plan.request_id,
                        "status_code": 200,
                        "headers": {
                            "content-type": "application/json"
                        },
                        "body": {
                            "json_body": {}
                        }
                    }))
                }
            }
        }),
    );

    let mut provider = sample_provider("provider-codex", "codex", 10);
    provider.provider_type = "codex".to_string();
    provider.proxy = Some(json!({"node_id":"proxy-node-provider","enabled":true}));
    let endpoint = sample_endpoint(
        "endpoint-codex-cli",
        "provider-codex",
        "openai:responses",
        "https://chatgpt.com/backend-api/codex",
    );

    let mut key = sample_key(
        "key-codex-oauth-refresh",
        "provider-codex",
        "openai:responses",
        "stale-codex-access-token",
    );
    key.auth_type = "oauth".to_string();
    key.proxy = Some(json!({"node_id":"proxy-node-key","enabled":true}));
    key.encrypted_auth_config = Some(
        encrypt_python_fernet_plaintext(
            DEV_FERNET_KEY,
            r#"{"provider_type":"codex","refresh_token":"old-codex-refresh-token","email":"alice@example.com","account_id":"acct-codex-123","plan_type":"plus","expires_at":1}"#,
        )
        .expect("auth config ciphertext should build"),
    );

    let provider_catalog_repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![provider],
        vec![endpoint],
        vec![key],
    ));
    let mut key_node = sample_proxy_node("proxy-node-key");
    key_node.status = "online".to_string();
    key_node.proxy_url = Some("http://proxy-key.example:8080".to_string());
    let mut provider_node = sample_proxy_node("proxy-node-provider");
    provider_node.status = "online".to_string();
    provider_node.proxy_url = Some("http://proxy-provider.example:8080".to_string());
    let mut system_node = sample_proxy_node("proxy-node-system");
    system_node.status = "online".to_string();
    system_node.proxy_url = Some("http://proxy-system.example:8080".to_string());
    let proxy_node_repository = Arc::new(InMemoryProxyNodeRepository::seed(vec![
        key_node,
        provider_node,
        system_node,
    ]));

    let oauth_refresh =
        crate::provider_transport::LocalOAuthRefreshCoordinator::with_adapters_for_tests(vec![
            Arc::new(
                crate::provider_transport::oauth_refresh::GenericOAuthRefreshAdapter::default()
                    .with_token_url_for_tests("codex", "https://oauth.example/oauth/token"),
            ),
        ]);

    let (execution_runtime_url, execution_runtime_handle) = start_server(execution_runtime).await;
    let gateway = build_router_with_state(
        build_state_with_execution_runtime_override(execution_runtime_url)
            .with_data_state_for_tests(
                GatewayDataState::with_provider_catalog_repository_for_tests(
                    provider_catalog_repository.clone(),
                )
                .attach_proxy_node_repository_for_tests(proxy_node_repository)
                .with_system_config_values_for_tests(vec![(
                    "system_proxy_node_id".to_string(),
                    json!("proxy-node-system"),
                )])
                .with_encryption_key_for_tests(DEV_FERNET_KEY),
            )
            .with_oauth_refresh_coordinator_for_tests(oauth_refresh),
    );
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .post(format!(
            "{gateway_url}/api/admin/provider-oauth/keys/key-codex-oauth-refresh/refresh"
        ))
        .bearer_auth(crate::tests::test_admin_access_token())
        .header("x-client-device-id", crate::tests::TEST_ADMIN_DEVICE_ID)
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);

    let plans = execution_plans.lock().expect("mutex should lock");
    let refresh_plan = plans
        .iter()
        .find(|plan| plan.request_id == "provider-oauth:local-refresh-token")
        .expect("local refresh plan should exist");
    assert_eq!(
        refresh_plan
            .proxy
            .as_ref()
            .and_then(|proxy| proxy.node_id.as_deref()),
        Some("proxy-node-key")
    );

    gateway_handle.abort();
    execution_runtime_handle.abort();
}
