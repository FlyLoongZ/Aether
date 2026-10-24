use super::*;

#[test]
fn gateway_handles_admin_provider_oauth_device_authorize_for_windsurf_browser() {
    run_admin_oauth_test(
        "gateway_handles_admin_provider_oauth_device_authorize_for_windsurf_browser",
        gateway_handles_admin_provider_oauth_device_authorize_for_windsurf_browser_impl,
    );
}

async fn gateway_handles_admin_provider_oauth_device_authorize_for_windsurf_browser_impl() {
    let mut provider = sample_provider("provider-windsurf", "windsurf", 10);
    provider.provider_type = "windsurf".to_string();
    let endpoint = sample_endpoint(
        "endpoint-windsurf-chat",
        "provider-windsurf",
        "openai:chat",
        "https://server.codeium.com",
    );
    let provider_catalog_repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![provider],
        vec![endpoint],
        vec![],
    ));
    let state = AppState::new()
        .expect("gateway should build")
        .with_data_state_for_tests(GatewayDataState::with_provider_catalog_reader_for_tests(
            provider_catalog_repository,
        ));

    let response = local_admin_provider_oauth_response(
        &state,
        http::Method::POST,
        "/api/admin/provider-oauth/providers/provider-windsurf/device-authorize",
        Some(json!({
            "auth_type": "browser",
            "login_option": "github",
            "proxy_node_id": "proxy-node-windsurf"
        })),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body should read");
    let payload: serde_json::Value = serde_json::from_slice(&body).expect("json body should parse");
    let session_id = payload["session_id"]
        .as_str()
        .expect("session_id should exist");
    assert_eq!(payload["auth_type"], "browser");
    assert_eq!(payload["login_option"], "github");
    assert_eq!(payload["redirect_uri"], "show-auth-token");
    assert_eq!(payload["callback_required"], true);

    let authorization_url = payload["verification_uri_complete"]
        .as_str()
        .expect("authorization url should exist");
    let parsed = url::Url::parse(authorization_url).expect("authorization url should parse");
    let params = parsed
        .query_pairs()
        .into_owned()
        .collect::<std::collections::BTreeMap<_, _>>();
    assert_eq!(
        parsed.as_str().split('?').next(),
        Some("https://windsurf.com/windsurf/signin")
    );
    assert_eq!(
        params.get("response_type").map(String::as_str),
        Some("token")
    );
    assert_eq!(params.get("state").map(String::as_str), Some(session_id));
    assert_eq!(
        params.get("redirect_uri").map(String::as_str),
        Some("show-auth-token")
    );
    assert_eq!(
        params.get("redirect_parameters_type").map(String::as_str),
        Some("query")
    );

    let stored = state
        .load_provider_oauth_device_session_for_tests(&format!("device_auth_session:{session_id}"))
        .expect("device session should be stored");
    let stored: serde_json::Value =
        serde_json::from_str(&stored).expect("device session json should parse");
    assert_eq!(stored["provider_id"], "provider-windsurf");
    assert_eq!(stored["auth_type"], "browser");
    assert_eq!(stored["social_provider"], "github");
    assert_eq!(stored["redirect_uri"], "show-auth-token");
    assert_eq!(stored["proxy_node_id"], "proxy-node-windsurf");
    assert_eq!(stored["status"], "pending");
}

#[test]
fn gateway_handles_admin_provider_oauth_device_authorize_for_xai() {
    run_admin_oauth_test(
        "gateway_handles_admin_provider_oauth_device_authorize_for_xai",
        gateway_handles_admin_provider_oauth_device_authorize_for_xai_impl,
    );
}

async fn gateway_handles_admin_provider_oauth_device_authorize_for_xai_impl() {
    let authorize_hits = Arc::new(Mutex::new(0usize));
    let authorize_hits_clone = Arc::clone(&authorize_hits);
    let oidc_server = Router::new().fallback(any(move |_request: Request| {
        let authorize_hits_inner = Arc::clone(&authorize_hits_clone);
        async move {
            *authorize_hits_inner.lock().expect("mutex should lock") += 1;
            Json(json!({
                "device_code": "xai-device-code",
                "user_code": "XAI-CODE",
                "verification_uri": "https://auth.x.ai/activate",
                "verification_uri_complete": "https://auth.x.ai/activate?user_code=XAI-CODE",
                "expires_in": 600,
                "interval": 5,
            }))
        }
    }));

    let mut provider = sample_provider("provider-xai", "xai", 10);
    provider.provider_type = "xai".to_string();
    let endpoint = sample_endpoint(
        "endpoint-xai-responses",
        "provider-xai",
        "openai:responses",
        "https://cli-chat-proxy.grok.com/v1",
    );
    let provider_catalog_repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![provider],
        vec![endpoint],
        vec![],
    ));
    let (oidc_url, oidc_handle) = start_server(oidc_server).await;
    let state = AppState::new()
        .expect("gateway should build")
        .with_data_state_for_tests(GatewayDataState::with_provider_catalog_reader_for_tests(
            provider_catalog_repository,
        ))
        .with_provider_oauth_token_url_for_tests(
            "xai_device",
            format!("{oidc_url}/oauth2/device/code"),
        )
        .with_provider_oauth_token_url_for_tests("xai", format!("{oidc_url}/oauth2/token"));

    let response = local_admin_provider_oauth_response(
        &state,
        http::Method::POST,
        "/api/admin/provider-oauth/providers/provider-xai/device-authorize",
        Some(json!({ "proxy_node_id": "proxy-node-xai" })),
    )
    .await;
    let status = response.status();
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body should read");
    let payload: serde_json::Value = serde_json::from_slice(&body).expect("json body should parse");
    assert_eq!(status, StatusCode::OK, "payload={payload}");
    let session_id = payload["session_id"]
        .as_str()
        .expect("session_id should exist")
        .to_string();
    assert_eq!(payload["user_code"], "XAI-CODE");
    assert_eq!(payload["verification_uri"], "https://auth.x.ai/activate");
    assert_eq!(
        payload["verification_uri_complete"],
        "https://auth.x.ai/activate?user_code=XAI-CODE"
    );
    assert_eq!(payload["auth_type"], "device");
    assert!(payload.get("callback_required").is_none() || payload["callback_required"] == false);
    assert_eq!(*authorize_hits.lock().expect("mutex should lock"), 1);

    let stored = state
        .load_provider_oauth_device_session_for_tests(&format!("device_auth_session:{session_id}"))
        .expect("device session should be stored");
    let stored: serde_json::Value =
        serde_json::from_str(&stored).expect("device session json should parse");
    assert_eq!(stored["provider_id"], "provider-xai");
    assert_eq!(stored["device_code"], "xai-device-code");
    assert_eq!(stored["auth_type"], "device");
    assert_eq!(stored["redirect_uri"], format!("{oidc_url}/oauth2/token"));
    assert_eq!(stored["proxy_node_id"], "proxy-node-xai");
    assert_eq!(stored["status"], "pending");

    oidc_handle.abort();
}

#[test]
fn gateway_handles_admin_provider_oauth_device_poll_for_xai() {
    run_admin_oauth_test(
        "gateway_handles_admin_provider_oauth_device_poll_for_xai",
        gateway_handles_admin_provider_oauth_device_poll_for_xai_impl,
    );
}

async fn gateway_handles_admin_provider_oauth_device_poll_for_xai_impl() {
    let token_hits = Arc::new(Mutex::new(0usize));
    let token_hits_clone = Arc::clone(&token_hits);
    let access_token = sample_kiro_device_access_token("user@x.ai");
    let id_token = access_token.clone();
    let token_server = Router::new().fallback(any(move |_request: Request| {
        let token_hits_inner = Arc::clone(&token_hits_clone);
        let access_token = access_token.clone();
        let id_token = id_token.clone();
        async move {
            let hit = {
                let mut hits = token_hits_inner.lock().expect("mutex should lock");
                *hits += 1;
                *hits
            };
            if hit == 1 {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(json!({ "error": "authorization_pending" })),
                )
                    .into_response();
            }
            Json(json!({
                "access_token": access_token,
                "refresh_token": "xai-refresh-token",
                "token_type": "Bearer",
                "expires_in": 3600,
                "id_token": id_token,
            }))
            .into_response()
        }
    }));

    let mut provider = sample_provider("provider-xai", "xai", 10);
    provider.provider_type = "xai".to_string();
    let endpoint = sample_endpoint(
        "endpoint-xai-responses",
        "provider-xai",
        "openai:responses",
        "https://cli-chat-proxy.grok.com/v1",
    );
    let provider_catalog_repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![provider],
        vec![endpoint],
        vec![],
    ));
    let (token_url, token_handle) = start_server(token_server).await;
    let resolved_token_url = format!("{token_url}/oauth2/token");
    let state = AppState::new()
        .expect("gateway should build")
        .with_data_state_for_tests(
            GatewayDataState::with_provider_catalog_repository_for_tests(
                provider_catalog_repository.clone(),
            )
            .with_encryption_key_for_tests(DEVELOPMENT_ENCRYPTION_KEY),
        )
        .with_provider_oauth_device_session_entry_for_tests(
            "session-xai",
            json!({
                "provider_id": "provider-xai",
                "region": "",
                "client_id": "b1a00492-073a-47ea-816f-4c329264a828",
                "client_secret": "",
                "device_code": "xai-device-code",
                "auth_type": "device",
                "social_provider": null,
                "code_verifier": null,
                "redirect_uri": resolved_token_url,
                "machine_id": null,
                "interval": 5,
                "expires_at_unix_secs": 4_102_444_800u64,
                "status": "pending",
                "proxy_node_id": null,
                "created_at_unix_ms": 1_711_000_000u64,
                "key_id": null,
                "email": null,
                "replaced": false,
                "error_msg": null,
            }),
        )
        .with_provider_oauth_token_url_for_tests("xai", resolved_token_url.clone());

    let pending = local_admin_provider_oauth_response(
        &state,
        http::Method::POST,
        "/api/admin/provider-oauth/providers/provider-xai/device-poll",
        Some(json!({ "session_id": "session-xai" })),
    )
    .await;
    let pending_body = to_bytes(pending.into_body(), usize::MAX)
        .await
        .expect("pending body should read");
    let pending_payload: serde_json::Value =
        serde_json::from_slice(&pending_body).expect("pending json should parse");
    assert_eq!(
        pending_payload["status"], "pending",
        "payload={pending_payload}"
    );

    let authorized = local_admin_provider_oauth_response(
        &state,
        http::Method::POST,
        "/api/admin/provider-oauth/providers/provider-xai/device-poll",
        Some(json!({ "session_id": "session-xai" })),
    )
    .await;
    let status = authorized.status();
    let body = to_bytes(authorized.into_body(), usize::MAX)
        .await
        .expect("authorized body should read");
    let payload: serde_json::Value = serde_json::from_slice(&body).expect("json body should parse");
    assert_eq!(status, StatusCode::OK, "payload={payload}");
    assert_eq!(payload["status"], "authorized");
    assert_eq!(payload["email"], "user@x.ai");
    assert_eq!(payload["replaced"], false);
    assert_eq!(*token_hits.lock().expect("mutex should lock"), 2);

    let stored = state
        .load_provider_oauth_device_session_for_tests("device_auth_session:session-xai")
        .expect("device session should persist");
    let stored: serde_json::Value =
        serde_json::from_str(&stored).expect("device session json should parse");
    assert_eq!(stored["status"], "authorized");
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
    let decrypted_auth_config = decrypt_persisted_provider_auth_config(&persisted);
    let auth_config: serde_json::Value =
        serde_json::from_str(&decrypted_auth_config).expect("auth config should parse");
    assert_eq!(auth_config["provider_type"], "xai");
    assert_eq!(auth_config["auth_method"], "oauth");
    assert_eq!(auth_config["using_api"], false);
    assert_eq!(auth_config["email"], "user@x.ai");

    token_handle.abort();
}

#[test]
fn gateway_handles_admin_provider_oauth_device_poll_for_windsurf_one_time_token() {
    run_admin_oauth_test(
        "gateway_handles_admin_provider_oauth_device_poll_for_windsurf_one_time_token",
        gateway_handles_admin_provider_oauth_device_poll_for_windsurf_one_time_token_impl,
    );
}

async fn gateway_handles_admin_provider_oauth_device_poll_for_windsurf_one_time_token_impl() {
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
                if plan.request_id == "provider-oauth:windsurf-register:new" {
                    return Json(windsurf_register_user_execution_result(&plan.request_id));
                }
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
        }),
    );

    let mut provider = sample_provider("provider-windsurf", "windsurf", 10);
    provider.provider_type = "windsurf".to_string();
    let endpoint = sample_endpoint(
        "endpoint-windsurf-chat",
        "provider-windsurf",
        "openai:chat",
        "https://server.codeium.com",
    );
    let provider_catalog_repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![provider],
        vec![endpoint],
        vec![],
    ));
    let mut proxy_node = sample_proxy_node("proxy-node-windsurf");
    proxy_node.status = "online".to_string();
    proxy_node.proxy_url = Some("http://proxy.example:8080".to_string());
    let proxy_node_repository = Arc::new(InMemoryProxyNodeRepository::seed(vec![proxy_node]));

    let (execution_runtime_url, execution_runtime_handle) = start_server(execution_runtime).await;
    let state = build_state_with_execution_runtime_override(execution_runtime_url)
        .with_data_state_for_tests(
            GatewayDataState::with_provider_catalog_repository_for_tests(
                provider_catalog_repository.clone(),
            )
            .with_encryption_key_for_tests(DEVELOPMENT_ENCRYPTION_KEY)
            .attach_proxy_node_repository_for_tests(proxy_node_repository),
        )
        .with_provider_oauth_device_session_entry_for_tests(
            "session-windsurf",
            json!({
                "provider_id": "provider-windsurf",
                "region": "",
                "client_id": "",
                "client_secret": "",
                "device_code": "",
                "auth_type": "browser",
                "social_provider": "google",
                "code_verifier": null,
                "redirect_uri": "show-auth-token",
                "machine_id": "123e4567-e89b-12d3-a456-426614174000",
                "interval": 5,
                "expires_at_unix_secs": 4_102_444_800u64,
                "status": "pending",
                "proxy_node_id": "proxy-node-windsurf",
                "created_at_unix_ms": 1_711_000_000u64,
                "key_id": null,
                "email": null,
                "replaced": false,
                "error_msg": null,
            }),
        );

    let response = local_admin_provider_oauth_response(
        &state,
        http::Method::POST,
        "/api/admin/provider-oauth/providers/provider-windsurf/device-poll",
        Some(json!({
            "session_id": "session-windsurf",
            "token": "ott$browser-token"
        })),
    )
    .await;
    let status = response.status();
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body should read");
    let payload: serde_json::Value = serde_json::from_slice(&body).expect("json body should parse");
    assert_eq!(status, StatusCode::OK, "payload={payload}");
    assert_eq!(payload["status"], "authorized");
    assert_eq!(payload["replaced"], false);

    let stored = state
        .load_provider_oauth_device_session_for_tests("device_auth_session:session-windsurf")
        .expect("device session should persist");
    let stored: serde_json::Value =
        serde_json::from_str(&stored).expect("device session json should parse");
    assert_eq!(stored["status"], "authorized");
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
        Some(json!({"node_id": "proxy-node-windsurf", "enabled": true}))
    );
    let decrypted_api_key = decrypt_persisted_provider_api_key(&persisted);
    assert_eq!(decrypted_api_key, "devin-session-token$registered");
    let decrypted_auth_config = decrypt_persisted_provider_auth_config(&persisted);
    let auth_config: serde_json::Value =
        serde_json::from_str(&decrypted_auth_config).expect("auth config should parse");
    assert_eq!(auth_config["provider_type"], "windsurf");
    assert_eq!(auth_config["auth_method"], "browser");
    assert_eq!(auth_config["register_source"], "new");
    assert_eq!(auth_config["social_provider"], "google");

    {
        let plans = execution_plans.lock().expect("mutex should lock");
        let register_plan = plans
            .iter()
            .find(|plan| plan.request_id == "provider-oauth:windsurf-register:new")
            .expect("register plan should execute");
        assert_eq!(register_plan.method, "POST");
        assert_eq!(
            register_plan.content_type.as_deref(),
            Some("application/proto")
        );
        assert!(register_plan.body.json_body.is_none());
        let encoded_body = register_plan
            .body
            .body_bytes_b64
            .as_deref()
            .expect("register body should be bytes");
        use base64::Engine as _;
        let body_bytes = base64::engine::general_purpose::STANDARD
            .decode(encoded_body)
            .expect("register body should decode");
        let mut expected_body = vec![0x0a, "ott$browser-token".len() as u8];
        expected_body.extend_from_slice(b"ott$browser-token");
        assert_eq!(body_bytes, expected_body);
        assert_eq!(
            register_plan
                .proxy
                .as_ref()
                .and_then(|proxy| proxy.node_id.as_deref()),
            Some("proxy-node-windsurf")
        );
    }

    execution_runtime_handle.abort();
}

#[test]
fn gateway_rejects_windsurf_callback_state_mismatch_and_missing_token() {
    run_admin_oauth_test(
        "gateway_rejects_windsurf_callback_state_mismatch_and_missing_token",
        gateway_rejects_windsurf_callback_state_mismatch_and_missing_token_impl,
    );
}

async fn gateway_rejects_windsurf_callback_state_mismatch_and_missing_token_impl() {
    let mut provider = sample_provider("provider-windsurf", "windsurf", 10);
    provider.provider_type = "windsurf".to_string();
    let endpoint = sample_endpoint(
        "endpoint-windsurf-chat",
        "provider-windsurf",
        "openai:chat",
        "https://server.codeium.com",
    );
    let provider_catalog_repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![provider],
        vec![endpoint],
        vec![],
    ));
    let state = AppState::new()
        .expect("gateway should build")
        .with_data_state_for_tests(GatewayDataState::with_provider_catalog_reader_for_tests(
            provider_catalog_repository,
        ))
        .with_provider_oauth_device_session_entry_for_tests(
            "session-windsurf",
            json!({
                "provider_id": "provider-windsurf",
                "region": "",
                "client_id": "",
                "client_secret": "",
                "device_code": "",
                "auth_type": "browser",
                "social_provider": "google",
                "code_verifier": null,
                "redirect_uri": "show-auth-token",
                "machine_id": "123e4567-e89b-12d3-a456-426614174000",
                "interval": 5,
                "expires_at_unix_secs": 4_102_444_800u64,
                "status": "pending",
                "proxy_node_id": null,
                "created_at_unix_ms": 1_711_000_000u64,
                "key_id": null,
                "email": null,
                "replaced": false,
                "error_msg": null,
            }),
        );

    let response = local_admin_provider_oauth_response(
        &state,
        http::Method::POST,
        "/api/admin/provider-oauth/providers/provider-windsurf/device-poll",
        Some(json!({
            "session_id": "session-windsurf",
            "callback_url": "https://windsurf.com/show-auth-token?token=ott$wrong-state&state=wrong-state"
        })),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body should read");
    let payload: serde_json::Value = serde_json::from_slice(&body).expect("json body should parse");
    assert_eq!(payload["status"], "error");
    assert!(payload["error"]
        .as_str()
        .is_some_and(|error| error.contains("state")));

    let response = local_admin_provider_oauth_response(
        &state,
        http::Method::POST,
        "/api/admin/provider-oauth/providers/provider-windsurf/device-poll",
        Some(json!({
            "session_id": "session-windsurf",
            "callback_url": "https://windsurf.com/show-auth-token?state=session-windsurf"
        })),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body should read");
    let payload: serde_json::Value = serde_json::from_slice(&body).expect("json body should parse");
    assert_eq!(payload["status"], "error");
    assert!(payload["error"]
        .as_str()
        .is_some_and(|error| error.contains("token")));
}

#[test]
fn gateway_handles_admin_provider_oauth_device_authorize_locally_with_trusted_admin_principal() {
    run_admin_oauth_test(
        "gateway_handles_admin_provider_oauth_device_authorize_locally_with_trusted_admin_principal",
        gateway_handles_admin_provider_oauth_device_authorize_locally_with_trusted_admin_principal_impl,
    );
}

async fn gateway_handles_admin_provider_oauth_device_authorize_locally_with_trusted_admin_principal_impl(
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

    let register_hits = Arc::new(Mutex::new(0usize));
    let register_hits_clone = Arc::clone(&register_hits);
    let authorize_hits = Arc::new(Mutex::new(0usize));
    let authorize_hits_clone = Arc::clone(&authorize_hits);
    let oidc_server = Router::new()
        .route(
            "/client/register",
            post(move |_request: Request| {
                let register_hits_inner = Arc::clone(&register_hits_clone);
                async move {
                    *register_hits_inner.lock().expect("mutex should lock") += 1;
                    Json(json!({
                        "clientId": "kiro-device-client",
                        "clientSecret": "kiro-device-secret",
                    }))
                }
            }),
        )
        .route(
            "/device_authorization",
            post(move |_request: Request| {
                let authorize_hits_inner = Arc::clone(&authorize_hits_clone);
                async move {
                    *authorize_hits_inner.lock().expect("mutex should lock") += 1;
                    Json(json!({
                        "deviceCode": "device-code-123",
                        "userCode": "USER-CODE",
                        "verificationUri": "https://device.example.com/verify",
                        "verificationUriComplete": "https://device.example.com/verify?user_code=USER-CODE",
                        "expiresIn": 600,
                        "interval": 5,
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
    let (oidc_url, oidc_handle) = start_server(oidc_server).await;
    let state = AppState::new()
        .expect("gateway should build")
        .with_data_state_for_tests(GatewayDataState::with_provider_catalog_reader_for_tests(
            provider_catalog_repository,
        ))
        .with_provider_oauth_device_session_entry_for_tests(
            "seed-session",
            json!({"status":"seed"}),
        )
        .with_provider_oauth_token_url_for_tests(
            "kiro_device_register",
            format!("{oidc_url}/client/register"),
        )
        .with_provider_oauth_token_url_for_tests(
            "kiro_device_authorize",
            format!("{oidc_url}/device_authorization"),
        );
    let gateway = build_router_with_state(state.clone());
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .post(format!(
            "{gateway_url}/api/admin/provider-oauth/providers/provider-kiro/device-authorize"
        ))
        .header(crate::constants::GATEWAY_HEADER, "rust-phase3b")
        .header(TRUSTED_ADMIN_USER_ID_HEADER, "admin-user-123")
        .header(TRUSTED_ADMIN_USER_ROLE_HEADER, "admin")
        .header(TRUSTED_ADMIN_SESSION_ID_HEADER, "session-123")
        .json(&json!({
            "start_url": "https://view.awsapps.com/start",
            "region": "us-east-1",
            "proxy_node_id": "proxy-node-kiro",
        }))
        .send()
        .await
        .expect("request should succeed");

    let status = response.status();
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(status, StatusCode::OK, "payload={payload}");
    let session_id = payload["session_id"]
        .as_str()
        .expect("session_id should exist")
        .to_string();
    assert_eq!(payload["user_code"], "USER-CODE");
    assert_eq!(
        payload["verification_uri_complete"],
        "https://device.example.com/verify?user_code=USER-CODE"
    );
    assert_eq!(payload["expires_in"], 600);
    assert_eq!(payload["interval"], 5);
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);
    assert_eq!(*register_hits.lock().expect("mutex should lock"), 1);
    assert_eq!(*authorize_hits.lock().expect("mutex should lock"), 1);

    let stored = state
        .load_provider_oauth_device_session_for_tests(&format!("device_auth_session:{session_id}"))
        .expect("device session should be stored");
    let stored: serde_json::Value =
        serde_json::from_str(&stored).expect("device session json should parse");
    assert_eq!(stored["provider_id"], "provider-kiro");
    assert_eq!(stored["region"], "us-east-1");
    assert_eq!(stored["client_id"], "kiro-device-client");
    assert_eq!(stored["client_secret"], "kiro-device-secret");
    assert_eq!(stored["device_code"], "device-code-123");
    assert_eq!(stored["proxy_node_id"], "proxy-node-kiro");
    assert_eq!(stored["status"], "pending");

    gateway_handle.abort();
    oidc_handle.abort();
    upstream_handle.abort();
}

#[test]
fn gateway_handles_admin_provider_oauth_device_authorize_for_kiro_google_social() {
    run_admin_oauth_test(
        "gateway_handles_admin_provider_oauth_device_authorize_for_kiro_google_social",
        gateway_handles_admin_provider_oauth_device_authorize_for_kiro_google_social_impl,
    );
}

async fn gateway_handles_admin_provider_oauth_device_authorize_for_kiro_google_social_impl() {
    let mut provider = sample_provider("provider-kiro", "kiro", 10);
    provider.provider_type = "kiro".to_string();
    let provider_catalog_repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![provider],
        vec![],
        vec![],
    ));
    let state = AppState::new()
        .expect("gateway should build")
        .with_data_state_for_tests(GatewayDataState::with_provider_catalog_reader_for_tests(
            provider_catalog_repository,
        ))
        .with_provider_oauth_token_url_for_tests(
            "kiro_social_portal",
            "https://portal.example.com/signin",
        );

    let response = local_admin_provider_oauth_response(
        &state,
        http::Method::POST,
        "/api/admin/provider-oauth/providers/provider-kiro/device-authorize",
        Some(json!({
            "auth_type": "google"
        })),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body should read");
    let payload: serde_json::Value = serde_json::from_slice(&body).expect("json body should parse");
    let session_id = payload["session_id"]
        .as_str()
        .expect("session_id should exist");
    assert_eq!(payload["auth_type"], "google");
    assert_eq!(payload["callback_required"], true);
    assert_eq!(payload["redirect_uri"], "http://localhost:49153");

    let authorization_url = payload["verification_uri_complete"]
        .as_str()
        .expect("authorization url should exist");
    let parsed = url::Url::parse(authorization_url).expect("authorization url should parse");
    let params = parsed
        .query_pairs()
        .into_owned()
        .collect::<std::collections::BTreeMap<_, _>>();
    assert_eq!(
        parsed.as_str().split('?').next(),
        Some("https://portal.example.com/signin")
    );
    assert_eq!(
        params.get("redirect_uri").map(String::as_str),
        Some("http://localhost:49153")
    );
    assert_eq!(params.get("state").map(String::as_str), Some(session_id));
    assert_eq!(
        params.get("code_challenge_method").map(String::as_str),
        Some("S256")
    );
    assert_eq!(
        params.get("redirect_from").map(String::as_str),
        Some("KiroIDE")
    );
    assert_eq!(
        params.get("login_option").map(String::as_str),
        Some("google")
    );
    assert!(params
        .get("code_challenge")
        .is_some_and(|value| !value.is_empty()));

    let stored = state
        .load_provider_oauth_device_session_for_tests(&format!("device_auth_session:{session_id}"))
        .expect("device session should be stored");
    let stored: serde_json::Value =
        serde_json::from_str(&stored).expect("device session json should parse");
    assert_eq!(stored["provider_id"], "provider-kiro");
    assert_eq!(stored["auth_type"], "social");
    assert_eq!(stored["social_provider"], "Google");
    assert_eq!(stored["redirect_uri"], "http://localhost:49153");
    assert!(stored["code_verifier"]
        .as_str()
        .is_some_and(|value| !value.is_empty()));
    assert!(stored["machine_id"]
        .as_str()
        .is_some_and(|value| !value.is_empty()));
    assert_eq!(stored["status"], "pending");
}

#[test]
fn gateway_handles_admin_provider_oauth_device_poll_locally_with_trusted_admin_principal() {
    run_admin_oauth_test(
        "gateway_handles_admin_provider_oauth_device_poll_locally_with_trusted_admin_principal",
        gateway_handles_admin_provider_oauth_device_poll_locally_with_trusted_admin_principal_impl,
    );
}

async fn gateway_handles_admin_provider_oauth_device_poll_locally_with_trusted_admin_principal_impl(
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
    let access_token = sample_kiro_device_access_token("kiro@example.com");
    let expected_access_token = access_token.clone();
    let token_server = Router::new().route(
        "/token",
        post(move |_request: Request| {
            let token_hits_inner = Arc::clone(&token_hits_clone);
            let access_token_inner = access_token.clone();
            async move {
                *token_hits_inner.lock().expect("mutex should lock") += 1;
                Json(json!({
                    "accessToken": access_token_inner,
                    "refreshToken": "kiro-device-refresh-token",
                    "expiresIn": 1800,
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
    let state = AppState::new()
        .expect("gateway should build")
        .with_data_state_for_tests(
            GatewayDataState::with_provider_catalog_repository_for_tests(
                provider_catalog_repository.clone(),
            )
            .with_encryption_key_for_tests(DEVELOPMENT_ENCRYPTION_KEY),
        )
        .with_provider_oauth_device_session_entry_for_tests(
            "session-123",
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
        .with_provider_oauth_token_url_for_tests("kiro_idc_refresh", token_url.to_string());
    let gateway = build_router_with_state(state.clone());
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .post(format!(
            "{gateway_url}/api/admin/provider-oauth/providers/provider-kiro/device-poll"
        ))
        .header(crate::constants::GATEWAY_HEADER, "rust-phase3b")
        .header(TRUSTED_ADMIN_USER_ID_HEADER, "admin-user-123")
        .header(TRUSTED_ADMIN_USER_ROLE_HEADER, "admin")
        .header(TRUSTED_ADMIN_SESSION_ID_HEADER, "session-123")
        .header(
            TRUSTED_ADMIN_MANAGEMENT_TOKEN_ID_HEADER,
            "management-token-123",
        )
        .json(&json!({
            "session_id": "session-123"
        }))
        .send()
        .await
        .expect("request should succeed");

    let status = response.status();
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(status, StatusCode::OK, "payload={payload}");
    assert_eq!(payload["status"], "authorized");
    assert_eq!(payload["email"], "kiro@example.com");
    assert_eq!(payload["replaced"], false);
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);
    assert_eq!(*token_hits.lock().expect("mutex should lock"), 2);

    let stored = state
        .load_provider_oauth_device_session_for_tests("device_auth_session:session-123")
        .expect("device session should persist");
    let stored: serde_json::Value =
        serde_json::from_str(&stored).expect("device session json should parse");
    assert_eq!(stored["status"], "authorized");
    assert_eq!(stored["email"], "kiro@example.com");
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
    assert_eq!(decrypted_api_key, expected_access_token);
    let decrypted_auth_config = decrypt_persisted_provider_auth_config(&persisted);
    let auth_config: serde_json::Value =
        serde_json::from_str(&decrypted_auth_config).expect("auth config should parse");
    assert_eq!(auth_config["provider_type"], "kiro");
    assert_eq!(auth_config["auth_method"], "idc");
    assert_eq!(auth_config["refresh_token"], "kiro-device-refresh-token");
    assert_eq!(auth_config["email"], "kiro@example.com");
    assert_eq!(auth_config["client_id"], "kiro-device-client");
    assert_eq!(auth_config["client_secret"], "kiro-device-secret");
    assert_eq!(auth_config["region"], "us-east-1");

    gateway_handle.abort();
    token_handle.abort();
    upstream_handle.abort();
}

#[test]
fn gateway_handles_admin_provider_oauth_device_poll_for_kiro_social_callback() {
    run_admin_oauth_test(
        "gateway_handles_admin_provider_oauth_device_poll_for_kiro_social_callback",
        gateway_handles_admin_provider_oauth_device_poll_for_kiro_social_callback_impl,
    );
}

async fn gateway_handles_admin_provider_oauth_device_poll_for_kiro_social_callback_impl() {
    let token_requests = Arc::new(Mutex::new(Vec::<(String, String)>::new()));
    let token_requests_clone = Arc::clone(&token_requests);
    let access_token = sample_kiro_device_access_token("social@example.com");
    let expected_access_token = access_token.clone();
    let token_server = Router::new().route(
        "/oauth/token",
        post(move |request: Request| {
            let token_requests_inner = Arc::clone(&token_requests_clone);
            let access_token_inner = access_token.clone();
            async move {
                let user_agent = request
                    .headers()
                    .get("user-agent")
                    .and_then(|value| value.to_str().ok())
                    .unwrap_or_default()
                    .to_string();
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
                    .push((user_agent, raw_body));
                Json(json!({
                    "accessToken": access_token_inner,
                    "refreshToken": "kiro-social-refresh-token",
                    "profileArn": "arn:aws:kiro:profile/social",
                    "idToken": "id-token-123",
                    "tokenType": "Bearer",
                    "expiresIn": 1800,
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

    let (token_url, token_handle) = start_server(token_server).await;
    let state = AppState::new()
        .expect("gateway should build")
        .with_data_state_for_tests(
            GatewayDataState::with_provider_catalog_repository_for_tests(
                provider_catalog_repository.clone(),
            )
            .with_encryption_key_for_tests(DEVELOPMENT_ENCRYPTION_KEY),
        )
        .with_provider_oauth_device_session_entry_for_tests(
            "session-social",
            json!({
                "provider_id": "provider-kiro",
                "region": "us-east-1",
                "client_id": "",
                "client_secret": "",
                "device_code": "",
                "auth_type": "social",
                "social_provider": "Github",
                "code_verifier": "verifier-123",
                "redirect_uri": "http://localhost:49153",
                "machine_id": "123e4567-e89b-12d3-a456-426614174000",
                "interval": 5,
                "expires_at_unix_secs": 4_102_444_800u64,
                "status": "pending",
                "proxy_node_id": null,
                "created_at_unix_ms": 1_711_000_000u64,
                "key_id": null,
                "email": null,
                "replaced": false,
                "error_msg": null,
            }),
        )
        .with_provider_oauth_token_url_for_tests(
            "kiro_social_token",
            format!("{token_url}/oauth/token"),
        );
    let gateway = build_router_with_state(state.clone());
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .post(format!(
            "{gateway_url}/api/admin/provider-oauth/providers/provider-kiro/device-poll"
        ))
        .header(crate::constants::GATEWAY_HEADER, "rust-phase3b")
        .header(TRUSTED_ADMIN_USER_ID_HEADER, "admin-user-123")
        .header(TRUSTED_ADMIN_USER_ROLE_HEADER, "admin")
        .header(TRUSTED_ADMIN_SESSION_ID_HEADER, "session-123")
        .header(
            TRUSTED_ADMIN_MANAGEMENT_TOKEN_ID_HEADER,
            "management-token-123",
        )
        .json(&json!({
            "session_id": "session-social",
            "callback_url": "http://localhost:49153/signin/callback?login_option=github&code=social-code-123&state=session-social"
        }))
        .send()
        .await
        .expect("request should succeed");

    let status = response.status();
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(status, StatusCode::OK, "payload={payload}");
    assert_eq!(payload["status"], "authorized");
    assert_eq!(payload["email"], "social@example.com");
    assert_eq!(payload["replaced"], false);

    {
        let requests = token_requests.lock().expect("mutex should lock");
        assert_eq!(requests.len(), 1);
        assert_eq!(
            requests[0].0,
            "KiroIDE-0.6.18-123e4567-e89b-12d3-a456-426614174000"
        );
        assert!(requests[0].1.contains("\"code\":\"social-code-123\""));
        assert!(requests[0].1.contains("\"code_verifier\":\"verifier-123\""));
        assert!(requests[0].1.contains(
            "\"redirect_uri\":\"http://localhost:49153/signin/callback?login_option=github\""
        ));
    }

    let stored = state
        .load_provider_oauth_device_session_for_tests("device_auth_session:session-social")
        .expect("device session should persist");
    let stored: serde_json::Value =
        serde_json::from_str(&stored).expect("device session json should parse");
    assert_eq!(stored["status"], "authorized");
    assert_eq!(stored["email"], "social@example.com");
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
    assert_eq!(persisted.name, "social@example.com (Github)");
    assert_eq!(persisted.auth_type, "oauth");
    let decrypted_api_key = decrypt_persisted_provider_api_key(&persisted);
    assert_eq!(decrypted_api_key, expected_access_token);
    let decrypted_auth_config = decrypt_persisted_provider_auth_config(&persisted);
    let auth_config: serde_json::Value =
        serde_json::from_str(&decrypted_auth_config).expect("auth config should parse");
    assert_eq!(auth_config["provider_type"], "kiro");
    assert_eq!(auth_config["provider"], "Github");
    assert_eq!(auth_config["auth_method"], "social");
    assert_eq!(auth_config["refresh_token"], "kiro-social-refresh-token");
    assert_eq!(auth_config["profile_arn"], "arn:aws:kiro:profile/social");
    assert_eq!(auth_config["email"], "social@example.com");
    assert_eq!(
        auth_config["machine_id"],
        "123e4567-e89b-12d3-a456-426614174000"
    );
    assert_eq!(auth_config["kiro_version"], "0.6.18");

    gateway_handle.abort();
    token_handle.abort();
}

#[test]
fn gateway_keeps_admin_provider_oauth_device_poll_pending_for_authorization_pending_error() {
    run_admin_oauth_test(
        "gateway_keeps_admin_provider_oauth_device_poll_pending_for_authorization_pending_error",
        gateway_keeps_admin_provider_oauth_device_poll_pending_for_authorization_pending_error_impl,
    );
}

async fn gateway_keeps_admin_provider_oauth_device_poll_pending_for_authorization_pending_error_impl(
) {
    let token_server = Router::new().route(
        "/token",
        post(move |_request: Request| async move {
            (
                StatusCode::BAD_REQUEST,
                Json(json!({
                    "error": "authorization_pending",
                    "error_description": "waiting for user confirmation",
                })),
            )
        }),
    );

    let mut provider = sample_provider("provider-kiro", "kiro", 10);
    provider.provider_type = "kiro".to_string();
    let provider_catalog_repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![provider],
        vec![],
        vec![],
    ));

    let (token_url, token_handle) = start_server(token_server).await;
    let state = AppState::new()
        .expect("gateway should build")
        .with_data_state_for_tests(GatewayDataState::with_provider_catalog_reader_for_tests(
            provider_catalog_repository,
        ))
        .with_provider_oauth_device_session_entry_for_tests(
            "session-pending",
            json!({
                "provider_id": "provider-kiro",
                "region": "us-east-1",
                "client_id": "kiro-device-client",
                "client_secret": "kiro-device-secret",
                "device_code": "device-code-123",
                "interval": 5,
                "expires_at_unix_secs": 4_102_444_800u64,
                "status": "pending",
                "proxy_node_id": null,
                "created_at_unix_ms": 1_711_000_000u64,
                "key_id": null,
                "email": null,
                "replaced": false,
                "error_msg": null,
            }),
        )
        .with_provider_oauth_token_url_for_tests("kiro_device_poll", format!("{token_url}/token"));

    let response = local_admin_provider_oauth_response(
        &state,
        http::Method::POST,
        "/api/admin/provider-oauth/providers/provider-kiro/device-poll",
        Some(json!({ "session_id": "session-pending" })),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    assert!(
        response.extensions().get::<AdminAuditEvent>().is_none(),
        "pending state should not attach terminal audit"
    );
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body should read");
    let payload: serde_json::Value = serde_json::from_slice(&body).expect("json body should parse");
    assert_eq!(payload["status"], "pending");
    assert_eq!(payload["replaced"], false);

    let stored = state
        .load_provider_oauth_device_session_for_tests("device_auth_session:session-pending")
        .expect("device session should persist");
    let stored: serde_json::Value =
        serde_json::from_str(&stored).expect("device session json should parse");
    assert_eq!(stored["status"], "pending");
    assert_eq!(stored["error_msg"], serde_json::Value::Null);

    token_handle.abort();
}

#[test]
fn local_admin_provider_oauth_device_poll_attaches_audit_only_when_transition_reaches_terminal_state(
) {
    run_admin_oauth_test(
        "local_admin_provider_oauth_device_poll_attaches_audit_only_when_transition_reaches_terminal_state",
        local_admin_provider_oauth_device_poll_attaches_audit_only_when_transition_reaches_terminal_state_impl,
    );
}

async fn local_admin_provider_oauth_device_poll_attaches_audit_only_when_transition_reaches_terminal_state_impl(
) {
    let access_token = sample_kiro_device_access_token("kiro@example.com");
    let token_server = Router::new().route(
        "/token",
        post(move |_request: Request| {
            let access_token_inner = access_token.clone();
            async move {
                Json(json!({
                    "accessToken": access_token_inner,
                    "refreshToken": "kiro-device-refresh-token",
                    "expiresIn": 1800,
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
    let (token_url, token_handle) = start_server(token_server).await;

    let terminal_state = AppState::new()
        .expect("gateway should build")
        .with_data_state_for_tests(
            GatewayDataState::with_provider_catalog_repository_for_tests(
                provider_catalog_repository.clone(),
            )
            .with_encryption_key_for_tests(DEVELOPMENT_ENCRYPTION_KEY),
        )
        .with_provider_oauth_device_session_entry_for_tests(
            "session-terminal",
            json!({
                "provider_id": "provider-kiro",
                "region": "us-east-1",
                "client_id": "kiro-device-client",
                "client_secret": "kiro-device-secret",
                "device_code": "device-code-123",
                "interval": 5,
                "expires_at_unix_secs": 4_102_444_800u64,
                "status": "pending",
                "proxy_node_id": null,
                "created_at_unix_ms": 1_711_000_000u64,
                "key_id": null,
                "email": null,
                "replaced": false,
                "error_msg": null,
            }),
        )
        .with_provider_oauth_token_url_for_tests("kiro_device_poll", format!("{token_url}/token"))
        .with_provider_oauth_token_url_for_tests("kiro_idc_refresh", token_url.to_string());

    let terminal_response = local_admin_provider_oauth_response(
        &terminal_state,
        http::Method::POST,
        "/api/admin/provider-oauth/providers/provider-kiro/device-poll",
        Some(json!({ "session_id": "session-terminal" })),
    )
    .await;
    assert_eq!(terminal_response.status(), StatusCode::OK);
    let terminal_audit = terminal_response
        .extensions()
        .get::<AdminAuditEvent>()
        .expect("terminal transition should attach audit");
    assert_eq!(
        terminal_audit.event_name,
        "admin_provider_oauth_device_authorization_completed"
    );
    assert_eq!(
        terminal_audit.action,
        "poll_provider_oauth_device_authorization_terminal_state"
    );
    assert_eq!(terminal_audit.target_type, "provider_oauth_device_session");
    assert_eq!(terminal_audit.target_id, "session-terminal");

    let non_terminal_state = AppState::new()
        .expect("gateway should build")
        .with_data_state_for_tests(GatewayDataState::with_provider_catalog_reader_for_tests(
            provider_catalog_repository,
        ))
        .with_provider_oauth_device_session_entry_for_tests(
            "session-existing-authorized",
            json!({
                "provider_id": "provider-kiro",
                "region": "us-east-1",
                "client_id": "kiro-device-client",
                "client_secret": "kiro-device-secret",
                "device_code": "device-code-123",
                "interval": 5,
                "expires_at_unix_secs": 4_102_444_800u64,
                "status": "authorized",
                "proxy_node_id": null,
                "created_at_unix_ms": 1_711_000_000u64,
                "key_id": "key-existing",
                "email": "kiro@example.com",
                "replaced": false,
                "error_msg": null,
            }),
        );

    let non_terminal_response = local_admin_provider_oauth_response(
        &non_terminal_state,
        http::Method::POST,
        "/api/admin/provider-oauth/providers/provider-kiro/device-poll",
        Some(json!({ "session_id": "session-existing-authorized" })),
    )
    .await;
    assert_eq!(non_terminal_response.status(), StatusCode::OK);
    assert!(
        non_terminal_response
            .extensions()
            .get::<AdminAuditEvent>()
            .is_none(),
        "existing terminal session should not attach a new audit event"
    );

    token_handle.abort();
}

#[test]
fn gateway_handles_admin_provider_oauth_device_authorize_via_execution_runtime_proxy_node() {
    run_admin_oauth_test(
        "gateway_handles_admin_provider_oauth_device_authorize_via_execution_runtime_proxy_node",
        gateway_handles_admin_provider_oauth_device_authorize_via_execution_runtime_proxy_node_impl,
    );
}

async fn gateway_handles_admin_provider_oauth_device_authorize_via_execution_runtime_proxy_node_impl(
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
                assert_eq!(proxy.node_id.as_deref(), Some("proxy-node-kiro"));
                assert_eq!(
                    plan.headers.get("host").map(String::as_str),
                    Some("oidc.us-east-1.amazonaws.com")
                );
                assert_eq!(
                    plan.headers
                        .get(EXECUTION_REQUEST_FOLLOW_REDIRECTS_HEADER)
                        .map(String::as_str),
                    Some("true")
                );
                if plan.request_id == "kiro_device_register" {
                    assert_eq!(plan.url, "https://oidc.us-east-1.amazonaws.com/client/register");
                    Json(json!({
                        "request_id": plan.request_id,
                        "status_code": 200,
                        "headers": {
                            "content-type": "application/json"
                        },
                        "body": {
                            "json_body": {
                                "clientId": "kiro-device-client",
                                "clientSecret": "kiro-device-secret"
                            }
                        }
                    }))
                } else {
                    assert_eq!(plan.request_id, "kiro_device_authorize");
                    assert_eq!(
                        plan.url,
                        "https://oidc.us-east-1.amazonaws.com/device_authorization"
                    );
                    Json(json!({
                        "request_id": plan.request_id,
                        "status_code": 200,
                        "headers": {
                            "content-type": "application/json"
                        },
                        "body": {
                            "json_body": {
                                "deviceCode": "device-code-123",
                                "userCode": "USER-CODE",
                                "verificationUri": "https://device.example.com/verify",
                                "verificationUriComplete": "https://device.example.com/verify?user_code=USER-CODE",
                                "expiresIn": 600,
                                "interval": 5
                            }
                        }
                    }))
                }
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
    let mut manual_node = sample_proxy_node("proxy-node-kiro");
    manual_node.status = "online".to_string();
    manual_node.proxy_url = Some("http://proxy.example:8080".to_string());
    let proxy_node_repository = Arc::new(InMemoryProxyNodeRepository::seed(vec![manual_node]));

    let (execution_runtime_url, execution_runtime_handle) = start_server(execution_runtime).await;
    let state = build_state_with_execution_runtime_override(execution_runtime_url)
        .with_data_state_for_tests(
            GatewayDataState::with_provider_catalog_reader_for_tests(provider_catalog_repository)
                .attach_proxy_node_repository_for_tests(proxy_node_repository),
        )
        .with_provider_oauth_device_session_entry_for_tests(
            "seed-session",
            json!({"status":"seed"}),
        )
        .with_provider_oauth_token_url_for_tests(
            "kiro_device_register",
            "https://oidc.us-east-1.amazonaws.com/client/register",
        )
        .with_provider_oauth_token_url_for_tests(
            "kiro_device_authorize",
            "https://oidc.us-east-1.amazonaws.com/device_authorization",
        );
    let gateway = build_router_with_state(state.clone());
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .post(format!(
            "{gateway_url}/api/admin/provider-oauth/providers/provider-kiro/device-authorize"
        ))
        .header(crate::constants::GATEWAY_HEADER, "rust-phase3b")
        .header(TRUSTED_ADMIN_USER_ID_HEADER, "admin-user-123")
        .header(TRUSTED_ADMIN_USER_ROLE_HEADER, "admin")
        .header(TRUSTED_ADMIN_SESSION_ID_HEADER, "session-123")
        .json(&json!({
            "start_url": "https://view.awsapps.com/start",
            "region": "us-east-1",
            "proxy_node_id": "proxy-node-kiro",
        }))
        .send()
        .await
        .expect("request should succeed");

    let status = response.status();
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(status, StatusCode::OK, "payload={payload}");
    let session_id = payload["session_id"]
        .as_str()
        .expect("session_id should exist")
        .to_string();
    assert_eq!(payload["user_code"], "USER-CODE");
    assert_eq!(payload["expires_in"], 600);
    assert_eq!(payload["interval"], 5);

    let stored = state
        .load_provider_oauth_device_session_for_tests(&format!("device_auth_session:{session_id}"))
        .expect("device session should be stored");
    let stored: serde_json::Value =
        serde_json::from_str(&stored).expect("device session json should parse");
    assert_eq!(stored["proxy_node_id"], "proxy-node-kiro");

    let plans = execution_plans.lock().expect("mutex should lock");
    assert_eq!(plans.len(), 2);

    gateway_handle.abort();
    execution_runtime_handle.abort();
}
