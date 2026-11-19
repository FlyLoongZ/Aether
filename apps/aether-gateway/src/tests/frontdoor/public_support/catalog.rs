use super::*;
use crate::tests::DEV_FERNET_KEY;

#[tokio::test]
async fn gateway_handles_public_catalog_site_info_without_proxying_upstream() {
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
    let gateway = build_router_with_state(
        AppState::new()
            .expect("gateway should build")
            .with_data_state_for_tests(
                crate::data::GatewayDataState::disabled().with_system_config_values_for_tests(
                    vec![
                        ("site_name".to_string(), json!("Aether Local")),
                        ("site_subtitle".to_string(), json!("Rust Only")),
                    ],
                ),
            ),
    );
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .get(format!("{gateway_url}/api/public/site-info"))
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["site_name"], "Aether Local");
    assert_eq!(payload["site_subtitle"], "Rust Only");
    assert_eq!(payload.as_object().map(|object| object.len()), Some(2));
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_rejects_unauthenticated_test_connection_before_using_provider_credentials() {
    let provider_hits = Arc::new(Mutex::new(0usize));
    let provider_hits_clone = Arc::clone(&provider_hits);
    let provider = Router::new().route(
        "/{*path}",
        any(move |_request: Request| {
            let provider_hits_inner = Arc::clone(&provider_hits_clone);
            async move {
                *provider_hits_inner.lock().expect("mutex should lock") += 1;
                Json(json!({"id": "must-not-run"})).into_response()
            }
        }),
    );
    let (provider_url, provider_handle) = start_server(provider).await;
    let provider_catalog_repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![sample_provider("provider-auth-required", "openai", 10)],
        vec![sample_endpoint(
            "endpoint-auth-required",
            "provider-auth-required",
            "openai:chat",
            &provider_url,
        )],
        vec![sample_key(
            "key-auth-required",
            "provider-auth-required",
            "openai:chat",
            "stored-provider-secret",
        )],
    ));
    let gateway = build_router_with_state(
        AppState::new()
            .expect("gateway should build")
            .with_data_state_for_tests(GatewayDataState::with_provider_transport_reader_for_tests(
                provider_catalog_repository,
                DEV_FERNET_KEY,
            )),
    );
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .get(format!(
            "{gateway_url}/v1/test-connection?provider=provider-auth-required&model=gpt-5&api_format=openai:chat"
        ))
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["detail"], "缺少用户凭证");
    assert_eq!(*provider_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    provider_handle.abort();
}

#[tokio::test]
async fn gateway_rejects_non_admin_test_connection_before_using_provider_credentials() {
    let provider_hits = Arc::new(Mutex::new(0usize));
    let provider_hits_clone = Arc::clone(&provider_hits);
    let provider = Router::new().route(
        "/{*path}",
        any(move |_request: Request| {
            let provider_hits_inner = Arc::clone(&provider_hits_clone);
            async move {
                *provider_hits_inner.lock().expect("mutex should lock") += 1;
                Json(json!({"id": "must-not-run"})).into_response()
            }
        }),
    );
    let (provider_url, provider_handle) = start_server(provider).await;
    let provider_catalog_repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![sample_provider("provider-admin-only", "openai", 10)],
        vec![sample_endpoint(
            "endpoint-admin-only",
            "provider-admin-only",
            "openai:chat",
            &provider_url,
        )],
        vec![sample_key(
            "key-admin-only",
            "provider-admin-only",
            "openai:chat",
            "stored-provider-secret",
        )],
    ));

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
                json!("session-test-connection-non-admin"),
            ),
        ]),
        now + chrono::Duration::hours(1),
    );
    let user_repository = Arc::new(InMemoryUserReadRepository::seed_auth_users(vec![user]));
    let gateway = build_router_with_state(
        AppState::new()
            .expect("gateway should build")
            .with_data_state_for_tests(
                GatewayDataState::with_provider_transport_reader_for_tests(
                    provider_catalog_repository,
                    DEV_FERNET_KEY,
                )
                .with_user_reader(user_repository),
            )
            .with_auth_session_for_tests(sample_auth_session(
                "user-auth-1",
                "session-test-connection-non-admin",
                "device-test-connection-non-admin",
                "refresh-token-placeholder",
                now,
            )),
    );
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .get(format!(
            "{gateway_url}/v1/test-connection?provider=provider-admin-only&model=gpt-5&api_format=openai:chat"
        ))
        .header("authorization", format!("Bearer {access_token}"))
        .header(
            "x-client-device-id",
            "device-test-connection-non-admin",
        )
        .header("user-agent", "AetherTest/1.0")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["detail"], "仅管理员可以测试供应商连接");
    assert_eq!(*provider_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    provider_handle.abort();
}

#[tokio::test]
async fn gateway_handles_users_me_usage_search_with_model_and_api_key_keywords() {
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
                json!("session-users-me-usage-multi-keyword-search"),
            ),
        ]),
        now + chrono::Duration::hours(1),
    );
    let usage_repository = Arc::new(InMemoryUsageReadRepository::seed(vec![
        sample_user_usage_audit(
            "usage-users-me-completed-multi-keyword-search",
            "req-users-me-completed-multi-keyword-search",
            "user-auth-1",
            "gpt-4.1",
            "OpenAI",
            "completed",
            now - chrono::Duration::minutes(20),
        ),
        sample_user_usage_audit(
            "usage-users-me-failed-multi-keyword-search",
            "req-users-me-failed-multi-keyword-search",
            "user-auth-1",
            "claude-3.5-sonnet",
            "Anthropic",
            "failed",
            now - chrono::Duration::minutes(10),
        ),
        sample_user_usage_audit(
            "usage-users-me-streaming-multi-keyword-search",
            "req-users-me-streaming-multi-keyword-search",
            "user-auth-1",
            "gpt-4.1-mini",
            "OpenAI",
            "streaming",
            now - chrono::Duration::minutes(5),
        ),
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
                    "session-users-me-usage-multi-keyword-search",
                    "device-users-me-usage-multi-keyword-search",
                    "refresh-token-users-me-usage-multi-keyword-search",
                    now,
                )])
        })
        .await;

    let response = reqwest::Client::new()
        .get(format!(
            "{gateway_url}/api/users/me/usage?limit=10&offset=0&search=gpt%20renamed-key"
        ))
        .header("authorization", format!("Bearer {access_token}"))
        .header(
            "x-client-device-id",
            "device-users-me-usage-multi-keyword-search",
        )
        .header("user-agent", "AetherTest/1.0")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["pagination"]["total"], 2);
    assert_eq!(
        payload["records"].as_array().expect("records array").len(),
        2
    );
    assert_eq!(payload["records"][0]["model"], "gpt-4.1-mini");
    assert_eq!(payload["records"][1]["model"], "gpt-4.1");
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_handles_users_me_providers_locally_without_proxying_upstream() {
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
                json!("session-users-me-providers"),
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
                "endpoint-openai-1",
                "provider-openai",
                "openai:chat",
                "https://api.openai.example",
            ),
            sample_endpoint(
                "endpoint-claude-1",
                "provider-claude",
                "claude:messages",
                "https://api.claude.example",
            ),
        ],
        vec![],
    ));
    let global_model_repository = Arc::new(
        InMemoryGlobalModelReadRepository::seed(Vec::<StoredPublicGlobalModel>::new())
            .with_public_catalog_models(vec![
                sample_public_catalog_model(
                    "model-openai-gpt5",
                    "provider-openai",
                    "openai",
                    "gpt-5-preview",
                    "gpt-5",
                    "GPT 5",
                ),
                sample_public_catalog_model(
                    "model-claude-sonnet",
                    "provider-claude",
                    "claude",
                    "claude-sonnet-4-5-20251001",
                    "claude-sonnet-4-5",
                    "Claude Sonnet 4.5",
                ),
            ]),
    );
    let user_repository = Arc::new(InMemoryUserReadRepository::seed_auth_users(vec![user]));
    let group = user_repository
        .create_user_group(UpsertUserGroupRecord {
            name: "OpenAI only".to_string(),
            description: None,
            priority: 0,
            allowed_providers: Some(vec!["openai".to_string()]),
            allowed_providers_mode: "specific".to_string(),
            allowed_api_formats: None,
            allowed_api_formats_mode: "unrestricted".to_string(),
            allowed_models: None,
            allowed_models_mode: "unrestricted".to_string(),
            rate_limit: None,
            rate_limit_mode: "system".to_string(),
        })
        .await
        .expect("group should create")
        .expect("group should exist");
    user_repository
        .add_user_to_group(&group.id, "user-auth-1")
        .await
        .expect("group membership should create");

    let (gateway_url, upstream_hits, gateway_handle, upstream_handle) =
        start_auth_gateway_with_builder(|| {
            let data_state = crate::data::GatewayDataState::with_global_model_reader_for_tests(
                global_model_repository,
            )
            .with_provider_catalog_reader(provider_catalog_repository)
            .with_user_reader(user_repository);
            AppState::new()
                .expect("gateway should build")
                .with_data_state_for_tests(data_state)
                .with_auth_sessions_for_tests([sample_auth_session(
                    "user-auth-1",
                    "session-users-me-providers",
                    "device-users-me-providers",
                    "refresh-token-users-me-providers",
                    now,
                )])
        })
        .await;

    let response = reqwest::Client::new()
        .get(format!("{gateway_url}/api/users/me/providers"))
        .header("authorization", format!("Bearer {access_token}"))
        .header("x-client-device-id", "device-users-me-providers")
        .header("user-agent", "AetherTest/1.0")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    let providers = payload.as_array().expect("providers should be array");
    assert_eq!(providers.len(), 1);
    assert_eq!(providers[0]["id"], "provider-openai");
    assert!(providers[0].get("name").is_none());
    assert!(providers[0].get("description").is_none());
    assert_eq!(providers[0]["endpoints"][0]["id"], "endpoint-openai-1");
    assert!(providers[0]["endpoints"][0].get("base_url").is_none());
    assert_eq!(providers[0]["models"][0]["name"], "gpt-5");
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_handles_users_me_available_models_locally_without_proxying_upstream() {
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
                json!("session-users-me-available-models"),
            ),
        ]),
        now + chrono::Duration::hours(1),
    );
    let global_model_repository = Arc::new(
        InMemoryGlobalModelReadRepository::seed(vec![
            sample_public_global_model("gm-1", "gpt-5", "GPT 5", true),
            sample_public_global_model("gm-2", "claude-sonnet-4-5", "Claude Sonnet 4.5", true),
            sample_public_global_model("gm-3", "disabled-model", "Disabled Model", false),
        ])
        .with_active_global_model_refs(vec![
            StoredProviderActiveGlobalModel::new("provider-openai".to_string(), "gm-1".to_string())
                .expect("active global model ref should build"),
            StoredProviderActiveGlobalModel::new("provider-claude".to_string(), "gm-2".to_string())
                .expect("active global model ref should build"),
        ]),
    );
    let provider_catalog_repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![
            sample_provider("provider-openai", "openai", 10),
            sample_provider("provider-claude", "claude", 20),
        ],
        vec![],
        vec![],
    ));
    let user_repository = Arc::new(InMemoryUserReadRepository::seed_auth_users(vec![user]));
    let (gateway_url, upstream_hits, gateway_handle, upstream_handle) =
        start_auth_gateway_with_builder(|| {
            let data_state = crate::data::GatewayDataState::with_global_model_reader_for_tests(
                global_model_repository,
            )
            .with_provider_catalog_reader(provider_catalog_repository)
            .with_user_reader(user_repository);
            AppState::new()
                .expect("gateway should build")
                .with_data_state_for_tests(data_state)
                .with_auth_sessions_for_tests([sample_auth_session(
                    "user-auth-1",
                    "session-users-me-available-models",
                    "device-users-me-available-models",
                    "refresh-token-placeholder",
                    now,
                )])
        })
        .await;

    let response = reqwest::Client::new()
        .get(format!(
            "{gateway_url}/api/users/me/available-models?search=gpt&skip=0&limit=10"
        ))
        .header("authorization", format!("Bearer {access_token}"))
        .header("x-client-device-id", "device-users-me-available-models")
        .header("user-agent", "AetherTest/1.0")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    let models = payload["models"]
        .as_array()
        .expect("models should be an array");
    assert_eq!(payload["total"], 1);
    assert_eq!(models.len(), 1);
    assert_eq!(models[0]["id"], "gm-1");
    assert_eq!(models[0]["name"], "gpt-5");
    assert_eq!(models[0]["display_name"], "GPT 5");
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_refreshes_users_me_available_models_after_group_assignment() {
    let now = Utc::now();
    let user = sample_auth_user(now);
    // Keep the legacy gpt-5 personal policy from sample_auth_user. Group policies are the
    // authority now, so this stale field must not narrow the user catalog.
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
                json!("session-users-me-group-models"),
            ),
        ]),
        now + chrono::Duration::hours(1),
    );
    let mut allowed_model =
        sample_public_global_model("gm-2", "claude-sonnet-4-5", "Claude Sonnet 4.5", true);
    allowed_model.config = Some(json!({
        "description": "Claude detail",
        "model_mappings": ["claude-upstream"]
    }));
    let mut blocked_model = sample_public_global_model("gm-1", "gpt-5", "GPT 5", true);
    blocked_model.config = Some(json!({
        "description": "GPT detail",
        "model_mappings": ["gpt-upstream"]
    }));
    let global_model_repository = Arc::new(InMemoryGlobalModelReadRepository::seed(vec![
        blocked_model,
        allowed_model,
    ]));
    let user_repository: Arc<dyn UserReadRepository> =
        Arc::new(InMemoryUserReadRepository::seed_auth_users(vec![user]));
    let group = user_repository
        .create_user_group(UpsertUserGroupRecord {
            name: "Claude only".to_string(),
            description: None,
            priority: 0,
            allowed_providers: None,
            allowed_providers_mode: "unrestricted".to_string(),
            allowed_api_formats: None,
            allowed_api_formats_mode: "unrestricted".to_string(),
            allowed_models: Some(vec!["claude-sonnet-4-5".to_string()]),
            allowed_models_mode: "specific".to_string(),
            rate_limit: None,
            rate_limit_mode: "system".to_string(),
        })
        .await
        .expect("group should create")
        .expect("group should exist");
    let data_state =
        crate::data::GatewayDataState::with_global_model_reader_for_tests(global_model_repository)
            .with_user_reader(Arc::clone(&user_repository));
    let state = AppState::new()
        .expect("gateway should build")
        .with_data_state_for_tests(data_state)
        .with_auth_sessions_for_tests([sample_auth_session(
            "user-auth-1",
            "session-users-me-group-models",
            "device-users-me-group-models",
            "refresh-token-placeholder",
            now,
        )]);
    let mutation_state = state.clone();
    let (gateway_url, upstream_hits, gateway_handle, upstream_handle) =
        start_auth_gateway_with_builder(|| state).await;

    let client = reqwest::Client::new();
    let response = client
        .get(format!("{gateway_url}/api/users/me/available-models"))
        .header("authorization", format!("Bearer {access_token}"))
        .header("x-client-device-id", "device-users-me-group-models")
        .header("user-agent", "AetherTest/1.0")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["total"], 2);

    mutation_state
        .replace_user_groups_for_user("user-auth-1", std::slice::from_ref(&group.id))
        .await
        .expect("group membership should create");

    let response = client
        .get(format!("{gateway_url}/api/users/me/available-models"))
        .header("authorization", format!("Bearer {access_token}"))
        .header("x-client-device-id", "device-users-me-group-models")
        .header("user-agent", "AetherTest/1.0")
        .send()
        .await
        .expect("request should succeed after group assignment");

    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    let models = payload["models"]
        .as_array()
        .expect("models should be an array");
    assert_eq!(payload["total"], 1);
    assert_eq!(models.len(), 1);
    assert_eq!(models[0]["name"], "claude-sonnet-4-5");
    assert_eq!(models[0]["config"]["description"], "Claude detail");
    assert!(models[0]["config"].get("model_mappings").is_none());
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_returns_no_users_me_available_models_when_group_denies_all_models() {
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
                json!("session-users-me-deny-all-models"),
            ),
        ]),
        now + chrono::Duration::hours(1),
    );
    let global_model_repository = Arc::new(InMemoryGlobalModelReadRepository::seed(vec![
        sample_public_global_model("gm-1", "gpt-5", "GPT 5", true),
        sample_public_global_model("gm-2", "claude-sonnet-4-5", "Claude Sonnet 4.5", true),
    ]));
    let user_repository: Arc<dyn UserReadRepository> =
        Arc::new(InMemoryUserReadRepository::seed_auth_users(vec![user]));
    let group = user_repository
        .create_user_group(UpsertUserGroupRecord {
            name: "No models".to_string(),
            description: None,
            priority: 0,
            allowed_providers: None,
            allowed_providers_mode: "unrestricted".to_string(),
            allowed_api_formats: None,
            allowed_api_formats_mode: "unrestricted".to_string(),
            allowed_models: None,
            allowed_models_mode: "deny_all".to_string(),
            rate_limit: None,
            rate_limit_mode: "system".to_string(),
        })
        .await
        .expect("group should create")
        .expect("group should exist");
    user_repository
        .add_user_to_group(&group.id, "user-auth-1")
        .await
        .expect("group membership should create");

    let (gateway_url, upstream_hits, gateway_handle, upstream_handle) =
        start_auth_gateway_with_builder(|| {
            let data_state = crate::data::GatewayDataState::with_global_model_reader_for_tests(
                global_model_repository,
            )
            .with_user_reader(Arc::clone(&user_repository));
            AppState::new()
                .expect("gateway should build")
                .with_data_state_for_tests(data_state)
                .with_auth_sessions_for_tests([sample_auth_session(
                    "user-auth-1",
                    "session-users-me-deny-all-models",
                    "device-users-me-deny-all-models",
                    "refresh-token-placeholder",
                    now,
                )])
        })
        .await;

    let response = reqwest::Client::new()
        .get(format!("{gateway_url}/api/users/me/available-models"))
        .header("authorization", format!("Bearer {access_token}"))
        .header("x-client-device-id", "device-users-me-deny-all-models")
        .header("user-agent", "AetherTest/1.0")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["total"], 0);
    assert_eq!(payload["models"].as_array().map(Vec::len), Some(0));
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_returns_service_unavailable_for_users_me_available_models_without_provider_catalog(
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
                json!("session-users-me-available-models-unavailable"),
            ),
        ]),
        now + chrono::Duration::hours(1),
    );
    let global_model_repository = Arc::new(
        InMemoryGlobalModelReadRepository::seed(vec![
            sample_public_global_model("gm-1", "gpt-5", "GPT 5", true),
            sample_public_global_model("gm-2", "claude-sonnet-4-5", "Claude Sonnet 4.5", true),
        ])
        .with_active_global_model_refs(vec![StoredProviderActiveGlobalModel::new(
            "provider-openai".to_string(),
            "gm-1".to_string(),
        )
        .expect("active global model ref should build")]),
    );
    let user_repository = Arc::new(InMemoryUserReadRepository::seed_auth_users(vec![user]));
    let group = user_repository
        .create_user_group(UpsertUserGroupRecord {
            name: "OpenAI provider only".to_string(),
            description: None,
            priority: 0,
            allowed_providers: Some(vec!["openai".to_string()]),
            allowed_providers_mode: "specific".to_string(),
            allowed_api_formats: None,
            allowed_api_formats_mode: "unrestricted".to_string(),
            allowed_models: None,
            allowed_models_mode: "unrestricted".to_string(),
            rate_limit: None,
            rate_limit_mode: "system".to_string(),
        })
        .await
        .expect("group should create")
        .expect("group should exist");
    user_repository
        .add_user_to_group(&group.id, "user-auth-1")
        .await
        .expect("group membership should create");
    let (gateway_url, upstream_hits, gateway_handle, upstream_handle) =
        start_auth_gateway_with_builder(|| {
            let data_state = crate::data::GatewayDataState::with_global_model_reader_for_tests(
                global_model_repository,
            )
            .with_user_reader(user_repository);
            AppState::new()
                .expect("gateway should build")
                .with_data_state_for_tests(data_state)
                .with_auth_sessions_for_tests([sample_auth_session(
                    "user-auth-1",
                    "session-users-me-available-models-unavailable",
                    "device-users-me-available-models-unavailable",
                    "refresh-token-placeholder",
                    now,
                )])
        })
        .await;

    let response = reqwest::Client::new()
        .get(format!("{gateway_url}/api/users/me/available-models"))
        .header("authorization", format!("Bearer {access_token}"))
        .header(
            "x-client-device-id",
            "device-users-me-available-models-unavailable",
        )
        .header("user-agent", "AetherTest/1.0")
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
async fn gateway_handles_users_me_model_capabilities_get_locally_without_proxying_upstream() {
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
                json!("session-users-me-model-cap-get"),
            ),
        ]),
        now + chrono::Duration::hours(1),
    );
    let user_repository = Arc::new(InMemoryUserReadRepository::seed_auth_users(vec![user]));
    let (gateway_url, upstream_hits, gateway_handle, upstream_handle) =
        start_auth_gateway_with_builder(|| {
            let data_state =
                crate::data::GatewayDataState::with_user_reader_for_tests(user_repository);
            AppState::new()
                .expect("gateway should build")
                .with_data_state_for_tests(data_state)
                .with_auth_sessions_for_tests([sample_auth_session(
                    "user-auth-1",
                    "session-users-me-model-cap-get",
                    "device-users-me-model-cap-get",
                    "refresh-token-placeholder",
                    now,
                )])
                .with_auth_user_model_capability_settings_for_tests(
                    "user-auth-1",
                    json!({"gpt-5": {"cache_1h": true}}),
                )
        })
        .await;

    let response = reqwest::Client::new()
        .get(format!("{gateway_url}/api/users/me/model-capabilities"))
        .header("authorization", format!("Bearer {access_token}"))
        .header("x-client-device-id", "device-users-me-model-cap-get")
        .header("user-agent", "AetherTest/1.0")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(
        payload["model_capability_settings"],
        json!({"gpt-5": {"cache_1h": true}})
    );
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_updates_users_me_model_capabilities_locally_without_proxying_upstream() {
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
                json!("session-users-me-model-cap-put"),
            ),
        ]),
        now + chrono::Duration::hours(1),
    );
    let user_repository = Arc::new(InMemoryUserReadRepository::seed_auth_users(vec![user]));
    let (gateway_url, upstream_hits, gateway_handle, upstream_handle) =
        start_auth_gateway_with_builder(|| {
            let data_state =
                crate::data::GatewayDataState::with_user_reader_for_tests(user_repository);
            AppState::new()
                .expect("gateway should build")
                .with_data_state_for_tests(data_state)
                .with_auth_sessions_for_tests([sample_auth_session(
                    "user-auth-1",
                    "session-users-me-model-cap-put",
                    "device-users-me-model-cap-put",
                    "refresh-token-placeholder",
                    now,
                )])
                .with_auth_user_model_capability_settings_for_tests(
                    "user-auth-1",
                    json!({"disabled-model": {"disabled_flag": true}}),
                )
        })
        .await;

    let client = reqwest::Client::new();
    let put_response = client
        .put(format!("{gateway_url}/api/users/me/model-capabilities"))
        .header("authorization", format!("Bearer {access_token}"))
        .header("x-client-device-id", "device-users-me-model-cap-put")
        .header("user-agent", "AetherTest/1.0")
        .json(&json!({
            "model_capability_settings": {}
        }))
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(put_response.status(), StatusCode::OK);
    let put_payload: serde_json::Value = put_response.json().await.expect("json body should parse");
    assert_eq!(put_payload["message"], "模型能力配置已更新");
    assert_eq!(put_payload["model_capability_settings"], json!({}));

    let get_response = client
        .get(format!("{gateway_url}/api/users/me/model-capabilities"))
        .header("authorization", format!("Bearer {access_token}"))
        .header("x-client-device-id", "device-users-me-model-cap-put")
        .header("user-agent", "AetherTest/1.0")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(get_response.status(), StatusCode::OK);
    let get_payload: serde_json::Value = get_response.json().await.expect("json body should parse");
    assert_eq!(get_payload["model_capability_settings"], json!({}));
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_returns_service_unavailable_for_users_me_model_capabilities_update_without_storage(
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
                json!("session-users-me-model-cap-unavailable"),
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
                    "session-users-me-model-cap-unavailable",
                    "device-users-me-model-cap-unavailable",
                    "refresh-token-placeholder",
                    now,
                )])
                .without_auth_user_model_capability_store_for_tests()
        })
        .await;

    let response = reqwest::Client::new()
        .put(format!("{gateway_url}/api/users/me/model-capabilities"))
        .header("authorization", format!("Bearer {access_token}"))
        .header(
            "x-client-device-id",
            "device-users-me-model-cap-unavailable",
        )
        .header("user-agent", "AetherTest/1.0")
        .json(&json!({
            "model_capability_settings": {}
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
