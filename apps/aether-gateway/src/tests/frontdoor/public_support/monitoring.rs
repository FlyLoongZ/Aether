use super::*;

#[tokio::test]
async fn health_v2_public_scope_filters_summary_lists_and_details_before_projection() {
    let now = Utc::now();
    let mut published = sample_user_usage_audit(
        "private-detail-id",
        "private-request-id",
        "private-user-id",
        "internal-published-model",
        "private-provider-name",
        "failed",
        now - chrono::Duration::minutes(5),
    );
    published.request_metadata = Some(json!({
        "analytics_failure": {"origin": "upstream", "stage": "response", "reason": "private-diagnostic", "schema_version": 1},
    }));
    let hidden = sample_user_usage_audit(
        "private-hidden-id",
        "private-hidden-request",
        "private-user-id",
        "unpublished-model",
        "private-provider-name",
        "completed",
        now - chrono::Duration::minutes(4),
    );
    let data = GatewayDataState::with_usage_reader_for_tests(Arc::new(InMemoryUsageReadRepository::seed(vec![published, hidden])))
        .with_system_config_values_for_tests(vec![("health_publication_v1".into(), json!({
            "enabled": true, "objects": [
                {"public_id": "model-api", "kind": "model", "value": "internal-published-model", "display_name": "Model API"},
            ],
        }))]);
    let (gateway_url, gateway_handle) = start_server(build_router_with_state(
        AppState::new().unwrap().with_data_state_for_tests(data),
    ))
    .await;
    let client = reqwest::Client::new();
    for resource in ["summary", "objects", "objects/model-api"] {
        let response = client
            .get(format!(
                "{gateway_url}/api/public/health/v2/{resource}?kind=model"
            ))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body: serde_json::Value = response.json().await.unwrap();
        let metrics = match resource {
            "summary" => &body["data"]["requests"],
            "objects" => &body["data"]["items"][0],
            _ => &body["data"],
        };
        assert_eq!(
            metrics["request_count"], 1,
            "{resource} includes only the published model"
        );
        if resource == "objects" {
            assert_eq!(body["data"]["total"], 1);
            assert!(!metrics["timeline"].as_array().unwrap().is_empty());
        }
        let encoded = body.to_string();
        for forbidden in [
            "private-",
            "internal-published-model",
            "unpublished-model",
            "provider_id",
            "api_key_id",
            "source_value",
            "attempts",
            "error_message",
            "analytics_failure",
        ] {
            assert!(
                !encoded.contains(forbidden),
                "{resource} leaked {forbidden}"
            );
        }
    }
    let empty_scope = client
        .get(format!(
            "{gateway_url}/api/public/health/v2/summary?kind=api_format"
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(empty_scope.status(), StatusCode::OK);
    let empty: serde_json::Value = empty_scope.json().await.unwrap();
    assert_eq!(empty["data"]["object_count"], 0);
    assert_eq!(empty["data"]["requests"]["request_count"], 0);
    gateway_handle.abort();
}

#[tokio::test]
async fn health_v2_authenticated_user_access_does_not_publish_anonymous_status() {
    let now = Utc::now();
    let user = sample_auth_user(now);
    let access_token = build_test_auth_token(
        "access",
        serde_json::Map::from_iter([
            ("user_id".into(), json!(user.id)),
            ("role".into(), json!(user.role)),
            (
                "created_at".into(),
                json!(user.created_at.map(|value| value.to_rfc3339())),
            ),
            ("session_id".into(), json!("health-user-session")),
        ]),
        now + chrono::Duration::hours(1),
    );
    let data = GatewayDataState::with_usage_reader_for_tests(Arc::new(
        InMemoryUsageReadRepository::seed(vec![sample_user_usage_audit(
            "health-user-row",
            "health-user-request",
            &user.id,
            "model-health",
            "internal-provider",
            "completed",
            now - chrono::Duration::minutes(5),
        )]),
    ))
    .with_user_reader(Arc::new(InMemoryUserReadRepository::seed_auth_users(vec![
        user,
    ])))
    .with_provider_catalog_reader(Arc::new(InMemoryProviderCatalogReadRepository::seed(
        Vec::new(),
        Vec::new(),
        Vec::new(),
    )))
    .with_system_config_values_for_tests(vec![(
        "health_publication_v1".into(),
        json!({ "enabled": false, "objects": [] }),
    )]);
    let state = AppState::new()
        .unwrap()
        .with_data_state_for_tests(data)
        .with_auth_session_for_tests(sample_auth_session(
            "user-auth-1",
            "health-user-session",
            "health-user-device",
            "refresh-token-placeholder",
            now,
        ));
    let (gateway_url, gateway_handle) = start_server(build_router_with_state(state)).await;
    let client = reqwest::Client::new();
    let url = format!("{gateway_url}/api/users/me/health/v2/objects?kind=api_format");
    let anonymous = client.get(&url).send().await.unwrap();
    assert_eq!(anonymous.status(), StatusCode::UNAUTHORIZED);
    let authenticated = client
        .get(&url)
        .bearer_auth(&access_token)
        .header("x-client-device-id", "health-user-device")
        .header("user-agent", "AetherTest/1.0")
        .send()
        .await
        .unwrap();
    assert_eq!(authenticated.status(), StatusCode::OK);
    let body: serde_json::Value = authenticated.json().await.unwrap();
    assert_eq!(body["meta"]["scope"]["kind"], "authenticated");
    assert_eq!(body["data"]["total"], 1);
    assert_eq!(body["data"]["items"][0]["request_count"], 1);
    assert!(body["data"]["items"][0].get("attempts").is_none());
    assert!(!body.to_string().contains("internal-provider"));
    let provider = client
        .get(format!(
            "{gateway_url}/api/users/me/health/v2/objects?kind=provider"
        ))
        .bearer_auth(&access_token)
        .header("x-client-device-id", "health-user-device")
        .header("user-agent", "AetherTest/1.0")
        .send()
        .await
        .unwrap();
    assert_eq!(provider.status(), StatusCode::BAD_REQUEST);
    let public = client
        .get(format!("{gateway_url}/api/public/health/v2/objects"))
        .send()
        .await
        .unwrap();
    assert_eq!(public.status(), StatusCode::NOT_FOUND);
    gateway_handle.abort();
}

#[tokio::test]
async fn gateway_handles_public_health_api_formats_without_proxying_upstream() {
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
        vec![],
    ));
    let now_unix_secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("current time should be after epoch")
        .as_secs() as i64;
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
            "cand-openai-skipped",
            "req-openai-skipped",
            "endpoint-openai",
            RequestCandidateStatus::Skipped,
            now_unix_secs - 1_000,
            Some(now_unix_secs - 980),
        ),
        sample_request_candidate(
            "cand-openai-pending",
            "req-openai-pending",
            "endpoint-openai",
            RequestCandidateStatus::Pending,
            now_unix_secs - 500,
            None,
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

    let (_upstream_url, upstream_handle) = start_server(upstream).await;
    let gateway = build_router_with_state(
        AppState::new()
            .expect("gateway should build")
            .with_data_state_for_tests(
                crate::data::GatewayDataState::with_provider_catalog_and_request_candidate_reader_for_tests(
                    provider_catalog_repository,
                    request_candidate_repository,
                ),
            ),
    );
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .get(format!(
            "{gateway_url}/api/public/health/api-formats?lookback_hours=6&per_format_limit=10"
        ))
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    let formats = payload["formats"]
        .as_array()
        .expect("formats should be an array");
    assert_eq!(formats.len(), 2);
    assert_eq!(formats[0]["api_format"], "claude:messages");
    assert_eq!(formats[0]["api_path"], "/v1/messages");
    assert_eq!(formats[0]["total_attempts"], 1);
    assert_eq!(formats[0]["success_rate"], 1.0);
    assert_eq!(formats[0]["events"].as_array().map(Vec::len), Some(1));
    assert_eq!(formats[0]["timeline"].as_array().map(Vec::len), Some(60));
    assert_eq!(formats[1]["api_format"], "openai:chat");
    assert_eq!(formats[1]["api_path"], "/v1/chat/completions");
    assert_eq!(formats[1]["total_attempts"], 3);
    assert_eq!(formats[1]["success_count"], 1);
    assert_eq!(formats[1]["failed_count"], 1);
    assert_eq!(formats[1]["skipped_count"], 1);
    assert_eq!(formats[1]["success_rate"], 0.5);
    assert_eq!(formats[1]["events"].as_array().map(Vec::len), Some(3));
    assert_eq!(formats[1]["timeline"].as_array().map(Vec::len), Some(60));
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_handles_user_monitoring_audit_logs_locally_without_proxying_upstream() {
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
                json!("session-monitoring-audit-logs"),
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
                "session-monitoring-audit-logs",
                "device-monitoring-audit-logs",
                "refresh-monitoring-audit-logs",
                now,
            )],
        )
        .await;

    let response = reqwest::Client::new()
        .get(format!(
            "{gateway_url}/api/monitoring/my-audit-logs?days=30&limit=50&offset=0"
        ))
        .header("authorization", format!("Bearer {access_token}"))
        .header("x-client-device-id", "device-monitoring-audit-logs")
        .header("user-agent", "AetherTest/1.0")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["items"], json!([]));
    assert_eq!(payload["meta"]["total"], 0);
    assert_eq!(payload["meta"]["limit"], 50);
    assert_eq!(payload["meta"]["offset"], 0);
    assert_eq!(payload["meta"]["count"], 0);
    assert_eq!(payload["filters"]["days"], 30);
    assert_eq!(payload["filters"]["event_type"], serde_json::Value::Null);
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_handles_user_monitoring_rate_limit_status_locally_without_proxying_upstream() {
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
                json!("session-monitoring-rate-limit-status"),
            ),
        ]),
        now + chrono::Duration::hours(1),
    );

    let auth_repository = Arc::new(InMemoryAuthApiKeySnapshotRepository::seed(vec![(
        Some("hash-monitoring-key-1".to_string()),
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
            "api-key-monitoring-1".to_string(),
            Some("monitoring-key".to_string()),
            true,
            false,
            false,
            Some(30),
            Some(5),
            None,
            None,
            None,
            None,
        )
        .expect("auth api key snapshot should build")
        .with_user_rate_limit(Some(80)),
    )]));
    let user_repository: Arc<dyn UserReadRepository> =
        Arc::new(InMemoryUserReadRepository::seed_auth_users(vec![
            sample_auth_user(now),
        ]));
    let group = user_repository
        .create_user_group(UpsertUserGroupRecord {
            name: "Monitoring Limits".to_string(),
            description: None,
            priority: 0,
            allowed_providers: None,
            allowed_providers_mode: "unrestricted".to_string(),
            allowed_api_formats: None,
            allowed_api_formats_mode: "unrestricted".to_string(),
            allowed_models: None,
            allowed_models_mode: "unrestricted".to_string(),
            rate_limit: Some(80),
            rate_limit_mode: "custom".to_string(),
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
            let data_state = crate::data::GatewayDataState::with_auth_api_key_repository_for_tests(
                auth_repository,
            )
            .with_user_reader(Arc::clone(&user_repository));
            AppState::new()
                .expect("gateway should build")
                .with_data_state_for_tests(data_state)
                .with_auth_sessions_for_tests([sample_auth_session(
                    "user-auth-1",
                    "session-monitoring-rate-limit-status",
                    "device-monitoring-rate-limit-status",
                    "refresh-monitoring-rate-limit-status",
                    now,
                )])
        })
        .await;

    let response = reqwest::Client::new()
        .get(format!("{gateway_url}/api/monitoring/rate-limit-status"))
        .header("authorization", format!("Bearer {access_token}"))
        .header("x-client-device-id", "device-monitoring-rate-limit-status")
        .header("user-agent", "AetherTest/1.0")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["user_id"], "user-auth-1");
    let api_keys = payload["api_keys"]
        .as_array()
        .expect("api_keys should be array");
    assert_eq!(api_keys.len(), 1);
    assert_eq!(api_keys[0]["api_key_name"], "monitoring-key");
    assert_eq!(api_keys[0]["limit"], 30);
    assert_eq!(api_keys[0]["remaining"], 30);
    assert_eq!(api_keys[0]["scope"], "key");
    assert_eq!(api_keys[0]["user_limit"], 80);
    assert_eq!(api_keys[0]["user_remaining"], 80);
    assert_eq!(api_keys[0]["key_limit"], 30);
    assert_eq!(api_keys[0]["key_remaining"], 30);
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_returns_service_unavailable_for_users_me_endpoint_status_without_health_data() {
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
                json!("session-user-endpoint-status-unavailable"),
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
                    "session-user-endpoint-status-unavailable",
                    "device-user-endpoint-status-unavailable",
                    "refresh-token-placeholder",
                    now,
                )])
        })
        .await;

    let response = reqwest::Client::new()
        .get(format!("{gateway_url}/api/users/me/endpoint-status"))
        .header("authorization", format!("Bearer {access_token}"))
        .header(
            "x-client-device-id",
            "device-user-endpoint-status-unavailable",
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
async fn gateway_rate_limits_repeated_auth_login_attempts_by_identifier() {
    let now = Utc::now();
    let user = sample_auth_user(now);
    let (gateway_url, upstream_hits, gateway_handle, upstream_handle) =
        start_auth_gateway_with_state(user, sample_auth_wallet("user-auth-1", now), []).await;

    let client = reqwest::Client::new();
    for attempt in 1..=10 {
        let response = client
            .post(format!("{gateway_url}/api/auth/login"))
            .header("x-client-device-id", "device-auth-login-rate-limit")
            .header("user-agent", "AetherTest/1.0")
            .json(&json!({
                "email": "missing@example.com",
                "password": "incorrect-password",
                "auth_type": "local",
            }))
            .send()
            .await
            .expect("login request should succeed");
        assert_eq!(
            response.status(),
            StatusCode::UNAUTHORIZED,
            "attempt {attempt} should reach authentication"
        );
    }

    let rejected = client
        .post(format!("{gateway_url}/api/auth/login"))
        .header("x-client-device-id", "device-auth-login-rate-limit")
        .header("user-agent", "AetherTest/1.0")
        .json(&json!({
            "email": "missing@example.com",
            "password": "incorrect-password",
            "auth_type": "local",
        }))
        .send()
        .await
        .expect("rate-limited login request should succeed");

    assert_eq!(rejected.status(), StatusCode::TOO_MANY_REQUESTS);
    let retry_after = rejected
        .headers()
        .get(reqwest::header::RETRY_AFTER)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
        .expect("Retry-After should be a positive integer");
    assert!(retry_after > 0 && retry_after <= 60);
    let payload: serde_json::Value = rejected.json().await.expect("json body should parse");
    assert_eq!(payload["detail"], "请求过于频繁，请稍后重试");
    assert_eq!(payload["retry_after"], retry_after);
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_auth_login_identifier_rate_limit_is_atomic_under_concurrency() {
    let now = Utc::now();
    let user = sample_auth_user(now);
    let (gateway_url, upstream_hits, gateway_handle, upstream_handle) =
        start_auth_gateway_with_state(user, sample_auth_wallet("user-auth-1", now), []).await;

    let client = reqwest::Client::new();
    let mut tasks = Vec::new();
    for _ in 0..12 {
        let client = client.clone();
        let gateway_url = gateway_url.clone();
        tasks.push(tokio::spawn(async move {
            client
                .post(format!("{gateway_url}/api/auth/login"))
                .header("x-client-device-id", "device-auth-login-concurrent")
                .header("user-agent", "AetherTest/1.0")
                .json(&json!({
                    "email": "concurrent-missing@example.com",
                    "password": "incorrect-password",
                    "auth_type": "local",
                }))
                .send()
                .await
                .expect("concurrent login request should succeed")
                .status()
        }));
    }

    let mut unauthorized = 0;
    let mut rate_limited = 0;
    for task in tasks {
        match task.await.expect("login task should complete") {
            StatusCode::UNAUTHORIZED => unauthorized += 1,
            StatusCode::TOO_MANY_REQUESTS => rate_limited += 1,
            status => panic!("unexpected concurrent login status: {status}"),
        }
    }
    assert_eq!(unauthorized, 10);
    assert_eq!(rate_limited, 2);
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}
