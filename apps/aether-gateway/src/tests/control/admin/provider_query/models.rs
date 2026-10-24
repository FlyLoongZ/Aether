use super::*;

#[test]
fn gateway_handles_admin_provider_query_models_fetches_upstream_for_selected_key() {
    run_provider_query_test(
        "gateway_handles_admin_provider_query_models_fetches_upstream_for_selected_key",
        gateway_handles_admin_provider_query_models_fetches_upstream_for_selected_key_impl,
    );
}

async fn gateway_handles_admin_provider_query_models_fetches_upstream_for_selected_key_impl() {
    let execution_runtime_hits = Arc::new(Mutex::new(0usize));
    let execution_runtime_hits_clone = Arc::clone(&execution_runtime_hits);
    let execution_runtime = Router::new().route(
        "/v1/execute/sync",
        any(move |Json(plan): Json<ExecutionPlan>| {
            let execution_runtime_hits_inner = Arc::clone(&execution_runtime_hits_clone);
            async move {
                *execution_runtime_hits_inner
                    .lock()
                    .expect("mutex should lock") += 1;
                assert_eq!(plan.url, "https://api.openai.example/v1/models");
                assert_eq!(
                    plan.headers.get("authorization").map(String::as_str),
                    Some("Bearer sk-test")
                );
                Json(json!({
                    "request_id": "req-provider-query-selected",
                    "status_code": 200,
                    "headers": {
                        "content-type": "application/json"
                    },
                    "body": {
                        "json_body": {
                            "data": [{
                                "id": "LLM-Research/Llama-4-Maverick-17B-128E-Instruct",
                                "object": "",
                                "owned_by": "system",
                                "created": 1732517497u64
                            }]
                        }
                    }
                }))
            }
        }),
    );

    let (execution_runtime_url, execution_runtime_handle) = start_server(execution_runtime).await;
    let mut provider = sample_provider("provider-openai", "OpenAI", 10);
    provider.provider_type = "openai".to_string();
    let provider_catalog_repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![provider],
        vec![StoredProviderCatalogEndpoint::new(
            "endpoint-openai-chat".to_string(),
            "provider-openai".to_string(),
            "openai:chat".to_string(),
            Some("chat".to_string()),
            Some("primary".to_string()),
            true,
        )
        .expect("endpoint should build")
        .with_transport_fields(
            "https://api.openai.example/v1".to_string(),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .expect("endpoint transport should build")],
        vec![sample_bound_key(
            "key-openai-selected",
            "provider-openai",
            "openai:chat",
            "sk-test",
        )],
    ));

    let gateway = build_router_with_state(
        build_state_with_execution_runtime_override(execution_runtime_url)
            .with_data_state_for_tests(GatewayDataState::with_provider_transport_reader_for_tests(
                provider_catalog_repository,
                DEVELOPMENT_ENCRYPTION_KEY.to_string(),
            )),
    );
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .post(format!("{gateway_url}/api/admin/provider-query/models"))
        .header(crate::constants::GATEWAY_HEADER, "rust-phase3b")
        .header(TRUSTED_ADMIN_USER_ID_HEADER, "admin-user-123")
        .header(TRUSTED_ADMIN_USER_ROLE_HEADER, "admin")
        .header(TRUSTED_ADMIN_SESSION_ID_HEADER, "session-123")
        .json(&json!({
            "provider_id": "provider-openai",
            "api_key_id": "key-openai-selected"
        }))
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["success"], json!(true));
    assert_eq!(payload["provider"]["id"], "provider-openai");
    assert_eq!(payload["provider"]["name"], "OpenAI");
    assert_eq!(payload["provider"]["display_name"], "OpenAI");
    assert_eq!(payload["data"]["error"], serde_json::Value::Null);
    assert_eq!(payload["data"]["from_cache"], json!(false));
    assert_eq!(payload["data"]["keys_total"], serde_json::Value::Null);
    let models = payload["data"]["models"]
        .as_array()
        .expect("models should be an array");
    assert_eq!(models.len(), 1);
    assert_eq!(
        models[0]["id"],
        json!("LLM-Research/Llama-4-Maverick-17B-128E-Instruct")
    );
    assert_eq!(models[0]["owned_by"], json!("system"));
    assert_eq!(models[0]["api_formats"], json!(["openai:chat"]));
    assert_eq!(
        *execution_runtime_hits.lock().expect("mutex should lock"),
        1
    );

    gateway_handle.abort();
    execution_runtime_handle.abort();
}

#[test]
fn gateway_handles_admin_provider_query_models_respecting_key_api_formats() {
    run_provider_query_test(
        "gateway_handles_admin_provider_query_models_respecting_key_api_formats",
        gateway_handles_admin_provider_query_models_respecting_key_api_formats_impl,
    );
}

async fn gateway_handles_admin_provider_query_models_respecting_key_api_formats_impl() {
    let execution_runtime_hits = Arc::new(Mutex::new(0usize));
    let execution_runtime_hits_clone = Arc::clone(&execution_runtime_hits);
    let execution_runtime = Router::new().route(
        "/v1/execute/sync",
        any(move |Json(plan): Json<ExecutionPlan>| {
            let execution_runtime_hits_inner = Arc::clone(&execution_runtime_hits_clone);
            async move {
                *execution_runtime_hits_inner
                    .lock()
                    .expect("mutex should lock") += 1;
                assert_eq!(plan.endpoint_id, "endpoint-openai-cli");
                assert_eq!(plan.provider_api_format, "openai:responses");
                Json(json!({
                    "request_id": "req-provider-query-cli",
                    "status_code": 200,
                    "headers": {
                        "content-type": "application/json"
                    },
                    "body": {
                        "json_body": {
                            "data": [{
                                "id": "gpt-5-cli",
                                "object": "model",
                                "owned_by": "system",
                                "created": 1732517497u64
                            }]
                        }
                    }
                }))
            }
        }),
    );

    let (execution_runtime_url, execution_runtime_handle) = start_server(execution_runtime).await;
    let mut provider = sample_provider("provider-openai", "OpenAI", 10);
    provider.provider_type = "openai".to_string();
    let provider_catalog_repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![provider],
        vec![
            StoredProviderCatalogEndpoint::new(
                "endpoint-openai-chat".to_string(),
                "provider-openai".to_string(),
                "openai:chat".to_string(),
                Some("chat".to_string()),
                Some("primary".to_string()),
                true,
            )
            .expect("endpoint should build")
            .with_transport_fields(
                "https://api.openai.example/v1".to_string(),
                None,
                None,
                None,
                None,
                None,
                None,
                None,
            )
            .expect("endpoint transport should build"),
            StoredProviderCatalogEndpoint::new(
                "endpoint-openai-cli".to_string(),
                "provider-openai".to_string(),
                "openai:responses".to_string(),
                Some("cli".to_string()),
                Some("secondary".to_string()),
                true,
            )
            .expect("endpoint should build")
            .with_transport_fields(
                "https://api.openai.example/v1".to_string(),
                None,
                None,
                None,
                None,
                None,
                None,
                None,
            )
            .expect("endpoint transport should build"),
        ],
        vec![sample_bound_key(
            "key-openai-cli",
            "provider-openai",
            "openai:responses",
            "sk-test-cli",
        )],
    ));

    let gateway = build_router_with_state(
        build_state_with_execution_runtime_override(execution_runtime_url)
            .with_data_state_for_tests(GatewayDataState::with_provider_transport_reader_for_tests(
                provider_catalog_repository,
                DEVELOPMENT_ENCRYPTION_KEY.to_string(),
            )),
    );
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .post(format!("{gateway_url}/api/admin/provider-query/models"))
        .header(crate::constants::GATEWAY_HEADER, "rust-phase3b")
        .header(TRUSTED_ADMIN_USER_ID_HEADER, "admin-user-123")
        .header(TRUSTED_ADMIN_USER_ROLE_HEADER, "admin")
        .header(TRUSTED_ADMIN_SESSION_ID_HEADER, "session-123")
        .json(&json!({
            "provider_id": "provider-openai",
            "api_key_id": "key-openai-cli"
        }))
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["success"], json!(true));
    assert_eq!(payload["data"]["error"], serde_json::Value::Null);
    assert_eq!(payload["data"]["from_cache"], json!(false));
    assert_eq!(
        payload["data"]["models"][0]["api_formats"],
        json!(["openai:responses"])
    );
    assert_eq!(
        *execution_runtime_hits.lock().expect("mutex should lock"),
        1
    );

    gateway_handle.abort();
    execution_runtime_handle.abort();
}

#[test]
fn gateway_handles_admin_provider_query_models_aggregating_active_keys() {
    run_provider_query_test(
        "gateway_handles_admin_provider_query_models_aggregating_active_keys",
        gateway_handles_admin_provider_query_models_aggregating_active_keys_impl,
    );
}

async fn gateway_handles_admin_provider_query_models_aggregating_active_keys_impl() {
    let execution_runtime_hits = Arc::new(Mutex::new(0usize));
    let execution_runtime_hits_clone = Arc::clone(&execution_runtime_hits);
    let execution_runtime = Router::new().route(
        "/v1/execute/sync",
        any(move |Json(plan): Json<ExecutionPlan>| {
            let execution_runtime_hits_inner = Arc::clone(&execution_runtime_hits_clone);
            async move {
                *execution_runtime_hits_inner
                    .lock()
                    .expect("mutex should lock") += 1;
                assert_eq!(plan.url, "https://api.openai.example/v1/models");
                let auth = plan
                    .headers
                    .get("authorization")
                    .map(String::as_str)
                    .unwrap_or_default()
                    .to_string();
                let body = if auth == "Bearer sk-test-1" {
                    json!({
                        "data": [{
                            "id": "gpt-5",
                            "api_formats": ["openai:chat"],
                            "object": "model",
                            "owned_by": "system",
                            "created": 1732517497u64
                        }]
                    })
                } else {
                    json!({
                        "data": [{
                            "id": "gpt-4.1",
                            "api_formats": ["openai:chat"],
                            "object": "model",
                            "owned_by": "system",
                            "created": 1732517498u64
                        }]
                    })
                };
                Json(json!({
                    "request_id": format!("req-provider-query-{auth}"),
                    "status_code": 200,
                    "headers": {
                        "content-type": "application/json"
                    },
                    "body": {
                        "json_body": body
                    }
                }))
            }
        }),
    );

    let (execution_runtime_url, execution_runtime_handle) = start_server(execution_runtime).await;
    let mut provider = sample_provider("provider-openai", "OpenAI", 10);
    provider.provider_type = "openai".to_string();
    let provider_catalog_repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![provider],
        vec![StoredProviderCatalogEndpoint::new(
            "endpoint-openai-chat".to_string(),
            "provider-openai".to_string(),
            "openai:chat".to_string(),
            Some("chat".to_string()),
            Some("primary".to_string()),
            true,
        )
        .expect("endpoint should build")
        .with_transport_fields(
            "https://api.openai.example/v1".to_string(),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .expect("endpoint transport should build")],
        vec![
            sample_bound_key(
                "key-openai-1",
                "provider-openai",
                "openai:chat",
                "sk-test-1",
            ),
            sample_bound_key(
                "key-openai-2",
                "provider-openai",
                "openai:chat",
                "sk-test-2",
            ),
        ],
    ));

    let gateway = build_router_with_state(
        build_state_with_execution_runtime_override(execution_runtime_url)
            .with_data_state_for_tests(GatewayDataState::with_provider_transport_reader_for_tests(
                provider_catalog_repository,
                DEVELOPMENT_ENCRYPTION_KEY.to_string(),
            )),
    );
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .post(format!("{gateway_url}/api/admin/provider-query/models"))
        .header(crate::constants::GATEWAY_HEADER, "rust-phase3b")
        .header(TRUSTED_ADMIN_USER_ID_HEADER, "admin-user-123")
        .header(TRUSTED_ADMIN_USER_ROLE_HEADER, "admin")
        .header(TRUSTED_ADMIN_SESSION_ID_HEADER, "session-123")
        .json(&json!({
            "provider_id": "provider-openai"
        }))
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["success"], json!(true));
    assert_eq!(payload["data"]["from_cache"], json!(false));
    assert_eq!(payload["data"]["keys_total"], json!(2));
    assert_eq!(payload["data"]["keys_cached"], json!(0));
    assert_eq!(payload["data"]["keys_fetched"], json!(2));
    let models = payload["data"]["models"]
        .as_array()
        .expect("models should be an array");
    assert_eq!(models.len(), 2);
    let model_ids = models
        .iter()
        .map(|model| model["id"].as_str().expect("id should exist"))
        .collect::<Vec<_>>();
    assert_eq!(model_ids, vec!["gpt-4.1", "gpt-5"]);
    assert_eq!(
        *execution_runtime_hits.lock().expect("mutex should lock"),
        2
    );

    gateway_handle.abort();
    execution_runtime_handle.abort();
}

#[test]
fn gateway_handles_admin_provider_query_models_for_fixed_provider_without_endpoint() {
    run_provider_query_test(
        "gateway_handles_admin_provider_query_models_for_fixed_provider_without_endpoint",
        gateway_handles_admin_provider_query_models_for_fixed_provider_without_endpoint_impl,
    );
}

async fn gateway_handles_admin_provider_query_models_for_fixed_provider_without_endpoint_impl() {
    let execution_runtime_hits = Arc::new(Mutex::new(0usize));
    let execution_runtime_hits_clone = Arc::clone(&execution_runtime_hits);
    let execution_runtime = Router::new().route(
        "/v1/execute/sync",
        any(move |_request: Request| {
            let execution_runtime_hits_inner = Arc::clone(&execution_runtime_hits_clone);
            async move {
                *execution_runtime_hits_inner
                    .lock()
                    .expect("mutex should lock") += 1;
                Json(json!({
                    "request_id": "unexpected",
                    "status_code": 500
                }))
            }
        }),
    );

    let (execution_runtime_url, execution_runtime_handle) = start_server(execution_runtime).await;
    let mut provider = sample_provider("provider-codex", "Codex", 10);
    provider.provider_type = "codex".to_string();
    let provider_catalog_repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![provider],
        vec![],
        vec![sample_bound_key(
            "key-codex-oauth",
            "provider-codex",
            "openai:responses",
            "sk-test-codex",
        )],
    ));

    let gateway = build_router_with_state(
        build_state_with_execution_runtime_override(execution_runtime_url)
            .with_data_state_for_tests(GatewayDataState::with_provider_transport_reader_for_tests(
                provider_catalog_repository,
                DEVELOPMENT_ENCRYPTION_KEY.to_string(),
            )),
    );
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .post(format!("{gateway_url}/api/admin/provider-query/models"))
        .header(crate::constants::GATEWAY_HEADER, "rust-phase3b")
        .header(TRUSTED_ADMIN_USER_ID_HEADER, "admin-user-123")
        .header(TRUSTED_ADMIN_USER_ROLE_HEADER, "admin")
        .header(TRUSTED_ADMIN_SESSION_ID_HEADER, "session-123")
        .json(&json!({
            "provider_id": "provider-codex",
            "api_key_id": "key-codex-oauth"
        }))
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["success"], json!(true));
    assert_eq!(payload["data"]["error"], serde_json::Value::Null);
    assert_eq!(payload["data"]["from_cache"], json!(false));
    let models = payload["data"]["models"]
        .as_array()
        .expect("models should be an array");
    assert!(models.iter().any(|model| model["id"] == "gpt-5.4"));
    assert_eq!(
        *execution_runtime_hits.lock().expect("mutex should lock"),
        0
    );

    gateway_handle.abort();
    execution_runtime_handle.abort();
}

#[test]
fn gateway_maps_admin_provider_model_before_model_list_test_request() {
    run_provider_query_test(
        "gateway_maps_admin_provider_model_before_model_list_test_request",
        gateway_maps_admin_provider_model_before_model_list_test_request_impl,
    );
}

async fn gateway_maps_admin_provider_model_before_model_list_test_request_impl() {
    let execution_runtime = Router::new().route(
        "/v1/execute/sync",
        any(move |Json(plan): Json<ExecutionPlan>| async move {
            assert_eq!(plan.provider_id, "provider-minimax");
            assert_eq!(plan.endpoint_id, "endpoint-minimax-chat");
            assert_eq!(plan.key_id, "key-minimax-primary");
            assert_eq!(plan.provider_api_format, "openai:chat");
            let requested_model = plan
                .model_name
                .as_deref()
                .expect("test plan should carry model name");
            assert!(
                matches!(
                    requested_model,
                    "MiniMax-M2.7-highspeed" | "MiniMax-M2.7-balanced" | "claude-opus-4-6"
                ),
                "unexpected requested model: {requested_model}"
            );
            assert_eq!(
                plan.body
                    .json_body
                    .as_ref()
                    .and_then(|body| body.get("model")),
                Some(&json!(requested_model))
            );
            let response_model = if requested_model.starts_with("MiniMax-M2.7") {
                "MiniMax-M2.7"
            } else {
                requested_model
            };
            Json(json!({
                "request_id": plan.request_id,
                "candidate_id": plan.candidate_id,
                "status_code": 200,
                "headers": {
                    "content-type": "application/json"
                },
                "body": {
                    "json_body": {
                        "id": "chatcmpl-minimax-mapped-model",
                        "object": "chat.completion",
                        "model": response_model,
                        "choices": [{
                            "message": {
                                "role": "assistant",
                                "content": "Hello from MiniMax"
                            }
                        }]
                    }
                },
                "telemetry": {
                    "elapsed_ms": 22
                }
            }))
        }),
    );

    let (execution_runtime_url, execution_runtime_handle) = start_server(execution_runtime).await;
    let mut provider = sample_provider("provider-minimax", "MiniMax", 10);
    provider.provider_type = "custom".to_string();
    let provider_catalog_repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![provider],
        vec![sample_endpoint(
            "endpoint-minimax-chat",
            "provider-minimax",
            "openai:chat",
            "https://api.minimax.example",
        )],
        vec![sample_bound_key(
            "key-minimax-primary",
            "provider-minimax",
            "openai:chat",
            "sk-minimax-primary",
        )],
    ));
    let mut provider_model = sample_admin_provider_model(
        "model-minimax-claude-opus",
        "provider-minimax",
        "global-claude-opus-4-6",
        "claude-opus-4-6",
    );
    provider_model.global_model_name = Some("claude-opus-4-6".to_string());
    provider_model.global_model_display_name = Some("Claude Opus 4.6".to_string());
    provider_model.provider_model_mappings = Some(json!([
        {
            "name": "ignored-anthropic-model",
            "priority": 1,
            "api_formats": ["anthropic:messages"],
        },
        {
            "name": "MiniMax-M2.7-highspeed",
            "priority": 2,
            "api_formats": ["openai:chat"],
            "endpoint_ids": ["endpoint-minimax-chat"],
        },
        {
            "name": "MiniMax-M2.7-balanced",
            "priority": 3,
            "api_formats": ["openai:chat"],
            "endpoint_ids": ["endpoint-minimax-chat"],
        }
    ]));
    let global_model_repository = Arc::new(
        InMemoryGlobalModelReadRepository::seed(Vec::new())
            .with_admin_provider_models(vec![provider_model]),
    );

    let gateway = build_router_with_state(
        build_state_with_execution_runtime_override(execution_runtime_url)
            .with_data_state_for_tests(
                GatewayDataState::with_provider_transport_reader_for_tests(
                    provider_catalog_repository,
                    DEVELOPMENT_ENCRYPTION_KEY.to_string(),
                )
                .with_global_model_repository_for_tests(global_model_repository),
            ),
    );
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .post(format!("{gateway_url}/api/admin/provider-query/test-model"))
        .header(GATEWAY_HEADER, "rust-phase3b")
        .header(TRUSTED_ADMIN_USER_ID_HEADER, "admin-user-123")
        .header(TRUSTED_ADMIN_USER_ROLE_HEADER, "admin")
        .header(TRUSTED_ADMIN_SESSION_ID_HEADER, "session-123")
        .json(&json!({
            "provider_id": "provider-minimax",
            "model": "claude-opus-4-6",
            "api_format": "openai:chat",
            "endpoint_id": "endpoint-minimax-chat"
        }))
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["success"], json!(true));
    assert_eq!(payload["model"], json!("claude-opus-4-6"));
    assert_eq!(
        payload["data"]["response"]["choices"][0]["message"]["content"],
        json!("Hello from MiniMax")
    );
    assert_eq!(
        payload["attempts"][0]["effective_model"],
        json!("MiniMax-M2.7-highspeed")
    );
    assert_eq!(
        payload["attempts"][0]["request_body"]["model"],
        json!("MiniMax-M2.7-highspeed")
    );
    assert_eq!(
        payload["attempts"][0]["response_body"]["model"],
        json!("MiniMax-M2.7")
    );

    let response = reqwest::Client::new()
        .post(format!("{gateway_url}/api/admin/provider-query/test-model"))
        .header(GATEWAY_HEADER, "rust-phase3b")
        .header(TRUSTED_ADMIN_USER_ID_HEADER, "admin-user-123")
        .header(TRUSTED_ADMIN_USER_ROLE_HEADER, "admin")
        .header(TRUSTED_ADMIN_SESSION_ID_HEADER, "session-123")
        .json(&json!({
            "provider_id": "provider-minimax",
            "model": "claude-opus-4-6",
            "api_format": "openai:chat",
            "endpoint_id": "endpoint-minimax-chat",
            "mapped_model_name": "MiniMax-M2.7-balanced",
            "request_body": {
                "model": "stale-model-from-ui",
                "messages": [{
                    "role": "user",
                    "content": "custom prompt"
                }]
            }
        }))
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["success"], json!(true));
    assert_eq!(
        payload["attempts"][0]["effective_model"],
        json!("MiniMax-M2.7-balanced")
    );
    assert_eq!(
        payload["attempts"][0]["request_body"]["model"],
        json!("MiniMax-M2.7-balanced")
    );

    let response = reqwest::Client::new()
        .post(format!("{gateway_url}/api/admin/provider-query/test-model"))
        .header(GATEWAY_HEADER, "rust-phase3b")
        .header(TRUSTED_ADMIN_USER_ID_HEADER, "admin-user-123")
        .header(TRUSTED_ADMIN_USER_ROLE_HEADER, "admin")
        .header(TRUSTED_ADMIN_SESSION_ID_HEADER, "session-123")
        .json(&json!({
            "provider_id": "provider-minimax",
            "model": "claude-opus-4-6",
            "api_format": "openai:chat",
            "endpoint_id": "endpoint-minimax-chat",
            "mapped_model_name": "ignored-anthropic-model"
        }))
        .send()
        .await
        .expect("request should fail");

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let response = reqwest::Client::new()
        .post(format!(
            "{gateway_url}/api/admin/provider-query/test-model-failover"
        ))
        .header(GATEWAY_HEADER, "rust-phase3b")
        .header(TRUSTED_ADMIN_USER_ID_HEADER, "admin-user-123")
        .header(TRUSTED_ADMIN_USER_ROLE_HEADER, "admin")
        .header(TRUSTED_ADMIN_SESSION_ID_HEADER, "session-123")
        .json(&json!({
            "provider_id": "provider-minimax",
            "mode": "global",
            "model": "claude-opus-4-6",
            "failover_models": ["claude-opus-4-6"],
            "api_format": "openai:chat",
            "endpoint_id": "endpoint-minimax-chat",
            "apply_model_mapping": false
        }))
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["success"], json!(true));
    assert_eq!(payload["model"], json!("claude-opus-4-6"));
    assert_eq!(
        payload["attempts"][0]["effective_model"],
        json!("claude-opus-4-6")
    );
    assert_eq!(
        payload["attempts"][0]["request_body"]["model"],
        json!("claude-opus-4-6")
    );
    assert_eq!(
        payload["attempts"][0]["response_body"]["model"],
        json!("claude-opus-4-6")
    );

    gateway_handle.abort();
    execution_runtime_handle.abort();
}
