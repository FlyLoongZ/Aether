use super::*;
use crate::tests::DEV_FERNET_KEY;

#[test]
fn gateway_handles_admin_provider_query_models_with_openai_responses_endpoint() {
    run_provider_query_test(
        "gateway_handles_admin_provider_query_models_with_openai_responses_endpoint",
        gateway_handles_admin_provider_query_models_with_openai_responses_endpoint_impl,
    );
}

async fn gateway_handles_admin_provider_query_models_with_openai_responses_endpoint_impl() {
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
                assert_eq!(plan.endpoint_id, "endpoint-openai-responses");
                assert_eq!(plan.provider_api_format, "openai:responses");
                Json(json!({
                    "request_id": "req-provider-query-responses",
                    "status_code": 200,
                    "headers": {
                        "content-type": "application/json"
                    },
                    "body": {
                        "json_body": {
                            "data": [{
                                "id": "gpt-4.1",
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
        vec![StoredProviderCatalogEndpoint::new(
            "endpoint-openai-responses".to_string(),
            "provider-openai".to_string(),
            "openai:responses".to_string(),
            Some("responses".to_string()),
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
            "key-openai-responses",
            "provider-openai",
            "openai:responses",
            "sk-test-responses",
        )],
    ));

    let gateway = build_router_with_state(
        build_state_with_execution_runtime_override(execution_runtime_url)
            .with_data_state_for_tests(GatewayDataState::with_provider_transport_reader_for_tests(
                provider_catalog_repository,
                DEV_FERNET_KEY.to_string(),
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
            "api_key_id": "key-openai-responses"
        }))
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["success"], json!(true));
    assert_eq!(payload["data"]["error"], serde_json::Value::Null);
    assert_eq!(payload["data"]["from_cache"], json!(false));
    assert_eq!(payload["data"]["models"][0]["id"], json!("gpt-4.1"));
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
fn gateway_recovers_codex_slug_only_models_from_a_stale_legacy_cache() {
    run_provider_query_test(
        "gateway_recovers_codex_slug_only_models_from_a_stale_legacy_cache",
        gateway_recovers_codex_slug_only_models_from_a_stale_legacy_cache_impl,
    );
}

async fn gateway_recovers_codex_slug_only_models_from_a_stale_legacy_cache_impl() {
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
                assert_eq!(
                    plan.url,
                    format!(
                        "https://chatgpt.com/backend-api/codex/models?client_version={}",
                        aether_ai_formats::codex_client_version()
                    )
                );
                assert_eq!(
                    plan.headers.get("user-agent").map(String::as_str),
                    Some(aether_ai_formats::codex_client_user_agent().as_str())
                );
                assert_eq!(plan.provider_api_format, "openai:responses");
                Json(json!({
                    "request_id": "req-provider-query-codex-slug-only",
                    "status_code": 200,
                    "headers": {
                        "content-type": "application/json"
                    },
                    "body": {
                        "json_body": {
                            "models": [{
                                "slug": "gpt-future-dynamic",
                                "display_name": "Future Dynamic",
                                "description": "A model unknown to this Aether build",
                                "model_messages": {
                                    "instructions_template": "Follow the dynamic instructions."
                                },
                                "api_format": "opaque-upstream-protocol",
                                "future_capability": {
                                    "opaque": true,
                                    "schema_version": 7
                                }
                            }]
                        }
                    }
                }))
            }
        }),
    );

    let (execution_runtime_url, execution_runtime_handle) = start_server(execution_runtime).await;
    let mut provider = sample_provider("provider-codex-dynamic", "Codex Dynamic", 10);
    provider.provider_type = "codex".to_string();
    let provider_catalog_repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![provider],
        vec![sample_endpoint(
            "endpoint-codex-dynamic",
            "provider-codex-dynamic",
            "openai:responses",
            "https://chatgpt.com/backend-api/codex",
        )],
        vec![sample_bound_key(
            "key-codex-dynamic",
            "provider-codex-dynamic",
            "openai:responses",
            "codex-dynamic-token",
        )],
    ));

    let state = build_state_with_execution_runtime_override(execution_runtime_url)
        .with_data_state_for_tests(GatewayDataState::with_provider_transport_reader_for_tests(
            provider_catalog_repository,
            DEV_FERNET_KEY.to_string(),
        ));
    state
        .runtime_state()
        .kv_set(
            "upstream_models:provider-codex-dynamic:key-codex-dynamic",
            json!([{"id": "gpt-stale-legacy"}]).to_string(),
            None,
        )
        .await
        .expect("stale legacy cache should seed");
    let cache_state = state.clone();
    let gateway = build_router_with_state(state);
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    for (request_index, expected_from_cache) in [
        (0usize, false),
        (1usize, true),
        (2usize, false),
        (3usize, true),
    ] {
        let response = reqwest::Client::new()
            .post(format!("{gateway_url}/api/admin/provider-query/models"))
            .header(crate::constants::GATEWAY_HEADER, "rust-phase3b")
            .header(TRUSTED_ADMIN_USER_ID_HEADER, "admin-user-123")
            .header(TRUSTED_ADMIN_USER_ROLE_HEADER, "admin")
            .header(TRUSTED_ADMIN_SESSION_ID_HEADER, "session-123")
            .json(&json!({
                "provider_id": "provider-codex-dynamic",
                "api_key_id": "key-codex-dynamic",
                "force_refresh": request_index == 2,
            }))
            .send()
            .await
            .expect("request should succeed");

        assert_eq!(response.status(), StatusCode::OK);
        let payload: serde_json::Value = response.json().await.expect("json body should parse");
        assert_eq!(payload["success"], json!(true));
        assert_eq!(payload["data"]["error"], serde_json::Value::Null);
        assert_eq!(
            payload["data"]["from_cache"],
            json!(expected_from_cache),
            "request {request_index} cache status"
        );
        assert_eq!(
            payload["data"]["models"][0]["id"],
            json!("gpt-future-dynamic")
        );
        assert_eq!(
            payload["data"]["models"][0]["api_formats"],
            json!(["openai:responses"])
        );
        assert_eq!(
            payload["data"]["models"][0]["api_format"],
            json!("opaque-upstream-protocol")
        );
        assert_eq!(
            payload["data"]["models"][0]["model_messages"]["instructions_template"],
            json!("Follow the dynamic instructions.")
        );
        assert_eq!(
            payload["data"]["models"][0]["future_capability"],
            json!({"opaque": true, "schema_version": 7})
        );
        assert_eq!(
            *execution_runtime_hits.lock().expect("mutex should lock"),
            if request_index < 2 { 1 } else { 2 },
            "request {request_index} must not cause another upstream fetch"
        );
        assert_eq!(
            payload["data"]["models"][0]["display_name"],
            if request_index == 1 {
                "Updated shared catalog"
            } else {
                "Future Dynamic"
            }
        );
        if request_index == 0 {
            let context = crate::model_fetch::read_codex_management_catalog(
                &cache_state,
                "provider-codex-dynamic",
                "key-codex-dynamic",
            )
            .await
            .unwrap();
            let mut models = context
                .models
                .clone()
                .expect("admin fetch publishes shared catalog");
            models[0]["display_name"] = json!("Updated shared catalog");
            let transport = cache_state
                .read_provider_transport_snapshot(
                    "provider-codex-dynamic",
                    "endpoint-codex-dynamic",
                    "key-codex-dynamic",
                )
                .await
                .unwrap()
                .unwrap();
            crate::model_fetch::store_codex_management_catalog(
                &cache_state,
                &context,
                &[transport],
                models,
                None,
            )
            .await;
            <AppState as crate::model_fetch::ModelFetchRuntimeState>::write_upstream_models_cache(
                &cache_state,
                "provider-codex-dynamic",
                "key-codex-dynamic",
                &[],
            )
            .await;
        }
    }

    gateway_handle.abort();
    execution_runtime_handle.abort();
}

#[test]
fn gateway_handles_admin_provider_query_models_falls_back_to_codex_preset_when_token_invalidated() {
    run_provider_query_test(
        "gateway_handles_admin_provider_query_models_falls_back_to_codex_preset_when_token_invalidated",
        gateway_handles_admin_provider_query_models_falls_back_to_codex_preset_when_token_invalidated_impl,
    );
}

async fn gateway_handles_admin_provider_query_models_falls_back_to_codex_preset_when_token_invalidated_impl(
) {
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
                assert_eq!(
                    plan.url,
                    format!(
                        "https://chatgpt.com/backend-api/codex/models?client_version={}",
                        aether_ai_formats::codex_client_version()
                    )
                );
                Json(json!({
                    "request_id": "req-provider-query-codex-invalidated",
                    "status_code": 403,
                    "headers": {
                        "content-type": "application/json"
                    },
                    "body": {
                        "json_body": {
                            "error": {
                                "message": "Your authentication token has been invalidated. Please sign in again."
                            }
                        }
                    }
                }))
            }
        }),
    );

    let (execution_runtime_url, execution_runtime_handle) = start_server(execution_runtime).await;
    let mut provider = sample_provider("provider-codex", "Codex", 10);
    provider.provider_type = "codex".to_string();
    let provider_catalog_repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![provider],
        vec![sample_endpoint(
            "endpoint-codex-responses",
            "provider-codex",
            "openai:responses",
            "https://chatgpt.com/backend-api/codex",
        )],
        vec![sample_bound_key(
            "key-codex-invalidated",
            "provider-codex",
            "openai:responses",
            "invalidated-token",
        )],
    ));

    let gateway = build_router_with_state(
        build_state_with_execution_runtime_override(execution_runtime_url)
            .with_data_state_for_tests(GatewayDataState::with_provider_transport_reader_for_tests(
                provider_catalog_repository,
                DEV_FERNET_KEY.to_string(),
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
            "api_key_id": "key-codex-invalidated"
        }))
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["success"], json!(true));
    assert_eq!(payload["data"]["error"], serde_json::Value::Null);
    let warning = payload["data"]["warning"]
        .as_str()
        .expect("Codex fallback warning should be present");
    assert!(warning.contains("Codex 动态模型目录不可用"));
    // Model-fetch diagnostics are intentionally projected to a credential-safe
    // category before being returned from the admin endpoint.  The raw
    // upstream invalidation text must not cross the response boundary.
    assert!(
        warning.contains("authorization failed (status 403)"),
        "warning={warning}"
    );
    let model_ids = payload["data"]["models"]
        .as_array()
        .expect("models should be an array")
        .iter()
        .map(|model| model["id"].as_str().expect("model id"))
        .collect::<Vec<_>>();
    assert_eq!(
        model_ids,
        vec![
            "codex-auto-review",
            "gpt-5.2",
            "gpt-5.4",
            "gpt-5.4-mini",
            "gpt-5.5",
            "gpt-5.6-luna",
            "gpt-5.6-sol",
            "gpt-5.6-terra",
            "gpt-6-astra",
            "gpt-6-luna",
            "gpt-6-sol",
            "gpt-6.1-sol",
            "gpt-daybreak-blue-latest",
            "gpt-daybreak-red-latest",
        ]
    );
    assert_eq!(
        *execution_runtime_hits.lock().expect("mutex should lock"),
        1
    );

    gateway_handle.abort();
    execution_runtime_handle.abort();
}

#[test]
fn gateway_streams_codex_openai_responses_upstream_for_admin_pool_model_test() {
    run_provider_query_test(
        "gateway_streams_codex_openai_responses_upstream_for_admin_pool_model_test",
        gateway_streams_codex_openai_responses_upstream_for_admin_pool_model_test_impl,
    );
}

async fn gateway_streams_codex_openai_responses_upstream_for_admin_pool_model_test_impl() {
    let execution_runtime = Router::new().route(
        "/v1/execute/sync",
        any(move |Json(plan): Json<ExecutionPlan>| async move {
            assert_eq!(plan.provider_id, "provider-codex");
            assert_eq!(plan.endpoint_id, "endpoint-codex-responses");
            assert_eq!(plan.key_id, "key-codex-primary");
            assert_eq!(plan.provider_api_format, "openai:responses");
            assert_eq!(plan.model_name.as_deref(), Some("gpt-5.3-codex-spark"));
            assert!(plan.stream, "Codex openai:responses must stream upstream");
            assert_eq!(
                plan.body
                    .json_body
                    .as_ref()
                    .and_then(|body| body.get("stream")),
                Some(&json!(true))
            );
            Json(json!({
                "request_id": plan.request_id,
                "candidate_id": plan.candidate_id,
                "status_code": 200,
                "headers": {
                    "content-type": "application/json"
                },
                "body": {
                    "json_body": {
                        "id": "resp-codex-model-test",
                        "model": "gpt-5.3-codex-spark",
                        "output_text": "ok"
                    }
                },
                "telemetry": {
                    "elapsed_ms": 18
                }
            }))
        }),
    );

    let (execution_runtime_url, execution_runtime_handle) = start_server(execution_runtime).await;
    let mut provider = sample_provider("provider-codex", "Codex", 10);
    provider.provider_type = "codex".to_string();
    let mut endpoint = sample_endpoint(
        "endpoint-codex-responses",
        "provider-codex",
        "openai:responses",
        "https://chatgpt.com/backend-api/codex",
    );
    endpoint.config = Some(json!({"upstream_stream_policy": "force_stream"}));
    let provider_catalog_repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![provider],
        vec![endpoint],
        vec![sample_bound_key(
            "key-codex-primary",
            "provider-codex",
            "openai:responses",
            "sk-codex-primary",
        )],
    ));

    let gateway = build_router_with_state(
        build_state_with_execution_runtime_override(execution_runtime_url)
            .with_data_state_for_tests(GatewayDataState::with_provider_transport_reader_for_tests(
                provider_catalog_repository,
                DEV_FERNET_KEY.to_string(),
            )),
    );
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .post(format!(
            "{gateway_url}/api/admin/provider-query/test-model-failover"
        ))
        .header(GATEWAY_HEADER, "rust-phase3b")
        .header(TRUSTED_ADMIN_USER_ID_HEADER, "admin-user-123")
        .header(TRUSTED_ADMIN_USER_ROLE_HEADER, "admin")
        .header(TRUSTED_ADMIN_SESSION_ID_HEADER, "session-123")
        .json(&json!({
            "provider_id": "provider-codex",
            "mode": "pool",
            "model": "gpt-5.3-codex-spark",
            "failover_models": ["gpt-5.3-codex-spark"],
            "api_format": "openai:responses",
            "endpoint_id": "endpoint-codex-responses",
            "request_body": {
                "model": "gpt-5.3-codex-spark",
                "input": "hello",
                "stream": true
            }
        }))
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["success"], json!(true));
    assert_eq!(
        payload["attempts"][0]["request_body"]["stream"],
        json!(true)
    );

    gateway_handle.abort();
    execution_runtime_handle.abort();
}

#[test]
fn gateway_executes_codex_search_admin_pool_model_test_with_search_contract() {
    run_provider_query_test(
        "gateway_executes_codex_search_admin_pool_model_test_with_search_contract",
        gateway_executes_codex_search_admin_pool_model_test_with_search_contract_impl,
    );
}

async fn gateway_executes_codex_search_admin_pool_model_test_with_search_contract_impl() {
    let execution_runtime = Router::new().route(
        "/v1/execute/sync",
        any(move |Json(plan): Json<ExecutionPlan>| async move {
            assert_eq!(plan.provider_id, "provider-codex-search");
            assert_eq!(plan.endpoint_id, "endpoint-codex-search");
            assert_eq!(plan.key_id, "key-codex-search");
            assert_eq!(plan.client_api_format, "openai:search");
            assert_eq!(plan.provider_api_format, "openai:search");
            assert_eq!(
                plan.url,
                "https://chatgpt.com/backend-api/codex/alpha/search"
            );
            assert_eq!(plan.model_name.as_deref(), Some("gpt-5.6-sol"));
            assert!(!plan.stream, "Codex Search is a synchronous JSON protocol");
            assert_eq!(
                plan.timeouts
                    .as_ref()
                    .and_then(|timeouts| timeouts.total_ms),
                Some(900_000)
            );
            assert_eq!(
                plan.headers.get("authorization").map(String::as_str),
                Some("Bearer codex-search-access-token")
            );
            assert_eq!(
                plan.headers.get("chatgpt-account-id").map(String::as_str),
                Some("account-search-admin")
            );
            assert_eq!(
                plan.headers.get("x-openai-fedramp").map(String::as_str),
                Some("true")
            );
            assert_eq!(
                plan.headers.get("originator").map(String::as_str),
                Some("codex_cli_rs")
            );
            assert!(plan
                .headers
                .get("user-agent")
                .is_some_and(|value| value.starts_with("codex_cli_rs/")));
            assert!(!plan.headers.contains_key("openai-beta"));
            assert!(!plan
                .headers
                .contains_key("x-openai-internal-codex-responses-lite"));
            assert_ne!(
                plan.headers.get("accept").map(String::as_str),
                Some("text/event-stream")
            );

            let body = plan.body.json_body.as_ref().expect("search json body");
            assert_eq!(
                body["id"],
                json!("aether-model-test-provider-query-search-trace")
            );
            assert_eq!(body["model"], json!("gpt-5.6-sol"));
            assert_eq!(body["input"], json!("find current OpenAI documentation"));
            assert_eq!(
                body["commands"]["search_query"][0]["q"],
                json!("OpenAI Codex Search")
            );
            assert!(body.get("stream").is_none());
            assert!(body.get("store").is_none());
            assert!(body.get("service_tier").is_none());
            assert!(body.get("unknown_field").is_none());

            Json(json!({
                "request_id": plan.request_id,
                "candidate_id": plan.candidate_id,
                "status_code": 200,
                "headers": {
                    "content-type": "application/json"
                },
                "body": {
                    "json_body": {
                        "output": "search result"
                    }
                },
                "telemetry": {
                    "elapsed_ms": 21
                }
            }))
        }),
    );

    let (execution_runtime_url, execution_runtime_handle) = start_server(execution_runtime).await;
    let mut provider = sample_provider("provider-codex-search", "Codex Search", 10);
    provider.provider_type = "codex".to_string();
    provider.request_timeout_secs = Some(900.0);
    let mut endpoint = sample_endpoint(
        "endpoint-codex-search",
        "provider-codex-search",
        "openai:search",
        "https://chatgpt.com/backend-api/codex",
    );
    endpoint.config = Some(json!({"upstream_stream_policy": "force_stream"}));
    let mut key = sample_bound_key(
        "key-codex-search",
        "provider-codex-search",
        "openai:search",
        "codex-search-access-token",
    );
    key.auth_type = "oauth".to_string();
    key.encrypted_auth_config = Some(sample_bound_auth_config(
        "provider-codex-search",
        "key-codex-search",
        r#"{"provider_type":"codex","account_id":"account-search-admin","is_fedramp":true}"#,
    ));
    let provider_catalog_repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![provider],
        vec![endpoint],
        vec![key],
    ));

    let gateway = build_router_with_state(
        build_state_with_execution_runtime_override(execution_runtime_url)
            .with_data_state_for_tests(GatewayDataState::with_provider_transport_reader_for_tests(
                provider_catalog_repository,
                DEV_FERNET_KEY.to_string(),
            )),
    );
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .post(format!(
            "{gateway_url}/api/admin/provider-query/test-model-failover"
        ))
        .header(GATEWAY_HEADER, "rust-phase3b")
        .header(TRUSTED_ADMIN_USER_ID_HEADER, "admin-user-123")
        .header(TRUSTED_ADMIN_USER_ROLE_HEADER, "admin")
        .header(TRUSTED_ADMIN_SESSION_ID_HEADER, "session-123")
        .json(&json!({
            "provider_id": "provider-codex-search",
            "mode": "pool",
            "model": "gpt-5.6-sol",
            "failover_models": ["gpt-5.6-sol"],
            "api_format": "openai:search",
            "endpoint_id": "endpoint-codex-search",
            "request_id": "provider-query-search-trace",
            "request_body": {
                "model": "gpt-5.6-sol",
                "input": "find current OpenAI documentation",
                "commands": {
                    "search_query": [{"q": "OpenAI Codex Search"}]
                },
                "max_output_tokens": 256,
                "stream": true,
                "store": false,
                "service_tier": "priority",
                "unknown_field": true
            }
        }))
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["success"], json!(true), "payload={payload}");
    assert_eq!(
        payload["attempts"][0]["request_body"]["id"],
        json!("aether-model-test-provider-query-search-trace")
    );
    assert_eq!(
        payload["attempts"][0]["response_body"]["output"],
        json!("search result")
    );

    gateway_handle.abort();
    execution_runtime_handle.abort();
}

#[test]
fn gateway_uses_pool_scheduler_order_for_admin_pool_model_test() {
    run_provider_query_test(
        "gateway_uses_pool_scheduler_order_for_admin_pool_model_test",
        gateway_uses_pool_scheduler_order_for_admin_pool_model_test_impl,
    );
}

async fn gateway_uses_pool_scheduler_order_for_admin_pool_model_test_impl() {
    let execution_runtime = Router::new().route(
        "/v1/execute/sync",
        any(move |Json(plan): Json<ExecutionPlan>| async move {
            assert_eq!(plan.key_id, "key-codex-plus");
            Json(json!({
                "request_id": plan.request_id,
                "candidate_id": plan.candidate_id,
                "status_code": 200,
                "headers": {
                    "content-type": "application/json"
                },
                "body": {
                    "json_body": {
                        "id": "resp-codex-plus",
                        "model": "gpt-5.4-mini",
                        "output_text": "ok"
                    }
                },
                "telemetry": {
                    "elapsed_ms": 21
                }
            }))
        }),
    );

    let (execution_runtime_url, execution_runtime_handle) = start_server(execution_runtime).await;
    let mut provider = sample_provider("provider-codex", "Codex", 10);
    provider.provider_type = "codex".to_string();
    provider.config = Some(json!({
        "pool_advanced": {
            "scheduling_presets": [
                {"preset": "plus_first", "enabled": true}
            ]
        }
    }));
    let mut free_key = sample_bound_key(
        "key-codex-free",
        "provider-codex",
        "openai:responses",
        "sk-codex-free",
    );
    free_key.internal_priority = 0;
    free_key.status_snapshot = Some(json!({
        "quota": {
            "provider_type": "codex",
            "code": "limited",
            "plan_type": "free",
            "usage_ratio": 0.1
        }
    }));
    let mut plus_key = sample_bound_key(
        "key-codex-plus",
        "provider-codex",
        "openai:responses",
        "sk-codex-plus",
    );
    plus_key.internal_priority = 100;
    plus_key.status_snapshot = Some(json!({
        "quota": {
            "provider_type": "codex",
            "code": "limited",
            "plan_type": "plus",
            "usage_ratio": 0.1
        }
    }));
    let provider_catalog_repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![provider],
        vec![sample_endpoint(
            "endpoint-codex-responses",
            "provider-codex",
            "openai:responses",
            "https://chatgpt.com/backend-api/codex",
        )],
        vec![free_key, plus_key],
    ));
    let request_candidate_repository = Arc::new(InMemoryRequestCandidateRepository::default());

    let gateway = build_router_with_state(
        build_state_with_execution_runtime_override(execution_runtime_url)
            .with_data_state_for_tests(
                GatewayDataState::with_request_candidate_repository_for_tests(
                    request_candidate_repository.clone(),
                )
                .with_encryption_key_for_tests(DEV_FERNET_KEY.to_string())
                .attach_provider_catalog_repository_for_tests(provider_catalog_repository),
            ),
    );
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .post(format!(
            "{gateway_url}/api/admin/provider-query/test-model-failover"
        ))
        .header(GATEWAY_HEADER, "rust-phase3b")
        .header(TRUSTED_ADMIN_USER_ID_HEADER, "admin-user-123")
        .header(TRUSTED_ADMIN_USER_ROLE_HEADER, "admin")
        .header(TRUSTED_ADMIN_SESSION_ID_HEADER, "session-123")
        .json(&json!({
            "provider_id": "provider-codex",
            "mode": "pool",
            "model": "gpt-5.4-mini",
            "failover_models": ["gpt-5.4-mini"],
            "api_format": "openai:responses",
            "endpoint_id": "endpoint-codex-responses",
            "request_id": "provider-query-model-test-trace-123",
            "request_body": {
                "model": "gpt-5.4-mini",
                "input": "hello",
                "stream": true
            }
        }))
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["success"], json!(true));
    assert_eq!(payload["attempts"][0]["key_id"], json!("key-codex-plus"));
    assert_eq!(
        payload["candidate_summary"]["winning_key_id"],
        json!("key-codex-plus")
    );
    assert_eq!(payload["candidate_summary"]["total_candidates"], json!(2));
    assert_eq!(payload["candidate_summary"]["attempted"], json!(1));
    assert_eq!(payload["candidate_summary"]["unused"], json!(1));
    let stored_candidates = request_candidate_repository
        .list_by_request_id("provider-query-model-test-trace-123")
        .await
        .expect("model-test trace candidates should read");
    assert_eq!(stored_candidates.len(), 2);
    assert_eq!(
        stored_candidates[0].key_id.as_deref(),
        Some("key-codex-plus")
    );
    assert_eq!(stored_candidates[0].status, RequestCandidateStatus::Success);
    assert_eq!(
        stored_candidates[1].key_id.as_deref(),
        Some("key-codex-free")
    );
    assert_eq!(stored_candidates[1].status, RequestCandidateStatus::Unused);

    gateway_handle.abort();
    execution_runtime_handle.abort();
}

#[test]
fn gateway_handles_admin_provider_query_test_model_for_kiro_locally() {
    run_provider_query_test(
        "gateway_handles_admin_provider_query_test_model_for_kiro_locally",
        gateway_handles_admin_provider_query_test_model_for_kiro_locally_impl,
    );
}

async fn gateway_handles_admin_provider_query_test_model_for_kiro_locally_impl() {
    let execution_runtime = Router::new().route(
        "/v1/execute/sync",
        any(move |Json(plan): Json<ExecutionPlan>| async move {
            assert_eq!(plan.provider_id, "provider-kiro");
            assert_eq!(plan.endpoint_id, "endpoint-kiro-cli");
            assert_eq!(plan.key_id, "key-kiro-primary");
            assert_eq!(plan.provider_api_format, "claude:messages");
            assert_eq!(plan.model_name.as_deref(), Some("claude-sonnet-4-upstream"));
            Json(json!({
                "request_id": plan.request_id,
                "candidate_id": plan.candidate_id,
                "status_code": 200,
                "headers": {
                    "content-type": "application/vnd.amazon.eventstream"
                },
                "body": {
                    "body_bytes_b64": base64::engine::general_purpose::STANDARD.encode(
                        [
                            encode_kiro_event_frame("assistantResponseEvent", json!({"content": "Hello from Kiro"})),
                            encode_kiro_exception_frame("ContentLengthExceededException"),
                        ]
                        .concat()
                    )
                },
                "telemetry": {
                    "elapsed_ms": 42
                }
            }))
        }),
    );

    let (execution_runtime_url, execution_runtime_handle) = start_server(execution_runtime).await;
    let mut provider = sample_provider("provider-kiro", "Kiro", 10);
    provider.provider_type = "kiro".to_string();
    let mut key = sample_bound_key(
        "key-kiro-primary",
        "provider-kiro",
        "claude:messages",
        "__placeholder__",
    );
    key.auth_type = "oauth".to_string();
    key.encrypted_auth_config = Some(sample_bound_auth_config(
        "provider-kiro",
        "key-kiro-primary",
        r#"{
                "provider_type":"kiro",
                "auth_method":"idc",
                "access_token":"cached-kiro-token",
                "expires_at":4102444800,
                "refresh_token":"rrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrr",
                "machine_id":"123e4567-e89b-12d3-a456-426614174000",
                "api_region":"us-east-1",
                "client_id":"client-id",
                "client_secret":"client-secret"
            }"#,
    ));

    let provider_catalog_repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![provider],
        vec![StoredProviderCatalogEndpoint::new(
            "endpoint-kiro-cli".to_string(),
            "provider-kiro".to_string(),
            "claude:messages".to_string(),
            Some("claude".to_string()),
            Some("messages".to_string()),
            true,
        )
        .expect("endpoint should build")
        .with_transport_fields(
            "https://q.{region}.amazonaws.com".to_string(),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .expect("endpoint transport should build")],
        vec![key],
    ));

    let gateway = build_router_with_state(
        build_state_with_execution_runtime_override(execution_runtime_url)
            .with_data_state_for_tests(GatewayDataState::with_provider_transport_reader_for_tests(
                provider_catalog_repository,
                DEV_FERNET_KEY.to_string(),
            )),
    );
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .post(format!("{gateway_url}/api/admin/provider-query/test-model"))
        .header(GATEWAY_HEADER, "rust-phase3b")
        .header(TRUSTED_ADMIN_USER_ID_HEADER, "admin-user-123")
        .header(TRUSTED_ADMIN_USER_ROLE_HEADER, "admin")
        .header(TRUSTED_ADMIN_SESSION_ID_HEADER, "session-123")
        .json(&json!({
            "provider_id": "provider-kiro",
            "model_name": "claude-sonnet-4-upstream",
            "api_format": "claude:messages"
        }))
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["success"], json!(true));
    assert_eq!(payload["provider"]["id"], json!("provider-kiro"));
    assert_eq!(payload["model"], json!("claude-sonnet-4-upstream"));
    assert_eq!(
        payload["data"]["response"]["content"][0]["text"],
        json!("Hello from Kiro")
    );

    gateway_handle.abort();
    execution_runtime_handle.abort();
}

#[test]
fn gateway_uses_kiro_mapped_model_name_for_explicit_model_mapping_test() {
    run_provider_query_test(
        "gateway_uses_kiro_mapped_model_name_for_explicit_model_mapping_test",
        gateway_uses_kiro_mapped_model_name_for_explicit_model_mapping_test_impl,
    );
}

async fn gateway_uses_kiro_mapped_model_name_for_explicit_model_mapping_test_impl() {
    let execution_runtime = Router::new().route(
        "/v1/execute/sync",
        any(move |Json(plan): Json<ExecutionPlan>| async move {
            assert_eq!(plan.provider_id, "provider-kiro");
            assert_eq!(plan.endpoint_id, "endpoint-kiro-cli");
            assert_eq!(plan.key_id, "key-kiro-primary");
            assert_eq!(plan.provider_api_format, "claude:messages");
            assert_eq!(plan.model_name.as_deref(), Some("claude-haiku-4.5"));
            let body = plan.body.json_body.as_ref().expect("json body");
            assert_eq!(
                body.pointer("/conversationState/currentMessage/userInputMessage/modelId"),
                Some(&json!("claude-haiku-4.5"))
            );
            Json(json!({
                "request_id": plan.request_id,
                "candidate_id": plan.candidate_id,
                "status_code": 200,
                "headers": {
                    "content-type": "application/vnd.amazon.eventstream"
                },
                "body": {
                    "body_bytes_b64": base64::engine::general_purpose::STANDARD.encode(
                        [
                            encode_kiro_event_frame("assistantResponseEvent", json!({"content": "Hello from Kiro mapped model"})),
                            encode_kiro_exception_frame("ContentLengthExceededException"),
                        ]
                        .concat()
                    )
                },
                "telemetry": {
                    "elapsed_ms": 42
                }
            }))
        }),
    );

    let (execution_runtime_url, execution_runtime_handle) = start_server(execution_runtime).await;
    let mut provider = sample_provider("provider-kiro", "Kiro", 10);
    provider.provider_type = "kiro".to_string();
    let mut key = sample_bound_key(
        "key-kiro-primary",
        "provider-kiro",
        "claude:messages",
        "__placeholder__",
    );
    key.auth_type = "oauth".to_string();
    key.encrypted_auth_config = Some(sample_bound_auth_config(
        "provider-kiro",
        "key-kiro-primary",
        r#"{
                "provider_type":"kiro",
                "auth_method":"idc",
                "access_token":"cached-kiro-token",
                "expires_at":4102444800,
                "refresh_token":"rrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrr",
                "machine_id":"123e4567-e89b-12d3-a456-426614174000",
                "api_region":"us-east-1",
                "client_id":"client-id",
                "client_secret":"client-secret"
            }"#,
    ));

    let provider_catalog_repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![provider],
        vec![StoredProviderCatalogEndpoint::new(
            "endpoint-kiro-cli".to_string(),
            "provider-kiro".to_string(),
            "claude:messages".to_string(),
            Some("claude".to_string()),
            Some("messages".to_string()),
            true,
        )
        .expect("endpoint should build")
        .with_transport_fields(
            "https://q.{region}.amazonaws.com".to_string(),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .expect("endpoint transport should build")],
        vec![key],
    ));
    let mut provider_model = sample_admin_provider_model(
        "model-kiro-haiku",
        "provider-kiro",
        "global-kiro-haiku",
        "claude-haiku-4-5-20251001",
    );
    provider_model.provider_model_mappings = Some(json!([{
        "name": "claude-haiku-4.5",
        "priority": 1,
        "api_formats": ["claude:messages"],
        "endpoint_ids": ["endpoint-kiro-cli"]
    }]));
    let global_model_repository = Arc::new(
        InMemoryGlobalModelReadRepository::seed(Vec::new())
            .with_admin_provider_models(vec![provider_model]),
    );

    let gateway = build_router_with_state(
        build_state_with_execution_runtime_override(execution_runtime_url)
            .with_data_state_for_tests(
                GatewayDataState::with_provider_transport_reader_for_tests(
                    provider_catalog_repository,
                    DEV_FERNET_KEY.to_string(),
                )
                .with_global_model_repository_for_tests(global_model_repository),
            ),
    );
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .post(format!(
            "{gateway_url}/api/admin/provider-query/test-model-failover"
        ))
        .header(GATEWAY_HEADER, "rust-phase3b")
        .header(TRUSTED_ADMIN_USER_ID_HEADER, "admin-user-123")
        .header(TRUSTED_ADMIN_USER_ROLE_HEADER, "admin")
        .header(TRUSTED_ADMIN_SESSION_ID_HEADER, "session-123")
        .json(&json!({
            "provider_id": "provider-kiro",
            "mode": "pool",
            "model_name": "claude-haiku-4-5-20251001",
            "failover_models": ["claude-haiku-4-5-20251001"],
            "mapped_model_name": "claude-haiku-4.5",
            "api_format": "claude:messages",
            "endpoint_id": "endpoint-kiro-cli",
            "request_id": "provider-test-kiro-mapped"
        }))
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["success"], json!(true));
    assert_eq!(
        payload["attempts"][0]["effective_model"],
        json!("claude-haiku-4.5")
    );
    assert_eq!(
        payload["attempts"][0]["request_body"]
            .pointer("/conversationState/currentMessage/userInputMessage/modelId"),
        Some(&json!("claude-haiku-4.5"))
    );
    assert_eq!(
        payload["data"]["response"]["content"][0]["text"],
        json!("Hello from Kiro mapped model")
    );

    let direct_response = reqwest::Client::new()
        .post(format!("{gateway_url}/api/admin/provider-query/test-model"))
        .header(GATEWAY_HEADER, "rust-phase3b")
        .header(TRUSTED_ADMIN_USER_ID_HEADER, "admin-user-123")
        .header(TRUSTED_ADMIN_USER_ROLE_HEADER, "admin")
        .header(TRUSTED_ADMIN_SESSION_ID_HEADER, "session-123")
        .json(&json!({
            "provider_id": "provider-kiro",
            "mode": "direct",
            "model_name": "claude-haiku-4-5-20251001",
            "mapped_model_name": "claude-haiku-4.5",
            "api_format": "claude:messages",
            "endpoint_id": "endpoint-kiro-cli",
            "request_id": "provider-test-kiro-mapped-direct"
        }))
        .send()
        .await
        .expect("direct request should succeed");

    assert_eq!(direct_response.status(), StatusCode::OK);
    let direct_payload: serde_json::Value = direct_response
        .json()
        .await
        .expect("json body should parse");
    assert_eq!(direct_payload["success"], json!(true));
    assert_eq!(
        direct_payload["data"]["response"]["content"][0]["text"],
        json!("Hello from Kiro mapped model")
    );

    gateway_handle.abort();
    execution_runtime_handle.abort();
}

#[test]
fn gateway_handles_openai_image_test_model_locally() {
    run_provider_query_test(
        "gateway_handles_openai_image_test_model_locally",
        gateway_handles_openai_image_test_model_locally_impl,
    );
}

async fn gateway_handles_openai_image_test_model_locally_impl() {
    let execution_runtime = Router::new().route(
        "/v1/execute/sync",
        any(move |Json(plan): Json<ExecutionPlan>| async move {
            assert_eq!(plan.provider_id, "provider-openai");
            assert_eq!(plan.endpoint_id, "endpoint-openai-image");
            assert_eq!(plan.key_id, "key-openai-image");
            assert_eq!(plan.client_api_format, "openai:image");
            assert_eq!(plan.provider_api_format, "openai:image");
            assert_eq!(plan.model_name.as_deref(), Some("gpt-image-1"));
            assert_eq!(plan.url, "https://api.openai.example/v1/images/generations");
            assert!(!plan.stream);
            assert_eq!(
                plan.headers.get("authorization").map(String::as_str),
                Some("Bearer sk-test-image")
            );
            assert_eq!(
                plan.body
                    .json_body
                    .as_ref()
                    .and_then(|body| body.get("model")),
                Some(&json!("gpt-image-1"))
            );
            assert_eq!(
                plan.body
                    .json_body
                    .as_ref()
                    .and_then(|body| body.get("prompt"))
                    .and_then(|value| value.as_str()),
                Some("Draw a small blue square")
            );
            assert!(plan
                .body
                .json_body
                .as_ref()
                .is_some_and(|body| body.get("stream").is_none()));
            Json(json!({
                "request_id": plan.request_id,
                "candidate_id": plan.candidate_id,
                "status_code": 200,
                "headers": {
                    "content-type": "application/json"
                },
                "body": {
                    "json_body": {
                        "created": 1776839946,
                        "model": "gpt-image-1",
                        "data": [{
                            "b64_json": "aGVsbG8=",
                            "revised_prompt": "revised prompt"
                        }],
                        "usage": {
                            "input_tokens": 171,
                            "output_tokens": 1372,
                            "total_tokens": 1543
                        }
                    }
                },
                "telemetry": {
                    "elapsed_ms": 19
                }
            }))
        }),
    );

    let (execution_runtime_url, execution_runtime_handle) = start_server(execution_runtime).await;
    let mut provider = sample_provider("provider-openai", "OpenAI", 10);
    provider.provider_type = "codex".to_string();
    let provider_catalog_repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![provider],
        vec![sample_endpoint(
            "endpoint-openai-image",
            "provider-openai",
            "openai:image",
            "https://api.openai.example/v1",
        )],
        vec![sample_bound_key(
            "key-openai-image",
            "provider-openai",
            "openai:image",
            "sk-test-image",
        )],
    ));

    let gateway = build_router_with_state(
        build_state_with_execution_runtime_override(execution_runtime_url)
            .with_data_state_for_tests(GatewayDataState::with_provider_transport_reader_for_tests(
                provider_catalog_repository,
                DEV_FERNET_KEY.to_string(),
            )),
    );
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .post(format!("{gateway_url}/api/admin/provider-query/test-model"))
        .header(GATEWAY_HEADER, "rust-phase3b")
        .header(TRUSTED_ADMIN_USER_ID_HEADER, "admin-user-123")
        .header(TRUSTED_ADMIN_USER_ROLE_HEADER, "admin")
        .header(TRUSTED_ADMIN_SESSION_ID_HEADER, "session-123")
        .json(&json!({
            "provider_id": "provider-openai",
            "model": "gpt-image-1",
            "api_format": "openai:image",
            "message": "Draw a small blue square"
        }))
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["success"], json!(true));
    assert_eq!(
        payload["data"]["response"]["data"][0]["b64_json"],
        json!("aGVsbG8=")
    );

    gateway_handle.abort();
    execution_runtime_handle.abort();
}

#[test]
fn gateway_reports_transport_unsupported_reason_for_non_kiro_provider() {
    run_provider_query_test(
        "gateway_reports_transport_unsupported_reason_for_non_kiro_provider",
        gateway_reports_transport_unsupported_reason_for_non_kiro_provider_impl,
    );
}

async fn gateway_reports_transport_unsupported_reason_for_non_kiro_provider_impl() {
    let mut provider = sample_provider("provider-antigravity", "Antigravity", 10);
    provider.provider_type = "antigravity".to_string();
    let provider_catalog_repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![provider],
        vec![sample_endpoint(
            "endpoint-antigravity-gemini",
            "provider-antigravity",
            "gemini:generate_content",
            "https://cloudcode-pa.googleapis.com",
        )],
        vec![sample_bound_key(
            "key-antigravity-gemini",
            "provider-antigravity",
            "gemini:generate_content",
            "sk-test-antigravity",
        )],
    ));

    let gateway = build_router_with_state(
        AppState::new()
            .expect("gateway should build")
            .with_data_state_for_tests(GatewayDataState::with_provider_transport_reader_for_tests(
                provider_catalog_repository,
                DEV_FERNET_KEY.to_string(),
            )),
    );
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .post(format!("{gateway_url}/api/admin/provider-query/test-model"))
        .header(GATEWAY_HEADER, "rust-phase3b")
        .header(TRUSTED_ADMIN_USER_ID_HEADER, "admin-user-123")
        .header(TRUSTED_ADMIN_USER_ROLE_HEADER, "admin")
        .header(TRUSTED_ADMIN_SESSION_ID_HEADER, "session-123")
        .json(&json!({
            "provider_id": "provider-antigravity",
            "model": "gemini-2.5-pro",
            "api_format": "gemini:generate_content"
        }))
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["success"], json!(false));
    assert_eq!(
        payload["error"],
        json!(
            "Rust local provider-query model test cannot execute endpoint format gemini:generate_content (transport_antigravity_auth_config_missing)"
        )
    );

    gateway_handle.abort();
}

#[test]
fn gateway_handles_antigravity_endpoint_test_model_locally() {
    run_provider_query_test(
        "gateway_handles_antigravity_endpoint_test_model_locally",
        gateway_handles_antigravity_endpoint_test_model_locally_impl,
    );
}

async fn gateway_handles_antigravity_endpoint_test_model_locally_impl() {
    let execution_runtime = Router::new().route(
        "/v1/execute/sync",
        any(move |Json(plan): Json<ExecutionPlan>| async move {
            assert_eq!(plan.provider_id, "provider-antigravity");
            assert_eq!(plan.endpoint_id, "endpoint-antigravity-gemini");
            assert_eq!(plan.key_id, "key-antigravity-gemini");
            assert_eq!(plan.provider_api_format, "gemini:generate_content");
            assert_eq!(
                plan.url,
                "https://antigravity.googleapis.com/v1internal:generateContent"
            );
            assert!(!plan.stream);
            assert_eq!(
                plan.body
                    .json_body
                    .as_ref()
                    .and_then(|body| body.get("project"))
                    .and_then(|value| value.as_str()),
                Some("project-ant-123")
            );
            assert_eq!(
                plan.body
                    .json_body
                    .as_ref()
                    .and_then(|body| body.get("requestType"))
                    .and_then(|value| value.as_str()),
                Some("endpoint_test")
            );
            assert_eq!(
                plan.body
                    .json_body
                    .as_ref()
                    .and_then(|body| body.get("model"))
                    .and_then(|value| value.as_str()),
                Some("gemini-2.5-pro")
            );
            assert_eq!(
                plan.body
                    .json_body
                    .as_ref()
                    .and_then(|body| body.get("request"))
                    .and_then(|request| request.get("contents"))
                    .and_then(|contents| contents.as_array())
                    .and_then(|items| items.first())
                    .and_then(|item| item.get("parts"))
                    .and_then(|parts| parts.as_array())
                    .and_then(|parts| parts.first())
                    .and_then(|part| part.get("text"))
                    .and_then(|value| value.as_str()),
                Some("Say hello")
            );
            assert_eq!(
                plan.headers.get("x-client-name").map(String::as_str),
                Some("antigravity")
            );
            assert!(plan.headers.contains_key("x-goog-api-client"));
            Json(json!({
                "request_id": plan.request_id,
                "candidate_id": plan.candidate_id,
                "status_code": 200,
                "headers": {
                    "content-type": "application/json"
                },
                "body": {
                    "json_body": {
                        "response": {
                            "candidates": [{
                                "content": {
                                    "parts": [
                                        {"text": "Hello from Antigravity EndpointTest"}
                                    ],
                                    "role": "model"
                                },
                                "finishReason": "STOP",
                                "index": 0
                            }],
                            "modelVersion": "claude-sonnet-4-5",
                            "usageMetadata": {
                                "promptTokenCount": 2,
                                "candidatesTokenCount": 3,
                                "totalTokenCount": 5
                            }
                        },
                        "responseId": "resp-antigravity-test-123"
                    }
                },
                "telemetry": {
                    "elapsed_ms": 23
                }
            }))
        }),
    );

    let (execution_runtime_url, execution_runtime_handle) = start_server(execution_runtime).await;
    let mut provider = sample_provider("provider-antigravity", "Antigravity", 10);
    provider.provider_type = "antigravity".to_string();
    let mut key = sample_bound_key(
        "key-antigravity-gemini",
        "provider-antigravity",
        "gemini:generate_content",
        "cached-antigravity-token",
    );
    key.auth_type = "oauth".to_string();
    key.encrypted_auth_config = Some(sample_bound_auth_config(
        "provider-antigravity",
        "key-antigravity-gemini",
        r#"{
                "provider_type":"antigravity",
                "project_id":"project-ant-123",
                "client_version":"1.2.3",
                "session_id":"sess-ant-123",
                "refresh_token":"rt-ant-123"
            }"#,
    ));
    let provider_catalog_repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![provider],
        vec![sample_endpoint(
            "endpoint-antigravity-gemini",
            "provider-antigravity",
            "gemini:generate_content",
            "https://antigravity.googleapis.com",
        )],
        vec![key],
    ));

    let gateway = build_router_with_state(
        build_state_with_execution_runtime_override(execution_runtime_url)
            .with_data_state_for_tests(GatewayDataState::with_provider_transport_reader_for_tests(
                provider_catalog_repository,
                DEV_FERNET_KEY.to_string(),
            )),
    );
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .post(format!("{gateway_url}/api/admin/provider-query/test-model"))
        .header(GATEWAY_HEADER, "rust-phase3b")
        .header(TRUSTED_ADMIN_USER_ID_HEADER, "admin-user-123")
        .header(TRUSTED_ADMIN_USER_ROLE_HEADER, "admin")
        .header(TRUSTED_ADMIN_SESSION_ID_HEADER, "session-123")
        .json(&json!({
            "provider_id": "provider-antigravity",
            "model": "gemini-2.5-pro",
            "api_format": "gemini:generate_content",
            "message": "Say hello"
        }))
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["success"], json!(true));
    assert_eq!(
        payload["data"]["response"]["choices"][0]["message"]["content"],
        json!("Hello from Antigravity EndpointTest")
    );

    gateway_handle.abort();
    execution_runtime_handle.abort();
}

#[test]
fn gateway_hydrates_antigravity_project_id_from_load_code_assist_for_test_model() {
    run_provider_query_test(
        "gateway_hydrates_antigravity_project_id_from_load_code_assist_for_test_model",
        gateway_hydrates_antigravity_project_id_from_load_code_assist_for_test_model_impl,
    );
}

async fn gateway_hydrates_antigravity_project_id_from_load_code_assist_for_test_model_impl() {
    let seen_urls = Arc::new(Mutex::new(Vec::<String>::new()));
    let seen_urls_clone = Arc::clone(&seen_urls);
    let execution_runtime = Router::new().route(
        "/v1/execute/sync",
        any(move |Json(plan): Json<ExecutionPlan>| {
            let seen_urls_inner = Arc::clone(&seen_urls_clone);
            async move {
                seen_urls_inner
                    .lock()
                    .expect("mutex should lock")
                    .push(plan.url.clone());
                if plan.url == "https://cloudcode-pa.googleapis.com/v1internal:loadCodeAssist" {
                    assert_eq!(plan.model_name.as_deref(), Some("loadCodeAssist"));
                    assert_eq!(
                        plan.headers.get("authorization").map(String::as_str),
                        Some("Bearer cached-antigravity-token")
                    );
                    assert_eq!(
                        plan.body.json_body.as_ref().and_then(|body| body
                            .get("metadata")
                            .and_then(|metadata| metadata.get("pluginType"))),
                        Some(&json!("GEMINI"))
                    );
                    return Json(json!({
                        "request_id": plan.request_id,
                        "candidate_id": plan.candidate_id,
                        "status_code": 200,
                        "headers": {
                            "content-type": "application/json"
                        },
                        "body": {
                            "json_body": {
                                "cloudaicompanionProject": {
                                    "id": "project-from-antigravity-load-code-assist"
                                },
                                "currentTier": {
                                    "id": "free"
                                }
                            }
                        }
                    }));
                }

                assert_eq!(
                    plan.url,
                    "https://daily-cloudcode-pa.googleapis.com/v1internal:generateContent"
                );
                assert_eq!(plan.provider_id, "provider-antigravity");
                assert_eq!(plan.endpoint_id, "endpoint-antigravity-gemini");
                assert_eq!(plan.key_id, "key-antigravity-gemini");
                assert_eq!(plan.provider_api_format, "gemini:generate_content");
                assert!(!plan.stream);
                assert_eq!(
                    plan.body.json_body.as_ref().unwrap()["project"],
                    json!("project-from-antigravity-load-code-assist")
                );
                assert_eq!(
                    plan.body.json_body.as_ref().unwrap()["requestType"],
                    json!("endpoint_test")
                );
                assert_eq!(
                    plan.body.json_body.as_ref().unwrap()["model"],
                    json!("gemini-3.1-flash-lite")
                );
                Json(json!({
                    "request_id": plan.request_id,
                    "candidate_id": plan.candidate_id,
                    "status_code": 200,
                    "headers": {
                        "content-type": "application/json"
                    },
                    "body": {
                        "json_body": {
                            "response": {
                                "candidates": [{
                                    "content": {
                                        "parts": [
                                            {"text": "Hello from hydrated Antigravity"}
                                        ],
                                        "role": "model"
                                    },
                                    "finishReason": "STOP",
                                    "index": 0
                                }],
                                "modelVersion": "gemini-3.1-flash-lite",
                                "usageMetadata": {
                                    "promptTokenCount": 2,
                                    "candidatesTokenCount": 4,
                                    "totalTokenCount": 6
                                }
                            },
                            "responseId": "resp-antigravity-hydrated-test-123"
                        }
                    },
                    "telemetry": {
                        "elapsed_ms": 23
                    }
                }))
            }
        }),
    );

    let (execution_runtime_url, execution_runtime_handle) = start_server(execution_runtime).await;
    let mut provider = sample_provider("provider-antigravity", "Antigravity", 10);
    provider.provider_type = "antigravity".to_string();
    let mut key = sample_bound_key(
        "key-antigravity-gemini",
        "provider-antigravity",
        "gemini:generate_content",
        "cached-antigravity-token",
    );
    key.auth_type = "oauth".to_string();
    key.encrypted_auth_config = Some(sample_bound_auth_config(
        "provider-antigravity",
        "key-antigravity-gemini",
        r#"{"provider_type":"antigravity","refresh_token":"rt-antigravity-123"}"#,
    ));
    let provider_catalog_repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![provider],
        vec![sample_endpoint(
            "endpoint-antigravity-gemini",
            "provider-antigravity",
            "gemini:generate_content",
            "https://daily-cloudcode-pa.googleapis.com",
        )],
        vec![key],
    ));

    let gateway = build_router_with_state(
        build_state_with_execution_runtime_override(execution_runtime_url)
            .with_data_state_for_tests(
                GatewayDataState::with_provider_catalog_repository_for_tests(Arc::clone(
                    &provider_catalog_repository,
                ))
                .with_encryption_key_for_tests(DEV_FERNET_KEY),
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
            "provider_id": "provider-antigravity",
            "model": "gemini-3.1-flash-lite",
            "api_format": "gemini:generate_content",
            "message": "Say hello"
        }))
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["success"], json!(true));
    assert_eq!(
        payload["data"]["response"]["choices"][0]["message"]["content"],
        json!("Hello from hydrated Antigravity")
    );
    assert_eq!(
        *seen_urls.lock().expect("mutex should lock"),
        vec![
            "https://cloudcode-pa.googleapis.com/v1internal:loadCodeAssist".to_string(),
            "https://daily-cloudcode-pa.googleapis.com/v1internal:generateContent".to_string(),
        ]
    );
    let reloaded = provider_catalog_repository
        .list_keys_by_ids(&["key-antigravity-gemini".to_string()])
        .await
        .expect("key should reload");
    assert_eq!(
        reloaded[0]
            .upstream_metadata
            .as_ref()
            .and_then(|metadata| metadata.get("antigravity"))
            .and_then(|metadata| metadata.get("project_id")),
        Some(&json!("project-from-antigravity-load-code-assist"))
    );

    gateway_handle.abort();
    execution_runtime_handle.abort();
}

#[test]
fn gateway_prefers_supported_non_kiro_endpoint_when_api_format_is_omitted() {
    run_provider_query_test(
        "gateway_prefers_supported_non_kiro_endpoint_when_api_format_is_omitted",
        gateway_prefers_supported_non_kiro_endpoint_when_api_format_is_omitted_impl,
    );
}

async fn gateway_prefers_supported_non_kiro_endpoint_when_api_format_is_omitted_impl() {
    let execution_runtime = Router::new().route(
        "/v1/execute/sync",
        any(move |Json(plan): Json<ExecutionPlan>| async move {
            assert_eq!(plan.endpoint_id, "endpoint-openai-chat");
            assert_eq!(plan.provider_api_format, "openai:chat");
            Json(json!({
                "request_id": plan.request_id,
                "candidate_id": plan.candidate_id,
                "status_code": 200,
                "headers": {
                    "content-type": "application/json"
                },
                "body": {
                    "json_body": {
                        "id": "chatcmpl-preferred-endpoint",
                        "choices": [{
                            "message": {
                                "role": "assistant",
                                "content": "Selected supported endpoint"
                            }
                        }]
                    }
                },
                "telemetry": {
                    "elapsed_ms": 10
                }
            }))
        }),
    );

    let (execution_runtime_url, execution_runtime_handle) = start_server(execution_runtime).await;
    let mut provider = sample_provider("provider-openai", "OpenAI", 10);
    provider.provider_type = "openai".to_string();
    let provider_catalog_repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![provider],
        vec![
            sample_endpoint(
                "endpoint-openai-cli",
                "provider-openai",
                "openai:responses",
                "https://api.openai.example/v1",
            ),
            sample_endpoint(
                "endpoint-openai-chat",
                "provider-openai",
                "openai:chat",
                "https://api.openai.example/v1",
            ),
        ],
        vec![
            sample_bound_key(
                "key-openai-cli",
                "provider-openai",
                "openai:responses",
                "sk-test-cli",
            ),
            sample_bound_key(
                "key-openai-chat",
                "provider-openai",
                "openai:chat",
                "sk-test-chat",
            ),
        ],
    ));

    let gateway = build_router_with_state(
        build_state_with_execution_runtime_override(execution_runtime_url)
            .with_data_state_for_tests(GatewayDataState::with_provider_transport_reader_for_tests(
                provider_catalog_repository,
                DEV_FERNET_KEY.to_string(),
            )),
    );
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .post(format!("{gateway_url}/api/admin/provider-query/test-model"))
        .header(GATEWAY_HEADER, "rust-phase3b")
        .header(TRUSTED_ADMIN_USER_ID_HEADER, "admin-user-123")
        .header(TRUSTED_ADMIN_USER_ROLE_HEADER, "admin")
        .header(TRUSTED_ADMIN_SESSION_ID_HEADER, "session-123")
        .json(&json!({
            "provider_id": "provider-openai",
            "model": "gpt-5.4-mini"
        }))
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["success"], json!(true));
    assert_eq!(
        payload["data"]["response"]["choices"][0]["message"]["content"],
        json!("Selected supported endpoint")
    );

    gateway_handle.abort();
    execution_runtime_handle.abort();
}

#[test]
fn gateway_prefers_transport_supported_non_kiro_endpoint_when_api_format_is_omitted() {
    run_provider_query_test(
        "gateway_prefers_transport_supported_non_kiro_endpoint_when_api_format_is_omitted",
        gateway_prefers_transport_supported_non_kiro_endpoint_when_api_format_is_omitted_impl,
    );
}

async fn gateway_prefers_transport_supported_non_kiro_endpoint_when_api_format_is_omitted_impl() {
    let execution_runtime = Router::new().route(
        "/v1/execute/sync",
        any(move |Json(plan): Json<ExecutionPlan>| async move {
            assert_eq!(plan.endpoint_id, "endpoint-openai-chat-supported");
            assert_eq!(plan.provider_api_format, "openai:chat");
            assert_eq!(
                plan.headers.get("content-type").map(String::as_str),
                Some("application/json")
            );
            Json(json!({
                "request_id": plan.request_id,
                "candidate_id": plan.candidate_id,
                "status_code": 200,
                "headers": {
                    "content-type": "application/json"
                },
                "body": {
                    "json_body": {
                        "id": "chatcmpl-supported-transport",
                        "choices": [{
                            "message": {
                                "role": "assistant",
                                "content": "Selected locally supported endpoint"
                            }
                        }]
                    }
                },
                "telemetry": {
                    "elapsed_ms": 12
                }
            }))
        }),
    );

    let (execution_runtime_url, execution_runtime_handle) = start_server(execution_runtime).await;
    let mut provider = sample_provider("provider-openai", "OpenAI", 10);
    provider.provider_type = "openai".to_string();
    let mut unsupported_endpoint = sample_endpoint(
        "endpoint-openai-chat-unsupported",
        "provider-openai",
        "openai:chat",
        "https://api.openai.example/v1",
    );
    unsupported_endpoint.header_rules = Some(json!({"invalid": true}));
    let provider_catalog_repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![provider],
        vec![
            unsupported_endpoint,
            sample_endpoint(
                "endpoint-openai-chat-supported",
                "provider-openai",
                "openai:chat",
                "https://api.openai.example/v1",
            ),
        ],
        vec![sample_bound_key(
            "key-openai-chat",
            "provider-openai",
            "openai:chat",
            "sk-test-chat",
        )],
    ));

    let gateway = build_router_with_state(
        build_state_with_execution_runtime_override(execution_runtime_url)
            .with_data_state_for_tests(GatewayDataState::with_provider_transport_reader_for_tests(
                provider_catalog_repository,
                DEV_FERNET_KEY.to_string(),
            )),
    );
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .post(format!("{gateway_url}/api/admin/provider-query/test-model"))
        .header(GATEWAY_HEADER, "rust-phase3b")
        .header(TRUSTED_ADMIN_USER_ID_HEADER, "admin-user-123")
        .header(TRUSTED_ADMIN_USER_ROLE_HEADER, "admin")
        .header(TRUSTED_ADMIN_SESSION_ID_HEADER, "session-123")
        .json(&json!({
            "provider_id": "provider-openai",
            "model": "gpt-5.4-mini"
        }))
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["success"], json!(true));
    assert_eq!(
        payload["data"]["response"]["choices"][0]["message"]["content"],
        json!("Selected locally supported endpoint")
    );

    gateway_handle.abort();
    execution_runtime_handle.abort();
}

#[test]
fn gateway_prefers_supported_non_kiro_endpoint_with_compatible_key_when_api_format_is_omitted() {
    run_provider_query_test(
        "gateway_prefers_supported_non_kiro_endpoint_with_compatible_key_when_api_format_is_omitted",
        gateway_prefers_supported_non_kiro_endpoint_with_compatible_key_when_api_format_is_omitted_impl,
    );
}

async fn gateway_prefers_supported_non_kiro_endpoint_with_compatible_key_when_api_format_is_omitted_impl(
) {
    let execution_runtime = Router::new().route(
        "/v1/execute/sync",
        any(move |Json(plan): Json<ExecutionPlan>| async move {
            assert_eq!(plan.endpoint_id, "endpoint-openai-chat");
            assert_eq!(plan.provider_api_format, "openai:chat");
            assert_eq!(plan.key_id, "key-openai-chat");
            Json(json!({
                "request_id": plan.request_id,
                "candidate_id": plan.candidate_id,
                "status_code": 200,
                "headers": {
                    "content-type": "application/json"
                },
                "body": {
                    "json_body": {
                        "id": "chatcmpl-key-compatible-endpoint",
                        "choices": [{
                            "message": {
                                "role": "assistant",
                                "content": "Selected endpoint with compatible key"
                            }
                        }]
                    }
                },
                "telemetry": {
                    "elapsed_ms": 11
                }
            }))
        }),
    );

    let (execution_runtime_url, execution_runtime_handle) = start_server(execution_runtime).await;
    let mut provider = sample_provider("provider-openai", "OpenAI", 10);
    provider.provider_type = "openai".to_string();
    let provider_catalog_repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![provider],
        vec![
            sample_endpoint(
                "endpoint-gemini-chat",
                "provider-openai",
                "gemini:generate_content",
                "https://api.gemini.example",
            ),
            sample_endpoint(
                "endpoint-openai-chat",
                "provider-openai",
                "openai:chat",
                "https://api.openai.example/v1",
            ),
        ],
        vec![sample_bound_key(
            "key-openai-chat",
            "provider-openai",
            "openai:chat",
            "sk-test-chat",
        )],
    ));

    let gateway = build_router_with_state(
        build_state_with_execution_runtime_override(execution_runtime_url)
            .with_data_state_for_tests(GatewayDataState::with_provider_transport_reader_for_tests(
                provider_catalog_repository,
                DEV_FERNET_KEY.to_string(),
            )),
    );
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .post(format!("{gateway_url}/api/admin/provider-query/test-model"))
        .header(GATEWAY_HEADER, "rust-phase3b")
        .header(TRUSTED_ADMIN_USER_ID_HEADER, "admin-user-123")
        .header(TRUSTED_ADMIN_USER_ROLE_HEADER, "admin")
        .header(TRUSTED_ADMIN_SESSION_ID_HEADER, "session-123")
        .json(&json!({
            "provider_id": "provider-openai",
            "model": "gpt-5.4-mini"
        }))
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["success"], json!(true));
    assert_eq!(
        payload["data"]["response"]["choices"][0]["message"]["content"],
        json!("Selected endpoint with compatible key")
    );

    gateway_handle.abort();
    execution_runtime_handle.abort();
}

#[test]
fn gateway_uses_compatible_cli_endpoint_when_api_format_is_omitted() {
    run_provider_query_test(
        "gateway_uses_compatible_cli_endpoint_when_api_format_is_omitted",
        gateway_uses_compatible_cli_endpoint_when_api_format_is_omitted_impl,
    );
}

async fn gateway_uses_compatible_cli_endpoint_when_api_format_is_omitted_impl() {
    let execution_runtime = Router::new().route(
        "/v1/execute/sync",
        any(move |Json(plan): Json<ExecutionPlan>| async move {
            assert_eq!(plan.endpoint_id, "endpoint-openai-cli");
            assert_eq!(plan.provider_api_format, "openai:responses");
            assert_eq!(plan.key_id, "key-openai-cli");
            Json(json!({
                "request_id": plan.request_id,
                "candidate_id": plan.candidate_id,
                "status_code": 200,
                "headers": {
                    "content-type": "application/json"
                },
                "body": {
                    "json_body": {
                        "id": "chatcmpl-cli-only-endpoint",
                        "choices": [{
                            "message": {
                                "role": "assistant",
                                "content": "Selected compatible CLI endpoint"
                            }
                        }]
                    }
                },
                "telemetry": {
                    "elapsed_ms": 13
                }
            }))
        }),
    );

    let (execution_runtime_url, execution_runtime_handle) = start_server(execution_runtime).await;
    let mut provider = sample_provider("provider-openai", "OpenAI", 10);
    provider.provider_type = "openai".to_string();
    let provider_catalog_repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![provider],
        vec![
            sample_endpoint(
                "endpoint-openai-chat",
                "provider-openai",
                "openai:chat",
                "https://api.openai.example/v1",
            ),
            sample_endpoint(
                "endpoint-openai-cli",
                "provider-openai",
                "openai:responses",
                "https://api.openai.example/v1",
            ),
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
                DEV_FERNET_KEY.to_string(),
            )),
    );
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .post(format!("{gateway_url}/api/admin/provider-query/test-model"))
        .header(GATEWAY_HEADER, "rust-phase3b")
        .header(TRUSTED_ADMIN_USER_ID_HEADER, "admin-user-123")
        .header(TRUSTED_ADMIN_USER_ROLE_HEADER, "admin")
        .header(TRUSTED_ADMIN_SESSION_ID_HEADER, "session-123")
        .json(&json!({
            "provider_id": "provider-openai",
            "model": "gpt-5.4-mini"
        }))
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["success"], json!(true));
    assert_eq!(
        payload["data"]["response"]["choices"][0]["message"]["content"],
        json!("Selected compatible CLI endpoint")
    );

    gateway_handle.abort();
    execution_runtime_handle.abort();
}

#[test]
fn gateway_uses_runnable_cli_endpoint_after_chat_preference_when_api_format_is_omitted() {
    run_provider_query_test(
        "gateway_uses_runnable_cli_endpoint_after_chat_preference_when_api_format_is_omitted",
        gateway_uses_runnable_cli_endpoint_after_chat_preference_when_api_format_is_omitted_impl,
    );
}

async fn gateway_uses_runnable_cli_endpoint_after_chat_preference_when_api_format_is_omitted_impl()
{
    let execution_runtime = Router::new().route(
        "/v1/execute/sync",
        any(move |Json(plan): Json<ExecutionPlan>| async move {
            assert_eq!(plan.endpoint_id, "endpoint-openai-cli-runnable");
            assert_eq!(plan.provider_api_format, "openai:responses");
            assert_eq!(plan.key_id, "key-openai-shared");
            Json(json!({
                "request_id": plan.request_id,
                "candidate_id": plan.candidate_id,
                "status_code": 200,
                "headers": {
                    "content-type": "application/json"
                },
                "body": {
                    "json_body": {
                        "id": "chatcmpl-cli-runnable-after-chat-preference",
                        "choices": [{
                            "message": {
                                "role": "assistant",
                                "content": "Selected runnable CLI endpoint after unsupported chat"
                            }
                        }]
                    }
                },
                "telemetry": {
                    "elapsed_ms": 18
                }
            }))
        }),
    );

    let (execution_runtime_url, execution_runtime_handle) = start_server(execution_runtime).await;
    let mut provider = sample_provider("provider-openai", "OpenAI", 10);
    provider.provider_type = "openai".to_string();
    let mut unsupported_chat_endpoint = sample_endpoint(
        "endpoint-openai-chat-unsupported",
        "provider-openai",
        "openai:chat",
        "https://api.openai.example/v1",
    );
    unsupported_chat_endpoint.header_rules = Some(json!({"invalid": true}));
    let cli_endpoint = sample_endpoint(
        "endpoint-openai-cli-runnable",
        "provider-openai",
        "openai:responses",
        "https://api.openai.example/v1",
    );
    let mut shared_key = sample_bound_key(
        "key-openai-shared",
        "provider-openai",
        "openai:chat",
        "sk-test-shared",
    );
    shared_key.api_formats = Some(json!(["openai:chat", "openai:responses"]));
    let provider_catalog_repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![provider],
        vec![unsupported_chat_endpoint, cli_endpoint],
        vec![shared_key],
    ));

    let gateway = build_router_with_state(
        build_state_with_execution_runtime_override(execution_runtime_url)
            .with_data_state_for_tests(GatewayDataState::with_provider_transport_reader_for_tests(
                provider_catalog_repository,
                DEV_FERNET_KEY.to_string(),
            )),
    );
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .post(format!("{gateway_url}/api/admin/provider-query/test-model"))
        .header(GATEWAY_HEADER, "rust-phase3b")
        .header(TRUSTED_ADMIN_USER_ID_HEADER, "admin-user-123")
        .header(TRUSTED_ADMIN_USER_ROLE_HEADER, "admin")
        .header(TRUSTED_ADMIN_SESSION_ID_HEADER, "session-123")
        .json(&json!({
            "provider_id": "provider-openai",
            "model": "gpt-5.4-mini"
        }))
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["success"], json!(true));
    assert_eq!(
        payload["data"]["response"]["choices"][0]["message"]["content"],
        json!("Selected runnable CLI endpoint after unsupported chat")
    );

    gateway_handle.abort();
    execution_runtime_handle.abort();
}

#[test]
fn gateway_handles_claude_cli_test_model_locally() {
    run_provider_query_test(
        "gateway_handles_claude_cli_test_model_locally",
        gateway_handles_claude_cli_test_model_locally_impl,
    );
}

async fn gateway_handles_claude_cli_test_model_locally_impl() {
    let execution_runtime = Router::new().route(
        "/v1/execute/sync",
        any(move |Json(plan): Json<ExecutionPlan>| async move {
            assert_eq!(plan.provider_id, "provider-claude");
            assert_eq!(plan.endpoint_id, "endpoint-claude-cli");
            assert_eq!(plan.key_id, "key-claude-cli");
            assert_eq!(plan.provider_api_format, "claude:messages");
            assert_eq!(plan.url, "https://api.anthropic.example/v1/messages");
            assert_eq!(plan.model_name.as_deref(), Some("claude-sonnet-4-5"));
            Json(json!({
                "request_id": plan.request_id,
                "candidate_id": plan.candidate_id,
                "status_code": 200,
                "headers": {
                    "content-type": "application/json"
                },
                "body": {
                    "json_body": {
                        "id": "chatcmpl-claude-cli-test-model",
                        "choices": [{
                            "message": {
                                "role": "assistant",
                                "content": "Hello from Claude CLI"
                            }
                        }]
                    }
                },
                "telemetry": {
                    "elapsed_ms": 14
                }
            }))
        }),
    );

    let (execution_runtime_url, execution_runtime_handle) = start_server(execution_runtime).await;
    let mut provider = sample_provider("provider-claude", "Claude", 10);
    provider.provider_type = "anthropic".to_string();
    let provider_catalog_repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![provider],
        vec![sample_endpoint(
            "endpoint-claude-cli",
            "provider-claude",
            "claude:messages",
            "https://api.anthropic.example/v1",
        )],
        vec![sample_bound_key(
            "key-claude-cli",
            "provider-claude",
            "claude:messages",
            "sk-test-claude-cli",
        )],
    ));

    let gateway = build_router_with_state(
        build_state_with_execution_runtime_override(execution_runtime_url)
            .with_data_state_for_tests(GatewayDataState::with_provider_transport_reader_for_tests(
                provider_catalog_repository,
                DEV_FERNET_KEY.to_string(),
            )),
    );
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .post(format!("{gateway_url}/api/admin/provider-query/test-model"))
        .header(GATEWAY_HEADER, "rust-phase3b")
        .header(TRUSTED_ADMIN_USER_ID_HEADER, "admin-user-123")
        .header(TRUSTED_ADMIN_USER_ROLE_HEADER, "admin")
        .header(TRUSTED_ADMIN_SESSION_ID_HEADER, "session-123")
        .json(&json!({
            "provider_id": "provider-claude",
            "model": "claude-sonnet-4-5",
            "api_format": "claude:messages"
        }))
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["success"], json!(true));
    assert_eq!(
        payload["data"]["response"]["choices"][0]["message"]["content"],
        json!("Hello from Claude CLI")
    );

    gateway_handle.abort();
    execution_runtime_handle.abort();
}

#[test]
fn gateway_uses_compatible_claude_cli_endpoint_when_api_format_is_omitted() {
    run_provider_query_test(
        "gateway_uses_compatible_claude_cli_endpoint_when_api_format_is_omitted",
        gateway_uses_compatible_claude_cli_endpoint_when_api_format_is_omitted_impl,
    );
}

async fn gateway_uses_compatible_claude_cli_endpoint_when_api_format_is_omitted_impl() {
    let execution_runtime = Router::new().route(
        "/v1/execute/sync",
        any(move |Json(plan): Json<ExecutionPlan>| async move {
            assert_eq!(plan.endpoint_id, "endpoint-claude-cli");
            assert_eq!(plan.provider_api_format, "claude:messages");
            assert_eq!(plan.key_id, "key-claude-cli");
            Json(json!({
                "request_id": plan.request_id,
                "candidate_id": plan.candidate_id,
                "status_code": 200,
                "headers": {
                    "content-type": "application/json"
                },
                "body": {
                    "json_body": {
                        "id": "chatcmpl-claude-cli-only-endpoint",
                        "choices": [{
                            "message": {
                                "role": "assistant",
                                "content": "Selected compatible Claude CLI endpoint"
                            }
                        }]
                    }
                },
                "telemetry": {
                    "elapsed_ms": 12
                }
            }))
        }),
    );

    let (execution_runtime_url, execution_runtime_handle) = start_server(execution_runtime).await;
    let mut provider = sample_provider("provider-claude", "Claude", 10);
    provider.provider_type = "anthropic".to_string();
    let provider_catalog_repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![provider],
        vec![sample_endpoint(
            "endpoint-claude-cli",
            "provider-claude",
            "claude:messages",
            "https://api.anthropic.example/v1",
        )],
        vec![sample_bound_key(
            "key-claude-cli",
            "provider-claude",
            "claude:messages",
            "sk-test-claude-cli",
        )],
    ));

    let gateway = build_router_with_state(
        build_state_with_execution_runtime_override(execution_runtime_url)
            .with_data_state_for_tests(GatewayDataState::with_provider_transport_reader_for_tests(
                provider_catalog_repository,
                DEV_FERNET_KEY.to_string(),
            )),
    );
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .post(format!("{gateway_url}/api/admin/provider-query/test-model"))
        .header(GATEWAY_HEADER, "rust-phase3b")
        .header(TRUSTED_ADMIN_USER_ID_HEADER, "admin-user-123")
        .header(TRUSTED_ADMIN_USER_ROLE_HEADER, "admin")
        .header(TRUSTED_ADMIN_SESSION_ID_HEADER, "session-123")
        .json(&json!({
            "provider_id": "provider-claude",
            "model": "claude-sonnet-4-5"
        }))
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["success"], json!(true));
    assert_eq!(
        payload["data"]["response"]["choices"][0]["message"]["content"],
        json!("Selected compatible Claude CLI endpoint")
    );

    gateway_handle.abort();
    execution_runtime_handle.abort();
}
