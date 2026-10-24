use super::*;

#[test]
fn gateway_handles_admin_provider_query_test_model_locally_with_trusted_admin_principal() {
    run_provider_query_test(
        "gateway_handles_admin_provider_query_test_model_locally_with_trusted_admin_principal",
        gateway_handles_admin_provider_query_test_model_locally_with_trusted_admin_principal_impl,
    );
}

async fn gateway_handles_admin_provider_query_test_model_locally_with_trusted_admin_principal_impl()
{
    let execution_runtime = Router::new().route(
        "/v1/execute/sync",
        any(move |Json(plan): Json<ExecutionPlan>| async move {
            assert_eq!(plan.provider_id, "provider-openai");
            assert_eq!(plan.endpoint_id, "endpoint-openai-chat");
            assert_eq!(plan.key_id, "key-openai-primary");
            assert_eq!(plan.provider_api_format, "openai:chat");
            assert_eq!(plan.model_name.as_deref(), Some("gpt-4.1"));
            assert!(!plan.stream);
            assert_eq!(
                plan.headers.get("content-type").map(String::as_str),
                Some("application/json")
            );
            assert_eq!(
                plan.headers.get("authorization").map(String::as_str),
                Some("Bearer sk-test-primary")
            );
            assert_eq!(
                plan.body
                    .json_body
                    .as_ref()
                    .and_then(|body| body.get("model")),
                Some(&json!("gpt-4.1"))
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
                        "id": "chatcmpl-test-model",
                        "object": "chat.completion",
                        "choices": [{
                            "message": {
                                "role": "assistant",
                                "content": "Hello from OpenAI"
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
    let provider_catalog_repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![provider],
        vec![sample_endpoint(
            "endpoint-openai-chat",
            "provider-openai",
            "openai:chat",
            "https://api.openai.example/v1",
        )],
        vec![sample_bound_key(
            "key-openai-primary",
            "provider-openai",
            "openai:chat",
            "sk-test-primary",
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
        .post(format!("{gateway_url}/api/admin/provider-query/test-model"))
        .header(GATEWAY_HEADER, "rust-phase3b")
        .header(TRUSTED_ADMIN_USER_ID_HEADER, "admin-user-123")
        .header(TRUSTED_ADMIN_USER_ROLE_HEADER, "admin")
        .header(TRUSTED_ADMIN_SESSION_ID_HEADER, "session-123")
        .json(&json!({
            "provider_id": "provider-openai",
            "model": "gpt-4.1",
            "api_format": "openai:chat"
        }))
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["success"], json!(true));
    assert_eq!(payload["provider"]["id"], json!("provider-openai"));
    assert_eq!(payload["model"], json!("gpt-4.1"));
    assert_eq!(payload["error"], serde_json::Value::Null);
    assert_eq!(
        payload["data"]["response"]["choices"][0]["message"]["content"],
        json!("Hello from OpenAI")
    );

    gateway_handle.abort();
    execution_runtime_handle.abort();
}

#[test]
fn gateway_handles_openai_responses_test_model_locally() {
    run_provider_query_test(
        "gateway_handles_openai_responses_test_model_locally",
        gateway_handles_openai_responses_test_model_locally_impl,
    );
}

async fn gateway_handles_openai_responses_test_model_locally_impl() {
    let prompt = "Tell me whether the CLI request preserved this prompt.";
    let execution_runtime = Router::new().route(
        "/v1/execute/sync",
        any(move |Json(plan): Json<ExecutionPlan>| async move {
            assert_eq!(plan.provider_id, "provider-openai");
            assert_eq!(plan.endpoint_id, "endpoint-openai-cli");
            assert_eq!(plan.key_id, "key-openai-cli");
            assert_eq!(plan.provider_api_format, "openai:responses");
            assert_eq!(plan.url, "https://tiger.bookapi.cc/codex/responses");
            assert!(plan.stream);
            assert_eq!(
                plan.headers.get("authorization").map(String::as_str),
                Some("Bearer sk-test-cli")
            );
            assert_eq!(
                plan.headers.get("x-stainless-runtime").map(String::as_str),
                Some("node")
            );
            assert_eq!(
                plan.body
                    .json_body
                    .as_ref()
                    .and_then(|body| body.get("model")),
                Some(&json!("gpt-5.4-mini"))
            );
            assert_eq!(
                plan.body
                    .json_body
                    .as_ref()
                    .and_then(|body| body.get("stream")),
                Some(&json!(true))
            );
            assert!(plan
                .body
                .json_body
                .as_ref()
                .and_then(|body| body.get("input"))
                .is_some());
            assert_eq!(
                plan.body
                    .json_body
                    .as_ref()
                    .and_then(|body| body.get("input"))
                    .and_then(|input| input.as_array())
                    .and_then(|items| items.first())
                    .and_then(|item| item.get("type"))
                    .and_then(|value| value.as_str()),
                Some("message")
            );
            assert_eq!(
                plan.body
                    .json_body
                    .as_ref()
                    .and_then(|body| body.get("input"))
                    .and_then(|input| input.as_array())
                    .and_then(|items| items.first())
                    .and_then(|item| item.get("content"))
                    .and_then(|content| content.as_array())
                    .and_then(|parts| parts.first())
                    .and_then(|part| part.get("text"))
                    .and_then(|value| value.as_str()),
                Some(prompt)
            );
            assert!(plan
                .body
                .json_body
                .as_ref()
                .and_then(|body| body.get("instructions"))
                .is_none());
            assert_eq!(
                plan.body
                    .json_body
                    .as_ref()
                    .and_then(|body| body.get("store")),
                Some(&json!(false))
            );
            assert!(plan
                .body
                .json_body
                .as_ref()
                .and_then(|body| body.get("prompt_cache_key"))
                .is_none());
            Json(json!({
                "request_id": plan.request_id,
                "candidate_id": plan.candidate_id,
                "status_code": 200,
                "headers": {
                    "content-type": "application/json"
                },
                "body": {
                    "json_body": {
                        "id": "chatcmpl-openai-cli-test-model",
                        "object": "chat.completion",
                        "choices": [{
                            "message": {
                                "role": "assistant",
                                "content": "Hello from OpenAI Responses"
                            }
                        }]
                    }
                },
                "telemetry": {
                    "elapsed_ms": 17
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
            "endpoint-openai-cli",
            "provider-openai",
            "openai:responses",
            "https://tiger.bookapi.cc/codex",
        )],
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
        .post(format!("{gateway_url}/api/admin/provider-query/test-model"))
        .header(GATEWAY_HEADER, "rust-phase3b")
        .header(TRUSTED_ADMIN_USER_ID_HEADER, "admin-user-123")
        .header(TRUSTED_ADMIN_USER_ROLE_HEADER, "admin")
        .header(TRUSTED_ADMIN_SESSION_ID_HEADER, "session-123")
        .json(&json!({
            "provider_id": "provider-openai",
            "model": "gpt-5.4-mini",
            "api_format": "openai:responses",
            "message": prompt,
            "request_headers": {
                "x-stainless-runtime": "node"
            }
        }))
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["success"], json!(true));
    assert_eq!(
        payload["data"]["response"]["choices"][0]["message"]["content"],
        json!("Hello from OpenAI Responses")
    );

    gateway_handle.abort();
    execution_runtime_handle.abort();
}

#[test]
fn gateway_preserves_non_success_status_for_test_model_local_wrapper() {
    run_provider_query_test(
        "gateway_preserves_non_success_status_for_test_model_local_wrapper",
        gateway_preserves_non_success_status_for_test_model_local_wrapper_impl,
    );
}

async fn gateway_preserves_non_success_status_for_test_model_local_wrapper_impl() {
    let gateway = build_router_with_state(AppState::new().expect("gateway should build"));
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .post(format!("{gateway_url}/api/admin/provider-query/test-model"))
        .header(GATEWAY_HEADER, "rust-phase3b")
        .header(TRUSTED_ADMIN_USER_ID_HEADER, "admin-user-123")
        .header(TRUSTED_ADMIN_USER_ROLE_HEADER, "admin")
        .header(TRUSTED_ADMIN_SESSION_ID_HEADER, "session-123")
        .json(&json!({
            "provider_id": "provider-does-not-exist",
            "model": "gpt-4.1"
        }))
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["detail"], json!("Provider not found"));

    gateway_handle.abort();
}

#[test]
fn gateway_rejects_admin_provider_query_test_model_without_model() {
    run_provider_query_test(
        "gateway_rejects_admin_provider_query_test_model_without_model",
        gateway_rejects_admin_provider_query_test_model_without_model_impl,
    );
}

async fn gateway_rejects_admin_provider_query_test_model_without_model_impl() {
    assert_admin_provider_query_route(
        "/api/admin/provider-query/test-model",
        json!({ "provider_id": "provider-openai" }),
        StatusCode::BAD_REQUEST,
        |payload| {
            assert_eq!(payload["detail"], json!("model is required"));
        },
    )
    .await;
}
