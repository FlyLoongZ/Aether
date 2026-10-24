use super::*;
use crate::tests::DEV_FERNET_KEY;

#[test]
fn gateway_handles_admin_provider_query_test_model_failover_locally_with_trusted_admin_principal() {
    run_provider_query_test(
        "gateway_handles_admin_provider_query_test_model_failover_locally_with_trusted_admin_principal",
        gateway_handles_admin_provider_query_test_model_failover_locally_with_trusted_admin_principal_impl,
    );
}

async fn gateway_handles_admin_provider_query_test_model_failover_locally_with_trusted_admin_principal_impl(
) {
    let execution_runtime = Router::new().route(
        "/v1/execute/sync",
        any(move |Json(plan): Json<ExecutionPlan>| async move {
            let auth = plan
                .headers
                .get("authorization")
                .map(String::as_str)
                .unwrap_or_default()
                .to_string();
            assert_eq!(plan.provider_id, "provider-openai");
            assert_eq!(plan.endpoint_id, "endpoint-openai-chat");
            assert_eq!(plan.provider_api_format, "openai:chat");
            assert_eq!(plan.model_name.as_deref(), Some("gpt-4.1"));
            assert_eq!(
                plan.headers.get("x-test-header").map(String::as_str),
                Some("from-admin")
            );
            assert_eq!(
                plan.body
                    .json_body
                    .as_ref()
                    .and_then(|body| body.get("messages"))
                    .and_then(|messages| messages.as_array())
                    .and_then(|messages| messages.first())
                    .and_then(|message| message.get("content")),
                Some(&json!("custom prompt"))
            );
            let payload = if auth == "Bearer sk-test-first" {
                json!({
                    "request_id": plan.request_id,
                    "candidate_id": plan.candidate_id,
                    "status_code": 429,
                    "headers": {
                        "content-type": "application/json"
                    },
                    "body": {
                        "json_body": {
                            "error": {
                                "message": "too many requests"
                            }
                        }
                    },
                    "telemetry": {
                        "elapsed_ms": 11
                    }
                })
            } else {
                json!({
                    "request_id": plan.request_id,
                    "candidate_id": plan.candidate_id,
                    "status_code": 200,
                    "headers": {
                        "content-type": "application/json"
                    },
                    "body": {
                        "json_body": {
                            "id": "chatcmpl-failover",
                            "object": "chat.completion",
                            "choices": [{
                                "message": {
                                    "role": "assistant",
                                    "content": "Recovered from OpenAI failover"
                                }
                            }]
                        }
                    },
                    "telemetry": {
                        "elapsed_ms": 27
                    }
                })
            };
            Json(payload)
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
        vec![
            sample_bound_key(
                "key-openai-first",
                "provider-openai",
                "openai:chat",
                "sk-test-first",
            ),
            sample_bound_key(
                "key-openai-second",
                "provider-openai",
                "openai:chat",
                "sk-test-second",
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
        .post(format!(
            "{gateway_url}/api/admin/provider-query/test-model-failover"
        ))
        .header(GATEWAY_HEADER, "rust-phase3b")
        .header(TRUSTED_ADMIN_USER_ID_HEADER, "admin-user-123")
        .header(TRUSTED_ADMIN_USER_ROLE_HEADER, "admin")
        .header(TRUSTED_ADMIN_SESSION_ID_HEADER, "session-123")
        .json(&json!({
            "provider_id": "provider-openai",
            "mode": "direct",
            "model_name": "gpt-4.1",
            "failover_models": ["gpt-4.1"],
            "api_format": "openai:chat",
            "request_headers": {
                "x-test-header": "from-admin"
            },
            "request_body": {
                "model": "ignored-model",
                "messages": [{
                    "role": "user",
                    "content": "custom prompt"
                }]
            },
            "request_id": "provider-test-openai"
        }))
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["success"], json!(true));
    assert_eq!(payload["total_candidates"], json!(2));
    assert_eq!(payload["total_attempts"], json!(2));
    assert_eq!(payload["candidate_summary"]["total_candidates"], json!(2));
    assert_eq!(payload["candidate_summary"]["attempted"], json!(2));
    assert_eq!(payload["candidate_summary"]["failed"], json!(1));
    assert_eq!(payload["candidate_summary"]["success"], json!(1));
    assert_eq!(
        payload["candidate_summary"]["stop_reason"],
        json!("first_success")
    );
    let attempts = payload["attempts"]
        .as_array()
        .expect("attempts should be an array");
    assert_eq!(attempts.len(), 2);
    assert_eq!(attempts[0]["status"], json!("failed"));
    assert_eq!(attempts[0]["status_code"], json!(429));
    assert_eq!(attempts[1]["status"], json!("success"));
    assert_eq!(attempts[1]["key_id"], json!("key-openai-second"));
    assert_eq!(attempts[1]["request_body"]["model"], json!("gpt-4.1"));
    assert_eq!(payload["data"]["stream"], json!(false));
    assert_eq!(
        payload["data"]["response"]["choices"][0]["message"]["content"],
        json!("Recovered from OpenAI failover")
    );

    gateway_handle.abort();
    execution_runtime_handle.abort();
}

#[test]
fn gateway_handles_admin_provider_query_test_model_failover_for_kiro_locally() {
    run_provider_query_test(
        "gateway_handles_admin_provider_query_test_model_failover_for_kiro_locally",
        gateway_handles_admin_provider_query_test_model_failover_for_kiro_locally_impl,
    );
}

async fn gateway_handles_admin_provider_query_test_model_failover_for_kiro_locally_impl() {
    let execution_runtime = Router::new().route(
        "/v1/execute/sync",
        any(move |Json(plan): Json<ExecutionPlan>| async move {
            let payload = if plan.key_id == "key-kiro-first" {
                json!({
                    "request_id": plan.request_id,
                    "candidate_id": plan.candidate_id,
                    "status_code": 429,
                    "headers": {
                        "content-type": "application/json"
                    },
                    "body": {
                        "json_body": {
                            "message": "too many requests"
                        }
                    },
                    "telemetry": {
                        "elapsed_ms": 11
                    }
                })
            } else {
                json!({
                    "request_id": plan.request_id,
                    "candidate_id": plan.candidate_id,
                    "status_code": 200,
                    "headers": {
                        "content-type": "application/vnd.amazon.eventstream"
                    },
                    "body": {
                        "body_bytes_b64": base64::engine::general_purpose::STANDARD.encode(
                            [
                                encode_kiro_event_frame("assistantResponseEvent", json!({"content": "Recovered from failover"})),
                                encode_kiro_exception_frame("ContentLengthExceededException"),
                            ]
                            .concat()
                        )
                    },
                    "telemetry": {
                        "elapsed_ms": 27
                    }
                })
            };
            Json(payload)
        }),
    );

    let (execution_runtime_url, execution_runtime_handle) = start_server(execution_runtime).await;
    let mut provider = sample_provider("provider-kiro", "Kiro", 10);
    provider.provider_type = "kiro".to_string();
    let build_key = |id: &str| {
        let mut key = sample_bound_key(id, "provider-kiro", "claude:messages", "__placeholder__");
        key.auth_type = "oauth".to_string();
        key.encrypted_auth_config = Some(sample_bound_auth_config(
            "provider-kiro",
            id,
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
        key
    };

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
        vec![build_key("key-kiro-first"), build_key("key-kiro-second")],
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
            "provider_id": "provider-kiro",
            "mode": "direct",
            "model_name": "claude-sonnet-4-upstream",
            "failover_models": ["claude-sonnet-4-upstream"],
            "api_format": "claude:messages",
            "request_id": "provider-test-kiro"
        }))
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["success"], json!(true));
    assert_eq!(payload["total_candidates"], json!(2));
    assert_eq!(payload["total_attempts"], json!(2));
    assert_eq!(payload["candidate_summary"]["total_candidates"], json!(2));
    assert_eq!(payload["candidate_summary"]["attempted"], json!(2));
    assert_eq!(payload["candidate_summary"]["failed"], json!(1));
    assert_eq!(payload["candidate_summary"]["success"], json!(1));
    assert_eq!(
        payload["candidate_summary"]["stop_reason"],
        json!("first_success")
    );
    let attempts = payload["attempts"]
        .as_array()
        .expect("attempts should be an array");
    assert_eq!(attempts.len(), 2);
    assert_eq!(attempts[0]["status"], json!("failed"));
    assert_eq!(attempts[0]["status_code"], json!(429));
    assert_eq!(attempts[1]["status"], json!("success"));
    assert_eq!(
        payload["data"]["response"]["content"][0]["text"],
        json!("Recovered from failover")
    );

    gateway_handle.abort();
    execution_runtime_handle.abort();
}

#[test]
fn gateway_retries_kiro_failover_after_http_error_without_message() {
    run_provider_query_test(
        "gateway_retries_kiro_failover_after_http_error_without_message",
        gateway_retries_kiro_failover_after_http_error_without_message_impl,
    );
}

async fn gateway_retries_kiro_failover_after_http_error_without_message_impl() {
    let execution_runtime = Router::new().route(
        "/v1/execute/sync",
        any(move |Json(plan): Json<ExecutionPlan>| async move {
            let payload = if plan.key_id == "key-kiro-first" {
                json!({
                    "request_id": plan.request_id,
                    "candidate_id": plan.candidate_id,
                    "status_code": 500,
                    "headers": {
                        "content-type": "application/json"
                    },
                    "body": {
                        "json_body": {}
                    },
                    "telemetry": {
                        "elapsed_ms": 9
                    }
                })
            } else {
                json!({
                    "request_id": plan.request_id,
                    "candidate_id": plan.candidate_id,
                    "status_code": 200,
                    "headers": {
                        "content-type": "application/vnd.amazon.eventstream"
                    },
                    "body": {
                        "body_bytes_b64": base64::engine::general_purpose::STANDARD.encode(
                            [
                                encode_kiro_event_frame("assistantResponseEvent", json!({"content": "Recovered from Kiro empty error"})),
                                encode_kiro_exception_frame("ContentLengthExceededException"),
                            ]
                            .concat()
                        )
                    },
                    "telemetry": {
                        "elapsed_ms": 21
                    }
                })
            };
            Json(payload)
        }),
    );

    let (execution_runtime_url, execution_runtime_handle) = start_server(execution_runtime).await;
    let mut provider = sample_provider("provider-kiro", "Kiro", 10);
    provider.provider_type = "kiro".to_string();
    let build_key = |id: &str| {
        let mut key = sample_bound_key(id, "provider-kiro", "claude:messages", "__placeholder__");
        key.auth_type = "oauth".to_string();
        key.encrypted_auth_config = Some(sample_bound_auth_config(
            "provider-kiro",
            id,
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
        key
    };

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
        vec![build_key("key-kiro-first"), build_key("key-kiro-second")],
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
            "provider_id": "provider-kiro",
            "mode": "direct",
            "model_name": "claude-sonnet-4-upstream",
            "failover_models": ["claude-sonnet-4-upstream"],
            "api_format": "claude:messages",
            "request_id": "provider-test-kiro-empty-error"
        }))
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["success"], json!(true));
    assert_eq!(payload["total_attempts"], json!(2));
    let attempts = payload["attempts"]
        .as_array()
        .expect("attempts should be an array");
    assert_eq!(attempts[0]["status"], json!("failed"));
    assert_eq!(attempts[0]["status_code"], json!(500));
    assert_eq!(attempts[1]["status"], json!("success"));
    assert_eq!(
        payload["data"]["response"]["content"][0]["text"],
        json!("Recovered from Kiro empty error")
    );

    gateway_handle.abort();
    execution_runtime_handle.abort();
}

#[test]
fn gateway_handles_non_kiro_multi_model_failover_locally() {
    run_provider_query_test(
        "gateway_handles_non_kiro_multi_model_failover_locally",
        gateway_handles_non_kiro_multi_model_failover_locally_impl,
    );
}

async fn gateway_handles_non_kiro_multi_model_failover_locally_impl() {
    let execution_runtime = Router::new().route(
        "/v1/execute/sync",
        any(move |Json(plan): Json<ExecutionPlan>| async move {
            let payload = if plan.model_name.as_deref() == Some("gpt-4.1") {
                json!({
                    "request_id": plan.request_id,
                    "candidate_id": plan.candidate_id,
                    "status_code": 500,
                    "headers": {
                        "content-type": "application/json"
                    },
                    "body": {
                        "json_body": {
                            "error": {
                                "message": "primary model failed"
                            }
                        }
                    },
                    "telemetry": {
                        "elapsed_ms": 8
                    }
                })
            } else {
                json!({
                    "request_id": plan.request_id,
                    "candidate_id": plan.candidate_id,
                    "status_code": 200,
                    "headers": {
                        "content-type": "application/json"
                    },
                    "body": {
                        "json_body": {
                            "id": "chatcmpl-multi-model",
                            "choices": [{
                                "message": {
                                    "role": "assistant",
                                    "content": "Recovered with fallback model"
                                }
                            }]
                        }
                    },
                    "telemetry": {
                        "elapsed_ms": 12
                    }
                })
            };
            Json(payload)
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
            "provider_id": "provider-openai",
            "failover_models": ["gpt-4.1", "gpt-4o-mini"],
            "api_format": "openai:chat"
        }))
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["success"], json!(true));
    assert_eq!(payload["total_attempts"], json!(2));
    let attempts = payload["attempts"]
        .as_array()
        .expect("attempts should be an array");
    assert_eq!(attempts[0]["effective_model"], json!("gpt-4.1"));
    assert_eq!(attempts[1]["effective_model"], json!("gpt-4o-mini"));
    assert_eq!(
        payload["data"]["response"]["choices"][0]["message"]["content"],
        json!("Recovered with fallback model")
    );

    gateway_handle.abort();
    execution_runtime_handle.abort();
}

#[test]
fn gateway_handles_openai_responses_test_model_failover_locally() {
    run_provider_query_test(
        "gateway_handles_openai_responses_test_model_failover_locally",
        gateway_handles_openai_responses_test_model_failover_locally_impl,
    );
}

async fn gateway_handles_openai_responses_test_model_failover_locally_impl() {
    let execution_runtime = Router::new().route(
        "/v1/execute/sync",
        any(move |Json(plan): Json<ExecutionPlan>| async move {
            assert_eq!(plan.provider_id, "provider-openai");
            assert_eq!(plan.endpoint_id, "endpoint-openai-cli");
            assert_eq!(plan.key_id, "key-openai-cli");
            assert_eq!(plan.provider_api_format, "openai:responses");
            assert_eq!(plan.model_name.as_deref(), Some("gpt-5.4-mini"));
            Json(json!({
                "request_id": plan.request_id,
                "candidate_id": plan.candidate_id,
                "status_code": 200,
                "headers": {
                    "content-type": "application/json"
                },
                "body": {
                    "json_body": {
                        "id": "chatcmpl-openai-cli-failover",
                        "choices": [{
                            "message": {
                                "role": "assistant",
                                "content": "OpenAI Responses failover path succeeded"
                            }
                        }]
                    }
                },
                "telemetry": {
                    "elapsed_ms": 15
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
            "endpoint-openai-cli",
            "provider-openai",
            "openai:responses",
            "https://api.openai.example/v1",
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
            "provider_id": "provider-openai",
            "failover_models": ["gpt-5.4-mini"],
            "api_format": "openai:responses"
        }))
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["success"], json!(true));
    assert_eq!(payload["total_attempts"], json!(1));
    assert_eq!(
        payload["data"]["response"]["choices"][0]["message"]["content"],
        json!("OpenAI Responses failover path succeeded")
    );

    gateway_handle.abort();
    execution_runtime_handle.abort();
}

#[test]
fn gateway_handles_claude_cli_test_model_failover_locally() {
    run_provider_query_test(
        "gateway_handles_claude_cli_test_model_failover_locally",
        gateway_handles_claude_cli_test_model_failover_locally_impl,
    );
}

async fn gateway_handles_claude_cli_test_model_failover_locally_impl() {
    let execution_runtime = Router::new().route(
        "/v1/execute/sync",
        any(move |Json(plan): Json<ExecutionPlan>| async move {
            assert_eq!(plan.provider_id, "provider-claude");
            assert_eq!(plan.endpoint_id, "endpoint-claude-cli");
            assert_eq!(plan.key_id, "key-claude-cli");
            assert_eq!(plan.provider_api_format, "claude:messages");
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
                        "id": "chatcmpl-claude-cli-failover",
                        "choices": [{
                            "message": {
                                "role": "assistant",
                                "content": "Claude CLI failover path succeeded"
                            }
                        }]
                    }
                },
                "telemetry": {
                    "elapsed_ms": 16
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
        .post(format!(
            "{gateway_url}/api/admin/provider-query/test-model-failover"
        ))
        .header(GATEWAY_HEADER, "rust-phase3b")
        .header(TRUSTED_ADMIN_USER_ID_HEADER, "admin-user-123")
        .header(TRUSTED_ADMIN_USER_ROLE_HEADER, "admin")
        .header(TRUSTED_ADMIN_SESSION_ID_HEADER, "session-123")
        .json(&json!({
            "provider_id": "provider-claude",
            "failover_models": ["claude-sonnet-4-5"],
            "api_format": "claude:messages"
        }))
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["success"], json!(true));
    assert_eq!(payload["total_attempts"], json!(1));
    assert_eq!(
        payload["data"]["response"]["choices"][0]["message"]["content"],
        json!("Claude CLI failover path succeeded")
    );

    gateway_handle.abort();
    execution_runtime_handle.abort();
}

#[test]
fn gateway_handles_gemini_cli_test_model_failover_locally() {
    run_provider_query_test(
        "gateway_handles_gemini_cli_test_model_failover_locally",
        gateway_handles_gemini_cli_test_model_failover_locally_impl,
    );
}

async fn gateway_handles_gemini_cli_test_model_failover_locally_impl() {
    let execution_runtime = Router::new().route(
        "/v1/execute/sync",
        any(move |Json(plan): Json<ExecutionPlan>| async move {
            assert_eq!(plan.provider_id, "provider-gemini");
            assert_eq!(plan.endpoint_id, "endpoint-gemini-cli");
            assert_eq!(plan.key_id, "key-gemini-cli");
            assert_eq!(plan.provider_api_format, "gemini:generate_content");
            assert_eq!(plan.model_name.as_deref(), Some("gemini-2.5-pro"));
            Json(json!({
                "request_id": plan.request_id,
                "candidate_id": plan.candidate_id,
                "status_code": 200,
                "headers": {
                    "content-type": "application/json"
                },
                "body": {
                    "json_body": {
                        "id": "chatcmpl-gemini-cli-failover",
                        "choices": [{
                            "message": {
                                "role": "assistant",
                                "content": "Gemini CLI failover path succeeded"
                            }
                        }]
                    }
                },
                "telemetry": {
                    "elapsed_ms": 23
                }
            }))
        }),
    );

    let (execution_runtime_url, execution_runtime_handle) = start_server(execution_runtime).await;
    let mut provider = sample_provider("provider-gemini", "Gemini", 10);
    provider.provider_type = "google".to_string();
    let provider_catalog_repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![provider],
        vec![sample_endpoint(
            "endpoint-gemini-cli",
            "provider-gemini",
            "gemini:generate_content",
            "https://generativelanguage.googleapis.com",
        )],
        vec![sample_bound_key(
            "key-gemini-cli",
            "provider-gemini",
            "gemini:generate_content",
            "sk-test-gemini-cli",
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
            "provider_id": "provider-gemini",
            "failover_models": ["gemini-2.5-pro"],
            "api_format": "gemini:generate_content"
        }))
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["success"], json!(true));
    assert_eq!(payload["total_attempts"], json!(1));
    assert_eq!(
        payload["data"]["response"]["choices"][0]["message"]["content"],
        json!("Gemini CLI failover path succeeded")
    );

    gateway_handle.abort();
    execution_runtime_handle.abort();
}

#[test]
fn gateway_unwraps_gemini_cli_v1internal_response_for_failover_model_test() {
    run_provider_query_test(
        "gateway_unwraps_gemini_cli_v1internal_response_for_failover_model_test",
        gateway_unwraps_gemini_cli_v1internal_response_for_failover_model_test_impl,
    );
}

async fn gateway_unwraps_gemini_cli_v1internal_response_for_failover_model_test_impl() {
    let execution_runtime = Router::new().route(
        "/v1/execute/sync",
        any(move |Json(plan): Json<ExecutionPlan>| async move {
            assert_eq!(plan.provider_id, "provider-gemini-cli");
            assert_eq!(plan.endpoint_id, "endpoint-gemini-cli");
            assert_eq!(plan.key_id, "key-gemini-cli");
            assert_eq!(plan.provider_api_format, "gemini:generate_content");
            assert!(!plan.stream);
            assert_eq!(
                plan.url,
                "https://cloudcode-pa.googleapis.com/v1internal:generateContent"
            );
            assert_eq!(
                plan.body.json_body.as_ref().unwrap()["project"],
                json!("project-1")
            );
            assert_eq!(
                plan.body.json_body.as_ref().unwrap()["model"],
                json!("gemini-3-flash-preview")
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
                                    "parts": [{"text":"Gemini CLI v1internal failover response"}],
                                    "role": "model"
                                },
                                "finishReason": "STOP",
                                "index": 0
                            }],
                            "modelVersion": "gemini-3-flash-preview",
                            "usageMetadata": {
                                "promptTokenCount": 2,
                                "candidatesTokenCount": 5,
                                "totalTokenCount": 7
                            }
                        },
                        "remainingCredits": 123,
                        "consumedCredits": 1,
                        "traceId": "trace-gemini-cli-1"
                    }
                },
                "telemetry": {
                    "elapsed_ms": 23
                }
            }))
        }),
    );

    let (execution_runtime_url, execution_runtime_handle) = start_server(execution_runtime).await;
    let mut provider = sample_provider("provider-gemini-cli", "Gemini CLI", 10);
    provider.provider_type = "gemini_cli".to_string();
    let mut key = sample_bound_key(
        "key-gemini-cli",
        "provider-gemini-cli",
        "gemini:generate_content",
        "cached-gemini-cli-token",
    );
    key.auth_type = "oauth".to_string();
    key.encrypted_auth_config = Some(sample_bound_auth_config(
        "provider-gemini-cli",
        "key-gemini-cli",
        r#"{"provider_type":"gemini_cli","project_id":"project-1"}"#,
    ));
    let provider_catalog_repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![provider],
        vec![sample_endpoint(
            "endpoint-gemini-cli",
            "provider-gemini-cli",
            "gemini:generate_content",
            "https://cloudcode-pa.googleapis.com",
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
        .post(format!(
            "{gateway_url}/api/admin/provider-query/test-model-failover"
        ))
        .header(GATEWAY_HEADER, "rust-phase3b")
        .header(TRUSTED_ADMIN_USER_ID_HEADER, "admin-user-123")
        .header(TRUSTED_ADMIN_USER_ROLE_HEADER, "admin")
        .header(TRUSTED_ADMIN_SESSION_ID_HEADER, "session-123")
        .json(&json!({
            "provider_id": "provider-gemini-cli",
            "failover_models": ["gemini-3-flash-preview"],
            "api_format": "gemini:generate_content"
        }))
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["success"], json!(true));
    assert_eq!(payload["total_attempts"], json!(1));
    assert_eq!(
        payload["data"]["response"]["candidates"][0]["content"]["parts"][0]["text"],
        json!("Gemini CLI v1internal failover response")
    );
    assert!(payload["data"]["response"].get("response").is_none());

    gateway_handle.abort();
    execution_runtime_handle.abort();
}

#[test]
fn gateway_handles_admin_provider_query_test_model_failover_with_single_model_name_alias() {
    run_provider_query_test(
        "gateway_handles_admin_provider_query_test_model_failover_with_single_model_name_alias",
        gateway_handles_admin_provider_query_test_model_failover_with_single_model_name_alias_impl,
    );
}

async fn gateway_handles_admin_provider_query_test_model_failover_with_single_model_name_alias_impl(
) {
    let execution_runtime = Router::new().route(
        "/v1/execute/sync",
        any(move |Json(plan): Json<ExecutionPlan>| async move {
            assert_eq!(plan.model_name.as_deref(), Some("gpt-4.1"));
            Json(json!({
                "request_id": plan.request_id,
                "candidate_id": plan.candidate_id,
                "status_code": 200,
                "headers": {
                    "content-type": "application/json"
                },
                "body": {
                    "json_body": {
                        "id": "chatcmpl-alias",
                        "choices": [{
                            "message": {
                                "role": "assistant",
                                "content": "Alias path succeeded"
                            }
                        }]
                    }
                },
                "telemetry": {
                    "elapsed_ms": 9
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
            "key-openai-alias",
            "provider-openai",
            "openai:chat",
            "sk-test-alias",
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
            "provider_id": "provider-openai",
            "failover_models": ["gpt-4.1"],
            "api_format": "openai:chat"
        }))
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["success"], json!(true));
    assert_eq!(payload["model"], json!("gpt-4.1"));
    assert_eq!(payload["total_attempts"], json!(1));
    assert_eq!(
        payload["data"]["response"]["choices"][0]["message"]["content"],
        json!("Alias path succeeded")
    );

    gateway_handle.abort();
    execution_runtime_handle.abort();
}

#[test]
fn gateway_retries_non_kiro_failover_after_http_error_without_message() {
    run_provider_query_test(
        "gateway_retries_non_kiro_failover_after_http_error_without_message",
        gateway_retries_non_kiro_failover_after_http_error_without_message_impl,
    );
}

async fn gateway_retries_non_kiro_failover_after_http_error_without_message_impl() {
    let execution_runtime = Router::new().route(
        "/v1/execute/sync",
        any(move |Json(plan): Json<ExecutionPlan>| async move {
            let auth = plan
                .headers
                .get("authorization")
                .map(String::as_str)
                .unwrap_or_default()
                .to_string();
            let payload = if auth == "Bearer sk-test-first" {
                json!({
                    "request_id": plan.request_id,
                    "candidate_id": plan.candidate_id,
                    "status_code": 500,
                    "headers": {
                        "content-type": "application/json"
                    },
                    "body": {
                        "json_body": {}
                    },
                    "telemetry": {
                        "elapsed_ms": 7
                    }
                })
            } else {
                json!({
                    "request_id": plan.request_id,
                    "candidate_id": plan.candidate_id,
                    "status_code": 200,
                    "headers": {
                        "content-type": "application/json"
                    },
                    "body": {
                        "json_body": {
                            "id": "chatcmpl-retry",
                            "choices": [{
                                "message": {
                                    "role": "assistant",
                                    "content": "Recovered after empty error"
                                }
                            }]
                        }
                    },
                    "telemetry": {
                        "elapsed_ms": 13
                    }
                })
            };
            Json(payload)
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
        vec![
            sample_bound_key(
                "key-openai-first",
                "provider-openai",
                "openai:chat",
                "sk-test-first",
            ),
            sample_bound_key(
                "key-openai-second",
                "provider-openai",
                "openai:chat",
                "sk-test-second",
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
        .post(format!(
            "{gateway_url}/api/admin/provider-query/test-model-failover"
        ))
        .header(GATEWAY_HEADER, "rust-phase3b")
        .header(TRUSTED_ADMIN_USER_ID_HEADER, "admin-user-123")
        .header(TRUSTED_ADMIN_USER_ROLE_HEADER, "admin")
        .header(TRUSTED_ADMIN_SESSION_ID_HEADER, "session-123")
        .json(&json!({
            "provider_id": "provider-openai",
            "failover_models": ["gpt-4.1"],
            "api_format": "openai:chat"
        }))
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["success"], json!(true));
    assert_eq!(payload["total_attempts"], json!(2));
    let attempts = payload["attempts"]
        .as_array()
        .expect("attempts should be an array");
    assert_eq!(attempts[0]["status"], json!("failed"));
    assert_eq!(attempts[0]["status_code"], json!(500));
    assert_eq!(attempts[1]["status"], json!("success"));

    gateway_handle.abort();
    execution_runtime_handle.abort();
}

#[test]
fn gateway_rejects_admin_provider_query_test_model_without_provider_id() {
    run_provider_query_test(
        "gateway_rejects_admin_provider_query_test_model_without_provider_id",
        gateway_rejects_admin_provider_query_test_model_without_provider_id_impl,
    );
}

async fn gateway_rejects_admin_provider_query_test_model_without_provider_id_impl() {
    assert_admin_provider_query_route(
        "/api/admin/provider-query/test-model",
        json!({ "model": "gpt-4.1" }),
        StatusCode::BAD_REQUEST,
        |payload| {
            assert_eq!(payload["detail"], json!("provider_id is required"));
        },
    )
    .await;
}

#[test]
fn gateway_rejects_admin_provider_query_test_model_failover_without_provider_id() {
    run_provider_query_test(
        "gateway_rejects_admin_provider_query_test_model_failover_without_provider_id",
        gateway_rejects_admin_provider_query_test_model_failover_without_provider_id_impl,
    );
}

async fn gateway_rejects_admin_provider_query_test_model_failover_without_provider_id_impl() {
    assert_admin_provider_query_route(
        "/api/admin/provider-query/test-model-failover",
        json!({ "failover_models": ["gpt-4.1"] }),
        StatusCode::BAD_REQUEST,
        |payload| {
            assert_eq!(payload["detail"], json!("provider_id is required"));
        },
    )
    .await;
}

#[test]
fn gateway_rejects_admin_provider_query_test_model_failover_without_models() {
    run_provider_query_test(
        "gateway_rejects_admin_provider_query_test_model_failover_without_models",
        gateway_rejects_admin_provider_query_test_model_failover_without_models_impl,
    );
}

async fn gateway_rejects_admin_provider_query_test_model_failover_without_models_impl() {
    assert_admin_provider_query_route(
        "/api/admin/provider-query/test-model-failover",
        json!({ "provider_id": "provider-openai", "failover_models": [] }),
        StatusCode::BAD_REQUEST,
        |payload| {
            assert_eq!(
                payload["detail"],
                json!("failover_models should not be empty")
            );
        },
    )
    .await;
}

#[test]
fn gateway_retries_non_kiro_failover_after_success_status_without_body() {
    run_provider_query_test(
        "gateway_retries_non_kiro_failover_after_success_status_without_body",
        gateway_retries_non_kiro_failover_after_success_status_without_body_impl,
    );
}

async fn gateway_retries_non_kiro_failover_after_success_status_without_body_impl() {
    let execution_runtime = Router::new().route(
        "/v1/execute/sync",
        any(move |Json(plan): Json<ExecutionPlan>| async move {
            let auth = plan
                .headers
                .get("authorization")
                .map(String::as_str)
                .unwrap_or_default()
                .to_string();
            let payload = if auth == "Bearer sk-test-first" {
                json!({
                    "request_id": plan.request_id,
                    "candidate_id": plan.candidate_id,
                    "status_code": 200,
                    "headers": {
                        "content-type": "application/json"
                    },
                    "body": {},
                    "telemetry": {
                        "elapsed_ms": 6
                    }
                })
            } else {
                json!({
                    "request_id": plan.request_id,
                    "candidate_id": plan.candidate_id,
                    "status_code": 200,
                    "headers": {
                        "content-type": "application/json"
                    },
                    "body": {
                        "json_body": {
                            "id": "chatcmpl-retry-empty-body",
                            "choices": [{
                                "message": {
                                    "role": "assistant",
                                    "content": "Recovered after empty success body"
                                }
                            }]
                        }
                    },
                    "telemetry": {
                        "elapsed_ms": 14
                    }
                })
            };
            Json(payload)
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
        vec![
            sample_bound_key(
                "key-openai-first",
                "provider-openai",
                "openai:chat",
                "sk-test-first",
            ),
            sample_bound_key(
                "key-openai-second",
                "provider-openai",
                "openai:chat",
                "sk-test-second",
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
        .post(format!(
            "{gateway_url}/api/admin/provider-query/test-model-failover"
        ))
        .header(GATEWAY_HEADER, "rust-phase3b")
        .header(TRUSTED_ADMIN_USER_ID_HEADER, "admin-user-123")
        .header(TRUSTED_ADMIN_USER_ROLE_HEADER, "admin")
        .header(TRUSTED_ADMIN_SESSION_ID_HEADER, "session-123")
        .json(&json!({
            "provider_id": "provider-openai",
            "failover_models": ["gpt-4.1"],
            "api_format": "openai:chat"
        }))
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["success"], json!(true));
    assert_eq!(payload["total_attempts"], json!(2));
    assert_eq!(
        payload["data"]["response"]["choices"][0]["message"]["content"],
        json!("Recovered after empty success body")
    );
    let attempts = payload["attempts"]
        .as_array()
        .expect("attempts should be an array");
    assert_eq!(attempts[0]["status"], json!("failed"));
    assert_eq!(attempts[0]["status_code"], json!(200));
    assert_eq!(
        attempts[0]["error_message"],
        // Attempt diagnostics intentionally expose only the status class;
        // detailed provider response text is not returned to the admin UI.
        json!("HTTP 200")
    );
    assert_eq!(attempts[1]["status"], json!("success"));

    gateway_handle.abort();
    execution_runtime_handle.abort();
}
