use super::*;
use crate::tests::support::sample_auth_snapshot_openai_gemini as sample_auth_snapshot;
use crate::tests::DEV_FERNET_KEY;

large_stack_async_test!(
    gateway_returns_openai_chat_error_for_local_cross_format_gemini_sync_failure,
    gateway_returns_openai_chat_error_for_local_cross_format_gemini_sync_failure_impl
);

large_stack_async_test!(
    gateway_returns_openai_chat_error_for_local_cross_format_claude_sync_failure,
    gateway_returns_openai_chat_error_for_local_cross_format_claude_sync_failure_impl
);

large_stack_async_test!(
    gateway_returns_openai_chat_error_for_local_cross_format_gemini_cli_sync_failure,
    gateway_returns_openai_chat_error_for_local_cross_format_gemini_cli_sync_failure_impl
);

large_stack_async_test!(
    gateway_returns_openai_chat_error_for_local_cross_format_claude_cli_sync_failure,
    gateway_returns_openai_chat_error_for_local_cross_format_claude_cli_sync_failure_impl
);

large_stack_async_test!(
    gateway_executes_openai_chat_sync_via_local_cross_format_gemini_candidate_without_external_control_config,
    gateway_executes_openai_chat_sync_via_local_cross_format_gemini_candidate_without_external_control_config_impl
);

async fn gateway_executes_openai_chat_sync_via_local_cross_format_gemini_candidate_without_external_control_config_impl(
) {
    #[derive(Debug, Clone)]
    struct SeenExecutionRuntimeSyncRequest {
        trace_id: String,
        url: String,
        provider_model: String,
        auth_header_value: String,
        endpoint_tag: String,
        system_text: String,
        user_text: String,
        max_output_tokens: u64,
        temperature: f64,
    }
    fn sample_candidate_row() -> StoredMinimalCandidateSelectionRow {
        crate::tests::support::minimal_candidate_row(&crate::tests::support::CandidateRowSpec {
            id_suffix: "openai-chat-gemini-local-1",
            provider_name: "gemini",
            provider_type: "custom",
            api_format: "gemini:generate_content",
            api_family: "gemini",
            endpoint_kind: Some("chat"),
            key_name: "prod",
            key_auth_type: "api_key",
            global_model_name: "gpt-5",
            upstream_model: "gemini-2.5-pro-upstream",
        })
    }

    fn sample_disabled_conversion_candidate_row() -> StoredMinimalCandidateSelectionRow {
        let mut row = sample_candidate_row();
        row.provider_id = "provider-openai-chat-gemini-local-disabled".to_string();
        row.provider_priority = 20;
        row.endpoint_id = "endpoint-openai-chat-gemini-local-disabled".to_string();
        row.key_id = "key-openai-chat-gemini-local-disabled".to_string();
        row.model_id = "model-openai-chat-gemini-local-disabled".to_string();
        row.global_model_id = "global-model-openai-chat-gemini-local-disabled".to_string();
        row.model_provider_model_name = "gemini-2.5-flash-upstream".to_string();
        row.model_provider_model_mappings = Some(vec![StoredProviderModelMapping {
            name: "gemini-2.5-flash-upstream".to_string(),
            priority: 2,
            api_formats: Some(vec!["gemini:generate_content".to_string()]),
            endpoint_ids: None,
            operations: None,
        }]);
        row
    }

    fn sample_provider_catalog_provider() -> StoredProviderCatalogProvider {
        StoredProviderCatalogProvider::new(
            "provider-openai-chat-gemini-local-1".to_string(),
            "gemini".to_string(),
            Some("https://example.com".to_string()),
            "custom".to_string(),
        )
        .expect("provider should build")
        .with_transport_fields(
            true,
            false,
            true,
            None,
            Some(2),
            None,
            Some(20.0),
            None,
            None,
        )
    }

    fn sample_disabled_conversion_provider_catalog_provider() -> StoredProviderCatalogProvider {
        StoredProviderCatalogProvider::new(
            "provider-openai-chat-gemini-local-disabled".to_string(),
            "gemini".to_string(),
            Some("https://example.com".to_string()),
            "custom".to_string(),
        )
        .expect("provider should build")
        .with_transport_fields(
            true,
            false,
            false,
            None,
            Some(2),
            None,
            Some(20.0),
            None,
            None,
        )
    }

    fn sample_provider_catalog_endpoint() -> StoredProviderCatalogEndpoint {
        StoredProviderCatalogEndpoint::new(
            "endpoint-openai-chat-gemini-local-1".to_string(),
            "provider-openai-chat-gemini-local-1".to_string(),
            "gemini:generate_content".to_string(),
            Some("gemini".to_string()),
            Some("chat".to_string()),
            true,
        )
        .expect("endpoint should build")
        .with_transport_fields(
            "https://generativelanguage.googleapis.com".to_string(),
            Some(serde_json::json!([
                {"action":"set","key":"x-endpoint-tag","value":"openai-chat-gemini-cross-format"}
            ])),
            None,
            Some(2),
            Some("/custom/v1beta/models/gemini-2.5-pro-upstream:generateContent".to_string()),
            None,
            None,
            None,
        )
        .expect("endpoint transport should build")
    }

    fn sample_disabled_conversion_provider_catalog_endpoint() -> StoredProviderCatalogEndpoint {
        StoredProviderCatalogEndpoint::new(
            "endpoint-openai-chat-gemini-local-disabled".to_string(),
            "provider-openai-chat-gemini-local-disabled".to_string(),
            "gemini:generate_content".to_string(),
            Some("gemini".to_string()),
            Some("chat".to_string()),
            true,
        )
        .expect("endpoint should build")
        .with_transport_fields(
            "https://generativelanguage.googleapis.com".to_string(),
            None,
            None,
            Some(2),
            Some("/custom/v1beta/models/gemini-2.5-flash-upstream:generateContent".to_string()),
            None,
            None,
            None,
        )
        .expect("endpoint transport should build")
    }

    fn sample_provider_catalog_key() -> StoredProviderCatalogKey {
        crate::tests::support::provider_catalog_key(&crate::tests::support::CatalogKeySpec {
            id_suffix: "openai-chat-gemini-local-1",
            key_name: "prod",
            auth_type: "api_key",
            api_format: "gemini:generate_content",
            secret: "sk-upstream-openai-chat-gemini",
        })
    }

    fn sample_disabled_conversion_provider_catalog_key() -> StoredProviderCatalogKey {
        StoredProviderCatalogKey::new(
            "key-openai-chat-gemini-local-disabled".to_string(),
            "provider-openai-chat-gemini-local-disabled".to_string(),
            "prod".to_string(),
            "api_key".to_string(),
            None,
            true,
        )
        .expect("key should build")
        .with_transport_fields(
            Some(serde_json::json!(["gemini:generate_content"])),
            encrypt_python_fernet_plaintext(
                DEV_FERNET_KEY,
                "sk-upstream-openai-chat-gemini-disabled",
            )
            .expect("api key should encrypt"),
            None,
            None,
            Some(serde_json::json!({"gemini:generate_content": 2})),
            None,
            None,
            None,
            None,
        )
        .expect("key transport should build")
    }

    let seen_execution_runtime = Arc::new(Mutex::new(None::<SeenExecutionRuntimeSyncRequest>));
    let seen_execution_runtime_clone = Arc::clone(&seen_execution_runtime);
    let seen_report = Arc::new(Mutex::new(false));
    let seen_report_clone = Arc::clone(&seen_report);
    let decision_hits = Arc::new(Mutex::new(0usize));
    let decision_hits_clone = Arc::clone(&decision_hits);
    let plan_hits = Arc::new(Mutex::new(0usize));
    let plan_hits_clone = Arc::clone(&plan_hits);
    let public_hits = Arc::new(Mutex::new(0usize));
    let public_hits_clone = Arc::clone(&public_hits);
    let request_candidate_repository = Arc::new(InMemoryRequestCandidateRepository::default());

    let upstream = Router::new()
        .route(
            "/api/internal/gateway/decision-sync",
            any(move |_request: Request| {
                let decision_hits_inner = Arc::clone(&decision_hits_clone);
                async move {
                    *decision_hits_inner.lock().expect("mutex should lock") += 1;
                    Json(json!({"action": "proxy_public"}))
                }
            }),
        )
        .route(
            "/api/internal/gateway/plan-sync",
            any(move |_request: Request| {
                let plan_hits_inner = Arc::clone(&plan_hits_clone);
                async move {
                    *plan_hits_inner.lock().expect("mutex should lock") += 1;
                    Json(json!({"action": "proxy_public"}))
                }
            }),
        )
        .route(
            "/api/internal/gateway/report-sync",
            any(move |request: Request| {
                let seen_report_inner = Arc::clone(&seen_report_clone);
                async move {
                    let (_parts, body) = request.into_parts();
                    let _raw_body = to_bytes(body, usize::MAX).await.expect("body should read");
                    *seen_report_inner.lock().expect("mutex should lock") = true;
                    Json(json!({"ok": true}))
                }
            }),
        )
        .route(
            "/v1/chat/completions",
            any(move |_request: Request| {
                let public_hits_inner = Arc::clone(&public_hits_clone);
                async move {
                    *public_hits_inner.lock().expect("mutex should lock") += 1;
                    (StatusCode::IM_A_TEAPOT, Body::from("public-route-hit"))
                }
            }),
        );

    let execution_runtime = Router::new().route(
        "/v1/execute/sync",
        any(move |request: Request| {
            let seen_execution_runtime_inner = Arc::clone(&seen_execution_runtime_clone);
            async move {
                let (parts, body) = request.into_parts();
                let raw_body = to_bytes(body, usize::MAX).await.expect("body should read");
                let payload: serde_json::Value = serde_json::from_slice(&raw_body)
                    .expect("execution runtime payload should parse");
                *seen_execution_runtime_inner
                    .lock()
                    .expect("mutex should lock") = Some(SeenExecutionRuntimeSyncRequest {
                    trace_id: crate::tests::support::header_string(&parts.headers, TRACE_ID_HEADER),
                    url: crate::tests::support::payload_string(&payload, &["url"]),
                    provider_model: payload
                        .get("body")
                        .and_then(|value| value.get("json_body"))
                        .and_then(|value| value.get("model"))
                        .and_then(|value| value.as_str())
                        .unwrap_or_default()
                        .to_string(),
                    auth_header_value: crate::tests::support::payload_string(
                        &payload,
                        &["headers", "x-goog-api-key"],
                    ),
                    endpoint_tag: crate::tests::support::payload_string(
                        &payload,
                        &["headers", "x-endpoint-tag"],
                    ),
                    system_text: payload
                        .get("body")
                        .and_then(|value| value.get("json_body"))
                        .and_then(|value| value.get("systemInstruction"))
                        .and_then(|value| value.get("parts"))
                        .and_then(|value| value.as_array())
                        .and_then(|value| value.first())
                        .and_then(|value| value.get("text"))
                        .and_then(|value| value.as_str())
                        .unwrap_or_default()
                        .to_string(),
                    user_text: payload
                        .get("body")
                        .and_then(|value| value.get("json_body"))
                        .and_then(|value| value.get("contents"))
                        .and_then(|value| value.as_array())
                        .and_then(|value| value.first())
                        .and_then(|value| value.get("parts"))
                        .and_then(|value| value.as_array())
                        .and_then(|value| value.first())
                        .and_then(|value| value.get("text"))
                        .and_then(|value| value.as_str())
                        .unwrap_or_default()
                        .to_string(),
                    max_output_tokens: payload
                        .get("body")
                        .and_then(|value| value.get("json_body"))
                        .and_then(|value| value.get("generationConfig"))
                        .and_then(|value| value.get("maxOutputTokens"))
                        .and_then(|value| value.as_u64())
                        .unwrap_or_default(),
                    temperature: payload
                        .get("body")
                        .and_then(|value| value.get("json_body"))
                        .and_then(|value| value.get("generationConfig"))
                        .and_then(|value| value.get("temperature"))
                        .and_then(|value| value.as_f64())
                        .unwrap_or_default(),
                });
                Json(json!({
                    "request_id": "trace-openai-chat-gemini-local-123",
                    "status_code": 200,
                    "headers": {
                        "content-type": "application/json"
                    },
                    "body": {
                        "json_body": {
                            "responseId": "resp_openai_chat_gemini_local_123",
                            "candidates": [{
                                "content": {
                                    "parts": [{"text": "Hello Gemini direct"}],
                                    "role": "model"
                                },
                                "finishReason": "STOP",
                                "index": 0
                            }],
                            "modelVersion": "gemini-2.5-pro-upstream",
                            "usageMetadata": {
                                "promptTokenCount": 1,
                                "candidatesTokenCount": 2,
                                "totalTokenCount": 3
                            }
                        }
                    },
                    "telemetry": {
                        "elapsed_ms": 25
                    }
                }))
            }
        }),
    );

    let auth_repository = Arc::new(InMemoryAuthApiKeySnapshotRepository::seed(vec![(
        Some(hash_api_key("sk-client-openai-chat-gemini")),
        sample_auth_snapshot(
            "api-key-openai-chat-gemini-local-1",
            "user-openai-chat-gemini-local-1",
        ),
    )]));
    let candidate_selection_repository =
        Arc::new(InMemoryMinimalCandidateSelectionReadRepository::seed(vec![
            sample_candidate_row(),
            sample_disabled_conversion_candidate_row(),
        ]));
    let provider_catalog_repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![
            sample_provider_catalog_provider(),
            sample_disabled_conversion_provider_catalog_provider(),
        ],
        vec![
            sample_provider_catalog_endpoint(),
            sample_disabled_conversion_provider_catalog_endpoint(),
        ],
        vec![
            sample_provider_catalog_key(),
            sample_disabled_conversion_provider_catalog_key(),
        ],
    ));

    let (_upstream_url, upstream_handle) = start_server(upstream).await;
    let (execution_runtime_url, execution_runtime_handle) = start_server(execution_runtime).await;
    let gateway_state =
        build_state_with_execution_runtime_override(execution_runtime_url.clone())
    .with_data_state_for_tests(
        crate::data::GatewayDataState::with_auth_candidate_selection_provider_catalog_and_request_candidate_repository_for_tests(
            auth_repository,
            candidate_selection_repository,
            provider_catalog_repository,
            Arc::clone(&request_candidate_repository),
            DEV_FERNET_KEY,
        ),
    );
    let gateway = build_router_with_state(gateway_state);
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .post(format!("{gateway_url}/v1/chat/completions"))
        .header(http::header::CONTENT_TYPE, "application/json")
        .header(
            http::header::AUTHORIZATION,
            "Bearer sk-client-openai-chat-gemini",
        )
        .header(TRACE_ID_HEADER, "trace-openai-chat-gemini-local-123")
        .body(
            "{\"model\":\"gpt-5\",\"messages\":[{\"role\":\"system\",\"content\":\"You are terse.\"},{\"role\":\"user\",\"content\":\"Say hello\"}],\"temperature\":0.2,\"max_tokens\":64}",
        )
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get(EXECUTION_PATH_HEADER)
            .and_then(|value| value.to_str().ok()),
        Some(EXECUTION_PATH_EXECUTION_RUNTIME_SYNC)
    );
    let response_json: serde_json::Value = response.json().await.expect("body should parse");
    assert_eq!(
        response_json,
        json!({
            "id": "resp_openai_chat_gemini_local_123",
            "object": "chat.completion",
            "model": "gemini-2.5-pro-upstream",
            "choices": [{
                "index": 0,
                "message": {
                    "role": "assistant",
                    "content": "Hello Gemini direct"
                },
                "finish_reason": "stop"
            }],
            "usage": {
                "prompt_tokens": 1,
                "completion_tokens": 2,
                "total_tokens": 3
            }
        })
    );

    let seen_execution_runtime_request = seen_execution_runtime
        .lock()
        .expect("mutex should lock")
        .clone()
        .expect("execution runtime sync should be captured");
    assert_eq!(
        seen_execution_runtime_request.trace_id,
        "trace-openai-chat-gemini-local-123"
    );
    assert_eq!(
        seen_execution_runtime_request.url,
        "https://generativelanguage.googleapis.com/custom/v1beta/models/gemini-2.5-pro-upstream:generateContent"
    );
    assert_eq!(
        seen_execution_runtime_request.provider_model,
        "gemini-2.5-pro-upstream"
    );
    assert_eq!(
        seen_execution_runtime_request.auth_header_value,
        "sk-upstream-openai-chat-gemini"
    );
    assert_eq!(
        seen_execution_runtime_request.endpoint_tag,
        "openai-chat-gemini-cross-format"
    );
    assert_eq!(seen_execution_runtime_request.system_text, "You are terse.");
    assert_eq!(seen_execution_runtime_request.user_text, "Say hello");
    assert_eq!(seen_execution_runtime_request.max_output_tokens, 64);
    assert!((seen_execution_runtime_request.temperature - 0.2).abs() < f64::EPSILON);

    let stored_candidates = request_candidate_repository
        .list_by_request_id("trace-openai-chat-gemini-local-123")
        .await
        .expect("request candidate trace should read");
    assert_eq!(stored_candidates.len(), 1);
    assert_eq!(
        stored_candidates
            .iter()
            .filter(|candidate| candidate.status == RequestCandidateStatus::Success)
            .count(),
        1
    );
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    assert!(
        !*seen_report.lock().expect("mutex should lock"),
        "report-sync should stay local when request candidate persistence is available"
    );

    assert_eq!(*decision_hits.lock().expect("mutex should lock"), 0);
    assert_eq!(*plan_hits.lock().expect("mutex should lock"), 0);
    assert_eq!(*public_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    execution_runtime_handle.abort();
    upstream_handle.abort();
}

async fn gateway_returns_openai_chat_error_for_local_cross_format_claude_cli_sync_failure_impl() {
    #[derive(Debug, Clone)]
    struct SeenExecutionRuntimeSyncRequest {
        trace_id: String,
        url: String,
        body: serde_json::Value,
        client_api_format: String,
        provider_api_format: String,
        auth_header_value: String,
        anthropic_version: String,
        endpoint_tag: String,
    }

    fn sample_auth_snapshot(api_key_id: &str, user_id: &str) -> StoredAuthApiKeySnapshot {
        crate::tests::support::sample_auth_snapshot_with(
            api_key_id,
            user_id,
            "alice",
            "alice@example.com",
            serde_json::json!(["openai", "claude", "claude_code"]),
            serde_json::json!(["openai:chat"]),
            serde_json::json!(["gpt-5"]),
        )
    }

    fn sample_candidate_row() -> StoredMinimalCandidateSelectionRow {
        crate::tests::support::minimal_candidate_row(&crate::tests::support::CandidateRowSpec {
            id_suffix: "openai-chat-claude-cli-local-1",
            provider_name: "claude_code",
            provider_type: "claude_code",
            api_format: "claude:messages",
            api_family: "claude",
            endpoint_kind: Some("cli"),
            key_name: "prod",
            key_auth_type: "oauth",
            global_model_name: "gpt-5",
            upstream_model: "claude-code-upstream",
        })
    }

    fn sample_provider_catalog_provider() -> StoredProviderCatalogProvider {
        StoredProviderCatalogProvider::new(
            "provider-openai-chat-claude-cli-local-1".to_string(),
            "claude_code".to_string(),
            Some("https://example.com".to_string()),
            "claude_code".to_string(),
        )
        .expect("provider should build")
        .with_transport_fields(
            true,
            false,
            true,
            None,
            Some(2),
            None,
            Some(20.0),
            None,
            Some(serde_json::json!({
                "claude_code_advanced": {
                    "cli_only_enabled": false
                },
                "failover_rules": {
                    "stop_on_status_codes": [429]
                }
            })),
        )
    }

    fn sample_provider_catalog_endpoint() -> StoredProviderCatalogEndpoint {
        StoredProviderCatalogEndpoint::new(
            "endpoint-openai-chat-claude-cli-local-1".to_string(),
            "provider-openai-chat-claude-cli-local-1".to_string(),
            "claude:messages".to_string(),
            Some("claude".to_string()),
            Some("cli".to_string()),
            true,
        )
        .expect("endpoint should build")
        .with_transport_fields(
            "https://api.anthropic.com/v1".to_string(),
            Some(serde_json::json!([
                {"action":"set","key":"x-endpoint-tag","value":"openai-chat-claude-cli-cross-format"}
            ])),
            None,
            Some(2),
            None,
            None,
            None,
            None,
        )
        .expect("endpoint transport should build")
    }

    fn sample_provider_catalog_key() -> StoredProviderCatalogKey {
        crate::tests::support::provider_catalog_key(&crate::tests::support::CatalogKeySpec {
            id_suffix: "openai-chat-claude-cli-local-1",
            key_name: "prod",
            auth_type: "oauth",
            api_format: "claude:messages",
            secret: "sk-upstream-openai-chat-claude-cli",
        })
    }

    let seen_execution_runtime = Arc::new(Mutex::new(None::<SeenExecutionRuntimeSyncRequest>));
    let seen_execution_runtime_clone = Arc::clone(&seen_execution_runtime);
    let seen_report = Arc::new(Mutex::new(false));
    let seen_report_clone = Arc::clone(&seen_report);
    let decision_hits = Arc::new(Mutex::new(0usize));
    let decision_hits_clone = Arc::clone(&decision_hits);
    let plan_hits = Arc::new(Mutex::new(0usize));
    let plan_hits_clone = Arc::clone(&plan_hits);
    let public_hits = Arc::new(Mutex::new(0usize));
    let public_hits_clone = Arc::clone(&public_hits);
    let request_candidate_repository = Arc::new(InMemoryRequestCandidateRepository::default());

    let upstream = Router::new()
        .route(
            "/api/internal/gateway/decision-sync",
            any(move |_request: Request| {
                let decision_hits_inner = Arc::clone(&decision_hits_clone);
                async move {
                    *decision_hits_inner.lock().expect("mutex should lock") += 1;
                    Json(json!({"action": "proxy_public"}))
                }
            }),
        )
        .route(
            "/api/internal/gateway/plan-sync",
            any(move |_request: Request| {
                let plan_hits_inner = Arc::clone(&plan_hits_clone);
                async move {
                    *plan_hits_inner.lock().expect("mutex should lock") += 1;
                    Json(json!({"action": "proxy_public"}))
                }
            }),
        )
        .route(
            "/api/internal/gateway/report-sync",
            any(move |request: Request| {
                let seen_report_inner = Arc::clone(&seen_report_clone);
                async move {
                    let (_parts, body) = request.into_parts();
                    let _raw_body = to_bytes(body, usize::MAX).await.expect("body should read");
                    *seen_report_inner.lock().expect("mutex should lock") = true;
                    Json(json!({"ok": true}))
                }
            }),
        )
        .route(
            "/v1/chat/completions",
            any(move |_request: Request| {
                let public_hits_inner = Arc::clone(&public_hits_clone);
                async move {
                    *public_hits_inner.lock().expect("mutex should lock") += 1;
                    (StatusCode::IM_A_TEAPOT, Body::from("public-route-hit"))
                }
            }),
        );

    let execution_runtime = Router::new().route(
        "/v1/execute/sync",
        any(move |request: Request| {
            let seen_execution_runtime_inner = Arc::clone(&seen_execution_runtime_clone);
            async move {
                let (parts, body) = request.into_parts();
                let raw_body = to_bytes(body, usize::MAX).await.expect("body should read");
                let payload: serde_json::Value = serde_json::from_slice(&raw_body)
                    .expect("execution runtime payload should parse");
                *seen_execution_runtime_inner
                    .lock()
                    .expect("mutex should lock") = Some(SeenExecutionRuntimeSyncRequest {
                    trace_id: crate::tests::support::header_string(&parts.headers, TRACE_ID_HEADER),
                    url: crate::tests::support::payload_string(&payload, &["url"]),
                    body: payload
                        .get("body")
                        .and_then(|value| value.get("json_body"))
                        .cloned()
                        .unwrap_or(serde_json::Value::Null),
                    client_api_format: crate::tests::support::payload_string(
                        &payload,
                        &["client_api_format"],
                    ),
                    provider_api_format: crate::tests::support::payload_string(
                        &payload,
                        &["provider_api_format"],
                    ),
                    auth_header_value: crate::tests::support::payload_string(
                        &payload,
                        &["headers", "authorization"],
                    ),
                    anthropic_version: crate::tests::support::payload_string(
                        &payload,
                        &["headers", "anthropic-version"],
                    ),
                    endpoint_tag: crate::tests::support::payload_string(
                        &payload,
                        &["headers", "x-endpoint-tag"],
                    ),
                });
                Json(json!({
                    "request_id": "trace-openai-chat-claude-cli-local-error-123",
                    "status_code": 429,
                    "headers": {
                        "content-type": "application/json"
                    },
                    "body": {
                        "json_body": {
                            "type": "error",
                            "error": {
                                "type": "rate_limit_error",
                                "message": "slow down"
                            }
                        }
                    },
                    "telemetry": {
                        "elapsed_ms": 26
                    }
                }))
            }
        }),
    );

    let auth_repository = Arc::new(InMemoryAuthApiKeySnapshotRepository::seed(vec![(
        Some(hash_api_key("sk-client-openai-chat-claude-cli-error")),
        sample_auth_snapshot(
            "api-key-openai-chat-claude-cli-local-error-1",
            "user-openai-chat-claude-cli-local-error-1",
        ),
    )]));
    let candidate_selection_repository =
        Arc::new(InMemoryMinimalCandidateSelectionReadRepository::seed(vec![
            sample_candidate_row(),
        ]));
    let provider_catalog_repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![
            crate::tests::ai_execute::ai_execute_provider_stop_on_status_code(
                sample_provider_catalog_provider(),
                429,
            ),
        ],
        vec![sample_provider_catalog_endpoint()],
        vec![sample_provider_catalog_key()],
    ));

    let (_upstream_url, upstream_handle) = start_server(upstream).await;
    let (execution_runtime_url, execution_runtime_handle) = start_server(execution_runtime).await;
    let gateway_state =
        build_state_with_execution_runtime_override(execution_runtime_url.clone())
    .with_data_state_for_tests(
        crate::data::GatewayDataState::with_auth_candidate_selection_provider_catalog_and_request_candidate_repository_for_tests(
            auth_repository,
            candidate_selection_repository,
            provider_catalog_repository,
            Arc::clone(&request_candidate_repository),
            DEV_FERNET_KEY,
        ),
    );
    let gateway = build_router_with_state(gateway_state);
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .post(format!("{gateway_url}/v1/chat/completions"))
        .header(http::header::CONTENT_TYPE, "application/json")
        .header(
            http::header::AUTHORIZATION,
            "Bearer sk-client-openai-chat-claude-cli-error",
        )
        .header(TRACE_ID_HEADER, "trace-openai-chat-claude-cli-local-error-123")
        .body(
            "{\"model\":\"gpt-5\",\"messages\":[{\"role\":\"system\",\"content\":\"You are terse.\"},{\"role\":\"user\",\"content\":\"Say hello\"}],\"temperature\":0.2,\"max_tokens\":64}",
        )
        .send()
        .await
        .expect("request should succeed");

    let status = response.status();
    let execution_path = response
        .headers()
        .get(EXECUTION_PATH_HEADER)
        .and_then(|value| value.to_str().ok())
        .map(ToOwned::to_owned);
    let response_json: serde_json::Value = response.json().await.expect("body should parse");
    assert_eq!(
        status,
        StatusCode::TOO_MANY_REQUESTS,
        "unexpected status={status} body={response_json}"
    );
    assert_eq!(
        execution_path.as_deref(),
        Some(EXECUTION_PATH_EXECUTION_RUNTIME_SYNC)
    );
    assert_eq!(
        response_json,
        json!({
            "error": {
                "message": "slow down",
                "type": "rate_limit_error"
            }
        })
    );

    let seen_execution_runtime_request = seen_execution_runtime
        .lock()
        .expect("mutex should lock")
        .clone()
        .expect("execution runtime sync should be captured");
    assert_eq!(
        seen_execution_runtime_request.trace_id,
        "trace-openai-chat-claude-cli-local-error-123"
    );
    assert_eq!(
        seen_execution_runtime_request.url,
        "https://api.anthropic.com/v1/messages"
    );
    assert_eq!(
        seen_execution_runtime_request.body["model"],
        "claude-code-upstream"
    );
    assert_eq!(seen_execution_runtime_request.body["max_tokens"], 64);
    // claude_code providers get the Claude Code body shape: billing header + identity +
    // generic prompt, with the client's own system moved into the message history.
    let system = seen_execution_runtime_request.body["system"]
        .as_array()
        .expect("claude_code system should be rewritten into blocks");
    assert_eq!(system.len(), 3);
    assert!(system[0]["text"]
        .as_str()
        .is_some_and(|text| text.starts_with("x-anthropic-billing-header: cc_version=")));
    assert_eq!(
        system[1]["text"],
        "You are Claude Code, Anthropic's official CLI for Claude."
    );
    assert_eq!(
        seen_execution_runtime_request.body["messages"],
        json!([
            {"role":"user","content":[{"type":"text","text":"[System Instructions]\nYou are terse."}]},
            {"role":"assistant","content":[{"type":"text","text":"Understood. I will follow these instructions."}]},
            {"role":"user","content":"Say hello"}
        ])
    );
    assert!(seen_execution_runtime_request.body["metadata"]["user_id"]
        .as_str()
        .is_some_and(|user_id| user_id.contains("\"session_id\"")));
    assert_eq!(
        seen_execution_runtime_request.client_api_format,
        "openai:chat"
    );
    assert_eq!(
        seen_execution_runtime_request.provider_api_format,
        "claude:messages"
    );
    assert_eq!(
        seen_execution_runtime_request.auth_header_value,
        "Bearer sk-upstream-openai-chat-claude-cli"
    );
    assert_eq!(
        seen_execution_runtime_request.anthropic_version,
        "2023-06-01"
    );
    assert_eq!(
        seen_execution_runtime_request.endpoint_tag,
        "openai-chat-claude-cli-cross-format"
    );

    let stored_candidates = request_candidate_repository
        .list_by_request_id("trace-openai-chat-claude-cli-local-error-123")
        .await
        .expect("request candidate trace should read");
    assert_eq!(stored_candidates.len(), 1);
    assert_eq!(stored_candidates[0].status, RequestCandidateStatus::Failed);
    let extra_data = stored_candidates[0]
        .extra_data
        .as_ref()
        .expect("request candidate extra_data should exist");
    assert_eq!(extra_data["client_api_format"], "openai:chat");
    assert_eq!(extra_data["provider_api_format"], "claude:messages");

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    assert!(
        !*seen_report.lock().expect("mutex should lock"),
        "report-sync should stay local when request candidate persistence is available"
    );

    assert_eq!(*decision_hits.lock().expect("mutex should lock"), 0);
    assert_eq!(*plan_hits.lock().expect("mutex should lock"), 0);
    assert_eq!(*public_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    execution_runtime_handle.abort();
    upstream_handle.abort();
}

async fn gateway_returns_openai_chat_error_for_local_cross_format_gemini_cli_sync_failure_impl() {
    #[derive(Debug, Clone)]
    struct SeenExecutionRuntimeSyncRequest {
        trace_id: String,
        url: String,
        outer_model: String,
        user_prompt_id: String,
        inner_model_present: bool,
        auth_header_value: String,
        endpoint_tag: String,
        has_contents: bool,
    }

    fn sample_auth_snapshot(api_key_id: &str, user_id: &str) -> StoredAuthApiKeySnapshot {
        crate::tests::support::sample_auth_snapshot_with(
            api_key_id,
            user_id,
            "alice",
            "alice@example.com",
            serde_json::json!(["openai", "gemini", "gemini_cli"]),
            serde_json::json!(["openai:chat"]),
            serde_json::json!(["gpt-5"]),
        )
    }

    fn sample_candidate_row() -> StoredMinimalCandidateSelectionRow {
        crate::tests::support::minimal_candidate_row(&crate::tests::support::CandidateRowSpec {
            id_suffix: "openai-chat-gemini-cli-local-1",
            provider_name: "gemini_cli",
            provider_type: "gemini_cli",
            api_format: "gemini:generate_content",
            api_family: "gemini",
            endpoint_kind: Some("cli"),
            key_name: "oauth",
            key_auth_type: "oauth",
            global_model_name: "gpt-5",
            upstream_model: "gemini-cli-upstream",
        })
    }

    fn sample_provider_catalog_provider() -> StoredProviderCatalogProvider {
        StoredProviderCatalogProvider::new(
            "provider-openai-chat-gemini-cli-local-1".to_string(),
            "gemini_cli".to_string(),
            Some("https://example.com".to_string()),
            "gemini_cli".to_string(),
        )
        .expect("provider should build")
        .with_transport_fields(
            true,
            false,
            true,
            None,
            Some(2),
            None,
            Some(20.0),
            None,
            Some(serde_json::json!({
                "failover_rules": {
                    "stop_on_status_codes": [429]
                }
            })),
        )
    }

    fn sample_provider_catalog_endpoint() -> StoredProviderCatalogEndpoint {
        StoredProviderCatalogEndpoint::new(
            "endpoint-openai-chat-gemini-cli-local-1".to_string(),
            "provider-openai-chat-gemini-cli-local-1".to_string(),
            "gemini:generate_content".to_string(),
            Some("gemini".to_string()),
            Some("cli".to_string()),
            true,
        )
        .expect("endpoint should build")
        .with_transport_fields(
            "https://cloudcode-pa.googleapis.com".to_string(),
            Some(serde_json::json!([
                {"action":"set","key":"x-endpoint-tag","value":"openai-chat-gemini-cli-cross-format"}
            ])),
            None,
            Some(2),
            None,
            None,
            None,
            None,
        )
        .expect("endpoint transport should build")
    }

    fn sample_provider_catalog_key() -> StoredProviderCatalogKey {
        crate::tests::support::provider_catalog_key(&crate::tests::support::CatalogKeySpec {
            id_suffix: "openai-chat-gemini-cli-local-1",
            key_name: "oauth",
            auth_type: "oauth",
            api_format: "gemini:generate_content",
            secret: "sk-upstream-openai-chat-gemini-cli",
        })
    }

    let seen_execution_runtime = Arc::new(Mutex::new(None::<SeenExecutionRuntimeSyncRequest>));
    let seen_execution_runtime_clone = Arc::clone(&seen_execution_runtime);
    let seen_report = Arc::new(Mutex::new(false));
    let seen_report_clone = Arc::clone(&seen_report);
    let decision_hits = Arc::new(Mutex::new(0usize));
    let decision_hits_clone = Arc::clone(&decision_hits);
    let plan_hits = Arc::new(Mutex::new(0usize));
    let plan_hits_clone = Arc::clone(&plan_hits);
    let public_hits = Arc::new(Mutex::new(0usize));
    let public_hits_clone = Arc::clone(&public_hits);
    let request_candidate_repository = Arc::new(InMemoryRequestCandidateRepository::default());

    let upstream = Router::new()
        .route(
            "/api/internal/gateway/decision-sync",
            any(move |_request: Request| {
                let decision_hits_inner = Arc::clone(&decision_hits_clone);
                async move {
                    *decision_hits_inner.lock().expect("mutex should lock") += 1;
                    Json(json!({"action": "proxy_public"}))
                }
            }),
        )
        .route(
            "/api/internal/gateway/plan-sync",
            any(move |_request: Request| {
                let plan_hits_inner = Arc::clone(&plan_hits_clone);
                async move {
                    *plan_hits_inner.lock().expect("mutex should lock") += 1;
                    Json(json!({"action": "proxy_public"}))
                }
            }),
        )
        .route(
            "/api/internal/gateway/report-sync",
            any(move |request: Request| {
                let seen_report_inner = Arc::clone(&seen_report_clone);
                async move {
                    let (_parts, body) = request.into_parts();
                    let _raw_body = to_bytes(body, usize::MAX).await.expect("body should read");
                    *seen_report_inner.lock().expect("mutex should lock") = true;
                    Json(json!({"ok": true}))
                }
            }),
        )
        .route(
            "/v1/chat/completions",
            any(move |_request: Request| {
                let public_hits_inner = Arc::clone(&public_hits_clone);
                async move {
                    *public_hits_inner.lock().expect("mutex should lock") += 1;
                    (StatusCode::IM_A_TEAPOT, Body::from("public-route-hit"))
                }
            }),
        );

    let execution_runtime = Router::new().route(
        "/v1/execute/sync",
        any(move |request: Request| {
            let seen_execution_runtime_inner = Arc::clone(&seen_execution_runtime_clone);
            async move {
                let (parts, body) = request.into_parts();
                let raw_body = to_bytes(body, usize::MAX).await.expect("body should read");
                let payload: serde_json::Value = serde_json::from_slice(&raw_body)
                    .expect("execution runtime payload should parse");
                *seen_execution_runtime_inner
                    .lock()
                    .expect("mutex should lock") = Some(SeenExecutionRuntimeSyncRequest {
                    trace_id: crate::tests::support::header_string(&parts.headers, TRACE_ID_HEADER),
                    url: crate::tests::support::payload_string(&payload, &["url"]),
                    outer_model: payload
                        .get("body")
                        .and_then(|value| value.get("json_body"))
                        .and_then(|value| value.get("model"))
                        .and_then(|value| value.as_str())
                        .unwrap_or_default()
                        .to_string(),
                    user_prompt_id: payload
                        .get("body")
                        .and_then(|value| value.get("json_body"))
                        .and_then(|value| value.get("user_prompt_id"))
                        .and_then(|value| value.as_str())
                        .unwrap_or_default()
                        .to_string(),
                    inner_model_present: payload
                        .get("body")
                        .and_then(|value| value.get("json_body"))
                        .and_then(|value| value.get("request"))
                        .and_then(|value| value.get("model"))
                        .is_some(),
                    auth_header_value: crate::tests::support::payload_string(
                        &payload,
                        &["headers", "authorization"],
                    ),
                    endpoint_tag: crate::tests::support::payload_string(
                        &payload,
                        &["headers", "x-endpoint-tag"],
                    ),
                    has_contents: payload
                        .get("body")
                        .and_then(|value| value.get("json_body"))
                        .and_then(|value| value.get("request"))
                        .and_then(|value| value.get("contents"))
                        .is_some(),
                });
                Json(json!({
                    "request_id": "trace-openai-chat-gemini-cli-local-error-123",
                    "status_code": 429,
                    "headers": {
                        "content-type": "application/json"
                    },
                    "body": {
                        "json_body": {
                            "error": {
                                "message": "quota reached",
                                "status": "RESOURCE_EXHAUSTED"
                            }
                        }
                    },
                    "telemetry": {
                        "elapsed_ms": 24
                    }
                }))
            }
        }),
    );

    let auth_repository = Arc::new(InMemoryAuthApiKeySnapshotRepository::seed(vec![(
        Some(hash_api_key("sk-client-openai-chat-gemini-cli-error")),
        sample_auth_snapshot(
            "api-key-openai-chat-gemini-cli-local-error-1",
            "user-openai-chat-gemini-cli-local-error-1",
        ),
    )]));
    let candidate_selection_repository =
        Arc::new(InMemoryMinimalCandidateSelectionReadRepository::seed(vec![
            sample_candidate_row(),
        ]));
    let provider_catalog_repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![
            crate::tests::ai_execute::ai_execute_provider_stop_on_status_code(
                sample_provider_catalog_provider(),
                429,
            ),
        ],
        vec![sample_provider_catalog_endpoint()],
        vec![sample_provider_catalog_key()],
    ));

    let (_upstream_url, upstream_handle) = start_server(upstream).await;
    let (execution_runtime_url, execution_runtime_handle) = start_server(execution_runtime).await;
    let gateway_state =
        build_state_with_execution_runtime_override(execution_runtime_url.clone())
    .with_data_state_for_tests(
        crate::data::GatewayDataState::with_auth_candidate_selection_provider_catalog_and_request_candidate_repository_for_tests(
            auth_repository,
            candidate_selection_repository,
            provider_catalog_repository,
            Arc::clone(&request_candidate_repository),
            DEV_FERNET_KEY,
        ),
    );
    let gateway = build_router_with_state(gateway_state);
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .post(format!("{gateway_url}/v1/chat/completions"))
        .header(http::header::CONTENT_TYPE, "application/json")
        .header(
            http::header::AUTHORIZATION,
            "Bearer sk-client-openai-chat-gemini-cli-error",
        )
        .header(TRACE_ID_HEADER, "trace-openai-chat-gemini-cli-local-error-123")
        .body(
            "{\"model\":\"gpt-5\",\"messages\":[{\"role\":\"system\",\"content\":\"You are terse.\"},{\"role\":\"user\",\"content\":\"Say hello\"}],\"temperature\":0.2,\"max_tokens\":64}",
        )
        .send()
        .await
        .expect("request should succeed");

    let response_status = response.status();
    let execution_path = response
        .headers()
        .get(EXECUTION_PATH_HEADER)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let response_body = response.text().await.expect("body should read");
    assert_eq!(
        execution_path.as_deref(),
        Some(EXECUTION_PATH_EXECUTION_RUNTIME_SYNC)
    );
    assert_eq!(response_status, StatusCode::TOO_MANY_REQUESTS);
    let response_json: serde_json::Value =
        serde_json::from_str(&response_body).expect("body should parse");
    assert_eq!(
        response_json,
        json!({
            "error": {
                "message": "quota reached",
                "type": "rate_limit_error",
                "code": "RESOURCE_EXHAUSTED"
            }
        })
    );

    let seen_execution_runtime_request = seen_execution_runtime
        .lock()
        .expect("mutex should lock")
        .clone()
        .expect("execution runtime sync should be captured");
    assert_eq!(
        seen_execution_runtime_request.trace_id,
        "trace-openai-chat-gemini-cli-local-error-123"
    );
    assert_eq!(
        seen_execution_runtime_request.url,
        "https://cloudcode-pa.googleapis.com/v1internal:generateContent"
    );
    assert_eq!(
        seen_execution_runtime_request.outer_model,
        "gemini-cli-upstream"
    );
    assert_eq!(
        seen_execution_runtime_request.user_prompt_id,
        "trace-openai-chat-gemini-cli-local-error-123"
    );
    assert!(!seen_execution_runtime_request.inner_model_present);
    assert_eq!(
        seen_execution_runtime_request.auth_header_value,
        "Bearer sk-upstream-openai-chat-gemini-cli"
    );
    assert_eq!(
        seen_execution_runtime_request.endpoint_tag,
        "openai-chat-gemini-cli-cross-format"
    );
    assert!(seen_execution_runtime_request.has_contents);

    let stored_candidates = request_candidate_repository
        .list_by_request_id("trace-openai-chat-gemini-cli-local-error-123")
        .await
        .expect("request candidate trace should read");
    assert_eq!(stored_candidates.len(), 1);
    assert_eq!(stored_candidates[0].status, RequestCandidateStatus::Failed);

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    assert!(
        !*seen_report.lock().expect("mutex should lock"),
        "report-sync should stay local when request candidate persistence is available"
    );

    assert_eq!(*decision_hits.lock().expect("mutex should lock"), 0);
    assert_eq!(*plan_hits.lock().expect("mutex should lock"), 0);
    assert_eq!(*public_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    execution_runtime_handle.abort();
    upstream_handle.abort();
}

async fn gateway_returns_openai_chat_error_for_local_cross_format_claude_sync_failure_impl() {
    #[derive(Debug, Clone)]
    struct SeenExecutionRuntimeSyncRequest {
        trace_id: String,
        url: String,
        model: String,
        auth_header_value: String,
        endpoint_tag: String,
        has_messages: bool,
    }

    fn sample_auth_snapshot(api_key_id: &str, user_id: &str) -> StoredAuthApiKeySnapshot {
        crate::tests::support::sample_auth_snapshot_with(
            api_key_id,
            user_id,
            "alice",
            "alice@example.com",
            serde_json::json!(["openai", "claude"]),
            serde_json::json!(["openai:chat"]),
            serde_json::json!(["gpt-5"]),
        )
    }

    fn sample_candidate_row() -> StoredMinimalCandidateSelectionRow {
        crate::tests::support::minimal_candidate_row(&crate::tests::support::CandidateRowSpec {
            id_suffix: "openai-chat-claude-local-1",
            provider_name: "claude",
            provider_type: "custom",
            api_format: "claude:messages",
            api_family: "claude",
            endpoint_kind: Some("chat"),
            key_name: "prod",
            key_auth_type: "api_key",
            global_model_name: "gpt-5",
            upstream_model: "claude-sonnet-4-5-upstream",
        })
    }

    fn sample_provider_catalog_provider() -> StoredProviderCatalogProvider {
        StoredProviderCatalogProvider::new(
            "provider-openai-chat-claude-local-1".to_string(),
            "claude".to_string(),
            Some("https://example.com".to_string()),
            "custom".to_string(),
        )
        .expect("provider should build")
        .with_transport_fields(
            true,
            false,
            true,
            None,
            Some(2),
            None,
            Some(20.0),
            None,
            Some(serde_json::json!({
                "failover_rules": {
                    "stop_on_status_codes": [429]
                }
            })),
        )
    }

    fn sample_provider_catalog_endpoint() -> StoredProviderCatalogEndpoint {
        StoredProviderCatalogEndpoint::new(
            "endpoint-openai-chat-claude-local-1".to_string(),
            "provider-openai-chat-claude-local-1".to_string(),
            "claude:messages".to_string(),
            Some("claude".to_string()),
            Some("chat".to_string()),
            true,
        )
        .expect("endpoint should build")
        .with_transport_fields(
            "https://api.anthropic.example".to_string(),
            Some(serde_json::json!([
                {"action":"set","key":"x-endpoint-tag","value":"openai-chat-claude-cross-format"}
            ])),
            None,
            Some(2),
            Some("/custom/v1/messages".to_string()),
            None,
            None,
            None,
        )
        .expect("endpoint transport should build")
    }

    fn sample_provider_catalog_key() -> StoredProviderCatalogKey {
        crate::tests::support::provider_catalog_key(&crate::tests::support::CatalogKeySpec {
            id_suffix: "openai-chat-claude-local-1",
            key_name: "prod",
            auth_type: "api_key",
            api_format: "claude:messages",
            secret: "sk-upstream-openai-chat-claude",
        })
    }

    let seen_execution_runtime = Arc::new(Mutex::new(None::<SeenExecutionRuntimeSyncRequest>));
    let seen_execution_runtime_clone = Arc::clone(&seen_execution_runtime);
    let seen_report = Arc::new(Mutex::new(false));
    let seen_report_clone = Arc::clone(&seen_report);
    let decision_hits = Arc::new(Mutex::new(0usize));
    let decision_hits_clone = Arc::clone(&decision_hits);
    let plan_hits = Arc::new(Mutex::new(0usize));
    let plan_hits_clone = Arc::clone(&plan_hits);
    let public_hits = Arc::new(Mutex::new(0usize));
    let public_hits_clone = Arc::clone(&public_hits);
    let request_candidate_repository = Arc::new(InMemoryRequestCandidateRepository::default());

    let upstream = Router::new()
        .route(
            "/api/internal/gateway/decision-sync",
            any(move |_request: Request| {
                let decision_hits_inner = Arc::clone(&decision_hits_clone);
                async move {
                    *decision_hits_inner.lock().expect("mutex should lock") += 1;
                    Json(json!({"action": "proxy_public"}))
                }
            }),
        )
        .route(
            "/api/internal/gateway/plan-sync",
            any(move |_request: Request| {
                let plan_hits_inner = Arc::clone(&plan_hits_clone);
                async move {
                    *plan_hits_inner.lock().expect("mutex should lock") += 1;
                    Json(json!({"action": "proxy_public"}))
                }
            }),
        )
        .route(
            "/api/internal/gateway/report-sync",
            any(move |request: Request| {
                let seen_report_inner = Arc::clone(&seen_report_clone);
                async move {
                    let (_parts, body) = request.into_parts();
                    let _raw_body = to_bytes(body, usize::MAX).await.expect("body should read");
                    *seen_report_inner.lock().expect("mutex should lock") = true;
                    Json(json!({"ok": true}))
                }
            }),
        )
        .route(
            "/v1/chat/completions",
            any(move |_request: Request| {
                let public_hits_inner = Arc::clone(&public_hits_clone);
                async move {
                    *public_hits_inner.lock().expect("mutex should lock") += 1;
                    (StatusCode::IM_A_TEAPOT, Body::from("public-route-hit"))
                }
            }),
        );

    let execution_runtime = Router::new().route(
        "/v1/execute/sync",
        any(move |request: Request| {
            let seen_execution_runtime_inner = Arc::clone(&seen_execution_runtime_clone);
            async move {
                let (parts, body) = request.into_parts();
                let raw_body = to_bytes(body, usize::MAX).await.expect("body should read");
                let payload: serde_json::Value = serde_json::from_slice(&raw_body)
                    .expect("execution runtime payload should parse");
                *seen_execution_runtime_inner
                    .lock()
                    .expect("mutex should lock") = Some(SeenExecutionRuntimeSyncRequest {
                    trace_id: crate::tests::support::header_string(&parts.headers, TRACE_ID_HEADER),
                    url: crate::tests::support::payload_string(&payload, &["url"]),
                    model: payload
                        .get("body")
                        .and_then(|value| value.get("json_body"))
                        .and_then(|value| value.get("model"))
                        .and_then(|value| value.as_str())
                        .unwrap_or_default()
                        .to_string(),
                    auth_header_value: crate::tests::support::payload_string(
                        &payload,
                        &["headers", "x-api-key"],
                    ),
                    endpoint_tag: crate::tests::support::payload_string(
                        &payload,
                        &["headers", "x-endpoint-tag"],
                    ),
                    has_messages: payload
                        .get("body")
                        .and_then(|value| value.get("json_body"))
                        .and_then(|value| value.get("messages"))
                        .is_some(),
                });
                Json(json!({
                    "request_id": "trace-openai-chat-claude-local-error-123",
                    "status_code": 429,
                    "headers": {
                        "content-type": "application/json"
                    },
                    "body": {
                        "json_body": {
                            "type": "error",
                            "error": {
                                "type": "rate_limit_error",
                                "message": "slow down"
                            }
                        }
                    },
                    "telemetry": {
                        "elapsed_ms": 27
                    }
                }))
            }
        }),
    );

    let auth_repository = Arc::new(InMemoryAuthApiKeySnapshotRepository::seed(vec![(
        Some(hash_api_key("sk-client-openai-chat-claude-error")),
        sample_auth_snapshot(
            "api-key-openai-chat-claude-local-error-1",
            "user-openai-chat-claude-local-error-1",
        ),
    )]));
    let candidate_selection_repository =
        Arc::new(InMemoryMinimalCandidateSelectionReadRepository::seed(vec![
            sample_candidate_row(),
        ]));
    let provider_catalog_repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![
            crate::tests::ai_execute::ai_execute_provider_stop_on_status_code(
                sample_provider_catalog_provider(),
                429,
            ),
        ],
        vec![sample_provider_catalog_endpoint()],
        vec![sample_provider_catalog_key()],
    ));

    let (_upstream_url, upstream_handle) = start_server(upstream).await;
    let (execution_runtime_url, execution_runtime_handle) = start_server(execution_runtime).await;
    let gateway_state =
        build_state_with_execution_runtime_override(execution_runtime_url.clone())
    .with_data_state_for_tests(
        crate::data::GatewayDataState::with_auth_candidate_selection_provider_catalog_and_request_candidate_repository_for_tests(
            auth_repository,
            candidate_selection_repository,
            provider_catalog_repository,
            Arc::clone(&request_candidate_repository),
            DEV_FERNET_KEY,
        ),
    );
    let gateway = build_router_with_state(gateway_state);
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .post(format!("{gateway_url}/v1/chat/completions"))
        .header(http::header::CONTENT_TYPE, "application/json")
        .header(
            http::header::AUTHORIZATION,
            "Bearer sk-client-openai-chat-claude-error",
        )
        .header(TRACE_ID_HEADER, "trace-openai-chat-claude-local-error-123")
        .body(
            "{\"model\":\"gpt-5\",\"messages\":[{\"role\":\"system\",\"content\":\"You are terse.\"},{\"role\":\"user\",\"content\":\"Say hello\"}],\"temperature\":0.2,\"max_tokens\":64}",
        )
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(
        response
            .headers()
            .get(EXECUTION_PATH_HEADER)
            .and_then(|value| value.to_str().ok()),
        Some(EXECUTION_PATH_EXECUTION_RUNTIME_SYNC)
    );
    let response_json: serde_json::Value = response.json().await.expect("body should parse");
    assert_eq!(
        response_json,
        json!({
            "error": {
                "message": "slow down",
                "type": "rate_limit_error"
            }
        })
    );

    let seen_execution_runtime_request = seen_execution_runtime
        .lock()
        .expect("mutex should lock")
        .clone()
        .expect("execution runtime sync should be captured");
    assert_eq!(
        seen_execution_runtime_request.trace_id,
        "trace-openai-chat-claude-local-error-123"
    );
    assert_eq!(
        seen_execution_runtime_request.url,
        "https://api.anthropic.example/custom/v1/messages"
    );
    assert_eq!(
        seen_execution_runtime_request.model,
        "claude-sonnet-4-5-upstream"
    );
    assert_eq!(
        seen_execution_runtime_request.auth_header_value,
        "sk-upstream-openai-chat-claude"
    );
    assert_eq!(
        seen_execution_runtime_request.endpoint_tag,
        "openai-chat-claude-cross-format"
    );
    assert!(seen_execution_runtime_request.has_messages);

    let stored_candidates = request_candidate_repository
        .list_by_request_id("trace-openai-chat-claude-local-error-123")
        .await
        .expect("request candidate trace should read");
    assert_eq!(stored_candidates.len(), 1);
    assert_eq!(stored_candidates[0].status, RequestCandidateStatus::Failed);

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    assert!(
        !*seen_report.lock().expect("mutex should lock"),
        "report-sync should stay local when request candidate persistence is available"
    );

    assert_eq!(*decision_hits.lock().expect("mutex should lock"), 0);
    assert_eq!(*plan_hits.lock().expect("mutex should lock"), 0);
    assert_eq!(*public_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    execution_runtime_handle.abort();
    upstream_handle.abort();
}

async fn gateway_returns_openai_chat_error_for_local_cross_format_gemini_sync_failure_impl() {
    #[derive(Debug, Clone)]
    struct SeenExecutionRuntimeSyncRequest {
        trace_id: String,
        url: String,
        provider_model: String,
        auth_header_value: String,
        endpoint_tag: String,
        system_text: String,
        user_text: String,
        max_output_tokens: u64,
        temperature: f64,
    }
    fn sample_candidate_row() -> StoredMinimalCandidateSelectionRow {
        crate::tests::support::minimal_candidate_row(&crate::tests::support::CandidateRowSpec {
            id_suffix: "openai-chat-gemini-local-1",
            provider_name: "gemini",
            provider_type: "custom",
            api_format: "gemini:generate_content",
            api_family: "gemini",
            endpoint_kind: Some("chat"),
            key_name: "prod",
            key_auth_type: "api_key",
            global_model_name: "gpt-5",
            upstream_model: "gemini-2.5-pro-upstream",
        })
    }

    fn sample_provider_catalog_provider() -> StoredProviderCatalogProvider {
        StoredProviderCatalogProvider::new(
            "provider-openai-chat-gemini-local-1".to_string(),
            "gemini".to_string(),
            Some("https://example.com".to_string()),
            "custom".to_string(),
        )
        .expect("provider should build")
        .with_transport_fields(
            true,
            false,
            true,
            None,
            Some(2),
            None,
            Some(20.0),
            None,
            Some(serde_json::json!({
                "failover_rules": {
                    "stop_on_status_codes": [429]
                }
            })),
        )
    }

    fn sample_provider_catalog_endpoint() -> StoredProviderCatalogEndpoint {
        StoredProviderCatalogEndpoint::new(
            "endpoint-openai-chat-gemini-local-1".to_string(),
            "provider-openai-chat-gemini-local-1".to_string(),
            "gemini:generate_content".to_string(),
            Some("gemini".to_string()),
            Some("chat".to_string()),
            true,
        )
        .expect("endpoint should build")
        .with_transport_fields(
            "https://generativelanguage.googleapis.com".to_string(),
            Some(serde_json::json!([
                {"action":"set","key":"x-endpoint-tag","value":"openai-chat-gemini-cross-format"}
            ])),
            None,
            Some(2),
            Some("/custom/v1beta/models/gemini-2.5-pro-upstream:generateContent".to_string()),
            None,
            None,
            None,
        )
        .expect("endpoint transport should build")
    }

    fn sample_provider_catalog_key() -> StoredProviderCatalogKey {
        crate::tests::support::provider_catalog_key(&crate::tests::support::CatalogKeySpec {
            id_suffix: "openai-chat-gemini-local-1",
            key_name: "prod",
            auth_type: "api_key",
            api_format: "gemini:generate_content",
            secret: "sk-upstream-openai-chat-gemini",
        })
    }

    let seen_execution_runtime = Arc::new(Mutex::new(None::<SeenExecutionRuntimeSyncRequest>));
    let seen_execution_runtime_clone = Arc::clone(&seen_execution_runtime);
    let seen_report = Arc::new(Mutex::new(false));
    let seen_report_clone = Arc::clone(&seen_report);
    let decision_hits = Arc::new(Mutex::new(0usize));
    let decision_hits_clone = Arc::clone(&decision_hits);
    let plan_hits = Arc::new(Mutex::new(0usize));
    let plan_hits_clone = Arc::clone(&plan_hits);
    let public_hits = Arc::new(Mutex::new(0usize));
    let public_hits_clone = Arc::clone(&public_hits);
    let request_candidate_repository = Arc::new(InMemoryRequestCandidateRepository::default());

    let upstream = Router::new()
        .route(
            "/api/internal/gateway/decision-sync",
            any(move |_request: Request| {
                let decision_hits_inner = Arc::clone(&decision_hits_clone);
                async move {
                    *decision_hits_inner.lock().expect("mutex should lock") += 1;
                    Json(json!({"action": "proxy_public"}))
                }
            }),
        )
        .route(
            "/api/internal/gateway/plan-sync",
            any(move |_request: Request| {
                let plan_hits_inner = Arc::clone(&plan_hits_clone);
                async move {
                    *plan_hits_inner.lock().expect("mutex should lock") += 1;
                    Json(json!({"action": "proxy_public"}))
                }
            }),
        )
        .route(
            "/api/internal/gateway/report-sync",
            any(move |request: Request| {
                let seen_report_inner = Arc::clone(&seen_report_clone);
                async move {
                    let (_parts, body) = request.into_parts();
                    let _raw_body = to_bytes(body, usize::MAX).await.expect("body should read");
                    *seen_report_inner.lock().expect("mutex should lock") = true;
                    Json(json!({"ok": true}))
                }
            }),
        )
        .route(
            "/v1/chat/completions",
            any(move |_request: Request| {
                let public_hits_inner = Arc::clone(&public_hits_clone);
                async move {
                    *public_hits_inner.lock().expect("mutex should lock") += 1;
                    (StatusCode::IM_A_TEAPOT, Body::from("public-route-hit"))
                }
            }),
        );

    let execution_runtime = Router::new().route(
        "/v1/execute/sync",
        any(move |request: Request| {
            let seen_execution_runtime_inner = Arc::clone(&seen_execution_runtime_clone);
            async move {
                let (parts, body) = request.into_parts();
                let raw_body = to_bytes(body, usize::MAX).await.expect("body should read");
                let payload: serde_json::Value = serde_json::from_slice(&raw_body)
                    .expect("execution runtime payload should parse");
                *seen_execution_runtime_inner
                    .lock()
                    .expect("mutex should lock") = Some(SeenExecutionRuntimeSyncRequest {
                    trace_id: crate::tests::support::header_string(&parts.headers, TRACE_ID_HEADER),
                    url: crate::tests::support::payload_string(&payload, &["url"]),
                    provider_model: payload
                        .get("body")
                        .and_then(|value| value.get("json_body"))
                        .and_then(|value| value.get("model"))
                        .and_then(|value| value.as_str())
                        .unwrap_or_default()
                        .to_string(),
                    auth_header_value: crate::tests::support::payload_string(
                        &payload,
                        &["headers", "x-goog-api-key"],
                    ),
                    endpoint_tag: crate::tests::support::payload_string(
                        &payload,
                        &["headers", "x-endpoint-tag"],
                    ),
                    system_text: payload
                        .get("body")
                        .and_then(|value| value.get("json_body"))
                        .and_then(|value| value.get("systemInstruction"))
                        .and_then(|value| value.get("parts"))
                        .and_then(|value| value.as_array())
                        .and_then(|value| value.first())
                        .and_then(|value| value.get("text"))
                        .and_then(|value| value.as_str())
                        .unwrap_or_default()
                        .to_string(),
                    user_text: payload
                        .get("body")
                        .and_then(|value| value.get("json_body"))
                        .and_then(|value| value.get("contents"))
                        .and_then(|value| value.as_array())
                        .and_then(|value| value.first())
                        .and_then(|value| value.get("parts"))
                        .and_then(|value| value.as_array())
                        .and_then(|value| value.first())
                        .and_then(|value| value.get("text"))
                        .and_then(|value| value.as_str())
                        .unwrap_or_default()
                        .to_string(),
                    max_output_tokens: payload
                        .get("body")
                        .and_then(|value| value.get("json_body"))
                        .and_then(|value| value.get("generationConfig"))
                        .and_then(|value| value.get("maxOutputTokens"))
                        .and_then(|value| value.as_u64())
                        .unwrap_or_default(),
                    temperature: payload
                        .get("body")
                        .and_then(|value| value.get("json_body"))
                        .and_then(|value| value.get("generationConfig"))
                        .and_then(|value| value.get("temperature"))
                        .and_then(|value| value.as_f64())
                        .unwrap_or_default(),
                });
                Json(json!({
                    "request_id": "trace-openai-chat-gemini-local-error-123",
                    "status_code": 429,
                    "headers": {
                        "content-type": "application/json"
                    },
                    "body": {
                        "json_body": {
                            "error": {
                                "message": "quota reached",
                                "status": "RESOURCE_EXHAUSTED"
                            }
                        }
                    },
                    "telemetry": {
                        "elapsed_ms": 25
                    }
                }))
            }
        }),
    );

    let auth_repository = Arc::new(InMemoryAuthApiKeySnapshotRepository::seed(vec![(
        Some(hash_api_key("sk-client-openai-chat-gemini-error")),
        sample_auth_snapshot(
            "api-key-openai-chat-gemini-local-error-1",
            "user-openai-chat-gemini-local-error-1",
        ),
    )]));
    let candidate_selection_repository =
        Arc::new(InMemoryMinimalCandidateSelectionReadRepository::seed(vec![
            sample_candidate_row(),
        ]));
    let provider_catalog_repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![
            crate::tests::ai_execute::ai_execute_provider_stop_on_status_code(
                sample_provider_catalog_provider(),
                429,
            ),
        ],
        vec![sample_provider_catalog_endpoint()],
        vec![sample_provider_catalog_key()],
    ));

    let (_upstream_url, upstream_handle) = start_server(upstream).await;
    let (execution_runtime_url, execution_runtime_handle) = start_server(execution_runtime).await;
    let gateway_state =
        build_state_with_execution_runtime_override(execution_runtime_url.clone())
    .with_data_state_for_tests(
        crate::data::GatewayDataState::with_auth_candidate_selection_provider_catalog_and_request_candidate_repository_for_tests(
            auth_repository,
            candidate_selection_repository,
            provider_catalog_repository,
            Arc::clone(&request_candidate_repository),
            DEV_FERNET_KEY,
        ),
    );
    let gateway = build_router_with_state(gateway_state);
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .post(format!("{gateway_url}/v1/chat/completions"))
        .header(http::header::CONTENT_TYPE, "application/json")
        .header(
            http::header::AUTHORIZATION,
            "Bearer sk-client-openai-chat-gemini-error",
        )
        .header(TRACE_ID_HEADER, "trace-openai-chat-gemini-local-error-123")
        .body(
            "{\"model\":\"gpt-5\",\"messages\":[{\"role\":\"system\",\"content\":\"You are terse.\"},{\"role\":\"user\",\"content\":\"Say hello\"}],\"temperature\":0.2,\"max_tokens\":64}",
        )
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(
        response
            .headers()
            .get(EXECUTION_PATH_HEADER)
            .and_then(|value| value.to_str().ok()),
        Some(EXECUTION_PATH_EXECUTION_RUNTIME_SYNC)
    );
    let response_json: serde_json::Value = response.json().await.expect("body should parse");
    assert_eq!(
        response_json,
        json!({
            "error": {
                "message": "quota reached",
                "type": "rate_limit_error",
                "code": "RESOURCE_EXHAUSTED"
            }
        })
    );

    let seen_execution_runtime_request = seen_execution_runtime
        .lock()
        .expect("mutex should lock")
        .clone()
        .expect("execution runtime sync should be captured");
    assert_eq!(
        seen_execution_runtime_request.trace_id,
        "trace-openai-chat-gemini-local-error-123"
    );
    assert_eq!(
        seen_execution_runtime_request.url,
        "https://generativelanguage.googleapis.com/custom/v1beta/models/gemini-2.5-pro-upstream:generateContent"
    );
    assert_eq!(
        seen_execution_runtime_request.provider_model,
        "gemini-2.5-pro-upstream"
    );
    assert_eq!(
        seen_execution_runtime_request.auth_header_value,
        "sk-upstream-openai-chat-gemini"
    );
    assert_eq!(
        seen_execution_runtime_request.endpoint_tag,
        "openai-chat-gemini-cross-format"
    );
    assert_eq!(seen_execution_runtime_request.system_text, "You are terse.");
    assert_eq!(seen_execution_runtime_request.user_text, "Say hello");
    assert_eq!(seen_execution_runtime_request.max_output_tokens, 64);
    assert!((seen_execution_runtime_request.temperature - 0.2).abs() < f64::EPSILON);

    let stored_candidates = request_candidate_repository
        .list_by_request_id("trace-openai-chat-gemini-local-error-123")
        .await
        .expect("request candidate trace should read");
    assert_eq!(stored_candidates.len(), 1);
    assert_eq!(stored_candidates[0].status, RequestCandidateStatus::Failed);

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    assert!(
        !*seen_report.lock().expect("mutex should lock"),
        "report-sync should stay local when request candidate persistence is available"
    );

    assert_eq!(*decision_hits.lock().expect("mutex should lock"), 0);
    assert_eq!(*plan_hits.lock().expect("mutex should lock"), 0);
    assert_eq!(*public_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    execution_runtime_handle.abort();
    upstream_handle.abort();
}
