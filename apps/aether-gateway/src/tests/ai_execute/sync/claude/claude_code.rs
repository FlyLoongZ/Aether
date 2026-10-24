use super::{
    any, build_router_with_state, build_state_with_execution_runtime_override,
    encrypt_python_fernet_plaintext, json, start_server, to_bytes, Arc, Body,
    InMemoryAuthApiKeySnapshotRepository, InMemoryMinimalCandidateSelectionReadRepository,
    InMemoryProviderCatalogReadRepository, InMemoryRequestCandidateRepository, Json, Mutex,
    Request, RequestCandidateReadRepository, RequestCandidateStatus, Router, StatusCode,
    StoredAuthApiKeySnapshot, StoredMinimalCandidateSelectionRow, StoredProviderCatalogEndpoint,
    StoredProviderCatalogKey, StoredProviderCatalogProvider, DEVELOPMENT_ENCRYPTION_KEY,
    TRACE_ID_HEADER,
};
use crate::tests::hash_api_key;

fn run_claude_code_cli_sync_test<F, Fut>(test_name: &'static str, make_future: F)
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: std::future::Future<Output = ()> + 'static,
{
    crate::tests::run_async_test_on_large_stack(
        test_name,
        crate::tests::LARGE_TEST_STACK_BYTES,
        make_future,
    );
}

#[test]
fn gateway_executes_claude_code_cli_sync_via_local_decision_gate_with_local_sync_decision() {
    run_claude_code_cli_sync_test(
        "gateway_executes_claude_code_cli_sync_via_local_decision_gate_with_local_sync_decision",
        gateway_executes_claude_code_cli_sync_via_local_decision_gate_with_local_sync_decision_impl,
    );
}

async fn gateway_executes_claude_code_cli_sync_via_local_decision_gate_with_local_sync_decision_impl(
) {
    #[derive(Debug, Clone)]
    struct SeenExecutionRuntimeSyncRequest {
        trace_id: String,
        url: String,
        model: String,
        client_api_format: String,
        provider_api_format: String,
        authorization: String,
        accept: String,
        anthropic_version: String,
        anthropic_beta: String,
        x_app: String,
        x_stainless_helper_method: String,
        x_stainless_package_version: String,
        user_agent: String,
        endpoint_tag: String,
        metadata_mode: String,
        metadata_source: String,
        assistant_content: serde_json::Value,
        proxy_node_id: String,
        transport_profile_id: String,
    }

    fn sample_auth_snapshot(api_key_id: &str, user_id: &str) -> StoredAuthApiKeySnapshot {
        crate::tests::support::sample_auth_snapshot_with(
            api_key_id,
            user_id,
            "alice",
            "alice@example.com",
            serde_json::json!(["claude", "claude_code"]),
            serde_json::json!(["claude:messages"]),
            serde_json::json!(["claude-code"]),
        )
    }

    fn sample_candidate_row() -> StoredMinimalCandidateSelectionRow {
        crate::tests::support::minimal_candidate_row(&crate::tests::support::CandidateRowSpec {
            id_suffix: "claude-code-cli-local-1",
            provider_name: "claude_code",
            provider_type: "claude_code",
            api_format: "claude:messages",
            api_family: "claude",
            endpoint_kind: Some("cli"),
            key_name: "prod",
            key_auth_type: "oauth",
            global_model_name: "claude-code",
            upstream_model: "claude-code-upstream",
        })
    }

    fn sample_provider_catalog_provider() -> StoredProviderCatalogProvider {
        StoredProviderCatalogProvider::new(
            "provider-claude-code-cli-local-1".to_string(),
            "claude_code".to_string(),
            Some("https://example.com".to_string()),
            "claude_code".to_string(),
        )
        .expect("provider should build")
        .with_transport_fields(
            true,
            false,
            false,
            None,
            Some(2),
            Some(serde_json::json!({"url":"http://provider-proxy.internal:8080"})),
            Some(20.0),
            None,
            Some(serde_json::json!({
                "claude_code_advanced": {
                    "cli_only_enabled": false
                }
            })),
        )
    }

    fn sample_provider_catalog_endpoint() -> StoredProviderCatalogEndpoint {
        StoredProviderCatalogEndpoint::new(
            "endpoint-claude-code-cli-local-1".to_string(),
            "provider-claude-code-cli-local-1".to_string(),
            "claude:messages".to_string(),
            Some("claude".to_string()),
            Some("cli".to_string()),
            true,
        )
        .expect("endpoint should build")
        .with_transport_fields(
            "https://api.anthropic.com/v1".to_string(),
            Some(serde_json::json!([
                {"action":"set","key":"x-endpoint-tag","value":"claude-code-cli-local"}
            ])),
            Some(serde_json::json!([
                {"action":"set","path":"metadata.mode","value":"safe"},
                {"action":"rename","from":"metadata.client","to":"metadata.source"}
            ])),
            Some(2),
            None,
            None,
            None,
            None,
        )
        .expect("endpoint transport should build")
    }

    fn sample_provider_catalog_key() -> StoredProviderCatalogKey {
        StoredProviderCatalogKey::new(
            "key-claude-code-cli-local-1".to_string(),
            "provider-claude-code-cli-local-1".to_string(),
            "prod".to_string(),
            "oauth".to_string(),
            None,
            true,
        )
        .expect("key should build")
        .with_transport_fields(
            Some(serde_json::json!(["claude:messages"])),
            encrypt_python_fernet_plaintext(
                DEVELOPMENT_ENCRYPTION_KEY,
                "sk-upstream-claude-code-oauth",
            )
            .expect("api key should encrypt"),
            None,
            None,
            Some(serde_json::json!({"claude:messages": 1})),
            None,
            None,
            Some(
                serde_json::json!({"enabled": true, "node_id":"proxy-node-claude-code-cli-local"}),
            ),
            Some(serde_json::json!({
                "transport_profile": {
                    "profile_id": "claude_code_nodejs",
                    "header_fingerprint": {
                        "user_agent":"Claude-Code/9.9",
                        "stainless_package_version":"1.0.5",
                        "stainless_runtime_version":"v22.12.0",
                        "stainless_timeout":"900"
                    }
                }
            })),
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
            "/v1/messages",
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
                    client_api_format: crate::tests::support::payload_string(
                        &payload,
                        &["client_api_format"],
                    ),
                    provider_api_format: crate::tests::support::payload_string(
                        &payload,
                        &["provider_api_format"],
                    ),
                    authorization: crate::tests::support::payload_string(
                        &payload,
                        &["headers", "authorization"],
                    ),
                    accept: crate::tests::support::payload_string(&payload, &["headers", "accept"]),
                    anthropic_version: crate::tests::support::payload_string(
                        &payload,
                        &["headers", "anthropic-version"],
                    ),
                    anthropic_beta: crate::tests::support::payload_string(
                        &payload,
                        &["headers", "anthropic-beta"],
                    ),
                    x_app: crate::tests::support::payload_string(&payload, &["headers", "x-app"]),
                    x_stainless_helper_method: crate::tests::support::payload_string(
                        &payload,
                        &["headers", "x-stainless-helper-method"],
                    ),
                    x_stainless_package_version: crate::tests::support::payload_string(
                        &payload,
                        &["headers", "x-stainless-package-version"],
                    ),
                    user_agent: crate::tests::support::payload_string(
                        &payload,
                        &["headers", "user-agent"],
                    ),
                    endpoint_tag: crate::tests::support::payload_string(
                        &payload,
                        &["headers", "x-endpoint-tag"],
                    ),
                    metadata_mode: payload
                        .get("body")
                        .and_then(|value| value.get("json_body"))
                        .and_then(|value| value.get("metadata"))
                        .and_then(|value| value.get("mode"))
                        .and_then(|value| value.as_str())
                        .unwrap_or_default()
                        .to_string(),
                    metadata_source: payload
                        .get("body")
                        .and_then(|value| value.get("json_body"))
                        .and_then(|value| value.get("metadata"))
                        .and_then(|value| value.get("source"))
                        .and_then(|value| value.as_str())
                        .unwrap_or_default()
                        .to_string(),
                    assistant_content: payload
                        .get("body")
                        .and_then(|value| value.get("json_body"))
                        .and_then(|value| value.get("messages"))
                        .and_then(|value| value.get(0))
                        .and_then(|value| value.get("content"))
                        .cloned()
                        .unwrap_or(serde_json::Value::Null),
                    proxy_node_id: crate::tests::support::payload_string(
                        &payload,
                        &["proxy", "node_id"],
                    ),
                    transport_profile_id: crate::tests::support::payload_string(
                        &payload,
                        &["transport_profile", "profile_id"],
                    ),
                });
                Json(json!({
                    "request_id": "trace-claude-code-cli-local-sync-123",
                    "status_code": 200,
                    "headers": {
                        "content-type": "application/json"
                    },
                    "body": {
                        "json_body": {
                            "id": "msg-local-claude-code-cli-123",
                            "type": "message",
                            "model": "claude-code-upstream",
                            "role": "assistant",
                            "content": [],
                            "usage": {
                                "input_tokens": 2,
                                "output_tokens": 3
                            }
                        }
                    },
                    "telemetry": {
                        "elapsed_ms": 29
                    }
                }))
            }
        }),
    );

    let auth_repository = Arc::new(InMemoryAuthApiKeySnapshotRepository::seed(vec![(
        Some(hash_api_key("sk-client-claude-code-cli-local")),
        sample_auth_snapshot(
            "api-key-claude-code-cli-local-1",
            "user-claude-code-cli-local-1",
        ),
    )]));
    let candidate_selection_repository =
        Arc::new(InMemoryMinimalCandidateSelectionReadRepository::seed(vec![
            sample_candidate_row(),
        ]));
    let request_candidate_repository = Arc::new(InMemoryRequestCandidateRepository::default());
    let provider_catalog_repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![sample_provider_catalog_provider()],
        vec![sample_provider_catalog_endpoint()],
        vec![sample_provider_catalog_key()],
    ));

    let (_upstream_url, upstream_handle) = start_server(upstream).await;
    let (execution_runtime_url, execution_runtime_handle) = start_server(execution_runtime).await;
    let gateway_state = build_state_with_execution_runtime_override(execution_runtime_url.clone())
    .with_data_state_for_tests(
        crate::data::GatewayDataState::with_auth_candidate_selection_provider_catalog_and_request_candidate_repository_for_tests(
            auth_repository,
            candidate_selection_repository,
            provider_catalog_repository,
            Arc::clone(&request_candidate_repository),
            DEVELOPMENT_ENCRYPTION_KEY,
        )
        .attach_proxy_node_repository_for_tests(
            crate::tests::ai_execute::ai_execute_proxy_node_repository([
                "proxy-node-claude-code-cli-local",
            ]),
        ),
    );
    let gateway = build_router_with_state(gateway_state);
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .post(format!("{gateway_url}/v1/messages"))
        .header(http::header::CONTENT_TYPE, "application/json")
        .header(http::header::USER_AGENT, "Claude-Code/2.1.0")
        .header(
            http::header::AUTHORIZATION,
            "Bearer sk-client-claude-code-cli-local",
        )
        .header("anthropic-beta", "context-1m-2025-08-07,custom-beta")
        .header(TRACE_ID_HEADER, "trace-claude-code-cli-local-sync-123")
        .body(
            serde_json::json!({
                "model": "claude-code",
                "thinking": {"type":"enabled"},
                "messages": [{
                    "role":"assistant",
                    "content":[
                        {"type":"thinking","thinking":"keep","signature":"sig_valid"},
                        {"type":"thinking","thinking":"drop-empty-signature","signature":""},
                        {"type":"redacted_thinking","data":"keep-redacted","signature":"sig_redacted"},
                        {"type":"redacted_thinking","data":"drop-no-signature"},
                        {"type":"text","text":"ok"}
                    ]
                }],
                "metadata":{"client":"desktop-claude-code-cli"}
            })
            .to_string(),
        )
        .send()
        .await
        .expect("request should succeed");

    let status = response.status();
    let response_body = response.text().await.expect("body should read");
    assert!(
        status == StatusCode::OK,
        "unexpected status={status} body={response_body} decision_hits={} plan_hits={} public_hits={}",
        *decision_hits.lock().expect("mutex should lock"),
        *plan_hits.lock().expect("mutex should lock"),
        *public_hits.lock().expect("mutex should lock"),
    );
    let response_json: serde_json::Value =
        serde_json::from_str(&response_body).expect("body should parse");
    assert_eq!(response_json["model"], "claude-code-upstream");

    let seen_execution_runtime_request = seen_execution_runtime
        .lock()
        .expect("mutex should lock")
        .clone()
        .expect("execution runtime sync should be captured");
    assert_eq!(
        seen_execution_runtime_request.trace_id,
        "trace-claude-code-cli-local-sync-123"
    );
    assert_eq!(
        seen_execution_runtime_request.url,
        "https://api.anthropic.com/v1/messages"
    );
    assert_eq!(seen_execution_runtime_request.model, "claude-code-upstream");
    assert_eq!(
        seen_execution_runtime_request.client_api_format,
        "claude:messages"
    );
    assert_eq!(
        seen_execution_runtime_request.provider_api_format,
        "claude:messages"
    );
    assert_eq!(
        seen_execution_runtime_request.authorization,
        "Bearer sk-upstream-claude-code-oauth"
    );
    assert_eq!(seen_execution_runtime_request.accept, "application/json");
    assert_eq!(
        seen_execution_runtime_request.anthropic_version,
        "2023-06-01"
    );
    assert_eq!(
        seen_execution_runtime_request.anthropic_beta,
        "claude-code-20250219,oauth-2025-04-20,interleaved-thinking-2025-05-14,prompt-caching-scope-2026-01-05,effort-2025-11-24,context-management-2025-06-27,extended-cache-ttl-2025-04-11,context-1m-2025-08-07,custom-beta"
    );
    assert_eq!(seen_execution_runtime_request.x_app, "cli");
    assert_eq!(seen_execution_runtime_request.x_stainless_helper_method, "");
    assert_eq!(
        seen_execution_runtime_request.x_stainless_package_version,
        "0.112.1"
    );
    assert_eq!(
        seen_execution_runtime_request.user_agent,
        "claude-cli/2.1.284 (external, cli)"
    );
    assert_eq!(
        seen_execution_runtime_request.endpoint_tag,
        "claude-code-cli-local"
    );
    assert_eq!(seen_execution_runtime_request.metadata_mode, "safe");
    assert_eq!(
        seen_execution_runtime_request.metadata_source,
        "desktop-claude-code-cli"
    );
    assert_eq!(
        seen_execution_runtime_request.assistant_content,
        json!([
            {"type":"thinking","thinking":"keep","signature":"sig_valid"},
            {"type":"redacted_thinking","data":"keep-redacted","signature":"sig_redacted"},
            {"type":"text","text":"ok"}
        ])
    );
    assert_eq!(
        seen_execution_runtime_request.proxy_node_id,
        "proxy-node-claude-code-cli-local"
    );
    assert_eq!(
        seen_execution_runtime_request.transport_profile_id,
        "claude_code_nodejs"
    );

    let stored_candidates = request_candidate_repository
        .list_by_request_id("trace-claude-code-cli-local-sync-123")
        .await
        .expect("request candidate trace should read");
    assert_eq!(stored_candidates.len(), 1);
    assert_eq!(stored_candidates[0].status, RequestCandidateStatus::Success);

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
