use super::{
    any, build_router_with_state, build_state_with_execution_runtime_override, json,
    run_stream_cli_test, start_server, to_bytes, Arc, Body, Bytes, HeaderValue, Infallible, Json,
    Mutex, Request, Response, Router, StatusCode, EXECUTION_PATH_EXECUTION_RUNTIME_SYNC,
    EXECUTION_PATH_HEADER, TRACE_ID_HEADER,
};
use crate::tests::hash_api_key;
use aether_crypto::{encrypt_python_fernet_plaintext, DEVELOPMENT_ENCRYPTION_KEY};
use aether_data::repository::auth::{
    InMemoryAuthApiKeySnapshotRepository, StoredAuthApiKeySnapshot,
};
use aether_data::repository::candidate_selection::InMemoryMinimalCandidateSelectionReadRepository;
use aether_data::repository::candidates::InMemoryRequestCandidateRepository;
use aether_data::repository::provider_catalog::InMemoryProviderCatalogReadRepository;
use aether_data_contracts::repository::candidate_selection::StoredMinimalCandidateSelectionRow;
use aether_data_contracts::repository::candidates::{
    RequestCandidateReadRepository, RequestCandidateStatus,
};
use aether_data_contracts::repository::provider_catalog::{
    StoredProviderCatalogEndpoint, StoredProviderCatalogKey, StoredProviderCatalogProvider,
};

#[test]
fn gateway_executes_openai_responses_compact_as_unary_request() {
    run_stream_cli_test(
        "gateway_executes_openai_responses_compact_as_unary_request",
        gateway_executes_openai_responses_compact_as_unary_request_impl,
    );
}

async fn gateway_executes_openai_responses_compact_as_unary_request_impl() {
    #[derive(Debug, Clone)]
    struct SeenExecutionRuntimeStreamRequest {
        trace_id: String,
        url: String,
        model: String,
        content_encoding: String,
        stream: bool,
        accept: String,
        turn_state: String,
        authorization: String,
        chatgpt_account_id: String,
        fedramp: String,
        responses_lite: String,
        session_id: String,
        thread_id: String,
        x_client_request_id_present: bool,
        endpoint_tag: String,
        conditional_header: String,
        renamed_header: String,
        dropped_header_present: bool,
        instructions: String,
        store_present: bool,
        body: serde_json::Value,
        proxy_node_id: String,
        transport_profile_id: String,
    }

    fn sample_auth_snapshot(api_key_id: &str, user_id: &str) -> StoredAuthApiKeySnapshot {
        crate::tests::support::sample_auth_snapshot_with(
            api_key_id,
            user_id,
            "alice",
            "alice@example.com",
            serde_json::json!(["openai"]),
            serde_json::json!(["openai:responses:compact"]),
            serde_json::json!(["gpt-5.6-sol"]),
        )
    }

    fn sample_candidate_row() -> StoredMinimalCandidateSelectionRow {
        crate::tests::support::minimal_candidate_row(&crate::tests::support::CandidateRowSpec {
            id_suffix: "openai-compact-local-1",
            provider_name: "openai",
            provider_type: "codex",
            api_format: "openai:responses:compact",
            api_family: "openai",
            endpoint_kind: Some("compact"),
            key_name: "prod",
            key_auth_type: "oauth",
            global_model_name: "gpt-5.6-sol",
            upstream_model: "deployment-production",
        })
    }

    fn sample_provider_catalog_provider() -> StoredProviderCatalogProvider {
        StoredProviderCatalogProvider::new(
            "provider-openai-compact-local-1".to_string(),
            "openai".to_string(),
            Some("https://example.com".to_string()),
            "codex".to_string(),
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
            None,
        )
    }

    fn sample_provider_catalog_endpoint() -> StoredProviderCatalogEndpoint {
        StoredProviderCatalogEndpoint::new(
            "endpoint-openai-compact-local-1".to_string(),
            "provider-openai-compact-local-1".to_string(),
            "openai:responses:compact".to_string(),
            Some("openai".to_string()),
            Some("compact".to_string()),
            true,
        )
        .expect("endpoint should build")
        .with_transport_fields(
            "https://api.openai.example".to_string(),
            Some(serde_json::json!([
                {"action":"set","key":"x-endpoint-tag","value":"openai-compact-local"},
                {"action":"set","key":"x-conditional-tag","value":"header-condition-hit","condition":{"path":"reasoning","op":"exists","source":"current"}},
                {"action":"rename","from":"x-client-rename","to":"x-upstream-rename"},
                {"action":"drop","key":"x-drop-me"}
            ])),
            Some(serde_json::json!([
                {"action":"set","path":"instructions","value":"Use the configured tools.","condition":{"path":"instructions","op":"not_exists","source":"current"}},
                {"action":"drop","path":"store"}
            ])),
            Some(2),
            Some("/custom/v1/responses/compact".to_string()),
            None,
            None,
            None,
        )
        .expect("endpoint transport should build")
    }

    fn sample_provider_catalog_key() -> StoredProviderCatalogKey {
        StoredProviderCatalogKey::new(
            "key-openai-compact-local-1".to_string(),
            "provider-openai-compact-local-1".to_string(),
            "prod".to_string(),
            "oauth".to_string(),
            None,
            true,
        )
        .expect("key should build")
        .with_transport_fields(
            Some(serde_json::json!(["openai:responses:compact"])),
            encrypt_python_fernet_plaintext(
                DEVELOPMENT_ENCRYPTION_KEY,
                "sk-upstream-openai-compact",
            )
            .expect("api key should encrypt"),
            Some(
                encrypt_python_fernet_plaintext(
                    DEVELOPMENT_ENCRYPTION_KEY,
                    r#"{"account_id":"acc-compact-local-123","is_fedramp":true}"#,
                )
                .expect("auth config should encrypt"),
            ),
            None,
            Some(serde_json::json!({"openai:responses:compact": 1})),
            None,
            None,
            Some(serde_json::json!({"enabled": true, "node_id":"proxy-node-openai-compact-local"})),
            Some(serde_json::json!({"transport_profile":"chrome_136"})),
        )
        .expect("key transport should build")
    }

    let seen_execution_runtime = Arc::new(Mutex::new(None::<SeenExecutionRuntimeStreamRequest>));
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
            "/api/internal/gateway/resolve",
            any(|_request: Request| async move {
                Json(json!({
                    "action": "proxy_public",
                    "route_class": "ai_public",
                    "route_family": "openai",
                    "route_kind": "compact",
                    "auth_endpoint_signature": "openai:responses:compact",
                    "execution_runtime_candidate": true,
                    "auth_context": {
                        "user_id": "user-openai-compact-local-123",
                        "api_key_id": "key-openai-compact-local-123",
                        "access_allowed": true
                    },
                    "public_path": "/v1/responses/compact"
                }))
            }),
        )
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
            "/v1/responses/compact",
            any(move |_request: Request| {
                let public_hits_inner = Arc::clone(&public_hits_clone);
                async move {
                    *public_hits_inner.lock().expect("mutex should lock") += 1;
                    let stream = futures_util::stream::iter([
                        Ok::<_, Infallible>(Bytes::from_static(b"event: response.completed\n")),
                        Ok::<_, Infallible>(Bytes::from_static(
                            b"data: {\"type\":\"response.completed\"}\n\n",
                        )),
                    ]);
                    let mut response = Response::builder()
                        .status(StatusCode::OK)
                        .body(Body::from_stream(stream))
                        .expect("response should build");
                    response.headers_mut().insert(
                        http::header::CONTENT_TYPE,
                        HeaderValue::from_static("text/event-stream"),
                    );
                    response
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
                    .expect("mutex should lock") = Some(SeenExecutionRuntimeStreamRequest {
                    trace_id: crate::tests::support::header_string(&parts.headers, TRACE_ID_HEADER),
                    url: crate::tests::support::payload_string(&payload, &["url"]),
                    model: payload
                        .get("body")
                        .and_then(|value| value.get("json_body"))
                        .and_then(|value| value.get("model"))
                        .and_then(|value| value.as_str())
                        .unwrap_or_default()
                        .to_string(),
                    content_encoding: crate::tests::support::payload_string(
                        &payload,
                        &["content_encoding"],
                    ),
                    stream: payload
                        .get("stream")
                        .and_then(|value| value.as_bool())
                        .unwrap_or(false),
                    accept: crate::tests::support::payload_string(&payload, &["headers", "accept"]),
                    turn_state: crate::tests::support::payload_string(
                        &payload,
                        &["headers", "x-codex-turn-state"],
                    ),
                    authorization: crate::tests::support::payload_string(
                        &payload,
                        &["headers", "authorization"],
                    ),
                    chatgpt_account_id: crate::tests::support::payload_string(
                        &payload,
                        &["headers", "chatgpt-account-id"],
                    ),
                    fedramp: crate::tests::support::payload_string(
                        &payload,
                        &["headers", "x-openai-fedramp"],
                    ),
                    responses_lite: crate::tests::support::payload_string(
                        &payload,
                        &["headers", "x-openai-internal-codex-responses-lite"],
                    ),
                    session_id: crate::tests::support::payload_string(
                        &payload,
                        &["headers", "session-id"],
                    ),
                    thread_id: crate::tests::support::payload_string(
                        &payload,
                        &["headers", "thread-id"],
                    ),
                    x_client_request_id_present: payload
                        .get("headers")
                        .and_then(|value| value.get("x-client-request-id"))
                        .is_some(),
                    endpoint_tag: crate::tests::support::payload_string(
                        &payload,
                        &["headers", "x-endpoint-tag"],
                    ),
                    conditional_header: crate::tests::support::payload_string(
                        &payload,
                        &["headers", "x-conditional-tag"],
                    ),
                    renamed_header: crate::tests::support::payload_string(
                        &payload,
                        &["headers", "x-upstream-rename"],
                    ),
                    dropped_header_present: payload
                        .get("headers")
                        .and_then(|value| value.get("x-drop-me"))
                        .is_some(),
                    instructions: payload
                        .get("body")
                        .and_then(|value| value.get("json_body"))
                        .and_then(|value| value.get("instructions"))
                        .and_then(|value| value.as_str())
                        .unwrap_or_default()
                        .to_string(),
                    store_present: payload
                        .get("body")
                        .and_then(|value| value.get("json_body"))
                        .and_then(|value| value.get("store"))
                        .is_some(),
                    body: payload
                        .get("body")
                        .and_then(|value| value.get("json_body"))
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
                    "request_id": "trace-openai-compact-local-123",
                    "status_code": 200,
                    "headers": {
                        "content-type": "application/json",
                        "x-codex-turn-state": "turn-state-compact-123"
                    },
                    "body": {
                        "json_body": {
                            "output": [{
                                "type": "compaction",
                                "id": "cmp-compact-local-123",
                                "encrypted_content": "encrypted-compact-history"
                            }]
                        }
                    },
                    "telemetry": {"elapsed_ms": 41, "ttfb_ms": 11}
                }))
            }
        }),
    );

    let client_api_key = "sk-client-openai-compact-local";
    let auth_repository = Arc::new(InMemoryAuthApiKeySnapshotRepository::seed(vec![(
        Some(hash_api_key(client_api_key)),
        sample_auth_snapshot(
            "key-openai-compact-local-123",
            "user-openai-compact-local-123",
        ),
    )]));
    let candidate_selection_repository =
        Arc::new(InMemoryMinimalCandidateSelectionReadRepository::seed(vec![
            sample_candidate_row(),
        ]));
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
                "proxy-node-openai-compact-local",
            ]),
        ),
    );
    let gateway = build_router_with_state(gateway_state);
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .post(format!("{gateway_url}/v1/responses/compact"))
        .header(http::header::CONTENT_TYPE, "application/json")
        .header(
            http::header::AUTHORIZATION,
            format!("Bearer {client_api_key}"),
        )
        .header("x-client-rename", "rename-openai-compact")
        .header("x-drop-me", "drop-openai-compact")
        .header("x-codex-turn-state", "turn-state-inbound-123")
        .header("session-id", "session-compact-local-123")
        .header("thread-id", "thread-compact-local-123")
        .header(TRACE_ID_HEADER, "trace-openai-compact-local-123")
        .body(r#"{"model":"gpt-5.6-sol","input":"hello","client_metadata":{"origin":"codex"},"include":["reasoning.encrypted_content"],"store":false,"stream":true,"stream_options":{"reasoning_summary_delivery":"sequential_cutoff"},"tool_choice":"auto","parallel_tool_calls":true,"reasoning":{"effort":"high"},"text":{"verbosity":"medium"},"tools":[{"type":"function","name":"lookup","parameters":{"type":"object"}}],"prompt_cache_key":"session:compact-e2e"}"#)
        .send()
        .await
        .expect("request should succeed");

    if response.status() != StatusCode::OK {
        let status = response.status();
        let headers = response.headers().clone();
        let body = response.text().await.expect("error body should read");
        panic!("Compact request failed: status={status}, headers={headers:?}, body={body}");
    }
    assert_eq!(
        response
            .headers()
            .get("x-codex-turn-state")
            .and_then(|value| value.to_str().ok()),
        Some("turn-state-compact-123")
    );
    assert_eq!(
        response
            .headers()
            .get(EXECUTION_PATH_HEADER)
            .and_then(|value| value.to_str().ok()),
        Some(EXECUTION_PATH_EXECUTION_RUNTIME_SYNC)
    );
    let body: serde_json::Value = response.json().await.expect("body should parse");
    assert_eq!(
        body["output"][0]["encrypted_content"],
        "encrypted-compact-history"
    );

    let seen_execution_runtime_request = seen_execution_runtime
        .lock()
        .expect("mutex should lock")
        .clone()
        .expect("execution runtime stream should be captured");
    assert_eq!(
        seen_execution_runtime_request.trace_id,
        "trace-openai-compact-local-123"
    );
    assert_eq!(
        seen_execution_runtime_request.url,
        "https://api.openai.example/custom/v1/responses/compact"
    );
    assert_eq!(
        seen_execution_runtime_request.model,
        "deployment-production"
    );
    assert!(seen_execution_runtime_request.content_encoding.is_empty());
    assert!(!seen_execution_runtime_request.stream);
    assert_ne!(seen_execution_runtime_request.accept, "text/event-stream");
    assert_eq!(
        seen_execution_runtime_request.turn_state,
        "turn-state-inbound-123"
    );
    assert_eq!(
        seen_execution_runtime_request.authorization,
        "Bearer sk-upstream-openai-compact"
    );
    assert_eq!(
        seen_execution_runtime_request.chatgpt_account_id,
        "acc-compact-local-123"
    );
    assert_eq!(seen_execution_runtime_request.fedramp, "true");
    assert_eq!(seen_execution_runtime_request.responses_lite, "true");
    assert_eq!(
        seen_execution_runtime_request.session_id,
        "session-compact-local-123"
    );
    assert_eq!(
        seen_execution_runtime_request.thread_id,
        "thread-compact-local-123"
    );
    assert!(!seen_execution_runtime_request.x_client_request_id_present);
    assert_eq!(
        seen_execution_runtime_request.endpoint_tag,
        "openai-compact-local"
    );
    assert_eq!(
        seen_execution_runtime_request.conditional_header,
        "header-condition-hit"
    );
    assert_eq!(
        seen_execution_runtime_request.renamed_header,
        "rename-openai-compact"
    );
    assert!(!seen_execution_runtime_request.dropped_header_present);
    assert!(seen_execution_runtime_request.instructions.is_empty());
    assert!(!seen_execution_runtime_request.store_present);
    for field in [
        "client_metadata",
        "include",
        "store",
        "stream",
        "stream_options",
        "tool_choice",
    ] {
        assert!(
            seen_execution_runtime_request.body.get(field).is_none(),
            "Compact request must omit {field}"
        );
    }
    assert_eq!(
        seen_execution_runtime_request.body["parallel_tool_calls"],
        json!(false)
    );
    assert_eq!(
        seen_execution_runtime_request.body["reasoning"]["effort"],
        json!("high")
    );
    assert_eq!(
        seen_execution_runtime_request.body["text"]["verbosity"],
        json!("medium")
    );
    assert_eq!(
        seen_execution_runtime_request.body["reasoning"]["context"],
        json!("all_turns")
    );
    assert_eq!(
        seen_execution_runtime_request.body["input"][0]["type"],
        json!("additional_tools")
    );
    assert_eq!(
        seen_execution_runtime_request.body["input"][0]["tools"][0]["name"],
        json!("lookup")
    );
    assert_eq!(
        seen_execution_runtime_request.body["input"][1]["role"],
        json!("developer")
    );
    assert_eq!(
        seen_execution_runtime_request.body["input"][1]["content"][0]["text"],
        json!("Use the configured tools.")
    );
    assert_eq!(
        seen_execution_runtime_request.body["input"][2]["content"][0]["text"],
        json!("hello")
    );
    assert_eq!(
        seen_execution_runtime_request.body["prompt_cache_key"],
        // Compact omits client_metadata, but keeps the same deterministic Codex cache identity.
        json!("f3eb8726-b7b2-56c5-90b5-8789d628c8cf")
    );
    assert_eq!(
        seen_execution_runtime_request.proxy_node_id,
        "proxy-node-openai-compact-local"
    );
    assert_eq!(
        seen_execution_runtime_request.transport_profile_id,
        "chrome_136"
    );

    let stored_candidates = request_candidate_repository
        .list_by_request_id("trace-openai-compact-local-123")
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
