use super::{
    any, build_router_with_state, build_state_with_execution_runtime_override, json, start_server,
    to_bytes, Arc, Body, Json, Mutex, Request, Router, StatusCode, TRACE_ID_HEADER,
};
use crate::data::GatewayDataState;
use crate::tests::hash_api_key;
use aether_crypto::DEVELOPMENT_ENCRYPTION_KEY;
use aether_data::repository::auth::{
    InMemoryAuthApiKeySnapshotRepository, StoredAuthApiKeySnapshot,
};
use aether_data::repository::candidate_selection::InMemoryMinimalCandidateSelectionReadRepository;
use aether_data::repository::candidates::InMemoryRequestCandidateRepository;
use aether_data::repository::provider_catalog::InMemoryProviderCatalogReadRepository;
use aether_data::repository::usage::InMemoryUsageReadRepository;
use aether_data_contracts::repository::candidate_selection::StoredMinimalCandidateSelectionRow;
use aether_data_contracts::repository::candidates::{
    RequestCandidateReadRepository, RequestCandidateStatus,
};
use aether_data_contracts::repository::provider_catalog::{
    StoredProviderCatalogEndpoint, StoredProviderCatalogKey, StoredProviderCatalogProvider,
};

fn run_claude_provider_finalize_test<F, Fut>(test_name: &'static str, make_future: F)
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
fn gateway_executes_claude_chat_sync_same_format_via_local_finalize_response() {
    run_claude_provider_finalize_test(
        "gateway_executes_claude_chat_sync_same_format_via_local_finalize_response",
        gateway_executes_claude_chat_sync_same_format_via_local_finalize_response_impl,
    );
}

async fn gateway_executes_claude_chat_sync_same_format_via_local_finalize_response_impl() {
    #[derive(Debug, Clone)]
    struct SeenRemoteExecutionRuntimeRequest {
        trace_id: String,
        request_id: String,
        url: String,
        model: String,
        auth_header_value: String,
        endpoint_tag: String,
    }

    fn sample_auth_snapshot(api_key_id: &str, user_id: &str) -> StoredAuthApiKeySnapshot {
        crate::tests::support::sample_auth_snapshot_with(
            api_key_id,
            user_id,
            "alice",
            "alice@example.com",
            serde_json::json!(["claude"]),
            serde_json::json!(["claude:messages"]),
            serde_json::json!(["claude-sonnet-4"]),
        )
    }

    fn sample_candidate_row() -> StoredMinimalCandidateSelectionRow {
        crate::tests::support::minimal_candidate_row(&crate::tests::support::CandidateRowSpec {
            id_suffix: "claude-chat-finalize-local-1",
            provider_name: "claude",
            provider_type: "custom",
            api_format: "claude:messages",
            api_family: "claude",
            endpoint_kind: Some("chat"),
            key_name: "prod",
            key_auth_type: "api_key",
            global_model_name: "claude-sonnet-4",
            upstream_model: "claude-sonnet-4-upstream",
        })
    }

    fn sample_provider_catalog_provider() -> StoredProviderCatalogProvider {
        StoredProviderCatalogProvider::new(
            "provider-claude-chat-finalize-local-1".to_string(),
            "claude".to_string(),
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
            "endpoint-claude-chat-finalize-local-1".to_string(),
            "provider-claude-chat-finalize-local-1".to_string(),
            "claude:messages".to_string(),
            Some("claude".to_string()),
            Some("chat".to_string()),
            true,
        )
        .expect("endpoint should build")
        .with_transport_fields(
            "https://api.anthropic.example".to_string(),
            Some(serde_json::json!([
                {"action":"set","key":"x-endpoint-tag","value":"claude-chat-finalize-local"}
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
            id_suffix: "claude-chat-finalize-local-1",
            key_name: "prod",
            auth_type: "api_key",
            api_format: "claude:messages",
            secret: "sk-upstream-claude-chat-finalize",
        })
    }

    let seen_remote_execution_runtime =
        Arc::new(Mutex::new(None::<SeenRemoteExecutionRuntimeRequest>));
    let seen_remote_execution_runtime_clone = Arc::clone(&seen_remote_execution_runtime);
    let report_hits = Arc::new(Mutex::new(0usize));
    let report_hits_clone = Arc::clone(&report_hits);
    let finalize_hits = Arc::new(Mutex::new(0usize));
    let finalize_hits_clone = Arc::clone(&finalize_hits);
    let decision_hits = Arc::new(Mutex::new(0usize));
    let decision_hits_clone = Arc::clone(&decision_hits);
    let plan_hits = Arc::new(Mutex::new(0usize));
    let plan_hits_clone = Arc::clone(&plan_hits);
    let public_hits = Arc::new(Mutex::new(0usize));
    let public_hits_clone = Arc::clone(&public_hits);
    let request_candidate_repository = Arc::new(InMemoryRequestCandidateRepository::default());
    let usage_repository = Arc::new(InMemoryUsageReadRepository::default());

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
            "/api/internal/gateway/finalize-sync",
            any(move |_request: Request| {
                let finalize_hits_inner = Arc::clone(&finalize_hits_clone);
                async move {
                    *finalize_hits_inner.lock().expect("mutex should lock") += 1;
                    (
                        StatusCode::IM_A_TEAPOT,
                        Body::from("finalize-sync-should-not-be-hit"),
                    )
                }
            }),
        )
        .route(
            "/api/internal/gateway/report-sync",
            any(move |_request: Request| {
                let report_hits_inner = Arc::clone(&report_hits_clone);
                async move {
                    *report_hits_inner.lock().expect("mutex should lock") += 1;
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
            let seen_remote_execution_runtime_inner =
                Arc::clone(&seen_remote_execution_runtime_clone);
            async move {
                let (parts, body) = request.into_parts();
                let raw_body = to_bytes(body, usize::MAX).await.expect("body should read");
                let payload: serde_json::Value = serde_json::from_slice(&raw_body)
                    .expect("execution runtime payload should parse");
                *seen_remote_execution_runtime_inner
                    .lock()
                    .expect("mutex should lock") = Some(SeenRemoteExecutionRuntimeRequest {
                    trace_id: crate::tests::support::header_string(&parts.headers, TRACE_ID_HEADER),
                    request_id: crate::tests::support::payload_string(&payload, &["request_id"]),
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
                });
                Json(json!({
                    "request_id": "trace-claude-chat-sync-local-123",
                    "status_code": 200,
                    "headers": {
                        "content-type": "application/json"
                    },
                    "body": {
                        "json_body": {
                            "id": "msg_claude_sync_local_123",
                            "type": "message",
                            "role": "assistant",
                            "model": "claude-sonnet-4-upstream",
                            "content": [{
                                "type": "text",
                                "text": "Hello Claude"
                            }],
                            "stop_reason": "end_turn",
                            "stop_sequence": null,
                            "usage": {
                                "input_tokens": 5,
                                "output_tokens": 7
                            }
                        }
                    },
                    "telemetry": {
                        "elapsed_ms": 35
                    }
                }))
            }
        }),
    );

    let auth_repository = Arc::new(InMemoryAuthApiKeySnapshotRepository::seed(vec![(
        Some(hash_api_key("sk-client-claude-chat-sync-local")),
        sample_auth_snapshot(
            "api-key-claude-chat-sync-local-1",
            "user-claude-chat-sync-local-1",
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
    let gateway_state =
        build_state_with_execution_runtime_override(execution_runtime_url.clone())
            .with_data_state_for_tests(
                GatewayDataState::with_auth_candidate_selection_provider_catalog_request_candidates_and_usage_for_tests(
                    auth_repository,
                    candidate_selection_repository,
                    provider_catalog_repository,
                    Arc::clone(&request_candidate_repository),
                    Arc::clone(&usage_repository),
                    DEVELOPMENT_ENCRYPTION_KEY,
                ),
            );
    let gateway = build_router_with_state(gateway_state);
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let started_at = std::time::Instant::now();
    let response = reqwest::Client::new()
        .post(format!("{gateway_url}/v1/messages"))
        .header(http::header::CONTENT_TYPE, "application/json")
        .header("x-api-key", "sk-client-claude-chat-sync-local")
        .header(TRACE_ID_HEADER, "trace-claude-chat-sync-local-123")
        .body("{\"model\":\"claude-sonnet-4\",\"messages\":[]}")
        .send()
        .await
        .expect("request should succeed");
    let elapsed = started_at.elapsed();

    assert_eq!(response.status(), StatusCode::OK);
    let response_json: serde_json::Value = response.json().await.expect("body should parse");
    assert_eq!(
        response_json,
        json!({
            "id": "msg_claude_sync_local_123",
            "type": "message",
            "role": "assistant",
            "model": "claude-sonnet-4-upstream",
            "content": [{
                "type": "text",
                "text": "Hello Claude"
            }],
            "stop_reason": "end_turn",
            "stop_sequence": null,
            "usage": {
                "input_tokens": 5,
                "output_tokens": 7
            }
        })
    );
    assert!(
        elapsed < std::time::Duration::from_millis(10_000),
        "response took unexpectedly long for local finalize path: elapsed={elapsed:?} finalize_hits={} report_hits={}",
        *finalize_hits.lock().expect("mutex should lock"),
        *report_hits.lock().expect("mutex should lock"),
    );

    let seen_remote_execution_runtime_request = seen_remote_execution_runtime
        .lock()
        .expect("mutex should lock")
        .clone()
        .expect("remote execution runtime plan should be captured");
    assert_eq!(
        seen_remote_execution_runtime_request.trace_id,
        "trace-claude-chat-sync-local-123"
    );
    assert_eq!(
        seen_remote_execution_runtime_request.request_id,
        "trace-claude-chat-sync-local-123"
    );
    assert_eq!(
        seen_remote_execution_runtime_request.url,
        "https://api.anthropic.example/custom/v1/messages"
    );
    assert_eq!(
        seen_remote_execution_runtime_request.model,
        "claude-sonnet-4-upstream"
    );
    assert_eq!(
        seen_remote_execution_runtime_request.auth_header_value,
        "sk-upstream-claude-chat-finalize"
    );
    assert_eq!(
        seen_remote_execution_runtime_request.endpoint_tag,
        "claude-chat-finalize-local"
    );

    let mut stored_candidates = Vec::new();
    for _ in 0..50 {
        stored_candidates = request_candidate_repository
            .list_by_request_id("trace-claude-chat-sync-local-123")
            .await
            .expect("request candidate trace should read");
        if stored_candidates.len() == 1
            && stored_candidates[0].status == RequestCandidateStatus::Success
        {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert_eq!(stored_candidates.len(), 1);
    assert_eq!(stored_candidates[0].status, RequestCandidateStatus::Success);

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    assert_eq!(
        *report_hits.lock().expect("mutex should lock"),
        0,
        "report-sync should stay local when request candidate persistence is available"
    );
    assert_eq!(*finalize_hits.lock().expect("mutex should lock"), 0);
    assert_eq!(*decision_hits.lock().expect("mutex should lock"), 0);
    assert_eq!(*plan_hits.lock().expect("mutex should lock"), 0);
    assert_eq!(*public_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    execution_runtime_handle.abort();
    upstream_handle.abort();
}

#[test]
fn gateway_executes_claude_chat_sync_upstream_stream_via_local_finalize_response() {
    run_claude_provider_finalize_test(
        "gateway_executes_claude_chat_sync_upstream_stream_via_local_finalize_response",
        gateway_executes_claude_chat_sync_upstream_stream_via_local_finalize_response_impl,
    );
}

async fn gateway_executes_claude_chat_sync_upstream_stream_via_local_finalize_response_impl() {
    use base64::Engine as _;

    #[derive(Debug, Clone)]
    struct SeenRemoteExecutionRuntimeRequest {
        trace_id: String,
        request_id: String,
        url: String,
        model: String,
        auth_header_value: String,
        endpoint_tag: String,
        accept: String,
    }

    fn sample_auth_snapshot(api_key_id: &str, user_id: &str) -> StoredAuthApiKeySnapshot {
        crate::tests::support::sample_auth_snapshot_with(
            api_key_id,
            user_id,
            "alice",
            "alice@example.com",
            serde_json::json!(["claude"]),
            serde_json::json!(["claude:messages"]),
            serde_json::json!(["claude-sonnet-4"]),
        )
    }

    fn sample_candidate_row() -> StoredMinimalCandidateSelectionRow {
        crate::tests::support::minimal_candidate_row(&crate::tests::support::CandidateRowSpec {
            id_suffix: "claude-chat-stream-finalize-local-1",
            provider_name: "claude",
            provider_type: "custom",
            api_format: "claude:messages",
            api_family: "claude",
            endpoint_kind: Some("chat"),
            key_name: "prod",
            key_auth_type: "api_key",
            global_model_name: "claude-sonnet-4",
            upstream_model: "claude-sonnet-4-upstream",
        })
    }

    fn sample_provider_catalog_provider() -> StoredProviderCatalogProvider {
        StoredProviderCatalogProvider::new(
            "provider-claude-chat-stream-finalize-local-1".to_string(),
            "claude".to_string(),
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
            "endpoint-claude-chat-stream-finalize-local-1".to_string(),
            "provider-claude-chat-stream-finalize-local-1".to_string(),
            "claude:messages".to_string(),
            Some("claude".to_string()),
            Some("chat".to_string()),
            true,
        )
        .expect("endpoint should build")
        .with_transport_fields(
            "https://api.anthropic.example".to_string(),
            Some(serde_json::json!([
                {"action":"set","key":"x-endpoint-tag","value":"claude-chat-stream-finalize-local"}
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
            id_suffix: "claude-chat-stream-finalize-local-1",
            key_name: "prod",
            auth_type: "api_key",
            api_format: "claude:messages",
            secret: "sk-upstream-claude-chat-stream-finalize",
        })
    }

    let seen_remote_execution_runtime =
        Arc::new(Mutex::new(None::<SeenRemoteExecutionRuntimeRequest>));
    let seen_remote_execution_runtime_clone = Arc::clone(&seen_remote_execution_runtime);
    let report_hits = Arc::new(Mutex::new(0usize));
    let report_hits_clone = Arc::clone(&report_hits);
    let finalize_hits = Arc::new(Mutex::new(0usize));
    let finalize_hits_clone = Arc::clone(&finalize_hits);
    let decision_hits = Arc::new(Mutex::new(0usize));
    let decision_hits_clone = Arc::clone(&decision_hits);
    let plan_hits = Arc::new(Mutex::new(0usize));
    let plan_hits_clone = Arc::clone(&plan_hits);
    let public_hits = Arc::new(Mutex::new(0usize));
    let public_hits_clone = Arc::clone(&public_hits);
    let request_candidate_repository = Arc::new(InMemoryRequestCandidateRepository::default());
    let usage_repository = Arc::new(InMemoryUsageReadRepository::default());

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
            "/api/internal/gateway/finalize-sync",
            any(move |_request: Request| {
                let finalize_hits_inner = Arc::clone(&finalize_hits_clone);
                async move {
                    *finalize_hits_inner.lock().expect("mutex should lock") += 1;
                    (
                        StatusCode::IM_A_TEAPOT,
                        Body::from("finalize-sync-should-not-be-hit"),
                    )
                }
            }),
        )
        .route(
            "/api/internal/gateway/report-sync",
            any(move |_request: Request| {
                let report_hits_inner = Arc::clone(&report_hits_clone);
                async move {
                    *report_hits_inner.lock().expect("mutex should lock") += 1;
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
        any(move |request: Request| async move {
            let (parts, body) = request.into_parts();
            let raw_body = to_bytes(body, usize::MAX).await.expect("body should read");
            let payload: serde_json::Value =
                serde_json::from_slice(&raw_body).expect("execution runtime payload should parse");
            *seen_remote_execution_runtime_clone
                .lock()
                .expect("mutex should lock") = Some(SeenRemoteExecutionRuntimeRequest {
                trace_id: crate::tests::support::header_string(&parts.headers, TRACE_ID_HEADER),
                request_id: crate::tests::support::payload_string(&payload, &["request_id"]),
                url: crate::tests::support::payload_string(&payload, &["url"]),
                model: payload
                    .get("body")
                    .and_then(|value| value.get("json_body"))
                    .and_then(|value| value.get("model"))
                    .and_then(|value| value.as_str())
                    .unwrap_or_default()
                    .to_string(),
                auth_header_value: crate::tests::support::payload_string(&payload, &["headers", "x-api-key"]),
                endpoint_tag: crate::tests::support::payload_string(&payload, &["headers", "x-endpoint-tag"]),
                accept: crate::tests::support::payload_string(&payload, &["headers", "accept"]),
            });
            Json(json!({
                "request_id": "trace-claude-chat-stream-sync-direct-123",
                "status_code": 200,
                "headers": {
                    "content-type": "text/event-stream"
                },
                "body": {
                    "body_bytes_b64": base64::engine::general_purpose::STANDARD.encode(
                        concat!(
                            "event: message_start\n",
                            "data: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_claude_sync_123\",\"type\":\"message\",\"role\":\"assistant\",\"model\":\"claude-sonnet-4-upstream\",\"content\":[],\"stop_reason\":null,\"stop_sequence\":null}}\n\n",
                            "event: content_block_start\n",
                            "data: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n",
                            "event: content_block_delta\n",
                            "data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"Hello\"}}\n\n",
                            "event: content_block_delta\n",
                            "data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\" Claude\"}}\n\n",
                            "event: content_block_stop\n",
                            "data: {\"type\":\"content_block_stop\",\"index\":0}\n\n",
                            "event: message_delta\n",
                            "data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"input_tokens\":5,\"output_tokens\":7}}\n\n",
                            "event: message_stop\n",
                            "data: {\"type\":\"message_stop\"}\n\n"
                        )
                    )
                },
                "telemetry": {
                    "elapsed_ms": 34
                }
            }))
        }),
    );

    let auth_repository = Arc::new(InMemoryAuthApiKeySnapshotRepository::seed(vec![(
        Some(hash_api_key("sk-client-claude-chat-stream-sync-local")),
        sample_auth_snapshot(
            "api-key-claude-chat-stream-sync-local-1",
            "user-claude-chat-stream-sync-local-1",
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
    let gateway_state =
        build_state_with_execution_runtime_override(execution_runtime_url.clone())
            .with_data_state_for_tests(
                GatewayDataState::with_auth_candidate_selection_provider_catalog_request_candidates_and_usage_for_tests(
                    auth_repository,
                    candidate_selection_repository,
                    provider_catalog_repository,
                    Arc::clone(&request_candidate_repository),
                    Arc::clone(&usage_repository),
                    DEVELOPMENT_ENCRYPTION_KEY,
                ),
            );
    let gateway = build_router_with_state(gateway_state);
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let started_at = std::time::Instant::now();
    let response = reqwest::Client::new()
        .post(format!("{gateway_url}/v1/messages"))
        .header(http::header::CONTENT_TYPE, "application/json")
        .header("x-api-key", "sk-client-claude-chat-stream-sync-local")
        .header(TRACE_ID_HEADER, "trace-claude-chat-stream-sync-direct-123")
        .body("{\"model\":\"claude-sonnet-4\",\"messages\":[]}")
        .send()
        .await
        .expect("request should succeed");
    let elapsed = started_at.elapsed();

    assert_eq!(response.status(), StatusCode::OK);
    let response_json: serde_json::Value = response.json().await.expect("body should parse");
    assert_eq!(
        response_json,
        json!({
            "id": "msg_claude_sync_123",
            "type": "message",
            "role": "assistant",
            "model": "claude-sonnet-4-upstream",
            "content": [{
                "type": "text",
                "text": "Hello Claude"
            }],
            "stop_reason": "end_turn",
            "stop_sequence": null,
            "usage": {
                "input_tokens": 5,
                "output_tokens": 7
            }
        })
    );
    assert!(
        elapsed < std::time::Duration::from_millis(10_000),
        "response took unexpectedly long for local finalize path: elapsed={elapsed:?} finalize_hits={} report_hits={}",
        *finalize_hits.lock().expect("mutex should lock"),
        *report_hits.lock().expect("mutex should lock"),
    );

    let seen_remote_execution_runtime_request = seen_remote_execution_runtime
        .lock()
        .expect("mutex should lock")
        .clone()
        .expect("remote execution runtime plan should be captured");
    assert_eq!(
        seen_remote_execution_runtime_request.trace_id,
        "trace-claude-chat-stream-sync-direct-123"
    );
    assert_eq!(
        seen_remote_execution_runtime_request.request_id,
        "trace-claude-chat-stream-sync-direct-123"
    );
    assert_eq!(
        seen_remote_execution_runtime_request.url,
        "https://api.anthropic.example/custom/v1/messages"
    );
    assert_eq!(
        seen_remote_execution_runtime_request.model,
        "claude-sonnet-4-upstream"
    );
    assert_eq!(
        seen_remote_execution_runtime_request.auth_header_value,
        "sk-upstream-claude-chat-stream-finalize"
    );
    assert_eq!(
        seen_remote_execution_runtime_request.endpoint_tag,
        "claude-chat-stream-finalize-local"
    );
    assert_eq!(seen_remote_execution_runtime_request.accept, "*/*");

    let mut stored_candidates = Vec::new();
    for _ in 0..50 {
        stored_candidates = request_candidate_repository
            .list_by_request_id("trace-claude-chat-stream-sync-direct-123")
            .await
            .expect("request candidate trace should read");
        if stored_candidates.len() == 1
            && stored_candidates[0].status == RequestCandidateStatus::Success
        {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert_eq!(stored_candidates.len(), 1);
    assert_eq!(stored_candidates[0].status, RequestCandidateStatus::Success);

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    assert_eq!(
        *report_hits.lock().expect("mutex should lock"),
        0,
        "report-sync should stay local when request candidate persistence is available"
    );
    assert_eq!(*finalize_hits.lock().expect("mutex should lock"), 0);
    assert_eq!(*decision_hits.lock().expect("mutex should lock"), 0);
    assert_eq!(*plan_hits.lock().expect("mutex should lock"), 0);
    assert_eq!(*public_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    execution_runtime_handle.abort();
    upstream_handle.abort();
}

#[test]
fn gateway_executes_claude_cli_sync_upstream_stream_via_local_finalize_response() {
    run_claude_provider_finalize_test(
        "gateway_executes_claude_cli_sync_upstream_stream_via_local_finalize_response",
        gateway_executes_claude_cli_sync_upstream_stream_via_local_finalize_response_impl,
    );
}

async fn gateway_executes_claude_cli_sync_upstream_stream_via_local_finalize_response_impl() {
    use base64::Engine as _;

    #[derive(Debug, Clone)]
    struct SeenRemoteExecutionRuntimeRequest {
        trace_id: String,
        request_id: String,
        url: String,
        model: String,
        authorization: String,
        endpoint_tag: String,
        accept: String,
    }

    fn sample_auth_snapshot(api_key_id: &str, user_id: &str) -> StoredAuthApiKeySnapshot {
        crate::tests::support::sample_auth_snapshot_with(
            api_key_id,
            user_id,
            "alice",
            "alice@example.com",
            serde_json::json!(["claude"]),
            serde_json::json!(["claude:messages"]),
            serde_json::json!(["claude-code"]),
        )
    }

    fn sample_candidate_row() -> StoredMinimalCandidateSelectionRow {
        crate::tests::support::minimal_candidate_row(&crate::tests::support::CandidateRowSpec {
            id_suffix: "claude-cli-finalize-local-1",
            provider_name: "claude",
            provider_type: "custom",
            api_format: "claude:messages",
            api_family: "claude",
            endpoint_kind: Some("cli"),
            key_name: "prod",
            key_auth_type: "bearer",
            global_model_name: "claude-code",
            upstream_model: "claude-code-upstream",
        })
    }

    fn sample_provider_catalog_provider() -> StoredProviderCatalogProvider {
        StoredProviderCatalogProvider::new(
            "provider-claude-cli-finalize-local-1".to_string(),
            "claude".to_string(),
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
            "endpoint-claude-cli-finalize-local-1".to_string(),
            "provider-claude-cli-finalize-local-1".to_string(),
            "claude:messages".to_string(),
            Some("claude".to_string()),
            Some("cli".to_string()),
            true,
        )
        .expect("endpoint should build")
        .with_transport_fields(
            "https://api.anthropic.example".to_string(),
            Some(serde_json::json!([
                {"action":"set","key":"x-endpoint-tag","value":"claude-cli-finalize-local"}
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
            id_suffix: "claude-cli-finalize-local-1",
            key_name: "prod",
            auth_type: "bearer",
            api_format: "claude:messages",
            secret: "sk-upstream-claude-cli-finalize",
        })
    }

    let seen_remote_execution_runtime =
        Arc::new(Mutex::new(None::<SeenRemoteExecutionRuntimeRequest>));
    let seen_remote_execution_runtime_clone = Arc::clone(&seen_remote_execution_runtime);
    let report_hits = Arc::new(Mutex::new(0usize));
    let report_hits_clone = Arc::clone(&report_hits);
    let finalize_hits = Arc::new(Mutex::new(0usize));
    let finalize_hits_clone = Arc::clone(&finalize_hits);
    let decision_hits = Arc::new(Mutex::new(0usize));
    let decision_hits_clone = Arc::clone(&decision_hits);
    let plan_hits = Arc::new(Mutex::new(0usize));
    let plan_hits_clone = Arc::clone(&plan_hits);
    let public_hits = Arc::new(Mutex::new(0usize));
    let public_hits_clone = Arc::clone(&public_hits);
    let request_candidate_repository = Arc::new(InMemoryRequestCandidateRepository::default());
    let usage_repository = Arc::new(InMemoryUsageReadRepository::default());

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
            "/api/internal/gateway/finalize-sync",
            any(move |_request: Request| {
                let finalize_hits_inner = Arc::clone(&finalize_hits_clone);
                async move {
                    *finalize_hits_inner.lock().expect("mutex should lock") += 1;
                    (
                        StatusCode::IM_A_TEAPOT,
                        Body::from("finalize-sync-should-not-be-hit"),
                    )
                }
            }),
        )
        .route(
            "/api/internal/gateway/report-sync",
            any(move |_request: Request| {
                let report_hits_inner = Arc::clone(&report_hits_clone);
                async move {
                    *report_hits_inner.lock().expect("mutex should lock") += 1;
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
        any(move |request: Request| async move {
            let (parts, body) = request.into_parts();
            let raw_body = to_bytes(body, usize::MAX).await.expect("body should read");
            let payload: serde_json::Value =
                serde_json::from_slice(&raw_body).expect("execution runtime payload should parse");
            *seen_remote_execution_runtime_clone
                .lock()
                .expect("mutex should lock") = Some(SeenRemoteExecutionRuntimeRequest {
                trace_id: crate::tests::support::header_string(&parts.headers, TRACE_ID_HEADER),
                request_id: crate::tests::support::payload_string(&payload, &["request_id"]),
                url: crate::tests::support::payload_string(&payload, &["url"]),
                model: payload
                    .get("body")
                    .and_then(|value| value.get("json_body"))
                    .and_then(|value| value.get("model"))
                    .and_then(|value| value.as_str())
                    .unwrap_or_default()
                    .to_string(),
                authorization: crate::tests::support::payload_string(&payload, &["headers", "authorization"]),
                endpoint_tag: crate::tests::support::payload_string(&payload, &["headers", "x-endpoint-tag"]),
                accept: crate::tests::support::payload_string(&payload, &["headers", "accept"]),
            });
            Json(json!({
                "request_id": "trace-claude-cli-stream-sync-direct-123",
                "status_code": 200,
                "headers": {
                    "content-type": "text/event-stream"
                },
                "body": {
                    "body_bytes_b64": base64::engine::general_purpose::STANDARD.encode(
                        concat!(
                            "event: message_start\n",
                            "data: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_claude_cli_sync_123\",\"type\":\"message\",\"role\":\"assistant\",\"model\":\"claude-code-upstream\",\"content\":[],\"stop_reason\":null,\"stop_sequence\":null}}\n\n",
                            "event: content_block_start\n",
                            "data: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n",
                            "event: content_block_delta\n",
                            "data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"Hello\"}}\n\n",
                            "event: content_block_delta\n",
                            "data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\" Claude CLI\"}}\n\n",
                            "event: content_block_stop\n",
                            "data: {\"type\":\"content_block_stop\",\"index\":0}\n\n",
                            "event: message_delta\n",
                            "data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"input_tokens\":4,\"output_tokens\":6}}\n\n",
                            "event: message_stop\n",
                            "data: {\"type\":\"message_stop\"}\n\n"
                        )
                    )
                },
                "telemetry": {
                    "elapsed_ms": 32
                }
            }))
        }),
    );

    let auth_repository = Arc::new(InMemoryAuthApiKeySnapshotRepository::seed(vec![(
        Some(hash_api_key("sk-client-claude-cli-stream-sync-local")),
        sample_auth_snapshot(
            "api-key-claude-cli-stream-sync-local-1",
            "user-claude-cli-stream-sync-local-1",
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
    let gateway_state =
        build_state_with_execution_runtime_override(execution_runtime_url.clone())
            .with_data_state_for_tests(
                GatewayDataState::with_auth_candidate_selection_provider_catalog_request_candidates_and_usage_for_tests(
                    auth_repository,
                    candidate_selection_repository,
                    provider_catalog_repository,
                    Arc::clone(&request_candidate_repository),
                    Arc::clone(&usage_repository),
                    DEVELOPMENT_ENCRYPTION_KEY,
                ),
            );
    let gateway = build_router_with_state(gateway_state);
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let started_at = std::time::Instant::now();
    let response = reqwest::Client::new()
        .post(format!("{gateway_url}/v1/messages"))
        .header(http::header::CONTENT_TYPE, "application/json")
        .header(http::header::USER_AGENT, "Claude-Code/2.1.0")
        .header(
            http::header::AUTHORIZATION,
            "Bearer sk-client-claude-cli-stream-sync-local",
        )
        .header(TRACE_ID_HEADER, "trace-claude-cli-stream-sync-direct-123")
        .body("{\"model\":\"claude-code\",\"messages\":[]}")
        .send()
        .await
        .expect("request should succeed");
    let elapsed = started_at.elapsed();

    assert_eq!(response.status(), StatusCode::OK);
    let response_json: serde_json::Value = response.json().await.expect("body should parse");
    assert_eq!(
        response_json,
        json!({
            "id": "msg_claude_cli_sync_123",
            "type": "message",
            "role": "assistant",
            "model": "claude-code-upstream",
            "content": [{
                "type": "text",
                "text": "Hello Claude CLI"
            }],
            "stop_reason": "end_turn",
            "stop_sequence": null,
            "usage": {
                "input_tokens": 4,
                "output_tokens": 6
            }
        })
    );
    assert!(
        elapsed < std::time::Duration::from_millis(10_000),
        "response took unexpectedly long for local finalize path: elapsed={elapsed:?} finalize_hits={} report_hits={}",
        *finalize_hits.lock().expect("mutex should lock"),
        *report_hits.lock().expect("mutex should lock"),
    );

    let seen_remote_execution_runtime_request = seen_remote_execution_runtime
        .lock()
        .expect("mutex should lock")
        .clone()
        .expect("remote execution runtime plan should be captured");
    assert_eq!(
        seen_remote_execution_runtime_request.trace_id,
        "trace-claude-cli-stream-sync-direct-123"
    );
    assert_eq!(
        seen_remote_execution_runtime_request.request_id,
        "trace-claude-cli-stream-sync-direct-123"
    );
    assert_eq!(
        seen_remote_execution_runtime_request.url,
        "https://api.anthropic.example/custom/v1/messages"
    );
    assert_eq!(
        seen_remote_execution_runtime_request.model,
        "claude-code-upstream"
    );
    assert_eq!(
        seen_remote_execution_runtime_request.authorization,
        "Bearer sk-upstream-claude-cli-finalize"
    );
    assert_eq!(
        seen_remote_execution_runtime_request.endpoint_tag,
        "claude-cli-finalize-local"
    );
    assert_eq!(seen_remote_execution_runtime_request.accept, "*/*");

    let mut stored_candidates = Vec::new();
    for _ in 0..50 {
        stored_candidates = request_candidate_repository
            .list_by_request_id("trace-claude-cli-stream-sync-direct-123")
            .await
            .expect("request candidate trace should read");
        if stored_candidates.len() == 1
            && stored_candidates[0].status == RequestCandidateStatus::Success
        {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert_eq!(stored_candidates.len(), 1);
    assert_eq!(stored_candidates[0].status, RequestCandidateStatus::Success);

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    assert_eq!(
        *report_hits.lock().expect("mutex should lock"),
        0,
        "report-sync should stay local when request candidate persistence is available"
    );
    assert_eq!(*finalize_hits.lock().expect("mutex should lock"), 0);
    assert_eq!(*decision_hits.lock().expect("mutex should lock"), 0);
    assert_eq!(*plan_hits.lock().expect("mutex should lock"), 0);
    assert_eq!(*public_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    execution_runtime_handle.abort();
    upstream_handle.abort();
}
