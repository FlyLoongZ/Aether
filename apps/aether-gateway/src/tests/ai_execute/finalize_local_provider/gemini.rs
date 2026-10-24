use super::{
    any, build_router_with_state, build_state_with_execution_runtime_override, json, start_server,
    to_bytes, Arc, Body, Json, Mutex, Request, Router, StatusCode, TRACE_ID_HEADER,
};
use crate::data::GatewayDataState;
use crate::tests::hash_api_key;
use aether_crypto::{encrypt_python_fernet_plaintext, DEVELOPMENT_ENCRYPTION_KEY};
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

fn run_gemini_provider_finalize_test<F, Fut>(test_name: &'static str, make_future: F)
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
fn gateway_executes_gemini_chat_sync_same_format_via_local_finalize_response() {
    run_gemini_provider_finalize_test(
        "gateway_executes_gemini_chat_sync_same_format_via_local_finalize_response",
        gateway_executes_gemini_chat_sync_same_format_via_local_finalize_response_impl,
    );
}

async fn gateway_executes_gemini_chat_sync_same_format_via_local_finalize_response_impl() {
    #[derive(Debug, Clone)]
    struct SeenRemoteExecutionRuntimeRequest {
        trace_id: String,
        request_id: String,
        url: String,
        has_model_field: bool,
        auth_header_value: String,
        exact_temperature: f64,
        endpoint_tag: String,
        metadata_mode: String,
        metadata_source: String,
        tool_config_present: bool,
        accept: String,
    }

    fn sample_auth_snapshot(api_key_id: &str, user_id: &str) -> StoredAuthApiKeySnapshot {
        crate::tests::support::sample_auth_snapshot_with(
            api_key_id,
            user_id,
            "alice",
            "alice@example.com",
            serde_json::json!(["gemini"]),
            serde_json::json!(["gemini:generate_content"]),
            serde_json::json!(["gemini-2.5-pro"]),
        )
    }

    fn sample_candidate_row() -> StoredMinimalCandidateSelectionRow {
        crate::tests::support::minimal_candidate_row(&crate::tests::support::CandidateRowSpec {
            id_suffix: "gemini-chat-finalize-local-1",
            provider_name: "gemini",
            provider_type: "custom",
            api_format: "gemini:generate_content",
            api_family: "gemini",
            endpoint_kind: Some("chat"),
            key_name: "prod",
            key_auth_type: "api_key",
            global_model_name: "gemini-2.5-pro",
            upstream_model: "gemini-2.5-pro-upstream",
        })
    }

    fn sample_provider_catalog_provider() -> StoredProviderCatalogProvider {
        StoredProviderCatalogProvider::new(
            "provider-gemini-chat-finalize-local-1".to_string(),
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
            "endpoint-gemini-chat-finalize-local-1".to_string(),
            "provider-gemini-chat-finalize-local-1".to_string(),
            "gemini:generate_content".to_string(),
            Some("gemini".to_string()),
            Some("chat".to_string()),
            true,
        )
        .expect("endpoint should build")
        .with_transport_fields(
            "https://generativelanguage.googleapis.com".to_string(),
            Some(serde_json::json!([
                {"action":"set","key":"x-endpoint-tag","value":"gemini-chat-finalize-local"}
            ])),
            Some(serde_json::json!([
                {"action":"set","path":"metadata.mode","value":"safe"},
                {"action":"rename","from":"metadata.client","to":"metadata.source"},
                {"action":"drop","path":"toolConfig"}
            ])),
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
            id_suffix: "gemini-chat-finalize-local-1",
            key_name: "prod",
            auth_type: "api_key",
            api_format: "gemini:generate_content",
            secret: "sk-upstream-gemini-chat-finalize",
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
            "/v1beta/models/gemini-2.5-pro:generateContent",
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
                    has_model_field: payload
                        .get("body")
                        .and_then(|value| value.get("json_body"))
                        .and_then(|value| value.get("model"))
                        .is_some(),
                    auth_header_value: crate::tests::support::payload_string(
                        &payload,
                        &["headers", "x-goog-api-key"],
                    ),
                    exact_temperature: payload
                        .get("body")
                        .and_then(|value| value.get("json_body"))
                        .and_then(|value| value.get("generationConfig"))
                        .and_then(|value| value.get("temperature"))
                        .and_then(|value| value.as_f64())
                        .unwrap_or_default(),
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
                    tool_config_present: payload
                        .get("body")
                        .and_then(|value| value.get("json_body"))
                        .and_then(|value| value.get("toolConfig"))
                        .is_some(),
                    accept: crate::tests::support::payload_string(&payload, &["headers", "accept"]),
                });
                Json(json!({
                    "request_id": "trace-gemini-chat-sync-local-123",
                    "status_code": 200,
                    "headers": {
                        "content-type": "application/json"
                    },
                    "body": {
                        "json_body": {
                            "candidates": [{
                                "content": {
                                    "parts": [{"text": "Hello Gemini"}],
                                    "role": "model"
                                },
                                "finishReason": "STOP",
                                "index": 0
                            }],
                            "modelVersion": "gemini-2.5-pro-upstream",
                            "usageMetadata": {
                                "promptTokenCount": 5,
                                "candidatesTokenCount": 7,
                                "totalTokenCount": 12
                            }
                        }
                    },
                    "telemetry": {
                        "elapsed_ms": 36
                    }
                }))
            }
        }),
    );

    let auth_repository = Arc::new(InMemoryAuthApiKeySnapshotRepository::seed(vec![(
        Some(hash_api_key("client-gemini-chat-finalize-local-key")),
        sample_auth_snapshot(
            "api-key-gemini-chat-finalize-local-1",
            "user-gemini-chat-finalize-local-1",
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
        .post(format!(
            "{gateway_url}/v1beta/models/gemini-2.5-pro:generateContent?key=client-gemini-chat-finalize-local-key&alt=sse"
        ))
        .header(http::header::CONTENT_TYPE, "application/json")
        .header(TRACE_ID_HEADER, "trace-gemini-chat-sync-local-123")
        .body(
            "{\"contents\":[],\"generationConfig\":{\"temperature\":0.2},\"metadata\":{\"client\":\"desktop-gemini-finalize\"},\"toolConfig\":{\"functionCallingConfig\":{\"mode\":\"AUTO\"}}}",
        )
        .send()
        .await
        .expect("request should succeed");
    let elapsed = started_at.elapsed();

    assert_eq!(response.status(), StatusCode::OK);
    let response_json: serde_json::Value = response.json().await.expect("body should parse");
    assert_eq!(
        response_json,
        json!({
            "candidates": [{
                "content": {
                    "parts": [{"text": "Hello Gemini"}],
                    "role": "model"
                },
                "finishReason": "STOP",
                "index": 0
            }],
            "modelVersion": "gemini-2.5-pro-upstream",
            "usageMetadata": {
                "promptTokenCount": 5,
                "candidatesTokenCount": 7,
                "totalTokenCount": 12
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
        "trace-gemini-chat-sync-local-123"
    );
    assert_eq!(
        seen_remote_execution_runtime_request.request_id,
        "trace-gemini-chat-sync-local-123"
    );
    assert_eq!(
        seen_remote_execution_runtime_request.url,
        "https://generativelanguage.googleapis.com/custom/v1beta/models/gemini-2.5-pro-upstream:generateContent?alt=sse"
    );
    assert!(!seen_remote_execution_runtime_request.has_model_field);
    assert_eq!(
        seen_remote_execution_runtime_request.auth_header_value,
        "sk-upstream-gemini-chat-finalize"
    );
    assert!((seen_remote_execution_runtime_request.exact_temperature - 0.2).abs() < f64::EPSILON);
    assert_eq!(
        seen_remote_execution_runtime_request.endpoint_tag,
        "gemini-chat-finalize-local"
    );
    assert_eq!(seen_remote_execution_runtime_request.metadata_mode, "safe");
    assert_eq!(
        seen_remote_execution_runtime_request.metadata_source,
        "desktop-gemini-finalize"
    );
    assert!(!seen_remote_execution_runtime_request.tool_config_present);
    assert_eq!(seen_remote_execution_runtime_request.accept, "*/*");

    let mut stored_candidates = Vec::new();
    for _ in 0..50 {
        stored_candidates = request_candidate_repository
            .list_by_request_id("trace-gemini-chat-sync-local-123")
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
fn gateway_executes_gemini_chat_sync_upstream_stream_via_local_finalize_response() {
    run_gemini_provider_finalize_test(
        "gateway_executes_gemini_chat_sync_upstream_stream_via_local_finalize_response",
        gateway_executes_gemini_chat_sync_upstream_stream_via_local_finalize_response_impl,
    );
}

async fn gateway_executes_gemini_chat_sync_upstream_stream_via_local_finalize_response_impl() {
    use base64::Engine as _;

    #[derive(Debug, Clone)]
    struct SeenRemoteExecutionRuntimeRequest {
        trace_id: String,
        request_id: String,
        url: String,
        has_model_field: bool,
        auth_header_value: String,
        exact_temperature: f64,
        endpoint_tag: String,
        metadata_mode: String,
        metadata_source: String,
        tool_config_present: bool,
        accept: String,
    }

    fn sample_auth_snapshot(api_key_id: &str, user_id: &str) -> StoredAuthApiKeySnapshot {
        crate::tests::support::sample_auth_snapshot_with(
            api_key_id,
            user_id,
            "alice",
            "alice@example.com",
            serde_json::json!(["gemini"]),
            serde_json::json!(["gemini:generate_content"]),
            serde_json::json!(["gemini-2.5-pro"]),
        )
    }

    fn sample_candidate_row() -> StoredMinimalCandidateSelectionRow {
        crate::tests::support::minimal_candidate_row(&crate::tests::support::CandidateRowSpec {
            id_suffix: "gemini-chat-stream-finalize-local-1",
            provider_name: "gemini",
            provider_type: "custom",
            api_format: "gemini:generate_content",
            api_family: "gemini",
            endpoint_kind: Some("chat"),
            key_name: "prod",
            key_auth_type: "api_key",
            global_model_name: "gemini-2.5-pro",
            upstream_model: "gemini-2.5-pro-upstream",
        })
    }

    fn sample_provider_catalog_provider() -> StoredProviderCatalogProvider {
        StoredProviderCatalogProvider::new(
            "provider-gemini-chat-stream-finalize-local-1".to_string(),
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
            "endpoint-gemini-chat-stream-finalize-local-1".to_string(),
            "provider-gemini-chat-stream-finalize-local-1".to_string(),
            "gemini:generate_content".to_string(),
            Some("gemini".to_string()),
            Some("chat".to_string()),
            true,
        )
        .expect("endpoint should build")
        .with_transport_fields(
            "https://generativelanguage.googleapis.com".to_string(),
            Some(serde_json::json!([
                {"action":"set","key":"x-endpoint-tag","value":"gemini-chat-stream-finalize-local"}
            ])),
            Some(serde_json::json!([
                {"action":"set","path":"metadata.mode","value":"safe"},
                {"action":"rename","from":"metadata.client","to":"metadata.source"},
                {"action":"drop","path":"toolConfig"}
            ])),
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
            id_suffix: "gemini-chat-stream-finalize-local-1",
            key_name: "prod",
            auth_type: "api_key",
            api_format: "gemini:generate_content",
            secret: "sk-upstream-gemini-chat-stream-finalize",
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
            "/v1beta/models/gemini-2.5-pro:generateContent",
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
            let seen_remote_execution_runtime_inner = Arc::clone(&seen_remote_execution_runtime_clone);
            async move {
                let (parts, body) = request.into_parts();
                let raw_body = to_bytes(body, usize::MAX).await.expect("body should read");
                let payload: serde_json::Value =
                    serde_json::from_slice(&raw_body).expect("execution runtime payload should parse");
                *seen_remote_execution_runtime_inner
                    .lock()
                    .expect("mutex should lock") = Some(SeenRemoteExecutionRuntimeRequest {
                    trace_id: crate::tests::support::header_string(&parts.headers, TRACE_ID_HEADER),
                    request_id: crate::tests::support::payload_string(&payload, &["request_id"]),
                    url: crate::tests::support::payload_string(&payload, &["url"]),
                    has_model_field: payload
                        .get("body")
                        .and_then(|value| value.get("json_body"))
                        .and_then(|value| value.get("model"))
                        .is_some(),
                    auth_header_value: crate::tests::support::payload_string(&payload, &["headers", "x-goog-api-key"]),
                    exact_temperature: payload
                        .get("body")
                        .and_then(|value| value.get("json_body"))
                        .and_then(|value| value.get("generationConfig"))
                        .and_then(|value| value.get("temperature"))
                        .and_then(|value| value.as_f64())
                        .unwrap_or_default(),
                    endpoint_tag: crate::tests::support::payload_string(&payload, &["headers", "x-endpoint-tag"]),
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
                    tool_config_present: payload
                        .get("body")
                        .and_then(|value| value.get("json_body"))
                        .and_then(|value| value.get("toolConfig"))
                        .is_some(),
                    accept: crate::tests::support::payload_string(&payload, &["headers", "accept"]),
                });
                Json(json!({
                    "request_id": "trace-gemini-chat-stream-sync-direct-123",
                    "status_code": 200,
                    "headers": {
                        "content-type": "text/event-stream"
                    },
                    "body": {
                        "body_bytes_b64": base64::engine::general_purpose::STANDARD.encode(
                            concat!(
                                "data: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"He\"}],\"role\":\"model\"},\"index\":0}],\"modelVersion\":\"gemini-2.5-pro-upstream\"}\n\n",
                                "data: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"Hello Gemini\"}],\"role\":\"model\"},\"finishReason\":\"STOP\",\"index\":0}],\"modelVersion\":\"gemini-2.5-pro-upstream\",\"usageMetadata\":{\"promptTokenCount\":5,\"candidatesTokenCount\":7,\"totalTokenCount\":12}}\n\n"
                            )
                        )
                    },
                    "telemetry": {
                        "elapsed_ms": 36
                    }
                }))
            }
        }),
    );

    let auth_repository = Arc::new(InMemoryAuthApiKeySnapshotRepository::seed(vec![(
        Some(hash_api_key("client-gemini-chat-stream-finalize-local-key")),
        sample_auth_snapshot(
            "api-key-gemini-chat-stream-finalize-local-1",
            "user-gemini-chat-stream-finalize-local-1",
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
        .post(format!(
            "{gateway_url}/v1beta/models/gemini-2.5-pro:generateContent?key=client-gemini-chat-stream-finalize-local-key&alt=sse"
        ))
        .header(http::header::CONTENT_TYPE, "application/json")
        .header(TRACE_ID_HEADER, "trace-gemini-chat-stream-sync-direct-123")
        .body(
            "{\"contents\":[],\"generationConfig\":{\"temperature\":0.2},\"metadata\":{\"client\":\"desktop-gemini-stream-finalize\"},\"toolConfig\":{\"functionCallingConfig\":{\"mode\":\"AUTO\"}}}",
        )
        .send()
        .await
        .expect("request should succeed");
    let elapsed = started_at.elapsed();

    assert_eq!(response.status(), StatusCode::OK);
    let response_json: serde_json::Value = response.json().await.expect("body should parse");
    assert_eq!(
        response_json,
        json!({
            "responseId": "resp-local-stream",
            "candidates": [{
                "content": {
                    "parts": [{"text": "Hello Gemini"}],
                    "role": "model"
                },
                "finishReason": "STOP",
                "index": 0
            }],
            "modelVersion": "gemini-2.5-pro-upstream",
            "usageMetadata": {
                "promptTokenCount": 5,
                "candidatesTokenCount": 7,
                "totalTokenCount": 12
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
        "trace-gemini-chat-stream-sync-direct-123"
    );
    assert_eq!(
        seen_remote_execution_runtime_request.request_id,
        "trace-gemini-chat-stream-sync-direct-123"
    );
    assert_eq!(
        seen_remote_execution_runtime_request.url,
        "https://generativelanguage.googleapis.com/custom/v1beta/models/gemini-2.5-pro-upstream:generateContent?alt=sse"
    );
    assert!(!seen_remote_execution_runtime_request.has_model_field);
    assert_eq!(
        seen_remote_execution_runtime_request.auth_header_value,
        "sk-upstream-gemini-chat-stream-finalize"
    );
    assert!((seen_remote_execution_runtime_request.exact_temperature - 0.2).abs() < f64::EPSILON);
    assert_eq!(
        seen_remote_execution_runtime_request.endpoint_tag,
        "gemini-chat-stream-finalize-local"
    );
    assert_eq!(seen_remote_execution_runtime_request.metadata_mode, "safe");
    assert_eq!(
        seen_remote_execution_runtime_request.metadata_source,
        "desktop-gemini-stream-finalize"
    );
    assert!(!seen_remote_execution_runtime_request.tool_config_present);
    assert_eq!(seen_remote_execution_runtime_request.accept, "*/*");

    let mut stored_candidates = Vec::new();
    for _ in 0..50 {
        stored_candidates = request_candidate_repository
            .list_by_request_id("trace-gemini-chat-stream-sync-direct-123")
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
fn gateway_executes_gemini_cli_sync_upstream_stream_via_local_finalize_response() {
    run_gemini_provider_finalize_test(
        "gateway_executes_gemini_cli_sync_upstream_stream_via_local_finalize_response",
        gateway_executes_gemini_cli_sync_upstream_stream_via_local_finalize_response_impl,
    );
}

async fn gateway_executes_gemini_cli_sync_upstream_stream_via_local_finalize_response_impl() {
    use base64::Engine as _;

    #[derive(Debug, Clone)]
    struct SeenRemoteExecutionRuntimeRequest {
        trace_id: String,
        request_id: String,
        url: String,
        has_model_field: bool,
        authorization: String,
        exact_temperature: f64,
        endpoint_tag: String,
        metadata_mode: String,
        metadata_source: String,
        tool_config_present: bool,
        accept: String,
    }

    fn sample_auth_snapshot(api_key_id: &str, user_id: &str) -> StoredAuthApiKeySnapshot {
        crate::tests::support::sample_auth_snapshot_with(
            api_key_id,
            user_id,
            "alice",
            "alice@example.com",
            serde_json::json!(["gemini"]),
            serde_json::json!(["gemini:generate_content"]),
            serde_json::json!(["gemini-cli"]),
        )
    }

    fn sample_candidate_row() -> StoredMinimalCandidateSelectionRow {
        crate::tests::support::minimal_candidate_row(&crate::tests::support::CandidateRowSpec {
            id_suffix: "gemini-cli-finalize-local-1",
            provider_name: "gemini",
            provider_type: "custom",
            api_format: "gemini:generate_content",
            api_family: "gemini",
            endpoint_kind: Some("cli"),
            key_name: "prod",
            key_auth_type: "bearer",
            global_model_name: "gemini-cli",
            upstream_model: "gemini-cli-upstream",
        })
    }

    fn sample_provider_catalog_provider() -> StoredProviderCatalogProvider {
        StoredProviderCatalogProvider::new(
            "provider-gemini-cli-finalize-local-1".to_string(),
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
            "endpoint-gemini-cli-finalize-local-1".to_string(),
            "provider-gemini-cli-finalize-local-1".to_string(),
            "gemini:generate_content".to_string(),
            Some("gemini".to_string()),
            Some("cli".to_string()),
            true,
        )
        .expect("endpoint should build")
        .with_transport_fields(
            "https://generativelanguage.googleapis.com".to_string(),
            Some(serde_json::json!([
                {"action":"set","key":"x-endpoint-tag","value":"gemini-cli-finalize-local"}
            ])),
            Some(serde_json::json!([
                {"action":"set","path":"metadata.mode","value":"safe"},
                {"action":"rename","from":"metadata.client","to":"metadata.source"},
                {"action":"drop","path":"toolConfig"}
            ])),
            Some(2),
            Some("/custom/v1beta/models/gemini-cli-upstream:generateContent".to_string()),
            None,
            None,
            None,
        )
        .expect("endpoint transport should build")
    }

    fn sample_provider_catalog_key() -> StoredProviderCatalogKey {
        crate::tests::support::provider_catalog_key(&crate::tests::support::CatalogKeySpec {
            id_suffix: "gemini-cli-finalize-local-1",
            key_name: "prod",
            auth_type: "bearer",
            api_format: "gemini:generate_content",
            secret: "sk-upstream-gemini-cli-finalize",
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
            "/v1beta/models/gemini-cli:generateContent",
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
            let seen_remote_execution_runtime_inner = Arc::clone(&seen_remote_execution_runtime_clone);
            async move {
                let (parts, body) = request.into_parts();
                let raw_body = to_bytes(body, usize::MAX).await.expect("body should read");
                let payload: serde_json::Value =
                    serde_json::from_slice(&raw_body).expect("execution runtime payload should parse");
                *seen_remote_execution_runtime_inner
                    .lock()
                    .expect("mutex should lock") = Some(SeenRemoteExecutionRuntimeRequest {
                    trace_id: crate::tests::support::header_string(&parts.headers, TRACE_ID_HEADER),
                    request_id: crate::tests::support::payload_string(&payload, &["request_id"]),
                    url: crate::tests::support::payload_string(&payload, &["url"]),
                    has_model_field: payload
                        .get("body")
                        .and_then(|value| value.get("json_body"))
                        .and_then(|value| value.get("model"))
                        .is_some(),
                    authorization: crate::tests::support::payload_string(&payload, &["headers", "authorization"]),
                    exact_temperature: payload
                        .get("body")
                        .and_then(|value| value.get("json_body"))
                        .and_then(|value| value.get("generationConfig"))
                        .and_then(|value| value.get("temperature"))
                        .and_then(|value| value.as_f64())
                        .unwrap_or_default(),
                    endpoint_tag: crate::tests::support::payload_string(&payload, &["headers", "x-endpoint-tag"]),
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
                    tool_config_present: payload
                        .get("body")
                        .and_then(|value| value.get("json_body"))
                        .and_then(|value| value.get("toolConfig"))
                        .is_some(),
                    accept: crate::tests::support::payload_string(&payload, &["headers", "accept"]),
                });
                Json(json!({
                    "request_id": "trace-gemini-cli-stream-sync-direct-123",
                    "status_code": 200,
                    "headers": {
                        "content-type": "text/event-stream"
                    },
                    "body": {
                        "body_bytes_b64": base64::engine::general_purpose::STANDARD.encode(
                            concat!(
                                "data: {\"response\":{\"responseId\":\"resp_gemini_cli_sync_123\",\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"He\"}],\"role\":\"model\"},\"index\":0}],\"modelVersion\":\"gemini-cli-upstream\"}}\n\n",
                                "data: {\"response\":{\"responseId\":\"resp_gemini_cli_sync_123\",\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"Hello Gemini CLI\"}],\"role\":\"model\"},\"finishReason\":\"STOP\",\"index\":0}],\"modelVersion\":\"gemini-cli-upstream\",\"usageMetadata\":{\"promptTokenCount\":3,\"candidatesTokenCount\":5,\"totalTokenCount\":10}}}\n\n"
                            )
                        )
                    },
                    "telemetry": {
                        "elapsed_ms": 33
                    }
                }))
            }
        }),
    );

    let auth_repository = Arc::new(InMemoryAuthApiKeySnapshotRepository::seed(vec![(
        Some(hash_api_key("client-gemini-cli-finalize-local-key")),
        sample_auth_snapshot(
            "api-key-gemini-cli-finalize-local-1",
            "user-gemini-cli-finalize-local-1",
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
        .post(format!(
            "{gateway_url}/v1beta/models/gemini-cli:generateContent"
        ))
        .header(http::header::CONTENT_TYPE, "application/json")
        .header("user-agent", "GeminiCLI/1.0")
        .header("x-goog-api-key", "client-gemini-cli-finalize-local-key")
        .header(TRACE_ID_HEADER, "trace-gemini-cli-stream-sync-direct-123")
        .body(
            "{\"contents\":[],\"generationConfig\":{\"temperature\":0.2},\"metadata\":{\"client\":\"desktop-gemini-cli-finalize\"},\"toolConfig\":{\"functionCallingConfig\":{\"mode\":\"AUTO\"}}}",
        )
        .send()
        .await
        .expect("request should succeed");
    let elapsed = started_at.elapsed();

    assert_eq!(response.status(), StatusCode::OK);
    let response_json: serde_json::Value = response.json().await.expect("body should parse");
    assert_eq!(
        response_json,
        json!({
            "responseId": "resp_gemini_cli_sync_123",
            "candidates": [{
                "content": {
                    "parts": [{"text": "Hello Gemini CLI"}],
                    "role": "model"
                },
                "finishReason": "STOP",
                "index": 0
            }],
            "modelVersion": "gemini-cli-upstream",
            "usageMetadata": {
                "promptTokenCount": 3,
                "candidatesTokenCount": 5,
                "totalTokenCount": 10
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
        "trace-gemini-cli-stream-sync-direct-123"
    );
    assert_eq!(
        seen_remote_execution_runtime_request.request_id,
        "trace-gemini-cli-stream-sync-direct-123"
    );
    assert_eq!(
        seen_remote_execution_runtime_request.url,
        "https://generativelanguage.googleapis.com/custom/v1beta/models/gemini-cli-upstream:generateContent"
    );
    assert!(!seen_remote_execution_runtime_request.has_model_field);
    assert_eq!(
        seen_remote_execution_runtime_request.authorization,
        "Bearer sk-upstream-gemini-cli-finalize"
    );
    assert!((seen_remote_execution_runtime_request.exact_temperature - 0.2).abs() < f64::EPSILON);
    assert_eq!(
        seen_remote_execution_runtime_request.endpoint_tag,
        "gemini-cli-finalize-local"
    );
    assert_eq!(seen_remote_execution_runtime_request.metadata_mode, "safe");
    assert_eq!(
        seen_remote_execution_runtime_request.metadata_source,
        "desktop-gemini-cli-finalize"
    );
    assert!(!seen_remote_execution_runtime_request.tool_config_present);
    assert_eq!(seen_remote_execution_runtime_request.accept, "*/*");

    let mut stored_candidates = Vec::new();
    for _ in 0..50 {
        stored_candidates = request_candidate_repository
            .list_by_request_id("trace-gemini-cli-stream-sync-direct-123")
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
fn gateway_executes_antigravity_gemini_cli_sync_upstream_stream_via_local_finalize_response() {
    run_gemini_provider_finalize_test(
        "gateway_executes_antigravity_gemini_cli_sync_upstream_stream_via_local_finalize_response",
        gateway_executes_antigravity_gemini_cli_sync_upstream_stream_via_local_finalize_response_impl,
    );
}

async fn gateway_executes_antigravity_gemini_cli_sync_upstream_stream_via_local_finalize_response_impl(
) {
    use base64::Engine as _;

    #[derive(Debug, Clone)]
    struct SeenRefreshRequest {
        content_type: String,
        body: String,
    }

    #[derive(Debug, Clone)]
    struct SeenRemoteExecutionRuntimeRequest {
        trace_id: String,
        url: String,
        accept: String,
        authorization: String,
        x_client_name: String,
        x_client_version: String,
        x_vscode_sessionid: String,
        x_goog_api_client: String,
        project: String,
        request_id: String,
        model: String,
        user_agent: String,
        request_type: String,
        contents_len: usize,
        exact_temperature: f64,
        request_has_model: bool,
    }

    fn sample_auth_snapshot(api_key_id: &str, user_id: &str) -> StoredAuthApiKeySnapshot {
        crate::tests::support::sample_auth_snapshot_with(
            api_key_id,
            user_id,
            "alice",
            "alice@example.com",
            serde_json::json!(["gemini", "antigravity"]),
            serde_json::json!(["gemini:generate_content"]),
            serde_json::json!(["gemini-cli"]),
        )
    }

    fn sample_candidate_row() -> StoredMinimalCandidateSelectionRow {
        crate::tests::support::minimal_candidate_row(&crate::tests::support::CandidateRowSpec {
            id_suffix: "antigravity-cli-finalize-local-1",
            provider_name: "antigravity",
            provider_type: "antigravity",
            api_format: "gemini:generate_content",
            api_family: "gemini",
            endpoint_kind: Some("cli"),
            key_name: "oauth",
            key_auth_type: "oauth",
            global_model_name: "gemini-cli",
            upstream_model: "claude-sonnet-4-5",
        })
    }

    fn sample_provider_catalog_provider() -> StoredProviderCatalogProvider {
        StoredProviderCatalogProvider::new(
            "provider-antigravity-cli-finalize-local-1".to_string(),
            "antigravity".to_string(),
            Some("https://example.com".to_string()),
            "antigravity".to_string(),
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
            "endpoint-antigravity-cli-finalize-local-1".to_string(),
            "provider-antigravity-cli-finalize-local-1".to_string(),
            "gemini:generate_content".to_string(),
            Some("gemini".to_string()),
            Some("cli".to_string()),
            true,
        )
        .expect("endpoint should build")
        .with_transport_fields(
            "https://antigravity.googleapis.com".to_string(),
            None,
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
        let encrypted_auth_config = encrypt_python_fernet_plaintext(
            DEVELOPMENT_ENCRYPTION_KEY,
            r#"{"provider_type":"antigravity","project_id":"project-antigravity-local-1","client_version":"1.2.3","session_id":"sess-antigravity-local-123","refresh_token":"rt-antigravity-cli-local-123"}"#,
        )
        .expect("auth config should encrypt");
        StoredProviderCatalogKey::new(
            "key-antigravity-cli-finalize-local-1".to_string(),
            "provider-antigravity-cli-finalize-local-1".to_string(),
            "oauth".to_string(),
            "oauth".to_string(),
            None,
            true,
        )
        .expect("key should build")
        .with_transport_fields(
            Some(serde_json::json!(["gemini:generate_content"])),
            encrypt_python_fernet_plaintext(DEVELOPMENT_ENCRYPTION_KEY, "__placeholder__")
                .expect("placeholder api key should encrypt"),
            Some(encrypted_auth_config),
            None,
            Some(serde_json::json!({"gemini:generate_content": 1})),
            None,
            None,
            None,
            None,
        )
        .expect("key transport should build")
    }

    let seen_remote_execution_runtime =
        Arc::new(Mutex::new(None::<SeenRemoteExecutionRuntimeRequest>));
    let seen_remote_execution_runtime_clone = Arc::clone(&seen_remote_execution_runtime);
    let seen_refresh = Arc::new(Mutex::new(None::<SeenRefreshRequest>));
    let seen_refresh_clone = Arc::clone(&seen_refresh);
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
            "/v1beta/models/gemini-cli:generateContent",
            any(move |_request: Request| {
                let public_hits_inner = Arc::clone(&public_hits_clone);
                async move {
                    *public_hits_inner.lock().expect("mutex should lock") += 1;
                    (StatusCode::IM_A_TEAPOT, Body::from("public-route-hit"))
                }
            }),
        );

    let refresh = Router::new().route(
        "/oauth/token",
        any(move |request: Request| {
            let seen_refresh_inner = Arc::clone(&seen_refresh_clone);
            async move {
                let (parts, body) = request.into_parts();
                let raw_body = to_bytes(body, usize::MAX).await.expect("body should read");
                *seen_refresh_inner.lock().expect("mutex should lock") = Some(SeenRefreshRequest {
                    content_type: crate::tests::support::header_string(
                        &parts.headers,
                        http::header::CONTENT_TYPE,
                    ),
                    body: String::from_utf8(raw_body.to_vec())
                        .expect("refresh body should be utf8"),
                });
                Json(json!({
                    "access_token": "refreshed-antigravity-cli-access-token",
                    "refresh_token": "rt-antigravity-cli-local-456",
                    "token_type": "Bearer",
                    "expires_in": 3600
                }))
            }
        }),
    );

    let execution_runtime = Router::new().route(
        "/v1/execute/sync",
        any(move |request: Request| {
            let seen_remote_execution_runtime_inner = Arc::clone(&seen_remote_execution_runtime_clone);
            async move {
                let (parts, body) = request.into_parts();
                let raw_body = to_bytes(body, usize::MAX).await.expect("body should read");
                let payload: serde_json::Value =
                    serde_json::from_slice(&raw_body).expect("execution runtime payload should parse");
                *seen_remote_execution_runtime_inner
                    .lock()
                    .expect("mutex should lock") = Some(SeenRemoteExecutionRuntimeRequest {
                    trace_id: crate::tests::support::header_string(&parts.headers, TRACE_ID_HEADER),
                    url: crate::tests::support::payload_string(&payload, &["url"]),
                    accept: crate::tests::support::payload_string(&payload, &["headers", "accept"]),
                    authorization: crate::tests::support::payload_string(&payload, &["headers", "authorization"]),
                    x_client_name: crate::tests::support::payload_string(&payload, &["headers", "x-client-name"]),
                    x_client_version: crate::tests::support::payload_string(&payload, &["headers", "x-client-version"]),
                    x_vscode_sessionid: crate::tests::support::payload_string(&payload, &["headers", "x-vscode-sessionid"]),
                    x_goog_api_client: crate::tests::support::payload_string(&payload, &["headers", "x-goog-api-client"]),
                    project: payload
                        .get("body")
                        .and_then(|value| value.get("json_body"))
                        .and_then(|value| value.get("project"))
                        .and_then(|value| value.as_str())
                        .unwrap_or_default()
                        .to_string(),
                    request_id: payload
                        .get("body")
                        .and_then(|value| value.get("json_body"))
                        .and_then(|value| value.get("requestId"))
                        .and_then(|value| value.as_str())
                        .unwrap_or_default()
                        .to_string(),
                    model: payload
                        .get("body")
                        .and_then(|value| value.get("json_body"))
                        .and_then(|value| value.get("model"))
                        .and_then(|value| value.as_str())
                        .unwrap_or_default()
                        .to_string(),
                    user_agent: payload
                        .get("body")
                        .and_then(|value| value.get("json_body"))
                        .and_then(|value| value.get("userAgent"))
                        .and_then(|value| value.as_str())
                        .unwrap_or_default()
                        .to_string(),
                    request_type: payload
                        .get("body")
                        .and_then(|value| value.get("json_body"))
                        .and_then(|value| value.get("requestType"))
                        .and_then(|value| value.as_str())
                        .unwrap_or_default()
                        .to_string(),
                    contents_len: payload
                        .get("body")
                        .and_then(|value| value.get("json_body"))
                        .and_then(|value| value.get("request"))
                        .and_then(|value| value.get("contents"))
                        .and_then(|value| value.as_array())
                        .map(Vec::len)
                        .unwrap_or_default(),
                    exact_temperature: payload
                        .get("body")
                        .and_then(|value| value.get("json_body"))
                        .and_then(|value| value.get("request"))
                        .and_then(|value| value.get("generationConfig"))
                        .and_then(|value| value.get("temperature"))
                        .and_then(|value| value.as_f64())
                        .unwrap_or_default(),
                    request_has_model: payload
                        .get("body")
                        .and_then(|value| value.get("json_body"))
                        .and_then(|value| value.get("request"))
                        .and_then(|value| value.get("model"))
                        .is_some(),
                });
                Json(json!({
                    "request_id": "trace-antigravity-cli-stream-sync-direct-123",
                    "status_code": 200,
                    "headers": {
                        "content-type": "text/event-stream"
                    },
                    "body": {
                        "body_bytes_b64": base64::engine::general_purpose::STANDARD.encode(
                            concat!(
                                "data: {\"response\":{\"candidates\":[{\"content\":{\"parts\":[{\"functionCall\":{\"name\":\"get_weather\",\"args\":{\"city\":\"SF\"}}}],\"role\":\"model\"},\"index\":0}],\"modelVersion\":\"claude-sonnet-4-5\"},\"responseId\":\"resp_antigravity_cli_sync_123\"}\n\n",
                                "data: {\"response\":{\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"Hello Antigravity CLI\"},{\"functionCall\":{\"name\":\"get_weather\",\"args\":{\"city\":\"SF\"}}}],\"role\":\"model\"},\"finishReason\":\"STOP\",\"index\":0}],\"modelVersion\":\"claude-sonnet-4-5\",\"usageMetadata\":{\"promptTokenCount\":3,\"candidatesTokenCount\":5,\"totalTokenCount\":10}},\"responseId\":\"resp_antigravity_cli_sync_123\"}\n\n"
                            )
                        )
                    },
                    "telemetry": {
                        "elapsed_ms": 33
                    }
                }))
            }
        }),
    );

    let client_api_key = "client-antigravity-cli-finalize-local";
    let auth_repository = Arc::new(InMemoryAuthApiKeySnapshotRepository::seed(vec![(
        Some(hash_api_key(client_api_key)),
        sample_auth_snapshot(
            "api-key-antigravity-cli-finalize-local-1",
            "user-antigravity-cli-finalize-local-1",
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
    let (refresh_url, refresh_handle) = start_server(refresh).await;
    let (execution_runtime_url, execution_runtime_handle) = start_server(execution_runtime).await;
    let oauth_refresh =
        crate::provider_transport::LocalOAuthRefreshCoordinator::with_adapters_for_tests(vec![
            Arc::new(
                crate::provider_transport::oauth_refresh::GenericOAuthRefreshAdapter::default()
                    .with_token_url_for_tests("antigravity", format!("{refresh_url}/oauth/token"))
                    .with_oauth_credentials_for_tests(
                        "antigravity",
                        "test-antigravity-client-id",
                        "test-antigravity-client-secret",
                    ),
            ),
        ]);
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
                )
                .with_system_default_routing_group_for_tests(),
            )
            .with_oauth_refresh_coordinator_for_tests(oauth_refresh);
    let gateway = build_router_with_state(gateway_state);
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let started_at = std::time::Instant::now();
    let response = reqwest::Client::new()
        .post(format!(
            "{gateway_url}/v1beta/models/gemini-cli:generateContent"
        ))
        .header(http::header::CONTENT_TYPE, "application/json")
        .header("user-agent", "GeminiCLI/1.0")
        .header("x-goog-api-key", client_api_key)
        .header(
            TRACE_ID_HEADER,
            "trace-antigravity-cli-stream-sync-direct-123",
        )
        .body("{\"contents\":[],\"generationConfig\":{\"temperature\":0.2}}")
        .send()
        .await
        .expect("request should succeed");
    let elapsed = started_at.elapsed();

    assert_eq!(response.status(), StatusCode::OK);
    let response_json: serde_json::Value = response.json().await.expect("body should parse");
    assert_eq!(
        response_json,
        json!({
            "responseId": "resp-local-stream",
            "_v1internal_response_id": "resp_antigravity_cli_sync_123",
            "candidates": [{
                "content": {
                    "parts": [
                        {
                            "functionCall": {
                                "name": "get_weather",
                                "args": {"city": "SF"},
                                "id": "call_get_weather_0"
                            }
                        },
                        {"text": "Hello Antigravity CLI"},
                        {
                            "functionCall": {
                                "name": "get_weather",
                                "args": {"city": "SF"},
                                "id": "call_get_weather_0"
                            }
                        }
                    ],
                    "role": "model"
                },
                "finishReason": "STOP",
                "index": 0
            }],
            "modelVersion": "claude-sonnet-4-5",
            "usageMetadata": {
                "promptTokenCount": 3,
                "candidatesTokenCount": 5,
                "totalTokenCount": 10
            }
        })
    );
    assert!(
        elapsed < std::time::Duration::from_millis(10_000),
        "response took unexpectedly long for local finalize path: elapsed={elapsed:?} finalize_hits={} report_hits={}",
        *finalize_hits.lock().expect("mutex should lock"),
        *report_hits.lock().expect("mutex should lock"),
    );

    let seen_refresh_request = seen_refresh
        .lock()
        .expect("mutex should lock")
        .clone()
        .expect("refresh request should be captured");
    assert_eq!(
        seen_refresh_request.content_type,
        "application/x-www-form-urlencoded"
    );
    assert!(seen_refresh_request
        .body
        .contains("grant_type=refresh_token"));
    assert!(seen_refresh_request
        .body
        .contains("client_id=test-antigravity-client-id"));
    assert!(seen_refresh_request
        .body
        .contains("client_secret=test-antigravity-client-secret"));
    assert!(seen_refresh_request
        .body
        .contains("refresh_token=rt-antigravity-cli-local-123"));

    let seen_remote_execution_runtime_request = seen_remote_execution_runtime
        .lock()
        .expect("mutex should lock")
        .clone()
        .expect("remote execution runtime plan should be captured");
    assert_eq!(
        seen_remote_execution_runtime_request.trace_id,
        "trace-antigravity-cli-stream-sync-direct-123"
    );
    assert_eq!(
        seen_remote_execution_runtime_request.url,
        "https://antigravity.googleapis.com/v1internal:streamGenerateContent?alt=sse"
    );
    assert_eq!(
        seen_remote_execution_runtime_request.accept,
        "text/event-stream"
    );
    assert_eq!(
        seen_remote_execution_runtime_request.authorization,
        "Bearer refreshed-antigravity-cli-access-token"
    );
    assert_eq!(
        seen_remote_execution_runtime_request.x_client_name,
        "antigravity"
    );
    assert_eq!(
        seen_remote_execution_runtime_request.x_client_version,
        "4.3.0"
    );
    assert_eq!(
        seen_remote_execution_runtime_request.x_vscode_sessionid,
        "sess-antigravity-local-123"
    );
    assert_eq!(
        seen_remote_execution_runtime_request.x_goog_api_client,
        "gl-node/18.18.2 fire/0.8.6 grpc/1.10.x"
    );
    assert_eq!(
        seen_remote_execution_runtime_request.project,
        "project-antigravity-local-1"
    );
    assert_eq!(
        seen_remote_execution_runtime_request.request_id,
        "trace-antigravity-cli-stream-sync-direct-123"
    );
    assert_eq!(
        seen_remote_execution_runtime_request.model,
        "claude-sonnet-4-5"
    );
    assert_eq!(
        seen_remote_execution_runtime_request.user_agent,
        aether_provider_transport::antigravity::ANTIGRAVITY_REQUEST_USER_AGENT
    );
    assert_eq!(seen_remote_execution_runtime_request.request_type, "");
    assert_eq!(seen_remote_execution_runtime_request.contents_len, 0);
    assert!((seen_remote_execution_runtime_request.exact_temperature - 0.2).abs() < f64::EPSILON);
    assert!(!seen_remote_execution_runtime_request.request_has_model);

    let mut stored_candidates = Vec::new();
    for _ in 0..50 {
        stored_candidates = request_candidate_repository
            .list_by_request_id("trace-antigravity-cli-stream-sync-direct-123")
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
    refresh_handle.abort();
    upstream_handle.abort();
}
