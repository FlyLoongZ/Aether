use super::{
    any, build_router_with_state, build_state_with_execution_runtime_override, json, start_server,
    to_bytes, Arc, Body, Json, Mutex, Request, Router, StatusCode, TRACE_ID_HEADER,
};
use crate::data::GatewayDataState;
use crate::tests::hash_api_key;
use crate::tests::support::encode_event_frame;
use crate::tests::support::sample_auth_snapshot_kiro as sample_auth_snapshot;
use aether_ai_formats::openai_responses_message_item_id;
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

fn run_kiro_claude_cli_finalize_test<F, Fut>(test_name: &'static str, make_future: F)
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
fn gateway_executes_openai_responses_sync_upstream_stream_via_local_finalize_response() {
    run_kiro_claude_cli_finalize_test(
        "gateway_executes_openai_responses_sync_upstream_stream_via_local_finalize_response",
        gateway_executes_openai_responses_sync_upstream_stream_via_local_finalize_response_impl,
    );
}

async fn gateway_executes_openai_responses_sync_upstream_stream_via_local_finalize_response_impl() {
    use base64::Engine as _;

    #[derive(Debug, Clone)]
    struct SeenRemoteExecutionRuntimeRequest {
        trace_id: String,
        request_id: String,
        url: String,
        model: String,
        authorization: String,
        endpoint_tag: String,
    }

    fn sample_auth_snapshot(api_key_id: &str, user_id: &str) -> StoredAuthApiKeySnapshot {
        crate::tests::support::sample_auth_snapshot_with(
            api_key_id,
            user_id,
            "alice",
            "alice@example.com",
            serde_json::json!(["openai"]),
            serde_json::json!(["openai:responses"]),
            serde_json::json!(["gpt-5"]),
        )
    }

    fn sample_candidate_row() -> StoredMinimalCandidateSelectionRow {
        crate::tests::support::minimal_candidate_row(&crate::tests::support::CandidateRowSpec {
            id_suffix: "openai-cli-finalize-local-1",
            provider_name: "openai",
            provider_type: "custom",
            api_format: "openai:responses",
            api_family: "openai",
            endpoint_kind: Some("cli"),
            key_name: "prod",
            key_auth_type: "bearer",
            global_model_name: "gpt-5",
            upstream_model: "gpt-5-upstream",
        })
    }

    fn sample_provider_catalog_provider() -> StoredProviderCatalogProvider {
        StoredProviderCatalogProvider::new(
            "provider-openai-cli-finalize-local-1".to_string(),
            "openai".to_string(),
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
            "endpoint-openai-cli-finalize-local-1".to_string(),
            "provider-openai-cli-finalize-local-1".to_string(),
            "openai:responses".to_string(),
            Some("openai".to_string()),
            Some("cli".to_string()),
            true,
        )
        .expect("endpoint should build")
        .with_transport_fields(
            "https://api.openai.example".to_string(),
            Some(serde_json::json!([
                {"action":"set","key":"x-endpoint-tag","value":"openai-cli-finalize-local"}
            ])),
            None,
            Some(2),
            Some("/custom/v1/responses".to_string()),
            None,
            None,
            None,
        )
        .expect("endpoint transport should build")
    }

    fn sample_provider_catalog_key() -> StoredProviderCatalogKey {
        crate::tests::support::provider_catalog_key(&crate::tests::support::CatalogKeySpec {
            id_suffix: "openai-cli-finalize-local-1",
            key_name: "prod",
            auth_type: "bearer",
            api_format: "openai:responses",
            secret: "sk-upstream-openai-cli-finalize",
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
            "/v1/responses",
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
                    model: payload
                        .get("body")
                        .and_then(|value| value.get("json_body"))
                        .and_then(|value| value.get("model"))
                        .and_then(|value| value.as_str())
                        .unwrap_or_default()
                        .to_string(),
                    authorization: crate::tests::support::payload_string(&payload, &["headers", "authorization"]),
                    endpoint_tag: crate::tests::support::payload_string(&payload, &["headers", "x-endpoint-tag"]),
                });
                Json(json!({
                    "request_id": "trace-openai-cli-stream-sync-direct-123",
                    "status_code": 200,
                    "headers": {
                        "content-type": "text/event-stream"
                    },
                    "body": {
                        "body_bytes_b64": base64::engine::general_purpose::STANDARD.encode(
                            concat!(
                                "event: response.created\n",
                                "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_stream_001\",\"object\":\"response\",\"model\":\"gpt-5-upstream\",\"status\":\"in_progress\",\"output\":[]}}\n\n",
                                "event: response.output_text.delta\n",
                                "data: {\"type\":\"response.output_text.delta\",\"output_index\":0,\"content_index\":0,\"delta\":\"Hello\"}\n\n",
                                "event: response.completed\n",
                                "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_stream_001\",\"object\":\"response\",\"model\":\"gpt-5-upstream\",\"status\":\"completed\",\"output\":[],\"usage\":{\"input_tokens\":1,\"output_tokens\":2,\"total_tokens\":3}}}\n\n"
                            )
                        )
                    },
                    "telemetry": {
                        "elapsed_ms": 29
                    }
                }))
            }
        }),
    );

    let auth_repository = Arc::new(InMemoryAuthApiKeySnapshotRepository::seed(vec![(
        Some(hash_api_key("sk-client-openai-cli-finalize-local")),
        sample_auth_snapshot(
            "key-openai-cli-finalize-local-123",
            "user-openai-cli-finalize-local-123",
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
        .post(format!("{gateway_url}/v1/responses"))
        .header(http::header::CONTENT_TYPE, "application/json")
        .header(
            http::header::AUTHORIZATION,
            "Bearer sk-client-openai-cli-finalize-local",
        )
        .header(TRACE_ID_HEADER, "trace-openai-cli-stream-sync-direct-123")
        .body("{\"model\":\"gpt-5\",\"input\":\"hello\"}")
        .send()
        .await
        .expect("request should succeed");
    let elapsed = started_at.elapsed();

    assert_eq!(response.status(), StatusCode::OK);
    let response_json: serde_json::Value = response.json().await.expect("body should parse");
    let created_at = response_json["created_at"]
        .as_i64()
        .expect("created_at should be a unix timestamp");
    assert_eq!(
        response_json,
        json!({
            "id": "resp_stream_001",
            "object": "response",
            "model": "gpt-5-upstream",
            "status": "completed",
            "created_at": created_at,
            "completed_at": created_at,
            "output_text": "Hello",
            "output": [{
                "type": "message",
                "id": openai_responses_message_item_id("resp_stream_001", 0),
                "role": "assistant",
                "status": "completed",
                "content": [{
                    "type": "output_text",
                    "text": "Hello",
                    "annotations": []
                }]
            }],
            "usage": {
                "input_tokens": 1,
                "output_tokens": 2,
                "total_tokens": 3
            }
        })
    );
    assert!(
        elapsed < std::time::Duration::from_millis(5_000),
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
        "trace-openai-cli-stream-sync-direct-123"
    );
    assert_eq!(
        seen_remote_execution_runtime_request.request_id,
        "trace-openai-cli-stream-sync-direct-123"
    );
    assert_eq!(
        seen_remote_execution_runtime_request.url,
        "https://api.openai.example/custom/v1/responses"
    );
    assert_eq!(
        seen_remote_execution_runtime_request.model,
        "gpt-5-upstream"
    );
    assert_eq!(
        seen_remote_execution_runtime_request.authorization,
        "Bearer sk-upstream-openai-cli-finalize"
    );
    assert_eq!(
        seen_remote_execution_runtime_request.endpoint_tag,
        "openai-cli-finalize-local"
    );

    let mut stored_candidates = Vec::new();
    for _ in 0..50 {
        stored_candidates = request_candidate_repository
            .list_by_request_id("trace-openai-cli-stream-sync-direct-123")
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
fn gateway_executes_kiro_claude_cli_sync_upstream_stream_via_local_finalize_response() {
    run_kiro_claude_cli_finalize_test(
        "gateway_executes_kiro_claude_cli_sync_upstream_stream_via_local_finalize_response",
        gateway_executes_kiro_claude_cli_sync_upstream_stream_via_local_finalize_response_impl,
    );
}

async fn gateway_executes_kiro_claude_cli_sync_upstream_stream_via_local_finalize_response_impl() {
    use base64::Engine as _;

    #[derive(Debug, Clone)]
    struct SeenRemoteExecutionRuntimeRequest {
        trace_id: String,
        request_id: String,
        url: String,
        authorization: String,
        accept: String,
        host: String,
        endpoint_tag: String,
        mapped_model: String,
        current_content: String,
        profile_arn: String,
        debug_tag: String,
        proxy_node_id: String,
        transport_profile_id: String,
    }
    fn sample_candidate_row() -> StoredMinimalCandidateSelectionRow {
        crate::tests::support::minimal_candidate_row(&crate::tests::support::CandidateRowSpec {
            id_suffix: "kiro-cli-finalize-local-1",
            provider_name: "kiro",
            provider_type: "kiro",
            api_format: "claude:messages",
            api_family: "claude",
            endpoint_kind: Some("cli"),
            key_name: "prod",
            key_auth_type: "bearer",
            global_model_name: "claude-sonnet-4",
            upstream_model: "claude-sonnet-4-upstream",
        })
    }

    fn sample_provider_catalog_provider() -> StoredProviderCatalogProvider {
        StoredProviderCatalogProvider::new(
            "provider-kiro-cli-finalize-local-1".to_string(),
            "kiro".to_string(),
            Some("https://example.com".to_string()),
            "kiro".to_string(),
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
            "endpoint-kiro-cli-finalize-local-1".to_string(),
            "provider-kiro-cli-finalize-local-1".to_string(),
            "claude:messages".to_string(),
            Some("claude".to_string()),
            Some("cli".to_string()),
            true,
        )
        .expect("endpoint should build")
        .with_transport_fields(
            "https://kiro.{region}.example?tenant=demo".to_string(),
            Some(serde_json::json!([
                {"action":"set","key":"accept","value":"text/plain"},
                {"action":"set","key":"x-endpoint-tag","value":"kiro-cli-finalize-local"}
            ])),
            Some(serde_json::json!([
                {"action":"set","path":"debugTag","value":"kiro-local-finalize"}
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
        let auth_config = serde_json::json!({
            "provider_type": "kiro",
            "access_token": "cached-kiro-finalize-access-token",
            "expires_at": 4102444800_u64,
            "refresh_token": "rrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrr",
            "machine_id": "123e4567-e89b-12d3-a456-426614174000",
            "api_region": "us-east-1",
            "kiro_version": "0.8.0",
            "system_version": "darwin#24.6.0",
            "node_version": "22.21.1",
            "profile_arn": "arn:aws:bedrock:us-east-1:123456789012:inference-profile/demo"
        });

        StoredProviderCatalogKey::new(
            "key-kiro-cli-finalize-local-1".to_string(),
            "provider-kiro-cli-finalize-local-1".to_string(),
            "prod".to_string(),
            "bearer".to_string(),
            None,
            true,
        )
        .expect("key should build")
        .with_transport_fields(
            Some(serde_json::json!(["claude:messages"])),
            encrypt_python_fernet_plaintext(DEVELOPMENT_ENCRYPTION_KEY, "__placeholder__")
                .expect("api key should encrypt"),
            Some(
                encrypt_python_fernet_plaintext(
                    DEVELOPMENT_ENCRYPTION_KEY,
                    auth_config.to_string().as_str(),
                )
                .expect("auth config should encrypt"),
            ),
            None,
            Some(serde_json::json!({"claude:messages": 1})),
            None,
            None,
            Some(serde_json::json!({"enabled": true, "node_id":"proxy-node-kiro-cli-finalize-local"})),
            Some(serde_json::json!({"transport_profile":"chrome_136"})),
        )
        .expect("key transport should build")
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
            any(move |request: Request| {
                let report_hits_inner = Arc::clone(&report_hits_clone);
                async move {
                    *report_hits_inner.lock().expect("mutex should lock") += 1;
                    let (_parts, body) = request.into_parts();
                    let _body = to_bytes(body, usize::MAX).await.expect("body should read");
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
                    authorization: crate::tests::support::payload_string(&payload, &["headers", "authorization"]),
                    accept: crate::tests::support::payload_string(&payload, &["headers", "accept"]),
                    host: crate::tests::support::payload_string(&payload, &["headers", "host"]),
                    endpoint_tag: crate::tests::support::payload_string(&payload, &["headers", "x-endpoint-tag"]),
                    mapped_model: payload
                        .get("body")
                        .and_then(|value| value.get("json_body"))
                        .and_then(|value| value.get("conversationState"))
                        .and_then(|value| value.get("currentMessage"))
                        .and_then(|value| value.get("userInputMessage"))
                        .and_then(|value| value.get("modelId"))
                        .and_then(|value| value.as_str())
                        .unwrap_or_default()
                        .to_string(),
                    current_content: payload
                        .get("body")
                        .and_then(|value| value.get("json_body"))
                        .and_then(|value| value.get("conversationState"))
                        .and_then(|value| value.get("currentMessage"))
                        .and_then(|value| value.get("userInputMessage"))
                        .and_then(|value| value.get("content"))
                        .and_then(|value| value.as_str())
                        .unwrap_or_default()
                        .to_string(),
                    profile_arn: payload
                        .get("body")
                        .and_then(|value| value.get("json_body"))
                        .and_then(|value| value.get("profileArn"))
                        .and_then(|value| value.as_str())
                        .unwrap_or_default()
                        .to_string(),
                    debug_tag: payload
                        .get("body")
                        .and_then(|value| value.get("json_body"))
                        .and_then(|value| value.get("debugTag"))
                        .and_then(|value| value.as_str())
                        .unwrap_or_default()
                        .to_string(),
                    proxy_node_id: crate::tests::support::payload_string(&payload, &["proxy", "node_id"]),
                    transport_profile_id: payload
                        .get("transport_profile").and_then(|value| value.get("profile_id"))
                        .and_then(|value| value.as_str())
                        .unwrap_or_default()
                        .to_string(),
                });

                let kiro_frames = [
                    encode_event_frame(
                        "event",
                        Some("assistantResponseEvent"),
                        json!({"content": "Hello from Kiro finalize"}),
                    ),
                    encode_event_frame(
                        "event",
                        Some("contextUsageEvent"),
                        json!({"contextUsagePercentage": 1.0}),
                    ),
                ]
                .concat();
                Json(json!({
                    "request_id": "trace-kiro-cli-finalize-local-123",
                    "status_code": 200,
                    "headers": {
                        "content-type": "application/vnd.amazon.eventstream"
                    },
                    "body": {
                        "body_bytes_b64": base64::engine::general_purpose::STANDARD.encode(kiro_frames)
                    },
                    "telemetry": {
                        "elapsed_ms": 27
                    }
                }))
            }
        }),
    );

    let auth_repository = Arc::new(InMemoryAuthApiKeySnapshotRepository::seed(vec![(
        Some(hash_api_key("sk-client-kiro-cli-finalize-local")),
        sample_auth_snapshot(
            "key-kiro-cli-finalize-local-123",
            "user-kiro-cli-finalize-local-123",
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
                )
                .attach_proxy_node_repository_for_tests(
                    crate::tests::ai_execute::ai_execute_proxy_node_repository([
                        "proxy-node-kiro-cli-finalize-local",
                    ]),
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
            "Bearer sk-client-kiro-cli-finalize-local",
        )
        .header("anthropic-beta", "output-128k-2025-02-19")
        .header(TRACE_ID_HEADER, "trace-kiro-cli-finalize-local-123")
        .body(
            "{\"model\":\"claude-sonnet-4\",\"messages\":[{\"role\":\"user\",\"content\":\"hello kiro\"}],\"thinking\":{\"type\":\"enabled\",\"budget_tokens\":64}}",
        )
        .send()
        .await
        .expect("request should succeed");
    let elapsed = started_at.elapsed();

    let status = response.status();
    let response_text = response.text().await.expect("body should read");
    assert_eq!(
        status,
        StatusCode::OK,
        "unexpected gateway response: {response_text}"
    );
    let response_json: serde_json::Value =
        serde_json::from_str(&response_text).expect("body should parse");
    let response_id = response_json
        .get("id")
        .and_then(|value| value.as_str())
        .expect("response id should exist");
    assert_eq!(
        response_json,
        json!({
            "id": response_id,
            "type": "message",
            "role": "assistant",
            "model": "claude-sonnet-4-upstream",
            "content": [{
                "type": "text",
                "text": "Hello from Kiro finalize"
            }],
            "stop_reason": "end_turn",
            "stop_sequence": null,
            "usage": {
                "input_tokens": 2000,
                "output_tokens": 6
            }
        })
    );
    assert!(
        response_json
            .get("id")
            .and_then(|value| value.as_str())
            .is_some_and(|value| value.starts_with("msg_")),
        "response id should be generated by the local Kiro rewriter"
    );
    assert!(
        elapsed < std::time::Duration::from_millis(5_000),
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
        "trace-kiro-cli-finalize-local-123"
    );
    assert_eq!(
        seen_remote_execution_runtime_request.request_id,
        "trace-kiro-cli-finalize-local-123"
    );
    assert_eq!(
        seen_remote_execution_runtime_request.url,
        "https://kiro.us-east-1.example/generateAssistantResponse?tenant=demo"
    );
    assert_eq!(
        seen_remote_execution_runtime_request.authorization,
        "Bearer cached-kiro-finalize-access-token"
    );
    assert_eq!(
        seen_remote_execution_runtime_request.accept,
        "application/vnd.amazon.eventstream"
    );
    assert_eq!(
        seen_remote_execution_runtime_request.host,
        "q.us-east-1.amazonaws.com"
    );
    assert_eq!(
        seen_remote_execution_runtime_request.endpoint_tag,
        "kiro-cli-finalize-local"
    );
    assert_eq!(
        seen_remote_execution_runtime_request.mapped_model,
        "claude-sonnet-4-upstream"
    );
    assert_eq!(
        seen_remote_execution_runtime_request.current_content,
        "<thinking_mode>enabled</thinking_mode><max_thinking_length>64</max_thinking_length>\nhello kiro"
    );
    assert_eq!(
        seen_remote_execution_runtime_request.profile_arn,
        "arn:aws:bedrock:us-east-1:123456789012:inference-profile/demo"
    );
    assert_eq!(
        seen_remote_execution_runtime_request.debug_tag,
        "kiro-local-finalize"
    );
    assert_eq!(
        seen_remote_execution_runtime_request.proxy_node_id,
        "proxy-node-kiro-cli-finalize-local"
    );
    assert_eq!(
        seen_remote_execution_runtime_request.transport_profile_id,
        "chrome_136"
    );

    let mut stored_candidates = Vec::new();
    for _ in 0..50 {
        stored_candidates = request_candidate_repository
            .list_by_request_id("trace-kiro-cli-finalize-local-123")
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
