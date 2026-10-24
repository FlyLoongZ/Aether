use super::{
    any, build_router_with_state, build_state_with_execution_runtime_override, json, start_server,
    Arc, Body, Bytes, HeaderValue, Infallible, Json, Mutex, Request, Response, Router, StatusCode,
    LOCAL_EXECUTION_RUNTIME_MISS_REASON_HEADER, TRACE_ID_HEADER,
};

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

use crate::data::GatewayDataState;
use crate::tests::next_non_keepalive_chunk;
use aether_crypto::DEVELOPMENT_ENCRYPTION_KEY;

fn run_lifecycle_test<F, Fut>(test_name: &'static str, make_future: F)
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

use crate::tests::hash_api_key;

async fn build_cancelling_gateway(state: crate::AppState) -> Router {
    state
        .data
        .update_routing_group(
            "system-default",
            aether_data_contracts::repository::routing_profiles::UpdateRoutingGroupRecord {
                config_json: Some(json!({"default_policy": {"cancel_on_client_disconnect": true}})),
                version: Some(2),
                updated_at: 2,
                ..Default::default()
            },
        )
        .await
        .expect("routing policy should update")
        .expect("default strategy should exist");
    build_router_with_state(state)
}

const LOCAL_OPENAI_SPEC: crate::tests::support::LocalOpenAiSpec<'_> =
    crate::tests::support::LocalOpenAiSpec {
        id_suffix: "openai-lifecycle-local-1",
        provider_name: "openai",
        provider_priority: 2,
        secret: "sk-upstream-openai",
    };

fn sample_local_openai_auth_snapshot(api_key_id: &str, user_id: &str) -> StoredAuthApiKeySnapshot {
    crate::tests::support::local_openai_auth_snapshot(api_key_id, user_id)
}

fn sample_local_openai_candidate_row() -> StoredMinimalCandidateSelectionRow {
    crate::tests::support::local_openai_candidate_row(&LOCAL_OPENAI_SPEC)
}

fn sample_local_openai_provider() -> StoredProviderCatalogProvider {
    crate::tests::support::local_openai_provider(&LOCAL_OPENAI_SPEC)
}

fn sample_local_openai_key() -> StoredProviderCatalogKey {
    crate::tests::support::local_openai_key(&LOCAL_OPENAI_SPEC)
}

fn sample_local_openai_endpoint() -> StoredProviderCatalogEndpoint {
    crate::tests::support::local_openai_endpoint(
        &LOCAL_OPENAI_SPEC,
        "https://api.openai.example/v1",
    )
}
#[test]
fn gateway_completes_sync_response_on_local_execution_runtime_path() {
    run_lifecycle_test(
        "gateway_completes_sync_response_on_local_execution_runtime_path",
        gateway_completes_sync_response_on_local_execution_runtime_path_impl,
    );
}

async fn gateway_completes_sync_response_on_local_execution_runtime_path_impl() {
    let public_hits = Arc::new(Mutex::new(0usize));
    let public_hits_clone = Arc::clone(&public_hits);
    let upstream = Router::new().route(
        "/v1/chat/completions",
        any(move |_request: Request| {
            let public_hits_inner = Arc::clone(&public_hits_clone);
            async move {
                *public_hits_inner.lock().expect("mutex should lock") += 1;
                (StatusCode::IM_A_TEAPOT, Body::from("public"))
            }
        }),
    );

    let execution_runtime = Router::new().route(
        "/v1/execute/sync",
        any(|_request: Request| async move {
            Json(json!({
                "request_id": "req-openai-chat-async-report-123",
                "status_code": 200,
                "headers": {
                    "content-type": "application/json"
                },
                "body": {
                    "json_body": {
                        "id": "chatcmpl-async-report-123",
                        "object": "chat.completion",
                        "model": "gpt-5",
                        "choices": [],
                        "usage": {
                            "prompt_tokens": 1,
                            "completion_tokens": 2,
                            "total_tokens": 3
                        }
                    }
                },
                "telemetry": {
                    "elapsed_ms": 12
                }
            }))
        }),
    );

    let (_upstream_url, upstream_handle) = start_server(upstream).await;
    let (execution_runtime_url, execution_runtime_handle) = start_server(execution_runtime).await;
    let auth_repository = Arc::new(InMemoryAuthApiKeySnapshotRepository::seed(vec![(
        Some(hash_api_key("sk-client-openai-async-report")),
        sample_local_openai_auth_snapshot(
            "api-key-openai-lifecycle-local-1",
            "user-openai-lifecycle-local-1",
        ),
    )]));
    let candidate_selection_repository =
        Arc::new(InMemoryMinimalCandidateSelectionReadRepository::seed(vec![
            sample_local_openai_candidate_row(),
        ]));
    let provider_catalog_repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![sample_local_openai_provider()],
        vec![sample_local_openai_endpoint()],
        vec![sample_local_openai_key()],
    ));
    let request_candidate_repository = Arc::new(InMemoryRequestCandidateRepository::default());
    let gateway = build_router_with_state(
        build_state_with_execution_runtime_override(execution_runtime_url)
            .with_data_state_for_tests(
                GatewayDataState::with_auth_candidate_selection_provider_catalog_and_request_candidate_repository_for_tests(
                    auth_repository,
                    candidate_selection_repository,
                    provider_catalog_repository,
                    request_candidate_repository,
                    DEVELOPMENT_ENCRYPTION_KEY,
                ),
            ),
    );
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .post(format!("{gateway_url}/v1/chat/completions"))
        .header(http::header::CONTENT_TYPE, "application/json")
        .header(
            http::header::AUTHORIZATION,
            "Bearer sk-client-openai-async-report",
        )
        .header(TRACE_ID_HEADER, "req-openai-chat-async-report-123")
        .body("{\"model\":\"gpt-5\",\"messages\":[]}")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(*public_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    execution_runtime_handle.abort();
    upstream_handle.abort();
}

#[test]
fn gateway_stops_execution_runtime_stream_when_client_disconnects() {
    run_lifecycle_test(
        "gateway_stops_execution_runtime_stream_when_client_disconnects",
        gateway_stops_execution_runtime_stream_when_client_disconnects_impl,
    );
}

async fn gateway_stops_execution_runtime_stream_when_client_disconnects_impl() {
    let seen_report = Arc::new(Mutex::new(0usize));
    let seen_report_clone = Arc::clone(&seen_report);
    let public_hits = Arc::new(Mutex::new(0usize));
    let public_hits_clone = Arc::clone(&public_hits);

    let upstream = Router::new()
        .route(
            "/api/internal/gateway/report-stream",
            any(move |_request: Request| {
                let seen_report_inner = Arc::clone(&seen_report_clone);
                async move {
                    *seen_report_inner.lock().expect("mutex should lock") += 1;
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
        "/v1/execute/stream",
        any(|_request: Request| async move {
            let body_stream = async_stream::stream! {
                yield Ok::<Bytes, Infallible>(Bytes::from_static(
                    b"{\"type\":\"headers\",\"payload\":{\"kind\":\"headers\",\"status_code\":200,\"headers\":{\"content-type\":\"text/event-stream\"}}}\n"
                ));
                yield Ok::<Bytes, Infallible>(Bytes::from_static(
                    b"{\"type\":\"data\",\"payload\":{\"kind\":\"data\",\"text\":\"data: {\\\"id\\\":\\\"chatcmpl-first\\\"}\\n\\n\"}}\n"
                ));
                tokio::time::sleep(std::time::Duration::from_millis(120)).await;
                yield Ok::<Bytes, Infallible>(Bytes::from_static(
                    b"{\"type\":\"data\",\"payload\":{\"kind\":\"data\",\"text\":\"data: [DONE]\\n\\n\"}}\n"
                ));
                yield Ok::<Bytes, Infallible>(Bytes::from_static(
                    b"{\"type\":\"telemetry\",\"payload\":{\"kind\":\"telemetry\",\"telemetry\":{\"elapsed_ms\":41,\"ttfb_ms\":12,\"upstream_bytes\":31}}}\n"
                ));
                yield Ok::<Bytes, Infallible>(Bytes::from_static(
                    b"{\"type\":\"eof\",\"payload\":{\"kind\":\"eof\"}}\n"
                ));
            };
            let mut response = Response::builder()
                .status(StatusCode::OK)
                .body(Body::from_stream(body_stream))
                .expect("response should build");
            response.headers_mut().insert(
                http::header::CONTENT_TYPE,
                HeaderValue::from_static("application/x-ndjson"),
            );
            response
        }),
    );

    let (_upstream_url, upstream_handle) = start_server(upstream).await;
    let (execution_runtime_url, execution_runtime_handle) = start_server(execution_runtime).await;
    let auth_repository = Arc::new(InMemoryAuthApiKeySnapshotRepository::seed(vec![(
        Some(hash_api_key("sk-client-openai-stream-disconnect")),
        sample_local_openai_auth_snapshot(
            "api-key-openai-lifecycle-local-1",
            "user-openai-lifecycle-local-1",
        ),
    )]));
    let candidate_selection_repository =
        Arc::new(InMemoryMinimalCandidateSelectionReadRepository::seed(vec![
            sample_local_openai_candidate_row(),
        ]));
    let provider_catalog_repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![sample_local_openai_provider()],
        vec![sample_local_openai_endpoint()],
        vec![sample_local_openai_key()],
    ));
    let request_candidate_repository = Arc::new(InMemoryRequestCandidateRepository::default());
    let gateway = build_cancelling_gateway(
        build_state_with_execution_runtime_override(execution_runtime_url)
            .with_data_state_for_tests(
                GatewayDataState::with_auth_candidate_selection_provider_catalog_and_request_candidate_repository_for_tests(
                    auth_repository,
                    candidate_selection_repository,
                    provider_catalog_repository,
                    request_candidate_repository,
                    DEVELOPMENT_ENCRYPTION_KEY,
                ),
            ),
    ).await;
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .post(format!("{gateway_url}/v1/chat/completions"))
        .header(http::header::CONTENT_TYPE, "application/json")
        .header(
            http::header::AUTHORIZATION,
            "Bearer sk-client-openai-stream-disconnect",
        )
        .header(TRACE_ID_HEADER, "trace-openai-chat-stream-disconnect-123")
        .body("{\"model\":\"gpt-5\",\"messages\":[],\"stream\":true}")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let mut response = response;
    let first_chunk = next_non_keepalive_chunk(&mut response).await;
    assert_eq!(
        first_chunk,
        Bytes::from_static(b"data: {\"id\":\"chatcmpl-first\"}\n\n")
    );
    drop(response);

    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    assert_eq!(*seen_report.lock().expect("mutex should lock"), 0);
    assert_eq!(*public_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    execution_runtime_handle.abort();
    upstream_handle.abort();
}

#[test]
fn gateway_settles_stream_attempt_when_client_disconnects_before_first_byte() {
    run_lifecycle_test(
        "gateway_settles_stream_attempt_when_client_disconnects_before_first_byte",
        gateway_settles_stream_attempt_when_client_disconnects_before_first_byte_impl,
    );
}

async fn gateway_settles_stream_attempt_when_client_disconnects_before_first_byte_impl() {
    // The execution runtime accepts the plan and then goes quiet, so the attempt
    // is parked between its `pending` rows and the first upstream byte.
    let execution_runtime = Router::new().route(
        "/v1/execute/stream",
        any(|_request: Request| async move {
            tokio::time::sleep(std::time::Duration::from_secs(30)).await;
            StatusCode::OK
        }),
    );

    let (execution_runtime_url, execution_runtime_handle) = start_server(execution_runtime).await;
    let auth_repository = Arc::new(InMemoryAuthApiKeySnapshotRepository::seed(vec![(
        Some(hash_api_key("sk-client-openai-stream-precommit-disconnect")),
        sample_local_openai_auth_snapshot(
            "api-key-openai-lifecycle-local-1",
            "user-openai-lifecycle-local-1",
        ),
    )]));
    let candidate_selection_repository =
        Arc::new(InMemoryMinimalCandidateSelectionReadRepository::seed(vec![
            sample_local_openai_candidate_row(),
        ]));
    let provider_catalog_repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![sample_local_openai_provider()],
        vec![sample_local_openai_endpoint()],
        vec![sample_local_openai_key()],
    ));
    let request_candidate_repository = Arc::new(InMemoryRequestCandidateRepository::default());
    let gateway = build_cancelling_gateway(
        build_state_with_execution_runtime_override(execution_runtime_url)
            .with_data_state_for_tests(
                GatewayDataState::with_auth_candidate_selection_provider_catalog_and_request_candidate_repository_for_tests(
                    auth_repository,
                    candidate_selection_repository,
                    provider_catalog_repository,
                    Arc::clone(&request_candidate_repository),
                    DEVELOPMENT_ENCRYPTION_KEY,
                ),
            ),
    ).await;
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let request = reqwest::Client::new()
        .post(format!("{gateway_url}/v1/chat/completions"))
        .header(http::header::CONTENT_TYPE, "application/json")
        .header(
            http::header::AUTHORIZATION,
            "Bearer sk-client-openai-stream-precommit-disconnect",
        )
        .header(
            TRACE_ID_HEADER,
            "trace-openai-chat-stream-precommit-disconnect-123",
        )
        .body("{\"model\":\"gpt-5\",\"messages\":[],\"stream\":true}")
        .send();

    // Drop the in-flight request the way a downstream client does when its own
    // first-byte timeout fires, before any response header exists.
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(750), request)
            .await
            .is_err(),
        "the execution runtime should not have answered before the client gave up"
    );

    let mut stored_candidates = Vec::new();
    for _ in 0..200 {
        stored_candidates = request_candidate_repository
            .list_by_request_id("trace-openai-chat-stream-precommit-disconnect-123")
            .await
            .expect("request candidate trace should read");
        if stored_candidates
            .iter()
            .any(|candidate| candidate.status == RequestCandidateStatus::Cancelled)
        {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }

    let cancelled = stored_candidates
        .iter()
        .find(|candidate| candidate.status == RequestCandidateStatus::Cancelled)
        .unwrap_or_else(|| {
            panic!("dropped stream attempt should settle as cancelled: {stored_candidates:?}")
        });
    assert_eq!(cancelled.status_code, Some(499));
    assert_eq!(
        cancelled.error_type.as_deref(),
        Some("local_stream_attempt_cancelled")
    );
    assert!(cancelled.finished_at_unix_ms.is_some());

    gateway_handle.abort();
    execution_runtime_handle.abort();
}

#[test]
fn gateway_returns_error_body_when_prefetch_detects_embedded_stream_error() {
    run_lifecycle_test(
        "gateway_returns_error_body_when_prefetch_detects_embedded_stream_error",
        gateway_returns_error_body_when_prefetch_detects_embedded_stream_error_impl,
    );
}

async fn gateway_returns_error_body_when_prefetch_detects_embedded_stream_error_impl() {
    let public_hits = Arc::new(Mutex::new(0usize));
    let public_hits_clone = Arc::clone(&public_hits);

    let upstream = Router::new().route(
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
        "/v1/execute/stream",
        any(|_request: Request| async move {
            let body_stream = async_stream::stream! {
                yield Ok::<Bytes, Infallible>(Bytes::from_static(
                    b"{\"type\":\"headers\",\"payload\":{\"kind\":\"headers\",\"status_code\":200,\"headers\":{\"content-type\":\"text/event-stream\"}}}\n"
                ));
                yield Ok::<Bytes, Infallible>(Bytes::from_static(
                    b"{\"type\":\"data\",\"payload\":{\"kind\":\"data\",\"text\":\"data: {\\\"error\\\":{\\\"message\\\":\\\"slow down\\\",\\\"type\\\":\\\"rate_limit_error\\\",\\\"code\\\":\\\"rate_limit\\\"}}\\n\\n\"}}\n"
                ));
            };
            let mut response = Response::builder()
                .status(StatusCode::OK)
                .body(Body::from_stream(body_stream))
                .expect("response should build");
            response.headers_mut().insert(
                http::header::CONTENT_TYPE,
                HeaderValue::from_static("application/x-ndjson"),
            );
            response
        }),
    );

    let (_upstream_url, upstream_handle) = start_server(upstream).await;
    let (execution_runtime_url, execution_runtime_handle) = start_server(execution_runtime).await;
    let auth_repository = Arc::new(InMemoryAuthApiKeySnapshotRepository::seed(vec![(
        Some(hash_api_key("sk-client-openai-stream-prefetch-error")),
        sample_local_openai_auth_snapshot(
            "api-key-openai-lifecycle-local-1",
            "user-openai-lifecycle-local-1",
        ),
    )]));
    let candidate_selection_repository =
        Arc::new(InMemoryMinimalCandidateSelectionReadRepository::seed(vec![
            sample_local_openai_candidate_row(),
        ]));
    let provider_catalog_repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![sample_local_openai_provider()],
        vec![sample_local_openai_endpoint()],
        vec![sample_local_openai_key()],
    ));
    let request_candidate_repository = Arc::new(InMemoryRequestCandidateRepository::default());
    let gateway = build_router_with_state(
        build_state_with_execution_runtime_override(execution_runtime_url)
            .with_data_state_for_tests(
                GatewayDataState::with_auth_candidate_selection_provider_catalog_and_request_candidate_repository_for_tests(
                    auth_repository,
                    candidate_selection_repository,
                    provider_catalog_repository,
                    Arc::clone(&request_candidate_repository),
                    DEVELOPMENT_ENCRYPTION_KEY,
                ),
            ),
    );
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .post(format!("{gateway_url}/v1/chat/completions"))
        .header(http::header::CONTENT_TYPE, "application/json")
        .header(
            http::header::AUTHORIZATION,
            "Bearer sk-client-openai-stream-prefetch-error",
        )
        .header(
            TRACE_ID_HEADER,
            "trace-openai-chat-stream-prefetch-error-123",
        )
        .body("{\"model\":\"gpt-5\",\"messages\":[],\"stream\":true}")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        response
            .headers()
            .get(http::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok()),
        Some("application/json")
    );
    assert_eq!(
        response
            .headers()
            .get(LOCAL_EXECUTION_RUNTIME_MISS_REASON_HEADER)
            .and_then(|value| value.to_str().ok()),
        Some("execution_runtime_candidates_exhausted")
    );
    let body_json: serde_json::Value = response.json().await.expect("response body should parse");
    assert_eq!(body_json["error"]["type"], "http_error");
    let stored_candidates = request_candidate_repository
        .list_by_request_id("trace-openai-chat-stream-prefetch-error-123")
        .await
        .expect("request candidate trace should read");
    let failed_candidate = stored_candidates
        .iter()
        .find(|candidate| candidate.status == RequestCandidateStatus::Failed)
        .expect("prefetched error should mark the attempted candidate as failed");
    assert!(stored_candidates
        .iter()
        .all(|candidate| candidate.status != RequestCandidateStatus::Success));
    assert_eq!(failed_candidate.status_code, Some(429));
    assert_eq!(
        failed_candidate.error_type.as_deref(),
        Some("rate_limit_error")
    );
    assert_eq!(failed_candidate.error_message.as_deref(), Some("slow down"));
    assert!(failed_candidate.finished_at_unix_ms.is_some());
    assert_eq!(*public_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    execution_runtime_handle.abort();
    upstream_handle.abort();
}
