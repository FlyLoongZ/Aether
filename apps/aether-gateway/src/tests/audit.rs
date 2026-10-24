use std::sync::Arc;

use aether_data::repository::auth::{
    InMemoryAuthApiKeySnapshotRepository, StoredAuthApiKeySnapshot,
};
use aether_data::repository::candidate_selection::InMemoryMinimalCandidateSelectionReadRepository;
use aether_data::repository::candidates::InMemoryRequestCandidateRepository;
use aether_data::repository::provider_catalog::InMemoryProviderCatalogReadRepository;
use aether_data::repository::usage::InMemoryUsageReadRepository;
use aether_data_contracts::repository::candidate_selection::StoredMinimalCandidateSelectionRow;
use aether_data_contracts::repository::candidates::{
    RequestCandidateStatus, StoredRequestCandidate,
};
use aether_data_contracts::repository::provider_catalog::{
    StoredProviderCatalogEndpoint, StoredProviderCatalogKey, StoredProviderCatalogProvider,
};
use aether_data_contracts::repository::usage::{StoredRequestUsageAudit, UsageReadRepository};
use serde_json::Value;

use super::{
    any, authenticated_operational_client, json, start_authenticated_operational_server,
    start_server, AppState, Json, Request, Router, StatusCode, UsageRuntimeConfig,
    CONTROL_REQUEST_ID_HEADER, TRACE_ID_HEADER,
};

use crate::tests::hash_api_key;
use crate::tests::DEV_FERNET_KEY;

fn run_async_test_on_large_stack<F>(name: &'static str, future: F)
where
    F: std::future::Future<Output = ()> + Send + 'static,
{
    crate::tests::run_async_test_on_large_stack(name, crate::tests::LARGE_TEST_STACK_BYTES, || {
        future
    });
}

const LOCAL_OPENAI_SPEC: crate::tests::support::LocalOpenAiSpec<'_> =
    crate::tests::support::LocalOpenAiSpec {
        id_suffix: "openai-audit-local-1",
        provider_name: "OpenAI",
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

fn sample_local_openai_endpoint(base_url: String) -> StoredProviderCatalogEndpoint {
    crate::tests::support::local_openai_endpoint(&LOCAL_OPENAI_SPEC, &base_url)
}
#[test]
fn gateway_exposes_request_id_header_for_local_execution_response() {
    run_async_test_on_large_stack(
        "gateway_exposes_request_id_header_for_local_execution_response",
        gateway_exposes_request_id_header_for_local_execution_response_impl(),
    );
}

async fn gateway_exposes_request_id_header_for_local_execution_response_impl() {
    let auth_repository = Arc::new(InMemoryAuthApiKeySnapshotRepository::seed(vec![(
        Some(hash_api_key("sk-client-openai-audit-bundle")),
        sample_local_openai_auth_snapshot("api-key-1", "user-1"),
    )]));
    let request_candidates = Arc::new(InMemoryRequestCandidateRepository::default());
    let usage_repository = Arc::new(InMemoryUsageReadRepository::default());
    let candidate_selection_repository =
        Arc::new(InMemoryMinimalCandidateSelectionReadRepository::seed(vec![
            sample_local_openai_candidate_row(),
        ]));

    let provider = Router::new().route(
        "/chat/completions",
        any(|_request: Request| async move {
            Json(json!({
                "id": "chatcmpl-direct-audit-123",
                "object": "chat.completion",
                "model": "gpt-5-upstream",
                "choices": [],
                "usage": {
                    "prompt_tokens": 2,
                    "completion_tokens": 3,
                    "total_tokens": 5
                }
            }))
        }),
    );

    let (provider_url, provider_handle) = start_server(provider).await;
    let provider_catalog = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![sample_local_openai_provider()],
        vec![sample_local_openai_endpoint(provider_url)],
        vec![sample_local_openai_key()],
    ));
    let gateway_state = AppState::new()
        .expect("gateway state should build")
        .with_data_state_for_tests(
            crate::data::GatewayDataState::with_auth_candidate_selection_provider_catalog_request_candidates_and_usage_for_tests(
                auth_repository,
                candidate_selection_repository,
                provider_catalog,
                Arc::clone(&request_candidates),
                Arc::clone(&usage_repository),
                DEV_FERNET_KEY,
            ),
        )
        .with_usage_runtime_for_tests(UsageRuntimeConfig {
            enabled: true,
            ..UsageRuntimeConfig::default()
        });
    let (gateway_url, gateway_handle, access_token) =
        start_authenticated_operational_server(gateway_state).await;
    let operational_client = authenticated_operational_client(&access_token);

    let response = reqwest::Client::new()
        .post(format!("{gateway_url}/v1/chat/completions"))
        .header(http::header::CONTENT_TYPE, "application/json")
        .header(
            http::header::AUTHORIZATION,
            "Bearer sk-client-openai-audit-bundle",
        )
        .header(TRACE_ID_HEADER, "req-direct-audit-123")
        .body("{\"model\":\"gpt-5\",\"messages\":[]}")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let request_id = response
        .headers()
        .get(CONTROL_REQUEST_ID_HEADER)
        .and_then(|value| value.to_str().ok())
        .expect("request id header should exist")
        .to_string();
    assert_eq!(request_id, "req-direct-audit-123");

    for _ in 0..50 {
        if usage_repository
            .find_by_request_id(&request_id)
            .await
            .expect("usage lookup should succeed")
            .is_some()
        {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }

    let audit_response = operational_client
        .get(format!(
            "{gateway_url}/_gateway/audit/request-audit/{request_id}?attempted_only=true"
        ))
        .send()
        .await
        .expect("request audit should succeed");

    assert_eq!(audit_response.status(), StatusCode::OK);
    let payload: Value = audit_response.json().await.expect("payload should parse");
    assert_eq!(payload["request_id"], "req-direct-audit-123");
    assert_eq!(payload["usage"]["provider_name"], "OpenAI");
    assert_eq!(payload["decision_trace"]["total_candidates"], 1);
    assert_eq!(payload["auth_snapshot"]["api_key_id"], "api-key-1");

    gateway_handle.abort();
    provider_handle.abort();
}

fn sample_request_candidate(
    id: &str,
    request_id: &str,
    candidate_index: i32,
    status: RequestCandidateStatus,
    started_at_unix_ms: Option<i64>,
    latency_ms: Option<i32>,
    status_code: Option<i32>,
) -> StoredRequestCandidate {
    StoredRequestCandidate::new(
        id.to_string(),
        request_id.to_string(),
        Some("user-1".to_string()),
        Some("api-key-1".to_string()),
        Some("alice".to_string()),
        Some("default".to_string()),
        candidate_index,
        0,
        Some("provider-1".to_string()),
        Some("endpoint-1".to_string()),
        Some("provider-key-1".to_string()),
        status,
        None,
        false,
        status_code,
        None,
        None,
        latency_ms,
        Some(1),
        None,
        None,
        (100 + i64::from(candidate_index)) * 1_000,
        started_at_unix_ms.map(|v| v * 1_000),
        started_at_unix_ms.map(|value| (value + 1) * 1_000),
    )
    .expect("candidate should build")
}

fn sample_auth_snapshot(api_key_id: &str, user_id: &str) -> StoredAuthApiKeySnapshot {
    crate::tests::support::sample_auth_snapshot_with(
        api_key_id,
        user_id,
        "alice",
        "alice@example.com",
        serde_json::json!(["openai"]),
        serde_json::json!(["openai:chat"]),
        serde_json::json!(["gpt-4.1"]),
    )
}

fn sample_provider_catalog_provider() -> StoredProviderCatalogProvider {
    StoredProviderCatalogProvider::new(
        "provider-1".to_string(),
        "OpenAI".to_string(),
        Some("https://openai.com".to_string()),
        "custom".to_string(),
    )
    .expect("provider should build")
}

fn sample_provider_catalog_endpoint() -> StoredProviderCatalogEndpoint {
    StoredProviderCatalogEndpoint::new(
        "endpoint-1".to_string(),
        "provider-1".to_string(),
        "openai:chat".to_string(),
        Some("openai".to_string()),
        Some("chat".to_string()),
        true,
    )
    .expect("endpoint should build")
}

fn sample_provider_catalog_key() -> StoredProviderCatalogKey {
    StoredProviderCatalogKey::new(
        "provider-key-1".to_string(),
        "provider-1".to_string(),
        "prod-key".to_string(),
        "api_key".to_string(),
        Some(serde_json::json!({"cache_1h": true})),
        true,
    )
    .expect("key should build")
}

fn sample_request_usage(request_id: &str) -> StoredRequestUsageAudit {
    StoredRequestUsageAudit::new(
        "usage-1".to_string(),
        request_id.to_string(),
        Some("user-1".to_string()),
        Some("api-key-1".to_string()),
        Some("alice".to_string()),
        Some("default".to_string()),
        "OpenAI".to_string(),
        "gpt-4.1".to_string(),
        Some("gpt-4.1-mini".to_string()),
        Some("provider-1".to_string()),
        Some("endpoint-1".to_string()),
        Some("provider-key-1".to_string()),
        Some("chat".to_string()),
        Some("openai:chat".to_string()),
        Some("openai".to_string()),
        Some("chat".to_string()),
        Some("openai:chat".to_string()),
        Some("openai".to_string()),
        Some("chat".to_string()),
        true,
        false,
        120,
        40,
        160,
        0.24,
        0.36,
        Some(200),
        None,
        None,
        Some(450),
        Some(120),
        "completed".to_string(),
        "settled".to_string(),
        100,
        101,
        Some(102),
    )
    .expect("usage should build")
}

#[tokio::test]
async fn gateway_exposes_request_usage_via_internal_audit_endpoint() {
    let repository = Arc::new(InMemoryUsageReadRepository::seed(vec![
        sample_request_usage("req-usage-2"),
    ]));
    let gateway_state = AppState::new()
        .expect("gateway state should build")
        .with_usage_data_reader_for_tests(repository);
    let (gateway_url, gateway_handle, access_token) =
        start_authenticated_operational_server(gateway_state).await;
    let operational_client = authenticated_operational_client(&access_token);

    let response = operational_client
        .get(format!(
            "{gateway_url}/_gateway/audit/request-usage/req-usage-2"
        ))
        .send()
        .await
        .expect("audit request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let payload: Value = response.json().await.expect("payload should parse");
    assert_eq!(payload["request_id"], "req-usage-2");
    assert_eq!(payload["provider_name"], "OpenAI");
    assert_eq!(payload["api_format"], "openai:chat");
    assert_eq!(payload["total_tokens"], 160);
    assert_eq!(payload["total_cost_usd"], 0.24);
    assert_eq!(payload["status"], "completed");
    assert_eq!(payload["billing_status"], "settled");

    gateway_handle.abort();
}

#[tokio::test]
async fn gateway_exposes_request_audit_bundle_via_internal_audit_endpoint() {
    let auth_repository = Arc::new(InMemoryAuthApiKeySnapshotRepository::seed(vec![(
        Some("hash-1".to_string()),
        sample_auth_snapshot("api-key-1", "user-1"),
    )]));
    let request_candidates = Arc::new(InMemoryRequestCandidateRepository::seed(vec![
        sample_request_candidate(
            "cand-1",
            "req-audit-1",
            0,
            RequestCandidateStatus::Success,
            Some(101),
            Some(37),
            Some(200),
        ),
    ]));
    let provider_catalog = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![sample_provider_catalog_provider()],
        vec![sample_provider_catalog_endpoint()],
        vec![sample_provider_catalog_key()],
    ));
    let usage_repository = Arc::new(InMemoryUsageReadRepository::seed(vec![
        sample_request_usage("req-audit-1"),
    ]));
    let gateway_state = AppState::new()
        .expect("gateway state should build")
        .with_request_audit_data_readers_for_tests(
            auth_repository,
            request_candidates,
            provider_catalog,
            usage_repository,
        );
    let (gateway_url, gateway_handle, access_token) =
        start_authenticated_operational_server(gateway_state).await;
    let operational_client = authenticated_operational_client(&access_token);

    let response = operational_client
        .get(format!(
            "{gateway_url}/_gateway/audit/request-audit/req-audit-1?attempted_only=true"
        ))
        .send()
        .await
        .expect("audit request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let payload: Value = response.json().await.expect("payload should parse");
    assert_eq!(payload["request_id"], "req-audit-1");
    assert_eq!(payload["usage"]["provider_name"], "OpenAI");
    assert_eq!(payload["usage"]["total_tokens"], 160);
    assert_eq!(payload["decision_trace"]["total_candidates"], 1);
    assert_eq!(
        payload["decision_trace"]["candidates"][0]["provider_key_name"],
        "prod-key"
    );
    assert_eq!(payload["auth_snapshot"]["api_key_id"], "api-key-1");
    assert_eq!(payload["auth_snapshot"]["currently_usable"], true);

    gateway_handle.abort();
}

#[tokio::test]
async fn gateway_exposes_request_candidate_trace_via_internal_audit_endpoint() {
    let mut failed_candidate = sample_request_candidate(
        "cand-2",
        "req-trace-1",
        1,
        RequestCandidateStatus::Failed,
        Some(101),
        Some(37),
        Some(502),
    );
    failed_candidate.error_message = Some("private upstream diagnostic".to_string());
    failed_candidate.extra_data = Some(json!({
        "upstream_response": {
            "status_code": 502,
            "headers": {"x-request-id": "private-upstream-id"},
            "body": {"error": {"message": "private upstream diagnostic"}}
        },
        "error_flow": {"status_code": 502, "message": "private upstream diagnostic"}
    }));
    let repository = Arc::new(InMemoryRequestCandidateRepository::seed(vec![
        sample_request_candidate(
            "cand-1",
            "req-trace-1",
            0,
            RequestCandidateStatus::Pending,
            None,
            None,
            None,
        ),
        failed_candidate,
    ]));

    let gateway_state = AppState::new()
        .expect("gateway state should build")
        .with_request_candidate_data_reader_for_tests(repository);
    let (gateway_url, gateway_handle, access_token) =
        start_authenticated_operational_server(gateway_state).await;
    let operational_client = authenticated_operational_client(&access_token);

    let response = operational_client
        .get(format!(
            "{gateway_url}/_gateway/audit/request-candidates/req-trace-1?attempted_only=true"
        ))
        .send()
        .await
        .expect("audit request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let payload: Value = response.json().await.expect("payload should parse");
    assert_eq!(payload["request_id"], "req-trace-1");
    assert_eq!(payload["total_candidates"], 1);
    assert_eq!(payload["final_status"], "failed");
    assert_eq!(payload["total_latency_ms"], 37);
    assert_eq!(
        payload["candidates"].as_array().map(|items| items.len()),
        Some(1)
    );
    assert_eq!(payload["candidates"][0]["id"], "cand-2");
    assert_eq!(payload["candidates"][0]["status"], "failed");
    assert!(payload["candidates"][0]["error_message"].is_null());
    assert_eq!(
        payload["candidates"][0]["extra_data"]["upstream_response"]["status_code"],
        502
    );
    let serialized = payload.to_string();
    assert!(!serialized.contains("private upstream diagnostic"));
    assert!(!serialized.contains("private-upstream-id"));

    gateway_handle.abort();
}

#[tokio::test]
async fn gateway_exposes_decision_trace_via_internal_audit_endpoint() {
    let request_candidates = Arc::new(InMemoryRequestCandidateRepository::seed(vec![
        sample_request_candidate(
            "cand-1",
            "req-trace-2",
            0,
            RequestCandidateStatus::Failed,
            Some(101),
            Some(37),
            Some(502),
        ),
    ]));
    let provider_catalog = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![sample_provider_catalog_provider()],
        vec![sample_provider_catalog_endpoint()],
        vec![sample_provider_catalog_key()],
    ));

    let gateway_state = AppState::new()
        .expect("gateway state should build")
        .with_decision_trace_data_readers_for_tests(request_candidates, provider_catalog);
    let (gateway_url, gateway_handle, access_token) =
        start_authenticated_operational_server(gateway_state).await;
    let operational_client = authenticated_operational_client(&access_token);

    let response = operational_client
        .get(format!(
            "{gateway_url}/_gateway/audit/decision-trace/req-trace-2?attempted_only=true"
        ))
        .send()
        .await
        .expect("audit request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let payload: Value = response.json().await.expect("payload should parse");
    assert_eq!(payload["request_id"], "req-trace-2");
    assert_eq!(payload["total_candidates"], 1);
    assert_eq!(payload["candidates"][0]["provider_name"], "OpenAI");
    assert_eq!(
        payload["candidates"][0]["provider_website"],
        "https://openai.com/"
    );
    assert_eq!(
        payload["candidates"][0]["endpoint_api_format"],
        "openai:chat"
    );
    assert_eq!(payload["candidates"][0]["provider_key_name"], "prod-key");
    assert_eq!(
        payload["candidates"][0]["provider_key_auth_type"],
        "api_key"
    );
    assert_eq!(
        payload["candidates"][0]["provider_key_capabilities"]["cache_1h"],
        true
    );

    gateway_handle.abort();
}

#[tokio::test]
async fn gateway_exposes_auth_api_key_snapshot_via_internal_audit_endpoint() {
    let repository = Arc::new(InMemoryAuthApiKeySnapshotRepository::seed(vec![(
        Some("hash-1".to_string()),
        sample_auth_snapshot("key-1", "user-1"),
    )]));

    let gateway_state = AppState::new()
        .expect("gateway state should build")
        .with_auth_api_key_data_reader_for_tests(repository);
    let (gateway_url, gateway_handle, access_token) =
        start_authenticated_operational_server(gateway_state).await;
    let operational_client = authenticated_operational_client(&access_token);

    let response = operational_client
        .get(format!(
            "{gateway_url}/_gateway/audit/auth/users/user-1/api-keys/key-1"
        ))
        .send()
        .await
        .expect("audit request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let payload: Value = response.json().await.expect("payload should parse");
    assert_eq!(payload["user_id"], "user-1");
    assert_eq!(payload["api_key_id"], "key-1");
    assert_eq!(payload["username"], "alice");
    assert_eq!(payload["user_role"], "user");
    assert_eq!(payload["api_key_name"], "default");
    assert_eq!(payload["currently_usable"], true);
    assert_eq!(payload["user_allowed_providers"][0], "openai");
    assert_eq!(payload["api_key_allowed_api_formats"][0], "openai:chat");

    gateway_handle.abort();
}
