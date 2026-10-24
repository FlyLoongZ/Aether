use crate::tests::DEV_FERNET_KEY;
use std::collections::BTreeMap;
use std::convert::Infallible;
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc, Mutex,
};
use std::time::{Duration, Instant};

use aether_ai_serving::{AiAttemptExecutionOutcome, AiAttemptRetryScope};
use aether_contracts::{
    ExecutionError, ExecutionErrorKind, ExecutionPhase, ExecutionPlan,
    ExecutionStreamTerminalSummary, ExecutionTelemetry, ExecutionTimeouts, RequestBody,
    StandardizedUsage, StreamFrame, StreamFramePayload, StreamFrameType,
};
use aether_data::repository::candidates::InMemoryRequestCandidateRepository;
use aether_data::repository::provider_catalog::InMemoryProviderCatalogReadRepository;

use aether_data::repository::usage::InMemoryUsageReadRepository;
use aether_data_contracts::repository::candidates::{
    PublicHealthStatusCount, PublicHealthTimelineBucket, RequestCandidateReadRepository,
    RequestCandidateStatus, RequestCandidateWriteRepository, StoredRequestCandidate,
    UpsertRequestCandidateRecord,
};
use aether_data_contracts::repository::provider_catalog::{
    StoredProviderCatalogEndpoint, StoredProviderCatalogKey, StoredProviderCatalogProvider,
};
use aether_data_contracts::repository::settlement::{StoredUsageSettlement, UsageSettlementInput};
use aether_data_contracts::repository::usage::UsageReadRepository;
use aether_data_contracts::repository::usage::{
    StoredRequestUsageAudit, UpsertUsageRecord, UsageBodyCaptureState,
};
use aether_data_contracts::DataLayerError;
use aether_usage_runtime::{
    apply_usage_body_capture_policy_to_event, UsageBillingEventEnricher, UsageBodyCapturePolicy,
    UsageEvent, UsageEventData, UsageEventType, UsageRecordWriter, UsageRequestRecordLevel,
    UsageRuntimeAccess, UsageRuntimeConfig, UsageSettlementWriter,
};
use async_stream::stream;
use async_trait::async_trait;
use axum::body::{to_bytes, Body, Bytes};

use axum::extract::Request;
use axum::routing::any;
use axum::{
    http::header, http::HeaderValue, http::StatusCode, response::IntoResponse, Json, Router,
};
use base64::Engine as _;
use futures_util::StreamExt as _;
use serde_json::{json, Value};
use tokio::sync::{mpsc, Notify};

use super::{
    activate_post_stop_frame_read_budget, build_sse_body_stream, build_stream_failure_report,
    build_stream_sync_payload, client_format_allows_proxy_generated_sse_control_blocks,
    direct_upstream_response_byte_stream, encode_terminal_sse_error_event_for_plan,
    ensure_stream_terminal_summary_for_missing_observed_finish, execute_execution_runtime_stream,
    execute_in_process_stream_with_oauth_retry, execute_stream_from_frame_stream,
    execute_stream_from_frame_stream_with_retry_scope, execution_stream_frame_codec,
    maybe_apply_kiro_prompt_cache_usage_to_stream_summary, merge_stream_terminal_summary,
    normalize_declared_stream_response_headers, parse_direct_passthrough_mode,
    prefetch_direct_stream_error_body, prefetched_openai_responses_body_has_output_boundary,
    record_sync_terminal_usage_with_handoff_after_spawn, resolve_provider_stream_error_status_code,
    select_direct_anthropic_prefetch_wait, should_limit_direct_finalize_prefetch,
    should_normalize_declared_stream_response_headers, should_probe_success_failover_before_stream,
    should_skip_direct_finalize_prefetch, stream_chunk_contains_sse_done,
    stream_requires_observed_terminal_event, stream_terminal_summary_missing_observed_finish,
    stream_terminal_summary_missing_observed_finish_with_requirement,
    stream_terminal_summary_represents_failure_with_requirement,
    wrap_non_json_binary_stream_error_for_client, ClientVisibleStreamCompletionTracker,
    DirectPassthroughFinalizer, DirectPassthroughFinalizerCore, DirectPassthroughInlineBodyState,
    DirectPassthroughMode, PostStopFrameReadBudget, PostStopLimitedStreamReader,
    ProviderStreamErrorInspection, ANTHROPIC_POST_STOP_DRAIN_MAX_BYTES,
    GEMINI_FILES_DOWNLOAD_PLAN_KIND, OPENAI_CHAT_STREAM_PLAN_KIND,
    OPENAI_RESPONSES_STREAM_PLAN_KIND, POST_STOP_MAX_EMPTY_CHUNKS_PER_POLL,
    PROVIDER_STREAM_ERROR_INSPECTION_MAX_BYTES,
};
use crate::control::GatewayControlDecision;
use crate::stage_metrics::RequestStageTrace;
use crate::AppState;
use aether_crypto::encrypt_python_fernet_plaintext;

fn provider_catalog_stop_429_for_plan(
    plan: &ExecutionPlan,
) -> InMemoryProviderCatalogReadRepository {
    provider_catalog_for_plan(
        plan,
        Some(json!({
            "failover_rules": {
                "stop_status_codes": [429]
            }
        })),
    )
}

fn provider_catalog_for_plan(
    plan: &ExecutionPlan,
    provider_config: Option<Value>,
) -> InMemoryProviderCatalogReadRepository {
    let credential_state = crate::tests::support::credential_fixture_state();
    let encrypted_api_key = credential_state
        .seal_provider_catalog_key_api_key(&plan.provider_id, &plan.key_id, "plain-upstream-key")
        .expect("api key should encrypt");
    let provider_type = plan.provider_name.as_deref().unwrap_or("custom");
    let provider = StoredProviderCatalogProvider::new(
        plan.provider_id.clone(),
        plan.provider_id.clone(),
        Some("https://provider.example".to_string()),
        provider_type.to_string(),
    )
    .expect("provider should build")
    .with_transport_fields(
        true,
        false,
        false,
        None,
        Some(3),
        None,
        None,
        None,
        provider_config,
    );
    let endpoint = StoredProviderCatalogEndpoint::new(
        plan.endpoint_id.clone(),
        plan.provider_id.clone(),
        plan.provider_api_format.clone(),
        None,
        None,
        true,
    )
    .expect("endpoint should build")
    .with_transport_fields(
        "https://provider.example".to_string(),
        None,
        None,
        Some(2),
        None,
        None,
        None,
        None,
    )
    .expect("endpoint transport should build");
    let key = StoredProviderCatalogKey::new(
        plan.key_id.clone(),
        plan.provider_id.clone(),
        plan.key_id.clone(),
        "api_key".to_string(),
        None,
        true,
    )
    .expect("key should build")
    .with_transport_fields(
        Some(json!([plan.provider_api_format.clone()])),
        encrypted_api_key,
        None,
        None,
        Some(json!({ "openai:chat": 1 })),
        None,
        None,
        None,
        None,
    )
    .expect("key transport should build");

    InMemoryProviderCatalogReadRepository::seed(vec![provider], vec![endpoint], vec![key])
}

#[test]
fn non_json_upstream_error_body_is_not_projected_to_clients() {
    let secret = b"Bearer upstream-secret https://user:password@example.test/private";
    let body = wrap_non_json_binary_stream_error_for_client(
        "openai_chat_stream",
        &BTreeMap::from([("content-type".to_string(), "text/plain".to_string())]),
        secret,
    )
    .expect("error body projection should succeed")
    .expect("non-JSON errors should receive a client projection");

    assert_eq!(body["error"]["message"], "Upstream request failed");
    assert!(!body.to_string().contains("upstream-secret"));
    assert!(!body.to_string().contains("password"));
}

fn provider_catalog_for_stream_auth_plan(
    plan: &ExecutionPlan,
    provider_type: &str,
    auth_type: &str,
    auth_config: Option<Value>,
) -> InMemoryProviderCatalogReadRepository {
    let provider = StoredProviderCatalogProvider::new(
        plan.provider_id.clone(),
        plan.provider_id.clone(),
        Some("https://provider.example".to_string()),
        provider_type.to_string(),
    )
    .expect("provider should build");
    let endpoint = StoredProviderCatalogEndpoint::new(
        plan.endpoint_id.clone(),
        plan.provider_id.clone(),
        plan.provider_api_format.clone(),
        None,
        None,
        true,
    )
    .expect("endpoint should build")
    .with_transport_fields(
        plan.url.clone(),
        None,
        None,
        Some(2),
        None,
        None,
        None,
        None,
    )
    .expect("endpoint transport should build");
    let encrypted_auth_config = auth_config.map(|config| {
        encrypt_python_fernet_plaintext(DEV_FERNET_KEY, &config.to_string())
            .expect("auth config should encrypt")
    });
    let key = StoredProviderCatalogKey::new(
        plan.key_id.clone(),
        plan.provider_id.clone(),
        plan.key_id.clone(),
        auth_type.to_string(),
        None,
        true,
    )
    .expect("key should build")
    .with_transport_fields(
        Some(json!([plan.provider_api_format.clone()])),
        None,
        encrypted_auth_config,
        None,
        None,
        None,
        None,
        None,
        None,
    )
    .expect("key transport should build");

    InMemoryProviderCatalogReadRepository::seed(vec![provider], vec![endpoint], vec![key])
}

fn direct_stream_test_plan(request_id: &str, url: String) -> ExecutionPlan {
    ExecutionPlan {
        request_id: request_id.to_string(),
        candidate_id: Some(format!("candidate-{request_id}")),
        provider_name: Some("codex".to_string()),
        provider_id: format!("provider-{request_id}"),
        endpoint_id: format!("endpoint-{request_id}"),
        key_id: format!("key-{request_id}"),
        method: "POST".to_string(),
        url,
        headers: BTreeMap::from([
            ("content-type".to_string(), "application/json".to_string()),
            (
                "authorization".to_string(),
                "AgentAssertion stale-task".to_string(),
            ),
        ]),
        content_type: Some("application/json".to_string()),
        content_encoding: None,
        body: RequestBody::from_json(json!({"stream": true})),
        stream: true,
        client_api_format: "openai:responses".to_string(),
        provider_api_format: "openai:responses".to_string(),
        model_name: Some("gpt-5".to_string()),
        proxy: None,
        transport_profile: None,
        timeouts: Some(ExecutionTimeouts {
            connect_ms: Some(30_000),
            first_byte_ms: Some(30_000),
            ..ExecutionTimeouts::default()
        }),
    }
}

fn agent_identity_test_auth_config(task_id: &str) -> Value {
    json!({
        "provider_type": "codex",
        "auth_mode": "agentIdentity",
        "agent_runtime_id": "runtime-test",
        "agent_private_key": "MC4CAQAwBQYDK2VwBCIEIAcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcH",
        "task_id": task_id
    })
}

fn generic_oauth_test_auth_config(provider_type: &str) -> Value {
    json!({
        "provider_type": provider_type,
        "access_token": "stale-access-token",
        "refresh_token": "refresh-token",
        "expires_at": 4_102_444_800_u64
    })
}

async fn collect_direct_execution_body(
    mut execution: crate::execution_runtime::DirectUpstreamStreamExecution,
) -> Result<Vec<u8>, String> {
    let prefetched_body = std::mem::take(&mut execution.prefetched_body);
    let mut stream = direct_upstream_response_byte_stream(prefetched_body, execution.response);
    let mut body = Vec::new();
    while let Some(item) = stream.next().await {
        body.extend_from_slice(&item?);
    }
    Ok(body)
}

fn codex_cyber_policy_plan(request_id: &str) -> ExecutionPlan {
    ExecutionPlan {
        request_id: request_id.to_string(),
        candidate_id: Some(format!("candidate-{request_id}")),
        provider_name: Some("codex".to_string()),
        provider_id: format!("provider-{request_id}"),
        endpoint_id: format!("endpoint-{request_id}"),
        key_id: format!("key-{request_id}"),
        method: "POST".to_string(),
        url: "https://chatgpt.com/backend-api/codex/responses".to_string(),
        headers: BTreeMap::from([
            ("content-type".to_string(), "application/json".to_string()),
            ("accept".to_string(), "text/event-stream".to_string()),
        ]),
        content_type: Some("application/json".to_string()),
        content_encoding: None,
        body: RequestBody::from_json(json!({
            "model": "gpt-5.5",
            "input": [],
            "stream": true
        })),
        stream: true,
        client_api_format: "openai:responses".to_string(),
        provider_api_format: "openai:responses".to_string(),
        model_name: Some("gpt-5.5".to_string()),
        proxy: None,
        transport_profile: None,
        timeouts: None,
    }
}

async fn execute_prefetched_codex_cyber_policy_failure(
    continue_failover: bool,
) -> Option<axum::http::Response<Body>> {
    let request_id = if continue_failover {
        "req-cyber-policy-retry"
    } else {
        "req-cyber-policy-stop"
    };
    let plan = codex_cyber_policy_plan(request_id);
    let provider_catalog = provider_catalog_for_plan(&plan, None);
    let data_state = crate::data::GatewayDataState::with_provider_transport_reader_for_tests(
        Arc::new(provider_catalog),
        DEV_FERNET_KEY,
    );
    let state = AppState::new()
        .expect("app state should build")
        .with_data_state_for_tests(data_state);
    let upstream_setup = "event: response.created\ndata: {\"type\":\"response.created\"}\n\n";
    let upstream_error = "event: response.failed\ndata: {\"type\":\"response.failed\",\"response\":{\"status\":\"failed\",\"error\":{\"type\":\"invalid_request\",\"message\":\"cyber policy rejected the request\",\"code\":\"cyber_policy_violation\",\"param\":\"input\"}}}\n\n";
    let frame_stream = stream! {
        yield Ok::<Bytes, std::io::Error>(ndjson_frame(StreamFrame {
            frame_type: StreamFrameType::Headers,
            payload: StreamFramePayload::Headers {
                status_code: 200,
                headers: BTreeMap::from([(
                    "content-type".to_string(),
                    "text/event-stream".to_string(),
                )]),
                response_observation: None,
            },
        }));
        yield Ok::<Bytes, std::io::Error>(ndjson_frame(StreamFrame {
            frame_type: StreamFrameType::Data,
            payload: StreamFramePayload::Data {
                chunk_b64: None,
                text: Some(upstream_setup.to_string()),
            },
        }));
        yield Ok::<Bytes, std::io::Error>(ndjson_frame(StreamFrame {
            frame_type: StreamFrameType::Data,
            payload: StreamFramePayload::Data {
                chunk_b64: None,
                text: Some(upstream_error.to_string()),
            },
        }));
        yield Ok::<Bytes, std::io::Error>(ndjson_frame(StreamFrame::eof()));
    }
    .boxed();

    execute_stream_from_frame_stream(
        &state,
        plan,
        &format!("trace-{request_id}"),
        &test_decision(),
        "openai_responses_stream",
        Some("openai_responses_stream_success".to_string()),
        Some(json!({
            "request_id": request_id,
            "candidate_id": format!("candidate-{request_id}"),
            "candidate_index": 0,
            "retry_index": 0,
            "provider_api_format": "openai:responses",
            "client_api_format": "openai:responses",
            "routing_execution_policy": {
                "cyber_continue_failover": continue_failover
            }
        })),
        crate::clock::current_unix_ms(),
        Instant::now(),
        RequestStageTrace::from_env(),
        true,
        frame_stream,
        None,
    )
    .await
    .expect("execution should succeed")
}

async fn execute_prefetched_transport_failure(
    stop_on_transport_errors: bool,
) -> AiAttemptExecutionOutcome<axum::http::Response<Body>> {
    let request_id = if stop_on_transport_errors {
        "req-prefetch-transport-stop"
    } else {
        "req-prefetch-transport-retry"
    };
    let plan = native_anthropic_stream_plan(request_id);
    let provider_config = stop_on_transport_errors.then(|| {
        json!({
            "failover_rules": {
                "stop_on_transport_errors": true,
            }
        })
    });
    let provider_catalog = provider_catalog_for_plan(&plan, provider_config);
    let data_state = crate::data::GatewayDataState::with_provider_transport_reader_for_tests(
        Arc::new(provider_catalog),
        DEV_FERNET_KEY,
    );
    let state = AppState::new()
        .expect("app state should build")
        .with_data_state_for_tests(data_state);
    let frame_stream = stream! {
        yield Ok::<Bytes, std::io::Error>(ndjson_frame(StreamFrame {
            frame_type: StreamFrameType::Headers,
            payload: StreamFramePayload::Headers {
                status_code: 200,
                headers: BTreeMap::from([(
                    "content-type".to_string(),
                    "text/event-stream".to_string(),
                )]),
                response_observation: None,
            },
        }));
        yield Ok::<Bytes, std::io::Error>(ndjson_frame(StreamFrame {
            frame_type: StreamFrameType::Error,
            payload: StreamFramePayload::Error {
                error: ExecutionError {
                    kind: ExecutionErrorKind::Internal,
                    phase: ExecutionPhase::StreamRead,
                    message: "connection reset before first body byte".to_string(),
                    upstream_status: None,
                    retryable: true,
                    failover_recommended: true,
                },
            },
        }));
    }
    .boxed();
    let mut retry_scope = AiAttemptRetryScope::Provider;
    let response = execute_stream_from_frame_stream_with_retry_scope(
        &state,
        plan,
        &format!("trace-{request_id}"),
        &test_decision(),
        "claude_chat_stream",
        Some("claude_chat_stream_success".to_string()),
        Some(json!({
            "request_id": request_id,
            "candidate_id": format!("candidate-{request_id}"),
            "candidate_index": 0,
            "retry_index": 0,
            "provider_api_format": "claude:messages",
            "client_api_format": "claude:messages"
        })),
        crate::clock::current_unix_ms(),
        Instant::now(),
        RequestStageTrace::from_env(),
        true,
        frame_stream,
        false,
        None,
        Some(&mut retry_scope),
        None,
        None,
    )
    .await
    .expect("prefetch transport execution should resolve");

    match response {
        Some(response) => AiAttemptExecutionOutcome::Responded(response),
        None => AiAttemptExecutionOutcome::Retry {
            scope: retry_scope,
            fallback_response: None,
        },
    }
}

async fn execute_prefetched_http_status_failure(
    continue_failover: bool,
) -> AiAttemptExecutionOutcome<axum::http::Response<Body>> {
    let request_id = if continue_failover {
        "req-prefetch-http-continue"
    } else {
        "req-prefetch-http-stop"
    };
    let plan = native_anthropic_stream_plan(request_id);
    let failover_rules = if continue_failover {
        json!({"continue_status_codes": [500]})
    } else {
        json!({"stop_status_codes": [500]})
    };
    let provider_catalog = provider_catalog_for_plan(
        &plan,
        Some(json!({
            "failover_rules": failover_rules,
        })),
    );
    let data_state = crate::data::GatewayDataState::with_provider_transport_reader_for_tests(
        Arc::new(provider_catalog),
        DEV_FERNET_KEY,
    );
    let state = AppState::new()
        .expect("app state should build")
        .with_data_state_for_tests(data_state);
    let frame_stream = stream! {
        yield Ok::<Bytes, std::io::Error>(ndjson_frame(StreamFrame {
            frame_type: StreamFrameType::Headers,
            payload: StreamFramePayload::Headers {
                status_code: 200,
                headers: BTreeMap::from([(
                    "content-type".to_string(),
                    "text/event-stream".to_string(),
                )]),
                response_observation: None,
            },
        }));
        yield Ok::<Bytes, std::io::Error>(ndjson_frame(StreamFrame {
            frame_type: StreamFrameType::Error,
            payload: StreamFramePayload::Error {
                error: ExecutionError {
                    kind: ExecutionErrorKind::Internal,
                    phase: ExecutionPhase::StreamRead,
                    message: "upstream returned 500 before the first body byte".to_string(),
                    upstream_status: Some(500),
                    retryable: true,
                    failover_recommended: true,
                },
            },
        }));
    }
    .boxed();
    let mut retry_scope = AiAttemptRetryScope::Provider;
    let response = execute_stream_from_frame_stream_with_retry_scope(
        &state,
        plan,
        &format!("trace-{request_id}"),
        &test_decision(),
        "claude_chat_stream",
        Some("claude_chat_stream_success".to_string()),
        Some(json!({
            "request_id": request_id,
            "candidate_id": format!("candidate-{request_id}"),
            "candidate_index": 0,
            "retry_index": 0,
            "provider_api_format": "claude:messages",
            "client_api_format": "claude:messages"
        })),
        crate::clock::current_unix_ms(),
        Instant::now(),
        RequestStageTrace::from_env(),
        true,
        frame_stream,
        false,
        None,
        Some(&mut retry_scope),
        None,
        None,
    )
    .await
    .expect("prefetch HTTP status execution should resolve");

    match response {
        Some(response) => AiAttemptExecutionOutcome::Responded(response),
        None => AiAttemptExecutionOutcome::Retry {
            scope: retry_scope,
            fallback_response: None,
        },
    }
}

async fn execute_generic_sse_precommit(
    chunks: Vec<&str>,
    routing_policy: Value,
    provider_config: Option<Value>,
    stall: bool,
) -> Option<axum::http::Response<Body>> {
    execute_generic_stream_precommit(
        chunks,
        routing_policy,
        provider_config,
        stall,
        "text/event-stream",
    )
    .await
}

async fn execute_generic_stream_precommit(
    chunks: Vec<&str>,
    routing_policy: Value,
    provider_config: Option<Value>,
    stall: bool,
    content_type: &str,
) -> Option<axum::http::Response<Body>> {
    execute_stream_precommit_for_format(
        chunks,
        routing_policy,
        provider_config,
        stall,
        content_type,
        "openai:responses",
    )
    .await
}

async fn execute_stream_precommit_for_format(
    chunks: Vec<&str>,
    routing_policy: Value,
    provider_config: Option<Value>,
    stall: bool,
    content_type: &str,
    api_format: &str,
) -> Option<axum::http::Response<Body>> {
    let request_id = format!("generic-precommit-{}", uuid::Uuid::new_v4());
    let mut plan = native_anthropic_stream_plan(&request_id);
    plan.provider_api_format = api_format.to_string();
    plan.client_api_format = api_format.to_string();
    plan.timeouts = Some(ExecutionTimeouts {
        first_byte_ms: Some(20),
        ..Default::default()
    });
    let provider_catalog = provider_catalog_for_plan(&plan, provider_config);
    let data_state = crate::data::GatewayDataState::with_provider_transport_reader_for_tests(
        Arc::new(provider_catalog),
        DEV_FERNET_KEY,
    );
    let state = AppState::new()
        .unwrap()
        .with_data_state_for_tests(data_state);
    let chunks = chunks.into_iter().map(str::to_string).collect::<Vec<_>>();
    let content_type = content_type.to_string();
    let frames = stream! {
        yield Ok::<Bytes, std::io::Error>(ndjson_frame(StreamFrame {
            frame_type: StreamFrameType::Headers,
            payload: StreamFramePayload::Headers {
                status_code: 200,
                headers: BTreeMap::from([("content-type".to_string(), content_type)]),
                response_observation: None,
            },
        }));
        for chunk in chunks {
            yield Ok(ndjson_frame(StreamFrame {
                frame_type: StreamFrameType::Data,
                payload: StreamFramePayload::Data { text: Some(chunk), chunk_b64: None },
            }));
        }
        if stall { std::future::pending::<()>().await; }
        yield Ok(ndjson_frame(StreamFrame {
            frame_type: StreamFrameType::Eof,
            payload: StreamFramePayload::Eof { summary: None },
        }));
    }
    .boxed();
    let mut scope = AiAttemptRetryScope::Provider;
    let plan_kind = if api_format == "openai:image" {
        "openai_image_stream"
    } else {
        "openai_responses_stream"
    };
    execute_stream_from_frame_stream_with_retry_scope(
        &state,
        plan,
        "trace-generic-precommit",
        &test_decision(),
        plan_kind,
        Some(format!("{plan_kind}_success")),
        Some(json!({
            "request_id": request_id, "candidate_id": format!("candidate-{request_id}"),
            "candidate_index": 0, "retry_index": 0,
            "provider_api_format": api_format, "client_api_format": api_format,
            "routing_execution_policy": routing_policy,
        })),
        crate::clock::current_unix_ms(),
        Instant::now(),
        RequestStageTrace::from_env(),
        true,
        frames,
        false,
        None,
        Some(&mut scope),
        None,
        None,
    )
    .await
    .unwrap()
}

#[tokio::test]
async fn prefetch_handoff_preserves_large_responses_setup_event() {
    let event = format!(
        "event: response.created\ndata: {}\n\n",
        json!({"type":"response.created", "response": {
            "id":"resp-large-setup", "status":"in_progress", "output":[],
            "tools":[{"name":"write", "description":"x".repeat(64 * 1024)}]
        }})
    );
    let done = "event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp-large-setup\",\"status\":\"completed\",\"output\":[],\"usage\":{\"input_tokens\":7,\"output_tokens\":2}}}\n\n";
    // Include the two observed transport boundaries, exact/near budget
    // boundaries, and multiple prefetch chunks crossing the budget.
    for cuts in [
        vec![16_383],
        vec![16_384],
        vec![17_735],
        vec![17_741],
        vec![8_192, 17_735],
    ] {
        let mut chunks = Vec::new();
        let mut start = 0;
        for end in cuts {
            chunks.push(&event[start..end]);
            start = end;
        }
        chunks.push(&event[start..]);
        chunks.push(done);
        let response = execute_generic_sse_precommit(chunks, json!({}), None, false)
            .await
            .expect("large setup should commit at the bounded prefetch limit");
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body = String::from_utf8(body.to_vec()).unwrap();
        assert!(
            body.starts_with(&event),
            "setup bytes lost or duplicated at split {start}"
        );
        let events: Vec<Value> = body
            .lines()
            .filter_map(|line| line.strip_prefix("data: "))
            .filter(|payload| *payload != "[DONE]")
            .map(|payload| {
                serde_json::from_str(payload).expect("every SSE payload must be valid JSON")
            })
            .collect();
        assert_eq!(events.len(), 2, "events must be forwarded exactly once");
        assert_eq!(events[1]["type"], "response.completed");
    }
}

#[tokio::test]
async fn prefetch_handoff_keeps_audit_usage_and_private_conversion() {
    for private in [false, true] {
        let request_id = format!("handoff-audit-{}", uuid::Uuid::new_v4());
        let mut plan = if private {
            antigravity_gemini_stream_plan(&request_id)
        } else {
            native_anthropic_stream_plan(&request_id)
        };
        if !private {
            plan.provider_api_format = "openai:responses".into();
            plan.client_api_format = "openai:responses".into();
        }
        let context = json!({
            "request_id": request_id, "candidate_id": plan.candidate_id,
            "candidate_index":0, "retry_index":0,
            "provider_api_format": plan.provider_api_format,
            "client_api_format": plan.client_api_format,
            "needs_conversion": private, "has_envelope": private,
            "envelope_name": if private { "antigravity:v1internal" } else { "" },
        });
        let repository = Arc::new(InMemoryUsageReadRepository::default());
        let catalog = provider_catalog_for_plan(&plan, None);
        let state = AppState::new()
            .unwrap()
            .with_data_state_for_tests(
                crate::data::GatewayDataState::with_usage_repository_for_tests(Arc::clone(
                    &repository,
                ))
                .with_provider_catalog_reader(Arc::new(catalog))
                .with_encryption_key_for_tests(DEV_FERNET_KEY)
                .with_system_config_values_for_tests([(
                    "request_record_level".into(),
                    json!("full"),
                )]),
            )
            .with_usage_runtime_for_tests(UsageRuntimeConfig {
                enabled: true,
                ..Default::default()
            });
        let text = "hello".repeat(12_000);
        let payload = if private {
            json!({"response":{"candidates":[{"content":{"role":"model","parts":[{"text":text}]},
                "finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":1234,"candidatesTokenCount":567},
                "modelVersion":"gemini-3.7-flash-tiered"}})
        } else {
            json!({"type":"response.completed","response":{"id":"resp-handoff-usage","status":"completed",
                "output":[{"type":"message","id":"msg-handoff","role":"assistant","status":"completed",
                    "content":[{"type":"output_text","text":text,"annotations":[]}]}],
                "usage":{"input_tokens":1234,"output_tokens":567,"total_tokens":1801}}})
        };
        let input = format!("data: {payload}\n\n");
        // One complete large chunk exercises an already-emitted prefetch
        // result; the private path exercises incomplete normalization too.
        let chunks = if private {
            vec![input[..17_735].to_string(), input[17_735..].to_string()]
        } else {
            vec![input.clone()]
        };
        let frames = stream! {
            yield Ok::<Bytes, std::io::Error>(ndjson_frame(StreamFrame {
                frame_type:StreamFrameType::Headers,
                payload:StreamFramePayload::Headers { status_code:200,
                    headers:BTreeMap::from([("content-type".into(),"text/event-stream".into())]),
                    response_observation:None },
            }));
            for chunk in chunks {
                yield Ok(ndjson_frame(StreamFrame { frame_type:StreamFrameType::Data,
                    payload:StreamFramePayload::Data { text:Some(chunk),chunk_b64:None } }));
            }
            yield Ok(ndjson_frame(StreamFrame::eof()));
        }
        .boxed();
        let response = execute_stream_from_frame_stream(
            &state,
            plan,
            "trace-handoff-audit",
            &test_decision(),
            OPENAI_RESPONSES_STREAM_PLAN_KIND,
            Some("openai_responses_stream_success".into()),
            Some(context),
            crate::clock::current_unix_ms(),
            Instant::now(),
            RequestStageTrace::from_env(),
            false,
            frames,
            None,
        )
        .await
        .unwrap()
        .unwrap();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body = String::from_utf8(body.to_vec()).unwrap();
        let events: Vec<Value> = body
            .lines()
            .filter_map(|l| l.strip_prefix("data: "))
            .filter(|p| *p != "[DONE]")
            .map(|p| serde_json::from_str(p).unwrap())
            .collect();
        assert_eq!(
            events
                .iter()
                .filter(|e| e["type"] == "response.completed")
                .count(),
            1
        );
        assert!(body.contains(&text));
        let usage = tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if let Some(u) = repository
                    .find_by_request_id(&request_id)
                    .await
                    .unwrap()
                    .filter(|u| u.status == "completed" || u.status == "failed")
                {
                    break u;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("usage should finalize");
        assert_eq!(usage.status, "completed", "{:?}", usage.error_message);
        assert_eq!(usage.input_tokens, 1234);
        assert_eq!(usage.output_tokens, 567);
        let captured = usage.response_body.as_ref().expect("provider capture");
        assert!(
            captured["metadata"].get("dropped_chunks").is_none(),
            "{captured}"
        );
        assert_eq!(captured["chunks"].as_array().unwrap(), &vec![payload]);
    }
}

#[tokio::test]
async fn generic_stream_success_regex_matches_fragmented_plain_body() {
    for chunks in [
        vec!["upstream CAPACITY ", "exhausted"],
        vec!["[upstream] CAPACITY ", "exhausted"],
    ] {
        assert!(execute_generic_stream_precommit(
            chunks,
            json!({"failover_rules": {"success_failover_patterns": [{"pattern": "(?i)capacity.*exhausted"}]}}),
            None,
            false,
            "text/plain",
        ).await.is_none());
    }
}

#[tokio::test]
async fn generic_stream_200_json_error_obeys_global_stop_rules() {
    for stop in [false, true] {
        let response = execute_generic_stream_precommit(
            vec![r#"{"error":{"type":"server_error","message":"do not retry"}}"#],
            if stop {
                json!({"failover_rules": {"error_stop_patterns": [{"pattern": "do not retry"}]}})
            } else {
                json!({})
            },
            None,
            false,
            "application/json",
        )
        .await;
        assert_eq!(response.is_some(), stop);
        if let Some(response) = response {
            assert!(response.status().is_server_error());
        }
    }
}

#[tokio::test]
async fn generic_sse_200_setup_then_error_retries_before_client_output() {
    let response = execute_generic_sse_precommit(vec![
        "event: response.created\ndata: {\"type\":\"response.created\"}\n\n",
        "event: response.failed\ndata: {\"type\":\"response.failed\",\"response\":{\"error\":{\"type\":\"server_error\",\"message\":\"capacity exhausted\"}}}\n\n",
    ], json!({}), None, false).await;
    assert!(response.is_none());
}

#[tokio::test]
async fn generic_sse_global_stop_rule_overrides_retryable_embedded_error() {
    let response = execute_generic_sse_precommit(vec![
        "event: response.failed\ndata: {\"type\":\"response.failed\",\"response\":{\"error\":{\"type\":\"server_error\",\"message\":\"capacity exhausted\"}}}\n\n",
    ], json!({ "failover_rules": { "error_stop_patterns": [{ "pattern": "capacity" }] } }), None, false).await;
    let response = response.expect("global stop must return a terminal response");
    assert!(response.status().is_server_error());
    to_bytes(response.into_body(), usize::MAX).await.unwrap();
}

#[tokio::test]
async fn generic_image_success_is_not_replayed_by_global_or_provider_success_regex() {
    let rule = json!({ "success_failover_patterns": [{ "pattern": "b64_json" }] });
    for (routing_policy, provider_config) in [
        (json!({ "failover_rules": rule }), None),
        (json!({}), Some(json!({ "failover_rules": rule }))),
    ] {
        let response = execute_stream_precommit_for_format(
            vec![r#"{"created":1,"data":[{"b64_json":"aGVsbG8="}]}"#],
            routing_policy,
            provider_config,
            false,
            "application/json",
            "openai:image",
        )
        .await
        .expect("successful image responses must retain their no-replay protection");
        assert_eq!(response.status(), axum::http::StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert!(String::from_utf8_lossy(&body).contains("aGVsbG8="));
    }
}

#[tokio::test]
async fn generic_complete_setup_events_and_json_bodies_still_match_success_regex() {
    for (content_type, chunks) in [
        (
            "text/event-stream",
            vec![
                "event: response.created\ndata: {\"type\":\"response.created\",\"response\":{\"metadata\":{\"warning\":\"capacity",
                " exhausted\"}}}\n\n",
            ],
        ),
        (
            "application/json",
            vec!["{\"warning\":\"capacity", " exhausted\"}"],
        ),
    ] {
        assert!(execute_generic_stream_precommit(
            chunks,
            json!({ "failover_rules": {
                "success_failover_patterns": [{ "pattern": "capacity.*exhausted" }],
            } }),
            None,
            false,
            content_type,
        )
        .await
        .is_none());
    }
}

#[tokio::test]
async fn generic_fragmented_errors_apply_stop_rules_before_success_regex() {
    for (content_type, chunks) in [
        (
            "text/event-stream",
            vec![
                "event: response.created\ndata: {\"type\":\"response.created\"}\n\n",
                "event: response.failed\ndata: {\"type\":\"response.failed\",\"response\":{\"error\":{\"type\":\"server_error\",\"message\":\"capacity",
                " exhausted\"}}}\n\n",
            ],
        ),
        (
            "application/json",
            vec![
                "{\"error\":{\"type\":\"server_error\",\"message\":\"capacity",
                " exhausted\"}}",
            ],
        ),
        (
            "application/json",
            vec![r#"{"error":{"type":"server_error","message":"capacity exhausted"}}"#],
        ),
    ] {
        let response = execute_generic_stream_precommit(
            chunks,
            json!({ "failover_rules": {
                "success_failover_patterns": [{ "pattern": "capacity" }],
                "error_stop_patterns": [{ "status_codes": [500], "pattern": "capacity" }],
            } }),
            None,
            false,
            content_type,
        )
        .await
        .unwrap_or_else(|| panic!("partial errors must be parsed before applying success regex rules ({content_type})"));
        assert!(response.status().is_server_error());
        to_bytes(response.into_body(), usize::MAX).await.unwrap();
    }
}

#[tokio::test]
async fn generic_sse_global_error_stop_precedes_global_or_provider_success_regex() {
    let success_rules = json!({ "success_failover_patterns": [{ "pattern": "capacity" }] });
    let stop_rule = json!([{ "status_codes": [500], "pattern": "capacity" }]);
    for (routing_policy, provider_config) in [
        (
            json!({ "failover_rules": {
                "success_failover_patterns": success_rules["success_failover_patterns"],
                "error_stop_patterns": stop_rule,
            } }),
            None,
        ),
        (
            json!({ "failover_rules": { "error_stop_patterns": stop_rule } }),
            Some(json!({ "failover_rules": success_rules })),
        ),
    ] {
        let response = execute_generic_sse_precommit(
            vec!["event: response.failed\ndata: {\"type\":\"response.failed\",\"response\":{\"error\":{\"type\":\"server_error\",\"message\":\"capacity exhausted\"}}}\n\n"],
            routing_policy,
            provider_config,
            false,
        )
        .await
        .expect("a matching global stop rule must win over a 200 success regex");
        assert!(response.status().is_server_error());
        to_bytes(response.into_body(), usize::MAX).await.unwrap();
    }
}

#[tokio::test]
async fn generic_sse_success_regex_applies_to_global_and_provider_rules() {
    let rule = json!({ "success_failover_patterns": [{ "pattern": "(?i)CAPACITY" }] });
    for (global, provider) in [
        (json!({ "failover_rules": rule.clone() }), None),
        (json!({}), Some(json!({ "failover_rules": rule }))),
    ] {
        let response = execute_generic_sse_precommit(vec![
            "event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"capacity exhausted\"}\n\n",
        ], global, provider, false).await;
        assert!(response.is_none());
    }
}

#[tokio::test]
async fn generic_sse_late_error_does_not_replay_committed_content() {
    let response = execute_generic_sse_precommit(vec![
        "event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"hello\"}\n\n",
        "event: response.failed\ndata: {\"type\":\"response.failed\",\"response\":{\"error\":{\"type\":\"server_error\",\"message\":\"late failure\"}}}\n\n",
    ], json!({}), None, false).await.expect("committed stream must not retry");
    assert_eq!(response.status(), axum::http::StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    assert!(String::from_utf8_lossy(&body).contains("hello"));
}

#[tokio::test]
async fn generic_sse_setup_timeout_ignores_removed_global_transport_stop_flag() {
    let response = execute_generic_sse_precommit(
        vec!["event: response.created\ndata: {\"type\":\"response.created\"}\n\n"],
        json!({ "failover_rules": { "stop_on_transport_errors": true } }),
        None,
        true,
    )
    .await;
    assert!(response.is_none());
}

#[tokio::test]
async fn generic_sse_setup_timeout_always_retries() {
    let response = execute_generic_sse_precommit(
        vec!["event: response.created\ndata: {\"type\":\"response.created\"}\n\n"],
        json!({}),
        None,
        true,
    )
    .await;
    assert!(response.is_none());
}

fn native_anthropic_stream_plan(request_id: &str) -> ExecutionPlan {
    ExecutionPlan {
        request_id: request_id.to_string(),
        candidate_id: Some(format!("candidate-{request_id}")),
        provider_name: Some("custom".to_string()),
        provider_id: format!("provider-{request_id}"),
        endpoint_id: format!("endpoint-{request_id}"),
        key_id: format!("key-{request_id}"),
        method: "POST".to_string(),
        url: "https://api.anthropic.com/v1/messages".to_string(),
        headers: BTreeMap::from([
            ("content-type".to_string(), "application/json".to_string()),
            ("accept".to_string(), "text/event-stream".to_string()),
        ]),
        content_type: Some("application/json".to_string()),
        content_encoding: None,
        body: RequestBody::from_json(json!({
            "model": "claude-sonnet-4-6",
            "messages": [{"role": "user", "content": "hello"}],
            "max_tokens": 32,
            "stream": true
        })),
        stream: true,
        client_api_format: "claude:messages".to_string(),
        provider_api_format: "claude:messages".to_string(),
        model_name: Some("claude-sonnet-4-6".to_string()),
        proxy: None,
        transport_profile: None,
        timeouts: None,
    }
}

fn antigravity_gemini_stream_plan(request_id: &str) -> ExecutionPlan {
    ExecutionPlan {
        request_id: request_id.to_string(),
        candidate_id: Some(format!("candidate-{request_id}")),
        provider_name: Some("antigravity".to_string()),
        provider_id: format!("provider-{request_id}"),
        endpoint_id: format!("endpoint-{request_id}"),
        key_id: format!("key-{request_id}"),
        method: "POST".to_string(),
        url: "https://cloudcode-pa.googleapis.com/v1internal:streamGenerateContent".to_string(),
        headers: BTreeMap::from([
            ("content-type".to_string(), "application/json".to_string()),
            ("accept".to_string(), "text/event-stream".to_string()),
        ]),
        content_type: Some("application/json".to_string()),
        content_encoding: None,
        body: RequestBody::from_json(json!({
            "model": "gemini-3.7-flash-tiered",
            "contents": [{"role": "user", "parts": [{"text": "validate"}]}]
        })),
        stream: true,
        client_api_format: "openai:responses".to_string(),
        provider_api_format: "gemini:generate_content".to_string(),
        model_name: Some("gemini-3.7-flash-tiered".to_string()),
        proxy: None,
        transport_profile: None,
        timeouts: None,
    }
}

struct StreamDropFlag(Arc<AtomicBool>);

impl Drop for StreamDropFlag {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

fn direct_anthropic_test_finalizer(request_id: &str) -> DirectPassthroughFinalizer {
    let state = AppState::new().expect("app state should build");
    let plan = native_anthropic_stream_plan(request_id);
    let lifecycle_seed = aether_usage_runtime::build_lifecycle_usage_seed(&plan, None);
    DirectPassthroughFinalizer::new(DirectPassthroughFinalizerCore {
        state,
        trace_id: format!("trace-{request_id}"),
        report_kind: None,
        report_context: None,
        lifecycle_seed,
        direct_stream_finalize_kind: None,
        stream_started_at: Instant::now(),
        stage_trace: RequestStageTrace::from_env(),
        request_diagnostics: None,
        request_id_for_log: request_id.to_string(),
        candidate_id: plan.candidate_id.clone(),
        request_candidate_status_snapshot: None,
        deferred_request_candidate_status_record: None,
        candidate_started_unix_secs: crate::clock::current_unix_ms(),
        status_code: 200,
        headers: BTreeMap::from([("content-type".to_string(), "text/event-stream".to_string())]),
        stream_usage_report_context: None,
        stream_usage_observer: None,
        stream_usage_observer_buffered: super::StreamUsageObservationBuffer::new(
            super::DEFAULT_USAGE_RESPONSE_BODY_CAPTURE_LIMIT_BYTES,
        ),
        provider_error_inspection: ProviderStreamErrorInspection::default(),
        max_stream_body_buffer_bytes: super::DEFAULT_USAGE_RESPONSE_BODY_CAPTURE_LIMIT_BYTES,
        provider_buffered_body: super::StreamBodyCapture::default(),
        buffered_body: super::StreamBodyCapture::default(),
        provider_body_truncated: false,
        client_body_truncated: false,
        client_stream_completion_tracker: ClientVisibleStreamCompletionTracker::default(),
        requires_anthropic_message_stop: true,
        client_visible_stream_completed: false,
        usage_stream_telemetry: None,
        telemetry: None,
        provider_stream_bytes: 0,
        client_stream_bytes: 0,
        last_client_chunk_elapsed_ms: 0,
        pending_recorded: false,
        stream_started_recorded: false,
        terminal_failure: None,
        _provider_pool_in_flight_guard: None,
        _upstream_target_permit: None,
        plan,
    })
}

fn discard_direct_test_finalizer(state: &mut DirectPassthroughInlineBodyState) {
    if let Some(mut finalizer) = state.finalizer.take() {
        finalizer.core.take();
    }
}

fn direct_anthropic_inline_state(
    request_id: &str,
    items: Vec<Result<Bytes, String>>,
) -> DirectPassthroughInlineBodyState {
    DirectPassthroughInlineBodyState {
        finalizer: Some(direct_anthropic_test_finalizer(request_id)),
        upstream: Some(futures_util::stream::iter(items).boxed()),
        upstream_control_filter: Some(super::SseControlBlockFilter::default()),
        upstream_started_at: Instant::now(),
        stream_first_byte_timeout: None,
        stream_idle_timeout: None,
        observed_first_body_poll: false,
        observed_first_client_yield: false,
        upstream_done: false,
        control_filter_flushed: false,
        terminal_error_sent: false,
        finalized: false,
    }
}

#[tokio::test]
async fn stream_capture_budget_exhaustion_preserves_inline_bytes_and_terminal_usage() {
    use super::super::capture_budget::{StreamBodyCapture, StreamCaptureBudget};

    let chunks = [
        Bytes::from_static(b"data: {\"id\":\"x\",\"model\":\"gpt\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hello\"},\"finish_reason\":null}]}\n\n"),
        Bytes::from_static(b"data: {\"id\":\"x\",\"model\":\"gpt\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":11,\"completion_tokens\":7,\"prompt_tokens_details\":{\"cached_tokens\":3}}}\n\n"),
        Bytes::from_static(b"data: [DONE]\n\n"),
    ];
    for budget_bytes in [0, 64] {
        let budget = StreamCaptureBudget::new(budget_bytes);
        let mut state = direct_anthropic_inline_state(
            "capture-budget-inline",
            chunks.iter().cloned().map(Ok).collect(),
        );
        let core = state.finalizer.as_mut().unwrap().core_mut();
        core.requires_anthropic_message_stop = false;
        core.plan.provider_api_format = "openai:chat".to_string();
        core.plan.client_api_format = "openai:chat".to_string();
        core.stream_usage_report_context = Some(json!({
            "provider_api_format": "openai:chat", "client_api_format": "openai:chat"
        }));
        core.stream_usage_observer = Some(super::StreamingStandardTerminalObserver::default());
        core.provider_buffered_body = StreamBodyCapture::with_budget(Arc::clone(&budget));
        core.buffered_body = StreamBodyCapture::with_budget(budget);
        for expected in &chunks {
            let (actual, next) = state.next_item().await.expect("streamed chunk");
            assert_eq!(actual.unwrap(), *expected);
            state = next;
        }
        let core = state.finalizer.as_mut().unwrap().core_mut();
        assert!(core.terminal_failure.is_none());
        assert!(core.client_visible_stream_completed);
        assert!(core.provider_body_truncated);
        assert!(core.client_body_truncated);
        assert!(core.provider_buffered_body.len() + core.buffered_body.len() <= budget_bytes);
        let summary = super::finalize_stream_usage_observer(
            &mut core.stream_usage_observer,
            core.stream_usage_report_context.as_ref(),
            &mut core.stream_usage_observer_buffered,
        )
        .unwrap();
        assert!(summary.observed_finish);
        assert!(summary.parser_error.is_none());
        let payload = super::build_stream_usage_payload(
            "capture-budget-inline".to_string(),
            "openai_chat_stream".to_string(),
            core.stream_usage_report_context.clone(),
            200,
            BTreeMap::new(),
            &core.provider_buffered_body,
            core.provider_body_truncated,
            &core.buffered_body,
            core.client_body_truncated,
            Some(summary),
            None,
        );
        let seed = aether_usage_runtime::build_stream_terminal_usage_payload_seed(&payload);
        let usage = seed.standardized_usage.unwrap();
        assert_eq!(usage.input_tokens, 11);
        assert_eq!(usage.output_tokens, 7);
        assert_eq!(usage.cache_read_tokens, 3);
        assert_eq!(
            payload.provider_body_state,
            Some(UsageBodyCaptureState::Truncated)
        );
        assert_eq!(
            payload.client_body_state,
            Some(UsageBodyCaptureState::Truncated)
        );
        discard_direct_test_finalizer(&mut state);
    }
}

#[test]
fn stream_capture_fallback_after_disabled_observer_updates_tokens_zero_cache_and_tier() {
    let context = json!({"provider_api_format": "openai:chat"});
    let mut observer = Some(super::StreamingStandardTerminalObserver::default());
    let mut buffer =
        super::StreamUsageObservationBuffer::new(super::BASIC_STREAM_BODY_ANALYSIS_LIMIT_BYTES);
    super::observe_stream_usage_bytes(observer.as_mut().unwrap(), &context, &mut buffer,
        b"data: {\"id\":\"x\",\"model\":\"gpt\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":100,\"completion_tokens\":10,\"prompt_tokens_details\":{\"cached_tokens\":30}}}\n\n");
    let oversized = format!(
        "data: {{\"content\":\"{}\"}}\n\n",
        "x".repeat(super::SSE_TERMINAL_DETECTOR_MAX_LINE_BYTES)
    );
    for part in oversized.as_bytes().chunks(4096) {
        super::observe_stream_usage_bytes(observer.as_mut().unwrap(), &context, &mut buffer, part);
    }
    super::observe_stream_usage_bytes(observer.as_mut().unwrap(), &context, &mut buffer,
        b"data: {\"\\u0075sage\":{\"prompt_tokens\":100,\"completion_tokens\":500,\"prompt_tokens_details\":{\"cached_tokens\":0}},\"service_tier\":\"priority\"}\n\n");
    let summary =
        super::finalize_stream_usage_observer(&mut observer, Some(&context), &mut buffer).unwrap();
    assert!(summary
        .parser_error
        .as_deref()
        .unwrap()
        .contains("exceeded"));
    assert!(summary.observed_finish);
    assert_eq!(
        summary.provider_actual_service_tier.as_deref(),
        Some("priority")
    );
    let usage = summary.standardized_usage.as_ref().unwrap();
    assert_eq!(usage.input_tokens, 100);
    assert_eq!(usage.output_tokens, 500);
    assert_eq!(usage.cache_read_tokens, 0);

    let eof_summary = ExecutionStreamTerminalSummary {
        standardized_usage: Some(StandardizedUsage {
            input_tokens: 100,
            output_tokens: 10,
            cache_read_tokens: 30,
            ..StandardizedUsage::new()
        }),
        response_id: Some("authoritative-eof-id".to_string()),
        finish_reason: Some("stop".to_string()),
        observed_finish: true,
        ..ExecutionStreamTerminalSummary::default()
    };
    let merged =
        super::merge_observed_stream_terminal_summary(Some(eof_summary), Some(summary), &buffer)
            .unwrap();
    assert_eq!(merged.response_id.as_deref(), Some("authoritative-eof-id"));
    assert_eq!(merged.finish_reason.as_deref(), Some("stop"));
    assert!(merged.observed_finish);
    assert!(merged.parser_error.as_deref().unwrap().contains("exceeded"));
    let usage = merged.standardized_usage.unwrap();
    assert_eq!(usage.input_tokens, 100);
    assert_eq!(usage.output_tokens, 500);
    assert_eq!(usage.cache_read_tokens, 0);
}

#[test]
fn stream_capture_budget_zero_preserves_conversion_bytes_and_usage() {
    use super::super::capture_budget::{StreamBodyCapture, StreamCaptureBudget};

    let context = json!({
        "provider_api_format": "openai:chat",
        "client_api_format": "claude:messages",
        "needs_conversion": true,
    });
    let chunks: [&[u8]; 3] = [
        b"data: {\"id\":\"x\",\"model\":\"gpt\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"hello\"},\"finish_reason\":null}]}\n\n",
        b"data: {\"id\":\"x\",\"model\":\"gpt\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":11,\"completion_tokens\":7,\"prompt_tokens_details\":{\"cached_tokens\":3}}}\n\n",
        b"data: [DONE]\n\n",
    ];
    let mut expected = None;
    for bytes in [32 * 1024, 0] {
        let budget = StreamCaptureBudget::new(bytes);
        let mut provider = StreamBodyCapture::with_budget(Arc::clone(&budget));
        let mut client = StreamBodyCapture::with_budget(budget);
        let mut provider_truncated = false;
        let mut client_truncated = false;
        let mut observer = Some(super::StreamingStandardTerminalObserver::default());
        let mut buffer = super::StreamUsageObservationBuffer::new(32 * 1024);
        let mut rewriter = super::maybe_build_stream_response_rewriter(Some(&context)).unwrap();
        let mut delivered = Vec::new();
        for (index, chunk) in chunks.into_iter().enumerate() {
            provider.append(chunk, 32 * 1024, &mut provider_truncated);
            super::observe_stream_usage_bytes(
                observer.as_mut().unwrap(),
                &context,
                &mut buffer,
                chunk,
            );
            let output = rewriter.push_chunk(chunk).unwrap();
            client.append(&output, 32 * 1024, &mut client_truncated);
            delivered.extend(output);
            if index == 0 {
                // Task handoff must also work when audit admits no bytes.
                rewriter = rewriter.into_owned();
            }
        }
        let tail = rewriter.finish().unwrap();
        client.append(&tail, 32 * 1024, &mut client_truncated);
        delivered.extend(tail);
        let summary =
            super::finalize_stream_usage_observer(&mut observer, Some(&context), &mut buffer)
                .unwrap();
        assert!(summary.observed_finish);
        assert!(summary.parser_error.is_none());
        let payload = super::build_stream_usage_payload(
            "capture-budget-conversion".to_string(),
            "claude_chat_stream".to_string(),
            Some(context.clone()),
            200,
            BTreeMap::new(),
            &provider,
            provider_truncated,
            &client,
            client_truncated,
            Some(summary),
            None,
        );
        let seed = aether_usage_runtime::build_stream_terminal_usage_payload_seed(&payload);
        let usage = seed.standardized_usage.unwrap();
        assert_eq!(usage.input_tokens, 11);
        assert_eq!(usage.output_tokens, 7);
        assert_eq!(usage.cache_read_tokens, 3);
        assert!(String::from_utf8_lossy(&delivered).contains("message_stop"));
        if let Some(expected) = &expected {
            assert_eq!(&delivered, expected);
            assert_eq!(
                payload.provider_body_state,
                Some(UsageBodyCaptureState::Truncated)
            );
            assert_eq!(
                payload.client_body_state,
                Some(UsageBodyCaptureState::Truncated)
            );
        } else {
            expected = Some(delivered);
        }
    }
}

#[test]
fn stream_capture_budget_zero_preserves_sync_json_bridge_terminal_usage_and_tier() {
    use super::super::capture_budget::{StreamBodyCapture, StreamCaptureBudget};

    let response = json!({
        "id": "chatcmpl-capture", "object": "chat.completion", "model": "gpt",
        "choices": [{"index": 0, "message": {"role": "assistant", "content": "hello"}, "finish_reason": "stop"}],
        "usage": {"prompt_tokens": 11, "completion_tokens": 7, "total_tokens": 18,
            "prompt_tokens_details": {"cached_tokens": 3}},
        "service_tier": "priority",
    });
    let outcome = super::maybe_bridge_standard_sync_json_to_stream(
        &response,
        "openai:chat",
        "openai:chat",
        None,
    )
    .unwrap()
    .unwrap();
    let delivered = outcome.sse_body;
    assert!(String::from_utf8_lossy(&delivered).contains("hello"));
    assert!(String::from_utf8_lossy(&delivered).contains("[DONE]"));
    let budget = StreamCaptureBudget::new(0);
    let mut provider = StreamBodyCapture::with_budget(Arc::clone(&budget));
    let mut client = StreamBodyCapture::with_budget(budget);
    let mut provider_truncated = false;
    let mut client_truncated = false;
    provider.append(
        &serde_json::to_vec(&response).unwrap(),
        32 * 1024,
        &mut provider_truncated,
    );
    client.append(&delivered, 32 * 1024, &mut client_truncated);
    let summary = outcome.terminal_summary.unwrap();
    assert!(summary.observed_finish);
    assert!(summary.parser_error.is_none());
    assert_eq!(
        summary.provider_actual_service_tier.as_deref(),
        Some("priority")
    );
    let payload = super::build_stream_usage_payload(
        "capture-budget-sync-bridge".to_string(),
        "openai_chat_stream".to_string(),
        None,
        200,
        BTreeMap::new(),
        &provider,
        provider_truncated,
        &client,
        client_truncated,
        Some(summary),
        None,
    );
    assert!(payload.provider_body_base64.is_none());
    assert!(payload.client_body_base64.is_none());
    assert_eq!(
        payload.provider_body_state,
        Some(UsageBodyCaptureState::Truncated)
    );
    let seed = aether_usage_runtime::build_stream_terminal_usage_payload_seed(&payload);
    let usage = seed.standardized_usage.unwrap();
    assert_eq!(usage.input_tokens, 11);
    assert_eq!(usage.output_tokens, 7);
    assert_eq!(usage.cache_read_tokens, 3);
    assert_eq!(
        seed.provider_actual_service_tier.as_deref(),
        Some("priority")
    );
}

#[tokio::test]
async fn direct_inline_idle_timeout_after_first_chunk_emits_terminal_read_timeout() {
    let message_start = Bytes::from_static(
        b"event: message_start\ndata: {\"type\":\"message_start\",\"message\":{}}\n\n",
    );
    let mut state = direct_anthropic_inline_state("req-inline-idle-timeout", Vec::new());
    state.stream_idle_timeout = Some(Duration::from_millis(5));
    state.upstream = Some(
        futures_util::stream::iter(vec![Ok(message_start.clone())])
            .chain(futures_util::stream::pending())
            .boxed(),
    );
    let (first, state) = state.next_item().await.expect("first chunk should stream");
    assert_eq!(first.expect("first chunk"), message_start);
    let (error, mut state) = tokio::time::timeout(Duration::from_secs(1), state.next_item())
        .await
        .expect("idle timeout should complete")
        .expect("terminal error should stream");
    let error = String::from_utf8(error.expect("terminal error should encode").to_vec()).unwrap();
    assert!(error.starts_with("event: error\n"));
    let failure = state
        .finalizer
        .as_ref()
        .unwrap()
        .terminal_failure()
        .unwrap();
    assert_eq!(failure.error_type, "read_timeout");
    assert_eq!(failure.status_code, 504);
    assert!(
        state.upstream.is_none(),
        "timeout should drop the upstream before settlement"
    );
    discard_direct_test_finalizer(&mut state);
}

#[tokio::test]
async fn direct_inline_idle_timeout_preserves_successful_protocol_completion() {
    for terminal in [
        "data: [DONE]\n\n",
        "event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\"}}\n\n",
    ] {
        let mut state = direct_anthropic_inline_state("req-inline-idle-completed", Vec::new());
        state.finalizer.as_mut().unwrap().core_mut().requires_anthropic_message_stop = false;
        state.stream_idle_timeout = Some(Duration::from_millis(5));
        state.upstream = Some(futures_util::stream::iter(vec![Ok(Bytes::from(terminal))])
            .chain(futures_util::stream::pending()).boxed());
        let (first, mut state) = state.next_item().await.expect("terminal chunk should stream");
        assert_eq!(first.unwrap(), Bytes::from(terminal));
        assert!(state.finalizer.as_ref().unwrap().core().client_stream_completion_tracker.successful_completion());
        let item = tokio::time::timeout(Duration::from_secs(1), state.next_upstream_item())
            .await.expect("teardown idle should finish");
        assert!(item.is_none());
        assert!(state.upstream.is_none());
        assert!(state.finalizer.as_ref().unwrap().terminal_failure().is_none(),
            "successful protocol terminal must not become a read timeout");
        discard_direct_test_finalizer(&mut state);
    }
}

#[test]
fn idle_timeout_completion_tracker_does_not_treat_failure_as_success() {
    for terminal in [
        "event: response.failed\ndata: {\"type\":\"response.failed\"}\n\n",
        "event: response.incomplete\ndata: {\"type\":\"response.incomplete\"}\n\n",
        "event: error\ndata: {\"type\":\"error\"}\n\n",
        "event: response.completed\ndata: {\"type\":\"response.failed\"}\n\n",
    ] {
        let mut tracker = ClientVisibleStreamCompletionTracker::default();
        assert!(tracker.observe_chunk(terminal.as_bytes()));
        assert!(!tracker.successful_completion());
    }
}

async fn execute_native_anthropic_prefetch_stream(
    request_id: &str,
    chunks: Vec<String>,
) -> AiAttemptExecutionOutcome<axum::http::Response<Body>> {
    execute_native_anthropic_prefetch_stream_with_terminal_error(request_id, chunks, None).await
}

async fn execute_native_anthropic_prefetch_stream_with_terminal_error(
    request_id: &str,
    chunks: Vec<String>,
    terminal_error: Option<String>,
) -> AiAttemptExecutionOutcome<axum::http::Response<Body>> {
    let plan = native_anthropic_stream_plan(request_id);
    let provider_catalog = provider_catalog_for_plan(&plan, None);
    let data_state = crate::data::GatewayDataState::with_provider_transport_reader_for_tests(
        Arc::new(provider_catalog),
        DEV_FERNET_KEY,
    );
    let state = AppState::new()
        .expect("app state should build")
        .with_data_state_for_tests(data_state);
    let frame_stream = stream! {
        yield Ok::<Bytes, std::io::Error>(ndjson_frame(StreamFrame {
            frame_type: StreamFrameType::Headers,
            payload: StreamFramePayload::Headers {
                status_code: 200,
                headers: BTreeMap::from([(
                    "content-type".to_string(),
                    "text/event-stream".to_string(),
                )]),
                response_observation: None,
            },
        }));
        for chunk in chunks {
            yield Ok::<Bytes, std::io::Error>(ndjson_frame(StreamFrame {
                frame_type: StreamFrameType::Data,
                payload: StreamFramePayload::Data {
                    chunk_b64: None,
                    text: Some(chunk),
                },
            }));
        }
        if let Some(error) = terminal_error {
            yield Err::<Bytes, std::io::Error>(std::io::Error::other(error));
        } else {
            yield Ok::<Bytes, std::io::Error>(ndjson_frame(StreamFrame::eof()));
        }
    }
    .boxed();

    let mut retry_scope = AiAttemptRetryScope::Candidate;
    let mut fallback_response = None;
    let response = execute_stream_from_frame_stream_with_retry_scope(
        &state,
        plan,
        &format!("trace-{request_id}"),
        &test_decision(),
        "claude_chat_stream",
        Some("claude_chat_stream_success".to_string()),
        Some(json!({
            "request_id": request_id,
            "candidate_id": format!("candidate-{request_id}"),
            "candidate_index": 0,
            "retry_index": 0,
            "provider_api_format": "claude:messages",
            "client_api_format": "claude:messages"
        })),
        crate::clock::current_unix_ms(),
        Instant::now(),
        RequestStageTrace::from_env(),
        true,
        frame_stream,
        false,
        None,
        Some(&mut retry_scope),
        Some(&mut fallback_response),
        None,
    )
    .await
    .expect("native Anthropic stream execution should succeed");
    match response {
        Some(response) => AiAttemptExecutionOutcome::Responded(response),
        None => AiAttemptExecutionOutcome::Retry {
            scope: retry_scope,
            fallback_response,
        },
    }
}

fn test_decision() -> GatewayControlDecision {
    GatewayControlDecision::synthetic(
        "/v1/chat/completions",
        Some("ai_public".to_string()),
        Some("openai".to_string()),
        Some("chat".to_string()),
        Some("openai:chat".to_string()),
    )
    .with_execution_runtime_candidate(true)
}

fn test_state() -> AppState {
    AppState::new().expect("gateway state should build")
}

#[tokio::test]
async fn agent_identity_stream_error_prefetch_is_bounded_and_replayed() {
    let upstream_body = format!(
        "{}{}",
        "x".repeat(crate::execution_runtime::MAX_ERROR_BODY_BYTES),
        "body-after-inspection-limit"
    );
    let expected_body = upstream_body.clone().into_bytes();
    let listener = crate::test_support::bind_loopback_listener()
        .await
        .expect("listener should bind");
    let addr = listener.local_addr().expect("address should resolve");
    let server = tokio::spawn(async move {
        let app = Router::new().route(
            "/responses",
            any(move || {
                let body = upstream_body.clone();
                async move { (StatusCode::UNAUTHORIZED, body) }
            }),
        );
        axum::serve(listener, app)
            .await
            .expect("server should start");
    });
    let plan = direct_stream_test_plan("bounded-agent-error", format!("http://{addr}/responses"));
    let mut execution = crate::execution_runtime::DirectSyncExecutionRuntime::new()
        .execute_stream(&plan)
        .await
        .expect("stream headers should execute");

    let inspected = prefetch_direct_stream_error_body(&mut execution)
        .await
        .expect("error body should be inspected");

    assert_eq!(
        inspected.len(),
        crate::execution_runtime::MAX_ERROR_BODY_BYTES
    );
    let replayed = collect_direct_execution_body(execution)
        .await
        .expect("prefetched response should replay");
    assert_eq!(replayed, expected_body);
    server.abort();
}

#[tokio::test]
async fn non_agent_stream_401_is_not_prefetched_and_body_passes_through() {
    let upstream_body = br#"{"error":{"code":"ordinary_unauthorized","message":"sign in"}}"#;
    let listener = crate::test_support::bind_loopback_listener()
        .await
        .expect("listener should bind");
    let addr = listener.local_addr().expect("address should resolve");
    let server = tokio::spawn(async move {
        let app = Router::new().route(
            "/responses",
            any(|| async {
                (
                    StatusCode::UNAUTHORIZED,
                    [(header::CONTENT_TYPE, "application/json")],
                    upstream_body.as_slice(),
                )
            }),
        );
        axum::serve(listener, app)
            .await
            .expect("server should start");
    });
    let mut plan = direct_stream_test_plan("non-agent-401", format!("http://{addr}/responses"));
    plan.provider_name = Some("openai".to_string());
    let repository = provider_catalog_for_stream_auth_plan(&plan, "openai", "api_key", None);
    let state = AppState::new()
        .expect("state should build")
        .with_data_state_for_tests(
            crate::data::GatewayDataState::with_provider_transport_reader_for_tests(
                Arc::new(repository),
                DEV_FERNET_KEY,
            ),
        );

    let execution =
        execute_in_process_stream_with_oauth_retry(&state, &mut plan, "trace-non-agent-401", None)
            .await
            .expect("stream request should execute");

    assert!(execution.prefetched_body.is_empty());
    let replayed = collect_direct_execution_body(execution)
        .await
        .expect("response body should pass through");
    assert_eq!(replayed, upstream_body);
    server.abort();
}

#[tokio::test]
async fn agent_identity_stream_non_task_401_replays_original_body_without_refresh() {
    let upstream_body = br#"{"error":{"code":"account_disabled","message":"account unavailable"}}"#;
    let listener = crate::test_support::bind_loopback_listener()
        .await
        .expect("listener should bind");
    let addr = listener.local_addr().expect("address should resolve");
    let task_registration_hits = Arc::new(AtomicUsize::new(0));
    let task_registration_hits_for_server = Arc::clone(&task_registration_hits);
    let server = tokio::spawn(async move {
        let app = Router::new()
            .route(
                "/responses",
                any(|| async {
                    (
                        StatusCode::UNAUTHORIZED,
                        [(header::CONTENT_TYPE, "application/json")],
                        upstream_body.as_slice(),
                    )
                }),
            )
            .route(
                "/api/accounts/v1/agent/runtime-test/task/register",
                any(move || {
                    let hits = Arc::clone(&task_registration_hits_for_server);
                    async move {
                        hits.fetch_add(1, Ordering::SeqCst);
                        Json(json!({"task_id": "unexpected-task"}))
                    }
                }),
            );
        axum::serve(listener, app)
            .await
            .expect("server should start");
    });
    let mut plan =
        direct_stream_test_plan("agent-non-task-401", format!("http://{addr}/responses"));
    let repository = Arc::new(provider_catalog_for_stream_auth_plan(
        &plan,
        "codex",
        "oauth",
        Some(agent_identity_test_auth_config("task-old")),
    ));
    let oauth_refresh =
        aether_provider_transport::LocalOAuthRefreshCoordinator::with_adapters_for_tests(vec![
            Arc::new(
                aether_provider_transport::CodexAgentIdentityRefreshAdapter::default()
                    .with_auth_api_base_url_for_tests(format!("http://{addr}/api/accounts")),
            ),
        ]);
    let state = AppState::new()
        .expect("state should build")
        .with_data_state_for_tests(
            crate::data::GatewayDataState::with_provider_catalog_repository_for_tests(repository)
                .with_encryption_key_for_tests(DEV_FERNET_KEY),
        )
        .with_oauth_refresh_coordinator_for_tests(oauth_refresh);

    let execution = execute_in_process_stream_with_oauth_retry(
        &state,
        &mut plan,
        "trace-agent-non-task-401",
        None,
    )
    .await
    .expect("stream request should execute");

    assert!(!execution.prefetched_body.is_empty());
    assert_eq!(task_registration_hits.load(Ordering::SeqCst), 0);
    let replayed = collect_direct_execution_body(execution)
        .await
        .expect("response body should replay");
    assert_eq!(replayed, upstream_body);
    server.abort();
}

#[tokio::test]
async fn agent_identity_stream_invalid_task_refreshes_and_retries_once() {
    let upstream_hits = Arc::new(AtomicUsize::new(0));
    let upstream_hits_for_server = Arc::clone(&upstream_hits);
    let task_registration_hits = Arc::new(AtomicUsize::new(0));
    let task_registration_hits_for_server = Arc::clone(&task_registration_hits);
    let observed_authorization = Arc::new(Mutex::new(Vec::<String>::new()));
    let observed_authorization_for_server = Arc::clone(&observed_authorization);
    let listener = crate::test_support::bind_loopback_listener()
        .await
        .expect("listener should bind");
    let addr = listener.local_addr().expect("address should resolve");
    let server = tokio::spawn(async move {
        let app = Router::new()
            .route(
                "/responses",
                any(move |request: Request| {
                    let hits = Arc::clone(&upstream_hits_for_server);
                    let authorizations = Arc::clone(&observed_authorization_for_server);
                    async move {
                        let authorization = request
                            .headers()
                            .get(header::AUTHORIZATION)
                            .and_then(|value| value.to_str().ok())
                            .unwrap_or_default()
                            .to_string();
                        authorizations
                            .lock()
                            .expect("authorization mutex should lock")
                            .push(authorization);
                        if hits.fetch_add(1, Ordering::SeqCst) == 0 {
                            (
                                StatusCode::UNAUTHORIZED,
                                Json(json!({
                                    "error": {
                                        "code": "invalid_task_id",
                                        "message": "registered task expired"
                                    }
                                })),
                            )
                                .into_response()
                        } else {
                            (StatusCode::OK, Json(json!({"ok": true}))).into_response()
                        }
                    }
                }),
            )
            .route(
                "/api/accounts/v1/agent/runtime-test/task/register",
                any(move || {
                    let hits = Arc::clone(&task_registration_hits_for_server);
                    async move {
                        hits.fetch_add(1, Ordering::SeqCst);
                        Json(json!({"task_id": "task-new"}))
                    }
                }),
            );
        axum::serve(listener, app)
            .await
            .expect("server should start");
    });
    let mut plan =
        direct_stream_test_plan("agent-invalid-task", format!("http://{addr}/responses"));
    let repository = Arc::new(provider_catalog_for_stream_auth_plan(
        &plan,
        "codex",
        "oauth",
        Some(agent_identity_test_auth_config("task-old")),
    ));
    let oauth_refresh =
        aether_provider_transport::LocalOAuthRefreshCoordinator::with_adapters_for_tests(vec![
            Arc::new(
                aether_provider_transport::CodexAgentIdentityRefreshAdapter::default()
                    .with_auth_api_base_url_for_tests(format!("http://{addr}/api/accounts")),
            ),
        ]);
    let state = AppState::new()
        .expect("state should build")
        .with_data_state_for_tests(
            crate::data::GatewayDataState::with_provider_catalog_repository_for_tests(repository)
                .with_encryption_key_for_tests(DEV_FERNET_KEY),
        )
        .with_oauth_refresh_coordinator_for_tests(oauth_refresh);
    let transport = state
        .read_provider_transport_snapshot(&plan.provider_id, &plan.endpoint_id, &plan.key_id)
        .await
        .expect("transport should load")
        .expect("transport should exist");
    let initial_authorization = match state
        .resolve_local_oauth_request_auth(&transport)
        .await
        .expect("initial Agent Identity auth should resolve")
        .expect("initial Agent Identity auth should exist")
    {
        aether_provider_transport::LocalResolvedOAuthRequestAuth::Header { name, value } => {
            assert_eq!(name, "authorization");
            value
        }
        aether_provider_transport::LocalResolvedOAuthRequestAuth::Kiro(_) => {
            panic!("Agent Identity should resolve to header auth")
        }
    };
    assert!(
        aether_provider_transport::codex_agent_identity_authorization_matches_transport(
            &transport,
            &initial_authorization,
        )
    );
    plan.headers
        .insert("authorization".to_string(), initial_authorization.clone());

    let execution = execute_in_process_stream_with_oauth_retry(
        &state,
        &mut plan,
        "trace-agent-invalid-task",
        None,
    )
    .await
    .expect("stream request should recover");

    assert_eq!(execution.status_code, 200);
    assert!(execution.prefetched_body.is_empty());
    assert_eq!(upstream_hits.load(Ordering::SeqCst), 2);
    assert_eq!(task_registration_hits.load(Ordering::SeqCst), 1);
    {
        let authorizations = observed_authorization
            .lock()
            .expect("authorization mutex should lock");
        assert_eq!(authorizations.len(), 2);
        assert_eq!(authorizations[0], initial_authorization);
        assert!(authorizations[1].starts_with("AgentAssertion "));
        assert_ne!(authorizations[1], authorizations[0]);
    }
    let replayed = collect_direct_execution_body(execution)
        .await
        .expect("retried response body should read");
    assert_eq!(
        serde_json::from_slice::<Value>(&replayed).expect("response should be JSON"),
        json!({"ok": true})
    );
    server.abort();
}

#[tokio::test]
async fn native_anthropic_embedded_auth_error_refreshes_oauth_and_retries_once() {
    let upstream_hits = Arc::new(AtomicUsize::new(0));
    let upstream_hits_for_server = Arc::clone(&upstream_hits);
    let refresh_hits = Arc::new(AtomicUsize::new(0));
    let refresh_hits_for_server = Arc::clone(&refresh_hits);
    let observed_authorization = Arc::new(Mutex::new(Vec::<String>::new()));
    let observed_authorization_for_server = Arc::clone(&observed_authorization);
    let listener = crate::test_support::bind_loopback_listener()
        .await
        .expect("listener should bind");
    let addr = listener.local_addr().expect("address should resolve");
    let server = tokio::spawn(async move {
        let app = Router::new()
            .route(
                "/v1/messages",
                any(move |request: Request| {
                    let hits = Arc::clone(&upstream_hits_for_server);
                    let authorizations = Arc::clone(&observed_authorization_for_server);
                    async move {
                        authorizations
                            .lock()
                            .expect("authorization mutex should lock")
                            .push(
                                request
                                    .headers()
                                    .get(header::AUTHORIZATION)
                                    .and_then(|value| value.to_str().ok())
                                    .unwrap_or_default()
                                    .to_string(),
                            );
                        let body = if hits.fetch_add(1, Ordering::SeqCst) == 0 {
                            concat!(
                                "event: error\n",
                                "data: {\"type\":\"error\",\"error\":{\"type\":\"authentication_error\",\"message\":\"expired token\"}}\n\n",
                            )
                        } else {
                            concat!(
                                "event: message_start\n",
                                "data: {\"type\":\"message_start\",\"message\":{}}\n\n",
                                "event: message_stop\n",
                                "data: {\"type\":\"message_stop\"}\n\n",
                            )
                        };
                        (
                            StatusCode::OK,
                            [(header::CONTENT_TYPE, "text/event-stream")],
                            body,
                        )
                            .into_response()
                    }
                }),
            )
            .route(
                "/oauth/token",
                any(move || {
                    let hits = Arc::clone(&refresh_hits_for_server);
                    async move {
                        hits.fetch_add(1, Ordering::SeqCst);
                        Json(json!({
                            "access_token": "fresh-access-token",
                            "refresh_token": "fresh-refresh-token",
                            "expires_in": 3600,
                            "token_type": "Bearer"
                        }))
                    }
                }),
            );
        axum::serve(listener, app)
            .await
            .expect("server should start");
    });

    let mut plan = native_anthropic_stream_plan("anthropic-embedded-oauth-refresh");
    plan.url = format!("http://{addr}/v1/messages");
    plan.provider_name = Some("claude_code".to_string());
    plan.headers.insert(
        "authorization".to_string(),
        "Bearer stale-access-token".to_string(),
    );
    let repository = Arc::new(provider_catalog_for_stream_auth_plan(
        &plan,
        "claude_code",
        "oauth",
        Some(generic_oauth_test_auth_config("claude_code")),
    ));
    let oauth_refresh =
        aether_provider_transport::LocalOAuthRefreshCoordinator::with_adapters_for_tests(vec![
            Arc::new(
                aether_provider_transport::GenericOAuthRefreshAdapter::default()
                    .with_token_url_for_tests("claude_code", format!("http://{addr}/oauth/token")),
            ),
        ]);
    let state = AppState::new()
        .expect("state should build")
        .with_data_state_for_tests(
            crate::data::GatewayDataState::with_provider_catalog_repository_for_tests(repository)
                .with_encryption_key_for_tests(DEV_FERNET_KEY),
        )
        .with_oauth_refresh_coordinator_for_tests(oauth_refresh);

    let execution = execute_in_process_stream_with_oauth_retry(
        &state,
        &mut plan,
        "trace-anthropic-embedded-oauth-refresh",
        None,
    )
    .await
    .expect("embedded authentication error should recover");
    let replayed = collect_direct_execution_body(execution)
        .await
        .expect("retried response should read");

    assert_eq!(upstream_hits.load(Ordering::SeqCst), 2);
    assert_eq!(refresh_hits.load(Ordering::SeqCst), 1);
    assert!(String::from_utf8_lossy(&replayed).contains("event: message_start"));
    assert_eq!(
        observed_authorization
            .lock()
            .expect("authorization mutex should lock")
            .as_slice(),
        [
            "Bearer stale-access-token".to_string(),
            "Bearer fresh-access-token".to_string(),
        ]
    );
    server.abort();
}

#[tokio::test]
async fn native_anthropic_http_permission_error_does_not_refresh_oauth() {
    let upstream_hits = Arc::new(AtomicUsize::new(0));
    let upstream_hits_for_server = Arc::clone(&upstream_hits);
    let refresh_hits = Arc::new(AtomicUsize::new(0));
    let refresh_hits_for_server = Arc::clone(&refresh_hits);
    let permission_body = concat!(
        "{\"type\":\"error\",\"error\":{",
        "\"type\":\"permission_error\",",
        "\"message\":\"this token is not authorized for the workspace\"}}",
    );
    let listener = crate::test_support::bind_loopback_listener()
        .await
        .expect("listener should bind");
    let addr = listener.local_addr().expect("address should resolve");
    let server = tokio::spawn(async move {
        let app = Router::new()
            .route(
                "/v1/messages",
                any(move || {
                    let hits = Arc::clone(&upstream_hits_for_server);
                    async move {
                        hits.fetch_add(1, Ordering::SeqCst);
                        (
                            StatusCode::FORBIDDEN,
                            [(header::CONTENT_TYPE, "application/json")],
                            permission_body,
                        )
                    }
                }),
            )
            .route(
                "/oauth/token",
                any(move || {
                    let hits = Arc::clone(&refresh_hits_for_server);
                    async move {
                        hits.fetch_add(1, Ordering::SeqCst);
                        Json(json!({
                            "access_token": "unexpected-access-token",
                            "refresh_token": "unexpected-refresh-token",
                            "expires_in": 3600,
                            "token_type": "Bearer"
                        }))
                    }
                }),
            );
        axum::serve(listener, app)
            .await
            .expect("server should start");
    });

    let mut plan = native_anthropic_stream_plan("anthropic-http-oauth-permission");
    plan.url = format!("http://{addr}/v1/messages");
    plan.provider_name = Some("claude_code".to_string());
    plan.headers.insert(
        "authorization".to_string(),
        "Bearer stale-access-token".to_string(),
    );
    let repository = Arc::new(provider_catalog_for_stream_auth_plan(
        &plan,
        "claude_code",
        "oauth",
        Some(generic_oauth_test_auth_config("claude_code")),
    ));
    let oauth_refresh =
        aether_provider_transport::LocalOAuthRefreshCoordinator::with_adapters_for_tests(vec![
            Arc::new(
                aether_provider_transport::GenericOAuthRefreshAdapter::default()
                    .with_token_url_for_tests("claude_code", format!("http://{addr}/oauth/token")),
            ),
        ]);
    let state = AppState::new()
        .expect("state should build")
        .with_data_state_for_tests(
            crate::data::GatewayDataState::with_provider_catalog_repository_for_tests(repository)
                .with_encryption_key_for_tests(DEV_FERNET_KEY),
        )
        .with_oauth_refresh_coordinator_for_tests(oauth_refresh);

    let execution = execute_in_process_stream_with_oauth_retry(
        &state,
        &mut plan,
        "trace-anthropic-http-oauth-permission",
        None,
    )
    .await
    .expect("permission response should remain available");
    assert_eq!(execution.status_code, StatusCode::FORBIDDEN.as_u16());
    let replayed = collect_direct_execution_body(execution)
        .await
        .expect("permission response should replay");

    assert_eq!(upstream_hits.load(Ordering::SeqCst), 1);
    assert_eq!(refresh_hits.load(Ordering::SeqCst), 0);
    assert_eq!(replayed, permission_body.as_bytes());
    server.abort();
}

#[test]
fn native_anthropic_oauth_prefetch_respects_short_first_byte_timeout() {
    let now = Instant::now();
    let precommit_started_at = now
        .checked_sub(Duration::from_millis(10))
        .expect("precommit start should be representable");
    let upstream_started_at = now
        .checked_sub(Duration::from_millis(90))
        .expect("upstream start should be representable");

    let first_byte_wait = select_direct_anthropic_prefetch_wait(
        precommit_started_at,
        Duration::from_millis(750),
        upstream_started_at,
        Some(Duration::from_millis(100)),
        false,
        now,
    );
    assert_eq!(first_byte_wait.remaining, Duration::from_millis(10));
    assert!(!first_byte_wait.commit_on_timeout);

    let precommit_wait = select_direct_anthropic_prefetch_wait(
        now.checked_sub(Duration::from_millis(750))
            .expect("precommit start should be representable"),
        Duration::from_millis(750),
        upstream_started_at,
        Some(Duration::from_secs(5)),
        false,
        now,
    );
    assert!(precommit_wait.remaining.is_zero());
    assert!(precommit_wait.commit_on_timeout);
}

#[tokio::test]
async fn native_anthropic_oauth_pending_events_do_not_start_a_second_precommit_wait() {
    let request_id = "anthropic-oauth-single-precommit";
    let plan = native_anthropic_stream_plan(request_id);
    let provider_catalog = provider_catalog_for_plan(&plan, None);
    let data_state = crate::data::GatewayDataState::with_provider_transport_reader_for_tests(
        Arc::new(provider_catalog),
        DEV_FERNET_KEY,
    );
    let state = AppState::new()
        .expect("app state should build")
        .with_data_state_for_tests(data_state);
    let frame_stream = stream! {
        yield Ok::<Bytes, std::io::Error>(ndjson_frame(StreamFrame {
            frame_type: StreamFrameType::Headers,
            payload: StreamFramePayload::Headers {
                status_code: 200,
                headers: BTreeMap::from([(
                    "content-type".to_string(),
                    "text/event-stream".to_string(),
                )]),
                response_observation: None,
            },
        }));
        yield Ok::<Bytes, std::io::Error>(ndjson_frame(StreamFrame {
            frame_type: StreamFrameType::Data,
            payload: StreamFramePayload::Data {
                chunk_b64: None,
                text: Some(": ping\n\n".to_string()),
            },
        }));
        yield Ok::<Bytes, std::io::Error>(ndjson_frame(StreamFrame {
            frame_type: StreamFrameType::Data,
            payload: StreamFramePayload::Data {
                chunk_b64: None,
                text: Some(
                    "event: future_event\ndata: {\"type\":\"future_event\",\"value\":1}\n\n"
                        .to_string(),
                ),
            },
        }));
        tokio::time::sleep(Duration::from_secs(2)).await;
        yield Ok::<Bytes, std::io::Error>(ndjson_frame(StreamFrame::eof()));
    }
    .boxed();
    let response = tokio::time::timeout(
        Duration::from_millis(300),
        execute_stream_from_frame_stream_with_retry_scope(
            &state,
            plan,
            "trace-anthropic-oauth-single-precommit",
            &test_decision(),
            "claude_chat_stream",
            Some("claude_chat_stream_success".to_string()),
            Some(json!({
                "request_id": request_id,
                "candidate_id": format!("candidate-{request_id}"),
                "candidate_index": 0,
                "retry_index": 0,
                "provider_api_format": "claude:messages",
                "client_api_format": "claude:messages"
            })),
            crate::clock::current_unix_ms(),
            Instant::now(),
            RequestStageTrace::from_env(),
            true,
            frame_stream,
            true,
            None,
            None,
            None,
            None,
        ),
    )
    .await
    .expect("the committed OAuth prefetch must not be followed by another 750 ms wait")
    .expect("frame stream execution should resolve");

    assert!(response.is_some());
}

#[tokio::test]
async fn native_anthropic_embedded_auth_error_does_not_refresh_api_key() {
    let upstream_hits = Arc::new(AtomicUsize::new(0));
    let upstream_hits_for_server = Arc::clone(&upstream_hits);
    let listener = crate::test_support::bind_loopback_listener()
        .await
        .expect("listener should bind");
    let addr = listener.local_addr().expect("address should resolve");
    let upstream_body = concat!(
        "event: error\n",
        "data: {\"type\":\"error\",\"error\":{\"type\":\"authentication_error\",\"message\":\"invalid key\"}}\n\n",
    );
    let server = tokio::spawn(async move {
        let app = Router::new().route(
            "/v1/messages",
            any(move || {
                let hits = Arc::clone(&upstream_hits_for_server);
                async move {
                    hits.fetch_add(1, Ordering::SeqCst);
                    (
                        StatusCode::OK,
                        [(header::CONTENT_TYPE, "text/event-stream")],
                        upstream_body,
                    )
                }
            }),
        );
        axum::serve(listener, app)
            .await
            .expect("server should start");
    });

    let mut plan = native_anthropic_stream_plan("anthropic-embedded-api-key");
    plan.url = format!("http://{addr}/v1/messages");
    plan.provider_name = Some("claude_code".to_string());
    plan.headers
        .insert("x-api-key".to_string(), "invalid-api-key".to_string());
    let repository = Arc::new(provider_catalog_for_stream_auth_plan(
        &plan,
        "claude_code",
        "api_key",
        None,
    ));
    let state = AppState::new()
        .expect("state should build")
        .with_data_state_for_tests(
            crate::data::GatewayDataState::with_provider_catalog_repository_for_tests(repository)
                .with_encryption_key_for_tests(DEV_FERNET_KEY),
        );

    let execution = execute_in_process_stream_with_oauth_retry(
        &state,
        &mut plan,
        "trace-anthropic-embedded-api-key",
        None,
    )
    .await
    .expect("API-key error response should remain available");
    let replayed = collect_direct_execution_body(execution)
        .await
        .expect("prefetched API-key error should replay");

    assert_eq!(upstream_hits.load(Ordering::SeqCst), 1);
    assert_eq!(replayed, upstream_body.as_bytes());
    server.abort();
}

struct BlockingStreamingRequestCandidateRepository {
    inner: InMemoryRequestCandidateRepository,
    block_streaming: AtomicBool,
    streaming_started: Notify,
    release_streaming: Notify,
}

impl Default for BlockingStreamingRequestCandidateRepository {
    fn default() -> Self {
        Self {
            inner: InMemoryRequestCandidateRepository::default(),
            block_streaming: AtomicBool::new(true),
            streaming_started: Notify::new(),
            release_streaming: Notify::new(),
        }
    }
}

#[async_trait]
impl RequestCandidateReadRepository for BlockingStreamingRequestCandidateRepository {
    async fn list_by_request_id(
        &self,
        request_id: &str,
    ) -> Result<Vec<StoredRequestCandidate>, DataLayerError> {
        self.inner.list_by_request_id(request_id).await
    }

    async fn list_recent(
        &self,
        limit: usize,
    ) -> Result<Vec<StoredRequestCandidate>, DataLayerError> {
        self.inner.list_recent(limit).await
    }

    async fn list_by_provider_id(
        &self,
        provider_id: &str,
        limit: usize,
    ) -> Result<Vec<StoredRequestCandidate>, DataLayerError> {
        self.inner.list_by_provider_id(provider_id, limit).await
    }

    async fn list_finalized_by_endpoint_ids_since(
        &self,
        endpoint_ids: &[String],
        since_unix_secs: u64,
        limit: usize,
    ) -> Result<Vec<StoredRequestCandidate>, DataLayerError> {
        self.inner
            .list_finalized_by_endpoint_ids_since(endpoint_ids, since_unix_secs, limit)
            .await
    }

    async fn count_finalized_statuses_by_endpoint_ids_since(
        &self,
        endpoint_ids: &[String],
        since_unix_secs: u64,
    ) -> Result<Vec<PublicHealthStatusCount>, DataLayerError> {
        self.inner
            .count_finalized_statuses_by_endpoint_ids_since(endpoint_ids, since_unix_secs)
            .await
    }

    async fn aggregate_finalized_timeline_by_endpoint_ids_since(
        &self,
        endpoint_ids: &[String],
        since_unix_secs: u64,
        until_unix_secs: u64,
        segments: u32,
    ) -> Result<Vec<PublicHealthTimelineBucket>, DataLayerError> {
        self.inner
            .aggregate_finalized_timeline_by_endpoint_ids_since(
                endpoint_ids,
                since_unix_secs,
                until_unix_secs,
                segments,
            )
            .await
    }
}

#[async_trait]
impl RequestCandidateWriteRepository for BlockingStreamingRequestCandidateRepository {
    async fn upsert(
        &self,
        candidate: UpsertRequestCandidateRecord,
    ) -> Result<StoredRequestCandidate, DataLayerError> {
        if candidate.status == RequestCandidateStatus::Streaming
            && self.block_streaming.swap(false, Ordering::AcqRel)
        {
            self.streaming_started.notify_one();
            self.release_streaming.notified().await;
        }
        self.inner.upsert(candidate).await
    }

    async fn delete_created_before(
        &self,
        created_before_unix_secs: u64,
        limit: usize,
    ) -> Result<usize, DataLayerError> {
        self.inner
            .delete_created_before(created_before_unix_secs, limit)
            .await
    }
}

fn stage_metric_count(stage: &str) -> u64 {
    crate::stage_metrics::gateway_stage_metric_samples()
        .into_iter()
        .find(|sample| {
            sample.name == "gateway_stage_latency_count"
                && sample
                    .labels
                    .iter()
                    .any(|label| label.key == "stage" && label.value == stage)
        })
        .map(|sample| sample.value)
        .unwrap_or_default()
}

#[derive(Clone)]
struct BlockingUsageAccess {
    policy_started: Arc<Notify>,
    release_policy: Arc<Notify>,
}

#[async_trait]
impl UsageRecordWriter for BlockingUsageAccess {
    async fn upsert_usage_record(
        &self,
        _record: UpsertUsageRecord,
    ) -> Result<Option<StoredRequestUsageAudit>, DataLayerError> {
        Ok(None)
    }
}

#[async_trait]
impl UsageSettlementWriter for BlockingUsageAccess {
    fn has_usage_settlement_writer(&self) -> bool {
        false
    }

    async fn settle_usage(
        &self,
        _input: UsageSettlementInput,
    ) -> Result<Option<StoredUsageSettlement>, DataLayerError> {
        Ok(None)
    }
}

#[async_trait]
impl UsageBillingEventEnricher for BlockingUsageAccess {
    async fn enrich_usage_event(&self, _event: &mut UsageEvent) -> Result<(), DataLayerError> {
        Ok(())
    }
}

#[async_trait]
impl UsageRuntimeAccess for BlockingUsageAccess {
    fn has_usage_writer(&self) -> bool {
        true
    }

    fn has_usage_worker_queue(&self) -> bool {
        false
    }

    fn usage_worker_queue(&self) -> Option<Arc<dyn aether_runtime_state::RuntimeQueueStore>> {
        None
    }

    fn supports_first_byte_usage_fast_path(&self) -> bool {
        false
    }

    async fn body_capture_policy(&self) -> Result<UsageBodyCapturePolicy, DataLayerError> {
        self.policy_started.notify_one();
        self.release_policy.notified().await;
        Ok(UsageBodyCapturePolicy::default())
    }
}

#[tokio::test]
async fn inline_first_chunk_does_not_wait_for_candidate_streaming_persistence() {
    let request_id = "req-inline-first-chunk-candidate-handoff";
    let request_candidate_repository =
        Arc::new(BlockingStreamingRequestCandidateRepository::default());
    let usage_repository = Arc::new(InMemoryUsageReadRepository::default());
    let state = AppState::new()
        .expect("app state should build")
        .with_data_state_for_tests(
            crate::data::GatewayDataState::with_request_candidate_and_usage_repository_for_tests(
                Arc::clone(&request_candidate_repository),
                Arc::clone(&usage_repository),
            ),
        )
        .with_usage_runtime_for_tests(UsageRuntimeConfig {
            enabled: true,
            ..UsageRuntimeConfig::default()
        });
    let plan = codex_cyber_policy_plan(request_id);
    let lifecycle_seed = aether_usage_runtime::build_lifecycle_usage_seed(&plan, None);
    let request_candidate_status_snapshot =
        crate::request_candidate_runtime::snapshot_local_request_candidate_status(&plan, None);
    let stream_started_at = Instant::now();
    let finalizer = DirectPassthroughFinalizer::new(DirectPassthroughFinalizerCore {
        state,
        trace_id: "trace-inline-first-chunk-candidate-handoff".to_string(),
        report_kind: None,
        report_context: None,
        lifecycle_seed,
        direct_stream_finalize_kind: None,
        stream_started_at,
        stage_trace: RequestStageTrace::from_env(),
        request_diagnostics: None,
        request_id_for_log: request_id.to_string(),
        candidate_id: plan.candidate_id.clone(),
        request_candidate_status_snapshot,
        deferred_request_candidate_status_record: None,
        candidate_started_unix_secs: crate::clock::current_unix_ms(),
        status_code: 200,
        headers: BTreeMap::new(),
        stream_usage_report_context: None,
        stream_usage_observer: None,
        stream_usage_observer_buffered: super::StreamUsageObservationBuffer::new(
            super::DEFAULT_USAGE_RESPONSE_BODY_CAPTURE_LIMIT_BYTES,
        ),
        provider_error_inspection: ProviderStreamErrorInspection::default(),
        max_stream_body_buffer_bytes: super::DEFAULT_USAGE_RESPONSE_BODY_CAPTURE_LIMIT_BYTES,
        provider_buffered_body: super::StreamBodyCapture::default(),
        buffered_body: super::StreamBodyCapture::default(),
        provider_body_truncated: false,
        client_body_truncated: false,
        client_stream_completion_tracker: ClientVisibleStreamCompletionTracker::default(),
        requires_anthropic_message_stop: false,
        client_visible_stream_completed: false,
        usage_stream_telemetry: Some(ExecutionTelemetry {
            ttfb_ms: Some(7),
            elapsed_ms: Some(7),
            upstream_bytes: Some(2),
        }),
        telemetry: None,
        provider_stream_bytes: 2,
        client_stream_bytes: 0,
        last_client_chunk_elapsed_ms: 0,
        pending_recorded: false,
        stream_started_recorded: false,
        terminal_failure: None,
        _provider_pool_in_flight_guard: None,
        _upstream_target_permit: None,
        plan,
    });
    let mut body_state = DirectPassthroughInlineBodyState {
        finalizer: Some(finalizer),
        upstream: None,
        upstream_control_filter: None,
        upstream_started_at: stream_started_at,
        stream_first_byte_timeout: None,
        stream_idle_timeout: None,
        observed_first_body_poll: true,
        observed_first_client_yield: false,
        upstream_done: false,
        control_filter_flushed: false,
        terminal_error_sent: false,
        finalized: false,
    };
    let first_yield_count = stage_metric_count("stream_first_client_yield");

    body_state.prepare_client_chunk_yield(&Bytes::from_static(b"hi"));

    assert!(body_state.observed_first_client_yield);
    assert!(
        stage_metric_count("stream_first_client_yield") > first_yield_count,
        "the first-yield metric must be recorded before candidate persistence completes"
    );
    tokio::time::timeout(
        Duration::from_secs(1),
        request_candidate_repository.streaming_started.notified(),
    )
    .await
    .expect("candidate streaming persistence should be handed off");
    assert!(
        request_candidate_repository
            .list_by_request_id(request_id)
            .await
            .expect("candidate read should succeed")
            .is_empty(),
        "the first chunk must return while candidate persistence is still blocked"
    );

    let live_usage = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if let Some(usage) = usage_repository
                .find_by_request_id(request_id)
                .await
                .expect("usage read should succeed")
            {
                if usage.status == "streaming" {
                    break usage;
                }
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("ordered usage lifecycle should reach streaming independently");
    assert_eq!(live_usage.billing_status, "pending");
    assert_eq!(live_usage.first_byte_time_ms, Some(7));

    request_candidate_repository.release_streaming.notify_one();
    let candidate = tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if let Some(candidate) = request_candidate_repository
                .list_by_request_id(request_id)
                .await
                .expect("candidate read should succeed")
                .into_iter()
                .next()
            {
                break candidate;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("candidate handoff should finish after release");
    assert_eq!(candidate.status, RequestCandidateStatus::Streaming);

    if let Some(mut finalizer) = body_state.finalizer.take() {
        finalizer.core.take();
    }
}

#[tokio::test]
async fn frame_stream_records_deferred_pending_before_waiting_for_headers() {
    let request_id = "req-frame-stream-deferred-pending";
    let usage_repository = Arc::new(InMemoryUsageReadRepository::default());
    let state = AppState::new()
        .expect("app state should build")
        .with_data_state_for_tests(
            crate::data::GatewayDataState::with_usage_repository_for_tests(Arc::clone(
                &usage_repository,
            )),
        )
        .with_usage_runtime_for_tests(UsageRuntimeConfig {
            enabled: true,
            ..UsageRuntimeConfig::default()
        });
    let plan = codex_cyber_policy_plan(request_id);
    let release_headers = Arc::new(Notify::new());
    let release_headers_for_stream = Arc::clone(&release_headers);
    let frame_stream = stream! {
        release_headers_for_stream.notified().await;
        yield Ok::<Bytes, std::io::Error>(ndjson_frame(StreamFrame {
            frame_type: StreamFrameType::Headers,
            payload: StreamFramePayload::Headers {
                status_code: 200,
                headers: BTreeMap::from([(
                    "content-type".to_string(),
                    "text/event-stream".to_string(),
                )]),
                response_observation: None,
            },
        }));
    }
    .boxed();
    let state_for_execution = state.clone();
    let execution = tokio::spawn(async move {
        execute_stream_from_frame_stream(
            &state_for_execution,
            plan,
            "trace-frame-stream-deferred-pending",
            &test_decision(),
            "openai_responses_stream",
            None,
            Some(json!({
                "provider_api_format": "openai:responses",
                "client_api_format": "openai:responses",
            })),
            crate::clock::current_unix_ms(),
            Instant::now(),
            RequestStageTrace::from_env(),
            false,
            frame_stream,
            None,
        )
        .await
    });

    let pending = tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if let Some(usage) = usage_repository
                .find_by_request_id(request_id)
                .await
                .expect("usage should read")
            {
                break usage;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("deferred pending usage should be recorded before headers");

    assert_eq!(pending.status, "pending");
    assert_eq!(pending.billing_status, "pending");
    assert!(
        !execution.is_finished(),
        "frame execution should still be waiting for upstream headers"
    );

    execution.abort();
    let _ = execution.await;
}

#[tokio::test]
async fn sync_terminal_handoff_survives_cancellation_during_admission_backpressure() {
    let blocker_request_id = "req-sync-terminal-admission-blocker";
    let target_request_id = "req-sync-terminal-handoff-cancelled";
    let usage_repository = Arc::new(InMemoryUsageReadRepository::default());
    let state = AppState::new()
        .expect("app state should build")
        .with_data_state_for_tests(
            crate::data::GatewayDataState::with_usage_repository_for_tests(Arc::clone(
                &usage_repository,
            )),
        )
        .with_usage_runtime_for_tests(UsageRuntimeConfig {
            enabled: true,
            terminal_submission_max_in_flight: 1,
            ..UsageRuntimeConfig::default()
        });
    let policy_started = Arc::new(Notify::new());
    let release_policy = Arc::new(Notify::new());
    let blocker = BlockingUsageAccess {
        policy_started: Arc::clone(&policy_started),
        release_policy: Arc::clone(&release_policy),
    };

    state
        .usage_runtime
        .submit_terminal_event(
            &blocker,
            UsageEvent::new(
                UsageEventType::Completed,
                blocker_request_id,
                UsageEventData {
                    provider_name: "test".to_string(),
                    model: "test-model".to_string(),
                    status_code: Some(200),
                    ..UsageEventData::default()
                },
            ),
        )
        .await;
    tokio::time::timeout(Duration::from_secs(1), policy_started.notified())
        .await
        .expect("first terminal submission should be blocked in body policy");

    let plan = codex_cyber_policy_plan(target_request_id);
    let payload = build_stream_sync_payload(
        "trace-sync-terminal-handoff-cancelled",
        "openai_responses_stream".to_string(),
        Some(json!({
            "request_id": target_request_id,
            "provider_api_format": "openai:responses",
            "client_api_format": "openai:responses"
        })),
        500,
        BTreeMap::new(),
        Some(json!({"error": "synthetic terminal failure"})),
        None,
        None,
    );
    let state_for_handoff = state.clone();
    let child_started = Arc::new(Notify::new());
    let release_child = Arc::new(Notify::new());
    let child_started_for_handoff = Arc::clone(&child_started);
    let release_child_for_handoff = Arc::clone(&release_child);
    let handoff = tokio::spawn(async move {
        record_sync_terminal_usage_with_handoff_after_spawn(
            &state_for_handoff,
            &plan,
            payload.report_context.as_ref(),
            &payload,
            async move {
                child_started_for_handoff.notify_one();
                release_child_for_handoff.notified().await;
            },
        )
        .await;
    });
    tokio::time::timeout(Duration::from_secs(1), child_started.notified())
        .await
        .expect("detached terminal child should start before cancellation");

    handoff.abort();
    if let Err(err) = handoff.await {
        assert!(err.is_cancelled(), "terminal handoff task should not panic");
    }
    release_child.notify_one();
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if state
                .usage_runtime
                .metrics_snapshot()
                .terminal_submission_pending
                >= 2
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("second terminal submission should reach the ordered backlog");

    release_policy.notify_one();
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if state
                .usage_runtime
                .metrics_snapshot()
                .terminal_submission_pending
                == 0
                && state
                    .usage_runtime
                    .metrics_snapshot()
                    .lifecycle_submission_pending
                    == 0
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("detached terminal handoff should release admission");

    let record = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if let Some(record) = usage_repository
                .find_by_request_id(target_request_id)
                .await
                .expect("usage repository read should succeed")
            {
                break record;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("detached terminal handoff should persist the target row");
    assert_eq!(record.status, "failed");
    assert_eq!(record.billing_status, "void");
}

#[test]
fn detects_client_visible_sse_terminal_events() {
    assert!(stream_chunk_contains_sse_done(b"data: [DONE]\n\n"));
    assert!(stream_chunk_contains_sse_done(
        b"event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n"
    ));
    assert!(stream_chunk_contains_sse_done(
        b"event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{}}\n\n"
    ));
    assert!(stream_chunk_contains_sse_done(
        b"event: response.failed\ndata: {\"type\":\"response.failed\",\"response\":{\"status\":\"failed\"}}\n\n"
    ));
    assert!(!stream_chunk_contains_sse_done(
        b"event: content_block_delta\ndata: {\"type\":\"content_block_delta\"}\n\n"
    ));
}

#[test]
fn detects_client_visible_sse_terminal_events_across_chunks() {
    let mut tracker = ClientVisibleStreamCompletionTracker::default();
    assert!(!tracker.observe_chunk(b"data: [DO"));
    assert!(!tracker.observe_chunk(b"NE]\n"));
    assert!(tracker.observe_chunk(b"\n"));

    let mut tracker = ClientVisibleStreamCompletionTracker::default();
    assert!(!tracker.observe_chunk(b"event: response.comp"));
    assert!(!tracker.observe_chunk(b"leted\r\n"));
    assert!(
        tracker.observe_chunk(b"data: {\"type\":\"response.completed\",\"response\":{}}\r\n\r\n")
    );
}

#[test]
fn client_visible_terminal_tracker_reports_the_exact_record_boundary() {
    let message_stop = b"event: message_stop\r\ndata: {\"type\":\"message_stop\"}\r\n\r\n";
    let trailing_error =
        b"event: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"api_error\"}}\n\n";
    let chunk = [message_stop.as_slice(), trailing_error.as_slice()].concat();
    let mut tracker = ClientVisibleStreamCompletionTracker::default();

    assert_eq!(
        tracker.observe_chunk_terminal_end(&chunk),
        Some(message_stop.len())
    );
    assert!(tracker.completed);
}

#[test]
fn anthropic_terminal_tracker_ignores_non_message_stop_terminals() {
    let mut tracker = ClientVisibleStreamCompletionTracker::default();

    assert!(!tracker.observe_anthropic_message_stop(b"data: [DONE]\n\n"));
    assert!(!tracker.observe_anthropic_message_stop(
        b"event: response.completed\ndata: {\"type\":\"response.completed\"}\n\n"
    ));
    assert!(tracker.observe_anthropic_message_stop(
        b"event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n"
    ));
}

#[test]
fn terminal_tracker_caps_multiline_record_and_resumes_after_boundary() {
    let line = b"data: short-payload\r\n";
    let repeated = super::SSE_TERMINAL_DETECTOR_MAX_RECORD_BYTES / line.len() + 2;
    let oversized_record = line.repeat(repeated);
    let mut tracker = ClientVisibleStreamCompletionTracker::default();

    assert!(!tracker.observe_anthropic_message_stop(&oversized_record));
    assert!(tracker.dropping_oversized_record);
    assert!(tracker.line_buffer.is_empty());
    assert!(tracker.data_payload.is_empty());

    assert!(!tracker.observe_anthropic_message_stop(b"\r\n"));
    assert!(!tracker.dropping_oversized_record);
    assert!(tracker.observe_anthropic_message_stop(
        b"event: message_stop\r\ndata: {\"type\":\"message_stop\"}\r\n\r\n"
    ));
}

#[test]
fn stream_capture_hard_caps_a_single_oversized_chunk() {
    let mut buffer = vec![1, 2];
    let mut truncated = false;

    super::append_stream_capture_bytes(&mut buffer, &[3, 4, 5, 6], 4, &mut truncated);

    assert_eq!(buffer, vec![1, 2, 3, 4]);
    assert!(truncated);
}

#[test]
fn stream_capture_encoding_defensively_caps_an_oversized_slice() {
    let (body, state) = super::build_stream_body_capture_with_limit(b"abcdef", false, 3);
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(body.expect("bounded capture should be encoded"))
        .expect("capture should be valid base64");

    assert_eq!(decoded, b"abc");
    assert_eq!(state, Some(UsageBodyCaptureState::Truncated));
}

#[test]
fn execution_stream_data_chunk_decode_is_bounded_before_allocation() {
    assert_eq!(
        super::decode_stream_data_chunk_with_limit(Some("YWJj"), None, 3)
            .expect("three decoded bytes"),
        b"abc"
    );
    assert!(super::decode_stream_data_chunk_with_limit(Some("YWJjZA=="), None, 3).is_err());
    assert!(super::decode_stream_data_chunk_with_limit(None, Some("abcd"), 3).is_err());
}

#[test]
fn stream_capture_policy_hard_caps_full_and_basic_analysis_buffers() {
    let oversized_chunk = vec![b'x'; super::BASIC_STREAM_BODY_ANALYSIS_LIMIT_BYTES + 1];

    let full_limit =
        super::stream_body_buffer_limit_for_record_level(UsageRequestRecordLevel::Full);
    assert_eq!(
        full_limit,
        crate::execution_runtime::MAX_STREAM_BODY_CAPTURE_BYTES
    );
    let mut full_buffer = Vec::new();
    let mut full_truncated = false;
    super::append_stream_capture_bytes(
        &mut full_buffer,
        &oversized_chunk,
        full_limit,
        &mut full_truncated,
    );
    assert_eq!(full_buffer, oversized_chunk);
    assert!(!full_truncated);
    let (full_body, full_state) = super::build_stream_body_capture(&full_buffer, full_truncated);
    assert!(full_body.is_some());
    assert_eq!(full_state, Some(UsageBodyCaptureState::Inline));
    drop(full_body);

    let basic_limit =
        super::stream_body_buffer_limit_for_record_level(UsageRequestRecordLevel::Basic);
    assert_eq!(basic_limit, super::BASIC_STREAM_BODY_ANALYSIS_LIMIT_BYTES);
    let mut basic_buffer = Vec::new();
    let mut basic_truncated = false;
    super::append_stream_capture_bytes(
        &mut basic_buffer,
        &oversized_chunk,
        basic_limit,
        &mut basic_truncated,
    );
    assert_eq!(basic_buffer.len(), basic_limit);
    assert!(basic_truncated);
    let (basic_body, basic_state) =
        super::build_stream_body_capture(&basic_buffer, basic_truncated);
    assert!(basic_body.is_some());
    assert_eq!(basic_state, Some(UsageBodyCaptureState::Truncated));

    let mut event = UsageEvent::new(
        UsageEventType::Completed,
        "req-basic-stream-capture",
        UsageEventData {
            provider_name: "provider".to_string(),
            model: "model".to_string(),
            response_body: basic_body.map(Value::String),
            response_body_state: basic_state,
            client_response_body: Some(json!("captured client body")),
            client_response_body_state: Some(UsageBodyCaptureState::Truncated),
            ..UsageEventData::default()
        },
    );
    apply_usage_body_capture_policy_to_event(
        UsageBodyCapturePolicy {
            record_level: UsageRequestRecordLevel::Basic,
        },
        &mut event,
    );

    assert_eq!(event.data.response_body, None);
    assert_eq!(
        event.data.response_body_state,
        Some(UsageBodyCaptureState::Disabled)
    );
    assert_eq!(event.data.client_response_body, None);
    assert_eq!(
        event.data.client_response_body_state,
        Some(UsageBodyCaptureState::Disabled)
    );
}

#[test]
fn provider_error_inspection_detects_response_failed_at_every_chunk_boundary() {
    let body = concat!(
        "event: response.created\n",
        "data: {\"type\":\"response.created\",\"response\":{\"status\":\"in_progress\"}}\n\n",
        "event: response.failed\n",
        "data: {\"type\":\"response.failed\",\"response\":{\"status\":\"failed\",\"error\":{\"type\":\"invalid_request\",\"message\":\"cyber policy rejected the request\",\"code\":\"cyber_policy_violation\",\"param\":\"input\"}}}\n\n",
    )
    .as_bytes();

    for split in 1..body.len() {
        let mut inspection = ProviderStreamErrorInspection::default();
        let detected = inspection
            .observe(None, &body[..split])
            .or_else(|| inspection.observe(None, &body[split..]))
            .unwrap_or_else(|| panic!("response.failed was missed at byte split {split}"));

        assert_eq!(
            detected.pointer("/error/code"),
            Some(&json!("cyber_policy_violation")),
            "string provider code changed at byte split {split}"
        );
        assert_eq!(
            detected.pointer("/error/param"),
            Some(&json!("input")),
            "provider error fields changed at byte split {split}"
        );
    }

    let mut inspection = ProviderStreamErrorInspection::default();
    let mut detected = None;
    for byte in body.chunks(1) {
        if let Some(error_body) = inspection.observe(None, byte) {
            detected = Some(error_body);
            break;
        }
    }
    let detected = detected.expect("byte-wise response.failed stream should be detected");
    assert_eq!(
        detected.pointer("/error/code"),
        Some(&json!("cyber_policy_violation"))
    );
    assert_eq!(detected.pointer("/error/param"), Some(&json!("input")));
}

#[test]
fn provider_error_inspection_bounds_oversized_chunks_and_keeps_boundary_detection() {
    let error_event = concat!(
        "event: response.failed\n",
        "data: {\"type\":\"response.failed\",\"response\":{\"status\":\"failed\",\"error\":{\"code\":\"cyber_policy_violation\"}}}\n\n",
    )
    .as_bytes();

    let mut prefix_chunk = vec![b'x'; PROVIDER_STREAM_ERROR_INSPECTION_MAX_BYTES + 64];
    prefix_chunk[..error_event.len()].copy_from_slice(error_event);
    let mut inspection = ProviderStreamErrorInspection::default();
    let detected = inspection
        .observe(None, &prefix_chunk)
        .expect("error at the bounded chunk prefix should be detected");
    assert_eq!(
        detected.pointer("/error/code"),
        Some(&json!("cyber_policy_violation"))
    );

    let mut suffix_chunk = vec![b'x'; PROVIDER_STREAM_ERROR_INSPECTION_MAX_BYTES + 64];
    let suffix_start = suffix_chunk.len() - error_event.len();
    suffix_chunk[suffix_start..].copy_from_slice(error_event);
    let mut inspection = ProviderStreamErrorInspection::default();
    let detected = inspection
        .observe(None, &suffix_chunk)
        .expect("error at the bounded chunk suffix should be detected");
    assert_eq!(
        detected.pointer("/error/code"),
        Some(&json!("cyber_policy_violation"))
    );

    // The JSON payload is split across chunks. The previous rolling tail
    // must still be combined with the prefix of the oversized chunk.
    let split = b"event: response.failed\ndata: {".len();
    let mut inspection = ProviderStreamErrorInspection::default();
    assert!(inspection.observe(None, &error_event[..split]).is_none());
    let mut continuation = vec![b'x'; PROVIDER_STREAM_ERROR_INSPECTION_MAX_BYTES + 64];
    let continuation_len = error_event.len() - split;
    continuation[..continuation_len].copy_from_slice(&error_event[split..]);
    let detected = inspection
        .observe(None, &continuation)
        .expect("error split across an oversized chunk boundary should be detected");
    assert_eq!(
        detected.pointer("/error/code"),
        Some(&json!("cyber_policy_violation"))
    );
}

#[tokio::test]
async fn prefetched_codex_cyber_policy_violation_stops_failover_by_default() {
    let response = execute_prefetched_codex_cyber_policy_failure(false)
        .await
        .expect("default Codex cyber policy handling should return the provider error");

    assert_eq!(response.status(), axum::http::StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn prefetched_codex_cyber_policy_violation_retries_when_routing_strategy_is_enabled() {
    assert!(
        execute_prefetched_codex_cyber_policy_failure(true)
            .await
            .is_none(),
        "enabling cyber failover should retry the next candidate"
    );
}

#[tokio::test]
async fn prefetched_transport_failure_retries_by_default() {
    assert!(matches!(
        execute_prefetched_transport_failure(false).await,
        AiAttemptExecutionOutcome::Retry {
            scope: AiAttemptRetryScope::Candidate,
            fallback_response: None,
        }
    ));
}

#[tokio::test]
async fn prefetched_transport_failure_can_stop_without_matching_http_status_rules() {
    let AiAttemptExecutionOutcome::Responded(response) =
        execute_prefetched_transport_failure(true).await
    else {
        panic!("transport stop policy should return a local response");
    };
    assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
}

#[tokio::test]
async fn prefetched_http_error_frame_honors_continue_status_codes() {
    assert!(matches!(
        execute_prefetched_http_status_failure(true).await,
        AiAttemptExecutionOutcome::Retry { .. }
    ));
}

#[tokio::test]
async fn prefetched_http_error_frame_honors_stop_status_codes() {
    let AiAttemptExecutionOutcome::Responded(response) =
        execute_prefetched_http_status_failure(false).await
    else {
        panic!("HTTP stop policy should return the upstream error");
    };
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
}

#[tokio::test]
async fn malformed_antigravity_function_call_streams_thought_then_fails_in_band() {
    let request_id = "req-antigravity-malformed-function-call";
    let plan = antigravity_gemini_stream_plan(request_id);
    let provider_catalog = provider_catalog_for_plan(
        &plan,
        Some(json!({
            "failover_rules": {
                "continue_status_codes": [502]
            }
        })),
    );
    let data_state = crate::data::GatewayDataState::with_provider_transport_reader_for_tests(
        Arc::new(provider_catalog),
        "development-key",
    );
    let state = AppState::new()
        .expect("app state should build")
        .with_data_state_for_tests(data_state);
    let frame_stream = stream! {
        yield Ok::<Bytes, std::io::Error>(ndjson_frame(StreamFrame {
            frame_type: StreamFrameType::Headers,
            payload: StreamFramePayload::Headers {
                status_code: 200,
                headers: BTreeMap::from([(
                    "content-type".to_string(),
                    "text/event-stream".to_string(),
                )]),
                response_observation: None,
            },
        }));
        for chunk in [
            r#"data: {"response":{"candidates":[{"content":{"role":"model","parts":[{"thought":true,"text":"Validating the document."}]} }],"modelVersion":"gemini-3.7-flash-tiered"}}

"#,
            r#"data: {"response":{"candidates":[{"content":{"role":"model","parts":[{"thoughtSignature":"signature","text":""}]},"finishReason":"MALFORMED_FUNCTION_CALL","finishMessage":"Malformed function call: Function call is empty - no input to parse."}],"modelVersion":"gemini-3.7-flash-tiered"}}

"#,
        ] {
            yield Ok::<Bytes, std::io::Error>(ndjson_frame(StreamFrame {
                frame_type: StreamFrameType::Data,
                payload: StreamFramePayload::Data {
                    chunk_b64: None,
                    text: Some(chunk.to_string()),
                },
            }));
        }
        yield Ok::<Bytes, std::io::Error>(ndjson_frame(StreamFrame::eof()));
    }
    .boxed();
    let mut retry_scope = AiAttemptRetryScope::Provider;

    let response = execute_stream_from_frame_stream_with_retry_scope(
        &state,
        plan,
        "trace-antigravity-malformed-function-call",
        &test_decision(),
        OPENAI_RESPONSES_STREAM_PLAN_KIND,
        Some("openai_responses_stream_success".to_string()),
        Some(json!({
            "request_id": request_id,
            "candidate_id": format!("candidate-{request_id}"),
            "candidate_index": 0,
            "retry_index": 0,
            "provider_api_format": "gemini:generate_content",
            "client_api_format": "openai:responses",
            "needs_conversion": true
        })),
        crate::clock::current_unix_ms(),
        Instant::now(),
        RequestStageTrace::from_env(),
        true,
        frame_stream,
        false,
        None,
        Some(&mut retry_scope),
        None,
        None,
    )
    .await
    .expect("malformed Antigravity stream should return a client stream")
    .expect("the first reasoning delta should commit the selected candidate");

    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("response body should read");
    let body = String::from_utf8(body.to_vec()).expect("response body should be utf8");
    assert!(
        body.contains("event: response.reasoning_text.delta\n"),
        "{body}"
    );
    assert!(
        !body.contains("event: response.reasoning_summary_text.delta\n"),
        "{body}"
    );
    assert!(
        body.contains("\"delta\":\"Validating the document.\""),
        "{body}"
    );
    assert!(body.contains("event: response.failed\n"), "{body}");
    assert!(
        body.contains("\"code\":\"MALFORMED_FUNCTION_CALL\""),
        "{body}"
    );
    assert!(
        body.contains(
            "\"message\":\"Malformed function call: Function call is empty - no input to parse.\""
        ),
        "{body}"
    );
    assert!(!body.contains("unsupported_finish_reason"), "{body}");
    assert_eq!(retry_scope, AiAttemptRetryScope::Provider);
}

fn connect_json_frame(flags: u8, payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(5 + payload.len());
    out.push(flags);
    out.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    out.extend_from_slice(payload);
    out
}

fn ndjson_frame(frame: StreamFrame) -> Bytes {
    let mut bytes = serde_json::to_vec(&frame).expect("stream frame should serialize");
    bytes.push(b'\n');
    Bytes::from(bytes)
}

#[test]
fn execution_stream_frame_codec_has_a_bounded_line_length() {
    assert_eq!(
        execution_stream_frame_codec().max_length(),
        crate::execution_runtime::MAX_EXECUTION_STREAM_FRAME_LINE_BYTES
    );
}

#[test]
fn post_stop_reader_yields_after_bounded_empty_chunks() {
    let polls = Arc::new(AtomicUsize::new(0));
    let polls_for_stream = Arc::clone(&polls);
    let stream = futures_util::stream::poll_fn(move |_| {
        let poll = polls_for_stream.fetch_add(1, Ordering::SeqCst);
        if poll < POST_STOP_MAX_EMPTY_CHUNKS_PER_POLL * 2 {
            std::task::Poll::Ready(Some(Ok::<Bytes, std::io::Error>(Bytes::new())))
        } else {
            std::task::Poll::Ready(Some(Ok::<Bytes, std::io::Error>(Bytes::from_static(b"x"))))
        }
    });
    let mut reader = Box::pin(PostStopLimitedStreamReader::new(
        stream,
        PostStopFrameReadBudget::new(),
    ));
    let waker = futures_util::task::noop_waker();
    let mut context = std::task::Context::from_waker(&waker);
    let mut storage = [0u8; 1];
    let mut read_buf = tokio::io::ReadBuf::new(&mut storage);

    let result = tokio::io::AsyncRead::poll_read(reader.as_mut(), &mut context, &mut read_buf);

    assert!(result.is_pending());
    assert_eq!(
        polls.load(Ordering::SeqCst),
        POST_STOP_MAX_EMPTY_CHUNKS_PER_POLL
    );
}

#[tokio::test]
async fn post_stop_activation_trims_prefetched_current_item_immediately() {
    const GIANT_TAIL_BYTES: usize = 4 * 1024 * 1024;

    let mut combined = ndjson_frame(StreamFrame {
        frame_type: StreamFrameType::Data,
        payload: StreamFramePayload::Data {
            chunk_b64: None,
            text: Some("event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n".to_string()),
        },
    })
    .to_vec();
    combined.resize(combined.len() + GIANT_TAIL_BYTES, b'x');
    let frame_stream =
        futures_util::stream::iter([Ok::<Bytes, std::io::Error>(Bytes::from(combined))]);
    let reader = PostStopLimitedStreamReader::new(frame_stream, PostStopFrameReadBudget::new());
    let mut lines = tokio_util::codec::FramedRead::new(reader, execution_stream_frame_codec());

    super::read_next_frame(&mut lines)
        .await
        .expect("frame should decode")
        .expect("data frame should exist");
    assert!(lines
        .get_ref()
        .current
        .as_ref()
        .is_some_and(|current| current.len() > ANTHROPIC_POST_STOP_DRAIN_MAX_BYTES));

    let already_buffered = lines.read_buffer().len();
    lines.read_buffer_mut().reserve(GIANT_TAIL_BYTES);
    assert!(lines.read_buffer().capacity() >= GIANT_TAIL_BYTES);
    assert!(!activate_post_stop_frame_read_budget(&mut lines));
    let retained = lines
        .get_ref()
        .current
        .as_ref()
        .map(Bytes::len)
        .unwrap_or_default();
    assert_eq!(
        retained,
        ANTHROPIC_POST_STOP_DRAIN_MAX_BYTES.saturating_sub(already_buffered)
    );
    assert_eq!(lines.read_buffer().len(), already_buffered);
    assert!(lines.read_buffer().capacity() <= ANTHROPIC_POST_STOP_DRAIN_MAX_BYTES);
}

#[test]
fn post_stop_activation_releases_over_limit_framed_buffer() {
    let frame_stream = futures_util::stream::empty::<Result<Bytes, std::io::Error>>();
    let reader = PostStopLimitedStreamReader::new(frame_stream, PostStopFrameReadBudget::new());
    let mut lines = tokio_util::codec::FramedRead::new(reader, execution_stream_frame_codec());
    lines
        .read_buffer_mut()
        .resize(ANTHROPIC_POST_STOP_DRAIN_MAX_BYTES + 1, b'x');

    assert!(activate_post_stop_frame_read_budget(&mut lines));
    assert!(lines.read_buffer().is_empty());
    assert_eq!(lines.read_buffer().capacity(), 0);
}

#[test]
fn merge_stream_terminal_summary_prefers_more_complete_observed_usage() {
    let mut runtime_usage = StandardizedUsage::new();
    runtime_usage.output_tokens = 137;
    let mut observed_usage = StandardizedUsage::new();
    observed_usage.input_tokens = 26;
    observed_usage.output_tokens = 137;

    let merged = merge_stream_terminal_summary(
        Some(ExecutionStreamTerminalSummary {
            standardized_usage: Some(runtime_usage),
            model: Some("gpt-5.5".to_string()),
            provider_actual_service_tier: Some("priority".to_string()),
            unknown_event_count: 1,
            ..ExecutionStreamTerminalSummary::default()
        }),
        Some(ExecutionStreamTerminalSummary {
            standardized_usage: Some(observed_usage),
            response_id: Some("resp_123".to_string()),
            provider_actual_service_tier: Some("default".to_string()),
            observed_finish: true,
            unknown_event_count: 2,
            ..ExecutionStreamTerminalSummary::default()
        }),
    )
    .expect("summary should merge");
    let usage = merged
        .standardized_usage
        .expect("merged usage should exist");

    assert_eq!(usage.input_tokens, 26);
    assert_eq!(usage.output_tokens, 137);
    assert_eq!(merged.model.as_deref(), Some("gpt-5.5"));
    assert_eq!(merged.response_id.as_deref(), Some("resp_123"));
    assert_eq!(
        merged.provider_actual_service_tier.as_deref(),
        Some("default")
    );
    assert!(merged.observed_finish);
    assert_eq!(merged.unknown_event_count, 3);
}

#[test]
fn detects_missing_observed_finish_only_without_usage_signal() {
    assert!(stream_terminal_summary_missing_observed_finish(Some(
        &ExecutionStreamTerminalSummary {
            response_id: Some("resp_missing_finish".to_string()),
            model: Some("gpt-5.5".to_string()),
            observed_finish: false,
            ..ExecutionStreamTerminalSummary::default()
        }
    )));

    let mut usage = StandardizedUsage::new();
    usage.output_tokens = 12;
    assert!(!stream_terminal_summary_missing_observed_finish(Some(
        &ExecutionStreamTerminalSummary {
            standardized_usage: Some(usage),
            observed_finish: false,
            ..ExecutionStreamTerminalSummary::default()
        }
    )));
    assert!(!stream_terminal_summary_missing_observed_finish(Some(
        &ExecutionStreamTerminalSummary {
            observed_finish: true,
            ..ExecutionStreamTerminalSummary::default()
        }
    )));
    assert!(!stream_terminal_summary_missing_observed_finish(None));
}

#[test]
fn requires_terminal_event_for_openai_responses_streams() {
    assert!(stream_requires_observed_terminal_event(
        "openai:responses",
        None
    ));
    assert!(stream_requires_observed_terminal_event(
        "openai:responses:compact",
        None
    ));
    assert!(!stream_requires_observed_terminal_event(
        "openai:chat",
        None
    ));
    assert!(stream_requires_observed_terminal_event(
        "openai:chat",
        Some(&json!({
            "provider_stream_event_api_format": "openai:responses"
        }))
    ));
}

#[test]
fn synthesizes_missing_terminal_summary_for_openai_responses_empty_stream() {
    let mut summary = None;
    ensure_stream_terminal_summary_for_missing_observed_finish(&mut summary, true);

    let summary = summary.expect("summary should be synthesized");
    assert!(!summary.observed_finish);
    assert_eq!(
        summary.parser_error.as_deref(),
        Some("execution runtime stream ended before provider terminal event")
    );
    assert!(stream_terminal_summary_missing_observed_finish_with_requirement(Some(&summary), true));
    assert!(stream_terminal_summary_represents_failure_with_requirement(
        Some(&summary),
        true
    ));
}

#[test]
fn terminal_required_stream_fails_even_with_usage_without_finish() {
    let mut usage = StandardizedUsage::new();
    usage.output_tokens = 12;
    let mut summary = Some(ExecutionStreamTerminalSummary {
        standardized_usage: Some(usage),
        observed_finish: false,
        ..ExecutionStreamTerminalSummary::default()
    });

    ensure_stream_terminal_summary_for_missing_observed_finish(&mut summary, true);
    let summary = summary.as_ref().expect("summary should remain present");
    assert!(stream_terminal_summary_missing_observed_finish_with_requirement(Some(summary), true));
    assert!(stream_terminal_summary_represents_failure_with_requirement(
        Some(summary),
        true
    ));
}

#[tokio::test]
async fn kiro_stream_summary_applies_prompt_cache_usage_from_original_request() {
    let request_body = json!({
        "model": "claude-opus-4-7",
        "system": [
            {
                "type": "text",
                "text": "cacheable system ".repeat(600),
                "cache_control": {"type": "ephemeral"}
            }
        ],
        "messages": [
            {
                "role": "user",
                "content": [
                    {
                        "type": "text",
                        "text": "cacheable prompt ".repeat(1200),
                        "cache_control": {"type": "ephemeral"}
                    }
                ]
            }
        ]
    });
    let report_context = json!({
        "original_request_body": request_body,
        "kiro_simulated_cache_enabled": true,
    });
    let plan = ExecutionPlan {
        request_id: "req-kiro-cache-stream".into(),
        candidate_id: Some("cand-kiro-cache-stream".into()),
        provider_name: Some("Kiro".into()),
        provider_id: "provider-kiro-cache-stream".into(),
        endpoint_id: "endpoint-kiro-cache-stream".into(),
        key_id: "key-kiro-cache-stream".into(),
        method: "POST".into(),
        url: "https://q.us-east-1.amazonaws.com/generateAssistantResponse?beta=true".into(),
        headers: BTreeMap::new(),
        content_type: Some("application/json".into()),
        content_encoding: None,
        body: RequestBody::from_json(json!({"conversationState": {}})),
        stream: true,
        client_api_format: "claude:messages".into(),
        provider_api_format: "claude:messages".into(),
        model_name: Some("claude-opus-4-7".into()),
        proxy: None,
        transport_profile: None,
        timeouts: None,
    };
    let state = test_state();

    let mut first_summary = Some(ExecutionStreamTerminalSummary {
        standardized_usage: Some(StandardizedUsage {
            input_tokens: 6_000,
            output_tokens: 17,
            ..StandardizedUsage::new()
        }),
        ..ExecutionStreamTerminalSummary::default()
    });
    maybe_apply_kiro_prompt_cache_usage_to_stream_summary(
        &state,
        &plan,
        Some(&report_context),
        &mut first_summary,
    )
    .await;
    let first_usage = first_summary
        .as_ref()
        .and_then(|summary| summary.standardized_usage.as_ref())
        .expect("first usage should exist");
    assert!(first_usage.cache_creation_tokens > 0);
    assert_eq!(first_usage.cache_read_tokens, 0);
    assert_eq!(
        first_usage.token_source,
        Some(aether_contracts::UsageTokenSource::Mixed)
    );

    let mut second_summary = Some(ExecutionStreamTerminalSummary {
        standardized_usage: Some(StandardizedUsage {
            input_tokens: 6_000,
            output_tokens: 19,
            ..StandardizedUsage::new()
        }),
        ..ExecutionStreamTerminalSummary::default()
    });
    maybe_apply_kiro_prompt_cache_usage_to_stream_summary(
        &state,
        &plan,
        Some(&report_context),
        &mut second_summary,
    )
    .await;
    let second_usage = second_summary
        .as_ref()
        .and_then(|summary| summary.standardized_usage.as_ref())
        .expect("second usage should exist");
    assert!(second_usage.cache_read_tokens > 0);
    assert_eq!(second_usage.cache_creation_tokens, 0);
    assert!(second_usage.input_tokens < 6_000);
    assert_eq!(second_usage.output_tokens, 19);
    assert_eq!(
        second_usage.token_source,
        Some(aether_contracts::UsageTokenSource::Mixed)
    );
}

#[tokio::test]
async fn kiro_stream_summary_reads_cached_prefix_within_prompt_cache_lookback_window() {
    let first_request_body = json!({
        "model": "claude-sonnet-4.6",
        "messages": [{
            "role": "user",
            "content": [{
                "type": "text",
                "text": "shared first turn ".repeat(600),
                "cache_control": {"type": "ephemeral"}
            }]
        }]
    });
    let mut second_messages = vec![json!({
        "role": "user",
        "content": [{
            "type": "text",
            "text": "shared first turn ".repeat(600)
        }]
    })];
    for index in 0..12 {
        second_messages.push(json!({
            "role": if index % 2 == 0 { "assistant" } else { "user" },
            "content": format!("intermediate stream turn {index}")
        }));
    }
    second_messages.push(json!({
        "role": "user",
        "content": [{
            "type": "text",
            "text": "new tail turn ".repeat(600),
            "cache_control": {"type": "ephemeral"}
        }]
    }));
    let second_request_body = json!({
        "model": "claude-sonnet-4.6",
        "messages": second_messages
    });
    let plan = ExecutionPlan {
        request_id: "req-kiro-cache-stream-long-tail".into(),
        candidate_id: Some("cand-kiro-cache-stream-long-tail".into()),
        provider_name: Some("Kiro".into()),
        provider_id: "provider-kiro-cache-stream-long-tail".into(),
        endpoint_id: "endpoint-kiro-cache-stream-long-tail".into(),
        key_id: "key-kiro-cache-stream-long-tail".into(),
        method: "POST".into(),
        url: "https://q.us-east-1.amazonaws.com/generateAssistantResponse?beta=true".into(),
        headers: BTreeMap::new(),
        content_type: Some("application/json".into()),
        content_encoding: None,
        body: RequestBody::from_json(json!({"conversationState": {}})),
        stream: true,
        client_api_format: "claude:messages".into(),
        provider_api_format: "claude:messages".into(),
        model_name: Some("claude-sonnet-4.6".into()),
        proxy: None,
        transport_profile: None,
        timeouts: None,
    };
    let first_report_context = json!({
        "original_request_body": first_request_body,
        "kiro_simulated_cache_enabled": true,
    });
    let second_report_context = json!({
        "original_request_body": second_request_body,
        "kiro_simulated_cache_enabled": true,
    });
    let state = test_state();

    let mut first_summary = Some(ExecutionStreamTerminalSummary {
        standardized_usage: Some(StandardizedUsage {
            input_tokens: 4_000,
            output_tokens: 17,
            ..StandardizedUsage::new()
        }),
        ..ExecutionStreamTerminalSummary::default()
    });
    maybe_apply_kiro_prompt_cache_usage_to_stream_summary(
        &state,
        &plan,
        Some(&first_report_context),
        &mut first_summary,
    )
    .await;
    let first_usage = first_summary
        .as_ref()
        .and_then(|summary| summary.standardized_usage.as_ref())
        .expect("first usage should exist");
    assert!(first_usage.cache_creation_tokens > 0);
    assert_eq!(first_usage.cache_read_tokens, 0);

    let mut second_summary = Some(ExecutionStreamTerminalSummary {
        standardized_usage: Some(StandardizedUsage {
            input_tokens: 8_000,
            output_tokens: 19,
            ..StandardizedUsage::new()
        }),
        ..ExecutionStreamTerminalSummary::default()
    });
    maybe_apply_kiro_prompt_cache_usage_to_stream_summary(
        &state,
        &plan,
        Some(&second_report_context),
        &mut second_summary,
    )
    .await;
    let second_usage = second_summary
        .as_ref()
        .and_then(|summary| summary.standardized_usage.as_ref())
        .expect("second usage should exist");
    assert!(
        second_usage.cache_read_tokens > 0,
        "stream summary should reuse the far earlier cached prefix"
    );
    assert!(second_usage.cache_creation_tokens > 0);
    assert_eq!(second_usage.output_tokens, 19);
}

#[tokio::test]
async fn kiro_stream_summary_seeds_input_tokens_without_cache_control() {
    let request_body = json!({
        "model": "claude-opus-4-7",
        "system": [
            {
                "type": "text",
                "text": "non cacheable system ".repeat(400)
            }
        ],
        "messages": [
            {
                "role": "user",
                "content": [
                    {
                        "type": "text",
                        "text": "non cacheable prompt ".repeat(800)
                    }
                ]
            }
        ]
    });
    let report_context = json!({
        "original_request_body": request_body,
        "kiro_simulated_cache_enabled": true,
    });
    let plan = ExecutionPlan {
        request_id: "req-kiro-non-cache".into(),
        candidate_id: Some("cand-kiro-non-cache".into()),
        provider_name: Some("Kiro".into()),
        provider_id: "provider-kiro-non-cache".into(),
        endpoint_id: "endpoint-kiro-non-cache".into(),
        key_id: "key-kiro-non-cache".into(),
        method: "POST".into(),
        url: "https://q.us-east-1.amazonaws.com/generateAssistantResponse?beta=true".into(),
        headers: BTreeMap::new(),
        content_type: Some("application/json".into()),
        content_encoding: None,
        body: RequestBody::from_json(json!({"conversationState": {}})),
        stream: true,
        client_api_format: "claude:messages".into(),
        provider_api_format: "claude:messages".into(),
        model_name: Some("claude-opus-4-7".into()),
        proxy: None,
        transport_profile: None,
        timeouts: None,
    };
    let state = test_state();

    let mut summary = Some(ExecutionStreamTerminalSummary {
        standardized_usage: Some(StandardizedUsage {
            input_tokens: 0,
            output_tokens: 13,
            ..StandardizedUsage::new()
        }),
        ..ExecutionStreamTerminalSummary::default()
    });

    maybe_apply_kiro_prompt_cache_usage_to_stream_summary(
        &state,
        &plan,
        Some(&report_context),
        &mut summary,
    )
    .await;

    let usage = summary
        .as_ref()
        .and_then(|summary| summary.standardized_usage.as_ref())
        .expect("usage should exist");

    assert!(usage.input_tokens > 0);
    assert_eq!(usage.cache_creation_tokens, 0);
    assert_eq!(usage.cache_read_tokens, 0);
    assert_eq!(usage.output_tokens, 13);
    assert_eq!(
        usage.token_source,
        Some(aether_contracts::UsageTokenSource::Mixed)
    );

    use aether_contracts::UsageTokenSource::{Estimated, Mixed};
    for (hint, source, input, output, cache, expected) in [
        (Some("estimated"), None, 0, 13, 0, Some(Estimated)),
        (None, Some(Estimated), 0, 13, 0, Some(Estimated)),
        (None, None, 0, 0, 200, Some(Mixed)),
        (None, None, 0, 0, 0, Some(Estimated)),
        (None, None, 50, 13, 0, None),
    ] {
        let mut context = report_context.clone();
        if let Some(hint) = hint {
            context["usage_token_source"] = json!(hint);
        }
        let mut summary = Some(ExecutionStreamTerminalSummary {
            standardized_usage: Some(StandardizedUsage {
                token_source: source,
                input_tokens: input,
                output_tokens: output,
                cache_read_tokens: cache,
                ..StandardizedUsage::new()
            }),
            ..Default::default()
        });
        maybe_apply_kiro_prompt_cache_usage_to_stream_summary(
            &state,
            &plan,
            Some(&context),
            &mut summary,
        )
        .await;
        let usage = summary.unwrap().standardized_usage.unwrap();
        assert!(usage.input_tokens > 0);
        assert_eq!(usage.output_tokens, output);
        assert_eq!(usage.cache_read_tokens, cache);
        assert_eq!(
            usage.token_source, expected,
            "hint={hint:?}, source={source:?}"
        );
    }
}

#[tokio::test]
async fn kiro_stream_summary_bills_existing_cache_usage_when_input_is_zero() {
    let request_body = json!({
        "model": "claude-opus-4-7",
        "system": [
            {
                "type": "text",
                "text": "cached system ".repeat(800)
            }
        ],
        "messages": [
            {
                "role": "user",
                "content": [
                    {
                        "type": "text",
                        "text": "cached prompt ".repeat(1400)
                    }
                ]
            }
        ]
    });
    let report_context = json!({
        "original_request_body": request_body,
        "kiro_simulated_cache_enabled": true,
    });
    let plan = ExecutionPlan {
        request_id: "req-kiro-existing-cache".into(),
        candidate_id: Some("cand-kiro-existing-cache".into()),
        provider_name: Some("Kiro".into()),
        provider_id: "provider-kiro-existing-cache".into(),
        endpoint_id: "endpoint-kiro-existing-cache".into(),
        key_id: "key-kiro-existing-cache".into(),
        method: "POST".into(),
        url: "https://q.us-east-1.amazonaws.com/generateAssistantResponse?beta=true".into(),
        headers: BTreeMap::new(),
        content_type: Some("application/json".into()),
        content_encoding: None,
        body: RequestBody::from_json(json!({"conversationState": {}})),
        stream: true,
        client_api_format: "claude:messages".into(),
        provider_api_format: "claude:messages".into(),
        model_name: Some("claude-opus-4-7".into()),
        proxy: None,
        transport_profile: None,
        timeouts: None,
    };
    let state = test_state();

    let mut summary = Some(ExecutionStreamTerminalSummary {
        standardized_usage: Some(StandardizedUsage {
            input_tokens: 0,
            output_tokens: 23,
            cache_read_tokens: 200,
            ..StandardizedUsage::new()
        }),
        ..ExecutionStreamTerminalSummary::default()
    });

    maybe_apply_kiro_prompt_cache_usage_to_stream_summary(
        &state,
        &plan,
        Some(&report_context),
        &mut summary,
    )
    .await;

    let usage = summary
        .as_ref()
        .and_then(|summary| summary.standardized_usage.as_ref())
        .expect("usage should exist");

    assert!(usage.input_tokens > 0);
    assert_eq!(usage.cache_read_tokens, 200);
    assert_eq!(usage.cache_creation_tokens, 0);
    assert_eq!(usage.output_tokens, 23);
}

#[tokio::test]
async fn kiro_stream_summary_clears_cache_usage_when_simulated_cache_disabled() {
    let request_body = json!({
        "model": "claude-opus-4-7",
        "system": [
            {
                "type": "text",
                "text": "disabled cache summary system ".repeat(800),
                "cache_control": {"type": "ephemeral"}
            }
        ],
        "messages": [
            {
                "role": "user",
                "content": [
                    {
                        "type": "text",
                        "text": "disabled cache summary prompt ".repeat(1400),
                        "cache_control": {"type": "ephemeral"}
                    }
                ]
            }
        ]
    });
    let report_context = json!({
        "original_request_body": request_body,
    });
    let plan = ExecutionPlan {
        request_id: "req-kiro-summary-cache-disabled".into(),
        candidate_id: Some("cand-kiro-summary-cache-disabled".into()),
        provider_name: Some("Kiro".into()),
        provider_id: "provider-kiro-summary-cache-disabled".into(),
        endpoint_id: "endpoint-kiro-summary-cache-disabled".into(),
        key_id: "key-kiro-summary-cache-disabled".into(),
        method: "POST".into(),
        url: "https://q.us-east-1.amazonaws.com/generateAssistantResponse?beta=true".into(),
        headers: BTreeMap::new(),
        content_type: Some("application/json".into()),
        content_encoding: None,
        body: RequestBody::from_json(json!({"conversationState": {}})),
        stream: true,
        client_api_format: "claude:messages".into(),
        provider_api_format: "claude:messages".into(),
        model_name: Some("claude-opus-4-7".into()),
        proxy: None,
        transport_profile: None,
        timeouts: None,
    };
    let state = test_state();

    let mut summary = Some(ExecutionStreamTerminalSummary {
        standardized_usage: Some(StandardizedUsage {
            input_tokens: 0,
            output_tokens: 23,
            cache_creation_tokens: 500,
            cache_read_tokens: 700,
            ..StandardizedUsage::new()
        }),
        ..ExecutionStreamTerminalSummary::default()
    });

    maybe_apply_kiro_prompt_cache_usage_to_stream_summary(
        &state,
        &plan,
        Some(&report_context),
        &mut summary,
    )
    .await;

    let usage = summary
        .as_ref()
        .and_then(|summary| summary.standardized_usage.as_ref())
        .expect("usage should exist");

    assert!(usage.input_tokens > 0);
    assert_eq!(usage.cache_creation_tokens, 0);
    assert_eq!(usage.cache_read_tokens, 0);
    assert_eq!(usage.output_tokens, 23);
}

#[tokio::test]
async fn kiro_stream_summary_does_not_subtract_cache_from_already_billed_input() {
    let request_body = json!({
        "model": "claude-opus-4-7",
        "messages": [
            {
                "role": "user",
                "content": [
                    {
                        "type": "text",
                        "text": "cached history ".repeat(400),
                        "cache_control": {"type": "ephemeral"}
                    },
                    {
                        "type": "text",
                        "text": "new user turn"
                    }
                ]
            }
        ]
    });
    let report_context = json!({
        "original_request_body": request_body,
        "input_tokens": 24_770,
        "cache_creation_input_tokens": 175,
        "cache_read_input_tokens": 24_463,
        "kiro_simulated_cache_enabled": true
    });
    let plan = ExecutionPlan {
        request_id: "req-kiro-billed-input".into(),
        candidate_id: Some("cand-kiro-billed-input".into()),
        provider_name: Some("Kiro".into()),
        provider_id: "provider-kiro-billed-input".into(),
        endpoint_id: "endpoint-kiro-billed-input".into(),
        key_id: "key-kiro-billed-input".into(),
        method: "POST".into(),
        url: "https://q.us-east-1.amazonaws.com/generateAssistantResponse?beta=true".into(),
        headers: BTreeMap::new(),
        content_type: Some("application/json".into()),
        content_encoding: None,
        body: RequestBody::from_json(json!({"conversationState": {}})),
        stream: true,
        client_api_format: "claude:messages".into(),
        provider_api_format: "claude:messages".into(),
        model_name: Some("claude-opus-4-7".into()),
        proxy: None,
        transport_profile: None,
        timeouts: None,
    };
    let state = test_state();

    let mut summary = Some(ExecutionStreamTerminalSummary {
        standardized_usage: Some(StandardizedUsage {
            input_tokens: 132,
            output_tokens: 167,
            cache_creation_tokens: 175,
            cache_read_tokens: 24_463,
            ..StandardizedUsage::new()
        }),
        ..ExecutionStreamTerminalSummary::default()
    });

    maybe_apply_kiro_prompt_cache_usage_to_stream_summary(
        &state,
        &plan,
        Some(&report_context),
        &mut summary,
    )
    .await;

    let usage = summary
        .as_ref()
        .and_then(|summary| summary.standardized_usage.as_ref())
        .expect("usage should exist");

    assert_eq!(usage.input_tokens, 132);
    assert_eq!(usage.cache_creation_tokens, 175);
    assert_eq!(usage.cache_read_tokens, 24_463);
    assert_eq!(usage.output_tokens, 167);
    assert_eq!(
        usage.token_source,
        Some(aether_contracts::UsageTokenSource::Mixed)
    );
}

#[tokio::test]
async fn kiro_report_context_seeds_input_tokens_from_original_request_body() {
    let request_body = json!({
        "model": "claude-opus-4-7",
        "system": [
            {
                "type": "text",
                "text": "seeded system ".repeat(600)
            }
        ],
        "messages": [
            {
                "role": "user",
                "content": [
                    {
                        "type": "text",
                        "text": "seeded prompt ".repeat(1200)
                    }
                ]
            }
        ]
    });
    let plan = ExecutionPlan {
        request_id: "req-kiro-seed".into(),
        candidate_id: Some("cand-kiro-seed".into()),
        provider_name: Some("Kiro".into()),
        provider_id: "provider-kiro-seed".into(),
        endpoint_id: "endpoint-kiro-seed".into(),
        key_id: "key-kiro-seed".into(),
        method: "POST".into(),
        url: "https://q.us-east-1.amazonaws.com/generateAssistantResponse?beta=true".into(),
        headers: BTreeMap::new(),
        content_type: Some("application/json".into()),
        content_encoding: None,
        body: RequestBody::from_json(json!({"conversationState": {}})),
        stream: true,
        client_api_format: "claude:messages".into(),
        provider_api_format: "claude:messages".into(),
        model_name: Some("claude-opus-4-7".into()),
        proxy: None,
        transport_profile: None,
        timeouts: None,
    };
    let mut report_context = Some(json!({
        "original_request_body": request_body,
        "kiro_simulated_cache_enabled": true,
    }));

    super::seed_kiro_report_context_input_tokens(&plan, &mut report_context);

    let input_tokens = report_context
        .as_ref()
        .and_then(|context| context.get("input_tokens"))
        .and_then(Value::as_u64)
        .expect("kiro input tokens should be seeded");
    assert!(input_tokens > 0);
}

#[tokio::test]
async fn kiro_report_context_seeds_prompt_cache_usage_before_stream_rewrite() {
    let request_body = json!({
        "model": "claude-opus-4-7",
        "system": [
            {
                "type": "text",
                "text": "cache seed system ".repeat(600),
                "cache_control": {"type": "ephemeral"}
            }
        ],
        "messages": [
            {
                "role": "user",
                "content": [
                    {
                        "type": "text",
                        "text": "cache seed prompt ".repeat(1200),
                        "cache_control": {"type": "ephemeral"}
                    }
                ]
            }
        ]
    });
    let plan = ExecutionPlan {
        request_id: "req-kiro-cache-seed".into(),
        candidate_id: Some("cand-kiro-cache-seed".into()),
        provider_name: Some("Kiro".into()),
        provider_id: "provider-kiro-cache-seed".into(),
        endpoint_id: "endpoint-kiro-cache-seed".into(),
        key_id: "key-kiro-cache-seed".into(),
        method: "POST".into(),
        url: "https://q.us-east-1.amazonaws.com/generateAssistantResponse?beta=true".into(),
        headers: BTreeMap::new(),
        content_type: Some("application/json".into()),
        content_encoding: None,
        body: RequestBody::from_json(json!({"conversationState": {}})),
        stream: true,
        client_api_format: "claude:messages".into(),
        provider_api_format: "claude:messages".into(),
        model_name: Some("claude-opus-4-7".into()),
        proxy: None,
        transport_profile: None,
        timeouts: None,
    };
    let mut report_context = Some(json!({
        "original_request_body": request_body,
        "kiro_simulated_cache_enabled": true,
    }));
    let state = AppState::new().expect("gateway state should build");

    super::seed_kiro_report_context_input_tokens(&plan, &mut report_context);
    super::seed_kiro_report_context_prompt_cache_usage(&state, &plan, &mut report_context).await;

    let context = report_context.as_ref().expect("context should exist");
    assert!(context
        .get("input_tokens")
        .and_then(Value::as_u64)
        .is_some_and(|value| value > 0));
    assert!(context
        .get("cache_creation_input_tokens")
        .and_then(Value::as_u64)
        .is_some_and(|value| value > 0));
    assert_eq!(
        context
            .get("cache_read_input_tokens")
            .and_then(Value::as_u64),
        Some(0)
    );
}

#[tokio::test]
async fn kiro_report_context_skips_prompt_cache_usage_when_disabled() {
    let request_body = json!({
        "model": "claude-opus-4-7",
        "system": [
            {
                "type": "text",
                "text": "disabled cache system ".repeat(600),
                "cache_control": {"type": "ephemeral"}
            }
        ],
        "messages": [
            {
                "role": "user",
                "content": [
                    {
                        "type": "text",
                        "text": "disabled cache prompt ".repeat(1200),
                        "cache_control": {"type": "ephemeral"}
                    }
                ]
            }
        ]
    });
    let plan = ExecutionPlan {
        request_id: "req-kiro-cache-disabled".into(),
        candidate_id: Some("cand-kiro-cache-disabled".into()),
        provider_name: Some("Kiro".into()),
        provider_id: "provider-kiro-cache-disabled".into(),
        endpoint_id: "endpoint-kiro-cache-disabled".into(),
        key_id: "key-kiro-cache-disabled".into(),
        method: "POST".into(),
        url: "https://q.us-east-1.amazonaws.com/generateAssistantResponse?beta=true".into(),
        headers: BTreeMap::new(),
        content_type: Some("application/json".into()),
        content_encoding: None,
        body: RequestBody::from_json(json!({"conversationState": {}})),
        stream: true,
        client_api_format: "claude:messages".into(),
        provider_api_format: "claude:messages".into(),
        model_name: Some("claude-opus-4-7".into()),
        proxy: None,
        transport_profile: None,
        timeouts: None,
    };
    let mut report_context = Some(json!({
        "original_request_body": request_body,
    }));
    let state = AppState::new().expect("gateway state should build");

    super::seed_kiro_report_context_input_tokens(&plan, &mut report_context);
    super::seed_kiro_report_context_prompt_cache_usage(&state, &plan, &mut report_context).await;

    let context = report_context.as_ref().expect("context should exist");
    assert!(context
        .get("input_tokens")
        .and_then(Value::as_u64)
        .is_some_and(|value| value > 0));
    assert_eq!(context.get("cache_creation_input_tokens"), None);
    assert_eq!(context.get("cache_read_input_tokens"), None);
}

#[test]
fn native_anthropic_event_stream_uses_bounded_precommit() {
    assert!(!should_skip_direct_finalize_prefetch(
        Some("claude_cli_sync_finalize"),
        Some("text/event-stream"),
        "claude:messages",
        "claude:messages",
        false,
        false,
        false,
    ));
}

#[test]
fn native_anthropic_terminal_error_uses_anthropic_sse_shape() {
    let plan = native_anthropic_stream_plan("anthropic-terminal-error-shape");
    let failure = build_stream_failure_report(
        "execution_runtime_stream_read_error",
        "upstream disconnected",
        502,
    );

    let event = encode_terminal_sse_error_event_for_plan(&plan, &failure)
        .expect("terminal event should encode");
    let event = String::from_utf8(event.to_vec()).expect("event should be utf8");

    assert!(event.starts_with("event: error\ndata: "));
    assert!(event.contains("\"type\":\"error\""));
    assert!(event.contains("\"type\":\"api_error\""));
    assert!(event.contains("Upstream response stream failed"));
    assert!(!event.contains("upstream disconnected"));
    assert!(!event.contains("[DONE]"));
}

#[tokio::test]
async fn native_anthropic_error_before_semantic_event_allows_failover() {
    let unknown = "event: future_event\ndata: {\"type\":\"future_event\",\"value\":1}\n\n";
    let upstream_error = "event: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"overloaded_error\",\"message\":\"Overloaded\"}}\n\n";
    let outcome = execute_native_anthropic_prefetch_stream(
        "req-anthropic-precommit-error",
        vec![unknown.to_string(), upstream_error.to_string()],
    )
    .await;

    let AiAttemptExecutionOutcome::Retry {
        scope,
        fallback_response: Some(fallback_response),
    } = outcome
    else {
        panic!("precommit 529 should retry with the upstream response preserved")
    };
    assert_eq!(scope, AiAttemptRetryScope::Provider);
    assert_eq!(
        fallback_response.status(),
        StatusCode::from_u16(529).expect("529 is a valid status")
    );
    let fallback_body = to_bytes(fallback_response.into_body(), usize::MAX)
        .await
        .expect("fallback response body should read");
    // Current pre-commit handling normalizes native Anthropic errors to the
    // client JSON error body; the raw SSE envelope is not forwarded.
    assert_eq!(
        fallback_body.as_ref(),
        br#"{"type":"error","error":{"type":"overloaded_error","message":"Overloaded"}}"#
    );
}

#[tokio::test]
async fn native_anthropic_eof_before_semantic_event_allows_failover() {
    let outcome = execute_native_anthropic_prefetch_stream(
        "req-anthropic-precommit-eof",
        vec![": ping\n\n".to_string()],
    )
    .await;

    let AiAttemptExecutionOutcome::Retry {
        scope,
        fallback_response,
    } = outcome
    else {
        panic!("EOF before the first semantic event should retry another endpoint")
    };
    assert_eq!(scope, AiAttemptRetryScope::Endpoint);
    assert!(fallback_response.is_none());
}

#[tokio::test]
async fn native_anthropic_auth_error_moves_to_the_next_credential() {
    let upstream_error = concat!(
        "event: error\n",
        "data: {\"type\":\"error\",\"error\":{\"type\":\"authentication_error\",\"message\":\"invalid credential\"}}\n\n",
    );
    let outcome = execute_native_anthropic_prefetch_stream(
        "req-anthropic-precommit-auth-error",
        vec![upstream_error.to_string()],
    )
    .await;

    let AiAttemptExecutionOutcome::Retry {
        scope,
        fallback_response: Some(fallback_response),
    } = outcome
    else {
        panic!("precommit authentication error should retry another credential")
    };
    assert_eq!(scope, AiAttemptRetryScope::Credential);
    let fallback_body = to_bytes(fallback_response.into_body(), usize::MAX)
        .await
        .expect("fallback response body should read");
    // Current pre-commit handling normalizes native Anthropic errors to the
    // client JSON error body; the raw SSE envelope is not forwarded.
    assert_eq!(
        fallback_body.as_ref(),
        br#"{"type":"error","error":{"type":"authentication_error","message":"invalid credential"}}"#
    );
}

#[tokio::test]
async fn native_anthropic_semantic_event_commits_before_later_error() {
    let raw = concat!(
        "event: message_start\n",
        "data: {\"type\":\"message_start\",\"message\":{}}\n\n",
        "event: content_block_delta\n",
        "data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"hello\"}}\n\n",
        "event: error\n",
        "data: {\"type\":\"error\",\"error\":{\"type\":\"overloaded_error\",\"message\":\"late\"}}\n\n",
    );
    let outcome = execute_native_anthropic_prefetch_stream(
        "req-anthropic-postcommit-error",
        vec![raw.to_string()],
    )
    .await;
    let AiAttemptExecutionOutcome::Responded(response) = outcome else {
        panic!("a semantic event should commit the selected candidate")
    };
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("response body should read");

    assert_eq!(body.as_ref(), raw.as_bytes());
}

#[tokio::test]
async fn native_anthropic_frame_error_after_commit_emits_anthropic_terminal_event() {
    let message_start = concat!(
        "event: message_start\n",
        "data: {\"type\":\"message_start\",\"message\":{}}\n\n",
        "event: content_block_delta\n",
        "data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"hello\"}}\n\n",
    );
    let original_error = "upstream disconnected after message_start";
    let outcome = execute_native_anthropic_prefetch_stream_with_terminal_error(
        "req-anthropic-postcommit-frame-error",
        vec![message_start.to_string()],
        Some(original_error.to_string()),
    )
    .await;
    let AiAttemptExecutionOutcome::Responded(response) = outcome else {
        panic!("a semantic event should commit the selected candidate")
    };
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("response body should read");
    let body = String::from_utf8(body.to_vec()).expect("body should be utf8");

    assert!(body.starts_with(message_start));
    assert!(body.contains("event: error\ndata: {\"type\":\"error\""));
    assert!(body.contains("\"type\":\"api_error\""));
    assert!(body.contains("Execution runtime stream protocol failed"));
    assert!(!body.contains(original_error));
    assert!(!body.contains("[DONE]"));
}

#[tokio::test]
async fn native_anthropic_eof_after_commit_emits_anthropic_terminal_event() {
    let message_start = concat!(
        "event: message_start\n",
        "data: {\"type\":\"message_start\",\"message\":{}}\n\n",
        "event: content_block_delta\n",
        "data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"hello\"}}\n\n",
    );
    let outcome = execute_native_anthropic_prefetch_stream(
        "req-anthropic-postcommit-eof",
        vec![message_start.to_string()],
    )
    .await;
    let AiAttemptExecutionOutcome::Responded(response) = outcome else {
        panic!("a semantic event should commit the selected candidate")
    };
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("response body should read");
    let body = String::from_utf8(body.to_vec()).expect("body should be utf8");

    assert!(body.starts_with(message_start));
    assert!(body.contains("event: error\ndata: {\"type\":\"error\""));
    assert!(body.contains("ended before message_stop"));
    assert!(!body.contains("[DONE]"));
}

#[tokio::test]
async fn native_anthropic_done_marker_does_not_replace_message_stop() {
    let message_start = concat!(
        "event: message_start\n",
        "data: {\"type\":\"message_start\",\"message\":{}}\n\n",
        "event: content_block_delta\n",
        "data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"hello\"}}\n\n",
    );
    let done = "data: [DONE]\n\n";
    let outcome = execute_native_anthropic_prefetch_stream(
        "req-anthropic-done-without-message-stop",
        vec![message_start.to_string(), done.to_string()],
    )
    .await;
    let AiAttemptExecutionOutcome::Responded(response) = outcome else {
        panic!("text output should commit the selected candidate")
    };
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("response body should read");
    let body = String::from_utf8(body.to_vec()).expect("body should be utf8");

    assert!(body.starts_with(message_start));
    assert!(body.contains(done));
    assert!(body.contains("event: error\ndata: {\"type\":\"error\""));
    assert!(body.contains("ended before message_stop"));
}

#[tokio::test]
async fn native_anthropic_hanging_tail_does_not_delay_body_eof_and_is_bounded() {
    let request_id = "req-anthropic-hanging-tail";
    let plan = native_anthropic_stream_plan(request_id);
    let provider_catalog = provider_catalog_for_plan(&plan, None);
    let usage_repository = Arc::new(InMemoryUsageReadRepository::default());
    let state = AppState::new()
        .expect("app state should build")
        .with_data_state_for_tests(
            crate::data::GatewayDataState::with_usage_repository_for_tests(Arc::clone(
                &usage_repository,
            ))
            .with_provider_catalog_reader(Arc::new(provider_catalog))
            .with_encryption_key_for_tests(DEV_FERNET_KEY),
        )
        .with_usage_runtime_for_tests(UsageRuntimeConfig {
            enabled: true,
            ..UsageRuntimeConfig::default()
        });
    let message_start = concat!(
        "event: message_start\n",
        "data: {\"type\":\"message_start\",\"message\":{}}\n\n",
    );
    let message_stop = "event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n";
    let stream_dropped = Arc::new(AtomicBool::new(false));
    let drop_flag = StreamDropFlag(Arc::clone(&stream_dropped));
    let frame_stream = stream! {
        let _drop_flag = drop_flag;
        yield Ok::<Bytes, std::io::Error>(ndjson_frame(StreamFrame {
            frame_type: StreamFrameType::Headers,
            payload: StreamFramePayload::Headers {
                status_code: 200,
                headers: BTreeMap::from([(
                    "content-type".to_string(),
                    "text/event-stream".to_string(),
                )]),
                response_observation: None,
            },
        }));
        yield Ok::<Bytes, std::io::Error>(ndjson_frame(StreamFrame {
            frame_type: StreamFrameType::Data,
            payload: StreamFramePayload::Data {
                chunk_b64: None,
                text: Some(message_start.to_string()),
            },
        }));
        yield Ok::<Bytes, std::io::Error>(ndjson_frame(StreamFrame {
            frame_type: StreamFrameType::Data,
            payload: StreamFramePayload::Data {
                chunk_b64: None,
                text: Some(message_stop.to_string()),
            },
        }));
        std::future::pending::<()>().await;
    }
    .boxed();

    let response = execute_stream_from_frame_stream(
        &state,
        plan,
        "trace-anthropic-hanging-tail",
        &test_decision(),
        "claude_chat_stream",
        Some("claude_chat_stream_success".to_string()),
        Some(json!({
            "request_id": request_id,
            "candidate_id": format!("candidate-{request_id}"),
            "candidate_index": 0,
            "retry_index": 0,
            "provider_api_format": "claude:messages",
            "client_api_format": "claude:messages"
        })),
        crate::clock::current_unix_ms(),
        Instant::now(),
        RequestStageTrace::from_env(),
        false,
        frame_stream,
        None,
    )
    .await
    .expect("stream execution should succeed")
    .expect("stream execution should return a response");

    let body = tokio::time::timeout(
        Duration::from_secs(1),
        to_bytes(response.into_body(), usize::MAX),
    )
    .await
    .expect("client body EOF must not wait for the hanging producer tail")
    .expect("client body should read");
    assert_eq!(
        body.as_ref(),
        format!("{message_start}{message_stop}").as_bytes()
    );

    tokio::time::timeout(Duration::from_secs(1), async {
        while !stream_dropped.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("producer tail should be dropped after the bounded drain window");

    let stored_usage = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let usage = usage_repository
                .find_by_request_id(request_id)
                .await
                .expect("usage should read");
            if usage
                .as_ref()
                .is_some_and(|usage| usage.status == "completed")
            {
                break usage.expect("completed usage should exist");
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("bounded drain timeout should still settle usage successfully");
    assert_eq!(stored_usage.status_code, Some(200));
}

#[tokio::test]
async fn native_anthropic_unterminated_oversized_tail_respects_read_budget() {
    const TAIL_CHUNK_BYTES: usize = 4 * 1024 * 1024;

    let request_id = "req-anthropic-oversized-tail";
    let plan = native_anthropic_stream_plan(request_id);
    let provider_catalog = provider_catalog_for_plan(&plan, None);
    let usage_repository = Arc::new(InMemoryUsageReadRepository::default());
    let state = AppState::new()
        .expect("app state should build")
        .with_data_state_for_tests(
            crate::data::GatewayDataState::with_usage_repository_for_tests(Arc::clone(
                &usage_repository,
            ))
            .with_provider_catalog_reader(Arc::new(provider_catalog))
            .with_encryption_key_for_tests(DEV_FERNET_KEY),
        )
        .with_usage_runtime_for_tests(UsageRuntimeConfig {
            enabled: true,
            ..UsageRuntimeConfig::default()
        });
    let message_start = concat!(
        "event: message_start\n",
        "data: {\"type\":\"message_start\",\"message\":{}}\n\n",
    );
    let message_stop = "event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n";
    let stream_dropped = Arc::new(AtomicBool::new(false));
    let drop_flag = StreamDropFlag(Arc::clone(&stream_dropped));
    let tail_chunks_polled = Arc::new(AtomicUsize::new(0));
    let tail_chunks_polled_for_stream = Arc::clone(&tail_chunks_polled);
    let tail_chunk = Bytes::from(vec![b'x'; TAIL_CHUNK_BYTES]);
    let tail_chunk_count = 32;
    let frame_stream = stream! {
        let _drop_flag = drop_flag;
        yield Ok::<Bytes, std::io::Error>(ndjson_frame(StreamFrame {
            frame_type: StreamFrameType::Headers,
            payload: StreamFramePayload::Headers {
                status_code: 200,
                headers: BTreeMap::from([(
                    "content-type".to_string(),
                    "text/event-stream".to_string(),
                )]),
                response_observation: None,
            },
        }));
        yield Ok::<Bytes, std::io::Error>(ndjson_frame(StreamFrame {
            frame_type: StreamFrameType::Data,
            payload: StreamFramePayload::Data {
                chunk_b64: None,
                text: Some(message_start.to_string()),
            },
        }));
        yield Ok::<Bytes, std::io::Error>(ndjson_frame(StreamFrame {
            frame_type: StreamFrameType::Data,
            payload: StreamFramePayload::Data {
                chunk_b64: None,
                text: Some(message_stop.to_string()),
            },
        }));
        for _ in 0..tail_chunk_count {
            tail_chunks_polled_for_stream.fetch_add(1, Ordering::SeqCst);
            yield Ok::<Bytes, std::io::Error>(tail_chunk.clone());
        }
        std::future::pending::<()>().await;
    }
    .boxed();

    let response = execute_stream_from_frame_stream(
        &state,
        plan,
        "trace-anthropic-oversized-tail",
        &test_decision(),
        "claude_chat_stream",
        Some("claude_chat_stream_success".to_string()),
        Some(json!({
            "request_id": request_id,
            "candidate_id": format!("candidate-{request_id}"),
            "candidate_index": 0,
            "retry_index": 0,
            "provider_api_format": "claude:messages",
            "client_api_format": "claude:messages"
        })),
        crate::clock::current_unix_ms(),
        Instant::now(),
        RequestStageTrace::from_env(),
        false,
        frame_stream,
        None,
    )
    .await
    .expect("stream execution should succeed")
    .expect("stream execution should return a response");

    let body = tokio::time::timeout(
        Duration::from_secs(1),
        to_bytes(response.into_body(), usize::MAX),
    )
    .await
    .expect("client body EOF must not wait for the oversized unterminated tail")
    .expect("client body should read");
    assert_eq!(
        body.as_ref(),
        format!("{message_start}{message_stop}").as_bytes()
    );

    tokio::time::timeout(Duration::from_secs(1), async {
        while !stream_dropped.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("oversized tail producer should be released by the read budget");
    assert!(
        tail_chunks_polled.load(Ordering::SeqCst) <= 1,
        "post-stop drain must retain at most one atomic upstream stream item"
    );

    let stored_usage = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let usage = usage_repository
                .find_by_request_id(request_id)
                .await
                .expect("usage should read");
            if usage
                .as_ref()
                .is_some_and(|usage| usage.status == "completed")
            {
                break usage.expect("completed usage should exist");
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("bounded oversized tail should still settle usage successfully");
    assert_eq!(stored_usage.status_code, Some(200));
}

#[tokio::test]
async fn native_anthropic_frame_error_after_message_stop_is_ignored() {
    let message_start = concat!(
        "event: message_start\n",
        "data: {\"type\":\"message_start\",\"message\":{}}\n\n",
    );
    let message_stop = "event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n";
    let outcome = execute_native_anthropic_prefetch_stream_with_terminal_error(
        "req-anthropic-error-after-message-stop",
        vec![message_start.to_string(), message_stop.to_string()],
        Some("connection reset after message_stop".to_string()),
    )
    .await;
    let AiAttemptExecutionOutcome::Responded(response) = outcome else {
        panic!("message_stop should keep the selected candidate committed")
    };
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("response body should read");

    assert_eq!(
        body.as_ref(),
        format!("{message_start}{message_stop}").as_bytes()
    );
}

#[tokio::test]
async fn native_anthropic_same_chunk_stops_at_message_stop_record() {
    let message_start = concat!(
        "event: message_start\n",
        "data: {\"type\":\"message_start\",\"message\":{}}\n\n",
    );
    let message_stop = "event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n";
    let trailing_error = concat!(
        "event: error\n",
        "data: {\"type\":\"error\",\"error\":{\"type\":\"api_error\",\"message\":\"after stop\"}}\n\n",
    );
    let outcome = execute_native_anthropic_prefetch_stream(
        "req-anthropic-same-chunk-message-stop",
        vec![format!("{message_start}{message_stop}{trailing_error}")],
    )
    .await;
    let AiAttemptExecutionOutcome::Responded(response) = outcome else {
        panic!("message_stop should complete the selected candidate")
    };
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("response body should read");

    assert_eq!(
        body.as_ref(),
        format!("{message_start}{message_stop}").as_bytes()
    );
}

#[tokio::test]
async fn direct_anthropic_stops_at_message_stop_and_ignores_teardown_error() {
    let message_start = Bytes::from_static(
        b"event: message_start\ndata: {\"type\":\"message_start\",\"message\":{}}\n\n",
    );
    let message_stop = "event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n";
    let trailing_error = concat!(
        "event: error\n",
        "data: {\"type\":\"error\",\"error\":{\"type\":\"api_error\",\"message\":\"after stop\"}}\n\n",
    );
    let state = direct_anthropic_inline_state(
        "req-direct-anthropic-message-stop",
        vec![
            Ok(message_start.clone()),
            Ok(Bytes::from(format!("{message_stop}{trailing_error}"))),
            Err("connection reset after message_stop".to_string()),
        ],
    );

    let (first, state) = state
        .next_item()
        .await
        .expect("message_start should stream");
    assert_eq!(first.expect("message_start should succeed"), message_start);
    let (second, state) = state.next_item().await.expect("message_stop should stream");
    assert_eq!(
        second.expect("message_stop should succeed").as_ref(),
        message_stop.as_bytes()
    );
    assert!(state.next_item().await.is_none());
}

#[tokio::test]
async fn direct_anthropic_clean_eof_after_message_start_emits_one_error() {
    let message_start = Bytes::from_static(
        b"event: message_start\ndata: {\"type\":\"message_start\",\"message\":{}}\n\n",
    );
    let state = direct_anthropic_inline_state(
        "req-direct-anthropic-premature-eof",
        vec![Ok(message_start.clone())],
    );

    let (first, state) = state
        .next_item()
        .await
        .expect("message_start should stream");
    assert_eq!(first.expect("message_start should succeed"), message_start);
    let (error, mut state) = state
        .next_item()
        .await
        .expect("premature EOF should emit an Anthropic error event");
    let error = String::from_utf8(error.expect("error event should succeed").to_vec())
        .expect("error event should be utf8");
    assert!(error.starts_with("event: error\ndata: "));
    assert!(error.contains("ended before message_stop"));
    discard_direct_test_finalizer(&mut state);
}

#[tokio::test]
async fn direct_anthropic_provider_error_is_not_duplicated() {
    let message_start = Bytes::from_static(
        b"event: message_start\ndata: {\"type\":\"message_start\",\"message\":{}}\n\n",
    );
    let provider_error = Bytes::from_static(
        b"event: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"overloaded_error\",\"message\":\"busy\"}}\n\n",
    );
    let state = direct_anthropic_inline_state(
        "req-direct-anthropic-provider-error",
        vec![Ok(message_start.clone()), Ok(provider_error.clone())],
    );

    let (first, state) = state
        .next_item()
        .await
        .expect("message_start should stream");
    assert_eq!(first.expect("message_start should succeed"), message_start);
    let (second, state) = state
        .next_item()
        .await
        .expect("provider error should stream");
    assert_eq!(
        second.expect("provider error should succeed"),
        provider_error
    );
    assert!(state.terminal_error_sent);
    assert!(state.next_item().await.is_none());
}

#[test]
fn postcommit_anthropic_errors_use_the_precommit_status_taxonomy() {
    for (error_type, expected_status) in [
        ("request_too_large", 413),
        ("overloaded_error", 529),
        ("api_error", 500),
    ] {
        let body = json!({
            "type": "error",
            "error": { "type": error_type, "message": "upstream failure" }
        });
        assert_eq!(
            resolve_provider_stream_error_status_code("claude:messages", 200, &body),
            expected_status,
        );
    }
}

#[tokio::test]
async fn native_anthropic_unknown_event_is_replayed_byte_for_byte() {
    let unknown = "event: future_event\ndata: {\"type\":\"future_event\",\"value\":1}\n\n";
    let message_start = concat!(
        "event: message_start\n",
        "data: {\"type\":\"message_start\",\"message\":{}}\n\n",
    );
    let message_stop = "event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n";
    let expected = format!("{unknown}{message_start}{message_stop}");
    let outcome = execute_native_anthropic_prefetch_stream(
        "req-anthropic-unknown-replay",
        vec![
            unknown.to_string(),
            message_start.to_string(),
            message_stop.to_string(),
        ],
    )
    .await;
    let AiAttemptExecutionOutcome::Responded(response) = outcome else {
        panic!("unknown event should not terminate the stream")
    };
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("response body should read");

    assert_eq!(body.as_ref(), expected.as_bytes());
}

#[test]
fn skips_prefetch_for_same_format_passthrough_streams_without_content_type() {
    assert!(should_skip_direct_finalize_prefetch(
        Some("claude_cli_sync_finalize"),
        None,
        "claude:messages",
        "claude:messages",
        false,
        false,
        false,
    ));
}

#[test]
fn keeps_prefetch_for_same_format_json_streams() {
    assert!(!should_skip_direct_finalize_prefetch(
        Some("claude_cli_sync_finalize"),
        Some("application/json"),
        "claude:messages",
        "claude:messages",
        false,
        false,
        false,
    ));
}

#[test]
fn declared_stream_response_headers_are_normalized_without_body_inspection() {
    let mut headers = BTreeMap::from([
        (
            "Content-Type".to_string(),
            "Application/Octet-Stream; charset=binary".to_string(),
        ),
        ("Content-Encoding".to_string(), "identity".to_string()),
        ("x-upstream-header".to_string(), "preserved".to_string()),
    ]);
    assert!(should_normalize_declared_stream_response_headers(
        OPENAI_CHAT_STREAM_PLAN_KIND,
        200,
        &headers,
        Some(&json!({"upstream_is_stream": true})),
    ));
    headers.insert("Content-Length".to_string(), "4096".to_string());
    normalize_declared_stream_response_headers(&mut headers);

    assert_eq!(
        headers.get("content-type").map(String::as_str),
        Some("text/event-stream")
    );
    assert!(!headers
        .keys()
        .any(|name| name.eq_ignore_ascii_case("content-encoding")));
    assert!(!headers
        .keys()
        .any(|name| name.eq_ignore_ascii_case("content-length")));
    assert_eq!(
        headers.get("x-upstream-header").map(String::as_str),
        Some("preserved")
    );
}

#[test]
fn declared_stream_header_normalization_requires_success_and_stream_context() {
    let headers = BTreeMap::from([(
        "content-type".to_string(),
        "application/octet-stream".to_string(),
    )]);
    assert!(!should_normalize_declared_stream_response_headers(
        OPENAI_CHAT_STREAM_PLAN_KIND,
        500,
        &headers,
        Some(&json!({"upstream_is_stream": true})),
    ));
    assert!(!should_normalize_declared_stream_response_headers(
        OPENAI_CHAT_STREAM_PLAN_KIND,
        200,
        &headers,
        Some(&json!({"upstream_is_stream": false})),
    ));
    assert!(!should_normalize_declared_stream_response_headers(
        OPENAI_CHAT_STREAM_PLAN_KIND,
        200,
        &BTreeMap::from([("content-type".to_string(), "text/event-stream".to_string(),)]),
        Some(&json!({"upstream_is_stream": true})),
    ));
    assert!(!should_normalize_declared_stream_response_headers(
        OPENAI_CHAT_STREAM_PLAN_KIND,
        200,
        &BTreeMap::from([("content-type".to_string(), "application/json".to_string(),)]),
        Some(&json!({"upstream_is_stream": true})),
    ));
    assert!(!should_normalize_declared_stream_response_headers(
        OPENAI_CHAT_STREAM_PLAN_KIND,
        200,
        &BTreeMap::from([("content-type".to_string(), "text/plain".to_string(),)]),
        Some(&json!({"upstream_is_stream": true})),
    ));
    assert!(!should_normalize_declared_stream_response_headers(
        OPENAI_CHAT_STREAM_PLAN_KIND,
        200,
        &BTreeMap::from([
            (
                "content-type".to_string(),
                "application/octet-stream".to_string(),
            ),
            ("content-encoding".to_string(), "gzip".to_string()),
        ]),
        Some(&json!({"upstream_is_stream": true})),
    ));
    assert!(!should_normalize_declared_stream_response_headers(
        OPENAI_CHAT_STREAM_PLAN_KIND,
        200,
        &BTreeMap::from([
            (
                "content-type".to_string(),
                "application/octet-stream".to_string(),
            ),
            ("content-length".to_string(), "128".to_string()),
        ]),
        Some(&json!({"upstream_is_stream": true})),
    ));
    assert!(!should_normalize_declared_stream_response_headers(
        GEMINI_FILES_DOWNLOAD_PLAN_KIND,
        200,
        &headers,
        Some(&json!({"upstream_is_stream": true})),
    ));
}

#[test]
fn keeps_prefetch_for_event_streams_even_when_cross_format_or_rewritten() {
    assert!(!should_skip_direct_finalize_prefetch(
        Some("claude_cli_sync_finalize"),
        Some("text/event-stream"),
        "openai:chat",
        "claude:messages",
        false,
        true,
        false,
    ));
}

#[test]
fn cyber_failover_setting_forces_prefetch_for_event_streams() {
    assert!(!should_skip_direct_finalize_prefetch(
        Some("openai_responses_sync_finalize"),
        Some("text/event-stream"),
        "openai:responses",
        "openai:responses",
        false,
        false,
        true,
    ));
}

#[test]
fn cyber_prefetch_waits_through_response_setup_until_output() {
    assert!(!prefetched_openai_responses_body_has_output_boundary(
        b"event: response.created\ndata: {\"type\":\"response.created\"}\n\n"
    ));
    assert!(prefetched_openai_responses_body_has_output_boundary(
        b"event: response.created\ndata: {\"type\":\"response.created\"}\n\nevent: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"hi\"}\n\n"
    ));
}

#[test]
fn skips_success_failover_probe_for_event_streams() {
    assert!(!should_probe_success_failover_before_stream(
        &BTreeMap::from([(
            "content-type".to_string(),
            "text/event-stream; charset=utf-8".to_string(),
        )])
    ));
    assert!(should_probe_success_failover_before_stream(
        &BTreeMap::from([("content-type".to_string(), "application/json".to_string(),)])
    ));
}

#[test]
fn limits_prefetch_for_openai_image_and_rewritten_streams() {
    assert!(should_limit_direct_finalize_prefetch(
        "openai_image_stream",
        false
    ));
    assert!(should_limit_direct_finalize_prefetch(
        "openai_chat_stream",
        true
    ));
    assert!(!should_limit_direct_finalize_prefetch(
        "openai_chat_stream",
        false
    ));
}

#[test]
fn direct_passthrough_mode_defaults_inline_and_accepts_legacy() {
    assert_eq!(
        parse_direct_passthrough_mode(""),
        DirectPassthroughMode::Inline
    );
    assert_eq!(
        parse_direct_passthrough_mode("inline"),
        DirectPassthroughMode::Inline
    );
    assert_eq!(
        parse_direct_passthrough_mode("legacy"),
        DirectPassthroughMode::Legacy
    );
    assert_eq!(
        parse_direct_passthrough_mode("mpsc"),
        DirectPassthroughMode::Legacy
    );
}

#[test]
fn openai_client_formats_disallow_proxy_generated_sse_control_blocks() {
    let mut plan = ExecutionPlan {
        request_id: "req-openai-keepalive".into(),
        candidate_id: Some("cand-openai-keepalive".into()),
        provider_name: Some("openai".into()),
        provider_id: "prov-1".into(),
        endpoint_id: "ep-1".into(),
        key_id: "key-1".into(),
        method: "POST".into(),
        url: "https://example.com/v1/chat/completions".into(),
        headers: BTreeMap::new(),
        content_type: Some("application/json".into()),
        content_encoding: None,
        body: RequestBody::from_json(json!({"stream": true})),
        stream: true,
        client_api_format: "openai:chat".into(),
        provider_api_format: "openai:chat".into(),
        model_name: Some("gpt-5.4".into()),
        proxy: None,
        transport_profile: None,
        timeouts: None,
    };

    assert!(!client_format_allows_proxy_generated_sse_control_blocks(
        &plan
    ));
    plan.client_api_format = "openai:responses".into();
    assert!(!client_format_allows_proxy_generated_sse_control_blocks(
        &plan
    ));
    plan.client_api_format = "claude:messages".into();
    assert!(client_format_allows_proxy_generated_sse_control_blocks(
        &plan
    ));
}

#[tokio::test]
async fn native_anthropic_sse_body_ends_at_message_stop_while_sender_is_alive() {
    let (tx, rx) = mpsc::channel::<Result<Bytes, std::io::Error>>(1);
    let message_stop =
        Bytes::from_static(b"event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n");
    tx.send(Ok(message_stop.clone()))
        .await
        .expect("message_stop should send");
    let mut body_stream = Box::pin(build_sse_body_stream(
        Vec::new(),
        rx,
        true,
        false,
        true,
        Duration::from_secs(60),
    ));

    let chunk = tokio::time::timeout(Duration::from_millis(50), body_stream.next())
        .await
        .expect("message_stop should arrive immediately")
        .expect("stream should yield message_stop")
        .expect("message_stop should be successful");
    assert_eq!(chunk, message_stop);
    assert!(
        tokio::time::timeout(Duration::from_millis(50), body_stream.next())
            .await
            .expect("body EOF must not wait for the producer")
            .is_none()
    );
    assert!(tx.is_closed(), "body EOF should drop the receiver");
}

#[tokio::test]
async fn sse_body_stream_emits_initial_and_periodic_keepalive_without_business_chunks() {
    let (_tx, rx) = mpsc::channel::<Result<Bytes, std::io::Error>>(1);
    let mut body_stream = Box::pin(build_sse_body_stream(
        Vec::new(),
        rx,
        true,
        true,
        false,
        Duration::from_millis(10),
    ));

    let first = tokio::time::timeout(Duration::from_millis(50), body_stream.next())
        .await
        .expect("initial keepalive should be immediate")
        .expect("stream should yield initial keepalive")
        .expect("initial keepalive should be ok");
    assert_eq!(first.as_ref(), b": aether-keepalive\n\n");

    let second = tokio::time::timeout(Duration::from_millis(100), body_stream.next())
        .await
        .expect("periodic keepalive should arrive")
        .expect("stream should yield periodic keepalive")
        .expect("periodic keepalive should be ok");
    assert_eq!(second.as_ref(), b": aether-keepalive\n\n");
}

#[tokio::test]
async fn sse_body_stream_filters_control_blocks_without_synthetic_keepalive() {
    let (tx, rx) = mpsc::channel::<Result<Bytes, std::io::Error>>(1);
    let mut body_stream = Box::pin(build_sse_body_stream(
        vec![Bytes::from_static(b": upstream-keepalive\n\n")],
        rx,
        true,
        false,
        false,
        Duration::from_millis(10),
    ));

    assert!(
        tokio::time::timeout(Duration::from_millis(30), body_stream.next())
            .await
            .is_err(),
        "control-only prefetched blocks should not produce client-visible chunks"
    );

    tx.send(Ok(Bytes::from_static(
        b"data: {\"id\":\"chatcmpl-no-keepalive\"}\n\n",
    )))
    .await
    .expect("business chunk should send");
    let chunk = tokio::time::timeout(Duration::from_millis(50), body_stream.next())
        .await
        .expect("business chunk should arrive")
        .expect("stream should yield business chunk")
        .expect("business chunk should be ok");
    assert_eq!(
        chunk.as_ref(),
        b"data: {\"id\":\"chatcmpl-no-keepalive\"}\n\n"
    );
}

#[tokio::test]
async fn sse_body_stream_drops_upstream_control_only_blocks() {
    let (_tx, rx) = mpsc::channel::<Result<Bytes, std::io::Error>>(1);
    let mut body_stream = Box::pin(build_sse_body_stream(
        vec![
            Bytes::from_static(b": upstream-keepalive\n\n"),
            Bytes::from_static(b"event: ping\nid: 1\nretry: 1000\n\n"),
            Bytes::from_static(
                b"event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"hi\"}\n\n",
            ),
        ],
        rx,
        true,
        true,
        false,
        Duration::from_secs(60),
    ));

    let chunk = tokio::time::timeout(Duration::from_millis(50), body_stream.next())
        .await
        .expect("business chunk should arrive")
        .expect("stream should yield business chunk")
        .expect("business chunk should be ok");
    let text = std::str::from_utf8(chunk.as_ref()).expect("chunk should be utf8");
    assert!(text.contains("event: response.output_text.delta"));
    assert!(text.contains("data: {\"type\":\"response.output_text.delta\""));
    assert!(!text.contains("upstream-keepalive"));
    assert!(!text.contains("event: ping"));
    assert!(!text.contains("retry: 1000"));
}

#[tokio::test]
async fn sse_body_stream_filters_control_blocks_across_chunk_boundaries() {
    let (_tx, rx) = mpsc::channel::<Result<Bytes, std::io::Error>>(1);
    let mut body_stream = Box::pin(build_sse_body_stream(
        vec![
            Bytes::from_static(b": upstream-keepalive\n"),
            Bytes::from_static(b"\n"),
            Bytes::from_static(b"event: response.created\n"),
            Bytes::from_static(b"data: {\"type\":\"response.created\"}\n\n"),
        ],
        rx,
        true,
        true,
        false,
        Duration::from_secs(60),
    ));

    let chunk = tokio::time::timeout(Duration::from_millis(50), body_stream.next())
        .await
        .expect("business chunk should arrive")
        .expect("stream should yield business chunk")
        .expect("business chunk should be ok");
    let text = std::str::from_utf8(chunk.as_ref()).expect("chunk should be utf8");
    assert_eq!(
        text,
        "event: response.created\ndata: {\"type\":\"response.created\"}\n\n"
    );
}

#[tokio::test]
async fn sse_body_stream_forwards_data_line_before_block_boundary() {
    let (tx, rx) = mpsc::channel::<Result<Bytes, std::io::Error>>(4);
    let mut body_stream = Box::pin(build_sse_body_stream(
        Vec::new(),
        rx,
        true,
        true,
        false,
        Duration::from_secs(60),
    ));

    let keepalive = tokio::time::timeout(Duration::from_millis(50), body_stream.next())
        .await
        .expect("initial keepalive should be immediate")
        .expect("stream should yield initial keepalive")
        .expect("initial keepalive should be ok");
    assert_eq!(keepalive.as_ref(), b": aether-keepalive\n\n");

    tx.send(Ok(Bytes::from_static(
        b"event: response.output_text.delta\n",
    )))
    .await
    .expect("event line should send");
    assert!(
        tokio::time::timeout(Duration::from_millis(20), body_stream.next())
            .await
            .is_err(),
        "event-only partial block should remain buffered"
    );

    tx.send(Ok(Bytes::from_static(
        b"data: {\"type\":\"response.output_text.delta\",\"delta\":\"hi\"}\n",
    )))
    .await
    .expect("data line should send");
    let data_chunk = tokio::time::timeout(Duration::from_millis(50), body_stream.next())
        .await
        .expect("data-bearing block should stream before terminator")
        .expect("stream should yield data-bearing block")
        .expect("data-bearing block should be ok");
    assert_eq!(
        data_chunk.as_ref(),
        b"event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"hi\"}\n"
    );

    tx.send(Ok(Bytes::from_static(b"\n")))
        .await
        .expect("terminator should send");
    let terminator = tokio::time::timeout(Duration::from_millis(50), body_stream.next())
        .await
        .expect("terminator should stream")
        .expect("stream should yield terminator")
        .expect("terminator should be ok");
    assert_eq!(terminator.as_ref(), b"\n");
}

#[tokio::test]
async fn sse_body_stream_uses_local_keepalive_when_prefetched_blocks_are_control_only() {
    let (_tx, rx) = mpsc::channel::<Result<Bytes, std::io::Error>>(1);
    let mut body_stream = Box::pin(build_sse_body_stream(
        vec![Bytes::from_static(b": upstream-keepalive\n\n")],
        rx,
        true,
        true,
        false,
        Duration::from_secs(60),
    ));

    let first = tokio::time::timeout(Duration::from_millis(50), body_stream.next())
        .await
        .expect("local keepalive should arrive")
        .expect("stream should yield local keepalive")
        .expect("local keepalive should be ok");
    assert_eq!(first.as_ref(), b": aether-keepalive\n\n");
}

#[tokio::test]
async fn execute_stream_from_frame_stream_does_not_finalize_rewritten_tool_call_after_midstream_error(
) {
    let usage_repository = Arc::new(InMemoryUsageReadRepository::default());
    let request_candidate_repository = Arc::new(InMemoryRequestCandidateRepository::default());
    let state = AppState::new()
        .expect("app state should build")
        .with_data_state_for_tests(
            crate::data::GatewayDataState::with_request_candidate_and_usage_repository_for_tests(
                Arc::clone(&request_candidate_repository),
                Arc::clone(&usage_repository),
            ),
        )
        .with_usage_runtime_for_tests(UsageRuntimeConfig {
            enabled: true,
            ..UsageRuntimeConfig::default()
        });
    let plan = ExecutionPlan {
        request_id: "req-responses-tool-midstream-error".into(),
        candidate_id: Some("cand-responses-tool-midstream-error".into()),
        provider_name: Some("openai".into()),
        provider_id: "provider-openai-responses".into(),
        endpoint_id: "endpoint-openai-responses".into(),
        key_id: "key-openai-responses".into(),
        method: "POST".into(),
        url: "https://api.openai.com/v1/responses".into(),
        headers: BTreeMap::from([
            ("content-type".into(), "application/json".into()),
            ("accept".into(), "text/event-stream".into()),
        ]),
        content_type: Some("application/json".into()),
        content_encoding: None,
        body: RequestBody::from_json(json!({
            "model": "gpt-5.5",
            "input": [],
            "stream": true
        })),
        stream: true,
        client_api_format: "claude:messages".into(),
        provider_api_format: "openai:responses".into(),
        model_name: Some("gpt-5.5".into()),
        proxy: None,
        transport_profile: None,
        timeouts: None,
    };
    let upstream_chunk = concat!(
        "event: response.created\n",
        "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_midstream_error\",\"model\":\"gpt-5.5\",\"status\":\"in_progress\"}}\n\n",
        "event: response.output_item.added\n",
        "data: {\"type\":\"response.output_item.added\",\"output_index\":0,\"item\":{\"type\":\"function_call\",\"id\":\"fc_1\",\"call_id\":\"call_1\",\"name\":\"lookup\",\"arguments\":\"\",\"status\":\"in_progress\"}}\n\n",
        "event: response.function_call_arguments.delta\n",
        "data: {\"type\":\"response.function_call_arguments.delta\",\"output_index\":0,\"item_id\":\"fc_1\",\"call_id\":\"call_1\",\"delta\":\"{\\\"query\\\":\\\"abc\"}\n\n"
    );
    let frame_stream = stream! {
        yield Ok::<Bytes, std::io::Error>(ndjson_frame(StreamFrame {
            frame_type: StreamFrameType::Headers,
            payload: StreamFramePayload::Headers {
                status_code: 200,
                headers: BTreeMap::from([(
                    "content-type".to_string(),
                    "text/event-stream".to_string(),
                )]),
                response_observation: None,
            },
        }));
        yield Ok::<Bytes, std::io::Error>(ndjson_frame(StreamFrame {
            frame_type: StreamFrameType::Data,
            payload: StreamFramePayload::Data {
                chunk_b64: None,
                text: Some(upstream_chunk.to_string()),
            },
        }));
        yield Ok::<Bytes, std::io::Error>(ndjson_frame(StreamFrame {
            frame_type: StreamFrameType::Error,
            payload: StreamFramePayload::Error {
                error: ExecutionError {
                    kind: ExecutionErrorKind::Internal,
                    phase: ExecutionPhase::StreamRead,
                    message: "error reading a body from connection: stream error received: unexpected internal error encountered".to_string(),
                    upstream_status: Some(200),
                    retryable: false,
                    failover_recommended: false,
                },
            },
        }));
    }
    .boxed();

    let response = execute_stream_from_frame_stream(
        &state,
        plan,
        "trace-responses-tool-midstream-error",
        &test_decision(),
        "openai_responses_stream",
        Some("openai_responses_stream_success".to_string()),
        Some(json!({
            "request_id": "req-responses-tool-midstream-error",
            "candidate_id": "cand-responses-tool-midstream-error",
            "candidate_index": 0,
            "retry_index": 0,
            "provider_api_format": "openai:responses",
            "client_api_format": "claude:messages",
            "needs_conversion": true,
        })),
        crate::clock::current_unix_ms(),
        Instant::now(),
        RequestStageTrace::from_env(),
        true,
        frame_stream,
        None,
    )
    .await
    .expect("execution should succeed")
    .expect("execution should return a client response");

    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("response body should read");
    let body_text = String::from_utf8(body.to_vec()).expect("body should be utf8");
    assert!(body_text.contains("event: content_block_start"));
    assert!(body_text.contains("event: content_block_delta"));
    assert!(body_text.contains("\"type\":\"tool_use\""));
    assert!(!body_text.contains("event: content_block_stop"));
    assert!(!body_text.contains("event: message_delta"));
    assert!(!body_text.contains("event: message_stop"));
    assert!(!body_text.contains("\"stop_reason\":\"tool_use\""));
    assert!(body_text.contains("\"error\""));
    assert!(body_text.contains("Execution runtime stream failed"));
    assert!(!body_text.contains("unexpected internal error encountered"));
    assert!(body_text.contains("data: [DONE]"));

    let candidates = tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let candidates = request_candidate_repository
                .list_by_request_id("req-responses-tool-midstream-error")
                .await
                .expect("request candidates should read");
            if candidates
                .first()
                .is_some_and(|candidate| candidate.status == RequestCandidateStatus::Failed)
            {
                break candidates;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("candidate should be marked failed");
    assert_eq!(candidates[0].status_code, Some(200));
    assert_eq!(candidates[0].error_type.as_deref(), Some("internal"));
}

#[tokio::test]
async fn openai_image_stream_ignores_plan_total_timeout() {
    let state = AppState::new().expect("app state should build");
    let plan = ExecutionPlan {
        request_id: "req-image-stream-timeout".into(),
        candidate_id: Some("cand-image-stream-timeout".into()),
        provider_name: Some("codex".into()),
        provider_id: "prov-1".into(),
        endpoint_id: "ep-1".into(),
        key_id: "key-1".into(),
        method: "POST".into(),
        url: "https://chatgpt.com/backend-api/codex/responses".into(),
        headers: BTreeMap::from([
            ("content-type".into(), "application/json".into()),
            ("accept".into(), "text/event-stream".into()),
        ]),
        content_type: Some("application/json".into()),
        content_encoding: None,
        body: RequestBody::from_json(json!({
            "model": "gpt-image-1",
            "prompt": "hello",
            "stream": true
        })),
        stream: true,
        client_api_format: "openai:image".into(),
        provider_api_format: "openai:image".into(),
        model_name: Some("gpt-image-1".into()),
        proxy: None,
        transport_profile: None,
        timeouts: Some(ExecutionTimeouts {
            total_ms: Some(25),
            ..ExecutionTimeouts::default()
        }),
    };
    let decision = GatewayControlDecision::synthetic(
        "/v1/images/generations",
        Some("ai_public".to_string()),
        Some("openai".to_string()),
        Some("image".to_string()),
        Some("openai:image".to_string()),
    )
    .with_execution_runtime_candidate(true);
    let frame_stream = stream! {
        yield Ok::<Bytes, std::io::Error>(Bytes::from_static(
            b"{\"type\":\"headers\",\"payload\":{\"kind\":\"headers\",\"status_code\":200,\"headers\":{\"content-type\":\"text/event-stream\"}}}\n",
        ));
        std::future::pending::<()>().await;
    }
    .boxed();

    let response = execute_stream_from_frame_stream(
        &state,
        plan,
        "trace-image-stream-timeout",
        &decision,
        "openai_image_stream",
        None,
        Some(json!({
            "provider_api_format": "openai:image",
            "client_api_format": "openai:image",
            "image_request": {
                "operation": "generate"
            }
        })),
        crate::clock::current_unix_ms(),
        Instant::now(),
        RequestStageTrace::from_env(),
        true,
        frame_stream,
        None,
    )
    .await
    .expect("execution should succeed")
    .expect("execution should return a client response");

    let mut body_stream = response.into_body().into_data_stream();
    let next_chunk = tokio::time::timeout(Duration::from_millis(100), body_stream.next()).await;
    assert!(
        next_chunk.is_err(),
        "stream total_ms must not synthesize a keepalive, image failure, or close the response body"
    );
}

#[tokio::test]
async fn execute_stream_from_frame_stream_treats_windsurf_connect_trailer_error_as_failure() {
    let usage_repository = Arc::new(InMemoryUsageReadRepository::default());
    let request_candidate_repository = Arc::new(InMemoryRequestCandidateRepository::default());
    let state = AppState::new()
        .expect("app state should build")
        .with_data_state_for_tests(
            crate::data::GatewayDataState::with_request_candidate_and_usage_repository_for_tests(
                Arc::clone(&request_candidate_repository),
                Arc::clone(&usage_repository),
            ),
        )
        .with_usage_runtime_for_tests(UsageRuntimeConfig {
            enabled: true,
            ..UsageRuntimeConfig::default()
        });
    let plan = ExecutionPlan {
        request_id: "req-windsurf-connect-error".into(),
        candidate_id: Some("cand-windsurf-connect-error".into()),
        provider_name: Some("windsurf".into()),
        provider_id: "provider-windsurf".into(),
        endpoint_id: "endpoint-windsurf-chat".into(),
        key_id: "key-windsurf".into(),
        method: "POST".into(),
        url:
            "https://server.codeium.com/exa.api_server_pb.ApiServerService/GetChatMessage?beta=true"
                .into(),
        headers: BTreeMap::from([
            ("content-type".into(), "application/connect+json".into()),
            ("accept".into(), "application/connect+json".into()),
        ]),
        content_type: Some("application/connect+json".into()),
        content_encoding: None,
        body: RequestBody::from_json(json!({
            "model": "claude-sonnet-4",
            "messages": [],
            "stream": true
        })),
        stream: true,
        client_api_format: "claude:messages".into(),
        provider_api_format: "openai:chat".into(),
        model_name: Some("claude-sonnet-4".into()),
        proxy: None,
        transport_profile: None,
        timeouts: None,
    };
    let state = state.with_data_state_for_tests(
        crate::data::GatewayDataState::with_request_candidate_and_usage_repository_for_tests(
            Arc::clone(&request_candidate_repository),
            Arc::clone(&usage_repository),
        )
        .with_provider_catalog_reader(Arc::new(provider_catalog_stop_429_for_plan(&plan)))
        .with_encryption_key_for_tests(DEV_FERNET_KEY),
    );
    let trailer_error = connect_json_frame(
        2,
        br#"{"error":{"code":"resource_exhausted","message":"an internal error occurred"}}"#,
    );
    let trailer_error_b64 = base64::engine::general_purpose::STANDARD.encode(trailer_error);
    let frame = format!(
        "{{\"type\":\"data\",\"payload\":{{\"kind\":\"data\",\"chunk_b64\":\"{trailer_error_b64}\"}}}}\n"
    );
    let frame_stream = stream! {
        yield Ok::<Bytes, std::io::Error>(Bytes::from_static(
            b"{\"type\":\"headers\",\"payload\":{\"kind\":\"headers\",\"status_code\":200,\"headers\":{\"content-type\":\"application/connect+json\"}}}\n",
        ));
        yield Ok::<Bytes, std::io::Error>(Bytes::from(frame));
        yield Ok::<Bytes, std::io::Error>(Bytes::from_static(
            b"{\"type\":\"eof\",\"payload\":{\"kind\":\"eof\"}}\n",
        ));
    }
    .boxed();

    let response = execute_stream_from_frame_stream(
        &state,
        plan,
        "trace-windsurf-connect-error",
        &test_decision(),
        "claude_chat_stream",
        Some("claude_chat_stream_success".to_string()),
        Some(json!({
            "request_id": "req-windsurf-connect-error",
            "candidate_id": "cand-windsurf-connect-error",
            "candidate_index": 0,
            "retry_index": 0,
            "provider_api_format": "openai:chat",
            "client_api_format": "claude:messages",
            "needs_conversion": true,
            "has_envelope": true,
            "envelope_name": "windsurf:GetChatMessage"
        })),
        crate::clock::current_unix_ms(),
        Instant::now(),
        RequestStageTrace::from_env(),
        true,
        frame_stream,
        None,
    )
    .await
    .expect("execution should succeed")
    .expect("execution should return a client response");

    let status = response.status();
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("response body should read");
    let body_json: Value =
        serde_json::from_slice(&body).expect("response body should decode as json");
    assert_eq!(status.as_u16(), 429);
    assert_eq!(body_json["type"], json!("error"));
    assert_eq!(body_json["error"]["type"], json!("rate_limit_error"));
    assert_eq!(body_json["error"]["code"], json!("resource_exhausted"));
    assert_eq!(
        body_json["error"]["message"],
        json!("an internal error occurred")
    );

    let candidates = tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let candidates = request_candidate_repository
                .list_by_request_id("req-windsurf-connect-error")
                .await
                .expect("request candidates should read");
            if candidates
                .first()
                .is_some_and(|candidate| candidate.status == RequestCandidateStatus::Failed)
            {
                break candidates;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("candidate should be marked failed");
    assert_eq!(candidates[0].status_code, Some(429));
    assert_eq!(
        candidates[0].error_type.as_deref(),
        Some("resource_exhausted")
    );
}

#[tokio::test]
async fn execute_stream_from_frame_stream_decodes_non_success_windsurf_connect_error_body() {
    let usage_repository = Arc::new(InMemoryUsageReadRepository::default());
    let request_candidate_repository = Arc::new(InMemoryRequestCandidateRepository::default());
    let state = AppState::new()
        .expect("app state should build")
        .with_data_state_for_tests(
            crate::data::GatewayDataState::with_request_candidate_and_usage_repository_for_tests(
                Arc::clone(&request_candidate_repository),
                Arc::clone(&usage_repository),
            ),
        )
        .with_usage_runtime_for_tests(UsageRuntimeConfig {
            enabled: true,
            ..UsageRuntimeConfig::default()
        });
    let plan = ExecutionPlan {
        request_id: "req-windsurf-connect-429".into(),
        candidate_id: Some("cand-windsurf-connect-429".into()),
        provider_name: Some("windsurf".into()),
        provider_id: "provider-windsurf".into(),
        endpoint_id: "endpoint-windsurf-chat".into(),
        key_id: "key-windsurf".into(),
        method: "POST".into(),
        url:
            "https://server.codeium.com/exa.api_server_pb.ApiServerService/GetChatMessage?beta=true"
                .into(),
        headers: BTreeMap::from([
            ("content-type".into(), "application/connect+json".into()),
            ("accept".into(), "application/connect+json".into()),
        ]),
        content_type: Some("application/connect+json".into()),
        content_encoding: None,
        body: RequestBody::from_json(json!({
            "model": "claude-sonnet-4",
            "messages": [],
            "stream": true
        })),
        stream: true,
        client_api_format: "claude:messages".into(),
        provider_api_format: "openai:chat".into(),
        model_name: Some("claude-sonnet-4".into()),
        proxy: None,
        transport_profile: None,
        timeouts: None,
    };
    let state = state.with_data_state_for_tests(
        crate::data::GatewayDataState::with_request_candidate_and_usage_repository_for_tests(
            Arc::clone(&request_candidate_repository),
            Arc::clone(&usage_repository),
        )
        .with_provider_catalog_reader(Arc::new(provider_catalog_stop_429_for_plan(&plan)))
        .with_encryption_key_for_tests(DEV_FERNET_KEY),
    );
    let connect_error = connect_json_frame(
        2,
        br#"{"error":{"code":"resource_exhausted","message":"quota exhausted"}}"#,
    );
    let connect_error_b64 = base64::engine::general_purpose::STANDARD.encode(connect_error);
    let frame_stream = stream! {
        yield Ok::<Bytes, std::io::Error>(Bytes::from_static(
            b"{\"type\":\"headers\",\"payload\":{\"kind\":\"headers\",\"status_code\":429,\"headers\":{\"content-type\":\"application/connect+json\"}}}\n",
        ));
        yield Ok::<Bytes, std::io::Error>(Bytes::from(format!(
            "{{\"type\":\"data\",\"payload\":{{\"kind\":\"data\",\"chunk_b64\":\"{connect_error_b64}\"}}}}\n"
        )));
        yield Ok::<Bytes, std::io::Error>(Bytes::from_static(
            b"{\"type\":\"eof\",\"payload\":{\"kind\":\"eof\"}}\n",
        ));
    }
    .boxed();

    let response = execute_stream_from_frame_stream(
        &state,
        plan,
        "trace-windsurf-connect-429",
        &test_decision(),
        "claude_chat_stream",
        Some("claude_chat_stream_success".to_string()),
        Some(json!({
            "request_id": "req-windsurf-connect-429",
            "candidate_id": "cand-windsurf-connect-429",
            "candidate_index": 0,
            "retry_index": 0,
            "provider_api_format": "openai:chat",
            "client_api_format": "claude:messages",
            "needs_conversion": true,
            "has_envelope": true,
            "envelope_name": "windsurf:GetChatMessage"
        })),
        crate::clock::current_unix_ms(),
        Instant::now(),
        RequestStageTrace::from_env(),
        true,
        frame_stream,
        None,
    )
    .await
    .expect("execution should succeed")
    .expect("execution should return a client response");

    assert_eq!(response.status().as_u16(), 429);
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("response body should read");
    let body_json: Value =
        serde_json::from_slice(&body).expect("response body should decode as json");
    assert_eq!(body_json["type"], json!("error"));
    assert_eq!(body_json["error"]["type"], json!("rate_limit_error"));
    assert_eq!(body_json["error"]["code"], json!("resource_exhausted"));
    assert_eq!(body_json["error"]["message"], json!("quota exhausted"));

    let record = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if let Some(usage) = usage_repository
                .find_by_request_id("req-windsurf-connect-429")
                .await
                .expect("usage should read")
                .filter(|usage| usage.status == "failed")
            {
                break usage;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("usage should be written");
    assert_eq!(record.status_code, Some(429));
    assert!(record.response_body.is_none());
    assert!(record.response_body_ref.is_none());
    assert!(record.client_response_body.is_none());
    assert!(record.client_response_body_ref.is_none());
}

#[tokio::test]
async fn execute_stream_from_frame_stream_honors_client_disconnect_policy() {
    for cancel_on_client_disconnect in [false, true] {
        let usage_repository = Arc::new(InMemoryUsageReadRepository::default());
        let request_candidate_repository = Arc::new(InMemoryRequestCandidateRepository::default());
        let state = AppState::new()
        .expect("app state should build")
        .with_data_state_for_tests(
            crate::data::GatewayDataState::with_request_candidate_and_usage_repository_for_tests(
                Arc::clone(&request_candidate_repository),
                Arc::clone(&usage_repository),
            ),
        )
        .with_usage_runtime_for_tests(UsageRuntimeConfig {
            enabled: true,
            ..UsageRuntimeConfig::default()
        });
        let plan = ExecutionPlan {
            request_id: "req-client-drop-cancels-upstream".into(),
            candidate_id: Some("cand-client-drop-cancels-upstream".into()),
            provider_name: Some("openai".into()),
            provider_id: "prov-1".into(),
            endpoint_id: "ep-1".into(),
            key_id: "key-1".into(),
            method: "POST".into(),
            url: "https://example.com/v1/chat/completions".into(),
            headers: BTreeMap::from([
                ("content-type".into(), "application/json".into()),
                ("accept".into(), "text/event-stream".into()),
            ]),
            content_type: Some("application/json".into()),
            content_encoding: None,
            body: RequestBody::from_json(json!({
                "model": "gpt-5.4",
                "messages": [],
                "stream": true
            })),
            stream: true,
            client_api_format: "openai:chat".into(),
            provider_api_format: "openai:chat".into(),
            model_name: Some("gpt-5.4".into()),
            proxy: None,
            transport_profile: None,
            timeouts: None,
        };
        let release_terminal = Arc::new(Notify::new());
        let terminal_frame_drained = Arc::new(Notify::new());
        let release_terminal_for_stream = Arc::clone(&release_terminal);
        let terminal_frame_drained_for_stream = Arc::clone(&terminal_frame_drained);
        let frame_stream = stream! {
        yield Ok::<Bytes, std::io::Error>(Bytes::from_static(
            b"{\"type\":\"headers\",\"payload\":{\"kind\":\"headers\",\"status_code\":200,\"headers\":{\"content-type\":\"text/event-stream\"}}}\n",
        ));
        yield Ok::<Bytes, std::io::Error>(Bytes::from_static(
            b"{\"type\":\"data\",\"payload\":{\"kind\":\"data\",\"text\":\"data: {\\\"id\\\":\\\"first\\\",\\\"choices\\\":[{\\\"index\\\":0,\\\"delta\\\":{\\\"content\\\":\\\"hello\\\"}}]}\\n\\n\"}}\n",
        ));
        release_terminal_for_stream.notified().await;
        yield Ok::<Bytes, std::io::Error>(Bytes::from_static(
            b"{\"type\":\"data\",\"payload\":{\"kind\":\"data\",\"text\":\"data: {\\\"id\\\":\\\"terminal\\\",\\\"object\\\":\\\"chat.completion.chunk\\\",\\\"model\\\":\\\"gpt-5.4\\\",\\\"choices\\\":[{\\\"index\\\":0,\\\"delta\\\":{},\\\"finish_reason\\\":\\\"stop\\\"}],\\\"usage\\\":{\\\"prompt_tokens\\\":7,\\\"completion_tokens\\\":11,\\\"total_tokens\\\":18}}\\n\\ndata: [DONE]\\n\\n\"}}\n",
        ));
        terminal_frame_drained_for_stream.notify_one();
    }
    .boxed();

        let response = crate::request_lifecycle::run_request(async move {
            crate::request_lifecycle::configure_client_disconnect(
                aether_routing_core::RoutingExecutionPolicy {
                    cancel_on_client_disconnect,
                    ..Default::default()
                },
            );
            execute_stream_from_frame_stream(
                &state,
                plan,
                "trace-client-drop-cancels-upstream",
                &test_decision(),
                "openai_chat_stream",
                None,
                Some(json!({
                    "request_id": "req-client-drop-cancels-upstream",
                    "candidate_id": "cand-client-drop-cancels-upstream",
                    "candidate_index": 0,
                    "retry_index": 0,
                    "provider_api_format": "openai:chat",
                    "client_api_format": "openai:chat"
                })),
                crate::clock::current_unix_ms(),
                Instant::now(),
                RequestStageTrace::from_env(),
                true,
                frame_stream,
                None,
            )
            .await
            .map(|response| response.expect("execution should return a client response"))
        })
        .await
        .expect("execution should succeed");

        let mut body_stream = response.into_body().into_data_stream();
        let first = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let chunk = body_stream
                    .next()
                    .await
                    .expect("body should yield first chunk")
                    .expect("first chunk should be ok");
                if chunk.as_ref() != b": aether-keepalive\n\n" {
                    break chunk;
                }
            }
        })
        .await
        .expect("first business chunk should arrive");
        assert_eq!(
        first.as_ref(),
        b"data: {\"id\":\"first\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hello\"}}]}\n\n"
    );
        tokio::time::sleep(Duration::from_millis(30)).await;
        drop(body_stream);
        if !cancel_on_client_disconnect {
            release_terminal.notify_one();
        }
        let expected_candidate_status = if cancel_on_client_disconnect {
            RequestCandidateStatus::Cancelled
        } else {
            RequestCandidateStatus::Success
        };
        let expected_usage_status = if cancel_on_client_disconnect {
            "cancelled"
        } else {
            "completed"
        };
        let candidates = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let candidates = request_candidate_repository
                    .list_by_request_id("req-client-drop-cancels-upstream")
                    .await
                    .expect("request candidates should read");
                if candidates
                    .first()
                    .is_some_and(|candidate| candidate.status == expected_candidate_status)
                {
                    break candidates;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("candidate should be marked cancelled");
        assert_eq!(
            candidates[0].status_code,
            Some(if cancel_on_client_disconnect {
                499
            } else {
                200
            })
        );
        assert_eq!(
            candidates[0].error_type.as_deref(),
            cancel_on_client_disconnect.then_some("downstream_disconnect")
        );

        let stored_usage = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let usage = usage_repository
                    .find_by_request_id("req-client-drop-cancels-upstream")
                    .await
                    .expect("usage should read");
                if usage
                    .as_ref()
                    .is_some_and(|usage| usage.status == expected_usage_status)
                {
                    break usage.expect("cancelled usage should exist");
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("usage should be marked cancelled");
        if !cancel_on_client_disconnect {
            assert_ne!(stored_usage.billing_status, "void");
            assert_eq!(stored_usage.status_code, Some(200));
            assert_eq!(stored_usage.input_tokens, 7);
            assert_eq!(stored_usage.output_tokens, 11);
            assert_eq!(stored_usage.total_tokens, 18);
            continue;
        }
        assert_eq!(stored_usage.billing_status, "void");
        assert_eq!(stored_usage.status_code, Some(499));
        assert_eq!(stored_usage.input_tokens, 0);
        assert_eq!(stored_usage.output_tokens, 0);
        assert_eq!(stored_usage.total_tokens, 0);
        let first_byte_time_ms = stored_usage
            .first_byte_time_ms
            .expect("cancelled stream should retain first byte time");
        let response_time_ms = stored_usage
            .response_time_ms
            .expect("cancelled stream should record terminal duration");
        assert!(
            response_time_ms > first_byte_time_ms,
            "terminal duration should include time after the first byte"
        );

        release_terminal.notify_one();
        assert!(
            tokio::time::timeout(
                Duration::from_millis(100),
                terminal_frame_drained.notified()
            )
            .await
            .is_err(),
            "upstream frame stream should stop when the client disconnects"
        );
    }
}

#[tokio::test]
async fn split_done_then_downstream_close_is_recorded_success() {
    let usage_repository = Arc::new(InMemoryUsageReadRepository::default());
    let request_candidate_repository = Arc::new(InMemoryRequestCandidateRepository::default());
    let state = AppState::new()
        .expect("app state should build")
        .with_data_state_for_tests(
            crate::data::GatewayDataState::with_request_candidate_and_usage_repository_for_tests(
                Arc::clone(&request_candidate_repository),
                Arc::clone(&usage_repository),
            ),
        )
        .with_usage_runtime_for_tests(UsageRuntimeConfig {
            enabled: true,
            ..UsageRuntimeConfig::default()
        });
    let plan = ExecutionPlan {
        request_id: "req-split-done-close-success".into(),
        candidate_id: Some("cand-split-done-close-success".into()),
        provider_name: Some("openai".into()),
        provider_id: "prov-1".into(),
        endpoint_id: "ep-1".into(),
        key_id: "key-1".into(),
        method: "POST".into(),
        url: "https://example.com/v1/chat/completions".into(),
        headers: BTreeMap::from([
            ("content-type".into(), "application/json".into()),
            ("accept".into(), "text/event-stream".into()),
        ]),
        content_type: Some("application/json".into()),
        content_encoding: None,
        body: RequestBody::from_json(json!({
            "model": "gpt-5.4",
            "messages": [],
            "stream": true
        })),
        stream: true,
        client_api_format: "openai:chat".into(),
        provider_api_format: "openai:chat".into(),
        model_name: Some("gpt-5.4".into()),
        proxy: None,
        transport_profile: None,
        timeouts: None,
    };
    let release_eof = Arc::new(Notify::new());
    let release_eof_for_stream = Arc::clone(&release_eof);
    let frame_stream = stream! {
        yield Ok::<Bytes, std::io::Error>(Bytes::from_static(
            b"{\"type\":\"headers\",\"payload\":{\"kind\":\"headers\",\"status_code\":200,\"headers\":{\"content-type\":\"text/event-stream\"}}}\n",
        ));
        yield Ok::<Bytes, std::io::Error>(Bytes::from_static(
            b"{\"type\":\"data\",\"payload\":{\"kind\":\"data\",\"text\":\"data: {\\\"id\\\":\\\"first\\\",\\\"object\\\":\\\"chat.completion.chunk\\\",\\\"model\\\":\\\"gpt-5.4\\\",\\\"choices\\\":[{\\\"index\\\":0,\\\"delta\\\":{\\\"content\\\":\\\"hi\\\"},\\\"finish_reason\\\":null}]}\\n\\n\"}}\n",
        ));
        yield Ok::<Bytes, std::io::Error>(Bytes::from_static(
            b"{\"type\":\"data\",\"payload\":{\"kind\":\"data\",\"text\":\"data: {\\\"id\\\":\\\"terminal\\\",\\\"object\\\":\\\"chat.completion.chunk\\\",\\\"model\\\":\\\"gpt-5.4\\\",\\\"choices\\\":[{\\\"index\\\":0,\\\"delta\\\":{},\\\"finish_reason\\\":\\\"stop\\\"}],\\\"usage\\\":{\\\"prompt_tokens\\\":7,\\\"completion_tokens\\\":11,\\\"total_tokens\\\":18}}\\n\\n\"}}\n",
        ));
        yield Ok::<Bytes, std::io::Error>(Bytes::from_static(
            b"{\"type\":\"data\",\"payload\":{\"kind\":\"data\",\"text\":\"data: [DO\"}}\n",
        ));
        yield Ok::<Bytes, std::io::Error>(Bytes::from_static(
            b"{\"type\":\"data\",\"payload\":{\"kind\":\"data\",\"text\":\"NE]\\n\\n\"}}\n",
        ));
        release_eof_for_stream.notified().await;
    }
    .boxed();

    let response = execute_stream_from_frame_stream(
        &state,
        plan,
        "trace-split-done-close-success",
        &test_decision(),
        "openai_chat_stream",
        None,
        Some(json!({
            "request_id": "req-split-done-close-success",
            "candidate_id": "cand-split-done-close-success",
            "candidate_index": 0,
            "retry_index": 0,
            "provider_api_format": "openai:chat",
            "client_api_format": "openai:chat"
        })),
        crate::clock::current_unix_ms(),
        Instant::now(),
        RequestStageTrace::from_env(),
        true,
        frame_stream,
        None,
    )
    .await
    .expect("execution should succeed")
    .expect("execution should return a client response");

    let mut body_stream = response.into_body().into_data_stream();
    let mut body = Vec::new();
    tokio::time::timeout(Duration::from_secs(1), async {
        while !String::from_utf8_lossy(&body).contains("data: [DONE]") {
            let chunk = body_stream
                .next()
                .await
                .expect("body should yield until done")
                .expect("chunk should be ok");
            body.extend_from_slice(&chunk);
        }
    })
    .await
    .expect("final DONE should arrive");
    drop(body_stream);
    release_eof.notify_one();

    let candidates = tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let candidates = request_candidate_repository
                .list_by_request_id("req-split-done-close-success")
                .await
                .expect("request candidates should read");
            if candidates
                .first()
                .is_some_and(|candidate| candidate.status == RequestCandidateStatus::Success)
            {
                break candidates;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("candidate should be marked success");
    assert_eq!(candidates[0].status_code, Some(200));

    let stored_usage = tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let usage = usage_repository
                .find_by_request_id("req-split-done-close-success")
                .await
                .expect("usage should read");
            if usage
                .as_ref()
                .is_some_and(|usage| usage.status == "completed")
            {
                break usage.expect("completed usage should exist");
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("usage should be marked completed");
    assert_eq!(stored_usage.status_code, Some(200));
    assert_eq!(stored_usage.input_tokens, 7);
    assert_eq!(stored_usage.output_tokens, 11);
    assert_eq!(stored_usage.total_tokens, 18);
}

#[tokio::test]
async fn image_stream_downstream_close_after_done_is_recorded_success() {
    let usage_repository = Arc::new(InMemoryUsageReadRepository::default());
    let request_candidate_repository = Arc::new(InMemoryRequestCandidateRepository::default());
    let state = AppState::new()
        .expect("app state should build")
        .with_data_state_for_tests(
            crate::data::GatewayDataState::with_request_candidate_and_usage_repository_for_tests(
                Arc::clone(&request_candidate_repository),
                Arc::clone(&usage_repository),
            ),
        )
        .with_usage_runtime_for_tests(UsageRuntimeConfig {
            enabled: true,
            ..UsageRuntimeConfig::default()
        });
    let plan = ExecutionPlan {
        request_id: "req-image-done-close-success".into(),
        candidate_id: Some("cand-image-done-close-success".into()),
        provider_name: Some("openai".into()),
        provider_id: "prov-1".into(),
        endpoint_id: "ep-1".into(),
        key_id: "key-1".into(),
        method: "POST".into(),
        url: "https://example.com/v1/images/generations".into(),
        headers: BTreeMap::from([("accept".into(), "text/event-stream".into())]),
        content_type: Some("application/json".into()),
        content_encoding: None,
        body: RequestBody::from_json(json!({
            "model": "gpt-image-2",
            "prompt": "draw a small image",
            "stream": true
        })),
        stream: true,
        client_api_format: "openai:chat".into(),
        provider_api_format: "openai:image".into(),
        model_name: Some("gpt-image-2".into()),
        proxy: None,
        transport_profile: None,
        timeouts: None,
    };
    let frame_stream = stream! {
        yield Ok::<Bytes, std::io::Error>(Bytes::from_static(
            b"{\"type\":\"headers\",\"payload\":{\"kind\":\"headers\",\"status_code\":200,\"headers\":{\"content-type\":\"text/event-stream\"}}}\n",
        ));
        yield Ok::<Bytes, std::io::Error>(Bytes::from_static(
            b"{\"type\":\"data\",\"payload\":{\"kind\":\"data\",\"text\":\"event: response.output_item.done\\ndata: {\\\"type\\\":\\\"response.output_item.done\\\",\\\"output_index\\\":0,\\\"item\\\":{\\\"id\\\":\\\"ig_1\\\",\\\"type\\\":\\\"image_generation_call\\\",\\\"result\\\":\\\"aGVsbG8=\\\"}}\\n\\nevent: response.completed\\ndata: {\\\"type\\\":\\\"response.completed\\\",\\\"response\\\":{\\\"id\\\":\\\"resp_1\\\",\\\"model\\\":\\\"gpt-image-2\\\",\\\"status\\\":\\\"completed\\\",\\\"usage\\\":null}}\\n\\n\"}}\n",
        ));
        std::future::pending::<()>().await;
    }
    .boxed();

    let response = execute_stream_from_frame_stream(
        &state,
        plan,
        "trace-image-done-close-success",
        &test_decision(),
        "openai_chat_stream",
        Some("openai_chat_stream_success".to_string()),
        Some(json!({
            "request_id": "req-image-done-close-success",
            "candidate_id": "cand-image-done-close-success",
            "candidate_index": 0,
            "retry_index": 0,
            "provider_api_format": "openai:image",
            "client_api_format": "openai:chat",
            "image_request": {
                "size": "1024x1024",
                "quality": "medium"
            }
        })),
        crate::clock::current_unix_ms(),
        Instant::now(),
        RequestStageTrace::from_env(),
        true,
        frame_stream,
        None,
    )
    .await
    .expect("execution should succeed")
    .expect("execution should return a client response");

    let mut body_stream = response.into_body().into_data_stream();
    let mut body = Vec::new();
    tokio::time::timeout(Duration::from_secs(1), async {
        while !String::from_utf8_lossy(&body).contains("data: [DONE]") {
            let chunk = body_stream
                .next()
                .await
                .expect("body should yield until done")
                .expect("chunk should be ok");
            body.extend_from_slice(&chunk);
        }
    })
    .await
    .expect("final DONE should arrive");
    drop(body_stream);

    let candidates = tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let candidates = request_candidate_repository
                .list_by_request_id("req-image-done-close-success")
                .await
                .expect("request candidates should read");
            if candidates
                .first()
                .is_some_and(|candidate| candidate.status == RequestCandidateStatus::Success)
            {
                break candidates;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("candidate should be marked success");
    assert_eq!(candidates[0].status_code, Some(200));

    let stored_usage = tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let usage = usage_repository
                .find_by_request_id("req-image-done-close-success")
                .await
                .expect("usage should read");
            if usage
                .as_ref()
                .is_some_and(|usage| usage.status == "completed")
            {
                break usage.expect("completed usage should exist");
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("usage should be marked completed");
    assert_eq!(stored_usage.status_code, Some(200));
    assert!(stored_usage.total_tokens > 0);
}

#[tokio::test]
async fn execute_execution_runtime_stream_records_first_data_as_streaming_before_terminal_telemetry(
) {
    let listener = crate::test_support::bind_loopback_listener()
        .await
        .expect("listener should bind");
    let addr = listener.local_addr().expect("local addr should resolve");
    let first_data_seen = Arc::new(Notify::new());
    let release_terminal = Arc::new(Notify::new());
    let first_data_seen_for_route = Arc::clone(&first_data_seen);
    let release_terminal_for_route = Arc::clone(&release_terminal);
    let server = tokio::spawn(async move {
        let app = Router::new().route(
            "/v1/execute/stream",
            any(move |_request: Request| {
                let first_data_seen = Arc::clone(&first_data_seen_for_route);
                let release_terminal = Arc::clone(&release_terminal_for_route);
                async move {
                    let frames = stream! {
                        yield Ok::<Bytes, Infallible>(Bytes::from_static(
                            b"{\"type\":\"headers\",\"payload\":{\"kind\":\"headers\",\"status_code\":200,\"headers\":{\"content-type\":\"text/event-stream\"}}}\n",
                        ));
                        tokio::time::sleep(Duration::from_millis(10)).await;
                        yield Ok::<Bytes, Infallible>(Bytes::from_static(
                            b"{\"type\":\"data\",\"payload\":{\"kind\":\"data\",\"text\":\"event: response.output_text.delta\\ndata: {\\\"type\\\":\\\"response.output_text.delta\\\",\\\"delta\\\":\\\"hi\\\"}\\n\\n\"}}\n",
                        ));
                        first_data_seen.notify_one();
                        release_terminal.notified().await;
                        yield Ok::<Bytes, Infallible>(Bytes::from_static(
                            b"{\"type\":\"telemetry\",\"payload\":{\"kind\":\"telemetry\",\"telemetry\":{\"ttfb_ms\":123,\"elapsed_ms\":456}}}\n",
                        ));
                        yield Ok::<Bytes, Infallible>(Bytes::from_static(
                            b"{\"type\":\"eof\",\"payload\":{\"kind\":\"eof\"}}\n",
                        ));
                    };
                    let mut response = axum::http::Response::new(Body::from_stream(frames));
                    response.headers_mut().insert(
                        header::CONTENT_TYPE,
                        HeaderValue::from_static("application/x-ndjson"),
                    );
                    response
                }
            }),
        );
        axum::serve(listener, app)
            .await
            .expect("server should start");
    });

    let usage_repository = Arc::new(InMemoryUsageReadRepository::default());
    let request_candidate_repository = Arc::new(InMemoryRequestCandidateRepository::default());
    let state = AppState::new()
        .expect("app state should build")
        .with_data_state_for_tests(
            crate::data::GatewayDataState::with_request_candidate_and_usage_repository_for_tests(
                Arc::clone(&request_candidate_repository),
                Arc::clone(&usage_repository),
            ),
        )
        .with_usage_runtime_for_tests(UsageRuntimeConfig {
            enabled: true,
            ..UsageRuntimeConfig::default()
        })
        .with_execution_runtime_override_base_url(format!("http://{addr}"));
    let plan = ExecutionPlan {
        request_id: "req-live-stream-first-data".into(),
        candidate_id: Some("cand-live-stream-first-data".into()),
        provider_name: Some("openai".into()),
        provider_id: "prov-1".into(),
        endpoint_id: "ep-1".into(),
        key_id: "key-1".into(),
        method: "POST".into(),
        url: "https://chatgpt.com/backend-api/codex/responses".into(),
        headers: BTreeMap::from([
            ("content-type".into(), "application/json".into()),
            ("accept".into(), "text/event-stream".into()),
        ]),
        content_type: Some("application/json".into()),
        content_encoding: None,
        body: RequestBody::from_json(json!({
            "model": "gpt-5.4",
            "input": "hello",
            "stream": true
        })),
        stream: true,
        client_api_format: "openai:responses".into(),
        provider_api_format: "openai:responses".into(),
        model_name: Some("gpt-5.4".into()),
        proxy: None,
        transport_profile: None,
        timeouts: Some(ExecutionTimeouts {
            connect_ms: Some(5_000),
            total_ms: Some(5_000),
            ..ExecutionTimeouts::default()
        }),
    };
    let decision = GatewayControlDecision::synthetic(
        "/v1/responses",
        Some("ai_public".to_string()),
        Some("openai".to_string()),
        Some("cli".to_string()),
        Some("openai:responses".to_string()),
    )
    .with_execution_runtime_candidate(true);

    let response = execute_execution_runtime_stream(
        &state,
        plan,
        "trace-live-stream-first-data",
        &decision,
        "openai_responses_stream",
        None,
        Some(json!({
            "provider_api_format": "openai:responses",
            "client_api_format": "openai:responses",
        })),
    )
    .await
    .expect("execution should succeed")
    .expect("execution should return a client response");

    first_data_seen.notified().await;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    let live_usage = loop {
        let usage = usage_repository
            .find_by_request_id("req-live-stream-first-data")
            .await
            .expect("usage should read");
        if usage
            .as_ref()
            .is_some_and(|usage| usage.status == "streaming" && usage.first_byte_time_ms.is_some())
        {
            break usage.expect("live usage should exist");
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "usage should record streaming status with first byte before terminal telemetry"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    };

    assert_eq!(live_usage.status, "streaming");
    assert!(live_usage.first_byte_time_ms.is_some());

    release_terminal.notify_one();
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("response body should read");
    let text = String::from_utf8(body.to_vec()).expect("response body should be utf8");
    assert!(text.contains("response.output_text.delta"));

    server.abort();
}

#[tokio::test]
async fn execute_execution_runtime_stream_records_first_stream_event_before_visible_text() {
    let listener = crate::test_support::bind_loopback_listener()
        .await
        .expect("listener should bind");
    let addr = listener.local_addr().expect("local addr should resolve");
    let first_event_seen = Arc::new(Notify::new());
    let release_text = Arc::new(Notify::new());
    let text_seen = Arc::new(Notify::new());
    let release_terminal = Arc::new(Notify::new());
    let first_event_seen_for_route = Arc::clone(&first_event_seen);
    let release_text_for_route = Arc::clone(&release_text);
    let text_seen_for_route = Arc::clone(&text_seen);
    let release_terminal_for_route = Arc::clone(&release_terminal);
    let server = tokio::spawn(async move {
        let app = Router::new().route(
            "/v1/execute/stream",
            any(move |_request: Request| {
                let first_event_seen = Arc::clone(&first_event_seen_for_route);
                let release_text = Arc::clone(&release_text_for_route);
                let text_seen = Arc::clone(&text_seen_for_route);
                let release_terminal = Arc::clone(&release_terminal_for_route);
                async move {
                    let frames = stream! {
                        yield Ok::<Bytes, Infallible>(Bytes::from_static(
                            b"{\"type\":\"headers\",\"payload\":{\"kind\":\"headers\",\"status_code\":200,\"headers\":{\"content-type\":\"text/event-stream\"}}}\n",
                        ));
                        yield Ok::<Bytes, Infallible>(Bytes::from_static(
                            b"{\"type\":\"data\",\"payload\":{\"kind\":\"data\",\"text\":\"\"}}\n",
                        ));
                        yield Ok::<Bytes, Infallible>(Bytes::from_static(
                            b"{\"type\":\"telemetry\",\"payload\":{\"kind\":\"telemetry\",\"telemetry\":{\"ttfb_ms\":11,\"elapsed_ms\":12}}}\n",
                        ));
                        first_event_seen.notify_one();
                        release_text.notified().await;
                        yield Ok::<Bytes, Infallible>(Bytes::from_static(
                            b"{\"type\":\"data\",\"payload\":{\"kind\":\"data\",\"text\":\"data: {\\\"choices\\\":[{\\\"delta\\\":{\\\"content\\\":\\\"hello\\\"}}]}\\n\\n\"}}\n",
                        ));
                        text_seen.notify_one();
                        release_terminal.notified().await;
                        yield Ok::<Bytes, Infallible>(Bytes::from_static(
                            b"{\"type\":\"telemetry\",\"payload\":{\"kind\":\"telemetry\",\"telemetry\":{\"elapsed_ms\":50}}}\n",
                        ));
                        yield Ok::<Bytes, Infallible>(Bytes::from_static(
                            b"{\"type\":\"eof\",\"payload\":{\"kind\":\"eof\"}}\n",
                        ));
                    };
                    let mut response = axum::http::Response::new(Body::from_stream(frames));
                    response.headers_mut().insert(
                        header::CONTENT_TYPE,
                        HeaderValue::from_static("application/x-ndjson"),
                    );
                    response
                }
            }),
        );
        axum::serve(listener, app)
            .await
            .expect("server should start");
    });

    let usage_repository = Arc::new(InMemoryUsageReadRepository::default());
    let request_candidate_repository = Arc::new(InMemoryRequestCandidateRepository::default());
    let state = AppState::new()
        .expect("app state should build")
        .with_data_state_for_tests(
            crate::data::GatewayDataState::with_request_candidate_and_usage_repository_for_tests(
                Arc::clone(&request_candidate_repository),
                Arc::clone(&usage_repository),
            ),
        )
        .with_usage_runtime_for_tests(UsageRuntimeConfig {
            enabled: true,
            ..UsageRuntimeConfig::default()
        })
        .with_execution_runtime_override_base_url(format!("http://{addr}"));
    let plan = ExecutionPlan {
        request_id: "req-live-stream-first-event".into(),
        candidate_id: Some("cand-live-stream-first-event".into()),
        provider_name: Some("openai".into()),
        provider_id: "prov-1".into(),
        endpoint_id: "ep-1".into(),
        key_id: "key-1".into(),
        method: "POST".into(),
        url: "https://api.openai.com/v1/chat/completions".into(),
        headers: BTreeMap::from([
            ("content-type".into(), "application/json".into()),
            ("accept".into(), "text/event-stream".into()),
        ]),
        content_type: Some("application/json".into()),
        content_encoding: None,
        body: RequestBody::from_json(json!({
            "model": "gpt-5.4",
            "messages": [{"role": "user", "content": "hello"}],
            "stream": true
        })),
        stream: true,
        client_api_format: "openai:chat".into(),
        provider_api_format: "openai:chat".into(),
        model_name: Some("gpt-5.4".into()),
        proxy: None,
        transport_profile: None,
        timeouts: Some(ExecutionTimeouts {
            connect_ms: Some(5_000),
            total_ms: Some(5_000),
            ..ExecutionTimeouts::default()
        }),
    };
    let decision = GatewayControlDecision::synthetic(
        "/v1/chat/completions",
        Some("ai_public".to_string()),
        Some("openai".to_string()),
        Some("chat".to_string()),
        Some("openai:chat".to_string()),
    )
    .with_execution_runtime_candidate(true);

    let execution_task = tokio::spawn(async move {
        execute_execution_runtime_stream(
            &state,
            plan,
            "trace-live-stream-first-event",
            &decision,
            "openai_chat_stream",
            None,
            Some(json!({
                "provider_api_format": "openai:chat",
                "client_api_format": "openai:chat",
            })),
        )
        .await
    });

    first_event_seen.notified().await;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    let first_event_usage = loop {
        let usage = usage_repository
            .find_by_request_id("req-live-stream-first-event")
            .await
            .expect("usage should read");
        if usage
            .as_ref()
            .is_some_and(|usage| usage.status == "streaming" && usage.first_byte_time_ms.is_some())
        {
            break usage.expect("streaming usage should exist");
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "usage should record first byte on the first upstream stream event"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    };
    assert!(first_event_usage.first_byte_time_ms.is_some());
    assert!(!execution_task.is_finished());

    release_text.notify_one();
    text_seen.notified().await;
    let response = tokio::time::timeout(Duration::from_secs(1), execution_task)
        .await
        .expect("semantic text should commit the response")
        .expect("execution task should complete")
        .expect("execution should succeed")
        .expect("execution should return a client response");

    release_terminal.notify_one();
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("response body should read");
    let text = String::from_utf8(body.to_vec()).expect("response body should be utf8");
    assert!(text.contains("\"content\":\"hello\""));

    server.abort();
}

#[tokio::test]
async fn execute_execution_runtime_stream_bridges_sync_json_body_from_remote_runtime_to_sse() {
    let listener = crate::test_support::bind_loopback_listener()
        .await
        .expect("listener should bind");
    let addr = listener.local_addr().expect("local addr should resolve");
    let server = tokio::spawn(async move {
        let app = Router::new().route(
            "/v1/execute/stream",
            any(|_request: Request| async move {
                let frames = concat!(
                    "{\"type\":\"headers\",\"payload\":{\"kind\":\"headers\",\"status_code\":200,\"headers\":{\"content-type\":\"application/json\"}}}\n",
                    "{\"type\":\"data\",\"payload\":{\"kind\":\"data\",\"text\":\"{\\\"id\\\":\\\"resp-remote-runtime-sync-json-123\\\",\\\"object\\\":\\\"response\\\",\\\"model\\\":\\\"gpt-5.4\\\",\\\"status\\\":\\\"completed\\\",\\\"output\\\":[{\\\"type\\\":\\\"message\\\",\\\"id\\\":\\\"msg-remote-runtime-sync-json-123\\\",\\\"role\\\":\\\"assistant\\\",\\\"content\\\":[{\\\"type\\\":\\\"output_text\\\",\\\"text\\\":\\\"Hello from remote runtime sync json\\\",\\\"annotations\\\":[]}]}],\\\"usage\\\":{\\\"input_tokens\\\":1,\\\"output_tokens\\\":2,\\\"total_tokens\\\":3}}\"}}\n",
                    "{\"type\":\"telemetry\",\"payload\":{\"kind\":\"telemetry\",\"telemetry\":{\"elapsed_ms\":41}}}\n",
                    "{\"type\":\"eof\",\"payload\":{\"kind\":\"eof\"}}\n"
                );
                let mut response = axum::http::Response::new(Body::from(frames));
                response.headers_mut().insert(
                    header::CONTENT_TYPE,
                    HeaderValue::from_static("application/x-ndjson"),
                );
                response
            }),
        );
        axum::serve(listener, app)
            .await
            .expect("server should start");
    });

    let state = AppState::new()
        .expect("app state should build")
        .with_execution_runtime_override_base_url(format!("http://{addr}"));
    let plan = ExecutionPlan {
        request_id: "req-remote-runtime-sync-json-stream".into(),
        candidate_id: Some("cand-remote-runtime-sync-json-stream".into()),
        provider_name: Some("openai".into()),
        provider_id: "prov-1".into(),
        endpoint_id: "ep-1".into(),
        key_id: "key-1".into(),
        method: "POST".into(),
        url: "https://chatgpt.com/backend-api/codex/responses".into(),
        headers: BTreeMap::from([
            ("content-type".into(), "application/json".into()),
            ("accept".into(), "text/event-stream".into()),
        ]),
        content_type: Some("application/json".into()),
        content_encoding: None,
        body: RequestBody::from_json(json!({
            "model": "gpt-5.4",
            "input": "hello",
            "stream": true
        })),
        stream: true,
        client_api_format: "openai:responses".into(),
        provider_api_format: "openai:responses".into(),
        model_name: Some("gpt-5.4".into()),
        proxy: None,
        transport_profile: None,
        timeouts: Some(ExecutionTimeouts {
            connect_ms: Some(5_000),
            total_ms: Some(5_000),
            ..ExecutionTimeouts::default()
        }),
    };
    let decision = GatewayControlDecision::synthetic(
        "/v1/responses",
        Some("ai_public".to_string()),
        Some("openai".to_string()),
        Some("cli".to_string()),
        Some("openai:responses".to_string()),
    )
    .with_execution_runtime_candidate(true);

    let response = execute_execution_runtime_stream(
        &state,
        plan,
        "trace-remote-runtime-sync-json-stream",
        &decision,
        "openai_responses_stream",
        None,
        Some(json!({
            "provider_api_format": "openai:responses",
            "client_api_format": "openai:responses",
            "upstream_is_stream": true,
        })),
    )
    .await
    .expect("execution should succeed")
    .expect("execution should return a client response");

    assert_eq!(
        response
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok()),
        Some("text/event-stream")
    );
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("response body should read");
    let text = String::from_utf8(body.to_vec()).expect("response body should be utf8");
    assert!(text.contains("event: response.output_text.delta"));
    assert!(text.contains("Hello from remote runtime sync json"));
    assert!(text.contains("event: response.completed"));

    server.abort();
}

#[tokio::test]
async fn execute_execution_runtime_stream_rewrites_redirect_to_structured_failure() {
    let listener = crate::test_support::bind_loopback_listener()
        .await
        .expect("listener should bind");
    let addr = listener.local_addr().expect("local addr should resolve");
    let server = tokio::spawn(async move {
        let app = Router::new().route(
            "/v1/execute/stream",
            any(|_request: Request| async move {
                let frames = concat!(
                    "{\"type\":\"headers\",\"payload\":{\"kind\":\"headers\",\"status_code\":302,\"headers\":{\"location\":\"/\",\"content-type\":\"text/html\",\"content-length\":\"0\"}}}\n",
                    "{\"type\":\"eof\",\"payload\":{\"kind\":\"eof\"}}\n"
                );
                let mut response = axum::http::Response::new(Body::from(frames));
                response.headers_mut().insert(
                    header::CONTENT_TYPE,
                    HeaderValue::from_static("application/x-ndjson"),
                );
                response
            }),
        );
        axum::serve(listener, app)
            .await
            .expect("server should start");
    });

    let usage_repository = Arc::new(InMemoryUsageReadRepository::default());
    let request_candidate_repository = Arc::new(InMemoryRequestCandidateRepository::default());
    let state = AppState::new()
        .expect("app state should build")
        .with_data_state_for_tests(
            crate::data::GatewayDataState::with_request_candidate_and_usage_repository_for_tests(
                Arc::clone(&request_candidate_repository),
                Arc::clone(&usage_repository),
            )
            .with_system_config_values_for_tests([(
                "request_record_level".to_string(),
                json!("full"),
            )]),
        )
        .with_usage_runtime_for_tests(UsageRuntimeConfig {
            enabled: true,
            ..UsageRuntimeConfig::default()
        })
        .with_execution_runtime_override_base_url(format!("http://{addr}"));
    let plan = ExecutionPlan {
        request_id: "req-remote-runtime-stream-redirect".into(),
        candidate_id: Some("cand-remote-runtime-stream-redirect".into()),
        provider_name: Some("ChatGPTWeb".into()),
        provider_id: "prov-redirect".into(),
        endpoint_id: "ep-redirect".into(),
        key_id: "key-redirect".into(),
        method: "POST".into(),
        url: "https://chatgpt.com/backend-api/codex/responses".into(),
        headers: BTreeMap::from([
            ("content-type".into(), "application/json".into()),
            ("accept".into(), "text/event-stream".into()),
        ]),
        content_type: Some("application/json".into()),
        content_encoding: None,
        body: RequestBody::from_json(json!({
            "model": "gpt-5.4",
            "input": "hello",
            "stream": true
        })),
        stream: true,
        client_api_format: "gemini:generate_content".into(),
        provider_api_format: "openai:responses".into(),
        model_name: Some("gemini-3.1-flash-image-preview".into()),
        proxy: None,
        transport_profile: None,
        timeouts: Some(ExecutionTimeouts {
            connect_ms: Some(5_000),
            total_ms: Some(5_000),
            ..ExecutionTimeouts::default()
        }),
    };
    let decision = GatewayControlDecision::synthetic(
        "/v1beta/models/gemini-3.1-flash-image-preview:streamGenerateContent",
        Some("ai_public".to_string()),
        Some("gemini".to_string()),
        Some("generate_content".to_string()),
        Some("gemini:generate_content".to_string()),
    )
    .with_execution_runtime_candidate(true);

    let response = execute_execution_runtime_stream(
        &state,
        plan,
        "trace-remote-runtime-stream-redirect",
        &decision,
        "gemini_chat_stream",
        None,
        Some(json!({
            "request_id": "req-remote-runtime-stream-redirect",
            "candidate_id": "cand-remote-runtime-stream-redirect",
            "candidate_index": 0,
            "retry_index": 0,
            "provider_api_format": "openai:responses",
            "client_api_format": "gemini:generate_content",
            "needs_conversion": true
        })),
    )
    .await
    .expect("execution should succeed")
    .expect("execution should return a client response");

    assert_eq!(response.status(), axum::http::StatusCode::BAD_GATEWAY);
    assert_eq!(
        response
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok()),
        Some("application/json")
    );
    assert_eq!(
        response
            .headers()
            .get("x-aether-upstream-status")
            .and_then(|value| value.to_str().ok()),
        Some("302")
    );
    assert!(
        response.headers().get(header::LOCATION).is_none(),
        "redirect location should not be forwarded to AI clients"
    );
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("response body should read");
    let body_json: Value =
        serde_json::from_slice(&body).expect("response body should decode as json");
    assert_eq!(
        body_json["error"]["type"],
        json!("execution_runtime_non_success_status")
    );
    assert_eq!(body_json["error"]["upstream_status"], json!(302));
    assert_eq!(body_json["error"]["location"], json!("/"));
    assert!(body_json["error"]["message"]
        .as_str()
        .is_some_and(|value| value.contains("non-success status 302")));

    let usage = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if let Some(usage) = usage_repository
                .find_by_request_id("req-remote-runtime-stream-redirect")
                .await
                .expect("usage should read")
                .filter(|usage| usage.status == "failed")
            {
                break usage;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("usage should be written");

    assert_eq!(usage.status_code, Some(302));
    assert_eq!(usage.error_category.as_deref(), Some("redirect"));
    assert!(usage.error_message.is_none());
    assert_eq!(
        usage.client_response_headers.as_ref().unwrap()["content-type"],
        json!("application/json")
    );
    assert_eq!(
        usage.response_headers.as_ref().unwrap()["content-type"],
        json!("text/html")
    );
    assert!(
        usage.response_body.is_none(),
        "upstream redirect did not include a body"
    );
    assert_eq!(usage.client_response_body.as_ref(), Some(&body_json));
    let candidates = request_candidate_repository
        .list_by_request_id("req-remote-runtime-stream-redirect")
        .await
        .expect("candidate trace should read");
    let candidate_extra = candidates
        .first()
        .and_then(|candidate| candidate.extra_data.as_ref())
        .expect("failed candidate extra_data should exist");
    assert_eq!(
        candidate_extra["upstream_response"]["status_code"],
        json!(302)
    );
    assert_eq!(
        candidate_extra["upstream_response"]["headers"]["location"],
        "/"
    );
    assert!(candidate_extra["upstream_response"].get("body").is_none());
    assert!(candidate_extra.get("client_response").is_none());

    server.abort();
}

#[tokio::test]
async fn execute_execution_runtime_stream_bridges_openai_image_sync_json_from_remote_runtime_to_image_sse(
) {
    let listener = crate::test_support::bind_loopback_listener()
        .await
        .expect("listener should bind");
    let addr = listener.local_addr().expect("local addr should resolve");
    let server = tokio::spawn(async move {
        let app = Router::new().route(
            "/v1/execute/stream",
            any(|_request: Request| async move {
                let frames = concat!(
                    "{\"type\":\"headers\",\"payload\":{\"kind\":\"headers\",\"status_code\":200,\"headers\":{\"content-type\":\"application/json\"}}}\n",
                    "{\"type\":\"data\",\"payload\":{\"kind\":\"data\",\"text\":\"{\\\"created\\\":1776972364,\\\"data\\\":[{\\\"b64_json\\\":\\\"aGVsbG8=\\\"}],\\\"usage\\\":{\\\"total_tokens\\\":100,\\\"input_tokens\\\":50,\\\"output_tokens\\\":50,\\\"input_tokens_details\\\":{\\\"text_tokens\\\":10,\\\"image_tokens\\\":40}}}\"}}\n",
                    "{\"type\":\"telemetry\",\"payload\":{\"kind\":\"telemetry\",\"telemetry\":{\"elapsed_ms\":41}}}\n",
                    "{\"type\":\"eof\",\"payload\":{\"kind\":\"eof\"}}\n"
                );
                let mut response = axum::http::Response::new(Body::from(frames));
                response.headers_mut().insert(
                    header::CONTENT_TYPE,
                    HeaderValue::from_static("application/x-ndjson"),
                );
                response
            }),
        );
        axum::serve(listener, app)
            .await
            .expect("server should start");
    });

    let state = AppState::new()
        .expect("app state should build")
        .with_execution_runtime_override_base_url(format!("http://{addr}"));
    let plan = ExecutionPlan {
        request_id: "req-remote-runtime-image-sync-json-stream".into(),
        candidate_id: Some("cand-remote-runtime-image-sync-json-stream".into()),
        provider_name: Some("openai".into()),
        provider_id: "prov-1".into(),
        endpoint_id: "ep-1".into(),
        key_id: "key-1".into(),
        method: "POST".into(),
        url: "https://chatgpt.com/backend-api/codex/responses".into(),
        headers: BTreeMap::from([
            ("content-type".into(), "application/json".into()),
            ("accept".into(), "text/event-stream".into()),
        ]),
        content_type: Some("application/json".into()),
        content_encoding: None,
        body: RequestBody::from_json(json!({
            "model": "gpt-image-1",
            "prompt": "hello",
            "stream": true
        })),
        stream: true,
        client_api_format: "openai:image".into(),
        provider_api_format: "openai:image".into(),
        model_name: Some("gpt-image-1".into()),
        proxy: None,
        transport_profile: None,
        timeouts: Some(ExecutionTimeouts {
            connect_ms: Some(5_000),
            total_ms: Some(5_000),
            ..ExecutionTimeouts::default()
        }),
    };
    let decision = GatewayControlDecision::synthetic(
        "/v1/images/generations",
        Some("ai_public".to_string()),
        Some("openai".to_string()),
        Some("image".to_string()),
        Some("openai:image".to_string()),
    )
    .with_execution_runtime_candidate(true);

    let response = execute_execution_runtime_stream(
        &state,
        plan,
        "trace-remote-runtime-image-sync-json-stream",
        &decision,
        "openai_image_stream",
        None,
        Some(json!({
            "provider_api_format": "openai:image",
            "client_api_format": "openai:image",
            "mapped_model": "gpt-image-1",
            "image_request": {
                "operation": "generate"
            }
        })),
    )
    .await
    .expect("execution should succeed")
    .expect("execution should return a client response");

    assert_eq!(
        response
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok()),
        Some("text/event-stream")
    );
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("response body should read");
    let text = String::from_utf8(body.to_vec()).expect("response body should be utf8");
    assert!(text.contains("event: image_generation.completed"));
    assert!(text.contains("\"type\":\"image_generation.completed\""));
    assert!(text.contains("\"b64_json\":\"aGVsbG8=\""));
    assert!(text.contains("\"total_tokens\":100"));

    server.abort();
}
