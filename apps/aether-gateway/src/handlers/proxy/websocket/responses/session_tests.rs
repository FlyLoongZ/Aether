use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use super::super::adapter::{
    resolve_responses_websocket_adapter, ResponsesWebSocketDrainDirective,
};
use super::super::binding::UpstreamBindingIdentity;
use super::super::client::adapter_drain_ready;
use super::super::quota::{
    is_usage_limit_error_event, observe_active_response_rebind_safety, record_exhausted_bound_key,
};
use super::super::redaction::ResponsesWebSocketRedactionRestorer;
use super::super::request::{
    changed_followup_response_create_model, normalize_followup_response_create,
    planned_response_create_event, response_create_model_or_current,
};
use super::super::state::{BoundResponsesConnection, ExhaustedResponsesWebSocketExclusions};
use super::super::turn::{
    ResponsesWebSocketTurnDeadline, ResponsesWebSocketTurnObservation,
    ResponsesWebSocketTurnOutcome, ResponsesWebSocketTurnTimeoutPhase,
};
use super::super::turn_state::{LogicalTurn, ResponsesTurnState};
use super::super::upstream::bind_responses_upstream;
use crate::ai_serving::{
    AiExecutionDecision, OpenAiResponsesReasoningReplayPolicy, ResponsesWebSocketBodyNormalization,
};
use crate::handlers::proxy::websocket::session::wait_for_optional_deadline;
use crate::handlers::proxy::websocket::transport::{
    websocket_handshake_headers, websocket_timeouts, websocket_upstream_url,
};
use crate::privacy::{RedactionSession, RedactionSessionConfig};
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::State;
use axum::http::header::{AUTHORIZATION, CONTENT_TYPE};
use axum::http::HeaderMap;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::Router;
use futures_util::{SinkExt, StreamExt};
use serde_json::json;
use tokio::sync::{oneshot, Mutex};

#[derive(Default)]
struct MockState {
    observed: Mutex<Option<oneshot::Sender<ObservedInitialEvent>>>,
}

struct ObservedInitialEvent {
    authorization_present: bool,
    account_header_present: bool,
    event: serde_json::Value,
}

struct BootstrapDropProbe(Arc<AtomicUsize>);

impl Drop for BootstrapDropProbe {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

struct TestAdmissionHealth(Arc<AtomicBool>);

impl aether_runtime::AdmissionPermitHealth for TestAdmissionHealth {
    fn is_healthy(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }
}

#[test]
fn initial_message_error_diagnostics_are_stable() {
    use super::{initial_message_diagnostic, InitialMessageError, InitialMessageFailure};

    let cases = [
        (
            InitialMessageError::TimedOut,
            "initial_response_create_timeout",
            "timeout",
            Some("Timed out waiting for the initial response.create event"),
            1013,
            true,
        ),
        (
            InitialMessageError::ClientClosed,
            "client_closed",
            "client_closed",
            None,
            1000,
            false,
        ),
        (
            InitialMessageError::ClientRead,
            "client_read_failed",
            "client_read_failed",
            Some("Failed to read the initial WebSocket event before response.create"),
            1008,
            false,
        ),
        (
            InitialMessageError::UnsupportedFrame,
            "initial_response_create_must_be_text",
            "unsupported_frame",
            Some("The initial response.create event must be sent as a text WebSocket message"),
            1008,
            false,
        ),
        (
            InitialMessageError::InvalidJson,
            "invalid_response_create",
            "invalid_json",
            Some("The initial WebSocket text message must be a JSON response.create object"),
            1008,
            false,
        ),
        (
            InitialMessageError::MissingResponseCreate,
            "expected_response_create",
            "unexpected_event_type",
            Some("The initial WebSocket JSON object must have type response.create"),
            1008,
            false,
        ),
        (
            InitialMessageError::MissingModel,
            "response_create_model_required",
            "missing_model",
            Some("The initial response.create event must include a model"),
            1008,
            false,
        ),
        (
            InitialMessageError::InvalidModel,
            "invalid_response_create_model",
            "invalid_model",
            Some("response.create.model must be a non-empty string no longer than 256 bytes"),
            1008,
            false,
        ),
        (
            InitialMessageError::InvalidPreviousResponseId,
            "invalid_response_create_previous_response_id",
            "invalid_previous_response_id",
            Some("response.create.previous_response_id must be null or a non-empty string"),
            1008,
            false,
        ),
        (
            InitialMessageError::InvalidStreamId,
            "invalid_response_create_stream_id",
            "invalid_stream_id",
            Some(
                "response.create.stream_id must be 1-256 ASCII letters, numbers, underscores, hyphens, or periods",
            ),
            1008,
            false,
        ),
        (
            InitialMessageError::UnsupportedStreamId,
            "responses_websocket_named_stream_unsupported",
            "unsupported_stream_id",
            Some(
                "Aether currently supports only the implicit default WebSocket lane; omit response.create.stream_id",
            ),
            1008,
            false,
        ),
    ];

    for (error, error_code, error_kind, client_message, close_code, timed_out) in cases {
        let diagnostic = initial_message_diagnostic(&InitialMessageFailure::new(error, None));
        assert_eq!(diagnostic.error_code, error_code);
        assert_eq!(diagnostic.error_kind, error_kind);
        assert_eq!(diagnostic.client_message, client_message);
        assert_eq!(diagnostic.close_code, close_code);
        assert_eq!(diagnostic.timed_out, timed_out);
        assert_eq!(diagnostic.last_frame_opcode, None);
        assert_eq!(diagnostic.last_frame_bytes, None);
    }
}

#[test]
fn initial_message_diagnostic_retains_only_safe_frame_shape() {
    use super::{
        initial_message_diagnostic, InitialMessageError, InitialMessageFailure,
        InitialMessageFrameMetadata,
    };

    let secret_body = r#"{"type":"not-response.create","token":"must-not-log"}"#;
    let frame = Message::Text(secret_body.to_string().into());
    let metadata = InitialMessageFrameMetadata::from_message(&frame);
    let diagnostic = initial_message_diagnostic(&InitialMessageFailure::new(
        InitialMessageError::MissingResponseCreate,
        Some(metadata),
    ));

    assert_eq!(diagnostic.last_frame_opcode, Some("text"));
    assert_eq!(diagnostic.last_frame_bytes, Some(secret_body.len()));
    assert!(!format!("{diagnostic:?}").contains("must-not-log"));
}

#[tokio::test]
async fn phase_supervisor_drops_work_at_the_absolute_connection_deadline() {
    let dropped = Arc::new(AtomicUsize::new(0));
    let task_dropped = Arc::clone(&dropped);
    let bootstrap = async move {
        let _probe = BootstrapDropProbe(task_dropped);
        std::future::pending::<()>().await;
    };

    let result = tokio::time::timeout(
        Duration::from_secs(1),
        super::supervise_responses_websocket_phase(
            bootstrap,
            tokio::time::Instant::now() + Duration::from_millis(20),
            None,
        ),
    )
    .await
    .expect("bootstrap supervisor should honor its absolute deadline");

    assert_eq!(
        result,
        Err(super::ConnectionTermination::ConnectionLimitReached)
    );
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn phase_supervisor_drops_work_when_connection_admission_is_lost() {
    let health = Arc::new(AtomicBool::new(false));
    let permit = aether_runtime::AdmissionPermit::from_parts(
        None,
        Some(TestAdmissionHealth(Arc::clone(&health))),
    )
    .expect("distributed test health should create a permit");
    let dropped = Arc::new(AtomicUsize::new(0));
    let task_dropped = Arc::clone(&dropped);
    let bootstrap = async move {
        let _probe = BootstrapDropProbe(task_dropped);
        std::future::pending::<()>().await;
    };

    let result = tokio::time::timeout(
        Duration::from_secs(1),
        super::supervise_responses_websocket_phase(
            bootstrap,
            tokio::time::Instant::now() + Duration::from_secs(30),
            Some(&permit),
        ),
    )
    .await
    .expect("unhealthy connection admission should stop bootstrap");

    assert_eq!(
        result,
        Err(super::ConnectionTermination::ConnectionAdmissionLost)
    );
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
}

#[test]
fn adapter_drain_waits_for_an_active_turn_terminal_event() {
    let directive = Some(ResponsesWebSocketDrainDirective {
        error_code: "adapter_draining",
        retry_current_turn: false,
        retry_exclusion_until_unix_secs: None,
    });
    assert!(!adapter_drain_ready(directive, true, None, false));
    assert!(adapter_drain_ready(
        directive,
        true,
        Some(ResponsesWebSocketTurnObservation::Terminal(
            ResponsesWebSocketTurnOutcome::upstream_closed()
        )),
        false,
    ));
    assert!(!adapter_drain_ready(None, false, None, false));
    assert!(adapter_drain_ready(directive, false, None, false));
    assert!(adapter_drain_ready(directive, true, None, true));
}

#[test]
fn exhausted_key_and_account_exclusions_expire_at_the_reported_reset_or_fallback() {
    let mut exclusions = ExhaustedResponsesWebSocketExclusions::default();

    assert_eq!(
        exclusions.exclude(
            "key-1".to_string(),
            Some("account-1".to_string()),
            Some(1_050),
            1_000,
        ),
        1_050
    );
    assert!(exclusions.key_ids(1_049).contains("key-1"));
    assert!(exclusions.codex_account_ids(1_049).contains("account-1"));
    assert!(!exclusions.key_ids(1_050).contains("key-1"));
    assert!(!exclusions.codex_account_ids(1_050).contains("account-1"));

    assert_eq!(
        exclusions.exclude("key-2".to_string(), None, None, 2_000),
        2_300
    );
    assert!(exclusions.key_ids(2_299).contains("key-2"));
    assert!(!exclusions.key_ids(2_300).contains("key-2"));

    assert_eq!(
        exclusions.exclude("key-3".to_string(), None, Some(3_100), 3_000),
        3_100
    );
    assert_eq!(
        exclusions.exclude("key-3".to_string(), None, Some(3_050), 3_001),
        3_100
    );
}

#[test]
fn exhausted_codex_binding_excludes_the_account_before_retry_planning() {
    let mut bound = sample_bound_for_rebind_safety();
    bound.decision_template.provider_type = Some("codex".to_string());
    bound.decision_template.key_id = Some("key-codex".to_string());
    bound.decision_template.provider_request_headers.insert(
        "ChatGPT-Account-ID".to_string(),
        "account-codex".to_string(),
    );

    // The exclusion deadline is evaluated against the wall clock, so a
    // provider reset time only survives if it is still in the future.
    let reset_at = crate::clock::current_unix_secs() + 600;

    assert_eq!(
        record_exhausted_bound_key(&mut bound, Some(reset_at)),
        Some(("key-codex".to_string(), reset_at))
    );
    assert!(bound
        .exhausted_exclusions
        .codex_account_ids(reset_at - 1)
        .contains("account-codex"));
    assert!(!bound
        .exhausted_exclusions
        .codex_account_ids(reset_at)
        .contains("account-codex"));
}

#[test]
fn maps_http_responses_url_to_websocket_url_without_losing_path_or_query() {
    let url = websocket_upstream_url(
        "https://example.test/v1/responses?x=1",
        "responses_upstream_url_invalid",
    )
    .expect("URL should convert");
    assert_eq!(url.as_str(), "wss://example.test/v1/responses?x=1");
}

#[test]
fn rejects_embedded_upstream_credentials() {
    assert!(websocket_upstream_url(
        "https://token@example.test/responses",
        "responses_upstream_url_invalid",
    )
    .is_err());
}

#[test]
fn strips_http_entity_headers_from_websocket_handshake() {
    let headers = websocket_handshake_headers(
        &BTreeMap::from([
            (
                "authorization".to_string(),
                "Bearer provider-token".to_string(),
            ),
            ("chatgpt-account-id".to_string(), "account-id".to_string()),
            ("content-type".to_string(), "application/json".to_string()),
        ]),
        "responses_websocket_headers_invalid",
    )
    .expect("headers should build");
    assert!(headers.contains_key(AUTHORIZATION));
    assert!(!headers.contains_key(CONTENT_TYPE));
}

#[test]
fn planned_event_uses_mapped_model_and_removes_http_stream_fields() {
    let mut decision = sample_decision();
    decision.provider_request_body = Some(json!({
        "model": "provider-model",
        "input": "hello",
        "stream": true,
        "background": true,
    }));
    let normalization = ResponsesWebSocketBodyNormalization::for_tests("provider-model");
    let event = planned_response_create_event(
        &decision,
        &normalization,
        &json!({
            "type": "response.create",
            "model": "public-model",
            "previous_response_id": "resp-previous",
            "generate": false,
        }),
    )
    .expect("event should serialize");
    let event: serde_json::Value = serde_json::from_str(&event).expect("event JSON");
    assert_eq!(event["type"], "response.create");
    assert_eq!(event["model"], "provider-model");
    assert_eq!(event["previous_response_id"], "resp-previous");
    assert_eq!(event["generate"], false);
    assert!(event.get("stream").is_none());
    assert!(event.get("background").is_none());
}

#[test]
fn only_an_actual_usage_limit_error_requests_transparent_retry() {
    assert!(is_usage_limit_error_event(&json!({
        "type": "error",
        "error": {"type": "usage_limit_reached"},
        "status_code": 429,
    })));
    assert!(!is_usage_limit_error_event(&json!({
        "type": "codex.rate_limits",
        "rate_limits": {"limit_reached": true},
    })));
    assert!(!is_usage_limit_error_event(&json!({
        "type": "response.completed",
        "response": {"id": "resp-completed"},
    })));
}

#[test]
fn followup_rewrites_the_provider_model_and_removes_http_stream_fields() {
    let event = json!({
        "type": "response.create",
        "model": "public-model",
        "stream": true,
        "background": true,
    });
    let normalized = normalize_followup_response_create(
        &event,
        "provider-model",
        &ResponsesWebSocketBodyNormalization::for_tests("provider-model"),
    )
    .expect("response.create should be normalized");
    let event: serde_json::Value = serde_json::from_str(&normalized).expect("event JSON");
    assert_eq!(event["model"], "provider-model");
    assert!(event.get("stream").is_none());
    assert!(event.get("background").is_none());
}

#[test]
fn followup_model_change_requires_per_turn_replanning() {
    let prewarm = json!({
        "type": "response.create",
        "model": "gpt-5.6-sol",
        "generate": false,
    });
    let turn = json!({
        "type": "response.create",
        "model": "gpt-5.6-terra",
        "input": [{"role": "user", "content": "hello"}],
    });

    assert_eq!(
        changed_followup_response_create_model(&prewarm, "gpt-5.6-sol"),
        Ok(None)
    );
    assert_eq!(
        changed_followup_response_create_model(&turn, "gpt-5.6-sol"),
        Ok(Some("gpt-5.6-terra".to_string()))
    );
}

#[test]
fn oversized_model_is_rejected_before_initial_or_followup_planning() {
    let oversized = "m".repeat(257);
    let initial = json!({
        "type": "response.create",
        "model": oversized,
        "input": [],
    });

    assert!(matches!(
        super::validate_initial_response_create(&initial),
        Err(super::InitialMessageError::InvalidModel)
    ));
    assert_eq!(
        changed_followup_response_create_model(&initial, "current-model"),
        Err("invalid_response_create_model")
    );
}

#[test]
fn malformed_initial_previous_response_id_is_rejected_before_planning() {
    for previous_response_id in [json!(""), json!(42), json!({"id": "resp_1"})] {
        let initial = json!({
            "type": "response.create",
            "model": "gpt-5.6-sol",
            "previous_response_id": previous_response_id,
            "input": [],
        });
        assert!(matches!(
            super::validate_initial_response_create(&initial),
            Err(super::InitialMessageError::InvalidPreviousResponseId)
        ));
    }

    let named = json!({
        "type": "response.create",
        "model": "gpt-5.6-sol",
        "stream_id": "main",
        "previous_response_id": "",
        "input": [],
    });
    let failure = super::InitialMessageFailure::for_event(
        super::InitialMessageError::InvalidPreviousResponseId,
        None,
        &named,
    );
    assert_eq!(failure.stream_id.as_deref(), Some("main"));
}

#[test]
fn valid_previous_response_can_start_a_new_socket() {
    let initial = json!({
        "type": "response.create",
        "model": "gpt-5.6-sol",
        "previous_response_id": "resp_existing",
        "input": [{"type": "function_call_output", "call_id": "call_1", "output": "ok"}],
    });
    assert!(super::validate_initial_response_create(&initial).is_ok());

    let null_previous = json!({
        "type": "response.create",
        "model": "gpt-5.6-sol",
        "previous_response_id": null,
        "input": [{"role": "user", "content": "new chain"}],
    });
    assert!(super::validate_initial_response_create(&null_previous).is_ok());
}

#[test]
fn cross_socket_registry_miss_never_falls_through_to_random_provider_planning() {
    let pinned = crate::ai_serving::ResponsesWebSocketPinnedCandidate::new(
        "provider-original",
        "endpoint-original",
        "key-original",
    )
    .expect("valid pinned candidate");

    assert_eq!(
        super::initial_continuation_planning_candidate(false, None),
        Ok(None),
        "a genuinely new response may use the ordinary planner"
    );
    assert_eq!(
        super::initial_continuation_planning_candidate(true, Some(pinned.clone())),
        Ok(Some(pinned)),
        "a proved continuation must retain its exact provider/endpoint/key"
    );
    assert_eq!(
        super::initial_continuation_planning_candidate(true, None),
        Err("previous_response_not_found"),
        "a registry miss must be rejected before an unpinned planner can choose another key"
    );
}

#[test]
fn cross_socket_continuation_rejects_an_effective_lite_mode_change() {
    let mut decision: AiExecutionDecision = serde_json::from_value(json!({
        "action": "local",
        "provider_type": "codex",
        "provider_api_format": "openai:responses",
        "provider_request_headers": {}
    }))
    .expect("minimal Codex decision");
    decision.provider_request_headers.insert(
        crate::ai_serving::CODEX_RESPONSES_LITE_HEADER.to_string(),
        "true".to_string(),
    );
    let normalization = ResponsesWebSocketBodyNormalization::for_tests("gpt-5.6-sol")
        .with_provider_type_for_tests("codex");

    let effective_lite =
        super::planned_request_uses_codex_responses_lite(&decision, &normalization);
    assert!(effective_lite);
    assert!(super::responses_lite_contract_modes_match(
        true,
        effective_lite
    ));

    // A non-null context_management object suppresses the converged Lite
    // contract/header even though the model capability remains enabled.
    // A chain whose stored prefix used Lite must not cross that boundary.
    decision.provider_request_body = Some(json!({
        "model": "gpt-5.6-sol",
        "context_management": {"compact_threshold": 1_000}
    }));
    let effective_lite =
        super::planned_request_uses_codex_responses_lite(&decision, &normalization);
    assert!(!effective_lite);
    assert!(!super::responses_lite_contract_modes_match(
        true,
        effective_lite
    ));
    assert!(super::responses_lite_contract_modes_match(
        false,
        effective_lite
    ));
}

#[test]
fn initial_named_stream_is_rejected_before_planning() {
    let initial = json!({
        "type": "response.create",
        "model": "gpt-5.6-sol",
        "stream_id": "main",
        "input": [],
    });
    assert!(matches!(
        super::validate_initial_response_create(&initial),
        Err(super::InitialMessageError::UnsupportedStreamId)
    ));

    let failure = super::InitialMessageFailure::for_event(
        super::InitialMessageError::UnsupportedStreamId,
        None,
        &initial,
    );
    assert_eq!(failure.stream_id.as_deref(), Some("main"));
}

#[test]
fn malformed_initial_stream_id_is_rejected_before_planning() {
    for stream_id in [json!(null), json!(""), json!("not/a/lane"), json!(42)] {
        let initial = json!({
            "type": "response.create",
            "model": "gpt-5.6-sol",
            "stream_id": stream_id,
            "input": [],
        });
        assert!(matches!(
            super::validate_initial_response_create(&initial),
            Err(super::InitialMessageError::InvalidStreamId)
        ));
        let failure = super::InitialMessageFailure::for_event(
            super::InitialMessageError::InvalidStreamId,
            None,
            &initial,
        );
        assert_eq!(failure.stream_id, None);
    }
}

#[test]
fn model_at_the_identifier_limit_remains_valid() {
    let model = "m".repeat(256);
    let event = json!({
        "type": "response.create",
        "model": model,
        "input": [],
    });

    assert!(super::validate_initial_response_create(&event).is_ok());
    assert_eq!(
        changed_followup_response_create_model(&event, "current-model"),
        Ok(Some(model))
    );
}

#[test]
fn followup_without_a_model_reuses_the_current_connection_model() {
    let event = json!({
        "type": "response.create",
        "input": "continue",
    });

    assert_eq!(
        changed_followup_response_create_model(&event, "gpt-5.6-sol"),
        Ok(None)
    );
}

#[test]
fn detached_followup_inherits_the_current_public_model() {
    let mut event = json!({
        "type": "response.create",
        "input": "start over",
    });

    assert_eq!(
        response_create_model_or_current(&mut event, "gpt-5.6-sol"),
        Ok("gpt-5.6-sol".to_string())
    );
    assert_eq!(event["model"], "gpt-5.6-sol");
}

#[test]
fn quota_retry_requires_an_explicitly_replay_safe_turn() {
    let mut request = LogicalTurn::new(
        json!({"type": "response.create", "model": "gpt-5.6-sol"}),
        2,
        "logical-turn".to_string(),
    );
    assert_eq!(request.quota_retry_block_reason(), None);

    request.mark_retry_unsafe("standard_response_event");
    assert_eq!(
        request.quota_retry_block_reason(),
        Some("standard_response_event")
    );

    let mut retried = LogicalTurn::new(
        json!({"type": "response.create", "model": "gpt-5.6-sol"}),
        2,
        "logical-turn".to_string(),
    );
    retried.retry_attempted = true;
    assert_eq!(
        retried.quota_retry_block_reason(),
        Some("quota_retry_already_attempted")
    );

    let mut client_control = LogicalTurn::new(
        json!({"type": "response.create", "model": "gpt-5.6-sol"}),
        2,
        "logical-turn".to_string(),
    );
    client_control.mark_retry_unsafe("client_control_event");
    assert_eq!(
        client_control.quota_retry_block_reason(),
        Some("client_control_event")
    );

    let continuation = LogicalTurn::new(
        json!({
            "type": "response.create",
            "model": "gpt-5.6-sol",
            "previous_response_id": "resp_previous",
        }),
        2,
        "logical-turn".to_string(),
    );
    assert_eq!(
        continuation.quota_retry_block_reason(),
        Some("previous_response_id")
    );
}

#[test]
fn adapter_safety_contract_controls_transparent_rebind_eligibility() {
    let mut bound = sample_bound_for_rebind_safety();
    observe_active_response_rebind_safety(
        &mut bound,
        &json!({
            "type": "codex.rate_limits",
            "rate_limits": {"allowed": true}
        }),
    );
    assert_eq!(
        bound
            .turn_state
            .logical()
            .and_then(LogicalTurn::quota_retry_block_reason),
        None
    );

    observe_active_response_rebind_safety(&mut bound, &json!({"type": "response.created"}));
    assert_eq!(
        bound
            .turn_state
            .logical()
            .and_then(LogicalTurn::quota_retry_block_reason),
        Some("standard_response_event")
    );

    let mut unknown = sample_bound_for_rebind_safety();
    observe_active_response_rebind_safety(&mut unknown, &json!({"type": "codex.unknown"}));
    assert_eq!(
        unknown
            .turn_state
            .logical()
            .and_then(LogicalTurn::quota_retry_block_reason),
        Some("unrecognized_upstream_event")
    );
}

#[test]
fn websocket_transport_keeps_only_the_connect_timeout() {
    let mut decision = sample_decision();
    decision.timeouts = Some(aether_contracts::ExecutionTimeouts {
        connect_ms: Some(123),
        read_ms: Some(456),
        first_byte_ms: Some(789),
        total_ms: Some(1_000),
        ..aether_contracts::ExecutionTimeouts::default()
    });

    let timeouts = websocket_timeouts(&decision).expect("timeouts should be retained");
    assert_eq!(timeouts.connect_ms, Some(123));
    assert_eq!(timeouts.read_ms, None);
    assert_eq!(timeouts.first_byte_ms, None);
    assert_eq!(timeouts.total_ms, None);
}

#[tokio::test]
async fn expired_turn_deadline_returns_without_waiting_for_socket_io() {
    let deadline = ResponsesWebSocketTurnDeadline {
        phase: ResponsesWebSocketTurnTimeoutPhase::AwaitingFirstEvent,
        deadline: Instant::now() - Duration::from_millis(1),
        timeout: Duration::from_secs(1),
    };

    tokio::time::timeout(
        Duration::from_millis(50),
        wait_for_optional_deadline(Some(deadline.deadline)),
    )
    .await
    .expect("expired deadline should resolve immediately");
}

#[tokio::test]
async fn upstream_binding_uses_provider_headers_and_rewrites_the_first_event() {
    let (upstream_url, observed, server) = spawn_mock_server().await;
    let mut decision = sample_decision();
    decision.upstream_url = Some(upstream_url);
    decision.provider_request_headers = BTreeMap::from([
        (
            "authorization".to_string(),
            "Bearer provider-token".to_string(),
        ),
        ("chatgpt-account-id".to_string(), "account-id".to_string()),
        ("content-type".to_string(), "application/json".to_string()),
    ]);
    decision.provider_request_body = Some(json!({
        "model": "provider-model",
        "input": "hello",
        "stream": true,
        "background": true,
    }));

    let mut bound = bind_responses_upstream(
        &decision,
        ResponsesWebSocketBodyNormalization::for_tests("provider-model"),
        &json!({
            "type": "response.create",
            "model": "public-model",
            "input": "hello",
        }),
        resolve_responses_websocket_adapter(
            crate::orchestration::ResponsesWebSocketAdapter::Standard,
        ),
        None,
        |_| {},
    )
    .await
    .expect("upstream binding should succeed");
    let observed = tokio::time::timeout(Duration::from_secs(2), observed)
        .await
        .expect("mock should observe first event")
        .expect("mock event channel should remain open");
    let response = tokio::time::timeout(
        Duration::from_secs(2),
        bound
            .upstream
            .as_mut()
            .expect("bound upstream should be present")
            .recv(),
    )
    .await
    .expect("mock should send a response event")
    .expect("upstream should remain open")
    .expect("upstream response should be valid");
    server.abort();

    assert!(observed.authorization_present);
    assert!(observed.account_header_present);
    assert_eq!(observed.event["type"], "response.create");
    assert_eq!(observed.event["model"], "provider-model");
    assert!(observed.event.get("stream").is_none());
    assert!(observed.event.get("background").is_none());
    assert!(matches!(response, wreq::ws::message::Message::Text(_)));
}

async fn spawn_mock_server() -> (
    String,
    oneshot::Receiver<ObservedInitialEvent>,
    tokio::task::JoinHandle<()>,
) {
    let (observed_tx, observed_rx) = oneshot::channel();
    let state = Arc::new(MockState {
        observed: Mutex::new(Some(observed_tx)),
    });
    let app = Router::new()
        .route("/v1/responses", get(mock_websocket))
        .with_state(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("mock listener should bind");
    let address = listener
        .local_addr()
        .expect("mock listener should expose address");
    let server = tokio::spawn(async move {
        axum::serve(listener, app)
            .await
            .expect("mock server should run");
    });
    (
        format!("http://{address}/v1/responses"),
        observed_rx,
        server,
    )
}

async fn mock_websocket(
    ws: WebSocketUpgrade,
    State(state): State<Arc<MockState>>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let authorization_present = headers
        .get("authorization")
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.starts_with("Bearer "));
    let account_header_present = headers.contains_key("chatgpt-account-id");
    ws.on_upgrade(move |socket| async move {
        serve_mock_socket(socket, state, authorization_present, account_header_present).await;
    })
}

async fn serve_mock_socket(
    socket: WebSocket,
    state: Arc<MockState>,
    authorization_present: bool,
    account_header_present: bool,
) {
    let (mut sender, mut receiver) = socket.split();
    let message = receiver
        .next()
        .await
        .expect("client should send the initial event")
        .expect("initial event should be valid");
    let Message::Text(text) = message else {
        panic!("expected a text response.create event");
    };
    let event = serde_json::from_str(text.as_str()).expect("event should be JSON");
    let _ = sender
        .send(Message::Text(
            json!({"type": "response.created", "response": {"id": "resp-test"}})
                .to_string()
                .into(),
        ))
        .await;
    if let Some(observed) = state.observed.lock().await.take() {
        let _ = observed.send(ObservedInitialEvent {
            authorization_present,
            account_header_present,
            event,
        });
    }
}

fn sample_decision() -> AiExecutionDecision {
    AiExecutionDecision {
        action: "local".to_string(),
        decision_kind: None,
        execution_strategy: None,
        conversion_mode: None,
        request_id: None,
        candidate_id: None,
        provider_name: None,
        provider_type: Some("custom".to_string()),
        provider_id: None,
        endpoint_id: None,
        key_id: None,
        upstream_base_url: None,
        upstream_url: Some("https://example.test/v1/responses".to_string()),
        provider_request_method: None,
        auth_header: None,
        auth_value: None,
        provider_api_format: Some("openai:responses".to_string()),
        client_api_format: Some("openai:responses".to_string()),
        provider_contract: None,
        client_contract: None,
        model_name: None,
        mapped_model: Some("provider-model".to_string()),
        prompt_cache_key: None,
        extra_headers: BTreeMap::new(),
        provider_request_headers: BTreeMap::new(),
        provider_request_body: None,
        provider_request_body_base64: None,
        content_type: None,
        content_encoding: None,
        request_gzip: None,
        proxy: None,
        transport_profile: None,
        timeouts: None,
        upstream_is_stream: true,
        report_kind: None,
        report_context: None,
        auth_context: None,
    }
}

fn sample_bound_for_rebind_safety() -> BoundResponsesConnection {
    let adapter =
        resolve_responses_websocket_adapter(crate::orchestration::ResponsesWebSocketAdapter::Codex);
    let decision = sample_decision();
    let binding_identity = UpstreamBindingIdentity::from_decision(adapter, &decision).unwrap();
    BoundResponsesConnection {
        upstream: None,
        adapter,
        client_model: "gpt-5.6-sol".to_string(),
        provider_model: "gpt-5.6-sol".to_string(),
        decision_template: decision,
        body_normalization: ResponsesWebSocketBodyNormalization::for_tests("gpt-5.6-sol"),
        responses_lite_static_config: None,
        binding_identity,
        continuation_response_ids: Default::default(),
        // Replanning：logical turn 在、attempt 不在。重放安全与配额排除都只看
        // logical turn，所以这些用例不需要真实 socket 或真实 attempt。
        turn_state: ResponsesTurnState::Replanning {
            logical: LogicalTurn::new(
                json!({"type": "response.create", "model": "gpt-5.6-sol"}),
                1,
                "logical-turn".to_string(),
            ),
        },
        next_turn_index: 2,
        upstream_response_headers: BTreeMap::new(),
        pending_adapter_drain: None,
        pending_adapter_observation: None,
        exhausted_exclusions: ExhaustedResponsesWebSocketExclusions::default(),
        pending_turn_finalization: None,
        redaction_restorer: ResponsesWebSocketRedactionRestorer::default(),
    }
}

#[test]
fn initial_deepseek_binding_upgrades_redaction_restore_policy() {
    let mut session = RedactionSession::new(RedactionSessionConfig::new(
        b"initial-deepseek-redaction-test".to_vec(),
        300,
        600,
    ));
    let sentinel = session.redact_text("alice@example.com").text;
    let provider_event = json!({
        "type": "response.completed",
        "response": {
            "output": [{
                "type": "reasoning",
                "encrypted_content": "provider-owned-state",
                "content": [{
                    "type": "reasoning_text",
                    "text": format!("opaque replay {sentinel}")
                }]
            }]
        }
    });

    let mut ordinary_bound = sample_bound_for_rebind_safety();
    super::register_initial_redaction_session(&mut ordinary_bound, session.clone());
    let restored = ordinary_bound
        .redaction_restorer
        .restore_provider_frame_text(&provider_event)
        .expect("ordinary OpenAI replay policy restoration must not fail")
        .expect("ordinary OpenAI replay policy should restore response text");
    let restored: serde_json::Value =
        serde_json::from_str(&restored).expect("restored provider event should remain JSON");
    assert_eq!(
        restored["response"]["output"][0]["content"][0]["text"],
        "opaque replay alice@example.com"
    );

    let mut deepseek_bound = sample_bound_for_rebind_safety();
    deepseek_bound.body_normalization =
        ResponsesWebSocketBodyNormalization::for_tests("deepseek-reasoner")
            .with_reasoning_replay_policy_for_tests(
                OpenAiResponsesReasoningReplayPolicy::DeepSeekOpaque,
            );
    super::register_initial_redaction_session(&mut deepseek_bound, session);
    assert!(
        deepseek_bound
            .redaction_restorer
            .restore_provider_frame_text(&provider_event)
            .expect("DeepSeek opaque restoration check must not fail")
            .is_none(),
        "the authenticated DeepSeek binding must keep opaque reasoning state byte-identical"
    );
}

/// 用 mpsc 驱动的 FakeSocket，实现 Stream + Sink 两个 trait。
/// 测试侧通过 tx 注入消息，通过 pong_rx 观察 Pong 回包。
struct FakeSocket {
    rx: tokio::sync::mpsc::Receiver<axum::extract::ws::Message>,
    pong_tx: tokio::sync::mpsc::UnboundedSender<axum::extract::ws::Message>,
}

struct FakeSocketPair {
    tx: tokio::sync::mpsc::Sender<axum::extract::ws::Message>,
    pong_rx: tokio::sync::mpsc::UnboundedReceiver<axum::extract::ws::Message>,
}

fn fake_socket() -> (FakeSocket, FakeSocketPair) {
    let (tx, rx) = tokio::sync::mpsc::channel(16);
    let (pong_tx, pong_rx) = tokio::sync::mpsc::unbounded_channel();
    (FakeSocket { rx, pong_tx }, FakeSocketPair { tx, pong_rx })
}

impl futures_util::Stream for FakeSocket {
    type Item = Result<axum::extract::ws::Message, axum::Error>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.rx.poll_recv(cx).map(|opt| opt.map(Ok))
    }
}

impl futures_util::Sink<axum::extract::ws::Message> for FakeSocket {
    type Error = axum::Error;

    fn poll_ready(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn start_send(
        self: std::pin::Pin<&mut Self>,
        item: axum::extract::ws::Message,
    ) -> Result<(), Self::Error> {
        let _ = self.pong_tx.send(item);
        Ok(())
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn poll_close(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
}

/// Emits one Ping and then permanently backpressures every Pong write.
/// This models a peer that keeps the TCP connection open but never reads.
struct StalledPongSocket {
    emitted_ping: bool,
}

impl futures_util::Stream for StalledPongSocket {
    type Item = Result<axum::extract::ws::Message, axum::Error>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        if self.emitted_ping {
            std::task::Poll::Pending
        } else {
            self.emitted_ping = true;
            std::task::Poll::Ready(Some(Ok(axum::extract::ws::Message::Ping(vec![7].into()))))
        }
    }
}

impl futures_util::Sink<axum::extract::ws::Message> for StalledPongSocket {
    type Error = axum::Error;

    fn poll_ready(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Pending
    }

    fn start_send(
        self: std::pin::Pin<&mut Self>,
        _item: axum::extract::ws::Message,
    ) -> Result<(), Self::Error> {
        unreachable!("a permanently backpressured sink is never ready")
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Pending
    }

    fn poll_close(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Pending
    }
}

#[tokio::test]
async fn initial_message_deadline_also_bounds_a_stalled_pong_write() {
    use super::{receive_initial_response_create_with_deadline, InitialMessageError};

    let mut socket = StalledPongSocket {
        emitted_ping: false,
    };
    let result = tokio::time::timeout(
        Duration::from_millis(300),
        receive_initial_response_create_with_deadline(&mut socket, Duration::from_millis(30)),
    )
    .await
    .expect("the initial-message deadline must cancel a stalled Pong write");

    assert!(matches!(
        &result,
        Err(failure) if failure.error == InitialMessageError::TimedOut
    ));
}

/// 验证 receive_initial_response_create_with_deadline 的绝对 deadline：
/// 客户端周期性发送 Ping 帧不会重置计时器，deadline 到期后返回 TimedOut。
/// 这直接驱动真实的循环逻辑，如果改回每次迭代 timeout(budget, ...) 则会变红。
///
/// 设计思路：deadline_budget = 80ms，Ping 每 30ms 发一次且永不停止。
/// - 绝对 deadline：~80ms 后函数返回 TimedOut（即使 Ping 仍在到来）。
/// - 每次迭代 timeout(80ms, ...)：每个 Ping 在 30ms 内到达 < 80ms，函数
///   永不超时，500ms 后外层 timeout 判定测试失败。
#[tokio::test]
async fn initial_message_times_out_despite_periodic_pings() {
    use super::{receive_initial_response_create_with_deadline, InitialMessageError};

    let (mut fake, pair) = fake_socket();

    let handle = tokio::spawn(async move {
        receive_initial_response_create_with_deadline(&mut fake, Duration::from_millis(80)).await
    });

    // 持续发送 Ping，间隔 30ms，永不停止（直到被测函数返回导致 rx drop）
    let ping_task = tokio::spawn(async move {
        let mut i = 0u8;
        loop {
            tokio::time::sleep(Duration::from_millis(30)).await;
            if pair
                .tx
                .send(axum::extract::ws::Message::Ping(vec![i].into()))
                .await
                .is_err()
            {
                break;
            }
            i = i.wrapping_add(1);
        }
    });

    // 绝对 deadline 应在 ~80ms 后触发；给 500ms 宽限等待结果。
    let result = tokio::time::timeout(Duration::from_millis(500), handle)
        .await
        .expect("function should return within 500ms (absolute deadline = 80ms)")
        .expect("task should not panic");

    ping_task.abort();

    assert!(
        matches!(
            &result,
            Err(failure) if failure.error == InitialMessageError::TimedOut
        ),
        "expected TimedOut after absolute deadline, got: {result:?}"
    );
}

/// 验证 deadline 内收到合法 response.create 时正常返回，Ping 被正确回复 Pong。
#[tokio::test]
async fn initial_message_succeeds_within_deadline() {
    use super::receive_initial_response_create_with_deadline;

    let (mut fake, mut pair) = fake_socket();

    let handle = tokio::spawn(async move {
        receive_initial_response_create_with_deadline(&mut fake, Duration::from_secs(5)).await
    });

    // 先发一个 Ping，验证 Pong 回包且不影响后续解析
    pair.tx
        .send(axum::extract::ws::Message::Ping(vec![42].into()))
        .await
        .unwrap();
    let pong = tokio::time::timeout(Duration::from_secs(1), pair.pong_rx.recv())
        .await
        .expect("should receive pong within 1s")
        .expect("pong channel should not close");
    assert!(
        matches!(pong, axum::extract::ws::Message::Pong(ref data) if data.as_ref() == [42]),
        "expected Pong([42]), got: {pong:?}"
    );

    // 发送合法的 response.create
    let event_text = r#"{"type":"response.create","model":"gpt-4o"}"#;
    pair.tx
        .send(axum::extract::ws::Message::Text(
            event_text.to_string().into(),
        ))
        .await
        .unwrap();

    let result = tokio::time::timeout(Duration::from_secs(2), handle)
        .await
        .expect("handle should finish within 2s")
        .expect("task should not panic");
    let (text, event) = result.expect("should return Ok");
    assert_eq!(text, event_text);
    assert_eq!(event["type"], "response.create");
    assert_eq!(event["model"], "gpt-4o");
}
