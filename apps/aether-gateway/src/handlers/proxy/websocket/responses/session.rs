//! Standard OpenAI Responses WebSocket session engine.
//!
//! An incoming client socket is authenticated at Upgrade time. Its first
//! `response.create` selects a provider through the normal Responses planner.
//! Later turns reuse that upstream while the requested model remains eligible
//! on the selected key. A model change is planned again and keeps the current
//! upstream when the planner resolves to the same target; an independent
//! request may replace it, but a continuation must stay on the original
//! connection and account.

use axum::extract::ws::{Message as AxumWsMessage, WebSocket};
use serde_json::Value;
use uuid::Uuid;

use super::adapter::resolve_responses_websocket_adapter;
use super::binding::UpstreamBindingIdentity;
use super::client::consume_response_create_rate_limit;
use super::connection::{relay_bound_connection, wait_for_connection_permit_loss};
use super::continuation::{
    ResponsesWebSocketContinuationRecord, ResponsesWebSocketContinuationRegistry,
};
use super::control::resolve_responses_websocket_turn_control;
use super::lifecycle::{
    await_pending_adapter_observation, await_pending_turn_finalization,
    await_turn_finalization_handle, finalize_active_turn, finalize_unbound_turn,
    responses_websocket_turn_start_close, send_responses_websocket_turn_start_error,
};
use super::ownership::{
    await_owned_responses_websocket_plan, begin_responses_websocket_turn_with_planned_lease,
    spawn_owned_responses_websocket_plan, OwnedResponsesWebSocketDecision,
};
use super::plan_admission::{
    acquire_responses_websocket_plan_admission, responses_websocket_plan_admission_close,
    send_responses_websocket_plan_admission_error,
    terminate_responses_websocket_for_plan_permit_loss,
};
use super::redaction::redact_responses_websocket_client_event_with_reasoning_replay_policy;
use super::relay_policy::{fatal_relay_policy, FatalRelaySignal};
use super::request::{
    build_planning_parts, planned_request_uses_codex_responses_lite, planned_response_create_event,
    prepare_responses_lite_continuation, validate_response_create_previous_response_id,
    validate_response_create_stream_id_support, validated_named_stream_id,
    validated_response_create_model, ResponsesLiteStaticConfig,
};
use super::state::BoundResponsesConnection;
use super::turn::{prepare_responses_websocket_turn_decision, ResponsesWebSocketTurnOutcome};
use super::turn_state::LogicalTurn;
use super::upstream::{
    bind_responses_upstream, close_bound_upstream, ResponsesWebSocketUpstreamBindError,
};

use crate::ai_serving::ResponsesWebSocketPinnedCandidate;
use crate::handlers::proxy::websocket::ingress::{
    WebSocketConnectionLog, WebSocketConnectionLogSpec, WebSocketRequestContext,
};
use crate::handlers::proxy::websocket::session::{
    CLOSE_INTERNAL_ERROR, CLOSE_POLICY_VIOLATION, CLOSE_TRY_AGAIN,
    RESPONSES_WEBSOCKET_SESSION_LIMITS, WEBSOCKET_LOG_TRANSPORT,
};
use crate::handlers::proxy::websocket::transport::{
    close_client_socket, send_gateway_error, send_gateway_error_with_status,
    send_gateway_error_with_stream_id, send_responses_websocket_error_with_param,
};
use crate::privacy::RedactionSession;
use crate::AppState;

const RESPONSES_WEBSOCKET_LOG_TARGET: &str = "aether_gateway::handlers::proxy::responses_ws";
const CONTINUATION_LOOKUP_TIMEOUT: std::time::Duration = std::time::Duration::from_millis(500);
const RESPONSES_CONNECTION_LOG_SPEC: WebSocketConnectionLogSpec = WebSocketConnectionLogSpec {
    opened_event_name: "responses_websocket_connection_opened",
    closed_event_name: "responses_websocket_connection_closed",
    opened_message: "gateway accepted Responses WebSocket connection",
    closed_message: "gateway closed Responses WebSocket connection",
    execution_path: "responses_websocket_bridge",
    provider_type: "responses",
};

macro_rules! warn {
    ($($arg:tt)*) => {
        tracing::warn!(target: RESPONSES_WEBSOCKET_LOG_TARGET, $($arg)*)
    };
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum InitialMessageError {
    TimedOut,
    ClientClosed,
    ClientRead,
    UnsupportedFrame,
    InvalidJson,
    MissingResponseCreate,
    MissingModel,
    InvalidModel,
    InvalidPreviousResponseId,
    InvalidStreamId,
    UnsupportedStreamId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct InitialMessageFrameMetadata {
    opcode: &'static str,
    bytes: usize,
}

impl InitialMessageFrameMetadata {
    fn from_message(message: &AxumWsMessage) -> Self {
        match message {
            AxumWsMessage::Text(text) => Self {
                opcode: "text",
                bytes: text.len(),
            },
            AxumWsMessage::Binary(payload) => Self {
                opcode: "binary",
                bytes: payload.len(),
            },
            AxumWsMessage::Ping(payload) => Self {
                opcode: "ping",
                bytes: payload.len(),
            },
            AxumWsMessage::Pong(payload) => Self {
                opcode: "pong",
                bytes: payload.len(),
            },
            AxumWsMessage::Close(frame) => Self {
                opcode: "close",
                // Only the payload length is retained. The untrusted close
                // reason itself must never cross the logging boundary.
                bytes: frame
                    .as_ref()
                    .map_or(0, |frame| 2usize.saturating_add(frame.reason.len())),
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct InitialMessageFailure {
    error: InitialMessageError,
    last_frame: Option<InitialMessageFrameMetadata>,
    stream_id: Option<String>,
}

impl InitialMessageFailure {
    const fn new(
        error: InitialMessageError,
        last_frame: Option<InitialMessageFrameMetadata>,
    ) -> Self {
        Self {
            error,
            last_frame,
            stream_id: None,
        }
    }

    fn for_event(
        error: InitialMessageError,
        last_frame: Option<InitialMessageFrameMetadata>,
        event: &Value,
    ) -> Self {
        // Preserve a valid lane identity for any request-scoped validation
        // error. Invalid `stream_id` values are rejected by the grammar helper
        // and are never reflected back to the client.
        let stream_id = validated_named_stream_id(event).map(str::to_string);
        Self {
            error,
            last_frame,
            stream_id,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct InitialMessageDiagnostic {
    error_code: &'static str,
    error_kind: &'static str,
    client_message: Option<&'static str>,
    close_code: u16,
    timed_out: bool,
    last_frame_opcode: Option<&'static str>,
    last_frame_bytes: Option<usize>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ConnectionTermination {
    ConnectionLimitReached,
    ConnectionAdmissionLost,
}

impl InitialMessageError {
    const fn code(self) -> &'static str {
        match self {
            Self::TimedOut => "initial_response_create_timeout",
            Self::ClientClosed => "client_closed",
            Self::ClientRead => "client_read_failed",
            Self::UnsupportedFrame => "initial_response_create_must_be_text",
            Self::InvalidJson => "invalid_response_create",
            Self::MissingResponseCreate => "expected_response_create",
            Self::MissingModel => "response_create_model_required",
            Self::InvalidModel => "invalid_response_create_model",
            Self::InvalidPreviousResponseId => "invalid_response_create_previous_response_id",
            Self::InvalidStreamId => "invalid_response_create_stream_id",
            Self::UnsupportedStreamId => "responses_websocket_named_stream_unsupported",
        }
    }

    const fn close_code(self) -> u16 {
        match self {
            Self::TimedOut => CLOSE_TRY_AGAIN,
            Self::ClientClosed => 1000,
            Self::ClientRead | Self::UnsupportedFrame | Self::InvalidJson => CLOSE_POLICY_VIOLATION,
            Self::MissingResponseCreate | Self::MissingModel | Self::InvalidModel => {
                CLOSE_POLICY_VIOLATION
            }
            Self::InvalidPreviousResponseId => CLOSE_POLICY_VIOLATION,
            Self::InvalidStreamId => CLOSE_POLICY_VIOLATION,
            Self::UnsupportedStreamId => CLOSE_POLICY_VIOLATION,
        }
    }

    const fn client_message(self) -> Option<&'static str> {
        match self {
            Self::TimedOut => Some("Timed out waiting for the initial response.create event"),
            Self::ClientClosed => None,
            Self::ClientRead => {
                Some("Failed to read the initial WebSocket event before response.create")
            }
            Self::UnsupportedFrame => {
                Some("The initial response.create event must be sent as a text WebSocket message")
            }
            Self::InvalidJson => {
                Some("The initial WebSocket text message must be a JSON response.create object")
            }
            Self::MissingResponseCreate => {
                Some("The initial WebSocket JSON object must have type response.create")
            }
            Self::MissingModel => Some("The initial response.create event must include a model"),
            Self::InvalidModel => {
                Some("response.create.model must be a non-empty string no longer than 256 bytes")
            }
            Self::InvalidPreviousResponseId => {
                Some("response.create.previous_response_id must be null or a non-empty string")
            }
            Self::InvalidStreamId => Some(
                "response.create.stream_id must be 1-256 ASCII letters, numbers, underscores, hyphens, or periods",
            ),
            Self::UnsupportedStreamId => Some(
                "Aether currently supports only the implicit default WebSocket lane; omit response.create.stream_id",
            ),
        }
    }

    const fn kind(self) -> &'static str {
        match self {
            Self::TimedOut => "timeout",
            Self::ClientClosed => "client_closed",
            Self::ClientRead => "client_read_failed",
            Self::UnsupportedFrame => "unsupported_frame",
            Self::InvalidJson => "invalid_json",
            Self::MissingResponseCreate => "unexpected_event_type",
            Self::MissingModel => "missing_model",
            Self::InvalidModel => "invalid_model",
            Self::InvalidPreviousResponseId => "invalid_previous_response_id",
            Self::InvalidStreamId => "invalid_stream_id",
            Self::UnsupportedStreamId => "unsupported_stream_id",
        }
    }

    const fn timed_out(self) -> bool {
        matches!(self, Self::TimedOut)
    }
}

fn initial_message_diagnostic(failure: &InitialMessageFailure) -> InitialMessageDiagnostic {
    InitialMessageDiagnostic {
        error_code: failure.error.code(),
        error_kind: failure.error.kind(),
        client_message: failure.error.client_message(),
        close_code: failure.error.close_code(),
        timed_out: failure.error.timed_out(),
        last_frame_opcode: failure.last_frame.map(|frame| frame.opcode),
        last_frame_bytes: failure.last_frame.map(|frame| frame.bytes),
    }
}

fn log_initial_message_failure(
    context: &WebSocketRequestContext,
    failure: &InitialMessageFailure,
    upgraded_at: std::time::Instant,
) {
    let diagnostic = initial_message_diagnostic(failure);
    let auth_context = context.decision.auth_context.as_ref();

    // Initial-frame validation intentionally runs before provider planning, so
    // no provider identity exists yet. Keep the usual identity fields in the
    // event schema and say so explicitly instead of guessing from request data.
    warn!(
        event_name = "responses_websocket_initial_event_rejected",
        log_type = "ops",
        transport = WEBSOCKET_LOG_TRANSPORT,
        websocket = true,
        trace_id = %context.trace_id,
        user_id = auth_context.map(|auth| auth.user_id.as_str()).unwrap_or("-"),
        api_key_id = auth_context
            .map(|auth| auth.api_key_id.as_str())
            .unwrap_or("-"),
        provider_selected = false,
        provider_id = "<unplanned>",
        endpoint_id = "<unplanned>",
        key_id = "<unplanned>",
        path = %context.uri.path(),
        route_class = context.decision.route_class.as_deref().unwrap_or("-"),
        route_kind = context.decision.route_kind.as_deref().unwrap_or("-"),
        error_code = diagnostic.error_code,
        error_kind = diagnostic.error_kind,
        close_code = diagnostic.close_code,
        timed_out = diagnostic.timed_out,
        upgrade_to_initial_outcome_ms = upgraded_at.elapsed().as_millis() as u64,
        initial_message_timeout_ms = RESPONSES_WEBSOCKET_SESSION_LIMITS
            .initial_message_timeout
            .as_millis() as u64,
        last_frame_opcode = diagnostic.last_frame_opcode.unwrap_or("none"),
        last_frame_bytes = ?diagnostic.last_frame_bytes,
        "gateway rejected the initial Responses WebSocket event"
    );
}

pub(super) async fn run_responses_websocket(
    mut client_socket: WebSocket,
    state: AppState,
    mut context: WebSocketRequestContext,
) {
    let upgraded_at = std::time::Instant::now();
    // The public connection limit starts when the HTTP Upgrade hands us the
    // socket, not after provider planning and its upstream handshake finish.
    let connection_deadline =
        tokio::time::Instant::now() + RESPONSES_WEBSOCKET_SESSION_LIMITS.max_connection_duration;
    let connection_permit = context.websocket_connection_permit.take();
    let connection_log = WebSocketConnectionLog::new(&context, RESPONSES_CONNECTION_LOG_SPEC);
    connection_log.log_opened();

    let bootstrap_result = supervise_responses_websocket_phase(
        bootstrap_responses_websocket(&mut client_socket, state.clone(), &context, upgraded_at),
        connection_deadline,
        connection_permit.as_ref(),
    )
    .await;

    let mut bound = match bootstrap_result {
        Ok(Some(bound)) => bound,
        Ok(None) => return,
        Err(termination) => {
            // The bootstrap future (and therefore every lease/attempt guard it
            // owned) has been dropped before the permit or client socket is
            // touched here.
            drop(connection_permit);
            close_terminated_bootstrap(&mut client_socket, &context, termination).await;
            return;
        }
    };

    let relay_result = supervise_responses_websocket_phase(
        relay_bound_connection(&mut client_socket, &mut bound, &state, &context),
        connection_deadline,
        connection_permit.as_ref(),
    )
    .await;

    if let Err(termination) = relay_result {
        // The relay future has been dropped before cleanup starts. This makes
        // cancellation safe even when a client-frame branch was awaiting a
        // live auth refresh, redaction, planning, admission, or rebind.
        let outcome = match termination {
            ConnectionTermination::ConnectionLimitReached => {
                ResponsesWebSocketTurnOutcome::connection_limit_reached()
            }
            ConnectionTermination::ConnectionAdmissionLost => {
                ResponsesWebSocketTurnOutcome::connection_admission_lost()
            }
        };
        finalize_active_turn(&mut bound, &state, outcome).await;
        close_bound_upstream(&mut bound).await;
        drop(connection_permit);
        close_terminated_relay(&mut client_socket, &context, termination).await;
    } else {
        drop(connection_permit);
    }
    await_pending_turn_finalization(&mut bound).await;
    await_pending_adapter_observation(&mut bound).await;
}

async fn supervise_responses_websocket_phase<F, T>(
    phase: F,
    connection_deadline: tokio::time::Instant,
    connection_permit: Option<&aether_runtime::AdmissionPermit>,
) -> Result<T, ConnectionTermination>
where
    F: std::future::Future<Output = T>,
{
    tokio::pin!(phase);
    tokio::select! {
        biased;
        _ = tokio::time::sleep_until(connection_deadline) => {
            Err(ConnectionTermination::ConnectionLimitReached)
        }
        _ = wait_for_connection_permit_loss(connection_permit) => {
            Err(ConnectionTermination::ConnectionAdmissionLost)
        }
        output = &mut phase => Ok(output),
    }
}

async fn bootstrap_responses_websocket(
    client_socket: &mut WebSocket,
    state: AppState,
    context: &WebSocketRequestContext,
    upgraded_at: std::time::Instant,
) -> Option<BoundResponsesConnection> {
    let (_first_text, first_event) = match receive_initial_response_create(client_socket).await {
        Ok(value) => value,
        Err(failure) => {
            if let Some(client_message) = failure.error.client_message() {
                log_initial_message_failure(context, &failure, upgraded_at);
                send_gateway_error_with_stream_id(
                    client_socket,
                    failure.error.code(),
                    client_message,
                    failure.stream_id.as_deref(),
                )
                .await;
                close_client_socket(
                    client_socket,
                    failure.error.close_code(),
                    "invalid_initial_event",
                )
                .await;
            }
            return None;
        }
    };

    let initial_previous_response_id = first_event
        .get("previous_response_id")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .map(str::to_string);

    // Keep the response-chain identity on the client's raw configuration.
    // Redaction sentinels may rotate between turns, but that must not turn
    // identical plaintext tools/instructions into a synthetic config change.
    let raw_responses_lite_static_config =
        ResponsesLiteStaticConfig::from_response_create(&first_event);

    let first_logical_turn_id = Uuid::now_v7().to_string();
    let mut planning_parts = build_planning_parts(context);
    let first_codex_fingerprint_context =
        crate::ai_serving::codex_context::attach_codex_logical_turn_context(
            &mut planning_parts,
            &first_event,
            &first_logical_turn_id,
        );
    let turn_control = match resolve_responses_websocket_turn_control(
        &state,
        context,
        &planning_parts,
        &first_event,
    )
    .await
    {
        Ok(control) => control,
        Err(error) => {
            warn!(
                event_name = "responses_websocket_initial_turn_control_rejected",
                log_type = "ops",
                transport = WEBSOCKET_LOG_TRANSPORT,
                websocket = true,
                trace_id = %context.trace_id,
                error = ?error,
                "gateway rejected a Responses WebSocket initial turn after live policy refresh"
            );
            send_responses_websocket_turn_start_error(client_socket, &error).await;
            let (close_code, close_reason) = responses_websocket_turn_start_close(&error);
            close_client_socket(client_socket, close_code, close_reason).await;
            return None;
        }
    };
    match consume_response_create_rate_limit(
        &state,
        &turn_control.decision,
        turn_control.rpm_bypassed,
    )
    .await
    {
        Ok(true) => {}
        Ok(false) => {
            send_gateway_error_with_status(
                client_socket,
                429,
                "rate_limit_exceeded",
                "Too many response.create events; retry later",
            )
            .await;
            close_client_socket(client_socket, CLOSE_TRY_AGAIN, "rate_limit_exceeded").await;
            return None;
        }
        Err(_) => {
            warn!(
                event_name = "responses_websocket_rate_limit_check_failed",
                log_type = "ops",
                transport = WEBSOCKET_LOG_TRANSPORT,
                websocket = true,
                trace_id = %context.trace_id,
                "gateway failed to consume WebSocket response rate limit"
            );
            send_gateway_error_with_status(
                client_socket,
                503,
                "gateway_rate_limit_unavailable",
                "Gateway could not evaluate the response rate limit",
            )
            .await;
            close_client_socket(
                client_socket,
                CLOSE_INTERNAL_ERROR,
                "rate_limit_unavailable",
            )
            .await;
            return None;
        }
    }

    let plan_usage_admission = match acquire_responses_websocket_plan_admission(
        &state,
        &turn_control.decision,
        &first_logical_turn_id,
    )
    .await
    {
        Ok(admission) => admission,
        Err(error) => {
            warn!(
                event_name = "responses_websocket_initial_plan_usage_rejected",
                log_type = "event",
                transport = WEBSOCKET_LOG_TRANSPORT,
                websocket = true,
                trace_id = %context.trace_id,
                logical_turn_id = %first_logical_turn_id,
                error = ?error,
                "gateway rejected the initial Responses WebSocket turn at its subscription plan limit"
            );
            send_responses_websocket_plan_admission_error(client_socket, &error).await;
            let (close_code, close_reason) = responses_websocket_plan_admission_close(&error);
            close_client_socket(client_socket, close_code, close_reason).await;
            return None;
        }
    };
    let crate::plan_usage_policy::PlanUsageAdmission {
        permit: plan_usage_permit,
        policy_snapshot: plan_usage_policy_snapshot,
    } = plan_usage_admission;
    // A cross-socket response chain must be owned by this exact live
    // authenticated principal. Missing, expired, corrupt or unavailable state
    // fails closed; allowing the normal scheduler to choose a provider/key
    // would disclose an opaque response ID to an unrelated account.
    let continuation_record = if let Some(previous_response_id) =
        initial_previous_response_id.as_deref()
    {
        let Some(auth_context) = turn_control.decision.auth_context.as_ref() else {
            reject_initial_previous_response(client_socket).await;
            return None;
        };
        let registry = ResponsesWebSocketContinuationRegistry::new(state.runtime_state.as_ref());
        match tokio::time::timeout(
            CONTINUATION_LOOKUP_TIMEOUT,
            registry.lookup(
                auth_context.user_id.as_str(),
                auth_context.api_key_id.as_str(),
                previous_response_id,
            ),
        )
        .await
        {
            Ok(Ok(Some(record)))
                if record.client_model()
                    == first_event
                        .get("model")
                        .and_then(Value::as_str)
                        .map(str::trim)
                        .unwrap_or_default()
                    && !record.has_connection_local_redaction() =>
            {
                Some(record)
            }
            Ok(Ok(Some(record))) => {
                warn!(
                    event_name = "responses_websocket_continuation_registry_rejected",
                    log_type = "ops",
                    transport = WEBSOCKET_LOG_TRANSPORT,
                    websocket = true,
                    trace_id = %context.trace_id,
                    user_id = %auth_context.user_id,
                    api_key_id = %auth_context.api_key_id,
                    provider_id = %record.pinned_candidate().provider_id(),
                    endpoint_id = %record.pinned_candidate().endpoint_id(),
                    key_id = %record.pinned_candidate().key_id(),
                    reason = if record.has_connection_local_redaction() {
                        "connection_local_redaction_state_unavailable"
                    } else {
                        "client_model_mismatch"
                    },
                    "gateway rejected a cross-socket Responses continuation"
                );
                reject_initial_previous_response(client_socket).await;
                return None;
            }
            Ok(Ok(None)) => {
                warn!(
                    event_name = "responses_websocket_continuation_registry_miss",
                    log_type = "ops",
                    transport = WEBSOCKET_LOG_TRANSPORT,
                    websocket = true,
                    trace_id = %context.trace_id,
                    user_id = %auth_context.user_id,
                    api_key_id = %auth_context.api_key_id,
                    reason = "not_found_or_expired",
                    "gateway could not prove ownership of a cross-socket Responses continuation"
                );
                reject_initial_previous_response(client_socket).await;
                return None;
            }
            Ok(Err(error)) => {
                warn!(
                    event_name = "responses_websocket_continuation_registry_lookup_failed",
                    log_type = "ops",
                    transport = WEBSOCKET_LOG_TRANSPORT,
                    websocket = true,
                    trace_id = %context.trace_id,
                    user_id = %auth_context.user_id,
                    api_key_id = %auth_context.api_key_id,
                    reason = error.kind(),
                    "gateway failed closed while looking up a cross-socket Responses continuation"
                );
                reject_initial_previous_response(client_socket).await;
                return None;
            }
            Err(_) => {
                warn!(
                    event_name = "responses_websocket_continuation_registry_lookup_failed",
                    log_type = "ops",
                    transport = WEBSOCKET_LOG_TRANSPORT,
                    websocket = true,
                    trace_id = %context.trace_id,
                    user_id = %auth_context.user_id,
                    api_key_id = %auth_context.api_key_id,
                    reason = "timeout",
                    timeout_ms = CONTINUATION_LOOKUP_TIMEOUT.as_millis() as u64,
                    "gateway timed out looking up a cross-socket Responses continuation"
                );
                reject_initial_previous_response(client_socket).await;
                return None;
            }
        }
    } else {
        None
    };

    // Validate and remove any repeated Lite static prefix against the stored
    // chain identity before PII redaction rotates per-turn sentinels.
    let first_event = match continuation_record
        .as_ref()
        .and_then(ResponsesWebSocketContinuationRecord::responses_lite_static_config)
    {
        Some(stored) => match prepare_responses_lite_continuation(&first_event, stored) {
            Ok(prepared) => prepared,
            Err(_) => {
                warn!(
                    event_name = "responses_websocket_continuation_static_contract_rejected",
                    log_type = "ops",
                    transport = WEBSOCKET_LOG_TRANSPORT,
                    websocket = true,
                    trace_id = %context.trace_id,
                    "gateway rejected changed Responses Lite static configuration on a continuation"
                );
                reject_initial_previous_response(client_socket).await;
                return None;
            }
        },
        None => first_event,
    };

    // 请求侧脱敏必须在规划之前完成，而且这一轮只在这里做一次：planner 会把这份
    // body 写进 upstream 请求体和审计 original_request_body，绑定上游的首条
    // response.create 也从它派生。脱敏失败时直接断开，绝不退回原文发上游。
    let reasoning_replay_policy = continuation_record
        .as_ref()
        .map(ResponsesWebSocketContinuationRecord::reasoning_replay_policy)
        .unwrap_or_default();
    let redacted_first_event =
        redact_responses_websocket_client_event_with_reasoning_replay_policy(
            &state,
            &planning_parts,
            &turn_control.decision,
            &first_event,
            reasoning_replay_policy,
        )
        .await;
    // 首轮的 mask session 要活到响应帧还原，但连接此刻还没绑定，只能先接住，
    // 等 `bind_responses_upstream` 之后登记到连接上。
    let (first_event, first_turn_redaction_session) = match redacted_first_event {
        Ok(Some(redaction)) => {
            // 这一轮真的替换过内容：把事实带进 planning parts，
            // planner 会把它写进使用记录（chat_pii_redaction）。
            planning_parts
                .extensions
                .insert(crate::privacy::ChatPiiRedactionTurnMarker);
            (redaction.client_event, Some(redaction.session))
        }
        Ok(None) => (first_event, None),
        Err(error) => {
            warn!(
                event_name = "responses_websocket_redaction_failed",
                log_type = "ops",
                transport = WEBSOCKET_LOG_TRANSPORT,
                websocket = true,
                trace_id = %context.trace_id,
                error = ?error,
                "gateway could not apply chat PII redaction to the initial Responses WebSocket event"
            );
            send_gateway_error_with_status(
                client_socket,
                500,
                "responses_websocket_redaction_unavailable",
                "Gateway could not apply the configured PII redaction",
            )
            .await;
            close_client_socket(
                client_socket,
                CLOSE_INTERNAL_ERROR,
                "responses_websocket_redaction_unavailable",
            )
            .await;
            return None;
        }
    };

    let pinned_candidate = match initial_continuation_planning_candidate(
        initial_previous_response_id.is_some(),
        continuation_record
            .as_ref()
            .map(|record| record.pinned_candidate().clone()),
    ) {
        Ok(candidate) => candidate,
        Err(_) => {
            // Keep this invariant next to the planner boundary as a second
            // fail-closed guard: an unproved response ID must never enter the
            // ordinary scheduler and land on an unrelated provider/key.
            reject_initial_previous_response(client_socket).await;
            return None;
        }
    };
    let planned = match await_owned_responses_websocket_plan(spawn_owned_responses_websocket_plan(
        state.clone(),
        planning_parts,
        context.trace_id.clone(),
        turn_control.decision.clone(),
        turn_control.auth_snapshot.clone(),
        first_event.clone(),
        None,
        None,
        pinned_candidate,
    ))
    .await
    {
        Ok(Some(decision)) => decision,
        Ok(None) => {
            if continuation_record.is_some() {
                warn!(
                    event_name = "responses_websocket_continuation_pinned_candidate_unavailable",
                    log_type = "ops",
                    transport = WEBSOCKET_LOG_TRANSPORT,
                    websocket = true,
                    trace_id = %context.trace_id,
                    "gateway could not revalidate the registered Responses continuation binding"
                );
                reject_initial_previous_response(client_socket).await;
                return None;
            }
            send_gateway_error_with_status(
                client_socket,
                503,
                "responses_provider_unavailable",
                "No eligible WebSocket-enabled Responses provider is available",
            )
            .await;
            close_client_socket(
                client_socket,
                CLOSE_TRY_AGAIN,
                "responses_provider_unavailable",
            )
            .await;
            return None;
        }
        Err(_) => {
            warn!(
                event_name = "responses_websocket_planning_failed",
                log_type = "ops",
                transport = WEBSOCKET_LOG_TRANSPORT,
                websocket = true,
                trace_id = %context.trace_id,
                "gateway failed to plan Responses WebSocket provider request"
            );
            send_gateway_error_with_status(
                client_socket,
                503,
                "responses_provider_unavailable",
                "Gateway could not prepare a Provider connection",
            )
            .await;
            close_client_socket(
                client_socket,
                CLOSE_INTERNAL_ERROR,
                "responses_planning_failed",
            )
            .await;
            return None;
        }
    };

    let OwnedResponsesWebSocketDecision {
        planned,
        planning_parts,
        planned_lease,
    } = planned;
    let adapter_kind = planned.adapter;
    let adapter = resolve_responses_websocket_adapter(adapter_kind);
    let normalization = planned.normalization;
    let decision = planned.execution;
    if let Some(record) = continuation_record.as_ref() {
        let planned_candidate = ResponsesWebSocketPinnedCandidate::from_decision(&decision);
        let planned_provider_model = decision
            .provider_request_body
            .as_ref()
            .and_then(|body| body.get("model"))
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .or_else(|| {
                decision
                    .mapped_model
                    .as_deref()
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
            });
        let planned_binding = UpstreamBindingIdentity::from_decision(adapter, &decision).ok();
        let planned_uses_responses_lite =
            planned_request_uses_codex_responses_lite(&decision, &normalization);
        let matches_record = record.adapter() == adapter_kind
            && planned_candidate.as_ref() == Some(record.pinned_candidate())
            && planned_provider_model == Some(record.provider_model())
            && responses_lite_contract_modes_match(
                record.responses_lite_static_config().is_some(),
                planned_uses_responses_lite,
            )
            && planned_binding
                .as_ref()
                .is_some_and(|binding| record.matches_contract(binding, &normalization));
        if !matches_record {
            planned_lease.release().await;
            warn!(
                event_name = "responses_websocket_continuation_binding_rejected",
                log_type = "ops",
                transport = WEBSOCKET_LOG_TRANSPORT,
                websocket = true,
                trace_id = %context.trace_id,
                provider_id = %record.pinned_candidate().provider_id(),
                endpoint_id = %record.pinned_candidate().endpoint_id(),
                key_id = %record.pinned_candidate().key_id(),
                "gateway rejected a cross-socket continuation after pinned planning changed its contract"
            );
            reject_initial_previous_response(client_socket).await;
            return None;
        }
    }
    let first_provider_event =
        match planned_response_create_event(&decision, &normalization, &first_event).and_then(
            |event| {
                serde_json::from_str::<Value>(&event)
                    .map_err(|_| "responses_websocket_request_invalid")
            },
        ) {
            Ok(event) => event,
            Err(code) => {
                planned_lease.release().await;
                warn!(
                    event_name = "responses_websocket_initial_event_normalization_failed",
                    log_type = "ops",
                    transport = WEBSOCKET_LOG_TRANSPORT,
                    websocket = true,
                    trace_id = %context.trace_id,
                    error_code = code,
                    "gateway could not normalize the initial Responses WebSocket event"
                );
                send_gateway_error(
                    client_socket,
                    code,
                    "Gateway could not prepare the Responses response.create event",
                )
                .await;
                close_client_socket(client_socket, CLOSE_POLICY_VIOLATION, code).await;
                return None;
            }
        };
    let first_turn_decision = prepare_responses_websocket_turn_decision(
        &decision,
        context.trace_id.clone(),
        true,
        &first_event,
        &first_provider_event,
        &context.trace_id,
        1,
        &first_logical_turn_id,
        1,
    );
    let mut first_turn = match begin_responses_websocket_turn_with_planned_lease(
        &state,
        &context.trace_id,
        planning_parts,
        &turn_control.decision,
        first_turn_decision,
        &first_event,
        plan_usage_policy_snapshot.clone(),
        planned_lease,
    )
    .await
    {
        Ok(turn) => turn,
        Err(error) => {
            warn!(
                event_name = "responses_websocket_turn_lifecycle_start_failed",
                log_type = "ops",
                transport = WEBSOCKET_LOG_TRANSPORT,
                websocket = true,
                trace_id = %context.trace_id,
                error = ?error,
                "gateway could not start Responses WebSocket usage/audit lifecycle"
            );
            send_responses_websocket_turn_start_error(client_socket, &error).await;
            let (close_code, close_reason) = responses_websocket_turn_start_close(&error);
            close_client_socket(client_socket, close_code, close_reason).await;
            return None;
        }
    };

    let mut bound = match bind_responses_upstream(
        &decision,
        normalization,
        &first_event,
        adapter,
        plan_usage_permit.as_ref(),
        |state| first_turn.record_upstream_request_state(state),
    )
    .await
    {
        Ok(connection) => connection,
        Err(ResponsesWebSocketUpstreamBindError::PlanUsagePermitLost) => {
            first_turn
                .release_plan_usage_cost_before_upstream_send(
                    &state,
                    "initial_plan_usage_permit_lost",
                )
                .await;
            let finalizer = finalize_unbound_turn(
                state.clone(),
                first_turn,
                ResponsesWebSocketTurnOutcome::connection_admission_lost(),
            );
            warn!(
                event_name = "responses_websocket_initial_plan_usage_concurrency_lost",
                log_type = "ops",
                transport = WEBSOCKET_LOG_TRANSPORT,
                websocket = true,
                trace_id = %context.trace_id,
                "gateway stopped the initial Responses WebSocket turn before its upstream send after the subscription plan concurrency lease became unhealthy"
            );
            terminate_responses_websocket_for_plan_permit_loss(client_socket).await;
            await_turn_finalization_handle(finalizer).await;
            return None;
        }
        Err(ResponsesWebSocketUpstreamBindError::Transport(code)) => {
            let finalizer = finalize_unbound_turn(
                state.clone(),
                first_turn,
                ResponsesWebSocketTurnOutcome::upstream_connect_failed(code),
            );
            warn!(
                event_name = "responses_websocket_upstream_connect_failed",
                log_type = "ops",
                transport = WEBSOCKET_LOG_TRANSPORT,
                websocket = true,
                trace_id = %context.trace_id,
                error_code = code,
                "gateway failed to establish Responses WebSocket upstream"
            );
            send_gateway_error_with_status(
                client_socket,
                502,
                code,
                "Gateway could not establish the Provider connection",
            )
            .await;
            close_client_socket(client_socket, CLOSE_TRY_AGAIN, code).await;
            await_turn_finalization_handle(finalizer).await;
            return None;
        }
    };
    if bound.responses_lite_static_config.is_some() {
        bound.responses_lite_static_config = continuation_record
            .as_ref()
            .and_then(ResponsesWebSocketContinuationRecord::responses_lite_static_config)
            .cloned()
            .or(Some(raw_responses_lite_static_config));
    }
    if let Some(previous_response_id) = initial_previous_response_id.as_deref() {
        // Reaching this point means the principal-scoped registry record and
        // the newly planned physical binding were both proved above. Keep the
        // parent as persisted ownership: the new physical socket may hydrate
        // it, but a failed attempt can evict only its connection-local copy.
        bound
            .continuation_response_ids
            .remember_persisted(previous_response_id);
    }
    first_turn.set_provider_response_headers(bound.upstream_response_headers.clone());
    let first_event_pii_redacted = first_turn_redaction_session.is_some();
    if let Some(session) = first_turn_redaction_session {
        register_initial_redaction_session(&mut bound, session);
    }
    bound.turn_state.begin(
        LogicalTurn::new(first_event, 1, first_logical_turn_id)
            .with_codex_fingerprint_context(first_codex_fingerprint_context)
            .with_provider_store(first_provider_event.get("store") == Some(&Value::Bool(true)))
            .with_turn_control(turn_control)
            .with_plan_usage_permit(plan_usage_permit)
            .with_plan_usage_policy_snapshot(plan_usage_policy_snapshot)
            .with_client_event_pii_redacted(first_event_pii_redacted),
        first_turn,
    );

    Some(bound)
}

fn register_initial_redaction_session(
    bound: &mut BoundResponsesConnection,
    mut session: RedactionSession,
) {
    session.set_reasoning_replay_policy(bound.body_normalization.reasoning_replay_policy());
    bound.redaction_restorer.register(session);
}

fn responses_lite_contract_modes_match(
    stored_chain_uses_responses_lite: bool,
    planned_request_uses_responses_lite: bool,
) -> bool {
    stored_chain_uses_responses_lite == planned_request_uses_responses_lite
}

fn initial_continuation_planning_candidate(
    has_previous_response_id: bool,
    registered_candidate: Option<ResponsesWebSocketPinnedCandidate>,
) -> Result<Option<ResponsesWebSocketPinnedCandidate>, &'static str> {
    match (has_previous_response_id, registered_candidate) {
        (false, None) => Ok(None),
        (true, Some(candidate)) => Ok(Some(candidate)),
        // A registry miss/corruption and an impossible stray record both fail
        // closed. Neither state is allowed to turn into an unpinned plan.
        (true, None) | (false, Some(_)) => Err("previous_response_not_found"),
    }
}

async fn reject_initial_previous_response(client_socket: &mut WebSocket) {
    send_responses_websocket_error_with_param(
        client_socket,
        400,
        "invalid_request_error",
        "previous_response_not_found",
        "The previous response is unavailable for this authenticated WebSocket connection",
        "previous_response_id",
    )
    .await;
    close_client_socket(
        client_socket,
        CLOSE_POLICY_VIOLATION,
        "previous_response_not_found",
    )
    .await;
}

async fn close_terminated_bootstrap(
    client_socket: &mut WebSocket,
    context: &WebSocketRequestContext,
    termination: ConnectionTermination,
) {
    match termination {
        ConnectionTermination::ConnectionLimitReached => {
            warn!(
                event_name = "responses_websocket_bootstrap_connection_limit_reached",
                log_type = "ops",
                transport = WEBSOCKET_LOG_TRANSPORT,
                websocket = true,
                trace_id = %context.trace_id,
                "gateway stopped a Responses WebSocket bootstrap at the absolute connection deadline"
            );
            send_gateway_error_with_status(
                client_socket,
                503,
                "websocket_connection_limit_reached",
                "WebSocket connection duration limit reached; reconnect to continue",
            )
            .await;
            close_client_socket(client_socket, CLOSE_TRY_AGAIN, "connection_limit_reached").await;
        }
        ConnectionTermination::ConnectionAdmissionLost => {
            let policy = fatal_relay_policy(FatalRelaySignal::ConnectionAdmissionLost);
            warn!(
                event_name = "responses_websocket_bootstrap_connection_admission_lost",
                log_type = "ops",
                transport = WEBSOCKET_LOG_TRANSPORT,
                websocket = true,
                trace_id = %context.trace_id,
                "gateway stopped a Responses WebSocket bootstrap after its connection admission became unhealthy"
            );
            send_gateway_error_with_status(
                client_socket,
                policy.status_code,
                policy.error_code,
                policy.client_message,
            )
            .await;
            close_client_socket(client_socket, policy.close_code, policy.close_reason).await;
        }
    }
}

async fn close_terminated_relay(
    client_socket: &mut WebSocket,
    context: &WebSocketRequestContext,
    termination: ConnectionTermination,
) {
    match termination {
        ConnectionTermination::ConnectionLimitReached => {
            warn!(
                event_name = "responses_websocket_connection_limit_reached",
                log_type = "ops",
                transport = WEBSOCKET_LOG_TRANSPORT,
                websocket = true,
                trace_id = %context.trace_id,
                "gateway stopped a Responses WebSocket relay at the absolute connection deadline"
            );
            send_gateway_error_with_status(
                client_socket,
                503,
                "websocket_connection_limit_reached",
                "WebSocket connection duration limit reached; reconnect to continue",
            )
            .await;
            close_client_socket(client_socket, CLOSE_TRY_AGAIN, "connection_limit_reached").await;
        }
        ConnectionTermination::ConnectionAdmissionLost => {
            let policy = fatal_relay_policy(FatalRelaySignal::ConnectionAdmissionLost);
            warn!(
                event_name = "responses_websocket_connection_admission_lost",
                log_type = "ops",
                transport = WEBSOCKET_LOG_TRANSPORT,
                websocket = true,
                trace_id = %context.trace_id,
                "gateway stopped a Responses WebSocket relay after its connection admission became unhealthy"
            );
            send_gateway_error_with_status(
                client_socket,
                policy.status_code,
                policy.error_code,
                policy.client_message,
            )
            .await;
            close_client_socket(client_socket, policy.close_code, policy.close_reason).await;
        }
    }
}

/// 等待客户端发送第一条 response.create 事件。
/// 使用绝对 deadline：从函数入口起计算一次截止时间，Ping/Pong 只会被正常回复，
/// 但不会重置计时器。防止客户端通过周期性 Ping 无限占用 connection permit。
async fn receive_initial_response_create(
    client_socket: &mut WebSocket,
) -> Result<(String, Value), InitialMessageFailure> {
    receive_initial_response_create_with_deadline(
        client_socket,
        RESPONSES_WEBSOCKET_SESSION_LIMITS.initial_message_timeout,
    )
    .await
}

/// 核心循环：在绝对 deadline 内等待客户端发送 response.create。
/// 泛型约束允许测试注入 fake socket，驱动真实逻辑。
///
/// - `deadline_budget`：从调用时刻起的最长等待时间，全循环共享同一截止时刻。
/// - Ping 帧被回复 Pong 但不重置计时器。
/// - Pong / 非法帧 / Close 按协议处理。
async fn receive_initial_response_create_with_deadline<S>(
    socket: &mut S,
    deadline_budget: std::time::Duration,
) -> Result<(String, Value), InitialMessageFailure>
where
    S: futures_util::Stream<Item = Result<AxumWsMessage, axum::Error>>
        + futures_util::Sink<AxumWsMessage, Error = axum::Error>
        + Unpin,
{
    use futures_util::{SinkExt as _, StreamExt as _};

    // 绝对 deadline：入口计算一次，后续所有迭代共享，Ping/Pong 不会重启
    let deadline = tokio::time::Instant::now() + deadline_budget;
    let mut last_frame = None;
    loop {
        let message = tokio::time::timeout_at(deadline, socket.next())
            .await
            .map_err(|_| InitialMessageFailure::new(InitialMessageError::TimedOut, last_frame))?;
        let Some(message) = message else {
            return Err(InitialMessageFailure::new(
                InitialMessageError::ClientClosed,
                last_frame,
            ));
        };
        let message = message
            .map_err(|_| InitialMessageFailure::new(InitialMessageError::ClientRead, last_frame))?;
        last_frame = Some(InitialMessageFrameMetadata::from_message(&message));
        match message {
            AxumWsMessage::Ping(payload) => {
                tokio::time::timeout_at(deadline, socket.send(AxumWsMessage::Pong(payload)))
                    .await
                    .map_err(|_| {
                        InitialMessageFailure::new(InitialMessageError::TimedOut, last_frame)
                    })?
                    .map_err(|_| {
                        InitialMessageFailure::new(InitialMessageError::ClientRead, last_frame)
                    })?;
            }
            AxumWsMessage::Pong(_) => {}
            AxumWsMessage::Close(_) => {
                return Err(InitialMessageFailure::new(
                    InitialMessageError::ClientClosed,
                    last_frame,
                ));
            }
            AxumWsMessage::Binary(_) => {
                return Err(InitialMessageFailure::new(
                    InitialMessageError::UnsupportedFrame,
                    last_frame,
                ));
            }
            AxumWsMessage::Text(text) => {
                let text = text.to_string();
                let event: Value = serde_json::from_str(&text).map_err(|_| {
                    InitialMessageFailure::new(InitialMessageError::InvalidJson, last_frame)
                })?;
                validate_initial_response_create(&event)
                    .map_err(|error| InitialMessageFailure::for_event(error, last_frame, &event))?;
                return Ok((text, event));
            }
        }
    }
}

fn validate_initial_response_create(event: &Value) -> Result<(), InitialMessageError> {
    let object = event.as_object().ok_or(InitialMessageError::InvalidJson)?;
    if object.get("type").and_then(Value::as_str) != Some("response.create") {
        return Err(InitialMessageError::MissingResponseCreate);
    }
    let model = object
        .get("model")
        .ok_or(InitialMessageError::MissingModel)?;
    validated_response_create_model(model).map_err(|_| InitialMessageError::InvalidModel)?;
    validate_response_create_previous_response_id(event)
        .map_err(|_| InitialMessageError::InvalidPreviousResponseId)?;
    match validate_response_create_stream_id_support(event) {
        Ok(()) => {}
        Err("invalid_response_create_stream_id") => {
            return Err(InitialMessageError::InvalidStreamId);
        }
        Err(_) => return Err(InitialMessageError::UnsupportedStreamId),
    }
    Ok(())
}

#[cfg(test)]
#[path = "session_tests.rs"]
mod tests;
