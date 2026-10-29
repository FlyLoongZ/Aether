use std::collections::BTreeMap;
use std::future::Future;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use aether_contracts::ExecutionErrorKind;
use aether_usage_runtime::{build_usage_event_data_seed, UsageEvent, UsageEventType};
use axum::body::Body;
use axum::http::{Response, StatusCode};
use serde_json::{json, Value};

use crate::ai_serving::{build_core_error_body_for_client_format, LocalCoreSyncErrorKind};
use crate::api::response::{attach_control_metadata_headers, build_client_response_from_parts};
use crate::control::GatewayControlDecision;
use crate::request_diagnostics::attach_current_request_diagnostics_and_candidate_timing_to_report_context;
use crate::{AppState, GatewayError};

use super::submission::align_gemini_error_code_with_status;

const TRANSPORT_ERROR_CLIENT_MESSAGE: &str =
    "Upstream transport failed before an HTTP response was received";
const UPSTREAM_STREAM_FAILURE_MESSAGE: &str = "Upstream response stream failed";
const EXECUTION_STREAM_PROTOCOL_FAILURE_MESSAGE: &str = "Execution runtime stream protocol failed";
const EXECUTION_STREAM_PROCESSING_FAILURE_MESSAGE: &str =
    "Execution runtime stream processing failed";
const EXECUTION_STREAM_FAILURE_MESSAGE: &str = "Execution runtime stream failed";

/// The single client-facing text table for execution failures.
///
/// Both the transport projection and the stream failure reports read it, so a
/// failure never reads differently depending on which path happened to handle
/// it. `None` means the text depends on the response (an upstream HTTP status)
/// rather than on the kind alone, and the caller supplies it.
pub(crate) fn client_message_for_execution_error_kind(
    kind: &ExecutionErrorKind,
) -> Option<&'static str> {
    match kind {
        ExecutionErrorKind::ConnectTimeout => Some("Upstream connection timed out"),
        ExecutionErrorKind::FirstByteTimeout => {
            Some("Upstream response timed out before the first byte")
        }
        ExecutionErrorKind::ReadTimeout => Some("Upstream response stream timed out"),
        ExecutionErrorKind::TlsError => Some("Upstream TLS connection failed"),
        ExecutionErrorKind::ProxyError => Some("Upstream proxy request failed"),
        ExecutionErrorKind::ProtocolError => Some(UPSTREAM_STREAM_FAILURE_MESSAGE),
        ExecutionErrorKind::Cancelled => Some("Request was cancelled"),
        ExecutionErrorKind::Internal => Some(EXECUTION_STREAM_FAILURE_MESSAGE),
        ExecutionErrorKind::Upstream4xx | ExecutionErrorKind::Upstream5xx => None,
    }
}

/// Failures the execution runtime reports by name rather than as an
/// `ExecutionErrorKind` value.
fn execution_runtime_failure_message(error_type: &str) -> Option<&'static str> {
    match error_type {
        "execution_runtime_stream_read_error" => Some(UPSTREAM_STREAM_FAILURE_MESSAGE),
        "execution_runtime_stream_frame_decode_error"
        | "execution_runtime_stream_chunk_decode_error" => {
            Some(EXECUTION_STREAM_PROTOCOL_FAILURE_MESSAGE)
        }
        "execution_runtime_sync_json_stream_bridge_error"
        | "execution_runtime_stream_rewrite_error"
        | "execution_runtime_stream_rewrite_flush_error" => {
            Some(EXECUTION_STREAM_PROCESSING_FAILURE_MESSAGE)
        }
        _ => None,
    }
}

/// Shared lookup by the `error_type` string that reaches the client. Falls back
/// to `None` for unknown names so each caller keeps its own generic text.
pub(crate) fn client_message_for_error_type(error_type: &str) -> Option<&'static str> {
    serde_json::from_value::<ExecutionErrorKind>(Value::String(error_type.to_string()))
        .ok()
        .as_ref()
        .and_then(client_message_for_execution_error_kind)
        .or_else(|| execution_runtime_failure_message(error_type))
}

/// Client-facing projection of a transport failure.
///
/// Timeout-class failures are gateway timeouts (the gateway gave up waiting);
/// everything else means the upstream response could not be used at all. The
/// message stays fine-grained per failure class so a client can tell a TLS
/// failure from a stream read timeout.
struct TransportFailureProjection {
    status_code: u16,
    message: &'static str,
}

fn project_transport_failure(error_type: &str) -> TransportFailureProjection {
    let lowered = error_type.trim().to_ascii_lowercase();
    let is_timeout = lowered.contains("timeout") || lowered.contains("timed_out");
    let status_code = if is_timeout {
        StatusCode::GATEWAY_TIMEOUT.as_u16()
    } else {
        StatusCode::BAD_GATEWAY.as_u16()
    };
    let message = client_message_for_error_type(&lowered).unwrap_or(if is_timeout {
        "Upstream response timed out"
    } else {
        TRANSPORT_ERROR_CLIENT_MESSAGE
    });
    TransportFailureProjection {
        status_code,
        message,
    }
}

#[derive(Debug, Default)]
pub(crate) struct StreamCandidateWatchdogProgress {
    terminal_started: AtomicBool,
    abandoned: AtomicBool,
}

tokio::task_local! {
    static STREAM_CANDIDATE_WATCHDOG_PROGRESS: Arc<StreamCandidateWatchdogProgress>;
}

impl StreamCandidateWatchdogProgress {
    pub(crate) fn shared() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub(crate) fn terminal_started(&self) -> bool {
        self.terminal_started.load(Ordering::Acquire)
    }

    /// The watchdog gave up waiting and settles this attempt itself.
    ///
    /// The attempt future is dropped once the watchdog returns, so its own
    /// cancellation guard must stay out of the way instead of racing the
    /// watchdog's terminal rows with a cancellation.
    pub(crate) fn mark_abandoned(&self) {
        self.abandoned.store(true, Ordering::Release);
    }

    pub(crate) fn abandoned(&self) -> bool {
        self.abandoned.load(Ordering::Acquire)
    }

    /// The watchdog watching the attempt on this task, if it runs under one.
    pub(crate) fn current() -> Option<Arc<Self>> {
        STREAM_CANDIDATE_WATCHDOG_PROGRESS.try_with(Arc::clone).ok()
    }

    pub(crate) async fn scope<F>(self: Arc<Self>, future: F) -> F::Output
    where
        F: Future,
    {
        STREAM_CANDIDATE_WATCHDOG_PROGRESS.scope(self, future).await
    }
}

pub(crate) fn mark_stream_candidate_watchdog_terminal_started() {
    let _ = STREAM_CANDIDATE_WATCHDOG_PROGRESS.try_with(|progress| {
        progress.terminal_started.store(true, Ordering::Release);
    });
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn build_transport_error_stop_response(
    state: &AppState,
    plan: &aether_contracts::ExecutionPlan,
    report_context: Option<&Value>,
    trace_id: &str,
    decision: &GatewayControlDecision,
    error_type: &str,
    error_message: &str,
    elapsed_ms: u64,
) -> Result<Response<Body>, GatewayError> {
    mark_stream_candidate_watchdog_terminal_started();
    let projection = project_transport_failure(error_type);
    let client_status_code = projection.status_code;
    let mut client_body = build_core_error_body_for_client_format(
        &plan.client_api_format,
        projection.message,
        Some("upstream_transport_error"),
        LocalCoreSyncErrorKind::ServerError,
    )
    .unwrap_or_else(|| {
        json!({
            "error": {
                "type": "server_error",
                "message": projection.message,
                "code": "upstream_transport_error",
            }
        })
    });
    // The Gemini error shape carries a numeric `code`; keep it aligned with the
    // status the client actually receives.
    align_gemini_error_code_with_status(
        &plan.client_api_format,
        &mut client_body,
        client_status_code,
    );
    let body_bytes =
        serde_json::to_vec(&client_body).map_err(|err| GatewayError::Internal(err.to_string()))?;
    let headers = BTreeMap::from([
        ("content-type".to_string(), "application/json".to_string()),
        ("content-length".to_string(), body_bytes.len().to_string()),
    ]);

    if state.usage_runtime.is_enabled() {
        let report_context_with_diagnostics =
            attach_current_request_diagnostics_and_candidate_timing_to_report_context(
                report_context,
                Some(elapsed_ms),
                None,
            );
        let mut usage_data = build_usage_event_data_seed(
            plan,
            report_context_with_diagnostics.as_ref().or(report_context),
        );
        usage_data.status_code = Some(client_status_code);
        usage_data.error_message = Some(error_message.to_string());
        usage_data.error_category = Some("server_error".to_string());
        usage_data.response_time_ms = Some(elapsed_ms);
        usage_data.response_headers = None;
        usage_data.response_body = None;
        usage_data.client_response_headers = Some(json!({"content-type": "application/json"}));
        usage_data.client_response_body = Some(client_body);
        let mut request_metadata = match usage_data.request_metadata.take() {
            Some(Value::Object(object)) => object,
            Some(other) => serde_json::Map::from_iter([("seed".to_string(), other)]),
            None => serde_json::Map::new(),
        };
        request_metadata.insert("transport_error".to_string(), Value::Bool(true));
        request_metadata.insert("analytics_failure".into(), json!({
            "origin": "transport", "stage": "connect", "reason": "upstream_transport_error", "schema_version": 1,
        }));
        request_metadata.insert(
            "transport_error_type".to_string(),
            Value::String(error_type.to_string()),
        );
        usage_data.request_metadata = Some(Value::Object(request_metadata));
        state
            .usage_runtime
            .record_terminal_event_direct(
                state.usage_lifecycle_data_state().as_ref(),
                UsageEvent::new(UsageEventType::Failed, plan.request_id.clone(), usage_data),
            )
            .await;
    }

    attach_control_metadata_headers(
        build_client_response_from_parts(
            client_status_code,
            &headers,
            Body::from(body_bytes),
            trace_id,
            Some(decision),
        )?,
        Some(plan.request_id.as_str()),
        plan.candidate_id.as_deref(),
    )
}

#[cfg(test)]
mod tests {
    use aether_contracts::ExecutionErrorKind;

    use super::{
        client_message_for_error_type, client_message_for_execution_error_kind,
        project_transport_failure,
    };

    #[test]
    fn timeout_class_failures_are_gateway_timeouts() {
        for error_type in [
            "first_byte_timeout",
            "read_timeout",
            "connect_timeout",
            "local_stream_candidate_watchdog_timeout",
            "direct_passthrough_first_byte_timed_out",
        ] {
            let projection = project_transport_failure(error_type);
            assert_eq!(projection.status_code, 504, "error_type: {error_type}");
        }
    }

    #[test]
    fn non_timeout_transport_failures_are_bad_gateways() {
        for error_type in [
            "execution_runtime_stream_read_error",
            "tls_error",
            "proxy_error",
            "execution_runtime_unavailable",
        ] {
            let projection = project_transport_failure(error_type);
            assert_eq!(projection.status_code, 502, "error_type: {error_type}");
        }
    }

    #[test]
    fn messages_stay_fine_grained_per_failure_class() {
        assert_eq!(
            project_transport_failure("tls_error").message,
            "Upstream TLS connection failed"
        );
        assert_eq!(
            project_transport_failure("read_timeout").message,
            "Upstream response stream timed out"
        );
        assert_eq!(
            project_transport_failure("first_byte_timeout").message,
            "Upstream response timed out before the first byte"
        );
        // Known-bad but unclassified failures fall back to the generic text
        // rather than leaking the internal transport message.
        assert_eq!(
            project_transport_failure("unclassified_transport_failure").message,
            "Upstream transport failed before an HTTP response was received"
        );
    }

    #[test]
    fn every_error_kind_resolves_to_the_same_text_from_a_string() {
        // The stop path resolves by `error_type`, the stream report path by the
        // kind value. They share one table, so the two must agree.
        for kind in [
            ExecutionErrorKind::ConnectTimeout,
            ExecutionErrorKind::FirstByteTimeout,
            ExecutionErrorKind::ReadTimeout,
            ExecutionErrorKind::TlsError,
            ExecutionErrorKind::ProxyError,
            ExecutionErrorKind::ProtocolError,
            ExecutionErrorKind::Cancelled,
            ExecutionErrorKind::Internal,
        ] {
            let error_type = serde_json::to_value(&kind)
                .expect("kind should serialize")
                .as_str()
                .expect("kind should serialize to a string")
                .to_string();
            assert_eq!(
                client_message_for_error_type(&error_type),
                client_message_for_execution_error_kind(&kind),
                "error_type: {error_type}"
            );
            assert!(
                client_message_for_error_type(&error_type).is_some(),
                "{error_type} must resolve"
            );
        }
    }

    #[test]
    fn upstream_status_kinds_leave_the_text_to_the_caller() {
        for kind in [
            ExecutionErrorKind::Upstream4xx,
            ExecutionErrorKind::Upstream5xx,
        ] {
            assert!(client_message_for_execution_error_kind(&kind).is_none());
        }
    }
}
