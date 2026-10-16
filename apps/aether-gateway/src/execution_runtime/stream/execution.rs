use std::collections::{BTreeMap, VecDeque};
use std::future::Future;
use std::io::Error as IoError;
use std::pin::Pin;
use std::sync::{
    atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    Arc,
};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use aether_ai_serving::{AiAttemptExecutionOutcome, AiAttemptRetryScope};
use aether_contracts::{
    ExecutionPlan, ExecutionResponseObservation, ExecutionStreamTerminalSummary,
    ExecutionTelemetry, StandardizedUsage, StreamFrame, StreamFramePayload, UsageTokenSource,
};
use aether_data_contracts::repository::candidates::{
    RequestCandidateStatus, UpsertRequestCandidateRecord,
};
use aether_data_contracts::repository::usage::UsageBodyCaptureState;
use aether_scheduler_core::{
    parse_request_candidate_report_context, SchedulerRequestCandidateStatusUpdate,
};
#[cfg(test)]
use aether_usage_runtime::DEFAULT_USAGE_RESPONSE_BODY_CAPTURE_LIMIT_BYTES;
use aether_usage_runtime::{
    build_lifecycle_usage_seed, build_stream_terminal_usage_payload_seed,
    build_sync_terminal_usage_payload_seed, build_terminal_usage_context_seed, LifecycleUsageSeed,
    SyncTerminalUsagePayloadSeed, TerminalUsageContextSeed, UsageRequestRecordLevel,
};
use async_stream::stream;
use axum::body::{Body, Bytes};
use axum::http::Response;
use base64::Engine as _;
use futures_util::stream::{self as futures_stream, BoxStream};
#[cfg(test)]
use futures_util::TryStreamExt;
use futures_util::{Stream, StreamExt};
use http_body_util::BodyExt;
use serde_json::{json, Value};
use tokio::io::{AsyncRead, ReadBuf};
use tokio::sync::mpsc;
use tokio::time::MissedTickBehavior;
use tokio_util::codec::{FramedRead, LinesCodec};
use tracing::{debug, info, warn};

use super::capture_budget::StreamBodyCapture;
use super::commit_policy::{
    anthropic_error_status_code, find_sse_record_boundary, StreamCommitGate, StreamCommitPolicy,
    StreamPrecommitObservation,
};
use super::error::{
    build_synthetic_non_success_stream_error_body, collect_error_body, decode_stream_error_body,
    inspect_prefetched_stream_body, read_next_frame,
    should_synthesize_non_success_stream_error_body,
    stream_client_error_status_code_for_upstream_status, synthetic_error_response_headers,
    StreamPrefetchInspection,
};
use super::usage_fallback::StreamUsageFallback;
#[path = "execution_failures.rs"]
mod execution_failures;
use self::execution_failures::{
    build_stream_failure_from_execution_error, build_stream_failure_from_provider_error_body,
    build_stream_failure_report, build_stream_transport_failure_report,
    handle_prefetch_provider_private_stream_error, handle_prefetch_stream_failure,
    submit_midstream_stream_failure, StreamFailureReport,
};
use crate::ai_serving::api::{
    extract_provider_private_stream_error_body, maybe_bridge_standard_sync_json_to_stream,
    maybe_build_provider_private_stream_normalizer, maybe_build_stream_response_rewriter,
    normalize_provider_private_report_context, StreamingStandardTerminalObserver,
    CLAUDE_CHAT_STREAM_PLAN_KIND, CLAUDE_CLI_STREAM_PLAN_KIND, GEMINI_CHAT_STREAM_PLAN_KIND,
    GEMINI_CLI_STREAM_PLAN_KIND, GEMINI_INTERACTIONS_STREAM_PLAN_KIND,
    OPENAI_CHAT_STREAM_PLAN_KIND, OPENAI_IMAGE_STREAM_PLAN_KIND,
    OPENAI_RESPONSES_COMPACT_STREAM_PLAN_KIND, OPENAI_RESPONSES_STREAM_PLAN_KIND,
    UPSTREAM_IS_STREAM_KEY,
};
use crate::ai_serving::is_openai_responses_family_format;
use crate::ai_serving::record_local_runtime_candidate_skip_reason;
#[cfg(test)]
pub(crate) use crate::api::response::build_client_response;
use crate::api::response::{
    attach_control_metadata_headers, build_client_response_from_parts,
    build_client_response_from_parts_with_mutator,
};
use crate::clock::current_unix_ms as current_request_candidate_unix_ms;
use crate::constants::{CONTROL_CANDIDATE_ID_HEADER, CONTROL_REQUEST_ID_HEADER};
use crate::control::GatewayControlDecision;
use crate::execution_runtime::attempt_cancellation::AttemptCancellationGuard;
use crate::execution_runtime::build_direct_execution_frame_stream;
use crate::execution_runtime::chatgpt_web_image::maybe_execute_chatgpt_web_image_stream;
use crate::execution_runtime::grok::maybe_execute_grok_stream;
use crate::execution_runtime::kiro_cache::{
    billed_input_tokens as kiro_billed_input_tokens, build_kiro_prompt_cache_profile,
    compute_kiro_prompt_cache_usage, estimate_kiro_prompt_input_tokens,
    kiro_simulated_cache_enabled_from_provider_config,
    kiro_simulated_cache_enabled_from_report_context, KiroPromptCacheUsage,
    KIRO_SIMULATED_CACHE_ENABLED_CONTEXT_FIELD,
};
use crate::execution_runtime::kiro_web_search::maybe_execute_kiro_web_search_stream;
use crate::execution_runtime::oauth_retry::refresh_oauth_plan_auth_for_retry;
#[cfg(test)]
use crate::execution_runtime::remote_compat::post_stream_plan_to_remote_execution_runtime;
use crate::execution_runtime::stream_read_timeout::{
    await_stream_idle_read, resolve_stream_idle_timeout, stream_idle_timeout_message,
};
use crate::execution_runtime::submission::{
    resolve_local_sync_error_status_code, strip_utf8_bom_and_ws,
    submit_local_core_error_or_sync_finalize,
};
use crate::execution_runtime::transport::{
    decode_base64_body_with_limit, direct_upstream_response_byte_stream, format_hyper_error_chain,
    format_upstream_request_error, format_wreq_upstream_request_error,
    record_manual_proxy_request_failure, record_manual_proxy_request_success,
    record_manual_proxy_stream_error, stream_first_byte_timeout_message,
    DirectSyncExecutionRuntime, DirectUpstreamResponse, DirectUpstreamStreamExecution,
    ExecutionRuntimeTransportError,
};
use crate::execution_runtime::windsurf::maybe_execute_windsurf_stream;
use crate::execution_runtime::{
    ai_attempt_retry_scope_from_failure_disposition, apply_endpoint_response_header_rules,
    attach_provider_response_headers_to_report_context, build_exhaustion_fallback,
    local_failover_response_text, resolve_core_stream_direct_finalize_report_kind,
    resolve_core_stream_error_finalize_report_kind,
    resolve_local_candidate_failover_analysis_stream, should_fallback_to_control_stream,
    should_retry_next_local_candidate_stream, ExhaustionFallbackInput, LocalFailoverDecision,
};
use crate::execution_runtime::{
    MAX_ERROR_BODY_BYTES, MAX_STREAM_PREFETCH_BYTES, MAX_STREAM_PREFETCH_FRAMES,
};
use crate::log_ids::short_request_id;
use crate::orchestration::{
    apply_local_execution_effect, build_local_error_flow_metadata, classify_failure_disposition,
    spawn_local_oauth_success_effect, trace_upstream_response_body, with_error_flow_report_context,
    with_upstream_response_report_context, FailureDisposition, FailureTokenAction,
    LocalAdaptiveRateLimitEffect, LocalAdaptiveSuccessEffect, LocalAttemptFailureEffect,
    LocalExecutionEffect, LocalExecutionEffectContext, LocalFailoverAnalysis,
    LocalHealthFailureEffect, LocalHealthSuccessEffect, LocalOAuthInvalidationEffect,
    LocalOAuthSuccessEffect, LocalPoolErrorEffect,
};
use crate::provider_pool_demand::{
    acquire_provider_pool_execution_guard, ProviderPoolInFlightAdmission, ProviderPoolInFlightGuard,
};
use crate::request_candidate_runtime::{
    ensure_execution_request_candidate_slot, persist_local_request_candidate_status_record,
    record_local_request_candidate_status, record_local_request_candidate_status_snapshot,
    snapshot_local_request_candidate_status, try_enqueue_local_request_candidate_status_snapshot,
    LocalRequestCandidateStatusSnapshot,
};
use crate::request_diagnostics::{
    attach_current_request_diagnostics_to_report_context,
    attach_request_diagnostics_and_candidate_start_timing_to_report_context,
    current_request_diagnostics, RequestDiagnostics,
};
use crate::stage_metrics::{
    attach_stage_trace_to_report_context, observe_gateway_stage_ms, observe_gateway_stage_trace_ms,
    record_stream_pre_first_byte_spawn, RequestStageTrace,
};
use crate::usage::submit_stream_report;
use crate::usage::{GatewayStreamReportRequest, GatewaySyncReportRequest};
use crate::{
    AppState, GatewayError, GEMINI_FILES_DOWNLOAD_PLAN_KIND, OPENAI_VIDEO_CONTENT_PLAN_KIND,
};

/// Settlement labels for a stream attempt whose future is dropped before the
/// transport reaches a terminal state.
const STREAM_ATTEMPT_CANCELLED_ERROR_TYPE: &str = "local_stream_attempt_cancelled";
const STREAM_ATTEMPT_CANCELLED_ERROR_MESSAGE: &str = "Local stream attempt was dropped before terminal finalization, usually because the client disconnected or the request task was cancelled.";

const SSE_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(15);
const SSE_KEEPALIVE_BYTES: &[u8] = b": aether-keepalive\n\n";
const SSE_CONTROL_FILTER_MAX_BUFFER_BYTES: usize = 1024 * 1024;
const SSE_TERMINAL_DETECTOR_MAX_LINE_BYTES: usize = 1024 * 1024;
const SSE_TERMINAL_DETECTOR_MAX_RECORD_BYTES: usize = SSE_TERMINAL_DETECTOR_MAX_LINE_BYTES;
const PROVIDER_STREAM_ERROR_INSPECTION_MAX_BYTES: usize = SSE_TERMINAL_DETECTOR_MAX_LINE_BYTES;
const BASIC_STREAM_BODY_ANALYSIS_LIMIT_BYTES: usize = 5 * 1024 * 1024;
const MAX_EXECUTION_STREAM_DATA_CHUNK_BYTES: usize = 64 * 1024 * 1024;
const STREAM_IDLE_LOG_INTERVAL: Duration = Duration::from_secs(60);
const STREAM_IDLE_LOG_INTERVAL_MS: u64 = 60_000;
const REWRITTEN_STREAM_PREFETCH_TIMEOUT: Duration = Duration::from_millis(750);
const OAUTH_ERROR_PREFETCH_MAX_WAIT: Duration = Duration::from_millis(750);
const ANTHROPIC_POST_STOP_DRAIN_MAX_WAIT: Duration = Duration::from_millis(250);
const ANTHROPIC_POST_STOP_DRAIN_MAX_FRAMES: usize = 8;
const ANTHROPIC_POST_STOP_DRAIN_MAX_BYTES: usize = 64 * 1024;
const POST_STOP_FRAME_READ_BUDGET_INACTIVE: usize = usize::MAX;
const POST_STOP_MAX_EMPTY_CHUNKS_PER_POLL: usize = 32;
const DEFAULT_DIRECT_PASSTHROUGH_CHANNEL_CAPACITY: usize = 16;
const MAX_DIRECT_PASSTHROUGH_CHANNEL_CAPACITY: usize = 1024;
const DIRECT_PASSTHROUGH_CHANNEL_CAPACITY_ENV: &str =
    "AETHER_GATEWAY_DIRECT_PASSTHROUGH_CHANNEL_CAPACITY";
const DIRECT_PASSTHROUGH_MODE_ENV: &str = "AETHER_GATEWAY_DIRECT_PASSTHROUGH_MODE";

/// Retains the incomplete tail needed to recognize provider error events split across transport
/// chunks without retaining an unbounded copy of the stream.
#[derive(Default)]
struct ProviderStreamErrorInspection {
    buffered: Vec<u8>,
}

impl ProviderStreamErrorInspection {
    fn observe(&mut self, report_context: Option<&Value>, chunk: &[u8]) -> Option<Value> {
        if chunk.is_empty() {
            return None;
        }

        // A transport implementation may deliver a very large chunk. Keep
        // every parser invocation bounded: inspect the prefix (including the
        // previous rolling tail for events split across chunks) and suffix,
        // while retaining only the bounded suffix for the next observation.
        // The middle of an oversized chunk is deliberately skipped because
        // this observer is best-effort and must never duplicate the client
        // stream or turn a single upstream read into an unbounded parse.
        if chunk.len() > PROVIDER_STREAM_ERROR_INSPECTION_MAX_BYTES {
            let prefix_len = PROVIDER_STREAM_ERROR_INSPECTION_MAX_BYTES
                .saturating_sub(self.buffered.len())
                .min(chunk.len());
            let mut boundary = Vec::with_capacity(
                self.buffered
                    .len()
                    .saturating_add(prefix_len)
                    .min(PROVIDER_STREAM_ERROR_INSPECTION_MAX_BYTES),
            );
            boundary.extend_from_slice(&self.buffered);
            boundary.extend_from_slice(&chunk[..prefix_len]);
            if let Some(error_body) =
                extract_provider_private_stream_error_body(report_context, &boundary)
            {
                return Some(error_body);
            }

            let prefix = &chunk[..PROVIDER_STREAM_ERROR_INSPECTION_MAX_BYTES];
            if let Some(error_body) =
                extract_provider_private_stream_error_body(report_context, prefix)
            {
                return Some(error_body);
            }

            let suffix_start = chunk.len() - PROVIDER_STREAM_ERROR_INSPECTION_MAX_BYTES;
            if let Some(error_body) =
                extract_provider_private_stream_error_body(report_context, &chunk[suffix_start..])
            {
                return Some(error_body);
            }
        } else if let Some(error_body) =
            extract_provider_private_stream_error_body(report_context, chunk)
        {
            return Some(error_body);
        }

        self.append_rolling(chunk);
        let error_body = extract_provider_private_stream_error_body(report_context, &self.buffered);
        if error_body.is_none() {
            self.trim_completed_sse_events();
        }
        error_body
    }

    fn append_rolling(&mut self, chunk: &[u8]) {
        if chunk.len() >= PROVIDER_STREAM_ERROR_INSPECTION_MAX_BYTES {
            self.buffered.clear();
            self.buffered.extend_from_slice(
                &chunk[chunk.len() - PROVIDER_STREAM_ERROR_INSPECTION_MAX_BYTES..],
            );
            return;
        }

        let overflow = self
            .buffered
            .len()
            .saturating_add(chunk.len())
            .saturating_sub(PROVIDER_STREAM_ERROR_INSPECTION_MAX_BYTES);
        if overflow > 0 {
            self.buffered.drain(..overflow);
        }
        self.buffered.extend_from_slice(chunk);
    }

    fn trim_completed_sse_events(&mut self) {
        let Ok(text) = std::str::from_utf8(&self.buffered) else {
            return;
        };
        if !text.lines().any(|line| {
            let line = line.trim_start();
            line.starts_with("event:") || line.starts_with("data:") || line.starts_with(':')
        }) {
            return;
        }

        let lf_end = self
            .buffered
            .windows(2)
            .rposition(|window| window == b"\n\n")
            .map(|index| index + 2);
        let crlf_end = self
            .buffered
            .windows(4)
            .rposition(|window| window == b"\r\n\r\n")
            .map(|index| index + 4);
        if let Some(event_end) = lf_end.into_iter().chain(crlf_end).max() {
            self.buffered.drain(..event_end);
        }
    }
}

struct StageElapsedGuard {
    stage: &'static str,
    started_at: Instant,
}

#[derive(Debug)]
enum InProcessStreamExecutionError {
    Transport(ExecutionRuntimeTransportError),
    Gateway(GatewayError),
}

impl From<ExecutionRuntimeTransportError> for InProcessStreamExecutionError {
    fn from(error: ExecutionRuntimeTransportError) -> Self {
        Self::Transport(error)
    }
}

impl From<GatewayError> for InProcessStreamExecutionError {
    fn from(error: GatewayError) -> Self {
        Self::Gateway(error)
    }
}

impl StageElapsedGuard {
    fn from_started_at(stage: &'static str, started_at: Instant) -> Self {
        Self { stage, started_at }
    }
}

fn report_context_with_stage_trace(
    report_context: Option<Value>,
    mut stage_trace: RequestStageTrace,
    stream_started_at: Instant,
    terminal_telemetry: Option<&ExecutionTelemetry>,
) -> Option<Value> {
    stage_trace.observe("stream_total", stream_elapsed_ms_since(stream_started_at));
    let fallback_elapsed_ms = terminal_telemetry.and_then(|telemetry| telemetry.ttfb_ms);
    attach_stage_trace_to_report_context(
        report_context,
        stage_trace.into_metadata_value(fallback_elapsed_ms),
    )
}

fn report_context_with_request_diagnostics(
    report_context: Option<Value>,
    diagnostics: Option<&Arc<RequestDiagnostics>>,
    candidate_started_at: Instant,
    terminal_telemetry: Option<&ExecutionTelemetry>,
) -> Option<Value> {
    attach_request_diagnostics_and_candidate_start_timing_to_report_context(
        report_context,
        diagnostics,
        Some(candidate_started_at),
        terminal_telemetry.and_then(|telemetry| telemetry.ttfb_ms),
    )
}

fn request_accepted_elapsed_ms(diagnostics: Option<&Arc<RequestDiagnostics>>) -> Option<u64> {
    diagnostics.and_then(|diagnostics| {
        diagnostics
            .request_accepted_at()
            .map(|accepted_at| accepted_at.elapsed().as_millis().min(u128::from(u64::MAX)) as u64)
    })
}

fn observe_request_accepted_stage_trace_ms(
    trace: &mut RequestStageTrace,
    diagnostics: Option<&Arc<RequestDiagnostics>>,
    stage: &'static str,
) {
    if let Some(elapsed_ms) = request_accepted_elapsed_ms(diagnostics) {
        observe_gateway_stage_trace_ms(trace, stage, elapsed_ms);
    }
}

impl Drop for StageElapsedGuard {
    fn drop(&mut self) {
        observe_gateway_stage_ms(self.stage, self.started_at.elapsed().as_millis() as u64);
    }
}

fn direct_passthrough_channel_capacity() -> usize {
    std::env::var(DIRECT_PASSTHROUGH_CHANNEL_CAPACITY_ENV)
        .ok()
        .and_then(|value| value.trim().parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_DIRECT_PASSTHROUGH_CHANNEL_CAPACITY)
        .clamp(1, MAX_DIRECT_PASSTHROUGH_CHANNEL_CAPACITY)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DirectPassthroughMode {
    Inline,
    Legacy,
}

fn direct_passthrough_mode() -> DirectPassthroughMode {
    std::env::var(DIRECT_PASSTHROUGH_MODE_ENV)
        .ok()
        .as_deref()
        .map(parse_direct_passthrough_mode)
        .unwrap_or(DirectPassthroughMode::Inline)
}

fn stream_body_buffer_limit_for_record_level(record_level: UsageRequestRecordLevel) -> usize {
    match record_level {
        UsageRequestRecordLevel::Basic => BASIC_STREAM_BODY_ANALYSIS_LIMIT_BYTES,
        UsageRequestRecordLevel::Full => crate::execution_runtime::MAX_STREAM_BODY_CAPTURE_BYTES,
    }
}

async fn resolve_stream_body_buffer_limit(state: &AppState) -> usize {
    if !state.usage_runtime.is_enabled() {
        return BASIC_STREAM_BODY_ANALYSIS_LIMIT_BYTES;
    }

    match state
        .usage_runtime
        .body_capture_policy_for(state.usage_lifecycle_data_state().as_ref())
        .await
    {
        Ok(policy) => stream_body_buffer_limit_for_record_level(policy.record_level),
        Err(_error) => {
            warn!(
                event_name = "stream_body_capture_policy_read_failed",
                log_type = "ops",
                error_category = "capture_policy_read_failed",
                fallback = "basic",
                "gateway could not resolve stream body capture policy"
            );
            BASIC_STREAM_BODY_ANALYSIS_LIMIT_BYTES
        }
    }
}

fn parse_direct_passthrough_mode(value: &str) -> DirectPassthroughMode {
    match value.trim().to_ascii_lowercase().as_str() {
        "legacy" | "pump" | "mpsc" => DirectPassthroughMode::Legacy,
        _ => DirectPassthroughMode::Inline,
    }
}

fn build_sync_terminal_usage_seeds(
    plan: &ExecutionPlan,
    report_context: Option<&serde_json::Value>,
    payload: &GatewaySyncReportRequest,
) -> (TerminalUsageContextSeed, SyncTerminalUsagePayloadSeed) {
    let analytics_context =
        crate::usage::reporting::failure::sync_analytics_context(report_context, payload);
    let report_context_with_diagnostics =
        attach_current_request_diagnostics_to_report_context(analytics_context.as_ref());
    let context_seed = build_terminal_usage_context_seed(
        plan,
        report_context_with_diagnostics
            .as_ref()
            .or(analytics_context.as_ref()),
    );
    let payload_seed = build_sync_terminal_usage_payload_seed(payload);
    (context_seed, payload_seed)
}

async fn record_sync_terminal_usage_with_handoff(
    state: &AppState,
    plan: &ExecutionPlan,
    report_context: Option<&serde_json::Value>,
    payload: &GatewaySyncReportRequest,
) {
    record_sync_terminal_usage_with_handoff_after_spawn(
        state,
        plan,
        report_context,
        payload,
        std::future::ready(()),
    )
    .await;
}

async fn record_sync_terminal_usage_with_handoff_after_spawn<F>(
    state: &AppState,
    plan: &ExecutionPlan,
    report_context: Option<&serde_json::Value>,
    payload: &GatewaySyncReportRequest,
    before_dispatch: F,
) where
    F: Future<Output = ()> + Send + 'static,
{
    crate::execution_runtime::mark_stream_candidate_watchdog_terminal_started();
    // Capture request task-local diagnostics before handing the work to a spawned task. Tokio
    // task-local values do not propagate across spawn boundaries.
    let (context_seed, payload_seed) =
        build_sync_terminal_usage_seeds(plan, report_context, payload);
    let state = state.clone();
    let usage_producer = state.usage_runtime.track_producer();
    let task = tokio::spawn(async move {
        let _usage_producer = usage_producer;
        before_dispatch.await;
        state
            .usage_runtime
            .record_sync_terminal(
                state.usage_lifecycle_data_state().as_ref(),
                context_seed,
                payload_seed,
            )
            .await;
    });
    if let Err(_err) = task.await {
        warn!(
            event_name = "sync_terminal_usage_handoff_failed",
            log_type = "ops",
            error_category = "terminal_usage_handoff_failed",
            "gateway sync terminal usage handoff task failed"
        );
    }
}

#[cfg(test)]
fn build_stream_sync_payload(
    trace_id: &str,
    report_kind: String,
    report_context: Option<Value>,
    status_code: u16,
    headers: BTreeMap<String, String>,
    body_json: Option<Value>,
    body_base64: Option<String>,
    telemetry: Option<ExecutionTelemetry>,
) -> GatewaySyncReportRequest {
    GatewaySyncReportRequest {
        trace_id: trace_id.to_string(),
        report_kind,
        report_context,
        status_code,
        headers,
        body_json,
        client_body_json: None,
        body_base64,
        telemetry,
    }
}

#[allow(clippy::too_many_arguments)]
fn build_stream_error_sync_payload(
    trace_id: &str,
    report_kind: String,
    report_context: Option<Value>,
    upstream_status_code: u16,
    provider_headers: BTreeMap<String, String>,
    provider_body_json: Option<Value>,
    provider_body_base64: Option<String>,
    client_headers: BTreeMap<String, String>,
    client_body_json: Option<Value>,
    telemetry: Option<ExecutionTelemetry>,
) -> GatewaySyncReportRequest {
    let client_status_code =
        stream_client_error_status_code_for_upstream_status(upstream_status_code);
    let mut report_context = report_context;
    if client_status_code != upstream_status_code || client_headers != provider_headers {
        let mut object = match report_context {
            Some(Value::Object(object)) => object,
            Some(other) => serde_json::Map::from_iter([("seed".to_string(), other)]),
            None => serde_json::Map::new(),
        };
        object.insert(
            "client_response_status_code".to_string(),
            Value::from(client_status_code),
        );
        object.insert(
            "client_response_headers".to_string(),
            serde_json::to_value(client_headers).unwrap_or(Value::Null),
        );
        report_context = Some(Value::Object(object));
    }

    GatewaySyncReportRequest {
        trace_id: trace_id.to_string(),
        report_kind,
        report_context,
        status_code: upstream_status_code,
        headers: provider_headers,
        body_json: provider_body_json,
        client_body_json,
        body_base64: provider_body_base64,
        telemetry,
    }
}

async fn record_stream_terminal_usage(
    state: &AppState,
    plan: &ExecutionPlan,
    report_context: Option<&serde_json::Value>,
    payload: &GatewayStreamReportRequest,
    cancelled: bool,
) {
    crate::execution_runtime::mark_stream_candidate_watchdog_terminal_started();
    let analytics_context = crate::usage::reporting::failure::stream_analytics_context(
        report_context,
        payload,
        cancelled,
    );
    let context_seed = build_terminal_usage_context_seed(plan, analytics_context.as_ref());
    let payload_seed = build_stream_terminal_usage_payload_seed(payload);
    state
        .usage_runtime
        .record_stream_terminal(
            state.usage_lifecycle_data_state().as_ref(),
            context_seed,
            payload_seed,
            cancelled,
        )
        .await;
}

async fn record_stream_admission_timeout_candidate_failure(
    state: &AppState,
    plan: &ExecutionPlan,
    report_context: Option<&Value>,
    candidate_started_unix_ms: u64,
    error: &GatewayError,
) {
    let status_code = 429;
    let error_type = "gateway_admission_timeout";
    let error_message = match error {
        GatewayError::AdmissionTimeout {
            gate,
            queue_budget_ms,
            ..
        } => format!("gateway admission gate {gate} timed out after {queue_budget_ms}ms"),
        other => format!("{other:?}"),
    };
    let terminal_unix_ms = current_request_candidate_unix_ms();
    let latency_ms = terminal_unix_ms.saturating_sub(candidate_started_unix_ms);
    record_local_request_candidate_status(
        state,
        plan,
        report_context,
        SchedulerRequestCandidateStatusUpdate {
            status: RequestCandidateStatus::Failed,
            status_code: Some(status_code),
            error_type: Some(error_type.to_string()),
            error_message: Some(error_message.clone()),
            latency_ms: Some(latency_ms),
            started_at_unix_ms: Some(candidate_started_unix_ms),
            finished_at_unix_ms: Some(terminal_unix_ms),
        },
    )
    .await;
}

fn build_stream_body_capture(
    body: &[u8],
    truncated: bool,
) -> (Option<String>, Option<UsageBodyCaptureState>) {
    build_stream_body_capture_with_limit(
        body,
        truncated,
        crate::execution_runtime::MAX_STREAM_BODY_CAPTURE_BYTES,
    )
}

fn build_stream_body_capture_with_limit(
    body: &[u8],
    truncated: bool,
    max_bytes: usize,
) -> (Option<String>, Option<UsageBodyCaptureState>) {
    let captured = &body[..body.len().min(max_bytes)];
    let body_base64 =
        (!captured.is_empty()).then(|| base64::engine::general_purpose::STANDARD.encode(captured));
    let body_state = Some(if truncated || captured.len() < body.len() {
        UsageBodyCaptureState::Truncated
    } else if captured.is_empty() {
        UsageBodyCaptureState::None
    } else {
        UsageBodyCaptureState::Inline
    });
    (body_base64, body_state)
}

fn wrap_non_json_binary_stream_error_for_client(
    plan_kind: &str,
    headers: &BTreeMap<String, String>,
    _error_body: &[u8],
) -> Result<Option<Value>, GatewayError> {
    let content_type = headers
        .get("content-type")
        .map(|value| value.to_ascii_lowercase())
        .unwrap_or_default();
    if content_type.starts_with("application/json") {
        return Ok(None);
    }

    let body = match plan_kind {
        GEMINI_FILES_DOWNLOAD_PLAN_KIND => json!({
            "error": "File download failed",
        }),
        OPENAI_VIDEO_CONTENT_PLAN_KIND => json!({
            "error": {
                "type": "upstream_error",
                "message": "Video not available",
            }
        }),
        _ => json!({
            "error": {
                "type": "upstream_error",
                "message": "Upstream request failed",
            }
        }),
    };
    Ok(Some(body))
}

fn with_stream_error_trace_context(
    report_context: Option<&Value>,
    status_code: u16,
    headers: &BTreeMap<String, String>,
    body_json: Option<&Value>,
    body_bytes: &[u8],
    response_text: Option<&str>,
    local_failover_analysis: crate::orchestration::LocalFailoverAnalysis,
) -> Option<Value> {
    let body = trace_upstream_response_body(body_json, body_bytes);
    let upstream_context = with_upstream_response_report_context(
        report_context,
        status_code,
        Some(headers),
        body.as_ref(),
        None,
        None,
    );
    with_error_flow_report_context(
        upstream_context.as_ref().or(report_context),
        build_local_error_flow_metadata(status_code, response_text, local_failover_analysis),
    )
}

#[allow(clippy::too_many_arguments)] // stream report payload assembly mirrors runtime state
fn build_stream_usage_payload(
    trace_id: String,
    report_kind: String,
    report_context: Option<Value>,
    status_code: u16,
    headers: BTreeMap<String, String>,
    provider_body: &[u8],
    provider_body_truncated: bool,
    client_body: &[u8],
    client_body_truncated: bool,
    terminal_summary: Option<ExecutionStreamTerminalSummary>,
    telemetry: Option<ExecutionTelemetry>,
) -> GatewayStreamReportRequest {
    let (provider_body_base64, provider_body_state) =
        build_stream_body_capture(provider_body, provider_body_truncated);
    let (client_body_base64, client_body_state) =
        build_stream_body_capture(client_body, client_body_truncated);
    GatewayStreamReportRequest {
        trace_id,
        report_kind,
        report_context,
        status_code,
        headers,
        provider_body_base64,
        provider_body_state,
        client_body_base64,
        client_body_state,
        terminal_summary,
        telemetry,
    }
}

fn seed_kiro_report_context_input_tokens(plan: &ExecutionPlan, report_context: &mut Option<Value>) {
    if !plan
        .provider_name
        .as_deref()
        .is_some_and(|provider_name| provider_name.eq_ignore_ascii_case("Kiro"))
    {
        return;
    }

    let Some(context) = report_context.as_mut().and_then(Value::as_object_mut) else {
        return;
    };
    if context
        .get("input_tokens")
        .and_then(Value::as_u64)
        .is_some_and(|input_tokens| input_tokens > 0)
    {
        return;
    }

    let Some(original_request_body) = context.get("original_request_body").cloned() else {
        return;
    };
    let estimated_input_tokens = estimate_kiro_prompt_input_tokens(&original_request_body);
    context.insert(
        "input_tokens".to_string(),
        Value::from(estimated_input_tokens),
    );
}

async fn seed_kiro_simulated_cache_enabled(
    state: &AppState,
    plan: &ExecutionPlan,
    report_context: &mut Option<Value>,
) {
    if !plan
        .provider_name
        .as_deref()
        .is_some_and(|provider_name| provider_name.eq_ignore_ascii_case("Kiro"))
    {
        return;
    }

    let enabled = match state
        .read_provider_catalog_providers_by_ids(std::slice::from_ref(&plan.provider_id))
        .await
    {
        Ok(providers) => providers
            .iter()
            .find(|provider| provider.id == plan.provider_id)
            .filter(|provider| provider.provider_type.eq_ignore_ascii_case("kiro"))
            .is_some_and(|provider| {
                kiro_simulated_cache_enabled_from_provider_config(provider.config.as_ref())
            }),
        Err(_err) => {
            warn!(
                event_name = "kiro_simulated_cache_config_read_failed",
                log_type = "event",
                request_id = %plan.request_id,
                provider_id = %plan.provider_id,
                error_category = "provider_catalog_read_failed",
                "failed to read Kiro simulated cache provider config; defaulting disabled"
            );
            false
        }
    };

    let Some(context) = report_context.as_mut().and_then(Value::as_object_mut) else {
        return;
    };
    if enabled {
        context.insert(
            KIRO_SIMULATED_CACHE_ENABLED_CONTEXT_FIELD.to_string(),
            Value::Bool(true),
        );
    } else {
        context.remove(KIRO_SIMULATED_CACHE_ENABLED_CONTEXT_FIELD);
    }
}

async fn seed_kiro_report_context_prompt_cache_usage(
    state: &AppState,
    plan: &ExecutionPlan,
    report_context: &mut Option<Value>,
) {
    if !plan
        .provider_name
        .as_deref()
        .is_some_and(|provider_name| provider_name.eq_ignore_ascii_case("Kiro"))
    {
        return;
    }

    let simulated_cache_enabled =
        kiro_simulated_cache_enabled_from_report_context(report_context.as_ref());
    let Some(context) = report_context.as_mut().and_then(Value::as_object_mut) else {
        return;
    };
    if context
        .get("kiro_web_search_mcp")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        return;
    }
    if !simulated_cache_enabled {
        return;
    }
    if kiro_cache_usage_from_context_object(context).is_some() {
        return;
    }

    let Some(original_request_body) = context.get("original_request_body").cloned() else {
        return;
    };
    let input_tokens = context
        .get("input_tokens")
        .and_then(Value::as_u64)
        .filter(|value| *value > 0)
        .unwrap_or_else(|| {
            let estimated = estimate_kiro_prompt_input_tokens(&original_request_body);
            context.insert("input_tokens".to_string(), Value::from(estimated));
            estimated
        });
    let Some(profile) = build_kiro_prompt_cache_profile(&original_request_body, input_tokens)
    else {
        return;
    };

    let cache_usage = compute_kiro_prompt_cache_usage(
        state.runtime_state(),
        kiro_stream_cache_credential_id(plan),
        &profile,
    )
    .await;
    if cache_usage.cache_creation_input_tokens == 0 && cache_usage.cache_read_input_tokens == 0 {
        return;
    }
    context.insert(
        "cache_creation_input_tokens".to_string(),
        Value::from(cache_usage.cache_creation_input_tokens),
    );
    context.insert(
        "cache_read_input_tokens".to_string(),
        Value::from(cache_usage.cache_read_input_tokens),
    );
}

fn kiro_stream_cache_credential_id(plan: &ExecutionPlan) -> String {
    format!("{}:{}:{}", plan.provider_id, plan.endpoint_id, plan.key_id)
}

fn kiro_cache_usage_from_context_object(
    context: &serde_json::Map<String, Value>,
) -> Option<KiroPromptCacheUsage> {
    let cache_creation_input_tokens = context
        .get("cache_creation_input_tokens")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let cache_read_input_tokens = context
        .get("cache_read_input_tokens")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    (cache_creation_input_tokens > 0 || cache_read_input_tokens > 0).then_some(
        KiroPromptCacheUsage {
            cache_creation_input_tokens,
            cache_read_input_tokens,
        },
    )
}

fn kiro_cache_usage_from_report_context(report_context: &Value) -> Option<KiroPromptCacheUsage> {
    report_context
        .as_object()
        .and_then(kiro_cache_usage_from_context_object)
}

async fn maybe_apply_kiro_prompt_cache_usage_to_stream_summary(
    state: &AppState,
    plan: &ExecutionPlan,
    report_context: Option<&Value>,
    summary: &mut Option<ExecutionStreamTerminalSummary>,
) {
    if !plan
        .provider_name
        .as_deref()
        .is_some_and(|provider_name| provider_name.eq_ignore_ascii_case("Kiro"))
    {
        return;
    }

    let Some(report_context) = report_context else {
        return;
    };
    let Some(original_request_body) = report_context.get("original_request_body") else {
        return;
    };
    let simulated_cache_enabled =
        kiro_simulated_cache_enabled_from_report_context(Some(report_context));

    let summary = summary.get_or_insert_with(ExecutionStreamTerminalSummary::default);
    let usage = summary
        .standardized_usage
        .get_or_insert_with(StandardizedUsage::new);
    let estimated_input_tokens = report_context
        .get("input_tokens")
        .and_then(Value::as_u64)
        .filter(|value| *value > 0)
        .unwrap_or_else(|| {
            let estimated_input_tokens = estimate_kiro_prompt_input_tokens(original_request_body);
            if estimated_input_tokens > 0 {
                estimated_input_tokens
            } else {
                usage.input_tokens.max(0) as u64
            }
        });

    if !simulated_cache_enabled {
        usage.cache_creation_tokens = 0;
        usage.cache_read_tokens = 0;
        if usage.input_tokens <= 0 {
            usage.input_tokens = estimated_input_tokens as i64;
            if usage.input_tokens > 0 {
                mark_kiro_stream_estimated_usage(usage, report_context, false);
            }
        }
        return;
    }

    if let Some(cache_usage) = kiro_cache_usage_from_report_context(report_context) {
        usage.input_tokens = kiro_billed_input_tokens(estimated_input_tokens, cache_usage) as i64;
        usage.cache_creation_tokens = cache_usage.cache_creation_input_tokens as i64;
        usage.cache_read_tokens = cache_usage.cache_read_input_tokens as i64;
        if usage.input_tokens > 0 || usage.cache_creation_tokens > 0 || usage.cache_read_tokens > 0
        {
            mark_kiro_stream_estimated_usage(usage, report_context, false);
        }
        return;
    }

    if usage.cache_creation_tokens > 0 || usage.cache_read_tokens > 0 {
        if usage.input_tokens <= 0 {
            usage.input_tokens = kiro_billed_input_tokens(
                estimated_input_tokens,
                KiroPromptCacheUsage {
                    cache_creation_input_tokens: usage.cache_creation_tokens.max(0) as u64,
                    cache_read_input_tokens: usage.cache_read_tokens.max(0) as u64,
                },
            ) as i64;
            if usage.input_tokens > 0 {
                mark_kiro_stream_estimated_usage(usage, report_context, true);
            }
        }
        return;
    }

    if usage.input_tokens <= 0 {
        usage.input_tokens = estimated_input_tokens as i64;
        if usage.input_tokens > 0 {
            mark_kiro_stream_estimated_usage(usage, report_context, true);
        }
    }

    let Some(profile) =
        build_kiro_prompt_cache_profile(original_request_body, estimated_input_tokens)
    else {
        return;
    };

    let cache_usage = compute_kiro_prompt_cache_usage(
        state.runtime_state(),
        kiro_stream_cache_credential_id(plan),
        &profile,
    )
    .await;
    if cache_usage.cache_creation_input_tokens == 0 && cache_usage.cache_read_input_tokens == 0 {
        return;
    }

    let billed_input_tokens = kiro_billed_input_tokens(estimated_input_tokens, cache_usage);
    usage.input_tokens = billed_input_tokens as i64;
    usage.cache_creation_tokens = cache_usage.cache_creation_input_tokens as i64;
    usage.cache_read_tokens = cache_usage.cache_read_input_tokens as i64;
    mark_kiro_stream_estimated_usage(usage, report_context, false);
}

fn mark_kiro_stream_estimated_usage(
    usage: &mut StandardizedUsage,
    report_context: &Value,
    retains_cache: bool,
) {
    let retained_source = usage.token_source.unwrap_or_else(|| {
        match report_context
            .get("usage_token_source")
            .and_then(Value::as_str)
        {
            Some("estimated") => UsageTokenSource::Estimated,
            Some("mixed") => UsageTokenSource::Mixed,
            _ => UsageTokenSource::Reported,
        }
    });
    let retains_reported_tokens = retained_source != UsageTokenSource::Estimated
        && (usage.output_tokens > 0
            || usage.reasoning_tokens > 0
            || usage.cache_creation_ephemeral_5m_tokens > 0
            || usage.cache_creation_ephemeral_1h_tokens > 0
            || (retains_cache && (usage.cache_creation_tokens > 0 || usage.cache_read_tokens > 0)));
    usage.token_source = Some(if retains_reported_tokens {
        UsageTokenSource::Mixed
    } else {
        UsageTokenSource::Estimated
    });
}

fn append_stream_capture_bytes(
    buffer: &mut Vec<u8>,
    chunk: &[u8],
    max_bytes: usize,
    truncated: &mut bool,
) {
    if chunk.is_empty() || max_bytes == 0 {
        return;
    }
    if buffer.len() >= max_bytes {
        *truncated = true;
        return;
    }
    let remaining = max_bytes - buffer.len();
    let keep_len = remaining.min(chunk.len());
    buffer.extend_from_slice(&chunk[..keep_len]);
    if keep_len < chunk.len() {
        *truncated = true;
    }
}

fn append_budgeted_stream_capture_bytes(
    buffer: &mut StreamBodyCapture,
    chunk: &[u8],
    max_bytes: usize,
    truncated: &mut bool,
) {
    buffer.append(chunk, max_bytes, truncated);
}

struct StreamUsageObservationBuffer {
    line: Vec<u8>,
    fallback: StreamUsageFallback,
    recovered_usage_after_parser_error: bool,
}

impl StreamUsageObservationBuffer {
    fn new(record_limit: usize) -> Self {
        Self {
            line: Vec::new(),
            fallback: StreamUsageFallback::new(record_limit),
            recovered_usage_after_parser_error: false,
        }
    }
}

fn observe_stream_usage_bytes(
    observer: &mut StreamingStandardTerminalObserver,
    report_context: &Value,
    buffer: &mut StreamUsageObservationBuffer,
    chunk: &[u8],
) {
    buffer.fallback.observe(report_context, chunk);
    let buffered = &mut buffer.line;
    if chunk.is_empty()
        || observer
            .latest_summary()
            .and_then(|summary| summary.parser_error.as_deref())
            .is_some()
    {
        return;
    }

    let mut remaining = chunk;
    while !remaining.is_empty() {
        let line_part_len = remaining
            .iter()
            .position(|byte| *byte == b'\n')
            .map_or(remaining.len(), |index| index + 1);
        if buffered.len().saturating_add(line_part_len) > SSE_TERMINAL_DETECTOR_MAX_LINE_BYTES {
            observer.disable_with_error(format!(
                "stream usage event exceeded {SSE_TERMINAL_DETECTOR_MAX_LINE_BYTES} bytes"
            ));
            *buffered = Vec::new();
            return;
        }
        buffered.extend_from_slice(&remaining[..line_part_len]);
        remaining = &remaining[line_part_len..];
        if buffered.last() == Some(&b'\n') {
            let line = std::mem::take(buffered);
            if let Err(_err) = observer.push_line(report_context, line) {
                observer.disable_with_error("stream usage parsing failed");
                *buffered = Vec::new();
                return;
            }
        }
    }
}

fn finalize_stream_usage_observer(
    observer: &mut Option<StreamingStandardTerminalObserver>,
    report_context: Option<&Value>,
    buffer: &mut StreamUsageObservationBuffer,
) -> Option<ExecutionStreamTerminalSummary> {
    let (Some(observer), Some(report_context)) = (observer.as_mut(), report_context) else {
        return None;
    };

    let buffered = &mut buffer.line;
    if !buffered.is_empty() {
        let line = std::mem::take(buffered);
        if let Err(_err) = observer.push_line(report_context, line) {
            observer.disable_with_error("stream usage parsing failed");
        }
    }

    let mut summary = match observer.finish(report_context) {
        Ok(summary) => summary,
        Err(_err) => {
            observer.disable_with_error("stream usage parsing failed");
            observer.latest_summary().cloned()
        }
    };
    let mut fallback_usage = buffer.fallback.finish(report_context);
    let fallback_tier = buffer.fallback.take_service_tier();
    if let Some(summary) = summary.as_mut() {
        if summary.parser_error.is_some() && fallback_usage.is_some() {
            // A disabled parser can retain an earlier usage snapshot. Later
            // complete fallback events remain authoritative even when their
            // signal score is unchanged or an explicit zero reduces it.
            summary.standardized_usage = fallback_usage.take();
            buffer.recovered_usage_after_parser_error = true;
        }
        if summary.provider_actual_service_tier.is_none() {
            summary.provider_actual_service_tier = fallback_tier.clone();
        }
    }
    let fallback = fallback_usage.map(|usage| ExecutionStreamTerminalSummary {
        standardized_usage: Some(usage),
        provider_actual_service_tier: summary.is_none().then_some(fallback_tier).flatten(),
        ..ExecutionStreamTerminalSummary::default()
    });
    merge_stream_terminal_summary(summary, fallback)
}

fn merge_observed_stream_terminal_summary(
    current: Option<ExecutionStreamTerminalSummary>,
    observed: Option<ExecutionStreamTerminalSummary>,
    usage_buffer: &StreamUsageObservationBuffer,
) -> Option<ExecutionStreamTerminalSummary> {
    let recovered_usage = usage_buffer
        .recovered_usage_after_parser_error
        .then(|| {
            observed
                .as_ref()
                .and_then(|summary| summary.standardized_usage.clone())
        })
        .flatten();
    let mut summary = merge_stream_terminal_summary(current, observed);
    if let (Some(summary), Some(usage)) = (summary.as_mut(), recovered_usage) {
        summary.standardized_usage = Some(usage);
    }
    summary
}

fn merge_stream_terminal_summary(
    mut current: Option<ExecutionStreamTerminalSummary>,
    observed: Option<ExecutionStreamTerminalSummary>,
) -> Option<ExecutionStreamTerminalSummary> {
    let Some(observed) = observed else {
        return current;
    };

    let Some(current_summary) = current.as_mut() else {
        return Some(observed);
    };

    if should_replace_stream_usage(
        current_summary.standardized_usage.as_ref(),
        observed.standardized_usage.as_ref(),
    ) {
        current_summary.standardized_usage = observed.standardized_usage;
    }
    if current_summary.finish_reason.is_none() {
        current_summary.finish_reason = observed.finish_reason;
    }
    if current_summary.response_id.is_none() {
        current_summary.response_id = observed.response_id;
    }
    if current_summary.model.is_none() {
        current_summary.model = observed.model;
    }
    if observed.provider_actual_service_tier.is_some() {
        current_summary.provider_actual_service_tier = observed.provider_actual_service_tier;
    }
    current_summary.observed_finish |= observed.observed_finish;
    current_summary.unknown_event_count = current_summary
        .unknown_event_count
        .saturating_add(observed.unknown_event_count);
    if current_summary.parser_error.is_none() {
        current_summary.parser_error = observed.parser_error;
    }

    current
}

fn should_replace_stream_usage(
    current: Option<&aether_contracts::StandardizedUsage>,
    observed: Option<&aether_contracts::StandardizedUsage>,
) -> bool {
    let Some(observed) = observed else {
        return false;
    };
    let Some(current) = current else {
        return true;
    };

    observed.is_more_complete_than(current)
}

fn stream_terminal_summary_missing_observed_finish(
    summary: Option<&ExecutionStreamTerminalSummary>,
) -> bool {
    summary.is_some_and(|summary| {
        !summary.observed_finish
            && !summary
                .standardized_usage
                .as_ref()
                .is_some_and(StandardizedUsage::has_token_signal)
    })
}

fn stream_report_context_format_field<'a>(
    report_context: Option<&'a Value>,
    field: &str,
) -> Option<&'a str> {
    report_context
        .and_then(Value::as_object)
        .and_then(|object| object.get(field))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

fn stream_requires_observed_terminal_event(
    provider_api_format: &str,
    report_context: Option<&Value>,
) -> bool {
    is_openai_responses_family_format(provider_api_format)
        || [
            "provider_stream_event_api_format",
            "provider_stream_api_format",
            "provider_api_format",
        ]
        .into_iter()
        .filter_map(|field| stream_report_context_format_field(report_context, field))
        .any(is_openai_responses_family_format)
}

fn stream_terminal_summary_missing_observed_finish_with_requirement(
    summary: Option<&ExecutionStreamTerminalSummary>,
    requires_observed_terminal_event: bool,
) -> bool {
    if !requires_observed_terminal_event {
        return stream_terminal_summary_missing_observed_finish(summary);
    }

    summary.is_some_and(|summary| !summary.observed_finish)
}

fn ensure_stream_terminal_summary_for_missing_observed_finish(
    summary: &mut Option<ExecutionStreamTerminalSummary>,
    requires_observed_terminal_event: bool,
) {
    if !requires_observed_terminal_event {
        return;
    }

    let summary = summary.get_or_insert_with(ExecutionStreamTerminalSummary::default);
    if !summary.observed_finish && summary.parser_error.is_none() {
        summary.parser_error =
            Some("execution runtime stream ended before provider terminal event".to_string());
    }
}

fn stream_terminal_summary_represents_failure_with_requirement(
    summary: Option<&ExecutionStreamTerminalSummary>,
    requires_observed_terminal_event: bool,
) -> bool {
    summary.is_some_and(|summary| {
        summary.parser_error.is_some()
            || stream_terminal_summary_missing_observed_finish_with_requirement(
                Some(summary),
                requires_observed_terminal_event,
            )
    })
}

async fn execute_in_process_stream(
    state: &AppState,
    plan: &ExecutionPlan,
    trace_id: &str,
) -> Result<DirectUpstreamStreamExecution, InProcessStreamExecutionError> {
    let upstream_target_permit = state
        .upstream_target_admission
        .acquire(plan, trace_id)
        .await?;
    match DirectSyncExecutionRuntime::new().execute_stream(plan).await {
        Ok(mut execution) => {
            execution.upstream_target_permit = upstream_target_permit;
            record_manual_proxy_request_success(state, plan).await;
            Ok(execution)
        }
        Err(error) => {
            record_manual_proxy_request_failure(state, plan).await;
            Err(error.into())
        }
    }
}

async fn execute_in_process_stream_with_oauth_retry(
    state: &AppState,
    plan: &mut ExecutionPlan,
    trace_id: &str,
    report_context: Option<&Value>,
) -> Result<DirectUpstreamStreamExecution, InProcessStreamExecutionError> {
    let mut execution = execute_in_process_stream(state, plan, trace_id).await?;
    apply_stream_summary_report_context(&mut execution, report_context);
    let uses_oauth_credential = stream_plan_uses_oauth_credential(state, plan).await;
    let embedded_oauth_credential = execution.status_code == 200
        && plan
            .provider_api_format
            .eq_ignore_ascii_case("claude:messages")
        && uses_oauth_credential;
    let prefetched_failure = if embedded_oauth_credential {
        prefetch_direct_anthropic_stream_failure(&mut execution, plan, report_context).await
    } else {
        None
    };
    let analyzed_prefetched_failure = match prefetched_failure {
        Some(failure) => {
            Some(analyze_prefetched_stream_failure(state, plan, report_context, failure).await)
        }
        None => None,
    };
    let response_text = if let Some(failure) = analyzed_prefetched_failure.as_ref() {
        Some(failure.response_text.clone())
    } else if execution.status_code == 403 && uses_oauth_credential {
        prefetch_direct_stream_error_body(&mut execution).await
    } else if execution.status_code == 401
        && stream_plan_uses_codex_agent_identity(state, plan).await
    {
        prefetch_direct_stream_error_body(&mut execution).await
    } else {
        None
    };
    let retry_status_code = analyzed_prefetched_failure
        .as_ref()
        .map(|failure| failure.status_code)
        .unwrap_or(execution.status_code);
    let retry_requested =
        analyzed_prefetched_failure
            .as_ref()
            .map_or(execution.status_code >= 400, |failure| {
                matches!(
                    failure.disposition.token_action,
                    FailureTokenAction::ForceRefresh
                )
            });
    if retry_requested
        && uses_oauth_credential
        && refresh_oauth_plan_auth_for_retry(
            state,
            plan,
            retry_status_code,
            response_text.as_deref(),
            trace_id,
            report_context,
            Some(execution.response_observation.request_started_at_unix_ms),
            Some(&execution.response_observation.request_order_id),
        )
        .await
    {
        drop(execution);
        execution = execute_in_process_stream(state, plan, trace_id).await?;
        apply_stream_summary_report_context(&mut execution, report_context);
    }
    Ok(execution)
}

#[derive(Debug)]
struct PrefetchedStreamFailure {
    status_code: u16,
    response_text: String,
}

#[derive(Debug)]
struct AnalyzedPrefetchedStreamFailure {
    status_code: u16,
    response_text: String,
    #[allow(dead_code)]
    analysis: LocalFailoverAnalysis,
    disposition: FailureDisposition,
}

async fn analyze_prefetched_stream_failure(
    state: &AppState,
    plan: &ExecutionPlan,
    report_context: Option<&Value>,
    failure: PrefetchedStreamFailure,
) -> AnalyzedPrefetchedStreamFailure {
    let analysis = resolve_local_candidate_failover_analysis_stream(
        state,
        plan,
        report_context,
        failure.status_code,
        Some(failure.response_text.as_str()),
    )
    .await;
    let disposition = classify_failure_disposition(
        plan.provider_api_format.as_str(),
        analysis.classification,
        failure.status_code,
        analysis.preserve_on_exhaustion,
    );
    AnalyzedPrefetchedStreamFailure {
        status_code: failure.status_code,
        response_text: failure.response_text,
        analysis,
        disposition,
    }
}

async fn stream_plan_uses_oauth_credential(state: &AppState, plan: &ExecutionPlan) -> bool {
    state
        .read_provider_transport_snapshot(&plan.provider_id, &plan.endpoint_id, &plan.key_id)
        .await
        .ok()
        .flatten()
        .as_ref()
        .is_some_and(|transport| {
            aether_provider_transport::auth::resolve_local_auth_type_for_transport_format(transport)
                .eq_ignore_ascii_case("oauth")
        })
}

async fn prefetch_direct_anthropic_stream_failure(
    execution: &mut DirectUpstreamStreamExecution,
    plan: &ExecutionPlan,
    report_context: Option<&Value>,
) -> Option<PrefetchedStreamFailure> {
    if execution.status_code != 200 {
        return None;
    }
    let normalized_stream_report_context =
        normalize_provider_private_report_context(report_context);
    let policy = StreamCommitPolicy::for_response(
        true,
        execution.headers.get("content-type").map(String::as_str),
        plan.provider_api_format.as_str(),
        plan.client_api_format.as_str(),
        maybe_build_provider_private_stream_normalizer(report_context).is_some(),
        maybe_build_stream_response_rewriter(normalized_stream_report_context.as_ref()).is_some(),
        false,
    );
    if !policy.is_native_anthropic() {
        return None;
    }

    let mut gate = StreamCommitGate::new(policy);
    let mut semantic_commit_observed = false;
    let precommit_started_at = Instant::now();
    let max_wait = policy.max_precommit_wait()?;
    let mut observed_first_body = execution
        .prefetched_body
        .iter()
        .any(|item| item.as_ref().is_ok_and(|chunk| !chunk.is_empty()));
    while gate.is_uncommitted() {
        let wait = select_direct_anthropic_prefetch_wait(
            precommit_started_at,
            max_wait,
            execution.started_at,
            execution.stream_first_byte_timeout,
            observed_first_body,
            Instant::now(),
        );
        if wait.remaining.is_zero() {
            if wait.commit_on_timeout {
                gate.commit();
            }
            break;
        }
        let next_chunk = match tokio::time::timeout(
            wait.remaining,
            next_direct_upstream_response_chunk(&mut execution.response),
        )
        .await
        {
            Ok(result) => result,
            Err(_) => {
                if wait.commit_on_timeout {
                    gate.commit();
                }
                break;
            }
        };
        let chunk = match next_chunk {
            Ok(Some(chunk)) => chunk,
            Ok(None) => break,
            Err(error) => {
                execution.prefetched_body.push_back(Err(error));
                break;
            }
        };
        if chunk.is_empty() {
            continue;
        }
        observed_first_body = true;
        execution.prefetched_body.push_back(Ok(chunk.clone()));
        match gate.observe_provider_bytes(&chunk) {
            StreamPrecommitObservation::Pending => {}
            StreamPrecommitObservation::Commit => {
                semantic_commit_observed = true;
                break;
            }
            StreamPrecommitObservation::UpstreamError {
                status_code,
                body_json,
            } => {
                let response_text = serde_json::to_string(&body_json)
                    .unwrap_or_else(|_| String::from_utf8_lossy(&chunk).into_owned());
                return Some(PrefetchedStreamFailure {
                    status_code,
                    response_text,
                });
            }
        }
    }
    execution.stream_precommit_committed = semantic_commit_observed;
    None
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DirectAnthropicPrefetchWait {
    remaining: Duration,
    commit_on_timeout: bool,
}

fn select_direct_anthropic_prefetch_wait(
    precommit_started_at: Instant,
    max_precommit_wait: Duration,
    upstream_started_at: Instant,
    first_byte_timeout: Option<Duration>,
    observed_first_body: bool,
    now: Instant,
) -> DirectAnthropicPrefetchWait {
    let precommit_remaining =
        max_precommit_wait.saturating_sub(now.saturating_duration_since(precommit_started_at));
    if observed_first_body {
        return DirectAnthropicPrefetchWait {
            remaining: precommit_remaining,
            commit_on_timeout: true,
        };
    }
    let Some(first_byte_timeout) = first_byte_timeout else {
        return DirectAnthropicPrefetchWait {
            remaining: precommit_remaining,
            commit_on_timeout: true,
        };
    };
    let first_byte_remaining =
        first_byte_timeout.saturating_sub(now.saturating_duration_since(upstream_started_at));
    if first_byte_remaining <= precommit_remaining {
        DirectAnthropicPrefetchWait {
            remaining: first_byte_remaining,
            commit_on_timeout: false,
        }
    } else {
        DirectAnthropicPrefetchWait {
            remaining: precommit_remaining,
            commit_on_timeout: true,
        }
    }
}

async fn stream_plan_uses_codex_agent_identity(state: &AppState, plan: &ExecutionPlan) -> bool {
    state
        .read_provider_transport_snapshot(&plan.provider_id, &plan.endpoint_id, &plan.key_id)
        .await
        .ok()
        .flatten()
        .as_ref()
        .is_some_and(aether_provider_transport::is_codex_agent_identity_transport)
}

async fn next_direct_upstream_response_chunk(
    response: &mut DirectUpstreamResponse,
) -> Result<Option<Bytes>, String> {
    match response {
        DirectUpstreamResponse::Reqwest(response) => response
            .chunk()
            .await
            .map_err(|err| format_upstream_request_error(&err)),
        DirectUpstreamResponse::HyperH2c(response) => loop {
            let Some(frame) = response.body_mut().frame().await else {
                return Ok(None);
            };
            let frame = frame.map_err(|err| format_hyper_error_chain(&err))?;
            if let Ok(chunk) = frame.into_data() {
                return Ok(Some(chunk));
            }
        },
        DirectUpstreamResponse::BrowserWreq(response) => response
            .chunk()
            .await
            .map_err(|err| format_wreq_upstream_request_error(&err)),
    }
}

async fn prefetch_direct_stream_error_body(
    execution: &mut DirectUpstreamStreamExecution,
) -> Option<String> {
    let prefetch_started_at = Instant::now();
    let mut inspected = Vec::with_capacity(MAX_ERROR_BODY_BYTES);
    let mut fully_buffered = false;
    while inspected.len() < MAX_ERROR_BODY_BYTES {
        let remaining = OAUTH_ERROR_PREFETCH_MAX_WAIT.saturating_sub(prefetch_started_at.elapsed());
        if remaining.is_zero() {
            break;
        }
        let next_chunk = if execution.prefetched_body.is_empty() {
            match tokio::time::timeout(
                remaining,
                await_direct_passthrough_first_item(
                    next_direct_upstream_response_chunk(&mut execution.response),
                    execution.started_at,
                    execution.stream_first_byte_timeout,
                ),
            )
            .await
            {
                Ok(Ok(item)) => item,
                Ok(Err(_)) | Err(_) => break,
            }
        } else {
            match tokio::time::timeout(
                remaining,
                next_direct_upstream_response_chunk(&mut execution.response),
            )
            .await
            {
                Ok(item) => item,
                Err(_) => break,
            }
        };
        let chunk = match next_chunk {
            Ok(Some(chunk)) => chunk,
            Ok(None) => {
                fully_buffered = true;
                break;
            }
            Err(error) => {
                execution.prefetched_body.push_back(Err(error));
                break;
            }
        };
        if chunk.is_empty() {
            continue;
        }

        let remaining = MAX_ERROR_BODY_BYTES.saturating_sub(inspected.len());
        inspected.extend_from_slice(&chunk[..chunk.len().min(remaining)]);
        execution.prefetched_body.push_back(Ok(chunk));

        let response_text = String::from_utf8_lossy(&inspected);
        if aether_provider_transport::is_codex_agent_identity_invalid_task_response(
            execution.status_code,
            Some(response_text.as_ref()),
        ) {
            break;
        }
    }

    if inspected.is_empty() {
        return None;
    }
    if fully_buffered {
        let (body_json, _) = decode_stream_error_body(&execution.headers, &inspected);
        if let Some(body_json) = body_json {
            if let Ok(response_text) = serde_json::to_string(&body_json) {
                return Some(response_text);
            }
        }
    }
    Some(String::from_utf8_lossy(&inspected).into_owned())
}

fn should_use_direct_sse_passthrough(
    plan: &ExecutionPlan,
    plan_kind: &str,
    report_context: Option<&Value>,
    execution: &DirectUpstreamStreamExecution,
) -> bool {
    if !(200..300).contains(&execution.status_code) {
        return false;
    }
    if plan_kind == OPENAI_IMAGE_STREAM_PLAN_KIND {
        return false;
    }
    if is_openai_responses_family_format(plan.provider_api_format.as_str())
        || is_openai_responses_family_format(plan.client_api_format.as_str())
    {
        return false;
    }
    if !response_headers_indicate_sse(&execution.headers) {
        return false;
    }
    if !plan
        .provider_api_format
        .eq_ignore_ascii_case(plan.client_api_format.as_str())
    {
        return false;
    }
    if client_format_allows_proxy_generated_sse_control_blocks(plan) {
        return false;
    }
    if maybe_build_provider_private_stream_normalizer(report_context).is_some() {
        return false;
    }
    let normalized_stream_report_context =
        normalize_provider_private_report_context(report_context);
    if maybe_build_stream_response_rewriter(normalized_stream_report_context.as_ref()).is_some() {
        return false;
    }

    let direct_stream_finalize_kind = resolve_core_stream_direct_finalize_report_kind(plan_kind);
    should_skip_direct_finalize_prefetch(
        direct_stream_finalize_kind.as_deref(),
        execution.headers.get("content-type").map(String::as_str),
        plan.provider_api_format.as_str(),
        plan.client_api_format.as_str(),
        false,
        false,
        false,
    )
}

type DirectUpstreamByteStream = BoxStream<'static, Result<Bytes, String>>;

async fn await_direct_passthrough_first_item<T, F>(
    future: F,
    started_at: Instant,
    timeout: Option<Duration>,
) -> Result<T, Duration>
where
    F: Future<Output = T>,
{
    let Some(timeout) = timeout else {
        return Ok(future.await);
    };
    let Some(remaining) = timeout.checked_sub(started_at.elapsed()) else {
        return Err(timeout);
    };
    if remaining.is_zero() {
        return Err(timeout);
    }
    tokio::time::timeout(remaining, future)
        .await
        .map_err(|_| timeout)
}

#[allow(clippy::too_many_arguments)]
async fn forward_direct_passthrough_client_chunk(
    tx: &mpsc::Sender<Result<Bytes, IoError>>,
    chunk: Bytes,
    downstream_dropped: &mut bool,
    client_visible_stream_completed: &mut bool,
    client_stream_completion_tracker: &mut ClientVisibleStreamCompletionTracker,
    observe_stream_completion: bool,
    client_stream_bytes: &mut u64,
    buffered_body: &mut StreamBodyCapture,
    client_body_truncated: &mut bool,
    max_stream_body_buffer_bytes: usize,
    stream_started_at: Instant,
    last_client_chunk_elapsed_ms: &mut u64,
    first_client_chunk: bool,
    trace_id: &str,
    request_id_for_log: &str,
    candidate_id: Option<&str>,
) -> bool {
    if chunk.is_empty() {
        return false;
    }
    append_budgeted_stream_capture_bytes(
        buffered_body,
        chunk.as_ref(),
        max_stream_body_buffer_bytes,
        client_body_truncated,
    );
    if *downstream_dropped {
        return false;
    }
    let chunk_len = u64::try_from(chunk.len()).unwrap_or(u64::MAX);
    let send_started_at = Instant::now();
    if tx.send(Ok(chunk.clone())).await.is_err() {
        debug!(
            event_name = "direct_passthrough_downstream_disconnected",
            log_type = "ops",
            trace_id = %trace_id,
            request_id = %request_id_for_log,
            candidate_id = ?candidate_id,
            "gateway direct passthrough downstream dropped; cancelling upstream stream"
        );
        *downstream_dropped = true;
        return false;
    }
    let send_wait_ms = send_started_at.elapsed().as_millis() as u64;
    observe_gateway_stage_ms("direct_passthrough_body_send_wait", send_wait_ms);
    if first_client_chunk {
        observe_gateway_stage_ms("direct_passthrough_first_client_send_wait", send_wait_ms);
    }

    if observe_stream_completion {
        *client_visible_stream_completed |=
            client_stream_completion_tracker.observe_chunk(chunk.as_ref());
    }
    *client_stream_bytes = client_stream_bytes.saturating_add(chunk_len);
    *last_client_chunk_elapsed_ms = stream_started_at
        .elapsed()
        .as_millis()
        .min(u128::from(u64::MAX)) as u64;
    true
}

struct DirectPassthroughFinalizer {
    core: Option<DirectPassthroughFinalizerCore>,
}

struct DirectPassthroughFinalizerCore {
    state: AppState,
    plan: ExecutionPlan,
    trace_id: String,
    report_kind: Option<String>,
    report_context: Option<Value>,
    lifecycle_seed: LifecycleUsageSeed,
    direct_stream_finalize_kind: Option<String>,
    stream_started_at: Instant,
    stage_trace: RequestStageTrace,
    request_diagnostics: Option<Arc<RequestDiagnostics>>,
    request_id_for_log: String,
    candidate_id: Option<String>,
    request_candidate_status_snapshot: Option<LocalRequestCandidateStatusSnapshot>,
    deferred_request_candidate_status_record: Option<UpsertRequestCandidateRecord>,
    candidate_started_unix_secs: u64,
    status_code: u16,
    headers: BTreeMap<String, String>,
    stream_usage_report_context: Option<Value>,
    stream_usage_observer: Option<StreamingStandardTerminalObserver>,
    stream_usage_observer_buffered: StreamUsageObservationBuffer,
    provider_error_inspection: ProviderStreamErrorInspection,
    max_stream_body_buffer_bytes: usize,
    provider_buffered_body: StreamBodyCapture,
    buffered_body: StreamBodyCapture,
    provider_body_truncated: bool,
    client_body_truncated: bool,
    client_stream_completion_tracker: ClientVisibleStreamCompletionTracker,
    requires_anthropic_message_stop: bool,
    client_visible_stream_completed: bool,
    usage_stream_telemetry: Option<ExecutionTelemetry>,
    telemetry: Option<ExecutionTelemetry>,
    provider_stream_bytes: u64,
    client_stream_bytes: u64,
    last_client_chunk_elapsed_ms: u64,
    pending_recorded: bool,
    stream_started_recorded: bool,
    terminal_failure: Option<StreamFailureReport>,
    _provider_pool_in_flight_guard: Option<ProviderPoolInFlightGuard>,
    _upstream_target_permit: Option<crate::upstream_admission::UpstreamTargetAdmissionPermit>,
}

impl DirectPassthroughFinalizer {
    fn new(core: DirectPassthroughFinalizerCore) -> Self {
        Self { core: Some(core) }
    }

    fn core(&self) -> &DirectPassthroughFinalizerCore {
        self.core
            .as_ref()
            .expect("direct passthrough finalizer core should exist")
    }

    fn core_mut(&mut self) -> &mut DirectPassthroughFinalizerCore {
        self.core
            .as_mut()
            .expect("direct passthrough finalizer core should exist")
    }

    fn ttfb_observed(&self) -> bool {
        self.core()
            .usage_stream_telemetry
            .as_ref()
            .and_then(|telemetry| telemetry.ttfb_ms)
            .is_some()
    }

    fn terminal_failure(&self) -> Option<&StreamFailureReport> {
        self.core().terminal_failure.as_ref()
    }

    fn set_terminal_failure(&mut self, failure: StreamFailureReport) {
        self.core_mut().terminal_failure = Some(failure);
    }

    fn prepare_upstream_chunk(&mut self, mut chunk: Bytes) -> Option<Bytes> {
        let core = self.core_mut();
        if !core.requires_anthropic_message_stop {
            return Some(chunk);
        }
        if core.client_visible_stream_completed {
            return None;
        }
        if let Some(terminal_end) = core
            .client_stream_completion_tracker
            .observe_anthropic_message_stop_terminal_end(chunk.as_ref())
        {
            chunk.truncate(terminal_end);
            core.client_visible_stream_completed = true;
        }
        Some(chunk)
    }

    fn fail_if_anthropic_message_stop_missing(&mut self) {
        let core = self.core_mut();
        if core.requires_anthropic_message_stop
            && !core.client_visible_stream_completed
            && core.terminal_failure.is_none()
        {
            core.terminal_failure = Some(build_anthropic_premature_eof_failure(
                "upstream Anthropic stream ended before message_stop",
            ));
        }
    }

    fn completed_native_anthropic_stream(&self) -> bool {
        let core = self.core();
        core.requires_anthropic_message_stop
            && core.client_visible_stream_completed
            && core.terminal_failure.is_none()
    }

    fn log_terminal_error_event_encode_failed(&self, _err: impl std::fmt::Debug) {
        let core = self.core();
        warn!(
            event_name = "direct_passthrough_terminal_error_event_encode_failed",
            log_type = "ops",
            trace_id = %core.trace_id,
            request_id = %core.request_id_for_log,
            candidate_id = ?core.candidate_id.as_deref(),
            error_category = "terminal_error_event_encode_failed",
            "gateway direct passthrough failed to encode terminal SSE error event"
        );
    }

    fn observe_first_body_poll(&mut self) {
        let core = self.core_mut();
        observe_gateway_stage_trace_ms(
            &mut core.stage_trace,
            "stream_body_inline_first_poll",
            stream_elapsed_ms_since(core.stream_started_at),
        );
        let request_diagnostics = core.request_diagnostics.clone();
        observe_request_accepted_stage_trace_ms(
            &mut core.stage_trace,
            request_diagnostics.as_ref(),
            "frontdoor_to_stream_body_first_poll",
        );
    }

    fn observe_upstream_chunk(&mut self, chunk: &Bytes, observed_at: Instant) {
        let core = self.core_mut();
        if core.provider_stream_bytes == 0 {
            observe_gateway_stage_trace_ms(
                &mut core.stage_trace,
                "direct_passthrough_upstream_body_first",
                stream_elapsed_ms_at(core.stream_started_at, observed_at),
            );
        }
        let captured_first_stream_event = maybe_capture_first_stream_event_telemetry(
            core.stream_started_at,
            observed_at,
            core.telemetry.as_ref(),
            &mut core.usage_stream_telemetry,
        );
        if captured_first_stream_event && core.provider_stream_bytes == 0 {
            observe_gateway_stage_trace_ms(
                &mut core.stage_trace,
                "stream_first_data",
                stream_elapsed_ms_at(core.stream_started_at, observed_at),
            );
        }

        core.provider_stream_bytes = core
            .provider_stream_bytes
            .saturating_add(u64::try_from(chunk.len()).unwrap_or(u64::MAX));
        append_budgeted_stream_capture_bytes(
            &mut core.provider_buffered_body,
            chunk.as_ref(),
            core.max_stream_body_buffer_bytes,
            &mut core.provider_body_truncated,
        );
        if let (Some(observer), Some(report_context)) = (
            core.stream_usage_observer.as_mut(),
            core.stream_usage_report_context.as_ref(),
        ) {
            observe_stream_usage_bytes(
                observer,
                report_context,
                &mut core.stream_usage_observer_buffered,
                chunk.as_ref(),
            );
        }
        if let Some(error_body_json) = core
            .provider_error_inspection
            .observe(core.stream_usage_report_context.as_ref(), chunk.as_ref())
        {
            let error_status_code = resolve_provider_stream_error_status_code(
                core.plan.provider_api_format.as_str(),
                core.status_code,
                &error_body_json,
            );
            core.terminal_failure = Some(build_stream_failure_from_provider_error_body(
                error_status_code,
                &error_body_json,
            ));
        }
    }

    fn observe_client_chunk(&mut self, chunk: &Bytes) {
        if chunk.is_empty() {
            return;
        }
        let core = self.core_mut();
        append_budgeted_stream_capture_bytes(
            &mut core.buffered_body,
            chunk.as_ref(),
            core.max_stream_body_buffer_bytes,
            &mut core.client_body_truncated,
        );
        if !core.requires_anthropic_message_stop {
            core.client_visible_stream_completed |= core
                .client_stream_completion_tracker
                .observe_chunk(chunk.as_ref());
        }
        core.client_stream_bytes = core
            .client_stream_bytes
            .saturating_add(u64::try_from(chunk.len()).unwrap_or(u64::MAX));
        core.last_client_chunk_elapsed_ms = core
            .stream_started_at
            .elapsed()
            .as_millis()
            .min(u128::from(u64::MAX)) as u64;
    }

    fn observe_first_client_yield(&mut self) {
        let core = self.core_mut();
        let elapsed_ms = stream_elapsed_ms_since(core.stream_started_at);
        observe_gateway_stage_trace_ms(
            &mut core.stage_trace,
            "direct_passthrough_first_client_send",
            elapsed_ms,
        );
        observe_gateway_stage_trace_ms(
            &mut core.stage_trace,
            "stream_first_client_yield",
            elapsed_ms,
        );
        let request_diagnostics = core.request_diagnostics.clone();
        observe_request_accepted_stage_trace_ms(
            &mut core.stage_trace,
            request_diagnostics.as_ref(),
            "frontdoor_to_stream_first_client_yield",
        );
    }

    fn release_upstream_target_permit_after_first_yield(&mut self) {
        let core = self.core_mut();
        if core._upstream_target_permit.take().is_some() {
            observe_gateway_stage_trace_ms(
                &mut core.stage_trace,
                "stream_upstream_target_permit_release",
                stream_elapsed_ms_since(core.stream_started_at),
            );
        }
    }

    fn record_client_visible_stream_started_if_needed(&mut self) {
        let Some(core) = self.core.as_mut() else {
            return;
        };
        core.record_client_visible_stream_started_if_needed();
    }

    async fn finalize(&mut self, downstream_dropped: bool) {
        let Some(core) = self.core.take() else {
            return;
        };
        // Move the owned terminal payload into a task before awaiting it. A
        // client disconnect or an execution timeout may cancel this body
        // future while terminal admission is backpressured; the handoff must
        // continue independently so the usage row cannot remain streaming.
        let usage_producer = core.state.usage_runtime.track_producer();
        let task = tokio::spawn(async move {
            let _usage_producer = usage_producer;
            core.finalize(downstream_dropped).await;
        });
        if let Err(_err) = task.await {
            warn!(
                event_name = "direct_passthrough_terminal_handoff_failed",
                log_type = "ops",
                error_category = "terminal_handoff_failed",
                "gateway direct passthrough terminal handoff task failed"
            );
        }
    }
}

impl Drop for DirectPassthroughFinalizer {
    fn drop(&mut self) {
        let Some(core) = self.core.take() else {
            return;
        };
        observe_gateway_stage_ms("stream_finalizer_enqueue", 0);
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            let usage_producer = core.state.usage_runtime.track_producer();
            handle.spawn(async move {
                let _usage_producer = usage_producer;
                core.finalize(true).await;
            });
        }
    }
}

fn enqueue_stream_candidate_status_update(
    state: &AppState,
    snapshot: LocalRequestCandidateStatusSnapshot,
    status_update: SchedulerRequestCandidateStatusUpdate,
) -> Option<UpsertRequestCandidateRecord> {
    let Err(record) =
        try_enqueue_local_request_candidate_status_snapshot(state, &snapshot, status_update)
    else {
        return None;
    };
    if state.request_candidate_queue.is_some() {
        return Some(record);
    }

    // Without an async queue, preserve the first-byte path's existing
    // fire-and-handoff behavior. Queue saturation uses the bounded deferred
    // record above and does not create one task per waiter.
    let state = state.clone();
    tokio::spawn(async move {
        persist_local_request_candidate_status_record(&state, record).await;
    });
    None
}

impl DirectPassthroughFinalizerCore {
    fn record_client_visible_stream_started_if_needed(&mut self) {
        if self.stream_started_recorded || self.client_stream_bytes == 0 {
            return;
        }
        self.stream_started_recorded = true;
        if !self.pending_recorded {
            self.pending_recorded = true;
            self.state.usage_runtime.record_pending(
                self.state.usage_lifecycle_data_state().as_ref(),
                self.lifecycle_seed.clone(),
            );
        }
        self.state.usage_runtime.record_stream_started(
            self.state.usage_lifecycle_data_state().as_ref(),
            &self.lifecycle_seed,
            self.status_code,
            self.usage_stream_telemetry.as_ref(),
        );
        if let Some(snapshot) = self.request_candidate_status_snapshot.take() {
            self.deferred_request_candidate_status_record = enqueue_stream_candidate_status_update(
                &self.state,
                snapshot,
                SchedulerRequestCandidateStatusUpdate {
                    status: RequestCandidateStatus::Streaming,
                    status_code: Some(self.status_code),
                    error_type: None,
                    error_message: None,
                    latency_ms: None,
                    started_at_unix_ms: Some(self.candidate_started_unix_secs),
                    finished_at_unix_ms: None,
                },
            );
        }
    }

    async fn finalize(mut self, mut downstream_dropped: bool) {
        self.record_client_visible_stream_started_if_needed();
        observe_gateway_stage_ms(
            "stream_total",
            stream_elapsed_ms_since(self.stream_started_at),
        );
        let stream_terminal_summary = finalize_stream_usage_observer(
            &mut self.stream_usage_observer,
            self.stream_usage_report_context.as_ref(),
            &mut self.stream_usage_observer_buffered,
        );

        let DirectPassthroughFinalizerCore {
            state,
            plan,
            trace_id,
            report_kind,
            report_context,
            lifecycle_seed: _,
            direct_stream_finalize_kind,
            stream_started_at,
            stage_trace,
            request_diagnostics,
            request_id_for_log,
            candidate_id,
            request_candidate_status_snapshot: _,
            deferred_request_candidate_status_record,
            candidate_started_unix_secs,
            status_code,
            headers,
            stream_usage_report_context,
            stream_usage_observer: _,
            stream_usage_observer_buffered: _,
            provider_error_inspection: _,
            max_stream_body_buffer_bytes: _,
            provider_buffered_body,
            buffered_body,
            provider_body_truncated,
            client_body_truncated,
            client_stream_completion_tracker: _,
            requires_anthropic_message_stop: _,
            client_visible_stream_completed,
            usage_stream_telemetry,
            telemetry,
            provider_stream_bytes,
            client_stream_bytes: _,
            last_client_chunk_elapsed_ms: _,
            pending_recorded: _,
            stream_started_recorded: _,
            terminal_failure,
            _provider_pool_in_flight_guard,
            _upstream_target_permit,
        } = self;

        // Queue backpressure must not keep scarce upstream/provider permits
        // occupied after the client-visible stream has already ended.
        drop(_provider_pool_in_flight_guard);
        drop(_upstream_target_permit);
        if let Some(record) = deferred_request_candidate_status_record {
            persist_local_request_candidate_status_record(&state, record).await;
        }

        if downstream_dropped && client_visible_stream_completed && terminal_failure.is_none() {
            debug!(
                event_name = "direct_passthrough_downstream_closed_after_done",
                log_type = "debug",
                trace_id = %trace_id,
                request_id = %request_id_for_log,
                candidate_id = ?candidate_id.as_deref(),
                "gateway treats direct passthrough downstream close after terminal SSE event as completed"
            );
            downstream_dropped = false;
        }

        if let Some(failure) = terminal_failure {
            record_manual_proxy_stream_error(&state, &plan).await;
            let terminal_telemetry = Some(build_terminal_stream_telemetry(
                stream_started_at,
                telemetry.as_ref(),
                usage_stream_telemetry.as_ref(),
                provider_stream_bytes,
            ));
            let report_context_for_payload = report_context_with_stage_trace(
                report_context,
                stage_trace,
                stream_started_at,
                terminal_telemetry.as_ref(),
            );
            let report_context_for_payload = report_context_with_request_diagnostics(
                report_context_for_payload,
                request_diagnostics.as_ref(),
                stream_started_at,
                terminal_telemetry.as_ref(),
            );
            submit_midstream_stream_failure(
                &state,
                &trace_id,
                &plan,
                direct_stream_finalize_kind.as_deref(),
                report_context_for_payload,
                headers,
                terminal_telemetry,
                &provider_buffered_body,
                candidate_started_unix_secs,
                failure,
            )
            .await;
            return;
        }

        if downstream_dropped {
            let terminal_telemetry = Some(build_terminal_stream_telemetry(
                stream_started_at,
                telemetry.as_ref(),
                usage_stream_telemetry.as_ref(),
                provider_stream_bytes,
            ));
            let report_context_for_payload = report_context_with_stage_trace(
                report_context,
                stage_trace,
                stream_started_at,
                terminal_telemetry.as_ref(),
            );
            let report_context_for_payload = report_context_with_request_diagnostics(
                report_context_for_payload,
                request_diagnostics.as_ref(),
                stream_started_at,
                terminal_telemetry.as_ref(),
            );
            let usage_payload = build_stream_usage_payload(
                trace_id,
                report_kind.unwrap_or_default(),
                report_context_for_payload,
                499,
                headers,
                &provider_buffered_body,
                provider_body_truncated,
                &buffered_body,
                client_body_truncated,
                stream_terminal_summary,
                terminal_telemetry,
            );
            record_stream_terminal_usage(
                &state,
                &plan,
                usage_payload.report_context.as_ref(),
                &usage_payload,
                true,
            )
            .await;
            record_local_request_candidate_status(
                &state,
                &plan,
                usage_payload.report_context.as_ref(),
                SchedulerRequestCandidateStatusUpdate {
                    status: RequestCandidateStatus::Cancelled,
                    status_code: Some(499),
                    error_type: Some("downstream_disconnect".to_string()),
                    error_message: Some("client disconnected before stream completion".to_string()),
                    latency_ms: usage_payload
                        .telemetry
                        .as_ref()
                        .and_then(|value| value.elapsed_ms),
                    started_at_unix_ms: Some(candidate_started_unix_secs),
                    finished_at_unix_ms: Some(current_request_candidate_unix_ms()),
                },
            )
            .await;
            return;
        }

        let mut stream_terminal_summary = stream_terminal_summary;
        maybe_apply_kiro_prompt_cache_usage_to_stream_summary(
            &state,
            &plan,
            report_context.as_ref(),
            &mut stream_terminal_summary,
        )
        .await;
        let requires_observed_terminal_event = stream_requires_observed_terminal_event(
            plan.provider_api_format.as_str(),
            stream_usage_report_context.as_ref(),
        );
        ensure_stream_terminal_summary_for_missing_observed_finish(
            &mut stream_terminal_summary,
            requires_observed_terminal_event,
        );
        let missing_observed_finish =
            stream_terminal_summary_missing_observed_finish_with_requirement(
                stream_terminal_summary.as_ref(),
                requires_observed_terminal_event,
            );
        let stream_failed = stream_terminal_summary_represents_failure_with_requirement(
            stream_terminal_summary.as_ref(),
            requires_observed_terminal_event,
        );
        let stream_terminal_error_message = stream_terminal_summary
            .as_ref()
            .and_then(|summary| summary.parser_error.clone())
            .or_else(|| {
                missing_observed_finish.then(|| {
                    "execution runtime stream ended before provider terminal event".to_string()
                })
            });
        let should_submit_report = report_kind.is_some();
        let terminal_telemetry = Some(build_terminal_stream_telemetry(
            stream_started_at,
            telemetry.as_ref(),
            usage_stream_telemetry.as_ref(),
            provider_stream_bytes,
        ));
        let report_context_for_payload = report_context_with_stage_trace(
            report_context,
            stage_trace,
            stream_started_at,
            terminal_telemetry.as_ref(),
        );
        let report_context_for_payload = report_context_with_request_diagnostics(
            report_context_for_payload,
            request_diagnostics.as_ref(),
            stream_started_at,
            terminal_telemetry.as_ref(),
        );
        let usage_payload = build_stream_usage_payload(
            trace_id.clone(),
            report_kind.unwrap_or_default(),
            report_context_for_payload,
            status_code,
            headers,
            &provider_buffered_body,
            provider_body_truncated,
            &buffered_body,
            client_body_truncated,
            stream_terminal_summary,
            terminal_telemetry,
        );
        if stream_failed {
            warn!(
                event_name = "direct_passthrough_stream_failed",
                log_type = "ops",
                trace_id = %trace_id,
                request_id = %request_id_for_log,
                candidate_id = ?candidate_id.as_deref(),
                status_code,
                error_message = stream_terminal_error_message.as_deref().unwrap_or_default(),
                "gateway direct passthrough stream ended with a failed terminal state"
            );
        } else {
            apply_local_execution_effect(
                &state,
                LocalExecutionEffectContext {
                    plan: &plan,
                    report_context: usage_payload.report_context.as_ref(),
                },
                LocalExecutionEffect::HealthSuccess(LocalHealthSuccessEffect),
            )
            .await;
            apply_local_execution_effect(
                &state,
                LocalExecutionEffectContext {
                    plan: &plan,
                    report_context: usage_payload.report_context.as_ref(),
                },
                LocalExecutionEffect::AdaptiveSuccess(LocalAdaptiveSuccessEffect),
            )
            .await;
            apply_local_execution_effect(
                &state,
                LocalExecutionEffectContext {
                    plan: &plan,
                    report_context: usage_payload.report_context.as_ref(),
                },
                LocalExecutionEffect::PoolSuccessStream {
                    payload: &usage_payload,
                },
            )
            .await;
        }
        record_stream_terminal_usage(
            &state,
            &plan,
            usage_payload.report_context.as_ref(),
            &usage_payload,
            false,
        )
        .await;
        record_local_request_candidate_status(
            &state,
            &plan,
            usage_payload.report_context.as_ref(),
            SchedulerRequestCandidateStatusUpdate {
                status: if stream_failed {
                    RequestCandidateStatus::Failed
                } else {
                    RequestCandidateStatus::Success
                },
                status_code: Some(status_code),
                error_type: if stream_failed {
                    if missing_observed_finish {
                        Some("stream_missing_terminal_event".to_string())
                    } else {
                        Some("stream_terminal_error".to_string())
                    }
                } else {
                    None
                },
                error_message: stream_failed
                    .then_some(stream_terminal_error_message)
                    .flatten(),
                latency_ms: usage_payload
                    .telemetry
                    .as_ref()
                    .and_then(|value| value.elapsed_ms),
                started_at_unix_ms: Some(candidate_started_unix_secs),
                finished_at_unix_ms: Some(current_request_candidate_unix_ms()),
            },
        )
        .await;

        if should_submit_report {
            if let Err(_err) = submit_stream_report(&state, usage_payload).await {
                warn!(
                    event_name = "execution_report_submit_failed",
                    log_type = "ops",
                    trace_id = %trace_id,
                    request_id = %request_id_for_log,
                    candidate_id = ?candidate_id.as_deref(),
                    report_scope = "direct_passthrough_stream",
                    error_category = "stream_report_submit_failed",
                    "gateway failed to submit direct passthrough stream execution report"
                );
            }
        }
    }
}

fn build_direct_passthrough_inline_body_stream(
    finalizer: DirectPassthroughFinalizer,
    prefetched_body: VecDeque<Result<Bytes, String>>,
    response: DirectUpstreamResponse,
    upstream_started_at: Instant,
    stream_first_byte_timeout: Option<Duration>,
) -> impl futures_util::Stream<Item = Result<Bytes, IoError>> + Send + 'static {
    let state = DirectPassthroughInlineBodyState::new(
        finalizer,
        prefetched_body,
        response,
        upstream_started_at,
        stream_first_byte_timeout,
    );
    futures_stream::unfold(state, |state| async move { state.next_item().await })
}

struct DirectPassthroughInlineBodyState {
    finalizer: Option<DirectPassthroughFinalizer>,
    upstream: Option<DirectUpstreamByteStream>,
    upstream_control_filter: Option<SseControlBlockFilter>,
    upstream_started_at: Instant,
    stream_first_byte_timeout: Option<Duration>,
    stream_idle_timeout: Option<Duration>,
    observed_first_body_poll: bool,
    observed_first_client_yield: bool,
    upstream_done: bool,
    control_filter_flushed: bool,
    terminal_error_sent: bool,
    finalized: bool,
}

impl DirectPassthroughInlineBodyState {
    fn new(
        finalizer: DirectPassthroughFinalizer,
        prefetched_body: VecDeque<Result<Bytes, String>>,
        response: DirectUpstreamResponse,
        upstream_started_at: Instant,
        stream_first_byte_timeout: Option<Duration>,
    ) -> Self {
        let stream_idle_timeout = resolve_stream_idle_timeout(&finalizer.core().plan);
        Self {
            finalizer: Some(finalizer),
            upstream: Some(direct_upstream_response_byte_stream(
                prefetched_body,
                response,
            )),
            upstream_control_filter: Some(SseControlBlockFilter::default()),
            upstream_started_at,
            stream_first_byte_timeout,
            stream_idle_timeout,
            observed_first_body_poll: false,
            observed_first_client_yield: false,
            upstream_done: false,
            control_filter_flushed: false,
            terminal_error_sent: false,
            finalized: false,
        }
    }

    async fn next_item(mut self) -> Option<(Result<Bytes, IoError>, Self)> {
        if self.finalized {
            return None;
        }
        if self
            .finalizer
            .as_ref()
            .is_some_and(DirectPassthroughFinalizer::completed_native_anthropic_stream)
        {
            self.upstream.take();
            self.finalized = true;
            drop(self.finalizer.take());
            return None;
        }
        if !self.observed_first_body_poll {
            self.observed_first_body_poll = true;
            if let Some(finalizer) = self.finalizer.as_mut() {
                finalizer.observe_first_body_poll();
            }
        }
        loop {
            if self.upstream_done
                || self
                    .finalizer
                    .as_ref()
                    .and_then(DirectPassthroughFinalizer::terminal_failure)
                    .is_some()
            {
                break;
            }

            let item = self.next_upstream_item().await;
            let Some(item) = item else {
                self.upstream_done = true;
                break;
            };
            let chunk = match item {
                Ok(chunk) => chunk,
                Err(message) => {
                    self.log_upstream_read_error_and_fail(message);
                    self.upstream_done = true;
                    break;
                }
            };
            if chunk.is_empty() {
                continue;
            }

            let observed_at = Instant::now();
            let Some(chunk) = self
                .finalizer
                .as_mut()
                .and_then(|finalizer| finalizer.prepare_upstream_chunk(chunk))
            else {
                continue;
            };
            let provider_error_detected = if let Some(finalizer) = self.finalizer.as_mut() {
                finalizer.observe_upstream_chunk(&chunk, observed_at);
                finalizer.terminal_failure().is_some()
            } else {
                false
            };
            if let Some(client_chunk) =
                filter_upstream_sse_control_chunk(&mut self.upstream_control_filter, chunk)
            {
                self.prepare_client_chunk_yield(&client_chunk);
                self.terminal_error_sent |= provider_error_detected;
                if self
                    .finalizer
                    .as_ref()
                    .is_some_and(DirectPassthroughFinalizer::completed_native_anthropic_stream)
                {
                    self.upstream.take();
                    self.upstream_done = true;
                }
                return Some((Ok(client_chunk), self));
            }
        }

        if let Some(finalizer) = self.finalizer.as_mut() {
            finalizer.fail_if_anthropic_message_stop_missing();
        }

        if !self.control_filter_flushed
            && self
                .finalizer
                .as_ref()
                .and_then(DirectPassthroughFinalizer::terminal_failure)
                .is_none()
        {
            self.control_filter_flushed = true;
            if let Some(client_chunk) =
                flush_upstream_sse_control_filter(&mut self.upstream_control_filter)
            {
                self.prepare_client_chunk_yield(&client_chunk);
                return Some((Ok(client_chunk), self));
            }
        }

        if !self.terminal_error_sent {
            if let Some(finalizer) = self.finalizer.as_mut() {
                if let Some(failure) = finalizer.terminal_failure() {
                    self.terminal_error_sent = true;
                    match encode_terminal_sse_error_event_for_plan(&finalizer.core().plan, failure)
                    {
                        Ok(error_event) => {
                            self.prepare_client_chunk_yield(&error_event);
                            return Some((Ok(error_event), self));
                        }
                        Err(err) => finalizer.log_terminal_error_event_encode_failed(err),
                    }
                }
            }
        }

        self.finalize(false).await;
        None
    }

    async fn next_upstream_item(&mut self) -> Option<Result<Bytes, String>> {
        let needs_first_byte_timeout = self
            .finalizer
            .as_ref()
            .is_some_and(|finalizer| !finalizer.ttfb_observed());
        let upstream = self.upstream.as_mut()?;
        if needs_first_byte_timeout {
            match await_direct_passthrough_first_item(
                upstream.next(),
                self.upstream_started_at,
                self.stream_first_byte_timeout,
            )
            .await
            {
                Ok(item) => item,
                Err(timeout) => {
                    if let Some(finalizer) = self.finalizer.as_mut() {
                        finalizer.set_terminal_failure(build_stream_transport_failure_report(
                            "first_byte_timeout",
                            stream_first_byte_timeout_message(timeout),
                            504,
                        ));
                    }
                    None
                }
            }
        } else {
            match await_stream_idle_read(upstream.next(), self.stream_idle_timeout).await {
                Ok(item) => item,
                Err(timeout) => {
                    self.upstream.take();
                    if let Some(finalizer) = self.finalizer.as_mut() {
                        if finalizer.terminal_failure().is_none()
                            && !finalizer
                                .core()
                                .client_stream_completion_tracker
                                .successful_completion()
                        {
                            finalizer.set_terminal_failure(build_stream_transport_failure_report(
                                "read_timeout",
                                stream_idle_timeout_message(timeout),
                                504,
                            ));
                        }
                        finalizer.core_mut()._provider_pool_in_flight_guard.take();
                    }
                    None
                }
            }
        }
    }

    fn prepare_client_chunk_yield(&mut self, chunk: &Bytes) {
        let Some(finalizer) = self.finalizer.as_mut() else {
            return;
        };
        finalizer.observe_client_chunk(chunk);
        if !self.observed_first_client_yield {
            self.observed_first_client_yield = true;
            finalizer.release_upstream_target_permit_after_first_yield();
            finalizer.record_client_visible_stream_started_if_needed();
            finalizer.observe_first_client_yield();
        }
    }

    fn log_upstream_read_error_and_fail(&mut self, message: String) {
        let Some(finalizer) = self.finalizer.as_mut() else {
            return;
        };
        if finalizer.completed_native_anthropic_stream() {
            let core = finalizer.core();
            debug!(
                event_name = "direct_passthrough_read_error_ignored_after_anthropic_stop",
                log_type = "debug",
                trace_id = %core.trace_id,
                request_id = %core.request_id_for_log,
                candidate_id = ?core.candidate_id.as_deref(),
                error_category = "upstream_body_read_failed",
                "gateway ignored direct passthrough teardown error after Anthropic message_stop"
            );
            return;
        }
        let core = finalizer.core();
        warn!(
            event_name = "direct_passthrough_body_read_error",
            log_type = "ops",
            trace_id = %core.trace_id,
            request_id = %core.request_id_for_log,
            candidate_id = ?core.candidate_id.as_deref(),
            upstream_bytes = core.provider_stream_bytes,
            error_category = "upstream_body_read_failed",
            "gateway direct passthrough upstream body read failed"
        );
        finalizer.set_terminal_failure(build_stream_transport_failure_report(
            "execution_runtime_stream_read_error",
            message,
            502,
        ));
    }

    async fn finalize(&mut self, downstream_dropped: bool) {
        if self.finalized {
            return;
        }
        self.finalized = true;
        self.upstream.take();
        if let Some(finalizer) = self.finalizer.as_mut() {
            finalizer.finalize(downstream_dropped).await;
        }
        self.finalizer.take();
    }
}

impl Drop for DirectPassthroughInlineBodyState {
    fn drop(&mut self) {
        // `finalized` only prevents another poll from entering finalization;
        // the finalizer may still be waiting for its handoff task to finish.
        // Keep the fallback armed while the finalizer is present.
        if self.finalizer.is_none() {
            return;
        }
        self.upstream.take();
        if let Some(finalizer) = self.finalizer.take() {
            observe_gateway_stage_ms("stream_finalizer_enqueue", 0);
            if let Ok(handle) = tokio::runtime::Handle::try_current() {
                let usage_producer = finalizer
                    .core
                    .as_ref()
                    .map(|core| core.state.usage_runtime.track_producer());
                handle.spawn(async move {
                    let _usage_producer = usage_producer;
                    let mut finalizer = finalizer;
                    finalizer.finalize(true).await;
                });
            }
        }
    }
}

async fn record_stream_pending_lifecycle(
    state: &AppState,
    lifecycle_seed: &LifecycleUsageSeed,
    stage_trace: &mut RequestStageTrace,
) {
    let usage_pending_started_at = Instant::now();
    let usage_data = state.usage_lifecycle_data_state().as_ref().clone();
    state
        .usage_runtime
        .record_pending_direct(&usage_data, lifecycle_seed.clone())
        .await;
    observe_gateway_stage_trace_ms(
        stage_trace,
        "stream_usage_pending",
        usage_pending_started_at.elapsed().as_millis() as u64,
    );
}

fn should_defer_stream_pending_for_direct_inline(
    _state: &AppState,
    plan: &ExecutionPlan,
    plan_kind: &str,
    report_context: Option<&Value>,
) -> bool {
    if direct_passthrough_mode() != DirectPassthroughMode::Inline {
        return false;
    }
    #[cfg(test)]
    if _state
        .execution_runtime_override_base_url()
        .is_some_and(|value| !value.trim().is_empty())
    {
        return false;
    }
    if plan_kind == OPENAI_IMAGE_STREAM_PLAN_KIND {
        return false;
    }
    if is_openai_responses_family_format(plan.provider_api_format.as_str())
        || is_openai_responses_family_format(plan.client_api_format.as_str())
    {
        return false;
    }
    if !plan
        .provider_api_format
        .eq_ignore_ascii_case(plan.client_api_format.as_str())
    {
        return false;
    }
    if client_format_allows_proxy_generated_sse_control_blocks(plan) {
        return false;
    }
    if maybe_build_provider_private_stream_normalizer(report_context).is_some() {
        return false;
    }
    let normalized_stream_report_context =
        normalize_provider_private_report_context(report_context);
    if maybe_build_stream_response_rewriter(normalized_stream_report_context.as_ref()).is_some() {
        return false;
    }
    true
}

#[allow(clippy::too_many_arguments)]
async fn execute_stream_from_direct_passthrough(
    state: &AppState,
    plan: ExecutionPlan,
    trace_id: &str,
    decision: &GatewayControlDecision,
    plan_kind: &str,
    report_kind: Option<String>,
    report_context: Option<serde_json::Value>,
    candidate_started_unix_secs: u64,
    stream_started_at: Instant,
    mut stage_trace: RequestStageTrace,
    execution: DirectUpstreamStreamExecution,
    in_flight_guard: Option<ProviderPoolInFlightGuard>,
    pending_recorded: bool,
) -> Result<Option<Response<Body>>, GatewayError> {
    let DirectUpstreamStreamExecution {
        status_code,
        mut headers,
        upstream_content_length: _,
        provider_api_format: _,
        stream_summary_report_context: _,
        prefetched_body,
        stream_precommit_committed: _,
        response,
        started_at: upstream_started_at,
        response_observation,
        stream_first_byte_timeout,
        stream_idle_timeout,
        upstream_target_permit,
    } = execution;

    let requires_anthropic_message_stop = status_code == 200
        && response_headers_indicate_sse(&headers)
        && plan
            .provider_api_format
            .eq_ignore_ascii_case("claude:messages")
        && plan
            .client_api_format
            .eq_ignore_ascii_case("claude:messages");

    let request_id = plan.request_id.clone();
    let candidate_id = plan.candidate_id.clone();
    let request_id_for_log = short_request_id(request_id.as_str());
    let mut report_context = attach_provider_response_headers_to_report_context(
        report_context,
        &headers,
        response_observation.request_started_at_unix_ms,
        response_observation.response_headers_observed_at_unix_ms,
        &response_observation.request_order_id,
    );
    spawn_local_oauth_success_effect(
        state.clone(),
        &plan,
        report_context.as_ref(),
        LocalOAuthSuccessEffect {
            status_code,
            request_started_at_unix_ms: Some(response_observation.request_started_at_unix_ms),
            request_order_id: Some(&response_observation.request_order_id),
        },
    );
    if status_code == 200 {
        seed_kiro_simulated_cache_enabled(state, &plan, &mut report_context).await;
        if kiro_simulated_cache_enabled_from_report_context(report_context.as_ref()) {
            seed_kiro_report_context_input_tokens(&plan, &mut report_context);
        }
        seed_kiro_report_context_prompt_cache_usage(state, &plan, &mut report_context).await;
    }

    let lifecycle_seed = build_lifecycle_usage_seed(&plan, report_context.as_ref());
    let max_stream_body_buffer_bytes = resolve_stream_body_buffer_limit(state).await;
    let request_candidate_status_snapshot =
        snapshot_local_request_candidate_status(&plan, report_context.as_ref());
    let passthrough_mode = direct_passthrough_mode();
    if passthrough_mode == DirectPassthroughMode::Legacy {
        state.usage_runtime.record_stream_started(
            state.usage_lifecycle_data_state().as_ref(),
            &lifecycle_seed,
            status_code,
            None,
        );
        if let Some(snapshot) = request_candidate_status_snapshot.as_ref() {
            record_local_request_candidate_status_snapshot(
                state,
                snapshot,
                SchedulerRequestCandidateStatusUpdate {
                    status: RequestCandidateStatus::Streaming,
                    status_code: Some(status_code),
                    error_type: None,
                    error_message: None,
                    latency_ms: None,
                    started_at_unix_ms: Some(candidate_started_unix_secs),
                    finished_at_unix_ms: None,
                },
            )
            .await;
        }
    }

    let response_header_rules_started_at = Instant::now();
    apply_endpoint_response_header_rules(state, &plan, &mut headers, None).await?;
    observe_gateway_stage_ms(
        "stream_response_header_rules",
        response_header_rules_started_at.elapsed().as_millis() as u64,
    );
    let headers_for_report = headers.clone();
    headers.insert(CONTROL_REQUEST_ID_HEADER.to_string(), request_id.clone());
    if let Some(candidate_id) = candidate_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        headers.insert(
            CONTROL_CANDIDATE_ID_HEADER.to_string(),
            candidate_id.to_string(),
        );
    }
    headers.remove("content-length");

    if passthrough_mode == DirectPassthroughMode::Inline {
        let direct_stream_finalize_kind =
            resolve_core_stream_direct_finalize_report_kind(plan_kind);
        let normalized_stream_report_context =
            normalize_provider_private_report_context(report_context.as_ref());
        let stream_usage_report_context = normalized_stream_report_context.clone().or_else(|| {
            Some(serde_json::json!({
                "provider_api_format": plan.provider_api_format.as_str(),
                "client_api_format": plan.client_api_format.as_str(),
            }))
        });
        let stream_usage_observer = stream_usage_report_context
            .as_ref()
            .map(|_| StreamingStandardTerminalObserver::default());
        observe_gateway_stage_trace_ms(
            &mut stage_trace,
            "stream_response_ready",
            stream_elapsed_ms_since(stream_started_at),
        );
        let request_diagnostics = current_request_diagnostics();
        observe_request_accepted_stage_trace_ms(
            &mut stage_trace,
            request_diagnostics.as_ref(),
            "frontdoor_to_stream_response_ready",
        );
        let finalizer = DirectPassthroughFinalizer::new(DirectPassthroughFinalizerCore {
            state: state.clone(),
            plan,
            trace_id: trace_id.to_string(),
            report_kind,
            report_context,
            lifecycle_seed,
            direct_stream_finalize_kind,
            stream_started_at,
            stage_trace,
            request_diagnostics,
            request_id_for_log,
            candidate_id,
            request_candidate_status_snapshot,
            deferred_request_candidate_status_record: None,
            candidate_started_unix_secs,
            status_code,
            headers: headers_for_report,
            stream_usage_report_context,
            stream_usage_observer,
            stream_usage_observer_buffered: StreamUsageObservationBuffer::new(
                max_stream_body_buffer_bytes,
            ),
            provider_error_inspection: ProviderStreamErrorInspection::default(),
            max_stream_body_buffer_bytes,
            provider_buffered_body: StreamBodyCapture::default(),
            buffered_body: StreamBodyCapture::default(),
            provider_body_truncated: false,
            client_body_truncated: false,
            client_stream_completion_tracker: ClientVisibleStreamCompletionTracker::default(),
            requires_anthropic_message_stop,
            client_visible_stream_completed: false,
            usage_stream_telemetry: None,
            telemetry: None,
            provider_stream_bytes: 0,
            client_stream_bytes: 0,
            last_client_chunk_elapsed_ms: 0,
            pending_recorded,
            stream_started_recorded: false,
            terminal_failure: None,
            _provider_pool_in_flight_guard: in_flight_guard,
            _upstream_target_permit: upstream_target_permit,
        });
        let body_stream = build_direct_passthrough_inline_body_stream(
            finalizer,
            prefetched_body,
            response,
            upstream_started_at,
            stream_first_byte_timeout,
        );
        return Ok(Some(build_client_response_from_parts(
            status_code,
            &headers,
            Body::from_stream(body_stream),
            trace_id,
            Some(decision),
        )?));
    }

    let (tx, rx) = mpsc::channel::<Result<Bytes, IoError>>(direct_passthrough_channel_capacity());
    let state_for_report = state.clone();
    let plan_for_report = plan;
    let trace_id_owned = trace_id.to_string();
    let report_kind_owned = report_kind;
    let report_context_owned = report_context;
    let lifecycle_seed_for_report = lifecycle_seed;
    let direct_stream_finalize_kind_owned =
        resolve_core_stream_direct_finalize_report_kind(plan_kind);
    let normalized_stream_report_context_owned =
        normalize_provider_private_report_context(report_context_owned.as_ref());
    let stream_started_at_for_report = stream_started_at;
    observe_gateway_stage_trace_ms(
        &mut stage_trace,
        "stream_response_ready",
        stream_elapsed_ms_since(stream_started_at),
    );
    let request_diagnostics_for_report = current_request_diagnostics();
    observe_request_accepted_stage_trace_ms(
        &mut stage_trace,
        request_diagnostics_for_report.as_ref(),
        "frontdoor_to_stream_response_ready",
    );
    let stage_trace_for_report = stage_trace;
    let _request_id_for_report = request_id.clone();
    let request_id_for_report_log = request_id_for_log.clone();
    let candidate_id_for_report = candidate_id.clone();
    let provider_pool_in_flight_guard_for_report = in_flight_guard;
    record_stream_pre_first_byte_spawn();
    let usage_producer = state_for_report.usage_runtime.track_producer();
    tokio::spawn(async move {
        let _usage_producer = usage_producer;
        let mut stage_trace_for_report = stage_trace_for_report;
        let _stream_total_guard =
            StageElapsedGuard::from_started_at("stream_total", stream_started_at_for_report);
        let _provider_pool_in_flight_guard = provider_pool_in_flight_guard_for_report;
        let _upstream_target_permit = upstream_target_permit;
        let stream_usage_report_context =
            normalized_stream_report_context_owned.clone().or_else(|| {
                Some(serde_json::json!({
                    "provider_api_format": plan_for_report.provider_api_format.as_str(),
                    "client_api_format": plan_for_report.client_api_format.as_str(),
                }))
            });
        let mut stream_usage_observer = stream_usage_report_context
            .as_ref()
            .map(|_| StreamingStandardTerminalObserver::default());
        let mut stream_usage_observer_buffered =
            StreamUsageObservationBuffer::new(max_stream_body_buffer_bytes);
        let mut provider_error_inspection = ProviderStreamErrorInspection::default();
        let mut provider_buffered_body = StreamBodyCapture::default();
        let mut buffered_body = StreamBodyCapture::default();
        let mut provider_body_truncated = false;
        let mut client_body_truncated = false;
        let mut upstream_control_filter = Some(SseControlBlockFilter::default());
        let mut client_stream_completion_tracker = ClientVisibleStreamCompletionTracker::default();
        let requires_anthropic_message_stop = requires_anthropic_message_stop;
        let mut client_visible_stream_completed = false;
        let mut usage_stream_telemetry: Option<ExecutionTelemetry> = None;
        let telemetry: Option<ExecutionTelemetry> = None;
        let mut provider_stream_bytes = 0u64;
        let mut client_stream_bytes = 0u64;
        let mut last_client_chunk_elapsed_ms = 0u64;
        let mut downstream_dropped = false;
        let mut terminal_failure: Option<StreamFailureReport> = None;
        let mut provider_error_forwarded_to_client = false;
        let mut upstream = direct_upstream_response_byte_stream(prefetched_body, response);
        let mut observed_first_upstream_body = false;
        let mut observed_first_client_send = false;

        loop {
            if downstream_dropped {
                break;
            }
            let item = if usage_stream_telemetry
                .as_ref()
                .and_then(|telemetry| telemetry.ttfb_ms)
                .is_none()
            {
                tokio::select! {
                    biased;
                    _ = tx.closed(), if !downstream_dropped => {
                        downstream_dropped = true;
                        break;
                    }
                    result = await_direct_passthrough_first_item(
                        upstream.next(),
                        upstream_started_at,
                        stream_first_byte_timeout,
                    ) => {
                        match result {
                            Ok(item) => item,
                            Err(timeout) => {
                                terminal_failure = Some(build_stream_transport_failure_report(
                                    "first_byte_timeout",
                                    stream_first_byte_timeout_message(timeout),
                                    504,
                                ));
                                break;
                            }
                        }
                    }
                }
            } else {
                tokio::select! {
                    biased;
                    _ = tx.closed(), if !downstream_dropped => {
                        downstream_dropped = true;
                        break;
                    }
                    result = await_stream_idle_read(upstream.next(), stream_idle_timeout) => {
                        match result {
                            Ok(item) => item,
                            Err(timeout) => {
                                if terminal_failure.is_none()
                                    && !client_stream_completion_tracker.successful_completion() {
                                    terminal_failure = Some(build_stream_transport_failure_report(
                                        "read_timeout", stream_idle_timeout_message(timeout), 504,
                                    ));
                                }
                                break;
                            }
                        }
                    },
                }
            };

            let Some(item) = item else {
                break;
            };
            let chunk = match item {
                Ok(chunk) => chunk,
                Err(message) => {
                    if requires_anthropic_message_stop && client_visible_stream_completed {
                        debug!(
                            event_name = "direct_passthrough_read_error_ignored_after_anthropic_stop",
                            log_type = "debug",
                            trace_id = %trace_id_owned,
                            request_id = %request_id_for_report_log,
                            candidate_id = ?candidate_id_for_report.as_deref(),
                            error_category = "upstream_body_read_failed",
                            "gateway ignored direct passthrough teardown error after Anthropic message_stop"
                        );
                        break;
                    }
                    warn!(
                        event_name = "direct_passthrough_body_read_error",
                        log_type = "ops",
                        trace_id = %trace_id_owned,
                        request_id = %request_id_for_report_log,
                        candidate_id = ?candidate_id_for_report.as_deref(),
                        upstream_bytes = provider_stream_bytes,
                        error_category = "upstream_body_read_failed",
                        "gateway direct passthrough upstream body read failed"
                    );
                    terminal_failure = Some(build_stream_transport_failure_report(
                        "execution_runtime_stream_read_error",
                        message,
                        502,
                    ));
                    break;
                }
            };
            if chunk.is_empty() {
                continue;
            }
            if requires_anthropic_message_stop && client_visible_stream_completed {
                continue;
            }

            let mut provider_chunk = chunk;
            if requires_anthropic_message_stop {
                if let Some(terminal_end) = client_stream_completion_tracker
                    .observe_anthropic_message_stop_terminal_end(provider_chunk.as_ref())
                {
                    provider_chunk.truncate(terminal_end);
                    client_visible_stream_completed = true;
                }
            }

            let observed_at = Instant::now();
            if !observed_first_upstream_body {
                observed_first_upstream_body = true;
                observe_gateway_stage_trace_ms(
                    &mut stage_trace_for_report,
                    "direct_passthrough_upstream_body_first",
                    stream_elapsed_ms_at(stream_started_at_for_report, observed_at),
                );
            }
            maybe_record_first_stream_event_started(
                &state_for_report,
                &lifecycle_seed_for_report,
                status_code,
                stream_started_at_for_report,
                observed_at,
                telemetry.as_ref(),
                &mut usage_stream_telemetry,
            );
            if usage_stream_telemetry
                .as_ref()
                .and_then(|telemetry| telemetry.ttfb_ms)
                .is_some()
                && provider_stream_bytes == 0
            {
                observe_gateway_stage_trace_ms(
                    &mut stage_trace_for_report,
                    "stream_first_data",
                    stream_elapsed_ms_at(stream_started_at_for_report, observed_at),
                );
            }

            let mut sent_client_chunk = false;
            if let Some(client_chunk) = filter_upstream_sse_control_chunk(
                &mut upstream_control_filter,
                provider_chunk.clone(),
            ) {
                sent_client_chunk = forward_direct_passthrough_client_chunk(
                    &tx,
                    client_chunk,
                    &mut downstream_dropped,
                    &mut client_visible_stream_completed,
                    &mut client_stream_completion_tracker,
                    !requires_anthropic_message_stop,
                    &mut client_stream_bytes,
                    &mut buffered_body,
                    &mut client_body_truncated,
                    max_stream_body_buffer_bytes,
                    stream_started_at_for_report,
                    &mut last_client_chunk_elapsed_ms,
                    !observed_first_client_send,
                    trace_id_owned.as_str(),
                    request_id_for_report_log.as_str(),
                    candidate_id_for_report.as_deref(),
                )
                .await;
                if sent_client_chunk && !observed_first_client_send {
                    observed_first_client_send = true;
                    observe_gateway_stage_trace_ms(
                        &mut stage_trace_for_report,
                        "direct_passthrough_first_client_send",
                        stream_elapsed_ms_since(stream_started_at_for_report),
                    );
                }
            }

            provider_stream_bytes = provider_stream_bytes
                .saturating_add(u64::try_from(provider_chunk.len()).unwrap_or(u64::MAX));
            append_budgeted_stream_capture_bytes(
                &mut provider_buffered_body,
                provider_chunk.as_ref(),
                max_stream_body_buffer_bytes,
                &mut provider_body_truncated,
            );
            if let (Some(observer), Some(report_context)) = (
                stream_usage_observer.as_mut(),
                stream_usage_report_context.as_ref(),
            ) {
                observe_stream_usage_bytes(
                    observer,
                    report_context,
                    &mut stream_usage_observer_buffered,
                    provider_chunk.as_ref(),
                );
            }
            let provider_private_error_body_json = provider_error_inspection.observe(
                stream_usage_report_context.as_ref(),
                provider_chunk.as_ref(),
            );
            if let Some(error_body_json) = provider_private_error_body_json {
                provider_error_forwarded_to_client = sent_client_chunk;
                let error_status_code = resolve_provider_stream_error_status_code(
                    plan_for_report.provider_api_format.as_str(),
                    status_code,
                    &error_body_json,
                );
                terminal_failure = Some(build_stream_failure_from_provider_error_body(
                    error_status_code,
                    &error_body_json,
                ));
                break;
            }
            if requires_anthropic_message_stop && client_visible_stream_completed {
                break;
            }
        }
        drop(upstream);
        drop(_provider_pool_in_flight_guard);
        drop(_upstream_target_permit);

        if terminal_failure.is_none()
            && !downstream_dropped
            && requires_anthropic_message_stop
            && !client_visible_stream_completed
        {
            terminal_failure = Some(build_anthropic_premature_eof_failure(
                "upstream Anthropic stream ended before message_stop",
            ));
        }

        if terminal_failure.is_none() {
            if let Some(client_chunk) =
                flush_upstream_sse_control_filter(&mut upstream_control_filter)
            {
                let _ = forward_direct_passthrough_client_chunk(
                    &tx,
                    client_chunk,
                    &mut downstream_dropped,
                    &mut client_visible_stream_completed,
                    &mut client_stream_completion_tracker,
                    !requires_anthropic_message_stop,
                    &mut client_stream_bytes,
                    &mut buffered_body,
                    &mut client_body_truncated,
                    max_stream_body_buffer_bytes,
                    stream_started_at_for_report,
                    &mut last_client_chunk_elapsed_ms,
                    !observed_first_client_send,
                    trace_id_owned.as_str(),
                    request_id_for_report_log.as_str(),
                    candidate_id_for_report.as_deref(),
                )
                .await;
            }
        }

        if let Some(failure) = terminal_failure
            .as_ref()
            .filter(|_| !downstream_dropped && !provider_error_forwarded_to_client)
        {
            match encode_terminal_sse_error_event_for_plan(&plan_for_report, failure) {
                Ok(error_event) => {
                    let _ = forward_direct_passthrough_client_chunk(
                        &tx,
                        error_event,
                        &mut downstream_dropped,
                        &mut client_visible_stream_completed,
                        &mut client_stream_completion_tracker,
                        true,
                        &mut client_stream_bytes,
                        &mut buffered_body,
                        &mut client_body_truncated,
                        max_stream_body_buffer_bytes,
                        stream_started_at_for_report,
                        &mut last_client_chunk_elapsed_ms,
                        !observed_first_client_send,
                        trace_id_owned.as_str(),
                        request_id_for_report_log.as_str(),
                        candidate_id_for_report.as_deref(),
                    )
                    .await;
                }
                Err(_err) => {
                    warn!(
                        event_name = "direct_passthrough_terminal_error_event_encode_failed",
                        log_type = "ops",
                        trace_id = %trace_id_owned,
                        request_id = %request_id_for_report_log,
                        candidate_id = ?candidate_id_for_report.as_deref(),
                        error_category = "terminal_error_event_encode_failed",
                        "gateway direct passthrough failed to encode terminal SSE error event"
                    );
                }
            }
        }
        drop(tx);

        let mut stream_terminal_summary = finalize_stream_usage_observer(
            &mut stream_usage_observer,
            stream_usage_report_context.as_ref(),
            &mut stream_usage_observer_buffered,
        );

        if downstream_dropped && client_visible_stream_completed && terminal_failure.is_none() {
            debug!(
                event_name = "direct_passthrough_downstream_closed_after_done",
                log_type = "debug",
                trace_id = %trace_id_owned,
                request_id = %request_id_for_report_log,
                candidate_id = ?candidate_id_for_report.as_deref(),
                "gateway treats direct passthrough downstream close after terminal SSE event as completed"
            );
            downstream_dropped = false;
        }

        if downstream_dropped {
            let terminal_telemetry = Some(build_terminal_stream_telemetry(
                stream_started_at_for_report,
                telemetry.as_ref(),
                usage_stream_telemetry.as_ref(),
                provider_stream_bytes,
            ));
            let report_context_for_payload = report_context_with_stage_trace(
                report_context_owned,
                stage_trace_for_report,
                stream_started_at_for_report,
                terminal_telemetry.as_ref(),
            );
            let report_context_for_payload = report_context_with_request_diagnostics(
                report_context_for_payload,
                request_diagnostics_for_report.as_ref(),
                stream_started_at_for_report,
                terminal_telemetry.as_ref(),
            );
            let usage_payload = build_stream_usage_payload(
                trace_id_owned,
                report_kind_owned.unwrap_or_default(),
                report_context_for_payload,
                499,
                headers_for_report,
                &provider_buffered_body,
                provider_body_truncated,
                &buffered_body,
                client_body_truncated,
                stream_terminal_summary,
                terminal_telemetry,
            );
            record_stream_terminal_usage(
                &state_for_report,
                &plan_for_report,
                usage_payload.report_context.as_ref(),
                &usage_payload,
                true,
            )
            .await;
            record_local_request_candidate_status(
                &state_for_report,
                &plan_for_report,
                usage_payload.report_context.as_ref(),
                SchedulerRequestCandidateStatusUpdate {
                    status: RequestCandidateStatus::Cancelled,
                    status_code: Some(499),
                    error_type: Some("downstream_disconnect".to_string()),
                    error_message: Some("client disconnected before stream completion".to_string()),
                    latency_ms: usage_payload
                        .telemetry
                        .as_ref()
                        .and_then(|value| value.elapsed_ms),
                    started_at_unix_ms: Some(candidate_started_unix_secs),
                    finished_at_unix_ms: Some(current_request_candidate_unix_ms()),
                },
            )
            .await;
            return;
        }

        if let Some(failure) = terminal_failure {
            record_manual_proxy_stream_error(&state_for_report, &plan_for_report).await;
            let terminal_telemetry = Some(build_terminal_stream_telemetry(
                stream_started_at_for_report,
                telemetry.as_ref(),
                usage_stream_telemetry.as_ref(),
                provider_stream_bytes,
            ));
            let report_context_for_payload = report_context_with_stage_trace(
                report_context_owned,
                stage_trace_for_report,
                stream_started_at_for_report,
                terminal_telemetry.as_ref(),
            );
            let report_context_for_payload = report_context_with_request_diagnostics(
                report_context_for_payload,
                request_diagnostics_for_report.as_ref(),
                stream_started_at_for_report,
                terminal_telemetry.as_ref(),
            );
            submit_midstream_stream_failure(
                &state_for_report,
                &trace_id_owned,
                &plan_for_report,
                direct_stream_finalize_kind_owned.as_deref(),
                report_context_for_payload,
                headers_for_report,
                terminal_telemetry,
                &provider_buffered_body,
                candidate_started_unix_secs,
                failure,
            )
            .await;
            return;
        }

        maybe_apply_kiro_prompt_cache_usage_to_stream_summary(
            &state_for_report,
            &plan_for_report,
            report_context_owned.as_ref(),
            &mut stream_terminal_summary,
        )
        .await;
        let requires_observed_terminal_event = stream_requires_observed_terminal_event(
            plan_for_report.provider_api_format.as_str(),
            stream_usage_report_context.as_ref(),
        );
        ensure_stream_terminal_summary_for_missing_observed_finish(
            &mut stream_terminal_summary,
            requires_observed_terminal_event,
        );
        let missing_observed_finish =
            stream_terminal_summary_missing_observed_finish_with_requirement(
                stream_terminal_summary.as_ref(),
                requires_observed_terminal_event,
            );
        let stream_failed = stream_terminal_summary_represents_failure_with_requirement(
            stream_terminal_summary.as_ref(),
            requires_observed_terminal_event,
        );
        let stream_terminal_error_message = stream_terminal_summary
            .as_ref()
            .and_then(|summary| summary.parser_error.clone())
            .or_else(|| {
                missing_observed_finish.then(|| {
                    "execution runtime stream ended before provider terminal event".to_string()
                })
            });
        let should_submit_report = report_kind_owned.is_some();
        let terminal_telemetry = Some(build_terminal_stream_telemetry(
            stream_started_at_for_report,
            telemetry.as_ref(),
            usage_stream_telemetry.as_ref(),
            provider_stream_bytes,
        ));
        let report_context_for_payload = report_context_with_stage_trace(
            report_context_owned,
            stage_trace_for_report,
            stream_started_at_for_report,
            terminal_telemetry.as_ref(),
        );
        let report_context_for_payload = report_context_with_request_diagnostics(
            report_context_for_payload,
            request_diagnostics_for_report.as_ref(),
            stream_started_at_for_report,
            terminal_telemetry.as_ref(),
        );
        let usage_payload = build_stream_usage_payload(
            trace_id_owned.clone(),
            report_kind_owned.unwrap_or_default(),
            report_context_for_payload,
            status_code,
            headers_for_report,
            &provider_buffered_body,
            provider_body_truncated,
            &buffered_body,
            client_body_truncated,
            stream_terminal_summary,
            terminal_telemetry,
        );
        if stream_failed {
            warn!(
                event_name = "direct_passthrough_stream_failed",
                log_type = "ops",
                trace_id = %trace_id_owned,
                request_id = %request_id_for_report_log,
                candidate_id = ?candidate_id_for_report.as_deref(),
                status_code,
                error_message = stream_terminal_error_message.as_deref().unwrap_or_default(),
                "gateway direct passthrough stream ended with a failed terminal state"
            );
        } else {
            apply_local_execution_effect(
                &state_for_report,
                LocalExecutionEffectContext {
                    plan: &plan_for_report,
                    report_context: usage_payload.report_context.as_ref(),
                },
                LocalExecutionEffect::HealthSuccess(LocalHealthSuccessEffect),
            )
            .await;
            apply_local_execution_effect(
                &state_for_report,
                LocalExecutionEffectContext {
                    plan: &plan_for_report,
                    report_context: usage_payload.report_context.as_ref(),
                },
                LocalExecutionEffect::AdaptiveSuccess(LocalAdaptiveSuccessEffect),
            )
            .await;
            apply_local_execution_effect(
                &state_for_report,
                LocalExecutionEffectContext {
                    plan: &plan_for_report,
                    report_context: usage_payload.report_context.as_ref(),
                },
                LocalExecutionEffect::PoolSuccessStream {
                    payload: &usage_payload,
                },
            )
            .await;
        }
        record_stream_terminal_usage(
            &state_for_report,
            &plan_for_report,
            usage_payload.report_context.as_ref(),
            &usage_payload,
            false,
        )
        .await;
        record_local_request_candidate_status(
            &state_for_report,
            &plan_for_report,
            usage_payload.report_context.as_ref(),
            SchedulerRequestCandidateStatusUpdate {
                status: if stream_failed {
                    RequestCandidateStatus::Failed
                } else {
                    RequestCandidateStatus::Success
                },
                status_code: Some(status_code),
                error_type: if stream_failed {
                    if missing_observed_finish {
                        Some("stream_missing_terminal_event".to_string())
                    } else {
                        Some("stream_terminal_error".to_string())
                    }
                } else {
                    None
                },
                error_message: stream_failed
                    .then_some(stream_terminal_error_message)
                    .flatten(),
                latency_ms: usage_payload
                    .telemetry
                    .as_ref()
                    .and_then(|value| value.elapsed_ms),
                started_at_unix_ms: Some(candidate_started_unix_secs),
                finished_at_unix_ms: Some(current_request_candidate_unix_ms()),
            },
        )
        .await;

        if should_submit_report {
            if let Err(_err) = submit_stream_report(&state_for_report, usage_payload).await {
                warn!(
                    event_name = "execution_report_submit_failed",
                    log_type = "ops",
                    trace_id = %trace_id_owned,
                    request_id = %request_id_for_report_log,
                    candidate_id = ?candidate_id_for_report.as_deref(),
                    report_scope = "direct_passthrough_stream",
                    error_category = "stream_report_submit_failed",
                    "gateway failed to submit direct passthrough stream execution report"
                );
            }
        }
    });

    let body_stream = build_sse_body_stream(
        Vec::new(),
        rx,
        false,
        false,
        requires_anthropic_message_stop,
        SSE_KEEPALIVE_INTERVAL,
    );
    Ok(Some(build_client_response_from_parts(
        status_code,
        &headers,
        Body::from_stream(body_stream),
        trace_id,
        Some(decision),
    )?))
}

#[allow(clippy::too_many_arguments)] // internal function, grouping would add unnecessary indirection
pub(crate) fn execute_execution_runtime_stream<'a>(
    state: &'a AppState,
    plan: ExecutionPlan,
    trace_id: &'a str,
    decision: &'a GatewayControlDecision,
    plan_kind: &'a str,
    report_kind: Option<String>,
    report_context: Option<serde_json::Value>,
) -> Pin<Box<dyn Future<Output = Result<Option<Response<Body>>, GatewayError>> + Send + 'a>> {
    Box::pin(async move {
        let mut cancellation_guard = AttemptCancellationGuard::disarmed(
            state,
            STREAM_ATTEMPT_CANCELLED_ERROR_TYPE,
            STREAM_ATTEMPT_CANCELLED_ERROR_MESSAGE,
        );
        let result = execute_execution_runtime_stream_inner(
            state,
            plan,
            trace_id,
            decision,
            plan_kind,
            report_kind,
            report_context,
            None,
            None,
            &mut cancellation_guard,
        )
        .await;
        // The attempt reached its own terminal path, or handed settlement to the
        // stream finalizer that now lives in the response body.
        cancellation_guard.disarm();
        result
    })
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn execute_execution_runtime_stream_with_retry_scope<'a>(
    state: &'a AppState,
    plan: ExecutionPlan,
    trace_id: &'a str,
    decision: &'a GatewayControlDecision,
    plan_kind: &'a str,
    report_kind: Option<String>,
    report_context: Option<serde_json::Value>,
) -> Pin<
    Box<
        dyn Future<Output = Result<AiAttemptExecutionOutcome<Response<Body>>, GatewayError>>
            + Send
            + 'a,
    >,
> {
    Box::pin(async move {
        let mut retry_scope = AiAttemptRetryScope::Candidate;
        let mut fallback_response = None;
        let mut cancellation_guard = AttemptCancellationGuard::disarmed(
            state,
            STREAM_ATTEMPT_CANCELLED_ERROR_TYPE,
            STREAM_ATTEMPT_CANCELLED_ERROR_MESSAGE,
        );
        let result = execute_execution_runtime_stream_inner(
            state,
            plan,
            trace_id,
            decision,
            plan_kind,
            report_kind,
            report_context,
            Some(&mut retry_scope),
            Some(&mut fallback_response),
            &mut cancellation_guard,
        )
        .await;
        // The attempt reached its own terminal path, or handed settlement to the
        // stream finalizer that now lives in the response body.
        cancellation_guard.disarm();
        let response = result?;
        Ok(match response {
            Some(response) => AiAttemptExecutionOutcome::Responded(response),
            None => AiAttemptExecutionOutcome::Retry {
                scope: retry_scope,
                fallback_response,
            },
        })
    })
}

async fn maybe_build_stream_transport_error_stop_response(
    state: &AppState,
    plan: &ExecutionPlan,
    report_context: Option<&Value>,
    trace_id: &str,
    decision: &GatewayControlDecision,
    error_type: &str,
    error_message: &str,
    elapsed_ms: u64,
) -> Result<Option<Response<Body>>, GatewayError> {
    let analysis = crate::orchestration::resolve_local_transport_failover_analysis_for_attempt(
        state,
        plan,
        report_context,
    )
    .await;
    if !matches!(analysis.decision, LocalFailoverDecision::StopLocalFailover) {
        return Ok(None);
    }

    crate::execution_runtime::build_transport_error_stop_response(
        state,
        plan,
        report_context,
        trace_id,
        decision,
        http::StatusCode::BAD_GATEWAY.as_u16(),
        error_type,
        error_message,
        elapsed_ms,
    )
    .await
    .map(Some)
}

#[allow(clippy::too_many_arguments)] // internal function, grouping would add unnecessary indirection
async fn execute_execution_runtime_stream_inner(
    state: &AppState,
    mut plan: ExecutionPlan,
    trace_id: &str,
    decision: &GatewayControlDecision,
    plan_kind: &str,
    report_kind: Option<String>,
    mut report_context: Option<serde_json::Value>,
    mut retry_scope_out: Option<&mut AiAttemptRetryScope>,
    mut retry_fallback_out: Option<&mut Option<Response<Body>>>,
    cancellation_guard: &mut AttemptCancellationGuard,
) -> Result<Option<Response<Body>>, GatewayError> {
    let stream_started_at = Instant::now();
    let mut stage_trace = RequestStageTrace::from_env();
    let candidate_slot_started_at = Instant::now();
    ensure_execution_request_candidate_slot(state, &mut plan, &mut report_context).await;
    observe_gateway_stage_trace_ms(
        &mut stage_trace,
        "stream_candidate_slot",
        candidate_slot_started_at.elapsed().as_millis() as u64,
    );
    let request_candidate_status_snapshot =
        snapshot_local_request_candidate_status(&plan, report_context.as_ref());
    let defer_stream_pending_for_direct_inline = should_defer_stream_pending_for_direct_inline(
        state,
        &plan,
        plan_kind,
        report_context.as_ref(),
    );
    let candidate_started_unix_secs = current_request_candidate_unix_ms();
    let provider_in_flight_started_at = Instant::now();
    let mut provider_pool_in_flight_guard =
        match acquire_provider_pool_execution_guard(state, &plan, report_context.as_ref()).await? {
            ProviderPoolInFlightAdmission::Acquired(guard) => guard,
            ProviderPoolInFlightAdmission::Saturated { limit } => {
                record_local_runtime_candidate_skip_reason(
                    state,
                    trace_id,
                    "provider_key_concurrency_limit_reached",
                );
                if let Some(retry_scope) = retry_scope_out.as_deref_mut() {
                    *retry_scope = AiAttemptRetryScope::Candidate;
                }
                if let Some(snapshot) = request_candidate_status_snapshot.as_ref() {
                    record_local_request_candidate_status_snapshot(
                        state,
                        snapshot,
                        SchedulerRequestCandidateStatusUpdate {
                            status: RequestCandidateStatus::Skipped,
                            status_code: Some(http::StatusCode::TOO_MANY_REQUESTS.as_u16()),
                            error_type: Some("provider_key_concurrency_limit_reached".to_string()),
                            error_message: Some(format!(
                                "provider key concurrency limit reached: {limit}"
                            )),
                            latency_ms: Some(0),
                            started_at_unix_ms: Some(candidate_started_unix_secs),
                            finished_at_unix_ms: Some(candidate_started_unix_secs),
                        },
                    )
                    .await;
                }
                return Ok(None);
            }
        };
    observe_gateway_stage_trace_ms(
        &mut stage_trace,
        "stream_provider_in_flight",
        provider_in_flight_started_at.elapsed().as_millis() as u64,
    );
    // Inline passthrough records its lifecycle seed after upstream headers are
    // available. Avoid constructing a throwaway seed on the common path.
    let mut lifecycle_seed = (!defer_stream_pending_for_direct_inline)
        .then(|| build_lifecycle_usage_seed(&plan, report_context.as_ref()));
    let mut lifecycle_pending_recorded = false;
    if let Some(seed) = lifecycle_seed.as_ref() {
        record_stream_pending_lifecycle(state, seed, &mut stage_trace).await;
        lifecycle_pending_recorded = true;
    }
    if let Some(snapshot) = request_candidate_status_snapshot.clone() {
        record_local_request_candidate_status_snapshot(
            state,
            &snapshot,
            SchedulerRequestCandidateStatusUpdate {
                status: RequestCandidateStatus::Pending,
                status_code: None,
                error_type: None,
                error_message: None,
                latency_ms: None,
                started_at_unix_ms: Some(candidate_started_unix_secs),
                finished_at_unix_ms: None,
            },
        )
        .await;
    }
    // From here the attempt owns non-terminal rows, and everything that could
    // settle them runs inside the downstream request future. Arm the guard so a
    // client disconnect before the stream finalizer exists still settles them.
    cancellation_guard.arm(
        &plan,
        report_context.as_ref(),
        request_candidate_status_snapshot.as_ref(),
        candidate_started_unix_secs,
        stream_started_at,
    );
    let plan_request_id_for_log = short_request_id(plan.request_id.as_str());
    let provider_name = plan
        .provider_name
        .clone()
        .unwrap_or_else(|| "-".to_string());
    let endpoint_id = plan.endpoint_id.clone();
    let key_id = plan.key_id.clone();
    let model_name = plan.model_name.clone().unwrap_or_else(|| "-".to_string());
    let candidate_index = parse_request_candidate_report_context(report_context.as_ref())
        .and_then(|context| context.candidate_index)
        .map(|value| value.to_string())
        .unwrap_or_else(|| "-".to_string());
    match maybe_execute_grok_stream(&plan, report_context.as_ref()).await {
        Ok(Some(grok_stream)) => {
            return execute_stream_from_frame_stream_with_retry_scope(
                state,
                plan,
                trace_id,
                decision,
                plan_kind,
                report_kind,
                grok_stream.report_context.or(report_context),
                candidate_started_unix_secs,
                stream_started_at,
                stage_trace,
                lifecycle_pending_recorded,
                grok_stream.frame_stream,
                false,
                provider_pool_in_flight_guard.take(),
                retry_scope_out.as_deref_mut(),
                retry_fallback_out.as_deref_mut(),
                None,
            )
            .await;
        }
        Ok(None) => {}
        Err(_err) => {
            let transport_error_message = "Grok stream execution unavailable".to_string();
            info!(
                event_name = "grok_execution_unavailable",
                log_type = "ops",
                trace_id = %trace_id,
                request_id = %plan_request_id_for_log,
                candidate_id = ?plan.candidate_id,
                provider_name = provider_name.as_str(),
                endpoint_id = %endpoint_id,
                key_id = %key_id,
                model_name = model_name.as_str(),
                candidate_index = candidate_index.as_str(),
                error_category = "grok_execution_unavailable",
                "gateway Grok stream execution unavailable"
            );
            let terminal_unix_secs = current_request_candidate_unix_ms();
            record_local_request_candidate_status(
                state,
                &plan,
                report_context.as_ref(),
                SchedulerRequestCandidateStatusUpdate {
                    status: RequestCandidateStatus::Failed,
                    status_code: None,
                    error_type: Some("grok_execution_unavailable".to_string()),
                    error_message: Some(transport_error_message.clone()),
                    latency_ms: Some(stream_elapsed_ms_since(stream_started_at)),
                    started_at_unix_ms: Some(candidate_started_unix_secs),
                    finished_at_unix_ms: Some(terminal_unix_secs),
                },
            )
            .await;
            if let Some(response) = maybe_build_stream_transport_error_stop_response(
                state,
                &plan,
                report_context.as_ref(),
                trace_id,
                decision,
                "grok_execution_unavailable",
                transport_error_message.as_str(),
                stream_elapsed_ms_since(stream_started_at),
            )
            .await?
            {
                return Ok(Some(response));
            }
            return Ok(None);
        }
    }
    match maybe_execute_windsurf_stream(state, &plan, report_context.as_ref()).await {
        Ok(Some(windsurf_stream)) => {
            return execute_stream_from_frame_stream_with_retry_scope(
                state,
                plan,
                trace_id,
                decision,
                plan_kind,
                report_kind,
                windsurf_stream.report_context.or(report_context),
                candidate_started_unix_secs,
                stream_started_at,
                stage_trace,
                lifecycle_pending_recorded,
                windsurf_stream.frame_stream,
                false,
                provider_pool_in_flight_guard.take(),
                retry_scope_out.as_deref_mut(),
                retry_fallback_out.as_deref_mut(),
                None,
            )
            .await;
        }
        Ok(None) => {}
        Err(_err) => {
            let transport_error_message = "Windsurf stream execution unavailable".to_string();
            info!(
                event_name = "windsurf_native_execution_unavailable",
                log_type = "ops",
                trace_id = %trace_id,
                request_id = %plan_request_id_for_log,
                candidate_id = ?plan.candidate_id,
                provider_name = provider_name.as_str(),
                endpoint_id = %endpoint_id,
                key_id = %key_id,
                model_name = model_name.as_str(),
                candidate_index = candidate_index.as_str(),
                error_category = "windsurf_execution_unavailable",
                "gateway native Windsurf stream execution unavailable"
            );
            let terminal_unix_secs = current_request_candidate_unix_ms();
            record_local_request_candidate_status(
                state,
                &plan,
                report_context.as_ref(),
                SchedulerRequestCandidateStatusUpdate {
                    status: RequestCandidateStatus::Failed,
                    status_code: None,
                    error_type: Some("windsurf_native_execution_unavailable".to_string()),
                    error_message: Some(transport_error_message.clone()),
                    latency_ms: Some(stream_elapsed_ms_since(stream_started_at)),
                    started_at_unix_ms: Some(candidate_started_unix_secs),
                    finished_at_unix_ms: Some(terminal_unix_secs),
                },
            )
            .await;
            if let Some(response) = maybe_build_stream_transport_error_stop_response(
                state,
                &plan,
                report_context.as_ref(),
                trace_id,
                decision,
                "windsurf_native_execution_unavailable",
                transport_error_message.as_str(),
                stream_elapsed_ms_since(stream_started_at),
            )
            .await?
            {
                return Ok(Some(response));
            }
            return Ok(None);
        }
    }
    match maybe_execute_kiro_web_search_stream(state, &plan, report_context.as_ref()).await {
        Ok(Some(kiro_web_search)) => {
            return execute_stream_from_frame_stream_with_retry_scope(
                state,
                plan,
                trace_id,
                decision,
                plan_kind,
                report_kind,
                kiro_web_search.report_context.or(report_context),
                candidate_started_unix_secs,
                stream_started_at,
                stage_trace,
                lifecycle_pending_recorded,
                kiro_web_search.frame_stream,
                false,
                provider_pool_in_flight_guard.take(),
                retry_scope_out.as_deref_mut(),
                retry_fallback_out.as_deref_mut(),
                None,
            )
            .await;
        }
        Ok(None) => {}
        Err(_err) => {
            let transport_error_message = "Kiro web search execution unavailable".to_string();
            info!(
                event_name = "kiro_web_search_mcp_unavailable",
                log_type = "ops",
                trace_id = %trace_id,
                request_id = %plan_request_id_for_log,
                candidate_id = ?plan.candidate_id,
                provider_name = provider_name.as_str(),
                endpoint_id = %endpoint_id,
                key_id = %key_id,
                model_name = model_name.as_str(),
                candidate_index = candidate_index.as_str(),
                error_category = "kiro_web_search_unavailable",
                "gateway Kiro web_search MCP execution unavailable"
            );
            let terminal_unix_secs = current_request_candidate_unix_ms();
            record_local_request_candidate_status(
                state,
                &plan,
                report_context.as_ref(),
                SchedulerRequestCandidateStatusUpdate {
                    status: RequestCandidateStatus::Failed,
                    status_code: None,
                    error_type: Some("kiro_web_search_mcp_unavailable".to_string()),
                    error_message: Some(transport_error_message.clone()),
                    latency_ms: Some(stream_elapsed_ms_since(stream_started_at)),
                    started_at_unix_ms: Some(candidate_started_unix_secs),
                    finished_at_unix_ms: Some(terminal_unix_secs),
                },
            )
            .await;
            if let Some(response) = maybe_build_stream_transport_error_stop_response(
                state,
                &plan,
                report_context.as_ref(),
                trace_id,
                decision,
                "kiro_web_search_mcp_unavailable",
                transport_error_message.as_str(),
                stream_elapsed_ms_since(stream_started_at),
            )
            .await?
            {
                return Ok(Some(response));
            }
            return Ok(None);
        }
    }
    match maybe_execute_chatgpt_web_image_stream(state, &plan, report_context.as_ref()).await {
        Ok(Some(chatgpt_web_image)) => {
            return execute_stream_from_frame_stream_with_retry_scope(
                state,
                plan,
                trace_id,
                decision,
                plan_kind,
                report_kind,
                chatgpt_web_image.report_context.or(report_context),
                candidate_started_unix_secs,
                stream_started_at,
                stage_trace,
                lifecycle_pending_recorded,
                chatgpt_web_image.frame_stream,
                false,
                provider_pool_in_flight_guard.take(),
                retry_scope_out.as_deref_mut(),
                retry_fallback_out.as_deref_mut(),
                None,
            )
            .await;
        }
        Ok(None) => {}
        Err(_err) => {
            let transport_error_message = "ChatGPT-Web image execution unavailable".to_string();
            info!(
                event_name = "chatgpt_web_image_execution_unavailable",
                log_type = "ops",
                trace_id = %trace_id,
                request_id = %plan_request_id_for_log,
                candidate_id = ?plan.candidate_id,
                provider_name = provider_name.as_str(),
                endpoint_id = %endpoint_id,
                key_id = %key_id,
                model_name = model_name.as_str(),
                candidate_index = candidate_index.as_str(),
                error_category = "chatgpt_web_image_execution_unavailable",
                "gateway ChatGPT-Web image stream execution unavailable"
            );
            let terminal_unix_secs = current_request_candidate_unix_ms();
            record_local_request_candidate_status(
                state,
                &plan,
                report_context.as_ref(),
                SchedulerRequestCandidateStatusUpdate {
                    status: RequestCandidateStatus::Failed,
                    status_code: None,
                    error_type: Some("chatgpt_web_image_execution_unavailable".to_string()),
                    error_message: Some(transport_error_message.clone()),
                    latency_ms: Some(stream_elapsed_ms_since(stream_started_at)),
                    started_at_unix_ms: Some(candidate_started_unix_secs),
                    finished_at_unix_ms: Some(terminal_unix_secs),
                },
            )
            .await;
            if let Some(response) = maybe_build_stream_transport_error_stop_response(
                state,
                &plan,
                report_context.as_ref(),
                trace_id,
                decision,
                "chatgpt_web_image_execution_unavailable",
                transport_error_message.as_str(),
                stream_elapsed_ms_since(stream_started_at),
            )
            .await?
            {
                return Ok(Some(response));
            }
            return Ok(None);
        }
    }
    #[cfg(not(test))]
    {
        let upstream_headers_started_at = Instant::now();
        let execution = match execute_in_process_stream_with_oauth_retry(
            state,
            &mut plan,
            trace_id,
            report_context.as_ref(),
        )
        .await
        {
            Ok(execution) => execution,
            Err(InProcessStreamExecutionError::Gateway(err)) => {
                if matches!(err, GatewayError::AdmissionTimeout { .. }) {
                    record_stream_admission_timeout_candidate_failure(
                        state,
                        &plan,
                        report_context.as_ref(),
                        candidate_started_unix_secs,
                        &err,
                    )
                    .await;
                }
                return Err(err);
            }
            Err(InProcessStreamExecutionError::Transport(_err)) => {
                let transport_error_message = "Execution runtime unavailable".to_string();
                info!(
                    event_name = "stream_execution_runtime_unavailable",
                    log_type = "ops",
                    trace_id = %trace_id,
                    request_id = %plan_request_id_for_log,
                    candidate_id = ?plan.candidate_id,
                    provider_name,
                    endpoint_id,
                    key_id,
                    model_name,
                    candidate_index = candidate_index.as_str(),
                    error_category = "execution_runtime_unavailable",
                    "gateway in-process stream execution unavailable"
                );
                let terminal_unix_secs = current_request_candidate_unix_ms();
                record_local_request_candidate_status(
                    state,
                    &plan,
                    report_context.as_ref(),
                    SchedulerRequestCandidateStatusUpdate {
                        status: RequestCandidateStatus::Failed,
                        status_code: None,
                        error_type: Some("execution_runtime_unavailable".to_string()),
                        error_message: Some(transport_error_message.clone()),
                        latency_ms: Some(stream_elapsed_ms_since(stream_started_at)),
                        started_at_unix_ms: Some(candidate_started_unix_secs),
                        finished_at_unix_ms: Some(terminal_unix_secs),
                    },
                )
                .await;
                if let Some(response) = maybe_build_stream_transport_error_stop_response(
                    state,
                    &plan,
                    report_context.as_ref(),
                    trace_id,
                    decision,
                    "execution_runtime_unavailable",
                    transport_error_message.as_str(),
                    stream_elapsed_ms_since(stream_started_at),
                )
                .await?
                {
                    return Ok(Some(response));
                }
                return Ok(None);
            }
        };
        observe_gateway_stage_trace_ms(
            &mut stage_trace,
            "stream_upstream_headers",
            upstream_headers_started_at.elapsed().as_millis() as u64,
        );
        if should_use_direct_sse_passthrough(&plan, plan_kind, report_context.as_ref(), &execution)
        {
            return Box::pin(execute_stream_from_direct_passthrough(
                state,
                plan,
                trace_id,
                decision,
                plan_kind,
                report_kind,
                report_context,
                candidate_started_unix_secs,
                stream_started_at,
                stage_trace,
                execution,
                provider_pool_in_flight_guard.take(),
                lifecycle_pending_recorded,
            ))
            .await;
        }
        if !lifecycle_pending_recorded {
            let seed = lifecycle_seed
                .get_or_insert_with(|| build_lifecycle_usage_seed(&plan, report_context.as_ref()));
            record_stream_pending_lifecycle(state, seed, &mut stage_trace).await;
            lifecycle_pending_recorded = true;
        }
        let report_context = attach_provider_response_headers_to_report_context(
            report_context,
            &execution.headers,
            execution.response_observation.request_started_at_unix_ms,
            execution
                .response_observation
                .response_headers_observed_at_unix_ms,
            &execution.response_observation.request_order_id,
        );
        let stream_precommit_committed = execution.stream_precommit_committed;
        let frame_stream = build_direct_execution_frame_stream(execution).boxed();
        return execute_stream_from_frame_stream_with_retry_scope(
            state,
            plan,
            trace_id,
            decision,
            plan_kind,
            report_kind,
            report_context,
            candidate_started_unix_secs,
            stream_started_at,
            stage_trace,
            lifecycle_pending_recorded,
            frame_stream,
            stream_precommit_committed,
            provider_pool_in_flight_guard.take(),
            retry_scope_out,
            retry_fallback_out,
            None,
        )
        .await;
    }
    #[cfg(test)]
    {
        let remote_execution_runtime_base_url = state
            .execution_runtime_override_base_url()
            .unwrap_or_default();
        if remote_execution_runtime_base_url.trim().is_empty() {
            let upstream_headers_started_at = Instant::now();
            let execution = match execute_in_process_stream_with_oauth_retry(
                state,
                &mut plan,
                trace_id,
                report_context.as_ref(),
            )
            .await
            {
                Ok(execution) => execution,
                Err(InProcessStreamExecutionError::Gateway(err)) => {
                    if matches!(err, GatewayError::AdmissionTimeout { .. }) {
                        record_stream_admission_timeout_candidate_failure(
                            state,
                            &plan,
                            report_context.as_ref(),
                            candidate_started_unix_secs,
                            &err,
                        )
                        .await;
                    }
                    return Err(err);
                }
                Err(InProcessStreamExecutionError::Transport(_err)) => {
                    let transport_error_message = "Execution runtime unavailable".to_string();
                    info!(
                        event_name = "stream_execution_runtime_unavailable",
                        log_type = "ops",
                        trace_id = %trace_id,
                        request_id = %plan_request_id_for_log,
                        candidate_id = ?plan.candidate_id,
                        provider_name,
                        endpoint_id,
                        key_id,
                        model_name,
                        candidate_index = candidate_index.as_str(),
                        error_category = "execution_runtime_unavailable",
                        "gateway in-process stream execution unavailable"
                    );
                    let terminal_unix_secs = current_request_candidate_unix_ms();
                    record_local_request_candidate_status(
                        state,
                        &plan,
                        report_context.as_ref(),
                        SchedulerRequestCandidateStatusUpdate {
                            status: RequestCandidateStatus::Failed,
                            status_code: None,
                            error_type: Some("execution_runtime_unavailable".to_string()),
                            error_message: Some(transport_error_message.clone()),
                            latency_ms: Some(stream_elapsed_ms_since(stream_started_at)),
                            started_at_unix_ms: Some(candidate_started_unix_secs),
                            finished_at_unix_ms: Some(terminal_unix_secs),
                        },
                    )
                    .await;
                    if let Some(response) = maybe_build_stream_transport_error_stop_response(
                        state,
                        &plan,
                        report_context.as_ref(),
                        trace_id,
                        decision,
                        "execution_runtime_unavailable",
                        transport_error_message.as_str(),
                        stream_elapsed_ms_since(stream_started_at),
                    )
                    .await?
                    {
                        return Ok(Some(response));
                    }
                    return Ok(None);
                }
            };
            observe_gateway_stage_trace_ms(
                &mut stage_trace,
                "stream_upstream_headers",
                upstream_headers_started_at.elapsed().as_millis() as u64,
            );
            if should_use_direct_sse_passthrough(
                &plan,
                plan_kind,
                report_context.as_ref(),
                &execution,
            ) {
                return Box::pin(execute_stream_from_direct_passthrough(
                    state,
                    plan,
                    trace_id,
                    decision,
                    plan_kind,
                    report_kind,
                    report_context,
                    candidate_started_unix_secs,
                    stream_started_at,
                    stage_trace,
                    execution,
                    provider_pool_in_flight_guard.take(),
                    lifecycle_pending_recorded,
                ))
                .await;
            }
            if !lifecycle_pending_recorded {
                let seed = lifecycle_seed.get_or_insert_with(|| {
                    build_lifecycle_usage_seed(&plan, report_context.as_ref())
                });
                record_stream_pending_lifecycle(state, seed, &mut stage_trace).await;
                lifecycle_pending_recorded = true;
            }
            let report_context = attach_provider_response_headers_to_report_context(
                report_context,
                &execution.headers,
                execution.response_observation.request_started_at_unix_ms,
                execution
                    .response_observation
                    .response_headers_observed_at_unix_ms,
                &execution.response_observation.request_order_id,
            );
            let stream_precommit_committed = execution.stream_precommit_committed;
            let frame_stream = build_direct_execution_frame_stream(execution).boxed();
            return execute_stream_from_frame_stream_with_retry_scope(
                state,
                plan,
                trace_id,
                decision,
                plan_kind,
                report_kind,
                report_context,
                candidate_started_unix_secs,
                stream_started_at,
                stage_trace,
                lifecycle_pending_recorded,
                frame_stream,
                stream_precommit_committed,
                provider_pool_in_flight_guard.take(),
                retry_scope_out.as_deref_mut(),
                retry_fallback_out.as_deref_mut(),
                None,
            )
            .await;
        }

        let remote_request_started_at_unix_ms = current_request_candidate_unix_ms();
        let remote_request_order_id = uuid::Uuid::now_v7().to_string();
        let response = match post_stream_plan_to_remote_execution_runtime(
            state,
            remote_execution_runtime_base_url,
            Some(trace_id),
            &plan,
        )
        .await
        {
            Ok(response) => response,
            Err(_err) => {
                let transport_error_message = "Remote execution runtime unavailable".to_string();
                warn!(
                    event_name = "stream_execution_runtime_remote_unavailable",
                    log_type = "ops",
                    trace_id = %trace_id,
                    request_id = %plan_request_id_for_log,
                    candidate_id = ?plan.candidate_id,
                    error_category = "execution_runtime_unavailable",
                    "gateway remote execution runtime stream unavailable"
                );
                let terminal_unix_secs = current_request_candidate_unix_ms();
                record_local_request_candidate_status(
                    state,
                    &plan,
                    report_context.as_ref(),
                    SchedulerRequestCandidateStatusUpdate {
                        status: RequestCandidateStatus::Failed,
                        status_code: None,
                        error_type: Some("execution_runtime_unavailable".to_string()),
                        error_message: Some(transport_error_message.clone()),
                        latency_ms: Some(stream_elapsed_ms_since(stream_started_at)),
                        started_at_unix_ms: Some(candidate_started_unix_secs),
                        finished_at_unix_ms: Some(terminal_unix_secs),
                    },
                )
                .await;
                if let Some(response) = maybe_build_stream_transport_error_stop_response(
                    state,
                    &plan,
                    report_context.as_ref(),
                    trace_id,
                    decision,
                    "execution_runtime_unavailable",
                    transport_error_message.as_str(),
                    stream_elapsed_ms_since(stream_started_at),
                )
                .await?
                {
                    return Ok(Some(response));
                }
                return Ok(None);
            }
        };

        if response.status() != http::StatusCode::OK {
            let terminal_unix_secs = current_request_candidate_unix_ms();
            record_local_request_candidate_status(
                state,
                &plan,
                report_context.as_ref(),
                SchedulerRequestCandidateStatusUpdate {
                    status: RequestCandidateStatus::Failed,
                    status_code: Some(response.status().as_u16()),
                    error_type: Some("execution_runtime_http_error".to_string()),
                    error_message: Some(format!(
                        "execution runtime returned HTTP {}",
                        response.status()
                    )),
                    latency_ms: None,
                    started_at_unix_ms: Some(candidate_started_unix_secs),
                    finished_at_unix_ms: Some(terminal_unix_secs),
                },
            )
            .await;
            return Ok(Some(attach_control_metadata_headers(
                build_client_response(response, trace_id, Some(decision))?,
                Some(plan.request_id.as_str()),
                plan.candidate_id.as_deref(),
            )?));
        }

        let remote_response_observed_at_unix_ms = current_request_candidate_unix_ms();
        let remote_fallback_observation = ExecutionResponseObservation {
            request_started_at_unix_ms: remote_request_started_at_unix_ms,
            response_headers_observed_at_unix_ms: remote_response_observed_at_unix_ms,
            request_order_id: remote_request_order_id,
        };
        let frame_stream = response
            .bytes_stream()
            .map_err(|err| IoError::other(err.to_string()))
            .boxed();
        return execute_stream_from_frame_stream_with_retry_scope(
            state,
            plan,
            trace_id,
            decision,
            plan_kind,
            report_kind,
            report_context,
            candidate_started_unix_secs,
            stream_started_at,
            stage_trace,
            lifecycle_pending_recorded,
            frame_stream,
            false,
            provider_pool_in_flight_guard.take(),
            retry_scope_out,
            retry_fallback_out,
            Some(remote_fallback_observation),
        )
        .await;
    }
}

fn decode_stream_data_chunk(
    chunk_b64: Option<&str>,
    text: Option<&str>,
) -> Result<Vec<u8>, GatewayError> {
    decode_stream_data_chunk_with_limit(chunk_b64, text, MAX_EXECUTION_STREAM_DATA_CHUNK_BYTES)
}

fn decode_stream_data_chunk_with_limit(
    chunk_b64: Option<&str>,
    text: Option<&str>,
    max_bytes: usize,
) -> Result<Vec<u8>, GatewayError> {
    if let Some(chunk_b64) = chunk_b64 {
        return decode_base64_body_with_limit(chunk_b64, max_bytes)
            .map_err(|err| GatewayError::Internal(err.to_string()));
    }
    let text = text.unwrap_or_default().as_bytes();
    if text.len() > max_bytes {
        return Err(GatewayError::Internal(format!(
            "execution runtime stream data chunk exceeds {max_bytes} bytes"
        )));
    }
    Ok(text.to_vec())
}

fn response_headers_indicate_sse(headers: &BTreeMap<String, String>) -> bool {
    headers
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case("content-type"))
        .map(|(_, value)| value.as_str())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .is_some_and(|value| value.to_ascii_lowercase().contains("text/event-stream"))
}

fn report_context_upstream_is_stream(report_context: Option<&Value>) -> bool {
    report_context
        .and_then(|value| value.get(UPSTREAM_IS_STREAM_KEY))
        .and_then(Value::as_bool)
        .unwrap_or(false)
}

fn response_headers_have_octet_stream_content_type(headers: &BTreeMap<String, String>) -> bool {
    headers
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case("content-type"))
        .map(|(_, value)| value.as_str())
        .and_then(|value| value.split(';').next())
        .map(str::trim)
        .is_some_and(|value| value.eq_ignore_ascii_case("application/octet-stream"))
}

fn response_headers_have_only_identity_content_encoding(
    headers: &BTreeMap<String, String>,
) -> bool {
    headers
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case("content-encoding"))
        .map(|(_, value)| value.as_str())
        .is_none_or(|value| {
            value
                .split(',')
                .map(str::trim)
                .all(|coding| coding.is_empty() || coding.eq_ignore_ascii_case("identity"))
        })
}

fn plan_kind_uses_text_event_stream(plan_kind: &str) -> bool {
    matches!(
        plan_kind,
        OPENAI_CHAT_STREAM_PLAN_KIND
            | OPENAI_RESPONSES_STREAM_PLAN_KIND
            | OPENAI_RESPONSES_COMPACT_STREAM_PLAN_KIND
            | OPENAI_IMAGE_STREAM_PLAN_KIND
            | CLAUDE_CHAT_STREAM_PLAN_KIND
            | CLAUDE_CLI_STREAM_PLAN_KIND
            | GEMINI_CHAT_STREAM_PLAN_KIND
            | GEMINI_CLI_STREAM_PLAN_KIND
            | GEMINI_INTERACTIONS_STREAM_PLAN_KIND
    )
}

fn should_normalize_declared_stream_response_headers(
    plan_kind: &str,
    status_code: u16,
    headers: &BTreeMap<String, String>,
    report_context: Option<&Value>,
) -> bool {
    plan_kind_uses_text_event_stream(plan_kind)
        && (200..300).contains(&status_code)
        && report_context_upstream_is_stream(report_context)
        && response_headers_have_octet_stream_content_type(headers)
        && response_headers_have_only_identity_content_encoding(headers)
        && !headers
            .keys()
            .any(|name| name.eq_ignore_ascii_case("content-length"))
}

fn normalize_declared_stream_response_headers(headers: &mut BTreeMap<String, String>) {
    headers.retain(|name, _| {
        !name.eq_ignore_ascii_case("content-encoding")
            && !name.eq_ignore_ascii_case("content-length")
            && !name.eq_ignore_ascii_case("content-type")
    });
    headers.insert("content-type".to_string(), "text/event-stream".to_string());
}

fn parse_prefetched_sync_json_body(body: &[u8]) -> Option<Value> {
    let stripped = strip_utf8_bom_and_ws(body);
    serde_json::from_slice::<Value>(stripped).ok()
}

fn success_failover_matchable_body<'body>(
    headers: &BTreeMap<String, String>,
    body: &'body [u8],
) -> Option<&'body [u8]> {
    if body.is_empty() {
        return None;
    }
    if response_headers_indicate_sse(headers) {
        let mut complete_end = 0;
        while let Some((record_end, separator_len)) =
            find_sse_record_boundary(&body[complete_end..])
        {
            complete_end += record_end + separator_len;
        }
        return (complete_end > 0).then_some(&body[..complete_end]);
    }
    let stripped = strip_utf8_bom_and_ws(body);
    if stripped.starts_with(b"{") || stripped.starts_with(b"[") {
        if serde_json::from_slice::<Value>(stripped).is_err_and(|error| error.is_eof()) {
            return None;
        }
    }
    Some(body)
}

fn resolve_provider_stream_error_status_code(
    provider_api_format: &str,
    upstream_status_code: u16,
    body_json: &Value,
) -> u16 {
    if (200..300).contains(&upstream_status_code)
        && provider_api_format
            .trim()
            .eq_ignore_ascii_case("claude:messages")
    {
        anthropic_error_status_code(body_json)
    } else {
        resolve_local_sync_error_status_code(upstream_status_code, body_json)
    }
}

fn anthropic_premature_eof_error_body(message: &str) -> Value {
    serde_json::json!({
        "type": "error",
        "error": {
            "type": "api_error",
            "message": message,
        }
    })
}

fn build_anthropic_premature_eof_failure(message: &str) -> StreamFailureReport {
    let body_json = anthropic_premature_eof_error_body(message);
    build_stream_failure_from_provider_error_body(
        anthropic_error_status_code(&body_json),
        &body_json,
    )
}

fn encode_terminal_sse_error_event(failure: &StreamFailureReport) -> Result<Bytes, std::io::Error> {
    let payload = failure
        .to_json_string()
        .map_err(|err| IoError::other(err.to_string()))?;
    let mut event = String::new();
    for line in payload.lines() {
        event.push_str("data: ");
        event.push_str(line);
        event.push('\n');
    }
    event.push_str("\ndata: [DONE]\n\n");
    Ok(Bytes::from(event))
}

fn encode_anthropic_terminal_sse_error_event(
    failure: &StreamFailureReport,
) -> Result<Bytes, std::io::Error> {
    let payload = serde_json::to_string(&serde_json::json!({
        "type": "error",
        "error": {
            "type": "api_error",
            "message": failure.error_message,
        }
    }))
    .map_err(|err| IoError::other(err.to_string()))?;
    Ok(Bytes::from(format!("event: error\ndata: {payload}\n\n")))
}

fn encode_terminal_sse_error_event_for_plan(
    plan: &ExecutionPlan,
    failure: &StreamFailureReport,
) -> Result<Bytes, std::io::Error> {
    if plan
        .client_api_format
        .trim()
        .eq_ignore_ascii_case("claude:messages")
        && plan
            .provider_api_format
            .trim()
            .eq_ignore_ascii_case("claude:messages")
    {
        encode_anthropic_terminal_sse_error_event(failure)
    } else {
        encode_terminal_sse_error_event(failure)
    }
}

fn image_stream_failed_event_name(report_context: Option<&Value>) -> &'static str {
    let operation = report_context
        .and_then(|value| value.get("image_request"))
        .and_then(|value| value.get("operation"))
        .and_then(Value::as_str)
        .map(str::trim)
        .unwrap_or_default();
    if operation == "edit" {
        "image_edit.failed"
    } else {
        "image_generation.failed"
    }
}

fn encode_openai_image_failed_event(
    report_context: Option<&Value>,
    failure: &StreamFailureReport,
) -> Result<Bytes, std::io::Error> {
    let event_name = image_stream_failed_event_name(report_context);
    let failure_body = failure
        .to_json_string()
        .map_err(|err| IoError::other(err.to_string()))?;
    let failure_json: Value =
        serde_json::from_str(&failure_body).map_err(|err| IoError::other(err.to_string()))?;
    let error = failure_json.get("error").cloned().unwrap_or_else(|| {
        serde_json::json!({
            "type": failure.error_type.as_str(),
            "message": failure.error_message.as_str(),
            "code": failure.status_code,
        })
    });
    let payload = serde_json::json!({
        "type": event_name,
        "error": error,
    });
    let payload = serde_json::to_string(&payload).map_err(|err| IoError::other(err.to_string()))?;
    let mut event = format!("event: {event_name}\n");
    for line in payload.lines() {
        event.push_str("data: ");
        event.push_str(line);
        event.push('\n');
    }
    event.push('\n');
    Ok(Bytes::from(event))
}

fn should_limit_direct_finalize_prefetch(plan_kind: &str, has_local_stream_rewriter: bool) -> bool {
    plan_kind == OPENAI_IMAGE_STREAM_PLAN_KIND || has_local_stream_rewriter
}

fn client_format_allows_proxy_generated_sse_control_blocks(plan: &ExecutionPlan) -> bool {
    // OpenAI-compatible clients commonly parse every client-visible SSE event as
    // an OpenAI JSON payload or [DONE]. Keep the downstream wire format strict:
    // do not inject proxy-generated comments, pings, or keepalives for openai:*.
    !plan
        .client_api_format
        .trim()
        .to_ascii_lowercase()
        .starts_with("openai:")
}

fn build_sse_body_stream(
    prefetched_chunks_for_body: Vec<Bytes>,
    mut rx: mpsc::Receiver<Result<Bytes, IoError>>,
    filter_control_blocks: bool,
    emit_keepalive: bool,
    anthropic_message_stop_terminates_body: bool,
    keepalive_interval: Duration,
) -> impl futures_util::Stream<Item = Result<Bytes, IoError>> + Send + 'static {
    stream! {
        let mut upstream_control_filter = filter_control_blocks.then(SseControlBlockFilter::default);
        let mut anthropic_completion_tracker =
            anthropic_message_stop_terminates_body.then(ClientVisibleStreamCompletionTracker::default);
        let mut sent_prefetched_chunk = false;
        for chunk in prefetched_chunks_for_body {
            if let Some(mut chunk) = filter_upstream_sse_control_chunk(&mut upstream_control_filter, chunk) {
                let completed = truncate_at_anthropic_message_stop(
                    anthropic_completion_tracker.as_mut(),
                    &mut chunk,
                );
                sent_prefetched_chunk = true;
                yield Ok(chunk);
                if completed {
                    return;
                }
            }
        }

        if emit_keepalive {
            if !sent_prefetched_chunk {
                yield Ok(Bytes::from_static(SSE_KEEPALIVE_BYTES));
            }
            let mut keepalive = tokio::time::interval(keepalive_interval);
            keepalive.set_missed_tick_behavior(MissedTickBehavior::Delay);
            keepalive.tick().await;
            loop {
                tokio::select! {
                    biased;
                    item = rx.recv() => {
                        let Some(item) = item else {
                            break;
                        };
                        match item {
                            Ok(chunk) => {
                                if let Some(mut chunk) = filter_upstream_sse_control_chunk(&mut upstream_control_filter, chunk) {
                                    let completed = truncate_at_anthropic_message_stop(
                                        anthropic_completion_tracker.as_mut(),
                                        &mut chunk,
                                    );
                                    yield Ok(chunk);
                                    if completed {
                                        break;
                                    }
                                }
                            }
                            Err(err) => yield Err(err),
                        }
                    }
                    _ = keepalive.tick() => {
                        yield Ok(Bytes::from_static(SSE_KEEPALIVE_BYTES));
                    }
                }
            }
            if !anthropic_completion_tracker
                .as_ref()
                .is_some_and(|tracker| tracker.completed)
            {
                if let Some(chunk) =
                    flush_upstream_sse_control_filter(&mut upstream_control_filter)
                {
                    yield Ok(chunk);
                }
            }
        } else {
            while let Some(item) = rx.recv().await {
                match item {
                    Ok(chunk) => {
                        if let Some(mut chunk) = filter_upstream_sse_control_chunk(&mut upstream_control_filter, chunk) {
                            let completed = truncate_at_anthropic_message_stop(
                                anthropic_completion_tracker.as_mut(),
                                &mut chunk,
                            );
                            yield Ok(chunk);
                            if completed {
                                break;
                            }
                        }
                    }
                    Err(err) => yield Err(err),
                }
            }
            if !anthropic_completion_tracker
                .as_ref()
                .is_some_and(|tracker| tracker.completed)
            {
                if let Some(chunk) =
                    flush_upstream_sse_control_filter(&mut upstream_control_filter)
                {
                    yield Ok(chunk);
                }
            }
        }
    }
}

fn truncate_at_anthropic_message_stop(
    tracker: Option<&mut ClientVisibleStreamCompletionTracker>,
    chunk: &mut Bytes,
) -> bool {
    let Some(tracker) = tracker else {
        return false;
    };
    if let Some(terminal_end) = tracker.observe_anthropic_message_stop_terminal_end(chunk.as_ref())
    {
        chunk.truncate(terminal_end);
        return true;
    }
    false
}

#[derive(Default)]
struct SseControlBlockFilter {
    buffered: Vec<u8>,
    emitted_len: usize,
    passthrough_current_block: bool,
}

impl SseControlBlockFilter {
    fn push_chunk(&mut self, chunk: &[u8]) -> Vec<u8> {
        if chunk.is_empty() {
            return Vec::new();
        }

        self.buffered.extend_from_slice(chunk);
        let mut output = Vec::new();
        while let Some((block_end, separator_len)) = find_sse_block_boundary(&self.buffered) {
            let block_len = block_end + separator_len;
            let block = self.buffered.drain(..block_len).collect::<Vec<_>>();
            if self.passthrough_current_block {
                let emitted_len = self.emitted_len.min(block.len());
                output.extend_from_slice(&block[emitted_len..]);
            } else if sse_block_has_data_line(&block) {
                output.extend_from_slice(&block);
            }
            self.emitted_len = 0;
            self.passthrough_current_block = false;
        }

        if self.passthrough_current_block {
            if self.buffered.len() > self.emitted_len {
                output.extend_from_slice(&self.buffered[self.emitted_len..]);
                self.emitted_len = self.buffered.len();
            }
        } else if sse_buffer_has_data_line(&self.buffered) {
            self.passthrough_current_block = true;
            output.extend_from_slice(&self.buffered);
            self.emitted_len = self.buffered.len();
        }

        if self.buffered.len() > SSE_CONTROL_FILTER_MAX_BUFFER_BYTES {
            let buffered = std::mem::take(&mut self.buffered);
            if self.passthrough_current_block {
                let emitted_len = self.emitted_len.min(buffered.len());
                output.extend_from_slice(&buffered[emitted_len..]);
            } else {
                output.extend(buffered);
            }
            self.emitted_len = 0;
            self.passthrough_current_block = false;
        }

        output
    }

    fn finish(&mut self) -> Vec<u8> {
        if self.buffered.is_empty() {
            return Vec::new();
        }

        let block = std::mem::take(&mut self.buffered);
        let emitted_len = self.emitted_len.min(block.len());
        let passthrough_current_block = self.passthrough_current_block;
        self.emitted_len = 0;
        self.passthrough_current_block = false;
        if passthrough_current_block {
            block[emitted_len..].to_vec()
        } else if sse_block_has_data_line(&block) {
            block
        } else {
            Vec::new()
        }
    }
}

fn filter_upstream_sse_control_chunk(
    filter: &mut Option<SseControlBlockFilter>,
    chunk: Bytes,
) -> Option<Bytes> {
    let Some(filter) = filter.as_mut() else {
        return Some(chunk);
    };

    let filtered = filter.push_chunk(chunk.as_ref());
    (!filtered.is_empty()).then(|| Bytes::from(filtered))
}

fn flush_upstream_sse_control_filter(filter: &mut Option<SseControlBlockFilter>) -> Option<Bytes> {
    let filtered = filter.as_mut()?.finish();
    (!filtered.is_empty()).then(|| Bytes::from(filtered))
}

fn find_sse_block_boundary(buffer: &[u8]) -> Option<(usize, usize)> {
    find_sse_record_boundary(buffer)
}

fn sse_block_has_data_line(block: &[u8]) -> bool {
    let Ok(text) = std::str::from_utf8(block) else {
        return true;
    };

    text.split(['\r', '\n'])
        .any(|line| line.trim_start().starts_with("data:"))
}

fn sse_buffer_has_data_line(buffer: &[u8]) -> bool {
    let Ok(text) = std::str::from_utf8(buffer) else {
        return true;
    };

    text.split(['\r', '\n'])
        .any(|line| line.trim_start().starts_with("data:"))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SseTerminalPolicy {
    AnyKnown,
    AnthropicMessageStop,
}

#[derive(Default)]
pub(crate) struct ClientVisibleStreamCompletionTracker {
    line_buffer: Vec<u8>,
    event_type: Option<String>,
    data_payload: String,
    has_data_payload: bool,
    record_bytes: usize,
    dropping_oversized_record: bool,
    discarded_line_nonempty: bool,
    skip_next_lf: bool,
    completed: bool,
    successfully_completed: bool,
}

impl ClientVisibleStreamCompletionTracker {
    pub(crate) fn observe_chunk(&mut self, chunk: &[u8]) -> bool {
        self.observe_chunk_terminal_end(chunk);
        self.completed
    }

    pub(crate) fn successful_completion(&self) -> bool {
        self.successfully_completed
    }

    pub(crate) fn observed_terminal(&self) -> bool {
        self.completed
    }

    fn observe_chunk_terminal_end(&mut self, chunk: &[u8]) -> Option<usize> {
        self.observe_chunk_terminal_end_with_policy(chunk, SseTerminalPolicy::AnyKnown)
    }

    fn observe_anthropic_message_stop(&mut self, chunk: &[u8]) -> bool {
        self.observe_anthropic_message_stop_terminal_end(chunk);
        self.completed
    }

    fn observe_anthropic_message_stop_terminal_end(&mut self, chunk: &[u8]) -> Option<usize> {
        self.observe_chunk_terminal_end_with_policy(chunk, SseTerminalPolicy::AnthropicMessageStop)
    }

    fn observe_chunk_terminal_end_with_policy(
        &mut self,
        chunk: &[u8],
        policy: SseTerminalPolicy,
    ) -> Option<usize> {
        if self.completed {
            return None;
        }
        if chunk.is_empty() {
            return None;
        }

        for (index, byte) in chunk.iter().enumerate() {
            self.record_bytes = self.record_bytes.saturating_add(1);
            if self.record_bytes > SSE_TERMINAL_DETECTOR_MAX_RECORD_BYTES
                && !self.dropping_oversized_record
            {
                self.dropping_oversized_record = true;
                self.discarded_line_nonempty = !self.line_buffer.is_empty();
                self.line_buffer.clear();
                self.reset_current_event();
            }

            if self.skip_next_lf {
                self.skip_next_lf = false;
                if *byte == b'\n' {
                    continue;
                }
            }

            if self.dropping_oversized_record {
                match *byte {
                    b'\n' => self.finish_discarded_line(),
                    b'\r' => {
                        self.finish_discarded_line();
                        self.skip_next_lf = true;
                    }
                    _ => self.discarded_line_nonempty = true,
                }
                continue;
            }

            match *byte {
                b'\n' => self.finish_line(policy),
                b'\r' => {
                    self.finish_line(policy);
                    self.skip_next_lf = true;
                }
                _ => self.line_buffer.push(*byte),
            }

            if self.completed {
                let terminal_end = if *byte == b'\r' && chunk.get(index + 1) == Some(&b'\n') {
                    index + 2
                } else {
                    index + 1
                };
                return Some(terminal_end);
            }
        }

        None
    }

    fn finish_line(&mut self, policy: SseTerminalPolicy) {
        let line = std::mem::take(&mut self.line_buffer);
        let Ok(line) = std::str::from_utf8(&line) else {
            self.reset_current_event();
            return;
        };
        let line = line.trim();

        if line.is_empty() {
            self.completed = self.current_event_is_terminal(policy);
            if self.completed {
                self.successfully_completed = self.current_event_is_successful();
            }
            self.reset_current_event();
            self.record_bytes = 0;
            return;
        }

        if let Some(event_type) = line.strip_prefix("event:").map(str::trim) {
            self.event_type = Some(event_type.to_string());
            return;
        }

        if let Some(data) = line.strip_prefix("data:").map(str::trim) {
            if data.is_empty() {
                return;
            }
            if self.has_data_payload {
                self.data_payload.push('\n');
            }
            self.data_payload.push_str(data);
            self.has_data_payload = true;
        }
    }

    fn finish_discarded_line(&mut self) {
        if !self.discarded_line_nonempty {
            self.dropping_oversized_record = false;
            self.record_bytes = 0;
            self.reset_current_event();
        }
        self.discarded_line_nonempty = false;
    }

    fn current_event_is_terminal(&self, policy: SseTerminalPolicy) -> bool {
        match policy {
            SseTerminalPolicy::AnyKnown => {
                self.event_type
                    .as_deref()
                    .is_some_and(is_terminal_sse_event_type)
                    || (self.has_data_payload && sse_data_payload_is_terminal(&self.data_payload))
            }
            SseTerminalPolicy::AnthropicMessageStop => {
                let payload_type = self
                    .has_data_payload
                    .then(|| serde_json::from_str::<serde_json::Value>(&self.data_payload).ok())
                    .flatten()
                    .and_then(|value| {
                        value
                            .get("type")
                            .and_then(serde_json::Value::as_str)
                            .map(ToOwned::to_owned)
                    });
                payload_type.as_deref() == Some("message_stop")
                    && self
                        .event_type
                        .as_deref()
                        .is_none_or(|event_type| event_type == "message_stop")
            }
        }
    }

    fn reset_current_event(&mut self) {
        self.event_type = None;
        self.data_payload.clear();
        self.has_data_payload = false;
    }

    fn current_event_is_successful(&self) -> bool {
        let payload = self
            .has_data_payload
            .then(|| serde_json::from_str::<Value>(&self.data_payload).ok())
            .flatten();
        let payload_type = payload
            .as_ref()
            .and_then(|value| value.get("type"))
            .and_then(Value::as_str);
        if [self.event_type.as_deref(), payload_type]
            .into_iter()
            .flatten()
            .any(|kind| matches!(kind, "response.failed" | "response.incomplete" | "error"))
        {
            return false;
        }
        if payload
            .as_ref()
            .and_then(|value| value.pointer("/response/status"))
            .and_then(Value::as_str)
            .is_some_and(|status| status != "completed")
        {
            return false;
        }
        self.data_payload == "[DONE]"
            || matches!(
                payload_type.or(self.event_type.as_deref()),
                Some("message_stop" | "response.completed")
            )
    }
}

fn is_terminal_sse_event_type(event_type: &str) -> bool {
    matches!(
        event_type,
        "message_stop" | "response.completed" | "response.failed" | "response.incomplete" | "error"
    )
}

fn sse_data_payload_is_terminal(data: &str) -> bool {
    data == "[DONE]"
        || serde_json::from_str::<serde_json::Value>(data).is_ok_and(|value| {
            value
                .get("type")
                .and_then(serde_json::Value::as_str)
                .is_some_and(is_terminal_sse_event_type)
        })
}

#[cfg(test)]
fn stream_chunk_contains_sse_done(chunk: &[u8]) -> bool {
    let mut tracker = ClientVisibleStreamCompletionTracker::default();
    tracker.observe_chunk(chunk)
}

struct ObservedStreamFrame {
    frame: StreamFrame,
    observed_at: Instant,
}

#[derive(Clone)]
struct PostStopFrameReadBudget {
    remaining: Arc<AtomicUsize>,
}

impl PostStopFrameReadBudget {
    fn new() -> Self {
        Self {
            remaining: Arc::new(AtomicUsize::new(POST_STOP_FRAME_READ_BUDGET_INACTIVE)),
        }
    }

    fn activate(&self, already_buffered: usize) -> bool {
        let remaining = ANTHROPIC_POST_STOP_DRAIN_MAX_BYTES.saturating_sub(already_buffered);
        let activated = self
            .remaining
            .compare_exchange(
                POST_STOP_FRAME_READ_BUDGET_INACTIVE,
                remaining,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok();
        activated && already_buffered > ANTHROPIC_POST_STOP_DRAIN_MAX_BYTES
    }
}

struct PostStopLimitedStreamReader<S> {
    stream: S,
    current: Option<Bytes>,
    budget: PostStopFrameReadBudget,
}

impl<S> PostStopLimitedStreamReader<S> {
    fn new(stream: S, budget: PostStopFrameReadBudget) -> Self {
        Self {
            stream,
            current: None,
            budget,
        }
    }

    fn activate_post_stop_budget(&mut self, already_buffered: usize) -> bool {
        let over_limit = self.budget.activate(already_buffered);
        let remaining = self.budget.remaining.load(Ordering::Acquire);
        self.trim_current_to_budget(remaining, true);
        over_limit
    }

    fn trim_current_to_budget(&mut self, remaining: usize, detach_backing: bool) {
        if remaining == POST_STOP_FRAME_READ_BUDGET_INACTIVE {
            return;
        }
        if remaining == 0 {
            self.current = None;
            return;
        }
        if let Some(current) = self.current.as_mut() {
            if detach_backing || current.len() > remaining {
                let retained = current.len().min(remaining);
                // Detach even a small slice because it can retain a giant
                // producer allocation across post-stop backpressure.
                *current = Bytes::copy_from_slice(&current[..retained]);
            }
        }
    }
}

impl<S> AsyncRead for PostStopLimitedStreamReader<S>
where
    S: Stream<Item = Result<Bytes, IoError>> + Unpin,
{
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<Result<(), IoError>> {
        let this = self.get_mut();
        if buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }

        let mut empty_chunks = 0usize;
        loop {
            let remaining = this.budget.remaining.load(Ordering::Acquire);
            if remaining == 0 {
                this.current = None;
                return Poll::Ready(Ok(()));
            }
            this.trim_current_to_budget(remaining, false);

            if let Some(current) = this.current.as_mut() {
                let read = current.len().min(buf.remaining());
                if read > 0 {
                    buf.put_slice(&current.split_to(read));
                    if remaining != POST_STOP_FRAME_READ_BUDGET_INACTIVE {
                        let previous = this.budget.remaining.fetch_sub(read, Ordering::AcqRel);
                        debug_assert!(previous != POST_STOP_FRAME_READ_BUDGET_INACTIVE);
                        debug_assert!(previous >= read);
                    }
                }
                if current.is_empty() {
                    this.current = None;
                }
                if read > 0 {
                    return Poll::Ready(Ok(()));
                }
            }

            match Pin::new(&mut this.stream).poll_next(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Some(Ok(chunk))) if chunk.is_empty() => {
                    empty_chunks += 1;
                    if empty_chunks >= POST_STOP_MAX_EMPTY_CHUNKS_PER_POLL {
                        cx.waker().wake_by_ref();
                        return Poll::Pending;
                    }
                }
                Poll::Ready(Some(Ok(chunk))) => {
                    this.current = Some(chunk);
                    if remaining != POST_STOP_FRAME_READ_BUDGET_INACTIVE {
                        this.trim_current_to_budget(remaining, true);
                    }
                }
                Poll::Ready(Some(Err(err))) => return Poll::Ready(Err(err)),
                Poll::Ready(None) => return Poll::Ready(Ok(())),
            }
        }
    }
}

fn activate_post_stop_frame_read_budget<S>(
    lines: &mut FramedRead<PostStopLimitedStreamReader<S>, LinesCodec>,
) -> bool {
    let already_buffered = lines.read_buffer().len();
    let over_limit = already_buffered > ANTHROPIC_POST_STOP_DRAIN_MAX_BYTES;
    let reader_over_limit = lines.get_mut().activate_post_stop_budget(already_buffered);
    let retained = if over_limit { 0 } else { already_buffered };
    let mut bounded = bytes::BytesMut::with_capacity(retained);
    bounded.extend_from_slice(&lines.read_buffer()[..retained]);
    *lines.read_buffer_mut() = bounded;
    reader_over_limit || over_limit
}

async fn read_next_observed_stream_frame<R>(
    lines: &mut FramedRead<R, LinesCodec>,
) -> Result<Option<ObservedStreamFrame>, GatewayError>
where
    R: tokio::io::AsyncRead + Unpin,
{
    Ok(read_next_frame(lines)
        .await?
        .map(|frame| ObservedStreamFrame {
            frame,
            observed_at: Instant::now(),
        }))
}

async fn next_stream_frame<R>(
    buffered_frames: &mut VecDeque<ObservedStreamFrame>,
    lines: &mut FramedRead<R, LinesCodec>,
) -> Result<Option<ObservedStreamFrame>, GatewayError>
where
    R: tokio::io::AsyncRead + Unpin,
{
    if let Some(frame) = buffered_frames.pop_front() {
        return Ok(Some(frame));
    }
    read_next_observed_stream_frame(lines).await
}

fn serialized_stream_frame_len(frame: &StreamFrame) -> usize {
    serde_json::to_vec(frame).map_or(usize::MAX, |encoded| encoded.len())
}

fn execution_stream_frame_codec() -> LinesCodec {
    LinesCodec::new_with_max_length(crate::execution_runtime::MAX_EXECUTION_STREAM_FRAME_LINE_BYTES)
}

fn should_refresh_stream_usage_telemetry(
    previous: Option<&ExecutionTelemetry>,
    next: &ExecutionTelemetry,
) -> bool {
    let previous_ttfb = previous.and_then(|telemetry| telemetry.ttfb_ms);
    let previous_elapsed = previous.and_then(|telemetry| telemetry.elapsed_ms);
    let next_ttfb = next.ttfb_ms;
    let next_elapsed = next.elapsed_ms;

    (next_ttfb.is_some() && next_ttfb != previous_ttfb)
        || (next_elapsed.is_some() && next_elapsed != previous_elapsed)
}

fn stream_elapsed_ms_since(started_at: Instant) -> u64 {
    started_at.elapsed().as_millis().min(u128::from(u64::MAX)) as u64
}

fn stream_elapsed_ms_at(started_at: Instant, observed_at: Instant) -> u64 {
    observed_at
        .saturating_duration_since(started_at)
        .as_millis()
        .min(u128::from(u64::MAX)) as u64
}

fn first_stream_event_telemetry(
    stream_started_at: Instant,
    event_observed_at: Instant,
    upstream_telemetry: Option<&ExecutionTelemetry>,
) -> ExecutionTelemetry {
    let elapsed_ms = stream_elapsed_ms_at(stream_started_at, event_observed_at);
    ExecutionTelemetry {
        ttfb_ms: Some(elapsed_ms),
        elapsed_ms: Some(elapsed_ms),
        upstream_bytes: upstream_telemetry.and_then(|telemetry| telemetry.upstream_bytes),
    }
}

fn maybe_capture_first_stream_event_telemetry(
    stream_started_at: Instant,
    event_observed_at: Instant,
    upstream_telemetry: Option<&ExecutionTelemetry>,
    usage_stream_telemetry: &mut Option<ExecutionTelemetry>,
) -> bool {
    if usage_stream_telemetry
        .as_ref()
        .and_then(|telemetry| telemetry.ttfb_ms)
        .is_some()
    {
        return false;
    }

    *usage_stream_telemetry = Some(first_stream_event_telemetry(
        stream_started_at,
        event_observed_at,
        upstream_telemetry,
    ));
    true
}

fn usage_refresh_telemetry(
    upstream_telemetry: &ExecutionTelemetry,
    usage_stream_telemetry: Option<&ExecutionTelemetry>,
) -> ExecutionTelemetry {
    ExecutionTelemetry {
        ttfb_ms: usage_stream_telemetry.and_then(|telemetry| telemetry.ttfb_ms),
        elapsed_ms: upstream_telemetry.elapsed_ms,
        upstream_bytes: upstream_telemetry.upstream_bytes,
    }
}

fn maybe_record_first_stream_event_started(
    state: &AppState,
    lifecycle_seed: &LifecycleUsageSeed,
    status_code: u16,
    stream_started_at: Instant,
    event_observed_at: Instant,
    upstream_telemetry: Option<&ExecutionTelemetry>,
    usage_stream_telemetry: &mut Option<ExecutionTelemetry>,
) {
    if !maybe_capture_first_stream_event_telemetry(
        stream_started_at,
        event_observed_at,
        upstream_telemetry,
        usage_stream_telemetry,
    ) {
        return;
    }
    let Some(telemetry) = usage_stream_telemetry.as_ref() else {
        return;
    };
    state.usage_runtime.record_stream_started(
        state.usage_lifecycle_data_state().as_ref(),
        lifecycle_seed,
        status_code,
        Some(telemetry),
    );
}

fn build_terminal_stream_telemetry(
    stream_started_at: Instant,
    telemetry: Option<&ExecutionTelemetry>,
    usage_stream_telemetry: Option<&ExecutionTelemetry>,
    upstream_bytes: u64,
) -> ExecutionTelemetry {
    let current_elapsed_ms = stream_elapsed_ms_since(stream_started_at);
    let ttfb_ms = usage_stream_telemetry.and_then(|telemetry| telemetry.ttfb_ms);
    let prior_elapsed_ms = telemetry
        .and_then(|telemetry| telemetry.elapsed_ms)
        .or_else(|| usage_stream_telemetry.and_then(|telemetry| telemetry.elapsed_ms))
        .unwrap_or(0);
    let elapsed_ms = current_elapsed_ms
        .max(prior_elapsed_ms)
        .max(ttfb_ms.unwrap_or(0));
    ExecutionTelemetry {
        ttfb_ms,
        elapsed_ms: Some(elapsed_ms),
        upstream_bytes: Some(upstream_bytes),
    }
}

fn should_skip_direct_finalize_prefetch(
    direct_stream_finalize_kind: Option<&str>,
    content_type: Option<&str>,
    provider_api_format: &str,
    client_api_format: &str,
    has_private_stream_normalizer: bool,
    has_local_stream_rewriter: bool,
    force_prefetch: bool,
) -> bool {
    StreamCommitPolicy::for_response(
        direct_stream_finalize_kind.is_some(),
        content_type,
        provider_api_format,
        client_api_format,
        has_private_stream_normalizer,
        has_local_stream_rewriter,
        force_prefetch,
    )
    .commits_on_response_headers()
}

fn prefetched_openai_responses_body_has_output_boundary(body: &[u8]) -> bool {
    let Ok(text) = std::str::from_utf8(body) else {
        return true;
    };
    for line in text.lines() {
        let Some(data) = line.trim().strip_prefix("data:").map(str::trim) else {
            continue;
        };
        if data.is_empty() {
            continue;
        }
        if data == "[DONE]" {
            return true;
        }
        let Ok(event) = serde_json::from_str::<Value>(data) else {
            continue;
        };
        let event_type = event.get("type").and_then(Value::as_str).map(str::trim);
        if !event_type.is_some_and(|event_type| {
            matches!(
                event_type,
                "response.created" | "response.in_progress" | "response.queued"
            )
        }) {
            return true;
        }
    }
    false
}

fn should_probe_success_failover_before_stream(headers: &BTreeMap<String, String>) -> bool {
    let content_type = headers
        .get("content-type")
        .map(String::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or_default()
        .to_ascii_lowercase();

    content_type.contains("json") || content_type.ends_with("+json")
}

async fn record_prefetch_success_failover(
    state: &AppState,
    plan: &ExecutionPlan,
    report_context: Option<&Value>,
    elapsed_ms: u64,
) {
    let finished_at = current_request_candidate_unix_ms();
    record_local_request_candidate_status(
        state,
        plan,
        report_context,
        SchedulerRequestCandidateStatusUpdate {
            status: RequestCandidateStatus::Failed,
            status_code: Some(200),
            error_type: Some("success_failover_pattern".to_string()),
            error_message: Some("HTTP 200 response matched a precommit failover rule".to_string()),
            latency_ms: Some(elapsed_ms),
            started_at_unix_ms: None,
            finished_at_unix_ms: Some(finished_at),
        },
    )
    .await;
}

async fn probe_local_stream_success_failover_text<R>(
    buffered_frames: &mut VecDeque<ObservedStreamFrame>,
    lines: &mut FramedRead<R, LinesCodec>,
) -> Result<Option<String>, GatewayError>
where
    R: tokio::io::AsyncRead + Unpin,
{
    while let Some(observed_frame) = read_next_observed_stream_frame(lines).await? {
        let probe_text = match &observed_frame.frame.payload {
            StreamFramePayload::Data { chunk_b64, text } => {
                match decode_stream_data_chunk(chunk_b64.as_deref(), text.as_deref()) {
                    Ok(chunk) if !chunk.is_empty() => {
                        Some(String::from_utf8_lossy(&chunk).into_owned())
                    }
                    Ok(_) | Err(_) => None,
                }
            }
            StreamFramePayload::Error { .. } | StreamFramePayload::Eof { .. } => None,
            StreamFramePayload::Headers { .. } | StreamFramePayload::Telemetry { .. } => None,
        };
        buffered_frames.push_back(observed_frame);
        if probe_text.is_some() {
            return Ok(probe_text);
        }
    }

    Ok(None)
}

#[cfg(test)]
async fn execute_stream_from_frame_stream(
    state: &AppState,
    plan: ExecutionPlan,
    trace_id: &str,
    decision: &GatewayControlDecision,
    plan_kind: &str,
    report_kind: Option<String>,
    report_context: Option<serde_json::Value>,
    candidate_started_unix_secs: u64,
    stream_started_at: Instant,
    stage_trace: RequestStageTrace,
    lifecycle_pending_recorded: bool,
    frame_stream: BoxStream<'static, Result<Bytes, IoError>>,
    in_flight_guard: Option<ProviderPoolInFlightGuard>,
) -> Result<Option<Response<Body>>, GatewayError> {
    execute_stream_from_frame_stream_with_retry_scope(
        state,
        plan,
        trace_id,
        decision,
        plan_kind,
        report_kind,
        report_context,
        candidate_started_unix_secs,
        stream_started_at,
        stage_trace,
        lifecycle_pending_recorded,
        frame_stream,
        false,
        in_flight_guard,
        None,
        None,
        None,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn execute_stream_from_frame_stream_with_retry_scope(
    state: &AppState,
    plan: ExecutionPlan,
    trace_id: &str,
    decision: &GatewayControlDecision,
    plan_kind: &str,
    report_kind: Option<String>,
    report_context: Option<serde_json::Value>,
    candidate_started_unix_secs: u64,
    stream_started_at: Instant,
    mut stage_trace: RequestStageTrace,
    lifecycle_pending_recorded: bool,
    frame_stream: BoxStream<'static, Result<Bytes, IoError>>,
    stream_precommit_committed: bool,
    in_flight_guard: Option<ProviderPoolInFlightGuard>,
    mut retry_scope_out: Option<&mut AiAttemptRetryScope>,
    mut retry_fallback_out: Option<&mut Option<Response<Body>>>,
    fallback_response_observation: Option<ExecutionResponseObservation>,
) -> Result<Option<Response<Body>>, GatewayError> {
    let request_id = plan.request_id.as_str();
    let request_id_for_log = short_request_id(request_id);
    let candidate_id = plan.candidate_id.as_deref();
    let provider_name = plan.provider_name.as_deref().unwrap_or("-");
    let model_name = plan.model_name.as_deref().unwrap_or("-");
    let lifecycle_seed = build_lifecycle_usage_seed(&plan, report_context.as_ref());
    if !lifecycle_pending_recorded {
        record_stream_pending_lifecycle(state, &lifecycle_seed, &mut stage_trace).await;
    }
    let max_stream_body_buffer_bytes = resolve_stream_body_buffer_limit(state).await;
    let request_candidate_status_snapshot =
        snapshot_local_request_candidate_status(&plan, report_context.as_ref());
    let candidate_index = parse_request_candidate_report_context(report_context.as_ref())
        .and_then(|context| context.candidate_index)
        .map(|value| value.to_string())
        .unwrap_or_else(|| "-".to_string());
    let reader = PostStopLimitedStreamReader::new(frame_stream, PostStopFrameReadBudget::new());
    let mut lines = FramedRead::new(reader, execution_stream_frame_codec());

    let first_frame_started_at = Instant::now();
    let first_frame = read_next_frame(&mut lines).await?.ok_or_else(|| {
        GatewayError::Internal("execution runtime stream ended before headers frame".to_string())
    })?;
    observe_gateway_stage_trace_ms(
        &mut stage_trace,
        "stream_first_frame",
        first_frame_started_at.elapsed().as_millis() as u64,
    );
    let StreamFramePayload::Headers {
        status_code,
        mut headers,
        response_observation,
    } = first_frame.payload
    else {
        return Err(GatewayError::Internal(
            "execution runtime stream must start with headers frame".to_string(),
        ));
    };
    let response_observation = response_observation
        .or(fallback_response_observation)
        .unwrap_or(ExecutionResponseObservation {
            request_started_at_unix_ms: candidate_started_unix_secs,
            response_headers_observed_at_unix_ms: current_request_candidate_unix_ms(),
            request_order_id: uuid::Uuid::now_v7().to_string(),
        });
    let mut report_context = attach_provider_response_headers_to_report_context(
        report_context,
        &headers,
        response_observation.request_started_at_unix_ms,
        response_observation.response_headers_observed_at_unix_ms,
        &response_observation.request_order_id,
    );
    spawn_local_oauth_success_effect(
        state.clone(),
        &plan,
        report_context.as_ref(),
        LocalOAuthSuccessEffect {
            status_code,
            request_started_at_unix_ms: Some(response_observation.request_started_at_unix_ms),
            request_order_id: Some(&response_observation.request_order_id),
        },
    );
    if status_code == 200 {
        seed_kiro_simulated_cache_enabled(state, &plan, &mut report_context).await;
        if kiro_simulated_cache_enabled_from_report_context(report_context.as_ref()) {
            seed_kiro_report_context_input_tokens(&plan, &mut report_context);
        }
        seed_kiro_report_context_prompt_cache_usage(state, &plan, &mut report_context).await;
    }
    let mut buffered_frames = VecDeque::new();
    let mut stream_terminal_summary: Option<ExecutionStreamTerminalSummary> = None;
    let direct_stream_finalize_kind = resolve_core_stream_direct_finalize_report_kind(plan_kind);
    if status_code == 200
        && direct_stream_finalize_kind.is_none()
        && should_probe_success_failover_before_stream(&headers)
    {
        let success_probe_text =
            probe_local_stream_success_failover_text(&mut buffered_frames, &mut lines).await?;
        if should_retry_next_local_candidate_stream(
            state,
            &plan,
            plan_kind,
            report_context.as_ref(),
            status_code,
            success_probe_text.as_deref(),
        )
        .await
        {
            let terminal_unix_secs = current_request_candidate_unix_ms();
            record_local_request_candidate_status(
                state,
                &plan,
                report_context.as_ref(),
                SchedulerRequestCandidateStatusUpdate {
                    status: RequestCandidateStatus::Failed,
                    status_code: Some(status_code),
                    error_type: Some("success_failover_pattern".to_string()),
                    error_message: Some(
                        "execution runtime stream matched provider success failover rule"
                            .to_string(),
                    ),
                    latency_ms: None,
                    started_at_unix_ms: Some(candidate_started_unix_secs),
                    finished_at_unix_ms: Some(terminal_unix_secs),
                },
            )
            .await;
            warn!(
                event_name = "local_stream_candidate_retry_scheduled",
                log_type = "event",
                trace_id = %trace_id,
                request_id = %request_id_for_log,
                status_code,
                provider_name = provider_name,
                endpoint_id = %plan.endpoint_id,
                key_id = %plan.key_id,
                model_name,
                candidate_index = candidate_index.as_str(),
                "gateway local stream decision retrying next candidate after success failover rule match"
            );
            return Ok(None);
        }
    }

    let stream_error_finalize_kind =
        resolve_core_stream_error_finalize_report_kind(plan_kind, status_code);

    if !(200..300).contains(&status_code) {
        let provider_error_body = collect_error_body(&mut lines).await?;
        let private_error_body_json = extract_provider_private_stream_error_body(
            report_context.as_ref(),
            &provider_error_body,
        );
        let provider_private_error_decoded = private_error_body_json.is_some();
        let synthetic_body_json = (!provider_private_error_decoded
            && should_synthesize_non_success_stream_error_body(status_code, &provider_error_body))
        .then(|| build_synthetic_non_success_stream_error_body(status_code, &headers));
        let (provider_body_json, provider_body_base64) =
            if let Some(error_body_json) = private_error_body_json {
                (Some(error_body_json), None)
            } else {
                decode_stream_error_body(&headers, &provider_error_body)
            };
        let client_status_code = stream_client_error_status_code_for_upstream_status(status_code);
        let wrapped_binary_body_json = if provider_private_error_decoded {
            None
        } else {
            wrap_non_json_binary_stream_error_for_client(plan_kind, &headers, &provider_error_body)?
        };
        let (client_body_json, client_error_body, payload_client_body_json) =
            if let Some(body_json) = synthetic_body_json.or(wrapped_binary_body_json) {
                let body_bytes = serde_json::to_vec(&body_json)
                    .map_err(|err| GatewayError::Internal(err.to_string()))?;
                (Some(body_json.clone()), body_bytes, Some(body_json))
            } else if provider_private_error_decoded {
                let body_json = provider_body_json.clone().ok_or_else(|| {
                    GatewayError::Internal(
                        "decoded provider private stream error body is missing".to_string(),
                    )
                })?;
                let body_bytes = serde_json::to_vec(&body_json)
                    .map_err(|err| GatewayError::Internal(err.to_string()))?;
                (Some(body_json), body_bytes, None)
            } else {
                (
                    provider_body_json.clone(),
                    provider_error_body.clone(),
                    provider_body_json.clone(),
                )
            };
        let error_response_text =
            local_failover_response_text(client_body_json.as_ref(), &client_error_body, None);
        let failover_analysis = resolve_local_candidate_failover_analysis_stream(
            state,
            &plan,
            report_context.as_ref(),
            status_code,
            error_response_text.as_deref(),
        )
        .await;
        apply_local_execution_effect(
            state,
            LocalExecutionEffectContext {
                plan: &plan,
                report_context: report_context.as_ref(),
            },
            LocalExecutionEffect::AttemptFailure(LocalAttemptFailureEffect {
                status_code,
                classification: failover_analysis.classification,
            }),
        )
        .await;
        apply_local_execution_effect(
            state,
            LocalExecutionEffectContext {
                plan: &plan,
                report_context: report_context.as_ref(),
            },
            LocalExecutionEffect::AdaptiveRateLimit(LocalAdaptiveRateLimitEffect {
                status_code,
                classification: failover_analysis.classification,
                headers: Some(&headers),
            }),
        )
        .await;
        apply_local_execution_effect(
            state,
            LocalExecutionEffectContext {
                plan: &plan,
                report_context: report_context.as_ref(),
            },
            LocalExecutionEffect::HealthFailure(LocalHealthFailureEffect {
                status_code,
                classification: failover_analysis.classification,
            }),
        )
        .await;
        apply_local_execution_effect(
            state,
            LocalExecutionEffectContext {
                plan: &plan,
                report_context: report_context.as_ref(),
            },
            LocalExecutionEffect::OauthInvalidation(LocalOAuthInvalidationEffect {
                status_code,
                response_text: error_response_text.as_deref(),
            }),
        )
        .await;
        apply_local_execution_effect(
            state,
            LocalExecutionEffectContext {
                plan: &plan,
                report_context: report_context.as_ref(),
            },
            LocalExecutionEffect::PoolError(LocalPoolErrorEffect {
                status_code,
                classification: failover_analysis.classification,
                headers: &headers,
                error_body: error_response_text.as_deref(),
            }),
        )
        .await;
        let failover_decision = failover_analysis.decision;
        debug!(
            event_name = "execution_runtime_stream_failover_decided",
            log_type = "debug",
            trace_id = %trace_id,
            request_id = %request_id_for_log,
            candidate_id = ?candidate_id,
            plan_kind,
            status_code,
            provider_name,
            endpoint_id = %plan.endpoint_id,
            key_id = %plan.key_id,
            model_name,
            candidate_index = candidate_index.as_str(),
            failover_decision = failover_decision.as_str(),
            "gateway resolved execution runtime stream failover decision"
        );
        if matches!(failover_decision, LocalFailoverDecision::RetryNextCandidate) {
            let failure_disposition = classify_failure_disposition(
                &plan.provider_api_format,
                failover_analysis.classification,
                status_code,
                failover_analysis.preserve_on_exhaustion,
            );
            if let Some(retry_scope) = retry_scope_out.as_deref_mut() {
                *retry_scope = ai_attempt_retry_scope_from_failure_disposition(failure_disposition);
            }
            if failure_disposition.preserve_upstream_error {
                if let Some(retry_fallback) = retry_fallback_out.as_deref_mut() {
                    *retry_fallback = Some(
                        build_exhaustion_fallback(
                            state,
                            ExhaustionFallbackInput {
                                plan: &plan,
                                report_context: report_context.as_ref(),
                                trace_id,
                                decision,
                                status_code,
                                provider_headers: &headers,
                                provider_body: &provider_error_body,
                                provider_body_json: provider_body_json.as_ref(),
                                rebuild_client_body: false,
                                request_id: Some(request_id),
                                candidate_id,
                            },
                        )
                        .await?,
                    );
                }
            }
            let terminal_unix_secs = current_request_candidate_unix_ms();
            let error_trace_report_context = with_stream_error_trace_context(
                report_context.as_ref(),
                status_code,
                &headers,
                provider_body_json.as_ref(),
                &provider_error_body,
                error_response_text.as_deref(),
                failover_analysis,
            );
            record_local_request_candidate_status(
                state,
                &plan,
                error_trace_report_context
                    .as_ref()
                    .or(report_context.as_ref()),
                SchedulerRequestCandidateStatusUpdate {
                    status: RequestCandidateStatus::Failed,
                    status_code: Some(status_code),
                    error_type: Some("retryable_upstream_status".to_string()),
                    error_message: Some(format!(
                        "execution runtime stream returned retryable status {status_code}"
                    )),
                    latency_ms: None,
                    started_at_unix_ms: Some(candidate_started_unix_secs),
                    finished_at_unix_ms: Some(terminal_unix_secs),
                },
            )
            .await;
            warn!(
                event_name = "local_stream_candidate_retry_scheduled",
                log_type = "event",
                trace_id = %trace_id,
                request_id = %request_id_for_log,
                status_code,
                provider_name = provider_name,
                endpoint_id = %plan.endpoint_id,
                key_id = %plan.key_id,
                model_name,
                candidate_index = candidate_index.as_str(),
                "gateway local stream decision retrying next candidate after retryable execution runtime status"
            );
            return Ok(None);
        }

        if !matches!(failover_decision, LocalFailoverDecision::StopLocalFailover)
            && should_fallback_to_control_stream(
                plan_kind,
                status_code,
                stream_error_finalize_kind.is_some(),
            )
        {
            let terminal_unix_secs = current_request_candidate_unix_ms();
            let error_trace_report_context = with_stream_error_trace_context(
                report_context.as_ref(),
                status_code,
                &headers,
                provider_body_json.as_ref(),
                &provider_error_body,
                error_response_text.as_deref(),
                failover_analysis,
            );
            record_local_request_candidate_status(
                state,
                &plan,
                error_trace_report_context
                    .as_ref()
                    .or(report_context.as_ref()),
                SchedulerRequestCandidateStatusUpdate {
                    status: RequestCandidateStatus::Failed,
                    status_code: Some(status_code),
                    error_type: Some("control_fallback".to_string()),
                    error_message: Some(format!(
                        "stream decision fell back to control after status {status_code}"
                    )),
                    latency_ms: None,
                    started_at_unix_ms: Some(candidate_started_unix_secs),
                    finished_at_unix_ms: Some(terminal_unix_secs),
                },
            )
            .await;
            return Ok(None);
        }

        let mut client_headers = if (300..400).contains(&status_code) {
            let mut headers = synthetic_error_response_headers(headers.clone());
            headers.insert(
                "x-aether-upstream-status".to_string(),
                status_code.to_string(),
            );
            headers
        } else {
            headers.clone()
        };
        if provider_private_error_decoded {
            client_headers.remove("content-encoding");
            client_headers.remove("content-length");
            client_headers.insert("content-type".to_string(), "application/json".to_string());
        }
        apply_endpoint_response_header_rules(
            state,
            &plan,
            &mut client_headers,
            client_body_json.as_ref(),
        )
        .await?;

        let client_response_headers = client_headers.clone();
        let error_trace_report_context = with_stream_error_trace_context(
            report_context.as_ref(),
            status_code,
            &headers,
            provider_body_json.as_ref(),
            &provider_error_body,
            error_response_text.as_deref(),
            failover_analysis,
        );
        let payload = build_stream_error_sync_payload(
            trace_id,
            stream_error_finalize_kind
                .as_deref()
                .or(report_kind.as_deref())
                .unwrap_or_default()
                .to_string(),
            error_trace_report_context.or(report_context),
            status_code,
            headers.clone(),
            provider_body_json,
            provider_body_base64,
            client_headers,
            payload_client_body_json,
            None,
        );
        record_sync_terminal_usage_with_handoff(
            state,
            &plan,
            payload.report_context.as_ref(),
            &payload,
        )
        .await;
        let terminal_unix_secs = current_request_candidate_unix_ms();
        record_local_request_candidate_status(
            state,
            &plan,
            payload.report_context.as_ref(),
            SchedulerRequestCandidateStatusUpdate {
                status: RequestCandidateStatus::Failed,
                status_code: Some(status_code),
                error_type: Some("execution_runtime_stream_non_success_status".to_string()),
                error_message: Some(format!(
                    "execution runtime stream returned non-success status {status_code}"
                )),
                latency_ms: None,
                started_at_unix_ms: Some(candidate_started_unix_secs),
                finished_at_unix_ms: Some(terminal_unix_secs),
            },
        )
        .await;
        if stream_error_finalize_kind.is_some() {
            let response =
                submit_local_core_error_or_sync_finalize(state, trace_id, decision, payload)
                    .await?;
            return Ok(Some(attach_control_metadata_headers(
                response,
                Some(request_id),
                candidate_id,
            )?));
        }
        let response = if (300..400).contains(&status_code) {
            build_client_response_from_parts_with_mutator(
                client_status_code,
                &client_response_headers,
                Body::from(client_error_body),
                trace_id,
                Some(decision),
                |headers| {
                    headers.insert(
                        http::HeaderName::from_static("x-aether-upstream-status"),
                        http::HeaderValue::from_str(&status_code.to_string())
                            .map_err(|error| GatewayError::Internal(error.to_string()))?,
                    );
                    Ok(())
                },
            )?
        } else {
            build_client_response_from_parts(
                client_status_code,
                &client_response_headers,
                Body::from(client_error_body),
                trace_id,
                Some(decision),
            )?
        };
        return Ok(Some(attach_control_metadata_headers(
            response,
            Some(request_id),
            candidate_id,
        )?));
    }

    let normalized_stream_report_context =
        normalize_provider_private_report_context(report_context.as_ref());
    // Observers follow the live protocol stream across prefetch and transfer.
    // Diagnostic capture limits must never determine parser state.
    let stream_usage_report_context = normalized_stream_report_context.clone().or_else(|| {
        Some(json!({
            "provider_api_format": plan.provider_api_format.as_str(),
            "client_api_format": plan.client_api_format.as_str(),
        }))
    });
    let mut stream_usage_observer = stream_usage_report_context
        .as_ref()
        .map(|_| StreamingStandardTerminalObserver::default());
    let mut stream_usage_observer_buffered =
        StreamUsageObservationBuffer::new(max_stream_body_buffer_bytes);
    let mut provider_error_inspection = ProviderStreamErrorInspection::default();
    let mut prefetched_provider_error = None;
    let upstream_headers = headers.clone();
    let mut private_stream_normalizer =
        maybe_build_provider_private_stream_normalizer(report_context.as_ref());
    let mut local_stream_rewriter =
        maybe_build_stream_response_rewriter(normalized_stream_report_context.as_ref());
    if private_stream_normalizer.is_some() || local_stream_rewriter.is_some() {
        headers.remove("content-encoding");
        headers.remove("content-length");
        headers.insert("content-type".to_string(), "text/event-stream".to_string());
    }
    let upstream_content_type = upstream_headers.get("content-type").map(String::as_str);
    let normalized_declared_stream_headers = private_stream_normalizer.is_none()
        && local_stream_rewriter.is_none()
        && should_normalize_declared_stream_response_headers(
            plan_kind,
            status_code,
            &upstream_headers,
            report_context.as_ref(),
        );
    if normalized_declared_stream_headers {
        normalize_declared_stream_response_headers(&mut headers);
        debug!(
            event_name = "execution_runtime_stream_content_type_corrected",
            log_type = "debug",
            trace_id = %trace_id,
            request_id = %request_id_for_log,
            candidate_id = ?candidate_id,
            plan_kind,
            provider_name,
            endpoint_id = %plan.endpoint_id,
            key_id = %plan.key_id,
            model_name,
            candidate_index = candidate_index.as_str(),
            upstream_content_type = upstream_content_type.unwrap_or("-"),
            "gateway normalized declared upstream stream response headers for the client"
        );
    }
    let prefetch_for_cyber_failover =
        is_openai_responses_family_format(plan.provider_api_format.as_str())
            && crate::orchestration::routing_execution_policy_from_report_context(
                report_context.as_ref(),
            )
            .is_some_and(|policy| policy.cyber_continue_failover);
    let prefetch_failover_policy =
        crate::orchestration::resolve_local_failover_policy(state, &plan, report_context.as_ref())
            .await;
    let prefetch_success_patterns = prefetch_failover_policy
        .routing_rules
        .success_failover_patterns
        .iter()
        .map(|rule| (&rule.pattern, &rule.status_codes))
        .chain(
            prefetch_failover_policy
                .success_failover_patterns
                .iter()
                .map(|rule| (&rule.pattern, &rule.status_codes)),
        )
        .filter(|_| {
            !crate::execution_runtime::fallback::openai_image_success_disables_local_success_failover(
                &plan,
                status_code,
            )
        })
        .filter(|(_, status_codes)| status_codes.is_empty() || status_codes.contains(&200))
        .filter_map(|(pattern, _)| regex::Regex::new(pattern.trim()).ok())
        .collect::<Vec<_>>();
    let stream_commit_policy = StreamCommitPolicy::for_response(
        direct_stream_finalize_kind.is_some(),
        upstream_content_type,
        plan.provider_api_format.as_str(),
        plan.client_api_format.as_str(),
        private_stream_normalizer.is_some(),
        local_stream_rewriter.is_some(),
        prefetch_for_cyber_failover || !prefetch_success_patterns.is_empty(),
    )
    .with_precommit_wait(Duration::from_millis(
        plan.timeouts
            .as_ref()
            .and_then(|timeouts| timeouts.first_byte_ms)
            .unwrap_or(30_000)
            .max(1),
    ));
    let reuse_committed_precommit = stream_precommit_committed
        && stream_commit_policy.is_native_anthropic()
        && prefetch_success_patterns.is_empty();
    let skip_direct_finalize_prefetch =
        stream_commit_policy.commits_on_response_headers() || reuse_committed_precommit;
    let limit_direct_finalize_prefetch =
        should_limit_direct_finalize_prefetch(plan_kind, local_stream_rewriter.is_some())
            || stream_commit_policy.requires_bounded_frame_wait()
            || !prefetch_success_patterns.is_empty();
    let mut stream_commit_gate = StreamCommitGate::new(stream_commit_policy);
    let mut prefetch_client_completion_tracker = ClientVisibleStreamCompletionTracker::default();
    let mut prefetched_client_visible_stream_completed = false;
    let mut prefetched_anthropic_message_stop_observed_at = None;
    let mut prefetched_anthropic_post_stop_buffer_over_limit = false;
    if reuse_committed_precommit {
        stream_commit_gate.commit();
    }
    let mut prefetched_chunks: Vec<Bytes> = Vec::new();
    let mut provider_prefetched_body = StreamBodyCapture::default();
    let mut provider_prefetched_bytes = 0_u64;
    let mut provider_prefetched_body_truncated = false;
    let mut prefetched_body = Vec::new();
    let mut prefetched_inspection_body = Vec::new();
    let mut prefetched_inspection_body_truncated = false;
    let mut prefetched_telemetry: Option<ExecutionTelemetry> = None;
    let mut prefetched_usage_telemetry: Option<ExecutionTelemetry> = None;
    let mut reached_eof = false;
    let mut sync_json_stream_bridge_active = false;
    let precommit_started_at = Instant::now();
    if skip_direct_finalize_prefetch {
        debug!(
            event_name = "execution_runtime_stream_prefetch_skipped",
            log_type = "debug",
            trace_id = %trace_id,
            request_id = %request_id_for_log,
            candidate_id = ?candidate_id,
            plan_kind,
            provider_name,
            endpoint_id = %plan.endpoint_id,
            key_id = %plan.key_id,
            model_name,
            candidate_index = candidate_index.as_str(),
            content_type = upstream_content_type.unwrap_or("-"),
            provider_api_format = plan.provider_api_format.as_str(),
            client_api_format = plan.client_api_format.as_str(),
            "gateway skipped direct finalize prefetch for same-format passthrough stream"
        );
    }
    if let Some(report_kind) = direct_stream_finalize_kind
        .as_ref()
        .filter(|_| !skip_direct_finalize_prefetch)
    {
        while (stream_commit_policy.requires_bounded_frame_wait()
            || prefetched_chunks.len() < MAX_STREAM_PREFETCH_FRAMES)
            && prefetched_inspection_body.len() < MAX_STREAM_PREFETCH_BYTES
        {
            let next_frame_result = if limit_direct_finalize_prefetch {
                let prefetch_timeout = stream_commit_policy
                    .max_precommit_wait()
                    .map(|max_wait| max_wait.saturating_sub(precommit_started_at.elapsed()))
                    .unwrap_or(REWRITTEN_STREAM_PREFETCH_TIMEOUT);
                if prefetch_timeout.is_zero() && !stream_commit_policy.requires_bounded_frame_wait()
                {
                    stream_commit_gate.commit();
                    debug!(
                        event_name = "execution_runtime_stream_prefetch_limited",
                        log_type = "debug",
                        trace_id = %trace_id,
                        request_id = %request_id_for_log,
                        candidate_id = ?candidate_id,
                        plan_kind,
                        report_kind,
                        provider_name,
                        endpoint_id = %plan.endpoint_id,
                        key_id = %plan.key_id,
                        model_name,
                        candidate_index = candidate_index.as_str(),
                        timeout_ms = stream_commit_policy
                            .max_precommit_wait()
                            .unwrap_or(REWRITTEN_STREAM_PREFETCH_TIMEOUT)
                            .as_millis() as u64,
                        "gateway reached bounded stream precommit deadline"
                    );
                    break;
                }
                match tokio::time::timeout(
                    prefetch_timeout,
                    next_stream_frame(&mut buffered_frames, &mut lines),
                )
                .await
                {
                    Ok(result) => result,
                    Err(_) => {
                        if stream_commit_policy.requires_bounded_frame_wait() {
                            let failure = build_stream_transport_failure_report(
                                "first_byte_timeout", "Upstream did not produce a semantic event before the first byte deadline", 504,
                            );
                            return handle_prefetch_stream_failure(
                                state,
                                trace_id,
                                decision,
                                &plan,
                                report_context,
                                request_id,
                                candidate_id,
                                report_kind,
                                upstream_headers.clone(),
                                prefetched_usage_telemetry.clone(),
                                &provider_prefetched_body,
                                candidate_started_unix_secs,
                                stream_elapsed_ms_since(stream_started_at),
                                failure,
                                retry_scope_out.as_deref_mut(),
                                retry_fallback_out.as_deref_mut(),
                            )
                            .await;
                        }
                        stream_commit_gate.commit();
                        debug!(
                            event_name = "execution_runtime_stream_prefetch_limited",
                            log_type = "debug",
                            trace_id = %trace_id,
                            request_id = %request_id_for_log,
                            candidate_id = ?candidate_id,
                            plan_kind,
                            report_kind,
                            provider_name,
                            endpoint_id = %plan.endpoint_id,
                            key_id = %plan.key_id,
                            model_name,
                            candidate_index = candidate_index.as_str(),
                            timeout_ms = stream_commit_policy
                                .max_precommit_wait()
                                .unwrap_or(REWRITTEN_STREAM_PREFETCH_TIMEOUT)
                                .as_millis() as u64,
                            "gateway stopped bounded stream prefetch before client-visible body"
                        );
                        break;
                    }
                }
            } else {
                next_stream_frame(&mut buffered_frames, &mut lines).await
            };
            let Some(observed_frame) = (match next_frame_result {
                Ok(frame) => frame,
                Err(err) => {
                    let failure = build_stream_failure_report(
                        "execution_runtime_stream_frame_decode_error",
                        format!("failed to decode execution runtime stream frame: {err:?}"),
                        502,
                    );
                    return handle_prefetch_stream_failure(
                        state,
                        trace_id,
                        decision,
                        &plan,
                        report_context,
                        request_id,
                        candidate_id,
                        report_kind,
                        upstream_headers.clone(),
                        prefetched_usage_telemetry.clone(),
                        &provider_prefetched_body,
                        candidate_started_unix_secs,
                        stream_elapsed_ms_since(stream_started_at),
                        failure,
                        None,
                        None,
                    )
                    .await;
                }
            }) else {
                if stream_commit_policy.requires_bounded_frame_wait()
                    && stream_commit_gate.is_uncommitted()
                {
                    let error_body_json = anthropic_premature_eof_error_body(
                        "upstream stream ended before the first semantic event",
                    );
                    let error_status_code = anthropic_error_status_code(&error_body_json);
                    return handle_prefetch_provider_private_stream_error(
                        state,
                        trace_id,
                        decision,
                        &plan,
                        report_context,
                        request_id,
                        candidate_id,
                        report_kind,
                        upstream_headers.clone(),
                        prefetched_usage_telemetry.clone(),
                        &provider_prefetched_body,
                        status_code,
                        error_status_code,
                        error_body_json,
                        retry_scope_out.as_deref_mut(),
                        None,
                    )
                    .await;
                }
                reached_eof = true;
                break;
            };
            let frame_observed_at = observed_frame.observed_at;
            match observed_frame.frame.payload {
                StreamFramePayload::Data { chunk_b64, text } => {
                    if maybe_capture_first_stream_event_telemetry(
                        stream_started_at,
                        frame_observed_at,
                        prefetched_telemetry.as_ref(),
                        &mut prefetched_usage_telemetry,
                    ) {
                        observe_gateway_stage_trace_ms(
                            &mut stage_trace,
                            "stream_first_data",
                            stream_elapsed_ms_at(stream_started_at, frame_observed_at),
                        );
                        state.usage_runtime.record_stream_started(
                            state.usage_lifecycle_data_state().as_ref(),
                            &lifecycle_seed,
                            status_code,
                            prefetched_usage_telemetry.as_ref(),
                        );
                    }
                    let mut chunk =
                        match decode_stream_data_chunk(chunk_b64.as_deref(), text.as_deref()) {
                            Ok(chunk) => chunk,
                            Err(err) => {
                                let failure = build_stream_failure_report(
                                    "execution_runtime_stream_chunk_decode_error",
                                    format!(
                                        "failed to decode execution runtime stream chunk: {err:?}"
                                    ),
                                    502,
                                );
                                return handle_prefetch_stream_failure(
                                    state,
                                    trace_id,
                                    decision,
                                    &plan,
                                    report_context,
                                    request_id,
                                    candidate_id,
                                    report_kind,
                                    upstream_headers.clone(),
                                    prefetched_usage_telemetry.clone(),
                                    &prefetched_body,
                                    candidate_started_unix_secs,
                                    stream_elapsed_ms_since(stream_started_at),
                                    failure,
                                    None,
                                    None,
                                )
                                .await;
                            }
                        };

                    if chunk.is_empty() {
                        continue;
                    }
                    if stream_commit_policy.is_native_anthropic() {
                        if prefetched_client_visible_stream_completed {
                            continue;
                        }
                        if let Some(terminal_end) = prefetch_client_completion_tracker
                            .observe_anthropic_message_stop_terminal_end(&chunk)
                        {
                            chunk.truncate(terminal_end);
                            prefetched_client_visible_stream_completed = true;
                            prefetched_anthropic_message_stop_observed_at
                                .get_or_insert_with(Instant::now);
                            prefetched_anthropic_post_stop_buffer_over_limit |=
                                activate_post_stop_frame_read_budget(&mut lines);
                        }
                    }

                    provider_prefetched_bytes =
                        provider_prefetched_bytes.saturating_add(chunk.len() as u64);
                    append_budgeted_stream_capture_bytes(
                        &mut provider_prefetched_body,
                        &chunk,
                        max_stream_body_buffer_bytes,
                        &mut provider_prefetched_body_truncated,
                    );
                    append_stream_capture_bytes(
                        &mut prefetched_inspection_body,
                        &chunk,
                        MAX_STREAM_PREFETCH_BYTES,
                        &mut prefetched_inspection_body_truncated,
                    );

                    let semantic_commit_ready =
                        match stream_commit_gate.observe_provider_bytes(&chunk) {
                            StreamPrecommitObservation::Pending => false,
                            StreamPrecommitObservation::Commit => true,
                            StreamPrecommitObservation::UpstreamError {
                                status_code: error_status_code,
                                body_json: error_body_json,
                            } => {
                                let error_status_code = if plan
                                    .provider_api_format
                                    .eq_ignore_ascii_case("claude:messages")
                                {
                                    anthropic_error_status_code(&error_body_json)
                                } else {
                                    error_status_code
                                };
                                return handle_prefetch_provider_private_stream_error(
                                    state,
                                    trace_id,
                                    decision,
                                    &plan,
                                    report_context,
                                    request_id,
                                    candidate_id,
                                    report_kind,
                                    upstream_headers.clone(),
                                    prefetched_usage_telemetry.clone(),
                                    &provider_prefetched_body,
                                    status_code,
                                    error_status_code,
                                    error_body_json,
                                    retry_scope_out.as_deref_mut(),
                                    retry_fallback_out.as_deref_mut(),
                                )
                                .await;
                            }
                        };

                    if !semantic_commit_ready || private_stream_normalizer.is_some() {
                        if let Some(error_body_json) = extract_provider_private_stream_error_body(
                            report_context.as_ref(),
                            &prefetched_inspection_body,
                        ) {
                            let error_status_code = resolve_provider_stream_error_status_code(
                                plan.provider_api_format.as_str(),
                                status_code,
                                &error_body_json,
                            );
                            return handle_prefetch_provider_private_stream_error(
                                state,
                                trace_id,
                                decision,
                                &plan,
                                report_context,
                                request_id,
                                candidate_id,
                                report_kind,
                                upstream_headers.clone(),
                                prefetched_usage_telemetry.clone(),
                                &provider_prefetched_body,
                                status_code,
                                error_status_code,
                                error_body_json,
                                retry_scope_out.as_deref_mut(),
                                retry_fallback_out.as_deref_mut(),
                            )
                            .await;
                        }
                    }

                    let inspection = if stream_commit_policy.requires_bounded_frame_wait() {
                        StreamPrefetchInspection::NeedMore
                    } else {
                        inspect_prefetched_stream_body(
                            &upstream_headers,
                            &prefetched_inspection_body,
                        )
                    };
                    match inspection {
                        StreamPrefetchInspection::EmbeddedError(body_json) => {
                            let error_status_code = resolve_provider_stream_error_status_code(
                                plan.provider_api_format.as_str(),
                                status_code,
                                &body_json,
                            );
                            return handle_prefetch_provider_private_stream_error(
                                state,
                                trace_id,
                                decision,
                                &plan,
                                report_context,
                                request_id,
                                candidate_id,
                                report_kind,
                                upstream_headers.clone(),
                                prefetched_usage_telemetry.clone(),
                                &provider_prefetched_body,
                                status_code,
                                error_status_code,
                                body_json,
                                retry_scope_out.as_deref_mut(),
                                retry_fallback_out.as_deref_mut(),
                            )
                            .await;
                        }
                        StreamPrefetchInspection::NeedMore => {}
                        StreamPrefetchInspection::NonError => {}
                    }

                    if !prefetch_success_patterns.is_empty()
                        && crate::orchestration::attempt_identity_from_report_context(
                            report_context.as_ref(),
                        )
                        .is_some()
                    {
                        if let Some(matchable_body) = success_failover_matchable_body(
                            &upstream_headers,
                            &prefetched_inspection_body,
                        ) {
                            let response_text = String::from_utf8_lossy(matchable_body);
                            if prefetch_success_patterns.iter().any(|pattern| pattern.is_match(&response_text))
                                && crate::orchestration::classify_local_failover(
                                    &prefetch_failover_policy,
                                    crate::orchestration::LocalFailoverInput::new(status_code, Some(&response_text)),
                                ) == crate::orchestration::LocalFailoverClassification::RetrySuccessPattern
                            {
                                record_prefetch_success_failover(state, &plan, report_context.as_ref(), stream_elapsed_ms_since(stream_started_at)).await;
                                if let Some(retry_scope) = retry_scope_out.as_deref_mut() {
                                    *retry_scope = AiAttemptRetryScope::Candidate;
                                }
                                warn!(event_name = "local_stream_candidate_retry_scheduled", log_type = "event", trace_id, request_id, status_code, "gateway retrying after a precommit success pattern match");
                                return Ok(None);
                            }
                        }
                    }

                    if !response_headers_indicate_sse(&upstream_headers)
                        && (200..300).contains(&status_code)
                    {
                        if let Some(body_json) =
                            parse_prefetched_sync_json_body(&prefetched_inspection_body)
                        {
                            match maybe_bridge_standard_sync_json_to_stream(
                                &body_json,
                                plan.provider_api_format.as_str(),
                                plan.client_api_format.as_str(),
                                report_context.as_ref(),
                            ) {
                                Ok(Some(outcome)) => {
                                    if let Some(record) = outcome.response_history_record {
                                        crate::ai_serving::persist_response_history_record(
                                            state, record,
                                        )
                                        .await;
                                    }
                                    headers.remove("content-encoding");
                                    headers.remove("content-length");
                                    headers.insert(
                                        "content-type".to_string(),
                                        "text/event-stream".to_string(),
                                    );
                                    stream_terminal_summary = outcome.terminal_summary;
                                    prefetched_body.extend_from_slice(&outcome.sse_body);
                                    prefetched_chunks.push(Bytes::from(outcome.sse_body));
                                    sync_json_stream_bridge_active = true;
                                    break;
                                }
                                Ok(None) => {}
                                Err(err) => {
                                    let failure = build_stream_failure_report(
                                        "execution_runtime_sync_json_stream_bridge_error",
                                        format!(
                                            "failed to bridge execution runtime sync json to stream: {err:?}"
                                        ),
                                        502,
                                    );
                                    return handle_prefetch_stream_failure(
                                        state,
                                        trace_id,
                                        decision,
                                        &plan,
                                        report_context,
                                        request_id,
                                        candidate_id,
                                        report_kind,
                                        upstream_headers.clone(),
                                        prefetched_usage_telemetry.clone(),
                                        &provider_prefetched_body,
                                        candidate_started_unix_secs,
                                        stream_elapsed_ms_since(stream_started_at),
                                        failure,
                                        None,
                                        None,
                                    )
                                    .await;
                                }
                            }
                        }
                    }

                    let normalized_chunk = if let Some(normalizer) =
                        private_stream_normalizer.as_mut()
                    {
                        match normalizer.push_chunk(&chunk) {
                            Ok(normalized_chunk) => normalized_chunk,
                            Err(err) => {
                                let failure = build_stream_failure_report(
                                    "execution_runtime_stream_rewrite_error",
                                    format!(
                                        "failed to normalize execution runtime stream chunk: {err:?}"
                                    ),
                                    502,
                                );
                                return handle_prefetch_stream_failure(
                                    state,
                                    trace_id,
                                    decision,
                                    &plan,
                                    report_context,
                                    request_id,
                                    candidate_id,
                                    report_kind,
                                    upstream_headers.clone(),
                                    prefetched_usage_telemetry.clone(),
                                    &provider_prefetched_body,
                                    candidate_started_unix_secs,
                                    stream_elapsed_ms_since(stream_started_at),
                                    failure,
                                    None,
                                    None,
                                )
                                .await;
                            }
                        }
                    } else {
                        chunk
                    };
                    if let Some(error) = provider_error_inspection
                        .observe(stream_usage_report_context.as_ref(), &normalized_chunk)
                    {
                        prefetched_provider_error.get_or_insert(error);
                    }
                    if let (Some(observer), Some(context)) = (
                        stream_usage_observer.as_mut(),
                        stream_usage_report_context.as_ref(),
                    ) {
                        observe_stream_usage_bytes(
                            observer,
                            context,
                            &mut stream_usage_observer_buffered,
                            &normalized_chunk,
                        );
                    }
                    let rewritten_chunk = if let Some(rewriter) = local_stream_rewriter.as_mut() {
                        match rewriter.push_chunk(&normalized_chunk) {
                            Ok(rewritten_chunk) => rewritten_chunk,
                            Err(err) => {
                                let failure = build_stream_failure_report(
                                    "execution_runtime_stream_rewrite_error",
                                    format!(
                                        "failed to rewrite execution runtime stream chunk: {err:?}"
                                    ),
                                    502,
                                );
                                return handle_prefetch_stream_failure(
                                    state,
                                    trace_id,
                                    decision,
                                    &plan,
                                    report_context,
                                    request_id,
                                    candidate_id,
                                    report_kind,
                                    upstream_headers.clone(),
                                    prefetched_usage_telemetry.clone(),
                                    &provider_prefetched_body,
                                    candidate_started_unix_secs,
                                    stream_elapsed_ms_since(stream_started_at),
                                    failure,
                                    None,
                                    None,
                                )
                                .await;
                            }
                        }
                    } else {
                        normalized_chunk
                    };
                    if !rewritten_chunk.is_empty() {
                        prefetched_body.extend_from_slice(&rewritten_chunk);
                        prefetched_chunks.push(Bytes::from(rewritten_chunk));
                    }

                    if semantic_commit_ready
                        || (matches!(inspection, StreamPrefetchInspection::NonError)
                            && (prefetch_success_patterns.is_empty()
                                || response_headers_indicate_sse(&upstream_headers)
                                || parse_prefetched_sync_json_body(&prefetched_inspection_body)
                                    .is_some())
                            && (!prefetch_for_cyber_failover
                                || prefetched_openai_responses_body_has_output_boundary(
                                    &prefetched_inspection_body,
                                )))
                    {
                        break;
                    }
                }
                StreamFramePayload::Telemetry {
                    telemetry: frame_telemetry,
                } => {
                    prefetched_telemetry = Some(frame_telemetry);
                }
                StreamFramePayload::Eof { summary } => {
                    if stream_commit_policy.requires_bounded_frame_wait()
                        && stream_commit_gate.is_uncommitted()
                    {
                        let error_body_json = anthropic_premature_eof_error_body(
                            "upstream stream ended before the first semantic event",
                        );
                        let error_status_code = anthropic_error_status_code(&error_body_json);
                        return handle_prefetch_provider_private_stream_error(
                            state,
                            trace_id,
                            decision,
                            &plan,
                            report_context,
                            request_id,
                            candidate_id,
                            report_kind,
                            upstream_headers.clone(),
                            prefetched_usage_telemetry.clone(),
                            &provider_prefetched_body,
                            status_code,
                            error_status_code,
                            error_body_json,
                            retry_scope_out.as_deref_mut(),
                            None,
                        )
                        .await;
                    }
                    if summary.is_some() {
                        stream_terminal_summary = summary;
                    }
                    reached_eof = true;
                    break;
                }
                StreamFramePayload::Error { error } => {
                    warn!(
                        event_name = "stream_execution_prefetch_error_frame",
                        log_type = "ops",
                        trace_id = %trace_id,
                        request_id,
                        candidate_id = ?candidate_id,
                        error_kind = ?error.kind,
                        error_phase = ?error.phase,
                        upstream_status = ?error.upstream_status,
                        "execution runtime stream emitted error frame during prefetch"
                    );
                    return handle_prefetch_stream_failure(
                        state,
                        trace_id,
                        decision,
                        &plan,
                        report_context,
                        request_id,
                        candidate_id,
                        report_kind,
                        upstream_headers.clone(),
                        prefetched_usage_telemetry.clone(),
                        &provider_prefetched_body,
                        candidate_started_unix_secs,
                        stream_elapsed_ms_since(stream_started_at),
                        build_stream_failure_from_execution_error(&error),
                        retry_scope_out.as_deref_mut(),
                        retry_fallback_out.as_deref_mut(),
                    )
                    .await;
                }
                StreamFramePayload::Headers { .. } => {}
            }
        }
    }
    if stream_commit_gate.is_uncommitted() {
        stream_commit_gate.commit();
    }
    if let Some(record) = local_stream_rewriter
        .as_mut()
        .and_then(|rewriter| rewriter.take_response_history_record())
    {
        crate::ai_serving::persist_response_history_record(state, record).await;
    }
    // Keep partial records and conversion state; replaying the bounded
    // inspection/capture prefix loses any bytes consumed beyond that prefix.
    let mut private_stream_normalizer = private_stream_normalizer.map(|parser| parser.into_owned());
    let mut local_stream_rewriter = local_stream_rewriter.map(|parser| parser.into_owned());
    if sync_json_stream_bridge_active {
        private_stream_normalizer = None;
        local_stream_rewriter = None;
        stream_usage_observer = None;
    }

    let initial_usage_telemetry = prefetched_usage_telemetry.clone().or_else(|| {
        prefetched_telemetry
            .as_ref()
            .map(|telemetry| usage_refresh_telemetry(telemetry, None))
    });
    state.usage_runtime.record_stream_started(
        state.usage_lifecycle_data_state().as_ref(),
        &lifecycle_seed,
        status_code,
        initial_usage_telemetry.as_ref(),
    );
    if let Some(snapshot) = request_candidate_status_snapshot {
        let latency_ms = prefetched_telemetry
            .as_ref()
            .and_then(|telemetry| telemetry.elapsed_ms);
        record_local_request_candidate_status_snapshot(
            state,
            &snapshot,
            SchedulerRequestCandidateStatusUpdate {
                status: RequestCandidateStatus::Streaming,
                status_code: Some(status_code),
                error_type: None,
                error_message: None,
                latency_ms,
                started_at_unix_ms: Some(candidate_started_unix_secs),
                finished_at_unix_ms: None,
            },
        )
        .await;
    }

    apply_endpoint_response_header_rules(state, &plan, &mut headers, None).await?;

    // Record the client headers alongside the assembled provider headers: the
    // seed falls back to the provider headers when no client value is set.
    let mut object = match report_context {
        Some(Value::Object(object)) => object,
        Some(other) => serde_json::Map::from_iter([("seed".to_string(), other)]),
        None => serde_json::Map::new(),
    };
    object.insert(
        "client_response_headers".to_string(),
        serde_json::to_value(&headers).unwrap_or(Value::Null),
    );
    report_context = Some(Value::Object(object));

    let request_id = request_id.to_string();
    let candidate_id = candidate_id.map(ToOwned::to_owned);
    let (tx, rx) = mpsc::channel::<Result<Bytes, IoError>>(16);
    let state_for_report = state.clone();
    let trace_id_owned = trace_id.to_string();
    let headers_for_report = headers.clone();
    let report_kind_owned = report_kind;
    let report_context_owned = report_context;
    let lifecycle_seed_for_report = lifecycle_seed;
    let provider_prefetched_body_for_report = provider_prefetched_body;
    let prefetched_body_for_report = prefetched_body;
    let prefetched_chunks_for_body = prefetched_chunks;
    let sync_json_stream_bridge_active_for_report = sync_json_stream_bridge_active;
    let initial_telemetry = prefetched_telemetry;
    let initial_reached_eof = reached_eof;
    let direct_stream_finalize_kind_owned = direct_stream_finalize_kind;
    let candidate_started_unix_secs_for_report = candidate_started_unix_secs;
    let _request_id_for_report = request_id.clone();
    let request_id_for_report_log = short_request_id(&request_id);
    let candidate_id_for_report = candidate_id.clone();
    let candidate_index_for_report = candidate_index.clone();
    let is_openai_image_stream_for_report = plan_kind == OPENAI_IMAGE_STREAM_PLAN_KIND;
    let response_headers_are_sse = response_headers_indicate_sse(&headers);
    let emit_proxy_generated_sse_control_blocks =
        response_headers_are_sse && client_format_allows_proxy_generated_sse_control_blocks(&plan);
    let native_anthropic_stream_for_report = stream_commit_policy.is_native_anthropic();
    let plan_for_report = plan;
    let emit_passthrough_sse_terminal_error = (skip_direct_finalize_prefetch
        || stream_commit_policy.requires_bounded_frame_wait()
        || normalized_declared_stream_headers)
        && (response_headers_indicate_sse(&upstream_headers) || normalized_declared_stream_headers)
        && !is_openai_image_stream_for_report;
    let plan_kind_for_report = plan_kind.to_string();
    let stream_started_at_for_report = stream_started_at;
    observe_gateway_stage_trace_ms(
        &mut stage_trace,
        "stream_response_ready",
        stream_elapsed_ms_since(stream_started_at),
    );
    let stage_trace_for_report = stage_trace;
    let request_diagnostics_for_report = current_request_diagnostics();
    let provider_pool_in_flight_guard_for_report = in_flight_guard;
    let usage_producer = state_for_report.usage_runtime.track_producer();
    tokio::spawn(async move {
        let _usage_producer = usage_producer;
        let mut stage_trace_for_report = stage_trace_for_report;
        let _stream_total_guard =
            StageElapsedGuard::from_started_at("stream_total", stream_started_at_for_report);
        let _provider_pool_in_flight_guard = provider_pool_in_flight_guard_for_report;
        let mut provider_buffered_body = provider_prefetched_body_for_report;
        let mut buffered_body = StreamBodyCapture::default();
        let mut provider_body_truncated = provider_prefetched_body_truncated;
        let mut client_body_truncated = false;
        append_budgeted_stream_capture_bytes(
            &mut buffered_body,
            &prefetched_body_for_report,
            max_stream_body_buffer_bytes,
            &mut client_body_truncated,
        );
        let mut client_stream_completion_tracker = ClientVisibleStreamCompletionTracker::default();
        let mut client_visible_stream_completed = if native_anthropic_stream_for_report {
            client_stream_completion_tracker
                .observe_anthropic_message_stop(&prefetched_body_for_report)
        } else {
            client_stream_completion_tracker.observe_chunk(&prefetched_body_for_report)
        };
        let mut anthropic_post_stop_drain_started_at = (native_anthropic_stream_for_report
            && client_visible_stream_completed)
            .then(|| prefetched_anthropic_message_stop_observed_at.unwrap_or_else(Instant::now));
        let mut anthropic_post_stop_buffer_over_limit =
            prefetched_anthropic_post_stop_buffer_over_limit;
        if anthropic_post_stop_drain_started_at.is_some()
            && prefetched_anthropic_message_stop_observed_at.is_none()
        {
            anthropic_post_stop_buffer_over_limit |=
                activate_post_stop_frame_read_budget(&mut lines);
        }
        let mut anthropic_post_stop_drain_frames = 0usize;
        let mut anthropic_post_stop_drain_bytes = 0usize;
        let mut usage_stream_telemetry: Option<ExecutionTelemetry> = initial_usage_telemetry;
        let mut telemetry: Option<ExecutionTelemetry> = initial_telemetry;
        let reached_eof = initial_reached_eof;
        let mut downstream_dropped = false;
        let mut terminal_failure: Option<StreamFailureReport> = None;
        let mut provider_error_forwarded_to_client = false;
        let initial_elapsed_ms = stream_started_at_for_report
            .elapsed()
            .as_millis()
            .min(u128::from(u64::MAX)) as u64;
        let last_upstream_frame_elapsed_ms = Arc::new(AtomicU64::new(initial_elapsed_ms));
        let last_client_chunk_elapsed_ms =
            Arc::new(AtomicU64::new(if prefetched_body_for_report.is_empty() {
                0
            } else {
                initial_elapsed_ms
            }));
        let provider_stream_bytes = Arc::new(AtomicU64::new(provider_prefetched_bytes));
        let client_stream_bytes = Arc::new(AtomicU64::new(
            u64::try_from(prefetched_body_for_report.len()).unwrap_or(u64::MAX),
        ));
        let idle_monitor_done = Arc::new(AtomicBool::new(false));
        let idle_monitor_handle = {
            let done = Arc::clone(&idle_monitor_done);
            let last_upstream = Arc::clone(&last_upstream_frame_elapsed_ms);
            let last_client = Arc::clone(&last_client_chunk_elapsed_ms);
            let provider_bytes = Arc::clone(&provider_stream_bytes);
            let client_bytes = Arc::clone(&client_stream_bytes);
            let trace_id_for_idle = trace_id_owned.clone();
            let request_id_for_idle = request_id_for_report_log.clone();
            let candidate_id_for_idle = candidate_id_for_report.clone();
            let candidate_index_for_idle = candidate_index_for_report.clone();
            let plan_kind_for_idle = plan_kind_for_report.clone();
            let provider_name_for_idle = plan_for_report
                .provider_name
                .clone()
                .unwrap_or_else(|| "-".to_string());
            let endpoint_id_for_idle = plan_for_report.endpoint_id.clone();
            let key_id_for_idle = plan_for_report.key_id.clone();
            let model_name_for_idle = plan_for_report
                .model_name
                .clone()
                .unwrap_or_else(|| "-".to_string());
            let has_local_stream_rewriter_for_idle = local_stream_rewriter.is_some();
            tokio::spawn(async move {
                let mut interval = tokio::time::interval(STREAM_IDLE_LOG_INTERVAL);
                interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
                interval.tick().await;
                loop {
                    interval.tick().await;
                    if done.load(Ordering::Relaxed) {
                        break;
                    }
                    let elapsed_ms = stream_started_at_for_report
                        .elapsed()
                        .as_millis()
                        .min(u128::from(u64::MAX)) as u64;
                    let last_upstream_frame_elapsed_ms = last_upstream.load(Ordering::Relaxed);
                    let last_client_chunk_elapsed_ms = last_client.load(Ordering::Relaxed);
                    let upstream_idle_ms =
                        elapsed_ms.saturating_sub(last_upstream_frame_elapsed_ms);
                    let client_idle_ms = if last_client_chunk_elapsed_ms == 0 {
                        elapsed_ms
                    } else {
                        elapsed_ms.saturating_sub(last_client_chunk_elapsed_ms)
                    };
                    if upstream_idle_ms >= STREAM_IDLE_LOG_INTERVAL_MS {
                        warn!(
                            event_name = "stream_execution_upstream_idle",
                            log_type = "ops",
                            trace_id = %trace_id_for_idle,
                            request_id = %request_id_for_idle,
                            candidate_id = ?candidate_id_for_idle.as_deref(),
                            candidate_index = candidate_index_for_idle.as_str(),
                            plan_kind = plan_kind_for_idle.as_str(),
                            provider_name = provider_name_for_idle.as_str(),
                            endpoint_id = %endpoint_id_for_idle,
                            key_id = %key_id_for_idle,
                            model_name = model_name_for_idle.as_str(),
                            elapsed_ms,
                            provider_bytes = provider_bytes.load(Ordering::Relaxed),
                            client_bytes = client_bytes.load(Ordering::Relaxed),
                            last_upstream_frame_elapsed_ms,
                            last_client_chunk_elapsed_ms,
                            "gateway stream has not received an upstream frame within the idle window"
                        );
                    } else if client_idle_ms >= STREAM_IDLE_LOG_INTERVAL_MS
                        && last_upstream_frame_elapsed_ms >= last_client_chunk_elapsed_ms
                    {
                        warn!(
                            event_name = "stream_execution_client_visible_idle",
                            log_type = "ops",
                            trace_id = %trace_id_for_idle,
                            request_id = %request_id_for_idle,
                            candidate_id = ?candidate_id_for_idle.as_deref(),
                            candidate_index = candidate_index_for_idle.as_str(),
                            plan_kind = plan_kind_for_idle.as_str(),
                            provider_name = provider_name_for_idle.as_str(),
                            endpoint_id = %endpoint_id_for_idle,
                            key_id = %key_id_for_idle,
                            model_name = model_name_for_idle.as_str(),
                            elapsed_ms,
                            provider_bytes = provider_bytes.load(Ordering::Relaxed),
                            client_bytes = client_bytes.load(Ordering::Relaxed),
                            last_upstream_frame_elapsed_ms,
                            last_client_chunk_elapsed_ms,
                            local_stream_rewriter = has_local_stream_rewriter_for_idle,
                            "gateway stream received upstream frames but has no recent client-visible chunk"
                        );
                    }
                }
            })
        };
        if let Some(error_body_json) = prefetched_provider_error {
            provider_error_forwarded_to_client = !prefetched_body_for_report.is_empty();
            let error_status_code = resolve_provider_stream_error_status_code(
                plan_for_report.provider_api_format.as_str(),
                status_code,
                &error_body_json,
            );
            terminal_failure = Some(build_stream_failure_from_provider_error_body(
                error_status_code,
                &error_body_json,
            ));
        }
        // Parser state is already current and capture owns its budgeted bytes.
        // This output prefix is needed only to initialize client-side trackers.
        drop(prefetched_body_for_report);

        if terminal_failure.is_none() && !reached_eof {
            loop {
                let draining_after_anthropic_stop = anthropic_post_stop_drain_started_at.is_some();
                let next_frame_result = if let Some(drain_started_at) =
                    anthropic_post_stop_drain_started_at
                {
                    if anthropic_post_stop_drain_frames >= ANTHROPIC_POST_STOP_DRAIN_MAX_FRAMES
                        || anthropic_post_stop_drain_bytes >= ANTHROPIC_POST_STOP_DRAIN_MAX_BYTES
                        || anthropic_post_stop_buffer_over_limit
                    {
                        break;
                    }
                    let remaining = ANTHROPIC_POST_STOP_DRAIN_MAX_WAIT
                        .saturating_sub(drain_started_at.elapsed());
                    if remaining.is_zero() {
                        break;
                    }
                    match tokio::time::timeout(
                        remaining,
                        next_stream_frame(&mut buffered_frames, &mut lines),
                    )
                    .await
                    {
                        Ok(result) => result,
                        Err(_) => break,
                    }
                } else {
                    tokio::select! {
                        biased;
                        _ = tx.closed(), if !downstream_dropped => {
                            downstream_dropped = true;
                            break;
                        }
                        result = next_stream_frame(&mut buffered_frames, &mut lines) => result,
                    }
                };
                let next_frame = match next_frame_result {
                    Ok(frame) => frame,
                    Err(err) => {
                        if native_anthropic_stream_for_report && client_visible_stream_completed {
                            debug!(
                                event_name = "stream_execution_frame_decode_ignored_after_anthropic_stop",
                                log_type = "debug",
                                trace_id = %trace_id_owned,
                                request_id = %request_id_for_report_log,
                                candidate_id = ?candidate_id_for_report.as_deref(),
                                error_category = "stream_frame_decode_failed",
                                "gateway ignored execution runtime teardown error after Anthropic message_stop"
                            );
                            break;
                        }
                        warn!(
                            event_name = "stream_execution_frame_decode_failed",
                            log_type = "ops",
                            trace_id = %trace_id_owned,
                            request_id = %request_id_for_report_log,
                            candidate_id = ?candidate_id_for_report.as_deref(),
                            error_category = "stream_frame_decode_failed",
                            "gateway failed to decode execution runtime stream frame"
                        );
                        terminal_failure = Some(build_stream_failure_report(
                            "execution_runtime_stream_frame_decode_error",
                            format!("failed to decode execution runtime stream frame: {err:?}"),
                            502,
                        ));
                        break;
                    }
                };
                let Some(observed_frame) = next_frame else {
                    if tx.is_closed() {
                        downstream_dropped = true;
                    } else if native_anthropic_stream_for_report && !client_visible_stream_completed
                    {
                        terminal_failure = Some(build_anthropic_premature_eof_failure(
                            "upstream Anthropic stream ended before message_stop",
                        ));
                    }
                    break;
                };
                if draining_after_anthropic_stop {
                    anthropic_post_stop_drain_frames =
                        anthropic_post_stop_drain_frames.saturating_add(1);
                    let frame_bytes = serialized_stream_frame_len(&observed_frame.frame);
                    if frame_bytes
                        > ANTHROPIC_POST_STOP_DRAIN_MAX_BYTES
                            .saturating_sub(anthropic_post_stop_drain_bytes)
                    {
                        break;
                    }
                    anthropic_post_stop_drain_bytes =
                        anthropic_post_stop_drain_bytes.saturating_add(frame_bytes);
                }
                let frame_observed_at = observed_frame.observed_at;
                let frame_elapsed_ms =
                    stream_elapsed_ms_at(stream_started_at_for_report, frame_observed_at);
                last_upstream_frame_elapsed_ms.store(frame_elapsed_ms, Ordering::Relaxed);
                match observed_frame.frame.payload {
                    StreamFramePayload::Data { chunk_b64, text } => {
                        if native_anthropic_stream_for_report && client_visible_stream_completed {
                            continue;
                        }
                        let first_data_before = usage_stream_telemetry
                            .as_ref()
                            .and_then(|telemetry| telemetry.ttfb_ms)
                            .is_some();
                        if maybe_capture_first_stream_event_telemetry(
                            stream_started_at_for_report,
                            frame_observed_at,
                            telemetry.as_ref(),
                            &mut usage_stream_telemetry,
                        ) {
                            state_for_report.usage_runtime.record_stream_started(
                                state_for_report.usage_lifecycle_data_state().as_ref(),
                                &lifecycle_seed_for_report,
                                status_code,
                                usage_stream_telemetry.as_ref(),
                            );
                        }
                        let first_data_after = usage_stream_telemetry
                            .as_ref()
                            .and_then(|telemetry| telemetry.ttfb_ms)
                            .is_some();
                        if !first_data_before && first_data_after {
                            observe_gateway_stage_trace_ms(
                                &mut stage_trace_for_report,
                                "stream_first_data",
                                stream_elapsed_ms_at(
                                    stream_started_at_for_report,
                                    frame_observed_at,
                                ),
                            );
                        }
                        if sync_json_stream_bridge_active_for_report {
                            continue;
                        }
                        let mut chunk =
                            match decode_stream_data_chunk(chunk_b64.as_deref(), text.as_deref()) {
                                Ok(chunk) => chunk,
                                Err(err) => {
                                    warn!(
                                        event_name = "stream_execution_chunk_decode_failed",
                                        log_type = "ops",
                                        trace_id = %trace_id_owned,
                                        request_id = %request_id_for_report_log,
                                        candidate_id = ?candidate_id_for_report.as_deref(),
                                        error_category = "stream_chunk_decode_failed",
                                        "gateway failed to decode execution runtime chunk"
                                    );
                                    terminal_failure = Some(build_stream_failure_report(
                                        "execution_runtime_stream_chunk_decode_error",
                                        format!(
                                        "failed to decode execution runtime stream chunk: {err:?}"
                                    ),
                                        502,
                                    ));
                                    break;
                                }
                            };

                        if chunk.is_empty() {
                            continue;
                        }
                        if native_anthropic_stream_for_report {
                            if let Some(terminal_end) = client_stream_completion_tracker
                                .observe_anthropic_message_stop_terminal_end(&chunk)
                            {
                                chunk.truncate(terminal_end);
                                client_visible_stream_completed = true;
                                if anthropic_post_stop_drain_started_at.is_none() {
                                    anthropic_post_stop_drain_started_at = Some(Instant::now());
                                    anthropic_post_stop_buffer_over_limit |=
                                        activate_post_stop_frame_read_budget(&mut lines);
                                }
                            }
                        }

                        provider_stream_bytes.fetch_add(
                            u64::try_from(chunk.len()).unwrap_or(u64::MAX),
                            Ordering::Relaxed,
                        );
                        append_budgeted_stream_capture_bytes(
                            &mut provider_buffered_body,
                            &chunk,
                            max_stream_body_buffer_bytes,
                            &mut provider_body_truncated,
                        );
                        let normalized_chunk = if let Some(normalizer) =
                            private_stream_normalizer.as_mut()
                        {
                            match normalizer.push_chunk(&chunk) {
                                Ok(normalized_chunk) => normalized_chunk,
                                Err(err) => {
                                    warn!(
                                        event_name = "stream_execution_chunk_normalize_failed",
                                        log_type = "ops",
                                        trace_id = %trace_id_owned,
                                        request_id = %request_id_for_report_log,
                                        candidate_id = ?candidate_id_for_report.as_deref(),
                                        error_category = "stream_chunk_normalize_failed",
                                        "gateway failed to normalize execution runtime stream chunk"
                                    );
                                    terminal_failure = Some(build_stream_failure_report(
                                            "execution_runtime_stream_rewrite_error",
                                            format!("failed to normalize execution runtime stream chunk: {err:?}"),
                                            502,
                                        ));
                                    break;
                                }
                            }
                        } else {
                            chunk
                        };
                        let provider_private_error_body_json = provider_error_inspection
                            .observe(stream_usage_report_context.as_ref(), &normalized_chunk);
                        if let (Some(observer), Some(report_context)) = (
                            stream_usage_observer.as_mut(),
                            stream_usage_report_context.as_ref(),
                        ) {
                            observe_stream_usage_bytes(
                                observer,
                                report_context,
                                &mut stream_usage_observer_buffered,
                                &normalized_chunk,
                            );
                        }
                        let rewritten_chunk = if let Some(rewriter) = local_stream_rewriter.as_mut()
                        {
                            match rewriter.push_chunk(&normalized_chunk) {
                                Ok(rewritten_chunk) => rewritten_chunk,
                                Err(err) => {
                                    warn!(
                                        event_name = "stream_execution_chunk_rewrite_failed",
                                        log_type = "ops",
                                        trace_id = %trace_id_owned,
                                        request_id = %request_id_for_report_log,
                                        candidate_id = ?candidate_id_for_report.as_deref(),
                                        error_category = "stream_chunk_rewrite_failed",
                                        "gateway failed to rewrite execution runtime stream chunk"
                                    );
                                    terminal_failure = Some(build_stream_failure_report(
                                        "execution_runtime_stream_rewrite_error",
                                        format!("failed to rewrite execution runtime stream chunk: {err:?}"),
                                        502,
                                    ));
                                    break;
                                }
                            }
                        } else {
                            normalized_chunk
                        };

                        if provider_private_error_body_json.is_none() {
                            if let Some(record) = local_stream_rewriter
                                .as_mut()
                                .and_then(|rewriter| rewriter.take_response_history_record())
                            {
                                crate::ai_serving::persist_response_history_record(
                                    &state_for_report,
                                    record,
                                )
                                .await;
                            }
                        }

                        if rewritten_chunk.is_empty() {
                            if let Some(error_body_json) = provider_private_error_body_json {
                                let error_status_code = resolve_provider_stream_error_status_code(
                                    plan_for_report.provider_api_format.as_str(),
                                    status_code,
                                    &error_body_json,
                                );
                                terminal_failure =
                                    Some(build_stream_failure_from_provider_error_body(
                                        error_status_code,
                                        &error_body_json,
                                    ));
                                break;
                            }
                            continue;
                        }

                        append_budgeted_stream_capture_bytes(
                            &mut buffered_body,
                            &rewritten_chunk,
                            max_stream_body_buffer_bytes,
                            &mut client_body_truncated,
                        );
                        let rewritten_chunk_len =
                            u64::try_from(rewritten_chunk.len()).unwrap_or(u64::MAX);
                        if downstream_dropped {
                            continue;
                        }
                        let rewritten_chunk = Bytes::from(rewritten_chunk);
                        if tx.send(Ok(rewritten_chunk.clone())).await.is_err() {
                            debug!(
                                event_name = "stream_execution_downstream_disconnected",
                                log_type = "ops",
                                trace_id = %trace_id_owned,
                                request_id = %request_id_for_report_log,
                                candidate_id = ?candidate_id_for_report.as_deref(),
                                "gateway stream downstream dropped; cancelling execution runtime stream"
                            );
                            downstream_dropped = true;
                            break;
                        } else {
                            if !native_anthropic_stream_for_report {
                                client_visible_stream_completed |= client_stream_completion_tracker
                                    .observe_chunk(rewritten_chunk.as_ref());
                            }
                            client_stream_bytes.fetch_add(rewritten_chunk_len, Ordering::Relaxed);
                            last_client_chunk_elapsed_ms.store(
                                stream_started_at_for_report
                                    .elapsed()
                                    .as_millis()
                                    .min(u128::from(u64::MAX))
                                    as u64,
                                Ordering::Relaxed,
                            );
                            provider_error_forwarded_to_client =
                                provider_private_error_body_json.is_some();
                        }
                        if let Some(error_body_json) = provider_private_error_body_json {
                            let error_status_code = resolve_provider_stream_error_status_code(
                                plan_for_report.provider_api_format.as_str(),
                                status_code,
                                &error_body_json,
                            );
                            terminal_failure = Some(build_stream_failure_from_provider_error_body(
                                error_status_code,
                                &error_body_json,
                            ));
                            break;
                        }
                    }
                    StreamFramePayload::Telemetry {
                        telemetry: frame_telemetry,
                    } => {
                        let usage_frame_telemetry = usage_refresh_telemetry(
                            &frame_telemetry,
                            usage_stream_telemetry.as_ref(),
                        );
                        let should_refresh_stream_usage = should_refresh_stream_usage_telemetry(
                            usage_stream_telemetry.as_ref(),
                            &usage_frame_telemetry,
                        );
                        if should_refresh_stream_usage {
                            // The first Data frame records the live streaming transition. Later
                            // telemetry frames only refine the terminal accumulator; persisting
                            // every elapsed-time update would create one usage task per frame.
                            usage_stream_telemetry = Some(usage_frame_telemetry);
                        }
                        telemetry = Some(frame_telemetry);
                    }
                    StreamFramePayload::Eof { summary } => {
                        stream_terminal_summary =
                            merge_stream_terminal_summary(stream_terminal_summary.take(), summary);
                        if native_anthropic_stream_for_report && !client_visible_stream_completed {
                            terminal_failure = Some(build_anthropic_premature_eof_failure(
                                "upstream Anthropic stream ended before message_stop",
                            ));
                        }
                        break;
                    }
                    StreamFramePayload::Error { error } => {
                        if native_anthropic_stream_for_report && client_visible_stream_completed {
                            debug!(
                                event_name = "stream_execution_error_frame_ignored_after_anthropic_stop",
                                log_type = "debug",
                                trace_id = %trace_id_owned,
                                request_id = %request_id_for_report_log,
                                candidate_id = ?candidate_id_for_report.as_deref(),
                                error_kind = ?error.kind,
                                error_phase = ?error.phase,
                                upstream_status = ?error.upstream_status,
                                "gateway ignored execution runtime error frame after Anthropic message_stop"
                            );
                            continue;
                        }
                        warn!(
                            event_name = "stream_execution_error_frame",
                            log_type = "ops",
                            trace_id = %trace_id_owned,
                            request_id = %request_id_for_report_log,
                            candidate_id = ?candidate_id_for_report.as_deref(),
                            error_kind = ?error.kind,
                            error_phase = ?error.phase,
                            upstream_status = ?error.upstream_status,
                            "execution runtime stream emitted error frame"
                        );
                        terminal_failure = Some(build_stream_failure_from_execution_error(&error));
                        break;
                    }
                    StreamFramePayload::Headers { .. } => {}
                }
            }
        }
        drop(lines);
        drop(buffered_frames);
        drop(_provider_pool_in_flight_guard);

        if downstream_dropped {
            debug!(
                event_name = "execution_runtime_stream_client_flush_skipped",
                log_type = "debug",
                debug_context = "redacted",
                stream_status = "downstream_disconnected",
                trace_id = %trace_id_owned,
                "gateway skipped client stream flush after downstream disconnect"
            );
        }
        // Buffered stream state is partial after a terminal failure; normal
        // finish paths may synthesize successful terminal events.
        let should_finish_stream_rewriters = terminal_failure.is_none();
        if let Some(normalizer) = private_stream_normalizer
            .as_mut()
            .filter(|_| should_finish_stream_rewriters)
        {
            match normalizer.finish() {
                Ok(normalized_chunk) if !normalized_chunk.is_empty() => {
                    let provider_private_error_body_json = provider_error_inspection
                        .observe(stream_usage_report_context.as_ref(), &normalized_chunk);
                    if let (Some(observer), Some(report_context)) = (
                        stream_usage_observer.as_mut(),
                        stream_usage_report_context.as_ref(),
                    ) {
                        observe_stream_usage_bytes(
                            observer,
                            report_context,
                            &mut stream_usage_observer_buffered,
                            &normalized_chunk,
                        );
                    }
                    if !downstream_dropped {
                        let rewritten_chunk = if let Some(rewriter) = local_stream_rewriter.as_mut()
                        {
                            match rewriter.push_chunk(&normalized_chunk) {
                                Ok(rewritten_chunk) => rewritten_chunk,
                                Err(err) => {
                                    warn!(
                                        event_name = "stream_execution_normalized_flush_rewrite_failed",
                                        log_type = "ops",
                                        trace_id = %trace_id_owned,
                                        request_id = %request_id_for_report_log,
                                        candidate_id = ?candidate_id_for_report.as_deref(),
                                        error_category = "stream_flush_rewrite_failed",
                                        "gateway failed to rewrite normalized private stream chunk during flush"
                                    );
                                    let failure = build_stream_failure_report(
                                        "execution_runtime_stream_rewrite_flush_error",
                                        format!("failed to rewrite normalized private stream chunk during flush: {err:?}"),
                                        502,
                                    );
                                    terminal_failure.get_or_insert(failure);
                                    Vec::new()
                                }
                            }
                        } else {
                            normalized_chunk
                        };
                        if provider_private_error_body_json.is_none() {
                            if let Some(record) = local_stream_rewriter
                                .as_mut()
                                .and_then(|rewriter| rewriter.take_response_history_record())
                            {
                                crate::ai_serving::persist_response_history_record(
                                    &state_for_report,
                                    record,
                                )
                                .await;
                            }
                        }
                        if !rewritten_chunk.is_empty() {
                            append_budgeted_stream_capture_bytes(
                                &mut buffered_body,
                                &rewritten_chunk,
                                max_stream_body_buffer_bytes,
                                &mut client_body_truncated,
                            );
                            let rewritten_chunk_len =
                                u64::try_from(rewritten_chunk.len()).unwrap_or(u64::MAX);
                            let rewritten_chunk = Bytes::from(rewritten_chunk);
                            if tx.send(Ok(rewritten_chunk.clone())).await.is_err() {
                                warn!(
                                    event_name = "stream_execution_downstream_flush_disconnected",
                                    log_type = "ops",
                                    trace_id = %trace_id_owned,
                                    request_id = %request_id_for_report_log,
                                candidate_id = ?candidate_id_for_report.as_deref(),
                                "gateway stream downstream dropped while flushing private stream normalization"
                                );
                                downstream_dropped = true;
                            } else {
                                client_visible_stream_completed |= client_stream_completion_tracker
                                    .observe_chunk(rewritten_chunk.as_ref());
                                client_stream_bytes
                                    .fetch_add(rewritten_chunk_len, Ordering::Relaxed);
                                last_client_chunk_elapsed_ms.store(
                                    stream_started_at_for_report
                                        .elapsed()
                                        .as_millis()
                                        .min(u128::from(u64::MAX))
                                        as u64,
                                    Ordering::Relaxed,
                                );
                            }
                        }
                        if let Some(error_body_json) = provider_private_error_body_json {
                            let error_status_code = resolve_provider_stream_error_status_code(
                                plan_for_report.provider_api_format.as_str(),
                                status_code,
                                &error_body_json,
                            );
                            terminal_failure.get_or_insert_with(|| {
                                build_stream_failure_from_provider_error_body(
                                    error_status_code,
                                    &error_body_json,
                                )
                            });
                        }
                    }
                }
                Ok(_) => {}
                Err(err) => {
                    warn!(
                        event_name = "stream_execution_normalization_flush_failed",
                        log_type = "ops",
                        trace_id = %trace_id_owned,
                        request_id = %request_id_for_report_log,
                        candidate_id = ?candidate_id_for_report.as_deref(),
                        error_category = "stream_normalization_flush_failed",
                        "gateway failed to flush private stream normalization"
                    );
                    terminal_failure.get_or_insert_with(|| {
                        build_stream_failure_report(
                            "execution_runtime_stream_rewrite_flush_error",
                            format!("failed to flush private stream normalization: {err:?}"),
                            502,
                        )
                    });
                }
            }
        }
        if !downstream_dropped && terminal_failure.is_none() {
            if let Some(rewriter) = local_stream_rewriter.as_mut() {
                let finish_result = rewriter.finish();
                if let Some(record) = rewriter.take_response_history_record() {
                    crate::ai_serving::persist_response_history_record(&state_for_report, record)
                        .await;
                }
                match finish_result {
                    Ok(flushed_chunk) if !flushed_chunk.is_empty() => {
                        append_budgeted_stream_capture_bytes(
                            &mut buffered_body,
                            &flushed_chunk,
                            max_stream_body_buffer_bytes,
                            &mut client_body_truncated,
                        );
                        let flushed_chunk_len =
                            u64::try_from(flushed_chunk.len()).unwrap_or(u64::MAX);
                        let flushed_chunk = Bytes::from(flushed_chunk);
                        if tx.send(Ok(flushed_chunk.clone())).await.is_err() {
                            warn!(
                                event_name = "stream_execution_downstream_rewrite_flush_disconnected",
                                log_type = "ops",
                                trace_id = %trace_id_owned,
                                request_id = %request_id_for_report_log,
                            candidate_id = ?candidate_id_for_report.as_deref(),
                            "gateway stream downstream dropped while flushing local stream rewrite"
                            );
                            downstream_dropped = true;
                        } else {
                            client_visible_stream_completed |= client_stream_completion_tracker
                                .observe_chunk(flushed_chunk.as_ref());
                            client_stream_bytes.fetch_add(flushed_chunk_len, Ordering::Relaxed);
                            last_client_chunk_elapsed_ms.store(
                                stream_started_at_for_report
                                    .elapsed()
                                    .as_millis()
                                    .min(u128::from(u64::MAX))
                                    as u64,
                                Ordering::Relaxed,
                            );
                        }
                    }
                    Ok(_) => {}
                    Err(err) => {
                        warn!(
                            event_name = "stream_execution_rewrite_flush_failed",
                            log_type = "ops",
                            trace_id = %trace_id_owned,
                            request_id = %request_id_for_report_log,
                            candidate_id = ?candidate_id_for_report.as_deref(),
                            error_category = "stream_rewrite_flush_failed",
                            "gateway failed to flush local stream rewrite"
                        );
                        terminal_failure.get_or_insert_with(|| {
                            build_stream_failure_report(
                                "execution_runtime_stream_rewrite_flush_error",
                                format!("failed to flush local stream rewrite: {err:?}"),
                                502,
                            )
                        });
                    }
                }
            }
        }

        if terminal_failure.is_none() {
            if let Some(record) = local_stream_rewriter
                .as_mut()
                .and_then(|rewriter| rewriter.take_response_history_record())
            {
                crate::ai_serving::persist_response_history_record(&state_for_report, record).await;
            }
        }

        if !downstream_dropped {
            if let Some(failure) = terminal_failure.as_ref() {
                let terminal_event = if is_openai_image_stream_for_report {
                    Some(encode_openai_image_failed_event(
                        report_context_owned.as_ref(),
                        failure,
                    ))
                } else if emit_passthrough_sse_terminal_error && !provider_error_forwarded_to_client
                {
                    Some(encode_terminal_sse_error_event_for_plan(
                        &plan_for_report,
                        failure,
                    ))
                } else {
                    None
                };
                if let Some(terminal_event) = terminal_event {
                    match terminal_event {
                        Ok(error_event) => {
                            let error_event_len =
                                u64::try_from(error_event.len()).unwrap_or(u64::MAX);
                            append_budgeted_stream_capture_bytes(
                                &mut buffered_body,
                                error_event.as_ref(),
                                max_stream_body_buffer_bytes,
                                &mut client_body_truncated,
                            );
                            if tx.send(Ok(error_event)).await.is_err() {
                                warn!(
                                event_name = "stream_execution_downstream_terminal_error_disconnected",
                                log_type = "ops",
                                trace_id = %trace_id_owned,
                                request_id = %request_id_for_report_log,
                                candidate_id = ?candidate_id_for_report.as_deref(),
                                "gateway stream downstream dropped while sending terminal SSE error event"
                                );
                                downstream_dropped = true;
                            } else {
                                client_stream_bytes.fetch_add(error_event_len, Ordering::Relaxed);
                                last_client_chunk_elapsed_ms.store(
                                    stream_started_at_for_report
                                        .elapsed()
                                        .as_millis()
                                        .min(u128::from(u64::MAX))
                                        as u64,
                                    Ordering::Relaxed,
                                );
                            }
                        }
                        Err(_err) => {
                            warn!(
                            event_name = "stream_execution_terminal_error_event_encode_failed",
                            log_type = "ops",
                            trace_id = %trace_id_owned,
                            request_id = %request_id_for_report_log,
                            candidate_id = ?candidate_id_for_report.as_deref(),
                            error_category = "terminal_error_event_encode_failed",
                                "gateway failed to encode terminal SSE error event"
                            );
                        }
                    }
                }
            }
        }

        drop(tx);
        idle_monitor_done.store(true, Ordering::Relaxed);
        idle_monitor_handle.abort();

        let observed_terminal_summary = finalize_stream_usage_observer(
            &mut stream_usage_observer,
            stream_usage_report_context.as_ref(),
            &mut stream_usage_observer_buffered,
        );
        stream_terminal_summary = merge_observed_stream_terminal_summary(
            stream_terminal_summary,
            observed_terminal_summary,
            &stream_usage_observer_buffered,
        );

        if downstream_dropped && client_visible_stream_completed && terminal_failure.is_none() {
            debug!(
                event_name = "execution_runtime_stream_downstream_closed_after_done",
                log_type = "debug",
                trace_id = %trace_id_owned,
                request_id = %request_id_for_report_log,
                candidate_id = ?candidate_id_for_report.as_deref(),
                "gateway treats downstream close after client-visible SSE DONE as completed"
            );
            downstream_dropped = false;
        }

        if downstream_dropped {
            debug!(
                event_name = "execution_runtime_stream_report_skipped",
                log_type = "debug",
                debug_context = "redacted",
                stream_status = "downstream_disconnected",
                status_code = 499_u16,
                trace_id = %trace_id_owned,
                "gateway skipped stream report because downstream disconnected before completion"
            );
            let terminal_telemetry = Some(build_terminal_stream_telemetry(
                stream_started_at_for_report,
                telemetry.as_ref(),
                usage_stream_telemetry.as_ref(),
                provider_stream_bytes.load(Ordering::Relaxed),
            ));
            let report_context_for_payload = report_context_with_stage_trace(
                report_context_owned,
                stage_trace_for_report,
                stream_started_at_for_report,
                terminal_telemetry.as_ref(),
            );
            let report_context_for_payload = report_context_with_request_diagnostics(
                report_context_for_payload,
                request_diagnostics_for_report.as_ref(),
                stream_started_at_for_report,
                terminal_telemetry.as_ref(),
            );
            let usage_payload = build_stream_usage_payload(
                trace_id_owned,
                report_kind_owned.unwrap_or_default(),
                report_context_for_payload,
                499,
                headers_for_report,
                &provider_buffered_body,
                provider_body_truncated,
                &buffered_body,
                client_body_truncated,
                stream_terminal_summary,
                terminal_telemetry,
            );
            record_stream_terminal_usage(
                &state_for_report,
                &plan_for_report,
                usage_payload.report_context.as_ref(),
                &usage_payload,
                true,
            )
            .await;
            record_local_request_candidate_status(
                &state_for_report,
                &plan_for_report,
                usage_payload.report_context.as_ref(),
                SchedulerRequestCandidateStatusUpdate {
                    status: RequestCandidateStatus::Cancelled,
                    status_code: Some(499),
                    error_type: Some("downstream_disconnect".to_string()),
                    error_message: Some("client disconnected before stream completion".to_string()),
                    latency_ms: usage_payload
                        .telemetry
                        .as_ref()
                        .and_then(|value| value.elapsed_ms),
                    started_at_unix_ms: Some(candidate_started_unix_secs_for_report),
                    finished_at_unix_ms: Some(current_request_candidate_unix_ms()),
                },
            )
            .await;
            return;
        }

        if let Some(failure) = terminal_failure {
            record_manual_proxy_stream_error(&state_for_report, &plan_for_report).await;
            let terminal_telemetry = Some(build_terminal_stream_telemetry(
                stream_started_at_for_report,
                telemetry.as_ref(),
                usage_stream_telemetry.as_ref(),
                provider_stream_bytes.load(Ordering::Relaxed),
            ));
            let report_context_for_payload = report_context_with_stage_trace(
                report_context_owned,
                stage_trace_for_report,
                stream_started_at_for_report,
                terminal_telemetry.as_ref(),
            );
            let report_context_for_payload = report_context_with_request_diagnostics(
                report_context_for_payload,
                request_diagnostics_for_report.as_ref(),
                stream_started_at_for_report,
                terminal_telemetry.as_ref(),
            );
            submit_midstream_stream_failure(
                &state_for_report,
                &trace_id_owned,
                &plan_for_report,
                direct_stream_finalize_kind_owned.as_deref(),
                report_context_for_payload,
                headers_for_report,
                terminal_telemetry,
                &provider_buffered_body,
                candidate_started_unix_secs_for_report,
                failure,
            )
            .await;
            return;
        }

        maybe_apply_kiro_prompt_cache_usage_to_stream_summary(
            &state_for_report,
            &plan_for_report,
            report_context_owned.as_ref(),
            &mut stream_terminal_summary,
        )
        .await;
        let requires_observed_terminal_event = stream_requires_observed_terminal_event(
            plan_for_report.provider_api_format.as_str(),
            stream_usage_report_context.as_ref(),
        );
        ensure_stream_terminal_summary_for_missing_observed_finish(
            &mut stream_terminal_summary,
            requires_observed_terminal_event,
        );
        let missing_observed_finish =
            stream_terminal_summary_missing_observed_finish_with_requirement(
                stream_terminal_summary.as_ref(),
                requires_observed_terminal_event,
            );

        let should_submit_report = report_kind_owned.is_some();
        let terminal_telemetry = Some(build_terminal_stream_telemetry(
            stream_started_at_for_report,
            telemetry.as_ref(),
            usage_stream_telemetry.as_ref(),
            provider_stream_bytes.load(Ordering::Relaxed),
        ));
        let stream_failed = stream_terminal_summary_represents_failure_with_requirement(
            stream_terminal_summary.as_ref(),
            requires_observed_terminal_event,
        );
        let stream_terminal_error_message = stream_terminal_summary
            .as_ref()
            .and_then(|summary| summary.parser_error.clone())
            .or_else(|| {
                missing_observed_finish.then(|| {
                    "execution runtime stream ended before provider terminal event".to_string()
                })
            });
        let report_context_for_payload = report_context_with_stage_trace(
            report_context_owned,
            stage_trace_for_report,
            stream_started_at_for_report,
            terminal_telemetry.as_ref(),
        );
        let report_context_for_payload = report_context_with_request_diagnostics(
            report_context_for_payload,
            request_diagnostics_for_report.as_ref(),
            stream_started_at_for_report,
            terminal_telemetry.as_ref(),
        );
        let usage_payload = build_stream_usage_payload(
            trace_id_owned.clone(),
            report_kind_owned.unwrap_or_default(),
            report_context_for_payload,
            status_code,
            headers_for_report,
            &provider_buffered_body,
            provider_body_truncated,
            &buffered_body,
            client_body_truncated,
            stream_terminal_summary,
            terminal_telemetry,
        );
        if stream_failed {
            warn!(
                event_name = "execution_runtime_stream_missing_terminal_event",
                log_type = "ops",
                trace_id = %trace_id_owned,
                request_id = %request_id_for_report_log,
                candidate_id = ?candidate_id_for_report.as_deref(),
                status_code,
                error_message = stream_terminal_error_message.as_deref().unwrap_or_default(),
                "gateway stream ended with a failed terminal state"
            );
        } else {
            apply_local_execution_effect(
                &state_for_report,
                LocalExecutionEffectContext {
                    plan: &plan_for_report,
                    report_context: usage_payload.report_context.as_ref(),
                },
                LocalExecutionEffect::HealthSuccess(LocalHealthSuccessEffect),
            )
            .await;
            apply_local_execution_effect(
                &state_for_report,
                LocalExecutionEffectContext {
                    plan: &plan_for_report,
                    report_context: usage_payload.report_context.as_ref(),
                },
                LocalExecutionEffect::AdaptiveSuccess(LocalAdaptiveSuccessEffect),
            )
            .await;
            apply_local_execution_effect(
                &state_for_report,
                LocalExecutionEffectContext {
                    plan: &plan_for_report,
                    report_context: usage_payload.report_context.as_ref(),
                },
                LocalExecutionEffect::PoolSuccessStream {
                    payload: &usage_payload,
                },
            )
            .await;
        }
        record_stream_terminal_usage(
            &state_for_report,
            &plan_for_report,
            usage_payload.report_context.as_ref(),
            &usage_payload,
            false,
        )
        .await;
        record_local_request_candidate_status(
            &state_for_report,
            &plan_for_report,
            usage_payload.report_context.as_ref(),
            SchedulerRequestCandidateStatusUpdate {
                status: if stream_failed {
                    RequestCandidateStatus::Failed
                } else {
                    RequestCandidateStatus::Success
                },
                status_code: Some(status_code),
                error_type: if stream_failed {
                    if missing_observed_finish {
                        Some("stream_missing_terminal_event".to_string())
                    } else {
                        Some("stream_terminal_error".to_string())
                    }
                } else {
                    None
                },
                error_message: stream_failed
                    .then_some(stream_terminal_error_message)
                    .flatten(),
                latency_ms: usage_payload
                    .telemetry
                    .as_ref()
                    .and_then(|value| value.elapsed_ms),
                started_at_unix_ms: Some(candidate_started_unix_secs_for_report),
                finished_at_unix_ms: Some(current_request_candidate_unix_ms()),
            },
        )
        .await;

        if should_submit_report {
            if let Err(_err) = submit_stream_report(&state_for_report, usage_payload).await {
                warn!(
                    event_name = "execution_report_submit_failed",
                    log_type = "ops",
                    trace_id = %trace_id_owned,
                    request_id = %request_id_for_report_log,
                    candidate_id = ?candidate_id_for_report.as_deref(),
                    report_scope = "stream",
                    error_category = "stream_report_submit_failed",
                    "gateway failed to submit stream execution report"
                );
            }
        }
    });

    headers.insert(CONTROL_REQUEST_ID_HEADER.to_string(), request_id.clone());

    if let Some(candidate_id) = candidate_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        headers.insert(
            CONTROL_CANDIDATE_ID_HEADER.to_string(),
            candidate_id.to_string(),
        );
    }

    if response_headers_are_sse {
        headers.remove("content-length");
    }
    let body_stream = build_sse_body_stream(
        prefetched_chunks_for_body,
        rx,
        response_headers_are_sse,
        emit_proxy_generated_sse_control_blocks,
        native_anthropic_stream_for_report,
        SSE_KEEPALIVE_INTERVAL,
    );

    Ok(Some(build_client_response_from_parts(
        status_code,
        &headers,
        Body::from_stream(body_stream),
        trace_id,
        Some(decision),
    )?))
}

fn apply_stream_summary_report_context(
    execution: &mut DirectUpstreamStreamExecution,
    report_context: Option<&Value>,
) {
    if let Some(report_context) = report_context.cloned() {
        execution.stream_summary_report_context = report_context;
    }
}

#[cfg(test)]
#[path = "execution/tests.rs"]
mod tests;
