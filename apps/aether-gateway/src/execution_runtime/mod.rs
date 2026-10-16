use serde_json::{Map, Value};

pub(crate) mod admission;
pub(crate) mod attempt_cancellation;
pub(crate) mod attempt_lifecycle;
mod chatgpt_web_image;
mod constants;
mod exhaustion_fallback;
mod fallback;
mod grok;
mod kiro_cache;
mod kiro_web_search;
pub(crate) mod ndjson;
mod oauth_retry;
#[cfg(test)]
pub(crate) mod remote_compat;
mod response_header_rules;
mod server;
pub(crate) mod stream;
mod stream_pump;
mod stream_read_timeout;
pub(crate) mod submission;
pub(crate) mod sync;
pub(crate) mod transport;
mod transport_failure;
mod windsurf;

pub(crate) use self::admission::{
    acquire_upstream_execution_gate, UpstreamExecutionGateProvider, UPSTREAM_EXECUTION_GATE_NAME,
};
pub(crate) use self::chatgpt_web_image::maybe_execute_chatgpt_web_image_sync;
pub(crate) use self::constants::{
    MAX_ERROR_BODY_BYTES, MAX_EXECUTION_STREAM_FRAME_LINE_BYTES, MAX_STREAM_BODY_CAPTURE_BYTES,
    MAX_STREAM_PREFETCH_BYTES, MAX_STREAM_PREFETCH_FRAMES,
};
pub(crate) use self::exhaustion_fallback::{
    build_exhaustion_fallback, ExhaustionFallbackInput,
};
pub(crate) use self::fallback::{
    analyze_local_candidate_failover_sync, local_failover_response_text,
    resolve_core_stream_direct_finalize_report_kind,
    resolve_core_stream_error_finalize_report_kind, resolve_core_sync_error_finalize_report_kind,
    resolve_local_candidate_failover_analysis_stream, should_fallback_to_control_stream,
    should_fallback_to_control_sync, should_finalize_sync_response,
    should_retry_next_local_candidate_stream,
};
pub(crate) use self::response_header_rules::{
    apply_endpoint_response_header_rules, attach_provider_response_headers_to_report_context,
};
pub(crate) use crate::orchestration::{
    append_local_failover_policy_to_value, LocalFailoverDecision,
};
pub(crate) use aether_ai_serving::AdaptationMode;
pub(crate) use aether_ai_serving::{ConversionMode, ExecutionStrategy};

pub(crate) fn ai_attempt_retry_scope_from_failure_disposition(
    disposition: crate::orchestration::FailureDisposition,
) -> aether_ai_serving::AiAttemptRetryScope {
    use crate::orchestration::{FailureRetryAction, FailureScope};
    use aether_ai_serving::AiAttemptRetryScope;

    match disposition.failure_scope {
        FailureScope::Credential | FailureScope::CredentialModel => AiAttemptRetryScope::Credential,
        FailureScope::Endpoint => AiAttemptRetryScope::Endpoint,
        FailureScope::Provider => AiAttemptRetryScope::Provider,
        FailureScope::None => match disposition.retry_action {
            FailureRetryAction::NextCredential => AiAttemptRetryScope::Credential,
            FailureRetryAction::NextEndpoint => AiAttemptRetryScope::Endpoint,
            FailureRetryAction::Stop | FailureRetryAction::NextCandidate => {
                AiAttemptRetryScope::Candidate
            }
        },
    }
}

#[cfg(test)]
mod retry_scope_tests {
    use aether_ai_serving::AiAttemptRetryScope;

    use super::ai_attempt_retry_scope_from_failure_disposition;
    use crate::orchestration::{classify_failure_disposition, LocalFailoverClassification};

    #[test]
    fn anthropic_failure_scope_survives_runtime_mapping() {
        let retry_scope = |status_code| {
            ai_attempt_retry_scope_from_failure_disposition(classify_failure_disposition(
                "claude:messages",
                LocalFailoverClassification::RetryUpstreamFailure,
                status_code,
                true,
            ))
        };

        assert_eq!(retry_scope(429), AiAttemptRetryScope::Credential);
        assert_eq!(retry_scope(500), AiAttemptRetryScope::Endpoint);
        assert_eq!(retry_scope(529), AiAttemptRetryScope::Provider);
        assert_eq!(retry_scope(400), AiAttemptRetryScope::Candidate);
    }

    #[test]
    fn non_anthropic_retry_keeps_existing_candidate_order() {
        let disposition = classify_failure_disposition(
            "openai:chat",
            LocalFailoverClassification::RetryUpstreamFailure,
            429,
            true,
        );

        assert_eq!(
            ai_attempt_retry_scope_from_failure_disposition(disposition),
            AiAttemptRetryScope::Candidate
        );
        assert!(disposition.preserve_upstream_error);

        let without_exhaustion_passthrough = classify_failure_disposition(
            "openai:chat",
            LocalFailoverClassification::RetryUpstreamFailure,
            429,
            false,
        );
        assert_eq!(
            ai_attempt_retry_scope_from_failure_disposition(without_exhaustion_passthrough),
            AiAttemptRetryScope::Candidate
        );
        assert!(!without_exhaustion_passthrough.preserve_upstream_error);
    }
}
pub use server::{
    build_execution_runtime_router, build_execution_runtime_router_with_request_concurrency_limit,
    build_execution_runtime_router_with_request_gates, serve_execution_runtime_tcp,
    serve_execution_runtime_unix,
};
pub use transport::DirectH2cSenderPrewarmReport;

pub async fn prewarm_direct_h2c_sender_cache_from_env_for_startup(
) -> Result<Option<DirectH2cSenderPrewarmReport>, String> {
    transport::prewarm_direct_h2c_sender_cache_from_env()
        .await
        .map_err(|err| err.to_string())
}

pub(crate) use stream::{
    execute_execution_runtime_stream, execute_execution_runtime_stream_with_retry_scope,
};
pub(crate) use stream_pump::build_direct_execution_frame_stream;
pub(crate) use sync::{
    execute_execution_runtime_sync, execute_execution_runtime_sync_with_retry_scope,
    maybe_build_local_sync_finalize_response, maybe_build_local_video_error_response,
    maybe_build_local_video_success_outcome, resolve_local_sync_error_background_report_kind,
    resolve_local_sync_success_background_report_kind, LocalVideoSyncSuccessBuild,
    LocalVideoSyncSuccessOutcome,
};
pub(crate) use transport::execute_sync_plan_with_report_context as execute_execution_runtime_sync_plan_with_report_context;
pub(crate) use transport::{
    execute_sync_plan as execute_execution_runtime_sync_plan, DirectSyncExecutionRuntime,
    DirectUpstreamStreamExecution, ExecutionRuntimeTransportError,
};
pub(crate) use transport_failure::{
    build_transport_error_stop_response, mark_stream_candidate_watchdog_terminal_started,
    StreamCandidateWatchdogProgress,
};

pub(crate) fn append_execution_contract_fields(
    object: &mut Map<String, Value>,
    execution_strategy: ExecutionStrategy,
    conversion_mode: ConversionMode,
    client_contract: &str,
    provider_contract: &str,
) {
    object.insert(
        "execution_strategy".to_string(),
        Value::String(execution_strategy.as_str().to_string()),
    );
    object.insert(
        "conversion_mode".to_string(),
        Value::String(conversion_mode.as_str().to_string()),
    );
    object.insert(
        "client_contract".to_string(),
        Value::String(client_contract.to_string()),
    );
    object.insert(
        "provider_contract".to_string(),
        Value::String(provider_contract.to_string()),
    );
    let default_adaptation_mode = if execution_strategy == ExecutionStrategy::LocalCrossFormat
        || conversion_mode != ConversionMode::None
    {
        AdaptationMode::CrossFormat
    } else {
        AdaptationMode::NativeTransparent
    };
    object
        .entry("adaptation_mode".to_string())
        .or_insert_with(|| Value::String(default_adaptation_mode.as_str().to_string()));
}

pub(crate) fn append_execution_contract_fields_to_value(
    value: Value,
    execution_strategy: ExecutionStrategy,
    conversion_mode: ConversionMode,
    client_contract: &str,
    provider_contract: &str,
) -> Value {
    match value {
        Value::Object(mut object) => {
            append_execution_contract_fields(
                &mut object,
                execution_strategy,
                conversion_mode,
                client_contract,
                provider_contract,
            );
            Value::Object(object)
        }
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use super::{append_execution_contract_fields_to_value, ConversionMode, ExecutionStrategy};
    use serde_json::json;

    #[test]
    fn execution_contract_helper_appends_unified_fields() {
        let value = append_execution_contract_fields_to_value(
            json!({"provider_api_format": "gemini:generate_content"}),
            ExecutionStrategy::LocalCrossFormat,
            ConversionMode::Bidirectional,
            "openai:chat",
            "gemini:generate_content",
        );

        assert_eq!(value["execution_strategy"], "local_cross_format");
        assert_eq!(value["conversion_mode"], "bidirectional");
        assert_eq!(value["client_contract"], "openai:chat");
        assert_eq!(value["provider_contract"], "gemini:generate_content");
        assert_eq!(value["adaptation_mode"], "cross_format");
        assert_eq!(value["provider_api_format"], "gemini:generate_content");
    }
}
