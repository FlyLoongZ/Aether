//! Standard contract planning surface.
//!
//! This groups the standard planning surface in one place:
//! request-side conversion, matrix registry, and decision payload builders.

use crate::ai_serving::GatewayControlDecision;
use crate::{AiExecutionDecision, AppState, GatewayError};

mod claude;
mod codex;
mod deepseek;
mod family;
mod gemini;
mod normalize;
mod openai;

pub(crate) use self::codex::{
    apply_codex_openai_special_headers, codex_model_capabilities_for_transport,
};
pub(crate) use self::deepseek::{
    apply_deepseek_tool_call_thinking_compat, is_deepseek_provider,
    openai_responses_reasoning_replay_policy,
};
pub(crate) use self::family::{build_local_stream_attempt_source, build_local_sync_attempt_source};

pub(crate) use self::normalize::{
    build_cross_format_openai_chat_request_body, build_cross_format_openai_chat_upstream_url,
    build_cross_format_openai_responses_request_body_with_codex_model_capabilities,
    build_cross_format_openai_responses_upstream_url, build_local_openai_chat_request_body,
    build_local_openai_chat_upstream_url,
    build_local_openai_responses_request_body_with_codex_model_capabilities,
    build_local_openai_responses_request_body_with_codex_model_capabilities_for_websocket_continuation,
    build_local_openai_responses_upstream_url, CrossFormatResponsesBodyInput,
};
pub(crate) use self::openai::{
    build_local_openai_chat_stream_attempt_source_for_kind,
    build_local_openai_chat_sync_attempt_source_for_kind,
    build_local_openai_responses_stream_attempt_source_for_kind,
    build_local_openai_responses_sync_attempt_source_for_kind,
    maybe_build_responses_websocket_decision, maybe_build_stream_local_decision_payload,
    maybe_build_stream_local_openai_responses_decision_payload,
    maybe_build_sync_local_decision_payload,
    maybe_build_sync_local_openai_responses_decision_payload,
    set_local_openai_chat_execution_exhausted_diagnostic, ResponsesWebSocketBodyNormalization,
    ResponsesWebSocketDecision, ResponsesWebSocketPinnedCandidate,
};

#[cfg(test)]
pub(crate) use self::normalize::{
    build_cross_format_openai_responses_request_body, build_local_openai_responses_request_body,
};
pub(crate) use crate::ai_serving::build_standard_request_body_with_model_directives_and_request_headers_and_reasoning_replay_policy;
pub(crate) use aether_ai_serving::{
    openai_provider_request_contract_failure_extra_data, request_body_build_failure_extra_data,
    request_conversion_failure_extra_data, same_format_provider_request_body_failure_extra_data,
};

pub(crate) fn build_standard_upstream_url(
    parts: &http::request::Parts,
    transport: &crate::ai_serving::GatewayProviderTransportSnapshot,
    mapped_model: &str,
    provider_api_format: &str,
    upstream_is_stream: bool,
    provider_request_body: Option<&serde_json::Value>,
) -> Option<String> {
    crate::ai_serving::build_provider_transport_request_url_for_request_body(
        transport,
        provider_api_format,
        Some(mapped_model),
        upstream_is_stream,
        parts.uri.query(),
        None,
        None,
        provider_request_body,
    )
}

pub(crate) async fn maybe_build_sync_local_standard_decision_payload(
    state: &AppState,
    parts: &http::request::Parts,
    trace_id: &str,
    decision: &GatewayControlDecision,
    body_json: &serde_json::Value,
    plan_kind: &str,
) -> Result<Option<AiExecutionDecision>, GatewayError> {
    if let Some(payload) = self::openai::maybe_build_sync_local_openai_embedding_decision_payload(
        state, parts, trace_id, decision, body_json, plan_kind,
    )
    .await?
    {
        return Ok(Some(payload));
    }

    if let Some(payload) = self::claude::maybe_build_sync_local_claude_decision_payload(
        state, parts, trace_id, decision, body_json, plan_kind,
    )
    .await?
    {
        return Ok(Some(payload));
    }

    self::gemini::maybe_build_sync_local_gemini_decision_payload(
        state, parts, trace_id, decision, body_json, plan_kind,
    )
    .await
}

pub(crate) async fn maybe_build_stream_local_standard_decision_payload(
    state: &AppState,
    parts: &http::request::Parts,
    trace_id: &str,
    decision: &GatewayControlDecision,
    body_json: &serde_json::Value,
    plan_kind: &str,
) -> Result<Option<AiExecutionDecision>, GatewayError> {
    if let Some(payload) = self::claude::maybe_build_stream_local_claude_decision_payload(
        state, parts, trace_id, decision, body_json, plan_kind,
    )
    .await?
    {
        return Ok(Some(payload));
    }

    self::gemini::maybe_build_stream_local_gemini_decision_payload(
        state, parts, trace_id, decision, body_json, plan_kind,
    )
    .await
}

#[cfg(test)]
pub(crate) use crate::ai_serving::build_standard_request_body;

#[cfg(test)]
mod tests {
    use super::build_standard_request_body;
    use serde_json::json;

    #[test]
    fn builds_openai_responses_request_from_claude_cli_source_with_forced_stream() {
        let request = json!({
            "model": "claude-sonnet-4-5",
            "messages": [
                {
                    "role": "user",
                    "content": [{"type": "text", "text": "Need OpenAI Responses output"}]
                }
            ],
            "max_tokens": 64
        });

        let converted = build_standard_request_body(
            &request,
            "claude:messages",
            "gpt-5",
            "openai",
            "openai:responses",
            "/v1/messages",
            true,
            None,
            None,
        )
        .expect("claude cli should convert to openai responses");

        assert_eq!(converted["model"], "gpt-5");
        assert_eq!(converted["input"][0]["role"], "user");
        assert_eq!(converted["input"][0]["content"][0]["type"], "input_text");
        assert_eq!(
            converted["input"][0]["content"][0]["text"],
            "Need OpenAI Responses output"
        );
        assert_eq!(converted["stream"], true);
    }

    #[test]
    fn strips_metadata_for_codex_openai_responses_requests() {
        let request = json!({
            "model": "claude-sonnet-4-5",
            "metadata": {"trace_id": "abc"},
            "messages": [{
                "role": "user",
                "content": [{"type": "text", "text": "Need OpenAI Responses output"}]
            }],
            "max_tokens": 64
        });

        let converted = build_standard_request_body(
            &request,
            "claude:messages",
            "gpt-5.4",
            "codex",
            "openai:responses",
            "/v1/messages",
            false,
            None,
            None,
        )
        .expect("claude cli should convert to codex request");

        assert!(converted.get("metadata").is_none());
        assert_eq!(converted["store"], false);
        assert!(converted.get("instructions").is_none());
        assert_eq!(converted["include"], json!(["reasoning.encrypted_content"]));
        assert_eq!(converted["parallel_tool_calls"], true);
        assert_eq!(converted["reasoning"]["effort"], "medium");
        assert_eq!(converted["reasoning"]["summary"], "auto");
    }

    #[test]
    fn strips_store_for_openai_responses_compact_requests() {
        let request = json!({
            "model": "gpt-5",
            "messages": [{
                "role": "user",
                "content": "Hello from OpenAI Chat"
            }],
            "store": true
        });

        let converted = build_standard_request_body(
            &request,
            "openai:chat",
            "gpt-5",
            "openai",
            "openai:responses:compact",
            "/v1/chat/completions",
            false,
            None,
            None,
        )
        .expect("openai chat should convert to openai responses compact");

        assert_eq!(converted["model"], "gpt-5");
        assert!(converted.get("store").is_none());
    }
}
