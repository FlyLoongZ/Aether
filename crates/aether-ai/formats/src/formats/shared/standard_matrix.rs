use std::borrow::Cow;

use aether_ai_formats::formats::conversion::request::{
    convert_openai_chat_request_to_claude_request, convert_openai_chat_request_to_gemini_request,
    convert_openai_chat_request_to_openai_responses_request,
    normalize_claude_request_to_openai_chat_request,
    normalize_gemini_request_to_openai_chat_request,
    normalize_openai_responses_request_to_openai_chat_request,
};
use aether_ai_formats::formats::registry::{convert_request, FormatContext};
use aether_ai_formats::provider_compat::proxy::rules::apply_local_body_rules_with_request_headers;
use serde_json::Value;

use crate::formats::shared::model_directives::apply_model_directive_overrides_from_request;

use crate::formats::openai::responses::codex::{
    apply_codex_openai_responses_chat_body_edits, apply_codex_openai_responses_special_body_edits,
    apply_openai_responses_compact_special_body_edits,
};
use crate::formats::openai::responses::xai::apply_xai_upstream_payload_edits_with_client;
use crate::formats::shared::standard_normalize::{
    build_local_openai_chat_request_body_with_model_directives,
    is_claude_messages_shaped_body_on_openai_chat_endpoint,
};

/// Tool schema preservation is a format-conversion policy, shared by the
/// standard matrix and provider-aware Chat/Responses entry points.
pub(super) fn preserves_gemini_tool_schemas(
    provider_type: &str,
    provider_api_format: &str,
) -> bool {
    provider_type.trim().eq_ignore_ascii_case("antigravity")
        && aether_ai_formats::normalize_api_format_alias(provider_api_format)
            == "gemini:generate_content"
}

#[allow(clippy::too_many_arguments)]
pub fn build_standard_request_body(
    body_json: &Value,
    client_api_format: &str,
    mapped_model: &str,
    provider_type: &str,
    provider_api_format: &str,
    request_path: &str,
    upstream_is_stream: bool,
    body_rules: Option<&Value>,
    user_api_key_id: Option<&str>,
) -> Option<Value> {
    build_standard_request_body_with_model_directives(
        body_json,
        client_api_format,
        mapped_model,
        provider_type,
        provider_api_format,
        request_path,
        upstream_is_stream,
        body_rules,
        user_api_key_id,
        false,
    )
}

#[allow(clippy::too_many_arguments)]
pub fn build_standard_request_body_with_model_directives(
    body_json: &Value,
    client_api_format: &str,
    mapped_model: &str,
    provider_type: &str,
    provider_api_format: &str,
    request_path: &str,
    upstream_is_stream: bool,
    body_rules: Option<&Value>,
    user_api_key_id: Option<&str>,
    enable_model_directives: bool,
) -> Option<Value> {
    build_standard_request_body_with_model_directives_and_request_headers(
        body_json,
        client_api_format,
        mapped_model,
        provider_type,
        provider_api_format,
        request_path,
        upstream_is_stream,
        body_rules,
        user_api_key_id,
        None,
        enable_model_directives,
    )
}

#[allow(clippy::too_many_arguments)]
pub fn build_standard_request_body_with_model_directives_and_request_headers(
    body_json: &Value,
    client_api_format: &str,
    mapped_model: &str,
    provider_type: &str,
    provider_api_format: &str,
    request_path: &str,
    upstream_is_stream: bool,
    body_rules: Option<&Value>,
    user_api_key_id: Option<&str>,
    request_headers: Option<&http::HeaderMap>,
    enable_model_directives: bool,
) -> Option<Value> {
    build_standard_request_body_with_model_directives_and_request_headers_and_reasoning_replay_policy(
        body_json,
        client_api_format,
        mapped_model,
        provider_type,
        provider_api_format,
        request_path,
        upstream_is_stream,
        body_rules,
        user_api_key_id,
        request_headers,
        enable_model_directives,
        crate::formats::openai::responses::OpenAiResponsesReasoningReplayPolicy::OpenAiItemIds,
    )
}

#[allow(clippy::too_many_arguments)]
pub fn build_standard_request_body_with_model_directives_and_request_headers_and_reasoning_replay_policy(
    body_json: &Value,
    client_api_format: &str,
    mapped_model: &str,
    provider_type: &str,
    provider_api_format: &str,
    request_path: &str,
    upstream_is_stream: bool,
    body_rules: Option<&Value>,
    user_api_key_id: Option<&str>,
    request_headers: Option<&http::HeaderMap>,
    enable_model_directives: bool,
    reasoning_replay_policy: crate::formats::openai::responses::OpenAiResponsesReasoningReplayPolicy,
) -> Option<Value> {
    let reasoning_replay_policy = if provider_type.trim().eq_ignore_ascii_case("xai") {
        crate::formats::openai::responses::OpenAiResponsesReasoningReplayPolicy::XaiEncrypted
    } else {
        reasoning_replay_policy
    };
    let mut format_context = FormatContext::default()
        .with_mapped_model(mapped_model)
        .with_request_path(request_path)
        .with_upstream_stream(upstream_is_stream);
    format_context.preserve_gemini_tool_schemas =
        preserves_gemini_tool_schemas(provider_type, provider_api_format);
    if let Some(history_scope) = user_api_key_id {
        format_context = format_context.with_history_scope(history_scope);
    }
    let source_api_format = compatible_source_format_for_standard_request(
        body_json,
        client_api_format,
        provider_api_format,
    );
    // Keep the specialized OpenAI builders' compatibility/history preprocessing
    // when routing them through the provider-aware schema-preserving path.
    let antigravity_chat_body = if format_context.preserve_gemini_tool_schemas
        && matches!(
            aether_ai_formats::normalize_api_format_alias(source_api_format.as_ref()).as_str(),
            "openai:chat" | "openai:responses" | "openai:responses:compact"
        ) {
        Some(
            crate::formats::shared::standard_normalize::chat_compatible_body_for_standard_source(
                body_json,
                source_api_format.as_ref(),
                user_api_key_id,
            )?,
        )
    } else {
        None
    };
    // DeepSeek and xAI replay opaque provider state. Preserve their native
    // Responses input items: canonical conversion can lose reasoning IDs and
    // encrypted-only items even when source and destination formats are equal.
    let mut provider_request_body = if is_wire_preserving_responses_hop(
        source_api_format.as_ref(),
        provider_api_format,
        reasoning_replay_policy,
    ) {
        let mut object = body_json.as_object()?.clone();
        object.insert("model".to_string(), Value::String(mapped_model.to_string()));
        Value::Object(object)
    } else {
        convert_request(
            if antigravity_chat_body.is_some() {
                "openai:chat"
            } else {
                source_api_format.as_ref()
            },
            provider_api_format,
            antigravity_chat_body.as_deref().unwrap_or(body_json),
            &format_context,
        )
        .ok()?
    };

    if enable_model_directives {
        apply_model_directive_overrides_from_request(
            &mut provider_request_body,
            provider_api_format,
            mapped_model,
            body_json,
            Some(request_path),
        );
    }

    if !apply_local_body_rules_with_request_headers(
        &mut provider_request_body,
        body_rules,
        Some(body_json),
        request_headers,
    ) {
        return None;
    }
    let client_is_openai_responses_family = matches!(
        aether_ai_formats::normalize_api_format_alias(client_api_format).as_str(),
        "openai:responses" | "openai:responses:compact"
    );
    if client_is_openai_responses_family {
        apply_codex_openai_responses_special_body_edits(
            &mut provider_request_body,
            provider_type,
            provider_api_format,
            body_rules,
            user_api_key_id,
        );
    } else {
        apply_codex_openai_responses_chat_body_edits(
            &mut provider_request_body,
            provider_type,
            provider_api_format,
            body_rules,
            user_api_key_id,
        );
    }
    apply_openai_responses_compact_special_body_edits(
        &mut provider_request_body,
        provider_api_format,
    );
    apply_xai_upstream_payload_edits_with_client(
        &mut provider_request_body,
        provider_type,
        provider_api_format,
        Some(client_api_format),
        Some(body_json),
    );
    crate::formats::openai::responses::strip_incompatible_openai_responses_reasoning_items_with_policy(
        &mut provider_request_body,
        provider_api_format,
        reasoning_replay_policy,
    );
    strip_openai_responses_input_content_cache_control(
        &mut provider_request_body,
        provider_api_format,
    );
    let require_body_stream_field = body_json
        .as_object()
        .is_some_and(|object| object.contains_key("stream"))
        || provider_request_body
            .as_object()
            .is_some_and(|object| object.contains_key("stream"));
    crate::formats::shared::request::enforce_request_body_stream_field(
        &mut provider_request_body,
        provider_api_format,
        upstream_is_stream,
        require_body_stream_field,
    );
    Some(provider_request_body)
}

fn is_wire_preserving_responses_hop(
    source_api_format: &str,
    provider_api_format: &str,
    reasoning_replay_policy: crate::formats::openai::responses::OpenAiResponsesReasoningReplayPolicy,
) -> bool {
    if !matches!(
        reasoning_replay_policy,
        crate::formats::openai::responses::OpenAiResponsesReasoningReplayPolicy::DeepSeekOpaque
            | crate::formats::openai::responses::OpenAiResponsesReasoningReplayPolicy::XaiEncrypted
    ) {
        return false;
    }
    let source_api_format = aether_ai_formats::normalize_api_format_alias(source_api_format);
    let provider_api_format = aether_ai_formats::normalize_api_format_alias(provider_api_format);
    source_api_format == "openai:responses" && source_api_format == provider_api_format
}

fn compatible_source_format_for_standard_request<'a>(
    body_json: &Value,
    client_api_format: &'a str,
    provider_api_format: &str,
) -> Cow<'a, str> {
    if matches!(
        aether_ai_formats::normalize_api_format_alias(client_api_format).as_str(),
        "openai:chat"
    ) && matches!(
        aether_ai_formats::normalize_api_format_alias(provider_api_format).as_str(),
        "claude:messages"
    ) && is_claude_messages_shaped_body_on_openai_chat_endpoint(body_json)
    {
        return Cow::Borrowed("claude:messages");
    }
    Cow::Borrowed(client_api_format)
}

pub fn build_standard_request_body_from_canonical(
    canonical_request: &Value,
    mapped_model: &str,
    provider_api_format: &str,
    upstream_is_stream: bool,
) -> Option<Value> {
    build_standard_request_body_from_canonical_with_model_directives(
        canonical_request,
        mapped_model,
        provider_api_format,
        upstream_is_stream,
        false,
    )
}

pub fn build_standard_request_body_from_canonical_with_model_directives(
    canonical_request: &Value,
    mapped_model: &str,
    provider_api_format: &str,
    upstream_is_stream: bool,
    enable_model_directives: bool,
) -> Option<Value> {
    let mut provider_request_body =
        match aether_ai_formats::normalize_api_format_alias(provider_api_format).as_str() {
            "openai:chat" => build_local_openai_chat_request_body_with_model_directives(
                canonical_request,
                mapped_model,
                upstream_is_stream,
                enable_model_directives,
            ),
            "openai:responses" => convert_openai_chat_request_to_openai_responses_request(
                canonical_request,
                mapped_model,
                upstream_is_stream,
                false,
            ),
            "openai:responses:compact" => convert_openai_chat_request_to_openai_responses_request(
                canonical_request,
                mapped_model,
                false,
                true,
            ),
            "claude:messages" => convert_openai_chat_request_to_claude_request(
                canonical_request,
                mapped_model,
                upstream_is_stream,
            ),
            "gemini:generate_content" => convert_openai_chat_request_to_gemini_request(
                canonical_request,
                mapped_model,
                upstream_is_stream,
            ),
            _ => None,
        }?;
    if enable_model_directives {
        apply_model_directive_overrides_from_request(
            &mut provider_request_body,
            provider_api_format,
            mapped_model,
            canonical_request,
            None,
        );
    }
    strip_openai_responses_input_content_cache_control(
        &mut provider_request_body,
        provider_api_format,
    );
    Some(provider_request_body)
}

fn strip_openai_responses_input_content_cache_control(
    provider_request_body: &mut Value,
    provider_api_format: &str,
) {
    if !matches!(
        aether_ai_formats::normalize_api_format_alias(provider_api_format).as_str(),
        "openai:responses" | "openai:responses:compact"
    ) {
        return;
    }
    let Some(input) = provider_request_body.get_mut("input") else {
        return;
    };
    strip_responses_input_items_content_cache_control(input);
}

fn strip_responses_input_items_content_cache_control(value: &mut Value) {
    match value {
        Value::Array(items) => {
            for item in items {
                strip_responses_input_items_content_cache_control(item);
            }
        }
        Value::Object(item) => {
            if let Some(content) = item.get_mut("content") {
                strip_responses_content_cache_control(content);
            }
        }
        _ => {}
    }
}

fn strip_responses_content_cache_control(content: &mut Value) {
    match content {
        Value::Array(parts) => {
            for part in parts {
                if let Some(part) = part.as_object_mut() {
                    part.remove("cache_control");
                }
            }
        }
        Value::Object(part) => {
            part.remove("cache_control");
        }
        _ => {}
    }
}

pub fn normalize_standard_request_to_openai_chat_request(
    body_json: &Value,
    client_api_format: &str,
    request_path: &str,
) -> Option<Value> {
    normalize_standard_request_to_openai_chat_request_cow(
        body_json,
        client_api_format,
        request_path,
    )
    .map(Cow::into_owned)
}

fn normalize_standard_request_to_openai_chat_request_cow<'a>(
    body_json: &'a Value,
    client_api_format: &str,
    request_path: &str,
) -> Option<Cow<'a, Value>> {
    match aether_ai_formats::normalize_api_format_alias(client_api_format).as_str() {
        "openai:chat" => Some(Cow::Borrowed(body_json)),
        "openai:responses" | "openai:responses:compact" => {
            normalize_openai_responses_request_to_openai_chat_request(body_json).map(Cow::Owned)
        }
        "claude:messages" => {
            normalize_claude_request_to_openai_chat_request(body_json).map(Cow::Owned)
        }
        "gemini:generate_content" => {
            normalize_gemini_request_to_openai_chat_request(body_json, request_path).map(Cow::Owned)
        }
        _ => None,
    }
}

#[cfg(test)]
#[path = "standard_matrix/tests.rs"]
mod tests;
