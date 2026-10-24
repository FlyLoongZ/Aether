use serde_json::{Map, Value};

use crate::formats::openai::namespace::NamespaceToolAliases;
use crate::formats::{
    aliyun,
    claude::messages as claude_messages,
    doubao,
    gemini::{self, generate_content as gemini_generate_content},
    id::FormatId,
    jina,
    openai::{self, chat as openai_chat, responses as openai_responses},
};
use crate::protocol::canonical::{
    CanonicalContentBlock, CanonicalEmbeddingInput, CanonicalRequest, CanonicalResponse,
    CanonicalStopReason,
};

pub use crate::formats::context::{
    ConversionFieldStatus, ConversionReport, Converted, FormatContext, FormatError,
};

pub fn parse_request(
    source_format: &str,
    body: &Value,
    ctx: &FormatContext,
) -> Result<CanonicalRequest, FormatError> {
    let source = parse_format(source_format)?;
    match source {
        FormatId::OpenAiChat => openai_chat::request::from(body, ctx),
        FormatId::OpenAiResponses | FormatId::OpenAiResponsesCompact => {
            openai_responses::request::from(body, ctx)
        }
        FormatId::ClaudeMessages => claude_messages::request::from(body, ctx),
        FormatId::GeminiGenerateContent => gemini_generate_content::request::from(body, ctx),
        FormatId::OpenAiEmbedding => openai::embedding::request::from(body, ctx),
        FormatId::JinaEmbedding => jina::embedding::request::from(body, ctx),
        FormatId::OpenAiRerank => openai::rerank::request::from(body, ctx),
        FormatId::JinaRerank => jina::rerank::request::from(body, ctx),
        FormatId::GeminiEmbedding => gemini::embedding::request::from(body, ctx),
        FormatId::DoubaoEmbedding => doubao::embedding::request::from(body, ctx),
        FormatId::AliyunMultimodalEmbedding => aliyun::embedding::request::from(body, ctx),
        FormatId::OpenAiRealtime
        | FormatId::OpenAiSearch
        | FormatId::CodexLive
        | FormatId::GeminiInteractions => None,
    }
    .ok_or_else(|| FormatError::RequestParseFailed {
        format: source.as_str().to_string(),
    })
}

pub fn emit_request(
    target_format: &str,
    request: &CanonicalRequest,
    ctx: &FormatContext,
) -> Result<Value, FormatError> {
    emit_request_inner(target_format, request, &ctx.without_runtime_request_edits())
}

fn emit_request_inner(
    target_format: &str,
    request: &CanonicalRequest,
    ctx: &FormatContext,
) -> Result<Value, FormatError> {
    let target = parse_format(target_format)?;
    match target {
        FormatId::OpenAiChat => openai_chat::request::to(request, ctx),
        FormatId::OpenAiResponses => openai_responses::request::to(request, ctx),
        FormatId::OpenAiResponsesCompact => openai_responses::request::to_compact(request, ctx),
        FormatId::ClaudeMessages => claude_messages::request::to(request, ctx),
        FormatId::GeminiGenerateContent => gemini_generate_content::request::to(request, ctx),
        FormatId::OpenAiEmbedding => openai::embedding::request::to(request, ctx),
        FormatId::JinaEmbedding => jina::embedding::request::to(request, ctx),
        FormatId::OpenAiRerank => openai::rerank::request::to(request, ctx),
        FormatId::JinaRerank => jina::rerank::request::to(request, ctx),
        FormatId::GeminiEmbedding => gemini::embedding::request::to(request, ctx),
        FormatId::DoubaoEmbedding => doubao::embedding::request::to(request, ctx),
        FormatId::AliyunMultimodalEmbedding => aliyun::embedding::request::to(request, ctx),
        FormatId::OpenAiRealtime
        | FormatId::OpenAiSearch
        | FormatId::CodexLive
        | FormatId::GeminiInteractions => None,
    }
    .ok_or_else(|| FormatError::RequestEmitFailed {
        format: target.as_str().to_string(),
    })
}

pub fn parse_request_pure(
    source_format: &str,
    body: &Value,
) -> Result<CanonicalRequest, FormatError> {
    parse_request(source_format, body, &FormatContext::default())
}

pub fn emit_request_pure(
    target_format: &str,
    request: &CanonicalRequest,
) -> Result<Value, FormatError> {
    emit_request_inner(target_format, request, &FormatContext::default())
}

pub fn convert_request_pure(
    source_format: &str,
    target_format: &str,
    body: &Value,
) -> Result<Converted<Value>, FormatError> {
    convert_request_pure_with_context(
        source_format,
        target_format,
        body,
        &FormatContext::default(),
    )
}

pub fn convert_request_pure_with_context(
    source_format: &str,
    target_format: &str,
    body: &Value,
    ctx: &FormatContext,
) -> Result<Converted<Value>, FormatError> {
    let pure_ctx = ctx.without_runtime_request_edits();
    let source = parse_format(source_format)?;
    let target = parse_format(target_format)?;
    let normalized_body = normalize_openai_responses_to_chat_body(source, target, body)?;
    let request = parse_request(source_format, &normalized_body, &pure_ctx)?;
    validate_openai_responses_target_contract(target_format, &normalized_body)?;
    validate_request_conversion(
        source_format,
        target_format,
        &normalized_body,
        &request,
        ctx.mapped_model.as_deref(),
    )?;
    let value = emit_request_inner(target_format, &request, &pure_ctx)?;
    let report = build_request_conversion_report(source_format, target_format, body, &value);
    Ok(Converted { value, report })
}

pub fn convert_request(
    source_format: &str,
    target_format: &str,
    body: &Value,
    ctx: &FormatContext,
) -> Result<Value, FormatError> {
    let source = parse_format(source_format)?;
    let target = parse_format(target_format)?;
    let expanded_body = if source == FormatId::OpenAiResponses && target == FormatId::OpenAiChat {
        Some(
            openai_responses::history::expand_previous_response_for_chat(
                body,
                ctx.history_scope.as_deref(),
            )
            .map_err(|reason| FormatError::UnsupportedField {
                format: source.as_str().to_string(),
                field: "previous_response_id".to_string(),
                reason,
            })?,
        )
    } else {
        None
    };
    let body = expanded_body.as_ref().unwrap_or(body);
    let normalized_body = normalize_openai_responses_to_chat_body(source, target, body)?;
    validate_openai_responses_target_contract(target_format, &normalized_body)?;
    let mut request = parse_request(source_format, &normalized_body, ctx)?;
    validate_runtime_request_conversion(
        source,
        target,
        &normalized_body,
        &request,
        ctx.mapped_model.as_deref(),
    )?;
    if let Some(mapped_model) = ctx
        .mapped_model
        .as_deref()
        .filter(|value| !value.trim().is_empty())
    {
        request.model = mapped_model.to_string();
    }
    emit_request_inner(target_format, &request, ctx)
}

fn normalize_openai_responses_to_chat_body(
    source: FormatId,
    target: FormatId,
    body: &Value,
) -> Result<Value, FormatError> {
    if !matches!(
        source,
        FormatId::OpenAiResponses | FormatId::OpenAiResponsesCompact
    ) || target != FormatId::OpenAiChat
    {
        return Ok(body.clone());
    }

    let Some(body_object) = body.as_object() else {
        return Ok(body.clone());
    };
    let Some(input) = body_object.get("input").and_then(Value::as_array) else {
        return Ok(body.clone());
    };
    let additional_tools_count = input
        .iter()
        .take_while(|item| is_openai_responses_additional_tools_item(item))
        .count();
    if additional_tools_count == 0 {
        return Ok(body.clone());
    }
    if body_object
        .get("tools")
        .is_some_and(|tools| !tools.is_array())
    {
        return Ok(body.clone());
    }

    let mut normalized = body.clone();
    let normalized_object = normalized
        .as_object_mut()
        .expect("Responses request body object was checked above");
    let normalized_input = normalized_object
        .get_mut("input")
        .and_then(Value::as_array_mut)
        .expect("Responses request input array was checked above");
    let additional_tools = normalized_input.drain(..additional_tools_count);

    let mut tools = Vec::new();
    for additional_tools in additional_tools {
        tools.extend(
            additional_tools["tools"]
                .as_array()
                .expect("additional_tools item was checked above")
                .iter()
                .cloned(),
        );
    }
    if let Some(existing_tools) = normalized_object.get("tools").and_then(Value::as_array) {
        tools.extend(existing_tools.iter().cloned());
    }
    normalized_object.insert("tools".to_string(), Value::Array(tools));
    Ok(normalized)
}

fn is_openai_responses_additional_tools_item(value: &Value) -> bool {
    let Some(object) = value.as_object() else {
        return false;
    };
    object
        .get("type")
        .and_then(Value::as_str)
        .is_some_and(|item_type| item_type.eq_ignore_ascii_case("additional_tools"))
        && object.get("role").and_then(Value::as_str) == Some("developer")
        && object.get("tools").is_some_and(Value::is_array)
        && object
            .keys()
            .all(|key| matches!(key.as_str(), "type" | "role" | "tools"))
}

fn validate_runtime_request_conversion(
    source: FormatId,
    target: FormatId,
    body: &Value,
    request: &CanonicalRequest,
    mapped_model: Option<&str>,
) -> Result<(), FormatError> {
    validate_gemini_mixed_tool_model(source, target, request, mapped_model)?;
    validate_openai_cross_format_store(source, target, body)?;
    validate_openai_prompt_cache_contract(source, body, mapped_model)?;
    validate_openai_reasoning_effort(source, target, body, mapped_model)?;
    validate_openai_responses_cross_format_input(source, target, body)?;
    validate_openai_responses_runtime_reasoning(source, target, body)?;
    validate_openai_responses_namespace_tools_to_chat(source, target, request)?;
    if matches!(source, FormatId::OpenAiChat)
        && matches!(
            target,
            FormatId::OpenAiResponses | FormatId::OpenAiResponsesCompact
        )
        && body
            .as_object()
            .is_some_and(openai_chat_request_has_unsupported_responses_cache_breakpoint)
    {
        return Err(FormatError::LossyConversionBlocked {
            source_format: source.as_str().to_string(),
            target_format: target.as_str().to_string(),
            field: "messages[].content[].prompt_cache_breakpoint".to_string(),
            reason: "OpenAI Responses supports prompt cache breakpoints only on input_text, input_image, and input_file blocks".to_string(),
        });
    }
    if source == FormatId::ClaudeMessages {
        match target {
            FormatId::OpenAiChat
                if claude_request_contains_unrepresentable_tool_result_content_for_openai_chat(
                    body,
                ) =>
            {
                return Err(FormatError::LossyConversionBlocked {
                    source_format: source.as_str().to_string(),
                    target_format: target.as_str().to_string(),
                    field: "messages[].content[].tool_result.content".to_string(),
                    reason: "OpenAI Chat tool messages cannot represent one or more Claude tool_result content blocks".to_string(),
                });
            }
            FormatId::OpenAiResponses | FormatId::OpenAiResponsesCompact
                if claude_request_contains_unrepresentable_tool_result_content_for_openai_responses(
                    body,
                ) =>
            {
                return Err(FormatError::LossyConversionBlocked {
                    source_format: source.as_str().to_string(),
                    target_format: target.as_str().to_string(),
                    field: "messages[].content[].tool_result.content".to_string(),
                    reason: "OpenAI Responses function_call_output cannot represent one or more Claude tool_result content blocks".to_string(),
                });
            }
            _ => {}
        }
    }
    Ok(())
}

fn validate_openai_responses_target_contract(
    target_format: &str,
    body: &Value,
) -> Result<(), FormatError> {
    if !matches!(
        FormatId::parse(target_format),
        Some(FormatId::OpenAiResponses | FormatId::OpenAiResponsesCompact)
    ) {
        return Ok(());
    }
    crate::formats::openai::responses::request::validate_openai_responses_request_contract(
        body,
        target_format,
    )
    .map_err(|violation| FormatError::InvalidTargetField {
        format: crate::normalize_api_format_alias(target_format),
        field: violation.field.to_string(),
        reason: violation.reason.to_string(),
    })
}

fn validate_openai_responses_runtime_reasoning(
    source: FormatId,
    target: FormatId,
    body: &Value,
) -> Result<(), FormatError> {
    if !matches!(
        source,
        FormatId::OpenAiResponses | FormatId::OpenAiResponsesCompact
    ) || matches!(
        target,
        FormatId::OpenAiResponses | FormatId::OpenAiResponsesCompact
    ) {
        return Ok(());
    }
    let Some(reasoning) = body.get("reasoning").and_then(Value::as_object) else {
        return Ok(());
    };
    if let Some(field) = reasoning
        .keys()
        .find(|field| !matches!(field.as_str(), "effort" | "summary"))
    {
        return openai_responses_lossy_input(
            source,
            target,
            format!("thinking.openai_responses.{field}"),
            "target format cannot preserve this provider-specific Responses reasoning field",
        );
    }
    Ok(())
}

pub fn parse_response(
    source_format: &str,
    body: &Value,
    ctx: &FormatContext,
) -> Result<CanonicalResponse, FormatError> {
    let source = parse_format(source_format)?;
    match source {
        FormatId::OpenAiChat => openai_chat::response::from(body, ctx),
        FormatId::OpenAiResponses | FormatId::OpenAiResponsesCompact => {
            openai_responses::response::from(body, ctx)
        }
        FormatId::ClaudeMessages => claude_messages::response::from(body, ctx),
        FormatId::GeminiGenerateContent => gemini_generate_content::response::from(body, ctx),
        FormatId::OpenAiEmbedding
        | FormatId::OpenAiRealtime
        | FormatId::OpenAiSearch
        | FormatId::JinaEmbedding
        | FormatId::OpenAiRerank
        | FormatId::JinaRerank
        | FormatId::GeminiEmbedding
        | FormatId::DoubaoEmbedding
        | FormatId::AliyunMultimodalEmbedding
        | FormatId::CodexLive
        | FormatId::GeminiInteractions => None,
    }
    .ok_or_else(|| FormatError::ResponseParseFailed {
        format: source.as_str().to_string(),
    })
}

pub fn emit_response(
    target_format: &str,
    response: &CanonicalResponse,
    ctx: &FormatContext,
) -> Result<Value, FormatError> {
    emit_response_inner(
        target_format,
        response,
        &ctx.without_runtime_request_edits(),
    )
}

fn emit_response_inner(
    target_format: &str,
    response: &CanonicalResponse,
    ctx: &FormatContext,
) -> Result<Value, FormatError> {
    let target = parse_format(target_format)?;
    match target {
        FormatId::OpenAiChat => openai_chat::response::to(response, ctx),
        FormatId::OpenAiResponses => openai_responses::response::to(response, ctx),
        FormatId::OpenAiResponsesCompact => openai_responses::response::to_compact(response, ctx),
        FormatId::ClaudeMessages => claude_messages::response::to(response, ctx),
        FormatId::GeminiGenerateContent => gemini_generate_content::response::to(response, ctx),
        FormatId::OpenAiEmbedding
        | FormatId::OpenAiRealtime
        | FormatId::OpenAiSearch
        | FormatId::JinaEmbedding
        | FormatId::OpenAiRerank
        | FormatId::JinaRerank
        | FormatId::GeminiEmbedding
        | FormatId::DoubaoEmbedding
        | FormatId::AliyunMultimodalEmbedding
        | FormatId::CodexLive
        | FormatId::GeminiInteractions => None,
    }
    .ok_or_else(|| FormatError::ResponseEmitFailed {
        format: target.as_str().to_string(),
    })
}

pub fn parse_response_pure(
    source_format: &str,
    body: &Value,
) -> Result<CanonicalResponse, FormatError> {
    parse_response(source_format, body, &FormatContext::default())
}

pub fn emit_response_pure(
    target_format: &str,
    response: &CanonicalResponse,
) -> Result<Value, FormatError> {
    emit_response_inner(target_format, response, &FormatContext::default())
}

pub fn convert_response_pure(
    source_format: &str,
    target_format: &str,
    body: &Value,
) -> Result<Converted<Value>, FormatError> {
    let response = parse_response_pure(source_format, body)?;
    validate_response_conversion(source_format, target_format, body, &response)?;
    let value = emit_response_pure(target_format, &response)?;
    let report = build_response_conversion_report(source_format, target_format, body, &value);
    Ok(Converted { value, report })
}

pub fn convert_response(
    source_format: &str,
    target_format: &str,
    body: &Value,
    ctx: &FormatContext,
) -> Result<Value, FormatError> {
    let mut response = parse_response(source_format, body, ctx)?;
    validate_response_conversion(source_format, target_format, body, &response)?;
    if response.model.trim().is_empty() || response.model == "unknown" {
        if let Some(mapped_model) = ctx
            .mapped_model
            .as_deref()
            .filter(|value| !value.trim().is_empty())
        {
            response.model = mapped_model.to_string();
        }
    }
    emit_response_inner(target_format, &response, ctx)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamTranscoderSpec {
    pub source: FormatId,
    pub target: FormatId,
}

pub fn build_stream_transcoder(
    source_format: &str,
    target_format: &str,
    _ctx: &FormatContext,
) -> Result<StreamTranscoderSpec, FormatError> {
    Ok(StreamTranscoderSpec {
        source: parse_format(source_format)?,
        target: parse_format(target_format)?,
    })
}

fn parse_format(format: &str) -> Result<FormatId, FormatError> {
    FormatId::parse(format).ok_or_else(|| FormatError::UnsupportedFormat(format.to_string()))
}

fn validate_request_conversion(
    source_format: &str,
    target_format: &str,
    body: &Value,
    request: &CanonicalRequest,
    mapped_model: Option<&str>,
) -> Result<(), FormatError> {
    let source = parse_format(source_format)?;
    let target = parse_format(target_format)?;
    validate_gemini_mixed_tool_model(source, target, request, mapped_model)?;
    validate_openai_prompt_cache_contract(source, body, mapped_model)?;
    validate_openai_reasoning_effort(source, target, body, mapped_model)?;
    if source == target {
        return Ok(());
    }
    if is_embedding_format(source) || is_embedding_format(target) {
        return validate_embedding_request_conversion(source, target, request);
    }
    if is_rerank_format(source) || is_rerank_format(target) {
        return validate_rerank_request_conversion(source, target, request);
    }
    validate_known_standard_request_root_fields(source, target, body)?;
    validate_openai_responses_cross_format_input(source, target, body)?;
    validate_cross_format_generation_target(source, target, request)?;
    validate_openai_cross_format_store(source, target, body)?;
    match (source, target) {
        (FormatId::OpenAiChat, FormatId::OpenAiResponses | FormatId::OpenAiResponsesCompact) => {
            validate_openai_chat_to_responses(body)?;
        }
        (FormatId::OpenAiResponses | FormatId::OpenAiResponsesCompact, FormatId::OpenAiChat) => {
            validate_openai_responses_to_chat(body, request)?;
        }
        (FormatId::ClaudeMessages, target) if target != FormatId::ClaudeMessages => {
            validate_claude_cross_format_request(body, target)?;
        }
        (FormatId::GeminiGenerateContent, target) if target != FormatId::GeminiGenerateContent => {
            validate_gemini_cross_format_request(body, target)?;
        }
        _ => {}
    }
    validate_cross_format_request_extensions(source, target, request)
}

fn validate_gemini_mixed_tool_model(
    source: FormatId,
    target: FormatId,
    request: &CanonicalRequest,
    mapped_model: Option<&str>,
) -> Result<(), FormatError> {
    if source == target
        || target != FormatId::GeminiGenerateContent
        || !gemini_generate_content::request::canonical_has_mixed_gemini_tools(request)
    {
        return Ok(());
    }
    let target_model = mapped_model
        .map(str::trim)
        .filter(|model| !model.is_empty())
        .unwrap_or(request.model.trim());
    if crate::formats::shared::model_directives::gemini_model_supports_mixed_tools(target_model) {
        return Ok(());
    }
    Err(FormatError::InvalidTargetField {
        format: target.as_str().to_string(),
        field: "tools".to_string(),
        reason: format!(
            "model {target_model:?} does not support combining built-in tools with custom function declarations; use a Gemini 3 model"
        ),
    })
}

fn validate_openai_responses_cross_format_input(
    source: FormatId,
    target: FormatId,
    body: &Value,
) -> Result<(), FormatError> {
    if !matches!(
        source,
        FormatId::OpenAiResponses | FormatId::OpenAiResponsesCompact
    ) || matches!(
        target,
        FormatId::OpenAiResponses | FormatId::OpenAiResponsesCompact
    ) {
        return Ok(());
    }
    if body
        .get("multi_agent")
        .and_then(Value::as_object)
        .and_then(|multi_agent| multi_agent.get("enabled"))
        .and_then(Value::as_bool)
        == Some(true)
    {
        return openai_responses_lossy_input(
            source,
            target,
            "multi_agent".to_string(),
            "target format cannot represent the OpenAI multi-agent request contract",
        );
    }

    let Some(items) = body.get("input").and_then(Value::as_array) else {
        return Ok(());
    };
    for (item_index, item) in items.iter().enumerate() {
        let Some(item_object) = item.as_object() else {
            if !item.is_string() {
                return openai_responses_lossy_input(
                    source,
                    target,
                    format!("input[{item_index}]"),
                    "target format cannot represent this raw Responses input item",
                );
            }
            continue;
        };
        let item_type = item_object
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or("message")
            .trim()
            .to_ascii_lowercase();
        if !matches!(
            item_type.as_str(),
            "message"
                | "reasoning"
                | "function_call"
                | "custom_tool_call"
                | "function_call_output"
                | "custom_tool_call_output"
                | "local_shell_call_output"
                | "shell_call_output"
                | "apply_patch_call_output"
                | "computer_call_output"
        ) {
            return openai_responses_lossy_input(
                source,
                target,
                format!("input[{item_index}]"),
                "target format has no lossless mapping for this Responses input item type",
            );
        }
        if matches!(item_type.as_str(), "function_call" | "function_call_output")
            && item_object.contains_key("caller")
        {
            return openai_responses_lossy_input(
                source,
                target,
                format!("input[{item_index}].caller"),
                "target format cannot preserve function call caller provenance",
            );
        }
        if item_type != "message" {
            continue;
        }
        let Some(content) = item_object.get("content").and_then(Value::as_array) else {
            continue;
        };
        for (content_index, block) in content.iter().enumerate() {
            let block_type = block
                .get("type")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .trim()
                .to_ascii_lowercase();
            if !matches!(
                block_type.as_str(),
                "input_text"
                    | "output_text"
                    | "text"
                    | "input_image"
                    | "output_image"
                    | "image_url"
                    | "input_file"
                    | "file"
                    | "input_audio"
                    | "audio"
                    | "output_audio"
                    | "reasoning"
                    | "thinking"
                    | "refusal"
            ) {
                return openai_responses_lossy_input(
                    source,
                    target,
                    format!("input[{item_index}].content[{content_index}]"),
                    "target format has no lossless mapping for this raw Responses content block",
                );
            }
        }
    }
    Ok(())
}

fn openai_responses_lossy_input(
    source: FormatId,
    target: FormatId,
    field: String,
    reason: &str,
) -> Result<(), FormatError> {
    Err(FormatError::LossyConversionBlocked {
        source_format: source.as_str().to_string(),
        target_format: target.as_str().to_string(),
        field,
        reason: reason.to_string(),
    })
}

fn validate_response_conversion(
    source_format: &str,
    target_format: &str,
    body: &Value,
    response: &CanonicalResponse,
) -> Result<(), FormatError> {
    let source = parse_format(source_format)?;
    let target = parse_format(target_format)?;
    if source == target {
        return Ok(());
    }

    validate_source_response_stop_enums(source, target, body)?;
    validate_response_content_has_no_unknown_blocks(source, target, response)?;
    validate_openai_responses_cross_format_response_extensions(source, target, response)?;
    validate_canonical_response_stop_reasons(source, target, response)
}

fn validate_openai_responses_cross_format_response_extensions(
    source: FormatId,
    target: FormatId,
    response: &CanonicalResponse,
) -> Result<(), FormatError> {
    if !matches!(
        source,
        FormatId::OpenAiResponses | FormatId::OpenAiResponsesCompact
    ) || matches!(
        target,
        FormatId::OpenAiResponses | FormatId::OpenAiResponsesCompact
    ) {
        return Ok(());
    }

    validate_openai_responses_response_extension_namespace(
        source,
        target,
        "response",
        &response.extensions,
        |key, value| {
            matches!(key, "raw_status" | "incomplete_details")
                || (key == "output_text" && value.is_string())
                || (matches!(key, "created_at" | "completed_at")
                    && (value.is_number() || value.is_null()))
                || (key == "error" && value.is_null())
                || (target == FormatId::OpenAiChat && key == "service_tier")
        },
    )?;

    for output in &response.outputs {
        validate_openai_responses_message_item_provenance(source, target, &output.extensions)?;
        validate_openai_responses_response_extension_namespace(
            source,
            target,
            "output[]",
            &output.extensions,
            |key, _| key == "message_items",
        )?;
    }

    if let Some(usage) = &response.usage {
        validate_openai_responses_usage_extension_fields(source, target, &usage.extensions)?;
        validate_openai_responses_response_extension_namespace(
            source,
            target,
            "usage",
            &usage.extensions,
            |key, _| matches!(key, "input_tokens_details" | "output_tokens_details"),
        )?;
    }

    for block in &response.content {
        if let CanonicalContentBlock::Thinking {
            encrypted_content: Some(encrypted_content),
            ..
        } = block
        {
            if !encrypted_content.is_empty() {
                return openai_responses_lossy_response(
                    source,
                    target,
                    "output[].encrypted_content",
                    "target format cannot preserve encrypted OpenAI reasoning content",
                );
            }
        }
        let (location, extensions): (&str, &std::collections::BTreeMap<String, Value>) = match block
        {
            CanonicalContentBlock::Text { extensions, .. } => ("output[].content[]", extensions),
            CanonicalContentBlock::Thinking { extensions, .. } => {
                ("output[].reasoning", extensions)
            }
            CanonicalContentBlock::Image { extensions, .. }
            | CanonicalContentBlock::File { extensions, .. }
            | CanonicalContentBlock::Audio { extensions, .. } => ("output[].content[]", extensions),
            CanonicalContentBlock::ToolUse { extensions, .. } => {
                ("output[].function_call", extensions)
            }
            CanonicalContentBlock::ToolResult { extensions, .. } => {
                ("output[].function_call_output", extensions)
            }
            CanonicalContentBlock::Unknown { .. } => continue,
        };
        let has_chat_metadata_passthrough = extensions.iter().any(|(namespace, value)| {
            matches!(namespace.as_str(), "openai_responses" | "openai_cli")
                && value.as_object().is_some_and(|object| {
                    object.contains_key("internal_chat_message_metadata_passthrough")
                })
        });
        validate_openai_responses_response_extension_namespace(
            source,
            target,
            location,
            extensions,
            |key, value| match block {
                CanonicalContentBlock::Text { .. } => {
                    key == "annotations"
                        && (target == FormatId::OpenAiChat
                            || value.as_array().is_some_and(Vec::is_empty))
                }
                CanonicalContentBlock::Thinking { .. } => key == "item_type",
                CanonicalContentBlock::ToolUse { .. } => {
                    // Codex may attach a private Responses-to-Chat transport sidecar.
                    // Only a Chat target may intentionally consume the tool call while
                    // discarding that paired transport metadata.
                    matches!(key, "item_id" | "item_type" | "status")
                        || (target == FormatId::OpenAiChat
                            && (key == "internal_chat_message_metadata_passthrough"
                                || (key == "metadata" && has_chat_metadata_passthrough)))
                }
                CanonicalContentBlock::ToolResult { .. } => key == "item_type",
                CanonicalContentBlock::Image { .. }
                | CanonicalContentBlock::File { .. }
                | CanonicalContentBlock::Audio { .. } => key == "item_type",
                CanonicalContentBlock::Unknown { .. } => false,
            },
        )?;
    }
    Ok(())
}

fn validate_openai_responses_message_item_provenance(
    source: FormatId,
    target: FormatId,
    extensions: &std::collections::BTreeMap<String, Value>,
) -> Result<(), FormatError> {
    for (namespace, value) in extensions {
        if !matches!(namespace.as_str(), "openai_responses" | "openai_cli") {
            continue;
        }
        let Some(message_items) = value
            .as_object()
            .and_then(|object| object.get("message_items"))
        else {
            continue;
        };
        let Some(message_items) = message_items.as_array() else {
            return openai_responses_lossy_response(
                source,
                target,
                "output[].message_items",
                "Responses message item provenance is not an array",
            );
        };
        for item in message_items {
            let Some(item) = item.as_object() else {
                return openai_responses_lossy_response(
                    source,
                    target,
                    "output[].message_items[]",
                    "Responses message item provenance entry is not an object",
                );
            };
            let output_index = item
                .get("output_index")
                .and_then(Value::as_u64)
                .map(|index| index.to_string())
                .unwrap_or_else(|| "?".to_string());
            let Some(fields) = item.get("fields").and_then(Value::as_object) else {
                return openai_responses_lossy_response(
                    source,
                    target,
                    &format!("output[{output_index}]"),
                    "Responses message item provenance fields are not an object",
                );
            };
            if let Some(field) = fields.keys().next() {
                return openai_responses_lossy_response(
                    source,
                    target,
                    &format!("output[{output_index}].{field}"),
                    "target format cannot preserve this Responses message item field",
                );
            }
        }
    }
    Ok(())
}

fn validate_openai_responses_usage_extension_fields(
    source: FormatId,
    target: FormatId,
    extensions: &std::collections::BTreeMap<String, Value>,
) -> Result<(), FormatError> {
    for (namespace, value) in extensions {
        if !matches!(namespace.as_str(), "openai_responses" | "openai_cli") {
            continue;
        }
        let Some(usage) = value.as_object() else {
            continue;
        };
        for (details_key, mapped_fields) in [
            (
                "input_tokens_details",
                &["cached_tokens", "cache_write_tokens"] as &[&str],
            ),
            ("output_tokens_details", &["reasoning_tokens"] as &[&str]),
        ] {
            let Some(details) = usage.get(details_key) else {
                continue;
            };
            let Some(details) = details.as_object() else {
                return openai_responses_lossy_response(
                    source,
                    target,
                    &format!("usage.{details_key}"),
                    "Responses usage details are not an object",
                );
            };
            for (field, value) in details {
                let mapped = mapped_fields.contains(&field.as_str()) && value.as_u64().is_some();
                let cache_write_supported =
                    field != "cache_write_tokens" || target != FormatId::GeminiGenerateContent;
                if mapped && cache_write_supported {
                    continue;
                }
                return openai_responses_lossy_response(
                    source,
                    target,
                    &format!("usage.{details_key}.{field}"),
                    "target format cannot preserve this Responses usage detail field",
                );
            }
        }
    }
    Ok(())
}

fn validate_openai_responses_response_extension_namespace(
    source: FormatId,
    target: FormatId,
    location: &str,
    extensions: &std::collections::BTreeMap<String, Value>,
    is_safe: impl Fn(&str, &Value) -> bool,
) -> Result<(), FormatError> {
    for (namespace, value) in extensions {
        if namespace == "aether" {
            continue;
        }
        let Some(object) = value.as_object() else {
            return openai_responses_lossy_response(
                source,
                target,
                location,
                "Responses extension namespace is not an object",
            );
        };
        for (key, value) in object {
            let internal_openai_field = namespace == "openai" && key == "omit_reasoning_parts";
            if internal_openai_field
                || (matches!(namespace.as_str(), "openai_responses" | "openai_cli")
                    && is_safe(key, value))
            {
                continue;
            }
            return openai_responses_lossy_response(
                source,
                target,
                &extension_field_path(location, namespace, key),
                "target format cannot preserve this Responses provider field",
            );
        }
    }
    Ok(())
}

fn openai_responses_lossy_response(
    source: FormatId,
    target: FormatId,
    field: &str,
    reason: &str,
) -> Result<(), FormatError> {
    Err(FormatError::LossyConversionBlocked {
        source_format: source.as_str().to_string(),
        target_format: target.as_str().to_string(),
        field: field.to_string(),
        reason: reason.to_string(),
    })
}

fn validate_response_content_has_no_unknown_blocks(
    source: FormatId,
    target: FormatId,
    response: &CanonicalResponse,
) -> Result<(), FormatError> {
    for block in response.content.iter().chain(
        response
            .outputs
            .iter()
            .flat_map(|output| output.content.iter()),
    ) {
        if let CanonicalContentBlock::Unknown { raw_type, .. } = block {
            if raw_type == "refusal"
                && matches!(
                    target,
                    FormatId::OpenAiChat
                        | FormatId::OpenAiResponses
                        | FormatId::OpenAiResponsesCompact
                )
            {
                continue;
            }
            return Err(FormatError::LossyConversionBlocked {
                source_format: source.as_str().to_string(),
                target_format: target.as_str().to_string(),
                field: "output[].type".to_string(),
                reason: format!(
                    "target format has no lossless mapping for unknown source output item type {raw_type:?}"
                ),
            });
        }
    }
    Ok(())
}

fn validate_known_standard_request_root_fields(
    source: FormatId,
    target: FormatId,
    body: &Value,
) -> Result<(), FormatError> {
    let Some(object) = body.as_object() else {
        return Ok(());
    };
    for key in object.keys() {
        if standard_request_root_field_is_audited(source, key) {
            continue;
        }
        return Err(FormatError::UnauditedField {
            source_format: source.as_str().to_string(),
            target_format: target.as_str().to_string(),
            field: key.clone(),
            reason: "source request root field is not in the audited provider schema for cross-format conversion".to_string(),
        });
    }
    Ok(())
}

fn standard_request_root_field_is_audited(source: FormatId, key: &str) -> bool {
    match source {
        FormatId::OpenAiChat => matches!(
            key,
            "audio"
                | "frequency_penalty"
                | "function_call"
                | "functions"
                | "logit_bias"
                | "logprobs"
                | "max_completion_tokens"
                | "max_tokens"
                | "messages"
                | "metadata"
                | "modalities"
                | "model"
                | "n"
                | "parallel_tool_calls"
                | "prediction"
                | "presence_penalty"
                | "prompt_cache_key"
                | "prompt_cache_options"
                | "prompt_cache_retention"
                | "reasoning_effort"
                | "response_format"
                | "safety_identifier"
                | "seed"
                | "service_tier"
                | "stop"
                | "store"
                | "stream"
                | "stream_options"
                | "temperature"
                | "tool_choice"
                | "tools"
                | "top_logprobs"
                | "top_p"
                | "user"
                | "verbosity"
                | "web_search_options"
        ),
        FormatId::OpenAiResponses | FormatId::OpenAiResponsesCompact => matches!(
            key,
            "background"
                | "client_metadata"
                | "context_management"
                | "conversation"
                | "include"
                | "input"
                | "instructions"
                | "max_output_tokens"
                | "max_tool_calls"
                | "metadata"
                | "model"
                | "multi_agent"
                | "parallel_tool_calls"
                | "previous_response_id"
                | "prompt"
                | "prompt_cache_key"
                | "prompt_cache_options"
                | "prompt_cache_retention"
                | "reasoning"
                | "safety_identifier"
                | "service_tier"
                | "store"
                | "stream"
                | "stream_options"
                | "temperature"
                | "text"
                | "tool_choice"
                | "tools"
                | "top_logprobs"
                | "top_p"
                | "truncation"
                | "user"
        ),
        FormatId::ClaudeMessages => matches!(
            key,
            "cache_control"
                | "container"
                | "context_management"
                | "inference_geo"
                | "max_tokens"
                | "messages"
                | "metadata"
                | "model"
                | "output_config"
                | "service_tier"
                | "stop_sequences"
                | "stream"
                | "system"
                | "temperature"
                | "thinking"
                | "tool_choice"
                | "tools"
                | "top_k"
                | "top_p"
        ),
        FormatId::GeminiGenerateContent => matches!(
            key,
            "cachedContent"
                | "cached_content"
                | "contents"
                | "generationConfig"
                | "generation_config"
                | "model"
                | "safetySettings"
                | "safety_settings"
                | "serviceTier"
                | "service_tier"
                | "store"
                | "systemInstruction"
                | "system_instruction"
                | "toolConfig"
                | "tool_config"
                | "tools"
        ),
        FormatId::GeminiInteractions => matches!(
            key,
            "agent"
                | "agent_config"
                | "background"
                | "generation_config"
                | "input"
                | "metadata"
                | "model"
                | "previous_interaction_id"
                | "response_format"
                | "safety_settings"
                | "store"
                | "stream"
                | "system_instruction"
                | "tool_config"
                | "tools"
        ),
        FormatId::OpenAiEmbedding
        | FormatId::OpenAiRealtime
        | FormatId::OpenAiSearch
        | FormatId::OpenAiRerank
        | FormatId::GeminiEmbedding
        | FormatId::JinaEmbedding
        | FormatId::JinaRerank
        | FormatId::DoubaoEmbedding
        | FormatId::AliyunMultimodalEmbedding
        | FormatId::CodexLive => true,
    }
}

fn validate_cross_format_generation_target(
    source: FormatId,
    target: FormatId,
    request: &CanonicalRequest,
) -> Result<(), FormatError> {
    let generation = &request.generation;
    match target {
        FormatId::OpenAiChat if generation.top_k.is_some() => {
            return lossy_generation_field(
                source,
                target,
                "top_k",
                "OpenAI Chat Completions has no official top_k request field",
            );
        }
        FormatId::OpenAiChat => {}
        FormatId::OpenAiResponses | FormatId::OpenAiResponsesCompact => {
            for (field, present, reason) in [
                (
                    "top_k",
                    generation.top_k.is_some(),
                    "OpenAI Responses has no top_k request field",
                ),
                (
                    "stop_sequences",
                    generation.stop_sequences.is_some(),
                    "OpenAI Responses has no stop sequence request field",
                ),
                (
                    "n",
                    generation.n.is_some(),
                    "OpenAI Responses has no multi-candidate n request field",
                ),
                (
                    "presence_penalty",
                    generation.presence_penalty.is_some(),
                    "OpenAI Responses has no presence_penalty request field",
                ),
                (
                    "frequency_penalty",
                    generation.frequency_penalty.is_some(),
                    "OpenAI Responses has no frequency_penalty request field",
                ),
                (
                    "seed",
                    generation.seed.is_some(),
                    "OpenAI Responses has no seed request field",
                ),
                (
                    "logprobs",
                    generation.logprobs.is_some(),
                    "OpenAI Responses has no logprobs boolean request field",
                ),
            ] {
                if present {
                    return lossy_generation_field(source, target, field, reason);
                }
            }
        }
        FormatId::ClaudeMessages => {
            for (field, present, reason) in [
                (
                    "n",
                    generation.n.is_some(),
                    "Claude Messages has no multi-candidate n request field",
                ),
                (
                    "presence_penalty",
                    generation.presence_penalty.is_some(),
                    "Claude Messages has no presence_penalty request field",
                ),
                (
                    "frequency_penalty",
                    generation.frequency_penalty.is_some(),
                    "Claude Messages has no frequency_penalty request field",
                ),
                (
                    "seed",
                    generation.seed.is_some(),
                    "Claude Messages has no seed request field",
                ),
                (
                    "logprobs",
                    generation.logprobs.is_some(),
                    "Claude Messages has no logprobs request field",
                ),
                (
                    "top_logprobs",
                    generation.top_logprobs.is_some(),
                    "Claude Messages has no top_logprobs request field",
                ),
            ] {
                if present {
                    return lossy_generation_field(source, target, field, reason);
                }
            }
        }
        FormatId::GeminiGenerateContent => {
            for (field, present, reason) in [
                (
                    "presence_penalty",
                    generation.presence_penalty.is_some(),
                    "Gemini GenerateContent has no presence_penalty request field",
                ),
                (
                    "frequency_penalty",
                    generation.frequency_penalty.is_some(),
                    "Gemini GenerateContent has no frequency_penalty request field",
                ),
                (
                    "logprobs",
                    generation.logprobs.is_some(),
                    "Gemini GenerateContent has no logprobs request field",
                ),
                (
                    "top_logprobs",
                    generation.top_logprobs.is_some(),
                    "Gemini GenerateContent has no top_logprobs request field",
                ),
            ] {
                if present {
                    return lossy_generation_field(source, target, field, reason);
                }
            }
        }
        _ => {}
    }
    Ok(())
}

fn lossy_generation_field(
    source: FormatId,
    target: FormatId,
    canonical_field: &str,
    reason: &str,
) -> Result<(), FormatError> {
    Err(FormatError::LossyConversionBlocked {
        source_format: source.as_str().to_string(),
        target_format: target.as_str().to_string(),
        field: source_generation_field_path(source, canonical_field),
        reason: reason.to_string(),
    })
}

fn source_generation_field_path(source: FormatId, canonical_field: &str) -> String {
    let field = match (source, canonical_field) {
        (FormatId::OpenAiChat, "max_tokens") => "max_tokens/max_completion_tokens",
        (FormatId::OpenAiChat, "stop_sequences") => "stop",
        (FormatId::OpenAiResponses | FormatId::OpenAiResponsesCompact, "max_tokens") => {
            "max_output_tokens"
        }
        (FormatId::ClaudeMessages, "stop_sequences") => "stop_sequences",
        (FormatId::GeminiGenerateContent, "max_tokens") => "generationConfig.maxOutputTokens",
        (FormatId::GeminiGenerateContent, "top_p") => "generationConfig.topP",
        (FormatId::GeminiGenerateContent, "top_k") => "generationConfig.topK",
        (FormatId::GeminiGenerateContent, "stop_sequences") => "generationConfig.stopSequences",
        (FormatId::GeminiGenerateContent, "n") => "generationConfig.candidateCount",
        (FormatId::GeminiGenerateContent, "seed") => "generationConfig.seed",
        (FormatId::GeminiGenerateContent, other) => return format!("generationConfig.{other}"),
        (_, other) => other,
    };
    field.to_string()
}

fn validate_cross_format_request_extensions(
    source: FormatId,
    target: FormatId,
    request: &CanonicalRequest,
) -> Result<(), FormatError> {
    validate_openai_responses_namespace_tools_to_chat(source, target, request)?;
    validate_request_content_has_no_unknown_blocks(source, target, request)?;
    let namespace_aliases = NamespaceToolAliases::from_canonical_tools(&request.tools);
    let request_extensions = mapped_namespace_request_extensions(source, target, request);
    validate_request_extension_namespace(source, target, "request", &request_extensions)?;
    for instruction in &request.instructions {
        validate_request_extension_namespace(
            source,
            target,
            "instructions[]",
            &instruction.extensions,
        )?;
    }
    for message in &request.messages {
        validate_request_extension_namespace(source, target, "messages[]", &message.extensions)?;
        for block in &message.content {
            match block {
                CanonicalContentBlock::Text { extensions, .. }
                | CanonicalContentBlock::Thinking { extensions, .. }
                | CanonicalContentBlock::Image { extensions, .. }
                | CanonicalContentBlock::File { extensions, .. }
                | CanonicalContentBlock::Audio { extensions, .. }
                | CanonicalContentBlock::ToolResult { extensions, .. }
                | CanonicalContentBlock::Unknown { extensions, .. } => {
                    validate_request_extension_namespace(
                        source,
                        target,
                        "messages[].content[]",
                        extensions,
                    )?;
                }
                CanonicalContentBlock::ToolUse {
                    name, extensions, ..
                } => {
                    let extensions = mapped_namespace_tool_use_extensions(
                        source,
                        target,
                        name,
                        extensions,
                        &namespace_aliases,
                    )?;
                    validate_request_extension_namespace(
                        source,
                        target,
                        "messages[].content[]",
                        &extensions,
                    )?;
                }
            }
        }
    }
    for (tool_index, tool) in request.tools.iter().enumerate() {
        if matches!(
            source,
            FormatId::OpenAiResponses | FormatId::OpenAiResponsesCompact
        ) && target == FormatId::OpenAiChat
            && namespace_aliases.is_representable_namespace_tool(tool_index)
        {
            // The namespace adapter has already audited the complete raw parent and every child.
            // Do not broadly allow these provider-extension keys for unrelated tool types.
            continue;
        }
        validate_request_extension_namespace(source, target, "tools[]", &tool.extensions)?;
    }
    if let Some(thinking) = &request.thinking {
        validate_request_extension_namespace(source, target, "thinking", &thinking.extensions)?;
    }
    if let Some(response_format) = &request.response_format {
        validate_request_extension_namespace(
            source,
            target,
            "response_format",
            &response_format.extensions,
        )?;
    }
    Ok(())
}

fn mapped_namespace_request_extensions(
    source: FormatId,
    target: FormatId,
    request: &CanonicalRequest,
) -> std::collections::BTreeMap<String, Value> {
    let mut mapped = request.extensions.clone();
    if !matches!(
        source,
        FormatId::OpenAiResponses | FormatId::OpenAiResponsesCompact
    ) || target != FormatId::OpenAiChat
    {
        return mapped;
    }

    let remove_tool_choice =
        openai_chat::request::raw_tool_choice_extension_is_representable_for_openai_chat(request);
    for provider_namespace in ["openai_responses", "openai_cli"] {
        let should_remove_namespace = mapped
            .get_mut(provider_namespace)
            .and_then(Value::as_object_mut)
            .is_some_and(|fields| {
                fields.remove("include");
                if remove_tool_choice {
                    fields.remove("tool_choice");
                }
                fields.is_empty()
            });
        if should_remove_namespace {
            mapped.remove(provider_namespace);
        }
    }
    mapped
}

fn mapped_namespace_tool_use_extensions(
    source: FormatId,
    target: FormatId,
    name: &str,
    extensions: &std::collections::BTreeMap<String, Value>,
    aliases: &NamespaceToolAliases,
) -> Result<std::collections::BTreeMap<String, Value>, FormatError> {
    if !matches!(
        source,
        FormatId::OpenAiResponses | FormatId::OpenAiResponsesCompact
    ) || !matches!(
        target,
        FormatId::OpenAiChat | FormatId::GeminiGenerateContent
    ) {
        return Ok(extensions.clone());
    }

    let mut mapped = extensions.clone();
    for provider_namespace in ["openai_responses", "openai_cli"] {
        let Some(provider_fields) = mapped
            .get_mut(provider_namespace)
            .and_then(Value::as_object_mut)
        else {
            continue;
        };
        if target == FormatId::OpenAiChat {
            if let Some(namespace) = provider_fields.get("namespace") {
                let Some(namespace) = namespace
                    .as_str()
                    .map(str::trim)
                    .filter(|namespace| !namespace.is_empty())
                else {
                    return Err(FormatError::LossyConversionBlocked {
                        source_format: source.as_str().to_string(),
                        target_format: target.as_str().to_string(),
                        field: format!("messages[].content[].{provider_namespace}.namespace"),
                        reason: "Responses namespace tool call has an invalid namespace identity"
                            .to_string(),
                    });
                };
                if aliases.chat_name(namespace, name).is_none() {
                    return Err(FormatError::LossyConversionBlocked {
                        source_format: source.as_str().to_string(),
                        target_format: target.as_str().to_string(),
                        field: format!("messages[].content[].{provider_namespace}.namespace"),
                        reason: "Responses namespace tool call does not match an expanded namespace child"
                            .to_string(),
                    });
                }
                provider_fields.remove("namespace");
            }
        }
        // Responses item IDs are distinct from executable call IDs, but Chat
        // and Gemini pair tools by the latter. A completed history item is
        // fully represented by the model function call, so these transport
        // sidecars can be discarded after any namespace identity is proven.
        provider_fields.remove("item_id");
        if provider_fields.get("status").and_then(Value::as_str) == Some("completed") {
            provider_fields.remove("status");
        }
    }
    mapped.retain(|_, value| value.as_object().is_none_or(|object| !object.is_empty()));
    Ok(mapped)
}

fn validate_request_content_has_no_unknown_blocks(
    source: FormatId,
    target: FormatId,
    request: &CanonicalRequest,
) -> Result<(), FormatError> {
    for message in &request.messages {
        for block in &message.content {
            if let CanonicalContentBlock::Unknown { raw_type, .. } = block {
                return Err(FormatError::LossyConversionBlocked {
                    source_format: source.as_str().to_string(),
                    target_format: target.as_str().to_string(),
                    field: "messages[].content[].type".to_string(),
                    reason: format!(
                        "target format has no lossless mapping for unknown source content block type {raw_type:?}"
                    ),
                });
            }
        }
    }
    Ok(())
}

fn validate_request_extension_namespace(
    source: FormatId,
    target: FormatId,
    location: &str,
    extensions: &std::collections::BTreeMap<String, Value>,
) -> Result<(), FormatError> {
    for (namespace, value) in extensions {
        if namespace == "aether" {
            continue;
        }
        let Some(object) = value.as_object() else {
            return Err(FormatError::UnsupportedField {
                format: source.as_str().to_string(),
                field: format!("{location}.{namespace}"),
                reason:
                    "provider extension namespace must be an object for cross-format conversion"
                        .to_string(),
            });
        };
        for key in object.keys() {
            if openai_responses_custom_tool_key_is_cross_format_safe(
                source, target, location, namespace, object, key,
            ) || request_extension_key_is_cross_format_safe(
                source, target, location, namespace, key,
            ) {
                continue;
            }
            return Err(FormatError::LossyConversionBlocked {
                source_format: source.as_str().to_string(),
                target_format: target.as_str().to_string(),
                field: extension_field_path(location, namespace, key),
                reason: "provider-specific extension field has no audited lossless target mapping"
                    .to_string(),
            });
        }
    }
    Ok(())
}

fn openai_responses_custom_tool_key_is_cross_format_safe(
    source: FormatId,
    target: FormatId,
    location: &str,
    namespace: &str,
    extension: &Map<String, Value>,
    key: &str,
) -> bool {
    matches!(
        (source, target, location, namespace),
        (
            FormatId::OpenAiResponses | FormatId::OpenAiResponsesCompact,
            FormatId::OpenAiChat,
            "tools[]",
            "openai_responses" | "openai_cli"
        )
    ) && extension
        .get("type")
        .and_then(Value::as_str)
        .is_some_and(|tool_type| tool_type.eq_ignore_ascii_case("custom"))
        && matches!(key, "type" | "name" | "description" | "format" | "custom")
}

fn request_extension_key_is_cross_format_safe(
    source: FormatId,
    target: FormatId,
    location: &str,
    namespace: &str,
    key: &str,
) -> bool {
    if matches!(
        source,
        FormatId::OpenAiResponses | FormatId::OpenAiResponsesCompact
    ) && matches!(namespace, "openai_responses" | "openai_cli")
        && key == "client_metadata"
    {
        return true;
    }
    if location == "messages[].content[]"
        && key == "prompt_cache_breakpoint"
        && matches!(
            (source, target, namespace),
            (
                FormatId::OpenAiChat,
                FormatId::OpenAiResponses | FormatId::OpenAiResponsesCompact,
                "openai",
            ) | (
                FormatId::OpenAiResponses | FormatId::OpenAiResponsesCompact,
                FormatId::OpenAiChat,
                "openai_responses" | "openai_cli",
            )
        )
    {
        return true;
    }
    if location == "messages[].content[]"
        && matches!(
            (source, target, namespace, key),
            (
                FormatId::OpenAiResponses | FormatId::OpenAiResponsesCompact,
                FormatId::GeminiGenerateContent,
                "gemini",
                "thoughtSignature",
            )
        )
    {
        return true;
    }
    if location == "tools[]" {
        return tool_extension_key_is_cross_format_safe(source, target, namespace, key);
    }
    if location == "thinking" {
        return thinking_extension_key_is_cross_format_safe(source, target, namespace, key);
    }
    if location == "response_format" {
        return response_format_extension_key_is_cross_format_safe(namespace, key);
    }
    matches!(
        (source, target, namespace, key),
        (
            FormatId::OpenAiChat,
            FormatId::OpenAiResponses | FormatId::OpenAiResponsesCompact,
            "openai",
            "stream"
                | "store"
                | "service_tier"
                | "safety_identifier"
                | "prompt_cache_key"
                | "prompt_cache_options"
                | "prompt_cache_retention"
                | "user"
                | "verbosity",
        ) | (
            FormatId::OpenAiChat,
            FormatId::OpenAiResponses | FormatId::OpenAiResponsesCompact,
            "openai_responses",
            "verbosity",
        ) | (
            FormatId::OpenAiChat,
            FormatId::GeminiGenerateContent,
            "openai",
            "web_search_options",
        ) | (
            FormatId::OpenAiResponses | FormatId::OpenAiResponsesCompact,
            FormatId::OpenAiChat,
            "openai_responses" | "openai_cli",
            "stream"
                | "store"
                | "service_tier"
                | "safety_identifier"
                | "prompt_cache_key"
                | "prompt_cache_options"
                | "prompt_cache_retention"
                | "user"
                | "verbosity",
        ) | (
            FormatId::ClaudeMessages,
            _,
            "claude",
            "context_management" | "output_config",
        ) | (
            FormatId::ClaudeMessages,
            FormatId::OpenAiChat | FormatId::GeminiGenerateContent,
            "openai",
            "web_search_options",
        ) | (
            FormatId::GeminiGenerateContent,
            _,
            "gemini",
            "thinking_config" | "raw_tools" | "raw_tool_config",
        ) | (
            FormatId::GeminiGenerateContent,
            FormatId::OpenAiChat,
            "gemini",
            "builtin_tools" | "grounding",
        ) | (
            FormatId::GeminiGenerateContent,
            FormatId::GeminiGenerateContent,
            "gemini",
            _
        ) | (
            FormatId::GeminiGenerateContent,
            FormatId::OpenAiChat,
            "openai",
            "web_search_options",
        )
    )
}

fn tool_extension_key_is_cross_format_safe(
    source: FormatId,
    target: FormatId,
    namespace: &str,
    key: &str,
) -> bool {
    matches!(
        (source, target, namespace, key),
        (_, _, "claude", "raw_input_schema")
            | (_, _, "gemini", "raw_parameters")
            | (
                FormatId::OpenAiResponses | FormatId::OpenAiResponsesCompact,
                FormatId::GeminiGenerateContent,
                "openai_responses" | "openai_cli",
                "type",
            )
    )
}

fn thinking_extension_key_is_cross_format_safe(
    source: FormatId,
    target: FormatId,
    namespace: &str,
    key: &str,
) -> bool {
    matches!(
        (source, target, namespace, key),
        (
            FormatId::OpenAiChat,
            FormatId::OpenAiResponses
                | FormatId::OpenAiResponsesCompact
                | FormatId::ClaudeMessages
                | FormatId::GeminiGenerateContent,
            "openai",
            "reasoning_effort",
        ) | (
            FormatId::OpenAiResponses | FormatId::OpenAiResponsesCompact,
            FormatId::OpenAiChat | FormatId::ClaudeMessages | FormatId::GeminiGenerateContent,
            "openai_responses" | "openai_cli",
            "effort",
        ) | (
            FormatId::OpenAiResponses | FormatId::OpenAiResponsesCompact,
            FormatId::OpenAiChat,
            "openai_responses" | "openai_cli",
            "summary",
        ) | (
            FormatId::ClaudeMessages,
            _,
            "claude",
            "type" | "budget_tokens" | "output_config",
        ) | (
            FormatId::ClaudeMessages,
            FormatId::OpenAiChat | FormatId::OpenAiResponses | FormatId::OpenAiResponsesCompact,
            "openai",
            "reasoning_effort",
        ) | (
            FormatId::GeminiGenerateContent,
            _,
            "gemini",
            "thinking_config" | "includeThoughts" | "thinkingBudget" | "thinkingLevel",
        ) | (
            FormatId::GeminiGenerateContent,
            FormatId::OpenAiChat | FormatId::OpenAiResponses | FormatId::OpenAiResponsesCompact,
            "openai",
            "reasoning_effort",
        )
    )
}

fn response_format_extension_key_is_cross_format_safe(namespace: &str, key: &str) -> bool {
    matches!((namespace, key), ("openai", _) | ("gemini", "raw_schema"))
}

fn extension_field_path(location: &str, namespace: &str, key: &str) -> String {
    if location == "request" {
        format!("{namespace}.{key}")
    } else {
        format!("{location}.{namespace}.{key}")
    }
}

fn validate_source_response_stop_enums(
    source: FormatId,
    target: FormatId,
    body: &Value,
) -> Result<(), FormatError> {
    match source {
        FormatId::OpenAiChat => validate_openai_chat_response_finish_reasons(body),
        FormatId::OpenAiResponses | FormatId::OpenAiResponsesCompact => {
            validate_openai_responses_response_status(body, target)
        }
        FormatId::ClaudeMessages => validate_claude_response_stop_reason(body),
        FormatId::GeminiGenerateContent => validate_gemini_response_finish_reasons(body, target),
        FormatId::GeminiInteractions => Ok(()),
        FormatId::OpenAiEmbedding
        | FormatId::OpenAiRealtime
        | FormatId::OpenAiSearch
        | FormatId::OpenAiRerank
        | FormatId::GeminiEmbedding
        | FormatId::JinaEmbedding
        | FormatId::JinaRerank
        | FormatId::DoubaoEmbedding
        | FormatId::AliyunMultimodalEmbedding
        | FormatId::CodexLive => Ok(()),
    }
}

fn validate_openai_chat_response_finish_reasons(body: &Value) -> Result<(), FormatError> {
    let Some(choices) = body.get("choices").and_then(Value::as_array) else {
        return Ok(());
    };
    for choice in choices {
        let Some(value) = choice.get("finish_reason") else {
            continue;
        };
        validate_nullable_string_field(
            value,
            FormatId::OpenAiChat.as_str(),
            "choices[].finish_reason",
        )?;
        let Some(raw) = value
            .as_str()
            .map(str::trim)
            .filter(|value| !value.is_empty())
        else {
            continue;
        };
        if !matches!(
            raw,
            "stop" | "length" | "tool_calls" | "function_call" | "content_filter"
        ) {
            return Err(FormatError::InvalidEnumValue {
                format: FormatId::OpenAiChat.as_str().to_string(),
                field: "choices[].finish_reason".to_string(),
                value: raw.to_string(),
            });
        }
    }
    Ok(())
}

fn validate_openai_responses_response_status(
    body: &Value,
    target: FormatId,
) -> Result<(), FormatError> {
    let Some(value) = body.get("status") else {
        return Ok(());
    };
    validate_nullable_string_field(value, FormatId::OpenAiResponses.as_str(), "status")?;
    let Some(raw) = value
        .as_str()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    else {
        return Ok(());
    };
    match raw {
        "completed" | "incomplete" => Ok(()),
        "queued" | "in_progress" | "cancelled" => Err(FormatError::LossyConversionBlocked {
            source_format: FormatId::OpenAiResponses.as_str().to_string(),
            target_format: target.as_str().to_string(),
            field: "status".to_string(),
            reason: "target sync response format cannot represent non-terminal Responses status"
                .to_string(),
        }),
        "failed" => Err(FormatError::LossyConversionBlocked {
            source_format: FormatId::OpenAiResponses.as_str().to_string(),
            target_format: target.as_str().to_string(),
            field: "status".to_string(),
            reason:
                "failed Responses objects must be handled as provider errors, not success responses"
                    .to_string(),
        }),
        _ => Err(FormatError::InvalidEnumValue {
            format: FormatId::OpenAiResponses.as_str().to_string(),
            field: "status".to_string(),
            value: raw.to_string(),
        }),
    }
}

fn validate_claude_response_stop_reason(body: &Value) -> Result<(), FormatError> {
    let Some(value) = body.get("stop_reason") else {
        return Ok(());
    };
    validate_nullable_string_field(value, FormatId::ClaudeMessages.as_str(), "stop_reason")?;
    let Some(raw) = value
        .as_str()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    else {
        return Ok(());
    };
    if !matches!(
        raw,
        "end_turn"
            | "max_tokens"
            | "stop_sequence"
            | "tool_use"
            | "pause_turn"
            | "refusal"
            | "content_filtered"
    ) {
        return Err(FormatError::InvalidEnumValue {
            format: FormatId::ClaudeMessages.as_str().to_string(),
            field: "stop_reason".to_string(),
            value: raw.to_string(),
        });
    }
    Ok(())
}

fn validate_gemini_response_finish_reasons(
    body: &Value,
    target: FormatId,
) -> Result<(), FormatError> {
    let Some(candidates) = body.get("candidates").and_then(Value::as_array) else {
        return Ok(());
    };
    for candidate in candidates {
        let Some(value) = candidate
            .get("finishReason")
            .or_else(|| candidate.get("finish_reason"))
        else {
            continue;
        };
        validate_nullable_string_field(
            value,
            FormatId::GeminiGenerateContent.as_str(),
            "candidates[].finishReason",
        )?;
        let Some(raw) = value
            .as_str()
            .map(str::trim)
            .filter(|value| !value.is_empty())
        else {
            continue;
        };
        let normalized = raw.to_ascii_uppercase();
        if gemini_finish_reason_is_cross_format_mappable(normalized.as_str()) {
            continue;
        }
        if gemini_finish_reason_is_known(normalized.as_str()) {
            return Err(FormatError::LossyConversionBlocked {
                source_format: FormatId::GeminiGenerateContent.as_str().to_string(),
                target_format: target.as_str().to_string(),
                field: "candidates[].finishReason".to_string(),
                reason: format!("Gemini finishReason {raw:?} has no lossless target finish reason"),
            });
        }
        return Err(FormatError::InvalidEnumValue {
            format: FormatId::GeminiGenerateContent.as_str().to_string(),
            field: "candidates[].finishReason".to_string(),
            value: raw.to_string(),
        });
    }
    Ok(())
}

fn validate_nullable_string_field(
    value: &Value,
    format: &str,
    field: &str,
) -> Result<(), FormatError> {
    if value.is_null() || value.is_string() {
        Ok(())
    } else {
        Err(FormatError::InvalidTargetField {
            format: format.to_string(),
            field: field.to_string(),
            reason: "enum field must be a string or null".to_string(),
        })
    }
}

fn gemini_finish_reason_is_cross_format_mappable(value: &str) -> bool {
    matches!(
        value,
        "STOP"
            | "MAX_TOKENS"
            | "SAFETY"
            | "RECITATION"
            | "LANGUAGE"
            | "BLOCKLIST"
            | "PROHIBITED_CONTENT"
            | "SPII"
            | "IMAGE_SAFETY"
            | "IMAGE_PROHIBITED_CONTENT"
            | "IMAGE_RECITATION"
    )
}

fn gemini_finish_reason_is_known(value: &str) -> bool {
    gemini_finish_reason_is_cross_format_mappable(value)
        || matches!(
            value,
            "FINISH_REASON_UNSPECIFIED"
                | "OTHER"
                | "MALFORMED_FUNCTION_CALL"
                | "IMAGE_OTHER"
                | "NO_IMAGE"
                | "UNEXPECTED_TOOL_CALL"
                | "TOO_MANY_TOOL_CALLS"
                | "MISSING_THOUGHT_SIGNATURE"
                | "MALFORMED_RESPONSE"
                | "ESCALATION"
        )
}

fn validate_canonical_response_stop_reasons(
    source: FormatId,
    target: FormatId,
    response: &CanonicalResponse,
) -> Result<(), FormatError> {
    if response.stop_reason == Some(CanonicalStopReason::Unknown)
        || response
            .outputs
            .iter()
            .any(|output| output.stop_reason == Some(CanonicalStopReason::Unknown))
    {
        return Err(FormatError::LossyConversionBlocked {
            source_format: source.as_str().to_string(),
            target_format: target.as_str().to_string(),
            field: "stop_reason".to_string(),
            reason: "source response stop reason cannot be represented losslessly in target format"
                .to_string(),
        });
    }
    Ok(())
}

fn is_embedding_format(format: FormatId) -> bool {
    matches!(
        format,
        FormatId::OpenAiEmbedding
            | FormatId::GeminiEmbedding
            | FormatId::JinaEmbedding
            | FormatId::DoubaoEmbedding
            | FormatId::AliyunMultimodalEmbedding
    )
}

fn is_rerank_format(format: FormatId) -> bool {
    matches!(format, FormatId::OpenAiRerank | FormatId::JinaRerank)
}

fn validate_embedding_request_conversion(
    source: FormatId,
    target: FormatId,
    request: &CanonicalRequest,
) -> Result<(), FormatError> {
    if !is_embedding_format(source) || !is_embedding_format(target) {
        return Err(FormatError::UnsupportedField {
            format: target.as_str().to_string(),
            field: "embedding".to_string(),
            reason: "embedding requests can only convert to embedding target formats".to_string(),
        });
    }
    let Some(embedding) = request.embedding.as_ref() else {
        return Err(FormatError::RequestParseFailed {
            format: source.as_str().to_string(),
        });
    };
    if embedding.input.is_empty() {
        return Err(FormatError::InvalidTargetField {
            format: source.as_str().to_string(),
            field: "input".to_string(),
            reason: "embedding input must not be empty".to_string(),
        });
    }
    validate_no_cross_format_embedding_extensions(source, target, embedding)?;
    match target {
        FormatId::OpenAiEmbedding => validate_openai_embedding_target(source, target, embedding),
        FormatId::JinaEmbedding => validate_text_embedding_target(source, target, embedding, true),
        FormatId::GeminiEmbedding => validate_gemini_embedding_target(source, target, embedding),
        FormatId::DoubaoEmbedding => validate_doubao_embedding_target(source, target, embedding),
        FormatId::AliyunMultimodalEmbedding => {
            validate_aliyun_embedding_target(source, target, embedding)
        }
        _ => Ok(()),
    }
}

fn validate_no_cross_format_embedding_extensions(
    source: FormatId,
    target: FormatId,
    embedding: &crate::protocol::canonical::CanonicalEmbeddingRequest,
) -> Result<(), FormatError> {
    if embedding.extensions.is_empty() {
        return Ok(());
    }
    let target_namespace = embedding_namespace(target);
    if embedding
        .extensions
        .keys()
        .all(|key| key == target_namespace)
    {
        return Ok(());
    }
    Err(FormatError::UnsupportedField {
        format: target.as_str().to_string(),
        field: "embedding.extensions".to_string(),
        reason: format!(
            "{} provider-specific embedding fields cannot be losslessly emitted as {}",
            source.as_str(),
            target.as_str()
        ),
    })
}

fn embedding_namespace(format: FormatId) -> &'static str {
    match format {
        FormatId::OpenAiEmbedding => "openai",
        FormatId::GeminiEmbedding => "gemini",
        FormatId::JinaEmbedding => "jina",
        FormatId::DoubaoEmbedding => "doubao",
        FormatId::AliyunMultimodalEmbedding => "aliyun",
        _ => "",
    }
}

fn validate_openai_embedding_target(
    source: FormatId,
    target: FormatId,
    embedding: &crate::protocol::canonical::CanonicalEmbeddingRequest,
) -> Result<(), FormatError> {
    if matches!(embedding.input, CanonicalEmbeddingInput::Multimodal(_)) {
        return lossy_embedding_field(
            source,
            target,
            "input",
            "OpenAI embeddings do not support multimodal embedding input",
        );
    }
    if embedding.task.is_some() {
        return lossy_embedding_field(
            source,
            target,
            "task",
            "OpenAI embeddings have no task field",
        );
    }
    if embedding.parameters.is_some() {
        return lossy_embedding_field(
            source,
            target,
            "parameters",
            "OpenAI embeddings have no generic parameters field",
        );
    }
    Ok(())
}

fn validate_text_embedding_target(
    source: FormatId,
    target: FormatId,
    embedding: &crate::protocol::canonical::CanonicalEmbeddingRequest,
    allow_task: bool,
) -> Result<(), FormatError> {
    if !matches!(
        embedding.input,
        CanonicalEmbeddingInput::String(_) | CanonicalEmbeddingInput::StringArray(_)
    ) {
        return lossy_embedding_field(
            source,
            target,
            "input",
            "target embedding format only supports text string inputs",
        );
    }
    if embedding.encoding_format.is_some() {
        return lossy_embedding_field(
            source,
            target,
            "encoding_format",
            "target embedding format has no encoding_format field",
        );
    }
    if embedding.user.is_some() {
        return lossy_embedding_field(
            source,
            target,
            "user",
            "target embedding format has no user field",
        );
    }
    if !allow_task && embedding.task.is_some() {
        return lossy_embedding_field(
            source,
            target,
            "task",
            "target embedding format has no task field",
        );
    }
    Ok(())
}

fn validate_gemini_embedding_target(
    source: FormatId,
    target: FormatId,
    embedding: &crate::protocol::canonical::CanonicalEmbeddingRequest,
) -> Result<(), FormatError> {
    validate_text_embedding_target(source, target, embedding, true)?;
    if embedding.parameters.is_some() {
        return lossy_embedding_field(
            source,
            target,
            "parameters",
            "Gemini embeddings require named embedContent config fields, not generic parameters",
        );
    }
    if let Some(task) = embedding.task.as_deref() {
        validate_gemini_embedding_task_type(task)?;
    }
    Ok(())
}

fn validate_gemini_embedding_task_type(task: &str) -> Result<(), FormatError> {
    let normalized = task.trim().replace(['-', ' '], "_").to_ascii_uppercase();
    if matches!(
        normalized.as_str(),
        "QUERY"
            | "DOCUMENT"
            | "TASK_TYPE_UNSPECIFIED"
            | "RETRIEVAL_QUERY"
            | "RETRIEVAL_DOCUMENT"
            | "TEXT_MATCHING"
            | "SEMANTIC_SIMILARITY"
            | "CLASSIFICATION"
            | "CLUSTERING"
            | "QUESTION_ANSWERING"
            | "FACT_VERIFICATION"
            | "CODE_RETRIEVAL_QUERY"
    ) {
        Ok(())
    } else {
        Err(FormatError::InvalidEnumValue {
            format: FormatId::GeminiEmbedding.as_str().to_string(),
            field: "taskType".to_string(),
            value: task.to_string(),
        })
    }
}

fn validate_doubao_embedding_target(
    source: FormatId,
    target: FormatId,
    embedding: &crate::protocol::canonical::CanonicalEmbeddingRequest,
) -> Result<(), FormatError> {
    validate_text_embedding_target(source, target, embedding, false)?;
    if embedding.parameters.is_some() {
        return lossy_embedding_field(
            source,
            target,
            "parameters",
            "Doubao embeddings have no generic parameters field",
        );
    }
    Ok(())
}

fn validate_aliyun_embedding_target(
    source: FormatId,
    target: FormatId,
    embedding: &crate::protocol::canonical::CanonicalEmbeddingRequest,
) -> Result<(), FormatError> {
    if matches!(
        embedding.input,
        CanonicalEmbeddingInput::TokenArray(_) | CanonicalEmbeddingInput::TokenArrayArray(_)
    ) {
        return lossy_embedding_field(
            source,
            target,
            "input",
            "Aliyun multimodal embeddings do not support token-array input",
        );
    }
    if embedding.encoding_format.is_some() {
        return lossy_embedding_field(
            source,
            target,
            "encoding_format",
            "Aliyun multimodal embeddings have no encoding_format field",
        );
    }
    if embedding.user.is_some() {
        return lossy_embedding_field(
            source,
            target,
            "user",
            "Aliyun multimodal embeddings have no user field",
        );
    }
    if embedding.task.is_some() {
        return lossy_embedding_field(
            source,
            target,
            "task",
            "Aliyun multimodal embeddings have no task field",
        );
    }
    Ok(())
}

fn lossy_embedding_field(
    source: FormatId,
    target: FormatId,
    field: &str,
    reason: &str,
) -> Result<(), FormatError> {
    Err(FormatError::LossyConversionBlocked {
        source_format: source.as_str().to_string(),
        target_format: target.as_str().to_string(),
        field: field.to_string(),
        reason: reason.to_string(),
    })
}

fn validate_rerank_request_conversion(
    source: FormatId,
    target: FormatId,
    request: &CanonicalRequest,
) -> Result<(), FormatError> {
    if !is_rerank_format(source) || !is_rerank_format(target) {
        return Err(FormatError::UnsupportedField {
            format: target.as_str().to_string(),
            field: "rerank".to_string(),
            reason: "rerank requests can only convert to rerank target formats".to_string(),
        });
    }
    let Some(rerank) = request.rerank.as_ref() else {
        return Err(FormatError::RequestParseFailed {
            format: source.as_str().to_string(),
        });
    };
    if rerank.is_empty() {
        return Err(FormatError::InvalidTargetField {
            format: source.as_str().to_string(),
            field: "query/documents".to_string(),
            reason: "rerank query and documents must not be empty".to_string(),
        });
    }
    if rerank.top_n == Some(0) {
        return Err(FormatError::InvalidTargetField {
            format: source.as_str().to_string(),
            field: "top_n".to_string(),
            reason: "rerank top_n must be greater than zero".to_string(),
        });
    }
    let target_namespace = rerank_namespace(target);
    if !rerank.extensions.is_empty() && !rerank.extensions.keys().all(|key| key == target_namespace)
    {
        return Err(FormatError::UnsupportedField {
            format: target.as_str().to_string(),
            field: "rerank.extensions".to_string(),
            reason: format!(
                "{} provider-specific rerank fields cannot be losslessly emitted as {}",
                source.as_str(),
                target.as_str()
            ),
        });
    }
    Ok(())
}

fn rerank_namespace(format: FormatId) -> &'static str {
    match format {
        FormatId::OpenAiRerank => "openai",
        FormatId::JinaRerank => "jina",
        _ => "",
    }
}

fn validate_openai_reasoning_effort(
    source: FormatId,
    target: FormatId,
    body: &Value,
    mapped_model: Option<&str>,
) -> Result<(), FormatError> {
    let same_openai_format = source == target
        && matches!(
            source,
            FormatId::OpenAiChat | FormatId::OpenAiResponses | FormatId::OpenAiResponsesCompact
        );
    let openai_cross_format = matches!(
        (source, target),
        (
            FormatId::OpenAiChat,
            FormatId::OpenAiResponses | FormatId::OpenAiResponsesCompact
        ) | (
            FormatId::OpenAiResponses | FormatId::OpenAiResponsesCompact,
            FormatId::OpenAiChat
        )
    );
    if !same_openai_format && !openai_cross_format {
        return Ok(());
    }
    crate::validate_openai_reasoning_request(
        source.as_str(),
        target.as_str(),
        mapped_model.unwrap_or_default(),
        body,
    )
    .map_err(|violation| match violation.kind {
        crate::OpenAiReasoningViolationKind::InvalidEnum => FormatError::InvalidEnumValue {
            format: source.as_str().to_string(),
            field: violation.field,
            value: violation.value.unwrap_or_default(),
        },
        crate::OpenAiReasoningViolationKind::InvalidType
        | crate::OpenAiReasoningViolationKind::UnsupportedForModel => {
            FormatError::InvalidTargetField {
                format: source.as_str().to_string(),
                field: violation.field,
                reason: violation.reason,
            }
        }
    })
}

fn validate_openai_prompt_cache_contract(
    source: FormatId,
    body: &Value,
    mapped_model: Option<&str>,
) -> Result<(), FormatError> {
    if !matches!(
        source,
        FormatId::OpenAiChat | FormatId::OpenAiResponses | FormatId::OpenAiResponsesCompact
    ) {
        return Ok(());
    }
    let provider_model = mapped_model
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .or_else(|| body.get("model").and_then(Value::as_str))
        .unwrap_or_default();
    crate::validate_openai_prompt_cache_request(source.as_str(), provider_model, body).map_err(
        |violation| match violation.kind {
            crate::OpenAiPromptCacheViolationKind::InvalidEnum => FormatError::InvalidEnumValue {
                format: source.as_str().to_string(),
                field: violation.field,
                value: violation.value.unwrap_or_default(),
            },
            crate::OpenAiPromptCacheViolationKind::InvalidType
            | crate::OpenAiPromptCacheViolationKind::UnsupportedForModel
            | crate::OpenAiPromptCacheViolationKind::UnsupportedContentBlock => {
                FormatError::InvalidTargetField {
                    format: source.as_str().to_string(),
                    field: violation.field,
                    reason: violation.reason,
                }
            }
        },
    )
}

fn validate_openai_cross_format_store(
    source: FormatId,
    target: FormatId,
    body: &Value,
) -> Result<(), FormatError> {
    if !matches!(
        (source, target),
        (
            FormatId::OpenAiChat,
            FormatId::OpenAiResponses | FormatId::OpenAiResponsesCompact
        ) | (
            FormatId::OpenAiResponses | FormatId::OpenAiResponsesCompact,
            FormatId::OpenAiChat
        )
    ) {
        return Ok(());
    }
    let Some(value) = body.get("store") else {
        return Ok(());
    };
    if target == FormatId::OpenAiResponsesCompact {
        return if value.is_boolean() {
            Ok(())
        } else {
            Err(FormatError::InvalidTargetField {
                format: source.as_str().to_string(),
                field: "store".to_string(),
                reason: "store must be a boolean".to_string(),
            })
        };
    }
    match value.as_bool() {
        Some(false) => Ok(()),
        Some(true) => Err(FormatError::LossyConversionBlocked {
            source_format: source.as_str().to_string(),
            target_format: target.as_str().to_string(),
            field: "store".to_string(),
            reason: "Chat and Responses assign different persistence behavior to store=true"
                .to_string(),
        }),
        None => Err(FormatError::InvalidTargetField {
            format: source.as_str().to_string(),
            field: "store".to_string(),
            reason: "store must be a boolean".to_string(),
        }),
    }
}

fn validate_openai_chat_to_responses(body: &Value) -> Result<(), FormatError> {
    let Some(object) = body.as_object() else {
        return Ok(());
    };
    for field in [
        "n",
        "stop",
        "presence_penalty",
        "frequency_penalty",
        "seed",
        "logprobs",
        "stream_options",
    ] {
        if object.contains_key(field) {
            return Err(FormatError::LossyConversionBlocked {
                source_format: FormatId::OpenAiChat.as_str().to_string(),
                target_format: FormatId::OpenAiResponses.as_str().to_string(),
                field: field.to_string(),
                reason: "OpenAI Responses request has no canonical equivalent for this Chat field"
                    .to_string(),
            });
        }
    }
    if openai_chat_request_has_unsupported_responses_cache_breakpoint(object) {
        return Err(FormatError::LossyConversionBlocked {
            source_format: FormatId::OpenAiChat.as_str().to_string(),
            target_format: FormatId::OpenAiResponses.as_str().to_string(),
            field: "messages[].content[].prompt_cache_breakpoint".to_string(),
            reason: "OpenAI Responses supports prompt cache breakpoints only on input_text, input_image, and input_file blocks".to_string(),
        });
    }
    Ok(())
}

fn openai_chat_request_has_unsupported_responses_cache_breakpoint(
    request: &Map<String, Value>,
) -> bool {
    request
        .get("messages")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_object)
        .any(|message| {
            let role = message
                .get("role")
                .and_then(Value::as_str)
                .map(str::trim)
                .map(str::to_ascii_lowercase);
            message
                .get("content")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(Value::as_object)
                .any(|part| {
                    part.contains_key("prompt_cache_breakpoint")
                        && !openai_chat_part_maps_to_cacheable_responses_input(
                            role.as_deref(),
                            part,
                        )
                })
        })
}

fn openai_chat_part_maps_to_cacheable_responses_input(
    role: Option<&str>,
    part: &Map<String, Value>,
) -> bool {
    if !matches!(role, Some("user" | "system" | "developer")) {
        return false;
    }
    matches!(
        part.get("type")
            .and_then(Value::as_str)
            .unwrap_or("text")
            .trim()
            .to_ascii_lowercase()
            .as_str(),
        "text" | "input_text" | "image_url" | "input_image" | "file" | "input_file"
    )
}

fn validate_openai_responses_to_chat(
    body: &Value,
    request: &CanonicalRequest,
) -> Result<(), FormatError> {
    validate_openai_responses_namespace_tools_to_chat(
        FormatId::OpenAiResponses,
        FormatId::OpenAiChat,
        request,
    )?;
    let Some(object) = body.as_object() else {
        return Ok(());
    };
    for field in [
        "previous_response_id",
        "truncation",
        "prompt",
        "conversation",
        "background",
        "max_tool_calls",
        "multi_agent",
    ] {
        if object.contains_key(field) {
            return Err(FormatError::LossyConversionBlocked {
                source_format: FormatId::OpenAiResponses.as_str().to_string(),
                target_format: FormatId::OpenAiChat.as_str().to_string(),
                field: field.to_string(),
                reason: "OpenAI Chat request has no canonical equivalent for this Responses field"
                    .to_string(),
            });
        }
    }
    if object
        .get("reasoning")
        .and_then(Value::as_object)
        .is_some_and(|reasoning| reasoning.contains_key("budget_tokens"))
    {
        return Err(FormatError::LossyConversionBlocked {
            source_format: FormatId::OpenAiResponses.as_str().to_string(),
            target_format: FormatId::OpenAiChat.as_str().to_string(),
            field: "reasoning.budget_tokens".to_string(),
            reason: "OpenAI Chat reasoning_effort cannot carry this Responses reasoning field"
                .to_string(),
        });
    }
    if let Some(tools) = object.get("tools").and_then(Value::as_array) {
        for tool in tools {
            let tool_type = tool
                .get("type")
                .and_then(Value::as_str)
                .unwrap_or("function")
                .trim()
                .to_ascii_lowercase();
            if !matches!(tool_type.as_str(), "function" | "custom" | "namespace") {
                return Err(FormatError::LossyConversionBlocked {
                    source_format: FormatId::OpenAiResponses.as_str().to_string(),
                    target_format: FormatId::OpenAiChat.as_str().to_string(),
                    field: "tools".to_string(),
                    reason: format!("OpenAI Chat only supports function tools, got {tool_type}"),
                });
            }
        }
    }
    if request.tools.iter().any(|tool| tool.name.trim().is_empty()) {
        return Err(FormatError::InvalidTargetField {
            format: FormatId::OpenAiChat.as_str().to_string(),
            field: "tools[].function.name".to_string(),
            reason: "OpenAI Chat function tools require a non-empty name".to_string(),
        });
    }
    Ok(())
}

fn validate_openai_responses_namespace_tools_to_chat(
    source: FormatId,
    target: FormatId,
    request: &CanonicalRequest,
) -> Result<(), FormatError> {
    if !matches!(
        source,
        FormatId::OpenAiResponses | FormatId::OpenAiResponsesCompact
    ) || target != FormatId::OpenAiChat
    {
        return Ok(());
    }

    let aliases = NamespaceToolAliases::from_canonical_tools(&request.tools);
    for (tool_index, _) in request.tools.iter().enumerate() {
        if aliases.is_namespace_tool(tool_index)
            && !aliases.is_representable_namespace_tool(tool_index)
        {
            return Err(FormatError::LossyConversionBlocked {
                source_format: source.as_str().to_string(),
                target_format: target.as_str().to_string(),
                field: format!("tools[{tool_index}]"),
                reason: "Responses namespace tool cannot be losslessly expanded into OpenAI Chat function tools"
                    .to_string(),
            });
        }
    }
    Ok(())
}

fn validate_claude_cross_format_request(body: &Value, target: FormatId) -> Result<(), FormatError> {
    if claude_request_contains_provider_cache_control(body) {
        return Err(FormatError::LossyConversionBlocked {
            source_format: FormatId::ClaudeMessages.as_str().to_string(),
            target_format: target.as_str().to_string(),
            field: "cache_control".to_string(),
            reason: "target format has no lossless equivalent for Claude cache_control".to_string(),
        });
    }

    if let Some(output_effort) = body
        .as_object()
        .and_then(|object| object.get("output_config"))
        .and_then(Value::as_object)
        .and_then(|output_config| output_config.get("effort"))
    {
        validate_claude_output_effort_value(output_effort)?;
    }

    match target {
        FormatId::OpenAiChat if claude_request_contains_tool_result_content_array(body) => {
            return Err(FormatError::LossyConversionBlocked {
                source_format: FormatId::ClaudeMessages.as_str().to_string(),
                target_format: target.as_str().to_string(),
                field: "messages[].content[].tool_result.content".to_string(),
                reason: "OpenAI Chat tool messages cannot losslessly preserve Claude multi-block tool_result content".to_string(),
            });
        }
        FormatId::OpenAiChat => {}
        FormatId::OpenAiResponses | FormatId::OpenAiResponsesCompact => {
            if claude_request_contains_message_thinking_blocks(body) {
                return Err(FormatError::LossyConversionBlocked {
                    source_format: FormatId::ClaudeMessages.as_str().to_string(),
                    target_format: target.as_str().to_string(),
                    field: "messages[].content[].type".to_string(),
                    reason: "OpenAI Responses request input cannot losslessly preserve Claude thinking blocks".to_string(),
                });
            }
            if claude_request_contains_tool_result_content_array(body) {
                return Err(FormatError::LossyConversionBlocked {
                    source_format: FormatId::ClaudeMessages.as_str().to_string(),
                    target_format: target.as_str().to_string(),
                    field: "messages[].content[].tool_result.content".to_string(),
                    reason: "OpenAI Responses function_call_output cannot losslessly preserve Claude multi-block tool_result content".to_string(),
                });
            }
        }
        FormatId::GeminiGenerateContent
            if claude_request_contains_redacted_thinking_blocks(body) =>
        {
            return Err(FormatError::LossyConversionBlocked {
                source_format: FormatId::ClaudeMessages.as_str().to_string(),
                target_format: target.as_str().to_string(),
                field: "messages[].content[].type".to_string(),
                reason: "Gemini request parts cannot losslessly preserve Claude redacted thinking blocks".to_string(),
            });
        }
        FormatId::GeminiGenerateContent => {}
        _ => {}
    }
    Ok(())
}

fn validate_claude_output_effort_value(value: &Value) -> Result<(), FormatError> {
    let Some(raw) = value.as_str() else {
        return Err(FormatError::InvalidTargetField {
            format: FormatId::ClaudeMessages.as_str().to_string(),
            field: "output_config.effort".to_string(),
            reason: "Claude output effort must be a string".to_string(),
        });
    };
    if crate::protocol::canonical::claude_output_effort_to_openai_reasoning_effort(raw).is_some() {
        Ok(())
    } else {
        Err(FormatError::InvalidEnumValue {
            format: FormatId::ClaudeMessages.as_str().to_string(),
            field: "output_config.effort".to_string(),
            value: raw.to_string(),
        })
    }
}

fn validate_gemini_cross_format_request(body: &Value, target: FormatId) -> Result<(), FormatError> {
    let Some(object) = body.as_object() else {
        return Ok(());
    };

    if object.contains_key("safetySettings") || object.contains_key("safety_settings") {
        return Err(FormatError::LossyConversionBlocked {
            source_format: FormatId::GeminiGenerateContent.as_str().to_string(),
            target_format: target.as_str().to_string(),
            field: "safetySettings".to_string(),
            reason: "target format has no lossless equivalent for Gemini safetySettings"
                .to_string(),
        });
    }
    if object.contains_key("cachedContent") || object.contains_key("cached_content") {
        return Err(FormatError::LossyConversionBlocked {
            source_format: FormatId::GeminiGenerateContent.as_str().to_string(),
            target_format: target.as_str().to_string(),
            field: "cachedContent".to_string(),
            reason: "target format has no lossless equivalent for Gemini cachedContent".to_string(),
        });
    }
    if let Some(generation_config) = object_by_case(object, "generationConfig", "generation_config")
    {
        if generation_config.contains_key("responseModalities")
            || generation_config.contains_key("response_modalities")
        {
            return Err(FormatError::LossyConversionBlocked {
                source_format: FormatId::GeminiGenerateContent.as_str().to_string(),
                target_format: target.as_str().to_string(),
                field: "generationConfig.responseModalities".to_string(),
                reason: "target format has no lossless equivalent for Gemini responseModalities"
                    .to_string(),
            });
        }
        if let Some(thinking_config) =
            object_by_case(generation_config, "thinkingConfig", "thinking_config")
        {
            validate_gemini_cross_format_thinking_config(thinking_config)?;
        }
    }
    if let Some(tool_config) = object_by_case(object, "toolConfig", "tool_config") {
        validate_gemini_cross_format_tool_config(tool_config, target)?;
    }
    if gemini_request_contains_builtin_tool(body, "codeExecution", "code_execution") {
        return Err(FormatError::LossyConversionBlocked {
            source_format: FormatId::GeminiGenerateContent.as_str().to_string(),
            target_format: target.as_str().to_string(),
            field: "tools[].codeExecution".to_string(),
            reason: "target format has no lossless equivalent for Gemini codeExecution".to_string(),
        });
    }
    if gemini_request_contains_builtin_tool(body, "urlContext", "url_context") {
        return Err(FormatError::LossyConversionBlocked {
            source_format: FormatId::GeminiGenerateContent.as_str().to_string(),
            target_format: target.as_str().to_string(),
            field: "tools[].urlContext".to_string(),
            reason: "target format has no lossless equivalent for Gemini urlContext".to_string(),
        });
    }
    if matches!(
        target,
        FormatId::OpenAiResponses | FormatId::OpenAiResponsesCompact
    ) && gemini_request_contains_thought_parts(body)
    {
        return Err(FormatError::LossyConversionBlocked {
            source_format: FormatId::GeminiGenerateContent.as_str().to_string(),
            target_format: target.as_str().to_string(),
            field: "contents[].parts[].thoughtSignature".to_string(),
            reason:
                "OpenAI Responses request input cannot losslessly preserve Gemini thought parts"
                    .to_string(),
        });
    }
    Ok(())
}

fn validate_gemini_cross_format_thinking_config(
    thinking_config: &Map<String, Value>,
) -> Result<(), FormatError> {
    if let Some(value) = thinking_config
        .get("thinkingLevel")
        .or_else(|| thinking_config.get("thinking_level"))
    {
        let Some(raw) = value.as_str() else {
            return Err(FormatError::InvalidTargetField {
                format: FormatId::GeminiGenerateContent.as_str().to_string(),
                field: "generationConfig.thinkingConfig.thinkingLevel".to_string(),
                reason: "Gemini thinkingLevel must be a string".to_string(),
            });
        };
        if !matches!(
            raw.trim().to_ascii_lowercase().as_str(),
            "low" | "medium" | "high"
        ) {
            return Err(FormatError::InvalidEnumValue {
                format: FormatId::GeminiGenerateContent.as_str().to_string(),
                field: "generationConfig.thinkingConfig.thinkingLevel".to_string(),
                value: raw.to_string(),
            });
        }
    }
    for key in thinking_config.keys() {
        if !matches!(
            key.as_str(),
            "includeThoughts"
                | "include_thoughts"
                | "thinkingBudget"
                | "thinking_budget"
                | "thinkingLevel"
                | "thinking_level"
        ) {
            return Err(FormatError::UnsupportedField {
                format: FormatId::GeminiGenerateContent.as_str().to_string(),
                field: format!("generationConfig.thinkingConfig.{key}"),
                reason: "Gemini thinkingConfig field has no canonical cross-format mapping"
                    .to_string(),
            });
        }
    }
    Ok(())
}

fn validate_gemini_cross_format_tool_config(
    tool_config: &Map<String, Value>,
    target: FormatId,
) -> Result<(), FormatError> {
    for key in tool_config.keys() {
        if !matches!(
            key.as_str(),
            "functionCallingConfig" | "function_calling_config"
        ) {
            return Err(FormatError::UnsupportedField {
                format: FormatId::GeminiGenerateContent.as_str().to_string(),
                field: format!("toolConfig.{key}"),
                reason: "Gemini toolConfig field has no canonical cross-format mapping".to_string(),
            });
        }
    }
    let Some(function_calling_config) = object_by_case(
        tool_config,
        "functionCallingConfig",
        "function_calling_config",
    ) else {
        return Err(FormatError::UnsupportedField {
            format: FormatId::GeminiGenerateContent.as_str().to_string(),
            field: "toolConfig".to_string(),
            reason: "Gemini toolConfig must use functionCallingConfig for cross-format conversion"
                .to_string(),
        });
    };
    for key in function_calling_config.keys() {
        if !matches!(
            key.as_str(),
            "mode" | "allowedFunctionNames" | "allowed_function_names"
        ) {
            return Err(FormatError::UnsupportedField {
                format: FormatId::GeminiGenerateContent.as_str().to_string(),
                field: format!("toolConfig.functionCallingConfig.{key}"),
                reason: "Gemini functionCallingConfig field has no canonical cross-format mapping"
                    .to_string(),
            });
        }
    }
    if let Some(value) = function_calling_config.get("mode") {
        let Some(raw) = value.as_str() else {
            return Err(FormatError::InvalidTargetField {
                format: FormatId::GeminiGenerateContent.as_str().to_string(),
                field: "toolConfig.functionCallingConfig.mode".to_string(),
                reason: "Gemini function calling mode must be a string".to_string(),
            });
        };
        if !matches!(
            raw.trim().to_ascii_uppercase().as_str(),
            "NONE" | "AUTO" | "ANY" | "REQUIRED"
        ) {
            return Err(FormatError::InvalidEnumValue {
                format: FormatId::GeminiGenerateContent.as_str().to_string(),
                field: "toolConfig.functionCallingConfig.mode".to_string(),
                value: raw.to_string(),
            });
        }
    }
    if let Some(value) = function_calling_config
        .get("allowedFunctionNames")
        .or_else(|| function_calling_config.get("allowed_function_names"))
    {
        let Some(names) = value.as_array() else {
            return Err(FormatError::InvalidTargetField {
                format: FormatId::GeminiGenerateContent.as_str().to_string(),
                field: "toolConfig.functionCallingConfig.allowedFunctionNames".to_string(),
                reason: "Gemini allowedFunctionNames must be an array of strings".to_string(),
            });
        };
        if !names.iter().all(|value| value.as_str().is_some()) {
            return Err(FormatError::InvalidTargetField {
                format: FormatId::GeminiGenerateContent.as_str().to_string(),
                field: "toolConfig.functionCallingConfig.allowedFunctionNames".to_string(),
                reason: "Gemini allowedFunctionNames must be an array of strings".to_string(),
            });
        }
        if names.len() > 1 {
            return Err(FormatError::LossyConversionBlocked {
                source_format: FormatId::GeminiGenerateContent.as_str().to_string(),
                target_format: target.as_str().to_string(),
                field: "toolConfig.functionCallingConfig.allowedFunctionNames".to_string(),
                reason: "target format can represent at most one named tool choice".to_string(),
            });
        }
    }
    Ok(())
}

fn object_by_case<'a>(
    object: &'a Map<String, Value>,
    camel: &str,
    snake: &str,
) -> Option<&'a Map<String, Value>> {
    object
        .get(camel)
        .or_else(|| object.get(snake))
        .and_then(Value::as_object)
}

fn claude_request_contains_message_thinking_blocks(body: &Value) -> bool {
    claude_request_contains_block_type(body, "thinking")
        || claude_request_contains_block_type(body, "redacted_thinking")
}

fn claude_request_contains_redacted_thinking_blocks(body: &Value) -> bool {
    claude_request_contains_block_type(body, "redacted_thinking")
}

fn claude_request_contains_block_type(body: &Value, expected_type: &str) -> bool {
    let Some(messages) = body
        .as_object()
        .and_then(|object| object.get("messages"))
        .and_then(Value::as_array)
    else {
        return false;
    };
    messages.iter().any(|message| {
        message
            .get("content")
            .and_then(Value::as_array)
            .is_some_and(|blocks| {
                blocks.iter().any(|block| {
                    block
                        .get("type")
                        .and_then(Value::as_str)
                        .is_some_and(|block_type| block_type.eq_ignore_ascii_case(expected_type))
                })
            })
    })
}

fn claude_request_contains_tool_result_content_array(body: &Value) -> bool {
    let Some(messages) = body
        .as_object()
        .and_then(|object| object.get("messages"))
        .and_then(Value::as_array)
    else {
        return false;
    };
    messages.iter().any(|message| {
        message
            .get("content")
            .and_then(Value::as_array)
            .is_some_and(|blocks| {
                blocks.iter().any(|block| {
                    block
                        .get("type")
                        .and_then(Value::as_str)
                        .is_some_and(|block_type| block_type.eq_ignore_ascii_case("tool_result"))
                        && block.get("content").is_some_and(Value::is_array)
                })
            })
    })
}

fn claude_request_contains_unrepresentable_tool_result_content_for_openai_chat(
    body: &Value,
) -> bool {
    claude_request_contains_unrepresentable_tool_result_content(body, |parts| {
        !openai_chat::request::claude_tool_result_parts_are_openai_chat_representable(parts)
    })
}

fn claude_request_contains_unrepresentable_tool_result_content_for_openai_responses(
    body: &Value,
) -> bool {
    claude_request_contains_unrepresentable_tool_result_content(body, |parts| {
        !openai_responses::request::claude_tool_result_parts_are_openai_responses_representable(
            parts,
        )
    })
}

fn claude_request_contains_unrepresentable_tool_result_content(
    body: &Value,
    is_unrepresentable: impl Fn(&[Value]) -> bool,
) -> bool {
    let Some(messages) = body
        .as_object()
        .and_then(|object| object.get("messages"))
        .and_then(Value::as_array)
    else {
        return false;
    };
    messages.iter().any(|message| {
        message
            .get("content")
            .and_then(Value::as_array)
            .is_some_and(|blocks| {
                blocks.iter().any(|block| {
                    block
                        .get("type")
                        .and_then(Value::as_str)
                        .is_some_and(|block_type| block_type.eq_ignore_ascii_case("tool_result"))
                        && block
                            .get("content")
                            .and_then(Value::as_array)
                            .is_some_and(|parts| is_unrepresentable(parts.as_slice()))
                })
            })
    })
}

fn gemini_request_contains_builtin_tool(body: &Value, camel: &str, snake: &str) -> bool {
    let Some(tools) = body
        .as_object()
        .and_then(|object| object.get("tools"))
        .and_then(Value::as_array)
    else {
        return false;
    };
    tools.iter().any(|tool| {
        tool.as_object()
            .is_some_and(|object| object.contains_key(camel) || object.contains_key(snake))
    })
}

fn gemini_request_contains_thought_parts(body: &Value) -> bool {
    let Some(contents) = body
        .as_object()
        .and_then(|object| object.get("contents"))
        .and_then(Value::as_array)
    else {
        return false;
    };
    contents.iter().any(|content| {
        content
            .get("parts")
            .and_then(Value::as_array)
            .is_some_and(|parts| {
                parts.iter().any(|part| {
                    part.get("thought")
                        .and_then(Value::as_bool)
                        .unwrap_or(false)
                })
            })
    })
}

fn claude_request_contains_provider_cache_control(body: &Value) -> bool {
    let Some(object) = body.as_object() else {
        return false;
    };
    object.contains_key("cache_control")
        || claude_system_contains_cache_control(object.get("system"))
        || claude_messages_contain_cache_control(object.get("messages"))
        || claude_tools_contain_cache_control(object.get("tools"))
}

fn claude_system_contains_cache_control(system: Option<&Value>) -> bool {
    match system {
        Some(Value::Array(blocks)) => blocks.iter().any(|block| {
            block
                .as_object()
                .is_some_and(|object| object.contains_key("cache_control"))
        }),
        _ => false,
    }
}

fn claude_messages_contain_cache_control(messages: Option<&Value>) -> bool {
    let Some(messages) = messages.and_then(Value::as_array) else {
        return false;
    };
    messages.iter().any(|message| {
        message
            .get("content")
            .is_some_and(claude_content_value_contains_cache_control)
    })
}

fn claude_content_value_contains_cache_control(content: &Value) -> bool {
    match content {
        Value::Array(blocks) => blocks
            .iter()
            .any(claude_content_block_contains_cache_control),
        _ => false,
    }
}

fn claude_content_block_contains_cache_control(block: &Value) -> bool {
    let Some(object) = block.as_object() else {
        return false;
    };
    if object.contains_key("cache_control") {
        return true;
    }
    object
        .get("type")
        .and_then(Value::as_str)
        .is_some_and(|block_type| block_type.eq_ignore_ascii_case("tool_result"))
        && object
            .get("content")
            .is_some_and(claude_content_value_contains_cache_control)
}

fn claude_tools_contain_cache_control(tools: Option<&Value>) -> bool {
    let Some(tools) = tools.and_then(Value::as_array) else {
        return false;
    };
    tools.iter().any(|tool| {
        tool.as_object()
            .is_some_and(|object| object.contains_key("cache_control"))
    })
}

fn build_request_conversion_report(
    source_format: &str,
    target_format: &str,
    body: &Value,
    output: &Value,
) -> ConversionReport {
    let mut report = ConversionReport::new(source_format, target_format);
    let source = body.as_object();
    let target = output.as_object();
    if let Some(source) = source {
        for key in source.keys() {
            let status = if target.is_some_and(|target| target.contains_key(key)) {
                ConversionFieldStatus::Native
            } else if request_field_has_known_mapping(source_format, target_format, key.as_str()) {
                ConversionFieldStatus::Mapped
            } else {
                ConversionFieldStatus::ExtensionPreserved
            };
            report.record(key.clone(), status, None);
        }
    }
    if let Some(target) = target {
        for key in target.keys() {
            if source.is_some_and(|source| source.contains_key(key)) {
                continue;
            }
            if request_field_is_target_native(source_format, target_format, key.as_str()) {
                report.record(
                    key.clone(),
                    ConversionFieldStatus::Mapped,
                    Some("emitted from canonical field".to_string()),
                );
            }
        }
    }
    report
}

fn build_response_conversion_report(
    source_format: &str,
    target_format: &str,
    body: &Value,
    output: &Value,
) -> ConversionReport {
    let mut report = ConversionReport::new(source_format, target_format);
    let source = body.as_object();
    let target = output.as_object();
    if let Some(source) = source {
        for key in source.keys() {
            let status = if target.is_some_and(|target| target.contains_key(key)) {
                ConversionFieldStatus::Native
            } else if response_field_has_known_mapping(source_format, target_format, key.as_str()) {
                ConversionFieldStatus::Mapped
            } else {
                ConversionFieldStatus::ExtensionPreserved
            };
            report.record(key.clone(), status, None);
        }
    }
    if let Some(target) = target {
        for key in target.keys() {
            if source.is_some_and(|source| source.contains_key(key)) {
                continue;
            }
            if response_field_is_target_native(source_format, target_format, key.as_str()) {
                report.record(
                    key.clone(),
                    ConversionFieldStatus::Mapped,
                    Some("emitted from canonical response field".to_string()),
                );
            }
        }
    }
    report
}

fn request_field_has_known_mapping(source_format: &str, target_format: &str, field: &str) -> bool {
    matches!(
        (
            normalize_known_format(source_format),
            normalize_known_format(target_format),
            field,
        ),
        (
            Some(FormatId::OpenAiChat),
            Some(FormatId::OpenAiResponses),
            "messages"
        ) | (
            Some(FormatId::OpenAiChat),
            Some(FormatId::OpenAiResponses),
            "max_tokens"
        ) | (
            Some(FormatId::OpenAiChat),
            Some(FormatId::OpenAiResponses),
            "max_completion_tokens"
        ) | (
            Some(FormatId::OpenAiChat),
            Some(FormatId::OpenAiResponses),
            "response_format"
        ) | (
            Some(FormatId::OpenAiChat),
            Some(FormatId::OpenAiResponses),
            "reasoning_effort"
        ) | (
            Some(FormatId::OpenAiResponses),
            Some(FormatId::OpenAiChat),
            "input"
        ) | (
            Some(FormatId::OpenAiResponses),
            Some(FormatId::OpenAiChat),
            "instructions"
        ) | (
            Some(FormatId::OpenAiResponses),
            Some(FormatId::OpenAiChat),
            "max_output_tokens"
        ) | (
            Some(FormatId::OpenAiResponses),
            Some(FormatId::OpenAiChat),
            "text"
        ) | (
            Some(FormatId::OpenAiResponses),
            Some(FormatId::OpenAiChat),
            "reasoning"
        )
    )
}

fn request_field_is_target_native(source_format: &str, target_format: &str, field: &str) -> bool {
    matches!(
        (
            normalize_known_format(source_format),
            normalize_known_format(target_format),
            field,
        ),
        (
            Some(FormatId::OpenAiChat),
            Some(FormatId::OpenAiResponses),
            "input"
        ) | (
            Some(FormatId::OpenAiChat),
            Some(FormatId::OpenAiResponses),
            "max_output_tokens"
        ) | (
            Some(FormatId::OpenAiChat),
            Some(FormatId::OpenAiResponses),
            "text"
        ) | (
            Some(FormatId::OpenAiChat),
            Some(FormatId::OpenAiResponses),
            "reasoning"
        ) | (
            Some(FormatId::OpenAiResponses),
            Some(FormatId::OpenAiChat),
            "messages"
        ) | (
            Some(FormatId::OpenAiResponses),
            Some(FormatId::OpenAiChat),
            "max_completion_tokens"
        ) | (
            Some(FormatId::OpenAiResponses),
            Some(FormatId::OpenAiChat),
            "response_format"
        ) | (
            Some(FormatId::OpenAiResponses),
            Some(FormatId::OpenAiChat),
            "reasoning_effort"
        )
    )
}

fn response_field_has_known_mapping(source_format: &str, target_format: &str, field: &str) -> bool {
    match (
        normalize_known_format(source_format),
        normalize_known_format(target_format),
    ) {
        (Some(FormatId::OpenAiChat), Some(FormatId::OpenAiResponses)) => {
            matches!(field, "choices")
        }
        (Some(FormatId::OpenAiResponses), Some(FormatId::OpenAiChat)) => {
            matches!(
                field,
                "output" | "status" | "incomplete_details" | "output_text"
            )
        }
        (
            Some(FormatId::ClaudeMessages),
            Some(FormatId::OpenAiChat | FormatId::OpenAiResponses),
        ) => {
            matches!(field, "content" | "stop_reason" | "stop_sequence")
        }
        (
            Some(FormatId::GeminiGenerateContent),
            Some(FormatId::OpenAiChat | FormatId::OpenAiResponses),
        ) => {
            matches!(field, "candidates" | "usageMetadata" | "usage_metadata")
        }
        (
            Some(FormatId::OpenAiChat | FormatId::OpenAiResponses),
            Some(FormatId::ClaudeMessages),
        ) => {
            matches!(
                field,
                "choices" | "output" | "status" | "incomplete_details"
            )
        }
        (
            Some(FormatId::OpenAiChat | FormatId::OpenAiResponses),
            Some(FormatId::GeminiGenerateContent),
        ) => {
            matches!(
                field,
                "choices" | "output" | "status" | "incomplete_details"
            )
        }
        _ => false,
    }
}

fn response_field_is_target_native(source_format: &str, target_format: &str, field: &str) -> bool {
    match (
        normalize_known_format(source_format),
        normalize_known_format(target_format),
    ) {
        (Some(FormatId::OpenAiChat), Some(FormatId::OpenAiResponses)) => {
            matches!(field, "output" | "status" | "output_text")
        }
        (Some(FormatId::OpenAiResponses), Some(FormatId::OpenAiChat)) => matches!(field, "choices"),
        (_, Some(FormatId::OpenAiChat)) => matches!(field, "choices"),
        (_, Some(FormatId::OpenAiResponses)) => {
            matches!(field, "output" | "status" | "output_text")
        }
        (_, Some(FormatId::ClaudeMessages)) => matches!(field, "content" | "stop_reason"),
        (_, Some(FormatId::GeminiGenerateContent)) => matches!(field, "candidates"),
        _ => false,
    }
}

fn normalize_known_format(format: &str) -> Option<FormatId> {
    match FormatId::parse(format)? {
        FormatId::OpenAiResponsesCompact => Some(FormatId::OpenAiResponses),
        value => Some(value),
    }
}

#[cfg(test)]
#[path = "registry/tests.rs"]
mod tests;
