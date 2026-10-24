use base64::Engine as _;
use std::collections::BTreeMap;

use aether_ai_formats::formats::conversion::response::{
    convert_claude_response_to_openai_responses, convert_gemini_chat_response_to_openai_chat,
    convert_openai_chat_response_to_claude_chat, convert_openai_chat_response_to_gemini_chat,
    convert_openai_chat_response_to_openai_responses,
    convert_openai_responses_response_to_openai_chat,
};
use aether_ai_formats::formats::openai::responses::response::ensure_modern_openai_responses_response_fields;
use aether_ai_formats::formats::openai::responses::{
    openai_responses_message_item_id, openai_responses_reasoning_text_parts,
    openai_responses_synthetic_reasoning_item_id,
};
use aether_ai_formats::formats::registry::{convert_response, FormatContext, FormatError};
use aether_ai_formats::{
    canonical_response_unknown_block_count, canonical_to_claude_response,
    canonical_to_embedding_response, canonical_to_gemini_response,
    canonical_to_openai_chat_response, canonical_to_openai_responses_compact_response,
    canonical_to_openai_responses_response, from_claude_to_canonical_response,
    from_embedding_to_canonical_response, from_gemini_to_canonical_response,
    from_openai_chat_to_canonical_response, from_openai_responses_to_canonical_response,
    sync_chat_response_conversion_kind, sync_cli_response_conversion_kind,
};
use serde_json::{json, Map, Value};

use super::{decode_sync_report_body_base64, AiSurfaceFinalizeError};
use crate::formats::claude::messages::stream::ClaudeProviderState;
use crate::formats::gemini::generate_content::stream::GeminiProviderState;
use crate::formats::openai::chat::response::openai_chat_reasoning_texts;
use crate::formats::openai::chat::stream::{OpenAIChatProviderState, OpenAIResponsesProviderState};
use crate::formats::shared::model_directives::model_directive_display_model_from_report_context;
use crate::formats::shared::response::sanitize_claude_read_tool_inputs;
use crate::formats::shared::stream_core::common::{
    content_part_from_openai_image_generation_item, gemini_usage_metadata_from_usage,
    map_openai_finish_reason_to_gemini, parse_json_arguments_value,
    unsupported_stream_event_message, CanonicalContentPart, CanonicalStreamEvent,
    CanonicalStreamFrame, CanonicalUsage,
};

#[derive(Clone, Debug, PartialEq)]
pub struct StandardCrossFormatSyncProduct {
    pub client_body_json: Value,
    pub provider_body_json: Value,
}

struct OpenAiCrossFormatProviderBody {
    body_json: Value,
    // Only validated stream aggregation may use a client projection that omits provider-only fields.
    aggregated_from_stream: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub enum StandardSyncFinalizeNormalizedProduct {
    SuccessBody(Value),
    CrossFormat(StandardCrossFormatSyncProduct),
}

pub fn maybe_build_standard_cross_format_sync_product_from_normalized_payload(
    report_kind: &str,
    status_code: u16,
    report_context: Option<&Value>,
    body_json: Option<&Value>,
    body_base64: Option<&str>,
) -> Result<Option<StandardCrossFormatSyncProduct>, AiSurfaceFinalizeError> {
    if status_code >= 400 {
        return Ok(None);
    }

    let Some(report_context) = report_context else {
        return Ok(None);
    };
    let provider_api_format = report_context
        .get("provider_api_format")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let client_api_format = report_context
        .get("client_api_format")
        .and_then(Value::as_str)
        .unwrap_or_default();

    let (aggregated_stream_body, aggregated_stream_api_format) = match body_base64 {
        Some(body_base64) => {
            let body_bytes = decode_sync_report_body_base64(body_base64)?;
            let provider_stream_event_api_format =
                provider_stream_event_api_format_for_report_context(
                    report_context,
                    provider_api_format,
                );
            if is_standard_chat_finalize_kind(report_kind) {
                let body = try_aggregate_standard_chat_stream_sync_response(
                    &body_bytes,
                    &provider_stream_event_api_format,
                )?;
                let api_format = body
                    .as_ref()
                    .map(|_| provider_stream_event_api_format.clone());
                (body, api_format)
            } else if is_standard_cli_finalize_kind(report_kind) {
                let body = try_aggregate_standard_cli_stream_sync_response(
                    &body_bytes,
                    &provider_stream_event_api_format,
                )?;
                let api_format = body
                    .as_ref()
                    .map(|_| provider_stream_event_api_format.clone());
                (body, api_format)
            } else {
                return Ok(None);
            }
        }
        None => (None, None),
    };
    let provider_body_api_format = aggregated_stream_api_format
        .as_deref()
        .unwrap_or(provider_api_format);

    let aggregated_from_stream = aggregated_stream_body.is_some();
    let Some(provider_body_json) = aggregated_stream_body.or_else(|| body_json.cloned()) else {
        return Ok(None);
    };
    let projection_fallback_body = aggregated_from_stream.then(|| provider_body_json.clone());

    let product = maybe_build_standard_cross_format_sync_product(
        report_kind,
        provider_body_api_format,
        client_api_format,
        report_context,
        provider_body_json,
    );
    if product.is_some() {
        return Ok(product);
    }
    let Some(provider_body_json) = projection_fallback_body else {
        return Ok(None);
    };
    Ok(project_validated_openai_responses_stream_sync_product(
        report_kind,
        provider_body_api_format,
        client_api_format,
        report_context,
        provider_body_json,
    ))
}

/// Forced-stream Responses upstreams (Codex, xAI) echo request metadata such as
/// `parallel_tool_calls`, `tools` and encrypted reasoning back in the aggregated
/// body, which the strict cross-format response check refuses. The aggregated
/// body stays the provider body, so the client projection may drop those
/// provider-only fields — mirroring the OpenAI Chat client path.
fn project_validated_openai_responses_stream_sync_product(
    report_kind: &str,
    provider_api_format: &str,
    client_api_format: &str,
    report_context: &Value,
    provider_body_json: Value,
) -> Option<StandardCrossFormatSyncProduct> {
    let provider_api_format = normalize_openai_responses_family_api_format(provider_api_format);
    if !matches!(
        provider_api_format.as_str(),
        "openai:responses" | "openai:responses:compact"
    ) {
        return None;
    }
    let client_api_format = client_api_format.trim().to_ascii_lowercase();
    if is_standard_chat_finalize_kind(report_kind) {
        sync_chat_response_conversion_kind(&provider_api_format, &client_api_format)?;
    } else if is_standard_cli_finalize_kind(report_kind) {
        sync_cli_response_conversion_kind(&provider_api_format, &client_api_format)?;
    } else {
        return None;
    }
    let client_body_json = project_validated_openai_responses_stream_to_client(
        &provider_body_json,
        &client_api_format,
        report_context,
    )?;
    let client_body_json =
        client_body_with_report_context_model(client_body_json, report_context, &client_api_format);
    Some(StandardCrossFormatSyncProduct {
        client_body_json,
        provider_body_json,
    })
}

pub fn maybe_build_standard_same_format_sync_body_from_normalized_payload(
    report_kind: &str,
    status_code: u16,
    report_context: Option<&Value>,
    body_json: Option<&Value>,
    body_base64: Option<&str>,
) -> Result<Option<Value>, AiSurfaceFinalizeError> {
    let stream_body = maybe_build_standard_same_format_stream_sync_body(
        report_kind,
        status_code,
        report_context,
        body_base64,
    )?;
    Ok(stream_body.or_else(|| {
        maybe_build_standard_same_format_sync_body(
            report_kind,
            status_code,
            report_context,
            body_json,
        )
    }))
}

pub fn maybe_build_openai_responses_same_family_sync_body_from_normalized_payload(
    report_kind: &str,
    status_code: u16,
    report_context: Option<&Value>,
    body_json: Option<&Value>,
    body_base64: Option<&str>,
) -> Result<Option<Value>, AiSurfaceFinalizeError> {
    let stream_body = maybe_build_openai_responses_same_family_stream_sync_body(
        report_kind,
        status_code,
        report_context,
        body_base64,
    )?;
    Ok(stream_body.or_else(|| {
        maybe_build_openai_responses_same_family_sync_body(
            report_kind,
            status_code,
            report_context,
            body_json,
        )
    }))
}

pub fn maybe_build_openai_chat_cross_format_sync_product_from_normalized_payload(
    report_kind: &str,
    status_code: u16,
    report_context: Option<&Value>,
    body_json: Option<&Value>,
    body_base64: Option<&str>,
) -> Result<Option<StandardCrossFormatSyncProduct>, AiSurfaceFinalizeError> {
    if report_kind != "openai_chat_sync_finalize" || status_code >= 400 {
        return Ok(None);
    }

    let Some(report_context) = report_context else {
        return Ok(None);
    };
    let provider_api_format = report_context
        .get("provider_api_format")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    let client_api_format = report_context
        .get("client_api_format")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();

    if client_api_format != "openai:chat"
        || sync_chat_response_conversion_kind(&provider_api_format, &client_api_format).is_none()
    {
        return Ok(None);
    }

    let Some(provider_body) =
        maybe_build_openai_cross_format_provider_body_from_normalized_payload(
            body_json,
            body_base64,
            &provider_api_format,
        )?
    else {
        return Ok(None);
    };

    let provider_body_json = provider_body.body_json;
    let Some(client_body_json) = (match provider_api_format.as_str() {
        "claude:messages" => {
            convert_claude_canonical_response_to_openai_chat(&provider_body_json, report_context)
        }
        "gemini:generate_content" => {
            convert_gemini_canonical_response_to_openai_chat(&provider_body_json, report_context)
        }
        "openai:responses" => {
            convert_openai_responses_response_to_openai_chat(&provider_body_json, report_context)
                .or_else(|| {
                    provider_body
                        .aggregated_from_stream
                        .then(|| {
                            project_validated_openai_responses_stream_to_openai_chat(
                                &provider_body_json,
                                report_context,
                            )
                        })
                        .flatten()
                })
        }
        _ => None,
    }) else {
        return Ok(None);
    };

    Ok(Some(StandardCrossFormatSyncProduct {
        client_body_json,
        provider_body_json,
    }))
}

pub fn maybe_build_openai_responses_cross_format_sync_product_from_normalized_payload(
    report_kind: &str,
    status_code: u16,
    report_context: Option<&Value>,
    body_json: Option<&Value>,
    body_base64: Option<&str>,
) -> Result<Option<StandardCrossFormatSyncProduct>, AiSurfaceFinalizeError> {
    if !is_openai_responses_finalize_kind(report_kind) || status_code >= 400 {
        return Ok(None);
    }

    let Some(report_context) = report_context else {
        return Ok(None);
    };
    let provider_api_format = report_context
        .get("provider_api_format")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    let client_api_format = report_context
        .get("client_api_format")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    let normalized_client_api_format =
        normalize_openai_responses_family_api_format(&client_api_format);

    if !matches!(
        normalized_client_api_format.as_str(),
        "openai:responses" | "openai:responses:compact"
    ) || sync_cli_response_conversion_kind(&provider_api_format, &normalized_client_api_format)
        .is_none()
    {
        return Ok(None);
    }

    let Some(provider_body) =
        maybe_build_openai_cross_format_provider_body_from_normalized_payload(
            body_json,
            body_base64,
            &provider_api_format,
        )?
    else {
        return Ok(None);
    };
    let provider_body_json = provider_body.body_json;

    let normalized_provider_api_format =
        normalize_openai_responses_family_api_format(&provider_api_format);
    let Some(client_body_json) = (match normalized_provider_api_format.as_str() {
        "openai:responses" | "openai:responses:compact" => Some(provider_body_json.clone()),
        "claude:messages" => {
            convert_claude_response_to_openai_responses(&provider_body_json, report_context)
        }
        "gemini:generate_content" => convert_gemini_canonical_response_to_openai_responses(
            &provider_body_json,
            report_context,
            normalized_client_api_format == "openai:responses:compact",
        ),
        _ => None,
    }) else {
        return Ok(None);
    };

    Ok(Some(StandardCrossFormatSyncProduct {
        client_body_json,
        provider_body_json,
    }))
}

fn maybe_encode_sync_capture_envelope_stream_body(
    body_json: Option<&Value>,
) -> Result<Option<String>, AiSurfaceFinalizeError> {
    let Some(body_json) = body_json else {
        return Ok(None);
    };
    let Some(body_object) = body_json.as_object() else {
        return Ok(None);
    };
    let Some(chunks) = body_object.get("chunks").and_then(Value::as_array) else {
        return Ok(None);
    };
    if is_error_like_sync_body(body_json) {
        return Ok(None);
    }
    if chunks.is_empty() {
        return Err(AiSurfaceFinalizeError::new(
            "Provider stream capture envelope contains no events",
        ));
    }

    if body_object
        .get("raw_response")
        .is_some_and(|value| !value.is_null())
    {
        return Err(AiSurfaceFinalizeError::new(
            "Provider stream capture envelope contains an unparsed raw response",
        ));
    }
    if let Some(metadata_value) = body_object.get("metadata") {
        let Some(metadata) = metadata_value.as_object() else {
            return Err(AiSurfaceFinalizeError::new(
                "Provider stream capture envelope metadata must be an object",
            ));
        };
        if metadata
            .get("parse_error")
            .is_some_and(|value| !value.is_null())
        {
            return Err(AiSurfaceFinalizeError::new(
                "Provider stream capture envelope reports a stream parse error",
            ));
        }
        if let Some(stream) = metadata.get("stream") {
            if stream.as_bool() != Some(true) {
                return Err(AiSurfaceFinalizeError::new(
                    "Provider stream capture envelope metadata.stream must be true",
                ));
            }
        }

        let dropped_chunks = sync_capture_metadata_u64(metadata, "dropped_chunks")?;
        let stored_chunks = sync_capture_metadata_u64(metadata, "stored_chunks")?;
        let total_chunks = sync_capture_metadata_u64(metadata, "total_chunks")?;
        if dropped_chunks.is_some_and(|dropped| dropped > 0) {
            return Err(incomplete_sync_capture_envelope_error());
        }
        match (stored_chunks, total_chunks) {
            (None, None) => {}
            (Some(stored), Some(total)) if stored == total && stored == chunks.len() as u64 => {}
            _ => return Err(incomplete_sync_capture_envelope_error()),
        }
    }

    let mut stream_body = Vec::new();
    for chunk in chunks {
        if !chunk.is_object() {
            return Err(AiSurfaceFinalizeError::new(
                "Provider stream capture envelope contains a non-object event",
            ));
        }
        stream_body.extend_from_slice(b"data: ");
        serde_json::to_writer(&mut stream_body, chunk)?;
        stream_body.extend_from_slice(b"\n\n");
    }

    Ok(Some(
        base64::engine::general_purpose::STANDARD.encode(stream_body),
    ))
}

fn sync_capture_metadata_u64(
    metadata: &Map<String, Value>,
    field: &str,
) -> Result<Option<u64>, AiSurfaceFinalizeError> {
    let Some(value) = metadata.get(field) else {
        return Ok(None);
    };
    value.as_u64().map(Some).ok_or_else(|| {
        AiSurfaceFinalizeError::new(format!(
            "Provider stream capture envelope metadata.{field} must be an unsigned integer"
        ))
    })
}

fn incomplete_sync_capture_envelope_error() -> AiSurfaceFinalizeError {
    AiSurfaceFinalizeError::new(
        "Provider stream capture envelope is incomplete and cannot be finalized losslessly",
    )
}

fn sync_finalize_supports_capture_envelope(
    report_kind: &str,
    report_context: Option<&Value>,
) -> bool {
    if !is_standard_chat_finalize_kind(report_kind) && !is_standard_cli_finalize_kind(report_kind) {
        return false;
    }
    let Some(report_context) = report_context else {
        return false;
    };
    let provider_api_format = report_context
        .get("provider_api_format")
        .and_then(Value::as_str)
        .unwrap_or_default();
    // Gemini private capture envelopes are normalized by provider_compat before this boundary.
    matches!(
        aether_ai_formats::normalize_api_format_alias(
            &provider_stream_event_api_format_for_report_context(
                report_context,
                provider_api_format,
            )
        )
        .as_str(),
        "openai:chat" | "openai:responses" | "openai:responses:compact" | "claude:messages"
    )
}

pub fn maybe_build_standard_sync_finalize_product_from_normalized_payload(
    report_kind: &str,
    status_code: u16,
    report_context: Option<&Value>,
    body_json: Option<&Value>,
    body_base64: Option<&str>,
) -> Result<Option<StandardSyncFinalizeNormalizedProduct>, AiSurfaceFinalizeError> {
    let capture_stream_body_base64 = if status_code < 400
        && body_base64.is_none()
        && sync_finalize_supports_capture_envelope(report_kind, report_context)
    {
        maybe_encode_sync_capture_envelope_stream_body(body_json)?
    } else {
        None
    };
    let capture_envelope_used = capture_stream_body_base64.is_some();
    let body_json = if capture_envelope_used {
        None
    } else {
        body_json
    };
    let body_base64 = body_base64.or(capture_stream_body_base64.as_deref());

    // Cross-format sync attempts can contain raw bytes because the plan requested a stream even
    // though the provider returned one complete JSON response. Do not feed that response into an
    // SSE aggregator. Capture envelopes and same-format responses retain their existing precedence.
    let non_stream_capture_body_json =
        if capture_envelope_used || !sync_finalize_needs_conversion(report_context) {
            None
        } else {
            body_base64.and_then(decode_non_stream_sync_capture_body)
        };
    let (body_json, body_base64) = match non_stream_capture_body_json.as_ref() {
        Some(capture_body_json) => (body_json.or(Some(capture_body_json)), None),
        None => (body_json, body_base64),
    };

    if let Some(body_json) = maybe_build_standard_same_format_sync_body_from_normalized_payload(
        report_kind,
        status_code,
        report_context,
        body_json,
        body_base64,
    )? {
        return Ok(Some(StandardSyncFinalizeNormalizedProduct::SuccessBody(
            body_json,
        )));
    }

    if let Some(body_json) =
        maybe_build_openai_responses_same_family_sync_body_from_normalized_payload(
            report_kind,
            status_code,
            report_context,
            body_json,
            body_base64,
        )?
    {
        return Ok(Some(StandardSyncFinalizeNormalizedProduct::SuccessBody(
            body_json,
        )));
    }

    if let Some(product) =
        maybe_build_openai_chat_cross_format_sync_product_from_normalized_payload(
            report_kind,
            status_code,
            report_context,
            body_json,
            body_base64,
        )?
    {
        return Ok(Some(StandardSyncFinalizeNormalizedProduct::CrossFormat(
            product,
        )));
    }

    if let Some(product) =
        maybe_build_openai_responses_cross_format_sync_product_from_normalized_payload(
            report_kind,
            status_code,
            report_context,
            body_json,
            body_base64,
        )?
    {
        return Ok(Some(StandardSyncFinalizeNormalizedProduct::CrossFormat(
            product,
        )));
    }

    if let Some(product) = maybe_build_embedding_cross_format_sync_product_from_normalized_payload(
        report_kind,
        status_code,
        report_context,
        body_json,
        body_base64,
    )? {
        return Ok(Some(StandardSyncFinalizeNormalizedProduct::CrossFormat(
            product,
        )));
    }

    let product = maybe_build_standard_cross_format_sync_product_from_normalized_payload(
        report_kind,
        status_code,
        report_context,
        body_json,
        body_base64,
    )?
    .map(StandardSyncFinalizeNormalizedProduct::CrossFormat);
    if capture_envelope_used && product.is_none() {
        return Err(AiSurfaceFinalizeError::new(
            "Provider stream capture envelope cannot be finalized losslessly",
        ));
    }
    Ok(product)
}

pub fn maybe_build_embedding_cross_format_sync_product_from_normalized_payload(
    report_kind: &str,
    status_code: u16,
    report_context: Option<&Value>,
    body_json: Option<&Value>,
    body_base64: Option<&str>,
) -> Result<Option<StandardCrossFormatSyncProduct>, AiSurfaceFinalizeError> {
    if report_kind != crate::contracts::OPENAI_EMBEDDING_SYNC_FINALIZE_REPORT_KIND
        || status_code >= 400
    {
        return Ok(None);
    }

    let Some(report_context) = report_context else {
        return Ok(None);
    };
    let provider_api_format = report_context
        .get("provider_api_format")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    let client_api_format = report_context
        .get("client_api_format")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();

    if provider_api_format == client_api_format {
        return Ok(None);
    }

    let Some(provider_namespace) =
        embedding_response_namespace_for_api_format(&provider_api_format)
    else {
        return Ok(None);
    };
    let Some(client_namespace) = embedding_response_namespace_for_api_format(&client_api_format)
    else {
        return Ok(None);
    };

    let provider_body_json = match body_base64 {
        Some(body_base64) => {
            let body_bytes = decode_sync_report_body_base64(body_base64)?;
            serde_json::from_slice::<Value>(&body_bytes).ok()
        }
        None => body_json.cloned(),
    };
    let Some(provider_body_json) =
        provider_body_json.filter(|value| !is_error_like_sync_body(value))
    else {
        return Ok(None);
    };

    let mut canonical =
        match from_embedding_to_canonical_response(&provider_body_json, provider_namespace) {
            Some(canonical) => canonical,
            None => return Ok(None),
        };
    apply_report_context_model_fallback(&mut canonical.model, report_context);
    let Some(client_body_json) = canonical_to_embedding_response(&canonical, client_namespace)
    else {
        return Ok(None);
    };
    let client_body_json =
        client_body_with_report_context_model(client_body_json, report_context, &client_api_format);

    Ok(Some(StandardCrossFormatSyncProduct {
        client_body_json,
        provider_body_json,
    }))
}

fn embedding_response_namespace_for_api_format(api_format: &str) -> Option<&'static str> {
    match aether_ai_formats::normalize_api_format_alias(api_format).as_str() {
        "openai:embedding" => Some("openai"),
        "jina:embedding" => Some("jina"),
        "gemini:embedding" => Some("gemini"),
        "aliyun:multimodal_embedding" => Some("aliyun"),
        _ => None,
    }
}

fn maybe_build_standard_same_format_sync_body(
    report_kind: &str,
    status_code: u16,
    report_context: Option<&Value>,
    body_json: Option<&Value>,
) -> Option<Value> {
    if status_code >= 400 {
        return None;
    }

    let report_context = report_context?;
    let expected_api_format = standard_same_format_api_format(report_kind)?;
    let provider_api_format = report_context
        .get("provider_api_format")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    let client_api_format = report_context
        .get("client_api_format")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    let needs_conversion = report_context
        .get("needs_conversion")
        .and_then(Value::as_bool)
        .unwrap_or(false);

    if provider_api_format != expected_api_format
        || client_api_format != expected_api_format
        || needs_conversion
    {
        return None;
    }

    let body_json = body_json?;
    if report_kind == "openai_memories_sync_finalize" {
        return Some(body_json.clone());
    }
    if is_error_like_sync_body(body_json) {
        return None;
    }
    if api_format_is_gemini_generate_content(expected_api_format)
        && !gemini_generate_content_body_has_visible_output(body_json)
    {
        return None;
    }

    let mut body_json = body_json.clone();
    if expected_api_format == "claude:messages"
        && anthropic_legacy_compatibility_enabled(report_context)
    {
        sanitize_claude_read_tool_inputs(&mut body_json);
    }

    Some(client_body_with_report_context_model(
        body_json,
        report_context,
        &client_api_format,
    ))
}

fn maybe_build_standard_same_format_stream_sync_body(
    report_kind: &str,
    status_code: u16,
    report_context: Option<&Value>,
    body_base64: Option<&str>,
) -> Result<Option<Value>, AiSurfaceFinalizeError> {
    if status_code >= 400 {
        return Ok(None);
    }

    let report_context = match report_context {
        Some(report_context) => report_context,
        None => return Ok(None),
    };
    let expected_api_format = match standard_same_format_api_format(report_kind) {
        Some(expected_api_format) => expected_api_format,
        None => return Ok(None),
    };
    let provider_api_format = report_context
        .get("provider_api_format")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    let client_api_format = report_context
        .get("client_api_format")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    let needs_conversion = report_context
        .get("needs_conversion")
        .and_then(Value::as_bool)
        .unwrap_or(false);

    if provider_api_format != expected_api_format
        || client_api_format != expected_api_format
        || needs_conversion
    {
        return Ok(None);
    }

    let Some(body_base64) = body_base64 else {
        return Ok(None);
    };
    let body_bytes = decode_sync_report_body_base64(body_base64)?;
    let provider_stream_event_api_format =
        provider_stream_event_api_format_for_report_context(report_context, &provider_api_format);
    let Some(mut body) = try_aggregate_standard_chat_stream_sync_response(
        &body_bytes,
        &provider_stream_event_api_format,
    )?
    else {
        return Ok(None);
    };
    if provider_stream_event_api_format == "claude:messages"
        && anthropic_legacy_compatibility_enabled(report_context)
    {
        sanitize_claude_read_tool_inputs(&mut body);
    }
    if api_format_is_gemini_generate_content(&provider_stream_event_api_format)
        && !gemini_generate_content_body_has_visible_output(&body)
    {
        return Ok(None);
    }
    let Some(client_body) = convert_aggregated_stream_body_to_client_sync_response(
        report_kind,
        body,
        &provider_stream_event_api_format,
        &client_api_format,
        report_context,
    ) else {
        return Ok(None);
    };
    Ok(Some(client_body_with_report_context_model(
        client_body,
        report_context,
        &client_api_format,
    )))
}

pub(super) fn anthropic_legacy_compatibility_enabled(report_context: &Value) -> bool {
    if let Some(profile) = report_context.get("anthropic_compatibility_profile") {
        return profile
            .as_str()
            .map(str::trim)
            .is_some_and(|profile| profile.eq_ignore_ascii_case("claude_code_legacy"));
    }

    report_context
        .get("provider_type")
        .and_then(Value::as_str)
        .map(str::trim)
        .is_some_and(|provider_type| provider_type.eq_ignore_ascii_case("claude_code"))
}

fn maybe_build_openai_responses_same_family_sync_body(
    report_kind: &str,
    status_code: u16,
    report_context: Option<&Value>,
    body_json: Option<&Value>,
) -> Option<Value> {
    if status_code >= 400 || !is_openai_responses_finalize_kind(report_kind) {
        return None;
    }

    let report_context = report_context?;
    let provider_api_format = report_context
        .get("provider_api_format")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    let client_api_format = report_context
        .get("client_api_format")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    let needs_conversion = report_context
        .get("needs_conversion")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let normalized_provider_api_format =
        normalize_openai_responses_family_api_format(&provider_api_format);
    let normalized_client_api_format =
        normalize_openai_responses_family_api_format(&client_api_format);
    if !is_openai_responses_family_api_format(&provider_api_format)
        || !is_openai_responses_family_api_format(&client_api_format)
        || (normalized_provider_api_format == normalized_client_api_format && needs_conversion)
    {
        return None;
    }

    let body_json = body_json?;
    if is_error_like_sync_body(body_json) {
        return None;
    }

    Some(client_body_with_report_context_model(
        body_json.clone(),
        report_context,
        &client_api_format,
    ))
}

fn maybe_build_openai_responses_same_family_stream_sync_body(
    report_kind: &str,
    status_code: u16,
    report_context: Option<&Value>,
    body_base64: Option<&str>,
) -> Result<Option<Value>, AiSurfaceFinalizeError> {
    if status_code >= 400 || !is_openai_responses_finalize_kind(report_kind) {
        return Ok(None);
    }

    let report_context = match report_context {
        Some(report_context) => report_context,
        None => return Ok(None),
    };
    let provider_api_format = report_context
        .get("provider_api_format")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    let client_api_format = report_context
        .get("client_api_format")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    let needs_conversion = report_context
        .get("needs_conversion")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let provider_stream_event_api_format =
        provider_stream_event_api_format_for_report_context(report_context, &provider_api_format);
    let normalized_provider_api_format =
        normalize_openai_responses_family_api_format(&provider_stream_event_api_format);
    let normalized_client_api_format =
        normalize_openai_responses_family_api_format(&client_api_format);
    if !is_openai_responses_family_api_format(&provider_stream_event_api_format)
        || !is_openai_responses_family_api_format(&client_api_format)
        || (is_openai_responses_family_api_format(&provider_api_format)
            && normalized_provider_api_format == normalized_client_api_format
            && needs_conversion)
    {
        return Ok(None);
    }

    let Some(body_base64) = body_base64 else {
        return Ok(None);
    };
    let body_bytes = decode_sync_report_body_base64(body_base64)?;
    // Same-family clients retain the authoritative terminal body verbatim, including future
    // output item fields, but unknown intermediate event types still fail closed.
    ensure_no_unknown_openai_responses_stream_events(&body_bytes, true)?;
    let terminal_body = validated_terminal_openai_responses_stream_response(&body_bytes)?;
    if terminal_body.get("status").and_then(Value::as_str) != Some("completed")
        || terminal_body
            .get("output")
            .and_then(Value::as_array)
            .is_some_and(|output| !output.is_empty())
    {
        return Ok(Some(client_body_with_report_context_model(
            terminal_body,
            report_context,
            &client_api_format,
        )));
    }
    Ok(
        aggregate_openai_responses_stream_sync_response_from_validated_terminal(
            &body_bytes,
            terminal_body,
        )
        .map(|body| {
            client_body_with_report_context_model(body, report_context, &client_api_format)
        }),
    )
}

fn validated_terminal_openai_responses_stream_response(
    body: &[u8],
) -> Result<Value, AiSurfaceFinalizeError> {
    let events = parse_stream_json_events(body).ok_or_else(|| {
        AiSurfaceFinalizeError::new("OpenAI Responses stream contains invalid event framing")
    })?;
    let mut terminal_response = None;

    for event in events {
        let event = event.as_object().ok_or_else(|| {
            AiSurfaceFinalizeError::new("OpenAI Responses stream contains a non-object event")
        })?;
        let Some(event_type) = event.get("type").and_then(Value::as_str) else {
            continue;
        };
        let expected_status = match event_type {
            "response.completed" | "response.done" => "completed",
            "response.incomplete" => "incomplete",
            "response.failed" => "failed",
            _ => continue,
        };
        if terminal_response.is_some() {
            return Err(AiSurfaceFinalizeError::new(
                "OpenAI Responses stream contains multiple authoritative terminal events",
            ));
        }
        let mut response = event
            .get("response")
            .and_then(Value::as_object)
            .cloned()
            .ok_or_else(|| {
                AiSurfaceFinalizeError::new(
                    "OpenAI Responses terminal event is missing its response object",
                )
            })?;
        if response
            .get("status")
            .and_then(Value::as_str)
            .is_some_and(|status| status != expected_status)
        {
            return Err(AiSurfaceFinalizeError::new(
                "OpenAI Responses terminal event conflicts with response.status",
            ));
        }
        response
            .entry("status".to_string())
            .or_insert_with(|| Value::String(expected_status.to_string()));
        terminal_response = Some(Value::Object(response));
    }

    terminal_response.ok_or_else(|| {
        AiSurfaceFinalizeError::new(
            "OpenAI Responses stream is missing an authoritative terminal event",
        )
    })
}

fn maybe_build_openai_cross_format_provider_body_from_normalized_payload(
    body_json: Option<&Value>,
    body_base64: Option<&str>,
    provider_api_format: &str,
) -> Result<Option<OpenAiCrossFormatProviderBody>, AiSurfaceFinalizeError> {
    let aggregated_stream_body = match body_base64 {
        Some(body_base64) => {
            let body_bytes = decode_sync_report_body_base64(body_base64)?;
            let normalized_provider_api_format =
                normalize_openai_responses_family_api_format(provider_api_format);
            match normalized_provider_api_format.as_str() {
                "claude:messages" => try_aggregate_claude_stream_sync_response(&body_bytes)?,
                "gemini:generate_content" => {
                    try_aggregate_gemini_stream_sync_response(&body_bytes)?
                }
                "openai:responses" | "openai:responses:compact" => {
                    try_aggregate_openai_responses_stream_sync_response(&body_bytes)?
                }
                _ => None,
            }
        }
        None => None,
    };

    let aggregated_from_stream = aggregated_stream_body.is_some();
    let provider_body_json = aggregated_stream_body.or_else(|| body_json.cloned());
    Ok(provider_body_json
        .filter(|value| !is_error_like_sync_body(value))
        .map(|body_json| OpenAiCrossFormatProviderBody {
            body_json,
            aggregated_from_stream,
        }))
}

fn sync_finalize_needs_conversion(report_context: Option<&Value>) -> bool {
    report_context
        .and_then(|report_context| report_context.get("needs_conversion"))
        .and_then(Value::as_bool)
        .unwrap_or(false)
}

fn decode_non_stream_sync_capture_body(body_base64: &str) -> Option<Value> {
    let body_bytes = base64::engine::general_purpose::STANDARD
        .decode(body_base64)
        .ok()?;
    serde_json::from_slice::<Value>(&body_bytes)
        .ok()
        .filter(Value::is_object)
        .filter(|body_json| !is_stream_event_object(body_json))
}

/// Unframed JSON events are accepted by the stream parsers and must not be mistaken for complete
/// provider response bodies merely because the entire capture parses as one JSON object.
fn is_stream_event_object(value: &Value) -> bool {
    let Some(object) = value.as_object() else {
        return false;
    };
    if object
        .get("object")
        .and_then(Value::as_str)
        .is_some_and(|object| object.ends_with(".chunk"))
    {
        return true;
    }

    object
        .get("type")
        .and_then(Value::as_str)
        .is_some_and(|event_type| {
            event_type.contains('.')
                || ["response", "message", "item", "delta", "content_block"]
                    .iter()
                    .any(|nested| object.contains_key(*nested))
        })
}

fn is_error_like_sync_body(value: &Value) -> bool {
    let Some(object) = value.as_object() else {
        return false;
    };

    object.get("error").is_some_and(|error| !error.is_null())
        || object.get("status").and_then(Value::as_str) == Some("failed")
        || object
            .get("type")
            .and_then(Value::as_str)
            .is_some_and(|value| value == "error")
        || object
            .get("chunks")
            .and_then(Value::as_array)
            .is_some_and(|chunks| {
                chunks.iter().any(|chunk| {
                    chunk.as_object().is_some_and(|chunk_object| {
                        chunk_object
                            .get("error")
                            .is_some_and(|error| !error.is_null())
                            || chunk_object
                                .get("type")
                                .and_then(Value::as_str)
                                .is_some_and(|value| value == "error")
                    })
                })
            })
}

fn gemini_generate_content_body_has_visible_output(value: &Value) -> bool {
    crate::formats::gemini::generate_content::response::from_raw(value).is_some()
}

fn api_format_is_gemini_generate_content(api_format: &str) -> bool {
    aether_ai_formats::normalize_api_format_alias(api_format) == "gemini:generate_content"
}

pub fn maybe_build_standard_cross_format_sync_product(
    report_kind: &str,
    provider_api_format: &str,
    client_api_format: &str,
    report_context: &Value,
    provider_body_json: Value,
) -> Option<StandardCrossFormatSyncProduct> {
    let provider_api_format = provider_api_format.trim().to_ascii_lowercase();
    let client_api_format = client_api_format.trim().to_ascii_lowercase();

    if provider_api_format == "openai:image" && client_api_format == "gemini:generate_content" {
        let client_body_json = match (
            provider_body_json.get("data").and_then(Value::as_array),
            provider_body_json.get("output").and_then(Value::as_array),
        ) {
            (Some(_), None) => crate::formats::shared::image_bridge::build_gemini_image_response_from_openai_image_response(
                &provider_body_json,
                Some(report_context),
            )?,
            (None, Some(_)) => crate::formats::shared::image_bridge::build_gemini_image_response_from_openai_responses_image_response(
                &provider_body_json,
                Some(report_context),
            )?,
            _ => return None,
        };
        return Some(StandardCrossFormatSyncProduct {
            client_body_json,
            provider_body_json,
        });
    }

    let client_body_json = if is_standard_chat_finalize_kind(report_kind) {
        sync_chat_response_conversion_kind(&provider_api_format, &client_api_format)?;
        convert_standard_chat_response(
            &provider_body_json,
            &provider_api_format,
            &client_api_format,
            report_context,
        )?
    } else if is_standard_cli_finalize_kind(report_kind) {
        sync_cli_response_conversion_kind(&provider_api_format, &client_api_format)?;
        convert_standard_cli_response(
            &provider_body_json,
            &provider_api_format,
            &client_api_format,
            report_context,
        )?
    } else {
        return None;
    };
    let client_body_json =
        client_body_with_report_context_model(client_body_json, report_context, &client_api_format);

    Some(StandardCrossFormatSyncProduct {
        client_body_json,
        provider_body_json,
    })
}

pub fn aggregate_standard_chat_stream_sync_response(
    body: &[u8],
    provider_api_format: &str,
) -> Option<Value> {
    try_aggregate_standard_chat_stream_sync_response(body, provider_api_format)
        .ok()
        .flatten()
}

fn try_aggregate_standard_chat_stream_sync_response(
    body: &[u8],
    provider_api_format: &str,
) -> Result<Option<Value>, AiSurfaceFinalizeError> {
    match aether_ai_formats::normalize_api_format_alias(provider_api_format).as_str() {
        "openai:chat" => try_aggregate_openai_chat_stream_sync_response(body),
        "openai:responses" | "openai:responses:compact" => {
            try_aggregate_openai_responses_stream_sync_response(body)
        }
        "claude:messages" => try_aggregate_claude_stream_sync_response(body),
        "gemini:generate_content" => try_aggregate_gemini_stream_sync_response(body),
        _ => Ok(None),
    }
}

pub fn convert_standard_chat_response(
    body_json: &Value,
    provider_api_format: &str,
    client_api_format: &str,
    report_context: &Value,
) -> Option<Value> {
    match convert_response(
        provider_api_format,
        client_api_format,
        body_json,
        &format_context_from_report_context(report_context),
    ) {
        Ok(converted) => return Some(converted),
        Err(error) if response_conversion_error_requires_fail_closed(&error) => return None,
        Err(_) => {}
    }

    if matches!(
        provider_api_format.trim().to_ascii_lowercase().as_str(),
        "openai:chat"
    ) {
        return convert_openai_chat_canonical_chat_response(
            body_json,
            client_api_format,
            report_context,
        );
    }
    if matches!(
        provider_api_format.trim().to_ascii_lowercase().as_str(),
        "claude:messages"
    ) {
        return convert_claude_canonical_chat_response(
            body_json,
            client_api_format,
            report_context,
        );
    }
    if matches!(
        provider_api_format.trim().to_ascii_lowercase().as_str(),
        "gemini:generate_content"
    ) {
        return convert_gemini_canonical_chat_response(
            body_json,
            client_api_format,
            report_context,
        );
    }

    match normalize_openai_responses_family_api_format(provider_api_format).as_str() {
        "openai:responses" | "openai:responses:compact" => {
            convert_openai_responses_canonical_chat_response(
                body_json,
                client_api_format,
                report_context,
            )
        }
        _ => None,
    }
}

pub fn aggregate_standard_cli_stream_sync_response(
    body: &[u8],
    provider_api_format: &str,
) -> Option<Value> {
    aggregate_standard_chat_stream_sync_response(body, provider_api_format)
}

fn try_aggregate_standard_cli_stream_sync_response(
    body: &[u8],
    provider_api_format: &str,
) -> Result<Option<Value>, AiSurfaceFinalizeError> {
    try_aggregate_standard_chat_stream_sync_response(body, provider_api_format)
}

pub fn convert_standard_cli_response(
    body_json: &Value,
    provider_api_format: &str,
    client_api_format: &str,
    report_context: &Value,
) -> Option<Value> {
    match convert_response(
        provider_api_format,
        client_api_format,
        body_json,
        &format_context_from_report_context(report_context),
    ) {
        Ok(converted) => return Some(converted),
        Err(error) if response_conversion_error_requires_fail_closed(&error) => return None,
        Err(_) => {}
    }

    if matches!(
        provider_api_format.trim().to_ascii_lowercase().as_str(),
        "openai:chat"
    ) {
        return convert_openai_chat_canonical_cli_response(
            body_json,
            client_api_format,
            report_context,
        );
    }
    if matches!(
        provider_api_format.trim().to_ascii_lowercase().as_str(),
        "claude:messages"
    ) {
        return convert_claude_canonical_cli_response(body_json, client_api_format, report_context);
    }
    if matches!(
        provider_api_format.trim().to_ascii_lowercase().as_str(),
        "gemini:generate_content"
    ) {
        return convert_gemini_canonical_cli_response(body_json, client_api_format, report_context);
    }

    let canonical = match normalize_openai_responses_family_api_format(provider_api_format).as_str()
    {
        "openai:responses" | "openai:responses:compact" => {
            return convert_openai_responses_canonical_responses_response(
                body_json,
                client_api_format,
                report_context,
            )
        }
        _ => convert_standard_chat_response(
            body_json,
            provider_api_format,
            "openai:chat",
            report_context,
        )?,
    };

    match normalize_openai_responses_family_api_format(client_api_format).as_str() {
        "openai:responses" => {
            convert_openai_chat_response_to_openai_responses(&canonical, report_context, false)
        }
        "openai:responses:compact" => {
            convert_openai_chat_response_to_openai_responses(&canonical, report_context, true)
        }
        "claude:messages" => {
            convert_openai_chat_response_to_claude_chat(&canonical, report_context)
        }
        "gemini:generate_content" => {
            convert_openai_chat_response_to_gemini_chat(&canonical, report_context)
        }
        _ => None,
    }
}

fn response_conversion_error_requires_fail_closed(error: &FormatError) -> bool {
    matches!(
        error,
        FormatError::UnsupportedField { .. }
            | FormatError::UnauditedField { .. }
            | FormatError::InvalidEnumValue { .. }
            | FormatError::LossyConversionBlocked { .. }
            | FormatError::InvalidTargetField { .. }
    )
}

fn format_context_from_report_context(report_context: &Value) -> FormatContext {
    let mut context = FormatContext::default().with_report_context(report_context.clone());
    if let Some(mapped_model) = report_context
        .get("mapped_model")
        .and_then(Value::as_str)
        .or_else(|| report_context.get("model").and_then(Value::as_str))
        .filter(|value| !value.trim().is_empty())
    {
        context = context.with_mapped_model(mapped_model);
    }
    context
}

fn client_body_with_report_context_model(
    mut body_json: Value,
    report_context: &Value,
    client_api_format: &str,
) -> Value {
    let Some(display_model) = model_directive_display_model_from_report_context(report_context)
    else {
        return body_json;
    };
    let Some(object) = body_json.as_object_mut() else {
        return body_json;
    };
    match normalize_openai_responses_family_api_format(client_api_format).as_str() {
        "openai:chat" | "openai:responses" | "openai:responses:compact" | "claude:messages" => {
            object.insert("model".to_string(), Value::String(display_model));
        }
        "openai:embedding" | "jina:embedding" => {
            object.insert("model".to_string(), Value::String(display_model));
        }
        "gemini:generate_content" => {
            object.insert("modelVersion".to_string(), Value::String(display_model));
        }
        _ => {}
    }
    body_json
}

fn convert_openai_chat_canonical_chat_response(
    body_json: &Value,
    client_api_format: &str,
    report_context: &Value,
) -> Option<Value> {
    if !openai_chat_response_can_use_single_response_canonical(body_json) {
        return match client_api_format.trim().to_ascii_lowercase().as_str() {
            "openai:chat" => Some(body_json.clone()),
            "claude:messages" => {
                convert_openai_chat_response_to_claude_chat(body_json, report_context)
            }
            "gemini:generate_content" => {
                convert_openai_chat_response_to_gemini_chat(body_json, report_context)
            }
            _ => None,
        };
    }
    match client_api_format.trim().to_ascii_lowercase().as_str() {
        "openai:chat" => Some(body_json.clone()),
        "claude:messages" => {
            let openai_chat =
                convert_openai_chat_canonical_response_to_openai_chat(body_json, report_context)?;
            convert_openai_chat_response_to_claude_chat(&openai_chat, report_context)
        }
        "gemini:generate_content" => {
            let openai_chat =
                convert_openai_chat_canonical_response_to_openai_chat(body_json, report_context)?;
            convert_openai_chat_response_to_gemini_chat(&openai_chat, report_context)
        }
        _ => None,
    }
}

fn convert_openai_chat_canonical_cli_response(
    body_json: &Value,
    client_api_format: &str,
    report_context: &Value,
) -> Option<Value> {
    if !openai_chat_response_can_use_single_response_canonical(body_json) {
        return match normalize_openai_responses_family_api_format(client_api_format).as_str() {
            "openai:responses" => {
                convert_openai_chat_response_to_openai_responses(body_json, report_context, false)
            }
            "openai:responses:compact" => {
                convert_openai_chat_response_to_openai_responses(body_json, report_context, true)
            }
            "claude:messages" => {
                convert_openai_chat_response_to_claude_chat(body_json, report_context)
            }
            "gemini:generate_content" => {
                convert_openai_chat_response_to_gemini_chat(body_json, report_context)
            }
            _ => None,
        };
    }
    let openai_chat =
        convert_openai_chat_canonical_response_to_openai_chat(body_json, report_context)?;
    match normalize_openai_responses_family_api_format(client_api_format).as_str() {
        "openai:responses" => {
            convert_openai_chat_response_to_openai_responses(&openai_chat, report_context, false)
        }
        "openai:responses:compact" => {
            convert_openai_chat_response_to_openai_responses(&openai_chat, report_context, true)
        }
        "claude:messages" => {
            convert_openai_chat_response_to_claude_chat(&openai_chat, report_context)
        }
        "gemini:generate_content" => {
            convert_openai_chat_response_to_gemini_chat(&openai_chat, report_context)
        }
        _ => None,
    }
}

fn convert_openai_chat_canonical_response_to_openai_chat(
    body_json: &Value,
    report_context: &Value,
) -> Option<Value> {
    let mut canonical = from_openai_chat_to_canonical_response(body_json)?;
    apply_report_context_model_fallback(&mut canonical.model, report_context);
    Some(canonical_to_openai_chat_response(&canonical))
}

fn project_validated_openai_responses_stream_to_openai_chat(
    body_json: &Value,
    report_context: &Value,
) -> Option<Value> {
    project_validated_openai_responses_stream_to_client(body_json, "openai:chat", report_context)
}

fn project_validated_openai_responses_stream_to_client(
    body_json: &Value,
    client_api_format: &str,
    report_context: &Value,
) -> Option<Value> {
    // The caller retains body_json as provider_body_json. This projection is therefore allowed
    // to omit provider-only response metadata, but never unknown canonical output blocks.
    if !matches!(
        body_json.get("status").and_then(Value::as_str),
        Some("completed" | "incomplete")
    ) {
        return None;
    }

    let mut canonical = from_openai_responses_to_canonical_response(body_json)?;
    if canonical_response_unknown_block_count(&canonical) > 0 {
        return None;
    }

    apply_report_context_model_fallback(&mut canonical.model, report_context);
    match client_api_format {
        "openai:chat" => Some(canonical_to_openai_chat_response(&canonical)),
        "claude:messages" => Some(canonical_to_claude_response(&canonical)),
        "gemini:generate_content" => canonical_to_gemini_response(&canonical, report_context),
        _ => None,
    }
}

fn openai_chat_response_can_use_single_response_canonical(body_json: &Value) -> bool {
    body_json
        .get("choices")
        .and_then(Value::as_array)
        .is_some_and(|choices| choices.len() <= 1)
}

fn convert_openai_responses_canonical_chat_response(
    body_json: &Value,
    client_api_format: &str,
    report_context: &Value,
) -> Option<Value> {
    let canonical = from_openai_responses_to_canonical_response(body_json).map(|mut canonical| {
        apply_report_context_model_fallback(&mut canonical.model, report_context);
        canonical
    });
    match client_api_format.trim().to_ascii_lowercase().as_str() {
        "openai:chat" => canonical
            .as_ref()
            .map(canonical_to_openai_chat_response)
            .or_else(|| {
                convert_openai_responses_response_to_openai_chat(body_json, report_context)
            }),
        "claude:messages" => canonical
            .as_ref()
            .map(canonical_to_claude_response)
            .or_else(|| {
                let openai_chat =
                    convert_openai_responses_response_to_openai_chat(body_json, report_context)?;
                convert_openai_chat_response_to_claude_chat(&openai_chat, report_context)
            }),
        "gemini:generate_content" => canonical
            .as_ref()
            .and_then(|canonical| canonical_to_gemini_response(canonical, report_context))
            .or_else(|| {
                let openai_chat =
                    convert_openai_responses_response_to_openai_chat(body_json, report_context)?;
                convert_openai_chat_response_to_gemini_chat(&openai_chat, report_context)
            }),
        _ => None,
    }
}

fn convert_openai_responses_canonical_responses_response(
    body_json: &Value,
    client_api_format: &str,
    report_context: &Value,
) -> Option<Value> {
    match normalize_openai_responses_family_api_format(client_api_format).as_str() {
        "openai:responses" => convert_openai_responses_canonical_response_to_openai_responses(
            body_json,
            report_context,
            false,
        )
        .or_else(|| {
            let openai_chat =
                convert_openai_responses_response_to_openai_chat(body_json, report_context)?;
            convert_openai_chat_response_to_openai_responses(&openai_chat, report_context, false)
        }),
        "openai:responses:compact" => {
            convert_openai_responses_canonical_response_to_openai_responses(
                body_json,
                report_context,
                true,
            )
            .or_else(|| {
                let openai_chat =
                    convert_openai_responses_response_to_openai_chat(body_json, report_context)?;
                convert_openai_chat_response_to_openai_responses(&openai_chat, report_context, true)
            })
        }
        "claude:messages" => {
            let canonical =
                from_openai_responses_to_canonical_response(body_json).map(|mut canonical| {
                    apply_report_context_model_fallback(&mut canonical.model, report_context);
                    canonical
                });
            canonical
                .as_ref()
                .map(canonical_to_claude_response)
                .or_else(|| {
                    let openai_chat = convert_openai_responses_response_to_openai_chat(
                        body_json,
                        report_context,
                    )?;
                    convert_openai_chat_response_to_claude_chat(&openai_chat, report_context)
                })
        }
        "gemini:generate_content" => {
            let canonical =
                from_openai_responses_to_canonical_response(body_json).map(|mut canonical| {
                    apply_report_context_model_fallback(&mut canonical.model, report_context);
                    canonical
                });
            canonical
                .as_ref()
                .and_then(|canonical| canonical_to_gemini_response(canonical, report_context))
                .or_else(|| {
                    let openai_chat = convert_openai_responses_response_to_openai_chat(
                        body_json,
                        report_context,
                    )?;
                    convert_openai_chat_response_to_gemini_chat(&openai_chat, report_context)
                })
        }
        _ => None,
    }
}

fn convert_openai_responses_canonical_response_to_openai_responses(
    body_json: &Value,
    report_context: &Value,
    compact: bool,
) -> Option<Value> {
    let mut canonical = from_openai_responses_to_canonical_response(body_json)?;
    apply_report_context_model_fallback(&mut canonical.model, report_context);
    Some(if compact {
        canonical_to_openai_responses_compact_response(&canonical, report_context)
    } else {
        canonical_to_openai_responses_response(&canonical, report_context)
    })
}

fn convert_claude_canonical_chat_response(
    body_json: &Value,
    client_api_format: &str,
    report_context: &Value,
) -> Option<Value> {
    match client_api_format.trim().to_ascii_lowercase().as_str() {
        "openai:chat" => {
            convert_claude_canonical_response_to_openai_chat(body_json, report_context)
        }
        "claude:messages" => convert_claude_canonical_response_to_claude(body_json, report_context),
        "gemini:generate_content" => {
            let openai_chat =
                convert_claude_canonical_response_to_openai_chat(body_json, report_context)?;
            convert_openai_chat_response_to_gemini_chat(&openai_chat, report_context)
        }
        _ => None,
    }
}

fn convert_claude_canonical_cli_response(
    body_json: &Value,
    client_api_format: &str,
    report_context: &Value,
) -> Option<Value> {
    match normalize_openai_responses_family_api_format(client_api_format).as_str() {
        "openai:responses" => {
            let openai_chat =
                convert_claude_canonical_response_to_openai_chat(body_json, report_context)?;
            convert_openai_chat_response_to_openai_responses(&openai_chat, report_context, false)
        }
        "openai:responses:compact" => {
            let openai_chat =
                convert_claude_canonical_response_to_openai_chat(body_json, report_context)?;
            convert_openai_chat_response_to_openai_responses(&openai_chat, report_context, true)
        }
        "claude:messages" => convert_claude_canonical_response_to_claude(body_json, report_context),
        "gemini:generate_content" => {
            let openai_chat =
                convert_claude_canonical_response_to_openai_chat(body_json, report_context)?;
            convert_openai_chat_response_to_gemini_chat(&openai_chat, report_context)
        }
        _ => None,
    }
}

fn convert_claude_canonical_response_to_openai_chat(
    body_json: &Value,
    report_context: &Value,
) -> Option<Value> {
    let mut canonical = from_claude_to_canonical_response(body_json)?;
    apply_report_context_model_fallback(&mut canonical.model, report_context);
    let mut response = canonical_to_openai_chat_response(&canonical);
    if let Some(service_tier) = report_context
        .get("original_request_body")
        .and_then(Value::as_object)
        .and_then(|request| request.get("service_tier"))
        .cloned()
    {
        response["service_tier"] = service_tier;
    }
    Some(response)
}

fn convert_claude_canonical_response_to_claude(
    body_json: &Value,
    report_context: &Value,
) -> Option<Value> {
    let mut canonical = from_claude_to_canonical_response(body_json)?;
    apply_report_context_model_fallback(&mut canonical.model, report_context);
    Some(canonical_to_claude_response(&canonical))
}

fn convert_gemini_canonical_chat_response(
    body_json: &Value,
    client_api_format: &str,
    report_context: &Value,
) -> Option<Value> {
    if !gemini_response_can_use_single_response_canonical(body_json) {
        let openai_chat = convert_gemini_chat_response_to_openai_chat(body_json, report_context)?;
        return match client_api_format.trim().to_ascii_lowercase().as_str() {
            "openai:chat" => Some(openai_chat),
            "claude:messages" => {
                convert_openai_chat_response_to_claude_chat(&openai_chat, report_context)
            }
            "gemini:generate_content" => {
                convert_openai_chat_response_to_gemini_chat(&openai_chat, report_context)
            }
            _ => None,
        };
    }
    match client_api_format.trim().to_ascii_lowercase().as_str() {
        "openai:chat" => {
            convert_gemini_canonical_response_to_openai_chat(body_json, report_context)
        }
        "claude:messages" => {
            let openai_chat =
                convert_gemini_canonical_response_to_openai_chat(body_json, report_context)?;
            convert_openai_chat_response_to_claude_chat(&openai_chat, report_context)
        }
        "gemini:generate_content" => {
            convert_gemini_canonical_response_to_gemini(body_json, report_context)
        }
        _ => None,
    }
}

fn convert_gemini_canonical_cli_response(
    body_json: &Value,
    client_api_format: &str,
    report_context: &Value,
) -> Option<Value> {
    if !gemini_response_can_use_single_response_canonical(body_json) {
        let openai_chat = convert_gemini_chat_response_to_openai_chat(body_json, report_context)?;
        return match normalize_openai_responses_family_api_format(client_api_format).as_str() {
            "openai:responses" => convert_openai_chat_response_to_openai_responses(
                &openai_chat,
                report_context,
                false,
            ),
            "openai:responses:compact" => {
                convert_openai_chat_response_to_openai_responses(&openai_chat, report_context, true)
            }
            "claude:messages" => {
                convert_openai_chat_response_to_claude_chat(&openai_chat, report_context)
            }
            "gemini:generate_content" => {
                convert_openai_chat_response_to_gemini_chat(&openai_chat, report_context)
            }
            _ => None,
        };
    }
    match normalize_openai_responses_family_api_format(client_api_format).as_str() {
        "openai:responses" => {
            convert_gemini_canonical_response_to_openai_responses(body_json, report_context, false)
        }
        "openai:responses:compact" => {
            convert_gemini_canonical_response_to_openai_responses(body_json, report_context, true)
        }
        "claude:messages" => {
            let openai_chat =
                convert_gemini_canonical_response_to_openai_chat(body_json, report_context)?;
            convert_openai_chat_response_to_claude_chat(&openai_chat, report_context)
        }
        "gemini:generate_content" => {
            convert_gemini_canonical_response_to_gemini(body_json, report_context)
        }
        _ => None,
    }
}

fn convert_gemini_canonical_response_to_openai_chat(
    body_json: &Value,
    report_context: &Value,
) -> Option<Value> {
    if !gemini_response_can_use_single_response_canonical(body_json) {
        return convert_gemini_chat_response_to_openai_chat(body_json, report_context);
    }
    let mut canonical = from_gemini_to_canonical_response(body_json)?;
    apply_report_context_model_fallback(&mut canonical.model, report_context);
    Some(canonical_to_openai_chat_response(&canonical))
}

fn convert_gemini_canonical_response_to_openai_responses(
    body_json: &Value,
    report_context: &Value,
    compact: bool,
) -> Option<Value> {
    let openai_chat = convert_gemini_canonical_response_to_openai_chat(body_json, report_context)?;
    convert_openai_chat_response_to_openai_responses(&openai_chat, report_context, compact)
}

fn convert_gemini_canonical_response_to_gemini(
    body_json: &Value,
    report_context: &Value,
) -> Option<Value> {
    if !gemini_response_can_use_single_response_canonical(body_json) {
        let openai_chat = convert_gemini_chat_response_to_openai_chat(body_json, report_context)?;
        return convert_openai_chat_response_to_gemini_chat(&openai_chat, report_context);
    }
    let mut canonical = from_gemini_to_canonical_response(body_json)?;
    apply_report_context_model_fallback(&mut canonical.model, report_context);
    canonical_to_gemini_response(&canonical, report_context)
}

fn gemini_response_can_use_single_response_canonical(body_json: &Value) -> bool {
    body_json
        .get("candidates")
        .and_then(Value::as_array)
        .is_some_and(|candidates| candidates.len() <= 1)
}

fn apply_report_context_model_fallback(model: &mut String, report_context: &Value) {
    if let Some(display_model) = model_directive_display_model_from_report_context(report_context) {
        *model = display_model;
        return;
    }
    if model != "unknown" && !model.trim().is_empty() {
        return;
    }
    if let Some(fallback) = report_context
        .get("mapped_model")
        .and_then(Value::as_str)
        .or_else(|| report_context.get("model").and_then(Value::as_str))
        .filter(|value| !value.trim().is_empty())
    {
        *model = fallback.to_string();
    }
}

#[derive(Debug, Default)]
struct OpenAIChatChoiceState {
    role: Option<String>,
    content: String,
    reasoning: String,
    finish_reason: Option<String>,
    tool_calls: BTreeMap<usize, OpenAIChatToolCallState>,
}

#[derive(Debug, Default)]
struct OpenAIChatToolCallState {
    id: Option<String>,
    tool_type: Option<String>,
    function_name: Option<String>,
    function_arguments: String,
}

#[derive(Debug, Default)]
struct ClaudeContentBlockState {
    object: Map<String, Value>,
    text: String,
    signature: Option<String>,
    partial_json: String,
}

fn is_standard_chat_finalize_kind(report_kind: &str) -> bool {
    matches!(
        report_kind,
        "openai_chat_sync_finalize" | "claude_chat_sync_finalize" | "gemini_chat_sync_finalize"
    )
}

fn is_standard_cli_finalize_kind(report_kind: &str) -> bool {
    matches!(
        report_kind,
        "openai_responses_sync_finalize"
            | "openai_responses_compact_sync_finalize"
            | "openai_cli_sync_finalize"
            | "openai_compact_sync_finalize"
            | "claude_cli_sync_finalize"
            | "gemini_cli_sync_finalize"
    )
}

fn is_openai_responses_finalize_kind(report_kind: &str) -> bool {
    matches!(
        report_kind,
        "openai_responses_sync_finalize"
            | "openai_responses_compact_sync_finalize"
            | "openai_cli_sync_finalize"
            | "openai_compact_sync_finalize"
    )
}

fn standard_same_format_api_format(report_kind: &str) -> Option<&'static str> {
    match report_kind {
        "openai_memories_sync_finalize" => Some("openai:responses"),
        "openai_chat_sync_finalize" => Some("openai:chat"),
        "claude_chat_sync_finalize" => Some("claude:messages"),
        "gemini_chat_sync_finalize" => Some("gemini:generate_content"),
        "openai_embedding_sync_finalize" => Some("openai:embedding"),
        "claude_cli_sync_finalize" => Some("claude:messages"),
        "gemini_cli_sync_finalize" => Some("gemini:generate_content"),
        _ => None,
    }
}

fn provider_stream_event_api_format_for_report_context(
    report_context: &Value,
    provider_api_format: &str,
) -> String {
    report_context_api_format_field(report_context, "provider_stream_event_api_format")
        .or_else(|| report_context_api_format_field(report_context, "provider_stream_api_format"))
        .unwrap_or_else(|| provider_api_format.trim().to_ascii_lowercase())
}

fn report_context_api_format_field(report_context: &Value, key: &str) -> Option<String> {
    let value = report_context.get(key)?.as_str()?.trim();
    (!value.is_empty()).then(|| value.to_ascii_lowercase())
}

fn api_formats_match(left: &str, right: &str) -> bool {
    aether_ai_formats::normalize_api_format_alias(left)
        == aether_ai_formats::normalize_api_format_alias(right)
}

fn convert_aggregated_stream_body_to_client_sync_response(
    report_kind: &str,
    body: Value,
    provider_stream_event_api_format: &str,
    client_api_format: &str,
    report_context: &Value,
) -> Option<Value> {
    if api_formats_match(provider_stream_event_api_format, client_api_format) {
        return Some(body);
    }
    if is_standard_chat_finalize_kind(report_kind) {
        return convert_standard_chat_response(
            &body,
            provider_stream_event_api_format,
            client_api_format,
            report_context,
        );
    }
    if is_standard_cli_finalize_kind(report_kind) {
        return convert_standard_cli_response(
            &body,
            provider_stream_event_api_format,
            client_api_format,
            report_context,
        );
    }
    None
}

fn is_openai_responses_family_api_format(api_format: &str) -> bool {
    matches!(
        normalize_openai_responses_family_api_format(api_format).as_str(),
        "openai:responses" | "openai:responses:compact"
    )
}

fn normalize_openai_responses_family_api_format(api_format: &str) -> String {
    aether_ai_formats::normalize_api_format_alias(api_format)
}

fn parse_stream_json_events(body: &[u8]) -> Option<Vec<Value>> {
    let text = std::str::from_utf8(body).ok()?;
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return Some(Vec::new());
    }

    if trimmed.starts_with('[') {
        let array_value: Value = serde_json::from_str(trimmed).ok()?;
        let array = array_value.as_array()?;
        return Some(
            array
                .iter()
                .filter(|value| value.is_object())
                .cloned()
                .collect(),
        );
    }

    let mut events = Vec::new();
    let mut current_event_type: Option<String> = None;

    for raw_line in text.lines() {
        let line = raw_line.trim_matches('\r').trim();
        if line.is_empty() || line.starts_with(':') {
            continue;
        }
        if let Some(event_name) = line.strip_prefix("event:") {
            current_event_type = Some(event_name.trim().to_string());
            continue;
        }
        let data_line = if let Some(rest) = line.strip_prefix("data:") {
            rest.trim()
        } else {
            line
        };
        if data_line.is_empty() || data_line == "[DONE]" {
            continue;
        }

        let mut event: Value = serde_json::from_str(data_line).ok()?;
        if let Some(event_object) = event.as_object_mut() {
            if !event_object.contains_key("type") {
                if let Some(event_name) = current_event_type.take() {
                    event_object.insert("type".to_string(), Value::String(event_name));
                }
            }
        }
        events.push(event);
        current_event_type = None;
    }

    Some(events)
}

fn try_aggregate_openai_chat_stream_sync_response(
    body: &[u8],
) -> Result<Option<Value>, AiSurfaceFinalizeError> {
    let report_context = Value::Object(Map::new());
    let mut provider = OpenAIChatProviderState::default();
    ensure_no_unknown_provider_stream_events(body, |line| {
        provider.push_line(&report_context, line)
    })?;
    Ok(aggregate_openai_chat_stream_sync_response(body))
}

fn try_aggregate_openai_responses_stream_sync_response(
    body: &[u8],
) -> Result<Option<Value>, AiSurfaceFinalizeError> {
    ensure_no_unknown_openai_responses_stream_events(body, false)?;
    let terminal_response = validated_terminal_openai_responses_stream_response(body)?;
    Ok(
        aggregate_openai_responses_stream_sync_response_from_validated_terminal(
            body,
            terminal_response,
        ),
    )
}

fn ensure_no_unknown_openai_responses_stream_events(
    body: &[u8],
    allow_authoritative_terminal_output_extensions: bool,
) -> Result<(), AiSurfaceFinalizeError> {
    let Ok(text) = std::str::from_utf8(body) else {
        return Ok(());
    };
    let report_context = Value::Object(Map::new());
    let mut provider = OpenAIResponsesProviderState::default();
    let mut declared_event_type: Option<String> = None;
    for raw_line in text.lines() {
        let frames = provider.push_line(&report_context, raw_line.as_bytes().to_vec())?;
        let raw_payload = raw_line.trim();
        if let Some(event_type) = raw_payload.strip_prefix("event:") {
            declared_event_type = Some(event_type.trim().to_string());
            continue;
        }
        let payload = raw_payload
            .strip_prefix("data:")
            .map(str::trim)
            .unwrap_or(raw_payload);
        let declared_event_type = declared_event_type.take();
        let payload_event_type = serde_json::from_str::<Value>(payload)
            .ok()
            .and_then(|payload| {
                payload
                    .get("type")
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned)
            });
        if matches!(
            (declared_event_type.as_deref(), payload_event_type.as_deref()),
            (Some(declared), Some(payload)) if declared != payload
        ) {
            return Err(AiSurfaceFinalizeError::new(
                "OpenAI Responses SSE event name conflicts with payload.type",
            ));
        }
        let event_type = payload_event_type.or(declared_event_type);
        let terminal_output_extensions_are_lossless = allow_authoritative_terminal_output_extensions
            && event_type.as_deref().is_some_and(|event_type| {
                matches!(
                    event_type,
                    "response.completed"
                        | "response.done"
                        | "response.incomplete"
                        | "response.failed"
                )
            });
        if let Some(payload) = frames.iter().find_map(|frame| match &frame.event {
            CanonicalStreamEvent::UnknownEvent(payload)
                if payload.get("type").and_then(Value::as_str) != Some("response.failed")
                    && !terminal_output_extensions_are_lossless =>
            {
                Some(payload)
            }
            _ => None,
        }) {
            return Err(unsupported_stream_event_finalize_error(payload));
        }
    }
    Ok(())
}

fn try_aggregate_claude_stream_sync_response(
    body: &[u8],
) -> Result<Option<Value>, AiSurfaceFinalizeError> {
    let report_context = Value::Object(Map::new());
    let mut provider = ClaudeProviderState::default();
    ensure_no_unknown_provider_stream_events(body, |line| {
        provider.push_line(&report_context, line)
    })?;
    Ok(aggregate_claude_stream_sync_response(body))
}

fn ensure_no_unknown_provider_stream_events(
    body: &[u8],
    mut push_line: impl FnMut(Vec<u8>) -> Result<Vec<CanonicalStreamFrame>, AiSurfaceFinalizeError>,
) -> Result<(), AiSurfaceFinalizeError> {
    let Ok(text) = std::str::from_utf8(body) else {
        return Ok(());
    };
    for raw_line in text.lines() {
        let frames = push_line(raw_line.as_bytes().to_vec())?;
        if let Some(payload) = frames.iter().find_map(|frame| match &frame.event {
            CanonicalStreamEvent::UnknownEvent(payload) => Some(payload),
            _ => None,
        }) {
            return Err(unsupported_stream_event_finalize_error(payload));
        }
    }
    Ok(())
}

pub fn aggregate_openai_chat_stream_sync_response(body: &[u8]) -> Option<Value> {
    let text = std::str::from_utf8(body).ok()?;
    let mut response_id: Option<String> = None;
    let mut model: Option<String> = None;
    let mut created: Option<u64> = None;
    let mut usage: Option<Value> = None;
    let mut choices: BTreeMap<usize, OpenAIChatChoiceState> = BTreeMap::new();
    let mut saw_chunk = false;

    for raw_line in text.lines() {
        let line = raw_line.trim_matches('\r').trim();
        if line.is_empty() || line.starts_with(':') || line.starts_with("event:") {
            continue;
        }

        let Some(data_line) = line.strip_prefix("data:") else {
            continue;
        };
        let data_line = data_line.trim();
        if data_line.is_empty() || data_line == "[DONE]" {
            continue;
        }

        let chunk: Value = serde_json::from_str(data_line).ok()?;
        let chunk_object = chunk.as_object()?;
        saw_chunk = true;

        if response_id.is_none() {
            response_id = chunk_object
                .get("id")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned);
        }
        if model.is_none() {
            model = chunk_object
                .get("model")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned);
        }
        if created.is_none() {
            created = chunk_object.get("created").and_then(Value::as_u64);
        }
        if let Some(u) = chunk_object.get("usage") {
            usage = Some(u.clone());
        }

        let Some(chunk_choices) = chunk_object.get("choices").and_then(Value::as_array) else {
            continue;
        };
        for chunk_choice in chunk_choices {
            let Some(choice_object) = chunk_choice.as_object() else {
                continue;
            };
            let Some(index) = choice_object
                .get("index")
                .and_then(Value::as_u64)
                .map(|value| value as usize)
            else {
                continue;
            };
            let state = choices.entry(index).or_default();
            if let Some(finish_reason) = choice_object.get("finish_reason").and_then(Value::as_str)
            {
                state.finish_reason = Some(finish_reason.to_string());
            }

            let Some(delta) = choice_object.get("delta").and_then(Value::as_object) else {
                continue;
            };
            if let Some(role) = delta.get("role").and_then(Value::as_str) {
                state.role = Some(role.to_string());
            }
            if let Some(content) = delta.get("content").and_then(Value::as_str) {
                state.content.push_str(content);
            }
            for (_, piece) in openai_chat_reasoning_texts(delta) {
                state.reasoning.push_str(&piece);
            }
            if let Some(tool_calls) = delta.get("tool_calls").and_then(Value::as_array) {
                for tool_call in tool_calls {
                    let Some(tool_call_object) = tool_call.as_object() else {
                        continue;
                    };
                    let tool_index = tool_call_object
                        .get("index")
                        .and_then(Value::as_u64)
                        .map(|value| value as usize)
                        .unwrap_or(0);
                    let tool_state = state.tool_calls.entry(tool_index).or_default();
                    if let Some(id) = tool_call_object.get("id").and_then(Value::as_str) {
                        tool_state.id = Some(id.to_string());
                    }
                    if let Some(tool_type) = tool_call_object.get("type").and_then(Value::as_str) {
                        tool_state.tool_type = Some(tool_type.to_string());
                    }
                    if let Some(function) =
                        tool_call_object.get("function").and_then(Value::as_object)
                    {
                        if let Some(name) = function.get("name").and_then(Value::as_str) {
                            tool_state.function_name = Some(name.to_string());
                        }
                        if let Some(arguments) = function.get("arguments").and_then(Value::as_str) {
                            tool_state.function_arguments.push_str(arguments);
                        }
                    }
                }
            }
        }
    }

    if !saw_chunk {
        return None;
    }

    let mut response_object = Map::new();
    response_object.insert(
        "id".to_string(),
        Value::String(response_id.unwrap_or_else(|| "chatcmpl-local-finalize".to_string())),
    );
    response_object.insert(
        "object".to_string(),
        Value::String("chat.completion".to_string()),
    );
    if let Some(created) = created {
        response_object.insert("created".to_string(), Value::Number(created.into()));
    }
    if let Some(model) = model {
        response_object.insert("model".to_string(), Value::String(model));
    }

    let mut response_choices = Vec::with_capacity(choices.len());
    for (index, state) in choices {
        let mut message = Map::new();
        message.insert(
            "role".to_string(),
            Value::String(state.role.unwrap_or_else(|| "assistant".to_string())),
        );
        // Reassemble under the spelling this crate emits for Chat clients; the
        // provider's own spelling was already normalized away by the parser.
        if !state.reasoning.is_empty() {
            message.insert(
                "reasoning_content".to_string(),
                Value::String(state.reasoning),
            );
        }
        if state.tool_calls.is_empty() {
            message.insert("content".to_string(), Value::String(state.content));
        } else {
            if state.content.is_empty() {
                message.insert("content".to_string(), Value::Null);
            } else {
                message.insert("content".to_string(), Value::String(state.content));
            }
            let tool_calls = state
                .tool_calls
                .into_iter()
                .map(|(tool_index, tool_state)| {
                    json!({
                        "index": tool_index,
                        "id": tool_state.id,
                        "type": tool_state.tool_type.unwrap_or_else(|| "function".to_string()),
                        "function": {
                            "name": tool_state.function_name,
                            "arguments": tool_state.function_arguments,
                        },
                    })
                })
                .collect::<Vec<_>>();
            message.insert("tool_calls".to_string(), Value::Array(tool_calls));
        }

        response_choices.push(json!({
            "index": index,
            "message": Value::Object(message),
            "finish_reason": state.finish_reason,
        }));
    }
    response_object.insert("choices".to_string(), Value::Array(response_choices));
    if let Some(usage) = usage {
        response_object.insert("usage".to_string(), usage);
    }

    Some(Value::Object(response_object))
}

pub fn aggregate_openai_responses_stream_sync_response(body: &[u8]) -> Option<Value> {
    try_aggregate_openai_responses_stream_sync_response(body)
        .ok()
        .flatten()
}

fn aggregate_openai_responses_stream_sync_response_from_validated_terminal(
    body: &[u8],
    terminal_response: Value,
) -> Option<Value> {
    let events = parse_stream_json_events(body)?;
    if events.is_empty() {
        return None;
    }

    let mut response = terminal_response.as_object()?.clone();
    let mut response_id: Option<String> = None;
    let mut model: Option<String> = None;
    let mut message_states: BTreeMap<usize, OpenAIResponsesSyncMessageState> = BTreeMap::new();
    let mut reasoning_states: BTreeMap<usize, OpenAIResponsesSyncReasoningState> = BTreeMap::new();
    let mut tool_states: BTreeMap<usize, OpenAIResponsesSyncToolState> = BTreeMap::new();
    let mut image_items: BTreeMap<usize, Value> = BTreeMap::new();
    let mut opaque_items: BTreeMap<usize, Value> = BTreeMap::new();
    let mut item_output_indexes = BTreeMap::<String, usize>::new();
    let mut generic_output_indexes = BTreeMap::<String, usize>::new();
    let mut next_output_index = 0_usize;

    for event in events {
        let event_object = event.as_object()?;
        if let Some(response) = event_object.get("response").and_then(Value::as_object) {
            response_id = response_id.or_else(|| {
                response
                    .get("id")
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned)
            });
            model = model.or_else(|| {
                response
                    .get("model")
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned)
            });
        }

        match event_object
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or_default()
        {
            "response.created" | "response.in_progress" => {}
            "response.output_text.delta" | "response.outtext.delta" => {
                let output_index = openai_responses_event_output_index(event_object).unwrap_or(0);
                let content_index = openai_responses_event_content_index(event_object);
                match event_object.get("delta") {
                    Some(Value::String(delta)) => {
                        append_openai_responses_message_text_delta(
                            message_states.entry(output_index).or_default(),
                            content_index,
                            delta,
                        );
                    }
                    Some(Value::Object(delta)) => {
                        if let Some(text) = delta.get("text").and_then(Value::as_str) {
                            merge_openai_responses_message_text_delta_object(
                                message_states.entry(output_index).or_default(),
                                content_index,
                                text,
                            );
                        }
                    }
                    _ => {}
                }
            }
            "response.output_text.done" => {
                let output_index = openai_responses_event_output_index(event_object).unwrap_or(0);
                let content_index = openai_responses_event_content_index(event_object);
                let part = event_object.get("part").and_then(Value::as_object);
                let text = event_object
                    .get("text")
                    .and_then(Value::as_str)
                    .or_else(|| {
                        event_object
                            .get("part")
                            .and_then(Value::as_object)
                            .and_then(|part| part.get("text"))
                            .and_then(Value::as_str)
                    })
                    .unwrap_or_default();
                merge_openai_responses_message_text_part(
                    message_states.entry(output_index).or_default(),
                    content_index,
                    text,
                    part,
                );
            }
            "response.output_text.annotation.added" => {
                let output_index = openai_responses_event_output_index(event_object).unwrap_or(0);
                let content_index = openai_responses_event_content_index(event_object);
                merge_openai_responses_message_text_annotation(
                    message_states.entry(output_index).or_default(),
                    content_index,
                    event_object,
                );
            }
            "response.refusal.delta" => {
                let output_index = openai_responses_event_output_index(event_object).unwrap_or(0);
                let content_index = openai_responses_event_content_index(event_object);
                let delta = event_object
                    .get("delta")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                append_openai_responses_message_refusal_delta(
                    message_states.entry(output_index).or_default(),
                    content_index,
                    delta,
                );
            }
            "response.refusal.done" => {
                let output_index = openai_responses_event_output_index(event_object).unwrap_or(0);
                let content_index = openai_responses_event_content_index(event_object);
                let part = event_object.get("part").and_then(Value::as_object);
                let refusal = event_object
                    .get("refusal")
                    .and_then(Value::as_str)
                    .or_else(|| {
                        event_object
                            .get("part")
                            .and_then(Value::as_object)
                            .and_then(|part| part.get("refusal"))
                            .and_then(Value::as_str)
                    })
                    .unwrap_or_default();
                merge_openai_responses_message_refusal_part(
                    message_states.entry(output_index).or_default(),
                    content_index,
                    refusal,
                    part,
                );
            }
            "response.content_part.added" | "response.content_part.done" => {
                let Some(part) = event_object.get("part").and_then(Value::as_object) else {
                    continue;
                };
                let output_index = openai_responses_event_output_index(event_object).unwrap_or(0);
                let content_index = openai_responses_event_content_index(event_object);
                merge_openai_responses_message_part(
                    message_states.entry(output_index).or_default(),
                    content_index,
                    part,
                );
            }
            "response.reasoning_text.delta" | "response.reasoning_summary_text.delta" => {
                let output_index = openai_responses_event_output_index(event_object).unwrap_or(0);
                let delta = event_object
                    .get("delta")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                if delta.is_empty() {
                    continue;
                }
                reasoning_states
                    .entry(output_index)
                    .or_default()
                    .reasoning_text
                    .push_str(delta);
            }
            "response.reasoning_text.done" | "response.reasoning_summary_text.done" => {
                let output_index = openai_responses_event_output_index(event_object).unwrap_or(0);
                let text = event_object
                    .get("text")
                    .and_then(Value::as_str)
                    .or_else(|| {
                        event_object
                            .get("part")
                            .and_then(Value::as_object)
                            .and_then(|part| part.get("text"))
                            .and_then(Value::as_str)
                    })
                    .unwrap_or_default();
                merge_openai_responses_reasoning_text(
                    reasoning_states.entry(output_index).or_default(),
                    text,
                );
            }
            "response.reasoning_summary_part.added" | "response.reasoning_summary_part.done" => {
                let Some(part) = event_object.get("part").and_then(Value::as_object) else {
                    continue;
                };
                if part.get("type").and_then(Value::as_str) != Some("summary_text") {
                    continue;
                }
                let output_index = openai_responses_event_output_index(event_object).unwrap_or(0);
                let text = part.get("text").and_then(Value::as_str).unwrap_or_default();
                merge_openai_responses_reasoning_text(
                    reasoning_states.entry(output_index).or_default(),
                    text,
                );
            }
            "response.output_item.added" | "response.output_item.done" => {
                let Some(item) = event_object.get("item").and_then(Value::as_object) else {
                    continue;
                };
                let item_key = openai_responses_generic_output_item_key(item);
                let output_index =
                    if let Some(output_index) = openai_responses_event_output_index(event_object) {
                        next_output_index = next_output_index.max(output_index.saturating_add(1));
                        generic_output_indexes.insert(item_key, output_index);
                        output_index
                    } else if let Some(output_index) = generic_output_indexes.get(&item_key) {
                        *output_index
                    } else {
                        let output_index = next_output_index;
                        next_output_index = next_output_index.saturating_add(1);
                        generic_output_indexes.insert(item_key, output_index);
                        output_index
                    };
                match item.get("type").and_then(Value::as_str).unwrap_or_default() {
                    "message" => merge_openai_responses_message_item(
                        message_states.entry(output_index).or_default(),
                        item,
                    ),
                    "reasoning" => merge_openai_responses_reasoning_item(
                        reasoning_states.entry(output_index).or_default(),
                        item,
                    ),
                    "function_call" | "custom_tool_call" => {
                        merge_openai_responses_tool_item(
                            tool_states.entry(output_index).or_default(),
                            item,
                        );
                        register_openai_responses_tool_aliases(
                            &mut item_output_indexes,
                            output_index,
                            item,
                        );
                    }
                    "image_generation_call" => {
                        image_items.insert(output_index, Value::Object(item.clone()));
                    }
                    _ if event_object.get("type").and_then(Value::as_str)
                        == Some("response.output_item.done") =>
                    {
                        opaque_items.insert(output_index, Value::Object(item.clone()));
                    }
                    _ => {}
                }
            }
            "response.function_call_arguments.delta" | "response.custom_tool_call_input.delta" => {
                let Some(output_index) =
                    resolve_openai_responses_tool_output_index(event_object, &item_output_indexes)
                else {
                    continue;
                };
                let delta = event_object
                    .get("delta")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                if delta.is_empty() {
                    continue;
                }
                let state = tool_states.entry(output_index).or_default();
                if event_object
                    .get("type")
                    .and_then(Value::as_str)
                    .is_some_and(|value| value.starts_with("response.custom_tool_call_input."))
                {
                    state
                        .item
                        .entry("type".to_string())
                        .or_insert_with(|| Value::String("custom_tool_call".to_string()));
                    if let Some(name) = event_object.get("name").and_then(Value::as_str) {
                        state
                            .item
                            .entry("name".to_string())
                            .or_insert_with(|| Value::String(name.to_string()));
                    }
                    if let Some(item_id) = event_object.get("item_id").and_then(Value::as_str) {
                        state
                            .item
                            .entry("id".to_string())
                            .or_insert_with(|| Value::String(item_id.to_string()));
                    }
                    if let Some(call_id) = event_object.get("call_id").and_then(Value::as_str) {
                        state
                            .item
                            .entry("call_id".to_string())
                            .or_insert_with(|| Value::String(call_id.to_string()));
                    }
                }
                state.arguments.push_str(delta);
                register_openai_responses_tool_event_aliases(
                    &mut item_output_indexes,
                    event_object,
                    output_index,
                );
            }
            "response.function_call_arguments.done" | "response.custom_tool_call_input.done" => {
                let Some(output_index) =
                    resolve_openai_responses_tool_output_index(event_object, &item_output_indexes)
                else {
                    continue;
                };
                if let Some(item) = event_object.get("item").and_then(Value::as_object) {
                    merge_openai_responses_tool_item(
                        tool_states.entry(output_index).or_default(),
                        item,
                    );
                    register_openai_responses_tool_aliases(
                        &mut item_output_indexes,
                        output_index,
                        item,
                    );
                }
                if event_object
                    .get("type")
                    .and_then(Value::as_str)
                    .is_some_and(|value| value.starts_with("response.custom_tool_call_input."))
                {
                    let state = tool_states.entry(output_index).or_default();
                    state
                        .item
                        .entry("type".to_string())
                        .or_insert_with(|| Value::String("custom_tool_call".to_string()));
                    if let Some(name) = event_object.get("name").and_then(Value::as_str) {
                        state
                            .item
                            .entry("name".to_string())
                            .or_insert_with(|| Value::String(name.to_string()));
                    }
                    if let Some(item_id) = event_object.get("item_id").and_then(Value::as_str) {
                        state
                            .item
                            .entry("id".to_string())
                            .or_insert_with(|| Value::String(item_id.to_string()));
                    }
                    if let Some(call_id) = event_object.get("call_id").and_then(Value::as_str) {
                        state
                            .item
                            .entry("call_id".to_string())
                            .or_insert_with(|| Value::String(call_id.to_string()));
                    }
                }
                let arguments = event_object
                    .get("arguments")
                    .or_else(|| event_object.get("input"))
                    .and_then(Value::as_str)
                    .or_else(|| {
                        event_object
                            .get("item")
                            .and_then(Value::as_object)
                            .and_then(|item| item.get("arguments").or_else(|| item.get("input")))
                            .and_then(Value::as_str)
                    })
                    .unwrap_or_default();
                merge_openai_responses_tool_arguments(
                    tool_states.entry(output_index).or_default(),
                    arguments,
                );
                register_openai_responses_tool_event_aliases(
                    &mut item_output_indexes,
                    event_object,
                    output_index,
                );
            }
            "response.completed" | "response.done" | "response.incomplete" | "response.failed" => {
                let Some(response) = event_object.get("response").and_then(Value::as_object) else {
                    continue;
                };
                if let Some(output) = response.get("output").and_then(Value::as_array) {
                    for (output_index, item) in output.iter().enumerate() {
                        let Some(item) = item.as_object() else {
                            continue;
                        };
                        match item.get("type").and_then(Value::as_str).unwrap_or_default() {
                            "message" => merge_openai_responses_message_item(
                                message_states.entry(output_index).or_default(),
                                item,
                            ),
                            "reasoning" => merge_openai_responses_reasoning_item(
                                reasoning_states.entry(output_index).or_default(),
                                item,
                            ),
                            "function_call" | "custom_tool_call" => {
                                merge_openai_responses_tool_item(
                                    tool_states.entry(output_index).or_default(),
                                    item,
                                );
                                register_openai_responses_tool_aliases(
                                    &mut item_output_indexes,
                                    output_index,
                                    item,
                                );
                            }
                            "image_generation_call" => {
                                image_items.insert(output_index, Value::Object(item.clone()));
                            }
                            _ => {
                                opaque_items.insert(output_index, Value::Object(item.clone()));
                            }
                        }
                    }
                }
            }
            _ => {}
        }
    }

    response
        .entry("object".to_string())
        .or_insert_with(|| Value::String("response".to_string()));
    if let Some(response_id) = response_id.as_ref() {
        response
            .entry("id".to_string())
            .or_insert_with(|| Value::String(response_id.clone()));
    }
    if let Some(model) = model.as_ref() {
        response
            .entry("model".to_string())
            .or_insert_with(|| Value::String(model.clone()));
    }

    let response_id = response
        .get("id")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .or(response_id)
        .unwrap_or_else(|| "resp-local-stream".to_string());

    if response
        .get("output")
        .and_then(Value::as_array)
        .is_some_and(|output| !output.is_empty())
    {
        ensure_modern_openai_responses_response_fields(&mut response);
        return Some(Value::Object(response));
    }

    let mut output_indexes = message_states
        .keys()
        .chain(reasoning_states.keys())
        .chain(tool_states.keys())
        .chain(image_items.keys())
        .chain(opaque_items.keys())
        .copied()
        .collect::<Vec<_>>();
    output_indexes.sort_unstable();
    output_indexes.dedup();

    if !output_indexes.is_empty() {
        let mut output = Vec::with_capacity(output_indexes.len());
        for output_index in output_indexes {
            if let Some(state) = reasoning_states.remove(&output_index) {
                output.push(materialize_openai_responses_reasoning_item(
                    &response_id,
                    state,
                ));
            }
            if let Some(state) = message_states.remove(&output_index) {
                output.push(materialize_openai_responses_message_item(
                    &response_id,
                    output_index,
                    state,
                ));
            }
            if let Some(state) = tool_states.remove(&output_index) {
                output.push(materialize_openai_responses_tool_item(output_index, state));
            }
            if let Some(item) = image_items.remove(&output_index) {
                output.push(item);
            }
            if let Some(item) = opaque_items.remove(&output_index) {
                output.push(item);
            }
        }
        response.insert("output".to_string(), Value::Array(output));
    }

    ensure_modern_openai_responses_response_fields(&mut response);

    Some(Value::Object(response))
}

#[derive(Default)]
struct OpenAIResponsesSyncMessageState {
    item: Map<String, Value>,
    parts: BTreeMap<usize, Value>,
}

#[derive(Default)]
struct OpenAIResponsesSyncReasoningState {
    item: Map<String, Value>,
    reasoning_text: String,
}

#[derive(Default)]
struct OpenAIResponsesSyncToolState {
    item: Map<String, Value>,
    arguments: String,
}

fn openai_responses_generic_output_item_key(item: &Map<String, Value>) -> String {
    let item_type = item.get("type").and_then(Value::as_str).unwrap_or_default();
    if let Some(item_id) = item.get("id").and_then(Value::as_str) {
        return format!("{item_type}:id:{item_id}");
    }
    if let Some(encrypted_content) = item.get("encrypted_content").and_then(Value::as_str) {
        return format!("{item_type}:encrypted_content:{encrypted_content}");
    }
    format!(
        "{item_type}:{}",
        serde_json::to_string(item).unwrap_or_default()
    )
}

fn openai_responses_event_output_index(event: &Map<String, Value>) -> Option<usize> {
    event
        .get("output_index")
        .and_then(Value::as_u64)
        .map(|value| value as usize)
}

fn openai_responses_event_content_index(event: &Map<String, Value>) -> usize {
    event
        .get("content_index")
        .and_then(Value::as_u64)
        .map(|value| value as usize)
        .unwrap_or(0)
}

fn reconcile_openai_responses_authoritative_text(buffer: &mut String, authoritative: &str) {
    if authoritative.is_empty() {
        return;
    }
    if authoritative.starts_with(buffer.as_str()) {
        buffer.push_str(&authoritative[buffer.len()..]);
    } else if buffer != authoritative {
        *buffer = authoritative.to_string();
    }
}

fn default_openai_responses_output_text_part() -> Value {
    json!({
        "type": "output_text",
        "text": "",
        "annotations": [],
    })
}

fn default_openai_responses_refusal_part() -> Value {
    json!({
        "type": "refusal",
        "refusal": "",
    })
}

fn append_openai_responses_message_text_delta(
    state: &mut OpenAIResponsesSyncMessageState,
    content_index: usize,
    delta: &str,
) {
    if delta.is_empty() {
        return;
    }
    let part = state
        .parts
        .entry(content_index)
        .or_insert_with(default_openai_responses_output_text_part);
    let Some(part) = part.as_object_mut() else {
        return;
    };
    if !part
        .get("type")
        .and_then(Value::as_str)
        .is_some_and(|value| matches!(value, "output_text" | "text"))
    {
        return;
    }
    let current = part.get("text").and_then(Value::as_str).unwrap_or_default();
    let current = current.to_string();
    part.insert("type".to_string(), Value::String("output_text".to_string()));
    part.insert(
        "text".to_string(),
        Value::String(format!("{current}{delta}")),
    );
    part.entry("annotations".to_string())
        .or_insert_with(|| Value::Array(Vec::new()));
}

fn merge_openai_responses_message_text_delta_object(
    state: &mut OpenAIResponsesSyncMessageState,
    content_index: usize,
    text: &str,
) {
    if text.is_empty() {
        return;
    }
    let part = state
        .parts
        .entry(content_index)
        .or_insert_with(default_openai_responses_output_text_part);
    let Some(part) = part.as_object_mut() else {
        return;
    };
    if !part
        .get("type")
        .and_then(Value::as_str)
        .is_some_and(|value| matches!(value, "output_text" | "text"))
    {
        return;
    }
    let current = part
        .get("text")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let merged = if text.starts_with(current.as_str()) {
        text.to_string()
    } else if current == text || current.starts_with(text) {
        current
    } else {
        format!("{current}{text}")
    };
    part.insert("type".to_string(), Value::String("output_text".to_string()));
    part.insert("text".to_string(), Value::String(merged));
    part.entry("annotations".to_string())
        .or_insert_with(|| Value::Array(Vec::new()));
}

fn merge_openai_responses_message_text_part(
    state: &mut OpenAIResponsesSyncMessageState,
    content_index: usize,
    text: &str,
    template_part: Option<&Map<String, Value>>,
) {
    if text.is_empty() && template_part.is_none() {
        return;
    }
    let part = state.parts.entry(content_index).or_insert_with(|| {
        template_part
            .map(|part| Value::Object(part.clone()))
            .unwrap_or_else(default_openai_responses_output_text_part)
    });
    let Some(part) = part.as_object_mut() else {
        return;
    };
    if let Some(template_part) = template_part {
        for (key, value) in template_part {
            if key != "text" {
                part.insert(key.clone(), value.clone());
            }
        }
    }
    part.insert("type".to_string(), Value::String("output_text".to_string()));
    let mut current = part
        .get("text")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    reconcile_openai_responses_authoritative_text(&mut current, text);
    part.insert("text".to_string(), Value::String(current));
    part.entry("annotations".to_string())
        .or_insert_with(|| Value::Array(Vec::new()));
}

fn append_openai_responses_message_refusal_delta(
    state: &mut OpenAIResponsesSyncMessageState,
    content_index: usize,
    delta: &str,
) {
    if delta.is_empty() {
        return;
    }
    let part = state
        .parts
        .entry(content_index)
        .or_insert_with(default_openai_responses_refusal_part);
    let Some(part) = part.as_object_mut() else {
        return;
    };
    if part.get("type").and_then(Value::as_str) != Some("refusal") {
        return;
    }
    let current = part
        .get("refusal")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    part.insert("type".to_string(), Value::String("refusal".to_string()));
    part.insert(
        "refusal".to_string(),
        Value::String(format!("{current}{delta}")),
    );
}

fn merge_openai_responses_message_refusal_part(
    state: &mut OpenAIResponsesSyncMessageState,
    content_index: usize,
    refusal: &str,
    template_part: Option<&Map<String, Value>>,
) {
    if refusal.is_empty() && template_part.is_none() {
        return;
    }
    let part = state.parts.entry(content_index).or_insert_with(|| {
        template_part
            .map(|part| Value::Object(part.clone()))
            .unwrap_or_else(default_openai_responses_refusal_part)
    });
    let Some(part) = part.as_object_mut() else {
        return;
    };
    if let Some(template_part) = template_part {
        for (key, value) in template_part {
            if key != "refusal" {
                part.insert(key.clone(), value.clone());
            }
        }
    }
    part.insert("type".to_string(), Value::String("refusal".to_string()));
    let mut current = part
        .get("refusal")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    reconcile_openai_responses_authoritative_text(&mut current, refusal);
    part.insert("refusal".to_string(), Value::String(current));
}

fn merge_openai_responses_message_text_annotation(
    state: &mut OpenAIResponsesSyncMessageState,
    content_index: usize,
    event: &Map<String, Value>,
) {
    let Some(annotation) = event.get("annotation") else {
        return;
    };
    let annotation_index = event
        .get("annotation_index")
        .and_then(Value::as_u64)
        .map(|value| value as usize);
    let part = state
        .parts
        .entry(content_index)
        .or_insert_with(default_openai_responses_output_text_part);
    let Some(part) = part.as_object_mut() else {
        return;
    };
    if !part
        .get("type")
        .and_then(Value::as_str)
        .is_some_and(|value| matches!(value, "output_text" | "text"))
    {
        return;
    }
    part.insert("type".to_string(), Value::String("output_text".to_string()));
    part.entry("text".to_string())
        .or_insert_with(|| Value::String(String::new()));
    let annotations = part
        .entry("annotations".to_string())
        .or_insert_with(|| Value::Array(Vec::new()));
    let Some(annotations) = annotations.as_array_mut() else {
        return;
    };
    if let Some(annotation_index) = annotation_index {
        if annotations.len() <= annotation_index {
            annotations.resize(annotation_index + 1, Value::Null);
        }
        annotations[annotation_index] = annotation.clone();
    } else {
        annotations.push(annotation.clone());
    }
}

fn merge_openai_responses_message_part(
    state: &mut OpenAIResponsesSyncMessageState,
    content_index: usize,
    part: &Map<String, Value>,
) {
    if part
        .get("type")
        .and_then(Value::as_str)
        .is_some_and(|value| matches!(value, "output_text" | "text"))
    {
        let text = part.get("text").and_then(Value::as_str).unwrap_or_default();
        merge_openai_responses_message_text_part(state, content_index, text, Some(part));
    } else if part.get("type").and_then(Value::as_str) == Some("refusal") {
        let refusal = part
            .get("refusal")
            .and_then(Value::as_str)
            .unwrap_or_default();
        merge_openai_responses_message_refusal_part(state, content_index, refusal, Some(part));
    } else {
        state
            .parts
            .insert(content_index, Value::Object(part.clone()));
    }
}

fn merge_openai_responses_reasoning_text(
    state: &mut OpenAIResponsesSyncReasoningState,
    text: &str,
) {
    if text.is_empty() {
        return;
    }
    if state.reasoning_text.is_empty() || text.len() >= state.reasoning_text.len() {
        state.reasoning_text = text.to_string();
    }
}

fn merge_openai_responses_tool_arguments(
    state: &mut OpenAIResponsesSyncToolState,
    arguments: &str,
) {
    if arguments.is_empty() {
        return;
    }
    if state.arguments.is_empty() || arguments.len() >= state.arguments.len() {
        state.arguments = arguments.to_string();
    }
}

fn extract_openai_responses_reasoning_text(item: &Map<String, Value>) -> Option<String> {
    extract_openai_responses_reasoning_parts(item.get("content"), "reasoning_text")
        .or_else(|| extract_openai_responses_reasoning_parts(item.get("summary"), "summary_text"))
}

fn extract_openai_responses_reasoning_parts(
    raw: Option<&Value>,
    expected_type: &str,
) -> Option<String> {
    raw.and_then(Value::as_array)
        .into_iter()
        .flatten()
        .find_map(|part| {
            let part = part.as_object()?;
            (part.get("type").and_then(Value::as_str) == Some(expected_type)).then(|| {
                part.get("text")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string()
            })
        })
        .filter(|text| !text.is_empty())
}

fn merge_openai_responses_message_item(
    state: &mut OpenAIResponsesSyncMessageState,
    item: &Map<String, Value>,
) {
    if let Some(content) = item.get("content").and_then(Value::as_array) {
        for (content_index, part) in content.iter().enumerate() {
            if let Some(part) = part.as_object() {
                merge_openai_responses_message_part(state, content_index, part);
            }
        }
    }
    state.item = item.clone();
}

fn merge_openai_responses_reasoning_item(
    state: &mut OpenAIResponsesSyncReasoningState,
    item: &Map<String, Value>,
) {
    if let Some(text) = extract_openai_responses_reasoning_text(item) {
        merge_openai_responses_reasoning_text(state, text.as_str());
    }
    state.item = item.clone();
}

fn merge_openai_responses_tool_item(
    state: &mut OpenAIResponsesSyncToolState,
    item: &Map<String, Value>,
) {
    if let Some(arguments) = item.get("arguments").and_then(Value::as_str) {
        merge_openai_responses_tool_arguments(state, arguments);
    }
    state.item = item.clone();
}

fn register_openai_responses_tool_aliases(
    aliases: &mut BTreeMap<String, usize>,
    output_index: usize,
    item: &Map<String, Value>,
) {
    if let Some(id) = item.get("id").and_then(Value::as_str).map(str::trim) {
        if !id.is_empty() {
            aliases.insert(id.to_string(), output_index);
        }
    }
    if let Some(call_id) = item.get("call_id").and_then(Value::as_str).map(str::trim) {
        if !call_id.is_empty() {
            aliases.insert(call_id.to_string(), output_index);
        }
    }
}

fn register_openai_responses_tool_event_aliases(
    aliases: &mut BTreeMap<String, usize>,
    event: &Map<String, Value>,
    output_index: usize,
) {
    for key in ["item_id", "call_id", "id"] {
        if let Some(value) = event.get(key).and_then(Value::as_str).map(str::trim) {
            if !value.is_empty() {
                aliases.insert(value.to_string(), output_index);
            }
        }
    }
}

fn resolve_openai_responses_tool_output_index(
    event: &Map<String, Value>,
    aliases: &BTreeMap<String, usize>,
) -> Option<usize> {
    openai_responses_event_output_index(event).or_else(|| {
        ["item_id", "call_id", "id"]
            .iter()
            .find_map(|key| event.get(*key).and_then(Value::as_str))
            .and_then(|value| aliases.get(value).copied())
    })
}

fn materialize_openai_responses_message_item(
    response_id: &str,
    output_index: usize,
    state: OpenAIResponsesSyncMessageState,
) -> Value {
    let mut item = state.item;
    item.entry("type".to_string())
        .or_insert_with(|| Value::String("message".to_string()));
    let message_id_is_valid = item
        .get("id")
        .and_then(Value::as_str)
        .is_some_and(|id| id.starts_with("msg"));
    if !message_id_is_valid {
        let source_id = item
            .get("id")
            .and_then(Value::as_str)
            .filter(|id| !id.trim().is_empty())
            .unwrap_or(response_id)
            .to_string();
        item.insert(
            "id".to_string(),
            Value::String(openai_responses_message_item_id(
                source_id.as_str(),
                output_index,
            )),
        );
    }
    item.entry("role".to_string())
        .or_insert_with(|| Value::String("assistant".to_string()));
    item.entry("status".to_string())
        .or_insert_with(|| Value::String("completed".to_string()));

    let mut content = match item.remove("content") {
        Some(Value::Array(content)) => content,
        _ => Vec::new(),
    };
    for (content_index, part) in state.parts {
        if content_index < content.len() {
            content[content_index] = part;
        } else {
            content.push(part);
        }
    }
    item.insert("content".to_string(), Value::Array(content));
    Value::Object(item)
}

fn materialize_openai_responses_reasoning_item(
    response_id: &str,
    state: OpenAIResponsesSyncReasoningState,
) -> Value {
    let mut item = state.item;
    let has_provider_opaque_state = item
        .get("encrypted_content")
        .and_then(Value::as_str)
        .is_some_and(|value| !value.trim().is_empty());
    if has_provider_opaque_state {
        return Value::Object(item);
    }
    item.entry("type".to_string())
        .or_insert_with(|| Value::String("reasoning".to_string()));
    item.entry("id".to_string()).or_insert_with(|| {
        Value::String(openai_responses_synthetic_reasoning_item_id(response_id, 0))
    });
    item.entry("status".to_string())
        .or_insert_with(|| Value::String("completed".to_string()));
    if !state.reasoning_text.is_empty()
        && reasoning_item_field_missing_or_empty(item.get("content"))
    {
        let content = openai_responses_reasoning_text_parts([&state.reasoning_text]);
        item.insert("content".to_string(), content);
    }
    // Raw chain-of-thought lives on `content` only; never mirror it onto
    // `summary`, or clients that render both channels show it twice.
    item.entry("summary".to_string())
        .or_insert_with(|| Value::Array(Vec::new()));
    Value::Object(item)
}

fn reasoning_item_field_missing_or_empty(value: Option<&Value>) -> bool {
    match value {
        None | Some(Value::Null) => true,
        Some(Value::Array(parts)) => parts.is_empty(),
        Some(Value::String(text)) => text.trim().is_empty(),
        _ => false,
    }
}

fn materialize_openai_responses_tool_item(
    output_index: usize,
    state: OpenAIResponsesSyncToolState,
) -> Value {
    let mut item = state.item;
    let item_type = item
        .get("type")
        .and_then(Value::as_str)
        .filter(|value| *value == "custom_tool_call")
        .unwrap_or("function_call")
        .to_string();
    let generated_id = format!("call_auto_{output_index}");
    let call_id = item
        .get("call_id")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .or_else(|| {
            item.get("id")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(ToOwned::to_owned)
        })
        .unwrap_or(generated_id.clone());

    item.insert("type".to_string(), Value::String(item_type.clone()));
    item.entry("id".to_string())
        .or_insert_with(|| Value::String(call_id.clone()));
    item.insert("call_id".to_string(), Value::String(call_id));
    item.entry("name".to_string())
        .or_insert_with(|| Value::String("unknown".to_string()));
    item.entry("status".to_string())
        .or_insert_with(|| Value::String("completed".to_string()));
    if !state.arguments.is_empty() {
        let argument_key = if item_type == "custom_tool_call" {
            "input"
        } else {
            "arguments"
        };
        item.insert(argument_key.to_string(), Value::String(state.arguments));
    } else {
        let argument_key = if item_type == "custom_tool_call" {
            "input"
        } else {
            "arguments"
        };
        item.entry(argument_key.to_string())
            .or_insert_with(|| Value::String(String::new()));
    }
    Value::Object(item)
}

#[derive(Default)]
struct GeminiSyncToolState {
    call_id: String,
    name: String,
    arguments: String,
    thought_signature: String,
    part_index: Option<usize>,
}

pub fn aggregate_claude_stream_sync_response(body: &[u8]) -> Option<Value> {
    let events = parse_stream_json_events(body)?;
    if events.is_empty() {
        return None;
    }

    let mut message_object: Option<Map<String, Value>> = None;
    let mut content_blocks: BTreeMap<usize, ClaudeContentBlockState> = BTreeMap::new();
    let mut usage: Option<Value> = None;
    let mut saw_message_start = false;

    for event in events {
        let event_object = event.as_object()?;
        let event_type = event_object
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or_default();

        match event_type {
            "message_start" => {
                let mut message = event_object.get("message")?.as_object()?.clone();
                usage = message.remove("usage");
                message_object = Some(message);
                saw_message_start = true;
            }
            "content_block_start" => {
                let index = event_object
                    .get("index")
                    .and_then(Value::as_u64)
                    .map(|value| value as usize)
                    .unwrap_or(0);
                let object = event_object
                    .get("content_block")
                    .and_then(Value::as_object)
                    .cloned()
                    .unwrap_or_default();
                content_blocks.insert(
                    index,
                    ClaudeContentBlockState {
                        object,
                        ..Default::default()
                    },
                );
            }
            "content_block_delta" => {
                let index = event_object
                    .get("index")
                    .and_then(Value::as_u64)
                    .map(|value| value as usize)
                    .unwrap_or(0);
                let state = content_blocks.entry(index).or_default();
                let Some(delta) = event_object.get("delta").and_then(Value::as_object) else {
                    continue;
                };
                match delta
                    .get("type")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                {
                    "text_delta" => {
                        if let Some(text) = delta.get("text").and_then(Value::as_str) {
                            state.text.push_str(text);
                        }
                    }
                    "input_json_delta" => {
                        if let Some(partial_json) =
                            delta.get("partial_json").and_then(Value::as_str)
                        {
                            state.partial_json.push_str(partial_json);
                        }
                    }
                    "thinking_delta" => {
                        if let Some(thinking) = delta
                            .get("thinking")
                            .and_then(Value::as_str)
                            .or_else(|| delta.get("text").and_then(Value::as_str))
                        {
                            state.text.push_str(thinking);
                        }
                    }
                    "signature_delta" => {
                        if let Some(signature) = delta.get("signature").and_then(Value::as_str) {
                            if !signature.is_empty() {
                                state.signature = Some(signature.to_string());
                            }
                        }
                    }
                    _ => {}
                }
            }
            "message_delta" => {
                if let Some(message) = message_object.as_mut() {
                    if let Some(delta) = event_object.get("delta").and_then(Value::as_object) {
                        if let Some(stop_reason) = delta.get("stop_reason") {
                            message.insert("stop_reason".to_string(), stop_reason.clone());
                        }
                        if let Some(stop_sequence) = delta.get("stop_sequence") {
                            message.insert("stop_sequence".to_string(), stop_sequence.clone());
                        }
                    }
                }
                if let Some(delta_usage) = event_object.get("usage") {
                    usage = Some(delta_usage.clone());
                }
            }
            "message_stop" => {}
            _ => {}
        }
    }

    if !saw_message_start {
        return None;
    }

    let mut message = message_object?;
    let mut content = Vec::with_capacity(content_blocks.len());
    for (_index, state) in content_blocks {
        let mut block = state.object;
        let block_type = block
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or("text")
            .to_string();
        match block_type.as_str() {
            "text" => {
                block.insert(
                    "text".to_string(),
                    Value::String(if state.text.is_empty() {
                        block
                            .get("text")
                            .and_then(Value::as_str)
                            .unwrap_or_default()
                            .to_string()
                    } else {
                        state.text
                    }),
                );
            }
            "thinking" => {
                block.insert(
                    "thinking".to_string(),
                    Value::String(if state.text.is_empty() {
                        block
                            .get("thinking")
                            .and_then(Value::as_str)
                            .unwrap_or_default()
                            .to_string()
                    } else {
                        state.text
                    }),
                );
                if let Some(signature) = state.signature {
                    block.insert("signature".to_string(), Value::String(signature));
                }
            }
            "tool_use" => {
                if !state.partial_json.is_empty() {
                    let input = serde_json::from_str::<Value>(&state.partial_json)
                        .unwrap_or(Value::String(state.partial_json));
                    block.insert("input".to_string(), input);
                }
            }
            _ => {
                if !state.text.is_empty() {
                    block.insert("text".to_string(), Value::String(state.text));
                }
                if let Some(signature) = state.signature {
                    block.insert("signature".to_string(), Value::String(signature));
                }
            }
        }
        content.push(Value::Object(block));
    }
    message.insert("content".to_string(), Value::Array(content));
    if let Some(usage_value) = usage {
        message.insert("usage".to_string(), usage_value);
    }

    Some(Value::Object(message))
}

pub fn aggregate_gemini_stream_sync_response(body: &[u8]) -> Option<Value> {
    try_aggregate_gemini_stream_sync_response(body)
        .ok()
        .flatten()
}

fn try_aggregate_gemini_stream_sync_response(
    body: &[u8],
) -> Result<Option<Value>, AiSurfaceFinalizeError> {
    let Some(events) = parse_stream_json_events(body) else {
        return Ok(None);
    };
    if events.is_empty() {
        return Ok(None);
    }

    let report_context = Value::Object(Map::new());
    let mut provider = GeminiProviderState::default();
    let mut response_id: Option<Value> = None;
    let mut private_response_id: Option<Value> = None;
    let mut model_version: Option<Value> = None;
    let mut usage_metadata: Option<Value> = None;
    let mut prompt_feedback: Option<Value> = None;
    let mut candidate: Map<String, Value> = Map::new();
    let mut role: Option<Value> = None;
    let mut saw_candidate = false;
    let mut parts: Vec<Value> = Vec::new();
    let mut tool_states: BTreeMap<usize, GeminiSyncToolState> = BTreeMap::new();
    let mut finish_reason: Option<String> = None;
    let mut usage_from_frames: Option<CanonicalUsage> = None;

    for event in &events {
        let Some(raw_event_object) = event.as_object() else {
            return Ok(None);
        };
        if let Some(id) = raw_event_object.get("responseId") {
            response_id = Some(id.clone());
        }
        if let Some(id) = raw_event_object.get("_v1internal_response_id") {
            private_response_id = Some(id.clone());
        }
        let event_object = if let Some(response) = raw_event_object
            .get("response")
            .and_then(Value::as_object)
            .filter(|response| response.contains_key("candidates"))
        {
            response
        } else {
            raw_event_object
        };
        if let Some(id) = event_object.get("responseId") {
            response_id = Some(id.clone());
        }
        if let Some(id) = event_object.get("_v1internal_response_id") {
            private_response_id = Some(id.clone());
        }
        if let Some(version) = event_object.get("modelVersion") {
            model_version = Some(version.clone());
        }
        if let Some(usage) = event_object.get("usageMetadata") {
            usage_metadata = Some(usage.clone());
        }
        if let Some(prompt) = event_object.get("promptFeedback") {
            prompt_feedback = Some(prompt.clone());
        }
        let Some(event_candidates) = event_object.get("candidates").and_then(Value::as_array)
        else {
            continue;
        };
        for event_candidate in event_candidates {
            let Some(candidate_object) = event_candidate.as_object() else {
                continue;
            };
            for (key, value) in candidate_object {
                if key != "content" {
                    candidate.insert(key.clone(), value.clone());
                }
            }
            if let Some(content) = candidate_object.get("content").and_then(Value::as_object) {
                if let Some(content_role) = content.get("role") {
                    role = Some(content_role.clone());
                }
            }
            saw_candidate = true;
        }

        let line = format!("data: {event}\n").into_bytes();
        let frames = provider.push_line(&report_context, line)?;
        for frame in frames {
            if response_id.is_none() && !frame.id.is_empty() {
                response_id = Some(Value::String(frame.id.clone()));
            }
            if model_version.is_none() && !frame.model.is_empty() {
                model_version = Some(Value::String(frame.model.clone()));
            }
            match frame.event {
                CanonicalStreamEvent::Start => {}
                CanonicalStreamEvent::TextDelta(text) => {
                    append_gemini_text_part(&mut parts, text, false);
                }
                // This rebuilds a raw Gemini body, and every non-`content`
                // candidate key — `groundingMetadata` included — is already
                // copied across above. Projecting it into citations is the
                // job of whoever converts that body onward.
                CanonicalStreamEvent::Citations(_) => {}
                CanonicalStreamEvent::ReasoningDelta(text) => {
                    append_gemini_text_part(&mut parts, text, true);
                }
                CanonicalStreamEvent::ReasoningSignature(signature) => {
                    attach_gemini_reasoning_signature(&mut parts, signature);
                }
                CanonicalStreamEvent::ContentPart(part) => {
                    parts.push(gemini_sync_part_from_canonical_content_part(part));
                }
                CanonicalStreamEvent::ImageGenerationCall { item, .. } => {
                    if let Some(part) = content_part_from_openai_image_generation_item(&item) {
                        parts.push(gemini_sync_part_from_canonical_content_part(part));
                    }
                }
                CanonicalStreamEvent::OpenAiResponsesOutputItem { raw_event, .. } => {
                    return Err(unsupported_stream_event_finalize_error(&raw_event))
                }
                CanonicalStreamEvent::ToolCallStart {
                    index,
                    call_id,
                    name,
                } => {
                    let generated_call_id = if call_id.trim().is_empty() {
                        format!("call_auto_{index}")
                    } else {
                        call_id
                    };
                    let generated_name = if name.trim().is_empty() {
                        "unknown".to_string()
                    } else {
                        name
                    };
                    let state = tool_states.entry(index).or_default();
                    state.call_id = generated_call_id;
                    state.name = generated_name;
                    if state.part_index.is_none() {
                        let part_index = parts.len();
                        parts.push(sync_gemini_function_call_part(state));
                        state.part_index = Some(part_index);
                    } else if let Some(part_index) = state.part_index {
                        parts[part_index] = sync_gemini_function_call_part(state);
                    }
                }
                CanonicalStreamEvent::ToolCallSignature { index, signature } => {
                    let state = tool_states.entry(index).or_default();
                    state.thought_signature = signature;
                    if let Some(part_index) = state.part_index {
                        parts[part_index] = sync_gemini_function_call_part(state);
                    }
                }
                CanonicalStreamEvent::ToolCallArgumentsDelta { index, arguments } => {
                    let state = tool_states.entry(index).or_default();
                    state.arguments.push_str(&arguments);
                    let part_index = if let Some(part_index) = state.part_index {
                        part_index
                    } else {
                        let part_index = parts.len();
                        parts.push(sync_gemini_function_call_part(state));
                        state.part_index = Some(part_index);
                        part_index
                    };
                    parts[part_index] = sync_gemini_function_call_part(state);
                }
                CanonicalStreamEvent::ToolResultDelta {
                    tool_use_id,
                    name,
                    content,
                    ..
                } => {
                    parts.push(sync_gemini_function_response_part(
                        tool_use_id,
                        name,
                        content,
                    ));
                }
                CanonicalStreamEvent::UnknownEvent(payload) => {
                    return Err(unsupported_stream_event_finalize_error(&payload))
                }
                CanonicalStreamEvent::ReasoningSummaryDone => {}
                CanonicalStreamEvent::Finish {
                    finish_reason: frame_finish_reason,
                    usage,
                } => {
                    finish_reason = frame_finish_reason
                        .map(|value| {
                            map_openai_finish_reason_to_gemini(Some(value.as_str())).to_string()
                        })
                        .or(finish_reason);
                    if usage.is_some() {
                        usage_from_frames = usage;
                    }
                }
            }
        }
    }

    let frames = provider.finish(&report_context)?;
    for frame in frames {
        if response_id.is_none() && !frame.id.is_empty() {
            response_id = Some(Value::String(frame.id.clone()));
        }
        if model_version.is_none() && !frame.model.is_empty() {
            model_version = Some(Value::String(frame.model.clone()));
        }
        match frame.event {
            CanonicalStreamEvent::UnknownEvent(payload) => {
                return Err(unsupported_stream_event_finalize_error(&payload))
            }
            CanonicalStreamEvent::Finish {
                finish_reason: frame_finish_reason,
                usage,
            } => {
                finish_reason = frame_finish_reason
                    .map(|value| {
                        map_openai_finish_reason_to_gemini(Some(value.as_str())).to_string()
                    })
                    .or(finish_reason);
                if usage.is_some() {
                    usage_from_frames = usage;
                }
            }
            _ => {}
        }
    }

    if !saw_candidate {
        return Ok(None);
    }

    candidate.insert(
        "content".to_string(),
        json!({
            "role": role.unwrap_or_else(|| Value::String("model".to_string())),
            "parts": parts,
        }),
    );
    candidate
        .entry("index".to_string())
        .or_insert_with(|| Value::from(0_u64));
    if let Some(finish_reason) = finish_reason {
        candidate.insert("finishReason".to_string(), Value::String(finish_reason));
    }

    let mut response = Map::new();
    if let Some(response_id) = response_id {
        response.insert("responseId".to_string(), response_id);
    }
    if let Some(private_response_id) = private_response_id {
        response.insert("_v1internal_response_id".to_string(), private_response_id);
    }
    response.insert(
        "candidates".to_string(),
        Value::Array(vec![Value::Object(candidate)]),
    );
    if let Some(version) = model_version {
        response.insert("modelVersion".to_string(), version);
    }
    if usage_metadata.is_none() {
        usage_metadata = usage_from_frames.map(gemini_usage_metadata_from_canonical);
    }
    if let Some(usage) = usage_metadata {
        response.insert("usageMetadata".to_string(), usage);
    }
    if let Some(prompt) = prompt_feedback {
        response.insert("promptFeedback".to_string(), prompt);
    }
    Ok(Some(Value::Object(response)))
}

fn unsupported_stream_event_finalize_error(payload: &Value) -> AiSurfaceFinalizeError {
    AiSurfaceFinalizeError::new(unsupported_stream_event_message(payload))
}

fn append_gemini_text_part(parts: &mut Vec<Value>, text: String, thought: bool) {
    if text.is_empty() {
        return;
    }
    let Some(existing) = parts
        .last_mut()
        .and_then(Value::as_object_mut)
        .filter(|part| is_mergeable_gemini_text_part(part, thought))
    else {
        let mut part = Map::new();
        part.insert("text".to_string(), Value::String(text));
        if thought {
            part.insert("thought".to_string(), Value::Bool(true));
        }
        parts.push(Value::Object(part));
        return;
    };
    let current = existing
        .get("text")
        .and_then(Value::as_str)
        .unwrap_or_default();
    existing.insert(
        "text".to_string(),
        Value::String(format!("{current}{text}")),
    );
}

fn attach_gemini_reasoning_signature(parts: &mut Vec<Value>, signature: String) {
    if signature.is_empty() {
        return;
    }
    for part in parts.iter_mut().rev() {
        let Some(part_object) = part.as_object_mut() else {
            continue;
        };
        if is_mergeable_gemini_text_part(part_object, true) {
            part_object.insert(
                "thoughtSignature".to_string(),
                Value::String(signature.clone()),
            );
            return;
        }
    }
    parts.push(json!({
        "text": "",
        "thought": true,
        "thoughtSignature": signature,
    }));
}

fn is_mergeable_gemini_text_part(part: &Map<String, Value>, thought: bool) -> bool {
    if !part.contains_key("text") {
        return false;
    }
    if part
        .get("thought")
        .and_then(Value::as_bool)
        .unwrap_or(false)
        != thought
    {
        return false;
    }
    part.keys().all(|key| {
        matches!(
            key.as_str(),
            "text" | "thought" | "thoughtSignature" | "thought_signature"
        )
    })
}

fn sync_gemini_function_call_part(state: &GeminiSyncToolState) -> Value {
    let mut part = json!({
        "functionCall": {
            "id": if state.call_id.trim().is_empty() {
                "call_auto_0".to_string()
            } else {
                state.call_id.clone()
            },
            "name": if state.name.trim().is_empty() {
                "unknown".to_string()
            } else {
                state.name.clone()
            },
            "args": sync_gemini_function_args_value(&state.arguments),
        }
    });
    if !state.thought_signature.is_empty() {
        part["thoughtSignature"] = Value::String(state.thought_signature.clone());
    }
    part
}

fn sync_gemini_function_response_part(
    tool_use_id: String,
    name: Option<String>,
    content: String,
) -> Value {
    json!({
        "functionResponse": {
            "id": if tool_use_id.trim().is_empty() {
                "call_auto_0".to_string()
            } else {
                tool_use_id
            },
            "name": name
                .filter(|value| !value.trim().is_empty())
                .unwrap_or_else(|| "unknown".to_string()),
            "response": sync_gemini_function_response_value(&content),
        }
    })
}

fn sync_gemini_function_response_value(content: &str) -> Value {
    if content.trim().is_empty() {
        return Value::Object(Map::new());
    }
    match serde_json::from_str::<Value>(content) {
        Ok(Value::Object(map)) => Value::Object(map),
        Ok(value) => json!({ "output": value }),
        Err(_) => json!({ "output": content }),
    }
}

fn sync_gemini_function_args_value(arguments: &str) -> Value {
    match parse_json_arguments_value(arguments) {
        Some(Value::Object(map)) => Value::Object(map),
        Some(value) => json!({ "raw": value }),
        None if arguments.trim().is_empty() => Value::Object(Map::new()),
        None => json!({ "raw": arguments }),
    }
}

fn gemini_sync_part_from_canonical_content_part(part: CanonicalContentPart) -> Value {
    match part {
        CanonicalContentPart::ImageUrl(url) => {
            if let Some((mime_type, data)) = parse_data_url(url.as_str()) {
                json!({
                    "inlineData": {
                        "mimeType": mime_type,
                        "data": data,
                    }
                })
            } else {
                json!({
                    "fileData": {
                        "fileUri": url.clone(),
                        "mimeType": guess_media_type_from_reference(url.as_str(), "image/jpeg"),
                    }
                })
            }
        }
        CanonicalContentPart::File {
            file_data,
            reference,
            mime_type,
            ..
        } => {
            if let Some(file_data) = file_data {
                if let Some((mime_type, data)) = parse_data_url(file_data.as_str()) {
                    json!({
                        "inlineData": {
                            "mimeType": mime_type,
                            "data": data,
                        }
                    })
                } else {
                    json!({ "text": "[File]" })
                }
            } else if let Some(reference) = reference {
                json!({
                    "fileData": {
                        "fileUri": reference.clone(),
                        "mimeType": mime_type.unwrap_or_else(|| {
                            guess_media_type_from_reference(reference.as_str(), "application/octet-stream")
                        }),
                    }
                })
            } else {
                json!({ "text": "[File]" })
            }
        }
        CanonicalContentPart::Audio { data, format } => json!({
            "inlineData": {
                "mimeType": format!("audio/{format}"),
                "data": data,
            }
        }),
    }
}

fn gemini_usage_metadata_from_canonical(usage: CanonicalUsage) -> Value {
    gemini_usage_metadata_from_usage(&usage)
}

fn parse_data_url(value: &str) -> Option<(String, String)> {
    let rest = value.strip_prefix("data:")?;
    let (meta, data) = rest.split_once(',')?;
    let mime_type = meta.strip_suffix(";base64")?;
    if mime_type.trim().is_empty() || data.trim().is_empty() {
        return None;
    }
    Some((mime_type.to_string(), data.to_string()))
}

fn guess_media_type_from_reference(reference: &str, default_mime: &str) -> String {
    let normalized = reference
        .split('?')
        .next()
        .unwrap_or(reference)
        .to_ascii_lowercase();
    if normalized.ends_with(".png") {
        "image/png".to_string()
    } else if normalized.ends_with(".gif") {
        "image/gif".to_string()
    } else if normalized.ends_with(".webp") {
        "image/webp".to_string()
    } else if normalized.ends_with(".jpg") || normalized.ends_with(".jpeg") {
        "image/jpeg".to_string()
    } else if normalized.ends_with(".pdf") {
        "application/pdf".to_string()
    } else {
        default_mime.to_string()
    }
}

#[cfg(test)]
#[path = "sync_products/tests.rs"]
mod tests;
