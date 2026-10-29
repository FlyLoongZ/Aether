use std::borrow::Cow;
use std::collections::BTreeMap;

use serde_json::Value;

use crate::formats::shared::stream_core::common::openai_stream_terminal_error_body;
use crate::formats::shared::AiSurfaceFinalizeError;
use crate::provider_compat::kiro_stream::KiroToClaudeCliStreamState;

use super::surfaces::{
    provider_adaptation_allows_sync_finalize_envelope, provider_adaptation_descriptor_for_envelope,
    provider_adaptation_should_unwrap_stream_envelope, ANTIGRAVITY_V1INTERNAL_ENVELOPE_NAME,
    GEMINI_CLI_V1INTERNAL_ENVELOPE_NAME, KIRO_ENVELOPE_NAME,
};

pub fn provider_private_response_allows_sync_finalize(report_context: &Value) -> bool {
    let has_envelope = report_context
        .get("has_envelope")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    if !has_envelope {
        return true;
    }
    let envelope_name = report_context
        .get("envelope_name")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let provider_api_format = report_context
        .get("provider_api_format")
        .and_then(Value::as_str)
        .unwrap_or_default();
    provider_adaptation_allows_sync_finalize_envelope(envelope_name, provider_api_format)
}

pub fn normalize_provider_private_report_context(report_context: Option<&Value>) -> Option<Value> {
    let report_context = report_context?;
    if !report_context
        .get("has_envelope")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        return Some(report_context.clone());
    }
    let envelope_name = report_context
        .get("envelope_name")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let provider_api_format = report_context
        .get("provider_api_format")
        .and_then(Value::as_str)
        .unwrap_or_default();
    if report_context_preserves_private_client_envelope(
        report_context,
        envelope_name,
        provider_api_format,
    ) {
        return Some(report_context.clone());
    }
    if provider_adaptation_descriptor_for_envelope(envelope_name, provider_api_format).is_none() {
        return Some(report_context.clone());
    }
    Some(clear_private_envelope_context(report_context))
}

pub fn normalize_provider_private_response_value(
    data: Value,
    report_context: &Value,
) -> Option<Value> {
    if !report_context
        .get("has_envelope")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        return Some(data);
    }
    let envelope_name = report_context
        .get("envelope_name")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let provider_api_format = report_context
        .get("provider_api_format")
        .and_then(Value::as_str)
        .unwrap_or_default();
    if report_context_preserves_private_client_envelope(
        report_context,
        envelope_name,
        provider_api_format,
    ) {
        return Some(data);
    }

    let mut unwrapped = match report_context.get("envelope_name").and_then(Value::as_str) {
        Some(KIRO_ENVELOPE_NAME) => data,
        Some(GEMINI_CLI_V1INTERNAL_ENVELOPE_NAME) => {
            if let Some(response) = data
                .get("response")
                .and_then(Value::as_object)
                .filter(|response| !response.contains_key("response"))
            {
                Value::Object(response.clone())
            } else {
                data
            }
        }
        Some(ANTIGRAVITY_V1INTERNAL_ENVELOPE_NAME) => {
            normalize_antigravity_sync_response_value(data, provider_api_format)
        }
        _ => return None,
    };
    postprocess_private_response_value(&mut unwrapped, report_context);
    Some(unwrapped)
}

fn normalize_antigravity_sync_response_value(data: Value, provider_api_format: &str) -> Value {
    if crate::normalize_api_format_alias(provider_api_format) == "gemini:generate_content" {
        if let Some(aggregated) = aggregate_antigravity_gemini_chunks(&data) {
            return aggregated;
        }
    }
    if let Some(response) = data
        .get("response")
        .and_then(Value::as_object)
        .filter(|response| !response.contains_key("response"))
    {
        let mut unwrapped = response.clone();
        if let Some(response_id) = data.get("responseId").cloned() {
            unwrapped.insert("_v1internal_response_id".to_string(), response_id);
        }
        Value::Object(unwrapped)
    } else {
        data
    }
}

fn aggregate_antigravity_gemini_chunks(data: &Value) -> Option<Value> {
    let chunks = data.get("chunks").and_then(Value::as_array)?;
    if chunks.is_empty() {
        return None;
    }
    let body = serde_json::to_vec(chunks).ok()?;
    let mut aggregated =
        crate::formats::shared::sync_products::aggregate_gemini_stream_sync_response(&body)?;
    if let Some(response_id) = antigravity_chunks_response_id(chunks) {
        if let Some(object) = aggregated.as_object_mut() {
            if object.get("responseId").is_none_or(|value| value.is_null()) {
                object.insert("responseId".to_string(), response_id);
            }
        }
    }
    Some(aggregated)
}

fn antigravity_chunks_response_id(chunks: &[Value]) -> Option<Value> {
    chunks.iter().find_map(|chunk| {
        chunk
            .get("responseId")
            .or_else(|| {
                chunk
                    .get("response")
                    .and_then(|response| response.get("responseId"))
            })
            .cloned()
            .filter(|value| !value.is_null())
    })
}

pub fn transform_provider_private_stream_line(
    report_context: &Value,
    line: Vec<u8>,
) -> Result<Vec<u8>, serde_json::Error> {
    transform_provider_private_stream_line_with_event_state(report_context, line, &mut None)
}

fn transform_provider_private_stream_line_with_event_state(
    report_context: &Value,
    line: Vec<u8>,
    current_event_type: &mut Option<String>,
) -> Result<Vec<u8>, serde_json::Error> {
    let Ok(text) = std::str::from_utf8(&line) else {
        return Ok(line);
    };
    let trimmed = text.trim_matches('\r').trim();
    if trimmed.is_empty() || trimmed.starts_with(':') {
        return Ok(Vec::new());
    }
    if let Some(event_name) = trimmed.strip_prefix("event:") {
        let event_name = event_name.trim().to_string();
        let is_error = event_name.eq_ignore_ascii_case("error");
        *current_event_type = (!event_name.is_empty()).then_some(event_name);
        return if is_error { Ok(line) } else { Ok(Vec::new()) };
    }
    let Some(data_line) = trimmed.strip_prefix("data:") else {
        return Ok(line);
    };
    let data_line = data_line.trim();
    if data_line.is_empty() || data_line == "[DONE]" {
        return Ok(line);
    }

    let body: Value = match serde_json::from_str(data_line) {
        Ok(value) => value,
        Err(_) => return Ok(line),
    };
    let event_is_error = current_event_type
        .as_deref()
        .is_some_and(|event| event.eq_ignore_ascii_case("error"));
    *current_event_type = None;
    if event_is_error {
        return Ok(line);
    }

    let envelope_name = report_context
        .get("envelope_name")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let provider_api_format = report_context
        .get("provider_api_format")
        .and_then(Value::as_str)
        .unwrap_or_default();
    if !provider_adaptation_should_unwrap_stream_envelope(envelope_name, provider_api_format) {
        return Ok(line);
    }
    if report_context_preserves_private_client_envelope(
        report_context,
        envelope_name,
        provider_api_format,
    ) {
        return Ok(line);
    }
    let unwrapped = match envelope_name {
        GEMINI_CLI_V1INTERNAL_ENVELOPE_NAME => body.get("response").cloned().unwrap_or(body),
        ANTIGRAVITY_V1INTERNAL_ENVELOPE_NAME => {
            let mut response = body.get("response").cloned().unwrap_or(body.clone());
            if let Some(response_id) = body.get("responseId").cloned() {
                if let Some(object) = response.as_object_mut() {
                    object
                        .entry("_v1internal_response_id".to_string())
                        .or_insert(response_id);
                }
            }
            inject_antigravity_stream_tool_ids(&mut response);
            response
        }
        _ => body,
    };

    let mut out = b"data: ".to_vec();
    out.extend(serde_json::to_vec(&unwrapped)?);
    out.extend_from_slice(b"\n\n");
    Ok(out)
}

// Keep malformed or incomplete provider streams from growing this parser's
// carry buffer without bound when no complete SSE record arrives.
const MAX_PRIVATE_STREAM_BUFFER_BYTES: usize = 16 * 1024 * 1024 + 5;

enum ProviderPrivateStreamNormalizeMode {
    EnvelopeUnwrap,
    KiroToClaudeCli(Box<KiroToClaudeCliStreamState>),
}

pub struct ProviderPrivateStreamNormalizer<'a> {
    report_context: Cow<'a, Value>,
    buffered: Vec<u8>,
    current_event_type: Option<String>,
    mode: ProviderPrivateStreamNormalizeMode,
}

pub fn maybe_build_provider_private_stream_normalizer<'a>(
    report_context: Option<&'a Value>,
) -> Option<ProviderPrivateStreamNormalizer<'a>> {
    let report_context = report_context?;
    if !report_context
        .get("has_envelope")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        return None;
    }
    let envelope_name = report_context
        .get("envelope_name")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let provider_api_format = report_context
        .get("provider_api_format")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let descriptor =
        provider_adaptation_descriptor_for_envelope(envelope_name, provider_api_format)?;
    if report_context_preserves_private_client_envelope(
        report_context,
        envelope_name,
        provider_api_format,
    ) {
        return None;
    }
    let mode = if descriptor
        .envelope_name
        .eq_ignore_ascii_case(KIRO_ENVELOPE_NAME)
    {
        ProviderPrivateStreamNormalizeMode::KiroToClaudeCli(Box::new(
            KiroToClaudeCliStreamState::new(report_context),
        ))
    } else if descriptor.unwraps_response_envelope {
        ProviderPrivateStreamNormalizeMode::EnvelopeUnwrap
    } else {
        return None;
    };
    Some(ProviderPrivateStreamNormalizer {
        report_context: Cow::Borrowed(report_context),
        buffered: Vec::new(),
        current_event_type: None,
        mode,
    })
}

pub fn extract_provider_private_stream_error_body(
    _report_context: Option<&Value>,
    body: &[u8],
) -> Option<Value> {
    extract_stream_error_event_body(body)
}

impl ProviderPrivateStreamNormalizer<'_> {
    /// Move parser state across task boundaries without replaying captured bytes.
    pub fn into_owned(self) -> ProviderPrivateStreamNormalizer<'static> {
        ProviderPrivateStreamNormalizer {
            report_context: Cow::Owned(self.report_context.into_owned()),
            buffered: self.buffered,
            current_event_type: self.current_event_type,
            mode: self.mode,
        }
    }

    pub fn push_chunk(&mut self, chunk: &[u8]) -> Result<Vec<u8>, AiSurfaceFinalizeError> {
        match &mut self.mode {
            ProviderPrivateStreamNormalizeMode::KiroToClaudeCli(state) => {
                state.push_chunk(self.report_context.as_ref(), chunk)
            }
            ProviderPrivateStreamNormalizeMode::EnvelopeUnwrap => {
                let next_len = self
                    .buffered
                    .len()
                    .checked_add(chunk.len())
                    .ok_or_else(|| {
                        AiSurfaceFinalizeError::new("provider stream normalization buffer overflow")
                    })?;
                if next_len > MAX_PRIVATE_STREAM_BUFFER_BYTES {
                    return Err(AiSurfaceFinalizeError::new(format!(
                        "provider stream normalization buffer exceeds {MAX_PRIVATE_STREAM_BUFFER_BYTES} bytes"
                    )));
                }
                self.buffered.extend_from_slice(chunk);
                let mut output = Vec::new();
                while let Some(line_end) = self.buffered.iter().position(|byte| *byte == b'\n') {
                    let line = self.buffered.drain(..=line_end).collect::<Vec<_>>();
                    output.extend(
                        transform_provider_private_stream_line_with_event_state(
                            self.report_context.as_ref(),
                            line,
                            &mut self.current_event_type,
                        )
                        .map_err(AiSurfaceFinalizeError::from)?,
                    );
                }
                Ok(output)
            }
        }
    }

    pub fn finish(&mut self) -> Result<Vec<u8>, AiSurfaceFinalizeError> {
        match &mut self.mode {
            ProviderPrivateStreamNormalizeMode::KiroToClaudeCli(state) => {
                state.finish(self.report_context.as_ref())
            }
            ProviderPrivateStreamNormalizeMode::EnvelopeUnwrap => {
                if self.buffered.is_empty() {
                    return Ok(Vec::new());
                }
                let line = std::mem::take(&mut self.buffered);
                transform_provider_private_stream_line_with_event_state(
                    self.report_context.as_ref(),
                    line,
                    &mut self.current_event_type,
                )
                .map_err(AiSurfaceFinalizeError::from)
            }
        }
    }
}

pub fn stream_body_contains_error_event(body: &[u8]) -> bool {
    extract_stream_error_event_body(body).is_some()
}

fn extract_stream_error_event_body(body: &[u8]) -> Option<Value> {
    let Ok(text) = std::str::from_utf8(body) else {
        return None;
    };
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
        let Ok(mut event) = serde_json::from_str::<Value>(data_line) else {
            continue;
        };
        if let Some(event_object) = event.as_object_mut() {
            if !event_object.contains_key("type") {
                if let Some(event_name) = current_event_type.take() {
                    event_object.insert("type".to_string(), Value::String(event_name));
                }
            }
        }
        if event
            .get("type")
            .and_then(Value::as_str)
            .is_some_and(|value| value.eq_ignore_ascii_case("error"))
        {
            return Some(normalize_provider_private_error_body(event));
        }
        if let Some(mut error_body) = openai_stream_terminal_error_body(&event) {
            let response_failed_without_provider_type = event
                .get("type")
                .and_then(Value::as_str)
                .is_some_and(|event_type| event_type.eq_ignore_ascii_case("response.failed"))
                && event
                    .pointer("/response/error/type")
                    .or_else(|| event.pointer("/error/type"))
                    .and_then(Value::as_str)
                    .map(str::trim)
                    .is_none_or(str::is_empty);
            if response_failed_without_provider_type {
                if let Some(error) = error_body.get_mut("error").and_then(Value::as_object_mut) {
                    error.insert(
                        "type".to_string(),
                        Value::String("stream_terminal_error".to_string()),
                    );
                }
            }
            return Some(error_body);
        }
        current_event_type = None;
    }
    None
}

fn normalize_provider_private_error_body(error: Value) -> Value {
    let mut error = if error.get("error").is_some_and(|value| !value.is_null()) {
        error
    } else {
        serde_json::json!({ "error": error })
    };

    if let Some(error_object) = error.get_mut("error").and_then(Value::as_object_mut) {
        if !error_object.contains_key("type") {
            if let Some(kind) = error_object
                .get("code")
                .or_else(|| error_object.get("status"))
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
            {
                error_object.insert("type".to_string(), Value::String(kind.to_string()));
            }
        }
    }

    error
}

fn clear_private_envelope_context(report_context: &Value) -> Value {
    let mut normalized = report_context.clone();
    if let Some(object) = normalized.as_object_mut() {
        object.insert("has_envelope".to_string(), Value::Bool(false));
        object.remove("envelope_name");
    }
    normalized
}

fn report_context_preserves_private_client_envelope(
    report_context: &Value,
    envelope_name: &str,
    provider_api_format: &str,
) -> bool {
    if envelope_name.is_empty()
        || provider_adaptation_descriptor_for_envelope(envelope_name, provider_api_format).is_none()
    {
        return false;
    }
    report_context
        .get("client_envelope_name")
        .and_then(Value::as_str)
        .is_some_and(|client_envelope_name| {
            client_envelope_name.eq_ignore_ascii_case(envelope_name)
        })
}

fn local_finalize_response_model(report_context: &Value) -> &str {
    report_context
        .get("mapped_model")
        .and_then(Value::as_str)
        .or_else(|| report_context.get("model").and_then(Value::as_str))
        .unwrap_or_default()
}

fn inject_antigravity_stream_tool_ids(value: &mut Value) {
    let Some(candidates) = value.get_mut("candidates").and_then(Value::as_array_mut) else {
        return;
    };

    for candidate in candidates {
        let Some(parts) = candidate
            .get_mut("content")
            .and_then(Value::as_object_mut)
            .and_then(|content| content.get_mut("parts"))
            .and_then(Value::as_array_mut)
        else {
            continue;
        };

        let mut counters: BTreeMap<String, usize> = BTreeMap::new();
        for part in parts {
            let Some(function_call) = part.get_mut("functionCall").and_then(Value::as_object_mut)
            else {
                continue;
            };
            let has_id = function_call
                .get("id")
                .and_then(Value::as_str)
                .is_some_and(|value| !value.is_empty());
            if has_id {
                continue;
            }
            let name = function_call
                .get("name")
                .and_then(Value::as_str)
                .filter(|value| !value.is_empty())
                .unwrap_or("unknown")
                .to_string();
            let index = counters.entry(name.clone()).or_insert(0);
            function_call.insert(
                "id".to_string(),
                Value::String(format!("call_{name}_{index}")),
            );
            *index += 1;
        }
    }
}

fn inject_antigravity_sync_tool_ids(response: &mut Value, model: &str) {
    if !model.to_ascii_lowercase().contains("claude") {
        return;
    }

    let Some(candidates) = response.get_mut("candidates").and_then(Value::as_array_mut) else {
        return;
    };

    for candidate in candidates {
        let Some(parts) = candidate
            .get_mut("content")
            .and_then(Value::as_object_mut)
            .and_then(|content| content.get_mut("parts"))
            .and_then(Value::as_array_mut)
        else {
            continue;
        };

        let mut name_counters: BTreeMap<String, usize> = BTreeMap::new();
        for part in parts {
            let function_call = if let Some(function_call) =
                part.get_mut("functionCall").and_then(Value::as_object_mut)
            {
                function_call
            } else if let Some(function_call) =
                part.get_mut("function_call").and_then(Value::as_object_mut)
            {
                function_call
            } else {
                continue;
            };
            let has_id = function_call
                .get("id")
                .and_then(Value::as_str)
                .is_some_and(|value| !value.is_empty());
            if has_id {
                continue;
            }
            let function_name = function_call
                .get("name")
                .and_then(Value::as_str)
                .filter(|value| !value.is_empty())
                .unwrap_or("unknown")
                .to_string();
            let count = name_counters.entry(function_name.clone()).or_insert(0);
            function_call.insert(
                "id".to_string(),
                Value::String(format!("call_{function_name}_{count}")),
            );
            *count += 1;
        }
    }
}

fn postprocess_private_response_value(data: &mut Value, report_context: &Value) {
    if !matches!(
        report_context.get("envelope_name").and_then(Value::as_str),
        Some(ANTIGRAVITY_V1INTERNAL_ENVELOPE_NAME)
    ) {
        return;
    }
    if let Some(object) = data.as_object_mut() {
        if !object.contains_key("_v1internal_response_id") {
            if let Some(response_id) = object.remove("responseId") {
                object.insert("_v1internal_response_id".to_string(), response_id);
            }
        }
    }
    inject_antigravity_sync_tool_ids(data, local_finalize_response_model(report_context));
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{
        extract_provider_private_stream_error_body, maybe_build_provider_private_stream_normalizer,
        normalize_provider_private_report_context, normalize_provider_private_response_value,
        stream_body_contains_error_event, transform_provider_private_stream_line,
    };

    #[test]
    fn normalizes_supported_private_report_context() {
        let report_context = json!({
            "has_envelope": true,
            "envelope_name": "antigravity:v1internal",
            "provider_api_format": "gemini:generate_content",
        });
        let normalized = normalize_provider_private_report_context(Some(&report_context))
            .expect("context should normalize");
        assert_eq!(normalized["has_envelope"], json!(false));
        assert!(normalized.get("envelope_name").is_none());
    }

    #[test]
    fn unwraps_antigravity_sync_response_and_injects_ids() {
        let report_context = json!({
            "has_envelope": true,
            "provider_api_format": "gemini:generate_content",
            "envelope_name": "antigravity:v1internal",
            "mapped_model": "claude-sonnet-4-5",
        });
        let body = json!({
            "response": {
                "candidates": [{
                    "content": {
                        "parts": [{
                            "functionCall": {
                                "name": "get_weather",
                                "args": {"city": "SF"}
                            }
                        }]
                    }
                }]
            },
            "responseId": "resp_123"
        });

        let normalized = normalize_provider_private_response_value(body, &report_context)
            .expect("body should normalize");
        assert_eq!(normalized["_v1internal_response_id"], json!("resp_123"));
        assert_eq!(
            normalized["candidates"][0]["content"]["parts"][0]["functionCall"]["id"],
            json!("call_get_weather_0")
        );
    }

    #[test]
    fn unwraps_antigravity_chunks_sync_response() {
        let report_context = json!({
            "has_envelope": true,
            "provider_api_format": "gemini:generate_content",
            "envelope_name": "antigravity:v1internal",
            "mapped_model": "gemini-3-flash-agent",
        });
        let body = json!({
            "chunks": [{
                "response": {
                    "responseId": "resp_antigravity_chunks_123",
                    "candidates": [{
                        "content": {
                            "parts": [{"text": "Hello "}],
                            "role": "model"
                        },
                        "index": 0
                    }],
                    "modelVersion": "gemini-3-flash-agent"
                },
                "traceId": "trace-antigravity-chunks"
            }, {
                "response": {
                    "responseId": "resp_antigravity_chunks_123",
                    "candidates": [{
                        "content": {
                            "parts": [{"text": "Gemini"}],
                            "role": "model"
                        },
                        "finishReason": "STOP",
                        "index": 0
                    }],
                    "modelVersion": "gemini-3-flash-agent",
                    "usageMetadata": {
                        "promptTokenCount": 2,
                        "candidatesTokenCount": 2,
                        "totalTokenCount": 4
                    }
                },
                "traceId": "trace-antigravity-chunks"
            }],
            "metadata": {
                "stream": true,
                "stored_chunks": 2,
                "total_chunks": 2
            }
        });

        let normalized = normalize_provider_private_response_value(body, &report_context)
            .expect("chunks should normalize");

        assert_eq!(
            normalized["candidates"][0]["content"]["parts"][0]["text"],
            json!("Hello Gemini")
        );
        assert_eq!(
            normalized["_v1internal_response_id"],
            json!("resp_antigravity_chunks_123")
        );
        assert_eq!(normalized["usageMetadata"]["totalTokenCount"], json!(4));
    }

    #[test]
    fn unwraps_antigravity_stream_line_and_injects_ids() {
        let report_context = json!({
            "has_envelope": true,
            "provider_api_format": "gemini:generate_content",
            "client_api_format": "gemini:generate_content",
            "envelope_name": "antigravity:v1internal",
            "mapped_model": "claude-sonnet-4-5",
        });
        let output = transform_provider_private_stream_line(
            &report_context,
            b"data: {\"response\":{\"candidates\":[{\"content\":{\"parts\":[{\"functionCall\":{\"name\":\"get_weather\",\"args\":{\"city\":\"SF\"}}}],\"role\":\"model\"},\"index\":0}],\"modelVersion\":\"claude-sonnet-4-5\"},\"responseId\":\"resp_123\"}\n\n".to_vec(),
        )
        .expect("unwrap should succeed");
        let output_text = String::from_utf8(output).expect("text should decode");
        assert!(output_text.contains("\"_v1internal_response_id\":\"resp_123\""));
        assert!(output_text.contains("\"id\":\"call_get_weather_0\""));
    }

    #[test]
    fn private_stream_normalizer_unwraps_antigravity_stream() {
        let report_context = json!({
            "has_envelope": true,
            "provider_api_format": "gemini:generate_content",
            "client_api_format": "gemini:generate_content",
            "envelope_name": "antigravity:v1internal",
            "mapped_model": "claude-sonnet-4-5",
        });
        let mut normalizer = maybe_build_provider_private_stream_normalizer(Some(&report_context))
            .expect("normalizer should exist");
        let output = normalizer
            .push_chunk(
                b"data: {\"response\":{\"candidates\":[{\"content\":{\"parts\":[{\"functionCall\":{\"name\":\"get_weather\",\"args\":{\"city\":\"SF\"}}}],\"role\":\"model\"},\"index\":0}],\"modelVersion\":\"claude-sonnet-4-5\"},\"responseId\":\"resp_123\"}\n\n",
            )
            .expect("unwrap should succeed");
        let output_text = String::from_utf8(output).expect("text should decode");
        assert!(output_text.contains("\"_v1internal_response_id\":\"resp_123\""));
        assert!(output_text.contains("\"id\":\"call_get_weather_0\""));
    }

    #[test]
    fn private_stream_normalizer_preserves_antigravity_native_client_envelope() {
        let report_context = json!({
            "has_envelope": true,
            "provider_api_format": "gemini:generate_content",
            "client_api_format": "gemini:generate_content",
            "envelope_name": "antigravity:v1internal",
            "client_envelope_name": "antigravity:v1internal",
            "mapped_model": "claude-sonnet-4-5",
        });

        assert!(maybe_build_provider_private_stream_normalizer(Some(&report_context)).is_none());
    }

    #[test]
    fn detects_sse_error_events_without_explicit_type_field() {
        let body = br#"event: error
data: {"message":"bad"}

"#;
        assert!(stream_body_contains_error_event(body));
    }

    #[test]
    fn extracts_openai_response_failed_error_without_losing_provider_fields() {
        let body = br#"event: response.failed
data: {"type":"response.failed","response":{"status":"failed","error":{"type":"invalid_request","message":"cyber policy rejected the request","code":"cyber_policy_violation","param":"input","details":{"policy_category":"cybersecurity","appeal_allowed":true}}}}

"#;

        let error_body = extract_provider_private_stream_error_body(None, body)
            .expect("response.failed should expose its provider error body");

        assert_eq!(
            error_body,
            json!({
                "error": {
                    "type": "invalid_request",
                    "message": "cyber policy rejected the request",
                    "code": "cyber_policy_violation",
                    "param": "input",
                    "details": {
                        "policy_category": "cybersecurity",
                        "appeal_allowed": true
                    }
                }
            })
        );
    }

    #[test]
    fn response_failed_without_provider_type_uses_stream_terminal_error_category() {
        let body = br#"event: response.failed
data: {"type":"response.failed","response":{"status":"failed","error":{"message":"cyber policy rejected the request","code":"cyber_policy"}}}

"#;

        let error_body = extract_provider_private_stream_error_body(None, body)
            .expect("response.failed should expose its provider error body");

        assert_eq!(
            error_body.pointer("/error/type"),
            Some(&json!("stream_terminal_error"))
        );
        assert_eq!(
            error_body.pointer("/error/code"),
            Some(&json!("cyber_policy"))
        );
    }

    #[test]
    fn private_stream_normalizer_rejects_unbounded_incomplete_buffer() {
        let report_context = json!({
            "has_envelope": true,
            "envelope_name": "antigravity:v1internal",
            "provider_api_format": "gemini:generate_content",
        });
        let mut normalizer = maybe_build_provider_private_stream_normalizer(Some(&report_context))
            .expect("normalizer should exist");
        let oversized = vec![b'x'; super::MAX_PRIVATE_STREAM_BUFFER_BYTES + 1];

        let error = normalizer
            .push_chunk(&oversized)
            .expect_err("incomplete provider stream must be bounded");
        assert!(error.0.contains("buffer exceeds"));
    }
}
