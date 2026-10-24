use aether_ai_formats::FormatId;
use aether_contracts::{ExecutionStreamTerminalSummary, StandardizedUsage};
use serde_json::Value;

use crate::formats::claude::messages::stream::{ClaudeClientEmitter, ClaudeProviderState};
use crate::formats::gemini::generate_content::stream::{GeminiClientEmitter, GeminiProviderState};
use crate::formats::openai::chat::stream::{
    OpenAIChatClientEmitter, OpenAIChatProviderState, OpenAIResponsesClientEmitter,
    OpenAIResponsesProviderState,
};
use crate::formats::openai::image::stream::OpenAiImageStreamTerminalState;
use crate::formats::openai::responses::history::{
    record_converted_response_history, ResponseHistoryRecord,
};
use crate::formats::shared::error_body::{
    build_core_error_body_for_client_format, LocalCoreSyncErrorKind,
};
use crate::formats::shared::sse::encode_json_sse;
use crate::formats::shared::stream_core::common::{
    canonical_usage_from_openai_usage, decode_json_data_line, openai_stream_terminal_error_body,
    openai_stream_terminal_error_message, unsupported_stream_event_message, CanonicalStreamEvent,
    CanonicalStreamFrame, CanonicalUsage,
};
use crate::formats::shared::AiSurfaceFinalizeError;

const PROVIDER_STREAM_FINISH_ERROR_MESSAGE: &str =
    "Upstream stream ended with finish reason: error";

#[derive(Default)]
pub struct StreamingStandardFormatMatrix {
    provider: Option<ProviderStreamParser>,
    client: Option<ClientStreamEmitter>,
    propagated_actual_service_tier: Option<String>,
    pending_sse_event: Option<String>,
    terminated: bool,
    history_recorded: bool,
    pending_history_record: Option<ResponseHistoryRecord>,
}

impl StreamingStandardFormatMatrix {
    pub fn transform_line(
        &mut self,
        report_context: &Value,
        line: Vec<u8>,
    ) -> Result<Vec<u8>, AiSurfaceFinalizeError> {
        if self.terminated {
            return Ok(Vec::new());
        }
        self.ensure_initialized(report_context);
        let line = self.apply_sse_event_type(line);
        if let Some(error_body) = build_client_error_body_for_line(report_context, &line) {
            self.terminated = true;
            return self.emit_error(error_body);
        }
        let (provider, client, propagated_actual_service_tier) = (
            &mut self.provider,
            &mut self.client,
            &mut self.propagated_actual_service_tier,
        );
        let Some(provider) = provider.as_mut() else {
            return Ok(Vec::new());
        };
        let frames = provider.push_line(report_context, line)?;
        if provider.actual_service_tier() != propagated_actual_service_tier.as_deref() {
            *propagated_actual_service_tier = provider.actual_service_tier().map(ToOwned::to_owned);
            if let Some(client) = client.as_mut() {
                client.set_actual_service_tier(propagated_actual_service_tier.as_deref());
            }
        }
        self.emit_frames(report_context, frames)
    }

    fn apply_sse_event_type(&mut self, line: Vec<u8>) -> Vec<u8> {
        apply_sse_event_type(&mut self.pending_sse_event, line)
    }

    pub fn finish(&mut self, report_context: &Value) -> Result<Vec<u8>, AiSurfaceFinalizeError> {
        if self.terminated {
            return Ok(Vec::new());
        }
        self.ensure_initialized(report_context);
        let (provider, client, propagated_actual_service_tier) = (
            &mut self.provider,
            &mut self.client,
            &mut self.propagated_actual_service_tier,
        );
        let Some(provider) = provider.as_mut() else {
            return Ok(Vec::new());
        };
        let frames = provider.finish(report_context)?;
        if provider.actual_service_tier() != propagated_actual_service_tier.as_deref() {
            *propagated_actual_service_tier = provider.actual_service_tier().map(ToOwned::to_owned);
            if let Some(client) = client.as_mut() {
                client.set_actual_service_tier(propagated_actual_service_tier.as_deref());
            }
        }
        let mut out = self.emit_frames(report_context, frames)?;
        if let Some(client) = self.client.as_mut() {
            out.extend(client.finish()?);
        }
        self.record_response_history(report_context);
        Ok(out)
    }

    fn ensure_initialized(&mut self, report_context: &Value) {
        if self.provider.is_some() && self.client.is_some() {
            return;
        }

        let provider_api_format = provider_api_format_for_context(report_context);
        let client_api_format = client_api_format_for_context(report_context);

        self.provider = ProviderStreamParser::for_api_format(provider_api_format.as_str());
        self.client =
            ClientStreamEmitter::for_api_format(client_api_format.as_str(), report_context);
    }

    fn emit_frames(
        &mut self,
        report_context: &Value,
        frames: Vec<CanonicalStreamFrame>,
    ) -> Result<Vec<u8>, AiSurfaceFinalizeError> {
        let Some(client) = self.client.as_mut() else {
            return Ok(Vec::new());
        };
        let mut out = Vec::new();
        for frame in frames {
            if let CanonicalStreamEvent::Finish {
                finish_reason: Some(ref finish_reason),
                ..
            } = frame.event
            {
                if !canonical_stream_finish_reason_is_supported(finish_reason) {
                    self.terminated = true;
                    out.extend(client.emit_finish_reason_error(finish_reason)?);
                    break;
                }
            }
            if let CanonicalStreamEvent::UnknownEvent(payload) = &frame.event {
                self.terminated = true;
                if openai_stream_terminal_error_body(payload).is_some() {
                    out.extend(client.emit_terminal_error_frame(frame)?);
                } else {
                    out.extend(client.emit_unknown_event(payload)?);
                }
                break;
            }
            if let CanonicalStreamEvent::OpenAiResponsesOutputItem { raw_event, .. } = &frame.event
            {
                if !matches!(client, ClientStreamEmitter::OpenAIResponses(_)) {
                    self.terminated = true;
                    out.extend(client.emit_unknown_event(raw_event)?);
                    break;
                }
            }
            out.extend(client.emit(frame)?);
        }
        self.record_response_history(report_context);
        Ok(out)
    }

    fn record_response_history(&mut self, report_context: &Value) {
        if self.history_recorded {
            return;
        }
        let Some(response) = self.client.as_ref().and_then(|client| match client {
            ClientStreamEmitter::OpenAIResponses(emitter) => {
                emitter.completed_response_for_history()
            }
            _ => None,
        }) else {
            return;
        };
        self.pending_history_record = record_converted_response_history(report_context, response);
        self.history_recorded = true;
    }

    pub fn take_response_history_record(&mut self) -> Option<ResponseHistoryRecord> {
        self.pending_history_record.take()
    }

    fn emit_error(&mut self, error_body: Value) -> Result<Vec<u8>, AiSurfaceFinalizeError> {
        let Some(client) = self.client.as_mut() else {
            return Ok(Vec::new());
        };
        client.emit_error(error_body)
    }
}

fn apply_sse_event_type(pending_sse_event: &mut Option<String>, line: Vec<u8>) -> Vec<u8> {
    let Ok(text) = std::str::from_utf8(&line) else {
        return line;
    };
    let trimmed = text.trim_matches('\r').trim();
    if let Some(event) = trimmed.strip_prefix("event:").map(str::trim) {
        *pending_sse_event = (!event.is_empty()).then(|| event.to_string());
        return line;
    }
    if trimmed.is_empty() {
        *pending_sse_event = None;
        return line;
    }
    if !trimmed.starts_with("data:") {
        return line;
    }
    let Some(event) = pending_sse_event.take() else {
        return line;
    };
    let Some(mut payload) = decode_json_data_line(&line) else {
        return line;
    };
    let Some(payload) = payload.as_object_mut() else {
        return line;
    };
    if payload.contains_key("type") {
        return line;
    }
    payload.insert("type".to_string(), Value::String(event));
    let mut normalized = b"data: ".to_vec();
    normalized.extend(serde_json::to_vec(&payload).expect("JSON value serialization cannot fail"));
    normalized.push(b'\n');
    normalized
}

#[derive(Default)]
pub struct StreamingStandardTerminalObserver {
    provider: Option<TerminalStreamParser>,
    latest_summary: Option<ExecutionStreamTerminalSummary>,
    pending_sse_event: Option<String>,
}

impl StreamingStandardTerminalObserver {
    pub fn push_line(
        &mut self,
        report_context: &Value,
        line: Vec<u8>,
    ) -> Result<(), AiSurfaceFinalizeError> {
        self.ensure_initialized(report_context);
        let line = apply_sse_event_type(&mut self.pending_sse_event, line);
        let Some(provider) = self.provider.as_mut() else {
            return Ok(());
        };
        match provider {
            TerminalStreamParser::Standard(provider) => {
                let frames = provider.push_line(report_context, line)?;
                let actual_service_tier = provider.actual_service_tier().map(ToOwned::to_owned);
                self.observe_frames(frames);
                if let Some(actual_service_tier) = actual_service_tier {
                    self.latest_summary
                        .get_or_insert_with(ExecutionStreamTerminalSummary::default)
                        .provider_actual_service_tier = Some(actual_service_tier);
                }
            }
            TerminalStreamParser::OpenAIImage(provider) => {
                if let Some(summary) = provider.push_line(report_context, line)? {
                    self.latest_summary = Some(summary);
                }
            }
        }
        Ok(())
    }

    /// 结构化入口：给已经持有解析好的协议事件的传输用（Responses WebSocket），
    /// 避免为了复用 [`Self::push_line`] 把事件重新拼成 `data: {json}` 再解析回来。
    ///
    /// 只要 provider 的协议状态机本身接受结构化事件，这条路径与 `push_line`
    /// 完全等价——`push_line` 现在就是「解码 + `push_event`」。
    ///
    /// `openai:image` 的终态状态机没有结构化入口（它按 SSE 行做增量解析），
    /// 这里返回 `Err`，由调用方 `disable_with_error` 把摘要标成 parser_error，
    /// 而不是静默丢事件。
    pub fn push_event(
        &mut self,
        report_context: &Value,
        event: &Value,
    ) -> Result<(), AiSurfaceFinalizeError> {
        self.ensure_initialized(report_context);
        let Some(provider) = self.provider.as_mut() else {
            return Ok(());
        };
        match provider {
            TerminalStreamParser::Standard(provider) => {
                let frames = provider.push_event(report_context, event)?;
                let actual_service_tier = provider.actual_service_tier().map(ToOwned::to_owned);
                self.observe_frames(frames);
                if let Some(actual_service_tier) = actual_service_tier {
                    self.latest_summary
                        .get_or_insert_with(ExecutionStreamTerminalSummary::default)
                        .provider_actual_service_tier = Some(actual_service_tier);
                }
            }
            TerminalStreamParser::OpenAIImage(_) => {
                return Err(AiSurfaceFinalizeError::new(
                    "openai:image terminal observation has no structured event entry",
                ));
            }
        }
        Ok(())
    }

    pub fn finish(
        &mut self,
        report_context: &Value,
    ) -> Result<Option<ExecutionStreamTerminalSummary>, AiSurfaceFinalizeError> {
        self.ensure_initialized(report_context);
        let Some(provider) = self.provider.as_mut() else {
            return Ok(self.latest_summary.clone());
        };
        match provider {
            TerminalStreamParser::Standard(provider) => {
                let frames = provider.finish(report_context)?;
                self.observe_frames(frames);
            }
            TerminalStreamParser::OpenAIImage(provider) => {
                if let Some(summary) = provider.finish(report_context)? {
                    self.latest_summary = Some(summary);
                }
            }
        }
        Ok(self.latest_summary.clone())
    }

    pub fn disable_with_error(&mut self, parser_error: impl Into<String>) {
        let parser_error = parser_error.into();
        if let Some(summary) = self.latest_summary.as_mut() {
            if summary.parser_error.is_none() {
                summary.parser_error = Some(parser_error);
            }
        } else {
            self.latest_summary = Some(ExecutionStreamTerminalSummary {
                parser_error: Some(parser_error),
                ..ExecutionStreamTerminalSummary::default()
            });
        }
        self.provider = None;
    }

    pub fn latest_summary(&self) -> Option<&ExecutionStreamTerminalSummary> {
        self.latest_summary.as_ref()
    }

    fn ensure_initialized(&mut self, report_context: &Value) {
        if self.provider.is_some() || self.latest_summary.is_some() {
            return;
        }
        let provider_api_format = provider_api_format_for_context(report_context);
        self.provider = TerminalStreamParser::for_api_format(provider_api_format.as_str());
    }

    fn observe_frames(&mut self, frames: Vec<CanonicalStreamFrame>) {
        for frame in frames {
            self.observe_frame(frame);
        }
    }

    fn observe_frame(&mut self, frame: CanonicalStreamFrame) {
        let CanonicalStreamFrame { id, model, event } = frame;
        let summary = self
            .latest_summary
            .get_or_insert_with(|| ExecutionStreamTerminalSummary {
                response_id: Some(id.clone()),
                model: Some(model.clone()),
                ..ExecutionStreamTerminalSummary::default()
            });
        if summary.response_id.is_none() {
            summary.response_id = Some(id);
        }
        if summary.model.is_none() {
            summary.model = Some(model);
        }
        match event {
            CanonicalStreamEvent::UnknownEvent(payload)
                if openai_stream_terminal_error_body(&payload).is_some() =>
            {
                summary.unknown_event_count = summary.unknown_event_count.saturating_add(1);
                summary.observed_finish = true;
                summary.finish_reason = Some("error".to_string());
                summary.parser_error = openai_stream_terminal_error_message(&payload);
                summary.standardized_usage = payload
                    .pointer("/response/usage")
                    .and_then(|usage| canonical_usage_from_openai_usage(Some(usage)))
                    .map(standardized_usage_from_canonical);
            }
            CanonicalStreamEvent::UnknownEvent(_) => {
                summary.unknown_event_count = summary.unknown_event_count.saturating_add(1);
            }
            CanonicalStreamEvent::Finish {
                finish_reason,
                usage,
            } => {
                if let Some(parser_error) = finish_reason
                    .as_deref()
                    .filter(|reason| !canonical_stream_finish_reason_is_supported(reason))
                    .map(|reason| {
                        if reason.trim() == "error" {
                            PROVIDER_STREAM_FINISH_ERROR_MESSAGE.to_string()
                        } else {
                            format!("unsupported provider stream finish reason: {reason}")
                        }
                    })
                {
                    summary.parser_error.get_or_insert(parser_error);
                }
                summary.finish_reason = finish_reason;
                summary.standardized_usage = usage.map(standardized_usage_from_canonical);
                summary.observed_finish = true;
            }
            _ => {}
        }
    }
}

enum TerminalStreamParser {
    Standard(ProviderStreamParser),
    OpenAIImage(OpenAiImageStreamTerminalState),
}

impl TerminalStreamParser {
    fn for_api_format(provider_api_format: &str) -> Option<Self> {
        if provider_api_format
            .trim()
            .eq_ignore_ascii_case("openai:image")
        {
            return Some(Self::OpenAIImage(OpenAiImageStreamTerminalState::default()));
        }
        let provider = match ProviderStreamParser::for_api_format(provider_api_format)? {
            ProviderStreamParser::OpenAIChat(_) => {
                ProviderStreamParser::OpenAIChat(OpenAIChatProviderState::terminal_observation())
            }
            ProviderStreamParser::OpenAIResponses(_) => ProviderStreamParser::OpenAIResponses(
                OpenAIResponsesProviderState::terminal_observation(),
            ),
            ProviderStreamParser::Gemini(_) => {
                ProviderStreamParser::Gemini(GeminiProviderState::terminal_observation())
            }
            provider @ ProviderStreamParser::Claude(_) => provider,
        };
        Some(Self::Standard(provider))
    }
}

#[cfg(test)]
#[path = "terminal_observation_tests.rs"]
mod terminal_observation_tests;

enum ProviderStreamParser {
    OpenAIChat(OpenAIChatProviderState),
    OpenAIResponses(OpenAIResponsesProviderState),
    Claude(ClaudeProviderState),
    Gemini(GeminiProviderState),
}

impl ProviderStreamParser {
    fn for_api_format(provider_api_format: &str) -> Option<Self> {
        Some(match FormatId::parse(provider_api_format)? {
            FormatId::OpenAiChat => Self::OpenAIChat(OpenAIChatProviderState::default()),
            FormatId::OpenAiResponses | FormatId::OpenAiResponsesCompact => {
                Self::OpenAIResponses(OpenAIResponsesProviderState::default())
            }
            FormatId::ClaudeMessages => Self::Claude(ClaudeProviderState::default()),
            FormatId::GeminiGenerateContent => Self::Gemini(GeminiProviderState::default()),
            FormatId::OpenAiEmbedding
            | FormatId::OpenAiRealtime
            | FormatId::OpenAiSearch
            | FormatId::OpenAiRerank
            | FormatId::GeminiEmbedding
            | FormatId::GeminiInteractions
            | FormatId::JinaEmbedding
            | FormatId::JinaRerank
            | FormatId::DoubaoEmbedding
            | FormatId::AliyunMultimodalEmbedding
            | FormatId::CodexLive => return None,
        })
    }

    fn push_line(
        &mut self,
        report_context: &Value,
        line: Vec<u8>,
    ) -> Result<Vec<CanonicalStreamFrame>, AiSurfaceFinalizeError> {
        match self {
            ProviderStreamParser::OpenAIChat(state) => state.push_line(report_context, line),
            ProviderStreamParser::OpenAIResponses(state) => state.push_line(report_context, line),
            ProviderStreamParser::Claude(state) => state.push_line(report_context, line),
            ProviderStreamParser::Gemini(state) => state.push_line(report_context, line),
        }
    }

    /// 结构化入口。目前只有 `openai:responses` 有传输会走它（Responses
    /// WebSocket）；其余格式的协议状态机同样可以按「解码 + push_event」机械拆分，
    /// 等到真有非 SSE 传输需要时再拆，不做无调用方的接口。
    fn push_event(
        &mut self,
        report_context: &Value,
        event: &Value,
    ) -> Result<Vec<CanonicalStreamFrame>, AiSurfaceFinalizeError> {
        match self {
            ProviderStreamParser::OpenAIResponses(state) => state.push_event(report_context, event),
            ProviderStreamParser::OpenAIChat(_)
            | ProviderStreamParser::Claude(_)
            | ProviderStreamParser::Gemini(_) => Err(AiSurfaceFinalizeError::new(
                "this provider stream parser has no structured event entry",
            )),
        }
    }

    fn finish(
        &mut self,
        report_context: &Value,
    ) -> Result<Vec<CanonicalStreamFrame>, AiSurfaceFinalizeError> {
        match self {
            ProviderStreamParser::OpenAIChat(state) => state.finish(report_context),
            ProviderStreamParser::OpenAIResponses(state) => state.finish(report_context),
            ProviderStreamParser::Claude(state) => state.finish(report_context),
            ProviderStreamParser::Gemini(state) => state.finish(report_context),
        }
    }

    fn actual_service_tier(&self) -> Option<&str> {
        match self {
            ProviderStreamParser::OpenAIChat(state) => state.actual_service_tier(),
            ProviderStreamParser::OpenAIResponses(state) => state.actual_service_tier(),
            ProviderStreamParser::Claude(_) | ProviderStreamParser::Gemini(_) => None,
        }
    }
}

enum ClientStreamEmitter {
    OpenAIChat(OpenAIChatClientEmitter),
    OpenAIResponses(Box<OpenAIResponsesClientEmitter>),
    Claude(ClaudeClientEmitter),
    Gemini(GeminiClientEmitter),
}

fn provider_api_format_for_context(report_context: &Value) -> String {
    string_context_field(report_context, "provider_stream_event_api_format")
        .or_else(|| string_context_field(report_context, "provider_stream_api_format"))
        .or_else(|| string_context_field(report_context, "provider_api_format"))
        .unwrap_or_default()
}

fn string_context_field(report_context: &Value, key: &str) -> Option<String> {
    let value = report_context.get(key)?.as_str()?.trim();
    (!value.is_empty()).then(|| value.to_ascii_lowercase())
}

fn client_api_format_for_context(report_context: &Value) -> String {
    report_context
        .get("client_api_format")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase()
}

fn standardized_usage_from_canonical(usage: CanonicalUsage) -> StandardizedUsage {
    let mut standardized = StandardizedUsage::new();
    standardized.input_tokens = usage.input_tokens as i64;
    standardized.output_tokens = usage.output_tokens as i64;
    standardized.cache_creation_tokens = usage.cache_creation_tokens as i64;
    standardized.cache_creation_ephemeral_5m_tokens =
        usage.cache_creation_ephemeral_5m_tokens as i64;
    standardized.cache_creation_ephemeral_1h_tokens =
        usage.cache_creation_ephemeral_1h_tokens as i64;
    standardized.cache_read_tokens = usage.cache_read_tokens as i64;
    standardized.reasoning_tokens = usage.reasoning_tokens as i64;
    standardized.dimensions.insert(
        "total_tokens".to_string(),
        serde_json::json!(usage.total_tokens),
    );
    standardized.normalize_cache_creation_breakdown()
}

impl ClientStreamEmitter {
    fn for_api_format(client_api_format: &str, report_context: &Value) -> Option<Self> {
        Some(match FormatId::parse(client_api_format)? {
            FormatId::OpenAiChat => Self::OpenAIChat(OpenAIChatClientEmitter::default()),
            FormatId::OpenAiResponses | FormatId::OpenAiResponsesCompact => {
                Self::OpenAIResponses(Box::new(OpenAIResponsesClientEmitter::with_report_context(
                    report_context,
                )))
            }
            FormatId::ClaudeMessages => Self::Claude(ClaudeClientEmitter::default()),
            FormatId::GeminiGenerateContent => Self::Gemini(GeminiClientEmitter::default()),
            FormatId::OpenAiEmbedding
            | FormatId::OpenAiRealtime
            | FormatId::OpenAiSearch
            | FormatId::OpenAiRerank
            | FormatId::GeminiEmbedding
            | FormatId::GeminiInteractions
            | FormatId::JinaEmbedding
            | FormatId::JinaRerank
            | FormatId::DoubaoEmbedding
            | FormatId::AliyunMultimodalEmbedding
            | FormatId::CodexLive => return None,
        })
    }

    fn emit(&mut self, frame: CanonicalStreamFrame) -> Result<Vec<u8>, AiSurfaceFinalizeError> {
        match self {
            ClientStreamEmitter::OpenAIChat(state) => state.emit(frame),
            ClientStreamEmitter::OpenAIResponses(state) => state.emit(frame),
            ClientStreamEmitter::Claude(state) => state.emit(frame),
            ClientStreamEmitter::Gemini(state) => state.emit(frame),
        }
    }

    fn set_actual_service_tier(&mut self, value: Option<&str>) {
        match self {
            ClientStreamEmitter::OpenAIChat(state) => state.set_actual_service_tier(value),
            ClientStreamEmitter::OpenAIResponses(state) => state.set_actual_service_tier(value),
            ClientStreamEmitter::Claude(_) | ClientStreamEmitter::Gemini(_) => {}
        }
    }

    fn finish(&mut self) -> Result<Vec<u8>, AiSurfaceFinalizeError> {
        match self {
            ClientStreamEmitter::OpenAIChat(state) => state.finish(),
            ClientStreamEmitter::OpenAIResponses(state) => state.finish(),
            ClientStreamEmitter::Claude(state) => state.finish(),
            ClientStreamEmitter::Gemini(state) => state.finish(),
        }
    }

    fn emit_error(&mut self, error_body: Value) -> Result<Vec<u8>, AiSurfaceFinalizeError> {
        match self {
            ClientStreamEmitter::OpenAIResponses(state) => state.emit_error(error_body),
            ClientStreamEmitter::Claude(_) => {
                let event = error_body.get("type").and_then(Value::as_str);
                encode_json_sse(event, &error_body)
            }
            ClientStreamEmitter::OpenAIChat(_) | ClientStreamEmitter::Gemini(_) => {
                encode_json_sse(None, &error_body)
            }
        }
    }

    fn emit_unknown_event(&mut self, payload: &Value) -> Result<Vec<u8>, AiSurfaceFinalizeError> {
        let Some(error_body) = build_core_error_body_for_client_format(
            self.api_format(),
            &unsupported_stream_event_message(payload),
            Some("unsupported_stream_event"),
            LocalCoreSyncErrorKind::ServerError,
        ) else {
            return Ok(Vec::new());
        };
        self.emit_error(error_body)
    }

    fn emit_terminal_error_frame(
        &mut self,
        frame: CanonicalStreamFrame,
    ) -> Result<Vec<u8>, AiSurfaceFinalizeError> {
        if matches!(
            self,
            ClientStreamEmitter::OpenAIChat(_) | ClientStreamEmitter::OpenAIResponses(_)
        ) {
            return self.emit(frame);
        }
        let CanonicalStreamEvent::UnknownEvent(payload) = frame.event else {
            return self.emit(frame);
        };
        let Some(source_error_body) = openai_stream_terminal_error_body(&payload) else {
            return self.emit_unknown_event(&payload);
        };
        let Some(error) = source_error_body.get("error") else {
            return self.emit_unknown_event(&payload);
        };
        let message = error
            .get("message")
            .and_then(Value::as_str)
            .unwrap_or("Upstream stream ended with an error");
        let code = error.get("code").and_then(|value| match value {
            Value::String(value) => Some(value.as_str()),
            _ => None,
        });
        let Some(error_body) = build_core_error_body_for_client_format(
            self.api_format(),
            message,
            code,
            LocalCoreSyncErrorKind::ServerError,
        ) else {
            return Ok(Vec::new());
        };
        self.emit_error(error_body)
    }

    fn emit_finish_reason_error(
        &mut self,
        finish_reason: &str,
    ) -> Result<Vec<u8>, AiSurfaceFinalizeError> {
        let (message, code) = if finish_reason.trim() == "error" {
            (
                PROVIDER_STREAM_FINISH_ERROR_MESSAGE.to_string(),
                "stream_terminal_error",
            )
        } else {
            (
                format!(
                    "Unsupported provider stream finish reason cannot be converted losslessly: field $.finish_reason = {}",
                    serde_json::json!(finish_reason)
                ),
                "unsupported_finish_reason",
            )
        };
        let Some(error_body) = build_core_error_body_for_client_format(
            self.api_format(),
            &message,
            Some(code),
            LocalCoreSyncErrorKind::ServerError,
        ) else {
            return Ok(Vec::new());
        };
        self.emit_error(error_body)
    }

    fn api_format(&self) -> &'static str {
        match self {
            ClientStreamEmitter::OpenAIChat(_) => "openai:chat",
            ClientStreamEmitter::OpenAIResponses(_) => "openai:responses",
            ClientStreamEmitter::Claude(_) => "claude:messages",
            ClientStreamEmitter::Gemini(_) => "gemini:generate_content",
        }
    }
}

fn canonical_stream_finish_reason_is_supported(finish_reason: &str) -> bool {
    matches!(
        finish_reason.trim(),
        "stop" | "length" | "tool_calls" | "function_call" | "content_filter"
    )
}

fn build_client_error_body_for_line(report_context: &Value, line: &[u8]) -> Option<Value> {
    let value = decode_json_data_line(line)?;
    let provider_api_format = provider_api_format_for_context(report_context);
    let client_api_format = report_context
        .get("client_api_format")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    let (message, code, kind) = parse_provider_error(&provider_api_format, &value)?;
    build_core_error_body_for_client_format(&client_api_format, &message, code.as_deref(), kind)
}

fn parse_provider_error(
    provider_api_format: &str,
    payload: &Value,
) -> Option<(String, Option<String>, LocalCoreSyncErrorKind)> {
    match FormatId::parse(provider_api_format)? {
        FormatId::OpenAiChat | FormatId::OpenAiResponses | FormatId::OpenAiResponsesCompact => {
            parse_openai_error(payload)
        }
        FormatId::ClaudeMessages => parse_claude_error(payload),
        FormatId::GeminiGenerateContent | FormatId::GeminiInteractions => {
            parse_gemini_error(payload)
        }
        FormatId::OpenAiEmbedding
        | FormatId::OpenAiRealtime
        | FormatId::OpenAiSearch
        | FormatId::OpenAiRerank
        | FormatId::GeminiEmbedding
        | FormatId::JinaEmbedding
        | FormatId::JinaRerank
        | FormatId::DoubaoEmbedding
        | FormatId::AliyunMultimodalEmbedding
        | FormatId::CodexLive => None,
    }
}

fn parse_openai_error(payload: &Value) -> Option<(String, Option<String>, LocalCoreSyncErrorKind)> {
    let error_body = openai_stream_terminal_error_body(payload)?;
    let error = error_body.get("error")?.as_object()?;
    let message = error.get("message").and_then(Value::as_str)?.to_string();
    let code = error
        .get("code")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned);
    let kind = match error
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or_default()
    {
        "invalid_request_error" => LocalCoreSyncErrorKind::InvalidRequest,
        "authentication_error" => LocalCoreSyncErrorKind::Authentication,
        "permission_error" => LocalCoreSyncErrorKind::PermissionDenied,
        "not_found_error" => LocalCoreSyncErrorKind::NotFound,
        "rate_limit_error" => LocalCoreSyncErrorKind::RateLimit,
        "context_length_exceeded" => LocalCoreSyncErrorKind::ContextLengthExceeded,
        "overloaded_error" => LocalCoreSyncErrorKind::Overloaded,
        _ => LocalCoreSyncErrorKind::ServerError,
    };
    Some((message, code, kind))
}

fn parse_claude_error(payload: &Value) -> Option<(String, Option<String>, LocalCoreSyncErrorKind)> {
    let error = payload.get("error")?.as_object()?;
    let message = error.get("message").and_then(Value::as_str)?.to_string();
    let code = error
        .get("code")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned);
    let kind = match error
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or_default()
    {
        "invalid_request_error" => LocalCoreSyncErrorKind::InvalidRequest,
        "authentication_error" => LocalCoreSyncErrorKind::Authentication,
        "permission_error" => LocalCoreSyncErrorKind::PermissionDenied,
        "not_found_error" => LocalCoreSyncErrorKind::NotFound,
        "rate_limit_error" => LocalCoreSyncErrorKind::RateLimit,
        "overloaded_error" => LocalCoreSyncErrorKind::Overloaded,
        _ => LocalCoreSyncErrorKind::ServerError,
    };
    Some((message, code, kind))
}

fn parse_gemini_error(payload: &Value) -> Option<(String, Option<String>, LocalCoreSyncErrorKind)> {
    let error = payload.get("error")?.as_object()?;
    let message = error.get("message").and_then(Value::as_str)?.to_string();
    let code = error.get("code").map(|value| match value {
        Value::String(text) => text.clone(),
        Value::Number(number) => number.to_string(),
        _ => String::new(),
    });
    let kind = match error
        .get("status")
        .and_then(Value::as_str)
        .unwrap_or_default()
    {
        "INVALID_ARGUMENT" => LocalCoreSyncErrorKind::InvalidRequest,
        "UNAUTHENTICATED" => LocalCoreSyncErrorKind::Authentication,
        "PERMISSION_DENIED" => LocalCoreSyncErrorKind::PermissionDenied,
        "NOT_FOUND" => LocalCoreSyncErrorKind::NotFound,
        "RESOURCE_EXHAUSTED" => LocalCoreSyncErrorKind::RateLimit,
        "UNAVAILABLE" => LocalCoreSyncErrorKind::Overloaded,
        _ => LocalCoreSyncErrorKind::ServerError,
    };
    let code = code.filter(|value| !value.is_empty());
    Some((message, code, kind))
}

#[cfg(test)]
#[path = "format_matrix/tests.rs"]
mod tests;

#[cfg(test)]
mod structured_entry_tests {
    use super::StreamingStandardTerminalObserver;
    use aether_contracts::ExecutionStreamTerminalSummary;
    use serde_json::{json, Value};

    fn report_context() -> Value {
        json!({
            "provider_api_format": "openai:responses",
            "client_api_format": "openai:responses",
            "mapped_model": "gpt-5-codex",
        })
    }

    /// 用 SSE 入口观测一组事件。这是 C5 之前 WebSocket 走的路径：把结构化事件
    /// 拼成 `data: {json}` 再交给解析器。
    fn summary_via_push_line(events: &[Value]) -> ExecutionStreamTerminalSummary {
        let context = report_context();
        let mut observer = StreamingStandardTerminalObserver::default();
        for event in events {
            observer
                .push_line(&context, format!("data: {event}\n\n").into_bytes())
                .expect("the SSE entry must accept these events");
        }
        observer
            .finish(&context)
            .expect("the observer must finish")
            .unwrap_or_default()
    }

    /// 用结构化入口观测同一组事件。这是 C5 之后的路径。
    fn summary_via_push_event(events: &[Value]) -> ExecutionStreamTerminalSummary {
        let context = report_context();
        let mut observer = StreamingStandardTerminalObserver::default();
        for event in events {
            observer
                .push_event(&context, event)
                .expect("the structured entry must accept these events");
        }
        observer
            .finish(&context)
            .expect("the observer must finish")
            .unwrap_or_default()
    }

    fn assert_entries_agree(label: &str, events: &[Value]) {
        let via_line = summary_via_push_line(events);
        let via_event = summary_via_push_event(events);
        assert_eq!(
            via_line, via_event,
            "the SSE entry and the structured entry must produce identical summaries for {label}"
        );
    }

    fn created() -> Value {
        json!({"type": "response.created", "response": {"id": "resp_diff", "model": "gpt-5-codex"}})
    }

    fn text_delta(piece: &str) -> Value {
        json!({
            "type": "response.output_text.delta",
            "item_id": "msg_diff",
            "output_index": 0,
            "content_index": 0,
            "delta": piece,
        })
    }

    /// 批量事件：WS 一帧可以带多个协议事件，逐个喂入的结果必须和逐行喂入一致。
    #[test]
    fn a_batched_delta_sequence_agrees_across_both_entries() {
        let events = vec![
            created(),
            text_delta("he"),
            text_delta("ll"),
            text_delta("o"),
            json!({
                "type": "response.completed",
                "response": {
                    "id": "resp_diff",
                    "model": "gpt-5-codex",
                    "status": "completed",
                    "usage": {"input_tokens": 11, "output_tokens": 3, "total_tokens": 14},
                },
            }),
        ];
        assert_entries_agree("a batched delta sequence", &events);
        let summary = summary_via_push_event(&events);
        assert!(summary.observed_finish);
        assert_eq!(summary.response_id.as_deref(), Some("resp_diff"));
        let usage = summary
            .standardized_usage
            .as_ref()
            .expect("completed carries usage");
        assert_eq!(usage.input_tokens, 11);
        assert_eq!(usage.output_tokens, 3);
    }

    /// 合法 `response.incomplete`：C1 定过的语义（终态、可计费），两条入口必须
    /// 得到同一个摘要，尤其是 finish_reason 与 parser_error 的取值。
    #[test]
    fn a_legitimate_incomplete_agrees_across_both_entries() {
        let events = vec![
            created(),
            text_delta("partial"),
            json!({
                "type": "response.incomplete",
                "response": {
                    "id": "resp_diff",
                    "model": "gpt-5-codex",
                    "status": "incomplete",
                    "incomplete_details": {"reason": "max_output_tokens"},
                    "usage": {"input_tokens": 7, "output_tokens": 5, "total_tokens": 12},
                },
            }),
        ];
        assert_entries_agree("a legitimate incomplete", &events);
        let summary = summary_via_push_event(&events);
        assert!(summary.observed_finish);
        assert!(
            summary.parser_error.is_none(),
            "a legitimate incomplete is not a parser error: {:?}",
            summary.parser_error
        );
    }

    #[test]
    fn a_terminal_error_agrees_across_both_entries() {
        let events = vec![
            created(),
            json!({
                "type": "error",
                "error": {"type": "server_error", "message": "upstream exploded"},
            }),
        ];
        assert_entries_agree("a terminal error", &events);
        let summary = summary_via_push_event(&events);
        assert!(summary.observed_finish);
        assert_eq!(summary.finish_reason.as_deref(), Some("error"));
        assert!(summary.parser_error.is_some());
    }

    #[test]
    fn a_response_failed_event_agrees_across_both_entries() {
        assert_entries_agree(
            "a response.failed event",
            &[
                created(),
                json!({
                    "type": "response.failed",
                    "response": {
                        "id": "resp_diff",
                        "model": "gpt-5-codex",
                        "status": "failed",
                        "error": {"type": "server_error", "message": "generation failed"},
                    },
                }),
            ],
        );
    }

    /// 未知事件只增计数、不改终态判定，两条入口的计数必须一致。
    #[test]
    fn unknown_events_agree_across_both_entries() {
        let events = vec![
            created(),
            json!({"type": "response.some_future_event", "payload": {"anything": true}}),
            json!({"type": "response.another_future_event"}),
            json!({
                "type": "response.completed",
                "response": {
                    "id": "resp_diff",
                    "model": "gpt-5-codex",
                    "status": "completed",
                    "usage": {"input_tokens": 1, "output_tokens": 1, "total_tokens": 2},
                },
            }),
        ];
        assert_entries_agree("unknown events", &events);
        let summary = summary_via_push_event(&events);
        assert!(summary.observed_finish);
        assert!(
            summary.unknown_event_count > 0,
            "unknown events are counted"
        );
    }

    /// 供应商声明的 service tier 通过两条入口都要落到摘要上。
    #[test]
    fn a_service_tier_agrees_across_both_entries() {
        assert_entries_agree(
            "a declared service tier",
            &[
                json!({
                    "type": "response.created",
                    "response": {
                        "id": "resp_diff",
                        "model": "gpt-5-codex",
                        "service_tier": "priority",
                    },
                }),
                json!({
                    "type": "response.completed",
                    "response": {
                        "id": "resp_diff",
                        "model": "gpt-5-codex",
                        "status": "completed",
                        "service_tier": "priority",
                        "usage": {"input_tokens": 2, "output_tokens": 2, "total_tokens": 4},
                    },
                }),
            ],
        );
    }

    /// 没有任何供应商终态事件。注意 `finish()` 会补一个 `stop`（这是 HTTP 与
    /// WebSocket 共享的既有行为，C5 不改），所以 `observed_finish` 为真而 usage
    /// 缺失——真正的「缺终态」判定看的是 usage 与被捕获的 body。这里要钉住的是
    /// 两条入口在这种不完整序列上仍然给出同一个摘要。
    #[test]
    fn a_missing_terminal_agrees_across_both_entries() {
        let events = vec![created(), text_delta("truncated")];
        assert_entries_agree("a missing terminal", &events);
        let summary = summary_via_push_event(&events);
        assert_eq!(summary.finish_reason.as_deref(), Some("stop"));
        assert!(
            summary.standardized_usage.is_none(),
            "a synthesized finish carries no usage"
        );
    }

    /// `openai:image` 没有结构化入口：必须显式报错，让调用方标记 parser_error，
    /// 而不是静默丢掉事件、把摘要留成「未观察到终态」。
    #[test]
    fn the_image_format_rejects_the_structured_entry() {
        let context = json!({
            "provider_api_format": "openai:image",
            "client_api_format": "openai:image",
            "mapped_model": "gpt-image-1",
        });
        let mut observer = StreamingStandardTerminalObserver::default();
        let error = observer
            .push_event(&context, &json!({"type": "image_generation.completed"}))
            .expect_err("openai:image has no structured entry");
        assert!(
            error.to_string().contains("structured event entry"),
            "the error must name the missing entry: {error}"
        );

        observer.disable_with_error(error.to_string());
        let summary = observer
            .latest_summary()
            .expect("disable_with_error records a summary");
        assert!(summary.parser_error.is_some());
    }
}
