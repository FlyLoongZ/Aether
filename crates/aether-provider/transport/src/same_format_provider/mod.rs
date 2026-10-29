use std::collections::BTreeMap;
use std::fmt;

use serde::Serialize;
use serde_json::Value;

use crate::anthropic_compat::{
    resolve_anthropic_compatibility_profile, AnthropicCompatibilityProfile,
};
use crate::antigravity::is_antigravity_provider_transport;
use crate::auth::{
    build_complete_passthrough_headers, build_complete_passthrough_headers_with_auth,
    replace_upstream_auth_headers, resolve_local_gemini_auth, resolve_local_openai_bearer_auth,
    resolve_local_standard_auth,
};
use crate::claude_code::{
    build_claude_code_passthrough_headers, current_claude_code_transport_identity_profile,
    local_claude_code_transport_unsupported_reason_with_network,
};
use crate::gemini_cli::is_gemini_cli_provider_transport;
use crate::headers::{force_identity_accept_encoding, upstream_credential_header_names};
use crate::kiro::{
    build_kiro_provider_headers, build_kiro_provider_request_body, is_kiro_provider_transport,
    local_kiro_request_transport_unsupported_reason_with_network, KiroAuthConfig,
    KiroProviderHeadersInput,
};
use crate::policy::{
    local_gemini_transport_unsupported_reason_with_network,
    local_standard_transport_unsupported_reason_with_network,
};
use crate::rules::{
    apply_local_body_rules_with_request_headers, apply_local_header_rules_with_request_headers,
};
use crate::snapshot::GatewayProviderTransportSnapshot;
use crate::vertex::{
    is_vertex_service_account_transport_context, is_vertex_transport_context,
    local_vertex_gemini_transport_unsupported_reason_with_network,
};

use crate::{build_transport_request_url_for_request_body, TransportRequestUrlParams};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SameFormatProviderFamily {
    Standard,
    Gemini,
}

#[derive(Debug, Clone, Copy)]
pub struct SameFormatProviderRequestBehaviorParams<'a> {
    pub require_streaming: bool,
    pub provider_api_format: &'a str,
    pub report_kind: &'static str,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SameFormatProviderRequestBehavior {
    pub is_antigravity: bool,
    pub is_gemini_cli: bool,
    pub is_claude_code: bool,
    pub is_claude_code_transport: bool,
    pub anthropic_compatibility_profile: AnthropicCompatibilityProfile,
    pub is_vertex: bool,
    pub is_kiro: bool,
    pub upstream_is_stream: bool,
    pub force_body_stream_field: bool,
    pub report_kind: &'static str,
}

#[derive(Clone, Copy)]
pub struct SameFormatProviderRequestBodyInput<'a> {
    pub body_json: &'a Value,
    pub mapped_model: &'a str,
    pub client_api_format: &'a str,
    pub provider_api_format: &'a str,
    pub source_model: Option<&'a str>,
    pub family: SameFormatProviderFamily,
    pub body_rules: Option<&'a Value>,
    pub request_headers: Option<&'a http::HeaderMap>,
    pub upstream_is_stream: bool,
    pub force_body_stream_field: bool,
    pub kiro_auth_config: Option<&'a KiroAuthConfig>,
    pub is_claude_code: bool,
    pub is_vertex: bool,
    pub enable_model_directives: bool,
}

impl fmt::Debug for SameFormatProviderRequestBodyInput<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SameFormatProviderRequestBodyInput")
            .field(
                "body_json_bytes",
                &serde_json::to_vec(self.body_json)
                    .ok()
                    .map(|bytes| bytes.len()),
            )
            .field("mapped_model", &self.mapped_model)
            .field("client_api_format", &self.client_api_format)
            .field("provider_api_format", &self.provider_api_format)
            .field("source_model", &self.source_model)
            .field("family", &self.family)
            .field("has_body_rules", &self.body_rules.is_some())
            .field(
                "request_header_names",
                &self
                    .request_headers
                    .map(|headers| headers.keys().map(|name| name.as_str()).collect::<Vec<_>>()),
            )
            .field("upstream_is_stream", &self.upstream_is_stream)
            .field("force_body_stream_field", &self.force_body_stream_field)
            .field("has_kiro_auth_config", &self.kiro_auth_config.is_some())
            .field("is_claude_code", &self.is_claude_code)
            .field("is_vertex", &self.is_vertex)
            .field("enable_model_directives", &self.enable_model_directives)
            .finish()
    }
}

#[derive(Clone, PartialEq, Eq, Serialize)]
pub struct SameFormatProviderRequestBodyOutput {
    pub body: Value,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub compatibility_edits: Vec<SameFormatProviderCompatibilityEdit>,
}

impl fmt::Debug for SameFormatProviderRequestBodyOutput {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SameFormatProviderRequestBodyOutput")
            .field(
                "body_bytes",
                &serde_json::to_vec(&self.body).ok().map(|bytes| bytes.len()),
            )
            .field(
                "compatibility_edits",
                &self
                    .compatibility_edits
                    .iter()
                    .map(|edit| (edit.field.as_str(), edit.action, edit.detail.len()))
                    .collect::<Vec<_>>(),
            )
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SameFormatProviderCompatibilityEdit {
    pub field: String,
    pub action: SameFormatProviderCompatibilityEditAction,
    pub detail: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SameFormatProviderCompatibilityEditAction {
    RuntimeRewrite,
    ProviderCompatibilityRewrite,
    ProviderEnvelope,
    OperatorRule,
}

#[derive(Clone, Copy)]
pub struct SameFormatProviderUpstreamUrlParams<'a> {
    pub provider_api_format: &'a str,
    pub mapped_model: &'a str,
    pub upstream_is_stream: bool,
    pub request_query: Option<&'a str>,
    pub kiro_api_region: Option<&'a str>,
    pub api_operation: Option<aether_ai_formats::ApiOperation>,
    pub provider_request_body: Option<&'a Value>,
}

impl fmt::Debug for SameFormatProviderUpstreamUrlParams<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SameFormatProviderUpstreamUrlParams")
            .field("provider_api_format", &self.provider_api_format)
            .field("mapped_model", &self.mapped_model)
            .field("upstream_is_stream", &self.upstream_is_stream)
            .field("has_request_query", &self.request_query.is_some())
            .field("request_query_len", &self.request_query.map(str::len))
            .field("kiro_api_region", &self.kiro_api_region)
            .field("api_operation", &self.api_operation)
            .field(
                "has_provider_request_body",
                &self.provider_request_body.is_some(),
            )
            .finish()
    }
}

#[derive(Clone, Copy)]
pub struct SameFormatProviderHeadersInput<'a> {
    pub headers: &'a http::HeaderMap,
    pub provider_request_body: &'a Value,
    pub original_request_body: &'a Value,
    pub header_rules: Option<&'a Value>,
    pub behavior: SameFormatProviderRequestBehavior,
    pub api_operation: Option<aether_ai_formats::ApiOperation>,
    pub auth_header: Option<&'a str>,
    pub auth_value: Option<&'a str>,
    pub extra_headers: &'a BTreeMap<String, String>,
    pub kiro_auth_config: Option<&'a KiroAuthConfig>,
    pub kiro_machine_id: Option<&'a str>,
}

impl fmt::Debug for SameFormatProviderHeadersInput<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SameFormatProviderHeadersInput")
            .field(
                "request_header_names",
                &self
                    .headers
                    .keys()
                    .map(|name| name.as_str())
                    .collect::<Vec<_>>(),
            )
            .field(
                "provider_request_body_bytes",
                &serde_json::to_vec(self.provider_request_body)
                    .ok()
                    .map(|bytes| bytes.len()),
            )
            .field(
                "original_request_body_bytes",
                &serde_json::to_vec(self.original_request_body)
                    .ok()
                    .map(|bytes| bytes.len()),
            )
            .field("has_header_rules", &self.header_rules.is_some())
            .field("behavior", &self.behavior)
            .field("api_operation", &self.api_operation)
            .field("auth_header", &self.auth_header)
            .field("has_auth_value", &self.auth_value.is_some())
            .field(
                "extra_header_names",
                &self.extra_headers.keys().collect::<Vec<_>>(),
            )
            .field("has_kiro_auth_config", &self.kiro_auth_config.is_some())
            .field("has_kiro_machine_id", &self.kiro_machine_id.is_some())
            .finish()
    }
}

pub fn classify_same_format_provider_request_behavior(
    transport: &GatewayProviderTransportSnapshot,
    params: SameFormatProviderRequestBehaviorParams<'_>,
) -> SameFormatProviderRequestBehavior {
    classify_same_format_provider_request_behavior_for_operation(transport, params, None)
}

pub fn classify_same_format_provider_request_behavior_for_operation(
    transport: &GatewayProviderTransportSnapshot,
    params: SameFormatProviderRequestBehaviorParams<'_>,
    api_operation: Option<aether_ai_formats::ApiOperation>,
) -> SameFormatProviderRequestBehavior {
    let is_antigravity = is_antigravity_provider_transport(transport);
    let is_gemini_cli = is_gemini_cli_provider_transport(transport);
    let is_claude_code_transport = transport
        .provider
        .provider_type
        .trim()
        .eq_ignore_ascii_case("claude_code");
    let anthropic_compatibility_profile =
        resolve_anthropic_compatibility_profile(transport, params.provider_api_format);
    let is_claude_code = anthropic_compatibility_profile.uses_claude_code_compatibility();
    let is_vertex = is_vertex_transport_context(transport);
    let is_kiro = is_kiro_provider_transport(transport);
    let gemini_cli_requires_upstream_streaming = is_gemini_cli
        && crate::gemini_cli::gemini_cli_v1internal_requires_upstream_streaming(
            params.provider_api_format,
            params.require_streaming,
        );
    let operation_requires_sync = matches!(
        api_operation,
        Some(
            aether_ai_formats::ApiOperation::ClaudeCountTokens
                | aether_ai_formats::ApiOperation::OpenAiMemoriesSummarize
        )
    );
    let upstream_is_stream = !operation_requires_sync
        && aether_ai_formats::resolve_upstream_is_stream_for_provider(
            transport.endpoint.config.as_ref(),
            transport.provider.provider_type.as_str(),
            params.provider_api_format,
            params.require_streaming,
            is_kiro || is_antigravity || gemini_cli_requires_upstream_streaming,
        );
    let force_body_stream_field = !operation_requires_sync
        && aether_ai_formats::api_format_uses_body_stream_field(params.provider_api_format)
        && aether_ai_formats::endpoint_config_forces_upstream_stream_policy(
            transport.endpoint.config.as_ref(),
        );
    let report_kind = if is_kiro && !params.require_streaming {
        "claude_cli_sync_finalize"
    } else if (is_gemini_cli || is_antigravity) && !params.require_streaming {
        match params.report_kind {
            "gemini_chat_sync_success" => "gemini_chat_sync_finalize",
            "gemini_cli_sync_success" => "gemini_cli_sync_finalize",
            _ => params.report_kind,
        }
    } else {
        params.report_kind
    };

    SameFormatProviderRequestBehavior {
        is_antigravity,
        is_gemini_cli,
        is_claude_code,
        is_claude_code_transport,
        anthropic_compatibility_profile,
        is_vertex,
        is_kiro,
        upstream_is_stream,
        force_body_stream_field,
        report_kind,
    }
}

pub fn build_same_format_provider_request_body(
    input: SameFormatProviderRequestBodyInput<'_>,
) -> Option<Value> {
    build_same_format_provider_request_body_inner(
        input,
        None,
        aether_ai_formats::OpenAiResponsesReasoningReplayPolicy::OpenAiItemIds,
        false,
    )
}

pub fn build_same_format_provider_request_body_with_compatibility_report(
    input: SameFormatProviderRequestBodyInput<'_>,
) -> Option<SameFormatProviderRequestBodyOutput> {
    build_same_format_provider_request_body_with_compatibility_report_and_reasoning_replay_policy(
        input,
        aether_ai_formats::OpenAiResponsesReasoningReplayPolicy::OpenAiItemIds,
    )
}

pub fn build_same_format_provider_request_body_with_compatibility_report_and_reasoning_replay_policy(
    input: SameFormatProviderRequestBodyInput<'_>,
    reasoning_replay_policy: aether_ai_formats::OpenAiResponsesReasoningReplayPolicy,
) -> Option<SameFormatProviderRequestBodyOutput> {
    build_same_format_provider_request_body_for_operation(input, reasoning_replay_policy, None)
}

pub fn build_same_format_provider_request_body_for_operation(
    input: SameFormatProviderRequestBodyInput<'_>,
    reasoning_replay_policy: aether_ai_formats::OpenAiResponsesReasoningReplayPolicy,
    api_operation: Option<aether_ai_formats::ApiOperation>,
) -> Option<SameFormatProviderRequestBodyOutput> {
    let native_memories =
        api_operation == Some(aether_ai_formats::ApiOperation::OpenAiMemoriesSummarize);
    if native_memories
        && (!aether_ai_formats::api_format_alias_matches(
            input.provider_api_format,
            "openai:responses",
        ) || !aether_ai_formats::api_format_alias_matches(
            input.client_api_format,
            "openai:responses",
        ) || input.kiro_auth_config.is_some()
            || input.is_claude_code)
    {
        return None;
    }
    let mut compatibility_edits = Vec::new();
    let body = build_same_format_provider_request_body_inner(
        input,
        Some(&mut compatibility_edits),
        reasoning_replay_policy,
        native_memories,
    )?;
    Some(SameFormatProviderRequestBodyOutput {
        body,
        compatibility_edits,
    })
}

pub fn enforce_same_format_provider_api_operation_body_policy(
    body: &mut Value,
    api_operation: Option<aether_ai_formats::ApiOperation>,
) -> bool {
    if !matches!(
        api_operation,
        Some(
            aether_ai_formats::ApiOperation::ClaudeCountTokens
                | aether_ai_formats::ApiOperation::OpenAiMemoriesSummarize
        )
    ) {
        return false;
    }
    body.as_object_mut()
        .is_some_and(|object| object.remove("stream").is_some())
}

fn build_same_format_provider_request_body_inner(
    input: SameFormatProviderRequestBodyInput<'_>,
    mut compatibility_edits: Option<&mut Vec<SameFormatProviderCompatibilityEdit>>,
    reasoning_replay_policy: aether_ai_formats::OpenAiResponsesReasoningReplayPolicy,
    native_memories: bool,
) -> Option<Value> {
    if let Some(kiro_auth_config) = input.kiro_auth_config {
        let body = build_kiro_provider_request_body(
            input.body_json,
            input.mapped_model,
            kiro_auth_config,
            input.body_rules,
            input.request_headers,
        )?;
        record_compatibility_edit(
            &mut compatibility_edits,
            "$",
            SameFormatProviderCompatibilityEditAction::ProviderEnvelope,
            "wrapped same-format request in Kiro provider envelope",
        );
        return Some(body);
    }

    if embedding_multimodal_input_requires_aliyun_provider(
        input.client_api_format,
        input.provider_api_format,
        input.body_json,
    ) {
        return None;
    }

    let mut provider_request_body = if aether_ai_formats::api_format_alias_matches(
        input.client_api_format,
        input.provider_api_format,
    ) {
        let request_body_object = input.body_json.as_object()?;
        serde_json::Map::from_iter(
            request_body_object
                .iter()
                .map(|(key, value)| (key.clone(), value.clone())),
        )
    } else {
        aether_ai_formats::convert_request_pure_with_context(
            input.client_api_format,
            input.provider_api_format,
            input.body_json,
            &aether_ai_formats::FormatContext::default().with_mapped_model(input.mapped_model),
        )
        .ok()?
        .value
        .as_object()?
        .clone()
    };
    match input.family {
        SameFormatProviderFamily::Standard => {
            let previous_model = provider_request_body.get("model").cloned();
            provider_request_body.insert(
                "model".to_string(),
                Value::String(input.mapped_model.to_string()),
            );
            if previous_model != Some(Value::String(input.mapped_model.to_string())) {
                record_compatibility_edit(
                    &mut compatibility_edits,
                    "model",
                    SameFormatProviderCompatibilityEditAction::RuntimeRewrite,
                    "rewrote request model to mapped upstream model",
                );
            }
        }
        SameFormatProviderFamily::Gemini => {
            if aether_ai_formats::api_format_alias_matches(
                input.provider_api_format,
                "gemini:interactions",
            ) {
                let rewrite_field = if provider_request_body.contains_key("model") {
                    "model"
                } else if provider_request_body.contains_key("agent") {
                    "agent"
                } else {
                    "model"
                };
                let previous_value = provider_request_body.get(rewrite_field).cloned();
                provider_request_body.insert(
                    rewrite_field.to_string(),
                    Value::String(input.mapped_model.to_string()),
                );
                if previous_value != Some(Value::String(input.mapped_model.to_string())) {
                    record_compatibility_edit(
                        &mut compatibility_edits,
                        rewrite_field,
                        SameFormatProviderCompatibilityEditAction::RuntimeRewrite,
                        "rewrote Gemini Interactions routing field to mapped upstream target",
                    );
                }
            } else {
                if provider_request_body.remove("model").is_some() {
                    record_compatibility_edit(
                        &mut compatibility_edits,
                        "model",
                        SameFormatProviderCompatibilityEditAction::RuntimeRewrite,
                        "removed top-level model because Gemini model is carried by the upstream URL",
                    );
                }
            }
        }
    }
    let mut provider_request_body = Value::Object(provider_request_body);
    if input.is_claude_code {
        let before = compatibility_edits
            .is_some()
            .then(|| provider_request_body.clone());
        crate::claude_code::sanitize_claude_code_request_body(&mut provider_request_body);
        if before.is_some_and(|before| before != provider_request_body) {
            record_compatibility_edit(
                &mut compatibility_edits,
                "body",
                SameFormatProviderCompatibilityEditAction::ProviderCompatibilityRewrite,
                "sanitized Claude Code request body for provider compatibility",
            );
        }
    }
    if input.enable_model_directives {
        if let Some(source_model) = input.source_model {
            let before = compatibility_edits
                .is_some()
                .then(|| provider_request_body.clone());
            aether_ai_formats::apply_model_directive_overrides_from_model(
                &mut provider_request_body,
                input.provider_api_format,
                input.mapped_model,
                source_model,
            );
            if before.is_some_and(|before| before != provider_request_body) {
                record_compatibility_edit(
                    &mut compatibility_edits,
                    "model_directives",
                    SameFormatProviderCompatibilityEditAction::RuntimeRewrite,
                    "applied model directive request body overrides",
                );
            }
        }
    }
    let before_body_rules = compatibility_edits
        .is_some()
        .then(|| provider_request_body.clone());
    if !apply_local_body_rules_with_request_headers(
        &mut provider_request_body,
        input.body_rules,
        Some(input.body_json),
        input.request_headers,
    ) {
        return None;
    }
    if before_body_rules.is_some_and(|before| before != provider_request_body) {
        record_compatibility_edit(
            &mut compatibility_edits,
            "body_rules",
            SameFormatProviderCompatibilityEditAction::OperatorRule,
            "applied configured provider body rules",
        );
    }
    if native_memories {
        // 记忆端点使用原生 JSON，不注入 Responses 的 input/store/include/stream，
        // 也不通过 Responses 规则归一化 traces。
        return Some(provider_request_body);
    }
    if matches!(input.family, SameFormatProviderFamily::Gemini)
        && aether_ai_formats::api_format_alias_matches(
            input.provider_api_format,
            "gemini:generate_content",
        )
    {
        let before_mixed_tool_compatibility = compatibility_edits
            .is_some()
            .then(|| provider_request_body.clone());
        aether_ai_formats::ensure_server_side_tool_invocations_for_mixed_tools(
            &mut provider_request_body,
        )?;
        if before_mixed_tool_compatibility.is_some_and(|before| before != provider_request_body) {
            record_compatibility_edit(
                &mut compatibility_edits,
                "toolConfig.includeServerSideToolInvocations",
                SameFormatProviderCompatibilityEditAction::ProviderCompatibilityRewrite,
                "enabled Gemini server-side tool invocations for mixed built-in and function tools",
            );
        }
    }
    if input.is_vertex
        && matches!(input.family, SameFormatProviderFamily::Gemini)
        && aether_ai_formats::api_format_alias_matches(
            input.provider_api_format,
            "gemini:generate_content",
        )
    {
        let stripped = strip_gemini_function_response_ids(&mut provider_request_body);
        if stripped > 0 {
            record_compatibility_edit(
                &mut compatibility_edits,
                "contents[].parts[].functionResponse.id",
                SameFormatProviderCompatibilityEditAction::ProviderCompatibilityRewrite,
                format!(
                    "stripped {stripped} Gemini functionResponse id field(s) rejected by upstreams"
                ),
            );
        }
    }
    let require_body_stream_field = input.force_body_stream_field
        || input
            .body_json
            .as_object()
            .is_some_and(|object| object.contains_key("stream"));
    let previous_stream = provider_request_body.get("stream").cloned();
    aether_ai_formats::enforce_request_body_stream_field(
        &mut provider_request_body,
        input.provider_api_format,
        input.upstream_is_stream,
        require_body_stream_field,
    );
    if previous_stream != provider_request_body.get("stream").cloned() {
        record_compatibility_edit(
            &mut compatibility_edits,
            "stream",
            SameFormatProviderCompatibilityEditAction::RuntimeRewrite,
            format!(
                "enforced provider stream policy with upstream_is_stream={}",
                input.upstream_is_stream
            ),
        );
    }
    let stripped_reasoning_items =
        aether_ai_formats::strip_incompatible_openai_responses_reasoning_items_with_policy(
            &mut provider_request_body,
            input.provider_api_format,
            reasoning_replay_policy,
        );
    if stripped_reasoning_items > 0 {
        record_compatibility_edit(
            &mut compatibility_edits,
            "input[].id",
            SameFormatProviderCompatibilityEditAction::ProviderCompatibilityRewrite,
            format!(
                "stripped {stripped_reasoning_items} non-replayable OpenAI Responses reasoning item(s)"
            ),
        );
    }
    let provider_model = provider_request_body
        .get("model")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or(input.mapped_model)
        .to_string();
    if !same_format_reasoning_effort_supported(
        &provider_request_body,
        input.provider_api_format,
        &provider_model,
        input.source_model.unwrap_or(input.mapped_model),
    ) {
        return None;
    }
    if aether_ai_formats::finalize_openai_provider_request_with_codex_model_capabilities_and_reasoning_replay_policy(
        &mut provider_request_body,
        aether_ai_formats::OpenAiProviderRequestFinalization {
            source_api_format: input.provider_api_format,
            provider_api_format: input.provider_api_format,
            provider_type: "",
            provider_model: &provider_model,
            source_model: input.source_model.unwrap_or(input.mapped_model),
            body_rules: input.body_rules,
            upstream_is_stream: input.upstream_is_stream,
            require_body_stream_field,
        },
        None,
        reasoning_replay_policy,
    )
    .is_err()
    {
        return None;
    }
    Some(provider_request_body)
}

fn same_format_reasoning_effort_supported(
    body: &Value,
    provider_api_format: &str,
    provider_model: &str,
    source_model: &str,
) -> bool {
    let provider_api_format = aether_ai_formats::normalize_api_format_alias(provider_api_format);
    let effort = match provider_api_format.as_str() {
        "openai:chat" => body.get("reasoning_effort"),
        "openai:responses" | "openai:responses:compact" | "openai:search" => body
            .get("reasoning")
            .and_then(Value::as_object)
            .and_then(|reasoning| reasoning.get("effort")),
        _ => return true,
    };
    let Some(effort) = effort.and_then(Value::as_str) else {
        return true;
    };
    let Some(effort) = aether_ai_formats::ReasoningEffort::parse(effort) else {
        return true;
    };
    aether_ai_formats::reasoning_effort_supported_for_model(
        provider_api_format.as_str(),
        provider_model,
        source_model,
        effort,
    )
}

fn record_compatibility_edit(
    edits: &mut Option<&mut Vec<SameFormatProviderCompatibilityEdit>>,
    field: impl Into<String>,
    action: SameFormatProviderCompatibilityEditAction,
    detail: impl Into<String>,
) {
    if let Some(edits) = edits.as_mut() {
        edits.push(SameFormatProviderCompatibilityEdit {
            field: field.into(),
            action,
            detail: detail.into(),
        });
    }
}

fn embedding_multimodal_input_requires_aliyun_provider(
    client_api_format: &str,
    provider_api_format: &str,
    body_json: &Value,
) -> bool {
    aether_ai_formats::is_embedding_api_format(client_api_format)
        && embedding_input_is_multimodal(body_json.get("input"))
        && aether_ai_formats::normalize_api_format_alias(provider_api_format)
            != "aliyun:multimodal_embedding"
}

fn embedding_input_is_multimodal(value: Option<&Value>) -> bool {
    value
        .and_then(Value::as_array)
        .is_some_and(|items| !items.is_empty() && items.iter().all(embedding_content_is_multimodal))
}

fn embedding_content_is_multimodal(value: &Value) -> bool {
    value.as_object().is_some_and(|object| {
        ["text", "image", "video", "multi_images"]
            .iter()
            .any(|key| object.contains_key(*key))
    })
}

fn strip_gemini_function_response_ids(value: &mut Value) -> usize {
    match value {
        Value::Object(object) => {
            let mut stripped = 0;
            for key in ["functionResponse", "function_response"] {
                if let Some(function_response) = object.get_mut(key).and_then(Value::as_object_mut)
                {
                    if function_response.remove("id").is_some() {
                        stripped += 1;
                    }
                }
            }
            for child in object.values_mut() {
                stripped += strip_gemini_function_response_ids(child);
            }
            stripped
        }
        Value::Array(items) => {
            let mut stripped = 0;
            for item in items {
                stripped += strip_gemini_function_response_ids(item);
            }
            stripped
        }
        _ => 0,
    }
}

pub fn build_same_format_provider_upstream_url(
    transport: &GatewayProviderTransportSnapshot,
    params: SameFormatProviderUpstreamUrlParams<'_>,
) -> Option<String> {
    build_transport_request_url_for_request_body(
        transport,
        TransportRequestUrlParams {
            provider_api_format: params.provider_api_format,
            mapped_model: Some(params.mapped_model),
            upstream_is_stream: params.upstream_is_stream,
            request_query: params.request_query,
            kiro_api_region: params.kiro_api_region,
            api_operation: params.api_operation,
        },
        params.provider_request_body,
    )
}

pub fn build_same_format_provider_headers(
    input: SameFormatProviderHeadersInput<'_>,
) -> Option<BTreeMap<String, String>> {
    if let Some(kiro_auth_config) = input.kiro_auth_config {
        return build_kiro_provider_headers(KiroProviderHeadersInput {
            headers: input.headers,
            provider_request_body: input.provider_request_body,
            original_request_body: input.original_request_body,
            header_rules: input.header_rules,
            auth_header: input.auth_header.unwrap_or_default(),
            auth_value: input.auth_value.unwrap_or_default(),
            auth_config: kiro_auth_config,
            machine_id: input.kiro_machine_id.unwrap_or_default(),
        });
    }

    let auth_header = input.auth_header.unwrap_or_default();
    let auth_value = input.auth_value.unwrap_or_default();
    let mut provider_request_headers = if input.behavior.is_claude_code_transport {
        build_claude_code_passthrough_headers(
            input.headers,
            auth_header,
            auth_value,
            input.extra_headers,
            input.behavior.upstream_is_stream,
        )
    } else if input.behavior.is_vertex {
        build_complete_passthrough_headers(
            input.headers,
            input.extra_headers,
            Some("application/json"),
        )
    } else {
        build_complete_passthrough_headers_with_auth(
            input.headers,
            auth_header,
            auth_value,
            input.extra_headers,
            Some("application/json"),
        )
    };

    let mut protected_headers = upstream_credential_header_names().to_vec();
    protected_headers.push("content-type");
    if !apply_local_header_rules_with_request_headers(
        &mut provider_request_headers,
        input.header_rules,
        &protected_headers,
        input.provider_request_body,
        Some(input.original_request_body),
        Some(input.headers),
    ) {
        return None;
    }
    if let (Some(auth_header), Some(auth_value)) = (input.auth_header, input.auth_value) {
        replace_upstream_auth_headers(&mut provider_request_headers, auth_header, auth_value);
    } else {
        replace_upstream_auth_headers(&mut provider_request_headers, "", "");
    }
    let claude_code_profile = current_claude_code_transport_identity_profile();
    if input.behavior.is_claude_code_transport {
        claude_code_profile.apply_fixed_headers(
            &mut provider_request_headers,
            input.behavior.upstream_is_stream,
        );
    }
    if input.behavior.is_claude_code_transport || input.behavior.is_claude_code {
        claude_code_profile.apply_beta_policy(&mut provider_request_headers, input.api_operation);
    }
    if matches!(
        input.api_operation,
        Some(aether_ai_formats::ApiOperation::ClaudeCountTokens)
    ) {
        provider_request_headers.insert("accept".to_string(), "application/json".to_string());
    } else if input.behavior.upstream_is_stream {
        provider_request_headers.insert("accept".to_string(), "text/event-stream".to_string());
        force_identity_accept_encoding(&mut provider_request_headers);
    }
    let declared_connection_headers =
        crate::headers::declared_connection_header_names(input.headers, input.extra_headers);
    crate::headers::remove_declared_connection_headers(
        &mut provider_request_headers,
        &declared_connection_headers,
    );
    Some(provider_request_headers)
}

pub fn same_format_provider_transport_supported(
    behavior: &SameFormatProviderRequestBehavior,
    transport: &GatewayProviderTransportSnapshot,
    family: SameFormatProviderFamily,
    api_format: &str,
) -> bool {
    same_format_provider_transport_unsupported_reason(behavior, transport, family, api_format)
        .is_none()
}

pub fn same_format_provider_transport_unsupported_reason(
    behavior: &SameFormatProviderRequestBehavior,
    transport: &GatewayProviderTransportSnapshot,
    family: SameFormatProviderFamily,
    api_format: &str,
) -> Option<&'static str> {
    if behavior.is_kiro {
        local_kiro_request_transport_unsupported_reason_with_network(transport)
    } else if behavior.is_antigravity {
        None
    } else if behavior.is_claude_code_transport {
        local_claude_code_transport_unsupported_reason_with_network(transport, api_format)
    } else if behavior.is_vertex {
        local_vertex_gemini_transport_unsupported_reason_with_network(transport)
    } else {
        match family {
            SameFormatProviderFamily::Standard => {
                local_standard_transport_unsupported_reason_with_network(transport, api_format)
            }
            SameFormatProviderFamily::Gemini => {
                local_gemini_transport_unsupported_reason_with_network(transport, api_format)
            }
        }
    }
}

pub fn same_format_provider_transport_unsupported_reason_for_trace(
    transport: &GatewayProviderTransportSnapshot,
    provider_api_format: &str,
) -> Option<&'static str> {
    let normalized_api_format =
        match aether_ai_formats::normalize_api_format_alias(provider_api_format).as_str() {
            "openai:chat" => "openai:chat",
            "openai:responses" => "openai:responses",
            "openai:responses:compact" => "openai:responses:compact",
            "openai:search" => "openai:search",
            "claude:messages" => "claude:messages",
            "gemini:generate_content" => "gemini:generate_content",
            "gemini:interactions" => "gemini:interactions",
            _ => return Some("transport_api_format_unsupported"),
        };
    let behavior = classify_same_format_provider_request_behavior(
        transport,
        SameFormatProviderRequestBehaviorParams {
            require_streaming: false,
            provider_api_format: normalized_api_format,
            report_kind: "trace_candidate_metadata",
        },
    );
    if !behavior.is_antigravity
        && !behavior.is_claude_code_transport
        && !behavior.is_gemini_cli
        && !behavior.is_vertex
        && !behavior.is_kiro
    {
        return None;
    }

    let family = if normalized_api_format.starts_with("gemini:") {
        SameFormatProviderFamily::Gemini
    } else {
        SameFormatProviderFamily::Standard
    };
    same_format_provider_transport_unsupported_reason(
        &behavior,
        transport,
        family,
        normalized_api_format,
    )
}

pub fn should_try_same_format_provider_oauth_auth(
    behavior: &SameFormatProviderRequestBehavior,
    transport: &GatewayProviderTransportSnapshot,
    family: SameFormatProviderFamily,
    provider_api_format: &str,
) -> bool {
    let provider_api_format = aether_ai_formats::normalize_api_format_alias(provider_api_format);
    behavior.is_kiro
        || matches!(family, SameFormatProviderFamily::Standard)
            && resolve_same_format_standard_direct_auth(transport, provider_api_format.as_str())
                .is_none()
        || matches!(family, SameFormatProviderFamily::Gemini)
            && behavior.is_vertex
            && is_vertex_service_account_transport_context(transport)
        || matches!(family, SameFormatProviderFamily::Gemini)
            && !behavior.is_vertex
            && resolve_local_gemini_auth(transport).is_none()
}

pub fn resolve_same_format_provider_direct_auth(
    behavior: &SameFormatProviderRequestBehavior,
    transport: &GatewayProviderTransportSnapshot,
    family: SameFormatProviderFamily,
    provider_api_format: &str,
) -> Option<(String, String)> {
    if behavior.is_vertex {
        None
    } else {
        match family {
            SameFormatProviderFamily::Standard => {
                resolve_same_format_standard_direct_auth(transport, provider_api_format)
            }
            SameFormatProviderFamily::Gemini => resolve_local_gemini_auth(transport),
        }
    }
}

fn resolve_same_format_standard_direct_auth(
    transport: &GatewayProviderTransportSnapshot,
    provider_api_format: &str,
) -> Option<(String, String)> {
    if aether_ai_formats::api_format_alias_matches(provider_api_format, "openai:embedding")
        || aether_ai_formats::api_format_alias_matches(provider_api_format, "openai:search")
        || aether_ai_formats::api_format_alias_matches(provider_api_format, "openai:realtime")
        || aether_ai_formats::api_format_alias_matches(provider_api_format, "codex:live")
    {
        resolve_local_openai_bearer_auth(transport)
    } else {
        resolve_local_standard_auth(transport)
    }
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
