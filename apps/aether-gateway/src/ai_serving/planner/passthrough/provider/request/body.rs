use serde_json::Value;

use super::super::LocalSameFormatProviderSpec;
use crate::ai_serving::transport::{
    build_same_format_provider_request_body_for_operation as build_same_format_provider_request_body_with_compatibility_report_impl,
    SameFormatProviderFamily, SameFormatProviderRequestBodyInput,
    SameFormatProviderRequestBodyOutput,
};

pub(crate) struct SameFormatBodyBuildInput<'a> {
    pub body_json: &'a Value,
    pub provider_api_format: &'a str,
    pub mapped_model: &'a str,
    pub spec: LocalSameFormatProviderSpec,
    pub body_rules: Option<&'a Value>,
    pub request_headers: Option<&'a http::HeaderMap>,
    pub upstream_is_stream: bool,
    pub force_body_stream_field: bool,
    pub kiro_auth: Option<&'a crate::ai_serving::transport::kiro::KiroRequestAuth>,
    pub is_claude_code: bool,
    pub is_vertex: bool,
    pub enable_model_directives: bool,
    pub reasoning_replay_policy: crate::ai_serving::OpenAiResponsesReasoningReplayPolicy,
}

pub(crate) fn build_same_format_provider_request_body_with_compatibility_report(
    input: SameFormatBodyBuildInput<'_>,
) -> Option<SameFormatProviderRequestBodyOutput> {
    let SameFormatBodyBuildInput {
        body_json,
        provider_api_format,
        mapped_model,
        spec,
        body_rules,
        request_headers,
        upstream_is_stream,
        force_body_stream_field,
        kiro_auth,
        is_claude_code,
        is_vertex,
        enable_model_directives,
        reasoning_replay_policy,
    } = input;
    build_same_format_provider_request_body_with_compatibility_report_impl(
        SameFormatProviderRequestBodyInput {
            body_json,
            mapped_model,
            client_api_format: spec.api_format,
            provider_api_format,
            source_model: body_json.get("model").and_then(Value::as_str),
            family: same_format_provider_family(spec.family),
            body_rules,
            request_headers,
            upstream_is_stream,
            force_body_stream_field,
            kiro_auth_config: kiro_auth.map(|auth| &auth.auth_config),
            is_claude_code,
            is_vertex,
            enable_model_directives,
        },
        reasoning_replay_policy,
        spec.operation,
    )
}

fn same_format_provider_family(
    family: super::super::LocalSameFormatProviderFamily,
) -> SameFormatProviderFamily {
    match family {
        super::super::LocalSameFormatProviderFamily::Standard => SameFormatProviderFamily::Standard,
        super::super::LocalSameFormatProviderFamily::Gemini => SameFormatProviderFamily::Gemini,
    }
}
