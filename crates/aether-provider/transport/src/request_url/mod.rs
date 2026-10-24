use std::collections::BTreeMap;
use std::fmt;
use std::sync::OnceLock;

use aether_ai_formats::ApiOperation;
use regex::Regex;
use serde_json::Value;
use url::{form_urlencoded, Url};

use crate::antigravity::{
    build_antigravity_v1internal_url, is_antigravity_provider_transport,
    AntigravityRequestUrlAction,
};
use crate::claude_code::build_claude_code_messages_url;
use crate::gemini_cli::{
    build_gemini_cli_v1internal_url, is_gemini_cli_provider_transport, GeminiCliRequestUrlAction,
};
use crate::snapshot::GatewayProviderTransportSnapshot;
use crate::url::{
    build_claude_count_tokens_url as build_default_claude_count_tokens_url,
    build_claude_messages_url, build_gemini_content_url, build_openai_chat_url,
    build_openai_responses_url, build_openai_search_url, build_passthrough_path_url,
    encode_url_path_segment, normalize_gemini_content_action_path,
    strip_gateway_credential_query_parameters, GATEWAY_CREDENTIAL_QUERY_KEYS,
};
use crate::vertex::{
    build_vertex_api_key_gemini_content_url, build_vertex_api_key_gemini_embedding_url,
    build_vertex_service_account_gemini_content_url,
    build_vertex_service_account_gemini_embedding_url, resolve_local_vertex_api_key_query_auth,
    resolve_local_vertex_service_account_auth_config,
};
#[derive(Clone, Copy)]
pub struct TransportRequestUrlParams<'a> {
    pub provider_api_format: &'a str,
    pub mapped_model: Option<&'a str>,
    pub upstream_is_stream: bool,
    pub request_query: Option<&'a str>,
    pub kiro_api_region: Option<&'a str>,
    pub api_operation: Option<aether_ai_formats::ApiOperation>,
}

impl fmt::Debug for TransportRequestUrlParams<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TransportRequestUrlParams")
            .field("provider_api_format", &self.provider_api_format)
            .field("mapped_model", &self.mapped_model)
            .field("upstream_is_stream", &self.upstream_is_stream)
            .field("has_request_query", &self.request_query.is_some())
            .field("request_query_len", &self.request_query.map(str::len))
            .field("kiro_api_region", &self.kiro_api_region)
            .field("api_operation", &self.api_operation)
            .finish()
    }
}

pub fn build_transport_request_url(
    transport: &GatewayProviderTransportSnapshot,
    params: TransportRequestUrlParams<'_>,
) -> Option<String> {
    build_transport_request_url_inner(transport, params, false)
}

pub fn build_transport_request_url_for_request_body(
    transport: &GatewayProviderTransportSnapshot,
    params: TransportRequestUrlParams<'_>,
    provider_request_body: Option<&Value>,
) -> Option<String> {
    let gemini_embedding_batch =
        gemini_embedding_request_body_uses_batch(params.provider_api_format, provider_request_body);
    build_transport_request_url_inner(transport, params, gemini_embedding_batch)
}

pub fn gemini_embedding_request_body_uses_batch(
    provider_api_format: &str,
    provider_request_body: Option<&Value>,
) -> bool {
    aether_ai_formats::normalize_api_format_alias(provider_api_format) == "gemini:embedding"
        && provider_request_body
            .and_then(|body| body.get("requests"))
            .and_then(Value::as_array)
            .is_some_and(|requests| !requests.is_empty())
}

fn build_transport_request_url_inner(
    transport: &GatewayProviderTransportSnapshot,
    params: TransportRequestUrlParams<'_>,
    gemini_embedding_batch: bool,
) -> Option<String> {
    let provider_api_format = params.provider_api_format.trim().to_ascii_lowercase();
    let normalized_provider_api_format =
        aether_ai_formats::normalize_api_format_alias(&provider_api_format);
    let sanitized_claude_request_query = (normalized_provider_api_format == "claude:messages")
        .then(|| strip_gateway_credential_query_parameters(params.request_query))
        .flatten();
    let params = if normalized_provider_api_format == "claude:messages" {
        TransportRequestUrlParams {
            request_query: sanitized_claude_request_query.as_deref(),
            ..params
        }
    } else {
        params
    };
    let is_claude_count_tokens = normalized_provider_api_format == "claude:messages"
        && params.api_operation == Some(ApiOperation::ClaudeCountTokens);
    if !transport_supports_api_operation(
        transport,
        normalized_provider_api_format.as_str(),
        params.api_operation,
    ) {
        return None;
    }
    if is_claude_count_tokens {
        if let Some(url) = build_configured_claude_count_tokens_url(transport, params.request_query)
        {
            return Some(url);
        }
    }
    if let Some(url) = build_transport_hook_url(transport, params) {
        return Some(url);
    }

    if params.api_operation == Some(ApiOperation::OpenAiMemoriesSummarize) {
        if normalized_provider_api_format != "openai:responses" {
            return None;
        }
        if let Some(path) = transport
            .endpoint
            .custom_path
            .as_deref()
            .map(str::trim)
            .filter(|path| !path.is_empty())
        {
            // 显式操作模板也适用于原生同步操作。
            if path.contains("{operation}") {
                let path = expand_custom_path_template(path, build_path_params(params, false))?;
                return build_passthrough_path_url(
                    &transport.endpoint.base_url,
                    &path,
                    params.request_query,
                    GATEWAY_CREDENTIAL_QUERY_KEYS,
                );
            }
            // 仅描述 Responses 的自定义路径无法承接该操作。
            return None;
        }
        // 以 Responses 的同一提供商根路径派生原生端点。
        let mut url = Url::parse(&build_openai_responses_url(
            &transport.endpoint.base_url,
            strip_gateway_credential_query_parameters(params.request_query).as_deref(),
            false,
        ))
        .ok()?;
        let root = url.path().strip_suffix("/responses")?;
        let path = format!("{root}/memories/trace_summarize");
        url.set_path(&path);
        return Some(url.to_string());
    }

    let xai_base =
        crate::xai::resolved_xai_upstream_base_url(transport, &normalized_provider_api_format);
    let request_base_url = xai_base
        .as_deref()
        .unwrap_or(transport.endpoint.base_url.as_str());

    let custom_path_template = transport
        .endpoint
        .custom_path
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let custom_path_handles_operation =
        custom_path_template.is_some_and(|path| path.contains("{operation}"));
    let custom_path = match custom_path_template {
        Some(path) => Some(expand_custom_path_template(
            path,
            build_path_params(params, gemini_embedding_batch),
        )?),
        None => None,
    };

    if let Some(path) = custom_path.as_deref() {
        let custom_path_is_complete_claude_count_tokens = normalized_provider_api_format
            == "claude:messages"
            && !custom_path_handles_operation
            && path
                .split_once('?')
                .map(|(path, _)| path)
                .unwrap_or(path)
                .trim_end_matches('/')
                .ends_with("/messages/count_tokens");
        let blocked_keys = if normalized_provider_api_format.starts_with("gemini:")
            || normalized_provider_api_format == "claude:messages"
            || matches!(
                normalized_provider_api_format.as_str(),
                "openai:realtime" | "codex:live"
            ) {
            GATEWAY_CREDENTIAL_QUERY_KEYS
        } else {
            &[][..]
        };
        let normalized_path = if normalized_provider_api_format == "gemini:generate_content" {
            normalize_gemini_content_action_path(path, params.upstream_is_stream)
        } else if normalized_provider_api_format == "gemini:embedding" {
            normalize_gemini_embedding_action_path(path, gemini_embedding_batch)
        } else {
            path.to_string()
        };
        let mut url = build_passthrough_path_url(
            request_base_url,
            normalized_path.as_str(),
            params.request_query,
            blocked_keys,
        )?;
        if is_claude_count_tokens && !custom_path_handles_operation {
            url = build_default_claude_count_tokens_url(&url, None);
        } else if params.api_operation == Some(ApiOperation::ClaudeMessagesCreate)
            && custom_path_is_complete_claude_count_tokens
        {
            url = build_claude_messages_url(&url, None);
        }
        let url = if normalized_provider_api_format == "openai:realtime" {
            replace_realtime_model_query(url, params.mapped_model?)?
        } else {
            url
        };
        return Some(maybe_add_gemini_stream_alt_sse(
            url,
            &provider_api_format,
            params.upstream_is_stream,
        ));
    }

    let url = match normalized_provider_api_format.as_str() {
        "openai:chat" => Some(build_openai_chat_url(
            request_base_url,
            params.request_query,
        )),
        "openai:responses" => Some(build_openai_responses_url(
            request_base_url,
            params.request_query,
            false,
        )),
        "openai:responses:compact" => Some(build_openai_responses_url(
            request_base_url,
            params.request_query,
            true,
        )),
        "openai:search" => Some(build_openai_search_url(
            request_base_url,
            params.request_query,
        )),
        "openai:realtime" => build_passthrough_path_url(
            request_base_url,
            "/v1/realtime",
            params.request_query,
            GATEWAY_CREDENTIAL_QUERY_KEYS,
        )
        .and_then(|url| replace_realtime_model_query(url, params.mapped_model?)),
        "codex:live" => build_passthrough_path_url(
            request_base_url,
            "/live",
            params.request_query,
            GATEWAY_CREDENTIAL_QUERY_KEYS,
        ),
        "openai:embedding" | "jina:embedding" => {
            build_provider_embedding_v1_url(request_base_url, params.request_query)
        }
        "aliyun:multimodal_embedding" => {
            build_aliyun_multimodal_embedding_url(request_base_url, params.request_query)
        }
        "openai:rerank" | "jina:rerank" => {
            build_provider_rerank_v1_url(request_base_url, params.request_query)
        }
        "claude:messages" => Some(if is_claude_count_tokens {
            build_default_claude_count_tokens_url(request_base_url, params.request_query)
        } else {
            build_claude_messages_url(request_base_url, params.request_query)
        }),
        "gemini:generate_content" => build_gemini_content_url(
            request_base_url,
            params.mapped_model?,
            params.upstream_is_stream,
            params.request_query,
        ),
        "gemini:embedding" => build_gemini_embedding_url(
            request_base_url,
            params.mapped_model?,
            params.request_query,
            gemini_embedding_batch,
        ),
        "gemini:interactions" => {
            build_gemini_interactions_url(request_base_url, params.request_query)
        }
        "doubao:embedding" => {
            build_passthrough_path_url(request_base_url, "/embeddings", params.request_query, &[])
        }
        _ => None,
    }?;

    Some(maybe_add_gemini_stream_alt_sse(
        url,
        &provider_api_format,
        params.upstream_is_stream,
    ))
}

fn replace_realtime_model_query(raw_url: String, mapped_model: &str) -> Option<String> {
    let mut url = Url::parse(raw_url.as_str()).ok()?;
    let retained = url
        .query_pairs()
        .filter(|(name, _)| !name.eq_ignore_ascii_case("model"))
        .map(|(name, value)| (name.into_owned(), value.into_owned()))
        .collect::<Vec<_>>();
    url.set_query(None);
    {
        let mut query = url.query_pairs_mut();
        for (name, value) in retained {
            query.append_pair(name.as_str(), value.as_str());
        }
        query.append_pair("model", mapped_model);
    }
    Some(url.to_string())
}

pub fn build_local_openai_chat_upstream_url(
    transport: &GatewayProviderTransportSnapshot,
    request_query: Option<&str>,
) -> Option<String> {
    build_transport_request_url(
        transport,
        TransportRequestUrlParams {
            provider_api_format: "openai:chat",
            mapped_model: None,
            upstream_is_stream: false,
            request_query,
            kiro_api_region: None,
            api_operation: None,
        },
    )
}

pub fn build_cross_format_openai_chat_upstream_url(
    transport: &GatewayProviderTransportSnapshot,
    mapped_model: &str,
    provider_api_format: &str,
    upstream_is_stream: bool,
    request_query: Option<&str>,
) -> Option<String> {
    aether_ai_formats::request_conversion_kind("openai:chat", provider_api_format)?;
    build_transport_request_url(
        transport,
        TransportRequestUrlParams {
            provider_api_format,
            mapped_model: Some(mapped_model),
            upstream_is_stream,
            request_query,
            kiro_api_region: None,
            api_operation: None,
        },
    )
}

pub fn build_local_openai_responses_upstream_url(
    transport: &GatewayProviderTransportSnapshot,
    compact: bool,
    request_query: Option<&str>,
) -> Option<String> {
    let provider_api_format = if compact {
        "openai:responses:compact"
    } else {
        "openai:responses"
    };
    build_transport_request_url(
        transport,
        TransportRequestUrlParams {
            provider_api_format,
            mapped_model: None,
            upstream_is_stream: false,
            request_query,
            kiro_api_region: None,
            api_operation: None,
        },
    )
}

pub fn build_cross_format_openai_responses_upstream_url(
    transport: &GatewayProviderTransportSnapshot,
    mapped_model: &str,
    client_api_format: &str,
    provider_api_format: &str,
    upstream_is_stream: bool,
    request_query: Option<&str>,
) -> Option<String> {
    aether_ai_formats::request_conversion_kind(client_api_format, provider_api_format)?;
    build_transport_request_url(
        transport,
        TransportRequestUrlParams {
            provider_api_format,
            mapped_model: Some(mapped_model),
            upstream_is_stream,
            request_query,
            kiro_api_region: None,
            api_operation: None,
        },
    )
}

pub fn build_kiro_cross_format_upstream_url(
    transport: &GatewayProviderTransportSnapshot,
    mapped_model: &str,
    provider_api_format: &str,
    upstream_is_stream: bool,
    request_query: Option<&str>,
    api_region: &str,
) -> Option<String> {
    build_transport_request_url(
        transport,
        TransportRequestUrlParams {
            provider_api_format,
            mapped_model: Some(mapped_model),
            upstream_is_stream,
            request_query,
            kiro_api_region: Some(api_region),
            api_operation: None,
        },
    )
}

fn build_transport_hook_url(
    transport: &GatewayProviderTransportSnapshot,
    params: TransportRequestUrlParams<'_>,
) -> Option<String> {
    if let Some(api_region) = params.kiro_api_region {
        return crate::kiro::build_kiro_generate_assistant_response_url(
            &transport.endpoint.base_url,
            params.request_query,
            Some(api_region),
        );
    }

    if transport
        .provider
        .provider_type
        .trim()
        .eq_ignore_ascii_case("claude_code")
    {
        let messages_url =
            build_claude_code_messages_url(&transport.endpoint.base_url, params.request_query);
        return Some(
            if params.api_operation == Some(aether_ai_formats::ApiOperation::ClaudeCountTokens) {
                build_default_claude_count_tokens_url(&messages_url, None)
            } else {
                messages_url
            },
        );
    }

    let normalized_provider_api_format =
        aether_ai_formats::normalize_api_format_alias(params.provider_api_format);
    match normalized_provider_api_format.as_str() {
        "gemini:generate_content" => {
            if is_gemini_cli_provider_transport(transport) {
                let query = params.request_query.map(|raw| {
                    form_urlencoded::parse(raw.as_bytes())
                        .into_owned()
                        .collect::<BTreeMap<String, String>>()
                });
                return build_gemini_cli_v1internal_url(
                    &transport.endpoint.base_url,
                    if params.upstream_is_stream {
                        GeminiCliRequestUrlAction::StreamGenerateContent
                    } else {
                        GeminiCliRequestUrlAction::GenerateContent
                    },
                    query.as_ref(),
                );
            }
            if let Some(auth) = resolve_local_vertex_api_key_query_auth(transport) {
                return build_vertex_api_key_gemini_content_url(
                    params.mapped_model?,
                    params.upstream_is_stream,
                    &auth.value,
                    params.request_query,
                );
            }
            if let Some(auth_config) = resolve_local_vertex_service_account_auth_config(transport) {
                return build_vertex_service_account_gemini_content_url(
                    params.mapped_model?,
                    params.upstream_is_stream,
                    &auth_config,
                    params.request_query,
                );
            }
        }
        "gemini:embedding" => {
            if let Some(auth) = resolve_local_vertex_api_key_query_auth(transport) {
                return build_vertex_api_key_gemini_embedding_url(
                    params.mapped_model?,
                    &auth.value,
                    params.request_query,
                );
            }
            if let Some(auth_config) = resolve_local_vertex_service_account_auth_config(transport) {
                return build_vertex_service_account_gemini_embedding_url(
                    params.mapped_model?,
                    &auth_config,
                    params.request_query,
                );
            }
        }
        _ => {}
    }

    if is_antigravity_provider_transport(transport)
        && normalized_provider_api_format == "gemini:generate_content"
    {
        let query = params.request_query.map(|raw| {
            form_urlencoded::parse(raw.as_bytes())
                .into_owned()
                .collect::<BTreeMap<String, String>>()
        });
        return build_antigravity_v1internal_url(
            &transport.endpoint.base_url,
            if params.upstream_is_stream {
                AntigravityRequestUrlAction::StreamGenerateContent
            } else {
                AntigravityRequestUrlAction::GenerateContent
            },
            query.as_ref(),
        );
    }

    if is_gemini_cli_provider_transport(transport)
        && normalized_provider_api_format == "gemini:generate_content"
    {
        let query = params.request_query.map(|raw| {
            form_urlencoded::parse(raw.as_bytes())
                .into_owned()
                .collect::<BTreeMap<String, String>>()
        });
        return build_gemini_cli_v1internal_url(
            &transport.endpoint.base_url,
            if params.upstream_is_stream {
                GeminiCliRequestUrlAction::StreamGenerateContent
            } else {
                GeminiCliRequestUrlAction::GenerateContent
            },
            query.as_ref(),
        );
    }

    None
}

fn build_path_params(
    params: TransportRequestUrlParams<'_>,
    gemini_embedding_batch: bool,
) -> BTreeMap<&'static str, &str> {
    let mut path_params = BTreeMap::new();
    if let Some(model) = params
        .mapped_model
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        path_params.insert("model", model);
    }
    let provider_api_format =
        aether_ai_formats::normalize_api_format_alias(params.provider_api_format);
    if let Some(operation) = params.api_operation {
        path_params.insert("operation", operation.as_str());
    }
    if provider_api_format == "gemini:generate_content" || provider_api_format == "gemini:embedding"
    {
        path_params.insert(
            "action",
            if provider_api_format == "gemini:embedding" {
                if gemini_embedding_batch {
                    "batchEmbedContents"
                } else {
                    "embedContent"
                }
            } else if params.upstream_is_stream {
                "streamGenerateContent"
            } else {
                "generateContent"
            },
        );
    }
    path_params
}

pub fn transport_supports_api_operation(
    transport: &GatewayProviderTransportSnapshot,
    provider_api_format: &str,
    operation: Option<ApiOperation>,
) -> bool {
    if operation == Some(ApiOperation::OpenAiMemoriesSummarize) {
        return aether_ai_formats::normalize_api_format_alias(provider_api_format)
            == "openai:responses"
            && !crate::kiro::is_kiro_provider_transport(transport)
            && !crate::grok::is_grok_provider_transport(transport)
            && !is_antigravity_provider_transport(transport)
            && !is_gemini_cli_provider_transport(transport);
    }
    if operation != Some(ApiOperation::ClaudeCountTokens) {
        return true;
    }
    if aether_ai_formats::normalize_api_format_alias(provider_api_format) != "claude:messages" {
        return false;
    }

    anthropic_count_tokens_supported(transport)
}

fn anthropic_count_tokens_supported(transport: &GatewayProviderTransportSnapshot) -> bool {
    // Private message adapters do not implement Anthropic's token-counting
    // operation. A config flag cannot make their request envelopes compatible.
    if crate::kiro::is_kiro_provider_transport(transport)
        || crate::grok::is_grok_provider_transport(transport)
    {
        return false;
    }

    let Some(operations) = anthropic_transport_config_field(transport, "supported_operations")
    else {
        return true;
    };
    operations.as_array().is_some_and(|operations| {
        operations.iter().any(|operation| {
            operation
                .as_str()
                .is_some_and(|value| value.eq_ignore_ascii_case("count_tokens"))
        })
    })
}

fn build_configured_claude_count_tokens_url(
    transport: &GatewayProviderTransportSnapshot,
    request_query: Option<&str>,
) -> Option<String> {
    let path = anthropic_transport_config_field(transport, "count_tokens_path")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let path = path?;
    let path = if path.starts_with('/') {
        path.to_string()
    } else {
        format!("/{path}")
    };
    build_passthrough_path_url(
        &transport.endpoint.base_url,
        path.as_str(),
        request_query,
        GATEWAY_CREDENTIAL_QUERY_KEYS,
    )
}

fn anthropic_transport_config_field<'a>(
    transport: &'a GatewayProviderTransportSnapshot,
    field: &str,
) -> Option<&'a Value> {
    let from_config = |config: Option<&'a Value>| {
        config
            .and_then(Value::as_object)
            .and_then(|config| config.get("anthropic"))
            .and_then(Value::as_object)
            .and_then(|anthropic| anthropic.get(field))
    };
    from_config(transport.endpoint.config.as_ref())
        .or_else(|| from_config(transport.provider.config.as_ref()))
}

fn normalize_gemini_embedding_action_path(path: &str, batch: bool) -> String {
    if batch {
        path.replace(":embedContent", ":batchEmbedContents")
    } else {
        path.replace(":batchEmbedContents", ":embedContent")
    }
}

fn build_provider_embedding_v1_url(upstream_base_url: &str, query: Option<&str>) -> Option<String> {
    build_provider_api_root_url(upstream_base_url, "/embeddings", query)
}

fn build_aliyun_multimodal_embedding_url(
    upstream_base_url: &str,
    query: Option<&str>,
) -> Option<String> {
    build_passthrough_path_url(
        upstream_base_url,
        "/api/v1/services/embeddings/multimodal-embedding/multimodal-embedding",
        query,
        &[],
    )
}

fn build_provider_rerank_v1_url(upstream_base_url: &str, query: Option<&str>) -> Option<String> {
    build_provider_api_root_url(upstream_base_url, "/rerank", query)
}

fn build_gemini_interactions_url(upstream_base_url: &str, query: Option<&str>) -> Option<String> {
    build_passthrough_path_url(upstream_base_url, "/v1/interactions", query, &["key"])
}

fn build_provider_api_root_url(
    upstream_base_url: &str,
    path: &str,
    query: Option<&str>,
) -> Option<String> {
    build_passthrough_path_url(upstream_base_url, path, query, &[])
}

fn build_gemini_embedding_url(
    upstream_base_url: &str,
    model: &str,
    query: Option<&str>,
    batch: bool,
) -> Option<String> {
    let trimmed_base_url = upstream_base_url
        .trim()
        .split_once('?')
        .map(|(base, _)| base)
        .unwrap_or_else(|| upstream_base_url.trim())
        .trim_end_matches('/');
    let trimmed_model = model.trim();
    if trimmed_base_url.is_empty() || trimmed_model.is_empty() {
        return None;
    }

    let action = if batch {
        "batchEmbedContents"
    } else {
        "embedContent"
    };
    let encoded_model = encode_url_path_segment(trimmed_model);
    let path = if trimmed_base_url.ends_with("/v1beta") {
        format!("/models/{encoded_model}:{action}")
    } else if trimmed_base_url.contains("/v1beta/models/") {
        format!(":{action}")
    } else {
        format!("/v1beta/models/{encoded_model}:{action}")
    };
    build_passthrough_path_url(upstream_base_url, &path, query, &["key"])
}

fn expand_custom_path_template(path: &str, params: BTreeMap<&'static str, &str>) -> Option<String> {
    if params.is_empty() {
        return Some(path.to_string());
    }

    let regex = custom_path_template_regex();
    let query_start = path.find('?');
    let mut missing_key = false;
    let replaced = regex.replace_all(path, |captures: &regex::Captures<'_>| {
        let key = captures
            .get(1)
            .map(|value| value.as_str())
            .unwrap_or_default();
        match params.get(key).copied() {
            Some(value) => {
                let capture_start = captures.get(0).map_or(0, |value| value.start());
                if query_start.is_some_and(|query_start| capture_start > query_start) {
                    url::form_urlencoded::byte_serialize(value.as_bytes()).collect()
                } else {
                    encode_url_path_segment(value)
                }
            }
            None => {
                missing_key = true;
                captures
                    .get(0)
                    .map(|value| value.as_str().to_string())
                    .unwrap_or_default()
            }
        }
    });

    if missing_key {
        Some(path.to_string())
    } else {
        let replaced = replaced.into_owned();
        if dynamic_template_value_created_dot_path_segment(path, &replaced, regex) {
            None
        } else {
            Some(replaced)
        }
    }
}

fn dynamic_template_value_created_dot_path_segment(
    template: &str,
    expanded: &str,
    regex: &Regex,
) -> bool {
    let template_path = template.split_once('?').map_or(template, |(path, _)| path);
    let expanded_path = expanded.split_once('?').map_or(expanded, |(path, _)| path);
    template_path.split('/').zip(expanded_path.split('/')).any(
        |(template_segment, expanded_segment)| {
            matches!(expanded_segment, "." | "..") && regex.is_match(template_segment)
        },
    )
}

fn maybe_add_gemini_stream_alt_sse(
    upstream_url: String,
    provider_api_format: &str,
    upstream_is_stream: bool,
) -> String {
    if aether_ai_formats::normalize_api_format_alias(provider_api_format)
        != "gemini:generate_content"
        || !upstream_is_stream
    {
        return upstream_url;
    }

    let has_alt = upstream_url
        .split_once('?')
        .map(|(_, query)| {
            form_urlencoded::parse(query.as_bytes())
                .any(|(key, _)| key.as_ref().eq_ignore_ascii_case("alt"))
        })
        .unwrap_or(false);
    if has_alt {
        return upstream_url;
    }

    if upstream_url.contains('?') {
        format!("{upstream_url}&alt=sse")
    } else {
        format!("{upstream_url}?alt=sse")
    }
}

fn custom_path_template_regex() -> &'static Regex {
    static REGEX: OnceLock<Regex> = OnceLock::new();
    REGEX.get_or_init(|| {
        Regex::new(r"\{([A-Za-z_][A-Za-z0-9_]*)\}")
            .expect("custom_path template regex should compile")
    })
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
