use aether_ai_formats::ApiOperation;

use super::{
    build_kiro_cross_format_upstream_url, build_transport_request_url,
    build_transport_request_url_for_request_body, TransportRequestUrlParams,
};
use crate::snapshot::{
    GatewayProviderTransportEndpoint, GatewayProviderTransportKey,
    GatewayProviderTransportProvider, GatewayProviderTransportSnapshot,
};
use serde_json::json;

fn sample_transport(
    provider_type: &str,
    api_format: &str,
    base_url: &str,
    custom_path: Option<&str>,
) -> GatewayProviderTransportSnapshot {
    GatewayProviderTransportSnapshot {
        provider: GatewayProviderTransportProvider {
            id: "provider-1".to_string(),
            name: "provider".to_string(),
            provider_type: provider_type.to_string(),
            website: None,
            is_active: true,
            keep_priority_on_conversion: false,
            enable_format_conversion: false,
            concurrent_limit: None,
            max_retries: None,
            proxy: None,
            request_timeout_secs: None,
            stream_first_byte_timeout_secs: None,
            config: None,
        },
        endpoint: GatewayProviderTransportEndpoint {
            id: "endpoint-1".to_string(),
            provider_id: "provider-1".to_string(),
            api_format: api_format.to_string(),
            api_family: None,
            endpoint_kind: None,
            is_active: true,
            base_url: base_url.to_string(),
            header_rules: None,
            body_rules: None,
            max_retries: None,
            custom_path: custom_path.map(ToOwned::to_owned),
            config: None,
            format_acceptance_config: None,
            proxy: None,
        },
        key: GatewayProviderTransportKey {
            id: "key-1".to_string(),
            provider_id: "provider-1".to_string(),
            name: "key".to_string(),
            auth_type: "api_key".to_string(),
            is_active: true,
            api_formats: None,
            auth_type_by_format: None,
            allow_auth_channel_mismatch_formats: None,

            allowed_models: None,
            capabilities: None,
            rate_multipliers: None,
            global_priority_by_format: None,
            expires_at_unix_secs: None,
            proxy: None,
            fingerprint: None,
            upstream_metadata: None,
            decrypted_api_key: "vertex-secret".to_string(),
            decrypted_auth_config: None,
        },
    }
}

#[test]
fn realtime_url_uses_mapped_model_and_drops_downstream_credentials() {
    let transport = sample_transport(
        "custom",
        "openai:realtime",
        "https://api.openai.com/v1",
        None,
    );

    let url = build_transport_request_url(
        &transport,
        TransportRequestUrlParams {
            provider_api_format: "openai:realtime",
            mapped_model: Some("gpt-realtime-provider"),
            upstream_is_stream: true,
            request_query: Some("model=client-alias&trace=1&key=downstream-secret"),
            kiro_api_region: None,
            api_operation: None,
        },
    )
    .expect("realtime URL");

    assert_eq!(
        url,
        "https://api.openai.com/v1/realtime?trace=1&model=gpt-realtime-provider"
    );
    assert!(!url.contains("downstream-secret"));
    assert!(!url.contains("client-alias"));
}

#[test]
fn realtime_custom_path_still_rewrites_the_model_query() {
    let transport = sample_transport(
        "custom",
        "openai:realtime",
        "https://voice.example.test/api",
        Some("/socket/realtime?deployment=future"),
    );

    let url = build_transport_request_url(
        &transport,
        TransportRequestUrlParams {
            provider_api_format: "openai:realtime",
            mapped_model: Some("provider-model"),
            upstream_is_stream: true,
            request_query: Some("model=client-model&tenant=alpha"),
            kiro_api_region: None,
            api_operation: None,
        },
    )
    .expect("custom realtime URL");

    assert_eq!(
        url,
        "https://voice.example.test/api/socket/realtime?deployment=future&tenant=alpha&model=provider-model"
    );
}

#[test]
fn codex_live_url_uses_the_dedicated_endpoint_without_forwarding_credentials() {
    let transport = sample_transport(
        "custom",
        "codex:live",
        "https://voice.example.test/v1",
        None,
    );

    let url = build_transport_request_url(
        &transport,
        TransportRequestUrlParams {
            provider_api_format: "codex:live",
            mapped_model: Some("provider-model"),
            upstream_is_stream: true,
            request_query: Some("trace=1&key=downstream-secret"),
            kiro_api_region: None,
            api_operation: None,
        },
    )
    .expect("Codex Live URL");

    assert_eq!(url, "https://voice.example.test/v1/live?trace=1");
    assert!(!url.contains("downstream-secret"));
}

#[test]
fn codex_live_custom_path_remains_an_explicit_protocol_endpoint() {
    let transport = sample_transport(
        "custom",
        "codex:live",
        "https://voice.example.test/v1",
        Some("/socket/live?deployment=future"),
    );

    let url = build_transport_request_url(
        &transport,
        TransportRequestUrlParams {
            provider_api_format: "codex:live",
            mapped_model: Some("provider-model"),
            upstream_is_stream: true,
            request_query: Some("tenant=alpha&key=downstream-secret"),
            kiro_api_region: None,
            api_operation: None,
        },
    )
    .expect("custom Codex Live URL");

    assert_eq!(
        url,
        "https://voice.example.test/v1/socket/live?deployment=future&tenant=alpha"
    );
}

#[test]
fn uses_vertex_hook_before_custom_path_for_custom_aiplatform_transport() {
    let transport = sample_transport(
        "custom",
        "gemini:generate_content",
        "https://aiplatform.googleapis.com",
        Some("/custom/{model}:{action}"),
    );

    let url = build_transport_request_url(
        &transport,
        TransportRequestUrlParams {
            provider_api_format: "gemini:generate_content",
            mapped_model: Some("gemini-3.1-pro-preview"),
            upstream_is_stream: true,
            request_query: Some("foo=bar"),
            kiro_api_region: None,
            api_operation: None,
        },
    )
    .expect("vertex hook url");

    assert_eq!(
        url,
        "https://aiplatform.googleapis.com/v1/publishers/google/models/gemini-3.1-pro-preview:streamGenerateContent?alt=sse&foo=bar&key=vertex-secret"
    );
}

#[test]
fn uses_vertex_service_account_hook_before_default_gemini_url() {
    let mut transport = sample_transport(
        "vertex_ai",
        "gemini:generate_content",
        "https://aiplatform.googleapis.com",
        None,
    );
    transport.key.auth_type = "service_account".to_string();
    transport.key.decrypted_api_key = "__placeholder__".to_string();
    transport.key.decrypted_auth_config = Some(
        r#"{
            "client_email":"svc@example.iam.gserviceaccount.com",
            "private_key":"TEST-PRIVATE-KEY",
            "project_id":"demo-project"
        }"#
        .to_string(),
    );

    let url = build_transport_request_url(
        &transport,
        TransportRequestUrlParams {
            provider_api_format: "gemini:generate_content",
            mapped_model: Some("gemini-3.1-pro-preview"),
            upstream_is_stream: false,
            request_query: Some("foo=bar&beta=1"),
            kiro_api_region: None,
            api_operation: None,
        },
    )
    .expect("vertex service account hook url");

    assert_eq!(
        url,
        "https://aiplatform.googleapis.com/v1/projects/demo-project/locations/global/publishers/google/models/gemini-3.1-pro-preview:generateContent?foo=bar"
    );
}

#[test]
fn uses_vertex_service_account_hook_for_gemini_embedding_url() {
    let mut transport = sample_transport(
        "vertex_ai",
        "gemini:embedding",
        "https://aiplatform.googleapis.com",
        None,
    );
    transport.endpoint.endpoint_kind = Some("embedding".to_string());
    transport.key.auth_type = "service_account".to_string();
    transport.key.decrypted_api_key = "__placeholder__".to_string();
    transport.key.decrypted_auth_config = Some(
        r#"{
            "client_email":"svc@example.iam.gserviceaccount.com",
            "private_key":"TEST-PRIVATE-KEY",
            "project_id":"demo-project"
        }"#
        .to_string(),
    );

    let provider_request_body = json!({
        "content": {"parts": [{"text": "hello"}]}
    });
    let url = build_transport_request_url_for_request_body(
        &transport,
        TransportRequestUrlParams {
            provider_api_format: "gemini:embedding",
            mapped_model: Some("gemini-embedding-2"),
            upstream_is_stream: false,
            request_query: Some("foo=bar&beta=1"),
            kiro_api_region: None,
            api_operation: None,
        },
        Some(&provider_request_body),
    )
    .expect("vertex embedding service account hook url");

    assert_eq!(
        url,
        "https://aiplatform.googleapis.com/v1/projects/demo-project/locations/global/publishers/google/models/gemini-embedding-2:predict?foo=bar"
    );
}

#[test]
fn gemini_cli_generate_content_uses_v1internal_code_assist_url() {
    let transport = sample_transport(
        "gemini_cli",
        "gemini:generate_content",
        "https://cloudcode-pa.googleapis.com",
        None,
    );

    assert_eq!(
        build_transport_request_url(
            &transport,
            TransportRequestUrlParams {
                provider_api_format: "gemini:generate_content",
                mapped_model: Some("gemini-2.5-pro"),
                upstream_is_stream: false,
                request_query: Some("key=blocked&beta=true&foo=bar"),
                kiro_api_region: None,
                api_operation: None,
            },
        )
        .as_deref(),
        Some("https://cloudcode-pa.googleapis.com/v1internal:generateContent?foo=bar")
    );
}

#[test]
fn gemini_cli_stream_generate_content_uses_v1internal_code_assist_url() {
    let transport = sample_transport(
        "gemini_cli",
        "gemini:generate_content",
        "https://cloudcode-pa.googleapis.com",
        None,
    );

    assert_eq!(
        build_transport_request_url(
            &transport,
            TransportRequestUrlParams {
                provider_api_format: "gemini:generate_content",
                mapped_model: Some("gemini-2.5-pro"),
                upstream_is_stream: true,
                request_query: Some("foo=bar"),
                kiro_api_region: None,
                api_operation: None,
            },
        )
        .as_deref(),
        Some(
            "https://cloudcode-pa.googleapis.com/v1internal:streamGenerateContent?alt=sse&foo=bar"
        )
    );
}

#[test]
fn vertex_gemini_embedding_batch_request_uses_vertex_predict_endpoint() {
    let mut transport = sample_transport(
        "vertex_ai",
        "gemini:embedding",
        "https://aiplatform.googleapis.com",
        None,
    );
    transport.endpoint.endpoint_kind = Some("embedding".to_string());
    transport.key.auth_type = "service_account".to_string();
    transport.key.decrypted_api_key = "__placeholder__".to_string();
    transport.key.decrypted_auth_config = Some(
        r#"{
            "client_email":"svc@example.iam.gserviceaccount.com",
            "private_key":"TEST-PRIVATE-KEY",
            "project_id":"demo-project"
        }"#
        .to_string(),
    );

    let batch_body = json!({
        "requests": [
            {
                "model": "models/gemini-embedding-2",
                "content": {"parts": [{"text": "alpha"}]}
            }
        ]
    });

    assert_eq!(
        build_transport_request_url_for_request_body(
            &transport,
            TransportRequestUrlParams {
                provider_api_format: "gemini:embedding",
                mapped_model: Some("gemini-embedding-2"),
                upstream_is_stream: false,
                request_query: None,
                kiro_api_region: None,
                api_operation: None,
            },
            Some(&batch_body),
        )
        .as_deref(),
        Some(
            "https://aiplatform.googleapis.com/v1/projects/demo-project/locations/global/publishers/google/models/gemini-embedding-2:predict"
        )
    );
}

#[test]
fn builds_openai_responses_url_for_formal_format_name() {
    let transport = sample_transport(
        "openai",
        "openai:responses",
        "https://api.openai.example/v1",
        None,
    );

    let url = build_transport_request_url(
        &transport,
        TransportRequestUrlParams {
            provider_api_format: "openai:responses",
            mapped_model: None,
            upstream_is_stream: false,
            request_query: Some("tenant=demo"),
            kiro_api_region: None,
            api_operation: None,
        },
    )
    .expect("openai responses url");

    assert_eq!(url, "https://api.openai.example/v1/responses?tenant=demo");
}

#[test]
fn memories_url_respects_operation_templates_and_rejects_incompatible_paths() {
    let params = TransportRequestUrlParams {
        provider_api_format: "openai:responses",
        mapped_model: Some("gpt-6.1-sol"),
        upstream_is_stream: false,
        request_query: Some("key=synthetic&tenant=demo"),
        kiro_api_region: None,
        api_operation: Some(ApiOperation::OpenAiMemoriesSummarize),
    };
    let transport = sample_transport(
        "codex",
        "openai:responses",
        "https://example.com",
        Some("/native/{operation}"),
    );
    assert_eq!(
        build_transport_request_url(&transport, params).as_deref(),
        Some("https://example.com/native/trace_summarize?tenant=demo")
    );
    let incompatible = sample_transport(
        "codex",
        "openai:responses",
        "https://example.com",
        Some("/native/responses"),
    );
    assert!(build_transport_request_url(&incompatible, params).is_none());
    for provider in ["kiro", "grok", "antigravity", "gemini_cli"] {
        let private = sample_transport(provider, "openai:responses", "https://example.com", None);
        assert!(
            build_transport_request_url(&private, params).is_none(),
            "{provider}"
        );
    }
}

#[test]
fn memories_url_uses_the_configured_provider_root_and_removes_gateway_auth() {
    for (provider, base, expected) in [
        (
            "codex",
            "https://chatgpt.com/backend-api/codex",
            "https://chatgpt.com/backend-api/codex/memories/trace_summarize?tenant=demo",
        ),
        (
            "custom",
            "https://example.com/v1",
            "https://example.com/v1/memories/trace_summarize?tenant=demo",
        ),
    ] {
        let transport = sample_transport(provider, "openai:responses", base, None);
        let result = build_transport_request_url(
            &transport,
            TransportRequestUrlParams {
                provider_api_format: "openai:responses",
                mapped_model: Some("gpt-6.1-sol"),
                upstream_is_stream: false,
                request_query: Some("key=synthetic-secret&tenant=demo"),
                kiro_api_region: None,
                api_operation: Some(ApiOperation::OpenAiMemoriesSummarize),
            },
        );
        assert_eq!(result.as_deref(), Some(expected));
    }
}

#[test]
fn builds_openai_search_url_for_codex_provider_root() {
    let transport = sample_transport(
        "codex",
        "openai:search",
        "https://chatgpt.com/backend-api/codex",
        None,
    );

    let url = build_transport_request_url(
        &transport,
        TransportRequestUrlParams {
            provider_api_format: "openai:search",
            mapped_model: Some("gpt-5.6-luna"),
            upstream_is_stream: false,
            request_query: Some("tenant=demo"),
            kiro_api_region: None,
            api_operation: None,
        },
    )
    .expect("openai search url");

    assert_eq!(
        url,
        "https://chatgpt.com/backend-api/codex/alpha/search?tenant=demo"
    );
}

#[test]
fn expands_custom_path_templates_when_hook_does_not_apply() {
    let transport = sample_transport(
        "custom",
        "gemini:generate_content",
        "https://generativelanguage.googleapis.com",
        Some("/v1beta/models/{model}:{action}"),
    );

    let url = build_transport_request_url(
        &transport,
        TransportRequestUrlParams {
            provider_api_format: "gemini:generate_content",
            mapped_model: Some("gemini-2.5-pro"),
            upstream_is_stream: false,
            request_query: Some("key=client-key&foo=bar"),
            kiro_api_region: None,
            api_operation: None,
        },
    )
    .expect("expanded custom path url");

    assert_eq!(
        url,
        "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-pro:generateContent?foo=bar"
    );
}

#[test]
fn rewrites_hardcoded_gemini_custom_path_action_to_match_stream_mode() {
    let stream_transport = sample_transport(
        "custom",
        "gemini:generate_content",
        "https://generativelanguage.googleapis.com",
        Some("/v1beta/models/{model}:generateContent"),
    );

    let stream_url = build_transport_request_url(
        &stream_transport,
        TransportRequestUrlParams {
            provider_api_format: "gemini:generate_content",
            mapped_model: Some("gemini-2.5-pro"),
            upstream_is_stream: true,
            request_query: Some("key=client-key&foo=bar"),
            kiro_api_region: None,
            api_operation: None,
        },
    )
    .expect("stream custom path url");

    assert_eq!(
        stream_url,
        "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-pro:streamGenerateContent?foo=bar&alt=sse"
    );

    let sync_transport = sample_transport(
        "custom",
        "gemini:generate_content",
        "https://generativelanguage.googleapis.com",
        Some("/v1beta/models/{model}:streamGenerateContent"),
    );

    let sync_url = build_transport_request_url(
        &sync_transport,
        TransportRequestUrlParams {
            provider_api_format: "gemini:generate_content",
            mapped_model: Some("gemini-2.5-pro"),
            upstream_is_stream: false,
            request_query: Some("foo=bar"),
            kiro_api_region: None,
            api_operation: None,
        },
    )
    .expect("sync custom path url");

    assert_eq!(
        sync_url,
        "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-pro:generateContent?foo=bar"
    );

    let v1_transport = sample_transport(
        "custom",
        "gemini:generate_content",
        "https://generativelanguage.googleapis.com",
        Some("/v1/models/{model}:generateContent"),
    );

    let v1_stream_url = build_transport_request_url(
        &v1_transport,
        TransportRequestUrlParams {
            provider_api_format: "gemini:generate_content",
            mapped_model: Some("gemini-2.5-pro"),
            upstream_is_stream: true,
            request_query: None,
            kiro_api_region: None,
            api_operation: None,
        },
    )
    .expect("v1 stream custom path url");

    assert_eq!(
        v1_stream_url,
        "https://generativelanguage.googleapis.com/v1/models/gemini-2.5-pro:streamGenerateContent?alt=sse"
    );
}

#[test]
fn keeps_original_custom_path_when_template_params_are_missing() {
    let transport = sample_transport(
        "custom",
        "claude:messages",
        "https://api.example.com",
        Some("/v1/messages/{model}"),
    );

    let url = build_transport_request_url(
        &transport,
        TransportRequestUrlParams {
            provider_api_format: "claude:messages",
            mapped_model: None,
            upstream_is_stream: false,
            request_query: None,
            kiro_api_region: None,
            api_operation: None,
        },
    )
    .expect("fallback custom path url");

    assert_eq!(url, "https://api.example.com/v1/messages/{model}");
}

#[test]
fn strips_gateway_query_key_from_claude_messages_url() {
    let transport = sample_transport(
        "custom",
        "claude:messages",
        "https://api.anthropic.example/v1?region=us",
        None,
    );

    let url = build_transport_request_url(
        &transport,
        TransportRequestUrlParams {
            provider_api_format: "claude:messages",
            mapped_model: Some("claude-sonnet-4"),
            upstream_is_stream: false,
            request_query: Some("key=gateway-secret&trace=1"),
            kiro_api_region: None,
            api_operation: Some(aether_ai_formats::ApiOperation::ClaudeMessagesCreate),
        },
    )
    .expect("messages url");

    assert_eq!(
        url,
        "https://api.anthropic.example/v1/messages?region=us&trace=1"
    );
    assert!(!url.contains("gateway-secret"));
}

#[test]
fn routes_claude_count_tokens_as_an_operation_on_messages_format() {
    let mut transport = sample_transport(
        "custom",
        "claude:messages",
        "https://api.anthropic.example/v1",
        None,
    );
    transport.endpoint.config = Some(json!({
        "anthropic": {
            "supported_operations": ["messages", "count_tokens"]
        }
    }));

    let url = build_transport_request_url(
        &transport,
        TransportRequestUrlParams {
            provider_api_format: "claude:messages",
            mapped_model: Some("claude-sonnet-4"),
            upstream_is_stream: false,
            request_query: Some("key=gateway-secret&trace=1"),
            kiro_api_region: None,
            api_operation: Some(aether_ai_formats::ApiOperation::ClaudeCountTokens),
        },
    )
    .expect("count_tokens url");

    assert_eq!(
        url,
        "https://api.anthropic.example/v1/messages/count_tokens?trace=1"
    );
}

#[test]
fn count_tokens_default_preserves_custom_anthropic_prefix() {
    let transport = sample_transport(
        "custom",
        "claude:messages",
        "https://proxy.example/anthropic?key=base-secret&tenant=base",
        None,
    );

    assert_eq!(
        build_transport_request_url(
            &transport,
            TransportRequestUrlParams {
                provider_api_format: "claude:messages",
                mapped_model: Some("claude-sonnet-4"),
                upstream_is_stream: false,
                request_query: Some("KEY=client-secret&trace=1"),
                kiro_api_region: None,
                api_operation: Some(aether_ai_formats::ApiOperation::ClaudeCountTokens),
            },
        )
        .as_deref(),
        Some(
            "https://proxy.example/anthropic/messages/count_tokens?key=base-secret&tenant=base&trace=1"
        )
    );
}

#[test]
fn count_tokens_uses_operation_aware_custom_path() {
    let mut transport = sample_transport(
        "custom",
        "claude:messages",
        "https://proxy.example/anthropic",
        Some("/operations/{operation}"),
    );
    transport.endpoint.config = Some(json!({
        "anthropic": {"supported_operations": ["messages", "count_tokens"]}
    }));

    assert_eq!(
        build_transport_request_url(
            &transport,
            TransportRequestUrlParams {
                provider_api_format: "claude:messages",
                mapped_model: Some("claude-sonnet-4"),
                upstream_is_stream: false,
                request_query: Some("key=client-secret&trace=1"),
                kiro_api_region: None,
                api_operation: Some(aether_ai_formats::ApiOperation::ClaudeCountTokens),
            },
        )
        .as_deref(),
        Some("https://proxy.example/anthropic/operations/count_tokens?trace=1")
    );
}

#[test]
fn count_tokens_derives_from_messages_only_custom_path() {
    let transport = sample_transport(
        "custom",
        "claude:messages",
        "https://api.anthropic.example",
        Some("/custom/v1/messages"),
    );

    assert_eq!(
        build_transport_request_url(
            &transport,
            TransportRequestUrlParams {
                provider_api_format: "claude:messages",
                mapped_model: Some("claude-sonnet-4"),
                upstream_is_stream: false,
                request_query: None,
                kiro_api_region: None,
                api_operation: Some(aether_ai_formats::ApiOperation::ClaudeCountTokens),
            },
        )
        .as_deref(),
        Some("https://api.anthropic.example/custom/v1/messages/count_tokens")
    );
}

#[test]
fn count_tokens_keeps_complete_custom_count_tokens_path() {
    let transport = sample_transport(
        "custom",
        "claude:messages",
        "https://api.anthropic.example",
        Some("/custom/v1/messages/count_tokens?key=path-secret"),
    );

    assert_eq!(
        build_transport_request_url(
            &transport,
            TransportRequestUrlParams {
                provider_api_format: "claude:messages",
                mapped_model: Some("claude-sonnet-4"),
                upstream_is_stream: false,
                request_query: Some("key=client-secret&trace=1"),
                kiro_api_region: None,
                api_operation: Some(aether_ai_formats::ApiOperation::ClaudeCountTokens),
            },
        )
        .as_deref(),
        Some(
            "https://api.anthropic.example/custom/v1/messages/count_tokens?key=path-secret&trace=1"
        )
    );
}

#[test]
fn messages_create_normalizes_complete_custom_count_tokens_path() {
    let transport = sample_transport(
        "custom",
        "claude:messages",
        "https://api.anthropic.example",
        Some("/custom/v1/messages/count_tokens?key=path-secret"),
    );

    assert_eq!(
        build_transport_request_url(
            &transport,
            TransportRequestUrlParams {
                provider_api_format: "claude:messages",
                mapped_model: Some("claude-sonnet-4"),
                upstream_is_stream: false,
                request_query: Some("key=client-secret&trace=1"),
                kiro_api_region: None,
                api_operation: Some(aether_ai_formats::ApiOperation::ClaudeMessagesCreate),
            },
        )
        .as_deref(),
        Some("https://api.anthropic.example/custom/v1/messages?key=path-secret&trace=1")
    );
}

#[test]
fn private_anthropic_adapters_reject_count_tokens_even_when_config_claims_support() {
    for provider_type in ["kiro", "grok"] {
        let mut transport = sample_transport(
            provider_type,
            "claude:messages",
            "https://private.example",
            None,
        );
        transport.endpoint.config = Some(json!({
            "anthropic": {
                "supported_operations": ["messages", "count_tokens"],
                "count_tokens_path": "/v1/messages/count_tokens"
            }
        }));

        assert!(build_transport_request_url(
            &transport,
            TransportRequestUrlParams {
                provider_api_format: "claude:messages",
                mapped_model: Some("claude-sonnet-4"),
                upstream_is_stream: false,
                request_query: None,
                kiro_api_region: Some("us-east-1"),
                api_operation: Some(ApiOperation::ClaudeCountTokens),
            },
        )
        .is_none());
    }
}

#[test]
fn claude_code_custom_root_keeps_messages_and_count_tokens_on_v1_surface() {
    let mut transport = sample_transport(
        "claude_code",
        "claude:messages",
        "https://proxy.example?key=base-secret",
        None,
    );
    transport.endpoint.config = Some(json!({
        "anthropic": {"supported_operations": ["messages", "count_tokens"]}
    }));

    assert_eq!(
        build_transport_request_url(
            &transport,
            TransportRequestUrlParams {
                provider_api_format: "claude:messages",
                mapped_model: Some("claude-sonnet-4"),
                upstream_is_stream: false,
                request_query: Some("key=client-secret&trace=messages"),
                kiro_api_region: None,
                api_operation: Some(aether_ai_formats::ApiOperation::ClaudeMessagesCreate),
            },
        )
        .as_deref(),
        Some("https://proxy.example/v1/messages?key=base-secret&trace=messages")
    );
    assert_eq!(
        build_transport_request_url(
            &transport,
            TransportRequestUrlParams {
                provider_api_format: "claude:messages",
                mapped_model: Some("claude-sonnet-4"),
                upstream_is_stream: false,
                request_query: Some("key=client-secret&trace=count"),
                kiro_api_region: None,
                api_operation: Some(aether_ai_formats::ApiOperation::ClaudeCountTokens),
            },
        )
        .as_deref(),
        Some("https://proxy.example/v1/messages/count_tokens?key=base-secret&trace=count")
    );
}

#[test]
fn claude_code_official_api_root_builds_documented_messages_urls() {
    let mut transport = sample_transport(
        "claude_code",
        "claude:messages",
        "https://api.anthropic.com/v1",
        None,
    );
    transport.endpoint.config = Some(json!({
        "anthropic": {"supported_operations": ["messages", "count_tokens"]}
    }));

    for (operation, expected_url) in [
        (
            ApiOperation::ClaudeMessagesCreate,
            "https://api.anthropic.com/v1/messages",
        ),
        (
            ApiOperation::ClaudeCountTokens,
            "https://api.anthropic.com/v1/messages/count_tokens",
        ),
    ] {
        assert_eq!(
            build_transport_request_url(
                &transport,
                TransportRequestUrlParams {
                    provider_api_format: "claude:messages",
                    mapped_model: Some("claude-opus-4-6"),
                    upstream_is_stream: false,
                    request_query: None,
                    kiro_api_region: None,
                    api_operation: Some(operation),
                },
            )
            .as_deref(),
            Some(expected_url)
        );
    }
}

#[test]
fn count_tokens_config_fields_fall_back_from_endpoint_to_provider() {
    let mut transport = sample_transport(
        "custom",
        "claude:messages",
        "https://api.anthropic.example/v1?key=base-secret",
        None,
    );
    transport.endpoint.config = Some(json!({
        "anthropic": {"profile": "native_transparent"}
    }));
    transport.provider.config = Some(json!({
        "anthropic": {
            "supported_operations": ["messages", "count_tokens"],
            "count_tokens_path": "/v1/messages/provider_count_tokens?key=path-secret"
        }
    }));

    assert_eq!(
        build_transport_request_url(
            &transport,
            TransportRequestUrlParams {
                provider_api_format: "claude:messages",
                mapped_model: Some("claude-sonnet-4"),
                upstream_is_stream: false,
                request_query: Some("key=client-secret&trace=1"),
                kiro_api_region: None,
                api_operation: Some(aether_ai_formats::ApiOperation::ClaudeCountTokens),
            },
        )
        .as_deref(),
        Some(
            "https://api.anthropic.example/v1/messages/provider_count_tokens?key=path-secret&trace=1"
        )
    );
}

#[test]
fn rejects_count_tokens_when_provider_capability_excludes_it() {
    let mut transport = sample_transport(
        "custom",
        "claude:messages",
        "https://api.anthropic.example/v1",
        None,
    );
    transport.endpoint.config = Some(json!({
        "anthropic": {"profile": "native_transparent"}
    }));
    transport.provider.config = Some(json!({
        "anthropic": {"supported_operations": ["messages"]}
    }));

    assert!(build_transport_request_url(
        &transport,
        TransportRequestUrlParams {
            provider_api_format: "claude:messages",
            mapped_model: Some("claude-sonnet-4"),
            upstream_is_stream: false,
            request_query: None,
            kiro_api_region: None,
            api_operation: Some(aether_ai_formats::ApiOperation::ClaudeCountTokens),
        },
    )
    .is_none());
}

#[test]
fn preserves_configured_query_credentials_and_strips_client_credentials() {
    let transport = sample_transport(
        "custom",
        "claude:messages",
        "https://proxy.example/anthropic?key=base-secret&tenant=base",
        Some("/messages?key=path-secret&variant=custom"),
    );

    assert_eq!(
        build_transport_request_url(
            &transport,
            TransportRequestUrlParams {
                provider_api_format: "claude:messages",
                mapped_model: Some("claude-sonnet-4"),
                upstream_is_stream: false,
                request_query: Some("KEY=client-secret&trace=1"),
                kiro_api_region: None,
                api_operation: None,
            },
        )
        .as_deref(),
        Some(
            "https://proxy.example/anthropic/messages?key=path-secret&tenant=base&trace=1&variant=custom"
        )
    );
}

#[test]
fn kiro_cross_format_helper_uses_region_specific_generate_assistant_url() {
    let transport = sample_transport(
        "kiro",
        "claude:messages",
        "https://codewhisperer.{region}.amazonaws.com/",
        None,
    );

    let url = build_kiro_cross_format_upstream_url(
        &transport,
        "claude-sonnet-4",
        "claude:messages",
        true,
        Some("key=gateway-secret&conversationId=abc"),
        "us-west-2",
    )
    .expect("kiro url");

    assert!(
        url.starts_with("https://codewhisperer.us-west-2.amazonaws.com/generateAssistantResponse")
    );
    assert!(url.contains("conversationId=abc"));
    assert!(!url.contains("gateway-secret"));
}

#[test]
fn embedding_request_url_builds_provider_default_paths() {
    let openai = sample_transport(
        "openai",
        "openai:embedding",
        "https://api.openai.example/v1",
        None,
    );
    let jina = sample_transport(
        "jina",
        "jina:embedding",
        "https://api.jina.example/v1",
        None,
    );
    let gemini = sample_transport(
        "gemini",
        "gemini:embedding",
        "https://generativelanguage.googleapis.com/v1beta",
        None,
    );
    let doubao = sample_transport(
        "doubao",
        "doubao:embedding",
        "https://ark.volces.example/api/v3",
        None,
    );
    let aliyun = sample_transport(
        "aliyun",
        "aliyun:multimodal_embedding",
        "https://dashscope.aliyuncs.com",
        None,
    );

    assert_eq!(
        build_transport_request_url(
            &openai,
            TransportRequestUrlParams {
                provider_api_format: "openai:embedding",
                mapped_model: Some("text-embedding-3-small"),
                upstream_is_stream: false,
                request_query: Some("tenant=demo"),
                kiro_api_region: None,
                api_operation: None,
            },
        )
        .as_deref(),
        Some("https://api.openai.example/v1/embeddings?tenant=demo")
    );
    assert_eq!(
        build_transport_request_url(
            &jina,
            TransportRequestUrlParams {
                provider_api_format: "jina:embedding",
                mapped_model: Some("jina-embeddings-v3"),
                upstream_is_stream: false,
                request_query: None,
                kiro_api_region: None,
                api_operation: None,
            },
        )
        .as_deref(),
        Some("https://api.jina.example/v1/embeddings")
    );
    assert_eq!(
        build_transport_request_url(
            &gemini,
            TransportRequestUrlParams {
                provider_api_format: "gemini:embedding",
                mapped_model: Some("gemini-embedding-001"),
                upstream_is_stream: false,
                request_query: Some("key=client-key&foo=bar"),
                kiro_api_region: None,
                api_operation: None,
            },
        )
        .as_deref(),
        Some(
            "https://generativelanguage.googleapis.com/v1beta/models/gemini-embedding-001:embedContent?foo=bar"
        )
    );
    assert_eq!(
        build_transport_request_url(
            &doubao,
            TransportRequestUrlParams {
                provider_api_format: "doubao:embedding",
                mapped_model: Some("doubao-embedding-text-240515"),
                upstream_is_stream: false,
                request_query: None,
                kiro_api_region: None,
                api_operation: None,
            },
        )
        .as_deref(),
        Some("https://ark.volces.example/api/v3/embeddings")
    );
    assert_eq!(
        build_transport_request_url(
            &aliyun,
            TransportRequestUrlParams {
                provider_api_format: "aliyun:multimodal_embedding",
                mapped_model: Some("qwen3-vl-embedding"),
                upstream_is_stream: false,
                request_query: None,
                kiro_api_region: None,
                api_operation: None,
            },
        )
        .as_deref(),
        Some("https://dashscope.aliyuncs.com/api/v1/services/embeddings/multimodal-embedding/multimodal-embedding")
    );
}

#[test]
fn gemini_interactions_request_url_uses_stable_v1_endpoint() {
    let gemini = sample_transport(
        "gemini",
        "gemini:interactions",
        "https://generativelanguage.googleapis.com",
        None,
    );
    let versioned = sample_transport(
        "gemini",
        "gemini:interactions",
        "https://generativelanguage.googleapis.com/v1",
        None,
    );

    assert_eq!(
        build_transport_request_url(
            &gemini,
            TransportRequestUrlParams {
                provider_api_format: "gemini:interactions",
                mapped_model: Some("gemini-3.5-flash"),
                upstream_is_stream: false,
                request_query: Some("key=client-key&trace=1"),
                kiro_api_region: None,
                api_operation: None,
            },
        )
        .as_deref(),
        Some("https://generativelanguage.googleapis.com/v1/interactions?trace=1")
    );
    assert_eq!(
        build_transport_request_url(
            &versioned,
            TransportRequestUrlParams {
                provider_api_format: "gemini:interactions",
                mapped_model: Some("gemini-3.5-flash"),
                upstream_is_stream: true,
                request_query: Some("key=client-key&trace=2"),
                kiro_api_region: None,
                api_operation: None,
            },
        )
        .as_deref(),
        Some("https://generativelanguage.googleapis.com/v1/interactions?trace=2")
    );
}

#[test]
fn antigravity_hook_is_limited_to_generate_content() {
    let transport = sample_transport(
        "antigravity",
        "gemini:interactions",
        "https://daily-cloudcode-pa.googleapis.com",
        None,
    );

    assert_eq!(
        build_transport_request_url(
            &transport,
            TransportRequestUrlParams {
                provider_api_format: "gemini:interactions",
                mapped_model: Some("gemini-3.5-flash"),
                upstream_is_stream: false,
                request_query: Some("key=client-key"),
                kiro_api_region: None,
                api_operation: None,
            },
        )
        .as_deref(),
        Some("https://daily-cloudcode-pa.googleapis.com/v1/interactions")
    );
}

#[test]
fn antigravity_generate_content_drops_client_api_key_query() {
    let transport = sample_transport(
        "antigravity",
        "gemini:generate_content",
        "https://daily-cloudcode-pa.googleapis.com",
        None,
    );

    assert_eq!(
        build_transport_request_url(
            &transport,
            TransportRequestUrlParams {
                provider_api_format: "gemini:generate_content",
                mapped_model: Some("gemini-3-flash-agent"),
                upstream_is_stream: true,
                request_query: Some("key=client-aether-key&trace=1&beta=true"),
                kiro_api_region: None,
                api_operation: None,
            },
        )
        .as_deref(),
        Some(
            "https://daily-cloudcode-pa.googleapis.com/v1internal:streamGenerateContent?alt=sse&trace=1"
        )
    );
}

#[test]
fn embedding_request_url_preserves_google_openai_compat_roots() {
    let developer_api_openai = sample_transport(
        "custom",
        "openai:embedding",
        "https://generativelanguage.googleapis.com/v1beta/openai",
        None,
    );
    let vertex_openai = sample_transport(
        "custom",
        "openai:embedding",
        "https://aiplatform.googleapis.com/v1/projects/project-1/locations/global/endpoints/openapi",
        None,
    );

    assert_eq!(
        build_transport_request_url(
            &developer_api_openai,
            TransportRequestUrlParams {
                provider_api_format: "openai:embedding",
                mapped_model: Some("gemini-embedding-001"),
                upstream_is_stream: false,
                request_query: Some("trace=1"),
                kiro_api_region: None,
                api_operation: None,
            },
        )
        .as_deref(),
        Some("https://generativelanguage.googleapis.com/v1beta/openai/embeddings?trace=1")
    );
    assert_eq!(
        build_transport_request_url(
            &vertex_openai,
            TransportRequestUrlParams {
                provider_api_format: "openai:embedding",
                mapped_model: Some("gemini-embedding-001"),
                upstream_is_stream: false,
                request_query: None,
                kiro_api_region: None,
                api_operation: None,
            },
        )
        .as_deref(),
        Some(
            "https://aiplatform.googleapis.com/v1/projects/project-1/locations/global/endpoints/openapi/embeddings"
        )
    );
}

#[test]
fn gemini_embedding_batch_body_uses_batch_endpoint() {
    let gemini = sample_transport(
        "gemini",
        "gemini:embedding",
        "https://generativelanguage.googleapis.com/v1beta",
        None,
    );
    let batch_body = json!({
        "requests": [
            {
                "model": "models/gemini-embedding-001",
                "content": {"parts": [{"text": "alpha"}]}
            },
            {
                "model": "models/gemini-embedding-001",
                "content": {"parts": [{"text": "beta"}]}
            }
        ]
    });

    assert_eq!(
        build_transport_request_url_for_request_body(
            &gemini,
            TransportRequestUrlParams {
                provider_api_format: "gemini:embedding",
                mapped_model: Some("gemini-embedding-001"),
                upstream_is_stream: false,
                request_query: Some("key=client-key&foo=bar"),
                kiro_api_region: None,
                api_operation: None,
            },
            Some(&batch_body),
        )
        .as_deref(),
        Some(
            "https://generativelanguage.googleapis.com/v1beta/models/gemini-embedding-001:batchEmbedContents?foo=bar"
        )
    );
}

#[test]
fn gemini_embedding_custom_action_template_follows_batch_body() {
    let gemini = sample_transport(
        "gemini",
        "gemini:embedding",
        "https://generativelanguage.googleapis.com",
        Some("/v1beta/models/{model}:{action}"),
    );
    let batch_body = json!({
        "requests": [
            {
                "model": "models/gemini-embedding-001",
                "content": {"parts": [{"text": "alpha"}]}
            }
        ]
    });

    assert_eq!(
        build_transport_request_url_for_request_body(
            &gemini,
            TransportRequestUrlParams {
                provider_api_format: "gemini:embedding",
                mapped_model: Some("gemini-embedding-001"),
                upstream_is_stream: false,
                request_query: None,
                kiro_api_region: None,
                api_operation: None,
            },
            Some(&batch_body),
        )
        .as_deref(),
        Some(
            "https://generativelanguage.googleapis.com/v1beta/models/gemini-embedding-001:batchEmbedContents"
        )
    );
}

#[test]
fn rerank_request_url_builds_provider_default_paths() {
    let openai = sample_transport(
        "openai",
        "openai:rerank",
        "https://api.openai.example/v1",
        None,
    );
    let jina = sample_transport("jina", "jina:rerank", "https://api.jina.example/v1", None);

    assert_eq!(
        build_transport_request_url(
            &openai,
            TransportRequestUrlParams {
                provider_api_format: "openai:rerank",
                mapped_model: Some("rerank-1"),
                upstream_is_stream: false,
                request_query: Some("tenant=demo"),
                kiro_api_region: None,
                api_operation: None,
            },
        )
        .as_deref(),
        Some("https://api.openai.example/v1/rerank?tenant=demo")
    );
    assert_eq!(
        build_transport_request_url(
            &jina,
            TransportRequestUrlParams {
                provider_api_format: "jina:rerank",
                mapped_model: Some("jina-reranker-v2-base-multilingual"),
                upstream_is_stream: false,
                request_query: None,
                kiro_api_region: None,
                api_operation: None,
            },
        )
        .as_deref(),
        Some("https://api.jina.example/v1/rerank")
    );
}

#[test]
fn embedding_request_url_handles_base_variants_and_queries() {
    let openai_without_v1 = sample_transport(
        "openai",
        "openai:embedding",
        "https://api.openai.example/root?tenant=base",
        None,
    );
    let jina_with_v1 = sample_transport(
        "jina",
        "jina:embedding",
        "https://api.jina.example/v1/",
        None,
    );
    let gemini_model_base = sample_transport(
        "gemini",
        "gemini:embedding",
        "https://generativelanguage.googleapis.com/v1beta/models/gemini-embedding-001",
        None,
    );
    let doubao_with_query = sample_transport(
        "doubao",
        "doubao:embedding",
        "https://ark.volces.example/api/v3?tenant=base",
        None,
    );

    assert_eq!(
        build_transport_request_url(
            &openai_without_v1,
            TransportRequestUrlParams {
                provider_api_format: "OPENAI:EMBEDDING",
                mapped_model: Some("text-embedding-3-small"),
                upstream_is_stream: false,
                request_query: Some("tenant=request&trace=1"),
                kiro_api_region: None,
                api_operation: None,
            },
        )
        .as_deref(),
        Some("https://api.openai.example/root/embeddings?tenant=request&trace=1")
    );
    assert_eq!(
        build_transport_request_url(
            &jina_with_v1,
            TransportRequestUrlParams {
                provider_api_format: "jina:embedding",
                mapped_model: Some("jina-embeddings-v3"),
                upstream_is_stream: false,
                request_query: Some("trace=2"),
                kiro_api_region: None,
                api_operation: None,
            },
        )
        .as_deref(),
        Some("https://api.jina.example/v1/embeddings?trace=2")
    );
    assert_eq!(
        build_transport_request_url(
            &gemini_model_base,
            TransportRequestUrlParams {
                provider_api_format: "gemini:embedding",
                mapped_model: Some("gemini-embedding-001"),
                upstream_is_stream: false,
                request_query: Some("key=client-key&trace=3"),
                kiro_api_region: None,
                api_operation: None,
            },
        )
        .as_deref(),
        Some("https://generativelanguage.googleapis.com/v1beta/models/gemini-embedding-001:embedContent?trace=3")
    );
    assert_eq!(
        build_transport_request_url(
            &doubao_with_query,
            TransportRequestUrlParams {
                provider_api_format: "doubao:embedding",
                mapped_model: Some("doubao-embedding-text-240515"),
                upstream_is_stream: false,
                request_query: Some("trace=4"),
                kiro_api_region: None,
                api_operation: None,
            },
        )
        .as_deref(),
        Some("https://ark.volces.example/api/v3/embeddings?tenant=base&trace=4")
    );
}

#[test]
fn gemini_embedding_request_url_requires_mapped_model_without_custom_path() {
    let transport = sample_transport(
        "gemini",
        "gemini:embedding",
        "https://generativelanguage.googleapis.com/v1beta",
        None,
    );

    assert!(build_transport_request_url(
        &transport,
        TransportRequestUrlParams {
            provider_api_format: "gemini:embedding",
            mapped_model: None,
            upstream_is_stream: false,
            request_query: None,
            kiro_api_region: None,
            api_operation: None,
        },
    )
    .is_none());
}

#[test]
fn embedding_request_url_expands_custom_gemini_embed_action() {
    let transport = sample_transport(
        "custom",
        "gemini:embedding",
        "https://generativelanguage.googleapis.com",
        Some("/v1beta/models/{model}:{action}"),
    );

    let url = build_transport_request_url(
        &transport,
        TransportRequestUrlParams {
            provider_api_format: "gemini:embedding",
            mapped_model: Some("gemini-embedding-001"),
            upstream_is_stream: true,
            request_query: Some("key=client-key&foo=bar"),
            kiro_api_region: None,
            api_operation: None,
        },
    )
    .expect("expanded custom embedding path url");

    assert_eq!(
        url,
        "https://generativelanguage.googleapis.com/v1beta/models/gemini-embedding-001:embedContent?foo=bar"
    );
}

#[test]
fn custom_path_template_encodes_dynamic_model_as_one_path_segment() {
    let transport = sample_transport(
        "custom",
        "gemini:generate_content",
        "https://generativelanguage.googleapis.com",
        Some("/v1beta/models/{model}:{action}"),
    );

    let url = build_transport_request_url(
        &transport,
        TransportRequestUrlParams {
            provider_api_format: "gemini:generate_content",
            mapped_model: Some("model/../../admin?key=attacker#fragment"),
            upstream_is_stream: false,
            request_query: None,
            kiro_api_region: None,
            api_operation: None,
        },
    )
    .expect("custom path should remain on the configured origin");

    assert_eq!(
        url,
        "https://generativelanguage.googleapis.com/v1beta/models/model%2F..%2F..%2Fadmin%3Fkey=attacker%23fragment:generateContent"
    );
}

#[test]
fn custom_path_template_rejects_dot_only_dynamic_path_segments() {
    let transport = sample_transport(
        "custom",
        "claude:messages",
        "https://api.example.com",
        Some("/v1/messages/{model}/invoke"),
    );

    assert_eq!(
        build_transport_request_url(
            &transport,
            TransportRequestUrlParams {
                provider_api_format: "claude:messages",
                mapped_model: Some(".."),
                upstream_is_stream: false,
                request_query: None,
                kiro_api_region: None,
                api_operation: None,
            },
        ),
        None
    );
}

#[test]
fn custom_path_template_query_values_cannot_inject_parameters() {
    let transport = sample_transport(
        "custom",
        "claude:messages",
        "https://api.example.com",
        Some("/v1/messages?model={model}"),
    );

    let url = build_transport_request_url(
        &transport,
        TransportRequestUrlParams {
            provider_api_format: "claude:messages",
            mapped_model: Some("claude&admin=true#fragment"),
            upstream_is_stream: false,
            request_query: None,
            kiro_api_region: None,
            api_operation: None,
        },
    )
    .expect("query template should build");

    assert_eq!(
        url,
        "https://api.example.com/v1/messages?model=claude%26admin%3Dtrue%23fragment"
    );
}

#[test]
fn xai_oauth_responses_use_cli_chat_proxy() {
    let mut transport = sample_transport(
        "xai",
        "openai:responses",
        "https://cli-chat-proxy.grok.com/v1",
        None,
    );
    transport.key.auth_type = "oauth".to_string();
    transport.key.decrypted_auth_config =
        Some(r#"{"refresh_token":"rt","using_api":false}"#.to_string());

    let url = build_transport_request_url(
        &transport,
        TransportRequestUrlParams {
            provider_api_format: "openai:responses",
            mapped_model: Some("grok-4"),
            upstream_is_stream: true,
            request_query: None,
            kiro_api_region: None,
            api_operation: None,
        },
    )
    .expect("xai oauth responses URL");

    assert_eq!(url, "https://cli-chat-proxy.grok.com/v1/responses");
}

#[test]
fn xai_compact_and_using_api_use_official_api() {
    let mut oauth = sample_transport(
        "xai",
        "openai:responses:compact",
        "https://cli-chat-proxy.grok.com/v1",
        None,
    );
    oauth.key.auth_type = "oauth".to_string();
    oauth.key.decrypted_auth_config =
        Some(r#"{"refresh_token":"rt","using_api":false}"#.to_string());

    let compact = build_transport_request_url(
        &oauth,
        TransportRequestUrlParams {
            provider_api_format: "openai:responses:compact",
            mapped_model: Some("grok-4"),
            upstream_is_stream: false,
            request_query: None,
            kiro_api_region: None,
            api_operation: None,
        },
    )
    .expect("xai compact URL");
    assert_eq!(compact, "https://api.x.ai/v1/responses/compact");

    let mut api_key = sample_transport(
        "xai",
        "openai:responses",
        "https://cli-chat-proxy.grok.com/v1",
        None,
    );
    api_key.key.auth_type = "oauth".to_string();
    api_key.key.decrypted_auth_config = Some(r#"{"using_api":true}"#.to_string());

    let official = build_transport_request_url(
        &api_key,
        TransportRequestUrlParams {
            provider_api_format: "openai:responses",
            mapped_model: Some("grok-4"),
            upstream_is_stream: true,
            request_query: None,
            kiro_api_region: None,
            api_operation: None,
        },
    )
    .expect("xai api key URL");
    assert_eq!(official, "https://api.x.ai/v1/responses");
}
