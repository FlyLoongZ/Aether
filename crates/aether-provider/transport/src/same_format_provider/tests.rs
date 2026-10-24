use std::collections::BTreeMap;

use super::*;
use crate::snapshot::{
    GatewayProviderTransportEndpoint, GatewayProviderTransportKey, GatewayProviderTransportProvider,
};
use serde_json::json;

#[test]
fn memories_preserves_native_traces_and_does_not_apply_responses_stream_policy() {
    let body = json!({"model": "memory-global", "reasoning": {"effort": "high"},
        "traces": [{"id": "synthetic-trace", "items": [{"future": 7}]}],
        "future_request": {"opaque": [1, 2, 3]}});
    let input = SameFormatProviderRequestBodyInput {
        body_json: &body,
        mapped_model: "memory-upstream",
        client_api_format: "openai:responses",
        provider_api_format: "openai:responses",
        source_model: Some("memory-global"),
        family: SameFormatProviderFamily::Standard,
        body_rules: None,
        request_headers: None,
        upstream_is_stream: false,
        force_body_stream_field: true,
        kiro_auth_config: None,
        is_claude_code: false,
        is_vertex: false,
        enable_model_directives: false,
    };
    let result = build_same_format_provider_request_body_for_operation(
        input,
        aether_ai_formats::OpenAiResponsesReasoningReplayPolicy::OpenAiItemIds,
        Some(aether_ai_formats::ApiOperation::OpenAiMemoriesSummarize),
    )
    .unwrap();
    let mut expected = body.clone();
    expected["model"] = json!("memory-upstream");
    assert_eq!(result.body, expected);
    assert!(result.body.get("input").is_none());
    assert!(result.body.get("stream").is_none());
}

fn sample_transport(provider_type: &str) -> GatewayProviderTransportSnapshot {
    GatewayProviderTransportSnapshot {
        provider: GatewayProviderTransportProvider {
            id: "provider-1".to_string(),
            name: "Provider".to_string(),
            provider_type: provider_type.to_string(),
            website: None,
            is_active: true,
            keep_priority_on_conversion: false,
            enable_format_conversion: true,
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
            api_format: "openai:chat".to_string(),
            api_family: None,
            endpoint_kind: None,
            is_active: true,
            base_url: "https://api.example.test".to_string(),
            header_rules: None,
            body_rules: None,
            max_retries: None,
            custom_path: None,
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
            decrypted_api_key: "secret".to_string(),
            decrypted_auth_config: None,
        },
    }
}

#[test]
fn classifies_streaming_and_report_kind_for_provider_private_transports() {
    let kiro = sample_transport("kiro");
    let behavior = classify_same_format_provider_request_behavior(
        &kiro,
        SameFormatProviderRequestBehaviorParams {
            require_streaming: false,
            provider_api_format: "claude:messages",
            report_kind: "claude_chat_sync_success",
        },
    );

    assert!(behavior.is_kiro);
    assert!(behavior.upstream_is_stream);
    assert_eq!(behavior.report_kind, "claude_cli_sync_finalize");

    let antigravity = sample_transport("antigravity");
    let behavior = classify_same_format_provider_request_behavior(
        &antigravity,
        SameFormatProviderRequestBehaviorParams {
            require_streaming: false,
            provider_api_format: "gemini:generate_content",
            report_kind: "gemini_chat_sync_success",
        },
    );

    assert!(behavior.is_antigravity);
    assert!(behavior.upstream_is_stream);
    assert_eq!(behavior.report_kind, "gemini_chat_sync_finalize");

    let gemini_cli = sample_transport("gemini_cli");
    let behavior = classify_same_format_provider_request_behavior(
        &gemini_cli,
        SameFormatProviderRequestBehaviorParams {
            require_streaming: false,
            provider_api_format: "gemini:generate_content",
            report_kind: "gemini_cli_sync_success",
        },
    );

    assert!(!behavior.upstream_is_stream);
    assert_eq!(behavior.report_kind, "gemini_cli_sync_finalize");
}

#[test]
fn anthropic_compatibility_profile_controls_legacy_claude_code_edits() {
    let mut native = sample_transport("custom");
    native.endpoint.api_format = "claude:messages".to_string();
    let native_behavior = classify_same_format_provider_request_behavior(
        &native,
        SameFormatProviderRequestBehaviorParams {
            require_streaming: false,
            provider_api_format: "claude:messages",
            report_kind: "claude_chat_sync_success",
        },
    );
    assert!(!native_behavior.is_claude_code);
    assert!(!native_behavior.is_claude_code_transport);
    assert_eq!(
        native_behavior.anthropic_compatibility_profile,
        AnthropicCompatibilityProfile::NativeTransparent
    );

    native.endpoint.config = Some(json!({
        "anthropic": {"compatibility_profile": "claude_code_legacy"}
    }));
    let compat_behavior = classify_same_format_provider_request_behavior(
        &native,
        SameFormatProviderRequestBehaviorParams {
            require_streaming: false,
            provider_api_format: "claude:messages",
            report_kind: "claude_chat_sync_success",
        },
    );
    assert!(compat_behavior.is_claude_code);
    assert!(!compat_behavior.is_claude_code_transport);
    assert_eq!(
        compat_behavior.anthropic_compatibility_profile,
        AnthropicCompatibilityProfile::ClaudeCodeLegacy
    );

    let request_body = json!({
        "model": "claude-client",
        "thinking": {"type": "enabled"},
        "context_management": {"edits": [{"type": "client_strategy"}]},
        "system": [{
            "type": "text",
            "text": "x-anthropic-billing-header: cc_version=9.9.9.abc; cc_entrypoint=cli;"
        }],
        "messages": [{
            "role": "assistant",
            "content": [
                {"type": "text", "text": "visible"},
                {"type": "thinking", "thinking": "unsigned", "signature": ""}
            ]
        }]
    });
    let build_body = |behavior: SameFormatProviderRequestBehavior| {
        build_same_format_provider_request_body(SameFormatProviderRequestBodyInput {
            body_json: &request_body,
            mapped_model: "claude-upstream",
            client_api_format: "claude:messages",
            provider_api_format: "claude:messages",
            source_model: Some("claude-client"),
            family: SameFormatProviderFamily::Standard,
            body_rules: None,
            request_headers: None,
            upstream_is_stream: false,
            force_body_stream_field: false,
            kiro_auth_config: None,
            is_claude_code: behavior.is_claude_code,
            is_vertex: behavior.is_vertex,
            enable_model_directives: false,
        })
        .expect("body should build")
    };
    assert_eq!(
        build_body(native_behavior)["messages"][0]["content"]
            .as_array()
            .map(Vec::len),
        Some(2),
        "native Anthropic requests must remain untouched"
    );
    assert_eq!(
        build_body(native_behavior)["system"][0]["text"],
        request_body["system"][0]["text"],
        "native transparent requests must not rewrite billing identity"
    );
    assert_eq!(
        build_body(native_behavior)["context_management"],
        request_body["context_management"],
        "native transparent requests must not apply Claude Code body gates"
    );
    assert_eq!(
        build_body(compat_behavior)["messages"][0]["content"]
            .as_array()
            .map(Vec::len),
        Some(1),
        "legacy compatibility may sanitize invalid thinking blocks"
    );
    assert_eq!(
        build_body(compat_behavior)["system"][0]["text"],
        "x-anthropic-billing-header: cc_version=2.1.284.abc; cc_entrypoint=cli;"
    );

    let mut legacy = sample_transport("claude_code");
    legacy.endpoint.api_format = "claude:messages".to_string();
    legacy.endpoint.config = Some(json!({
        "anthropic": {"compatibility_profile": "native_transparent"}
    }));
    let transparent_legacy_transport = classify_same_format_provider_request_behavior(
        &legacy,
        SameFormatProviderRequestBehaviorParams {
            require_streaming: false,
            provider_api_format: "claude:messages",
            report_kind: "claude_chat_sync_success",
        },
    );
    assert!(!transparent_legacy_transport.is_claude_code);
    assert!(transparent_legacy_transport.is_claude_code_transport);

    let provider_request_body = json!({"model": "claude-upstream"});
    let empty_headers = http::HeaderMap::new();
    let empty_extra_headers = BTreeMap::new();
    let build_headers =
        |behavior: SameFormatProviderRequestBehavior,
         api_operation: Option<aether_ai_formats::ApiOperation>| {
            build_same_format_provider_headers(SameFormatProviderHeadersInput {
                headers: &empty_headers,
                provider_request_body: &provider_request_body,
                original_request_body: &request_body,
                header_rules: None,
                behavior,
                api_operation,
                auth_header: Some("x-api-key"),
                auth_value: Some("upstream-secret"),
                extra_headers: &empty_extra_headers,
                kiro_auth_config: None,
                kiro_machine_id: None,
            })
            .expect("headers should build")
        };
    assert!(
        !build_headers(compat_behavior, None).contains_key("x-app"),
        "compatibility profile must not impersonate the Claude Code transport"
    );
    assert_eq!(
        build_headers(transparent_legacy_transport, None)
            .get("x-app")
            .map(String::as_str),
        Some("cli"),
        "Claude Code transport headers must survive a transparent body profile"
    );
    assert!(
        !build_headers(
            native_behavior,
            Some(aether_ai_formats::ApiOperation::ClaudeCountTokens),
        )
        .contains_key("anthropic-beta"),
        "native transparent token counting must not inject compatibility betas"
    );
    assert!(
        build_headers(
            compat_behavior,
            Some(aether_ai_formats::ApiOperation::ClaudeCountTokens),
        )["anthropic-beta"]
            .split(',')
            .any(|token| token == "token-counting-2024-11-01"),
        "legacy compatibility token counting requires the token-counting beta"
    );
    assert!(
        build_headers(
            transparent_legacy_transport,
            Some(aether_ai_formats::ApiOperation::ClaudeCountTokens),
        )["anthropic-beta"]
            .split(',')
            .any(|token| token == "token-counting-2024-11-01"),
        "Claude Code transport token counting requires the token-counting beta"
    );
}

#[test]
fn same_format_behavior_resolves_endpoint_stream_policy() {
    let mut force_stream = sample_transport("openai");
    force_stream.endpoint.config = Some(json!({
        "upstream_stream_policy": "force_stream"
    }));
    let behavior = classify_same_format_provider_request_behavior(
        &force_stream,
        SameFormatProviderRequestBehaviorParams {
            require_streaming: false,
            provider_api_format: "openai:chat",
            report_kind: "openai_chat_sync_success",
        },
    );
    assert!(behavior.upstream_is_stream);

    let mut force_non_stream = sample_transport("openai");
    force_non_stream.endpoint.config = Some(json!({
        "upstreamStreamPolicy": "force_non_stream"
    }));
    let behavior = classify_same_format_provider_request_behavior(
        &force_non_stream,
        SameFormatProviderRequestBehaviorParams {
            require_streaming: true,
            provider_api_format: "openai:chat",
            report_kind: "openai_chat_stream_success",
        },
    );
    assert!(!behavior.upstream_is_stream);

    let mut auto = sample_transport("openai");
    auto.endpoint.config = Some(json!({
        "upstream_stream": "auto"
    }));
    let stream_behavior = classify_same_format_provider_request_behavior(
        &auto,
        SameFormatProviderRequestBehaviorParams {
            require_streaming: true,
            provider_api_format: "openai:chat",
            report_kind: "openai_chat_stream_success",
        },
    );
    assert!(stream_behavior.upstream_is_stream);
    let sync_behavior = classify_same_format_provider_request_behavior(
        &auto,
        SameFormatProviderRequestBehaviorParams {
            require_streaming: false,
            provider_api_format: "openai:chat",
            report_kind: "openai_chat_sync_success",
        },
    );
    assert!(!sync_behavior.upstream_is_stream);

    let mut compact = sample_transport("codex");
    compact.endpoint.config = Some(json!({
        "upstream_stream_policy": "force_stream"
    }));
    let compact_behavior = classify_same_format_provider_request_behavior(
        &compact,
        SameFormatProviderRequestBehaviorParams {
            require_streaming: true,
            provider_api_format: "openai:responses:compact",
            report_kind: "openai_responses_compact_sync_success",
        },
    );
    assert!(!compact_behavior.upstream_is_stream);

    let mut search = sample_transport("codex");
    search.endpoint.config = Some(json!({
        "upstream_stream_policy": "force_stream"
    }));
    let search_behavior = classify_same_format_provider_request_behavior(
        &search,
        SameFormatProviderRequestBehaviorParams {
            require_streaming: true,
            provider_api_format: "openai:search",
            report_kind: "openai_search_sync_success",
        },
    );
    assert!(!search_behavior.upstream_is_stream);
    assert!(!search_behavior.force_body_stream_field);
}

#[test]
fn count_tokens_overrides_endpoint_stream_policy_and_removes_stream_field() {
    let mut transport = sample_transport("custom");
    transport.endpoint.api_format = "claude:messages".to_string();
    transport.endpoint.config = Some(json!({
        "upstream_stream_policy": "force_stream"
    }));
    let behavior = classify_same_format_provider_request_behavior_for_operation(
        &transport,
        SameFormatProviderRequestBehaviorParams {
            require_streaming: false,
            provider_api_format: "claude:messages",
            report_kind: "claude_count_tokens_sync_success",
        },
        Some(aether_ai_formats::ApiOperation::ClaudeCountTokens),
    );

    assert!(!behavior.upstream_is_stream);
    assert!(!behavior.force_body_stream_field);

    let mut body = json!({"model": "claude-sonnet-4", "messages": [], "stream": true});
    assert!(enforce_same_format_provider_api_operation_body_policy(
        &mut body,
        Some(aether_ai_formats::ApiOperation::ClaudeCountTokens),
    ));
    assert!(body.get("stream").is_none());

    let headers = build_same_format_provider_headers(SameFormatProviderHeadersInput {
        headers: &http::HeaderMap::new(),
        provider_request_body: &body,
        original_request_body: &body,
        header_rules: None,
        behavior,
        api_operation: Some(aether_ai_formats::ApiOperation::ClaudeCountTokens),
        auth_header: Some("x-api-key"),
        auth_value: Some("secret"),
        extra_headers: &BTreeMap::new(),
        kiro_auth_config: None,
        kiro_machine_id: None,
    })
    .expect("headers should build");
    assert_eq!(
        headers.get("accept").map(String::as_str),
        Some("application/json")
    );
}

#[test]
fn same_format_behavior_preserves_hard_streaming_constraint() {
    let mut kiro = sample_transport("kiro");
    kiro.endpoint.config = Some(json!({
        "upstream_stream_policy": "force_non_stream"
    }));

    let behavior = classify_same_format_provider_request_behavior(
        &kiro,
        SameFormatProviderRequestBehaviorParams {
            require_streaming: true,
            provider_api_format: "claude:messages",
            report_kind: "claude_chat_stream_success",
        },
    );

    assert!(behavior.upstream_is_stream);
}

#[test]
fn same_format_behavior_preserves_gemini_cli_streaming_requests() {
    let mut gemini_cli = sample_transport("gemini_cli");
    gemini_cli.endpoint.config = Some(json!({
        "upstream_stream_policy": "force_non_stream"
    }));

    let stream_behavior = classify_same_format_provider_request_behavior(
        &gemini_cli,
        SameFormatProviderRequestBehaviorParams {
            require_streaming: true,
            provider_api_format: "gemini:generate_content",
            report_kind: "gemini_cli_stream_success",
        },
    );
    assert!(stream_behavior.upstream_is_stream);

    let sync_behavior = classify_same_format_provider_request_behavior(
        &gemini_cli,
        SameFormatProviderRequestBehaviorParams {
            require_streaming: false,
            provider_api_format: "gemini:generate_content",
            report_kind: "gemini_cli_sync_success",
        },
    );
    assert!(!sync_behavior.upstream_is_stream);
}

#[test]
fn same_format_policy_resolution_drives_standard_body_stream_field() {
    for (endpoint_config, client_is_stream, expected_stream) in [
        (
            json!({"upstream_stream_policy": "force_stream"}),
            false,
            true,
        ),
        (
            json!({"upstreamStreamPolicy": "force_non_stream"}),
            true,
            false,
        ),
        (json!({"upstream_stream": "auto"}), true, true),
        (json!({"upstream_stream": "auto"}), false, false),
    ] {
        let mut transport = sample_transport("openai");
        transport.endpoint.config = Some(endpoint_config);
        let behavior = classify_same_format_provider_request_behavior(
            &transport,
            SameFormatProviderRequestBehaviorParams {
                require_streaming: client_is_stream,
                provider_api_format: "openai:chat",
                report_kind: "openai_chat_policy_test",
            },
        );
        let body = build_same_format_provider_request_body(SameFormatProviderRequestBodyInput {
            body_json: &json!({
                "model": "client-model",
                "messages": [{"role": "user", "content": "hello"}],
                "stream": client_is_stream
            }),
            mapped_model: "upstream-model",
            client_api_format: "openai:chat",
            provider_api_format: "openai:chat",
            source_model: Some("client-model"),
            family: SameFormatProviderFamily::Standard,
            body_rules: None,
            request_headers: None,
            upstream_is_stream: behavior.upstream_is_stream,
            force_body_stream_field: behavior.force_body_stream_field,
            kiro_auth_config: None,
            is_claude_code: false,
            is_vertex: false,
            enable_model_directives: false,
        })
        .expect("body should build");

        assert_eq!(body.get("stream"), Some(&json!(expected_stream)));
    }
}

#[test]
fn resolves_direct_auth_except_vertex() {
    let transport = sample_transport("openai");
    let behavior = classify_same_format_provider_request_behavior(
        &transport,
        SameFormatProviderRequestBehaviorParams {
            require_streaming: false,
            provider_api_format: "openai:chat",
            report_kind: "openai_chat_sync_success",
        },
    );

    assert_eq!(
        resolve_same_format_provider_direct_auth(
            &behavior,
            &transport,
            SameFormatProviderFamily::Standard,
            "openai:chat",
        ),
        Some(("x-api-key".to_string(), "secret".to_string()))
    );
}

#[test]
fn resolves_openai_embedding_direct_auth_with_bearer_header() {
    let mut transport = sample_transport("custom");
    transport.endpoint.api_format = "openai:embedding".to_string();
    transport.key.auth_type = "api_key".to_string();
    let behavior = classify_same_format_provider_request_behavior(
        &transport,
        SameFormatProviderRequestBehaviorParams {
            require_streaming: false,
            provider_api_format: "openai:embedding",
            report_kind: "openai_embedding_sync_success",
        },
    );

    assert_eq!(
        resolve_same_format_provider_direct_auth(
            &behavior,
            &transport,
            SameFormatProviderFamily::Standard,
            "openai:embedding",
        ),
        Some(("authorization".to_string(), "Bearer secret".to_string()))
    );
}

#[test]
fn resolves_openai_search_direct_auth_with_bearer_header() {
    let mut transport = sample_transport("custom");
    transport.endpoint.api_format = "openai:search".to_string();
    transport.key.auth_type = "api_key".to_string();
    let behavior = classify_same_format_provider_request_behavior(
        &transport,
        SameFormatProviderRequestBehaviorParams {
            require_streaming: false,
            provider_api_format: "openai:search",
            report_kind: "openai_search_sync_success",
        },
    );

    assert_eq!(
        resolve_same_format_provider_direct_auth(
            &behavior,
            &transport,
            SameFormatProviderFamily::Standard,
            "openai:search",
        ),
        Some(("authorization".to_string(), "Bearer secret".to_string()))
    );
}

#[test]
fn resolves_openai_realtime_direct_auth_with_bearer_header() {
    let mut transport = sample_transport("openai");
    transport.endpoint.api_format = "openai:realtime".to_string();
    transport.key.auth_type = "api_key".to_string();
    let behavior = classify_same_format_provider_request_behavior(
        &transport,
        SameFormatProviderRequestBehaviorParams {
            require_streaming: true,
            provider_api_format: "openai:realtime",
            report_kind: "openai_realtime_websocket_success",
        },
    );

    assert_eq!(
        resolve_same_format_provider_direct_auth(
            &behavior,
            &transport,
            SameFormatProviderFamily::Standard,
            "openai:realtime",
        ),
        Some(("authorization".to_string(), "Bearer secret".to_string()))
    );
}

#[test]
fn resolves_codex_live_direct_auth_with_bearer_header() {
    let mut transport = sample_transport("openai");
    transport.endpoint.api_format = "codex:live".to_string();
    transport.key.auth_type = "api_key".to_string();
    let behavior = classify_same_format_provider_request_behavior(
        &transport,
        SameFormatProviderRequestBehaviorParams {
            require_streaming: true,
            provider_api_format: "codex:live",
            report_kind: "codex_live_websocket_success",
        },
    );

    assert_eq!(
        resolve_same_format_provider_direct_auth(
            &behavior,
            &transport,
            SameFormatProviderFamily::Standard,
            "codex:live",
        ),
        Some(("authorization".to_string(), "Bearer secret".to_string()))
    );
}

#[test]
fn keeps_claude_same_format_api_key_on_x_api_key_header() {
    let mut transport = sample_transport("custom");
    transport.endpoint.api_format = "claude:messages".to_string();
    transport.key.auth_type = "api_key".to_string();
    let behavior = classify_same_format_provider_request_behavior(
        &transport,
        SameFormatProviderRequestBehaviorParams {
            require_streaming: false,
            provider_api_format: "claude:messages",
            report_kind: "claude_chat_sync_success",
        },
    );

    assert_eq!(
        resolve_same_format_provider_direct_auth(
            &behavior,
            &transport,
            SameFormatProviderFamily::Standard,
            "claude:messages",
        ),
        Some(("x-api-key".to_string(), "secret".to_string()))
    );
}

#[test]
fn builds_same_format_standard_body_with_mapped_model_and_stream_flag() {
    let body = build_same_format_provider_request_body(SameFormatProviderRequestBodyInput {
        body_json: &json!({
            "model": "client-model",
            "messages": [{"role": "user", "content": "hello"}]
        }),
        mapped_model: "upstream-model",
        client_api_format: "openai:chat",
        provider_api_format: "openai:chat",
        source_model: Some("client-model"),
        family: SameFormatProviderFamily::Standard,
        body_rules: None,
        request_headers: None,
        upstream_is_stream: true,
        force_body_stream_field: false,
        kiro_auth_config: None,
        is_claude_code: false,
        is_vertex: false,
        enable_model_directives: false,
    })
    .expect("body should build");

    assert_eq!(body.get("model"), Some(&json!("upstream-model")));
    assert_eq!(body.get("stream"), Some(&json!(true)));
}

#[test]
fn same_format_standard_body_preserves_fields_that_cross_format_would_block() {
    let body = build_same_format_provider_request_body(SameFormatProviderRequestBodyInput {
        body_json: &json!({
            "model": "client-model",
            "messages": [{"role": "user", "content": "hello"}],
            "n": 2,
            "reasoning_effort": "max",
            "unknown_vendor_field": {"keep": true}
        }),
        mapped_model: "client-model",
        client_api_format: "openai:chat",
        provider_api_format: "/v1/chat/completions",
        source_model: Some("client-model"),
        family: SameFormatProviderFamily::Standard,
        body_rules: None,
        request_headers: None,
        upstream_is_stream: false,
        force_body_stream_field: false,
        kiro_auth_config: None,
        is_claude_code: false,
        is_vertex: false,
        enable_model_directives: false,
    })
    .expect("same-format body should bypass canonical conversion");

    assert_eq!(body["n"], 2);
    assert_eq!(body["reasoning_effort"], "max");
    assert_eq!(body["unknown_vendor_field"]["keep"], true);
}

#[test]
fn same_format_responses_body_preserves_gpt_5_6_protocol_contract_verbatim() {
    let input = json!([
        {
            "type": "message",
            "role": "user",
            "content": [
                {
                    "type": "input_text",
                    "text": "before",
                    "prompt_cache_breakpoint": {"mode": "explicit"}
                },
                {"type": "input_text", "text": "after"}
            ]
        },
        {
            "type": "program",
            "call_id": "program-call-1",
            "fingerprint": "fp-program-1"
        },
        {
            "type": "program_output",
            "call_id": "program-call-1",
            "result": "hello",
            "status": "completed"
        },
        {
            "type": "multi_agent_call",
            "call_id": "agent-call-1",
            "action": "delegate",
            "arguments": {"query": "release notes"},
            "agent": "researcher"
        },
        {
            "type": "agent_message",
            "author": "researcher",
            "recipient": "assistant",
            "encrypted_content": "encrypted-agent-message"
        },
        {
            "type": "function_call_output",
            "call_id": "call-1",
            "output": "ok",
            "caller": {"type": "multi_agent", "id": "agent-call-1"}
        }
    ]);
    let prompt_cache_options = json!({
        "mode": "explicit",
        "ttl": "30m"
    });
    let body_json = json!({
        "model": "gpt-5.6-sol",
        "input": input,
        "reasoning": {
            "effort": "max",
            "mode": "pro",
            "context": "all_turns"
        },
        "prompt_cache_options": prompt_cache_options,
        "multi_agent": {
            "enabled": true,
            "max_concurrent_subagents": 2
        }
    });

    let body = build_same_format_provider_request_body(SameFormatProviderRequestBodyInput {
        body_json: &body_json,
        mapped_model: "gpt-5.6-sol",
        client_api_format: "openai:responses",
        provider_api_format: "openai:responses",
        source_model: Some("gpt-5.6-sol"),
        family: SameFormatProviderFamily::Standard,
        body_rules: None,
        request_headers: None,
        upstream_is_stream: false,
        force_body_stream_field: false,
        kiro_auth_config: None,
        is_claude_code: false,
        is_vertex: false,
        enable_model_directives: false,
    })
    .expect("valid GPT-5.6 Responses request should use raw same-format transport");

    assert_eq!(body["input"], input);
    assert_eq!(body["reasoning"], body_json["reasoning"]);
    assert_eq!(body["prompt_cache_options"], prompt_cache_options);
    assert_eq!(body["multi_agent"], body_json["multi_agent"]);
}

#[test]
fn same_format_prompt_cache_capability_uses_mapped_provider_model() {
    let body_json = json!({
        "model": "gpt-5.5",
        "messages": [{"role": "user", "content": "hello"}],
        "prompt_cache_options": {"mode": "explicit", "ttl": "30m"}
    });
    let body = build_same_format_provider_request_body(SameFormatProviderRequestBodyInput {
        body_json: &body_json,
        mapped_model: "gpt-5.6-sol",
        client_api_format: "openai:chat",
        provider_api_format: "openai:chat",
        source_model: Some("gpt-5.5"),
        family: SameFormatProviderFamily::Standard,
        body_rules: None,
        request_headers: None,
        upstream_is_stream: false,
        force_body_stream_field: false,
        kiro_auth_config: None,
        is_claude_code: false,
        is_vertex: false,
        enable_model_directives: false,
    })
    .expect("final GPT-5.6 provider model should accept prompt_cache_options");

    assert_eq!(body["model"], "gpt-5.6-sol");
    assert_eq!(
        body["prompt_cache_options"],
        body_json["prompt_cache_options"]
    );

    let rejected = build_same_format_provider_request_body(SameFormatProviderRequestBodyInput {
        body_json: &body_json,
        mapped_model: "gpt-5.5",
        client_api_format: "openai:chat",
        provider_api_format: "openai:chat",
        source_model: Some("gpt-5.5"),
        family: SameFormatProviderFamily::Standard,
        body_rules: None,
        request_headers: None,
        upstream_is_stream: false,
        force_body_stream_field: false,
        kiro_auth_config: None,
        is_claude_code: false,
        is_vertex: false,
        enable_model_directives: false,
    });
    assert!(rejected.is_none());
}

#[test]
fn same_format_reasoning_capability_uses_mapped_provider_model() {
    let body_json = json!({
        "model": "deployment-alias",
        "input": "hello",
        "reasoning": {"effort": "max"}
    });
    let build = |mapped_model: &str| {
        build_same_format_provider_request_body(SameFormatProviderRequestBodyInput {
            body_json: &body_json,
            mapped_model,
            client_api_format: "openai:responses",
            provider_api_format: "openai:responses",
            source_model: Some("deployment-alias"),
            family: SameFormatProviderFamily::Standard,
            body_rules: None,
            request_headers: None,
            upstream_is_stream: false,
            force_body_stream_field: false,
            kiro_auth_config: None,
            is_claude_code: false,
            is_vertex: false,
            enable_model_directives: false,
        })
    };

    assert!(build("gpt-5.6-sol").is_some());
    assert!(build("gpt-5.4").is_none());
}

#[test]
fn same_format_gpt_5_6_requires_prompt_cache_options() {
    let body_json = json!({
        "model": "client-alias",
        "input": "hello",
        "prompt_cache_retention": "24h"
    });
    let body = build_same_format_provider_request_body(SameFormatProviderRequestBodyInput {
        body_json: &body_json,
        mapped_model: "gpt-5.6-luna",
        client_api_format: "openai:responses",
        provider_api_format: "openai:responses",
        source_model: Some("client-alias"),
        family: SameFormatProviderFamily::Standard,
        body_rules: None,
        request_headers: None,
        upstream_is_stream: false,
        force_body_stream_field: false,
        kiro_auth_config: None,
        is_claude_code: false,
        is_vertex: false,
        enable_model_directives: false,
    });

    assert!(body.is_none());
}

#[test]
fn same_format_responses_body_preserves_opaque_extension_fields() {
    let opaque_option = json!({"mode": "custom"});
    let body = build_same_format_provider_request_body(SameFormatProviderRequestBodyInput {
        body_json: &json!({
            "model": "gpt-5.6-sol",
            "input": "hello",
            "multi_agent": {"enabled": true},
            "opaque_responses_option": opaque_option
        }),
        mapped_model: "gpt-5.6-sol",
        client_api_format: "openai:responses",
        provider_api_format: "openai:responses",
        source_model: Some("gpt-5.6-sol"),
        family: SameFormatProviderFamily::Standard,
        body_rules: None,
        request_headers: None,
        upstream_is_stream: false,
        force_body_stream_field: false,
        kiro_auth_config: None,
        is_claude_code: false,
        is_vertex: false,
        enable_model_directives: false,
    });

    let body = body.expect("same-format Responses request should remain transparent");
    assert_eq!(body["multi_agent"], json!({"enabled": true}));
    assert_eq!(body["opaque_responses_option"], opaque_option);
}

#[test]
fn cross_format_standard_body_fails_closed_for_lossy_chat_fields() {
    let body = build_same_format_provider_request_body(SameFormatProviderRequestBodyInput {
        body_json: &json!({
            "model": "client-model",
            "messages": [{"role": "user", "content": "hello"}],
            "n": 2
        }),
        mapped_model: "upstream-model",
        client_api_format: "openai:chat",
        provider_api_format: "openai:responses",
        source_model: Some("client-model"),
        family: SameFormatProviderFamily::Standard,
        body_rules: None,
        request_headers: None,
        upstream_is_stream: false,
        force_body_stream_field: false,
        kiro_auth_config: None,
        is_claude_code: false,
        is_vertex: false,
        enable_model_directives: false,
    });

    assert!(body.is_none());
}

#[test]
fn same_format_embedding_body_rejects_multimodal_for_openai_like_provider() {
    let body = build_same_format_provider_request_body(SameFormatProviderRequestBodyInput {
        body_json: &json!({
            "model": "qwen3-vl-embedding",
            "input": [
                {"text": "white running shoes"},
                {"image": "https://example.com/shoe.png"}
            ]
        }),
        mapped_model: "openai-qwen-fallback",
        client_api_format: "openai:embedding",
        provider_api_format: "openai:embedding",
        source_model: Some("qwen3-vl-embedding"),
        family: SameFormatProviderFamily::Standard,
        body_rules: None,
        request_headers: None,
        upstream_is_stream: false,
        force_body_stream_field: false,
        kiro_auth_config: None,
        is_claude_code: false,
        is_vertex: false,
        enable_model_directives: false,
    });

    assert!(body.is_none());
}

#[test]
fn same_format_standard_body_overrides_client_stream_for_non_stream_upstream() {
    let body = build_same_format_provider_request_body(SameFormatProviderRequestBodyInput {
        body_json: &json!({
            "model": "client-model",
            "messages": [{"role": "user", "content": "hello"}],
            "stream": true
        }),
        mapped_model: "upstream-model",
        client_api_format: "openai:chat",
        provider_api_format: "openai:chat",
        source_model: Some("client-model"),
        family: SameFormatProviderFamily::Standard,
        body_rules: None,
        request_headers: None,
        upstream_is_stream: false,
        force_body_stream_field: false,
        kiro_auth_config: None,
        is_claude_code: false,
        is_vertex: false,
        enable_model_directives: false,
    })
    .expect("body should build");

    assert_eq!(body.get("model"), Some(&json!("upstream-model")));
    assert_eq!(body.get("stream"), Some(&json!(false)));
}

#[test]
fn same_format_standard_body_does_not_add_stream_false_for_plain_sync_body() {
    let body = build_same_format_provider_request_body(SameFormatProviderRequestBodyInput {
        body_json: &json!({
            "model": "client-model",
            "messages": [{"role": "user", "content": "hello"}]
        }),
        mapped_model: "upstream-model",
        client_api_format: "openai:chat",
        provider_api_format: "openai:chat",
        source_model: Some("client-model"),
        family: SameFormatProviderFamily::Standard,
        body_rules: None,
        request_headers: None,
        upstream_is_stream: false,
        force_body_stream_field: false,
        kiro_auth_config: None,
        is_claude_code: false,
        is_vertex: false,
        enable_model_directives: false,
    })
    .expect("body should build");

    assert_eq!(body.get("model"), Some(&json!("upstream-model")));
    assert!(body.get("stream").is_none());
}

#[test]
fn same_format_standard_body_forced_policy_adds_stream_false() {
    let body = build_same_format_provider_request_body(SameFormatProviderRequestBodyInput {
        body_json: &json!({
            "model": "client-model",
            "messages": [{"role": "user", "content": "hello"}]
        }),
        mapped_model: "upstream-model",
        client_api_format: "openai:chat",
        provider_api_format: "openai:chat",
        source_model: Some("client-model"),
        family: SameFormatProviderFamily::Standard,
        body_rules: None,
        request_headers: None,
        upstream_is_stream: false,
        force_body_stream_field: true,
        kiro_auth_config: None,
        is_claude_code: false,
        is_vertex: false,
        enable_model_directives: false,
    })
    .expect("body should build");

    assert_eq!(body.get("model"), Some(&json!("upstream-model")));
    assert_eq!(body.get("stream"), Some(&json!(false)));
}

#[test]
fn same_format_gemini_body_removes_leaked_client_stream_field() {
    let body = build_same_format_provider_request_body(SameFormatProviderRequestBodyInput {
        body_json: &json!({
            "contents": [{"role": "user", "parts": [{"text": "hello"}]}],
            "stream": true
        }),
        mapped_model: "gemini-upstream",
        client_api_format: "gemini:generate_content",
        provider_api_format: "gemini:generate_content",
        source_model: None,
        family: SameFormatProviderFamily::Gemini,
        body_rules: None,
        request_headers: None,
        upstream_is_stream: false,
        force_body_stream_field: false,
        kiro_auth_config: None,
        is_claude_code: false,
        is_vertex: false,
        enable_model_directives: false,
    })
    .expect("body should build");

    assert!(body.get("stream").is_none());
}

#[test]
fn same_format_gemini_interactions_body_keeps_body_routing_target() {
    let body = build_same_format_provider_request_body(SameFormatProviderRequestBodyInput {
        body_json: &json!({
            "model": "client-model",
            "input": [{"role": "user", "content": "hello"}],
            "stream": true
        }),
        mapped_model: "gemini-3.5-flash",
        client_api_format: "gemini:interactions",
        provider_api_format: "gemini:interactions",
        source_model: Some("client-model"),
        family: SameFormatProviderFamily::Gemini,
        body_rules: None,
        request_headers: None,
        upstream_is_stream: true,
        force_body_stream_field: false,
        kiro_auth_config: None,
        is_claude_code: false,
        is_vertex: false,
        enable_model_directives: false,
    })
    .expect("body should build");

    assert_eq!(body.get("model"), Some(&json!("gemini-3.5-flash")));
    assert_eq!(body.get("stream"), Some(&json!(true)));
}

#[test]
fn same_format_gemini_interactions_body_rewrites_agent_target() {
    let body = build_same_format_provider_request_body(SameFormatProviderRequestBodyInput {
        body_json: &json!({
            "agent": "antigravity-preview-05-2026",
            "input": "hello"
        }),
        mapped_model: "antigravity-preview-05-2026",
        client_api_format: "gemini:interactions",
        provider_api_format: "gemini:interactions",
        source_model: None,
        family: SameFormatProviderFamily::Gemini,
        body_rules: None,
        request_headers: None,
        upstream_is_stream: false,
        force_body_stream_field: false,
        kiro_auth_config: None,
        is_claude_code: false,
        is_vertex: false,
        enable_model_directives: false,
    })
    .expect("body should build");

    assert_eq!(
        body.get("agent"),
        Some(&json!("antigravity-preview-05-2026"))
    );
    assert!(body.get("model").is_none());
}

#[test]
fn same_format_vertex_body_strips_function_response_id_for_upstream() {
    let body = build_same_format_provider_request_body(SameFormatProviderRequestBodyInput {
        body_json: &json!({
            "contents": [
                {
                    "role": "user",
                    "parts": [
                        {
                            "functionResponse": {
                                "id": "call_123",
                                "name": "lookup",
                                "response": {
                                    "result": {
                                        "id": "keep_result_id",
                                        "ok": true
                                    }
                                }
                            }
                        },
                        {
                            "function_response": {
                                "id": "call_456",
                                "name": "lookup_snake",
                                "response": {"ok": true}
                            }
                        }
                    ]
                }
            ],
            "stream": true
        }),
        mapped_model: "gemini-upstream",
        client_api_format: "gemini:generate_content",
        provider_api_format: "gemini:generate_content",
        source_model: None,
        family: SameFormatProviderFamily::Gemini,
        body_rules: None,
        request_headers: None,
        upstream_is_stream: true,
        force_body_stream_field: false,
        kiro_auth_config: None,
        is_claude_code: false,
        is_vertex: true,
        enable_model_directives: false,
    })
    .expect("body should build");

    let function_response = &body["contents"][0]["parts"][0]["functionResponse"];
    assert!(function_response.get("id").is_none());
    assert_eq!(function_response["name"], "lookup");
    assert_eq!(
        function_response["response"]["result"]["id"],
        "keep_result_id"
    );

    let function_response = &body["contents"][0]["parts"][1]["function_response"];
    assert!(function_response.get("id").is_none());
    assert_eq!(function_response["name"], "lookup_snake");
}

#[test]
fn same_format_gemini_body_preserves_function_response_id_for_native_upstream() {
    let body = build_same_format_provider_request_body(SameFormatProviderRequestBodyInput {
        body_json: &json!({
            "contents": [
                {
                    "role": "user",
                    "parts": [
                        {
                            "functionResponse": {
                                "id": "call_123",
                                "name": "lookup",
                                "response": {
                                    "result": {
                                        "id": "keep_result_id",
                                        "ok": true
                                    }
                                }
                            }
                        },
                        {
                            "function_response": {
                                "id": "call_456",
                                "name": "lookup_snake",
                                "response": {"ok": true}
                            }
                        }
                    ]
                }
            ],
            "stream": true
        }),
        mapped_model: "gemini-upstream",
        client_api_format: "gemini:generate_content",
        provider_api_format: "gemini:generate_content",
        source_model: None,
        family: SameFormatProviderFamily::Gemini,
        body_rules: None,
        request_headers: None,
        upstream_is_stream: true,
        force_body_stream_field: false,
        kiro_auth_config: None,
        is_claude_code: false,
        is_vertex: false,
        enable_model_directives: false,
    })
    .expect("body should build");

    let function_response = &body["contents"][0]["parts"][0]["functionResponse"];
    assert_eq!(function_response["id"], "call_123");
    assert_eq!(function_response["name"], "lookup");
    assert_eq!(
        function_response["response"]["result"]["id"],
        "keep_result_id"
    );

    let function_response = &body["contents"][0]["parts"][1]["function_response"];
    assert_eq!(function_response["id"], "call_456");
    assert_eq!(function_response["name"], "lookup_snake");
}

#[test]
fn same_format_gemini_body_enables_mixed_tool_invocations() {
    let output = build_same_format_provider_request_body_with_compatibility_report(
        SameFormatProviderRequestBodyInput {
            body_json: &json!({
                "contents": [{
                    "role": "user",
                    "parts": [{"text": "Search, then save the result."}]
                }],
                "tools": [
                    {"googleSearch": {}},
                    {
                        "functionDeclarations": [{
                            "name": "save_result",
                            "parameters": {"type": "object"}
                        }]
                    }
                ],
                "toolConfig": {
                    "functionCallingConfig": {"mode": "AUTO"}
                }
            }),
            mapped_model: "gemini-3.7-flash",
            client_api_format: "gemini:generate_content",
            provider_api_format: "gemini:generate_content",
            source_model: Some("gemini-3.7-flash"),
            family: SameFormatProviderFamily::Gemini,
            body_rules: None,
            request_headers: None,
            upstream_is_stream: true,
            force_body_stream_field: false,
            kiro_auth_config: None,
            is_claude_code: false,
            is_vertex: false,
            enable_model_directives: false,
        },
    )
    .expect("same-format Gemini body should build");

    assert_eq!(output.body["tools"][0]["googleSearch"], json!({}));
    assert_eq!(
        output.body["tools"][1]["functionDeclarations"][0]["name"],
        "save_result"
    );
    assert_eq!(
        output.body["toolConfig"]["includeServerSideToolInvocations"],
        true
    );
    assert!(output.compatibility_edits.iter().any(|edit| {
        edit.field == "toolConfig.includeServerSideToolInvocations"
            && edit.action
                == SameFormatProviderCompatibilityEditAction::ProviderCompatibilityRewrite
    }));
}

#[test]
fn same_format_body_report_records_provider_compatibility_edits() {
    let output = build_same_format_provider_request_body_with_compatibility_report(
        SameFormatProviderRequestBodyInput {
            body_json: &json!({
                "model": "client-model",
                "contents": [
                    {
                        "role": "user",
                        "parts": [
                            {
                                "functionResponse": {
                                    "id": "call_123",
                                    "name": "lookup",
                                    "response": {"ok": true}
                                }
                            },
                            {
                                "function_response": {
                                    "id": "call_456",
                                    "name": "lookup_snake",
                                    "response": {"ok": true}
                                }
                            }
                        ]
                    }
                ],
                "stream": true
            }),
            mapped_model: "gemini-upstream",
            client_api_format: "gemini:generate_content",
            provider_api_format: "gemini:generate_content",
            source_model: Some("client-model"),
            family: SameFormatProviderFamily::Gemini,
            body_rules: None,
            request_headers: None,
            upstream_is_stream: false,
            force_body_stream_field: false,
            kiro_auth_config: None,
            is_claude_code: false,
            is_vertex: true,
            enable_model_directives: false,
        },
    )
    .expect("same-format body should build");

    assert!(output.body.get("model").is_none());
    assert!(output.body.get("stream").is_none());
    assert!(output
        .body
        .pointer("/contents/0/parts/0/functionResponse/id")
        .is_none());
    assert!(output
        .body
        .pointer("/contents/0/parts/1/function_response/id")
        .is_none());
    assert!(output.compatibility_edits.iter().any(|edit| {
        edit.field == "model"
            && edit.action == SameFormatProviderCompatibilityEditAction::RuntimeRewrite
    }));
    assert!(output.compatibility_edits.iter().any(|edit| {
        edit.field == "stream"
            && edit.action == SameFormatProviderCompatibilityEditAction::RuntimeRewrite
    }));
    assert!(output.compatibility_edits.iter().any(|edit| {
        edit.field == "contents[].parts[].functionResponse.id"
            && edit.action
                == SameFormatProviderCompatibilityEditAction::ProviderCompatibilityRewrite
            && edit.detail.contains("2")
    }));
}

#[test]
fn same_format_responses_body_strips_foreign_reasoning_ids_and_reports_the_edit() {
    let request_body = json!({
        "model": "gpt-5.4",
        "input": [
            {"type": "reasoning", "id": "rs_provider_123", "summary": []},
            {
                "type": "reasoning",
                "id": "item_72d3bd8d367d01977ace23f1",
                "summary": []
            },
            {"type": "message", "role": "user", "content": "continue"}
        ]
    });
    let output = build_same_format_provider_request_body_with_compatibility_report(
        SameFormatProviderRequestBodyInput {
            body_json: &request_body,
            mapped_model: "gpt-5.4",
            client_api_format: "openai:responses",
            provider_api_format: "openai:responses",
            source_model: Some("gpt-5.4"),
            family: SameFormatProviderFamily::Standard,
            body_rules: None,
            request_headers: None,
            upstream_is_stream: false,
            force_body_stream_field: false,
            kiro_auth_config: None,
            is_claude_code: false,
            is_vertex: false,
            enable_model_directives: false,
        },
    )
    .expect("same-format Responses body should build");

    let input = output.body["input"].as_array().expect("input array");
    assert_eq!(input.len(), 2);
    assert_eq!(input[0]["id"], "rs_provider_123");
    assert_eq!(input[1]["type"], "message");
    assert!(output.compatibility_edits.iter().any(|edit| {
        edit.field == "input[].id"
            && edit.action
                == SameFormatProviderCompatibilityEditAction::ProviderCompatibilityRewrite
    }));
}

#[test]
fn same_format_responses_body_preserves_deepseek_opaque_reasoning_replay() {
    let reasoning_items = (0..66)
        .map(|index| {
            json!({
                "type": "reasoning",
                "encrypted_content": format!("opaque-deepseek-state-{index}"),
                "content": [{
                    "type": "reasoning_text",
                    "text": format!("provider thinking state {index}")
                }]
            })
        })
        .chain(std::iter::once(json!({
            "type": "message",
            "role": "user",
            "content": "continue"
        })))
        .collect::<Vec<_>>();
    let request_body = json!({
        "model": "deepseek-v4-flash",
        "input": reasoning_items
    });
    let input = SameFormatProviderRequestBodyInput {
        body_json: &request_body,
        mapped_model: "deepseek-v4-flash",
        client_api_format: "openai:responses",
        provider_api_format: "openai:responses",
        source_model: Some("deepseek-v4-flash"),
        family: SameFormatProviderFamily::Standard,
        body_rules: None,
        request_headers: None,
        upstream_is_stream: false,
        force_body_stream_field: false,
        kiro_auth_config: None,
        is_claude_code: false,
        is_vertex: false,
        enable_model_directives: false,
    };

    let deepseek =
        build_same_format_provider_request_body_with_compatibility_report_and_reasoning_replay_policy(
            input,
            aether_ai_formats::OpenAiResponsesReasoningReplayPolicy::DeepSeekOpaque,
        )
        .expect("DeepSeek same-format Responses body should build");
    let strict = build_same_format_provider_request_body_with_compatibility_report(input)
        .expect("strict same-format Responses body should build");

    assert_eq!(
        deepseek.body["input"]
            .as_array()
            .expect("DeepSeek input array")
            .iter()
            .filter(|item| item["type"] == "reasoning")
            .count(),
        66
    );
    assert_eq!(
        strict.body["input"]
            .as_array()
            .expect("strict input array")
            .iter()
            .filter(|item| item["type"] == "reasoning")
            .count(),
        0
    );
}

#[test]
fn same_format_compact_keeps_strict_reasoning_replay_under_deepseek_policy() {
    let request_body = json!({
        "model": "deepseek-v4-flash",
        "input": [{
            "type": "reasoning",
            "encrypted_content": "opaque-deepseek-state",
            "content": [{"type": "reasoning_text", "text": "thinking"}]
        }]
    });
    let output = build_same_format_provider_request_body_with_compatibility_report_and_reasoning_replay_policy(
        SameFormatProviderRequestBodyInput {
            body_json: &request_body,
            mapped_model: "deepseek-v4-flash",
            client_api_format: "openai:responses:compact",
            provider_api_format: "openai:responses:compact",
            source_model: Some("deepseek-v4-flash"),
            family: SameFormatProviderFamily::Standard,
            body_rules: None,
            request_headers: None,
            upstream_is_stream: false,
            force_body_stream_field: false,
            kiro_auth_config: None,
            is_claude_code: false,
            is_vertex: false,
            enable_model_directives: false,
        },
        aether_ai_formats::OpenAiResponsesReasoningReplayPolicy::DeepSeekOpaque,
    )
    .expect("Compact body should keep the strict OpenAI replay contract");

    assert_eq!(output.body["input"].as_array().map(Vec::len), Some(0));
    assert!(output.compatibility_edits.iter().any(|edit| {
        edit.field == "input[].id"
            && edit.action
                == SameFormatProviderCompatibilityEditAction::ProviderCompatibilityRewrite
    }));
}

#[test]
fn same_format_stream_policy_wins_after_body_rules() {
    let body_rules = json!([
        {"action":"set","path":"stream","value":true}
    ]);

    let body = build_same_format_provider_request_body(SameFormatProviderRequestBodyInput {
        body_json: &json!({
            "model": "client-model",
            "messages": [{"role": "user", "content": "hello"}],
            "stream": true
        }),
        mapped_model: "upstream-model",
        client_api_format: "openai:chat",
        provider_api_format: "openai:chat",
        source_model: Some("client-model"),
        family: SameFormatProviderFamily::Standard,
        body_rules: Some(&body_rules),
        request_headers: None,
        upstream_is_stream: false,
        force_body_stream_field: false,
        kiro_auth_config: None,
        is_claude_code: false,
        is_vertex: false,
        enable_model_directives: false,
    })
    .expect("body should build");

    assert_eq!(body.get("stream"), Some(&json!(false)));
}

#[test]
fn same_format_compact_stream_policy_wins_after_body_rules() {
    let body_rules = json!([
        {"action":"set","path":"stream","value":true}
    ]);

    let body = build_same_format_provider_request_body(SameFormatProviderRequestBodyInput {
        body_json: &json!({
            "model": "client-model",
            "input": "hello",
            "stream": true
        }),
        mapped_model: "upstream-model",
        client_api_format: "openai:responses:compact",
        provider_api_format: "openai:responses:compact",
        source_model: Some("client-model"),
        family: SameFormatProviderFamily::Standard,
        body_rules: Some(&body_rules),
        request_headers: None,
        upstream_is_stream: false,
        force_body_stream_field: false,
        kiro_auth_config: None,
        is_claude_code: false,
        is_vertex: false,
        enable_model_directives: false,
    })
    .expect("body should build");

    assert!(body.get("stream").is_none());
}

#[test]
fn same_format_body_applies_model_directive_before_body_rules() {
    let body = build_same_format_provider_request_body(SameFormatProviderRequestBodyInput {
        body_json: &json!({
            "model": "gpt-5.4-high",
            "messages": [{"role": "user", "content": "hello"}],
            "reasoning_effort": "low"
        }),
        mapped_model: "upstream-model",
        client_api_format: "openai:chat",
        provider_api_format: "openai:chat",
        source_model: Some("gpt-5.4-high"),
        family: SameFormatProviderFamily::Standard,
        body_rules: Some(&json!([
            {"action":"set","path":"metadata.body_rule_seen","value":true}
        ])),
        request_headers: None,
        upstream_is_stream: false,
        force_body_stream_field: false,
        kiro_auth_config: None,
        is_claude_code: false,
        is_vertex: false,
        enable_model_directives: true,
    })
    .expect("body should build");

    assert_eq!(body["model"], "upstream-model");
    assert_eq!(body["reasoning_effort"], "high");
    assert_eq!(body["metadata"]["body_rule_seen"], true);
}

#[test]
fn search_body_projects_the_protocol_contract_and_keeps_fast_routing_only() {
    let body = build_same_format_provider_request_body(SameFormatProviderRequestBodyInput {
        body_json: &json!({
            "id": "session-1",
            "model": "gpt-5.6-luna-high-fast",
            "commands": {"search_query": [{"q": "Aether"}]},
            "settings": {"allowed_callers": ["direct"]},
            "store": false,
            "future_extension": {"enabled": true},
            "stream": true
        }),
        mapped_model: "gpt-5.6-luna",
        client_api_format: "openai:search",
        provider_api_format: "openai:search",
        source_model: Some("gpt-5.6-luna-high-fast"),
        family: SameFormatProviderFamily::Standard,
        body_rules: None,
        request_headers: None,
        upstream_is_stream: false,
        force_body_stream_field: false,
        kiro_auth_config: None,
        is_claude_code: false,
        is_vertex: false,
        enable_model_directives: true,
    })
    .expect("search body should build");

    assert_eq!(body["id"], "session-1");
    assert_eq!(body["model"], "gpt-5.6-luna");
    assert_eq!(body["commands"]["search_query"][0]["q"], "Aether");
    assert_eq!(body["settings"]["allowed_callers"][0], "direct");
    assert_eq!(body["reasoning"]["effort"], "high");
    assert!(body.get("store").is_none());
    assert!(body.get("future_extension").is_none());
    assert!(body.get("service_tier").is_none());
    assert!(body.get("stream").is_none());
}

#[test]
fn builds_same_format_headers_with_auth_and_stream_accept() {
    let provider_request_body = json!({"model": "upstream-model"});
    let original_request_body = json!({"model": "client-model"});
    let mut request_headers = http::HeaderMap::new();
    request_headers.insert(
        http::header::ACCEPT_ENCODING,
        http::HeaderValue::from_static("gzip, br"),
    );
    let headers = build_same_format_provider_headers(SameFormatProviderHeadersInput {
        headers: &request_headers,
        provider_request_body: &provider_request_body,
        original_request_body: &original_request_body,
        header_rules: None,
        behavior: SameFormatProviderRequestBehavior {
            is_antigravity: false,
            is_gemini_cli: false,
            is_claude_code: false,
            is_claude_code_transport: false,
            anthropic_compatibility_profile: AnthropicCompatibilityProfile::NativeTransparent,
            is_vertex: false,
            is_kiro: false,
            upstream_is_stream: true,
            force_body_stream_field: false,
            report_kind: "openai_chat_stream_success",
        },
        api_operation: None,
        auth_header: Some("x-api-key"),
        auth_value: Some("secret"),
        extra_headers: &BTreeMap::new(),
        kiro_auth_config: None,
        kiro_machine_id: None,
    })
    .expect("headers should build");

    assert_eq!(headers.get("x-api-key").map(String::as_str), Some("secret"));
    assert_eq!(
        headers.get("content-type").map(String::as_str),
        Some("application/json")
    );
    assert_eq!(
        headers.get("accept").map(String::as_str),
        Some("text/event-stream")
    );
    assert_eq!(
        headers.get("accept-encoding").map(String::as_str),
        Some("identity")
    );
}

#[test]
fn same_format_headers_cannot_restore_credentials_or_internal_headers() {
    let provider_request_body = json!({"model": "upstream-model"});
    let original_request_body = json!({"model": "client-model"});
    let mut request_headers = http::HeaderMap::new();
    for (name, value) in [
        ("authorization", "Bearer client"),
        ("api-key", "client-api-key"),
        ("x-api-key", "client-x-api-key"),
        ("cookie", "session=client"),
        ("proxy-authorization", "Basic client"),
        ("x-aether-auth-user-id", "user-private"),
        ("x-aether-auth-api-key-id", "key-private"),
        ("x-aether-auth-balance-remaining", "12.34"),
        ("x-aether-gateway", "gateway-internal"),
    ] {
        request_headers.insert(
            http::HeaderName::from_bytes(name.as_bytes()).expect("valid header name"),
            http::HeaderValue::from_str(value).expect("valid header value"),
        );
    }
    let behavior = SameFormatProviderRequestBehavior {
        is_antigravity: false,
        is_gemini_cli: false,
        is_claude_code: false,
        is_claude_code_transport: false,
        anthropic_compatibility_profile: AnthropicCompatibilityProfile::NativeTransparent,
        is_vertex: false,
        is_kiro: false,
        upstream_is_stream: false,
        force_body_stream_field: false,
        report_kind: "claude_chat_sync_success",
    };
    let header_rules = json!([
        {"action": "set", "key": "cookie", "value": "session=rule"},
        {"action": "set", "key": "authorization", "value": "Bearer rule"},
        {"action": "set", "key": "x-aether-auth-user-id", "value": "user-rule"}
    ]);
    let extra_headers = BTreeMap::from([
        ("api-key".to_string(), "extra-api-key".to_string()),
        ("authorization".to_string(), "Bearer extra".to_string()),
        (
            "x-aether-auth-balance-remaining".to_string(),
            "99.99".to_string(),
        ),
        ("anthropic-beta".to_string(), "custom-beta".to_string()),
    ]);

    let headers = build_same_format_provider_headers(SameFormatProviderHeadersInput {
        headers: &request_headers,
        provider_request_body: &provider_request_body,
        original_request_body: &original_request_body,
        header_rules: Some(&header_rules),
        behavior,
        api_operation: None,
        auth_header: Some("x-api-key"),
        auth_value: Some("upstream-secret"),
        extra_headers: &extra_headers,
        kiro_auth_config: None,
        kiro_machine_id: None,
    })
    .expect("headers should build");

    assert_eq!(
        headers.get("x-api-key").map(String::as_str),
        Some("upstream-secret")
    );
    for stripped in ["authorization", "api-key", "cookie", "proxy-authorization"] {
        assert!(!headers.contains_key(stripped), "should strip {stripped}");
    }
    assert!(
        headers.keys().all(|name| !name.starts_with("x-aether-")),
        "Aether-owned headers must never leave provider egress: {headers:?}"
    );
    assert_eq!(
        headers.get("anthropic-beta").map(String::as_str),
        Some("custom-beta")
    );
}
