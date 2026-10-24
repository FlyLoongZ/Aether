use super::{
    build_redaction_session_config, detect_candidates_with_probe, mask_chat_request_json,
    mask_chat_request_json_with_options, parse_chat_pii_redaction_rules, restore_json_strings,
    restore_sync_response_body, try_mask_chat_pii_request_json_with_options,
    try_mask_chat_pii_request_value_with_cache_options,
    try_mask_chat_request_json_with_cache_options, try_mask_chat_request_json_with_options,
    ChatPiiRedactionRequestFormat, ChatPiiRedactionRuntimeConfig, DetectorProbe, MappingKey,
    MaskChatRequestOptions, RedactionKind, RedactionMapping, RedactionMaskError, RedactionSession,
    RedactionSessionConfig, RedactionSessionSlot, RedisRedactionMappingCache, SentinelMatcher,
    StreamingResponseRestorer,
};
use std::collections::BTreeMap;
use std::time::Duration;

use aether_runtime_state::{RedisClientConfig, RuntimeState};
use aether_test_support::ManagedRedisServer;
use serde_json::{json, Value};

fn assert_debug_surface_hides_values(debug: &str, originals: &[&str], sentinels: &[String]) {
    for original in originals {
        assert!(
            !debug.contains(original),
            "debug leaked original {original}"
        );
    }
    for sentinel in sentinels {
        assert!(
            !debug.contains(sentinel),
            "debug leaked sentinel {sentinel}"
        );
    }
}

fn source_chunk_before_struct<'a>(source: &'a str, marker: &str) -> &'a str {
    let marker_start = source.find(marker).expect("struct marker should exist");
    let chunk_start = marker_start.saturating_sub(220);
    &source[chunk_start..marker_start]
}

fn assert_not_serde_derived(source: &str, marker: &str) {
    let chunk = source_chunk_before_struct(source, marker);
    assert!(
        !chunk.contains("Serialize"),
        "{marker} must not derive Serialize"
    );
    assert!(
        !chunk.contains("Deserialize"),
        "{marker} must not derive Deserialize"
    );
}

fn session_at(now_unix_secs: u64) -> RedactionSession {
    RedactionSession::new(RedactionSessionConfig::new(
        b"redaction-test-key".to_vec(),
        300,
        now_unix_secs,
    ))
}

fn assert_base32_sentinel(sentinel: &str) {
    let body = sentinel
        .strip_prefix("<AETHER:")
        .and_then(|rest| rest.strip_suffix('>'))
        .expect("sentinel should use Aether wrapper");
    let (_, payload) = body
        .split_once(':')
        .expect("sentinel should include type and payload");
    assert_eq!(payload.len(), 20, "HMAC96 base32 payload is 20 chars");
    assert!(payload
        .chars()
        .all(|ch| matches!(ch, 'A'..='Z' | '2'..='7')));
    assert_ne!(payload.len(), 24, "old hex HMAC96 payload must not be used");
}

fn assert_namespaced_base32_sentinel(sentinel: &str, namespace: &str, rule_label: &str) {
    let body = sentinel
        .strip_prefix('<')
        .and_then(|rest| rest.strip_suffix('>'))
        .expect("sentinel should use wrapper");
    let parts = body.split(':').collect::<Vec<_>>();
    assert_eq!(parts.len(), 3);
    assert_eq!(parts[0], namespace);
    assert_eq!(parts[1], rule_label);
    assert_eq!(parts[2].len(), 20, "HMAC96 base32 payload is 20 chars");
    assert!(parts[2]
        .chars()
        .all(|ch| matches!(ch, 'A'..='Z' | '2'..='7')));
}

fn session_with_response_mapping(original: &str, sentinel: &str) -> RedactionSession {
    let mut session = session_at(600);
    let key = MappingKey {
        rule_label: "EMAIL".to_string(),
        original: original.to_string(),
    };
    session
        .sentinel_index
        .insert(sentinel.to_string(), key.clone());
    session.mappings.insert(
        key,
        RedactionMapping {
            rule_label: "EMAIL".to_string(),
            kind: Some(RedactionKind::Email),
            original: original.to_string(),
            normalized_value: original.to_string(),
            sentinel: sentinel.to_string(),
            bucket: 2,
            created_at_unix_secs: 600,
            expires_at_unix_secs: 900,
        },
    );
    session
}

async fn start_managed_redis_or_skip() -> Option<ManagedRedisServer> {
    match ManagedRedisServer::start().await {
        Ok(server) => Some(server),
        Err(err) if err.to_string().contains("No such file or directory") => None,
        Err(err) => panic!("redis server should start: {err}"),
    }
}

async fn redis_cache_runtime_state(redis_url: &str, key_prefix: &str) -> RuntimeState {
    RuntimeState::redis(
        RedisClientConfig {
            url: redis_url.to_string(),
            key_prefix: Some(key_prefix.to_string()),
        },
        Some(1_000),
    )
    .await
    .expect("redis runtime state should build")
}

fn email_only_runtime_config() -> ChatPiiRedactionRuntimeConfig {
    ChatPiiRedactionRuntimeConfig {
        enabled: true,
        rules: parse_chat_pii_redaction_rules(Some(&json!([{
            "id": "email",
            "name": "邮箱",
            "pattern": "(?i)[A-Z0-9._%+-]{1,64}@[A-Z0-9.-]{1,253}\\.[A-Z]{2,63}",
            "enabled": true,
            "kind": "email",
            "system": true
        }])))
        .expect("email rule should compile"),
        ..ChatPiiRedactionRuntimeConfig::default()
    }
}

#[test]
fn pii_redaction_session_detects_reuses_rolls_over_and_avoids_collisions() {
    let input = concat!(
        "email alice@example.com again alice@example.com ",
        "mobile 13800138000 landline 010-12345678 phone +14155552671 ",
        "id 11010519491231002X card 4111 1111 1111 1111 ",
        "ips 192.168.0.1 2001:db8::1 ",
        "keys sk-proj-AbCdEfGhIjKlMnOpQrStUvWxYz123456 sk-ant-api03-AbCdEfGhIjKlMnOpQrStUvWxYz1234567890 ",
        "ghp_abcdefghijklmnopqrstuvwxyzABCDEFGHIJ ",
        "xo", "xb-123456789012-123456789012-AbCdEfGhIjKlMnOpQrStUvWx ",
        "AKIAIOSFODNN7EXAMPLE Bearer abcdefghijklmnopqrstuvwxyzABCDEF1234567890 ",
        "access_token=accessValueABCDEF1234567890abcdef secret_key=secretValueABCDEF1234567890abcdef ",
        "eyJhbGciOiJub25lIiwidHlwIjoiSldUIn0.eyJpc3MiOiJ0ZXN0IiwiZXhwIjoyMDAwMDAwMDAwfQ.signature123456789 ",
        "q8HjK2LmN9PqR4StU7VwX0YzA3BcD6Ef"
    );
    let originals = [
        "alice@example.com",
        "13800138000",
        "010-12345678",
        "+14155552671",
        "11010519491231002X",
        "4111 1111 1111 1111",
        "192.168.0.1",
        "2001:db8::1",
        "sk-proj-AbCdEfGhIjKlMnOpQrStUvWxYz123456",
        "sk-ant-api03-AbCdEfGhIjKlMnOpQrStUvWxYz1234567890",
        "ghp_abcdefghijklmnopqrstuvwxyzABCDEFGHIJ",
        concat!("xo", "xb-123456789012-123456789012-AbCdEfGhIjKlMnOpQrStUvWx"),
        "AKIAIOSFODNN7EXAMPLE",
        "Bearer abcdefghijklmnopqrstuvwxyzABCDEF1234567890",
        "access_token=accessValueABCDEF1234567890abcdef",
        "secret_key=secretValueABCDEF1234567890abcdef",
        "eyJhbGciOiJub25lIiwidHlwIjoiSldUIn0.eyJpc3MiOiJ0ZXN0IiwiZXhwIjoyMDAwMDAwMDAwfQ.signature123456789",
        "q8HjK2LmN9PqR4StU7VwX0YzA3BcD6Ef",
    ];

    let mut session = session_at(600);
    let redacted = session.redact_text(input);

    for original in originals {
        assert!(!redacted.text.contains(original), "leaked {original}");
    }
    assert!(redacted.text.contains("<AETHER:EMAIL:"));
    assert!(redacted.text.contains("<AETHER:CN_PHONE:"));
    assert!(redacted.text.contains("<AETHER:ACCESS_TOKEN:"));
    assert!(redacted.text.contains("<AETHER:SECRET_KEY:"));
    assert!(
        redacted
            .matches
            .iter()
            .all(|matched| matched.sentinel.starts_with("<AETHER:")
                && matched.sentinel.ends_with('>'))
    );
    for matched in &redacted.matches {
        assert_base32_sentinel(&matched.sentinel);
    }
    assert!(redacted.matches.iter().any(|matched| {
        matched.kind == Some(RedactionKind::ChinaMobile) && matched.sentinel.contains(":CN_PHONE:")
    }));
    assert!(redacted.matches.iter().any(|matched| {
        matched.kind == Some(RedactionKind::ChinaLandline)
            && matched.sentinel.contains(":CN_PHONE:")
    }));
    assert!(redacted
        .matches
        .iter()
        .any(|matched| matched.kind == Some(RedactionKind::AccessToken)));
    assert!(redacted
        .matches
        .iter()
        .any(|matched| matched.kind == Some(RedactionKind::SecretKey)));
    assert_eq!(session.mapping_count(), originals.len());

    let first_email = session
        .sentinel_for_original("alice@example.com")
        .expect("email sentinel should exist")
        .to_string();
    assert_base32_sentinel(&first_email);
    assert_eq!(redacted.text.matches(&first_email).count(), 2);

    let debug = format!("{session:?}");
    assert!(debug.contains("mapping_count"));
    assert!(!debug.contains("alice@example.com"));
    assert!(!debug.contains(&first_email));
    assert!(!debug.contains("redaction-test-key"));

    let mut same_bucket = session_at(899);
    same_bucket.redact_text("alice@example.com");
    assert_eq!(
        same_bucket.sentinel_for_original("alice@example.com"),
        Some(first_email.as_str())
    );

    let mut next_bucket = session_at(900);
    next_bucket.redact_text("alice@example.com");
    assert_ne!(
        next_bucket.sentinel_for_original("alice@example.com"),
        Some(first_email.as_str())
    );

    let colliding_literal = first_email;
    let mut collision_session = session_at(600);
    collision_session.redact_text(&format!(
        "literal {colliding_literal} value alice@example.com"
    ));
    assert_ne!(
        collision_session.sentinel_for_original("alice@example.com"),
        Some(colliding_literal.as_str())
    );
}

#[test]
fn pii_redaction_hides_agent_identity_private_keys() {
    let private_key =
        "agent_private_key=MC4CAQAwBQYDK2VwBCIEIAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
    let mut session = session_at(601);

    let redacted = session.redact_text(private_key);

    assert!(!redacted.text.contains(private_key));
    assert!(redacted.text.contains("<AETHER:SECRET_KEY:"));
    assert!(redacted
        .matches
        .iter()
        .any(|matched| matched.kind == Some(RedactionKind::SecretKey)));
}

#[test]
fn pii_redaction_session_uses_configured_sentinel_namespace() {
    let mut session = RedactionSession::new(
        RedactionSessionConfig::new(b"redaction-test-key".to_vec(), 300, 600)
            .with_sentinel_namespace("vendor_safe"),
    );

    let redacted = session.redact_text("contact alice@example.com");
    let sentinel = session
        .sentinel_for_original("alice@example.com")
        .expect("email sentinel should exist");

    assert_namespaced_base32_sentinel(sentinel, "VENDOR_SAFE", "EMAIL");
    assert!(redacted.text.contains(sentinel));
    assert!(!redacted.text.contains("<AETHER:"));
    assert_eq!(
        session
            .restore_text(&redacted.text)
            .expect("restoration must succeed")
            .text,
        "contact alice@example.com"
    );
}

#[test]
fn pii_redaction_no_mapping_leak() {
    let originals = [
        "alice@example.com",
        "sk-proj-AbCdEfGhIjKlMnOpQrStUvWxYz123456",
        "access_token=accessValueABCDEF1234567890abcdef",
    ];
    let request = json!({
        "model": "gpt-5",
        "messages": [{
            "role": "user",
            "content": format!(
                "Email {} key {} token {}",
                originals[0], originals[1], originals[2]
            ),
        }]
    });
    let masked = mask_chat_request_json(
        &serde_json::to_vec(&request).expect("request should serialize"),
        test_config(),
    );
    assert!(masked.redacted);
    let sentinels = originals
        .iter()
        .map(|original| {
            masked
                .session
                .sentinel_for_original(original)
                .expect("sentinel should exist")
                .to_string()
        })
        .collect::<Vec<_>>();

    for debug in [
        format!("{:?}", masked.session),
        format!("{:?}", masked),
        format!(
            "{:?}",
            RedactionSessionConfig::new(originals[1].as_bytes().to_vec(), 300, 600)
        ),
    ] {
        assert_debug_surface_hides_values(&debug, &originals, &sentinels);
    }

    let mut session = session_at(600);
    let redacted = session.redact_text(&format!(
        "Email {} key {} token {}",
        originals[0], originals[1], originals[2]
    ));
    let redacted_debug = format!("{redacted:?}");
    assert_debug_surface_hides_values(&redacted_debug, &originals, &sentinels);
    for matched in &redacted.matches {
        let matched_debug = format!("{matched:?}");
        assert_debug_surface_hides_values(&matched_debug, &originals, &sentinels);
        assert!(matched_debug.contains("<AETHER:"));
        assert!(matched_debug.contains(":redacted>"));
    }
    for mapping in session.mappings() {
        let mapping_debug = format!("{mapping:?}");
        assert_debug_surface_hides_values(&mapping_debug, &originals, &sentinels);
        assert!(mapping_debug.contains("original_len"));
        assert!(mapping_debug.contains("normalized_value_len"));
        assert!(mapping_debug.contains(":redacted>"));
    }

    let privacy_source = include_str!("mod.rs");
    for marker in [
        "pub(crate) struct RedactionSession {",
        "pub(crate) struct RedactionSessionSlot {",
        "pub(crate) struct MaskedChatRequest {",
        "pub(crate) struct RedactionMatch {",
        "pub(crate) struct RedactionMapping {",
    ] {
        assert_not_serde_derived(privacy_source, marker);
    }
    let session_serialize_impl = format!("impl {} for {}", "Serialize", "RedactionSession");
    let mapping_serialize_impl = format!("impl {} for {}", "Serialize", "RedactionMapping");
    assert!(!privacy_source.contains(&session_serialize_impl));
    assert!(!privacy_source.contains(&mapping_serialize_impl));

    let report_context_source = include_str!("../ai_serving/planner/report_context.rs");
    for forbidden in [
        "RedactionSession",
        "RedactionMapping",
        "RedactionSessionSlot",
        "redaction_session",
        "redaction_mapping",
        "sentinel_index",
    ] {
        assert!(
            !report_context_source.contains(forbidden),
            "report context source must not expose {forbidden}"
        );
    }
    assert!(report_context_source.contains("original_request_body_json"));
}

#[test]
fn pii_redaction_false_positives_are_not_detected() {
    let input = concat!(
        "Order AETHER-20240502-123456 date 2026-05-02 decimal 12345.67890 ",
        "short ids 123456 9876543210 invalid card 4111111111111112 ",
        "short sk word sk-test low entropy aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa ",
        "name Alice Zhang address 北京市朝阳区建国路88号"
    );
    let mut session = session_at(600);
    let redacted = session.redact_text(input);

    assert_eq!(redacted.text, input);
    assert!(redacted.matches.is_empty());
    assert_eq!(session.mapping_count(), 0);
}

#[test]
fn pii_redaction_request_masks_chat_message_text_and_tool_arguments() {
    let request = json!({
        "model": "gpt-5",
        "metadata": {
            "owner": "metadata@example.com"
        },
        "temperature": 0.2,
        "tools": [{
            "type": "function",
            "function": {
                "name": "lookup_customer",
                "description": "schema owner schema@example.com stays visible",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "email": {"type": "string", "description": "example schema@example.com"}
                    }
                }
            }
        }],
        "messages": [
            {
                "role": "system",
                "content": "Contact alice@example.com with quote \"ok\", path C:\\tmp, and newline\nend."
            },
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": "Phone +14155552671 should be masked."},
                    {"type": "image_url", "text": "image text bob@example.com must stay."},
                    {"type": "input_text", "text": "input text carol@example.com must stay."},
                    {"type": "text", "text": 42}
                ]
            },
            {
                "role": "assistant",
                "content": null,
                "tool_calls": [{
                    "type": "function",
                    "function": {
                        "name": "lookup_customer",
                        "arguments": "{\"phone\":\"13800138000\",\"secret\":\"secret_key=secretValueABCDEF1234567890abcdef\"}"
                    }
                }]
            },
            {
                "role": "tool",
                "tool_call_id": "call_1",
                "content": "Tool result access_token=accessValueABCDEF1234567890abcdef"
            }
        ]
    });
    let raw = serde_json::to_vec(&request).expect("request should serialize");

    let masked = mask_chat_request_json(&raw, test_config());

    assert!(masked.redacted);
    assert_eq!(masked.session.mapping_count(), 5);
    let masked_json: serde_json::Value =
        serde_json::from_slice(&masked.body).expect("masked request should stay valid JSON");

    assert_eq!(masked_json["model"], "gpt-5");
    assert_eq!(masked_json["metadata"]["owner"], "metadata@example.com");
    assert_eq!(masked_json["temperature"], 0.2);
    assert_eq!(
        masked_json["tools"][0]["function"]["description"],
        "schema owner schema@example.com stays visible"
    );
    assert_eq!(
        masked_json["tools"][0]["function"]["parameters"]["properties"]["email"]["description"],
        "example schema@example.com"
    );

    let system_content = masked_json["messages"][0]["content"]
        .as_str()
        .expect("system content should remain a string");
    assert!(!system_content.contains("alice@example.com"));
    assert!(system_content.contains("\"ok\""));
    assert!(system_content.contains("C:\\tmp"));
    assert!(system_content.contains("newline\nend"));
    assert!(system_content.contains(
        masked
            .session
            .sentinel_for_original("alice@example.com")
            .expect("email sentinel should exist")
    ));

    let text_part = masked_json["messages"][1]["content"][0]["text"]
        .as_str()
        .expect("text part should remain a string");
    assert!(!text_part.contains("+14155552671"));
    assert!(text_part.contains(
        masked
            .session
            .sentinel_for_original("+14155552671")
            .expect("phone sentinel should exist")
    ));
    assert_eq!(
        masked_json["messages"][1]["content"][1]["text"],
        "image text bob@example.com must stay."
    );
    assert_eq!(
        masked_json["messages"][1]["content"][2]["text"],
        "input text carol@example.com must stay."
    );

    let arguments = masked_json["messages"][2]["tool_calls"][0]["function"]["arguments"]
        .as_str()
        .expect("tool call arguments should remain a string");
    assert!(!arguments.contains("13800138000"));
    assert!(!arguments.contains("secretValueABCDEF1234567890abcdef"));
    assert!(arguments.contains(
        masked
            .session
            .sentinel_for_original("13800138000")
            .expect("mobile sentinel should exist")
    ));
    assert!(arguments.contains(
        masked
            .session
            .sentinel_for_original("secret_key=secretValueABCDEF1234567890abcdef")
            .expect("secret sentinel should exist")
    ));

    let tool_content = masked_json["messages"][3]["content"]
        .as_str()
        .expect("tool content should remain a string");
    assert!(!tool_content.contains("accessValueABCDEF1234567890abcdef"));
    assert!(tool_content.contains(
        masked
            .session
            .sentinel_for_original("access_token=accessValueABCDEF1234567890abcdef")
            .expect("access token sentinel should exist")
    ));
}

#[test]
fn pii_redaction_request_masks_claude_messages_system_and_content() {
    let request = json!({
        "model": "claude-sonnet-4",
        "system": [
            {"type": "text", "text": "System owner alice@example.com"},
            {"type": "image", "source": {"type": "base64", "data": "ignored"}}
        ],
        "metadata": {"owner": "metadata@example.com"},
        "messages": [{
            "role": "user",
            "content": [
                {"type": "text", "text": "Call 13800138000 before noon."},
                {"type": "tool_result", "content": "token access_token=accessValueABCDEF1234567890abcdef"},
                {"type": "image", "text": "image bob@example.com must stay"}
            ]
        }]
    });
    let raw = serde_json::to_vec(&request).expect("request should serialize");

    let masked = try_mask_chat_pii_request_json_with_options(
        &raw,
        ChatPiiRedactionRequestFormat::ClaudeMessages,
        test_config(),
        MaskChatRequestOptions::runtime(),
    )
    .expect("claude messages request should mask");

    assert!(masked.redacted);
    assert_eq!(masked.session.mapping_count(), 3);
    let masked_json: serde_json::Value =
        serde_json::from_slice(&masked.body).expect("masked request should stay valid JSON");
    assert_eq!(masked_json["metadata"]["owner"], "metadata@example.com");
    assert!(!masked_json["system"][0]["text"]
        .as_str()
        .expect("system text should remain a string")
        .contains("alice@example.com"));
    assert!(!masked_json["messages"][0]["content"][0]["text"]
        .as_str()
        .expect("message text should remain a string")
        .contains("13800138000"));
    assert!(!masked_json["messages"][0]["content"][1]["content"]
        .as_str()
        .expect("tool result should remain a string")
        .contains("accessValueABCDEF1234567890abcdef"));
    assert_eq!(
        masked_json["messages"][0]["content"][2]["text"],
        "image bob@example.com must stay"
    );
}

#[test]
fn pii_redaction_request_masks_email_before_sentence_period() {
    let request = json!({
        "model": "gpt-5",
        "messages": [{
            "role": "user",
            "content": "Contact alice@example.com."
        }]
    });
    let raw = serde_json::to_vec(&request).expect("request should serialize");

    let masked = try_mask_chat_pii_request_json_with_options(
        &raw,
        ChatPiiRedactionRequestFormat::OpenAiChat,
        test_config(),
        MaskChatRequestOptions::runtime(),
    )
    .expect("chat request should mask");

    assert!(masked.redacted);
    let masked_json: serde_json::Value =
        serde_json::from_slice(&masked.body).expect("masked request should stay valid JSON");
    let content = masked_json["messages"][0]["content"]
        .as_str()
        .expect("content should stay text");
    assert!(content.contains("<AETHER:EMAIL:"));
    assert!(content.ends_with('.'));
    assert!(!content.contains("alice@example.com"));
}

#[test]
fn pii_redaction_request_masks_openai_responses_input_and_function_arguments() {
    let request = json!({
        "model": "gpt-5",
        "instructions": "Route replies for alice@example.com contact.",
        "metadata": {"owner": "metadata@example.com"},
        "input": [
            {
                "type": "message",
                "role": "user",
                "content": [
                    {"type": "input_text", "text": "Phone +14155552671"},
                    {"type": "input_image", "text": "image bob@example.com must stay"}
                ]
            },
            {
                "type": "function_call",
                "name": "lookup_customer",
                "arguments": "{\"secret\":\"secret_key=secretValueABCDEF1234567890abcdef\"}"
            }
        ]
    });
    let raw = serde_json::to_vec(&request).expect("request should serialize");

    let masked = try_mask_chat_pii_request_json_with_options(
        &raw,
        ChatPiiRedactionRequestFormat::OpenAiResponses,
        test_config(),
        MaskChatRequestOptions::runtime(),
    )
    .expect("responses request should mask");

    assert!(masked.redacted);
    assert!(masked.session.mapping_count() >= 2);
    let masked_json: serde_json::Value =
        serde_json::from_slice(&masked.body).expect("masked request should stay valid JSON");
    assert_eq!(masked_json["metadata"]["owner"], "metadata@example.com");
    let instructions = masked_json["instructions"]
        .as_str()
        .expect("instructions should remain a string");
    assert!(!instructions.contains("alice@example.com"));
    assert!(!masked_json["input"][0]["content"][0]["text"]
        .as_str()
        .expect("input text should remain a string")
        .contains("+14155552671"));
    assert_eq!(
        masked_json["input"][0]["content"][1]["text"],
        "image bob@example.com must stay"
    );
    assert!(!masked_json["input"][1]["arguments"]
        .as_str()
        .expect("arguments should remain a string")
        .contains("secretValueABCDEF1234567890abcdef"));
}

#[test]
fn pii_redaction_preserves_opaque_reasoning_state_while_masking_user_input() {
    let request = json!({
        "model": "deepseek-reasoner",
        "input": [
            {
                "type": "reasoning",
                "encrypted_content": "opaque-state-must-remain-byte-identical",
                "future_state": {"version": 2},
                "content": [
                    {
                        "type": "reasoning_text",
                        "text": "Provider reasoning state must remain byte-identical"
                    },
                    {
                        "type": "summary_text",
                        "text": "Bound future text must also remain byte-identical"
                    }
                ]
            },
            {
                "type": "message",
                "role": "user",
                "content": [{
                    "type": "input_text",
                    "text": "Please contact bob@example.net"
                }]
            }
        ]
    });
    let raw = serde_json::to_vec(&request).expect("request should serialize");

    let masked = try_mask_chat_pii_request_json_with_options(
        &raw,
        ChatPiiRedactionRequestFormat::OpenAiResponses,
        test_config(),
        deepseek_opaque_replay_options(),
    )
    .expect("reasoning request should mask");

    assert!(masked.redacted);
    let masked_json: serde_json::Value =
        serde_json::from_slice(&masked.body).expect("masked request should stay valid JSON");
    assert_eq!(
        masked_json["input"][0]["encrypted_content"],
        "opaque-state-must-remain-byte-identical"
    );
    assert_eq!(
        masked_json["input"][0]["future_state"],
        json!({"version": 2})
    );
    assert_eq!(
        masked_json["input"][0]["content"][0]["text"],
        "Provider reasoning state must remain byte-identical"
    );
    assert_eq!(
        masked_json["input"][0]["content"][1]["text"],
        "Bound future text must also remain byte-identical",
        "the complete provider-bound item must remain unchanged"
    );
    let user_text = masked_json["input"][1]["content"][0]["text"]
        .as_str()
        .expect("user text should remain a string");
    assert!(!user_text.contains("bob@example.net"));
    assert!(user_text.contains("<AETHER:EMAIL:"));

    let sentinel = masked
        .session
        .sentinel_for_original("bob@example.net")
        .expect("masked user email should have a sentinel");
    let provider_response = serde_json::to_vec(&json!({
        "output": [
            {
                "type": "reasoning",
                "encrypted_content": "provider-bound-state",
                "future_state": {"version": 3},
                "content": [{
                    "type": "reasoning_text",
                    "text": format!("opaque {sentinel}")
                }]
            },
            {
                "type": "message",
                "content": [{
                    "type": "output_text",
                    "text": format!("visible {sentinel}")
                }]
            }
        ]
    }))
    .expect("provider response should serialize");
    let mut headers =
        BTreeMap::from([("content-type".to_string(), "application/json".to_string())]);
    let restored =
        restore_sync_response_body(&mut headers, provider_response.as_slice(), &masked.session)
            .expect("provider response should restore");
    let restored_json: serde_json::Value =
        serde_json::from_slice(&restored.body).expect("restored response should remain JSON");
    let nested_reasoning_text = restored_json["output"][0]["content"][0].clone();
    assert_eq!(
        restored_json["output"][0]["content"][0]["text"],
        format!("opaque {sentinel}"),
        "provider-bound reasoning text must remain byte-identical"
    );
    assert_eq!(
        restored_json["output"][0]["encrypted_content"],
        "provider-bound-state"
    );
    assert_eq!(
        restored_json["output"][0]["future_state"],
        json!({"version": 3})
    );
    assert_eq!(
        restored_json["output"][1]["content"][0]["text"], "visible bob@example.net",
        "ordinary assistant output should still be restored"
    );
    assert_eq!(
        nested_reasoning_text,
        json!({
            "type": "reasoning_text",
            "text": format!("opaque {sentinel}")
        }),
        "opaque reasoning parts must retain all original fields and values"
    );
}

#[test]
fn pii_redaction_rejects_sensitive_provider_bound_reasoning_state() {
    let request = json!({
        "model": "deepseek-reasoner",
        "input": [{
            "type": "reasoning",
            "encrypted_content": "opaque-state-must-remain-byte-identical",
            "content": [{
                "type": "reasoning_text",
                "text": "Provider state contains alice@example.com"
            }]
        }]
    });
    let raw = serde_json::to_vec(&request).expect("request should serialize");

    let error = try_mask_chat_pii_request_json_with_options(
        &raw,
        ChatPiiRedactionRequestFormat::OpenAiResponses,
        test_config(),
        deepseek_opaque_replay_options(),
    )
    .expect_err("sensitive opaque state must fail closed");

    assert_eq!(error, RedactionMaskError::SensitiveOpaqueReasoningState);
}

#[test]
fn forged_opaque_reasoning_shape_cannot_opt_out_of_pii_masking() {
    let request = json!({
        "model": "client-selected-model",
        "input": [{
            "type": "reasoning",
            "encrypted_content": "client-forged-opaque-state",
            "content": [{
                "type": "reasoning_text",
                "text": "Send this to alice@example.com"
            }]
        }]
    });
    let raw = serde_json::to_vec(&request).expect("request should serialize");

    let masked = try_mask_chat_pii_request_json_with_options(
        &raw,
        ChatPiiRedactionRequestFormat::OpenAiResponses,
        test_config(),
        MaskChatRequestOptions::runtime(),
    )
    .expect("an untrusted reasoning shape should use ordinary masking");

    assert!(masked.redacted);
    let masked_json: Value =
        serde_json::from_slice(&masked.body).expect("masked request should remain JSON");
    let text = masked_json["input"][0]["content"][0]["text"]
        .as_str()
        .expect("reasoning text should remain a string");
    assert!(!text.contains("alice@example.com"));
    assert!(text.contains("<AETHER:EMAIL:"));
    assert_eq!(
        masked_json["input"][0]["encrypted_content"],
        "client-forged-opaque-state"
    );
}

#[tokio::test]
async fn cached_pii_redaction_rejects_sensitive_provider_bound_reasoning_state() {
    let request = json!({
        "model": "deepseek-reasoner",
        "input": [{
            "type": "reasoning",
            "encrypted_content": "opaque-state-must-remain-byte-identical",
            "future_state": {
                "nested": {"future_owner": "alice@example.com"}
            },
            "content": [{
                "type": "reasoning_text",
                "text": "Provider state remains byte-identical"
            }]
        }]
    });

    let error = try_mask_chat_pii_request_value_with_cache_options(
        &request,
        ChatPiiRedactionRequestFormat::OpenAiResponses,
        test_config(),
        deepseek_opaque_replay_options(),
        None,
    )
    .await
    .expect_err("sensitive future text in opaque state must fail closed");

    assert_eq!(error, RedactionMaskError::SensitiveOpaqueReasoningState);
}

#[test]
fn pii_restore_still_restores_unbound_reasoning_text_events() {
    let masked = try_mask_chat_pii_request_json_with_options(
        br#"{"model":"gpt","input":"alice@example.com"}"#,
        ChatPiiRedactionRequestFormat::OpenAiResponses,
        test_config(),
        MaskChatRequestOptions::runtime(),
    )
    .expect("input should mask");
    let sentinel = masked
        .session
        .sentinel_for_original("alice@example.com")
        .expect("masked email should have a sentinel");
    let mut event = json!({
        "type": "response.reasoning_text.delta",
        "delta": format!("thinking about {sentinel}")
    });

    assert!(restore_json_strings(&mut event, &masked.session));
    assert_eq!(
        event["delta"], "thinking about alice@example.com",
        "an event name alone must not be treated as opaque bound state"
    );

    let mut unbound_part = json!({
        "type": "reasoning_text",
        "text": format!("thinking about {sentinel}")
    });
    assert!(restore_json_strings(&mut unbound_part, &masked.session));
    assert_eq!(
        unbound_part["text"], "thinking about alice@example.com",
        "a reasoning_text part without encrypted parent state must still restore"
    );

    let unbound_request = json!({
        "model": "gpt",
        "input": [{
            "type": "reasoning",
            "content": [{
                "type": "reasoning_text",
                "text": "unbound alice@example.com"
            }]
        }]
    });
    let unbound_raw =
        serde_json::to_vec(&unbound_request).expect("unbound request should serialize");
    let unbound_masked = try_mask_chat_pii_request_json_with_options(
        &unbound_raw,
        ChatPiiRedactionRequestFormat::OpenAiResponses,
        test_config(),
        MaskChatRequestOptions::runtime(),
    )
    .expect("unbound reasoning request should mask");
    let unbound_masked_json: Value = serde_json::from_slice(&unbound_masked.body)
        .expect("masked unbound request should remain JSON");
    let masked_reasoning_text = unbound_masked_json["input"][0]["content"][0]["text"]
        .as_str()
        .expect("unbound reasoning text should remain a string");
    assert!(!masked_reasoning_text.contains("alice@example.com"));
    assert!(masked_reasoning_text.contains("<AETHER:EMAIL:"));

    let mut invalid_opaque_item = json!({
        "type": "reasoning",
        "encrypted_content": "non-empty-but-unbound",
        "content": [{
            "type": "summary_text",
            "text": format!("thinking about {sentinel}")
        }]
    });
    assert!(restore_json_strings(
        &mut invalid_opaque_item,
        &masked.session
    ));
    assert_eq!(
        invalid_opaque_item["content"][0]["text"], "thinking about alice@example.com",
        "encrypted state without a reasoning_text part must not suppress restoration"
    );

    let mut identified_reasoning_item = json!({
        "type": "reasoning",
        "id": "rs_provider_123",
        "encrypted_content": "provider-state",
        "content": [{
            "type": "reasoning_text",
            "text": format!("thinking about {sentinel}")
        }]
    });
    assert!(restore_json_strings(
        &mut identified_reasoning_item,
        &masked.session
    ));
    assert_eq!(
        identified_reasoning_item["content"][0]["text"], "thinking about alice@example.com",
        "only id-less DeepSeek-style state may bypass ordinary restoration"
    );

    let identified_request = json!({
        "model": "gpt",
        "input": [{
            "type": "reasoning",
            "id": "rs_provider_123",
            "encrypted_content": "provider-state",
            "content": [{
                "type": "reasoning_text",
                "text": "identified alice@example.com"
            }]
        }]
    });
    let identified_masked = try_mask_chat_pii_request_json_with_options(
        &serde_json::to_vec(&identified_request).expect("request should serialize"),
        ChatPiiRedactionRequestFormat::OpenAiResponses,
        test_config(),
        MaskChatRequestOptions::runtime(),
    )
    .expect("identified reasoning request should mask");
    let identified_masked_json: Value = serde_json::from_slice(&identified_masked.body)
        .expect("identified reasoning request should remain JSON");
    let identified_text = identified_masked_json["input"][0]["content"][0]["text"]
        .as_str()
        .expect("identified reasoning text should remain a string");
    assert!(!identified_text.contains("alice@example.com"));
    assert!(identified_text.contains("<AETHER:EMAIL:"));
}

#[test]
fn pii_redaction_request_masks_openai_search_text_fields() {
    let request = json!({
        "id": "session-1",
        "model": "gpt-5.6",
        "input": "Find alice@example.com",
        "commands": {
            "search_query": [{"q": "Phone +14155552671"}],
            "image_query": [{"q": "Image for bob@example.com"}],
            "find": [{"ref_id": "https://example.com/alice@example.com", "pattern": "secret_key=secretValueABCDEF1234567890abcdef"}],
            "weather": [{"location": "Contact carol@example.com"}],
            "open": [{"ref_id": "https://example.com/alice@example.com"}]
        }
    });
    let raw = serde_json::to_vec(&request).expect("request should serialize");

    let masked = try_mask_chat_pii_request_json_with_options(
        &raw,
        ChatPiiRedactionRequestFormat::OpenAiSearch,
        test_config(),
        MaskChatRequestOptions::runtime(),
    )
    .expect("search request should mask");
    let masked_json: Value =
        serde_json::from_slice(&masked.body).expect("masked request should parse");

    assert!(masked.redacted);
    assert!(!masked_json["input"]
        .as_str()
        .unwrap()
        .contains("alice@example.com"));
    assert!(!masked_json["commands"]["search_query"][0]["q"]
        .as_str()
        .unwrap()
        .contains("+14155552671"));
    assert!(!masked_json["commands"]["image_query"][0]["q"]
        .as_str()
        .unwrap()
        .contains("bob@example.com"));
    assert!(!masked_json["commands"]["find"][0]["pattern"]
        .as_str()
        .unwrap()
        .contains("secretValueABCDEF1234567890abcdef"));
    assert!(!masked_json["commands"]["weather"][0]["location"]
        .as_str()
        .unwrap()
        .contains("carol@example.com"));
    assert_eq!(
        masked_json["commands"]["open"][0]["ref_id"],
        "https://example.com/alice@example.com"
    );
}

#[tokio::test]
async fn pii_redaction_async_request_masks_openai_search_text_fields() {
    let request = json!({
        "id": "session-1",
        "model": "gpt-5.6",
        "input": "Find alice@example.com",
        "commands": {
            "search_query": [{"q": "Phone +14155552671"}],
            "find": [{"ref_id": "turn0search0", "pattern": "bob@example.com"}]
        }
    });

    let masked = try_mask_chat_pii_request_value_with_cache_options(
        &request,
        ChatPiiRedactionRequestFormat::OpenAiSearch,
        test_config(),
        MaskChatRequestOptions::runtime(),
        None,
    )
    .await
    .expect("search request should mask");
    let masked_json = masked.body_json.expect("masked body should be present");

    assert!(masked.redacted);
    assert!(!masked_json["input"]
        .as_str()
        .unwrap()
        .contains("alice@example.com"));
    assert!(!masked_json["commands"]["search_query"][0]["q"]
        .as_str()
        .unwrap()
        .contains("+14155552671"));
    assert!(!masked_json["commands"]["find"][0]["pattern"]
        .as_str()
        .unwrap()
        .contains("bob@example.com"));
    assert_eq!(masked_json["commands"]["find"][0]["ref_id"], "turn0search0");
}

#[test]
fn pii_redaction_request_avoids_cross_message_and_tool_argument_sentinel_collisions() {
    let mut probe = session_at(600);
    probe.redact_text("Contact alice@example.com");
    let colliding_literal = probe
        .sentinel_for_original("alice@example.com")
        .expect("probe sentinel should exist")
        .to_string();
    let request = json!({
        "model": "gpt-5",
        "messages": [
            {"role": "user", "content": format!("literal {colliding_literal} stays unknown")},
            {
                "role": "assistant",
                "content": null,
                "tool_calls": [{
                    "type": "function",
                    "function": {
                        "name": "lookup_contact",
                        "arguments": "{\"email\":\"alice@example.com\"}"
                    }
                }]
            }
        ]
    });

    let masked = mask_chat_request_json(
        &serde_json::to_vec(&request).expect("request should serialize"),
        test_config(),
    );

    let current_sentinel = masked
        .session
        .sentinel_for_original("alice@example.com")
        .expect("current request sentinel should exist")
        .to_string();
    assert_ne!(current_sentinel, colliding_literal);
    let masked_json: serde_json::Value =
        serde_json::from_slice(&masked.body).expect("masked request should parse");
    assert_eq!(
        masked_json["messages"][0]["content"],
        format!("literal {colliding_literal} stays unknown")
    );
    let arguments = masked_json["messages"][1]["tool_calls"][0]["function"]["arguments"]
        .as_str()
        .expect("arguments should be text");
    assert!(!arguments.contains("alice@example.com"));
    assert!(arguments.contains(&current_sentinel));

    let mut headers =
        BTreeMap::from([("content-type".to_string(), "application/json".to_string())]);
    let restored = restore_sync_response_body(
        &mut headers,
        serde_json::to_vec(&json!({
            "message": format!("literal {colliding_literal} current {current_sentinel}")
        }))
        .expect("response should serialize")
        .as_slice(),
        &masked.session,
    )
    .expect("restore should succeed");
    let restored_json: serde_json::Value =
        serde_json::from_slice(&restored.body).expect("restored body should parse");
    assert_eq!(
        restored_json["message"],
        format!("literal {colliding_literal} current alice@example.com")
    );
}

#[test]
fn pii_redaction_request_noops_for_invalid_absent_or_malformed_messages() {
    for raw in [
        b"{not json".to_vec(),
        br#"{"model":"gpt-5","metadata":{"owner":"alice@example.com"}}"#.to_vec(),
        br#"{"model":"gpt-5","messages":{"role":"user","content":"alice@example.com"}}"#.to_vec(),
    ] {
        let masked = mask_chat_request_json(&raw, test_config());

        assert!(!masked.redacted);
        assert_eq!(masked.body, raw);
        assert_eq!(masked.session.mapping_count(), 0);
    }
}

#[test]
fn pii_redaction_request_noops_when_chat_messages_are_clean() {
    let raw = br#"{"model":"gpt-5","messages":[{"role":"user","content":"hello"}],"metadata":{"trace":"keep"}}"#;

    let masked = mask_chat_request_json(raw, test_config());

    assert!(!masked.redacted);
    assert_eq!(masked.body, raw);
    assert_eq!(masked.session.mapping_count(), 0);
}

#[test]
fn pii_redaction_sync_restore_json_strings_reserialize_safely_and_updates_content_length() {
    let sentinel = "<AETHER:EMAIL:ABCDEFGHIJKLMNOPQRST>";
    let original = "alice@example.com said \"ok\" from C:\\tmp\\a\nnext";
    let session = session_with_response_mapping(original, sentinel);
    let body = serde_json::to_vec(&json!({
        "choices": [{
            "message": {
                "role": "assistant",
                "content": format!("Restored: {sentinel}"),
            }
        }]
    }))
    .expect("response should serialize");
    let mut headers = BTreeMap::from([
        ("content-type".to_string(), "application/json".to_string()),
        ("content-length".to_string(), body.len().to_string()),
    ]);

    let restored =
        restore_sync_response_body(&mut headers, &body, &session).expect("restore should succeed");

    assert!(restored.restored);
    let restored_len = restored.body.len().to_string();
    assert_eq!(
        headers.get("content-length").map(String::as_str),
        Some(restored_len.as_str())
    );
    let restored_json: serde_json::Value =
        serde_json::from_slice(&restored.body).expect("restored body should stay valid JSON");
    assert_eq!(
        restored_json["choices"][0]["message"]["content"],
        format!("Restored: {original}")
    );
}

#[test]
fn pii_redaction_sync_restore_assistant_content_and_tool_call_arguments() {
    let request = json!({
        "model": "gpt-5",
        "messages": [
            {"role": "user", "content": "Email alice@example.com"},
            {
                "role": "assistant",
                "content": null,
                "tool_calls": [{
                    "type": "function",
                    "function": {
                        "name": "lookup_customer",
                        "arguments": "{\"phone\":\"13800138000\"}"
                    }
                }]
            }
        ]
    });
    let masked = mask_chat_request_json(
        &serde_json::to_vec(&request).expect("request should serialize"),
        test_config(),
    );
    let email_sentinel = masked
        .session
        .sentinel_for_original("alice@example.com")
        .expect("email sentinel should exist");
    let phone_sentinel = masked
        .session
        .sentinel_for_original("13800138000")
        .expect("phone sentinel should exist");
    let body = serde_json::to_vec(&json!({
        "choices": [{
            "message": {
                "role": "assistant",
                "content": format!("Customer {email_sentinel}"),
                "tool_calls": [{
                    "type": "function",
                    "function": {
                        "name": "lookup_customer",
                        "arguments": format!("{{\"phone\":\"{phone_sentinel}\"}}")
                    }
                }]
            }
        }]
    }))
    .expect("response should serialize");
    let mut headers = BTreeMap::from([(
        "content-type".to_string(),
        "application/json; charset=utf-8".to_string(),
    )]);

    let restored = restore_sync_response_body(&mut headers, &body, &masked.session)
        .expect("restore should succeed");

    let restored_json: serde_json::Value =
        serde_json::from_slice(&restored.body).expect("restored body should stay valid JSON");
    assert_eq!(
        restored_json["choices"][0]["message"]["content"],
        "Customer alice@example.com"
    );
    assert_eq!(
        restored_json["choices"][0]["message"]["tool_calls"][0]["function"]["arguments"],
        "{\"phone\":\"13800138000\"}"
    );
}

#[test]
fn pii_redaction_sync_restore_unknown_sentinel_looking_strings_remain_unchanged() {
    let known_sentinel = "<AETHER:EMAIL:ABCDEFGHIJKLMNOPQRST>";
    let unknown_sentinel = "<AETHER:EMAIL:TSRQPONMLKJIHGFEDCBA>";
    let session = session_with_response_mapping("alice@example.com", known_sentinel);
    let body = serde_json::to_vec(&json!({
        "message": format!("known {known_sentinel} unknown {unknown_sentinel}"),
    }))
    .expect("response should serialize");
    let mut headers =
        BTreeMap::from([("content-type".to_string(), "application/json".to_string())]);

    let restored =
        restore_sync_response_body(&mut headers, &body, &session).expect("restore should succeed");

    let restored_json: serde_json::Value =
        serde_json::from_slice(&restored.body).expect("restored body should stay valid JSON");
    assert_eq!(
        restored_json["message"],
        format!("known alice@example.com unknown {unknown_sentinel}")
    );
}

#[test]
fn pii_redaction_sync_restore_text_body_replaces_current_request_sentinels() {
    let known_sentinel = "<AETHER:EMAIL:ABCDEFGHIJKLMNOPQRST>";
    let unknown_sentinel = "<AETHER:EMAIL:TSRQPONMLKJIHGFEDCBA>";
    let session = session_with_response_mapping("alice@example.com", known_sentinel);
    let body = format!("plain {known_sentinel} and {unknown_sentinel}").into_bytes();
    let mut headers = BTreeMap::from([
        ("content-type".to_string(), "text/plain".to_string()),
        ("Content-Length".to_string(), body.len().to_string()),
    ]);

    let restored =
        restore_sync_response_body(&mut headers, &body, &session).expect("restore should succeed");

    assert_eq!(
        std::str::from_utf8(&restored.body).expect("restored text should be utf8"),
        format!("plain alice@example.com and {unknown_sentinel}")
    );
    let restored_len = restored.body.len().to_string();
    assert_eq!(headers.get("Content-Length"), None);
    assert_eq!(
        headers.get("content-length").map(String::as_str),
        Some(restored_len.as_str())
    );
}

#[test]
fn pii_redaction_sync_restore_rejects_expansion_before_allocating_output() {
    let sentinel = "<AETHER:EMAIL:ABCDEFGHIJKLMNOPQRST>";
    let original = "x".repeat(256);
    let session = session_with_response_mapping(&original, sentinel);
    let body = sentinel.repeat(2).into_bytes();
    let mut headers = BTreeMap::from([
        ("content-type".to_string(), "text/plain".to_string()),
        ("content-length".to_string(), body.len().to_string()),
    ]);

    let error = super::restore_sync_response_body_with_limit(&mut headers, &body, &session, 64)
        .expect_err("sentinel amplification above the expansion budget must fail");

    assert!(format!("{error:?}").contains("restoration expansion exceeds"));
    let original_len = body.len().to_string();
    assert_eq!(
        headers.get("content-length").map(String::as_str),
        Some(original_len.as_str())
    );
}

#[test]
fn pii_redaction_sync_restore_expansion_limit_is_independent_of_body_size() {
    let sentinel = "<AETHER:EMAIL:ABCDEFGHIJKLMNOPQRST>";
    let original = "alice@example.com";
    let session = session_with_response_mapping(original, sentinel);
    let prefix = "p".repeat(256);
    let body = format!("{prefix}{sentinel}").into_bytes();
    let mut headers = BTreeMap::from([("content-type".to_string(), "text/plain".to_string())]);

    let restored =
        super::restore_sync_response_body_with_limit(&mut headers, &body, &session, original.len())
            .expect("a body larger than the expansion budget must still restore");

    assert_eq!(
        std::str::from_utf8(&restored.body).expect("restored body should be UTF-8"),
        format!("{prefix}{original}")
    );
}

#[test]
fn pii_redaction_compressed_response_safe_error() {
    let sentinel = "<AETHER:EMAIL:ABCDEFGHIJKLMNOPQRST>";
    let session = session_with_response_mapping("alice@example.com", sentinel);
    let body = format!("compressed bytes with {sentinel}").into_bytes();
    let mut headers = BTreeMap::from([
        ("Content-Encoding".to_string(), "identity".to_string()),
        ("content-encoding".to_string(), "gzip".to_string()),
        ("content-length".to_string(), body.len().to_string()),
    ]);

    let err = restore_sync_response_body(&mut headers, &body, &session)
        .expect_err("compressed response should fail safely");

    let message = format!("{err:?}");
    assert!(message.contains("encoded response bodies"));
    assert!(!message.contains("alice@example.com"));
    assert!(!message.contains(sentinel));
    assert_eq!(
        headers.get("Content-Encoding").map(String::as_str),
        Some("identity")
    );
    assert_eq!(
        headers.get("content-encoding").map(String::as_str),
        Some("gzip")
    );
    let original_len = body.len().to_string();
    assert_eq!(
        headers.get("content-length").map(String::as_str),
        Some(original_len.as_str())
    );

    let stream_err = match StreamingResponseRestorer::new(&headers, &session) {
        Ok(_) => panic!("compressed stream response should fail safely"),
        Err(err) => err,
    };
    let stream_message = format!("{stream_err:?}");
    assert!(stream_message.contains("encoded response bodies"));
    assert!(!stream_message.contains("alice@example.com"));
    assert!(!stream_message.contains(sentinel));
}

#[test]
fn pii_redaction_stream_restore_raw_text_split_sentinel_across_three_chunks() {
    let known_sentinel = "<AETHER:EMAIL:ABCDEFGHIJKLMNOPQRST>";
    let unknown_sentinel = "<AETHER:EMAIL:TSRQPONMLKJIHGFEDCBA>";
    let session = session_with_response_mapping("alice@example.com", known_sentinel);
    let mut restorer = StreamingResponseRestorer::for_text(&session);
    let chunks = [
        b"prefix <AET".as_slice(),
        b"HER:EMAIL:ABC".as_slice(),
        b"DEFGHIJKLMNOPQRST> middle ".as_slice(),
        unknown_sentinel.as_bytes(),
    ];

    let mut output = Vec::new();
    for chunk in chunks {
        output.extend(restorer.push_chunk(chunk).expect("restore should succeed"));
        assert!(restorer.pending_text_carry_len() <= restorer.max_text_carry_len());
    }
    output.extend(restorer.finish().expect("finish should succeed"));

    assert_eq!(
        String::from_utf8(output).expect("restored stream should be utf8"),
        format!("prefix alice@example.com middle {unknown_sentinel}")
    );
    assert_eq!(restorer.pending_text_carry_len(), 0);
}

#[test]
fn pii_redaction_stream_restore_bounded_carry_and_final_flush() {
    let sentinel = "<AETHER:EMAIL:ABCDEFGHIJKLMNOPQRST>";
    let session = session_with_response_mapping("alice@example.com", sentinel);
    let mut restorer = StreamingResponseRestorer::for_text(&session);
    let chunk = vec![b'x'; sentinel.len() * 3];

    let first = restorer.push_chunk(&chunk).expect("restore should succeed");

    assert!(!first.is_empty());
    assert!(restorer.pending_text_carry_len() <= restorer.max_text_carry_len());
    let flushed = restorer.finish().expect("finish should succeed");
    assert_eq!(first.len() + flushed.len(), chunk.len());
    assert_eq!(restorer.pending_text_carry_len(), 0);
}

#[test]
fn pii_redaction_stream_restore_rejects_only_expansion_not_passthrough_chunk_size() {
    let sentinel = "<AETHER:EMAIL:ABCDEFGHIJKLMNOPQRST>";
    let original = "x".repeat(256);
    let session = session_with_response_mapping(&original, sentinel);
    let mut restorer = StreamingResponseRestorer::for_text_with_expansion_limit(&session, 32);

    let passthrough = vec![b'p'; 4 * 1024];
    let output = restorer
        .push_chunk(&passthrough)
        .expect("an unmatched chunk is not subject to the expansion budget");
    assert_eq!(
        output.len() + restorer.pending_text_carry_len(),
        passthrough.len()
    );

    let error = restorer
        .push_chunk(sentinel.as_bytes())
        .expect_err("a matching sentinel above the per-push expansion budget must fail");
    assert!(format!("{error:?}").contains("restoration expansion exceeds"));
}

#[test]
fn pii_redaction_stream_restore_expansion_budget_resets_for_each_push() {
    let sentinel = "<AETHER:EMAIL:ABCDEFGHIJKLMNOPQRST>";
    let original = "x".repeat(sentinel.len() + 8);
    let session = session_with_response_mapping(&original, sentinel);
    let mut restorer = StreamingResponseRestorer::for_text_with_expansion_limit(&session, 8);

    let first = restorer
        .push_chunk(sentinel.as_bytes())
        .expect("first chunk should fit its expansion budget");
    let second = restorer
        .push_chunk(sentinel.as_bytes())
        .expect("second chunk should receive a fresh expansion budget");
    let tail = restorer.finish().expect("carry should flush");

    assert_eq!(first.len() + second.len() + tail.len(), original.len() * 2);
}

#[test]
fn pii_redaction_stream_restore_sse_rejects_event_expansion_before_serializing() {
    let sentinel = "<AETHER:EMAIL:ABCDEFGHIJKLMNOPQRST>";
    let original = "x".repeat(256);
    let session = session_with_response_mapping(&original, sentinel);
    let mut restorer = StreamingResponseRestorer::for_sse_with_expansion_limit(&session, 32);
    let event = format!("data: {{\"value\":\"{sentinel}\"}}\n\n");

    let error = restorer
        .push_chunk(event.as_bytes())
        .expect_err("an SSE event above the per-push expansion budget must fail");

    assert!(format!("{error:?}").contains("restoration expansion exceeds"));
}

#[test]
fn pii_redaction_stream_restore_rejects_unbounded_sse_line() {
    let sentinel = "<AETHER:EMAIL:ABCDEFGHIJKLMNOPQRST>";
    let session = session_with_response_mapping("alice@example.com", sentinel);
    let mut restorer = StreamingResponseRestorer::for_sse(&session);
    let oversized = vec![b'x'; super::MAX_STREAM_RESTORE_BUFFER_BYTES + 1];

    let error = restorer
        .push_chunk(&oversized)
        .expect_err("an incomplete SSE line must be bounded");
    assert!(format!("{error:?}").contains("stream redaction buffer exceeds"));
}

#[test]
fn pii_redaction_stream_restore_rejects_unbounded_sse_json_event() {
    let sentinel = "<AETHER:EMAIL:ABCDEFGHIJKLMNOPQRST>";
    let session = session_with_response_mapping("alice@example.com", sentinel);
    let mut restorer = StreamingResponseRestorer::for_sse(&session);
    let payload_len = super::MAX_STREAM_RESTORE_BUFFER_BYTES / 2;
    let first = format!("data: {{\"value\":\"{}\n", "x".repeat(payload_len));
    let second = format!("data: {}\n", "x".repeat(payload_len));

    restorer
        .push_chunk(first.as_bytes())
        .expect("first JSON line should fit");
    let error = restorer
        .push_chunk(second.as_bytes())
        .expect_err("an incomplete SSE JSON event must be bounded");
    assert!(format!("{error:?}").contains("stream redaction SSE event exceeds"));
}

#[test]
fn pii_redaction_stream_restore_sse_json_split_sentinel_across_three_chunks() {
    let sentinel = "<AETHER:EMAIL:ABCDEFGHIJKLMNOPQRST>";
    let session = session_with_response_mapping("alice@example.com", sentinel);
    let mut restorer = StreamingResponseRestorer::for_sse(&session);
    let chunks = [
        b"data: {\"choices\":[{\"delta\":{\"content\":\"hello <AET".as_slice(),
        b"HER:EMAIL:ABC".as_slice(),
        b"DEFGHIJKLMNOPQRST>\"}}]}\n\n".as_slice(),
    ];

    let mut output = Vec::new();
    for chunk in chunks {
        output.extend(restorer.push_chunk(chunk).expect("restore should succeed"));
    }
    output.extend(restorer.finish().expect("finish should succeed"));
    let output_text = String::from_utf8(output).expect("restored stream should be utf8");

    assert!(output_text.contains("hello alice@example.com"));
    assert!(!output_text.contains(sentinel));
    assert!(output_text.ends_with("\n\n"));
}

#[test]
fn pii_redaction_stream_restore_sse_tool_call_argument_delta_json_strings() {
    let request = json!({
        "model": "gpt-5",
        "messages": [
            {"role": "user", "content": "Email alice@example.com"},
            {
                "role": "assistant",
                "content": null,
                "tool_calls": [{
                    "type": "function",
                    "function": {
                        "name": "lookup_customer",
                        "arguments": "{\"phone\":\"13800138000\"}"
                    }
                }]
            }
        ]
    });
    let masked = mask_chat_request_json(
        &serde_json::to_vec(&request).expect("request should serialize"),
        test_config(),
    );
    let email_sentinel = masked
        .session
        .sentinel_for_original("alice@example.com")
        .expect("email sentinel should exist");
    let phone_sentinel = masked
        .session
        .sentinel_for_original("13800138000")
        .expect("phone sentinel should exist");
    let event = format!(
        "data: {}\n\n",
        serde_json::to_string(&json!({
            "choices": [{
                "delta": {
                    "content": format!("Customer {email_sentinel}"),
                    "tool_calls": [{
                        "index": 0,
                        "function": {
                            "arguments": format!("{{\"phone\":\"{phone_sentinel}\"}}")
                        }
                    }]
                }
            }]
        }))
        .expect("event should serialize")
    );
    let mut restorer = StreamingResponseRestorer::for_sse(&masked.session);

    let output = restorer
        .push_chunk(event.as_bytes())
        .expect("restore should succeed");

    let output_text = String::from_utf8(output).expect("restored stream should be utf8");
    assert!(output_text.contains("Customer alice@example.com"));
    assert!(output_text.contains("\\\"phone\\\":\\\"13800138000\\\""));
    assert!(!output_text.contains(email_sentinel));
    assert!(!output_text.contains(phone_sentinel));
    assert!(restorer.finish().expect("finish should succeed").is_empty());
}

#[test]
fn pii_redaction_stream_restore_sse_preserves_comments_blank_crlf_multiple_data_and_done() {
    let sentinel = "<AETHER:EMAIL:ABCDEFGHIJKLMNOPQRST>";
    let session = session_with_response_mapping("alice@example.com", sentinel);
    let mut restorer = StreamingResponseRestorer::for_sse(&session);
    let input = format!(
        ": keep-alive\r\n\r\nevent: message\r\ndata: plain {sentinel}\r\ndata: second line\r\n\r\ndata: [DONE]\r\n\r\n"
    );

    let mut output = restorer
        .push_chunk(input.as_bytes())
        .expect("restore should succeed");
    output.extend(restorer.finish().expect("finish should succeed"));

    assert_eq!(
        String::from_utf8(output).expect("restored stream should be utf8"),
        concat!(
            ": keep-alive\r\n",
            "\r\n",
            "event: message\r\n",
            "data: plain alice@example.com\r\n",
            "data: second line\r\n",
            "\r\n",
            "data: [DONE]\r\n",
            "\r\n"
        )
    );
}

#[test]
fn proxy_pii_redaction_rule_selection_controls_email_detector() {
    let config = ChatPiiRedactionRuntimeConfig {
        enabled: true,
        rules: parse_chat_pii_redaction_rules(Some(&json!([{
            "id": "email",
            "name": "邮箱",
            "pattern": "(?i)[A-Z0-9._%+-]{1,64}@[A-Z0-9.-]{1,253}\\.[A-Z]{2,63}",
            "enabled": true,
            "kind": "email",
            "system": true
        }])))
        .expect("email rule should compile"),
        ..ChatPiiRedactionRuntimeConfig::default()
    };
    let request = json!({
        "model": "gpt-5",
        "messages": [{"role": "user", "content": "Email alice@example.com phone +14155552671"}]
    });

    let masked = mask_chat_request_json_with_options(
        &serde_json::to_vec(&request).expect("request should serialize"),
        build_redaction_session_config(b"redaction-test-key".to_vec(), &config, 600),
        MaskChatRequestOptions::runtime(),
    );

    let masked_json: serde_json::Value =
        serde_json::from_slice(&masked.body).expect("masked request should parse");
    let content = masked_json["messages"][0]["content"]
        .as_str()
        .expect("content should stay string");
    assert!(!content.contains("alice@example.com"));
    assert!(content.contains("+14155552671"));
    assert!(content.contains("<AETHER:EMAIL:"));
    assert!(!content.contains("<AETHER:PHONE:"));
}

#[test]
fn proxy_pii_redaction_disabled_rules_disable_all_detectors() {
    let config = ChatPiiRedactionRuntimeConfig {
        enabled: true,
        rules: parse_chat_pii_redaction_rules(Some(&json!([{
            "id": "email",
            "name": "邮箱",
            "pattern": "(?i)[A-Z0-9._%+-]{1,64}@[A-Z0-9.-]{1,253}\\.[A-Z]{2,63}",
            "enabled": false,
            "kind": "email",
            "system": true
        }])))
        .expect("disabled email rule should compile"),
        ..ChatPiiRedactionRuntimeConfig::default()
    };
    let request = json!({
        "model": "gpt-5",
        "messages": [{"role": "user", "content": "Email alice@example.com phone +14155552671"}]
    });

    let masked = mask_chat_request_json_with_options(
        &serde_json::to_vec(&request).expect("request should serialize"),
        build_redaction_session_config(b"redaction-test-key".to_vec(), &config, 600),
        MaskChatRequestOptions::runtime(),
    );

    assert!(!masked.redacted);
    let masked_json: serde_json::Value =
        serde_json::from_slice(&masked.body).expect("masked request should parse");
    assert_eq!(masked_json, request);
    assert!(masked_json.to_string().contains("alice@example.com"));
    assert!(!masked_json.to_string().contains("<AETHER:"));
}

#[test]
fn redaction_session_slot_uses_single_candidate_when_response_header_missing() {
    let slot = RedactionSessionSlot::default();
    let sentinel = "<AETHER:EMAIL:SINGLECANDIDATE001>";
    slot.put_for_candidate(
        "candidate-1",
        session_with_response_mapping("alice@example.com", sentinel),
    );

    let session = slot
        .take_for_candidate(None)
        .expect("single candidate session should be used without header");

    assert_eq!(
        session.sentinel_for_original("alice@example.com"),
        Some(sentinel)
    );
    assert!(slot.take_for_candidate(Some("candidate-1")).is_none());
}

#[test]
fn redaction_session_slot_does_not_guess_when_response_header_missing_for_multiple_candidates() {
    let slot = RedactionSessionSlot::default();
    let alice_sentinel = "<AETHER:EMAIL:MULTICANDIDATE001>";
    let bob_sentinel = "<AETHER:EMAIL:MULTICANDIDATE002>";
    slot.put_for_candidate(
        "candidate-1",
        session_with_response_mapping("alice@example.com", alice_sentinel),
    );
    slot.put_for_candidate(
        "candidate-2",
        session_with_response_mapping("bob@example.com", bob_sentinel),
    );

    assert!(slot.take_for_candidate(None).is_none());

    let session = slot
        .take_for_candidate(Some("candidate-2"))
        .expect("specific candidate session should remain available");
    assert_eq!(
        session.sentinel_for_original("bob@example.com"),
        Some(bob_sentinel)
    );
}

#[test]
fn redaction_session_slot_does_not_fallback_to_other_candidate_on_header_miss() {
    let slot = RedactionSessionSlot::default();
    slot.put_for_candidate(
        "candidate-1",
        session_with_response_mapping("alice@example.com", "<AETHER:EMAIL:HEADERMISS001>"),
    );

    assert!(slot.take_for_candidate(Some("candidate-2")).is_none());
    assert!(slot.take_for_candidate(Some("candidate-1")).is_some());
}

#[test]
fn proxy_pii_redaction_provider_bound_request_uses_sentinels_without_prompt_notice() {
    let config = ChatPiiRedactionRuntimeConfig::default();
    let request = json!({
        "model": "gpt-5",
        "messages": [
            {"role": "system", "content": "be brief"},
            {"role": "user", "content": "Contact alice@example.com"},
            {"role": "assistant", "content": "ok"}
        ]
    });

    let masked = mask_chat_request_json_with_options(
        &serde_json::to_vec(&request).expect("request should serialize"),
        build_redaction_session_config(b"redaction-test-key".to_vec(), &config, 600),
        MaskChatRequestOptions::runtime(),
    );

    assert!(masked.redacted);
    let masked_json: serde_json::Value =
        serde_json::from_slice(&masked.body).expect("masked request should parse");
    let messages = masked_json["messages"]
        .as_array()
        .expect("messages should be an array");
    assert_eq!(messages.len(), 3);
    assert_eq!(messages[0]["role"], "system");
    assert!(messages[1..]
        .iter()
        .all(|message| message["role"].as_str() != Some("system")));
    assert_eq!(messages[1]["role"], "user");
    let content = messages[1]["content"]
        .as_str()
        .expect("user content should be text");
    assert!(!content.contains("alice@example.com"));
    assert!(content.contains("<AETHER:EMAIL:"));
    assert_eq!(messages[2]["role"], "assistant");
}

#[test]
fn proxy_pii_redaction_sync_and_stream_client_responses_restore_current_sentinels() {
    let request = json!({
        "model": "gpt-5",
        "messages": [{"role": "user", "content": "Email alice@example.com"}]
    });
    let masked = mask_chat_request_json(
        &serde_json::to_vec(&request).expect("request should serialize"),
        test_config(),
    );
    let sentinel = masked
        .session
        .sentinel_for_original("alice@example.com")
        .expect("email sentinel should exist");
    let sync_body = serde_json::to_vec(&json!({
        "choices": [{"message": {"role": "assistant", "content": format!("hello {sentinel}")}}]
    }))
    .expect("response should serialize");
    let mut headers =
        BTreeMap::from([("content-type".to_string(), "application/json".to_string())]);

    let restored = restore_sync_response_body(&mut headers, &sync_body, &masked.session)
        .expect("sync restore should succeed");
    let restored_json: serde_json::Value =
        serde_json::from_slice(&restored.body).expect("restored response should parse");
    assert_eq!(
        restored_json["choices"][0]["message"]["content"],
        "hello alice@example.com"
    );

    let sse = format!("data: {{\"choices\":[{{\"delta\":{{\"content\":\"{sentinel}\"}}}}]}}\n\n");
    let mut restorer = StreamingResponseRestorer::for_sse(&masked.session);
    let mut output = restorer
        .push_chunk(sse.as_bytes())
        .expect("stream restore should succeed");
    output.extend(restorer.finish().expect("stream finish should succeed"));
    let output = String::from_utf8(output).expect("stream should be utf8");
    assert!(output.contains("alice@example.com"));
    assert!(!output.contains(sentinel));
}

#[test]
fn pii_redaction_performance_prefilters_skip_impossible_expensive_validators() {
    let mut clean_probe = DetectorProbe::default();
    let clean_candidates = detect_candidates_with_probe(
        "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz",
        Some(&mut clean_probe),
    );

    assert!(clean_candidates.is_empty());
    for kind in [
        RedactionKind::ChinaResidentId,
        RedactionKind::PaymentCard,
        RedactionKind::Jwt,
        RedactionKind::ApiKey,
    ] {
        assert_eq!(clean_probe.validator_calls(kind), 0);
    }

    let mut token_probe = DetectorProbe::default();
    let token_candidates = detect_candidates_with_probe(
        "token q8HjK2LmN9PqR4StU7VwX0YzA3BcD6Ef",
        Some(&mut token_probe),
    );

    assert_eq!(token_probe.validator_calls(RedactionKind::ApiKey), 1);
    assert!(token_candidates
        .iter()
        .any(|candidate| candidate.kind == Some(RedactionKind::ApiKey)));
}

#[test]
fn pii_redaction_accepts_text_and_detection_counts_above_previous_caps() {
    let large_request = json!({
        "model": "gpt-5",
        "messages": [{
            "role": "user",
            "content": format!("alice@example.com {}", "x".repeat(2 * 1024 * 1024 + 1))
        }]
    });
    let large_masked = try_mask_chat_request_json_with_options(
        &serde_json::to_vec(&large_request).expect("request should serialize"),
        test_config(),
        MaskChatRequestOptions::runtime(),
    )
    .expect("text above the previous scan cap should be redacted");
    assert!(large_masked.redacted);
    assert_eq!(large_masked.session.mapping_count(), 1);

    let dense_content = (0..1025)
        .map(|index| format!("user{index}@example.com"))
        .collect::<Vec<_>>()
        .join(" ");
    let dense_request = json!({
        "model": "gpt-5",
        "messages": [{"role": "user", "content": dense_content}]
    });
    let dense_masked = try_mask_chat_request_json_with_options(
        &serde_json::to_vec(&dense_request).expect("request should serialize"),
        test_config(),
        MaskChatRequestOptions::runtime(),
    )
    .expect("detections above the previous cap should be redacted");
    assert!(dense_masked.redacted);
    assert_eq!(dense_masked.session.mapping_count(), 1025);
}

#[tokio::test]
async fn pii_redaction_performance_multiturn_cache_reuses_and_rolls_over_sentinels() {
    let Some(redis) = start_managed_redis_or_skip().await else {
        return;
    };
    let runtime_state =
        redis_cache_runtime_state(redis.redis_url(), "pii_redaction_performance_cache").await;
    let cache = RedisRedactionMappingCache::new(&runtime_state);
    let config = email_only_runtime_config();

    let first_request = json!({
        "model": "gpt-5",
        "messages": [{"role": "user", "content": "Contact Alice@Example.com"}],
        "provider_context": "provider-a"
    });
    let first_masked = try_mask_chat_request_json_with_cache_options(
        &serde_json::to_vec(&first_request).expect("request should serialize"),
        build_redaction_session_config(b"redaction-test-key".to_vec(), &config, 600),
        MaskChatRequestOptions::runtime(),
        Some(&cache),
    )
    .await
    .expect("first masking should succeed");
    let first_sentinel = first_masked
        .session
        .sentinel_for_original("Alice@Example.com")
        .expect("first sentinel should exist")
        .to_string();

    let second_request = json!({
        "model": "gpt-5",
        "messages": [{"role": "user", "content": "Contact alice@example.com and bob@example.net"}],
        "provider_context": "provider-b"
    });
    let second_masked = try_mask_chat_request_json_with_cache_options(
        &serde_json::to_vec(&second_request).expect("request should serialize"),
        build_redaction_session_config(b"redaction-test-key".to_vec(), &config, 899),
        MaskChatRequestOptions::runtime(),
        Some(&cache),
    )
    .await
    .expect("second masking should succeed");
    assert_eq!(
        second_masked
            .session
            .sentinel_for_original("alice@example.com"),
        Some(first_sentinel.as_str())
    );
    assert!(second_masked
        .session
        .sentinel_for_original("bob@example.net")
        .is_some());
    let second_masked_json: serde_json::Value =
        serde_json::from_slice(&second_masked.body).expect("second masked request should parse");
    assert_eq!(second_masked_json["provider_context"], "provider-b");
    let second_content = second_masked_json["messages"][0]["content"]
        .as_str()
        .expect("second content should be text");
    assert!(second_content.contains(&first_sentinel));
    assert!(!second_content.contains("alice@example.com"));
    assert!(!second_content.contains("bob@example.net"));
    let mut headers =
        BTreeMap::from([("content-type".to_string(), "application/json".to_string())]);
    let restored = restore_sync_response_body(
        &mut headers,
        serde_json::to_vec(&json!({"message": format!("cached {first_sentinel}")}))
            .expect("response should serialize")
            .as_slice(),
        &second_masked.session,
    )
    .expect("restore should succeed");
    let restored_json: serde_json::Value =
        serde_json::from_slice(&restored.body).expect("restored body should parse");
    assert_eq!(restored_json["message"], "cached alice@example.com");

    let rolled_masked = try_mask_chat_request_json_with_cache_options(
        &serde_json::to_vec(&second_request).expect("request should serialize"),
        build_redaction_session_config(b"redaction-test-key".to_vec(), &config, 900),
        MaskChatRequestOptions::runtime(),
        Some(&cache),
    )
    .await
    .expect("rolled masking should succeed");
    let rolled_sentinel = rolled_masked
        .session
        .sentinel_for_original("alice@example.com")
        .expect("rolled sentinel should exist")
        .to_string();
    assert_ne!(rolled_sentinel, first_sentinel);
    let mut rolled_headers =
        BTreeMap::from([("content-type".to_string(), "application/json".to_string())]);
    let rolled_restored = restore_sync_response_body(
        &mut rolled_headers,
        serde_json::to_vec(&json!({"message": format!("rolled {rolled_sentinel}")}))
            .expect("response should serialize")
            .as_slice(),
        &rolled_masked.session,
    )
    .expect("rolled restore should succeed");
    let rolled_restored_json: serde_json::Value =
        serde_json::from_slice(&rolled_restored.body).expect("rolled restored body should parse");
    assert_eq!(rolled_restored_json["message"], "rolled alice@example.com");

    let keys = runtime_state
        .scan_keys("privacy:redaction:mapping:*", 100)
        .await
        .expect("keys should read");
    assert!(!keys.is_empty());
    for key in keys {
        assert!(!key.contains("alice@example.com"));
        let raw_key = runtime_state.strip_namespace(&key).to_string();
        let ttl = runtime_state
            .kv_ttl_seconds(&raw_key)
            .await
            .expect("ttl should read");
        assert!(
            ttl.unwrap_or_default() > 0,
            "redaction cache key {key} should have ttl"
        );
    }
}

#[tokio::test]
async fn pii_redaction_performance_cache_hit_avoids_cached_sentinel_literal_collision() {
    let Some(redis) = start_managed_redis_or_skip().await else {
        return;
    };
    let runtime_state = redis_cache_runtime_state(
        redis.redis_url(),
        "pii_redaction_performance_cache_collision",
    )
    .await;
    let cache = RedisRedactionMappingCache::new(&runtime_state);
    let config = email_only_runtime_config();

    let first_request = json!({
        "model": "gpt-5",
        "messages": [{"role": "user", "content": "Contact alice@example.com"}]
    });
    let first_masked = try_mask_chat_request_json_with_cache_options(
        &serde_json::to_vec(&first_request).expect("request should serialize"),
        build_redaction_session_config(b"redaction-test-key".to_vec(), &config, 600),
        MaskChatRequestOptions::runtime(),
        Some(&cache),
    )
    .await
    .expect("first masking should succeed");
    let cached_sentinel = first_masked
        .session
        .sentinel_for_original("alice@example.com")
        .expect("cached sentinel should exist")
        .to_string();

    let colliding_request = json!({
        "model": "gpt-5",
        "messages": [
            {
                "role": "user",
                "content": format!("literal {cached_sentinel} stays unknown")
            },
            {
                "role": "assistant",
                "content": null,
                "tool_calls": [{
                    "type": "function",
                    "function": {
                        "name": "lookup_contact",
                        "arguments": "{\"email\":\"alice@example.com\"}"
                    }
                }]
            }
        ]
    });
    let second_masked = try_mask_chat_request_json_with_cache_options(
        &serde_json::to_vec(&colliding_request).expect("request should serialize"),
        build_redaction_session_config(b"redaction-test-key".to_vec(), &config, 899),
        MaskChatRequestOptions::runtime(),
        Some(&cache),
    )
    .await
    .expect("collision masking should succeed");
    let current_sentinel = second_masked
        .session
        .sentinel_for_original("alice@example.com")
        .expect("current request sentinel should exist")
        .to_string();
    assert_ne!(current_sentinel, cached_sentinel);

    let masked_json: serde_json::Value =
        serde_json::from_slice(&second_masked.body).expect("masked body should parse");
    let content = masked_json["messages"][0]["content"]
        .as_str()
        .expect("content should be text");
    assert_eq!(content.matches(&cached_sentinel).count(), 1);
    assert!(!content.contains("alice@example.com"));
    let arguments = masked_json["messages"][1]["tool_calls"][0]["function"]["arguments"]
        .as_str()
        .expect("arguments should be text");
    assert_eq!(arguments.matches(&current_sentinel).count(), 1);
    assert!(!arguments.contains("alice@example.com"));

    let mut headers =
        BTreeMap::from([("content-type".to_string(), "application/json".to_string())]);
    let restored = restore_sync_response_body(
        &mut headers,
        serde_json::to_vec(&json!({
            "message": format!("literal {cached_sentinel} and {current_sentinel}")
        }))
        .expect("response should serialize")
        .as_slice(),
        &second_masked.session,
    )
    .expect("restore should succeed");
    let restored_json: serde_json::Value =
        serde_json::from_slice(&restored.body).expect("restored body should parse");
    assert_eq!(
        restored_json["message"],
        format!("literal {cached_sentinel} and alice@example.com")
    );
}

#[tokio::test]
async fn pii_redaction_performance_ignores_malformed_cached_sentinel() {
    let Some(redis) = start_managed_redis_or_skip().await else {
        return;
    };
    let runtime_state =
        redis_cache_runtime_state(redis.redis_url(), "pii_redaction_performance_bad_cache").await;
    let cache = RedisRedactionMappingCache::new(&runtime_state);
    let config = email_only_runtime_config();
    runtime_state
        .kv_set(
            &cache.forward_cache_key("EMAIL", "alice@example.com", 2),
            "<AETHER:PHONE:ABCDEFGHIJKLMNOPQRST>",
            Some(Duration::from_secs(300)),
        )
        .await
        .expect("malformed cache value should write");

    let request = json!({
        "model": "gpt-5",
        "messages": [{"role": "user", "content": "Contact alice@example.com"}]
    });
    let masked = try_mask_chat_request_json_with_cache_options(
        &serde_json::to_vec(&request).expect("request should serialize"),
        build_redaction_session_config(b"redaction-test-key".to_vec(), &config, 600),
        MaskChatRequestOptions::runtime(),
        Some(&cache),
    )
    .await
    .expect("masking should ignore malformed cache value");

    let sentinel = masked
        .session
        .sentinel_for_original("alice@example.com")
        .expect("email sentinel should exist");
    assert!(sentinel.starts_with("<AETHER:EMAIL:"));
    assert_ne!(sentinel, "<AETHER:PHONE:ABCDEFGHIJKLMNOPQRST>");
}

#[tokio::test]
async fn pii_redaction_performance_ignores_cached_sentinel_with_wrong_namespace() {
    let Some(redis) = start_managed_redis_or_skip().await else {
        return;
    };
    let runtime_state = redis_cache_runtime_state(
        redis.redis_url(),
        "pii_redaction_performance_wrong_namespace",
    )
    .await;
    let cache = RedisRedactionMappingCache::new(&runtime_state);
    let config = ChatPiiRedactionRuntimeConfig {
        placeholder_prefix: "SAFE".to_string(),
        ..email_only_runtime_config()
    };
    runtime_state
        .kv_set(
            &cache.forward_cache_key("EMAIL", "alice@example.com", 2),
            "<AETHER:EMAIL:ABCDEFGHIJKLMNOPQRST>",
            Some(Duration::from_secs(300)),
        )
        .await
        .expect("wrong namespace cache value should write");

    let request = json!({
        "model": "gpt-5",
        "messages": [{"role": "user", "content": "Contact alice@example.com"}]
    });
    let masked = try_mask_chat_request_json_with_cache_options(
        &serde_json::to_vec(&request).expect("request should serialize"),
        build_redaction_session_config(b"redaction-test-key".to_vec(), &config, 600),
        MaskChatRequestOptions::runtime(),
        Some(&cache),
    )
    .await
    .expect("masking should ignore wrong namespace cache value");

    let sentinel = masked
        .session
        .sentinel_for_original("alice@example.com")
        .expect("email sentinel should exist");
    assert!(sentinel.starts_with("<SAFE:EMAIL:"));
    assert_ne!(sentinel, "<AETHER:EMAIL:ABCDEFGHIJKLMNOPQRST>");
}

#[test]
fn pii_redaction_performance_restore_uses_large_mapping_matcher() {
    let mut session = session_at(600);
    for index in 0..32 {
        let original = format!("user{index}@example.com");
        let sentinel = format!("<AETHER:EMAIL:{index:0>20}>");
        let key = MappingKey {
            rule_label: "EMAIL".to_string(),
            original: original.clone(),
        };
        session.sentinel_index.insert(sentinel.clone(), key.clone());
        session.mappings.insert(
            key,
            RedactionMapping {
                rule_label: "EMAIL".to_string(),
                kind: Some(RedactionKind::Email),
                original,
                normalized_value: format!("user{index}@example.com"),
                sentinel,
                bucket: 2,
                created_at_unix_secs: 600,
                expires_at_unix_secs: 900,
            },
        );
    }
    assert!(matches!(
        SentinelMatcher::new(&session),
        SentinelMatcher::Direct(_)
    ));

    for index in 32..40 {
        let original = format!("user{index}@example.com");
        let sentinel = format!("<AETHER:EMAIL:{index:0>20}>");
        let key = MappingKey {
            rule_label: "EMAIL".to_string(),
            original: original.clone(),
        };
        session.sentinel_index.insert(sentinel.clone(), key.clone());
        session.mappings.insert(
            key,
            RedactionMapping {
                rule_label: "EMAIL".to_string(),
                kind: Some(RedactionKind::Email),
                original,
                normalized_value: format!("user{index}@example.com"),
                sentinel,
                bucket: 2,
                created_at_unix_secs: 600,
                expires_at_unix_secs: 900,
            },
        );
    }
    assert!(matches!(
        SentinelMatcher::new(&session),
        SentinelMatcher::Trie(_)
    ));

    let text = "before <AETHER:EMAIL:00000000000000000039> middle <AETHER:EMAIL:00000000000000000001> after";
    let restored = session
        .restore_text(text)
        .expect("restoration must succeed");
    assert!(restored.restored);
    assert_eq!(
        restored.text,
        "before user39@example.com middle user1@example.com after"
    );
}

#[test]
fn pii_redaction_performance_documents_segment_scan_bypass_deferral() {
    let source = include_str!("mod.rs");
    assert!(source.contains("Segment-level scan-bypass cache is intentionally deferred"));
}

fn test_config() -> RedactionSessionConfig {
    RedactionSessionConfig::new(b"redaction-test-key".to_vec(), 300, 600)
}

fn deepseek_opaque_replay_options() -> MaskChatRequestOptions {
    MaskChatRequestOptions::runtime().with_reasoning_replay_policy(
        crate::ai_serving::OpenAiResponsesReasoningReplayPolicy::DeepSeekOpaque,
    )
}
