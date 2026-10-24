use super::{
    build_pending_usage_event_from_owned_seed, build_pending_usage_record,
    build_pending_usage_record_from_seed, build_stream_terminal_usage_event,
    build_streaming_usage_event_from_owned_seed, build_streaming_usage_record,
    build_sync_terminal_usage_event, build_sync_terminal_usage_payload_seed,
    build_sync_terminal_usage_seed, build_terminal_usage_context_seed,
    build_terminal_usage_event_from_seed, build_usage_event_data_seed,
    build_usage_event_data_seed_describing_request_bodies, decode_body_for_storage,
    extract_token_counts_from_json, extract_token_counts_from_value, headers_to_json,
    mask_sensitive_body_fields, parse_sse_body_for_storage, resolve_error_message,
    validate_usage_headers_in_json_value, LifecycleUsageSeed, TerminalUsageSeed, UsageBodyRefsSeed,
    UsageBodyStatesSeed, UsageRoutingSeed, UsageTerminalState, MAX_USAGE_CAPTURE_BYTES,
    MAX_USAGE_CAPTURE_DEPTH,
};
use crate::{
    build_upsert_usage_record_from_event, GatewayStreamReportRequest, GatewaySyncReportRequest,
    UsageEventType,
};
use aether_contracts::{
    ExecutionPlan, ExecutionStreamTerminalSummary, RequestBody, StandardizedUsage,
};
use aether_data_contracts::repository::usage::UsageBodyCaptureState;
use base64::Engine as _;
use serde_json::{json, Value};
use std::collections::BTreeMap;

#[test]
fn extracts_openai_usage_tokens() {
    let tokens = extract_token_counts_from_json(&json!({
        "usage": {
            "input_tokens": 3,
            "output_tokens": 5,
            "total_tokens": 8
        }
    }))
    .expect("tokens should exist");

    assert_eq!(tokens, (3, 5, 8));
}

#[test]
fn extracts_openai_usage_tokens_with_cache_components() {
    let tokens = extract_token_counts_from_json(&json!({
        "usage": {
            "input_tokens": 3,
            "output_tokens": 5,
            "total_tokens": 8,
            "input_tokens_details": {
                "cached_tokens": 2,
                "cached_creation_tokens": 1
            }
        }
    }))
    .expect("tokens should exist");

    assert_eq!(tokens, (3, 5, 8));
}

#[test]
fn extracts_openai_usage_tokens_with_prompt_token_details() {
    let tokens = extract_token_counts_from_json(&json!({
        "usage": {
            "prompt_tokens": 3,
            "completion_tokens": 5,
            "total_tokens": 8,
            "prompt_tokens_details": {
                "cached_tokens": 2,
                "cached_creation_tokens": 1
            }
        }
    }))
    .expect("tokens should exist");

    assert_eq!(tokens, (3, 5, 8));
}

#[test]
fn extracts_claude_usage_tokens_with_cache_components() {
    let tokens = extract_token_counts_from_json(&json!({
        "usage": {
            "input_tokens": 6,
            "output_tokens": 20,
            "cache_creation_input_tokens": 41857,
            "cache_read_input_tokens": 0
        }
    }))
    .expect("tokens should exist");

    assert_eq!(tokens, (6, 20, 26));
}

#[test]
fn extracts_gemini_usage_tokens_without_adding_cached_content_twice() {
    let tokens = extract_token_counts_from_json(&json!({
        "usageMetadata": {
            "promptTokenCount": 14,
            "candidatesTokenCount": 6,
            "cachedContentTokenCount": 2,
            "totalTokenCount": 20
        }
    }))
    .expect("tokens should exist");

    assert_eq!(tokens, (14, 6, 20));
}

#[test]
fn extracts_usage_tokens_from_last_matching_chunk() {
    let tokens = extract_token_counts_from_json(&json!({
        "chunks": [
            {
                "response": {
                    "usage": {
                        "input_tokens": 1,
                        "output_tokens": 2,
                        "total_tokens": 3
                    }
                }
            },
            {
                "response": {
                    "usage": {
                        "input_tokens": 9,
                        "output_tokens": 4,
                        "total_tokens": 13,
                        "input_tokens_details": {
                            "cached_tokens": 5,
                            "cached_creation_tokens": 2
                        }
                    }
                }
            }
        ]
    }))
    .expect("tokens should exist");

    assert_eq!(tokens, (9, 4, 13));
}

#[test]
fn pending_usage_records_stay_lightweight() {
    let plan = ExecutionPlan {
        request_id: "req-pending-usage-1".to_string(),
        candidate_id: Some("cand-pending-usage-1".to_string()),
        provider_name: Some("Codex Proxy".to_string()),
        provider_id: "provider-1".to_string(),
        endpoint_id: "endpoint-1".to_string(),
        key_id: "key-1".to_string(),
        method: "POST".to_string(),
        url: "https://example.com/v1/messages".to_string(),
        headers: BTreeMap::from([(
            "authorization".to_string(),
            "Bearer pending-secret".to_string(),
        )]),
        content_type: Some("application/json".to_string()),
        content_encoding: None,
        body: RequestBody::from_json(json!({
            "model": "gpt-5.4",
            "messages": [{"role": "user", "content": "hello"}],
            "reasoning": {"effort": "max"}
        })),
        stream: false,
        client_api_format: "claude:messages".to_string(),
        provider_api_format: "openai:responses".to_string(),
        model_name: Some("gpt-5.4".to_string()),
        proxy: None,
        transport_profile: None,
        timeouts: None,
    };

    let record = build_pending_usage_record(
        &plan,
        Some(&json!({
            "candidate_id": "cand-pending-usage-1",
            "candidate_index": 0,
            "key_name": "codex-upstream",
            "planner_kind": "claude_cli_sync",
            "route_family": "claude",
            "route_kind": "cli",
            "execution_path": "local_execution_runtime_miss",
            "local_execution_runtime_miss_reason": "all_candidates_skipped",
            "original_headers": {"authorization": "Bearer upstream-secret"},
            "original_request_body": {"messages": [{"content": "should not be persisted in pending"}]},
            "provider_request_headers": {"authorization": "Bearer provider-secret"},
            "provider_request_body": {"input": "should not be persisted in pending"}
        })),
        1_700_000_000,
    )
    .expect("pending usage should build");

    assert_eq!(record.status, "pending");
    assert_eq!(record.billing_status, "pending");
    assert!(record.request_headers.is_none());
    assert!(record.request_body.is_none());
    assert!(record.provider_request_headers.is_none());
    assert!(record.provider_request_body.is_none());
    assert_eq!(record.provider_id.as_deref(), Some("provider-1"));
    assert_eq!(record.provider_endpoint_id.as_deref(), Some("endpoint-1"));
    assert_eq!(record.provider_api_key_id.as_deref(), Some("key-1"));
    assert_eq!(record.candidate_id.as_deref(), Some("cand-pending-usage-1"));
    assert_eq!(record.candidate_index, Some(0));
    assert_eq!(record.key_name.as_deref(), Some("codex-upstream"));
    assert_eq!(record.planner_kind.as_deref(), Some("claude_cli_sync"));
    assert_eq!(record.route_family.as_deref(), Some("claude"));
    assert_eq!(record.route_kind.as_deref(), Some("cli"));
    assert_eq!(
        record.execution_path.as_deref(),
        Some("local_execution_runtime_miss")
    );
    assert_eq!(
        record.local_execution_runtime_miss_reason.as_deref(),
        Some("all_candidates_skipped")
    );
    let metadata = record
        .request_metadata
        .as_ref()
        .and_then(Value::as_object)
        .expect("pending usage should only keep lightweight request metadata");
    assert_eq!(
        metadata.get("provider_reasoning_effort"),
        Some(&json!("max"))
    );
    let body_size = metadata
        .get("body_size")
        .and_then(Value::as_object)
        .expect("pending usage should keep request body size metadata");
    assert_eq!(
        body_size.get("basis"),
        Some(&json!(
            "serialized gateway request bodies after normalization"
        ))
    );
    assert!(body_size.get("client_request_body").is_some());
    assert!(body_size.get("provider_request_body").is_some());
    assert!(body_size.get("provider_over_client").is_some());
}

#[test]
fn pending_usage_event_round_trips_to_lightweight_upsert_record() {
    let plan = ExecutionPlan {
        request_id: "req-pending-event-1".to_string(),
        candidate_id: Some("cand-pending-event-1".to_string()),
        provider_name: Some("Codex Proxy".to_string()),
        provider_id: "provider-1".to_string(),
        endpoint_id: "endpoint-1".to_string(),
        key_id: "key-1".to_string(),
        method: "POST".to_string(),
        url: "https://example.com/v1/messages".to_string(),
        headers: BTreeMap::new(),
        content_type: Some("application/json".to_string()),
        content_encoding: None,
        body: RequestBody::from_json(json!({"model": "gpt-5.4"})),
        stream: false,
        client_api_format: "claude:messages".to_string(),
        provider_api_format: "openai:responses".to_string(),
        model_name: Some("gpt-5.4".to_string()),
        proxy: None,
        transport_profile: None,
        timeouts: None,
    };

    let event = build_pending_usage_event_from_owned_seed(
        super::build_lifecycle_usage_seed(
            &plan,
            Some(&json!({
                "candidate_id": "cand-pending-event-1",
                "candidate_index": 3,
                "routing_group_id": "group-free",
                "routing_group_name": "免费分组",
                "routing_group_billing_multiplier": 0.0,
                "billing_multiplier_snapshot": {
                    "version": 1,
                    "factors": {"routing_group": 0.0},
                    "multiplier": 0.0
                },
                "websocket_mode": true,
                "websocket_transport": "responses",
                "original_request_body": {"messages": [{"content": "omit me"}]},
                "provider_request_body": {
                    "input": [{"type": "compaction_trigger"}]
                }
            })),
        ),
        1_700_000_020,
    )
    .expect("pending usage event should build");
    let record = build_upsert_usage_record_from_event(&event).expect("record should build");

    assert_eq!(event.event_type, UsageEventType::Pending);
    assert_eq!(record.request_id, "req-pending-event-1");
    assert_eq!(record.status, "pending");
    assert_eq!(record.billing_status, "pending");
    assert_eq!(record.request_type.as_deref(), Some("compact"));
    assert_eq!(record.finalized_at_unix_secs, None);
    assert_eq!(record.updated_at_unix_secs, 1_700_000_020);
    assert!(record.request_body.is_none());
    assert!(record.provider_request_body.is_none());
    assert_eq!(record.candidate_id.as_deref(), Some("cand-pending-event-1"));
    assert_eq!(record.candidate_index, Some(3));
    let metadata = record.request_metadata.as_ref().expect("routing snapshot");
    assert_eq!(metadata["routing_group_id"], "group-free");
    assert_eq!(metadata["routing_group_name"], "免费分组");
    assert_eq!(metadata["routing_group_billing_multiplier"], 0.0);
    assert_eq!(
        aether_data_contracts::repository::usage::billing_multiplier_snapshot(Some(metadata))
            .unwrap()
            .unwrap()
            .multiplier(),
        0.0
    );
    assert_eq!(
        record
            .request_metadata
            .as_ref()
            .and_then(Value::as_object)
            .and_then(|metadata| metadata.get("websocket_mode")),
        Some(&json!(true))
    );
    assert_eq!(
        record
            .request_metadata
            .as_ref()
            .and_then(Value::as_object)
            .and_then(|metadata| metadata.get("websocket_transport")),
        Some(&json!("responses"))
    );
}

#[test]
fn streaming_usage_event_round_trips_to_lightweight_upsert_record() {
    let plan = ExecutionPlan {
        request_id: "req-streaming-event-1".to_string(),
        candidate_id: Some("cand-streaming-event-1".to_string()),
        provider_name: Some("Codex Proxy".to_string()),
        provider_id: "provider-1".to_string(),
        endpoint_id: "endpoint-1".to_string(),
        key_id: "key-1".to_string(),
        method: "POST".to_string(),
        url: "https://example.com/v1/messages".to_string(),
        headers: BTreeMap::new(),
        content_type: Some("application/json".to_string()),
        content_encoding: None,
        body: RequestBody::from_json(json!({"model": "gpt-5.4"})),
        stream: true,
        client_api_format: "claude:messages".to_string(),
        provider_api_format: "openai:responses".to_string(),
        model_name: Some("gpt-5.4".to_string()),
        proxy: None,
        transport_profile: None,
        timeouts: None,
    };

    let event = build_streaming_usage_event_from_owned_seed(
        super::build_lifecycle_usage_seed(
            &plan,
            Some(&json!({
                "candidate_id": "cand-streaming-event-1",
                "candidate_index": 4,
                "routing_group_id": "group-discount",
                "routing_group_name": "折扣分组",
                "routing_group_billing_multiplier": 0.5,
                "billing_multiplier_snapshot": {
                    "version": 1,
                    "factors": {"routing_group": 0.5},
                    "multiplier": 0.5
                },
                "provider_request_body": {"input": "omit me"}
            })),
        ),
        200,
        None,
        1_700_000_030,
    )
    .expect("streaming usage event should build");
    let record = build_upsert_usage_record_from_event(&event).expect("record should build");

    assert_eq!(event.event_type, UsageEventType::Streaming);
    assert_eq!(record.request_id, "req-streaming-event-1");
    assert_eq!(record.status, "streaming");
    assert_eq!(record.billing_status, "pending");
    assert_eq!(record.status_code, Some(200));
    assert_eq!(record.finalized_at_unix_secs, None);
    assert_eq!(record.updated_at_unix_secs, 1_700_000_030);
    assert!(record.request_body.is_none());
    assert!(record.provider_request_body.is_none());
    assert_eq!(
        record.candidate_id.as_deref(),
        Some("cand-streaming-event-1")
    );
    assert_eq!(record.candidate_index, Some(4));
    let metadata = record.request_metadata.as_ref().expect("routing snapshot");
    assert_eq!(metadata["routing_group_id"], "group-discount");
    assert_eq!(metadata["routing_group_name"], "折扣分组");
    assert_eq!(metadata["routing_group_billing_multiplier"], 0.5);
    assert_eq!(
        aether_data_contracts::repository::usage::billing_multiplier_snapshot(Some(metadata))
            .unwrap()
            .unwrap()
            .multiplier(),
        0.5
    );
}

#[test]
fn lifecycle_routing_snapshot_preserves_legacy_factors_and_invalid_markers() {
    use aether_data_contracts::repository::usage::billing_multiplier_snapshot;

    let plan = ExecutionPlan {
        request_id: "req-lifecycle-snapshot".to_string(),
        candidate_id: None,
        provider_name: Some("OpenAI".to_string()),
        provider_id: "provider-1".to_string(),
        endpoint_id: "endpoint-1".to_string(),
        key_id: "key-1".to_string(),
        method: "POST".to_string(),
        url: "https://example.com/v1/responses".to_string(),
        headers: BTreeMap::new(),
        content_type: Some("application/json".to_string()),
        content_encoding: None,
        body: RequestBody::from_json(json!({"model": "gpt-5.4"})),
        stream: true,
        client_api_format: "openai:responses".to_string(),
        provider_api_format: "openai:responses".to_string(),
        model_name: Some("gpt-5.4".to_string()),
        proxy: None,
        transport_profile: None,
        timeouts: None,
    };

    for (snapshot, expected_multiplier) in [
        (json!({"routing_group_billing_multiplier": 2.0}), Some(2.0)),
        (json!({"routing_group_billing_multiplier": -1}), None),
        (
            json!({
                "routing_group_billing_multiplier": 0.5,
                "billing_multiplier_snapshot": null
            }),
            None,
        ),
        (
            json!({
                "routing_group_billing_multiplier": 0.5,
                "billing_multiplier_snapshot": {
                    "version": 1,
                    "factors": {"routing_group": 2.0},
                    "multiplier": 1.0
                }
            }),
            None,
        ),
    ] {
        let mut context = snapshot;
        context["routing_group_id"] = json!("group-1");
        context["routing_group_name"] = json!("请求时分组");
        context["original_request_body"] = json!({"secret": "do not capture"});
        context["billing_snapshot"] = json!({"payload": "x".repeat(32 * 1024)});
        let seed = super::build_lifecycle_usage_seed(&plan, Some(&context));
        let pending_event =
            build_pending_usage_event_from_owned_seed(seed.clone(), 1_700_000_000).unwrap();
        let streaming_event =
            build_streaming_usage_event_from_owned_seed(seed.clone(), 200, None, 1_700_000_001)
                .unwrap();
        let records = [
            build_pending_usage_record(&plan, Some(&context), 1_700_000_000).unwrap(),
            build_streaming_usage_record(&plan, Some(&context), 200, None, 1_700_000_001).unwrap(),
            build_upsert_usage_record_from_event(&pending_event).unwrap(),
            build_upsert_usage_record_from_event(&streaming_event).unwrap(),
        ];

        for metadata in
            std::iter::once(seed.request_metadata.as_ref()).chain(records.iter().map(|record| {
                assert!(record.request_body.is_none());
                assert!(record.provider_request_body.is_none());
                record.request_metadata.as_ref()
            }))
        {
            let metadata = metadata.expect("lifecycle routing snapshot");
            assert_eq!(metadata["routing_group_id"], "group-1");
            assert_eq!(metadata["routing_group_name"], "请求时分组");
            assert!(metadata.get("original_request_body").is_none());
            assert!(metadata.get("billing_snapshot").is_none());
            if let Some(expected) = expected_multiplier {
                assert_eq!(
                    billing_multiplier_snapshot(Some(metadata))
                        .unwrap()
                        .unwrap()
                        .multiplier(),
                    expected
                );
            } else {
                assert_eq!(
                    metadata.get("billing_multiplier_snapshot"),
                    Some(&Value::Null)
                );
                assert!(billing_multiplier_snapshot(Some(metadata)).is_err());
            }
        }
    }
}

#[test]
fn pending_usage_record_preserves_standalone_key_metadata() {
    let plan = ExecutionPlan {
        request_id: "req-pending-standalone-1".to_string(),
        candidate_id: None,
        provider_name: Some("OpenAI".to_string()),
        provider_id: "provider-1".to_string(),
        endpoint_id: "endpoint-1".to_string(),
        key_id: "key-1".to_string(),
        method: "POST".to_string(),
        url: "https://example.com/v1/responses".to_string(),
        headers: BTreeMap::new(),
        content_type: Some("application/json".to_string()),
        content_encoding: None,
        body: RequestBody::from_json(json!({"model": "gpt-5.4"})),
        stream: false,
        client_api_format: "openai:responses".to_string(),
        provider_api_format: "openai:responses".to_string(),
        model_name: Some("gpt-5.4".to_string()),
        proxy: None,
        transport_profile: None,
        timeouts: None,
    };

    let record = build_pending_usage_record(
        &plan,
        Some(&json!({
            "api_key_is_standalone": true,
            "client_ip": "203.0.113.8",
            "user_agent": "Claude-Code/1.0"
        })),
        1_700_000_000,
    )
    .expect("pending usage should build");

    let metadata = record
        .request_metadata
        .as_ref()
        .and_then(Value::as_object)
        .expect("pending usage should keep request metadata");
    assert_eq!(metadata.get("api_key_is_standalone"), Some(&json!(true)));
    assert_eq!(metadata.get("client_ip"), Some(&json!("203.0.113.8")));
    assert_eq!(metadata.get("client_family"), Some(&json!("claude_code")));
    assert!(metadata.get("user_agent").is_none());
    let body_size = metadata
        .get("body_size")
        .and_then(Value::as_object)
        .expect("pending usage should keep request body size metadata");
    assert_eq!(
        body_size.get("basis"),
        Some(&json!(
            "serialized gateway request bodies after normalization"
        ))
    );
    assert!(body_size.get("provider_request_body").is_some());
    assert!(body_size.get("client_request_body").is_none());
    assert!(body_size.get("provider_over_client").is_none());
}

#[test]
fn streaming_usage_records_stay_lightweight_by_default() {
    let plan = ExecutionPlan {
        request_id: "req-streaming-usage-1".to_string(),
        candidate_id: Some("cand-streaming-usage-1".to_string()),
        provider_name: Some("Codex Proxy".to_string()),
        provider_id: "provider-1".to_string(),
        endpoint_id: "endpoint-1".to_string(),
        key_id: "key-1".to_string(),
        method: "POST".to_string(),
        url: "https://example.com/v1/messages".to_string(),
        headers: BTreeMap::new(),
        content_type: Some("application/json".to_string()),
        content_encoding: None,
        body: RequestBody::from_json(json!({"model": "gpt-5.4"})),
        stream: true,
        client_api_format: "claude:messages".to_string(),
        provider_api_format: "openai:responses".to_string(),
        model_name: Some("gpt-5.4".to_string()),
        proxy: None,
        transport_profile: None,
        timeouts: None,
    };

    let record = build_streaming_usage_record(
        &plan,
        Some(&json!({
            "candidate_id": "cand-streaming-usage-1",
            "original_request_body": {"messages": [{"content": "omit me"}]},
            "provider_request_body": {"input": "omit me too"}
        })),
        200,
        None,
        1_700_000_010,
    )
    .expect("streaming usage should build");

    assert_eq!(record.status, "streaming");
    assert_eq!(record.billing_status, "pending");
    assert_eq!(record.status_code, Some(200));
    assert!(record.request_headers.is_none());
    assert!(record.request_body.is_none());
    assert!(record.provider_request_headers.is_none());
    assert!(record.provider_request_body.is_none());
    assert!(record.response_headers.is_none());
    assert!(record.client_response_headers.is_none());
}

#[test]
fn sync_terminal_usage_preserves_overdeep_request_payloads_for_repo_body_storage() {
    const DEEP_NESTED_LEVELS: usize = MAX_USAGE_CAPTURE_DEPTH + 8;
    let mut nested = Value::String("leaf".to_string());
    for depth in 0..DEEP_NESTED_LEVELS {
        nested = json!({
            "depth": depth,
            "child": nested
        });
    }

    let plan = ExecutionPlan {
        request_id: "req-sync-usage-deep-1".to_string(),
        candidate_id: Some("cand-sync-usage-deep-1".to_string()),
        provider_name: Some("Codex Proxy".to_string()),
        provider_id: "provider-1".to_string(),
        endpoint_id: "endpoint-1".to_string(),
        key_id: "key-1".to_string(),
        method: "POST".to_string(),
        url: "https://example.com/v1/messages".to_string(),
        headers: BTreeMap::new(),
        content_type: Some("application/json".to_string()),
        content_encoding: None,
        body: RequestBody::from_json(json!({"model": "gpt-5.4"})),
        stream: false,
        client_api_format: "claude:messages".to_string(),
        provider_api_format: "openai:responses".to_string(),
        model_name: Some("gpt-5.4".to_string()),
        proxy: None,
        transport_profile: None,
        timeouts: None,
    };
    let payload = GatewaySyncReportRequest {
        trace_id: "trace-sync-usage-deep-1".to_string(),
        report_kind: "claude_cli_sync_success".to_string(),
        report_context: Some(json!({
            "client_api_format": "claude:messages",
            "provider_api_format": "openai:responses",
            "needs_conversion": true,
            "original_request_body": nested,
            "provider_request_body": {"input": "safe"}
        })),
        status_code: 200,
        headers: BTreeMap::new(),
        body_json: Some(json!({"id": "resp-1"})),
        body_base64: None,
        client_body_json: Some(json!({"type": "message"})),
        telemetry: None,
    };

    let event = build_sync_terminal_usage_event(&plan, payload.report_context.as_ref(), &payload)
        .expect("terminal usage should build");

    assert_eq!(
        event.data.request_body,
        payload
            .report_context
            .as_ref()
            .and_then(|value| value.get("original_request_body"))
            .cloned()
    );
    assert_eq!(
        event.data.provider_request_body,
        Some(json!({"input": "safe"}))
    );
}

#[test]
fn usage_body_capture_redacts_nested_provider_secrets() {
    let masked = mask_sensitive_body_fields(json!({
        "metadata": {
            "apiKey": "devin-session-token$secret-value",
            "nested": {
                "sessionToken": "session-token-secret"
            }
        },
        "password": "plain-password",
        "messages": [{"content": "safe text"}]
    }));

    assert_ne!(
        masked.pointer("/metadata/apiKey").and_then(Value::as_str),
        Some("devin-session-token$secret-value")
    );
    assert_ne!(
        masked
            .pointer("/metadata/nested/sessionToken")
            .and_then(Value::as_str),
        Some("session-token-secret")
    );
    assert_ne!(
        masked.get("password").and_then(Value::as_str),
        Some("plain-password")
    );
    assert_eq!(
        masked
            .pointer("/messages/0/content")
            .and_then(Value::as_str),
        Some("safe text")
    );
}

#[test]
fn usage_body_capture_preserves_token_limits_and_counts() {
    let masked = mask_sensitive_body_fields(json!({
        "max_token": 4096,
        "max_tokens": 128000,
        "max_completion_tokens": 128000,
        "max_output_tokens": 64000,
        "usage": {
            "prompt_tokens": 1,
            "completion_tokens": 2,
            "total_tokens": 3,
            "promptTokenCount": 4,
            "candidatesTokenCount": 5
        },
        "generationConfig": {
            "maxOutputTokens": 8192,
            "maxOutputToken": 4096
        },
        "metadata": {
            "access_token": "secret-access-token-value",
            "authToken": "secret-auth-token-value",
            "token": "secret-token-value"
        }
    }));

    assert_eq!(masked.get("max_token"), Some(&json!(4096)));
    assert_eq!(masked.get("max_tokens"), Some(&json!(128000)));
    assert_eq!(masked.get("max_completion_tokens"), Some(&json!(128000)));
    assert_eq!(masked.get("max_output_tokens"), Some(&json!(64000)));
    assert_eq!(masked.pointer("/usage/prompt_tokens"), Some(&json!(1)));
    assert_eq!(masked.pointer("/usage/completion_tokens"), Some(&json!(2)));
    assert_eq!(masked.pointer("/usage/total_tokens"), Some(&json!(3)));
    assert_eq!(masked.pointer("/usage/promptTokenCount"), Some(&json!(4)));
    assert_eq!(
        masked.pointer("/usage/candidatesTokenCount"),
        Some(&json!(5))
    );
    assert_eq!(
        masked.pointer("/generationConfig/maxOutputTokens"),
        Some(&json!(8192))
    );
    assert_eq!(
        masked.pointer("/generationConfig/maxOutputToken"),
        Some(&json!(4096))
    );
    assert_ne!(
        masked
            .pointer("/metadata/access_token")
            .and_then(Value::as_str),
        Some("secret-access-token-value")
    );
    assert_ne!(
        masked
            .pointer("/metadata/authToken")
            .and_then(Value::as_str),
        Some("secret-auth-token-value")
    );
    assert_ne!(
        masked.pointer("/metadata/token").and_then(Value::as_str),
        Some("secret-token-value")
    );
}

#[test]
fn sync_terminal_usage_redacts_provider_request_body_secrets_from_context() {
    let plan = ExecutionPlan {
        request_id: "req-sync-redact-provider-request-1".to_string(),
        candidate_id: Some("cand-sync-redact-provider-request-1".to_string()),
        provider_name: Some("Windsurf".to_string()),
        provider_id: "provider-windsurf".to_string(),
        endpoint_id: "endpoint-windsurf".to_string(),
        key_id: "key-windsurf".to_string(),
        method: "POST".to_string(),
        url: "https://server.codeium.com/exa.api_server_pb.ApiServerService/GetChatMessage"
            .to_string(),
        headers: BTreeMap::new(),
        content_type: Some("application/json".to_string()),
        content_encoding: None,
        body: RequestBody::from_json(json!({"model": "windsurf-model"})),
        stream: false,
        client_api_format: "openai:chat".to_string(),
        provider_api_format: "openai:chat".to_string(),
        model_name: Some("windsurf-model".to_string()),
        proxy: None,
        transport_profile: None,
        timeouts: None,
    };
    let payload = GatewaySyncReportRequest {
        trace_id: "trace-sync-redact-provider-request-1".to_string(),
        report_kind: "openai_chat_sync_success".to_string(),
        report_context: Some(json!({
            "client_api_format": "openai:chat",
            "provider_api_format": "openai:chat",
            "provider_request_body": {
                "metadata": {
                    "apiKey": "devin-session-token$abc",
                    "sessionToken": "session-token-secret"
                },
                "message": "safe prompt"
            }
        })),
        status_code: 200,
        headers: BTreeMap::new(),
        body_json: Some(json!({"id": "resp_1", "choices": []})),
        client_body_json: None,
        body_base64: None,
        telemetry: None,
    };

    let event = build_sync_terminal_usage_event(&plan, payload.report_context.as_ref(), &payload)
        .expect("terminal usage should build");
    let provider_request = event
        .data
        .provider_request_body
        .as_ref()
        .expect("provider request body should be captured");

    assert_ne!(
        provider_request
            .pointer("/metadata/apiKey")
            .and_then(Value::as_str),
        Some("devin-session-token$abc")
    );
    assert_ne!(
        provider_request
            .pointer("/metadata/sessionToken")
            .and_then(Value::as_str),
        Some("session-token-secret")
    );
    assert_eq!(
        provider_request.pointer("/message").and_then(Value::as_str),
        Some("safe prompt")
    );
}

#[test]
fn builds_stream_terminal_usage_from_provider_body_and_preserves_client_body() {
    let plan = ExecutionPlan {
        request_id: "req-stream-usage-1".to_string(),
        candidate_id: Some("cand-stream-usage-1".to_string()),
        provider_name: Some("OpenAI".to_string()),
        provider_id: "provider-1".to_string(),
        endpoint_id: "endpoint-1".to_string(),
        key_id: "key-1".to_string(),
        method: "POST".to_string(),
        url: "https://example.com/v1/responses".to_string(),
        headers: BTreeMap::new(),
        content_type: None,
        content_encoding: None,
        body: RequestBody {
            json_body: None,
            body_bytes_b64: None,
            body_ref: None,
        },
        stream: true,
        client_api_format: "openai:chat".to_string(),
        provider_api_format: "openai:responses".to_string(),
        model_name: Some("gpt-5.4".to_string()),
        proxy: None,
        transport_profile: None,
        timeouts: None,
    };
    let payload = GatewayStreamReportRequest {
        trace_id: "trace-stream-usage-1".to_string(),
        report_kind: "openai_chat_stream_success".to_string(),
        report_context: Some(json!({
            "client_api_format": "openai:chat",
            "provider_api_format": "openai:responses",
            "needs_conversion": true
        })),
        status_code: 200,
        headers: BTreeMap::new(),
        provider_body_base64: Some(
            base64::engine::general_purpose::STANDARD.encode(
                serde_json::to_vec(&json!({
                    "usage": {
                        "prompt_tokens": 3,
                        "completion_tokens": 5,
                        "total_tokens": 8
                    }
                }))
                .expect("provider body should encode"),
            ),
        ),
        provider_body_state: Some(UsageBodyCaptureState::Inline),
        client_body_base64: Some(
            base64::engine::general_purpose::STANDARD
                .encode("data: {\"id\":\"chatcmpl_123\"}\n\ndata: [DONE]\n"),
        ),
        client_body_state: Some(UsageBodyCaptureState::Inline),
        terminal_summary: None,
        telemetry: None,
    };

    let event = build_stream_terminal_usage_event(&plan, payload.report_context.as_ref(), &payload)
        .expect("usage event should build");

    assert_eq!(event.data.input_tokens, Some(3));
    assert_eq!(event.data.output_tokens, Some(5));
    assert_eq!(event.data.total_tokens, Some(8));
    assert_eq!(
        event.data.response_body,
        Some(json!({
            "usage": {
                "prompt_tokens": 3,
                "completion_tokens": 5,
                "total_tokens": 8
            }
        }))
    );
    assert_eq!(
        event.data.client_response_body,
        Some(json!({
            "chunks": [
                {
                    "id": "chatcmpl_123"
                }
            ],
            "metadata": {
                "stream": true,
                "total_chunks": 1,
                "stored_chunks": 1,
                "content_length": 42,
                "has_completion": true
            }
        }))
    );
}

#[test]
fn cancelled_stream_usage_does_not_estimate_tokens_from_request_or_partial_response() {
    let plan = ExecutionPlan {
        request_id: "req-stream-cancelled-estimated-usage-1".to_string(),
        candidate_id: Some("cand-stream-cancelled-estimated-usage-1".to_string()),
        provider_name: Some("OpenAI".to_string()),
        provider_id: "provider-1".to_string(),
        endpoint_id: "endpoint-1".to_string(),
        key_id: "key-1".to_string(),
        method: "POST".to_string(),
        url: "https://example.com/v1/responses".to_string(),
        headers: BTreeMap::new(),
        content_type: Some("application/json".to_string()),
        content_encoding: None,
        body: RequestBody::from_json(json!({
            "model": "gpt-5.4",
            "input": "Write a short greeting for a usage test",
            "stream": true
        })),
        stream: true,
        client_api_format: "openai:responses".to_string(),
        provider_api_format: "openai:responses".to_string(),
        model_name: Some("gpt-5.4".to_string()),
        proxy: None,
        transport_profile: None,
        timeouts: None,
    };
    let sse_body = concat!(
        "event: response.created\n",
        "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_cancel_123\",\"model\":\"gpt-5.4\"}}\n\n",
        "event: response.output_text.delta\n",
        "data: {\"type\":\"response.output_text.delta\",\"delta\":\"Hello there friend\"}\n\n"
    );
    let payload = GatewayStreamReportRequest {
        trace_id: "trace-stream-cancelled-estimated-usage-1".to_string(),
        report_kind: "openai_responses_stream_cancelled".to_string(),
        report_context: Some(json!({
            "client_api_format": "openai:responses",
            "provider_api_format": "openai:responses",
            "provider_request_body": {
                "model": "gpt-5.4",
                "input": "Write a short greeting for a usage test",
                "stream": true
            }
        })),
        status_code: 499,
        headers: BTreeMap::new(),
        provider_body_base64: Some(base64::engine::general_purpose::STANDARD.encode(sse_body)),
        provider_body_state: Some(UsageBodyCaptureState::Inline),
        client_body_base64: None,
        client_body_state: Some(UsageBodyCaptureState::None),
        terminal_summary: None,
        telemetry: None,
    };

    let event = build_stream_terminal_usage_event(&plan, payload.report_context.as_ref(), &payload)
        .expect("usage event should build");

    assert_eq!(event.event_type, UsageEventType::Cancelled);
    assert_eq!(event.data.input_tokens, None);
    assert_eq!(event.data.output_tokens, None);
    assert_eq!(event.data.total_tokens, None);
    assert_eq!(event.data.cache_read_input_tokens, None);
}

#[test]
fn cancelled_stream_usage_does_not_infer_cache_or_token_estimates_from_prompt_cache_key() {
    let request_body = json!({
        "model": "gpt-5.4",
        "input": "Use the cached project context and answer briefly",
        "prompt_cache_key": "prompt-session-1",
        "stream": true
    });
    let plan = ExecutionPlan {
        request_id: "req-stream-cancelled-cache-estimated-usage-1".to_string(),
        candidate_id: Some("cand-stream-cancelled-cache-estimated-usage-1".to_string()),
        provider_name: Some("OpenAI".to_string()),
        provider_id: "provider-1".to_string(),
        endpoint_id: "endpoint-1".to_string(),
        key_id: "key-1".to_string(),
        method: "POST".to_string(),
        url: "https://example.com/v1/responses".to_string(),
        headers: BTreeMap::new(),
        content_type: Some("application/json".to_string()),
        content_encoding: None,
        body: RequestBody::from_json(request_body.clone()),
        stream: true,
        client_api_format: "openai:responses".to_string(),
        provider_api_format: "openai:responses".to_string(),
        model_name: Some("gpt-5.4".to_string()),
        proxy: None,
        transport_profile: None,
        timeouts: None,
    };
    let payload = GatewayStreamReportRequest {
        trace_id: "trace-stream-cancelled-cache-estimated-usage-1".to_string(),
        report_kind: "openai_responses_stream_cancelled".to_string(),
        report_context: Some(json!({
            "client_api_format": "openai:responses",
            "provider_api_format": "openai:responses",
            "provider_request_body": request_body
        })),
        status_code: 499,
        headers: BTreeMap::new(),
        provider_body_base64: Some(base64::engine::general_purpose::STANDARD.encode(
            "data: {\"type\":\"response.output_text.delta\",\"delta\":\"cached answer\"}\n\n",
        )),
        provider_body_state: Some(UsageBodyCaptureState::Inline),
        client_body_base64: None,
        client_body_state: Some(UsageBodyCaptureState::None),
        terminal_summary: None,
        telemetry: None,
    };

    let event = build_stream_terminal_usage_event(&plan, payload.report_context.as_ref(), &payload)
        .expect("usage event should build");

    assert_eq!(event.event_type, UsageEventType::Cancelled);
    assert_eq!(event.data.input_tokens, None);
    assert_eq!(event.data.output_tokens, None);
    assert_eq!(event.data.total_tokens, None);
    assert_eq!(event.data.cache_read_input_tokens, None);
}

#[test]
fn cancelled_stream_usage_preserves_terminal_summary_usage() {
    let plan = ExecutionPlan {
        request_id: "req-stream-cancelled-summary-usage-1".to_string(),
        candidate_id: Some("cand-stream-cancelled-summary-usage-1".to_string()),
        provider_name: Some("OpenAI".to_string()),
        provider_id: "provider-1".to_string(),
        endpoint_id: "endpoint-1".to_string(),
        key_id: "key-1".to_string(),
        method: "POST".to_string(),
        url: "https://example.com/v1/responses".to_string(),
        headers: BTreeMap::new(),
        content_type: Some("application/json".to_string()),
        content_encoding: None,
        body: RequestBody::from_json(json!({
            "model": "gpt-5.4",
            "input": "This cancelled request has terminal upstream usage",
            "stream": true
        })),
        stream: true,
        client_api_format: "openai:responses".to_string(),
        provider_api_format: "openai:responses".to_string(),
        model_name: Some("gpt-5.4".to_string()),
        proxy: None,
        transport_profile: None,
        timeouts: None,
    };
    let mut standardized_usage = StandardizedUsage::new();
    standardized_usage.input_tokens = 13;
    standardized_usage.output_tokens = 21;
    standardized_usage.cache_creation_tokens = 2;
    standardized_usage.cache_read_tokens = 3;
    let payload = GatewayStreamReportRequest {
        trace_id: "trace-stream-cancelled-summary-usage-1".to_string(),
        report_kind: "openai_responses_stream_cancelled".to_string(),
        report_context: Some(json!({
            "client_api_format": "openai:responses",
            "provider_api_format": "openai:responses"
        })),
        status_code: 499,
        headers: BTreeMap::new(),
        provider_body_base64: None,
        provider_body_state: Some(UsageBodyCaptureState::None),
        client_body_base64: None,
        client_body_state: Some(UsageBodyCaptureState::None),
        terminal_summary: Some(ExecutionStreamTerminalSummary {
            standardized_usage: Some(standardized_usage),
            finish_reason: None,
            response_id: Some("resp_cancel_summary_1".to_string()),
            model: Some("gpt-5.4".to_string()),
            provider_actual_service_tier: None,
            observed_finish: true,
            unknown_event_count: 0,
            parser_error: None,
        }),
        telemetry: None,
    };

    let event = build_stream_terminal_usage_event(&plan, payload.report_context.as_ref(), &payload)
        .expect("usage event should build");

    assert_eq!(event.event_type, UsageEventType::Cancelled);
    assert_eq!(event.data.input_tokens, Some(13));
    assert_eq!(event.data.output_tokens, Some(21));
    assert_eq!(event.data.total_tokens, Some(34));
    assert_eq!(event.data.cache_creation_input_tokens, Some(2));
    assert_eq!(event.data.cache_read_input_tokens, Some(3));
}

#[test]
fn failed_stream_usage_does_not_estimate_partial_response_tokens() {
    let plan = ExecutionPlan {
        request_id: "req-stream-failed-estimated-usage-1".to_string(),
        candidate_id: Some("cand-stream-failed-estimated-usage-1".to_string()),
        provider_name: Some("OpenAI".to_string()),
        provider_id: "provider-1".to_string(),
        endpoint_id: "endpoint-1".to_string(),
        key_id: "key-1".to_string(),
        method: "POST".to_string(),
        url: "https://example.com/v1/responses".to_string(),
        headers: BTreeMap::new(),
        content_type: Some("application/json".to_string()),
        content_encoding: None,
        body: RequestBody::from_json(json!({
            "model": "gpt-5.4",
            "input": "This failed request should not be billed",
            "stream": true
        })),
        stream: true,
        client_api_format: "openai:responses".to_string(),
        provider_api_format: "openai:responses".to_string(),
        model_name: Some("gpt-5.4".to_string()),
        proxy: None,
        transport_profile: None,
        timeouts: None,
    };
    let payload = GatewayStreamReportRequest {
        trace_id: "trace-stream-failed-estimated-usage-1".to_string(),
        report_kind: "openai_responses_stream_failed".to_string(),
        report_context: Some(json!({
            "client_api_format": "openai:responses",
            "provider_api_format": "openai:responses",
            "provider_request_body": {
                "model": "gpt-5.4",
                "input": "This failed request should not be billed",
                "stream": true
            }
        })),
        status_code: 500,
        headers: BTreeMap::new(),
        provider_body_base64: Some(base64::engine::general_purpose::STANDARD.encode(
            "data: {\"type\":\"response.output_text.delta\",\"delta\":\"partial text\"}\n\n",
        )),
        provider_body_state: Some(UsageBodyCaptureState::Inline),
        client_body_base64: None,
        client_body_state: Some(UsageBodyCaptureState::None),
        terminal_summary: None,
        telemetry: None,
    };

    let event = build_stream_terminal_usage_event(&plan, payload.report_context.as_ref(), &payload)
        .expect("usage event should build");

    assert_eq!(event.event_type, UsageEventType::Failed);
    assert_eq!(event.data.input_tokens, None);
    assert_eq!(event.data.output_tokens, None);
    assert_eq!(event.data.total_tokens, None);
}

#[test]
fn stream_terminal_usage_marks_redirect_status_as_failed() {
    let plan = ExecutionPlan {
        request_id: "req-stream-redirect-usage".to_string(),
        candidate_id: Some("cand-stream-redirect-usage".to_string()),
        provider_name: Some("ChatGPTWeb".to_string()),
        provider_id: "provider-redirect".to_string(),
        endpoint_id: "endpoint-redirect".to_string(),
        key_id: "key-redirect".to_string(),
        method: "POST".to_string(),
        url: "https://example.com/v1beta/models/gemini:streamGenerateContent".to_string(),
        headers: BTreeMap::new(),
        content_type: None,
        content_encoding: None,
        body: RequestBody {
            json_body: None,
            body_bytes_b64: None,
            body_ref: None,
        },
        stream: true,
        client_api_format: "gemini:generate_content".to_string(),
        provider_api_format: "gemini:generate_content".to_string(),
        model_name: Some("gemini".to_string()),
        proxy: None,
        transport_profile: None,
        timeouts: None,
    };
    let client_body = json!({
        "error": {
            "type": "execution_runtime_non_success_status",
            "message": "execution runtime stream returned non-success status 302",
            "code": 302,
            "upstream_status": 302,
            "location": "/"
        }
    });
    let payload = GatewayStreamReportRequest {
        trace_id: "trace-stream-redirect-usage".to_string(),
        report_kind: "gemini_chat_stream_success".to_string(),
        report_context: Some(json!({
            "client_api_format": "gemini:generate_content",
            "provider_api_format": "gemini:generate_content"
        })),
        status_code: 302,
        headers: BTreeMap::from([
            ("content-type".to_string(), "application/json".to_string()),
            ("x-aether-upstream-status".to_string(), "302".to_string()),
        ]),
        provider_body_base64: Some(
            base64::engine::general_purpose::STANDARD
                .encode(br#"{"error":{"message":"raw redirect body"}}"#),
        ),
        provider_body_state: Some(UsageBodyCaptureState::Inline),
        client_body_base64: Some(
            base64::engine::general_purpose::STANDARD
                .encode(serde_json::to_vec(&client_body).expect("body should encode")),
        ),
        client_body_state: Some(UsageBodyCaptureState::Inline),
        terminal_summary: None,
        telemetry: None,
    };

    let event = build_stream_terminal_usage_event(&plan, payload.report_context.as_ref(), &payload)
        .expect("usage event should build");

    assert_eq!(event.event_type, UsageEventType::Failed);
    assert_eq!(event.data.status_code, Some(302));
    assert_eq!(event.data.error_category.as_deref(), Some("redirect"));
    assert_eq!(
        event.data.error_message.as_deref(),
        Some("raw redirect body")
    );
}

#[test]
fn builds_stream_terminal_usage_from_terminal_summary_usage_without_decoding_bodies() {
    let plan = ExecutionPlan {
        request_id: "req-stream-summary-usage-1".to_string(),
        candidate_id: Some("cand-stream-summary-usage-1".to_string()),
        provider_name: Some("OpenAI".to_string()),
        provider_id: "provider-1".to_string(),
        endpoint_id: "endpoint-1".to_string(),
        key_id: "key-1".to_string(),
        method: "POST".to_string(),
        url: "https://example.com/v1/responses".to_string(),
        headers: BTreeMap::new(),
        content_type: None,
        content_encoding: None,
        body: RequestBody {
            json_body: None,
            body_bytes_b64: None,
            body_ref: None,
        },
        stream: true,
        client_api_format: "openai:chat".to_string(),
        provider_api_format: "openai:responses".to_string(),
        model_name: Some("gpt-5.4".to_string()),
        proxy: None,
        transport_profile: None,
        timeouts: None,
    };
    let mut standardized_usage = StandardizedUsage::new();
    standardized_usage.input_tokens = 13;
    standardized_usage.output_tokens = 21;
    standardized_usage.cache_creation_tokens = 2;
    standardized_usage.cache_read_tokens = 3;
    let payload = GatewayStreamReportRequest {
        trace_id: "trace-stream-summary-usage-1".to_string(),
        report_kind: "openai_chat_stream_success".to_string(),
        report_context: Some(json!({
            "client_api_format": "openai:chat",
            "provider_api_format": "openai:responses",
            "needs_conversion": true
        })),
        status_code: 200,
        headers: BTreeMap::new(),
        provider_body_base64: None,
        provider_body_state: Some(UsageBodyCaptureState::None),
        client_body_base64: None,
        client_body_state: Some(UsageBodyCaptureState::None),
        terminal_summary: Some(ExecutionStreamTerminalSummary {
            standardized_usage: Some(standardized_usage),
            finish_reason: Some("stop".to_string()),
            response_id: Some("resp_summary_1".to_string()),
            model: Some("gpt-5.4".to_string()),
            provider_actual_service_tier: Some("Default".to_string()),
            observed_finish: true,
            unknown_event_count: 0,
            parser_error: None,
        }),
        telemetry: None,
    };

    let event = build_stream_terminal_usage_event(&plan, payload.report_context.as_ref(), &payload)
        .expect("usage event should build");

    assert_eq!(event.data.input_tokens, Some(13));
    assert_eq!(event.data.output_tokens, Some(21));
    assert_eq!(event.data.total_tokens, Some(34));
    assert_eq!(event.data.cache_creation_input_tokens, Some(2));
    assert_eq!(event.data.cache_read_input_tokens, Some(3));
    assert!(event.data.response_body.is_none());
    assert!(event.data.client_response_body.is_none());
    assert_eq!(
        event
            .data
            .request_metadata
            .as_ref()
            .and_then(|metadata| metadata.get("provider_actual_service_tier"))
            .and_then(Value::as_str),
        Some("default")
    );
}

#[test]
fn stream_terminal_usage_marks_missing_observed_finish_as_failed() {
    let plan = ExecutionPlan {
        request_id: "req-stream-missing-finish-1".to_string(),
        candidate_id: Some("cand-stream-missing-finish-1".to_string()),
        provider_name: Some("OpenAI".to_string()),
        provider_id: "provider-1".to_string(),
        endpoint_id: "endpoint-1".to_string(),
        key_id: "key-1".to_string(),
        method: "POST".to_string(),
        url: "https://example.com/v1/responses".to_string(),
        headers: BTreeMap::new(),
        content_type: None,
        content_encoding: None,
        body: RequestBody {
            json_body: None,
            body_bytes_b64: None,
            body_ref: None,
        },
        stream: true,
        client_api_format: "openai:responses".to_string(),
        provider_api_format: "openai:responses".to_string(),
        model_name: Some("gpt-5.5".to_string()),
        proxy: None,
        transport_profile: None,
        timeouts: None,
    };
    let payload = GatewayStreamReportRequest {
        trace_id: "trace-stream-missing-finish-1".to_string(),
        report_kind: "openai_responses_stream_success".to_string(),
        report_context: Some(json!({
            "client_api_format": "openai:responses",
            "provider_api_format": "openai:responses"
        })),
        status_code: 200,
        headers: BTreeMap::new(),
        provider_body_base64: None,
        provider_body_state: Some(UsageBodyCaptureState::None),
        client_body_base64: None,
        client_body_state: Some(UsageBodyCaptureState::None),
        terminal_summary: Some(ExecutionStreamTerminalSummary {
            response_id: Some("resp_missing_finish".to_string()),
            model: Some("gpt-5.5".to_string()),
            observed_finish: false,
            ..ExecutionStreamTerminalSummary::default()
        }),
        telemetry: None,
    };

    let event = build_stream_terminal_usage_event(&plan, payload.report_context.as_ref(), &payload)
        .expect("usage event should build");

    assert_eq!(event.event_type, UsageEventType::Failed);
    assert_eq!(event.data.status_code, Some(200));
    assert_eq!(
        event.data.error_category.as_deref(),
        Some("stream_missing_terminal_event")
    );
    assert_eq!(
        event.data.error_message.as_deref(),
        Some("execution runtime stream ended before provider terminal event")
    );
    assert_eq!(event.data.input_tokens, None);
    assert_eq!(event.data.output_tokens, None);
}

#[test]
fn stream_terminal_usage_marks_empty_openai_responses_capture_as_missing_terminal() {
    let plan = ExecutionPlan {
        request_id: "req-stream-empty-capture-1".to_string(),
        candidate_id: Some("cand-stream-empty-capture-1".to_string()),
        provider_name: Some("OpenAI".to_string()),
        provider_id: "provider-1".to_string(),
        endpoint_id: "endpoint-1".to_string(),
        key_id: "key-1".to_string(),
        method: "POST".to_string(),
        url: "https://example.com/v1/responses".to_string(),
        headers: BTreeMap::new(),
        content_type: None,
        content_encoding: None,
        body: RequestBody {
            json_body: None,
            body_bytes_b64: None,
            body_ref: None,
        },
        stream: true,
        client_api_format: "openai:responses".to_string(),
        provider_api_format: "openai:responses".to_string(),
        model_name: Some("gpt-5.5".to_string()),
        proxy: None,
        transport_profile: None,
        timeouts: None,
    };
    let payload = GatewayStreamReportRequest {
        trace_id: "trace-stream-empty-capture-1".to_string(),
        report_kind: "openai_responses_stream_success".to_string(),
        report_context: Some(json!({
            "client_api_format": "openai:responses",
            "provider_api_format": "openai:responses"
        })),
        status_code: 200,
        headers: BTreeMap::new(),
        provider_body_base64: None,
        provider_body_state: Some(UsageBodyCaptureState::None),
        client_body_base64: None,
        client_body_state: Some(UsageBodyCaptureState::None),
        terminal_summary: None,
        telemetry: None,
    };

    let event = build_stream_terminal_usage_event(&plan, payload.report_context.as_ref(), &payload)
        .expect("usage event should build");

    assert_eq!(event.event_type, UsageEventType::Failed);
    assert_eq!(event.data.status_code, Some(200));
    assert_eq!(
        event.data.error_category.as_deref(),
        Some("stream_missing_terminal_event")
    );
    assert_eq!(
        event.data.error_message.as_deref(),
        Some("execution runtime stream ended before provider terminal event")
    );
    assert_eq!(
        event
            .data
            .client_response_body
            .as_ref()
            .and_then(|body| body.get("error"))
            .and_then(|error| error.get("type"))
            .and_then(Value::as_str),
        Some("stream_missing_terminal_event")
    );
    assert_eq!(
        event.data.client_response_body_state,
        Some(UsageBodyCaptureState::Inline)
    );
}

#[test]
fn stream_terminal_usage_marks_missing_captured_openai_responses_terminal_as_failed() {
    let plan = ExecutionPlan {
        request_id: "req-stream-missing-captured-finish-1".to_string(),
        candidate_id: Some("cand-stream-missing-captured-finish-1".to_string()),
        provider_name: Some("OpenAI".to_string()),
        provider_id: "provider-1".to_string(),
        endpoint_id: "endpoint-1".to_string(),
        key_id: "key-1".to_string(),
        method: "POST".to_string(),
        url: "https://example.com/v1/responses".to_string(),
        headers: BTreeMap::new(),
        content_type: None,
        content_encoding: None,
        body: RequestBody {
            json_body: None,
            body_bytes_b64: None,
            body_ref: None,
        },
        stream: true,
        client_api_format: "openai:responses".to_string(),
        provider_api_format: "openai:responses".to_string(),
        model_name: Some("gpt-5.5".to_string()),
        proxy: None,
        transport_profile: None,
        timeouts: None,
    };
    let provider_sse = concat!(
        "event: response.created\n",
        "data: {\"type\":\"response.created\"}\n\n",
        "event: response.output_text.delta\n",
        "data: {\"type\":\"response.output_text.delta\",\"delta\":\"hello\"}\n\n"
    );
    let payload = GatewayStreamReportRequest {
        trace_id: "trace-stream-missing-captured-finish-1".to_string(),
        report_kind: "openai_responses_stream_success".to_string(),
        report_context: Some(json!({
            "client_api_format": "openai:responses",
            "provider_api_format": "openai:responses"
        })),
        status_code: 200,
        headers: BTreeMap::from([("content-type".to_string(), "text/event-stream".to_string())]),
        provider_body_base64: Some(
            base64::engine::general_purpose::STANDARD.encode(provider_sse.as_bytes()),
        ),
        provider_body_state: Some(UsageBodyCaptureState::Inline),
        client_body_base64: None,
        client_body_state: Some(UsageBodyCaptureState::None),
        terminal_summary: None,
        telemetry: None,
    };

    let event = build_stream_terminal_usage_event(&plan, payload.report_context.as_ref(), &payload)
        .expect("usage event should build");

    assert_eq!(event.event_type, UsageEventType::Failed);
    assert_eq!(event.data.status_code, Some(200));
    assert_eq!(
        event.data.error_category.as_deref(),
        Some("stream_missing_terminal_event")
    );
    assert_eq!(
        event.data.error_message.as_deref(),
        Some("execution runtime stream ended before provider terminal event")
    );
}

#[test]
fn stream_terminal_usage_ignores_missing_terminal_from_truncated_capture() {
    let plan = ExecutionPlan {
        request_id: "req-stream-truncated-captured-finish-1".to_string(),
        candidate_id: Some("cand-stream-truncated-captured-finish-1".to_string()),
        provider_name: Some("OpenAI".to_string()),
        provider_id: "provider-1".to_string(),
        endpoint_id: "endpoint-1".to_string(),
        key_id: "key-1".to_string(),
        method: "POST".to_string(),
        url: "https://example.com/v1/responses".to_string(),
        headers: BTreeMap::new(),
        content_type: None,
        content_encoding: None,
        body: RequestBody {
            json_body: None,
            body_bytes_b64: None,
            body_ref: None,
        },
        stream: true,
        client_api_format: "openai:responses".to_string(),
        provider_api_format: "openai:responses".to_string(),
        model_name: Some("gpt-5.5".to_string()),
        proxy: None,
        transport_profile: None,
        timeouts: None,
    };
    let provider_sse = concat!(
        "event: response.created\n",
        "data: {\"type\":\"response.created\"}\n\n",
        "event: response.output_text.delta\n",
        "data: {\"type\":\"response.output_text.delta\",\"delta\":\"hello\"}\n\n"
    );
    let payload = GatewayStreamReportRequest {
        trace_id: "trace-stream-truncated-captured-finish-1".to_string(),
        report_kind: "openai_responses_stream_success".to_string(),
        report_context: Some(json!({
            "client_api_format": "openai:responses",
            "provider_api_format": "openai:responses"
        })),
        status_code: 200,
        headers: BTreeMap::from([("content-type".to_string(), "text/event-stream".to_string())]),
        provider_body_base64: Some(
            base64::engine::general_purpose::STANDARD.encode(provider_sse.as_bytes()),
        ),
        provider_body_state: Some(UsageBodyCaptureState::Truncated),
        client_body_base64: None,
        client_body_state: Some(UsageBodyCaptureState::None),
        terminal_summary: None,
        telemetry: None,
    };

    let event = build_stream_terminal_usage_event(&plan, payload.report_context.as_ref(), &payload)
        .expect("usage event should build");

    assert_eq!(event.event_type, UsageEventType::Completed);
    assert_eq!(event.data.status_code, Some(200));
    assert_eq!(event.data.error_category, None);
    assert_eq!(event.data.error_message, None);
}

#[test]
fn stream_terminal_usage_marks_http_200_response_failed_as_stream_terminal_error() {
    let plan = ExecutionPlan {
        request_id: "req-stream-response-failed-1".to_string(),
        candidate_id: Some("cand-stream-response-failed-1".to_string()),
        provider_name: Some("OpenAI".to_string()),
        provider_id: "provider-1".to_string(),
        endpoint_id: "endpoint-1".to_string(),
        key_id: "key-1".to_string(),
        method: "POST".to_string(),
        url: "https://example.com/v1/responses".to_string(),
        headers: BTreeMap::new(),
        content_type: None,
        content_encoding: None,
        body: RequestBody {
            json_body: None,
            body_bytes_b64: None,
            body_ref: None,
        },
        stream: true,
        client_api_format: "openai:responses".to_string(),
        provider_api_format: "openai:responses".to_string(),
        model_name: Some("gpt-5.5".to_string()),
        proxy: None,
        transport_profile: None,
        timeouts: None,
    };
    let message = "This content was flagged for possible cybersecurity risk";
    let provider_sse = format!(
        concat!(
            "event: response.failed\n",
            "data: {{\"type\":\"response.failed\",\"response\":{{\"status\":\"failed\",\"error\":{{\"message\":\"{}\",\"code\":\"cyber_policy\"}}}}}}\n\n"
        ),
        message
    );
    let payload = GatewayStreamReportRequest {
        trace_id: "trace-stream-response-failed-1".to_string(),
        report_kind: "openai_responses_stream_success".to_string(),
        report_context: Some(json!({
            "client_api_format": "openai:responses",
            "provider_api_format": "openai:responses"
        })),
        status_code: 200,
        headers: BTreeMap::from([("content-type".to_string(), "text/event-stream".to_string())]),
        provider_body_base64: Some(base64::engine::general_purpose::STANDARD.encode(provider_sse)),
        provider_body_state: Some(UsageBodyCaptureState::Inline),
        client_body_base64: None,
        client_body_state: Some(UsageBodyCaptureState::None),
        terminal_summary: Some(ExecutionStreamTerminalSummary {
            response_id: Some("resp_failed".to_string()),
            model: Some("gpt-5.5".to_string()),
            observed_finish: true,
            parser_error: Some(message.to_string()),
            ..ExecutionStreamTerminalSummary::default()
        }),
        telemetry: None,
    };

    let event = build_stream_terminal_usage_event(&plan, payload.report_context.as_ref(), &payload)
        .expect("usage event should build");

    assert_eq!(event.event_type, UsageEventType::Failed);
    assert_eq!(event.data.status_code, Some(200));
    assert_eq!(event.data.error_message.as_deref(), Some(message));
    assert_eq!(
        event.data.error_category.as_deref(),
        Some("stream_terminal_error")
    );
    assert_eq!(
        event
            .data
            .client_response_body
            .as_ref()
            .and_then(|body| body.get("error"))
            .and_then(|error| error.get("code"))
            .and_then(Value::as_str),
        Some("cyber_policy")
    );
    assert_eq!(
        event
            .data
            .client_response_body
            .as_ref()
            .and_then(|body| body.get("error"))
            .and_then(|error| error.get("type"))
            .and_then(Value::as_str),
        Some("stream_terminal_error")
    );
}

#[test]
fn completed_image_usage_estimates_request_tokens_when_provider_usage_is_missing() {
    let plan = ExecutionPlan {
        request_id: "req-image-completed-estimate-1".to_string(),
        candidate_id: Some("cand-image-completed-estimate-1".to_string()),
        provider_name: Some("OpenAI".to_string()),
        provider_id: "provider-1".to_string(),
        endpoint_id: "endpoint-1".to_string(),
        key_id: "key-1".to_string(),
        method: "POST".to_string(),
        url: "https://example.com/v1/images/generations".to_string(),
        headers: BTreeMap::new(),
        content_type: Some("application/json".to_string()),
        content_encoding: None,
        body: RequestBody::from_json(json!({
            "model": "gpt-image-2",
            "prompt": "draw a small red cube on a clean desk",
            "size": "1024x1024",
            "quality": "medium"
        })),
        stream: true,
        client_api_format: "openai:chat".to_string(),
        provider_api_format: "openai:image".to_string(),
        model_name: Some("gpt-image-2".to_string()),
        proxy: None,
        transport_profile: None,
        timeouts: None,
    };
    let mut standardized_usage = StandardizedUsage::new();
    standardized_usage.request_count = 1;
    standardized_usage
        .dimensions
        .insert("image_count".to_string(), json!(1));
    standardized_usage
        .dimensions
        .insert("image_size".to_string(), json!("1024x1024"));
    standardized_usage
        .dimensions
        .insert("image_quality".to_string(), json!("medium"));
    let payload = GatewayStreamReportRequest {
        trace_id: "trace-image-completed-estimate-1".to_string(),
        report_kind: "openai_chat_stream_success".to_string(),
        report_context: Some(json!({
            "client_api_format": "openai:chat",
            "provider_api_format": "openai:image",
            "image_request": {
                "size": "1024x1024",
                "quality": "medium"
            }
        })),
        status_code: 200,
        headers: BTreeMap::new(),
        provider_body_base64: None,
        provider_body_state: Some(UsageBodyCaptureState::None),
        client_body_base64: None,
        client_body_state: Some(UsageBodyCaptureState::None),
        terminal_summary: Some(ExecutionStreamTerminalSummary {
            standardized_usage: Some(standardized_usage),
            finish_reason: Some("stop".to_string()),
            response_id: Some("resp_image_estimate_1".to_string()),
            model: Some("gpt-image-2".to_string()),
            provider_actual_service_tier: None,
            observed_finish: true,
            unknown_event_count: 0,
            parser_error: None,
        }),
        telemetry: None,
    };

    let event = build_stream_terminal_usage_event(&plan, payload.report_context.as_ref(), &payload)
        .expect("usage event should build");

    assert_eq!(event.event_type, UsageEventType::Completed);
    assert!(event.data.input_tokens.unwrap_or_default() > 0);
    assert_eq!(event.data.output_tokens.unwrap_or_default(), 0);
    assert_eq!(event.data.total_tokens, event.data.input_tokens);
    assert_eq!(
        event.data.request_metadata.as_ref().unwrap()["analytics_measurement"]["source"],
        "estimated"
    );
    assert_eq!(
        event
            .data
            .request_metadata
            .as_ref()
            .and_then(|metadata| metadata.get("dimensions"))
            .and_then(|dimensions| dimensions.get("image_count"))
            .and_then(Value::as_i64),
        Some(1)
    );
}

#[test]
fn sync_completed_openai_image_usage_infers_image_dimensions_from_response() {
    let request_body = json!({
        "model": "gpt-image-2",
        "prompt": "draw a small red cube on a clean desk",
        "size": "1024x1024",
        "quality": "medium",
        "output_format": "png"
    });
    let plan = ExecutionPlan {
        request_id: "req-image-sync-completed-dimensions-1".to_string(),
        candidate_id: Some("cand-image-sync-completed-dimensions-1".to_string()),
        provider_name: Some("OpenAI".to_string()),
        provider_id: "provider-1".to_string(),
        endpoint_id: "endpoint-1".to_string(),
        key_id: "key-1".to_string(),
        method: "POST".to_string(),
        url: "https://example.com/v1/images/generations".to_string(),
        headers: BTreeMap::new(),
        content_type: Some("application/json".to_string()),
        content_encoding: None,
        body: RequestBody::from_json(request_body.clone()),
        stream: false,
        client_api_format: "openai:image".to_string(),
        provider_api_format: "openai:image".to_string(),
        model_name: Some("gpt-image-2".to_string()),
        proxy: None,
        transport_profile: None,
        timeouts: None,
    };
    let payload = GatewaySyncReportRequest {
        trace_id: "trace-image-sync-completed-dimensions-1".to_string(),
        report_kind: "openai_image_sync_success".to_string(),
        report_context: Some(json!({
            "client_api_format": "openai:image",
            "provider_api_format": "openai:image",
            "provider_request_body": request_body
        })),
        status_code: 200,
        headers: BTreeMap::new(),
        body_json: Some(json!({
            "created": 1_700_000_000,
            "data": [{ "b64_json": "abc" }]
        })),
        client_body_json: None,
        body_base64: None,
        telemetry: None,
    };

    let event = build_sync_terminal_usage_event(&plan, payload.report_context.as_ref(), &payload)
        .expect("usage event should build");

    let dimensions = event
        .data
        .request_metadata
        .as_ref()
        .and_then(|metadata| metadata.get("dimensions"))
        .expect("dimensions should exist");
    assert_eq!(event.event_type, UsageEventType::Completed);
    assert!(event.data.input_tokens.unwrap_or_default() > 0);
    assert_eq!(dimensions.get("image_count"), Some(&json!(1)));
    assert_eq!(dimensions.get("image_size"), Some(&json!("1024x1024")));
    assert_eq!(dimensions.get("image_quality"), Some(&json!("medium")));
    assert_eq!(dimensions.get("image_output_format"), Some(&json!("png")));
}

#[test]
fn sync_completed_openai_image_usage_preserves_native_usage_with_image_count() {
    let request_body = json!({
        "model": "gpt-image-2",
        "prompt": "draw a small red cube on a clean desk",
        "size": "1024x1024",
        "quality": "medium"
    });
    let plan = ExecutionPlan {
        request_id: "req-image-sync-native-usage-1".to_string(),
        candidate_id: Some("cand-image-sync-native-usage-1".to_string()),
        provider_name: Some("OpenAI".to_string()),
        provider_id: "provider-1".to_string(),
        endpoint_id: "endpoint-1".to_string(),
        key_id: "key-1".to_string(),
        method: "POST".to_string(),
        url: "https://example.com/v1/images/generations".to_string(),
        headers: BTreeMap::new(),
        content_type: Some("application/json".to_string()),
        content_encoding: None,
        body: RequestBody::from_json(request_body.clone()),
        stream: false,
        client_api_format: "openai:image".to_string(),
        provider_api_format: "openai:image".to_string(),
        model_name: Some("gpt-image-2".to_string()),
        proxy: None,
        transport_profile: None,
        timeouts: None,
    };
    let payload = GatewaySyncReportRequest {
        trace_id: "trace-image-sync-native-usage-1".to_string(),
        report_kind: "openai_image_sync_success".to_string(),
        report_context: Some(json!({
            "client_api_format": "openai:image",
            "provider_api_format": "openai:image",
            "provider_request_body": request_body
        })),
        status_code: 200,
        headers: BTreeMap::new(),
        body_json: Some(json!({
            "usage": {
                "input_tokens": 11,
                "output_tokens": 22,
                "total_tokens": 33
            },
            "data": [{ "b64_json": "abc" }]
        })),
        client_body_json: None,
        body_base64: None,
        telemetry: None,
    };

    let event = build_sync_terminal_usage_event(&plan, payload.report_context.as_ref(), &payload)
        .expect("usage event should build");

    assert_eq!(event.event_type, UsageEventType::Completed);
    assert_eq!(event.data.input_tokens, Some(11));
    assert_eq!(event.data.output_tokens, Some(22));
    assert_eq!(event.data.total_tokens, Some(33));
    assert_eq!(
        event
            .data
            .request_metadata
            .as_ref()
            .and_then(|metadata| metadata.get("dimensions"))
            .and_then(|dimensions| dimensions.get("image_count")),
        Some(&json!(1))
    );
}

#[test]
fn stream_terminal_usage_prefers_more_complete_provider_chunks_usage() {
    let plan = ExecutionPlan {
        request_id: "req-stream-provider-chunks-usage-1".to_string(),
        candidate_id: Some("cand-stream-provider-chunks-usage-1".to_string()),
        provider_name: Some("OpenAI".to_string()),
        provider_id: "provider-1".to_string(),
        endpoint_id: "endpoint-1".to_string(),
        key_id: "key-1".to_string(),
        method: "POST".to_string(),
        url: "https://example.com/v1/responses".to_string(),
        headers: BTreeMap::new(),
        content_type: None,
        content_encoding: None,
        body: RequestBody {
            json_body: None,
            body_bytes_b64: None,
            body_ref: None,
        },
        stream: true,
        client_api_format: "openai:responses".to_string(),
        provider_api_format: "openai:responses".to_string(),
        model_name: Some("gpt-5.5".to_string()),
        proxy: None,
        transport_profile: None,
        timeouts: None,
    };
    let mut partial_summary_usage = StandardizedUsage::new();
    partial_summary_usage.output_tokens = 148;
    let provider_body = json!({
        "chunks": [
            {
                "type": "response.created",
                "response": {
                    "id": "resp_123",
                    "object": "response",
                    "model": "gpt-5.5",
                    "status": "in_progress",
                    "usage": null
                }
            },
            {
                "type": "response.completed",
                "response": {
                    "id": "resp_123",
                    "object": "response",
                    "model": "gpt-5.5",
                    "status": "completed",
                    "usage": {
                        "input_tokens": 26,
                        "input_tokens_details": {
                            "cached_tokens": 0
                        },
                        "output_tokens": 148,
                        "output_tokens_details": {
                            "reasoning_tokens": 10
                        },
                        "total_tokens": 174
                    }
                },
                "sequence_number": 141
            }
        ],
        "metadata": {
            "stream": true,
            "total_chunks": 142,
            "stored_chunks": 142
        }
    });
    let payload = GatewayStreamReportRequest {
        trace_id: "trace-stream-provider-chunks-usage-1".to_string(),
        report_kind: "openai_responses_stream_success".to_string(),
        report_context: Some(json!({
            "client_api_format": "openai:responses",
            "provider_api_format": "openai:responses",
        })),
        status_code: 200,
        headers: BTreeMap::new(),
        provider_body_base64: Some(
            base64::engine::general_purpose::STANDARD.encode(provider_body.to_string()),
        ),
        provider_body_state: Some(UsageBodyCaptureState::Inline),
        client_body_base64: None,
        client_body_state: Some(UsageBodyCaptureState::None),
        terminal_summary: Some(ExecutionStreamTerminalSummary {
            standardized_usage: Some(partial_summary_usage),
            finish_reason: None,
            response_id: Some("resp_123".to_string()),
            model: Some("gpt-5.5".to_string()),
            provider_actual_service_tier: None,
            observed_finish: true,
            unknown_event_count: 0,
            parser_error: None,
        }),
        telemetry: None,
    };

    let event = build_stream_terminal_usage_event(&plan, payload.report_context.as_ref(), &payload)
        .expect("usage event should build");

    assert_eq!(event.data.input_tokens, Some(26));
    assert_eq!(event.data.output_tokens, Some(148));
    assert_eq!(event.data.total_tokens, Some(174));
    assert_eq!(event.data.cache_read_input_tokens, None);
}

#[test]
fn builds_stream_terminal_usage_from_sse_chunks_and_extracts_usage() {
    let plan = ExecutionPlan {
        request_id: "req-stream-usage-2".to_string(),
        candidate_id: Some("cand-stream-usage-2".to_string()),
        provider_name: Some("OpenAI".to_string()),
        provider_id: "provider-1".to_string(),
        endpoint_id: "endpoint-1".to_string(),
        key_id: "key-1".to_string(),
        method: "POST".to_string(),
        url: "https://example.com/v1/responses".to_string(),
        headers: BTreeMap::new(),
        content_type: None,
        content_encoding: None,
        body: RequestBody {
            json_body: None,
            body_bytes_b64: None,
            body_ref: None,
        },
        stream: true,
        client_api_format: "openai:responses".to_string(),
        provider_api_format: "openai:responses".to_string(),
        model_name: Some("gpt-5.4".to_string()),
        proxy: None,
        transport_profile: None,
        timeouts: None,
    };
    let sse_body = concat!(
        "event: response.created\n",
        "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_123\",\"object\":\"response\",\"model\":\"gpt-5.4\",\"status\":\"in_progress\"}}\n\n",
        "event: response.output_text.delta\n",
        "data: {\"type\":\"response.output_text.delta\",\"delta\":\"Hello from Responses stream\"}\n\n",
        "event: response.completed\n",
        "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_123\",\"object\":\"response\",\"model\":\"gpt-5.4\",\"status\":\"completed\",\"output\":[{\"type\":\"message\",\"role\":\"assistant\",\"content\":[{\"type\":\"output_text\",\"text\":\"Hello from Responses stream\"}]}],\"usage\":{\"input_tokens\":3,\"input_tokens_details\":{\"cached_tokens\":2,\"cached_creation_tokens\":1},\"output_tokens\":5,\"output_tokens_details\":{\"reasoning_tokens\":1},\"total_tokens\":8}}}\n\n",
        "data: [DONE]\n",
    );
    let payload = GatewayStreamReportRequest {
        trace_id: "trace-stream-usage-2".to_string(),
        report_kind: "openai_responses_stream_success".to_string(),
        report_context: Some(json!({
            "client_api_format": "openai:responses",
            "provider_api_format": "openai:responses",
        })),
        status_code: 200,
        headers: BTreeMap::new(),
        provider_body_base64: Some(base64::engine::general_purpose::STANDARD.encode(sse_body)),
        provider_body_state: Some(UsageBodyCaptureState::Inline),
        client_body_base64: None,
        client_body_state: Some(UsageBodyCaptureState::None),
        terminal_summary: None,
        telemetry: None,
    };

    let event = build_stream_terminal_usage_event(&plan, payload.report_context.as_ref(), &payload)
        .expect("usage event should build");

    assert_eq!(event.data.input_tokens, Some(3));
    assert_eq!(event.data.output_tokens, Some(5));
    assert_eq!(event.data.total_tokens, Some(8));
    assert_eq!(event.data.cache_creation_input_tokens, Some(1));
    assert_eq!(event.data.cache_read_input_tokens, Some(2));
    assert_eq!(
        event.data.response_body,
        Some(json!({
            "chunks": [
                {
                    "type": "response.created",
                    "response": {
                        "id": "resp_123",
                        "object": "response",
                        "model": "gpt-5.4",
                        "status": "in_progress"
                    }
                },
                {
                    "type": "response.output_text.delta",
                    "delta": "Hello from Responses stream"
                },
                {
                    "type": "response.completed",
                    "response": {
                        "id": "resp_123",
                        "object": "response",
                        "model": "gpt-5.4",
                        "status": "completed",
                        "output": [
                            {
                                "type": "message",
                                "role": "assistant",
                                "content": [
                                    {
                                        "type": "output_text",
                                        "text": "Hello from Responses stream"
                                    }
                                ]
                            }
                        ],
                        "usage": {
                            "input_tokens": 3,
                            "input_tokens_details": {
                                "cached_tokens": 2,
                                "cached_creation_tokens": 1
                            },
                            "output_tokens": 5,
                            "output_tokens_details": {
                                "reasoning_tokens": 1
                            },
                            "total_tokens": 8
                        }
                    }
                }
            ],
            "metadata": {
                "stream": true,
                "total_chunks": 3,
                "stored_chunks": 3,
                "content_length": sse_body.len(),
                "has_completion": true
            }
        }))
    );
}

#[test]
fn builds_sync_terminal_usage_from_provider_body_and_preserves_client_body() {
    let plan = ExecutionPlan {
        request_id: "req-sync-usage-1".to_string(),
        candidate_id: Some("cand-sync-usage-1".to_string()),
        provider_name: Some("Gemini".to_string()),
        provider_id: "provider-2".to_string(),
        endpoint_id: "endpoint-2".to_string(),
        key_id: "key-2".to_string(),
        method: "POST".to_string(),
        url: "https://example.com/v1beta/models/gemini:generateContent".to_string(),
        headers: BTreeMap::new(),
        content_type: None,
        content_encoding: None,
        body: RequestBody {
            json_body: None,
            body_bytes_b64: None,
            body_ref: None,
        },
        stream: false,
        client_api_format: "openai:chat".to_string(),
        provider_api_format: "gemini:generate_content".to_string(),
        model_name: Some("gpt-5".to_string()),
        proxy: None,
        transport_profile: None,
        timeouts: None,
    };
    let payload = GatewaySyncReportRequest {
        trace_id: "trace-sync-usage-1".to_string(),
        report_kind: "openai_chat_sync_success".to_string(),
        report_context: Some(json!({
            "client_api_format": "openai:chat",
            "provider_api_format": "gemini:generate_content",
            "needs_conversion": true
        })),
        status_code: 200,
        headers: BTreeMap::from([
            (
                "authorization".to_string(),
                "Bearer very-secret-token".to_string(),
            ),
            ("content-type".to_string(), "application/json".to_string()),
        ]),
        body_json: Some(json!({
            "usageMetadata": {
                "promptTokenCount": 4,
                "candidatesTokenCount": 6,
                "totalTokenCount": 10
            }
        })),
        client_body_json: Some(json!({
            "id": "chatcmpl_456",
            "object": "chat.completion"
        })),
        body_base64: None,
        telemetry: None,
    };

    let event = build_sync_terminal_usage_event(&plan, payload.report_context.as_ref(), &payload)
        .expect("usage event should build");

    assert_eq!(event.data.input_tokens, Some(4));
    assert_eq!(event.data.output_tokens, Some(6));
    assert_eq!(event.data.total_tokens, Some(10));
    assert_eq!(
        event.data.request_metadata.as_ref().unwrap()["analytics_measurement"]["source"],
        "reported"
    );
    for source in ["estimated", "mixed"] {
        let mut payload = payload.clone();
        payload.report_context.as_mut().unwrap()["usage_token_source"] = json!(source);
        let event =
            build_sync_terminal_usage_event(&plan, payload.report_context.as_ref(), &payload)
                .unwrap();
        assert_eq!(event.data.input_tokens, Some(4));
        assert_eq!(event.data.output_tokens, Some(6));
        assert_eq!(event.data.total_tokens, Some(10));
        assert_eq!(
            event.data.request_metadata.as_ref().unwrap()["analytics_measurement"]["source"],
            source
        );
        assert!(event
            .data
            .request_metadata
            .as_ref()
            .unwrap()
            .get("usage_token_source")
            .is_none());

        payload.status_code = 500;
        payload.body_json = Some(json!({"error": {"message": "no token measurement"}}));
        let failed =
            build_sync_terminal_usage_event(&plan, payload.report_context.as_ref(), &payload)
                .unwrap();
        assert!(failed
            .data
            .request_metadata
            .as_ref()
            .unwrap()
            .get("analytics_measurement")
            .is_none());
    }
    assert_eq!(
        event.data.response_headers,
        Some(json!({
            "authorization": "Bearer very-secret-token",
            "content-type": "application/json"
        }))
    );
    assert_eq!(
        event.data.client_response_headers,
        Some(json!({
            "authorization": "Bearer very-secret-token",
            "content-type": "application/json"
        }))
    );
    assert_eq!(
        event.data.response_body,
        Some(json!({
            "usageMetadata": {
                "promptTokenCount": 4,
                "candidatesTokenCount": 6,
                "totalTokenCount": 10
            }
        }))
    );
    assert_eq!(
        event.data.client_response_body,
        Some(json!({
            "id": "chatcmpl_456",
            "object": "chat.completion"
        }))
    );
}

#[test]
fn sync_terminal_usage_prefers_upstream_stream_body_over_aggregated_sync_body() {
    let sse_body = concat!(
        "event: response.created\n",
        "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_sync_stream_123\",\"object\":\"response\",\"model\":\"gpt-5.4\",\"status\":\"in_progress\",\"output\":[]}}\n\n",
        "event: response.output_text.delta\n",
        "data: {\"type\":\"response.output_text.delta\",\"delta\":\"Hello from upstream stream\"}\n\n",
        "event: response.completed\n",
        "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_sync_stream_123\",\"object\":\"response\",\"model\":\"gpt-5.4\",\"status\":\"completed\",\"output\":[],\"usage\":{\"input_tokens\":3,\"output_tokens\":5,\"total_tokens\":8}}}\n\n",
        "data: [DONE]\n",
    );
    let plan = ExecutionPlan {
        request_id: "req-sync-upstream-stream-1".to_string(),
        candidate_id: Some("cand-sync-upstream-stream-1".to_string()),
        provider_name: Some("OpenAI".to_string()),
        provider_id: "provider-1".to_string(),
        endpoint_id: "endpoint-1".to_string(),
        key_id: "key-1".to_string(),
        method: "POST".to_string(),
        url: "https://example.com/v1/responses".to_string(),
        headers: BTreeMap::new(),
        content_type: None,
        content_encoding: None,
        body: RequestBody {
            json_body: None,
            body_bytes_b64: None,
            body_ref: None,
        },
        stream: false,
        client_api_format: "openai:responses".to_string(),
        provider_api_format: "openai:responses".to_string(),
        model_name: Some("gpt-5.4".to_string()),
        proxy: None,
        transport_profile: None,
        timeouts: None,
    };
    let payload = GatewaySyncReportRequest {
        trace_id: "trace-sync-upstream-stream-1".to_string(),
        report_kind: "openai_responses_sync_success".to_string(),
        report_context: Some(json!({
            "client_api_format": "openai:responses",
            "provider_api_format": "openai:responses",
            "upstream_is_stream": true
        })),
        status_code: 200,
        headers: BTreeMap::from([("content-type".to_string(), "text/event-stream".to_string())]),
        body_json: Some(json!({
            "id": "resp_sync_stream_123",
            "object": "response",
            "status": "completed",
            "output": [],
            "usage": {
                "input_tokens": 1,
                "output_tokens": 1,
                "total_tokens": 2
            }
        })),
        client_body_json: Some(json!({
            "id": "resp_sync_stream_123",
            "object": "response",
            "status": "completed",
            "output": [{
                "type": "message",
                "role": "assistant",
                "content": [{
                    "type": "output_text",
                    "text": "Hello from upstream stream"
                }]
            }],
            "usage": {
                "input_tokens": 3,
                "output_tokens": 5,
                "total_tokens": 8
            }
        })),
        body_base64: Some(base64::engine::general_purpose::STANDARD.encode(sse_body)),
        telemetry: None,
    };

    let event = build_sync_terminal_usage_event(&plan, payload.report_context.as_ref(), &payload)
        .expect("usage event should build");

    assert_eq!(event.data.input_tokens, Some(3));
    assert_eq!(event.data.output_tokens, Some(5));
    assert_eq!(event.data.total_tokens, Some(8));
    assert_eq!(
        event
            .data
            .response_body
            .as_ref()
            .and_then(|value| value.get("chunks"))
            .and_then(Value::as_array)
            .and_then(|chunks| chunks.get(1))
            .and_then(|chunk| chunk.get("type"))
            .and_then(Value::as_str),
        Some("response.output_text.delta")
    );
    assert_eq!(
        event.data.client_response_body.as_ref().and_then(|value| {
            value
                .get("output")
                .and_then(Value::as_array)
                .and_then(|output| output.first())
                .and_then(|item| item.get("content"))
                .and_then(Value::as_array)
                .and_then(|content| content.first())
                .and_then(|part| part.get("text"))
                .and_then(Value::as_str)
        }),
        Some("Hello from upstream stream")
    );
}

#[test]
fn sync_terminal_usage_prefers_error_body_over_partial_upstream_stream_body() {
    let partial_sse_body = concat!(
        "event: response.created\n",
        "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_partial_123\",\"object\":\"response\",\"model\":\"gpt-5.5\",\"status\":\"in_progress\",\"output\":[]}}\n\n",
        "event: response.output_item.added\n",
        "data: {\"type\":\"response.output_item.added\",\"output_index\":0,\"item\":{\"type\":\"function_call\",\"name\":\"exec_command\"}}\n\n",
        "event: response.function_call_arguments.delta\n",
        "data: {\"type\":\"response.function_call_arguments.delta\",\"delta\":\"{\\\"cmd\\\":\"}\n\n",
    );
    let plan = ExecutionPlan {
        request_id: "req-sync-upstream-stream-error-1".to_string(),
        candidate_id: Some("cand-sync-upstream-stream-error-1".to_string()),
        provider_name: Some("OpenAI".to_string()),
        provider_id: "provider-1".to_string(),
        endpoint_id: "endpoint-1".to_string(),
        key_id: "key-1".to_string(),
        method: "POST".to_string(),
        url: "https://example.com/v1/responses".to_string(),
        headers: BTreeMap::new(),
        content_type: None,
        content_encoding: None,
        body: RequestBody {
            json_body: None,
            body_bytes_b64: None,
            body_ref: None,
        },
        stream: false,
        client_api_format: "claude:messages".to_string(),
        provider_api_format: "openai:responses".to_string(),
        model_name: Some("gpt-5.5".to_string()),
        proxy: None,
        transport_profile: None,
        timeouts: None,
    };
    let payload = GatewaySyncReportRequest {
        trace_id: "trace-sync-upstream-stream-error-1".to_string(),
        report_kind: "openai_responses_sync_error".to_string(),
        report_context: Some(json!({
            "client_api_format": "claude:messages",
            "provider_api_format": "openai:responses",
            "upstream_is_stream": true,
            "needs_conversion": true
        })),
        status_code: 200,
        headers: BTreeMap::from([("content-type".to_string(), "text/event-stream".to_string())]),
        body_json: Some(json!({
            "error": {
                "type": "internal",
                "message": "error decoding response body: stream error received"
            }
        })),
        client_body_json: None,
        body_base64: Some(base64::engine::general_purpose::STANDARD.encode(partial_sse_body)),
        telemetry: None,
    };

    let event = build_sync_terminal_usage_event(&plan, payload.report_context.as_ref(), &payload)
        .expect("usage event should build");

    assert_eq!(event.event_type, UsageEventType::Failed);
    assert_eq!(event.data.status_code, Some(200));
    assert_eq!(
        event.data.error_message.as_deref(),
        Some("error decoding response body: stream error received")
    );
    assert_eq!(
        event
            .data
            .response_body
            .as_ref()
            .and_then(|value| value.pointer("/error/type"))
            .and_then(Value::as_str),
        Some("internal")
    );
}

#[test]
fn sync_terminal_seed_path_matches_legacy_wrapper_event() {
    let plan = ExecutionPlan {
        request_id: "req-sync-seed-match-1".to_string(),
        candidate_id: Some("cand-sync-seed-match-1".to_string()),
        provider_name: Some("Gemini".to_string()),
        provider_id: "provider-2".to_string(),
        endpoint_id: "endpoint-2".to_string(),
        key_id: "key-2".to_string(),
        method: "POST".to_string(),
        url: "https://example.com/v1beta/models/gemini:generateContent".to_string(),
        headers: BTreeMap::new(),
        content_type: None,
        content_encoding: None,
        body: RequestBody {
            json_body: None,
            body_bytes_b64: None,
            body_ref: None,
        },
        stream: false,
        client_api_format: "openai:chat".to_string(),
        provider_api_format: "gemini:generate_content".to_string(),
        model_name: Some("gpt-5".to_string()),
        proxy: None,
        transport_profile: None,
        timeouts: None,
    };
    let payload = GatewaySyncReportRequest {
        trace_id: "trace-sync-seed-match-1".to_string(),
        report_kind: "openai_chat_sync_success".to_string(),
        report_context: Some(json!({
            "client_api_format": "openai:chat",
            "provider_api_format": "gemini:generate_content",
            "needs_conversion": true
        })),
        status_code: 200,
        headers: BTreeMap::new(),
        body_json: Some(json!({
            "usageMetadata": {
                "promptTokenCount": 4,
                "candidatesTokenCount": 6,
                "totalTokenCount": 10
            }
        })),
        client_body_json: Some(json!({
            "id": "chatcmpl_456",
            "object": "chat.completion"
        })),
        body_base64: None,
        telemetry: None,
    };

    let legacy_event =
        build_sync_terminal_usage_event(&plan, payload.report_context.as_ref(), &payload)
            .expect("legacy terminal usage should build");
    let context_seed = build_terminal_usage_context_seed(&plan, payload.report_context.as_ref());
    let payload_seed = build_sync_terminal_usage_payload_seed(&payload);
    let seed_event = build_terminal_usage_event_from_seed(build_sync_terminal_usage_seed(
        context_seed,
        payload_seed,
    ))
    .expect("seed terminal usage should build");

    assert_eq!(seed_event.event_type, legacy_event.event_type);
    assert_eq!(seed_event.request_id, legacy_event.request_id);
    assert_eq!(seed_event.data, legacy_event.data);
}

#[test]
fn sync_terminal_usage_exposes_provider_request_body_ref_as_typed_field() {
    let plan = ExecutionPlan {
        request_id: "req-sync-body-ref-1".to_string(),
        candidate_id: Some("cand-sync-body-ref-1".to_string()),
        provider_name: Some("OpenAI".to_string()),
        provider_id: "provider-1".to_string(),
        endpoint_id: "endpoint-1".to_string(),
        key_id: "key-1".to_string(),
        method: "POST".to_string(),
        url: "https://example.com/v1/responses".to_string(),
        headers: BTreeMap::new(),
        content_type: Some("application/json".to_string()),
        content_encoding: None,
        body: RequestBody {
            json_body: Some(json!({
                "model": "gpt-5.4",
                "input": "this should stay out of usage storage"
            })),
            body_bytes_b64: None,
            body_ref: Some("blob://provider-request-1".to_string()),
        },
        stream: false,
        client_api_format: "openai:responses".to_string(),
        provider_api_format: "openai:responses".to_string(),
        model_name: Some("gpt-5.4".to_string()),
        proxy: None,
        transport_profile: None,
        timeouts: None,
    };
    let payload = GatewaySyncReportRequest {
        trace_id: "trace-sync-body-ref-1".to_string(),
        report_kind: "openai_responses_sync_success".to_string(),
        report_context: Some(json!({
            "client_api_format": "openai:responses",
            "provider_api_format": "openai:responses",
            "trace_id": "trace-sync-body-ref-1"
        })),
        status_code: 200,
        headers: BTreeMap::new(),
        body_json: Some(json!({"id": "resp_123"})),
        client_body_json: None,
        body_base64: None,
        telemetry: None,
    };

    let event = build_sync_terminal_usage_event(&plan, payload.report_context.as_ref(), &payload)
        .expect("usage event should build");

    assert!(event.data.request_body.is_none());
    assert!(event.data.provider_request_body.is_none());
    assert_eq!(
        event.data.provider_request_body_ref.as_deref(),
        Some("blob://provider-request-1")
    );
    assert!(event
        .data
        .request_metadata
        .as_ref()
        .is_none_or(|value| { value.get("provider_request_body_ref").is_none() }));
}

#[test]
fn sync_terminal_usage_records_human_readable_request_body_sizes_in_metadata() {
    let client_body_bytes = vec![b'c'; 1024];
    let provider_body_bytes = vec![b'p'; 4096];
    let client_body_base64 = base64::engine::general_purpose::STANDARD.encode(client_body_bytes);
    let provider_body_base64 =
        base64::engine::general_purpose::STANDARD.encode(provider_body_bytes);
    let plan = ExecutionPlan {
        request_id: "req-sync-body-size-1".to_string(),
        candidate_id: Some("cand-sync-body-size-1".to_string()),
        provider_name: Some("OpenAI".to_string()),
        provider_id: "provider-1".to_string(),
        endpoint_id: "endpoint-1".to_string(),
        key_id: "key-1".to_string(),
        method: "POST".to_string(),
        url: "https://example.com/v1/responses".to_string(),
        headers: BTreeMap::new(),
        content_type: Some("application/json".to_string()),
        content_encoding: None,
        body: RequestBody {
            json_body: None,
            body_bytes_b64: Some(provider_body_base64),
            body_ref: None,
        },
        stream: false,
        client_api_format: "openai:chat".to_string(),
        provider_api_format: "openai:responses".to_string(),
        model_name: Some("gpt-5.4".to_string()),
        proxy: None,
        transport_profile: None,
        timeouts: None,
    };
    let payload = GatewaySyncReportRequest {
        trace_id: "trace-sync-body-size-1".to_string(),
        report_kind: "openai_chat_sync_success".to_string(),
        report_context: Some(json!({
            "client_api_format": "openai:chat",
            "provider_api_format": "openai:responses",
            "needs_conversion": true,
            "original_request_body": {
                "body_bytes_b64": client_body_base64
            }
        })),
        status_code: 200,
        headers: BTreeMap::new(),
        body_json: Some(json!({"id": "resp_123"})),
        client_body_json: None,
        body_base64: None,
        telemetry: None,
    };

    let context_seed = build_terminal_usage_context_seed(&plan, payload.report_context.as_ref());
    assert_request_body_size_metadata(
        context_seed.request_metadata.as_ref(),
        "context seed should include body size metadata",
    );

    let payload_seed = build_sync_terminal_usage_payload_seed(&payload);
    let seed_event = build_terminal_usage_event_from_seed(build_sync_terminal_usage_seed(
        context_seed,
        payload_seed,
    ))
    .expect("seed usage event should build");
    assert_request_body_size_metadata(
        seed_event.data.request_metadata.as_ref(),
        "seed event should include body size metadata",
    );

    let wrapper_event =
        build_sync_terminal_usage_event(&plan, payload.report_context.as_ref(), &payload)
            .expect("wrapper usage event should build");
    assert_request_body_size_metadata(
        wrapper_event.data.request_metadata.as_ref(),
        "wrapper event should include body size metadata",
    );
}

fn assert_request_body_size_metadata(metadata: Option<&Value>, message: &str) {
    let body_size = metadata
        .and_then(|metadata| metadata.get("body_size"))
        .unwrap_or_else(|| panic!("{message}: {metadata:?}"));

    assert_eq!(body_size.get("client_request_body"), Some(&json!("1 KB")));
    assert_eq!(body_size.get("provider_request_body"), Some(&json!("4 KB")));
    assert_eq!(body_size.get("provider_over_client"), Some(&json!("4x")));
}

#[test]
fn stream_terminal_usage_records_base64_response_sizes_in_metadata() {
    let provider_bytes = b"{\"usage\":{\"input_tokens\":1,\"output_tokens\":2,\"total_tokens\":3}}";
    let client_bytes = b"data: {\"id\":\"chatcmpl_123\"}\n\ndata: [DONE]\n";
    let plan = ExecutionPlan {
        request_id: "req-stream-bytes-1".to_string(),
        candidate_id: Some("cand-stream-bytes-1".to_string()),
        provider_name: Some("OpenAI".to_string()),
        provider_id: "provider-1".to_string(),
        endpoint_id: "endpoint-1".to_string(),
        key_id: "key-1".to_string(),
        method: "POST".to_string(),
        url: "https://example.com/v1/responses".to_string(),
        headers: BTreeMap::new(),
        content_type: None,
        content_encoding: None,
        body: RequestBody {
            json_body: None,
            body_bytes_b64: None,
            body_ref: None,
        },
        stream: true,
        client_api_format: "openai:chat".to_string(),
        provider_api_format: "openai:responses".to_string(),
        model_name: Some("gpt-5.4".to_string()),
        proxy: None,
        transport_profile: None,
        timeouts: None,
    };
    let payload = GatewayStreamReportRequest {
        trace_id: "trace-stream-bytes-1".to_string(),
        report_kind: "openai_chat_stream_success".to_string(),
        report_context: Some(json!({
            "client_api_format": "openai:chat",
            "provider_api_format": "openai:responses",
            "trace_id": "trace-stream-bytes-1"
        })),
        status_code: 200,
        headers: BTreeMap::new(),
        provider_body_base64: Some(
            base64::engine::general_purpose::STANDARD.encode(provider_bytes),
        ),
        provider_body_state: Some(UsageBodyCaptureState::Inline),
        client_body_base64: Some(base64::engine::general_purpose::STANDARD.encode(client_bytes)),
        client_body_state: Some(UsageBodyCaptureState::Inline),
        terminal_summary: None,
        telemetry: None,
    };

    let event = build_stream_terminal_usage_event(&plan, payload.report_context.as_ref(), &payload)
        .expect("usage event should build");

    assert_eq!(
        event
            .data
            .request_metadata
            .as_ref()
            .and_then(|value| value.get("provider_response_body_base64_bytes"))
            .and_then(|value| value.as_u64()),
        Some(provider_bytes.len() as u64)
    );
    assert_eq!(
        event
            .data
            .request_metadata
            .as_ref()
            .and_then(|value| value.get("client_response_body_base64_bytes"))
            .and_then(|value| value.as_u64()),
        Some(client_bytes.len() as u64)
    );
}

#[test]
fn stream_terminal_usage_preserves_large_sse_response_for_repo_body_storage() {
    let large_delta = "x".repeat(MAX_USAGE_CAPTURE_BYTES);
    let sse_body = format!(
        concat!(
            "event: response.created\n",
            "data: {{\"type\":\"response.created\",\"response\":{{\"id\":\"resp_123\",\"object\":\"response\",\"model\":\"gpt-5.4\",\"status\":\"in_progress\"}}}}\n\n",
            "event: response.output_text.delta\n",
            "data: {{\"type\":\"response.output_text.delta\",\"delta\":\"{delta}\"}}\n\n",
            "event: response.completed\n",
            "data: {{\"type\":\"response.completed\",\"response\":{{\"id\":\"resp_123\",\"object\":\"response\",\"model\":\"gpt-5.4\",\"status\":\"completed\",\"usage\":{{\"input_tokens\":3,\"input_tokens_details\":{{\"cached_tokens\":2,\"cached_creation_tokens\":1}},\"output_tokens\":5,\"output_tokens_details\":{{\"reasoning_tokens\":1}},\"total_tokens\":8}}}}}}\n\n",
            "data: [DONE]\n",
        ),
        delta = large_delta
    );
    let plan = ExecutionPlan {
        request_id: "req-stream-usage-large-1".to_string(),
        candidate_id: Some("cand-stream-usage-large-1".to_string()),
        provider_name: Some("OpenAI".to_string()),
        provider_id: "provider-1".to_string(),
        endpoint_id: "endpoint-1".to_string(),
        key_id: "key-1".to_string(),
        method: "POST".to_string(),
        url: "https://example.com/v1/responses".to_string(),
        headers: BTreeMap::new(),
        content_type: None,
        content_encoding: None,
        body: RequestBody {
            json_body: None,
            body_bytes_b64: None,
            body_ref: None,
        },
        stream: true,
        client_api_format: "openai:responses".to_string(),
        provider_api_format: "openai:responses".to_string(),
        model_name: Some("gpt-5.4".to_string()),
        proxy: None,
        transport_profile: None,
        timeouts: None,
    };
    let payload = GatewayStreamReportRequest {
        trace_id: "trace-stream-usage-large-1".to_string(),
        report_kind: "openai_responses_stream_success".to_string(),
        report_context: Some(json!({
            "client_api_format": "openai:responses",
            "provider_api_format": "openai:responses",
        })),
        status_code: 200,
        headers: BTreeMap::new(),
        provider_body_base64: Some(base64::engine::general_purpose::STANDARD.encode(&sse_body)),
        provider_body_state: Some(UsageBodyCaptureState::Truncated),
        client_body_base64: None,
        client_body_state: Some(UsageBodyCaptureState::None),
        terminal_summary: None,
        telemetry: None,
    };

    let event = build_stream_terminal_usage_event(&plan, payload.report_context.as_ref(), &payload)
        .expect("usage event should build");

    assert_eq!(event.data.input_tokens, Some(3));
    assert_eq!(event.data.output_tokens, Some(5));
    assert_eq!(event.data.total_tokens, Some(8));
    assert_eq!(event.data.cache_creation_input_tokens, Some(1));
    assert_eq!(event.data.cache_read_input_tokens, Some(2));
    assert_eq!(
        event
            .data
            .response_body
            .as_ref()
            .and_then(|value| value.get("chunks"))
            .and_then(Value::as_array)
            .and_then(|chunks| chunks.get(1))
            .and_then(|value| value.get("delta"))
            .and_then(Value::as_str),
        Some(large_delta.as_str())
    );
}

#[test]
fn sync_terminal_usage_does_not_fallback_request_body_to_plan_when_client_echo_is_absent() {
    let plan = ExecutionPlan {
        request_id: "req-sync-no-client-echo-1".to_string(),
        candidate_id: Some("cand-sync-no-client-echo-1".to_string()),
        provider_name: Some("OpenAI".to_string()),
        provider_id: "provider-1".to_string(),
        endpoint_id: "endpoint-1".to_string(),
        key_id: "key-1".to_string(),
        method: "POST".to_string(),
        url: "https://example.com/v1/responses".to_string(),
        headers: BTreeMap::new(),
        content_type: Some("application/json".to_string()),
        content_encoding: None,
        body: RequestBody::from_json(json!({
            "model": "gpt-5.4",
            "input": [{"role": "user", "content": "provider-side compiled body"}],
        })),
        stream: false,
        client_api_format: "claude:messages".to_string(),
        provider_api_format: "openai:responses".to_string(),
        model_name: Some("gpt-5.4".to_string()),
        proxy: None,
        transport_profile: None,
        timeouts: None,
    };
    let payload = GatewaySyncReportRequest {
        trace_id: "trace-sync-no-client-echo-1".to_string(),
        report_kind: "claude_cli_sync_success".to_string(),
        report_context: Some(json!({
            "client_api_format": "claude:messages",
            "provider_api_format": "openai:responses",
            "needs_conversion": true,
            "original_request_body": null,
        })),
        status_code: 200,
        headers: BTreeMap::new(),
        body_json: Some(json!({"id": "resp_1"})),
        client_body_json: None,
        body_base64: None,
        telemetry: None,
    };

    let event = build_sync_terminal_usage_event(&plan, payload.report_context.as_ref(), &payload)
        .expect("usage event should build");

    assert!(event.data.request_body.is_none());
    assert_eq!(
        event.data.provider_request_body,
        Some(json!({
            "model": "gpt-5.4",
            "input": [{"role": "user", "content": "provider-side compiled body"}],
        }))
    );
    let body_size = event
        .data
        .request_metadata
        .as_ref()
        .and_then(|metadata| metadata.get("body_size"))
        .and_then(Value::as_object)
        .expect("provider body size metadata should exist");
    assert!(body_size.get("provider_request_body").is_some());
    assert!(body_size.get("client_request_body").is_none());
    assert!(body_size.get("provider_over_client").is_none());
}

#[test]
fn openai_image_sync_terminal_usage_captures_request_and_response_bodies() {
    let plan = ExecutionPlan {
        request_id: "req-openai-image-sync-body-1".to_string(),
        candidate_id: Some("cand-openai-image-sync-body-1".to_string()),
        provider_name: Some("Upstream Aether".to_string()),
        provider_id: "provider-aether-1".to_string(),
        endpoint_id: "endpoint-aether-1".to_string(),
        key_id: "key-aether-1".to_string(),
        method: "POST".to_string(),
        url: "https://upstream-aether.example/v1/images/generations".to_string(),
        headers: BTreeMap::from([("content-type".to_string(), "application/json".to_string())]),
        content_type: Some("application/json".to_string()),
        content_encoding: None,
        body: RequestBody::from_json(json!({
            "model": "gpt-image-2-upstream",
            "prompt": "Draw a red kite",
            "size": "1024x1024",
            "n": 1,
            "response_format": "b64_json"
        })),
        stream: false,
        client_api_format: "openai:image".to_string(),
        provider_api_format: "openai:image".to_string(),
        model_name: Some("gpt-image-2".to_string()),
        proxy: None,
        transport_profile: None,
        timeouts: None,
    };
    let payload = GatewaySyncReportRequest {
        trace_id: "trace-openai-image-sync-body-1".to_string(),
        report_kind: "openai_image_sync_finalize".to_string(),
        report_context: Some(json!({
            "client_api_format": "openai:image",
            "provider_api_format": "openai:image",
            "needs_conversion": true,
            "original_request_body": {
                "model": "gpt-image-2",
                "prompt": "Draw a red kite",
                "size": "1024x1024",
                "response_format": "b64_json"
            },
            "provider_request_body": {
                "model": "gpt-image-2-upstream",
                "prompt": "Draw a red kite",
                "size": "1024x1024",
                "n": 1,
                "response_format": "b64_json"
            }
        })),
        status_code: 200,
        headers: BTreeMap::from([("content-type".to_string(), "application/json".to_string())]),
        body_json: Some(json!({
            "created": 1776839946,
            "data": [{
                "b64_json": "aGVsbG8=",
                "revised_prompt": "red kite"
            }],
            "usage": {
                "input_tokens": 11,
                "output_tokens": 22,
                "total_tokens": 33
            }
        })),
        client_body_json: None,
        body_base64: None,
        telemetry: None,
    };

    let event = build_sync_terminal_usage_event(&plan, payload.report_context.as_ref(), &payload)
        .expect("usage event should build");

    assert_eq!(
        event.data.request_body,
        payload
            .report_context
            .as_ref()
            .and_then(|value| value.get("original_request_body"))
            .cloned()
    );
    assert_eq!(
        event.data.provider_request_body,
        payload
            .report_context
            .as_ref()
            .and_then(|value| value.get("provider_request_body"))
            .cloned()
    );
    assert_eq!(event.data.response_body, payload.body_json);
    assert!(event.data.client_response_body.is_none());
    assert_eq!(
        event.data.request_body_state,
        Some(UsageBodyCaptureState::Inline)
    );
    assert_eq!(
        event.data.provider_request_body_state,
        Some(UsageBodyCaptureState::Inline)
    );
    assert_eq!(
        event.data.response_body_state,
        Some(UsageBodyCaptureState::Inline)
    );
}

#[test]
fn sync_terminal_usage_applies_kiro_simulated_cache_context() {
    let plan = ExecutionPlan {
        request_id: "req-sync-kiro-cache-context-1".to_string(),
        candidate_id: Some("cand-sync-kiro-cache-context-1".to_string()),
        provider_name: Some("Kiro".to_string()),
        provider_id: "provider-kiro-1".to_string(),
        endpoint_id: "endpoint-kiro-1".to_string(),
        key_id: "key-kiro-1".to_string(),
        method: "POST".to_string(),
        url: "https://kiro.example/generateAssistantResponse".to_string(),
        headers: BTreeMap::new(),
        content_type: Some("application/json".to_string()),
        content_encoding: None,
        body: RequestBody::from_json(json!({
            "model": "claude-sonnet-4",
            "messages": [{"role": "user", "content": "hello kiro"}],
        })),
        stream: false,
        client_api_format: "claude:messages".to_string(),
        provider_api_format: "claude:messages".to_string(),
        model_name: Some("claude-sonnet-4".to_string()),
        proxy: None,
        transport_profile: None,
        timeouts: None,
    };
    let payload = GatewaySyncReportRequest {
        trace_id: "trace-sync-kiro-cache-context-1".to_string(),
        report_kind: "claude_cli_sync_success".to_string(),
        report_context: Some(json!({
            "client_api_format": "claude:messages",
            "provider_api_format": "claude:messages",
            "provider_name": "Kiro",
            "model": "claude-sonnet-4",
            "input_tokens": 1800,
            "kiro_simulated_cache_enabled": true,
            "cache_creation_input_tokens": 1200,
            "cache_read_input_tokens": 300,
        })),
        status_code: 200,
        headers: BTreeMap::new(),
        body_json: Some(json!({"id": "kiro-sync-response-1"})),
        client_body_json: None,
        body_base64: None,
        telemetry: None,
    };

    let event = build_sync_terminal_usage_event(&plan, payload.report_context.as_ref(), &payload)
        .expect("usage event should build");

    assert_eq!(event.event_type, UsageEventType::Completed);
    assert_eq!(event.data.input_tokens, Some(300));
    assert_eq!(event.data.cache_creation_input_tokens, Some(1200));
    assert_eq!(event.data.cache_read_input_tokens, Some(300));
    assert_eq!(event.data.total_tokens, Some(300));
    assert_eq!(
        event.data.request_metadata.as_ref().unwrap()["analytics_measurement"]["source"],
        "estimated"
    );

    let mut payload = payload;
    payload.body_json = Some(json!({"usage": {"output_tokens": 17}}));
    let mixed =
        build_sync_terminal_usage_event(&plan, payload.report_context.as_ref(), &payload).unwrap();
    assert_eq!(mixed.data.output_tokens, Some(17));
    assert_eq!(mixed.data.cache_read_input_tokens, Some(300));
    assert_eq!(
        mixed.data.request_metadata.as_ref().unwrap()["analytics_measurement"]["source"],
        "mixed"
    );

    payload.report_context.as_mut().unwrap()["usage_token_source"] = json!("estimated");
    let native =
        build_sync_terminal_usage_event(&plan, payload.report_context.as_ref(), &payload).unwrap();
    assert_eq!(native.data.output_tokens, Some(17));
    assert_eq!(
        native.data.request_metadata.as_ref().unwrap()["analytics_measurement"]["source"],
        "estimated"
    );
}

#[test]
fn sync_terminal_usage_treats_null_error_field_as_success() {
    let plan = ExecutionPlan {
        request_id: "req-sync-null-error-1".to_string(),
        candidate_id: Some("cand-sync-null-error-1".to_string()),
        provider_name: Some("OpenAI".to_string()),
        provider_id: "provider-1".to_string(),
        endpoint_id: "endpoint-1".to_string(),
        key_id: "key-1".to_string(),
        method: "POST".to_string(),
        url: "https://example.com/v1/messages".to_string(),
        headers: BTreeMap::new(),
        content_type: Some("application/json".to_string()),
        content_encoding: None,
        body: RequestBody::from_json(json!({"model": "gpt-5.4"})),
        stream: false,
        client_api_format: "claude:messages".to_string(),
        provider_api_format: "openai:responses".to_string(),
        model_name: Some("gpt-5.4".to_string()),
        proxy: None,
        transport_profile: None,
        timeouts: None,
    };
    let payload = GatewaySyncReportRequest {
        trace_id: "trace-sync-null-error-1".to_string(),
        report_kind: "claude_cli_sync_success".to_string(),
        report_context: Some(json!({
            "client_api_format": "claude:messages",
            "provider_api_format": "openai:responses",
            "needs_conversion": true,
        })),
        status_code: 200,
        headers: BTreeMap::new(),
        body_json: Some(json!({
            "id": "resp_1",
            "status": "completed",
            "error": null,
            "usage": {
                "input_tokens": 24,
                "output_tokens": 11,
                "total_tokens": 35
            }
        })),
        client_body_json: Some(json!({
            "type": "message",
            "usage": {
                "input_tokens": 24,
                "output_tokens": 11
            }
        })),
        body_base64: None,
        telemetry: None,
    };

    let event = build_sync_terminal_usage_event(&plan, payload.report_context.as_ref(), &payload)
        .expect("usage event should build");

    assert_eq!(event.event_type, UsageEventType::Completed);
    assert_eq!(event.data.status_code, Some(200));
    assert_eq!(event.data.input_tokens, Some(24));
    assert_eq!(event.data.output_tokens, Some(11));
}

#[test]
fn manual_terminal_seed_event_builder_sanitizes_metadata_but_preserves_headers_and_bodies() {
    let event = build_terminal_usage_event_from_seed(TerminalUsageSeed {
        token_measurement_source: "reported",
        terminal_state: UsageTerminalState::Completed,
        client_contract: "openai:chat".to_string(),
        provider_contract: "openai:chat".to_string(),
        request_id: "req-manual-seed-1".to_string(),
        user_id: Some("user-1".to_string()),
        api_key_id: Some("key-1".to_string()),
        username: Some("alice".to_string()),
        api_key_name: Some("primary".to_string()),
        provider_name: "OpenAI".to_string(),
        model: "gpt-5".to_string(),
        target_model: None,
        model_id: None,
        global_model_id: None,
        provider_id: Some("provider-1".to_string()),
        provider_endpoint_id: Some("endpoint-1".to_string()),
        provider_api_key_id: Some("upstream-key-1".to_string()),
        request_type: "chat".to_string(),
        has_format_conversion: false,
        is_stream: false,
        body_refs: UsageBodyRefsSeed::default(),
        body_states: UsageBodyStatesSeed::default(),
        routing: UsageRoutingSeed {
            candidate_id: Some("cand-1".to_string()),
            ..UsageRoutingSeed::default()
        },
        status_code: 200,
        terminal_error_message: None,
        terminal_failure_category: None,
        response_time_ms: Some(123),
        first_byte_time_ms: Some(45),
        request_headers: Some(json!({
            "authorization": "Bearer very-secret-token",
            "accept": "application/json"
        })),
        request_body: Some(json!({
            "payload": "x".repeat(MAX_USAGE_CAPTURE_BYTES + 1)
        })),
        provider_request_headers: Some(json!({
            "x-api-key": "sk-proj-super-secret"
        })),
        provider_request: None,
        provider_response_headers: Some(json!({
            "set-cookie": "session=extremely-secret-cookie"
        })),
        provider_response: Some(json!({
            "usage": {
                "input_tokens": 1,
                "output_tokens": 2,
                "total_tokens": 3
            }
        })),
        client_response_headers: Some(json!({
            "authorization": "Bearer client-secret"
        })),
        client_response: None,
        request_metadata: Some(json!({
            "candidate_id": "cand-1",
            "billing_snapshot": {
                "payload": "x".repeat(32 * 1024)
            }
        })),
        audit_payload: None,
        standardized_usage: None,
    })
    .expect("usage event should build");

    assert_eq!(
        event.data.request_headers,
        Some(json!({
            "authorization": "Bearer very-secret-token",
            "accept": "application/json"
        }))
    );
    assert_eq!(
        event.data.provider_request_headers,
        Some(json!({
            "x-api-key": "sk-proj-super-secret"
        }))
    );
    assert_eq!(
        event.data.response_headers,
        Some(json!({
            "set-cookie": "session=extremely-secret-cookie"
        }))
    );
    assert_eq!(
        event.data.client_response_headers,
        Some(json!({
            "authorization": "Bearer client-secret"
        }))
    );
    assert_eq!(
        event.data.request_body,
        Some(json!({
            "payload": "x".repeat(MAX_USAGE_CAPTURE_BYTES + 1)
        }))
    );
    assert_eq!(event.data.input_tokens, Some(1));
    assert_eq!(event.data.output_tokens, Some(2));
    assert_eq!(event.data.total_tokens, Some(3));
    assert_eq!(event.data.candidate_id.as_deref(), Some("cand-1"));
    assert_eq!(
        event
            .data
            .request_metadata
            .as_ref()
            .and_then(|value| value.get("candidate_id")),
        None
    );
    assert_eq!(
        event
            .data
            .request_metadata
            .as_ref()
            .and_then(|value| value.get("billing_snapshot")),
        None
    );
}

#[test]
fn lifecycle_seed_builder_sanitizes_manual_request_metadata() {
    let record = build_pending_usage_record_from_seed(
        &LifecycleUsageSeed {
            request_id: "req-lifecycle-manual-1".to_string(),
            user_id: Some("user-1".to_string()),
            api_key_id: Some("key-1".to_string()),
            username: Some("alice".to_string()),
            api_key_name: Some("primary".to_string()),
            provider_name: "OpenAI".to_string(),
            model: "gpt-5".to_string(),
            target_model: None,
            model_id: None,
            global_model_id: None,
            provider_id: Some("provider-1".to_string()),
            provider_endpoint_id: Some("endpoint-1".to_string()),
            provider_api_key_id: Some("upstream-key-1".to_string()),
            request_type: "chat".to_string(),
            api_format: Some("openai:chat".to_string()),
            api_family: Some("openai".to_string()),
            endpoint_kind: Some("chat".to_string()),
            endpoint_api_format: Some("openai:chat".to_string()),
            provider_api_family: Some("openai".to_string()),
            provider_endpoint_kind: Some("chat".to_string()),
            has_format_conversion: Some(false),
            is_stream: false,
            body_states: UsageBodyStatesSeed::default(),
            routing: UsageRoutingSeed {
                candidate_id: Some("cand-1".to_string()),
                ..UsageRoutingSeed::default()
            },
            request_metadata: Some(json!({
                "billing_snapshot": {
                    "payload": "x".repeat(32 * 1024)
                }
            })),
        },
        1_700_000_000,
    )
    .expect("pending record should build");

    assert_eq!(record.candidate_id.as_deref(), Some("cand-1"));
    assert_eq!(record.request_metadata, None);
}

#[test]
fn usage_event_data_seed_preserves_headers_before_outcome_paths_use_it() {
    let plan = ExecutionPlan {
        request_id: "req-seed-sanitize-1".to_string(),
        candidate_id: Some("cand-seed-sanitize-1".to_string()),
        provider_name: Some("OpenAI".to_string()),
        provider_id: "provider-1".to_string(),
        endpoint_id: "endpoint-1".to_string(),
        key_id: "key-1".to_string(),
        method: "POST".to_string(),
        url: "https://example.com/v1/chat/completions".to_string(),
        headers: BTreeMap::new(),
        content_type: Some("application/json".to_string()),
        content_encoding: None,
        body: RequestBody::from_json(json!({"model": "gpt-5"})),
        stream: false,
        client_api_format: "openai:chat".to_string(),
        provider_api_format: "openai:chat".to_string(),
        model_name: Some("gpt-5".to_string()),
        proxy: None,
        transport_profile: None,
        timeouts: None,
    };

    let data = build_usage_event_data_seed(
        &plan,
        Some(&json!({
            "original_headers": {
                "authorization": "Bearer outcome-secret",
                "accept": "application/json"
            },
            "original_request_body": {
                "payload": "x".repeat(MAX_USAGE_CAPTURE_BYTES + 1)
            }
        })),
    );

    assert_eq!(
        data.request_headers,
        Some(json!({
            "authorization": "Bearer outcome-secret",
            "accept": "application/json"
        }))
    );
    assert_eq!(
        data.request_body,
        Some(json!({
            "payload": "x".repeat(MAX_USAGE_CAPTURE_BYTES + 1)
        }))
    );
}

#[test]
fn usage_event_data_seed_preserves_timing_and_runtime_body_metadata() {
    let plan = ExecutionPlan {
        request_id: "req-seed-metadata-1".to_string(),
        candidate_id: Some("cand-seed-metadata-1".to_string()),
        provider_name: Some("OpenAI".to_string()),
        provider_id: "provider-1".to_string(),
        endpoint_id: "endpoint-1".to_string(),
        key_id: "key-1".to_string(),
        method: "POST".to_string(),
        url: "https://example.com/v1/chat/completions".to_string(),
        headers: BTreeMap::new(),
        content_type: Some("application/json".to_string()),
        content_encoding: None,
        body: RequestBody::from_json(json!({"model": "gpt-5"})),
        stream: false,
        client_api_format: "openai:chat".to_string(),
        provider_api_format: "openai:chat".to_string(),
        model_name: Some("gpt-5".to_string()),
        proxy: None,
        transport_profile: None,
        timeouts: None,
    };

    let data = build_usage_event_data_seed(
        &plan,
        Some(&json!({
            "trace_id": "trace-seed-metadata-1",
            "end_to_end_time_ms": 10_626,
            "end_to_end_first_byte_time_ms": 10_120,
            "db_timings_ms": {"query_count": 2},
            "original_request_body": {"messages": []}
        })),
    );
    let metadata = data
        .request_metadata
        .as_ref()
        .and_then(Value::as_object)
        .expect("request metadata should be preserved");

    assert_eq!(metadata.get("end_to_end_time_ms"), Some(&json!(10_626)));
    assert_eq!(
        metadata.get("end_to_end_first_byte_time_ms"),
        Some(&json!(10_120))
    );
    assert_eq!(metadata.get("db_timings_ms"), None);
    assert_eq!(
        metadata.get("trace_id"),
        Some(&json!("trace-seed-metadata-1"))
    );
    let body_size = metadata
        .get("body_size")
        .and_then(Value::as_object)
        .expect("runtime body size metadata should remain");
    assert!(body_size.get("client_request_body").is_some());
    assert!(body_size.get("provider_request_body").is_some());
}

#[test]
fn describing_request_bodies_matches_the_capturing_seed_apart_from_the_bodies() {
    let plan = ExecutionPlan {
        request_id: "req-seed-describe-1".to_string(),
        candidate_id: Some("cand-seed-describe-1".to_string()),
        provider_name: Some("OpenAI".to_string()),
        provider_id: "provider-1".to_string(),
        endpoint_id: "endpoint-1".to_string(),
        key_id: "key-1".to_string(),
        method: "POST".to_string(),
        url: "https://example.com/v1/chat/completions".to_string(),
        headers: BTreeMap::new(),
        content_type: Some("application/json".to_string()),
        content_encoding: None,
        body: RequestBody::from_json(json!({
            "model": "gpt-5",
            "service_tier": "priority",
            "reasoning": {"effort": "high"}
        })),
        stream: false,
        client_api_format: "openai:chat".to_string(),
        provider_api_format: "openai:chat".to_string(),
        model_name: Some("gpt-5".to_string()),
        proxy: None,
        transport_profile: None,
        timeouts: None,
    };
    let report_context = json!({
        "client_api_format": "openai:chat",
        "provider_api_format": "openai:chat",
        "original_request_body": {"model": "gpt-5", "messages": []},
        "original_headers": {"accept": "application/json"}
    });

    let captured = build_usage_event_data_seed(&plan, Some(&report_context));
    let described =
        build_usage_event_data_seed_describing_request_bodies(&plan, Some(&report_context));

    // Only the two heavy values differ.
    assert!(captured.request_body.is_some());
    assert!(captured.provider_request_body.is_some());
    assert_eq!(described.request_body, None);
    assert_eq!(described.provider_request_body, None);

    // Everything a terminal write reads to decide what to do with the stored
    // capture is identical, so the described seed preserves it rather than
    // clearing it.
    assert_eq!(
        described.request_body_state,
        Some(UsageBodyCaptureState::Inline)
    );
    assert_eq!(
        described.provider_request_body_state,
        Some(UsageBodyCaptureState::Inline)
    );
    assert_eq!(described.request_body_state, captured.request_body_state);
    assert_eq!(
        described.provider_request_body_state,
        captured.provider_request_body_state
    );
    assert_eq!(described.request_body_ref, captured.request_body_ref);
    assert_eq!(
        described.provider_request_body_ref,
        captured.provider_request_body_ref
    );
    assert_eq!(described.request_type, captured.request_type);
    assert_eq!(described.request_metadata, captured.request_metadata);
    assert_eq!(
        described.provider_request_headers,
        captured.provider_request_headers
    );
    assert_eq!(described.request_headers, captured.request_headers);
}

#[test]
fn describing_request_bodies_keeps_the_unavailable_marker_for_raw_bodies() {
    let mut plan = ExecutionPlan {
        request_id: "req-seed-describe-2".to_string(),
        candidate_id: None,
        provider_name: Some("OpenAI".to_string()),
        provider_id: "provider-1".to_string(),
        endpoint_id: "endpoint-1".to_string(),
        key_id: "key-1".to_string(),
        method: "POST".to_string(),
        url: "https://example.com/v1/chat/completions".to_string(),
        headers: BTreeMap::new(),
        content_type: Some("application/json".to_string()),
        content_encoding: None,
        body: RequestBody::from_json(json!({"model": "gpt-5"})),
        stream: false,
        client_api_format: "openai:chat".to_string(),
        provider_api_format: "openai:chat".to_string(),
        model_name: Some("gpt-5".to_string()),
        proxy: None,
        transport_profile: None,
        timeouts: None,
    };
    plan.body.json_body = None;
    plan.body.body_bytes_b64 = Some("eyJtb2RlbCI6ICJncHQtNSJ9".to_string());

    let described = build_usage_event_data_seed_describing_request_bodies(&plan, None);

    assert_eq!(
        described.provider_request_body_state,
        Some(UsageBodyCaptureState::Unavailable)
    );
}

#[test]
fn headers_to_json_preserves_original_headers_at_source() {
    let mut headers = BTreeMap::new();
    headers.insert(
        "authorization".to_string(),
        "Bearer eyJhbGciOiJSUzI1NiJ9.body.signature".to_string(),
    );
    headers.insert("user-agent".to_string(), "codex-tui/0.1".to_string());
    headers.insert(
        "x-api-key".to_string(),
        "sk-proj-1234567890abcdef".to_string(),
    );

    let value = headers_to_json(&headers).expect("expected object");
    let object = value.as_object().expect("expected object");

    let auth = object
        .get("authorization")
        .and_then(|v| v.as_str())
        .expect("authorization should be string");
    assert_eq!(auth, "Bearer eyJhbGciOiJSUzI1NiJ9.body.signature");

    let api_key = object
        .get("x-api-key")
        .and_then(|v| v.as_str())
        .expect("x-api-key should be string");
    assert_eq!(api_key, "sk-proj-1234567890abcdef");

    assert_eq!(
        object.get("user-agent").and_then(|v| v.as_str()),
        Some("codex-tui/0.1"),
    );
}

#[test]
fn headers_to_json_returns_none_for_empty_headers() {
    assert!(headers_to_json(&BTreeMap::new()).is_none());
}

#[test]
fn validate_usage_headers_in_json_value_handles_object_form() {
    let value = json!({
        "Authorization": "Bearer eyJhbGciOiJSUzI1NiJ9.body.signature",
        "Cookie": "session=verylongcookievalue1234",
        "Accept": "application/json",
    });
    let captured =
        validate_usage_headers_in_json_value(Some(value)).expect("captured value should exist");
    let object = captured.as_object().expect("expected object");

    let auth = object
        .get("Authorization")
        .and_then(|v| v.as_str())
        .expect("Authorization should be string");
    assert_eq!(auth, "Bearer eyJhbGciOiJSUzI1NiJ9.body.signature");

    let cookie = object
        .get("Cookie")
        .and_then(|v| v.as_str())
        .expect("Cookie should be string");
    assert_eq!(cookie, "session=verylongcookievalue1234");

    assert_eq!(
        object.get("Accept").and_then(|v| v.as_str()),
        Some("application/json"),
    );
}

#[test]
fn validate_usage_headers_discards_non_object() {
    // None 输入返回 None
    assert!(validate_usage_headers_in_json_value(None).is_none());
    // 非 object 不是可验证的 header map，直接丢弃。
    let masked = validate_usage_headers_in_json_value(Some(json!("not an object")));
    assert_eq!(masked, None);
}

#[test]
fn resolve_error_message_extracts_generic_message_from_base64_body() {
    let body_base64 = base64::engine::general_purpose::STANDARD.encode(
        serde_json::to_vec(&json!({
            "message": "upstream exploded"
        }))
        .expect("json should serialize"),
    );

    assert_eq!(
        resolve_error_message(500, None, Some(body_base64.as_str())),
        Some("upstream exploded".to_string()),
    );
}

#[test]
fn decode_body_for_storage_extracts_connect_json_error_frames() {
    let payload = br#"{"error":{"code":"resource_exhausted","message":"quota exhausted"}}"#;
    let mut framed = Vec::new();
    framed.push(2);
    framed.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    framed.extend_from_slice(payload);
    let body_base64 = base64::engine::general_purpose::STANDARD.encode(framed);

    assert_eq!(
        decode_body_for_storage(Some(body_base64.as_str())),
        Some(json!({
            "error": {
                "code": "resource_exhausted",
                "message": "quota exhausted",
                "type": "resource_exhausted"
            }
        }))
    );
}

#[test]
fn parse_sse_body_for_storage_handles_crlf_and_cr_line_endings() {
    let sse_body = concat!(
        "data: {\"id\":\"chunk-1\"}\r\n",
        "\r\n",
        "data: {\"id\":\"chunk-2\"}\r",
        "\r",
        "data: [DONE]\r",
    );

    assert_eq!(
        parse_sse_body_for_storage(sse_body),
        Some(json!({
            "chunks": [
                {"id": "chunk-1"},
                {"id": "chunk-2"}
            ],
            "metadata": {
                "stream": true,
                "total_chunks": 2,
                "stored_chunks": 2,
                "content_length": sse_body.len(),
                "has_completion": true
            }
        })),
    );
}

#[test]
fn parse_sse_body_for_storage_reports_bad_event_before_valid_terminal() {
    let body = concat!(
        "data: {\"tools\":[}\n\n",
        "data: {\"type\":\"response.completed\"}\n\n",
    );
    let parsed = parse_sse_body_for_storage(body).unwrap();
    assert_eq!(parsed["metadata"]["dropped_chunks"], 1);
    assert_eq!(parsed["metadata"]["first_parse_error"]["chunk_index"], 0);
    assert!(parsed["metadata"]["first_parse_error"]["message"].is_string());
    assert_eq!(parsed["chunks"][0]["type"], "response.completed");
    assert!(parsed.get("raw_response").is_none());
}

#[test]
fn extract_token_counts_from_value_handles_crlf_and_cr_sse_text() {
    let sse_body = concat!(
        "event: response.created\r\n",
        "data: {\"type\":\"response.created\"}\r\n",
        "\r\n",
        "event: response.completed\r",
        "data: {\"type\":\"response.completed\",\"response\":{\"usage\":{\"input_tokens\":3,\"input_tokens_details\":{\"cached_tokens\":2,\"cached_creation_tokens\":1},\"output_tokens\":5,\"total_tokens\":8}}}\r",
        "\r",
        "data: [DONE]\r",
    );

    assert_eq!(
        extract_token_counts_from_value(&Value::String(sse_body.to_string())),
        Some((3, 5, 8)),
    );
}
