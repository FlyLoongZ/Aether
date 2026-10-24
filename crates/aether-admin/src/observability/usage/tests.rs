use std::collections::BTreeMap;

use serde_json::{json, Value};

use super::{
    admin_usage_active_request_json, admin_usage_client_is_stream, admin_usage_has_body_value,
    admin_usage_has_fallback, admin_usage_is_failed, admin_usage_is_success,
    admin_usage_matches_search, admin_usage_matches_status, admin_usage_matches_username,
    admin_usage_record_json, admin_usage_resolve_request_capture_body,
    admin_usage_safe_metadata_value, admin_usage_total_tokens, admin_usage_upstream_is_stream,
    build_admin_usage_detail_payload,
};
use aether_data_contracts::repository::usage::{StoredRequestUsageAudit, UsageBodyField};

fn sample_usage(
    status: &str,
    status_code: Option<i32>,
    error_message: Option<&str>,
) -> StoredRequestUsageAudit {
    StoredRequestUsageAudit::new(
        "usage-1".to_string(),
        "req-1".to_string(),
        Some("user-1".to_string()),
        Some("api-key-1".to_string()),
        Some("alice".to_string()),
        Some("default".to_string()),
        "OpenAI".to_string(),
        "gpt-5".to_string(),
        None,
        Some("provider-1".to_string()),
        Some("endpoint-1".to_string()),
        Some("provider-key-1".to_string()),
        Some("chat".to_string()),
        Some("openai:chat".to_string()),
        Some("openai".to_string()),
        Some("chat".to_string()),
        Some("openai:chat".to_string()),
        Some("openai".to_string()),
        Some("chat".to_string()),
        false,
        false,
        10,
        20,
        30,
        0.0,
        0.0,
        status_code,
        error_message.map(ToOwned::to_owned),
        None,
        Some(120),
        None,
        status.to_string(),
        "settled".to_string(),
        100,
        101,
        Some(102),
    )
    .expect("usage should build")
}

#[test]
fn explicit_completed_status_wins_over_legacy_failure_fields() {
    let item = sample_usage(
        "completed",
        Some(429),
        Some("rate limited on first attempt"),
    );
    assert!(!admin_usage_is_failed(&item));
    assert!(!admin_usage_matches_status(&item, Some("failed")));
    assert!(admin_usage_matches_status(&item, Some("completed")));
}

#[test]
fn admin_usage_summaries_do_not_return_historical_raw_error_text() {
    let item = StoredRequestUsageAudit {
        error_category: Some("authentication_error".to_string()),
        ..sample_usage(
            "failed",
            Some(401),
            Some(
                "upstream said Authorization: Bearer live-secret at https://api.example?key=secret",
            ),
        )
    };

    let record = admin_usage_record_json(
        &item,
        &BTreeMap::new(),
        &BTreeMap::new(),
        false,
        false,
        None,
    );
    let active = admin_usage_active_request_json(&item, None, None, None);

    assert_eq!(record["error_message"], "authentication_error");
    assert_eq!(active["error_message"], "authentication_error");
    assert!(!record.to_string().contains("live-secret"));
    assert!(!active.to_string().contains("live-secret"));
}

#[test]
fn client_requested_stream_prefers_request_metadata_flag() {
    let item = StoredRequestUsageAudit {
        is_stream: true,
        request_metadata: Some(json!({
            "client_requested_stream": false
        })),
        ..sample_usage("completed", Some(200), None)
    };

    assert!(!admin_usage_client_is_stream(&item));

    let record = admin_usage_record_json(
        &item,
        &BTreeMap::new(),
        &BTreeMap::new(),
        false,
        false,
        None,
    );
    assert_eq!(record["is_stream"], true);
    assert_eq!(record["upstream_is_stream"], true);
    assert_eq!(record["client_requested_stream"], false);
    assert_eq!(record["client_is_stream"], false);
}

#[test]
fn admin_usage_payloads_preserve_routing_group_snapshot_and_precise_display_cost() {
    for (metadata, multiplier, cost, group_name) in [
        (None, 1.0, json!(0.0), Value::Null),
        (
            Some(json!({"routing_group_billing_multiplier": 0.0})),
            0.0,
            json!(0.0),
            Value::Null,
        ),
        (
            Some(json!({
                "routing_group_billing_multiplier": 2.5,
                "routing_group_id": "group-1",
                "routing_group_name": "请求时的分组",
                "rate_multiplier": 0.5
            })),
            2.5,
            json!(0.000004),
            json!("请求时的分组"),
        ),
        (
            Some(json!({
                "billing_multiplier_snapshot": {"version": 1, "factors": {"routing_group": 2.5, "user_group": 2.0}, "multiplier": 5.0},
                "routing_group_billing_multiplier": 2.5,
                "routing_group_id": "group-1",
                "routing_group_name": "请求时的分组",
                "rate_multiplier": 0.5
            })),
            5.0,
            json!(0.000007),
            json!("请求时的分组"),
        ),
    ] {
        let item = StoredRequestUsageAudit {
            total_cost_usd: 0.00000149,
            actual_total_cost_usd: 0.0000002,
            request_metadata: metadata,
            ..sample_usage("completed", Some(200), None)
        };
        let record = admin_usage_record_json(
            &item,
            &BTreeMap::new(),
            &BTreeMap::new(),
            false,
            false,
            None,
        );
        let active = admin_usage_active_request_json(&item, None, None, None);
        let detail = build_admin_usage_detail_payload(
            &item,
            &BTreeMap::new(),
            &BTreeMap::new(),
            false,
            false,
            None,
            false,
            None,
            &BTreeMap::new(),
        );
        for payload in [&record, &active, &detail] {
            assert_eq!(payload["billing_multiplier"], multiplier);
            assert_eq!(payload["billing_cost"], cost);
            assert_eq!(payload["routing_group_name"], group_name);
            assert_eq!(
                payload["routing_group_id"],
                if group_name.is_null() {
                    Value::Null
                } else {
                    json!("group-1")
                }
            );
            assert_eq!(
                payload["rate_multiplier"],
                if group_name.is_null() {
                    Value::Null
                } else {
                    json!(0.5)
                }
            );
            assert_eq!(payload["cost"], 0.000001);
            assert_eq!(payload["actual_cost"], 0.0);
        }
    }
    let item = StoredRequestUsageAudit {
        total_cost_usd: f64::MAX,
        request_metadata: Some(json!({"routing_group_billing_multiplier": 2.0})),
        ..sample_usage("completed", Some(200), None)
    };
    assert!(admin_usage_active_request_json(&item, None, None, None)["billing_cost"].is_null());
}

#[test]
fn admin_usage_payloads_expose_response_model_separately_from_mapping() {
    let item = StoredRequestUsageAudit {
        target_model: Some("provider-mapped-model".to_string()),
        request_metadata: Some(json!({
            "provider_response_model": "gpt-5.1"
        })),
        ..sample_usage("completed", Some(200), None)
    };

    let record = admin_usage_record_json(
        &item,
        &BTreeMap::new(),
        &BTreeMap::new(),
        false,
        false,
        None,
    );
    let active = admin_usage_active_request_json(&item, None, None, None);

    for payload in [&record, &active] {
        assert_eq!(payload["target_model"], "provider-mapped-model");
        assert_eq!(payload["response_model"], "gpt-5.1");
    }
}

#[test]
fn admin_usage_payloads_project_end_to_end_timings_from_metadata() {
    let item = StoredRequestUsageAudit {
        response_time_ms: Some(626),
        first_byte_time_ms: Some(120),
        request_metadata: Some(json!({
            "end_to_end_time_ms": 10_626,
            "end_to_end_first_byte_time_ms": 10_120,
        })),
        ..sample_usage("completed", Some(200), None)
    };

    let record = admin_usage_record_json(
        &item,
        &BTreeMap::new(),
        &BTreeMap::new(),
        false,
        false,
        None,
    );
    let active = admin_usage_active_request_json(&item, None, None, None);

    for payload in [&record, &active] {
        assert_eq!(payload["response_time_ms"], 626);
        assert_eq!(payload["first_byte_time_ms"], 120);
        assert_eq!(payload["end_to_end_time_ms"], 10_626);
        assert_eq!(payload["end_to_end_first_byte_time_ms"], 10_120);
    }
}

#[test]
fn admin_usage_payloads_expose_websocket_transport() {
    let item = StoredRequestUsageAudit {
        request_metadata: Some(json!({
            "websocket_mode": true,
            "websocket_transport": "responses",
            "usage_available": false,
            "usage_pricing_available": false,
            "realtime_session": {
                "input_audio_tokens": 7,
                "output_audio_tokens": 3,
            },
        })),
        ..sample_usage("completed", Some(200), None)
    };

    let record = admin_usage_record_json(
        &item,
        &BTreeMap::new(),
        &BTreeMap::new(),
        false,
        false,
        None,
    );
    let active = admin_usage_active_request_json(&item, None, None, None);

    assert_eq!(record["is_websocket"], true);
    assert_eq!(active["is_websocket"], true);
    assert_eq!(record["websocket_transport"], "responses");
    assert_eq!(active["websocket_transport"], "responses");
    assert_eq!(record["usage_available"], false);
    assert_eq!(active["usage_available"], false);
    assert_eq!(record["usage_pricing_available"], false);
    assert_eq!(active["usage_pricing_available"], false);
    assert_eq!(record["input_audio_tokens"], 7);
    assert_eq!(active["input_audio_tokens"], 7);
    assert_eq!(record["output_audio_tokens"], 3);
    assert_eq!(active["output_audio_tokens"], 3);
    assert!(admin_usage_matches_status(&item, Some("websocket")));
    assert!(admin_usage_matches_status(&item, Some("ws")));
    assert!(admin_usage_matches_status(&item, Some("WS")));
    assert!(!admin_usage_matches_status(&item, Some("standard")));
    assert!(!admin_usage_matches_status(&item, Some("stream")));
}

#[test]
fn admin_usage_detail_preserves_websocket_and_session_metadata() {
    let live_session = json!({
        "schema_version": "1",
        "transport": "websocket",
        "mode": "direct",
        "state": "closed",
        "client_frames": 4,
        "upstream_frames": 8,
    });
    let realtime_session = json!({
        "schema_version": "1",
        "transport": "websocket",
        "usage_state": "authoritative",
        "input_audio_tokens": 7,
        "output_audio_tokens": 3,
    });
    let item = StoredRequestUsageAudit {
        request_type: Some("live".to_string()),
        api_format: Some("codex:live".to_string()),
        endpoint_api_format: Some("codex:live".to_string()),
        is_stream: true,
        request_metadata: Some(json!({
            "websocket_mode": true,
            "websocket_transport": "codex_live_direct",
            "usage_available": false,
            "usage_pricing_available": false,
            "live_session": live_session,
            "realtime_session": realtime_session,
        })),
        ..sample_usage("completed", Some(200), None)
    };

    let payload = build_admin_usage_detail_payload(
        &item,
        &BTreeMap::new(),
        &BTreeMap::new(),
        false,
        false,
        None,
        false,
        None,
        &BTreeMap::new(),
    );

    assert_eq!(payload["is_websocket"], true);
    assert_eq!(payload["websocket_transport"], "codex_live_direct");
    assert_eq!(payload["live_session"], live_session);
    assert_eq!(payload["realtime_session"], realtime_session);
    assert_eq!(payload["metadata"]["live_session"], live_session);
    assert_eq!(payload["metadata"]["realtime_session"], realtime_session);
}

#[test]
fn admin_usage_safe_metadata_keeps_only_numeric_realtime_audio_counters() {
    let projected = admin_usage_safe_metadata_value(&json!({
        "input_audio_tokens": 99,
        "refresh_token": "should-be-removed",
        "realtime_session": {
            "input_audio_tokens": 7,
            "output_audio_tokens": 3,
            "input_audio_tokens_text": "should-be-removed",
            "refresh_token": 11,
            "nested": {
                "output_audio_tokens": 5
            }
        }
    }));

    assert!(projected.get("input_audio_tokens").is_none());
    assert!(projected.get("refresh_token").is_none());
    assert_eq!(projected["realtime_session"]["input_audio_tokens"], 7);
    assert_eq!(projected["realtime_session"]["output_audio_tokens"], 3);
    assert!(projected["realtime_session"]
        .get("input_audio_tokens_text")
        .is_none());
    assert!(projected["realtime_session"].get("refresh_token").is_none());
    assert!(projected["realtime_session"]["nested"]
        .get("output_audio_tokens")
        .is_none());
}

#[test]
fn admin_usage_record_infers_client_family_from_user_agent() {
    let item = StoredRequestUsageAudit {
        request_metadata: Some(json!({
            "client_ip": "192.168.0.28",
            "user_agent": "codex_vscode/0.131.0-alpha.9 (Windows 10.0.26200; x86_64)"
        })),
        ..sample_usage("completed", Some(200), None)
    };

    let record = admin_usage_record_json(
        &item,
        &BTreeMap::new(),
        &BTreeMap::new(),
        false,
        false,
        None,
    );
    let active = admin_usage_active_request_json(&item, None, None, None);

    assert_eq!(record["client_family"], "codex_vscode");
    assert_eq!(record["client_ip"], "192.168.0.28");
    assert_eq!(active["client_family"], "codex_vscode");
}

#[test]
fn admin_usage_record_labels_openai_js_user_agent_as_sdk() {
    let item = StoredRequestUsageAudit {
        request_metadata: Some(json!({
            "user_agent": "OpenAI/JS 6.34.0"
        })),
        ..sample_usage("completed", Some(200), None)
    };

    let record = admin_usage_record_json(
        &item,
        &BTreeMap::new(),
        &BTreeMap::new(),
        false,
        false,
        None,
    );

    assert_eq!(record["client_family"], "openai_js_sdk");
}

#[test]
fn admin_usage_record_prefers_typed_client_family() {
    let item = StoredRequestUsageAudit {
        client_family: Some("codex".to_string()),
        request_metadata: Some(json!({
            "user_agent": "OpenAI/JS 6.34.0"
        })),
        ..sample_usage("completed", Some(200), None)
    };

    let record = admin_usage_record_json(
        &item,
        &BTreeMap::new(),
        &BTreeMap::new(),
        false,
        false,
        None,
    );

    assert_eq!(record["client_family"], "codex");
}

#[test]
fn admin_usage_record_includes_requested_and_provider_reasoning_efforts() {
    let item = StoredRequestUsageAudit {
        request_body: Some(json!({
            "reasoning": { "effort": "xhigh" }
        })),
        provider_request_body: Some(json!({
            "reasoning": { "effort": "max" },
            "service_tier": "priority"
        })),
        ..sample_usage("completed", Some(200), None)
    };

    let record = admin_usage_record_json(
        &item,
        &BTreeMap::new(),
        &BTreeMap::new(),
        false,
        false,
        None,
    );
    let active = admin_usage_active_request_json(&item, None, None, None);

    assert_eq!(record["requested_reasoning_effort"], "xhigh");
    assert_eq!(active["requested_reasoning_effort"], "xhigh");
    assert_eq!(record["reasoning_effort"], "max");
    assert_eq!(active["reasoning_effort"], "max");
    assert_eq!(record["service_tier"], "priority");
    assert_eq!(active["service_tier"], "priority");
    assert!(record["updated_at"].is_string());
    assert_eq!(record["updated_at"], active["updated_at"]);
}

#[test]
fn client_requested_stream_uses_stream_generate_content_path_over_stale_metadata_flag() {
    let item = StoredRequestUsageAudit {
        is_stream: true,
        request_metadata: Some(json!({
            "client_requested_stream": false,
            "request_path": "/v1beta/models/gemini-3.1-flash-image-preview:streamGenerateContent",
            "request_path_and_query": "/v1beta/models/gemini-3.1-flash-image-preview:streamGenerateContent?alt=sse"
        })),
        ..sample_usage("completed", Some(200), None)
    };

    assert!(admin_usage_client_is_stream(&item));

    let record = admin_usage_record_json(
        &item,
        &BTreeMap::new(),
        &BTreeMap::new(),
        false,
        false,
        None,
    );
    assert_eq!(record["upstream_is_stream"], true);
    assert_eq!(record["client_requested_stream"], true);
    assert_eq!(record["client_is_stream"], true);
}

#[test]
fn client_requested_stream_falls_back_to_request_body_stream_flag() {
    let item = StoredRequestUsageAudit {
        is_stream: true,
        request_body: Some(json!({
            "model": "gpt-5.4",
            "stream": false
        })),
        ..sample_usage("completed", Some(200), None)
    };

    assert!(!admin_usage_client_is_stream(&item));

    let active = admin_usage_active_request_json(&item, None, None, None);
    assert_eq!(active["is_stream"], true);
    assert_eq!(active["upstream_is_stream"], true);
    assert_eq!(active["client_requested_stream"], false);
    assert_eq!(active["client_is_stream"], false);
}

#[test]
fn client_requested_stream_defaults_to_non_stream_for_openai_responses_request_body_without_flag() {
    let item = StoredRequestUsageAudit {
        is_stream: true,
        api_format: Some("openai:responses".to_string()),
        request_body: Some(json!({
            "model": "gpt-5.4",
            "input": [{"role": "user", "content": "hi"}],
            "store": false
        })),
        ..sample_usage("completed", Some(200), None)
    };

    assert!(!admin_usage_client_is_stream(&item));

    let record = admin_usage_record_json(
        &item,
        &BTreeMap::new(),
        &BTreeMap::new(),
        false,
        false,
        None,
    );
    assert_eq!(record["is_stream"], true);
    assert_eq!(record["upstream_is_stream"], true);
    assert_eq!(record["client_requested_stream"], false);
    assert_eq!(record["client_is_stream"], false);
}

#[test]
fn client_requested_stream_defaults_to_non_stream_for_openai_search() {
    let item = StoredRequestUsageAudit {
        is_stream: true,
        api_format: Some("openai:search".to_string()),
        request_body: Some(json!({
            "id": "session-search-1",
            "model": "gpt-5.6-sol",
            "input": "current documentation"
        })),
        ..sample_usage("completed", Some(200), None)
    };

    assert!(!admin_usage_client_is_stream(&item));

    let record = admin_usage_record_json(
        &item,
        &BTreeMap::new(),
        &BTreeMap::new(),
        false,
        false,
        None,
    );
    assert_eq!(record["client_requested_stream"], false);
    assert_eq!(record["client_is_stream"], false);
}

#[test]
fn upstream_stream_prefers_request_metadata_flag() {
    let item = StoredRequestUsageAudit {
        is_stream: false,
        request_metadata: Some(json!({
            "client_requested_stream": false,
            "upstream_is_stream": true
        })),
        ..sample_usage("completed", Some(200), None)
    };

    let record = admin_usage_record_json(
        &item,
        &BTreeMap::new(),
        &BTreeMap::new(),
        false,
        false,
        None,
    );
    assert_eq!(record["is_stream"], false);
    assert_eq!(record["upstream_is_stream"], true);
    assert_eq!(record["client_requested_stream"], false);
    assert_eq!(record["client_is_stream"], false);
}

#[test]
fn stream_modes_fall_back_to_captured_response_bodies_when_request_metadata_is_missing() {
    let item = StoredRequestUsageAudit {
        is_stream: true,
        response_body: Some(json!({
            "chunks": [
                {"type": "response.created"},
                {"type": "response.output_text.delta", "delta": "Hello"}
            ],
            "metadata": {
                "stream": true,
                "stored_chunks": 2,
                "total_chunks": 2
            }
        })),
        client_response_body: Some(json!({
            "id": "resp-1",
            "object": "response",
            "status": "completed",
            "output": []
        })),
        ..sample_usage("completed", Some(200), None)
    };

    assert!(!admin_usage_client_is_stream(&item));
    assert!(admin_usage_upstream_is_stream(&item));

    let record = admin_usage_record_json(
        &item,
        &BTreeMap::new(),
        &BTreeMap::new(),
        false,
        false,
        None,
    );
    assert_eq!(record["is_stream"], true);
    assert_eq!(record["upstream_is_stream"], true);
    assert_eq!(record["client_requested_stream"], false);
    assert_eq!(record["client_is_stream"], false);
}

#[test]
fn stream_modes_fall_back_to_captured_response_headers_when_bodies_are_detached() {
    let item = StoredRequestUsageAudit {
        is_stream: true,
        response_headers: Some(json!({
            "content-type": "text/event-stream; charset=utf-8"
        })),
        client_response_headers: Some(json!({
            "content-type": "application/json"
        })),
        response_body_ref: Some("usage://request/req-1/response_body".to_string()),
        client_response_body_ref: Some("usage://request/req-1/client_response_body".to_string()),
        ..sample_usage("completed", Some(200), None)
    };

    assert!(!admin_usage_client_is_stream(&item));
    assert!(admin_usage_upstream_is_stream(&item));

    let record = admin_usage_record_json(
        &item,
        &BTreeMap::new(),
        &BTreeMap::new(),
        false,
        false,
        None,
    );
    assert_eq!(record["is_stream"], true);
    assert_eq!(record["upstream_is_stream"], true);
    assert_eq!(record["client_requested_stream"], false);
    assert_eq!(record["client_is_stream"], false);
}

#[test]
fn replay_body_defaults_stream_to_client_requested_mode() {
    let item = StoredRequestUsageAudit {
        is_stream: true,
        request_body: Some(json!({
            "model": "gpt-5.4",
            "input": "hello"
        })),
        request_metadata: Some(json!({
            "client_requested_stream": false
        })),
        ..sample_usage("completed", Some(200), None)
    };

    let body =
        admin_usage_resolve_request_capture_body(&item, None).expect("replay body should resolve");
    assert_eq!(body["stream"], false);
}

#[test]
fn legacy_failure_signals_still_work_when_status_is_missing() {
    let item = StoredRequestUsageAudit {
        status: String::new(),
        ..sample_usage("completed", Some(429), Some("rate limited"))
    };
    assert!(admin_usage_is_failed(&item));
    assert!(admin_usage_matches_status(&item, Some("failed")));
}

#[test]
fn redirect_status_is_not_admin_usage_success() {
    let item = sample_usage("completed", Some(302), None);

    assert!(!admin_usage_is_success(&item));
    assert!(!admin_usage_is_failed(&item));
    assert!(admin_usage_matches_status(&item, Some("error")));
    assert!(admin_usage_matches_status(&item, Some("completed")));
}

#[test]
fn failed_redirect_status_counts_as_admin_usage_failed() {
    let item = sample_usage(
        "failed",
        Some(302),
        Some("execution runtime stream returned non-success status 302"),
    );

    assert!(admin_usage_is_failed(&item));
    assert!(admin_usage_matches_status(&item, Some("failed")));
    assert!(admin_usage_matches_status(&item, Some("error")));
    assert!(!admin_usage_is_success(&item));
}

#[test]
fn active_status_with_failure_signal_counts_as_failed() {
    let item = sample_usage("pending", Some(503), Some("upstream failed"));

    assert!(admin_usage_is_failed(&item));
    assert!(admin_usage_matches_status(&item, Some("failed")));
}

#[test]
fn admin_usage_fallback_flag_uses_routing_candidate_index() {
    let mut item = sample_usage("completed", Some(200), None);
    item.candidate_index = Some(2);

    assert!(admin_usage_has_fallback(&item));
    assert!(admin_usage_matches_status(&item, Some("has_fallback")));

    let payload = admin_usage_record_json(
        &item,
        &BTreeMap::new(),
        &BTreeMap::new(),
        false,
        false,
        Some("primary"),
    );
    assert_eq!(payload["has_fallback"], true);
}

#[test]
fn body_availability_considers_typed_reference_fields() {
    let item = StoredRequestUsageAudit {
        request_body_ref: Some("usage://request/req-1/request_body".to_string()),
        ..sample_usage("completed", Some(200), None)
    };

    assert!(admin_usage_has_body_value(
        &item,
        item.request_body.as_ref(),
        UsageBodyField::RequestBody
    ));
}

#[test]
fn detail_payload_preserves_original_headers_with_or_without_bodies() {
    let item = StoredRequestUsageAudit {
        request_headers: Some(json!({
            "authorization": "Bearer original-client-token",
            "originator": "codex-cli",
            "session-id": "original-session",
            "thread-id": "original-thread",
            "x-codex-turn-metadata": "{\"turn_id\":\"original-turn\"}",
            "x-openai-subagent": "reviewer"
        })),
        provider_request_headers: Some(json!({
            "authorization": "Bearer original-provider-token",
            "x-api-key": "original-provider-key"
        })),
        response_headers: Some(json!({
            "set-cookie": ["session=original-upstream", "preference=original"],
            "x-upstream-custom": "original-upstream-value"
        })),
        client_response_headers: Some(json!({
            "set-cookie": "session=original-client",
            "x-client-custom": "original-client-value"
        })),
        ..sample_usage("completed", Some(200), None)
    };

    for include_bodies in [false, true] {
        let payload = build_admin_usage_detail_payload(
            &item,
            &BTreeMap::new(),
            &BTreeMap::new(),
            false,
            false,
            None,
            include_bodies,
            None,
            &BTreeMap::new(),
        );

        for (field, expected) in [
            ("request_headers", &item.request_headers),
            ("provider_request_headers", &item.provider_request_headers),
            ("response_headers", &item.response_headers),
            ("client_response_headers", &item.client_response_headers),
        ] {
            assert_eq!(
                &payload[field],
                expected.as_ref().unwrap(),
                "{field} should retain original values when include_bodies={include_bodies}"
            );
        }
    }
}

#[test]
fn detail_payload_marks_reference_backed_bodies_as_available() {
    let item = StoredRequestUsageAudit {
        request_body: None,
        request_body_ref: Some("usage://request/req-1/request_body".to_string()),
        provider_request_body: None,
        provider_request_body_ref: Some("usage://request/req-1/provider_request_body".to_string()),
        response_body: None,
        response_body_ref: Some("usage://request/req-1/response_body".to_string()),
        client_response_body: None,
        client_response_body_ref: Some("usage://request/req-1/client_response_body".to_string()),
        request_metadata: Some(json!({
            "request_body_ref": "usage://request/req-1/request_body",
            "provider_request_body_ref": "usage://request/req-1/provider_request_body",
            "response_body_ref": "usage://request/req-1/response_body",
            "client_response_body_ref": "usage://request/req-1/client_response_body",
        })),
        ..sample_usage("completed", Some(200), None)
    };

    let payload = build_admin_usage_detail_payload(
        &item,
        &BTreeMap::new(),
        &BTreeMap::new(),
        false,
        false,
        None,
        false,
        Some(json!({"model": "gpt-5"})),
        &BTreeMap::new(),
    );

    assert!(payload["metadata"]["request_body_ref"].is_null());
    assert!(payload["metadata"]["provider_request_body_ref"].is_null());
    assert!(payload["metadata"]["response_body_ref"].is_null());
    assert!(payload["metadata"]["client_response_body_ref"].is_null());
    assert_eq!(payload["body_capture"]["request"]["available"], true);
    assert_eq!(payload["body_capture"]["request"]["storage"], "reference");
    assert_eq!(
        payload["body_capture"]["request"]["body_ref"],
        "usage://request/req-1/request_body"
    );
    assert_eq!(
        payload["body_capture"]["request"]["capture_source"],
        "stored_reference"
    );
    assert_eq!(
        payload["body_capture"]["provider_request"]["body_ref"],
        "usage://request/req-1/provider_request_body"
    );
    assert_eq!(
        payload["body_capture"]["response"]["body_ref"],
        "usage://request/req-1/response_body"
    );
    assert_eq!(
        payload["body_capture"]["client_response"]["body_ref"],
        "usage://request/req-1/client_response_body"
    );
    assert_eq!(payload["body_capture"]["response"]["available"], true);
    assert_eq!(
        payload["body_capture"]["client_response"]["available"],
        true
    );
    assert_eq!(payload["has_provider_request_body"], true);
    assert_eq!(payload["has_response_body"], true);
    assert_eq!(payload["has_client_response_body"], true);
}

#[test]
fn detail_payload_sanitizes_historical_request_metadata() {
    let item = StoredRequestUsageAudit {
        request_metadata: Some(json!({
            "trace_id": "internal-trace",
            "authorization": "Bearer live-secret",
            "nested": {
                "refresh_token": "refresh-secret",
                "endpoint_url": "https://user:password@api.example.test/v1?key=secret#fragment",
                "safe_count": 2
            }
        })),
        ..sample_usage("failed", Some(500), Some("failed"))
    };

    let payload = build_admin_usage_detail_payload(
        &item,
        &BTreeMap::new(),
        &BTreeMap::new(),
        false,
        false,
        None,
        false,
        None,
        &BTreeMap::new(),
    );
    let metadata = &payload["metadata"];
    assert!(metadata.get("authorization").is_none());
    assert!(metadata["nested"].get("refresh_token").is_none());
    assert_eq!(
        metadata["nested"]["endpoint_url"],
        "https://api.example.test/v1"
    );
    assert_eq!(metadata["nested"]["safe_count"], 2);
    let encoded = metadata.to_string();
    for secret in ["live-secret", "refresh-secret", "password", "key=secret"] {
        assert!(!encoded.contains(secret), "leaked {secret}");
    }
}

#[test]
fn detail_payload_separates_upstream_client_and_summary_errors() {
    let item = StoredRequestUsageAudit {
        error_message: Some("execution runtime stream returned retryable status 400".to_string()),
        error_category: Some("server_error".to_string()),
        response_headers: Some(json!({
            "content-type": "application/json"
        })),
        response_body: Some(json!({
            "error": {
                "type": "retryable_upstream_status",
                "message": "execution runtime stream returned retryable status 400",
                "code": 400
            }
        })),
        client_response_headers: Some(json!({
            "content-type": "application/json"
        })),
        client_response_body: Some(json!({
            "error": {
                "type": "http_error",
                "message": "local execution runtime exhausted"
            }
        })),
        ..sample_usage(
            "failed",
            Some(503),
            Some("execution runtime stream returned retryable status 400"),
        )
    };

    let payload = build_admin_usage_detail_payload(
        &item,
        &BTreeMap::new(),
        &BTreeMap::new(),
        false,
        false,
        None,
        true,
        Some(json!({"model": "gpt-5.4"})),
        &BTreeMap::new(),
    );

    assert_eq!(payload["upstream_error"]["source"], "upstream_response");
    assert_eq!(
        payload["errors"]["upstream_error"],
        payload["upstream_error"]
    );
    assert_eq!(
        payload["upstream_error"]["message"],
        "execution runtime stream returned retryable status 400"
    );
    assert_eq!(payload["client_error"]["source"], "client_response");
    assert_eq!(
        payload["client_error"]["message"],
        "local execution runtime exhausted"
    );
    assert_eq!(payload["failure_summary"]["source"], "client_response");
    assert_eq!(
        payload["failure_summary"]["message"],
        "local execution runtime exhausted"
    );
    assert_eq!(payload["error_flow"]["propagation"], "converted");
    assert!(payload["request_error"].is_null());
}

#[test]
fn detail_payload_marks_sensitive_upstream_account_errors_suppressed() {
    let item = StoredRequestUsageAudit {
        error_message: Some("credit balance exhausted".to_string()),
        error_category: Some("server_error".to_string()),
        response_body: Some(json!({
            "error": {
                "type": "insufficient_quota",
                "message": "credit balance exhausted"
            }
        })),
        client_response_body: Some(json!({
            "error": {
                "type": "http_error",
                "message": "upstream provider unavailable"
            }
        })),
        ..sample_usage("failed", Some(503), Some("credit balance exhausted"))
    };

    let payload = build_admin_usage_detail_payload(
        &item,
        &BTreeMap::new(),
        &BTreeMap::new(),
        false,
        false,
        None,
        true,
        Some(json!({"model": "gpt-5.4"})),
        &BTreeMap::new(),
    );

    assert_eq!(payload["error_flow"]["propagation"], "suppressed");
    assert_eq!(payload["error_flow"]["safe_to_expose_upstream"], false);
    assert_eq!(payload["upstream_error"]["type"], "insufficient_quota");
    assert_eq!(
        payload["client_error"]["message"],
        "upstream provider unavailable"
    );
}

#[test]
fn detail_payload_does_not_promote_local_client_error_to_upstream_error() {
    let message = "没有可用提供商支持模型 gpt-5.4 的同步请求。请检查模型映射、端点启用状态和 API Key 权限（原因代码: candidate_list_empty）";
    let client_message = "没有可用提供商支持模型 gpt-5.4 的同步请求";
    let item = StoredRequestUsageAudit {
        provider_api_key_id: None,
        provider_request_headers: None,
        provider_request_body: None,
        provider_request_body_ref: None,
        candidate_id: None,
        execution_path: Some("local_execution_runtime_miss".to_string()),
        local_execution_runtime_miss_reason: Some("candidate_list_empty".to_string()),
        error_category: Some("http_error".to_string()),
        client_response_body: Some(json!({
            "error": {
                "type": "http_error",
                "message": client_message
            }
        })),
        response_body: Some(json!({
            "error": {
                "type": "http_error",
                "message": message
            }
        })),
        ..sample_usage("failed", Some(503), Some(message))
    };

    let payload = build_admin_usage_detail_payload(
        &item,
        &BTreeMap::new(),
        &BTreeMap::new(),
        false,
        false,
        None,
        true,
        Some(json!({"model": "gpt-5.4"})),
        &BTreeMap::new(),
    );

    assert!(payload["upstream_error"].is_null());
    assert_eq!(payload["client_error"]["message"], client_message);
    assert_eq!(
        payload["client_response_body"]["error"]["message"],
        client_message
    );
    assert_eq!(payload["error_flow"]["source"], "gateway");
    assert_eq!(payload["error_flow"]["propagation"], "local");
}

#[test]
fn detail_payload_simplifies_local_client_error_when_client_body_is_unloaded() {
    let message = "没有可用提供商支持模型 gpt-5.4 的流式请求。请检查模型映射、端点启用状态和 API Key 权限（原因代码: candidate_list_empty）";
    let item = StoredRequestUsageAudit {
        provider_api_key_id: None,
        provider_request_headers: None,
        provider_request_body: None,
        provider_request_body_ref: None,
        candidate_id: None,
        execution_path: Some("local_execution_runtime_miss".to_string()),
        local_execution_runtime_miss_reason: Some("candidate_list_empty".to_string()),
        error_category: Some("http_error".to_string()),
        client_response_headers: Some(json!({"content-type": "application/json"})),
        client_response_body: None,
        client_response_body_ref: Some("usage://request/req-1/client_response_body".to_string()),
        response_body: None,
        ..sample_usage("failed", Some(503), Some(message))
    };

    let payload = build_admin_usage_detail_payload(
        &item,
        &BTreeMap::new(),
        &BTreeMap::new(),
        false,
        false,
        None,
        false,
        Some(json!({"model": "gpt-5.4", "stream": true})),
        &BTreeMap::new(),
    );

    assert!(payload["client_error"]["message"].is_null());
    assert!(payload["failure_summary"].is_null());
    assert_eq!(
        payload["scheduling_failure"]["title"],
        "本地调度失败：没有可调度候选"
    );
    assert_eq!(
        payload["scheduling_failure"]["reason"],
        "candidate_list_empty"
    );
    assert!(payload["scheduling_failure"]["reason_summary"].is_null());
    assert!(payload["scheduling_failure"]["message"].is_null());
    assert_eq!(payload["scheduling_failure"]["no_upstream_attempt"], true);
}

#[test]
fn detail_payload_simplifies_all_candidates_skipped_when_client_body_is_unloaded() {
    let message = "找到 1 个支持模型 gpt-5.4 的候选提供商，但本次流式请求全部不可用：provider_quota_blocked 2 次（原因代码: all_candidates_skipped）";
    let item = StoredRequestUsageAudit {
        provider_api_key_id: None,
        provider_request_headers: None,
        provider_request_body: None,
        provider_request_body_ref: None,
        candidate_id: None,
        execution_path: Some("local_execution_runtime_miss".to_string()),
        local_execution_runtime_miss_reason: Some("all_candidates_skipped".to_string()),
        error_category: Some("http_error".to_string()),
        client_response_headers: Some(json!({"content-type": "application/json"})),
        client_response_body: None,
        client_response_body_ref: Some("usage://request/req-1/client_response_body".to_string()),
        response_body: None,
        ..sample_usage("failed", Some(503), Some(message))
    };

    let payload = build_admin_usage_detail_payload(
        &item,
        &BTreeMap::new(),
        &BTreeMap::new(),
        false,
        false,
        None,
        false,
        Some(json!({"model": "gpt-5.4", "stream": true})),
        &BTreeMap::new(),
    );

    assert!(payload["client_error"]["message"].is_null());
    assert!(payload["failure_summary"].is_null());
    assert_eq!(
        payload["scheduling_failure"]["title"],
        "本地调度失败：所有候选均被跳过"
    );
    assert_eq!(
        payload["scheduling_failure"]["reason"],
        "all_candidates_skipped"
    );
    assert!(payload["scheduling_failure"]["reason_summary"].is_null());
    assert!(payload["scheduling_failure"]["message"].is_null());
    assert_eq!(payload["scheduling_failure"]["no_upstream_attempt"], true);
}

#[test]
fn detail_payload_preserves_legacy_body_capture_metadata_keys() {
    let item = StoredRequestUsageAudit {
        request_metadata: Some(json!({
            "request_preview_source": "stored_original",
            "original_request_body_available": true,
            "original_response_body_available": true,
            "custom_key": "keep-me"
        })),
        ..sample_usage("completed", Some(200), None)
    };

    let payload = build_admin_usage_detail_payload(
        &item,
        &BTreeMap::new(),
        &BTreeMap::new(),
        false,
        false,
        None,
        false,
        Some(json!({"model": "gpt-5"})),
        &BTreeMap::new(),
    );

    assert_eq!(
        payload["metadata"]["request_preview_source"],
        "stored_original"
    );
    assert_eq!(payload["metadata"]["original_request_body_available"], true);
    assert_eq!(
        payload["metadata"]["original_response_body_available"],
        true
    );
    assert_eq!(payload["metadata"]["custom_key"], "keep-me");
}

#[test]
fn admin_usage_record_prefers_typed_pricing_metadata_with_billing_snapshot_fallback() {
    let item = StoredRequestUsageAudit {
        output_price_per_1m: Some(15.0),
        request_metadata: Some(json!({
            "rate_multiplier": 0.5,
            "is_free_tier": false,
            "input_price_per_1m": 3.0,
            "cache_creation_price_per_1m": 3.75,
            "billing_snapshot": {
                "resolved_variables": {
                    "cache_read_price_per_1m": 0.30,
                    "output_price_per_1m": 15.0
                }
            }
        })),
        ..sample_usage("completed", Some(200), None)
    };

    let payload = admin_usage_record_json(
        &item,
        &BTreeMap::new(),
        &BTreeMap::new(),
        false,
        false,
        Some("primary"),
    );

    assert_eq!(payload["rate_multiplier"], 0.5);
    assert_eq!(payload["is_free_tier"], false);
    assert_eq!(payload["input_price_per_1m"], 3.0);
    assert_eq!(payload["output_price_per_1m"], 15.0);
    assert_eq!(payload["cache_creation_price_per_1m"], 3.75);
    assert_eq!(payload["cache_read_price_per_1m"], 0.30);
}

#[test]
fn admin_usage_record_rehydrates_cache_creation_total_from_classified_fields() {
    let item = StoredRequestUsageAudit {
        cache_creation_input_tokens: 0,
        cache_creation_ephemeral_5m_input_tokens: 12,
        cache_creation_ephemeral_1h_input_tokens: 8,
        ..sample_usage("completed", Some(200), None)
    };

    let payload = admin_usage_record_json(
        &item,
        &BTreeMap::new(),
        &BTreeMap::new(),
        false,
        false,
        Some("primary"),
    );

    assert_eq!(payload["cache_creation_input_tokens"], 20);
    assert_eq!(payload["cache_creation_ephemeral_5m_input_tokens"], 12);
    assert_eq!(payload["cache_creation_ephemeral_1h_input_tokens"], 8);
    assert_eq!(payload["total_tokens"], 40);
}

#[test]
fn admin_usage_total_tokens_uses_effective_input_for_cached_openai_usage() {
    let item = StoredRequestUsageAudit {
        input_tokens: 100,
        output_tokens: 20,
        total_tokens: 999,
        cache_creation_input_tokens: 0,
        cache_creation_ephemeral_5m_input_tokens: 12,
        cache_creation_ephemeral_1h_input_tokens: 8,
        cache_read_input_tokens: 80,
        ..sample_usage("completed", Some(200), None)
    };

    assert_eq!(admin_usage_total_tokens(&item), 120);
}

#[test]
fn detail_payload_exposes_typed_settlement_section() {
    let item = StoredRequestUsageAudit {
        output_price_per_1m: Some(15.0),
        request_metadata: Some(json!({
            "trace_id": "trace-detail",
            "billing_snapshot_schema_version": "v2",
            "billing_snapshot_status": "resolved",
            "rate_multiplier": 0.5,
            "is_free_tier": false,
            "input_price_per_1m": 3.0,
            "output_price_per_1m": 9.0,
            "cache_creation_price_per_1m": 3.75,
            "cache_read_price_per_1m": 0.30,
            "price_per_request": 0.02,
            "settlement_snapshot_schema_version": "3.0",
            "billing_dimensions": {
                "input_tokens": 35,
                "total_input_context": 42
            },
            "settlement_snapshot": {
                "schema_version": "3.0",
                "pricing_snapshot": {
                    "pricing_source": "provider_override",
                    "tiered_pricing_source": "provider_override",
                    "billing_processing_tier": "fast",
                    "processing_tier_price_multiplier": 2.5,
                    "tiered_pricing": {
                        "tiers": [{
                            "up_to": null,
                            "input_price_per_1m": 7.5,
                            "output_price_per_1m": 37.5
                        }]
                    }
                }
            },
            "billing_snapshot": {
                "resolved_variables": {
                    "output_price_per_1m": 11.0
                }
            }
        })),
        ..sample_usage("completed", Some(200), None)
    };

    let payload = build_admin_usage_detail_payload(
        &item,
        &BTreeMap::new(),
        &BTreeMap::new(),
        false,
        false,
        None,
        false,
        Some(json!({"model": "gpt-5"})),
        &BTreeMap::new(),
    );

    assert_eq!(payload["trace"]["trace_id"], "trace-detail");
    assert_eq!(
        payload["settlement"]["billing_snapshot_schema_version"],
        "v2"
    );
    assert_eq!(
        payload["settlement"]["billing_snapshot"]["resolved_variables"]["output_price_per_1m"],
        11.0
    );
    assert_eq!(
        payload["settlement"]["settlement_snapshot_schema_version"],
        "3.0"
    );
    assert_eq!(
        payload["settlement"]["settlement_snapshot"]["pricing_snapshot"]["pricing_source"],
        "provider_override"
    );
    assert_eq!(
        payload["settlement"]["settlement_snapshot"]["pricing_snapshot"]["billing_processing_tier"],
        "fast"
    );
    assert_eq!(
        payload["settlement"]["settlement_snapshot"]["pricing_snapshot"]
            ["processing_tier_price_multiplier"],
        2.5
    );
    assert_eq!(
        payload["settlement"]["settlement_snapshot"]["pricing_snapshot"]["tiered_pricing"]["tiers"]
            [0]["input_price_per_1m"],
        7.5
    );
    assert_eq!(
        payload["settlement"]["billing_dimensions"]["input_tokens"],
        35
    );
    assert_eq!(payload["settlement"]["billing_snapshot_status"], "resolved");
    assert_eq!(payload["settlement"]["rate_multiplier"], 0.5);
    assert_eq!(payload["settlement"]["is_free_tier"], false);
    assert_eq!(payload["settlement"]["input_price_per_1m"], 3.0);
    assert_eq!(payload["settlement"]["output_price_per_1m"], 9.0);
    assert_eq!(payload["settlement"]["cache_creation_price_per_1m"], 3.75);
    assert_eq!(payload["settlement"]["cache_read_price_per_1m"], 0.30);
    assert_eq!(payload["settlement"]["price_per_request"], 0.02);
    assert!(payload["metadata"]["trace_id"].is_null());
    assert!(payload["metadata"]["billing_snapshot"].is_null());
    assert!(payload["metadata"]["billing_snapshot_schema_version"].is_null());
    assert!(payload["metadata"]["billing_snapshot_status"].is_null());
    assert!(payload["metadata"]["settlement_snapshot"].is_null());
    assert!(payload["metadata"]["settlement_snapshot_schema_version"].is_null());
    assert!(payload["metadata"]["billing_dimensions"].is_null());
    assert!(payload["metadata"]["rate_multiplier"].is_null());
    assert!(payload["metadata"]["is_free_tier"].is_null());
    assert!(payload["metadata"]["input_price_per_1m"].is_null());
    assert!(payload["metadata"]["output_price_per_1m"].is_null());
    assert!(payload["metadata"]["cache_creation_price_per_1m"].is_null());
    assert!(payload["metadata"]["cache_read_price_per_1m"].is_null());
    assert!(payload["metadata"]["price_per_request"].is_null());
}

#[test]
fn admin_usage_provider_key_name_falls_back_to_typed_routing_metadata() {
    let item = StoredRequestUsageAudit {
        request_metadata: Some(json!({
            "key_name": "upstream-primary"
        })),
        ..sample_usage("completed", Some(200), None)
    };

    assert_eq!(
        super::admin_usage_provider_key_name(&item, &BTreeMap::new()),
        Some("upstream-primary".to_string())
    );
}

#[test]
fn detail_payload_exposes_typed_routing_section() {
    let item = StoredRequestUsageAudit {
        request_metadata: Some(json!({
            "trace_id": "trace-routing-detail",
            "model_id": "model-1",
            "global_model_id": "global-model-1",
            "global_model_name": "gpt-5"
        })),
        candidate_id: Some("cand-1".to_string()),
        candidate_index: Some(2),
        key_name: Some("upstream-primary".to_string()),
        planner_kind: Some("claude_cli_sync".to_string()),
        route_family: Some("claude".to_string()),
        route_kind: Some("cli".to_string()),
        execution_path: Some("local_execution_runtime_miss".to_string()),
        local_execution_runtime_miss_reason: Some("all_candidates_skipped".to_string()),
        ..sample_usage("completed", Some(200), None)
    };

    let payload = build_admin_usage_detail_payload(
        &item,
        &BTreeMap::new(),
        &BTreeMap::new(),
        false,
        false,
        Some("resolved-primary"),
        false,
        Some(json!({"model": "gpt-5"})),
        &BTreeMap::new(),
    );

    assert_eq!(payload["routing"]["candidate_id"], "cand-1");
    assert_eq!(payload["routing"]["candidate_index"], 2);
    assert_eq!(payload["routing"]["key_name"], "resolved-primary");
    assert_eq!(payload["routing"]["model_id"], "model-1");
    assert_eq!(payload["routing"]["global_model_id"], "global-model-1");
    assert_eq!(payload["routing"]["global_model_name"], "gpt-5");
    assert_eq!(payload["routing"]["planner_kind"], "claude_cli_sync");
    assert_eq!(payload["routing"]["route_family"], "claude");
    assert_eq!(payload["routing"]["route_kind"], "cli");
    assert_eq!(
        payload["routing"]["execution_path"],
        "local_execution_runtime_miss"
    );
    assert_eq!(
        payload["routing"]["local_execution_runtime_miss_reason"],
        "all_candidates_skipped"
    );
    assert!(payload["metadata"]["candidate_id"].is_null());
    assert!(payload["metadata"]["candidate_index"].is_null());
    assert!(payload["metadata"]["key_name"].is_null());
    assert!(payload["metadata"]["model_id"].is_null());
    assert!(payload["metadata"]["global_model_id"].is_null());
    assert!(payload["metadata"]["global_model_name"].is_null());
    assert!(payload["metadata"]["planner_kind"].is_null());
    assert!(payload["metadata"]["trace_id"].is_null());
    assert!(payload["metadata"]["route_family"].is_null());
    assert!(payload["metadata"]["route_kind"].is_null());
    assert!(payload["metadata"]["execution_path"].is_null());
    assert!(payload["metadata"]["local_execution_runtime_miss_reason"].is_null());
}

#[test]
fn admin_usage_record_prefers_joined_user_and_api_key_names_over_legacy_columns() {
    let item = StoredRequestUsageAudit {
        username: Some("legacy-alice".to_string()),
        api_key_name: Some("legacy-default".to_string()),
        ..sample_usage("completed", Some(200), None)
    };
    let users_by_id = BTreeMap::from([(
        "user-1".to_string(),
        aether_data::repository::users::StoredUserSummary::new(
            "user-1".to_string(),
            "fresh-alice".to_string(),
            Some("fresh-alice@example.com".to_string()),
            "user".to_string(),
            true,
            false,
        )
        .expect("user summary should build"),
    )]);
    let api_key_names = BTreeMap::from([("api-key-1".to_string(), "fresh-default".to_string())]);

    let payload = admin_usage_record_json(
        &item,
        &users_by_id,
        &api_key_names,
        true,
        true,
        Some("primary"),
    );

    assert_eq!(payload["username"], "fresh-alice");
    assert_eq!(payload["api_key"]["name"], "fresh-default");
    assert_eq!(payload["api_key"]["display"], "fresh-default");
    assert_eq!(payload["api_key_name"], "fresh-default");
    assert!(admin_usage_matches_search(
        &item,
        Some("fresh-default"),
        &users_by_id,
        &api_key_names,
        true,
        true
    ));
    assert!(admin_usage_matches_username(
        &item,
        Some("fresh-alice"),
        &users_by_id,
        true
    ));
}

#[test]
fn admin_usage_record_does_not_fallback_to_legacy_username_when_reader_is_available() {
    let item = StoredRequestUsageAudit {
        username: Some("legacy-alice".to_string()),
        ..sample_usage("completed", Some(200), None)
    };

    let payload = admin_usage_record_json(
        &item,
        &BTreeMap::new(),
        &BTreeMap::new(),
        true,
        false,
        Some("primary"),
    );

    assert_eq!(payload["username"], "已删除用户");
    assert!(!admin_usage_matches_search(
        &item,
        Some("legacy-alice"),
        &BTreeMap::new(),
        &BTreeMap::new(),
        true,
        false
    ));
    assert!(!admin_usage_matches_username(
        &item,
        Some("legacy-alice"),
        &BTreeMap::new(),
        true
    ));
}

#[test]
fn admin_usage_record_does_not_fallback_to_legacy_api_key_name_when_reader_is_available() {
    let item = StoredRequestUsageAudit {
        api_key_name: Some("legacy-default".to_string()),
        ..sample_usage("completed", Some(200), None)
    };

    let payload = admin_usage_record_json(
        &item,
        &BTreeMap::new(),
        &BTreeMap::new(),
        false,
        true,
        Some("primary"),
    );

    assert_eq!(payload["api_key"]["id"], "api-key-1");
    assert!(payload["api_key"]["name"].is_null());
    assert_eq!(payload["api_key"]["display"], "api-key-1");
    assert!(payload["api_key_name"].is_null());
    assert!(!admin_usage_matches_search(
        &item,
        Some("legacy-default"),
        &BTreeMap::new(),
        &BTreeMap::new(),
        false,
        true
    ));
}

#[test]
fn admin_usage_record_json_exposes_chat_pii_redaction_marker() {
    let item = StoredRequestUsageAudit {
        request_metadata: Some(json!({"chat_pii_redaction": true})),
        ..sample_usage("completed", Some(200), None)
    };

    let record = admin_usage_record_json(
        &item,
        &BTreeMap::new(),
        &BTreeMap::new(),
        false,
        false,
        None,
    );
    assert_eq!(record["chat_pii_redaction"], true);
    let active = admin_usage_active_request_json(&item, None, None, None);
    assert_eq!(active["chat_pii_redaction"], true);

    // 非布尔形态属于非法事实：不得把它当成“已脱敏”。
    let invalid = StoredRequestUsageAudit {
        request_metadata: Some(json!({"chat_pii_redaction": "true"})),
        ..sample_usage("completed", Some(200), None)
    };
    let record = admin_usage_record_json(
        &invalid,
        &BTreeMap::new(),
        &BTreeMap::new(),
        false,
        false,
        None,
    );
    assert_eq!(record["chat_pii_redaction"], false);
}
