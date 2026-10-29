use super::{
    canonical_usage_body_ref_for, extract_provider_actual_service_tier_from_response,
    extract_provider_response_model_from_bodies, extract_provider_service_tier_from_body,
    normalize_provider_reasoning_effort, normalize_provider_response_model,
    normalize_provider_service_tier, resolve_provider_cache_ttl_minutes, usage_body_ref,
    StoredRequestUsageAudit, UpsertUsageRecord, UsageBodyCaptureState, UsageBodyCaptureStorage,
    UsageBodyField, UsageProviderPerformanceQuery, REALTIME_SESSION_METADATA_KEY,
    USAGE_AVAILABLE_METADATA_KEY, USAGE_PRICING_AVAILABLE_METADATA_KEY,
    WEBSOCKET_MODE_METADATA_KEY, WEBSOCKET_TRANSPORT_METADATA_KEY,
};
use serde_json::{json, Value};

#[test]
fn provider_fact_normalization_preserves_trimmed_byte_limit() {
    for normalize in [
        normalize_provider_reasoning_effort as fn(&str) -> Option<String>,
        normalize_provider_service_tier,
    ] {
        assert_eq!(normalize(" \t\r\n"), None);
        assert_eq!(normalize("  HIGH\n"), Some("high".to_string()));
        assert_eq!(normalize(&"A".repeat(64)), Some("a".repeat(64)));
        assert_eq!(
            normalize(&format!(" \t{}\n", "A".repeat(64))),
            Some("a".repeat(64))
        );
        assert_eq!(normalize(&"A".repeat(65)), None);
    }
}

#[test]
fn response_model_requires_authoritative_different_top_level_models() {
    let request = json!({"model": " gpt-5 "});
    let response = json!({"model": " gpt-5.1 "});
    assert_eq!(
        extract_provider_response_model_from_bodies(
            Some(&request),
            Some(UsageBodyCaptureState::Inline),
            Some("openai:chat"),
            Some(&response),
            Some(UsageBodyCaptureState::Inline),
            Some("openai:chat"),
        ),
        Some("gpt-5.1".to_string())
    );
    assert_eq!(
        extract_provider_response_model_from_bodies(
            Some(&request),
            Some(UsageBodyCaptureState::Inline),
            Some("openai:responses"),
            Some(&json!({"model": "gpt-5"})),
            Some(UsageBodyCaptureState::Inline),
            Some("openai:responses"),
        ),
        None
    );
    assert_eq!(
        extract_provider_response_model_from_bodies(
            Some(&request),
            Some(UsageBodyCaptureState::Truncated),
            Some("openai:chat"),
            Some(&response),
            Some(UsageBodyCaptureState::Inline),
            Some("openai:chat"),
        ),
        None
    );
}

#[test]
fn response_model_keeps_case_and_rejects_invalid_values() {
    assert_eq!(
        normalize_provider_response_model("  GPT-5.1  "),
        Some("GPT-5.1".to_string())
    );
    assert_eq!(normalize_provider_response_model("  "), None);
    assert_eq!(normalize_provider_response_model(&"x".repeat(257)), None);
    assert_eq!(
        extract_provider_response_model_from_bodies(
            Some(&json!({"model": "gpt-5"})),
            None,
            Some("openai:chat"),
            Some(&json!({"model": 42})),
            None,
            Some("openai:chat"),
        ),
        None
    );
}

#[test]
fn response_model_uses_provider_format_specific_nested_paths() {
    let request = json!({"model": "gemini-2.5-flash"});
    let response = json!({
        "response": {
            "modelVersion": "gemini-2.5-flash-001",
            "candidates": [{"content": {"role": "model"}}]
        }
    });
    assert_eq!(
        extract_provider_response_model_from_bodies(
            Some(&request),
            Some(UsageBodyCaptureState::Inline),
            Some("gemini:generate_content"),
            Some(&response),
            Some(UsageBodyCaptureState::Inline),
            Some("gemini:generate_content"),
        ),
        Some("gemini-2.5-flash-001".to_string())
    );

    let wrapped_chunks = json!({
        "chunks": [
            {"response": {"modelVersion": "gemini-old"}},
            {"response": {"modelVersion": "gemini-final"}}
        ]
    });
    assert_eq!(
        extract_provider_response_model_from_bodies(
            Some(&request),
            Some(UsageBodyCaptureState::Inline),
            Some("gemini:generate_content"),
            Some(&wrapped_chunks),
            Some(UsageBodyCaptureState::Inline),
            Some("gemini:generate_content"),
        ),
        Some("gemini-final".to_string())
    );

    let ambiguous = json!({
        "metadata": {"model": "do-not-use"},
        "candidates": [{"content": {"role": "model"}}]
    });
    assert_eq!(
        extract_provider_response_model_from_bodies(
            Some(&request),
            Some(UsageBodyCaptureState::Inline),
            Some("gemini:generate_content"),
            Some(&ambiguous),
            Some(UsageBodyCaptureState::Inline),
            Some("gemini:generate_content"),
        ),
        None
    );
}

#[test]
fn provider_fact_normalization_preserves_non_ascii_case_and_byte_count() {
    for normalize in [
        normalize_provider_reasoning_effort as fn(&str) -> Option<String>,
        normalize_provider_service_tier,
    ] {
        let accepted = format!("{}A", "\u{00c9}".repeat(31));
        assert_eq!(
            normalize(&accepted),
            Some(format!("{}a", "\u{00c9}".repeat(31)))
        );
        assert_eq!(
            normalize(&"\u{00c9}".repeat(32)),
            Some("\u{00c9}".repeat(32))
        );
        assert_eq!(normalize(&format!("{}A", "\u{00c9}".repeat(32))), None);
        assert_eq!(normalize("\u{2003}FAST\u{2003}"), Some("fast".to_string()));
    }
}

#[test]
fn provider_fact_normalization_rejects_large_input_before_copying() {
    let oversized = "A".repeat(4 * 1024 * 1024);
    assert_eq!(normalize_provider_reasoning_effort(&oversized), None);
    assert_eq!(normalize_provider_service_tier(&oversized), None);
}

fn sample_usage() -> StoredRequestUsageAudit {
    StoredRequestUsageAudit::new(
        "usage-1".to_string(),
        "req-1".to_string(),
        None,
        None,
        None,
        None,
        "OpenAI".to_string(),
        "gpt-4.1".to_string(),
        None,
        None,
        None,
        None,
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
        0.1,
        0.1,
        Some(200),
        None,
        None,
        Some(120),
        Some(80),
        "completed".to_string(),
        "settled".to_string(),
        100,
        101,
        Some(102),
    )
    .expect("usage should build")
}

#[test]
fn canonical_body_ref_requires_matching_request_and_field() {
    assert_eq!(
        canonical_usage_body_ref_for(
            "  usage://request/req-1/request_body  ",
            "req-1",
            UsageBodyField::RequestBody,
        ),
        Some(usage_body_ref("req-1", UsageBodyField::RequestBody))
    );
    assert_eq!(
        canonical_usage_body_ref_for(
            "usage://request/req-2/request_body",
            "req-1",
            UsageBodyField::RequestBody,
        ),
        None
    );
    assert_eq!(
        canonical_usage_body_ref_for(
            "usage://request/req-1/response_body",
            "req-1",
            UsageBodyField::RequestBody,
        ),
        None
    );
    assert_eq!(
        canonical_usage_body_ref_for("blob://opaque", "req-1", UsageBodyField::RequestBody),
        None
    );
}

#[test]
fn provider_performance_query_defaults_timeline_on_for_legacy_payloads() {
    let mut payload = json!({
        "created_from_unix_secs": 1,
        "created_until_unix_secs": 2,
        "granularity": "hour",
        "tz_offset_minutes": 0,
        "limit": 8,
        "provider_id": null,
        "model": null,
        "api_format": null,
        "endpoint_kind": null,
        "is_stream": null,
        "has_format_conversion": null,
        "slow_threshold_ms": 10000
    });

    let legacy: UsageProviderPerformanceQuery =
        serde_json::from_value(payload.clone()).expect("legacy query should deserialize");
    assert!(legacy.include_timeline);

    payload["include_timeline"] = Value::Bool(false);
    let without_timeline: UsageProviderPerformanceQuery =
        serde_json::from_value(payload).expect("explicit timeline preference should deserialize");
    assert!(!without_timeline.include_timeline);
}

#[test]
fn rejects_empty_request_id() {
    assert!(StoredRequestUsageAudit::new(
        "usage-1".to_string(),
        "".to_string(),
        None,
        None,
        None,
        None,
        "OpenAI".to_string(),
        "gpt-4.1".to_string(),
        None,
        None,
        None,
        None,
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
        0.1,
        0.1,
        Some(200),
        None,
        None,
        Some(120),
        Some(80),
        "completed".to_string(),
        "settled".to_string(),
        100,
        101,
        Some(102),
    )
    .is_err());
}

#[test]
fn rejects_negative_token_count() {
    assert!(StoredRequestUsageAudit::new(
        "usage-1".to_string(),
        "req-1".to_string(),
        None,
        None,
        None,
        None,
        "OpenAI".to_string(),
        "gpt-4.1".to_string(),
        None,
        None,
        None,
        None,
        Some("chat".to_string()),
        Some("openai:chat".to_string()),
        Some("openai".to_string()),
        Some("chat".to_string()),
        Some("openai:chat".to_string()),
        Some("openai".to_string()),
        Some("chat".to_string()),
        false,
        false,
        -1,
        20,
        30,
        0.1,
        0.1,
        Some(200),
        None,
        None,
        Some(120),
        Some(80),
        "completed".to_string(),
        "settled".to_string(),
        100,
        101,
        Some(102),
    )
    .is_err());
}

#[test]
fn rejects_invalid_upsert_payload() {
    let mut record = UpsertUsageRecord {
        capture_retention: Default::default(),
        request_id: "".to_string(),
        user_id: None,
        api_key_id: None,
        username: None,
        api_key_name: None,
        provider_name: "openai".to_string(),
        model: "gpt-5".to_string(),
        target_model: None,
        provider_id: None,
        provider_endpoint_id: None,
        provider_api_key_id: None,
        request_type: Some("chat".to_string()),
        api_format: Some("openai:chat".to_string()),
        api_family: Some("openai".to_string()),
        endpoint_kind: Some("chat".to_string()),
        endpoint_api_format: Some("openai:chat".to_string()),
        provider_api_family: Some("openai".to_string()),
        provider_endpoint_kind: Some("chat".to_string()),
        has_format_conversion: Some(false),
        is_stream: Some(false),
        input_tokens: Some(10),
        output_tokens: Some(20),
        total_tokens: Some(30),
        cache_creation_input_tokens: None,
        cache_creation_ephemeral_5m_input_tokens: None,
        cache_creation_ephemeral_1h_input_tokens: None,
        cache_read_input_tokens: None,
        cache_creation_cost_usd: None,
        cache_read_cost_usd: None,
        output_price_per_1m: None,
        total_cost_usd: None,
        actual_total_cost_usd: None,
        status_code: Some(200),
        error_message: None,
        error_category: None,
        response_time_ms: Some(120),
        first_byte_time_ms: None,
        status: "completed".to_string(),
        billing_status: "pending".to_string(),
        request_headers: Some(json!({"authorization": "Bearer test"})),
        request_body: Some(json!({"model": "gpt-5"})),
        request_body_ref: None,
        provider_request_headers: None,
        provider_request_body: None,
        provider_request_body_ref: None,
        response_headers: None,
        response_body: None,
        response_body_ref: None,
        client_response_headers: None,
        client_response_body: None,
        client_response_body_ref: None,
        request_body_state: None,
        provider_request_body_state: None,
        response_body_state: None,
        client_response_body_state: None,
        candidate_id: None,
        candidate_index: None,
        key_name: None,
        planner_kind: None,
        route_family: None,
        route_kind: None,
        execution_path: None,
        local_execution_runtime_miss_reason: None,
        request_metadata: None,
        finalized_at_unix_secs: None,
        created_at_unix_ms: Some(100),
        updated_at_unix_secs: 101,
    };

    assert!(record.validate().is_err());

    record.request_id = "req-1".to_string();
    assert!(record.validate().is_ok());

    for invalid_status in ["", " completed ", "success", "COMPLETED"] {
        record.status = invalid_status.to_string();
        assert!(
            record.validate().is_err(),
            "accepted status {invalid_status:?}"
        );
    }

    record.status = "completed".to_string();
    for invalid_billing_status in ["", " settled ", "paid", "SETTLED"] {
        record.billing_status = invalid_billing_status.to_string();
        assert!(
            record.validate().is_err(),
            "accepted billing status {invalid_billing_status:?}"
        );
    }

    record.billing_status = "pending".to_string();
    record.total_cost_usd = Some(-0.01);
    assert!(record.validate().is_err());
    record.total_cost_usd = None;
    record.actual_total_cost_usd = Some(-0.01);
    assert!(record.validate().is_err());
    record.actual_total_cost_usd = None;
    record.cache_creation_cost_usd = Some(-0.01);
    assert!(record.validate().is_err());
    record.cache_creation_cost_usd = None;
    record.cache_read_cost_usd = Some(-0.01);
    assert!(record.validate().is_err());
    record.cache_read_cost_usd = None;
    record.output_price_per_1m = Some(-0.01);
    assert!(record.validate().is_err());
}

#[test]
fn routing_group_snapshot_defaults_legacy_multiplier_without_inventing_a_group() {
    let mut usage = sample_usage();
    usage.total_cost_usd = 4.0;
    assert_eq!(usage.routing_group_billing_multiplier(), 1.0);
    assert_eq!(usage.routing_group_billing_cost(), Some(4.0));
    assert_eq!(usage.routing_group_id(), None);
    assert_eq!(usage.routing_group_name(), None);
    for (value, multiplier, cost) in [
        (json!(0), 0.0, 0.0),
        (json!(0.25), 0.25, 1.0),
        (json!(2.5), 2.5, 10.0),
        (json!(-2), 1.0, 4.0),
        (json!("Infinity"), 1.0, 4.0),
        (json!(f64::INFINITY), 1.0, 4.0),
        (json!(f64::NAN), 1.0, 4.0),
    ] {
        usage.request_metadata = Some(json!({
            "routing_group_billing_multiplier": value,
            "routing_group_id": "group-recorded",
            "routing_group_name": "请求时的分组",
            "rate_multiplier": 0.75
        }));
        assert_eq!(usage.routing_group_billing_multiplier(), multiplier);
        assert_eq!(usage.routing_group_billing_cost(), Some(cost));
        assert_eq!(usage.routing_group_id(), Some("group-recorded"));
        assert_eq!(usage.routing_group_name(), Some("请求时的分组"));
        assert_eq!(usage.settlement_rate_multiplier(), Some(0.75));
    }
    usage.request_metadata = Some(json!({"routing_group_billing_multiplier": 2.0}));
    usage.total_cost_usd = f64::MAX;
    assert_eq!(usage.routing_group_billing_cost(), None);
}

#[test]
fn settlement_accessors_prefer_typed_metadata() {
    let mut usage = sample_usage();
    usage.output_price_per_1m = Some(11.0);
    usage.request_metadata = Some(json!({
        "billing_snapshot_schema_version": "v2",
        "billing_snapshot_status": "resolved",
        "rate_multiplier": 0.5,
        "is_free_tier": false,
        "input_price_per_1m": 3.0,
        "output_price_per_1m": 9.0,
        "cache_creation_price_per_1m": 3.75,
        "cache_read_price_per_1m": 0.3,
        "price_per_request": 0.02,
        "billing_snapshot": {
            "resolved_variables": {
                "output_price_per_1m": 11.0
            }
        }
    }));

    assert_eq!(
        usage.settlement_billing_snapshot_schema_version(),
        Some("v2")
    );
    assert_eq!(usage.settlement_billing_snapshot_status(), Some("resolved"));
    assert_eq!(usage.settlement_rate_multiplier(), Some(0.5));
    assert_eq!(usage.settlement_is_free_tier(), Some(false));
    assert_eq!(usage.settlement_input_price_per_1m(), Some(3.0));
    assert_eq!(usage.settlement_output_price_per_1m(), Some(9.0));
    assert_eq!(usage.settlement_cache_creation_price_per_1m(), Some(3.75));
    assert_eq!(usage.settlement_cache_read_price_per_1m(), Some(0.3));
    assert_eq!(usage.settlement_price_per_request(), Some(0.02));
}

#[test]
fn websocket_transport_uses_typed_request_metadata() {
    let mut usage = sample_usage();
    assert!(!usage.is_websocket());
    assert!(usage.usage_available());
    assert!(usage.usage_pricing_available());

    usage.request_metadata = Some(json!({
        (WEBSOCKET_MODE_METADATA_KEY): true,
        (WEBSOCKET_TRANSPORT_METADATA_KEY): "responses",
        (USAGE_AVAILABLE_METADATA_KEY): false,
        (USAGE_PRICING_AVAILABLE_METADATA_KEY): false,
        (REALTIME_SESSION_METADATA_KEY): {
            "input_audio_tokens": 7,
            "output_audio_tokens": 3,
        },
    }));

    assert!(usage.is_websocket());
    assert_eq!(usage.websocket_transport(), Some("responses"));
    assert!(!usage.usage_available());
    assert!(!usage.usage_pricing_available());
    assert_eq!(usage.realtime_input_audio_tokens(), Some(7));
    assert_eq!(usage.realtime_output_audio_tokens(), Some(3));
}

#[test]
fn settlement_accessors_fall_back_to_billing_snapshot_and_legacy_output_price() {
    let mut usage = sample_usage();
    usage.output_price_per_1m = Some(15.0);
    usage.request_metadata = Some(json!({
        "billing_snapshot": {
            "resolved_variables": {
                "input_price_per_1m": 3.0,
                "cache_creation_price_per_1m": 3.75,
                "cache_read_price_per_1m": 0.3,
                "price_per_request": 0.02
            }
        }
    }));

    assert_eq!(usage.settlement_input_price_per_1m(), Some(3.0));
    assert_eq!(usage.settlement_output_price_per_1m(), Some(15.0));
    assert_eq!(usage.settlement_cache_creation_price_per_1m(), Some(3.75));
    assert_eq!(usage.settlement_cache_read_price_per_1m(), Some(0.3));
    assert_eq!(usage.settlement_price_per_request(), Some(0.02));
}

#[test]
fn body_ref_is_derived_from_capture_state_and_routing_accessors_prefer_typed_fields() {
    let mut usage = sample_usage();
    usage.request_body_state = Some(UsageBodyCaptureState::Reference);
    usage.provider_request_body_state = Some(UsageBodyCaptureState::Reference);
    usage.response_body_state = Some(UsageBodyCaptureState::Reference);
    usage.client_response_body_state = Some(UsageBodyCaptureState::Reference);
    usage.candidate_id = Some("cand-typed".to_string());
    usage.key_name = Some("primary-typed".to_string());
    usage.planner_kind = Some("claude_cli_sync".to_string());
    usage.route_family = Some("claude".to_string());
    usage.route_kind = Some("cli".to_string());
    usage.execution_path = Some("local_execution_runtime_miss".to_string());
    usage.local_execution_runtime_miss_reason = Some("all_candidates_skipped".to_string());
    usage.request_metadata = Some(json!({
        "request_body_ref": "blob://legacy-request",
        "provider_request_body_ref": "blob://legacy-provider",
        "response_body_ref": "blob://legacy-response",
        "client_response_body_ref": "blob://legacy-client-response",
        "candidate_id": "cand-legacy",
        "key_name": "primary-legacy",
        "routing_candidate_skip_reason": "provider_request_body_build_failed",
        "routing_failure_diagnostic": {
            "path": "$.reasoning.summary",
            "message": "invalid reasoning summary"
        }
    }));

    assert_eq!(
        usage.body_ref(UsageBodyField::RequestBody).as_deref(),
        Some("usage://request/req-1/request_body")
    );
    assert_eq!(
        usage
            .body_ref(UsageBodyField::ProviderRequestBody)
            .as_deref(),
        Some("usage://request/req-1/provider_request_body")
    );
    assert_eq!(
        usage.body_ref(UsageBodyField::ResponseBody).as_deref(),
        Some("usage://request/req-1/response_body")
    );
    assert_eq!(
        usage
            .body_ref(UsageBodyField::ClientResponseBody)
            .as_deref(),
        Some("usage://request/req-1/client_response_body")
    );
    assert_eq!(usage.routing_candidate_id(), Some("cand-typed"));
    assert_eq!(usage.routing_key_name(), Some("primary-typed"));
    assert_eq!(usage.routing_planner_kind(), Some("claude_cli_sync"));
    assert_eq!(usage.routing_route_family(), Some("claude"));
    assert_eq!(usage.routing_route_kind(), Some("cli"));
    assert_eq!(
        usage.routing_execution_path(),
        Some("local_execution_runtime_miss")
    );
    assert_eq!(
        usage.routing_local_execution_runtime_miss_reason(),
        Some("all_candidates_skipped")
    );
    assert_eq!(
        usage.routing_candidate_skip_reason(),
        Some("provider_request_body_build_failed")
    );
    assert_eq!(
        usage
            .routing_failure_diagnostic()
            .and_then(|diagnostic| diagnostic.get("path")),
        Some(&json!("$.reasoning.summary"))
    );
}

#[test]
fn body_ref_accessor_ignores_legacy_metadata_compatibility_keys() {
    let mut usage = sample_usage();
    usage.request_metadata = Some(json!({
        "request_body_ref": "blob://legacy-request",
        "provider_request_body_ref": "blob://legacy-provider",
        "response_body_ref": "blob://legacy-response",
        "client_response_body_ref": "blob://legacy-client-response"
    }));

    assert_eq!(usage.body_ref(UsageBodyField::RequestBody), None);
    assert_eq!(usage.body_ref(UsageBodyField::ProviderRequestBody), None);
    assert_eq!(usage.body_ref(UsageBodyField::ResponseBody), None);
    assert_eq!(usage.body_ref(UsageBodyField::ClientResponseBody), None);
}

#[test]
fn body_capture_result_prefers_typed_state_over_inline_body_presence() {
    let mut usage = sample_usage();
    usage.request_body = Some(json!({"model": "gpt-5"}));
    usage.request_body_state = Some(UsageBodyCaptureState::Disabled);

    let result =
        usage.body_capture_result(UsageBodyField::RequestBody, usage.request_body.as_ref());

    assert!(!result.available);
    assert_eq!(result.storage, UsageBodyCaptureStorage::Disabled);
    assert_eq!(result.state_label(), "disabled");
    assert_eq!(result.request_capture_source(), "disabled");
}

#[test]
fn body_capture_result_infers_legacy_inline_storage_when_typed_state_is_missing() {
    let mut usage = sample_usage();
    usage.request_body = Some(json!({"model": "gpt-5"}));

    let result =
        usage.body_capture_result(UsageBodyField::RequestBody, usage.request_body.as_ref());

    assert!(result.available);
    assert_eq!(result.storage, UsageBodyCaptureStorage::Inline);
    assert_eq!(result.state_label(), "legacy_unknown");
    assert_eq!(result.request_capture_source(), "stored_original");
}

#[test]
fn provider_reasoning_effort_prefers_provider_request_body_over_metadata() {
    let mut usage = sample_usage();
    usage.provider_request_body = Some(json!({
        "reasoning": { "effort": "XHigh" },
        "service_tier": "Priority"
    }));

    assert_eq!(usage.provider_reasoning_effort().as_deref(), Some("xhigh"));
    assert_eq!(usage.provider_service_tier().as_deref(), Some("priority"));

    usage.request_metadata = Some(json!({
        "provider_reasoning_effort": "max",
        "provider_service_tier": "standard"
    }));

    assert_eq!(usage.provider_reasoning_effort().as_deref(), Some("xhigh"));
    assert_eq!(usage.provider_service_tier().as_deref(), Some("priority"));

    usage.provider_request_body = None;
    assert_eq!(usage.provider_reasoning_effort().as_deref(), Some("max"));
    assert_eq!(usage.provider_service_tier().as_deref(), Some("standard"));

    usage.request_metadata = None;
    usage.provider_request_body = Some(json!({
        "output_config": { "effort": "High" },
        "service_tier": "priority"
    }));

    assert_eq!(usage.provider_reasoning_effort().as_deref(), Some("high"));
    assert_eq!(usage.provider_service_tier().as_deref(), Some("priority"));

    usage.provider_request_body = Some(json!({
        "reasoning_effort": "medium"
    }));

    assert_eq!(usage.provider_reasoning_effort().as_deref(), Some("medium"));

    usage.request_metadata = Some(json!({
        "provider_reasoning_effort": "max",
        "provider_service_tier": "standard"
    }));
    usage.provider_request_body = Some(json!({
        "model": "gpt-5"
    }));

    assert_eq!(usage.provider_reasoning_effort(), None);
    assert_eq!(usage.provider_service_tier(), None);
}

#[test]
fn gemini_thinking_level_supplies_reasoning_effort_for_both_body_spellings() {
    let mut usage = sample_usage();
    usage.provider_request_body = Some(json!({
        "generationConfig": {
            "thinkingConfig": { "includeThoughts": true, "thinkingLevel": "HIGH" }
        }
    }));

    assert_eq!(usage.provider_reasoning_effort().as_deref(), Some("high"));

    // The converted provider body keeps snake_case keys, and the client body may carry the
    // protobuf enum spelling. Both must land on the same badge vocabulary.
    usage.provider_request_body = Some(json!({
        "generation_config": {
            "thinking_config": { "thinking_level": "thinking_level_medium" }
        }
    }));

    assert_eq!(usage.provider_reasoning_effort().as_deref(), Some("medium"));

    usage.provider_request_body = Some(json!({
        "generationConfig": {
            "thinkingConfig": { "thinkingLevel": "  low  " }
        }
    }));

    assert_eq!(usage.provider_reasoning_effort().as_deref(), Some("low"));
}

#[test]
fn gemini_thinking_budget_supplies_reasoning_effort_without_collapsing_zero() {
    let mut usage = sample_usage();
    usage.provider_request_body = Some(json!({
        "generationConfig": { "thinkingConfig": { "thinkingBudget": 8192 } }
    }));

    assert_eq!(usage.provider_reasoning_effort().as_deref(), Some("xhigh"));

    // `0` disables reasoning. The shared budget ladder maps 0..=1664 to `low`, which would
    // report an explicitly disabled request as shallow, so the Gemini path reports `none`.
    usage.provider_request_body = Some(json!({
        "generationConfig": { "thinkingConfig": { "thinkingBudget": 0 } }
    }));

    assert_eq!(usage.provider_reasoning_effort().as_deref(), Some("none"));

    usage.provider_request_body = Some(json!({
        "generation_config": { "thinking_config": { "thinking_budget": 1280 } }
    }));

    assert_eq!(usage.provider_reasoning_effort().as_deref(), Some("low"));
}

#[test]
fn gemini_thinking_config_without_level_or_budget_yields_no_reasoning_effort() {
    let mut usage = sample_usage();
    usage.provider_request_body = Some(json!({
        "generationConfig": { "thinkingConfig": { "includeThoughts": true } }
    }));

    assert_eq!(usage.provider_reasoning_effort(), None);

    // A level-less, budget-less config must not fall back to metadata either: the captured
    // body is authoritative and it says nothing about depth.
    usage.request_metadata = Some(json!({ "provider_reasoning_effort": "max" }));
    assert_eq!(usage.provider_reasoning_effort(), None);

    // `THINKING_LEVEL_UNSPECIFIED` is the enum's "no explicit level" member, not a depth.
    usage.request_metadata = None;
    usage.provider_request_body = Some(json!({
        "generationConfig": {
            "thinkingConfig": { "thinkingLevel": "THINKING_LEVEL_UNSPECIFIED" }
        }
    }));

    assert_eq!(usage.provider_reasoning_effort(), None);
}

/// The v1internal envelope nests the real `generateContent` payload under `request`. This is
/// the shape the Antigravity/Gemini transports actually send upstream, so the extraction
/// has to descend into it or every converted `openai:chat -> gemini` request loses its badge.
#[test]
fn gemini_thinking_config_is_read_from_the_v1internal_envelope() {
    let mut usage = sample_usage();
    usage.provider_request_body = Some(json!({
        "model": "gemini-3.8-flash-tiered",
        "project": "aicode-consumers",
        "requestId": "req-1",
        "requestType": "agent",
        "userAgent": "vscode/1.X.X (Antigravity/4.3.0)",
        "request": {
            "contents": [{ "role": "user", "parts": [{ "text": "hi" }] }],
            "generationConfig": {
                "maxOutputTokens": 65536,
                "thinkingConfig": { "includeThoughts": true, "thinkingLevel": "high" }
            }
        }
    }));

    assert_eq!(usage.provider_reasoning_effort().as_deref(), Some("high"));

    usage.provider_request_body = Some(json!({
        "model": "gemini-3.8-flash-tiered",
        "request": {
            "generation_config": {
                "thinking_config": { "include_thoughts": true, "thinking_budget": 32768 }
            }
        }
    }));

    assert_eq!(usage.provider_reasoning_effort().as_deref(), Some("xhigh"));

    usage.provider_request_body = Some(json!({
        "model": "gemini-3.8-flash-tiered",
        "request": {
            "generationConfig": { "thinkingConfig": { "thinkingBudget": 0 } }
        }
    }));

    assert_eq!(usage.provider_reasoning_effort().as_deref(), Some("none"));
}

/// A converted request that carries only `maxOutputTokens` must stay badge-less rather than
/// picking up a depth from somewhere else in the envelope.
#[test]
fn v1internal_envelope_without_thinking_config_yields_no_reasoning_effort() {
    let mut usage = sample_usage();
    usage.provider_request_body = Some(json!({
        "model": "gemini-3.8-flash-tiered",
        "project": "aicode-consumers",
        "request": {
            "contents": [{ "role": "user", "parts": [{ "text": "hi" }] }],
            "generationConfig": { "maxOutputTokens": 65536 }
        }
    }));

    assert_eq!(usage.provider_reasoning_effort(), None);
}

#[test]
fn gemini_thinking_config_does_not_shadow_explicit_effort_fields() {
    let mut usage = sample_usage();
    usage.provider_request_body = Some(json!({
        "reasoning_effort": "max",
        "generationConfig": { "thinkingConfig": { "thinkingLevel": "low" } }
    }));

    assert_eq!(usage.provider_reasoning_effort().as_deref(), Some("max"));
}

#[test]
fn requested_and_provider_reasoning_efforts_remain_independent() {
    let mut usage = sample_usage();
    usage.request_body = Some(json!({
        "reasoning": { "effort": "XHigh" }
    }));
    usage.provider_request_body = Some(json!({
        "reasoning_effort": "max"
    }));
    usage.request_metadata = Some(json!({
        "requested_reasoning_effort": "low",
        "provider_reasoning_effort": "medium"
    }));

    assert_eq!(usage.requested_reasoning_effort().as_deref(), Some("xhigh"));
    assert_eq!(usage.provider_reasoning_effort().as_deref(), Some("max"));

    usage.request_body = Some(json!({ "model": "gpt-5" }));
    assert_eq!(usage.requested_reasoning_effort(), None);

    usage.request_body = None;
    assert_eq!(usage.requested_reasoning_effort().as_deref(), Some("low"));
}

#[test]
fn explicit_none_capture_ignores_stale_request_bodies_and_metadata() {
    let mut usage = sample_usage();
    usage.request_body = Some(json!({
        "reasoning": { "effort": "xhigh" }
    }));
    usage.request_body_state = Some(UsageBodyCaptureState::None);
    usage.provider_request_body = Some(json!({
        "reasoning_effort": "max",
        "service_tier": "priority"
    }));
    usage.provider_request_body_state = Some(UsageBodyCaptureState::None);
    usage.request_metadata = Some(json!({
        "requested_reasoning_effort": "high",
        "provider_reasoning_effort": "medium",
        "provider_service_tier": "priority"
    }));

    assert_eq!(usage.requested_reasoning_effort(), None);
    assert_eq!(usage.provider_reasoning_effort(), None);
    assert_eq!(usage.provider_service_tier(), None);
}

#[test]
fn reasoning_mapping_uses_derived_metadata_after_request_bodies_are_truncated() {
    let truncated = json!({
        "truncated": true,
        "reason": "body_capture_limit_exceeded"
    });
    let mut usage = sample_usage();
    usage.request_body = Some(truncated.clone());
    usage.request_body_state = Some(UsageBodyCaptureState::Truncated);
    usage.provider_request_body = Some(truncated);
    usage.provider_request_body_state = Some(UsageBodyCaptureState::Truncated);
    usage.request_metadata = Some(json!({
        "requested_reasoning_effort": "xhigh",
        "provider_reasoning_effort": "max"
    }));

    assert_eq!(usage.requested_reasoning_effort().as_deref(), Some("xhigh"));
    assert_eq!(usage.provider_reasoning_effort().as_deref(), Some("max"));
}

#[test]
fn provider_service_tier_does_not_infer_fast_from_response_or_stale_metadata() {
    let mut usage = sample_usage();
    usage.provider_request_body = Some(json!({
        "model": "gpt-5"
    }));
    usage.request_metadata = Some(json!({
        "provider_service_tier": "priority",
        "provider_actual_service_tier": "priority"
    }));
    usage.response_body = Some(json!({
        "service_tier": "priority"
    }));

    assert_eq!(usage.provider_service_tier(), None);
}

#[test]
fn provider_service_tier_uses_derived_metadata_after_final_body_is_stripped() {
    let mut usage = sample_usage();
    usage.provider_request_body = None;
    usage.provider_request_body_state = Some(UsageBodyCaptureState::Disabled);
    usage.request_metadata = Some(json!({
        "provider_service_tier": "priority"
    }));
    usage.response_body = Some(json!({
        "service_tier": "default"
    }));

    assert_eq!(usage.provider_service_tier().as_deref(), Some("priority"));
}

#[test]
fn provider_service_tier_uses_request_derived_metadata_after_body_truncation() {
    let mut usage = sample_usage();
    usage.provider_request_body = Some(json!({
        "truncated": true,
        "reason": "body_capture_limit_exceeded",
        "max_bytes": 128,
        "source_bytes": 4096,
        "value_kind": "object"
    }));
    usage.provider_request_body_state = Some(UsageBodyCaptureState::Truncated);
    usage.request_metadata = Some(json!({
        "provider_service_tier": "priority"
    }));
    usage.response_body = Some(json!({
        "service_tier": "default"
    }));

    assert_eq!(usage.provider_service_tier().as_deref(), Some("priority"));
}

#[test]
fn provider_cache_ttl_uses_final_openai_contract_then_preserved_metadata() {
    let mut usage = sample_usage();
    usage.model = "gpt-5.6-sol".to_string();
    usage.target_model = Some("gpt-5.6-sol".to_string());
    usage.provider_request_body = Some(json!({"model": "gpt-5.6-sol"}));
    usage.request_metadata = Some(json!({"provider_cache_ttl_minutes": 60}));

    assert_eq!(usage.provider_cache_ttl_minutes(), Some(30));

    usage.provider_request_body = None;
    usage.request_metadata = Some(json!({"provider_cache_ttl_minutes": 30}));
    assert_eq!(usage.provider_cache_ttl_minutes(), Some(30));
}

#[test]
fn provider_cache_ttl_prefers_final_body_model() {
    assert_eq!(
        resolve_provider_cache_ttl_minutes(
            Some("openai:responses"),
            Some("gpt-5.5"),
            Some("client-alias"),
            Some(&json!({"model": "gpt-5.6-sol"})),
        ),
        Some(30)
    );
    assert_eq!(
        resolve_provider_cache_ttl_minutes(
            Some("openai:responses"),
            Some("gpt-5.6-sol"),
            Some("client-alias"),
            Some(&json!({"model": "gpt-5.5"})),
        ),
        None
    );
}

#[test]
fn actual_service_tier_is_independent_from_requested_tier() {
    let mut usage = sample_usage();
    usage.request_metadata = Some(json!({
        "provider_service_tier": "priority",
        "provider_actual_service_tier": "Default"
    }));

    assert_eq!(usage.provider_service_tier().as_deref(), Some("priority"));
    assert_eq!(
        usage.provider_actual_service_tier().as_deref(),
        Some("default")
    );

    usage.response_body = Some(json!({"service_tier": "flex"}));
    assert_eq!(
        usage.provider_actual_service_tier().as_deref(),
        Some("default")
    );

    usage.request_metadata = Some(json!({"provider_service_tier": "priority"}));
    assert_eq!(
        usage.provider_actual_service_tier().as_deref(),
        Some("flex")
    );
}

#[test]
fn extracts_terminal_actual_service_tier_from_sync_and_stream_responses() {
    let sync = json!({"id": "resp_1", "service_tier": "Priority"});
    let chat_stream = json!({
        "service_tier": "priority",
        "chunks": [
            {"service_tier": "priority"},
            {"service_tier": "Default", "usage": {"total_tokens": 12}}
        ]
    });
    let responses_stream = json!({
        "chunks": [
            {"type": "response.created", "response": {"service_tier": "priority"}},
            {
                "type": "response.completed",
                "response": {"service_tier": "Flex", "status": "completed"}
            }
        ]
    });

    assert_eq!(
        extract_provider_actual_service_tier_from_response(Some(&sync)).as_deref(),
        Some("priority")
    );
    assert_eq!(
        extract_provider_actual_service_tier_from_response(Some(&chat_stream)).as_deref(),
        Some("default")
    );
    assert_eq!(
        extract_provider_actual_service_tier_from_response(Some(&responses_stream)).as_deref(),
        Some("flex")
    );
}

#[test]
fn extracts_anthropic_fast_speed_as_its_own_processing_tier() {
    let request = json!({"speed": " FAST ", "service_tier": "default"});
    let sync_response = json!({
        "usage": {"speed": "fast", "service_tier": "standard"}
    });
    let stream_response = json!({
        "chunks": [{
            "type": "message_start",
            "message": {"usage": {"speed": "fast", "service_tier": "standard"}}
        }]
    });
    let standard_response = json!({
        "usage": {"speed": "standard"}
    });

    assert_eq!(
        extract_provider_service_tier_from_body(Some(&request)).as_deref(),
        Some("fast")
    );
    assert_eq!(
        extract_provider_actual_service_tier_from_response(Some(&sync_response)).as_deref(),
        Some("fast")
    );
    assert_eq!(
        extract_provider_actual_service_tier_from_response(Some(&stream_response)).as_deref(),
        Some("fast")
    );
    assert_eq!(
        extract_provider_actual_service_tier_from_response(Some(&standard_response)).as_deref(),
        Some("standard")
    );
}

#[test]
fn request_body_capture_json_entry_includes_capture_source_and_body_ref() {
    let mut usage = sample_usage();
    usage.request_body_state = Some(UsageBodyCaptureState::Reference);

    let entry = usage.request_body_capture_json_entry();

    assert_eq!(entry.get("available"), Some(&Value::Bool(true)));
    assert_eq!(
        entry.get("storage"),
        Some(&Value::String("reference".to_string()))
    );
    assert_eq!(
        entry.get("state"),
        Some(&Value::String("reference".to_string()))
    );
    assert_eq!(
        entry.get("body_ref"),
        Some(&Value::String(
            "usage://request/req-1/request_body".to_string()
        ))
    );
    assert_eq!(
        entry.get("capture_source"),
        Some(&Value::String("stored_reference".to_string()))
    );
}

#[test]
fn curl_body_source_prefers_provider_request_body_over_request_body() {
    let mut usage = sample_usage();
    usage.request_body = Some(json!({"client": true}));
    usage.provider_request_body = Some(json!({"provider": true}));

    assert_eq!(
        usage.preferred_request_body_source_field(),
        Some(UsageBodyField::ProviderRequestBody)
    );
    assert_eq!(usage.curl_body_source(), "provider_request");

    usage.provider_request_body = None;
    assert_eq!(
        usage.preferred_request_body_source_field(),
        Some(UsageBodyField::RequestBody)
    );
    assert_eq!(usage.curl_body_source(), "request");

    usage.request_body = None;
    assert_eq!(usage.preferred_request_body_source_field(), None);
    assert_eq!(usage.curl_body_source(), "unavailable");
}
