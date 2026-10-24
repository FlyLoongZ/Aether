use std::sync::Arc;

use aether_data_contracts::repository::billing::StoredBillingModelContext;
use aether_data_contracts::repository::usage::UsageBodyCaptureState;
use aether_runtime_state::{MemoryRuntimeStateConfig, RuntimeState};
use aether_usage_runtime::{
    UsageEvent, UsageEventData, UsageEventType, UsageQueue, UsageRuntimeConfig,
};
use async_trait::async_trait;
use serde_json::json;
use serde_json::Value;

use super::{
    enrich_usage_event_with_billing, usage_event_processing_tiers, BillingModelContextLookup,
};

struct TestLookup {
    name_context: Option<StoredBillingModelContext>,
    model_id_context: Option<StoredBillingModelContext>,
}

#[async_trait]
impl BillingModelContextLookup for TestLookup {
    async fn find_billing_model_context_by_model_id(
        &self,
        _provider_id: &str,
        _provider_api_key_id: Option<&str>,
        _model_id: &str,
    ) -> Result<Option<StoredBillingModelContext>, aether_data_contracts::DataLayerError> {
        Ok(self.model_id_context.clone())
    }

    async fn find_billing_model_context(
        &self,
        _provider_id: &str,
        _provider_api_key_id: Option<&str>,
        _global_model_name: &str,
    ) -> Result<Option<StoredBillingModelContext>, aether_data_contracts::DataLayerError> {
        Ok(self.name_context.clone())
    }
}

fn wire_billing_lookup(pricing: Option<Value>, request_price: Option<f64>) -> TestLookup {
    TestLookup {
        name_context: Some(
            StoredBillingModelContext::new(
                "provider-wire".to_string(),
                Some("pay_as_you_go".to_string()),
                Some("key-wire".to_string()),
                None,
                Some(5),
                "global-model-wire".to_string(),
                "wire-model".to_string(),
                None,
                request_price,
                pricing,
                Some("model-wire".to_string()),
                Some("wire-model".to_string()),
                None,
                None,
                None,
            )
            .expect("wire billing context"),
        ),
        model_id_context: None,
    }
}

fn wire_billing_event(request_id: &str) -> UsageEvent {
    UsageEvent::new(
        UsageEventType::Completed,
        request_id,
        UsageEventData {
            user_id: Some("user-wire".to_string()),
            api_key_id: Some("key-wire".to_string()),
            provider_name: "OpenAI".to_string(),
            provider_id: Some("provider-wire".to_string()),
            provider_api_key_id: Some("key-wire".to_string()),
            model: "gpt-5.6-sol".to_string(),
            target_model: Some("gpt-5.6-sol".to_string()),
            request_type: Some("chat".to_string()),
            api_format: Some("openai:responses".to_string()),
            endpoint_api_format: Some("openai:responses".to_string()),
            input_tokens: Some(1_000),
            output_tokens: Some(100),
            total_tokens: Some(1_100),
            cache_creation_input_tokens: Some(0),
            cache_creation_ephemeral_5m_input_tokens: Some(0),
            cache_creation_ephemeral_1h_input_tokens: Some(0),
            cache_read_input_tokens: Some(0),
            status_code: Some(200),
            first_byte_time_ms: Some(12),
            response_time_ms: Some(30),
            request_headers: Some(json!({"x-audit": "request"})),
            provider_request_headers: Some(json!({"x-audit": "provider request"})),
            response_headers: Some(json!({"x-audit": "provider response"})),
            client_response_headers: Some(json!({"x-audit": "client response"})),
            provider_request_body: Some(json!({"model": "gpt-5.6-sol"})),
            provider_request_body_state: Some(UsageBodyCaptureState::Inline),
            response_body: Some(json!({"service_tier": "default", "output": "x".repeat(8192)})),
            response_body_state: Some(UsageBodyCaptureState::Inline),
            request_metadata: Some(json!({
                "usage_available": true,
                "usage_pricing_available": true,
                "api_key_is_standalone": true,
                "plan_usage_reservation_token": "550e8400-e29b-41d4-a716-446655440000",
                "plan_usage_reservation_deferred": true
            })),
            ..UsageEventData::default()
        },
    )
}

fn wire_billing_result(event: &UsageEvent) -> Value {
    let mut data = serde_json::to_value(&event.data).expect("serialized billing event");
    let object = data.as_object_mut().expect("event data object");
    for key in [
        "request_body",
        "provider_request_body",
        "response_body",
        "client_response_body",
        "request_body_state",
        "provider_request_body_state",
        "response_body_state",
        "client_response_body_state",
        "request_headers",
        "provider_request_headers",
        "response_headers",
        "client_response_headers",
    ] {
        object.remove(key);
    }
    if let Some(metadata) = object
        .get_mut("request_metadata")
        .and_then(Value::as_object_mut)
    {
        metadata.retain(|key, _| {
            matches!(
                key.as_str(),
                "usage_available"
                    | "usage_pricing_available"
                    | "api_key_is_standalone"
                    | "plan_usage_reservation_token"
                    | "plan_usage_reservation_deferred"
                    | "cancelled_request_fee"
                    | "dimensions"
                    | "billing_dimensions"
                    | "billing_snapshot"
                    | "settlement_snapshot"
                    | "rate_multiplier"
                    | "is_free_tier"
                    | "settlement_snapshot_schema_version"
            )
        });
        for key in ["billing_snapshot", "settlement_snapshot"] {
            if let Some(snapshot) = metadata.get_mut(key).and_then(Value::as_object_mut) {
                snapshot.remove("calculated_at");
            }
        }
    }
    json!({
        "event_type": event.event_type,
        "request_id": event.request_id,
        "timestamp_ms": event.timestamp_ms,
        "data": data,
    })
}

async fn assert_wire_billing_equivalent(lookup: &TestLookup, original: UsageEvent) -> UsageEvent {
    const LIMIT: usize = 4096;
    let original_fields = original.to_stream_fields().expect("legacy full envelope");
    assert!(original_fields["payload"].len() > LIMIT);
    let provider_body_present = original
        .data
        .provider_request_body
        .as_ref()
        .is_some_and(|body| !body.is_null());
    let queue = UsageQueue::new(
        Arc::new(RuntimeState::memory(MemoryRuntimeStateConfig::default())),
        UsageRuntimeConfig {
            enabled: true,
            queue_payload_max_bytes: LIMIT,
            consumer_block_ms: 1,
            ..UsageRuntimeConfig::default()
        },
    )
    .expect("bounded billing queue");
    queue.ensure_consumer_group().await.expect("billing group");
    queue
        .enqueue(&original)
        .await
        .expect("diagnostic projection should fit");
    let entries = queue
        .read_group("billing-wire-reader")
        .await
        .expect("billing queue read");
    assert_eq!(entries.len(), 1);
    assert!(entries[0].fields["payload"].len() <= LIMIT);
    // The legacy consumer also prepares request facts after decoding the full envelope.
    let mut original =
        UsageEvent::from_stream_fields(&original_fields).expect("legacy consumer event");
    let mut queued = UsageEvent::from_stream_fields(&entries[0].fields).expect("projected event");
    assert!(original.data.response_body.is_some());
    assert_eq!(
        original.data.provider_request_body.is_some(),
        provider_body_present
    );
    assert!(queued.data.response_body.is_none());
    assert!(queued.data.request_headers.is_none());
    assert!(queued.data.provider_request_headers.is_none());
    assert!(queued.data.response_headers.is_none());
    assert!(queued.data.client_response_headers.is_none());
    assert_eq!(
        queued.data.response_body_state,
        Some(UsageBodyCaptureState::Truncated)
    );

    enrich_usage_event_with_billing(lookup, &mut original)
        .await
        .expect("original billing");
    enrich_usage_event_with_billing(lookup, &mut queued)
        .await
        .expect("projected billing");
    assert_eq!(wire_billing_result(&queued), wire_billing_result(&original));
    queued
}

#[tokio::test]
async fn wire_projection_preserves_openai_requested_tier_and_effective_cache_ttl() {
    let lookup = wire_billing_lookup(
        Some(json!({
            "tiers": [{"up_to": null, "input_price_per_1m": 5.0,
                "output_price_per_1m": 30.0, "cache_creation_price_per_1m": 6.25,
                "cache_read_price_per_1m": 0.5,
                "cache_ttl_pricing": [{"ttl_minutes": 60,
                    "cache_creation_price_per_1m": 100.0, "cache_read_price_per_1m": 100.0}]}],
            "processing_tiers": {"priority": {"price_multiplier": 2.0}}
        })),
        None,
    );
    let mut event = wire_billing_event("wire-openai-tier");
    event.data.cache_creation_input_tokens = Some(100);
    event.data.provider_request_body = Some(json!({
        "model": "gpt-5.6-sol", "service_tier": "priority", "reasoning": {"effort": "high"}
    }));
    event.data.request_metadata.as_mut().unwrap()["provider_service_tier"] = json!("flex");
    event.data.request_metadata.as_mut().unwrap()["provider_actual_service_tier"] = json!("flex");
    let queued = assert_wire_billing_equivalent(&lookup, event).await;
    let metadata = queued.data.request_metadata.as_ref().unwrap();
    assert_eq!(metadata["provider_service_tier"], "priority");
    assert_eq!(metadata["provider_actual_service_tier"], "flex");
    assert_eq!(metadata["provider_reasoning_effort"], "high");
    assert_eq!(metadata["billing_dimensions"]["cache_ttl_minutes"], 30);
    assert_eq!(
        metadata["billing_dimensions"]["billing_processing_tier"],
        "priority"
    );
    assert!(queued.data.total_cost_usd.unwrap() > 0.0);
}

#[tokio::test]
async fn wire_projection_preserves_non_object_body_authority_and_null_decode_semantics() {
    let lookup = wire_billing_lookup(
        Some(json!({
            "tiers": [{"up_to": null, "input_price_per_1m": 5.0,
                "output_price_per_1m": 30.0, "cache_creation_price_per_1m": 6.25,
                "cache_read_price_per_1m": 0.5,
                "cache_ttl_pricing": [{"ttl_minutes": 60,
                    "cache_creation_price_per_1m": 100.0, "cache_read_price_per_1m": 100.0}]}],
            "processing_tiers": {"priority": {"price_multiplier": 2.0}}
        })),
        None,
    );
    for (kind, body) in [
        ("string", json!("not an object")),
        ("array", json!([{"service_tier": "flex"}])),
        ("number", json!(42)),
        ("boolean", json!(false)),
        ("null", Value::Null),
    ] {
        for state in [
            Some(UsageBodyCaptureState::Inline),
            Some(UsageBodyCaptureState::Reference),
            None,
        ] {
            let mut event = wire_billing_event(&format!("wire-{kind}-{state:?}"));
            event.data.input_tokens = Some(1_000_000);
            event.data.output_tokens = Some(0);
            event.data.total_tokens = Some(1_000_000);
            event.data.cache_creation_input_tokens = Some(1_000_000);
            event.data.provider_request_body = Some(body.clone());
            event.data.provider_request_body_state = state;
            let metadata = event.data.request_metadata.as_mut().unwrap();
            metadata["provider_service_tier"] = json!("priority");
            metadata["provider_reasoning_effort"] = json!("high");
            metadata["provider_cache_ttl_minutes"] = json!(60);

            let queued = assert_wire_billing_equivalent(&lookup, event).await;
            let metadata = queued.data.request_metadata.as_ref().unwrap();
            assert!(queued.data.provider_request_body.is_none());
            assert_eq!(metadata["provider_cache_ttl_minutes"], 60);
            assert_eq!(metadata["billing_dimensions"]["cache_ttl_minutes"], 60);
            let expected_tier = if body.is_null() && state.is_some() {
                Some("priority")
            } else {
                None
            };
            assert_eq!(
                usage_event_processing_tiers(&queued.data)
                    .requested
                    .as_deref(),
                expected_tier,
                "{kind} with {state:?}"
            );
            assert_eq!(
                queued.data.total_cost_usd,
                Some(if expected_tier.is_some() {
                    200.0
                } else {
                    100.0
                }),
                "{kind} with {state:?}"
            );
            if body.is_null() {
                // Option<Value> decodes JSON null as absent, so the old capture marker remains.
                assert_eq!(queued.data.provider_request_body_state, state);
                assert_eq!(metadata["provider_service_tier"], "priority");
                assert_eq!(metadata["provider_reasoning_effort"], "high");
            } else {
                assert_eq!(
                    queued.data.provider_request_body_state,
                    Some(UsageBodyCaptureState::Truncated)
                );
                assert!(metadata.get("provider_service_tier").is_none());
                assert!(metadata.get("provider_reasoning_effort").is_none());
            }
        }
    }
}

#[tokio::test]
async fn wire_projection_preserves_raw_body_ttl_with_non_authoritative_capture_states() {
    let lookup = wire_billing_lookup(
        Some(json!({
            "tiers": [{"up_to": null, "input_price_per_1m": 5.0,
                "output_price_per_1m": 30.0, "cache_creation_price_per_1m": 6.25,
                "cache_read_price_per_1m": 0.5,
                "cache_ttl_pricing": [{"ttl_minutes": 60,
                    "cache_creation_price_per_1m": 100.0, "cache_read_price_per_1m": 100.0}]}]
        })),
        None,
    );
    for state in [
        UsageBodyCaptureState::Disabled,
        UsageBodyCaptureState::Unavailable,
        UsageBodyCaptureState::Truncated,
    ] {
        let mut event = wire_billing_event(&format!("wire-capture-state-{state:?}"));
        event.data.input_tokens = Some(1_000_000);
        event.data.output_tokens = Some(0);
        event.data.total_tokens = Some(1_000_000);
        event.data.cache_creation_input_tokens = Some(1_000_000);
        event.data.provider_request_body = Some(json!({
            "model": "gpt-5.6-sol", "prompt_cache_options": {"ttl": "30m"}
        }));
        event.data.provider_request_body_state = Some(state);
        event.data.request_metadata.as_mut().unwrap()["provider_cache_ttl_minutes"] = json!(60);

        let queued = assert_wire_billing_equivalent(&lookup, event).await;
        assert_eq!(queued.data.provider_request_body_state, Some(state));
        assert!(queued.data.provider_request_body.is_none());
        assert_eq!(queued.data.total_cost_usd, Some(6.25));
        assert_eq!(
            queued.data.request_metadata.as_ref().unwrap()["billing_dimensions"]
                ["cache_ttl_minutes"],
            30
        );
    }
}

#[tokio::test]
async fn wire_projection_rejects_typed_none_when_omitting_raw_ttl_would_change_billing() {
    let lookup = wire_billing_lookup(
        Some(json!({
            "tiers": [{"up_to": null, "input_price_per_1m": 5.0,
                "output_price_per_1m": 30.0, "cache_creation_price_per_1m": 6.25,
                "cache_read_price_per_1m": 0.5,
                "cache_ttl_pricing": [{"ttl_minutes": 60,
                    "cache_creation_price_per_1m": 100.0, "cache_read_price_per_1m": 100.0}]}]
        })),
        None,
    );
    let mut event = wire_billing_event("wire-typed-none-ttl");
    event.data.input_tokens = Some(1_000_000);
    event.data.output_tokens = Some(0);
    event.data.total_tokens = Some(1_000_000);
    event.data.cache_creation_input_tokens = Some(1_000_000);
    event.data.provider_request_body = Some(json!({
        "model": "gpt-5.6-sol", "prompt_cache_options": {"ttl": "30m"}
    }));
    event.data.provider_request_body_state = Some(UsageBodyCaptureState::None);
    event.data.request_metadata.as_mut().unwrap()["provider_cache_ttl_minutes"] = json!(60);
    let original_fields = event.to_stream_fields().expect("legacy full envelope");
    assert!(original_fields["payload"].len() > 4096);
    let mut legacy = UsageEvent::from_stream_fields(&original_fields).expect("legacy consumer");
    assert_eq!(
        legacy.data.provider_request_body_state,
        Some(UsageBodyCaptureState::None)
    );
    assert!(legacy.data.provider_request_body.is_some());
    assert!(legacy
        .data
        .request_metadata
        .as_ref()
        .unwrap()
        .get("provider_cache_ttl_minutes")
        .is_none());
    enrich_usage_event_with_billing(&lookup, &mut legacy)
        .await
        .expect("legacy billing");
    assert_eq!(legacy.data.total_cost_usd, Some(6.25));

    let queue = UsageQueue::new(
        Arc::new(RuntimeState::memory(MemoryRuntimeStateConfig::default())),
        UsageRuntimeConfig {
            enabled: true,
            queue_payload_max_bytes: 4096,
            consumer_block_ms: 1,
            ..UsageRuntimeConfig::default()
        },
    )
    .expect("bounded billing queue");
    queue.ensure_consumer_group().await.expect("billing group");
    assert!(matches!(
        queue.enqueue(&event).await,
        Err(aether_data_contracts::DataLayerError::InvalidInput(_))
    ));
    assert_eq!(event.to_stream_fields().unwrap(), original_fields);
    assert!(queue
        .read_group("typed-none-reader")
        .await
        .unwrap()
        .is_empty());

    // The unchanged source event remains usable by the terminal direct-write fallback.
    enrich_usage_event_with_billing(&lookup, &mut event)
        .await
        .expect("direct fallback billing");
    assert_eq!(wire_billing_result(&event), wire_billing_result(&legacy));
}

#[tokio::test]
async fn wire_projection_preserves_claude_cache_ttl_and_explicit_zero_segments() {
    let lookup = wire_billing_lookup(
        Some(json!({
            "tiers": [{"up_to": null, "input_price_per_1m": 3.0,
                "output_price_per_1m": 15.0, "cache_creation_price_per_1m": 3.75,
                "cache_read_price_per_1m": 0.3,
                "cache_ttl_pricing": [{"ttl_minutes": 60,
                    "cache_creation_price_per_1m": 6.0, "cache_read_price_per_1m": 0.6}]}]
        })),
        None,
    );
    let mut event = wire_billing_event("wire-claude-cache");
    event.data.model = "claude-sonnet-4-6".to_string();
    event.data.target_model = Some("claude-sonnet-4-6".to_string());
    event.data.api_format = Some("claude:messages".to_string());
    event.data.endpoint_api_format = Some("claude:messages".to_string());
    event.data.provider_request_body = None;
    event.data.provider_request_body_state = Some(UsageBodyCaptureState::Disabled);
    event.data.cache_creation_input_tokens = Some(200);
    event.data.cache_creation_ephemeral_5m_input_tokens = Some(0);
    event.data.cache_creation_ephemeral_1h_input_tokens = Some(100);
    event.data.request_metadata.as_mut().unwrap()["provider_cache_ttl_minutes"] = json!(60);
    let queued = assert_wire_billing_equivalent(&lookup, event).await;
    assert_eq!(
        queued.data.cache_creation_ephemeral_5m_input_tokens,
        Some(0)
    );
    assert_eq!(queued.data.cache_read_input_tokens, Some(0));
    let dimensions = &queued.data.request_metadata.as_ref().unwrap()["billing_dimensions"];
    assert_eq!(dimensions["cache_ttl_minutes"], 60);
    assert_eq!(dimensions["cache_creation_ephemeral_1h_tokens"], 100);
    assert_eq!(dimensions["cache_creation_uncategorized_tokens"], 100);
}

#[tokio::test]
async fn wire_projection_preserves_unknown_zero_error_and_cancellation_billing() {
    let lookup = wire_billing_lookup(None, Some(0.02));
    for mode in ["unknown", "unpriced", "zero", "error_present", "cancelled"] {
        let mut event = wire_billing_event(mode);
        event.data.input_tokens = Some(0);
        event.data.output_tokens = Some(0);
        event.data.total_tokens = Some(0);
        match mode {
            "unknown" => {
                event.data.input_tokens = None;
                event.data.output_tokens = None;
                event.data.total_tokens = None;
                event.data.request_metadata.as_mut().unwrap()["usage_available"] = json!(false);
            }
            "unpriced" => {
                event.data.input_tokens = Some(12);
                event.data.output_tokens = Some(3);
                event.data.total_tokens = Some(15);
                event.data.request_metadata.as_mut().unwrap()["usage_pricing_available"] =
                    json!(false);
            }
            "error_present" => event.data.error_message = Some(String::new()),
            "cancelled" => event.event_type = UsageEventType::Cancelled,
            _ => {}
        }
        let queued = assert_wire_billing_equivalent(&lookup, event).await;
        match mode {
            "unknown" => {
                assert_eq!(queued.data.input_tokens, None);
                assert_eq!(queued.data.total_cost_usd, None);
            }
            "unpriced" => {
                assert_eq!(queued.data.input_tokens, Some(12));
                assert_eq!(queued.data.total_cost_usd, None);
            }
            "error_present" => {
                assert_eq!(queued.data.error_message.as_deref(), Some(""));
                assert_eq!(queued.data.total_cost_usd, Some(0.0));
            }
            "zero" | "cancelled" => assert_eq!(queued.data.total_cost_usd, Some(0.02)),
            _ => unreachable!(),
        }
    }
}

#[tokio::test]
async fn wire_projection_preserves_image_matrix_dimensions_and_request_count() {
    let lookup = wire_billing_lookup(
        Some(json!({
            "image_output_price_default": 0.01,
            "image_output_prices": {"1536x1024": {"medium": 0.041, "high": 0.165}}
        })),
        Some(0.02),
    );
    let mut event = wire_billing_event("wire-image");
    event.data.request_type = Some("image".to_string());
    event.data.api_format = Some("openai:image".to_string());
    event.data.endpoint_api_format = Some("openai:image".to_string());
    event.data.input_tokens = Some(0);
    event.data.output_tokens = Some(0);
    event.data.total_tokens = Some(0);
    event.data.request_metadata.as_mut().unwrap()["dimensions"] = json!({
        "image_count": 2, "image_size": "1536x1024", "image_quality": "medium",
        "image_output_format": "png"
    });
    let queued = assert_wire_billing_equivalent(&lookup, event).await;
    let metadata = queued.data.request_metadata.as_ref().unwrap();
    assert_eq!(metadata["billing_dimensions"]["image_count"], 2);
    assert_eq!(metadata["billing_dimensions"]["request_count"], 2);
    assert_eq!(
        metadata["billing_dimensions"]["image_price_key"],
        "1536x1024:medium"
    );
    assert_eq!(
        metadata["billing_snapshot"]["cost_breakdown"]["image_output_cost"],
        0.082
    );
    assert_eq!(
        metadata["billing_snapshot"]["cost_breakdown"]["request_cost"],
        0.04
    );
}

#[tokio::test]
async fn unmetered_session_audit_does_not_fabricate_tokens_or_request_cost() {
    let lookup = TestLookup {
        name_context: None,
        model_id_context: None,
    };
    let mut event = UsageEvent::new(
        UsageEventType::Completed,
        "req-live-unmetered",
        UsageEventData {
            provider_name: "OpenAI".to_string(),
            provider_id: Some("provider-live".to_string()),
            model: "gpt-live".to_string(),
            input_tokens: Some(0),
            output_tokens: Some(0),
            total_tokens: Some(0),
            total_cost_usd: Some(0.0),
            actual_total_cost_usd: Some(0.0),
            request_metadata: Some(json!({"usage_available": false})),
            ..UsageEventData::default()
        },
    );

    enrich_usage_event_with_billing(&lookup, &mut event)
        .await
        .expect("unmetered session audit should be accepted");

    assert_eq!(event.data.input_tokens, None);
    assert_eq!(event.data.output_tokens, None);
    assert_eq!(event.data.total_tokens, None);
    assert_eq!(event.data.total_cost_usd, None);
    assert_eq!(event.data.actual_total_cost_usd, None);
}

#[tokio::test]
async fn authoritative_realtime_audio_tokens_are_visible_but_not_text_priced() {
    let lookup = TestLookup {
        name_context: None,
        model_id_context: None,
    };
    let mut event = UsageEvent::new(
        UsageEventType::Completed,
        "req-realtime-audio",
        UsageEventData {
            provider_name: "OpenAI".to_string(),
            provider_id: Some("provider-realtime".to_string()),
            model: "gpt-realtime".to_string(),
            input_tokens: Some(120),
            output_tokens: Some(40),
            total_tokens: Some(160),
            request_metadata: Some(json!({
                "usage_available": true,
                "usage_pricing_available": false,
            })),
            ..UsageEventData::default()
        },
    );

    enrich_usage_event_with_billing(&lookup, &mut event)
        .await
        .expect("unsupported audio pricing should not fail enrichment");

    assert_eq!(event.data.input_tokens, Some(120));
    assert_eq!(event.data.output_tokens, Some(40));
    assert_eq!(event.data.total_tokens, Some(160));
    assert_eq!(event.data.total_cost_usd, None);
    assert_eq!(event.data.actual_total_cost_usd, None);
}

#[test]
fn processing_tier_fact_uses_final_provider_request_body() {
    let data = UsageEventData {
        provider_request_body: Some(json!({"service_tier": "Priority"})),
        response_body: Some(json!({"service_tier": "priority"})),
        request_metadata: Some(json!({
            "provider_service_tier": "batch",
            "provider_actual_service_tier": "Default"
        })),
        ..UsageEventData::default()
    };

    let tiers = usage_event_processing_tiers(&data);

    assert_eq!(tiers.requested.as_deref(), Some("priority"));
}

#[test]
fn processing_tier_facts_recognize_anthropic_fast_speed() {
    let data = UsageEventData {
        provider_request_body: Some(json!({"speed": "fast"})),
        response_body: Some(json!({
            "usage": {"speed": "fast", "service_tier": "standard"}
        })),
        ..UsageEventData::default()
    };

    let tiers = usage_event_processing_tiers(&data);

    assert_eq!(tiers.requested.as_deref(), Some("fast"));
}

#[test]
fn processing_tier_does_not_fall_back_to_stale_metadata_when_body_is_present() {
    let data = UsageEventData {
        provider_request_body: Some(json!({"model": "gpt-5"})),
        response_body: Some(json!({"service_tier": "priority"})),
        request_metadata: Some(json!({
            "provider_service_tier": "priority",
            "provider_actual_service_tier": "priority"
        })),
        ..UsageEventData::default()
    };

    let tiers = usage_event_processing_tiers(&data);

    assert_eq!(tiers.requested, None);
}

#[test]
fn processing_tier_uses_request_derived_metadata_when_body_capture_was_disabled() {
    let data = UsageEventData {
        provider_request_body: None,
        provider_request_body_state: Some(UsageBodyCaptureState::Disabled),
        response_body: Some(json!({"service_tier": "flex"})),
        request_metadata: Some(json!({
            "provider_service_tier": "priority",
            "provider_actual_service_tier": "flex"
        })),
        ..UsageEventData::default()
    };

    let tiers = usage_event_processing_tiers(&data);

    assert_eq!(tiers.requested.as_deref(), Some("priority"));
}

#[test]
fn processing_tier_does_not_use_metadata_or_response_when_final_request_body_is_missing() {
    let data = UsageEventData {
        provider_request_body: None,
        provider_request_body_state: Some(UsageBodyCaptureState::None),
        response_body: Some(json!({"service_tier": "priority"})),
        request_metadata: Some(json!({
            "provider_service_tier": "priority",
            "provider_actual_service_tier": "priority"
        })),
        ..UsageEventData::default()
    };

    let tiers = usage_event_processing_tiers(&data);

    assert_eq!(tiers.requested, None);
}

#[test]
fn processing_tier_uses_request_derived_metadata_when_body_capture_was_truncated() {
    let data = UsageEventData {
        provider_request_body: Some(json!({
            "truncated": true,
            "reason": "body_capture_limit_exceeded",
            "max_bytes": 128,
            "source_bytes": 4096,
            "value_kind": "object"
        })),
        provider_request_body_state: Some(UsageBodyCaptureState::Truncated),
        response_body: Some(json!({"service_tier": "default"})),
        request_metadata: Some(json!({
            "provider_service_tier": "priority",
            "provider_actual_service_tier": "default"
        })),
        ..UsageEventData::default()
    };

    let tiers = usage_event_processing_tiers(&data);

    assert_eq!(tiers.requested.as_deref(), Some("priority"));
}

#[tokio::test]
async fn settlement_uses_effective_gpt_5_6_cache_ttl_after_body_capture() {
    let lookup = TestLookup {
        name_context: Some(
            StoredBillingModelContext::new(
                "provider-1".to_string(),
                Some("pay_as_you_go".to_string()),
                Some("key-1".to_string()),
                None,
                Some(60),
                "global-model-1".to_string(),
                "gpt-5.6-sol".to_string(),
                None,
                None,
                Some(json!({
                    "tiers": [{
                        "up_to": null,
                        "input_price_per_1m": 5.0,
                        "output_price_per_1m": 30.0,
                        "cache_creation_price_per_1m": 6.25,
                        "cache_read_price_per_1m": 0.5,
                        "cache_ttl_pricing": [{
                            "ttl_minutes": 60,
                            "cache_creation_price_per_1m": 100.0,
                            "cache_read_price_per_1m": 100.0
                        }]
                    }]
                })),
                Some("model-1".to_string()),
                Some("gpt-5.6-sol".to_string()),
                None,
                None,
                None,
            )
            .expect("billing context should build"),
        ),
        model_id_context: None,
    };

    for (request_id, provider_request_body, request_metadata) in [
        (
            "req-cache-body",
            Some(json!({"model": "gpt-5.6-sol"})),
            None,
        ),
        (
            "req-cache-metadata",
            None,
            Some(json!({"provider_cache_ttl_minutes": 30})),
        ),
    ] {
        let mut event = UsageEvent::new(
            UsageEventType::Completed,
            request_id,
            UsageEventData {
                provider_name: "OpenAI".to_string(),
                model: "gpt-5.6-sol".to_string(),
                target_model: Some("gpt-5.6-sol".to_string()),
                provider_id: Some("provider-1".to_string()),
                provider_api_key_id: Some("key-1".to_string()),
                request_type: Some("chat".to_string()),
                api_format: Some("openai:responses".to_string()),
                endpoint_api_format: Some("openai:responses".to_string()),
                provider_request_body,
                request_metadata,
                input_tokens: Some(1_000_000),
                cache_creation_input_tokens: Some(1_000_000),
                status_code: Some(200),
                ..UsageEventData::default()
            },
        );

        enrich_usage_event_with_billing(&lookup, &mut event)
            .await
            .expect("billing should succeed");

        let snapshot = event
            .data
            .request_metadata
            .as_ref()
            .and_then(|value| value.get("billing_snapshot"))
            .expect("billing snapshot should exist");
        assert_eq!(
            snapshot
                .get("resolved_dimensions")
                .and_then(|value| value.get("cache_ttl_minutes")),
            Some(&json!(30))
        );
        assert_eq!(
            snapshot
                .get("resolved_variables")
                .and_then(|value| value.get("cache_creation_price_per_1m")),
            Some(&json!(6.25))
        );
        assert_eq!(event.data.total_cost_usd, Some(6.25));
    }
}

#[tokio::test]
async fn enriches_completed_usage_event_with_billing_snapshot() {
    let lookup = TestLookup {
        name_context: Some(
            StoredBillingModelContext::new(
                "provider-1".to_string(),
                Some("pay_as_you_go".to_string()),
                Some("key-1".to_string()),
                Some(json!({"openai:chat": 0.5})),
                Some(60),
                "global-model-1".to_string(),
                "gpt-5".to_string(),
                None,
                Some(0.02),
                Some(json!({"tiers":[{"up_to":null,"input_price_per_1m":3.0,"output_price_per_1m":15.0,"cache_creation_price_per_1m":3.75,"cache_read_price_per_1m":0.30}]})),
                Some("model-1".to_string()),
                Some("gpt-5-upstream".to_string()),
                None,
                None,
                None,
            )
            .expect("billing context should build"),
        ),
        model_id_context: None,
    };
    let mut event = UsageEvent::new(
        UsageEventType::Completed,
        "req-billing-1",
        UsageEventData {
            provider_name: "OpenAI".to_string(),
            model: "gpt-5".to_string(),
            provider_id: Some("provider-1".to_string()),
            provider_api_key_id: Some("key-1".to_string()),
            request_type: Some("chat".to_string()),
            api_format: Some("openai:chat".to_string()),
            endpoint_api_format: Some("openai:chat".to_string()),
            input_tokens: Some(1_000),
            output_tokens: Some(500),
            cache_read_input_tokens: Some(100),
            status_code: Some(200),
            ..UsageEventData::default()
        },
    );

    enrich_usage_event_with_billing(&lookup, &mut event)
        .await
        .expect("billing should succeed");

    assert!(event.data.total_cost_usd.unwrap_or_default() > 0.0);
    assert!(event.data.actual_total_cost_usd.unwrap_or_default() > 0.0);
    assert_eq!(
        event
            .data
            .request_metadata
            .as_ref()
            .and_then(|value| value.get("billing_snapshot"))
            .and_then(|value| value.get("status"))
            .and_then(Value::as_str),
        Some("complete")
    );
}

#[tokio::test]
async fn openai_fast_usage_without_overlay_inherits_global_model_pricing() {
    let lookup = TestLookup {
        name_context: Some(
            StoredBillingModelContext::new(
                "provider-1".to_string(),
                Some("pay_as_you_go".to_string()),
                Some("key-1".to_string()),
                None,
                None,
                "global-model-1".to_string(),
                "gpt-5.6-sol".to_string(),
                None,
                None,
                Some(json!({
                    "tiers": [{
                        "up_to": null,
                        "input_price_per_1m": 3.0,
                        "output_price_per_1m": 15.0
                    }]
                })),
                Some("model-1".to_string()),
                Some("gpt-5.6-sol".to_string()),
                None,
                None,
                None,
            )
            .expect("billing context should build"),
        ),
        model_id_context: None,
    };
    let mut event = UsageEvent::new(
        UsageEventType::Completed,
        "req-fast-global-fallback",
        UsageEventData {
            provider_name: "OpenAI".to_string(),
            model: "gpt-5.6-sol".to_string(),
            target_model: Some("gpt-5.6-sol".to_string()),
            provider_id: Some("provider-1".to_string()),
            provider_api_key_id: Some("key-1".to_string()),
            request_type: Some("chat".to_string()),
            api_format: Some("openai:responses".to_string()),
            endpoint_api_format: Some("openai:responses".to_string()),
            // OpenAI calls the Fast request tier `priority` on the wire.
            provider_request_body: Some(json!({
                "model": "gpt-5.6-sol",
                "service_tier": "priority"
            })),
            input_tokens: Some(1_000_000),
            status_code: Some(200),
            ..UsageEventData::default()
        },
    );

    enrich_usage_event_with_billing(&lookup, &mut event)
        .await
        .expect("billing should succeed");

    assert_eq!(event.data.total_cost_usd, Some(3.0));
    let metadata = event.data.request_metadata.as_ref().expect("metadata");
    assert_eq!(
        metadata.pointer("/billing_snapshot/status"),
        Some(&json!("complete"))
    );
    let pricing = metadata
        .pointer("/settlement_snapshot/pricing_snapshot")
        .expect("settlement pricing snapshot");
    assert_eq!(pricing["billing_processing_tier"], "priority");
    assert_eq!(pricing["pricing_source"], "global_default");
    assert_eq!(pricing["tiered_pricing_source"], "global_default");
    assert_eq!(
        pricing["tiered_pricing"]["tiers"][0]["input_price_per_1m"],
        3.0
    );
}

#[tokio::test]
async fn settlement_uses_requested_processing_tier_catalog_and_ignores_response_tier() {
    let lookup = TestLookup {
        name_context: Some(
            StoredBillingModelContext::new(
                "provider-1".to_string(),
                Some("pay_as_you_go".to_string()),
                Some("key-1".to_string()),
                None,
                Some(60),
                "global-model-1".to_string(),
                "gpt-5.6".to_string(),
                None,
                None,
                Some(json!({
                    "tiers": [{"up_to": null, "input_price_per_1m": 5.0, "output_price_per_1m": 30.0}],
                    "processing_tiers": {
                        "flex": {"price_multiplier": 0.5}
                    }
                })),
                Some("model-1".to_string()),
                Some("gpt-5.6-upstream".to_string()),
                None,
                None,
                Some(json!({
                    "processing_tiers": {
                        "priority": {"tiers": [{"up_to": 272000, "input_price_per_1m": 10.0, "output_price_per_1m": 60.0}]}
                    }
                })),
            )
            .expect("billing context should build"),
        ),
        model_id_context: None,
    };
    let mut event = UsageEvent::new(
        UsageEventType::Completed,
        "req-billing-tier-1",
        UsageEventData {
            provider_name: "OpenAI".to_string(),
            model: "gpt-5.6".to_string(),
            provider_id: Some("provider-1".to_string()),
            provider_api_key_id: Some("key-1".to_string()),
            request_type: Some("chat".to_string()),
            api_format: Some("openai:responses".to_string()),
            endpoint_api_format: Some("openai:responses".to_string()),
            provider_request_body: Some(json!({"service_tier": "priority"})),
            response_body: Some(json!({"service_tier": "priority"})),
            request_metadata: Some(json!({
                "provider_service_tier": "priority",
                "provider_actual_service_tier": "flex"
            })),
            input_tokens: Some(1_000),
            output_tokens: Some(100),
            status_code: Some(200),
            ..UsageEventData::default()
        },
    );

    enrich_usage_event_with_billing(&lookup, &mut event)
        .await
        .expect("billing should succeed");

    let pricing_snapshot = event
        .data
        .request_metadata
        .as_ref()
        .and_then(|value| value.pointer("/settlement_snapshot/pricing_snapshot"))
        .expect("settlement pricing snapshot should exist");
    assert_eq!(pricing_snapshot["requested_processing_tier"], "priority");
    assert!(pricing_snapshot["actual_processing_tier"].is_null());
    assert_eq!(pricing_snapshot["billing_processing_tier"], "priority");
    assert_eq!(
        pricing_snapshot["tiered_pricing_source"],
        "provider_override"
    );
    assert_eq!(
        pricing_snapshot["processing_tier_price_multiplier"],
        Value::Null
    );
    assert_eq!(
        pricing_snapshot["tiered_pricing"]["tiers"][0]["input_price_per_1m"],
        10.0
    );
    // The response fact remains available for audit, but does not influence settlement.
    assert_eq!(
        event
            .data
            .request_metadata
            .as_ref()
            .and_then(|value| value.get("provider_actual_service_tier")),
        Some(&json!("flex"))
    );
    assert_eq!(
        event
            .data
            .request_metadata
            .as_ref()
            .and_then(|value| { value.pointer("/billing_dimensions/actual_processing_tier") }),
        Some(&Value::Null)
    );
}

#[tokio::test]
async fn requested_processing_catalog_controls_image_price_with_independent_fixed_price() {
    let lookup = TestLookup {
        name_context: Some(
            StoredBillingModelContext::new(
                "provider-1".to_string(),
                Some("pay_as_you_go".to_string()),
                Some("key-1".to_string()),
                None,
                None,
                "global-image-1".to_string(),
                "gpt-image-2".to_string(),
                None,
                Some(0.01),
                Some(json!({
                    "image_output_price_default": 0.1,
                    "processing_tiers": {
                        "flex": {"image_output_price_default": 0.2}
                    }
                })),
                Some("model-image-1".to_string()),
                Some("gpt-image-2".to_string()),
                None,
                Some(0.02),
                Some(json!({
                    "processing_tiers": {
                        "priority": {"image_output_price_default": 0.4}
                    }
                })),
            )
            .expect("billing context should build"),
        ),
        model_id_context: None,
    };
    let mut event = UsageEvent::new(
        UsageEventType::Completed,
        "req-image-processing-tier-1",
        UsageEventData {
            provider_name: "OpenAI Image".to_string(),
            model: "gpt-image-2".to_string(),
            provider_id: Some("provider-1".to_string()),
            provider_api_key_id: Some("key-1".to_string()),
            request_type: Some("image".to_string()),
            api_format: Some("openai:image".to_string()),
            endpoint_api_format: Some("openai:image".to_string()),
            provider_request_body: Some(json!({"service_tier": "priority"})),
            request_metadata: Some(json!({
                "provider_actual_service_tier": "flex",
                "dimensions": {"image_count": 2}
            })),
            status_code: Some(200),
            ..UsageEventData::default()
        },
    );

    enrich_usage_event_with_billing(&lookup, &mut event)
        .await
        .expect("billing should succeed");

    assert_eq!(event.data.total_cost_usd, Some(0.84));
    assert_eq!(event.data.actual_total_cost_usd, Some(0.84));
    let metadata = event.data.request_metadata.as_ref().expect("metadata");
    let pricing = metadata
        .pointer("/settlement_snapshot/pricing_snapshot")
        .expect("pricing snapshot");
    assert_eq!(pricing["billing_processing_tier"], "priority");
    assert_eq!(pricing["tiered_pricing_source"], "provider_override");
    assert_eq!(pricing["price_per_request_source"], "provider_override");
    assert_eq!(pricing["pricing_source"], "provider_override");
    assert_eq!(
        metadata
            .pointer("/billing_snapshot/resolved_variables/image_output_price_per_image")
            .and_then(Value::as_f64),
        Some(0.4)
    );
    assert_eq!(
        metadata
            .pointer("/billing_snapshot/cost_breakdown/image_output_cost")
            .and_then(Value::as_f64),
        Some(0.8)
    );
    assert_eq!(
        metadata
            .pointer("/billing_snapshot/cost_breakdown/request_cost")
            .and_then(Value::as_f64),
        Some(0.04)
    );
}

#[tokio::test]
async fn image_usage_uses_image_count_for_request_cost() {
    let lookup = TestLookup {
        name_context: Some(
            StoredBillingModelContext::new(
                "provider-1".to_string(),
                Some("pay_as_you_go".to_string()),
                Some("key-1".to_string()),
                None,
                None,
                "global-image-1".to_string(),
                "gpt-image-2".to_string(),
                None,
                Some(0.02),
                None,
                Some("model-image-1".to_string()),
                Some("gpt-image-2".to_string()),
                None,
                None,
                None,
            )
            .expect("billing context should build"),
        ),
        model_id_context: None,
    };
    let mut event = UsageEvent::new(
        UsageEventType::Completed,
        "req-image-billing-1",
        UsageEventData {
            provider_name: "OpenAI Image".to_string(),
            model: "gpt-image-2".to_string(),
            provider_id: Some("provider-1".to_string()),
            provider_api_key_id: Some("key-1".to_string()),
            request_type: Some("chat".to_string()),
            api_format: Some("openai:chat".to_string()),
            endpoint_api_format: Some("openai:image".to_string()),
            request_metadata: Some(json!({
                "dimensions": {
                    "image_count": 3
                }
            })),
            status_code: Some(200),
            ..UsageEventData::default()
        },
    );

    enrich_usage_event_with_billing(&lookup, &mut event)
        .await
        .expect("billing should succeed");

    assert_eq!(event.data.total_cost_usd, Some(0.06));
    assert_eq!(event.data.actual_total_cost_usd, Some(0.06));
    assert_eq!(
        event
            .data
            .request_metadata
            .as_ref()
            .and_then(|value| value.get("billing_dimensions"))
            .and_then(|value| value.get("request_count"))
            .and_then(Value::as_i64),
        Some(3)
    );
    assert_eq!(
        event
            .data
            .request_metadata
            .as_ref()
            .and_then(|value| value.get("billing_dimensions"))
            .and_then(|value| value.get("image_count"))
            .and_then(Value::as_i64),
        Some(3)
    );
    assert_eq!(
        event
            .data
            .request_metadata
            .as_ref()
            .and_then(|value| value.get("billing_dimensions"))
            .and_then(|value| value.get("effective_task_type"))
            .and_then(Value::as_str),
        Some("image")
    );
}

#[tokio::test]
async fn image_usage_uses_configured_output_price_matrix() {
    let lookup = TestLookup {
        name_context: Some(
            StoredBillingModelContext::new(
                "provider-1".to_string(),
                Some("pay_as_you_go".to_string()),
                Some("key-1".to_string()),
                None,
                None,
                "global-image-1".to_string(),
                "gpt-image-2".to_string(),
                None,
                None,
                Some(json!({
                    "tiers": [{
                        "up_to": null,
                        "input_price_per_1m": 5.0,
                        "output_price_per_1m": 30.0,
                        "cache_read_price_per_1m": 1.25
                    }],
                    "image_output_price_default": 0.01,
                    "image_output_prices": {
                        "1024x1024": {"low": 0.006, "medium": 0.053, "high": 0.211},
                        "1536x1024": {"low": 0.005, "medium": 0.041, "high": 0.165},
                        "1024x1536": {"low": 0.005, "medium": 0.041, "high": 0.165}
                    }
                })),
                Some("model-image-1".to_string()),
                Some("gpt-image-2".to_string()),
                None,
                None,
                None,
            )
            .expect("billing context should build"),
        ),
        model_id_context: None,
    };
    let mut event = UsageEvent::new(
        UsageEventType::Completed,
        "req-image-billing-matrix-1",
        UsageEventData {
            provider_name: "OpenAI Image".to_string(),
            model: "gpt-image-2".to_string(),
            provider_id: Some("provider-1".to_string()),
            provider_api_key_id: Some("key-1".to_string()),
            request_type: Some("chat".to_string()),
            api_format: Some("openai:chat".to_string()),
            endpoint_api_format: Some("openai:image".to_string()),
            request_metadata: Some(json!({
                "dimensions": {
                    "image_count": 2,
                    "image_size": "1536x1024",
                    "image_quality": "medium",
                    "image_output_format": "png"
                }
            })),
            status_code: Some(200),
            ..UsageEventData::default()
        },
    );

    enrich_usage_event_with_billing(&lookup, &mut event)
        .await
        .expect("billing should succeed");

    assert_eq!(event.data.total_cost_usd, Some(0.082));
    assert_eq!(event.data.actual_total_cost_usd, Some(0.082));
    let metadata = event.data.request_metadata.as_ref().expect("metadata");
    assert_eq!(
        metadata
            .get("billing_dimensions")
            .and_then(|value| value.get("image_price_key"))
            .and_then(Value::as_str),
        Some("1536x1024:medium")
    );
    assert_eq!(
        metadata
            .get("billing_snapshot")
            .and_then(|value| value.get("resolved_variables"))
            .and_then(|value| value.get("image_output_price_per_image"))
            .and_then(Value::as_f64),
        Some(0.041)
    );
    assert_eq!(
        metadata
            .get("billing_snapshot")
            .and_then(|value| value.get("cost_breakdown"))
            .and_then(|value| value.get("image_output_cost"))
            .and_then(Value::as_f64),
        Some(0.082)
    );
}

#[tokio::test]
async fn cancelled_usage_bills_only_configured_request_fee() {
    for (request_type, request_price) in
        [("chat", None), ("chat", Some(0.02)), ("image", Some(0.02))]
    {
        let lookup = TestLookup {
        name_context: Some(
            StoredBillingModelContext::new(
                "provider-1".to_string(),
                Some("pay_as_you_go".to_string()),
                Some("key-1".to_string()),
                Some(json!({"openai:responses": 0.5})),
                Some(60),
                "global-model-1".to_string(),
                "gpt-5".to_string(),
                None,
                request_price,
                Some(json!({"tiers":[{"up_to":null,"input_price_per_1m":3.0,"output_price_per_1m":15.0,"cache_creation_price_per_1m":3.75,"cache_read_price_per_1m":0.30}]})),
                Some("model-1".to_string()),
                Some("gpt-5-upstream".to_string()),
                None,
                None,
                None,
            )
            .expect("billing context should build"),
        ),
        model_id_context: None,
    };
        let mut event = UsageEvent::new(
            UsageEventType::Cancelled,
            "req-billing-cancelled-1",
            UsageEventData {
                provider_name: "OpenAI".to_string(),
                model: "gpt-5".to_string(),
                provider_id: Some("provider-1".to_string()),
                provider_api_key_id: Some("key-1".to_string()),
                request_type: Some(request_type.to_string()),
                api_format: Some("openai:responses".to_string()),
                endpoint_api_format: Some("openai:responses".to_string()),
                input_tokens: Some(1_000),
                output_tokens: Some(500),
                cache_read_input_tokens: Some(100),
                status_code: Some(499),
                request_metadata: Some(json!({"cancelled_request_fee": true, "image_count": 3})),
                ..UsageEventData::default()
            },
        );

        enrich_usage_event_with_billing(&lookup, &mut event)
            .await
            .expect("billing should succeed");

        let expected_cost = request_price.unwrap_or(0.0);
        assert_eq!(event.data.total_cost_usd, Some(expected_cost));
        assert_eq!(event.data.actual_total_cost_usd, Some(expected_cost * 0.5));
        assert_eq!(event.data.input_tokens, Some(1_000));
        assert_eq!(event.data.output_tokens, Some(500));
        let metadata = event.data.request_metadata.as_ref().unwrap();
        assert_eq!(
            aether_data_contracts::repository::usage::cancelled_request_fee_is_billable(Some(
                metadata
            )),
            request_price.is_some()
        );
        if request_price.is_some() {
            assert_eq!(
                metadata.pointer("/billing_snapshot/cost_breakdown/request_cost"),
                Some(&json!(expected_cost))
            );
            assert_eq!(
                metadata.pointer("/billing_dimensions/input_tokens"),
                Some(&json!(0))
            );
            assert_eq!(
                metadata.pointer("/billing_dimensions/output_tokens"),
                Some(&json!(0))
            );
            assert_eq!(
                metadata.pointer("/billing_dimensions/cache_read_tokens"),
                Some(&json!(0))
            );
            assert_eq!(
                metadata.pointer("/billing_dimensions/request_count"),
                Some(&json!(1))
            );
        } else {
            assert!(metadata.get("billing_snapshot").is_none());
        }
    }
}

#[tokio::test]
async fn failed_usage_event_remains_unbilled() {
    let lookup = TestLookup {
        name_context: None,
        model_id_context: None,
    };
    let mut event = UsageEvent::new(
        UsageEventType::Failed,
        "req-billing-failed-1",
        UsageEventData {
            provider_name: "OpenAI".to_string(),
            model: "gpt-5".to_string(),
            provider_id: Some("provider-1".to_string()),
            provider_api_key_id: Some("key-1".to_string()),
            request_type: Some("chat".to_string()),
            input_tokens: Some(1_000),
            output_tokens: Some(500),
            status_code: Some(500),
            ..UsageEventData::default()
        },
    );

    enrich_usage_event_with_billing(&lookup, &mut event)
        .await
        .expect("billing should succeed");

    assert_eq!(event.data.total_cost_usd, Some(0.0));
    assert_eq!(event.data.actual_total_cost_usd, Some(0.0));
    assert!(event.data.request_metadata.is_none());
}

#[tokio::test]
async fn enriches_by_provider_model_id_before_name_fallback() {
    let blank_name_context = StoredBillingModelContext::new(
        "provider-1".to_string(),
        Some("pay_as_you_go".to_string()),
        Some("key-1".to_string()),
        None,
        Some(60),
        "global-model-blank".to_string(),
        "claude-sonnet-4-6".to_string(),
        None,
        None,
        None,
        Some("model-blank".to_string()),
        Some("claude-sonnet-4-6".to_string()),
        None,
        None,
        None,
    )
    .expect("blank billing context should build");
    let priced_model_context = StoredBillingModelContext::new(
        "provider-1".to_string(),
        Some("pay_as_you_go".to_string()),
        Some("key-1".to_string()),
        None,
        Some(60),
        "global-model-priced".to_string(),
        "claude-sonnet-4-6".to_string(),
        None,
        None,
        None,
        Some("model-priced".to_string()),
        Some("claude-sonnet-4-6".to_string()),
        None,
        None,
        Some(json!({"tiers":[{"up_to":null,"input_price_per_1m":3.0,"output_price_per_1m":15.0}]})),
    )
    .expect("priced billing context should build");
    let lookup = TestLookup {
        name_context: Some(blank_name_context),
        model_id_context: Some(priced_model_context),
    };
    let mut event = UsageEvent::new(
        UsageEventType::Completed,
        "req-billing-model-id-1",
        UsageEventData {
            provider_name: "NekoCode".to_string(),
            model: "claude-sonnet-4-6".to_string(),
            model_id: Some("model-priced".to_string()),
            provider_id: Some("provider-1".to_string()),
            provider_api_key_id: Some("key-1".to_string()),
            request_type: Some("chat".to_string()),
            input_tokens: Some(1_000),
            output_tokens: Some(500),
            status_code: Some(200),
            ..UsageEventData::default()
        },
    );

    enrich_usage_event_with_billing(&lookup, &mut event)
        .await
        .expect("billing should succeed");

    assert!(event.data.total_cost_usd.unwrap_or_default() > 0.0);
    assert_eq!(
        event
            .data
            .request_metadata
            .as_ref()
            .and_then(|value| value.get("billing_snapshot"))
            .and_then(|value| value.get("status"))
            .and_then(Value::as_str),
        Some("complete")
    );
}
