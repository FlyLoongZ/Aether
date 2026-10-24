use serde_json::json;

use super::BillingService;
use crate::{
    BillingAuthorizationEstimateInput, BillingModelPricingSnapshot, BillingPricingSource,
    BillingSnapshotStatus, BillingUsageInput,
};

fn pricing() -> BillingModelPricingSnapshot {
    BillingModelPricingSnapshot {
        provider_id: "provider-1".to_string(),
        provider_billing_type: Some("pay_as_you_go".to_string()),
        provider_api_key_id: Some("key-1".to_string()),
        provider_api_key_rate_multipliers: Some(json!({"openai:chat": 0.5})),
        provider_api_key_cache_ttl_minutes: Some(60),
        global_model_id: "global-model-1".to_string(),
        global_model_name: "gpt-5".to_string(),
        global_model_config: None,
        default_price_per_request: Some(0.02),
        default_tiered_pricing: Some(json!({
            "tiers": [{
                "up_to": null,
                "input_price_per_1m": 3.0,
                "output_price_per_1m": 15.0,
                "cache_creation_price_per_1m": 3.75,
                "cache_read_price_per_1m": 0.30
            }]
        })),
        model_id: Some("model-1".to_string()),
        model_provider_model_name: Some("gpt-5-upstream".to_string()),
        model_config: None,
        model_price_per_request: None,
        model_tiered_pricing: None,
    }
}

fn processing_pricing() -> BillingModelPricingSnapshot {
    BillingModelPricingSnapshot {
        provider_api_key_rate_multipliers: None,
        default_price_per_request: None,
        default_tiered_pricing: Some(json!({
            "tiers": [
                {"up_to": 272000, "input_price_per_1m": 5.0, "cache_creation_price_per_1m": 6.25, "cache_read_price_per_1m": 0.5, "output_price_per_1m": 30.0},
                {"up_to": null, "input_price_per_1m": 10.0, "cache_creation_price_per_1m": 12.5, "cache_read_price_per_1m": 1.0, "output_price_per_1m": 45.0}
            ],
            "processing_tiers": {
                "flex": {"tiers": [
                    {"up_to": 272000, "input_price_per_1m": 2.5, "cache_creation_price_per_1m": 3.125, "cache_read_price_per_1m": 0.25, "output_price_per_1m": 15.0},
                    {"up_to": null, "input_price_per_1m": 5.0, "cache_creation_price_per_1m": 6.25, "cache_read_price_per_1m": 0.5, "output_price_per_1m": 22.5}
                ]}
            }
        })),
        model_tiered_pricing: Some(json!({
            "processing_tiers": {
                "priority": {"tiers": [
                    {"up_to": 272000, "input_price_per_1m": 10.0, "cache_creation_price_per_1m": 12.5, "cache_read_price_per_1m": 1.0, "output_price_per_1m": 60.0}
                ]}
            }
        })),
        ..pricing()
    }
}

fn processing_usage(
    requested: Option<&str>,
    actual: Option<&str>,
    input_tokens: i64,
) -> BillingUsageInput {
    BillingUsageInput {
        api_format: Some("openai:responses".to_string()),
        requested_processing_tier: requested.map(ToOwned::to_owned),
        actual_processing_tier: actual.map(ToOwned::to_owned),
        input_tokens,
        cache_creation_tokens: 10,
        cache_ttl_minutes: Some(30),
        ..BillingUsageInput::new("chat")
    }
}

#[test]
fn calculates_complete_snapshot_for_usage() {
    let result = BillingService::new()
        .calculate(
            &pricing(),
            &BillingUsageInput {
                task_type: "chat".to_string(),
                api_format: Some("openai:chat".to_string()),
                requested_processing_tier: None,
                actual_processing_tier: None,
                request_count: 1,
                input_tokens: 1_000,
                output_tokens: 500,
                cache_creation_tokens: 0,
                cache_creation_ephemeral_5m_tokens: 0,
                cache_creation_ephemeral_1h_tokens: 0,
                cache_read_tokens: 100,
                image_count: 0,
                image_size: None,
                image_quality: None,
                image_output_format: None,
                cache_ttl_minutes: Some(60),
            },
        )
        .expect("billing should calculate");

    assert_eq!(result.cost_result.status, BillingSnapshotStatus::Complete);
    assert!(result.cost_result.cost > 0.0);
    assert!(result.actual_total_cost > 0.0);
    assert_eq!(result.rate_multiplier, 0.5);
}

#[test]
fn openai_cache_hit_context_does_not_double_count_cache_read() {
    let result = BillingService::new()
        .calculate(
            &pricing(),
            &BillingUsageInput {
                task_type: "chat".to_string(),
                api_format: Some("openai:responses".to_string()),
                requested_processing_tier: None,
                actual_processing_tier: None,
                request_count: 1,
                input_tokens: 1_000,
                output_tokens: 10,
                cache_creation_tokens: 0,
                cache_creation_ephemeral_5m_tokens: 0,
                cache_creation_ephemeral_1h_tokens: 0,
                cache_read_tokens: 800,
                image_count: 0,
                image_size: None,
                image_quality: None,
                image_output_format: None,
                cache_ttl_minutes: Some(60),
            },
        )
        .expect("billing should calculate");

    assert_eq!(
        result
            .cost_result
            .snapshot
            .resolved_dimensions
            .get("input_tokens"),
        Some(&json!(200))
    );
    assert_eq!(
        result
            .cost_result
            .snapshot
            .resolved_dimensions
            .get("total_input_context"),
        Some(&json!(1_000))
    );
}

#[test]
fn openai_cache_write_and_read_are_billed_separately() {
    let result = BillingService::new()
        .calculate(
            &pricing(),
            &BillingUsageInput {
                task_type: "chat".to_string(),
                api_format: Some("openai:responses".to_string()),
                requested_processing_tier: None,
                actual_processing_tier: None,
                request_count: 1,
                input_tokens: 1_000,
                output_tokens: 10,
                cache_creation_tokens: 100,
                cache_creation_ephemeral_5m_tokens: 0,
                cache_creation_ephemeral_1h_tokens: 0,
                cache_read_tokens: 800,
                image_count: 0,
                image_size: None,
                image_quality: None,
                image_output_format: None,
                cache_ttl_minutes: Some(60),
            },
        )
        .expect("billing should calculate");

    let dimensions = &result.cost_result.snapshot.resolved_dimensions;
    assert_eq!(dimensions.get("input_tokens"), Some(&json!(100)));
    assert_eq!(dimensions.get("cache_creation_tokens"), Some(&json!(100)));
    assert_eq!(dimensions.get("cache_read_tokens"), Some(&json!(800)));
    assert_eq!(dimensions.get("total_input_context"), Some(&json!(1_000)));

    let costs = &result.cost_result.snapshot.cost_breakdown;
    assert!(costs.get("input_cost").copied().unwrap_or_default() > 0.0);
    assert!(
        costs
            .get("cache_creation_uncategorized_cost")
            .copied()
            .unwrap_or_default()
            > 0.0
    );
    assert!(costs.get("cache_read_cost").copied().unwrap_or_default() > 0.0);
}

#[test]
fn nonstandard_request_without_actual_tier_uses_requested_catalog() {
    let result = BillingService::new()
        .calculate(
            &processing_pricing(),
            &processing_usage(Some("priority"), None, 100),
        )
        .expect("billing should calculate");

    assert_eq!(result.cost_result.status, BillingSnapshotStatus::Complete);
    assert_eq!(
        result.cost_result.snapshot.resolved_dimensions["billing_processing_tier"],
        json!("priority")
    );
    assert_eq!(
        result.cost_result.snapshot.resolved_variables["input_price_per_1m"],
        json!(10.0)
    );
}

#[test]
fn openai_fast_request_without_overlay_uses_global_standard_catalog() {
    let pricing = BillingModelPricingSnapshot {
        default_price_per_request: None,
        default_tiered_pricing: Some(json!({
            "tiers": [{
                "up_to": null,
                "input_price_per_1m": 3.0,
                "output_price_per_1m": 15.0
            }]
        })),
        model_tiered_pricing: None,
        ..pricing()
    };

    let result = BillingService::new()
        .calculate(
            &pricing,
            &BillingUsageInput {
                api_format: Some("openai:responses".to_string()),
                requested_processing_tier: Some("priority".to_string()),
                input_tokens: 1_000_000,
                ..BillingUsageInput::new("chat")
            },
        )
        .expect("global Standard pricing should calculate OpenAI Fast usage");

    assert_eq!(result.cost_result.status, BillingSnapshotStatus::Complete);
    assert_eq!(result.cost_result.cost, 3.0);
    assert_eq!(
        result.pricing_resolution.tiered_pricing_source,
        Some(crate::BillingPricingSource::GlobalDefault)
    );
    assert_eq!(
        result.cost_result.snapshot.resolved_variables["input_price_per_1m"],
        json!(3.0)
    );
}

#[test]
fn nonstandard_request_with_only_fixed_price_is_still_billable() {
    let pricing = BillingModelPricingSnapshot {
        default_price_per_request: Some(0.02),
        default_tiered_pricing: None,
        model_tiered_pricing: None,
        ..pricing()
    };

    let result = BillingService::new()
        .calculate(&pricing, &processing_usage(Some("fast"), None, 1_000))
        .expect("fixed request pricing should calculate Fast usage");

    assert_eq!(result.cost_result.status, BillingSnapshotStatus::Complete);
    assert_eq!(result.cost_result.cost, 0.02);
    assert_eq!(result.pricing_resolution.tiered_pricing, None);
    assert_eq!(result.pricing_resolution.price_per_request, Some(0.02));
}

#[test]
fn response_actual_tier_does_not_override_requested_catalog() {
    let cases = ["default", "flex", "priority"];

    for actual in cases {
        let result = BillingService::new()
            .calculate(
                &processing_pricing(),
                &processing_usage(Some("priority"), Some(actual), 100),
            )
            .expect("processing tier should resolve");

        assert_eq!(result.cost_result.status, BillingSnapshotStatus::Complete);
        assert_eq!(
            result.pricing_resolution.actual_processing_tier.as_deref(),
            Some(actual)
        );
        assert_eq!(
            result.pricing_resolution.billing_processing_tier.as_deref(),
            Some("priority")
        );
        assert_eq!(
            result.cost_result.snapshot.resolved_variables["input_price_per_1m"],
            json!(10.0)
        );
        assert_eq!(
            result.cost_result.snapshot.resolved_variables["cache_creation_price_per_1m"],
            json!(12.5)
        );
        assert_eq!(
            result.pricing_resolution.tiered_pricing_source,
            Some(BillingPricingSource::ProviderOverride)
        );
    }
}

#[test]
fn processing_multiplier_is_settled_and_authorized_from_standard_prices() {
    let pricing = BillingModelPricingSnapshot {
        provider_api_key_rate_multipliers: None,
        default_price_per_request: Some(0.02),
        default_tiered_pricing: Some(json!({
            "tiers": [{
                "up_to": null,
                "input_price_per_1m": 2.0,
                "output_price_per_1m": 4.0,
                "cache_creation_price_per_1m": 0.0,
                "cache_read_price_per_1m": 0.0
            }],
            "processing_tiers": {
                "priority": {"price_multiplier": 2.5}
            }
        })),
        model_price_per_request: None,
        model_tiered_pricing: None,
        ..pricing()
    };
    let usage = BillingUsageInput {
        api_format: Some("openai:responses".to_string()),
        requested_processing_tier: Some("priority".to_string()),
        actual_processing_tier: Some("priority".to_string()),
        input_tokens: 1_000_000,
        ..BillingUsageInput::new("chat")
    };

    let settled = BillingService::new()
        .calculate(&pricing, &usage)
        .expect("multiplier pricing should settle");
    assert_eq!(settled.cost_result.status, BillingSnapshotStatus::Complete);
    assert_eq!(settled.cost_result.cost, 5.02);
    assert_eq!(settled.actual_total_cost, 5.02);
    assert_eq!(
        settled.cost_result.snapshot.resolved_variables["input_price_per_1m"],
        json!(5.0)
    );
    assert_eq!(
        settled.cost_result.snapshot.resolved_variables["price_per_request"],
        json!(0.02),
        "processing multiplier must not affect price_per_request"
    );

    let mut estimate = BillingAuthorizationEstimateInput::new("chat", 1_000_000);
    estimate.api_format = Some("openai:responses".to_string());
    estimate.requested_processing_tier = Some("priority".to_string());
    estimate.max_output_tokens = Some(0);
    assert_eq!(
        BillingService::new()
            .estimate_authorization_cost_upper_bound(&pricing, &estimate)
            .expect("multiplier pricing should authorize"),
        Some(5.02)
    );
}

#[test]
fn invalid_processing_multiplier_fails_closed_at_settlement_and_authorization() {
    let pricing = BillingModelPricingSnapshot {
        provider_api_key_rate_multipliers: None,
        default_price_per_request: None,
        default_tiered_pricing: Some(json!({
            "tiers": [{"up_to": null, "input_price_per_1m": 2.0}],
            "processing_tiers": {
                "priority": {"price_multiplier": -1.0}
            }
        })),
        model_tiered_pricing: None,
        ..pricing()
    };
    let usage = processing_usage(Some("priority"), Some("priority"), 1_000);

    let settlement_error = BillingService::new()
        .calculate(&pricing, &usage)
        .expect_err("invalid multiplier settlement must return a configuration error");
    assert!(settlement_error
        .to_string()
        .contains("price_multiplier must be a non-negative finite number"));

    let mut estimate = BillingAuthorizationEstimateInput::new("chat", 1_000);
    estimate.api_format = Some("openai:responses".to_string());
    estimate.requested_processing_tier = Some("priority".to_string());
    estimate.max_output_tokens = Some(0);
    let authorization_error = BillingService::new()
        .estimate_authorization_cost_upper_bound(&pricing, &estimate)
        .expect_err("invalid multiplier authorization must return a configuration error");
    assert!(authorization_error
        .to_string()
        .contains("price_multiplier must be a non-negative finite number"));
}

#[test]
fn empty_processing_catalog_cannot_turn_an_invalid_multiplier_into_zero_cost() {
    let pricing = BillingModelPricingSnapshot {
        default_price_per_request: None,
        default_tiered_pricing: Some(json!({
            "tiers": [{"up_to": null, "input_price_per_1m": 2.0}],
            "processing_tiers": {
                "priority": {
                    "tiers": [{}],
                    "price_multiplier": 2.0
                }
            }
        })),
        model_tiered_pricing: None,
        ..pricing()
    };
    let usage = processing_usage(Some("priority"), Some("priority"), 1_000);

    let settlement_error = BillingService::new()
        .calculate(&pricing, &usage)
        .expect_err("malformed processing pricing must not settle as zero");
    assert!(settlement_error
        .to_string()
        .contains("explicit catalog contains malformed or unrecognized prices"));

    let mut estimate = BillingAuthorizationEstimateInput::new("chat", 1_000);
    estimate.requested_processing_tier = Some("priority".to_string());
    estimate.max_output_tokens = Some(0);
    let authorization_error = BillingService::new()
        .estimate_authorization_cost_upper_bound(&pricing, &estimate)
        .expect_err("malformed processing pricing must stop authorization");
    assert!(authorization_error
        .to_string()
        .contains("explicit catalog contains malformed or unrecognized prices"));
}

#[test]
fn authorization_accepts_input_only_processing_catalog() {
    let pricing = BillingModelPricingSnapshot {
        default_price_per_request: None,
        default_tiered_pricing: Some(json!({
            "tiers": [{"up_to": null, "input_price_per_1m": 0.1}],
            "processing_tiers": {
                "embedding": {
                    "tiers": [{
                        "up_to": null,
                        "input_price_per_1m": 0.2,
                        "output_price_per_1m": null
                    }]
                }
            }
        })),
        model_tiered_pricing: None,
        ..pricing()
    };
    let mut estimate = BillingAuthorizationEstimateInput::new("embedding", 1_000_000);
    estimate.requested_processing_tier = Some("embedding".to_string());

    assert_eq!(
        BillingService::new()
            .estimate_authorization_cost_upper_bound(&pricing, &estimate)
            .expect("input-only processing catalog should be valid"),
        Some(0.45),
        "the bound includes the runtime's conservative cache-write fallback"
    );
}

#[test]
fn invalid_image_catalog_is_reported_before_the_unavailable_image_estimate() {
    let pricing = BillingModelPricingSnapshot {
        default_price_per_request: None,
        default_tiered_pricing: Some(json!({
            "image_output_price_ranges": {
                "1048576": {"low": 0.04}
            }
        })),
        model_tiered_pricing: None,
        ..pricing()
    };
    let estimate = BillingAuthorizationEstimateInput::new("image", 0);

    let err = BillingService::new()
        .estimate_authorization_cost_upper_bound(&pricing, &estimate)
        .expect_err("an unparseable image range must be a configuration error");
    assert!(err
        .to_string()
        .contains("Standard catalog contains malformed or unrecognized prices"));
}

#[test]
fn historical_noncanonical_processing_tier_key_is_a_configuration_error() {
    let pricing = BillingModelPricingSnapshot {
        default_tiered_pricing: Some(json!({
            "tiers": [{"up_to": null, "input_price_per_1m": 1.0}],
            "processing_tiers": {
                "Priority": {"price_multiplier": 2.0}
            }
        })),
        model_tiered_pricing: None,
        ..pricing()
    };
    let mut estimate = BillingAuthorizationEstimateInput::new("chat", 1_000);
    estimate.requested_processing_tier = Some("priority".to_string());
    estimate.max_output_tokens = Some(0);

    let err = BillingService::new()
        .estimate_authorization_cost_upper_bound(&pricing, &estimate)
        .expect_err("a noncanonical historical key must not disappear during lookup");
    assert!(err
        .to_string()
        .contains("must be canonical lowercase without surrounding whitespace"));
}

#[test]
fn finite_processing_catalog_fails_only_on_requested_catalog_bounds() {
    let priority = BillingService::new()
        .calculate(
            &processing_pricing(),
            &processing_usage(Some("priority"), Some("priority"), 300_000),
        )
        .expect("billing should calculate");
    assert_eq!(priority.cost_result.status, BillingSnapshotStatus::NoRule);
    assert_eq!(
        priority.cost_result.snapshot.missing_required,
        vec!["input_context_tier"]
    );

    let conflicting_actual = BillingService::new()
        .calculate(
            &processing_pricing(),
            &processing_usage(Some("priority"), Some("expedited"), 100),
        )
        .expect("billing should calculate");
    assert_eq!(
        conflicting_actual.cost_result.status,
        BillingSnapshotStatus::Complete
    );
    assert_eq!(
        conflicting_actual
            .pricing_resolution
            .billing_processing_tier
            .as_deref(),
        Some("priority")
    );
}

#[test]
fn fast_multiplier_uses_one_fixed_first_band_above_context_threshold() {
    let pricing = BillingModelPricingSnapshot {
        default_tiered_pricing: Some(json!({
            "tiers": [
                {
                    "up_to": 271999,
                    "input_price_per_1m": 5.0,
                    "output_price_per_1m": 30.0
                },
                {
                    "up_to": null,
                    "input_price_per_1m": 10.0,
                    "output_price_per_1m": 45.0
                }
            ],
            "processing_tiers": {
                "priority": {"price_multiplier": 2.0}
            }
        })),
        model_tiered_pricing: None,
        ..pricing()
    };

    for input_tokens in [271_999, 272_000, 300_000] {
        let result = BillingService::new()
            .calculate(
                &pricing,
                &processing_usage(Some("priority"), Some("priority"), input_tokens),
            )
            .expect("Fast pricing should settle at every context size");
        assert_eq!(
            result.cost_result.status,
            BillingSnapshotStatus::Complete,
            "context: {input_tokens}"
        );
        assert_eq!(
            result.cost_result.snapshot.resolved_variables["input_price_per_1m"],
            json!(10.0),
            "Fast must use the first Standard band at context {input_tokens}"
        );
        assert_eq!(
            result.cost_result.snapshot.resolved_variables["output_price_per_1m"],
            json!(60.0),
            "Fast output must remain fixed at context {input_tokens}"
        );
    }
}

#[test]
fn processing_catalog_boundaries_match_context_and_priority_contracts() {
    let cases = [
        (
            "default",
            272_000,
            BillingSnapshotStatus::Complete,
            Some(5.0),
        ),
        (
            "default",
            272_001,
            BillingSnapshotStatus::Complete,
            Some(10.0),
        ),
        ("flex", 272_000, BillingSnapshotStatus::Complete, Some(2.5)),
        ("flex", 272_001, BillingSnapshotStatus::Complete, Some(5.0)),
        (
            "priority",
            272_000,
            BillingSnapshotStatus::Complete,
            Some(10.0),
        ),
        ("priority", 272_001, BillingSnapshotStatus::NoRule, None),
    ];

    for (actual, input_tokens, status, input_price) in cases {
        let result = BillingService::new()
            .calculate(
                &processing_pricing(),
                &processing_usage(Some(actual), Some(actual), input_tokens),
            )
            .expect("processing boundary should resolve");
        assert_eq!(
            result.cost_result.status, status,
            "{actual} at {input_tokens}"
        );
        if let Some(input_price) = input_price {
            assert_eq!(
                result.cost_result.snapshot.resolved_variables["input_price_per_1m"],
                json!(input_price),
                "{actual} at {input_tokens}"
            );
        } else {
            assert_eq!(
                result.cost_result.snapshot.missing_required,
                vec!["input_context_tier"]
            );
        }
    }
}

#[test]
fn authorization_estimate_uses_known_request_cache_ttl() {
    let pricing = BillingModelPricingSnapshot {
        provider_api_key_rate_multipliers: None,
        default_price_per_request: None,
        default_tiered_pricing: Some(json!({
            "tiers": [{
                "up_to": null,
                "input_price_per_1m": 1.0,
                "output_price_per_1m": 0.0,
                "cache_creation_price_per_1m": 1.25,
                "cache_read_price_per_1m": 0.1,
                "cache_ttl_pricing": [{
                    "ttl_minutes": 60,
                    "cache_creation_price_per_1m": 100.0,
                    "cache_read_price_per_1m": 100.0
                }]
            }]
        })),
        ..pricing()
    };
    let service = BillingService::new();
    let mut estimate = BillingAuthorizationEstimateInput::new("chat", 1_000_000);
    estimate.api_format = Some("openai:responses".to_string());
    estimate.max_output_tokens = Some(0);
    estimate.cache_ttl_minutes = Some(30);

    assert_eq!(
        service
            .estimate_authorization_cost_upper_bound(&pricing, &estimate)
            .expect("known TTL estimate should calculate"),
        Some(1.25)
    );

    estimate.cache_ttl_minutes = None;
    assert_eq!(
        service
            .estimate_authorization_cost_upper_bound(&pricing, &estimate)
            .expect("unknown TTL estimate should calculate"),
        Some(100.0)
    );
}

#[test]
fn authorization_estimate_uses_only_processing_catalogs_eligible_for_context() {
    let service = BillingService::new();
    let mut estimate = BillingAuthorizationEstimateInput::new("chat", 300_000);
    estimate.api_format = Some("openai:responses".to_string());
    estimate.max_output_tokens = Some(0);
    estimate.cache_ttl_minutes = Some(30);

    for (requested_processing_tier, expected) in [
        (None, 3.75),
        (Some("standard"), 3.75),
        (Some("flex"), 1.875),
    ] {
        estimate.requested_processing_tier = requested_processing_tier.map(ToOwned::to_owned);
        assert_eq!(
            service
                .estimate_authorization_cost_upper_bound(&processing_pricing(), &estimate)
                .expect("eligible processing catalogs should calculate"),
            Some(expected),
            "requested tier: {requested_processing_tier:?}"
        );
    }

    estimate.requested_processing_tier = Some("priority".to_string());
    assert_eq!(
        service
            .estimate_authorization_cost_upper_bound(&processing_pricing(), &estimate)
            .expect("ineligible requested catalog should resolve"),
        None
    );
}

#[test]
fn unknown_actual_tier_does_not_override_requested_tier_or_fixed_price() {
    let pricing = BillingModelPricingSnapshot {
        default_price_per_request: Some(0.02),
        ..processing_pricing()
    };
    let result = BillingService::new()
        .calculate(
            &pricing,
            &processing_usage(Some("priority"), Some("expedited"), 100),
        )
        .expect("billing should calculate");

    assert_eq!(result.cost_result.status, BillingSnapshotStatus::Complete);
    assert_eq!(
        result.pricing_resolution.billing_processing_tier.as_deref(),
        Some("priority")
    );
    assert_eq!(
        result.pricing_resolution.actual_processing_tier.as_deref(),
        Some("expedited")
    );
    assert_eq!(result.pricing_resolution.price_per_request, Some(0.02));
}

#[test]
fn authorization_estimate_bounds_only_the_requested_catalog() {
    let service = BillingService::new();
    let mut estimate = BillingAuthorizationEstimateInput::new("chat", 100_000);
    estimate.api_format = Some("openai:responses".to_string());
    estimate.max_output_tokens = Some(1_000_000);

    estimate.requested_processing_tier = Some("priority".to_string());
    let priority = service
        .estimate_authorization_cost_upper_bound(&processing_pricing(), &estimate)
        .expect("priority estimate should calculate")
        .expect("priority estimate should be bounded");

    estimate.requested_processing_tier = Some("flex".to_string());
    let flex = service
        .estimate_authorization_cost_upper_bound(&processing_pricing(), &estimate)
        .expect("flex estimate should calculate")
        .expect("flex estimate should be bounded");

    assert_eq!(priority, 61.25);
    assert_eq!(flex, 15.3125);
    assert!(priority > flex);
}

#[test]
fn authorization_estimate_returns_none_when_the_bound_cannot_be_proven() {
    let service = BillingService::new();
    let mut estimate = BillingAuthorizationEstimateInput::new("chat", 100);
    estimate.api_format = Some("openai:responses".to_string());
    estimate.requested_processing_tier = Some("priority".to_string());

    assert_eq!(
        service
            .estimate_authorization_cost_upper_bound(&processing_pricing(), &estimate)
            .expect("unbounded output estimate should resolve"),
        None
    );

    estimate.max_output_tokens = Some(10);
    estimate.requested_processing_tier = Some("expedited".to_string());
    assert_eq!(
        service
            .estimate_authorization_cost_upper_bound(&processing_pricing(), &estimate)
            .expect("unknown tier should inherit the effective Standard catalog"),
        Some(0.000925)
    );

    estimate.requested_processing_tier = Some("priority".to_string());
    estimate.input_tokens = 300_000;
    assert_eq!(
        service
            .estimate_authorization_cost_upper_bound(&processing_pricing(), &estimate)
            .expect("finite catalog estimate should resolve"),
        None
    );
}

#[test]
fn authorization_estimate_supports_standard_fixed_price_and_free_tier() {
    let service = BillingService::new();
    let estimate = BillingAuthorizationEstimateInput::new("chat", 1_000);
    let fixed_pricing = BillingModelPricingSnapshot {
        default_tiered_pricing: None,
        default_price_per_request: Some(0.02),
        provider_api_key_rate_multipliers: None,
        ..pricing()
    };
    assert_eq!(
        service
            .estimate_authorization_cost_upper_bound(&fixed_pricing, &estimate)
            .expect("fixed estimate should calculate"),
        Some(0.02)
    );

    let free_pricing = BillingModelPricingSnapshot {
        provider_billing_type: Some("free_tier".to_string()),
        ..processing_pricing()
    };
    assert_eq!(
        service
            .estimate_authorization_cost_upper_bound(&free_pricing, &estimate)
            .expect("free estimate should calculate"),
        Some(0.0)
    );
}

#[test]
fn authorization_estimate_checks_every_reachable_non_monotonic_price_tier() {
    let pricing = BillingModelPricingSnapshot {
        default_price_per_request: None,
        default_tiered_pricing: Some(json!({
            "tiers": [
                {
                    "up_to": 10_000,
                    "input_price_per_1m": 100.0,
                    "output_price_per_1m": 100.0,
                    "cache_creation_price_per_1m": 100.0,
                    "cache_read_price_per_1m": 100.0
                },
                {
                    "up_to": null,
                    "input_price_per_1m": 1.0,
                    "output_price_per_1m": 1.0,
                    "cache_creation_price_per_1m": 1.0,
                    "cache_read_price_per_1m": 1.0
                }
            ]
        })),
        model_tiered_pricing: None,
        ..pricing()
    };
    let mut estimate = BillingAuthorizationEstimateInput::new("chat", 100_000);
    estimate.api_format = Some("openai:responses".to_string());
    estimate.max_output_tokens = Some(0);

    assert_eq!(
        BillingService::new()
            .estimate_authorization_cost_upper_bound(&pricing, &estimate)
            .expect("non-monotonic catalog should calculate"),
        Some(10.0)
    );
}

#[test]
fn authorization_estimate_uses_api_key_cache_read_ttl_price() {
    let pricing = BillingModelPricingSnapshot {
        provider_api_key_cache_ttl_minutes: Some(60),
        default_price_per_request: None,
        default_tiered_pricing: Some(json!({
            "tiers": [{
                "up_to": null,
                "input_price_per_1m": 1.0,
                "output_price_per_1m": 1.0,
                "cache_creation_price_per_1m": 1.25,
                "cache_read_price_per_1m": 0.1,
                "cache_ttl_pricing": [{
                    "ttl_minutes": 60,
                    "cache_creation_price_per_1m": 1.25,
                    "cache_read_price_per_1m": 200.0
                }]
            }]
        })),
        model_tiered_pricing: None,
        ..pricing()
    };
    let mut estimate = BillingAuthorizationEstimateInput::new("chat", 100_000);
    estimate.api_format = Some("openai:responses".to_string());
    estimate.max_output_tokens = Some(0);

    assert_eq!(
        BillingService::new()
            .estimate_authorization_cost_upper_bound(&pricing, &estimate)
            .expect("cache read TTL catalog should calculate"),
        Some(20.0)
    );
}

#[test]
fn fixed_request_pricing_remains_independent_for_standard_usage() {
    let pricing = BillingModelPricingSnapshot {
        default_tiered_pricing: None,
        default_price_per_request: Some(0.02),
        ..pricing()
    };
    let result = BillingService::new()
        .calculate(&pricing, &processing_usage(None, None, 1_000))
        .expect("fixed request pricing should calculate");

    assert_eq!(result.cost_result.status, BillingSnapshotStatus::Complete);
    assert_eq!(result.cost_result.cost, 0.02);
    assert_eq!(result.pricing_resolution.tiered_pricing, None);
    assert_eq!(result.pricing_resolution.price_per_request, Some(0.02));
}

#[test]
fn image_token_usage_without_image_output_price_bills_tokens_only() {
    let pricing = BillingModelPricingSnapshot {
        default_price_per_request: None,
        default_tiered_pricing: Some(json!({
            "tiers": [{
                "up_to": null,
                "input_price_per_1m": 1.0,
                "output_price_per_1m": 2.0
            }]
        })),
        ..pricing()
    };

    let result = BillingService::new()
        .calculate(
            &pricing,
            &BillingUsageInput {
                task_type: "image".to_string(),
                api_format: Some("openai:image".to_string()),
                requested_processing_tier: None,
                actual_processing_tier: None,
                request_count: 1,
                input_tokens: 1_000,
                output_tokens: 20_000,
                cache_creation_tokens: 0,
                cache_creation_ephemeral_5m_tokens: 0,
                cache_creation_ephemeral_1h_tokens: 0,
                cache_read_tokens: 0,
                image_count: 1,
                image_size: Some("1024x1024".to_string()),
                image_quality: Some("medium".to_string()),
                image_output_format: Some("png".to_string()),
                cache_ttl_minutes: None,
            },
        )
        .expect("billing should calculate");

    assert_eq!(
        result
            .cost_result
            .snapshot
            .resolved_dimensions
            .get("image_output_pricing_mode"),
        Some(&json!("none"))
    );
    assert_eq!(
        result
            .cost_result
            .snapshot
            .resolved_dimensions
            .get("image_count_unmetered"),
        Some(&json!(0))
    );
    assert_eq!(
        result
            .cost_result
            .snapshot
            .cost_breakdown
            .get("image_output_cost"),
        Some(&0.0)
    );
    assert_eq!(result.cost_result.cost, 0.041);
}

#[test]
fn image_default_output_price_adds_image_cost_even_with_token_usage() {
    let pricing = BillingModelPricingSnapshot {
        default_price_per_request: None,
        default_tiered_pricing: Some(json!({
            "tiers": [{
                "up_to": null,
                "input_price_per_1m": 1.0,
                "output_price_per_1m": 2.0
            }],
            "image_output_price_default": 0.05
        })),
        ..pricing()
    };

    let result = BillingService::new()
        .calculate(
            &pricing,
            &BillingUsageInput {
                task_type: "image".to_string(),
                api_format: Some("openai:image".to_string()),
                requested_processing_tier: None,
                actual_processing_tier: None,
                request_count: 1,
                input_tokens: 1_000,
                output_tokens: 20_000,
                cache_creation_tokens: 0,
                cache_creation_ephemeral_5m_tokens: 0,
                cache_creation_ephemeral_1h_tokens: 0,
                cache_read_tokens: 0,
                image_count: 1,
                image_size: Some("1024x1024".to_string()),
                image_quality: Some("medium".to_string()),
                image_output_format: Some("png".to_string()),
                cache_ttl_minutes: None,
            },
        )
        .expect("billing should calculate");

    assert_eq!(
        result
            .cost_result
            .snapshot
            .resolved_dimensions
            .get("image_output_pricing_mode"),
        Some(&json!("per_image"))
    );
    assert_eq!(
        result
            .cost_result
            .snapshot
            .cost_breakdown
            .get("image_output_cost"),
        Some(&0.05)
    );
    assert_eq!(result.cost_result.cost, 0.091);
}

#[test]
fn image_default_output_price_generates_rule_without_token_tiers() {
    let pricing = BillingModelPricingSnapshot {
        default_price_per_request: None,
        default_tiered_pricing: Some(json!({
            "image_output_price_default": 0.05
        })),
        ..pricing()
    };

    let result = BillingService::new()
        .calculate(
            &pricing,
            &BillingUsageInput {
                task_type: "image".to_string(),
                api_format: Some("openai:image".to_string()),
                requested_processing_tier: None,
                actual_processing_tier: None,
                request_count: 1,
                input_tokens: 0,
                output_tokens: 0,
                cache_creation_tokens: 0,
                cache_creation_ephemeral_5m_tokens: 0,
                cache_creation_ephemeral_1h_tokens: 0,
                cache_read_tokens: 0,
                image_count: 2,
                image_size: Some("1024x1024".to_string()),
                image_quality: Some("medium".to_string()),
                image_output_format: Some("png".to_string()),
                cache_ttl_minutes: None,
            },
        )
        .expect("billing should calculate");

    assert_eq!(result.cost_result.status, BillingSnapshotStatus::Complete);
    assert_eq!(
        result
            .cost_result
            .snapshot
            .cost_breakdown
            .get("image_output_cost"),
        Some(&0.1)
    );
    assert_eq!(result.cost_result.cost, 0.1);
}

#[test]
fn image_pixel_ranges_generate_rule_without_token_tiers() {
    let pricing = BillingModelPricingSnapshot {
        default_price_per_request: None,
        default_tiered_pricing: Some(json!({
            "image_output_price_ranges": [{
                "up_to_pixels": null,
                "prices": { "medium": 0.04 }
            }]
        })),
        ..pricing()
    };

    let result = BillingService::new()
        .calculate(
            &pricing,
            &BillingUsageInput {
                task_type: "image".to_string(),
                api_format: Some("openai:image".to_string()),
                requested_processing_tier: None,
                actual_processing_tier: None,
                request_count: 1,
                input_tokens: 0,
                output_tokens: 0,
                cache_creation_tokens: 0,
                cache_creation_ephemeral_5m_tokens: 0,
                cache_creation_ephemeral_1h_tokens: 0,
                cache_read_tokens: 0,
                image_count: 2,
                image_size: Some("1024x1024".to_string()),
                image_quality: Some("medium".to_string()),
                image_output_format: Some("png".to_string()),
                cache_ttl_minutes: None,
            },
        )
        .expect("billing should calculate");

    assert_eq!(result.cost_result.status, BillingSnapshotStatus::Complete);
    assert_eq!(
        result
            .cost_result
            .snapshot
            .resolved_dimensions
            .get("image_output_pricing_mode"),
        Some(&json!("pixel_tiers"))
    );
    assert_eq!(
        result
            .cost_result
            .snapshot
            .cost_breakdown
            .get("image_output_cost"),
        Some(&0.08)
    );
    assert_eq!(result.cost_result.cost, 0.08);
}

#[test]
fn image_token_usage_with_matrix_adds_matrix_image_cost() {
    let pricing = BillingModelPricingSnapshot {
        default_price_per_request: None,
        default_tiered_pricing: Some(json!({
            "tiers": [{
                "up_to": null,
                "input_price_per_1m": 1.0,
                "output_price_per_1m": 2.0
            }],
            "image_output_price_default": 0.01,
            "image_output_prices": {
                "1024x1024": { "medium": 0.05 }
            }
        })),
        ..pricing()
    };

    let result = BillingService::new()
        .calculate(
            &pricing,
            &BillingUsageInput {
                task_type: "image".to_string(),
                api_format: Some("openai:image".to_string()),
                requested_processing_tier: None,
                actual_processing_tier: None,
                request_count: 1,
                input_tokens: 1_000,
                output_tokens: 20_000,
                cache_creation_tokens: 0,
                cache_creation_ephemeral_5m_tokens: 0,
                cache_creation_ephemeral_1h_tokens: 0,
                cache_read_tokens: 0,
                image_count: 1,
                image_size: Some("1024x1024".to_string()),
                image_quality: Some("medium".to_string()),
                image_output_format: Some("png".to_string()),
                cache_ttl_minutes: None,
            },
        )
        .expect("billing should calculate");

    assert_eq!(
        result
            .cost_result
            .snapshot
            .resolved_dimensions
            .get("image_output_pricing_mode"),
        Some(&json!("matrix"))
    );
    assert_eq!(
        result
            .cost_result
            .snapshot
            .cost_breakdown
            .get("image_output_cost"),
        Some(&0.05)
    );
}

#[test]
fn image_token_usage_with_pixel_ranges_adds_range_image_cost() {
    let pricing = BillingModelPricingSnapshot {
        default_price_per_request: None,
        default_tiered_pricing: Some(json!({
            "tiers": [{
                "up_to": null,
                "input_price_per_1m": 1.0,
                "output_price_per_1m": 2.0
            }],
            "image_output_price_default": 0.01,
            "image_output_price_ranges": [
                {
                    "up_to_pixels": 1_048_576,
                    "prices": { "medium": 0.04 }
                },
                {
                    "up_to_pixels": 2_097_152,
                    "prices": { "medium": 0.08 }
                }
            ]
        })),
        ..pricing()
    };

    let result = BillingService::new()
        .calculate(
            &pricing,
            &BillingUsageInput {
                task_type: "image".to_string(),
                api_format: Some("openai:image".to_string()),
                requested_processing_tier: None,
                actual_processing_tier: None,
                request_count: 1,
                input_tokens: 1_000,
                output_tokens: 20_000,
                cache_creation_tokens: 0,
                cache_creation_ephemeral_5m_tokens: 0,
                cache_creation_ephemeral_1h_tokens: 0,
                cache_read_tokens: 0,
                image_count: 1,
                image_size: Some("1536 x 1024".to_string()),
                image_quality: Some("medium".to_string()),
                image_output_format: Some("png".to_string()),
                cache_ttl_minutes: None,
            },
        )
        .expect("billing should calculate");

    assert_eq!(
        result
            .cost_result
            .snapshot
            .resolved_dimensions
            .get("image_output_pricing_mode"),
        Some(&json!("pixel_tiers"))
    );
    assert_eq!(
        result
            .cost_result
            .snapshot
            .resolved_dimensions
            .get("image_pixels"),
        Some(&json!(1_572_864))
    );
    assert_eq!(
        result
            .cost_result
            .snapshot
            .resolved_dimensions
            .get("image_output_price_bucket"),
        Some(&json!("<=2097152px"))
    );
    assert_eq!(
        result
            .cost_result
            .snapshot
            .resolved_variables
            .get("image_output_price_per_image"),
        Some(&json!(0.08))
    );
    assert_eq!(
        result
            .cost_result
            .snapshot
            .cost_breakdown
            .get("image_output_cost"),
        Some(&0.08)
    );
    assert_eq!(result.cost_result.cost, 0.121);
}

#[test]
fn five_minute_cache_ttl_uses_base_cache_prices() {
    let pricing = BillingModelPricingSnapshot {
        provider_id: "provider-1".to_string(),
        provider_billing_type: Some("pay_as_you_go".to_string()),
        provider_api_key_id: Some("key-1".to_string()),
        provider_api_key_rate_multipliers: None,
        provider_api_key_cache_ttl_minutes: Some(5),
        global_model_id: "global-model-1".to_string(),
        global_model_name: "gpt-5.4".to_string(),
        global_model_config: None,
        default_price_per_request: None,
        default_tiered_pricing: Some(json!({
            "tiers": [{
                "up_to": null,
                "input_price_per_1m": 2.5,
                "output_price_per_1m": 15.0,
                "cache_creation_price_per_1m": 3.125,
                "cache_read_price_per_1m": 0.25,
                "cache_ttl_pricing": [{
                    "ttl_minutes": 60,
                    "cache_creation_price_per_1m": 5.0,
                    "cache_read_price_per_1m": null
                }]
            }]
        })),
        model_id: None,
        model_provider_model_name: None,
        model_config: None,
        model_price_per_request: None,
        model_tiered_pricing: None,
    };

    let result = BillingService::new()
        .calculate(
            &pricing,
            &BillingUsageInput {
                task_type: "chat".to_string(),
                api_format: None,
                requested_processing_tier: None,
                actual_processing_tier: None,
                request_count: 1,
                input_tokens: 1_000,
                output_tokens: 10,
                cache_creation_tokens: 0,
                cache_creation_ephemeral_5m_tokens: 0,
                cache_creation_ephemeral_1h_tokens: 0,
                cache_read_tokens: 100,
                image_count: 0,
                image_size: None,
                image_quality: None,
                image_output_format: None,
                cache_ttl_minutes: Some(5),
            },
        )
        .expect("billing should calculate");

    assert_eq!(
        result
            .cost_result
            .snapshot
            .resolved_variables
            .get("cache_creation_price_per_1m"),
        Some(&json!(3.125))
    );
    assert_eq!(
        result
            .cost_result
            .snapshot
            .resolved_variables
            .get("cache_read_price_per_1m"),
        Some(&json!(0.25))
    );
}

#[test]
fn one_hour_cache_ttl_keeps_base_cache_read_when_ttl_entry_omits_it() {
    let pricing = BillingModelPricingSnapshot {
        provider_id: "provider-1".to_string(),
        provider_billing_type: Some("pay_as_you_go".to_string()),
        provider_api_key_id: Some("key-1".to_string()),
        provider_api_key_rate_multipliers: None,
        provider_api_key_cache_ttl_minutes: Some(60),
        global_model_id: "global-model-1".to_string(),
        global_model_name: "gpt-5.4".to_string(),
        global_model_config: None,
        default_price_per_request: None,
        default_tiered_pricing: Some(json!({
            "tiers": [{
                "up_to": null,
                "input_price_per_1m": 2.5,
                "output_price_per_1m": 15.0,
                "cache_creation_price_per_1m": 3.125,
                "cache_read_price_per_1m": 0.25,
                "cache_ttl_pricing": [{
                    "ttl_minutes": 60,
                    "cache_creation_price_per_1m": 5.0,
                    "cache_read_price_per_1m": null
                }]
            }]
        })),
        model_id: None,
        model_provider_model_name: None,
        model_config: None,
        model_price_per_request: None,
        model_tiered_pricing: None,
    };

    let result = BillingService::new()
        .calculate(
            &pricing,
            &BillingUsageInput {
                task_type: "chat".to_string(),
                api_format: None,
                requested_processing_tier: None,
                actual_processing_tier: None,
                request_count: 1,
                input_tokens: 1_000,
                output_tokens: 10,
                cache_creation_tokens: 0,
                cache_creation_ephemeral_5m_tokens: 0,
                cache_creation_ephemeral_1h_tokens: 0,
                cache_read_tokens: 100,
                image_count: 0,
                image_size: None,
                image_quality: None,
                image_output_format: None,
                cache_ttl_minutes: Some(60),
            },
        )
        .expect("billing should calculate");

    assert_eq!(
        result
            .cost_result
            .snapshot
            .resolved_variables
            .get("cache_creation_price_per_1m"),
        Some(&json!(5.0))
    );
    assert_eq!(
        result
            .cost_result
            .snapshot
            .resolved_variables
            .get("cache_read_price_per_1m"),
        Some(&json!(0.25))
    );
}
