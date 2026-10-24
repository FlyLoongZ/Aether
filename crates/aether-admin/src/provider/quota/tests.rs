use super::{
    codex_build_invalid_state, codex_oauth_success_request_order_is_stale,
    codex_rate_limit_metadata_exhausted, codex_runtime_invalid_reason,
    codex_websocket_response_has_usage_limit_error, codex_websocket_usage_limit_reset_at,
    extract_execution_error_detail, merge_codex_quota_metadata_snapshot,
    normalize_codex_reset_credit_consume_outcome, parse_antigravity_quota_summary_response,
    parse_antigravity_usage_response, parse_chatgpt_web_conversation_init_response,
    parse_codex_backend_me_response, parse_codex_usage_headers,
    parse_codex_websocket_rate_limits_response, parse_codex_wham_reset_credits_detail_response,
    parse_codex_wham_usage_response, parse_gemini_cli_retrieve_user_quota_response,
    parse_gemini_cli_v1internal_credits_response, parse_windsurf_model_configs_response,
    parse_windsurf_rate_limit_response, parse_windsurf_user_status_response,
    provider_auto_remove_quota_exhausted_keys, quota_refresh_success_invalid_state,
    should_auto_remove_structured_reason, CodexQuotaMergeContext, CodexQuotaWindowCoverage,
    OAUTH_ACCOUNT_BLOCK_PREFIX, OAUTH_EXPIRED_PREFIX, OAUTH_REFRESH_FAILED_PREFIX,
    OAUTH_REQUEST_FAILED_PREFIX,
};
use aether_contracts::{ExecutionResult, ResponseBody};
use aether_data_contracts::repository::provider_catalog::StoredProviderCatalogKey;
use serde_json::json;
use std::collections::BTreeMap;

fn merge_codex_quota(
    current: Option<&serde_json::Value>,
    incoming: &serde_json::Value,
    observed_at_unix_secs: u64,
    request_started_at_unix_ms: u64,
    coverage: CodexQuotaWindowCoverage,
) -> super::CodexQuotaMergeOutcome {
    merge_codex_quota_metadata_snapshot(
        current,
        incoming,
        CodexQuotaMergeContext {
            observed_at_unix_secs,
            request_started_at_unix_ms: Some(request_started_at_unix_ms),
            request_order_id: None,
            observed_reset_generation: Some(0),
            authoritative_reset_generation: None,
            observed_credential_generation: None,
            account_reset_fence_id: None,
            coverage,
        },
    )
    .expect("quota metadata should merge")
}

fn merge_codex_quota_ordered(
    current: Option<&serde_json::Value>,
    incoming: &serde_json::Value,
    observed_at_unix_secs: u64,
    request_started_at_unix_ms: u64,
    request_order_id: &str,
    coverage: CodexQuotaWindowCoverage,
) -> super::CodexQuotaMergeOutcome {
    merge_codex_quota_metadata_snapshot(
        current,
        incoming,
        CodexQuotaMergeContext {
            observed_at_unix_secs,
            request_started_at_unix_ms: Some(request_started_at_unix_ms),
            request_order_id: Some(request_order_id),
            observed_reset_generation: Some(0),
            authoritative_reset_generation: None,
            observed_credential_generation: None,
            account_reset_fence_id: None,
            coverage,
        },
    )
    .expect("quota metadata should merge")
}

fn merge_codex_quota_after_explicit_reset(
    current: Option<&serde_json::Value>,
    incoming: &serde_json::Value,
    observed_at_unix_secs: u64,
    request_started_at_unix_ms: u64,
) -> super::CodexQuotaMergeOutcome {
    merge_codex_quota_metadata_snapshot(
        current,
        incoming,
        CodexQuotaMergeContext {
            observed_at_unix_secs,
            request_started_at_unix_ms: Some(request_started_at_unix_ms),
            request_order_id: Some("reset-refresh"),
            observed_reset_generation: Some(0),
            authoritative_reset_generation: None,
            observed_credential_generation: None,
            account_reset_fence_id: Some("reset-fence"),
            coverage: CodexQuotaWindowCoverage::AccountSnapshot,
        },
    )
    .expect("reset quota metadata should merge")
}

#[test]
fn codex_oauth_success_allows_equal_quota_and_oauth_watermarks() {
    let quota_watermark = json!({
        "account_quota_request_started_at_unix_ms": 1_000_u64,
        "account_quota_request_id": "01900000-0000-7000-8000-000000000010"
    });
    let oauth_watermark = json!({
        "oauth_state_request_started_at_unix_ms": 1_000_u64,
        "oauth_state_request_id": "01900000-0000-7000-8000-000000000010"
    });

    for current in [&quota_watermark, &oauth_watermark] {
        assert!(!codex_oauth_success_request_order_is_stale(
            current.as_object(),
            Some(1_000),
            Some("01900000-0000-7000-8000-000000000010"),
        ));
    }
}

#[test]
fn codex_oauth_success_rejects_older_order_and_same_millisecond_lower_id() {
    let current = json!({
        "quota_metadata_request_started_at_unix_ms": 1_000_u64,
        "quota_metadata_request_id": "01900000-0000-7000-8000-000000000010"
    });

    assert!(codex_oauth_success_request_order_is_stale(
        current.as_object(),
        Some(999),
        Some("01900000-0000-7000-8000-000000000099"),
    ));
    assert!(codex_oauth_success_request_order_is_stale(
        current.as_object(),
        Some(1_000),
        Some("01900000-0000-7000-8000-000000000009"),
    ));
}

#[test]
fn codex_quota_merge_keeps_usage_monotonic_within_one_generation() {
    let current = json!({
        "primary_used_percent": 60.0,
        "primary_reset_at": 2_000u64,
        "primary_window_minutes": 300u64,
        "account_quota_request_started_at_unix_ms": 90_000u64,
        "updated_at": 100u64
    });
    let incoming = json!({
        "primary_used_percent": 50.0,
        "primary_reset_at": 2_012u64,
        "primary_window_minutes": 300u64
    });

    let outcome = merge_codex_quota(
        Some(&current),
        &incoming,
        101,
        90_000,
        CodexQuotaWindowCoverage::AccountSnapshot,
    );

    assert!(!outcome.changed);
    assert_eq!(outcome.metadata, current);
}

#[test]
fn codex_quota_same_value_advances_request_watermark_once() {
    let current = json!({
        "primary_used_percent": 60.0,
        "primary_reset_at": 2_000u64,
        "primary_window_minutes": 300u64,
        "account_quota_request_started_at_unix_ms": 90_000u64,
        "updated_at": 100u64
    });
    let incoming = json!({
        "primary_used_percent": 60.0,
        "primary_reset_at": 2_000u64,
        "primary_window_minutes": 300u64
    });

    let outcome = merge_codex_quota(
        Some(&current),
        &incoming,
        101,
        100_000,
        CodexQuotaWindowCoverage::AccountSnapshot,
    );

    assert!(outcome.changed);
    assert_eq!(
        outcome.metadata["account_quota_request_started_at_unix_ms"],
        json!(100_000u64)
    );
    assert_eq!(outcome.metadata["updated_at"], json!(101u64));

    let duplicate = merge_codex_quota(
        Some(&outcome.metadata),
        &incoming,
        102,
        100_000,
        CodexQuotaWindowCoverage::AccountSnapshot,
    );
    assert!(!duplicate.changed);
    assert_eq!(duplicate.metadata, outcome.metadata);
}

#[test]
fn codex_quota_request_id_breaks_same_millisecond_ties() {
    let first = merge_codex_quota_ordered(
        None,
        &json!({
            "plan_type": "free",
            "primary_used_percent": 60.0,
            "primary_reset_at": 2_000u64,
            "primary_window_minutes": 300u64
        }),
        100,
        100_000,
        "request-a",
        CodexQuotaWindowCoverage::AccountSnapshot,
    );
    assert_eq!(
        first.metadata["account_quota_request_id"],
        json!("request-a")
    );
    assert_eq!(
        first.metadata["quota_metadata_request_id"],
        json!("request-a")
    );

    let newer = merge_codex_quota_ordered(
        Some(&first.metadata),
        &json!({
            "plan_type": "team",
            "primary_used_percent": 5.0,
            "primary_reset_at": 3_000_000u64,
            "primary_window_minutes": 43_800u64
        }),
        101,
        100_000,
        "request-z",
        CodexQuotaWindowCoverage::AccountSnapshot,
    );
    assert!(newer.changed);
    assert_eq!(newer.metadata["plan_type"], json!("team"));
    assert_eq!(newer.metadata["primary_window_minutes"], json!(43_800u64));
    assert_eq!(
        newer.metadata["account_quota_request_id"],
        json!("request-z")
    );
    assert_eq!(
        newer.metadata["quota_metadata_request_id"],
        json!("request-z")
    );

    let delayed = merge_codex_quota_ordered(
        Some(&newer.metadata),
        &json!({
            "plan_type": "plus",
            "primary_used_percent": 80.0,
            "primary_reset_at": 2_000u64,
            "primary_window_minutes": 300u64
        }),
        102,
        100_000,
        "request-m",
        CodexQuotaWindowCoverage::AccountSnapshot,
    );
    assert!(!delayed.changed);
    assert_eq!(delayed.metadata, newer.metadata);
}

#[test]
fn codex_quota_stale_same_millisecond_request_cannot_advance_generation() {
    let newer = merge_codex_quota_ordered(
        None,
        &json!({
            "primary_used_percent": 60.0,
            "primary_reset_at": 1_001u64,
            "primary_window_minutes": 300u64
        }),
        1,
        100_000,
        "request-b",
        CodexQuotaWindowCoverage::AccountSnapshot,
    );
    assert_eq!(newer.metadata["primary_reset_at"], json!(1_001u64));
    assert_eq!(newer.metadata["primary_used_percent"], json!(60.0));

    let delayed = merge_codex_quota_ordered(
        Some(&newer.metadata),
        &json!({
            "primary_used_percent": 5.0,
            "primary_reset_after_seconds": 60u64,
            "primary_window_minutes": 300u64
        }),
        1_000,
        100_000,
        "request-a",
        CodexQuotaWindowCoverage::AccountSnapshot,
    );

    assert!(!delayed.changed);
    assert_eq!(delayed.metadata, newer.metadata);
    assert_eq!(delayed.metadata["primary_reset_at"], json!(1_001u64));
    assert_eq!(delayed.metadata["primary_used_percent"], json!(60.0));
    assert_eq!(
        delayed.metadata["account_quota_request_id"],
        json!("request-b")
    );
}

#[test]
fn codex_quota_stale_request_can_raise_usage_within_same_generation() {
    let current = json!({
        "primary_used_percent": 60.0,
        "primary_reset_at": 1_001u64,
        "primary_window_minutes": 300u64,
        "account_quota_request_started_at_unix_ms": 100_000u64,
        "account_quota_request_id": "request-b",
        "updated_at": 1u64
    });
    let delayed = merge_codex_quota_ordered(
        Some(&current),
        &json!({
            "primary_used_percent": 80.0,
            "primary_reset_at": 1_010u64,
            "primary_window_minutes": 300u64
        }),
        2,
        100_000,
        "request-a",
        CodexQuotaWindowCoverage::AccountSnapshot,
    );

    assert!(delayed.changed);
    assert_eq!(delayed.metadata["primary_used_percent"], json!(80.0));
    assert_eq!(delayed.metadata["primary_reset_at"], json!(1_001u64));
    assert_eq!(
        delayed.metadata["account_quota_request_id"],
        json!("request-b")
    );
}

#[test]
fn codex_quota_request_id_supersedes_legacy_same_millisecond_watermark() {
    let legacy = json!({
        "primary_used_percent": 60.0,
        "primary_reset_at": 2_000u64,
        "primary_window_minutes": 300u64,
        "account_quota_request_started_at_unix_ms": 100_000u64,
        "updated_at": 100u64
    });

    let outcome = merge_codex_quota_ordered(
        Some(&legacy),
        &json!({
            "primary_used_percent": 5.0,
            "primary_reset_at": 3_000_000u64,
            "primary_window_minutes": 43_800u64
        }),
        101,
        100_000,
        "request-a",
        CodexQuotaWindowCoverage::AccountSnapshot,
    );

    assert!(outcome.changed);
    assert_eq!(outcome.metadata["primary_window_minutes"], json!(43_800u64));
    assert_eq!(
        outcome.metadata["account_quota_request_id"],
        json!("request-a")
    );
}

#[test]
fn codex_quota_merge_allows_usage_drop_after_deadline_advances() {
    let current = json!({
        "primary_used_percent": 100.0,
        "primary_reset_at": 2_000u64,
        "primary_window_minutes": 300u64,
        "updated_at": 100u64
    });
    let incoming = json!({
        "primary_used_percent": 2.0,
        "primary_reset_at": 20_000u64,
        "primary_window_minutes": 300u64
    });

    let outcome = merge_codex_quota(
        Some(&current),
        &incoming,
        101,
        100_000,
        CodexQuotaWindowCoverage::AccountSnapshot,
    );

    assert!(outcome.changed);
    assert_eq!(outcome.metadata["primary_used_percent"], json!(2.0));
    assert_eq!(outcome.metadata["primary_reset_at"], json!(20_000u64));
    assert_eq!(outcome.metadata["updated_at"], json!(101u64));
}

#[test]
fn codex_quota_full_snapshot_repairs_account_window_copied_from_spark() {
    let current = json!({
        "primary_used_percent": 32.0,
        "primary_reset_at": 1_788_153_237u64,
        "primary_window_minutes": 10_080u64,
        // Adjacent runtime and WHAM observations can move slightly while still proving
        // that the account window was copied from this Spark generation.
        "spark_secondary_used_percent": 30.0,
        "spark_secondary_reset_at": 1_788_153_238u64,
        "spark_secondary_window_minutes": 10_080u64,
        "account_quota_request_started_at_unix_ms": 100_000u64,
        "spark_quota_request_started_at_unix_ms": 100_000u64,
        "updated_at": 100u64
    });
    let authoritative_wham = parse_codex_wham_usage_response(
        &json!({
            "plan_type": "pro",
            "rate_limit": {
                "allowed": false,
                "limit_reached": true,
                "primary_window": {
                    "limit_window_seconds": 604_800u64,
                    "used_percent": 100.0,
                    "reset_at": 1_788_137_653u64
                },
                "secondary_window": null
            },
            "additional_rate_limits": [{
                "limit_name": "GPT-5.3-Codex-Spark",
                "metered_feature": "codex_bengalfox",
                "rate_limit": {
                    "primary_window": {
                        "limit_window_seconds": 18_000u64,
                        "used_percent": 0.0
                    },
                    "secondary_window": {
                        "limit_window_seconds": 604_800u64,
                        "used_percent": 32.0,
                        "reset_at": 1_788_153_238u64
                    }
                }
            }]
        }),
        110,
    )
    .expect("production-shaped WHAM response should parse");

    let outcome = merge_codex_quota(
        Some(&current),
        &authoritative_wham,
        110,
        110_000,
        CodexQuotaWindowCoverage::FullSnapshot,
    );

    assert!(outcome.changed);
    assert_eq!(outcome.metadata["primary_used_percent"], json!(100.0));
    assert_eq!(outcome.metadata["primary_window_minutes"], json!(10_080u64));
    assert_eq!(outcome.metadata["allowed"], json!(false));
    assert_eq!(outcome.metadata["limit_reached"], json!(true));
    assert!(codex_rate_limit_metadata_exhausted(&outcome.metadata));
    assert_eq!(
        outcome.metadata["primary_reset_at"],
        json!(1_788_137_653u64)
    );
    assert_eq!(outcome.metadata["spark_primary_used_percent"], json!(0.0));
    assert_eq!(
        outcome.metadata["spark_secondary_used_percent"],
        json!(32.0)
    );
    assert_eq!(
        outcome.metadata["spark_secondary_reset_at"],
        json!(1_788_153_238u64)
    );
}

#[test]
fn codex_quota_full_snapshot_rejects_deadline_only_contamination_fingerprint() {
    let current = json!({
        "primary_used_percent": 72.0,
        "primary_reset_at": 1_788_153_237u64,
        "primary_window_minutes": 10_080u64,
        "spark_secondary_used_percent": 32.0,
        "spark_secondary_reset_at": 1_788_153_238u64,
        "spark_secondary_window_minutes": 10_080u64,
        "account_quota_request_started_at_unix_ms": 100_000u64,
        "spark_quota_request_started_at_unix_ms": 100_000u64,
        "updated_at": 100u64
    });
    let authoritative_wham = json!({
        "primary_used_percent": 100.0,
        "primary_reset_at": 1_788_137_653u64,
        "primary_window_minutes": 10_080u64,
        "spark_secondary_used_percent": 32.0,
        "spark_secondary_reset_at": 1_788_153_238u64,
        "spark_secondary_window_minutes": 10_080u64
    });

    let outcome = merge_codex_quota(
        Some(&current),
        &authoritative_wham,
        110,
        110_000,
        CodexQuotaWindowCoverage::FullSnapshot,
    );

    assert_eq!(outcome.metadata["primary_used_percent"], json!(72.0));
    assert_eq!(
        outcome.metadata["primary_reset_at"],
        json!(1_788_153_237u64)
    );
}

#[test]
fn codex_quota_full_snapshot_does_not_replace_unrelated_later_account_generation() {
    let current = json!({
        "primary_used_percent": 32.0,
        "primary_reset_at": 1_788_153_237u64,
        "primary_window_minutes": 10_080u64,
        "account_quota_request_started_at_unix_ms": 100_000u64,
        "updated_at": 100u64
    });
    let authoritative_wham = json!({
        "primary_used_percent": 100.0,
        "primary_reset_at": 1_788_137_653u64,
        "primary_window_minutes": 10_080u64,
        "spark_secondary_used_percent": 33.0,
        "spark_secondary_reset_at": 1_788_190_000u64,
        "spark_secondary_window_minutes": 10_080u64
    });

    let outcome = merge_codex_quota(
        Some(&current),
        &authoritative_wham,
        110,
        110_000,
        CodexQuotaWindowCoverage::FullSnapshot,
    );

    assert_eq!(outcome.metadata["primary_used_percent"], json!(32.0));
    assert_eq!(
        outcome.metadata["primary_reset_at"],
        json!(1_788_153_237u64)
    );
}

#[test]
fn codex_quota_full_snapshot_requires_stored_spark_contamination_fingerprint() {
    let current = json!({
        "primary_used_percent": 32.0,
        "primary_reset_at": 1_788_153_237u64,
        "primary_window_minutes": 10_080u64,
        "spark_secondary_used_percent": 32.0,
        "spark_secondary_reset_at": 1_788_190_000u64,
        "spark_secondary_window_minutes": 10_080u64,
        "account_quota_request_started_at_unix_ms": 100_000u64,
        "spark_quota_request_started_at_unix_ms": 100_000u64,
        "updated_at": 100u64
    });
    let authoritative_wham = json!({
        "primary_used_percent": 100.0,
        "primary_reset_at": 1_788_137_653u64,
        "primary_window_minutes": 10_080u64,
        "spark_secondary_used_percent": 32.0,
        "spark_secondary_reset_at": 1_788_153_238u64,
        "spark_secondary_window_minutes": 10_080u64
    });

    let outcome = merge_codex_quota(
        Some(&current),
        &authoritative_wham,
        110,
        110_000,
        CodexQuotaWindowCoverage::FullSnapshot,
    );

    assert_eq!(outcome.metadata["primary_used_percent"], json!(32.0));
    assert_eq!(
        outcome.metadata["primary_reset_at"],
        json!(1_788_153_237u64)
    );
}

#[test]
fn codex_quota_full_snapshot_without_account_duration_cannot_repair_account_window() {
    let current = json!({
        "primary_used_percent": 32.0,
        "primary_reset_at": 1_788_153_237u64,
        "primary_window_minutes": 10_080u64,
        "account_quota_request_started_at_unix_ms": 100_000u64,
        "updated_at": 100u64
    });
    let authoritative_wham = json!({
        "primary_used_percent": 100.0,
        "primary_reset_at": 1_788_137_653u64,
        "spark_secondary_used_percent": 33.0,
        "spark_secondary_reset_at": 1_788_153_238u64,
        "spark_secondary_window_minutes": 10_080u64
    });

    let outcome = merge_codex_quota(
        Some(&current),
        &authoritative_wham,
        110,
        110_000,
        CodexQuotaWindowCoverage::FullSnapshot,
    );

    assert_eq!(outcome.metadata["primary_used_percent"], json!(32.0));
    assert_eq!(
        outcome.metadata["primary_reset_at"],
        json!(1_788_153_237u64)
    );
}

#[test]
fn codex_quota_stale_full_snapshot_cannot_repair_account_window_copied_from_spark() {
    let current = json!({
        "primary_used_percent": 32.0,
        "primary_reset_at": 1_788_153_237u64,
        "primary_window_minutes": 10_080u64,
        "spark_secondary_used_percent": 31.0,
        "spark_secondary_reset_at": 1_788_153_238u64,
        "spark_secondary_window_minutes": 10_080u64,
        "account_quota_request_started_at_unix_ms": 120_000u64,
        "spark_quota_request_started_at_unix_ms": 120_000u64,
        "updated_at": 100u64
    });
    let stale_wham = json!({
        "primary_used_percent": 100.0,
        "primary_reset_at": 1_788_137_653u64,
        "primary_window_minutes": 10_080u64,
        "spark_secondary_used_percent": 31.0,
        "spark_secondary_reset_at": 1_788_153_238u64,
        "spark_secondary_window_minutes": 10_080u64
    });

    let outcome = merge_codex_quota(
        Some(&current),
        &stale_wham,
        110,
        110_000,
        CodexQuotaWindowCoverage::FullSnapshot,
    );

    assert!(!outcome.changed);
    assert_eq!(outcome.metadata, current);
}

#[test]
fn codex_quota_deadline_tolerance_has_inclusive_thirty_second_boundary() {
    let current = json!({
        "primary_used_percent": 90.0,
        "primary_reset_at": 2_000u64,
        "primary_window_minutes": 300u64,
        "updated_at": 100u64
    });
    let cases = [
        (2_030u64, 10.0, 90.0, 2_000u64),
        (2_031u64, 10.0, 10.0, 2_031u64),
        (1_970u64, 95.0, 95.0, 2_000u64),
        (1_969u64, 95.0, 90.0, 2_000u64),
    ];

    for (incoming_deadline, incoming_usage, expected_usage, expected_deadline) in cases {
        let outcome = merge_codex_quota(
            Some(&current),
            &json!({
                "primary_used_percent": incoming_usage,
                "primary_reset_at": incoming_deadline,
                "primary_window_minutes": 300u64
            }),
            110,
            110_000,
            CodexQuotaWindowCoverage::AccountSnapshot,
        );

        assert_eq!(
            outcome.metadata["primary_used_percent"],
            json!(expected_usage),
            "incoming deadline {incoming_deadline}"
        );
        assert_eq!(
            outcome.metadata["primary_reset_at"],
            json!(expected_deadline),
            "incoming deadline {incoming_deadline}"
        );
    }
}

#[test]
fn codex_quota_legacy_window_without_deadline_stays_conservative() {
    let current = json!({
        "primary_used_percent": 100.0,
        "primary_window_minutes": 300u64,
        "updated_at": 100u64
    });

    let outcome = merge_codex_quota(
        Some(&current),
        &json!({
            "primary_used_percent": 1.0,
            "primary_reset_at": 20_000u64,
            "primary_window_minutes": 300u64
        }),
        110,
        110_000,
        CodexQuotaWindowCoverage::AccountSnapshot,
    );

    assert_eq!(outcome.metadata["primary_used_percent"], json!(100.0));
    assert_eq!(outcome.metadata["primary_reset_at"], json!(20_000u64));
}

#[test]
fn codex_quota_failed_reset_credit_detail_preserves_last_known_count_and_items() {
    let current = json!({
        "reset_credits": {
            "available_count": 2,
            "updated_at": 100u64,
            "detail_source": "wham_readonly",
            "detail_status": "available",
            "credits": [{
                "id": "credit-1",
                "display_key": "credit",
                "status": "available",
                "expires_at": 20_000u64
            }]
        },
        "updated_at": 100u64
    });
    let incoming = json!({
        "reset_credits": {
            "updated_at": 110u64,
            "detail_source": "wham_readonly",
            "detail_status": "failed",
            "detail_error": "HTTP 429",
            "credits": []
        }
    });

    let outcome = merge_codex_quota(
        Some(&current),
        &incoming,
        110,
        110_000,
        CodexQuotaWindowCoverage::Patch,
    );

    assert!(outcome.changed);
    assert_eq!(
        outcome.metadata["reset_credits"]["available_count"],
        json!(2u64)
    );
    assert_eq!(
        outcome.metadata["reset_credits"]["credits"][0]["id"],
        json!("credit-1")
    );
    assert_eq!(
        outcome.metadata["reset_credits"]["detail_status"],
        json!("failed")
    );
    assert_eq!(
        outcome.metadata["reset_credits"]["detail_error"],
        json!("HTTP 429")
    );
}

#[test]
fn codex_quota_explicit_reset_allows_usage_drop_with_same_deadline() {
    let current = json!({
        "primary_used_percent": 100.0,
        "primary_reset_at": 20_000u64,
        "primary_window_minutes": 300u64,
        "account_quota_request_started_at_unix_ms": 100_000u64,
        "account_quota_request_id": "before-reset",
            "account_quota_reset_fence_unix_ms": 105_000u64,
            "account_quota_reset_fence_id": "reset-fence",
            "account_quota_reset_processed_ids": ["redeem-once"],
        "account_quota_reset_pending": true,
        "updated_at": 100u64
    });
    let incoming = json!({
        "primary_used_percent": 0.0,
        "primary_reset_at": 20_000u64,
        "primary_window_minutes": 300u64
    });

    let outcome = merge_codex_quota_after_explicit_reset(Some(&current), &incoming, 110, 110_000);

    assert!(outcome.changed);
    assert_eq!(outcome.metadata["primary_used_percent"], json!(0.0));
    assert_eq!(outcome.metadata["primary_reset_at"], json!(20_000u64));
    assert_eq!(
        outcome.metadata["account_quota_reset_pending"],
        json!(false)
    );
}

#[test]
fn codex_quota_reset_fence_rejects_pre_reset_response_after_baseline() {
    let baseline = merge_codex_quota_after_explicit_reset(
        Some(&json!({
            "primary_used_percent": 100.0,
            "primary_reset_at": 20_000u64,
            "primary_window_minutes": 300u64,
            "account_quota_request_started_at_unix_ms": 100_000u64,
            "account_quota_request_id": "before-reset",
            "account_quota_reset_fence_unix_ms": 105_000u64,
            "account_quota_reset_fence_id": "reset-fence",
            "account_quota_reset_pending": true,
            "updated_at": 100u64
        })),
        &json!({
            "primary_used_percent": 0.0,
            "primary_reset_at": 20_000u64,
            "primary_window_minutes": 300u64
        }),
        110,
        110_000,
    );

    let delayed = merge_codex_quota_ordered(
        Some(&baseline.metadata),
        &json!({
            "primary_used_percent": 100.0,
            "primary_reset_at": 20_000u64,
            "primary_window_minutes": 300u64
        }),
        120,
        100_000,
        "old-in-flight",
        CodexQuotaWindowCoverage::AccountSnapshot,
    );

    assert!(!delayed.changed);
    assert_eq!(delayed.metadata, baseline.metadata);
    assert_eq!(delayed.metadata["primary_used_percent"], json!(0.0));
}

#[test]
fn codex_quota_reset_fence_rejects_pre_reset_account_metadata() {
    let current = json!({
        "plan_type": "plus",
        "reset_credits": {
            "available_count": 0,
            "updated_at": 110,
        },
        "primary_used_percent": 0.0,
        "primary_reset_at": 20_000u64,
        "primary_window_minutes": 300u64,
        "account_quota_request_started_at_unix_ms": 110_000u64,
        "account_quota_request_id": "after-reset",
        "quota_metadata_request_started_at_unix_ms": 110_000u64,
        "quota_metadata_request_id": "after-reset",
        "account_quota_reset_fence_unix_ms": 105_000u64,
        "account_quota_reset_fence_id": "reset-fence",
        "account_quota_reset_pending": false,
        "updated_at": 110u64
    });
    let delayed = merge_codex_quota_ordered(
        Some(&current),
        &json!({
            "plan_type": "team",
            "reset_credits": {
                "available_count": 1,
                "updated_at": 100,
            },
            "primary_used_percent": 100.0,
            "primary_reset_at": 20_000u64,
            "primary_window_minutes": 300u64
        }),
        120,
        100_000,
        "before-reset",
        CodexQuotaWindowCoverage::AccountSnapshot,
    );

    assert!(!delayed.changed);
    assert_eq!(delayed.metadata, current);
    assert_eq!(delayed.metadata["plan_type"], json!("plus"));
    assert_eq!(
        delayed.metadata["reset_credits"]["available_count"],
        json!(0)
    );
}

#[test]
fn codex_quota_pending_reset_ignores_runtime_patch_but_not_spark() {
    let current = json!({
        "primary_used_percent": 100.0,
        "primary_reset_at": 20_000u64,
        "primary_window_minutes": 300u64,
        "spark_primary_used_percent": 20.0,
        "spark_primary_reset_at": 30_000u64,
        "spark_primary_window_minutes": 300u64,
        "account_quota_reset_fence_unix_ms": 105_000u64,
        "account_quota_reset_fence_id": "reset-fence",
        "account_quota_reset_pending": true,
        "updated_at": 100u64
    });
    let incoming = json!({
        "primary_used_percent": 0.0,
        "primary_reset_at": 20_000u64,
        "primary_window_minutes": 300u64,
        "spark_primary_used_percent": 30.0,
        "spark_primary_reset_at": 30_000u64,
        "spark_primary_window_minutes": 300u64
    });

    let outcome = merge_codex_quota_ordered(
        Some(&current),
        &incoming,
        110,
        110_000,
        "after-reset-runtime",
        CodexQuotaWindowCoverage::Patch,
    );

    assert_eq!(outcome.metadata["primary_used_percent"], json!(100.0));
    assert_eq!(outcome.metadata["spark_primary_used_percent"], json!(30.0));
    assert_eq!(outcome.metadata["account_quota_reset_pending"], json!(true));
}

#[test]
fn codex_quota_reset_fence_treats_same_millisecond_request_as_pre_reset() {
    let current = json!({
        "primary_used_percent": 100.0,
        "primary_reset_at": 20_000u64,
        "primary_window_minutes": 300u64,
        "account_quota_reset_fence_unix_ms": 105_000u64,
        "account_quota_reset_fence_id": "reset-fence",
        "account_quota_reset_pending": true,
        "updated_at": 100u64
    });

    let outcome = merge_codex_quota_ordered(
        Some(&current),
        &json!({
            "primary_used_percent": 0.0,
            "primary_reset_at": 20_000u64,
            "primary_window_minutes": 300u64
        }),
        110,
        105_000,
        "uuid-that-sorts-after-fence",
        CodexQuotaWindowCoverage::AccountSnapshot,
    );

    assert!(!outcome.changed);
    assert_eq!(outcome.metadata, current);
}

#[test]
fn codex_quota_reset_pending_waits_for_usage_to_drop() {
    let current = json!({
        "primary_used_percent": 100.0,
        "primary_reset_at": 20_000u64,
        "primary_window_minutes": 300u64,
        "account_quota_reset_fence_unix_ms": 105_000u64,
        "account_quota_reset_fence_id": "reset-fence",
        "account_quota_reset_pending": true,
        "updated_at": 100u64
    });
    let unchanged = merge_codex_quota_after_explicit_reset(
        Some(&current),
        &json!({
            "primary_used_percent": 100.0,
            "primary_reset_at": 20_000u64,
            "primary_window_minutes": 300u64
        }),
        110,
        110_000,
    );
    assert!(!unchanged.changed);
    assert_eq!(
        unchanged.metadata["account_quota_reset_pending"],
        json!(true)
    );

    let reset = merge_codex_quota_after_explicit_reset(
        Some(&unchanged.metadata),
        &json!({
            "primary_used_percent": 0.0,
            "primary_reset_at": 20_000u64,
            "primary_window_minutes": 300u64
        }),
        111,
        111_000,
    );
    assert!(reset.changed);
    assert_eq!(reset.metadata["primary_used_percent"], json!(0.0));
    assert_eq!(reset.metadata["account_quota_reset_pending"], json!(false));
}

#[test]
fn codex_quota_reset_pending_disambiguates_equal_duration_slot_swap() {
    let current = json!({
        "primary_used_percent": 90.0,
        "primary_reset_at": 1_000u64,
        "primary_window_minutes": 300u64,
        "secondary_used_percent": 10.0,
        "secondary_reset_at": 2_000u64,
        "secondary_window_minutes": 300u64,
        "account_quota_reset_fence_unix_ms": 105_000u64,
        "account_quota_reset_fence_id": "reset-fence",
        "account_quota_reset_pending": true,
        "updated_at": 100u64
    });
    let swapped_but_increased = json!({
        "primary_used_percent": 20.0,
        "primary_reset_at": 2_005u64,
        "primary_window_minutes": 300u64,
        "secondary_used_percent": 95.0,
        "secondary_reset_at": 1_005u64,
        "secondary_window_minutes": 300u64
    });

    let outcome = merge_codex_quota_after_explicit_reset(
        Some(&current),
        &swapped_but_increased,
        110,
        110_000,
    );

    assert!(!outcome.changed);
    assert_eq!(outcome.metadata, current);
    assert_eq!(outcome.metadata["account_quota_reset_pending"], json!(true));
}

#[test]
fn codex_quota_reset_pending_accepts_authoritative_zero_after_zero_baseline() {
    let current = json!({
        "primary_used_percent": 0.0,
        "primary_reset_at": 20_000u64,
        "primary_window_minutes": 300u64,
        "account_quota_reset_fence_unix_ms": 105_000u64,
        "account_quota_reset_fence_id": "reset-fence",
        "account_quota_reset_pending": true,
        "updated_at": 100u64
    });
    let confirmed = merge_codex_quota_after_explicit_reset(
        Some(&current),
        &json!({
            "primary_used_percent": 0.0,
            "primary_reset_at": 20_000u64,
            "primary_window_minutes": 300u64
        }),
        110,
        110_000,
    );

    assert!(confirmed.changed);
    assert_eq!(confirmed.metadata["primary_used_percent"], json!(0.0));
    assert_eq!(
        confirmed.metadata["account_quota_reset_pending"],
        json!(false)
    );
}

#[test]
fn codex_quota_pre_reset_generation_cannot_touch_account_but_spark_still_merges() {
    let current = json!({
        "plan_type": "plus",
        "primary_used_percent": 20.0,
        "primary_reset_at": 2_000u64,
        "primary_window_minutes": 300u64,
        "spark_primary_used_percent": 10.0,
        "spark_primary_reset_at": 3_000u64,
        "spark_primary_window_minutes": 300u64,
        "account_quota_reset_generation": 2u64,
        "updated_at": 100u64
    });
    let incoming = json!({
        "plan_type": "team",
        "primary_used_percent": 90.0,
        "primary_reset_at": 2_000u64,
        "primary_window_minutes": 300u64,
        "spark_primary_used_percent": 30.0,
        "spark_primary_reset_at": 3_000u64,
        "spark_primary_window_minutes": 300u64
    });

    let outcome = merge_codex_quota_metadata_snapshot(
        Some(&current),
        &incoming,
        CodexQuotaMergeContext {
            observed_at_unix_secs: 110,
            request_started_at_unix_ms: Some(110_000),
            request_order_id: Some("pre-reset"),
            observed_reset_generation: Some(1),
            authoritative_reset_generation: None,
            observed_credential_generation: None,
            account_reset_fence_id: None,
            coverage: CodexQuotaWindowCoverage::FullSnapshot,
        },
    )
    .expect("quota metadata should merge");

    assert!(outcome.changed);
    assert_eq!(outcome.metadata["plan_type"], json!("plus"));
    assert_eq!(outcome.metadata["primary_used_percent"], json!(20.0));
    assert_eq!(outcome.metadata["spark_primary_used_percent"], json!(30.0));
}

#[test]
fn codex_quota_credential_generation_mismatch_rejects_every_field() {
    let current = json!({
        "credential_generation": "credential-new",
        "plan_type": "plus",
        "primary_used_percent": 20.0,
        "primary_reset_at": 2_000u64,
        "primary_window_minutes": 300u64,
        "spark_primary_used_percent": 10.0,
        "spark_primary_reset_at": 3_000u64,
        "spark_primary_window_minutes": 300u64,
        "updated_at": 100u64
    });
    let incoming = json!({
        "plan_type": "team",
        "primary_used_percent": 90.0,
        "spark_primary_used_percent": 30.0
    });

    let outcome = merge_codex_quota_metadata_snapshot(
        Some(&current),
        &incoming,
        CodexQuotaMergeContext {
            observed_at_unix_secs: 110,
            request_started_at_unix_ms: Some(110_000),
            request_order_id: Some("old-credential"),
            observed_reset_generation: Some(0),
            authoritative_reset_generation: None,
            observed_credential_generation: Some("credential-old"),
            account_reset_fence_id: None,
            coverage: CodexQuotaWindowCoverage::FullSnapshot,
        },
    )
    .expect("quota metadata should be acknowledged");

    assert!(!outcome.changed);
    assert_eq!(outcome.metadata, current);
}

#[test]
fn codex_quota_only_latest_reset_generation_can_close_pending_zero_baseline() {
    let current = json!({
        "primary_used_percent": 0.0,
        "primary_reset_at": 2_000u64,
        "primary_window_minutes": 300u64,
        "account_quota_reset_generation": 2u64,
        "account_quota_reset_pending_generation": 2u64,
        "account_quota_reset_pending": true,
        "account_quota_reset_fence_unix_ms": 105_000u64,
        "account_quota_reset_fence_id": "reset:b",
        "updated_at": 100u64
    });
    let incoming = json!({
        "primary_used_percent": 0.0,
        "primary_reset_at": 2_000u64,
        "primary_window_minutes": 300u64
    });
    let merge = |generation| {
        merge_codex_quota_metadata_snapshot(
            Some(&current),
            &incoming,
            CodexQuotaMergeContext {
                observed_at_unix_secs: 110,
                request_started_at_unix_ms: Some(110_000),
                request_order_id: Some("reset-refresh"),
                observed_reset_generation: Some(generation),
                authoritative_reset_generation: Some(generation),
                observed_credential_generation: None,
                account_reset_fence_id: Some("reset:b"),
                coverage: CodexQuotaWindowCoverage::AccountSnapshot,
            },
        )
        .expect("quota metadata should merge")
    };

    let stale = merge(1);
    assert!(!stale.changed);
    assert_eq!(stale.metadata["account_quota_reset_pending"], json!(true));

    let current_generation = merge(2);
    assert!(current_generation.changed);
    assert_eq!(
        current_generation.metadata["account_quota_reset_pending"],
        json!(false)
    );
}

#[test]
fn codex_quota_pending_reset_recovers_from_later_current_generation_snapshot() {
    let current = json!({
        "primary_used_percent": 100.0,
        "primary_reset_at": 2_000u64,
        "primary_window_minutes": 300u64,
        "account_quota_reset_generation": 2u64,
        "account_quota_reset_pending_generation": 2u64,
        "account_quota_reset_pending": true,
        "account_quota_reset_fence_unix_ms": 105_000u64,
        "account_quota_reset_fence_id": "reset:b",
        "updated_at": 100u64
    });
    let incoming = json!({
        "primary_used_percent": 20.0,
        "primary_reset_at": 2_000u64,
        "primary_window_minutes": 300u64
    });
    let merge = |request_started_at_unix_ms, coverage| {
        merge_codex_quota_metadata_snapshot(
            Some(&current),
            &incoming,
            CodexQuotaMergeContext {
                observed_at_unix_secs: 120,
                request_started_at_unix_ms: Some(request_started_at_unix_ms),
                request_order_id: Some("later-refresh"),
                observed_reset_generation: Some(2),
                authoritative_reset_generation: None,
                observed_credential_generation: None,
                account_reset_fence_id: None,
                coverage,
            },
        )
        .expect("quota metadata should merge")
    };

    let pre_reset_request = merge(100_000, CodexQuotaWindowCoverage::AccountSnapshot);
    assert!(!pre_reset_request.changed);
    assert_eq!(
        pre_reset_request.metadata["account_quota_reset_pending"],
        json!(true)
    );

    let partial_headers = merge(120_000, CodexQuotaWindowCoverage::Patch);
    assert!(!partial_headers.changed);
    assert_eq!(
        partial_headers.metadata["account_quota_reset_pending"],
        json!(true)
    );

    let settled = merge(120_000, CodexQuotaWindowCoverage::AccountSnapshot);
    assert!(settled.changed);
    assert_eq!(settled.metadata["primary_used_percent"], json!(20.0));
    assert_eq!(
        settled.metadata["account_quota_reset_pending"],
        json!(false)
    );
}

#[test]
fn codex_quota_merge_derives_stable_deadline_from_observation_time() {
    let first = merge_codex_quota(
        None,
        &json!({
            "primary_used_percent": 60.0,
            "primary_reset_after_seconds": 900u64,
            "primary_window_minutes": 300u64
        }),
        1_000,
        900_000,
        CodexQuotaWindowCoverage::AccountSnapshot,
    );
    assert_eq!(first.metadata["primary_reset_at"], json!(1_900u64));

    let delayed = merge_codex_quota(
        Some(&first.metadata),
        &json!({
            "primary_used_percent": 50.0,
            "primary_reset_after_seconds": 890u64,
            "primary_window_minutes": 300u64
        }),
        1_010,
        800_000,
        CodexQuotaWindowCoverage::AccountSnapshot,
    );

    assert!(!delayed.changed);
    assert_eq!(delayed.metadata["primary_used_percent"], json!(60.0));
    assert_eq!(delayed.metadata["primary_reset_at"], json!(1_900u64));
}

#[test]
fn codex_quota_merge_replaces_paid_windows_with_monthly_shape() {
    let current = json!({
        "primary_used_percent": 70.0,
        "primary_reset_at": 10_000u64,
        "primary_window_minutes": 10_080u64,
        "secondary_used_percent": 20.0,
        "secondary_reset_at": 2_000u64,
        "secondary_window_minutes": 300u64,
        "account_quota_request_started_at_unix_ms": 100_000u64,
        "updated_at": 100u64
    });
    let monthly = json!({
        "primary_used_percent": 5.0,
        "primary_reset_at": 3_000_000u64,
        "primary_window_minutes": 43_800u64
    });

    let outcome = merge_codex_quota(
        Some(&current),
        &monthly,
        200,
        150_000,
        CodexQuotaWindowCoverage::AccountSnapshot,
    );

    assert!(outcome.changed);
    assert_eq!(outcome.metadata["primary_window_minutes"], json!(43_800u64));
    assert_eq!(outcome.metadata["primary_used_percent"], json!(5.0));
    assert!(outcome.metadata.get("secondary_used_percent").is_none());
    assert!(outcome.metadata.get("secondary_window_minutes").is_none());
}

#[test]
fn codex_quota_merge_stale_request_cannot_restore_old_window_shape() {
    let monthly = json!({
        "primary_used_percent": 5.0,
        "primary_reset_at": 3_000_000u64,
        "primary_window_minutes": 43_800u64,
        "account_quota_request_started_at_unix_ms": 150_000u64,
        "updated_at": 200u64
    });
    let delayed_paid = json!({
        "primary_used_percent": 70.0,
        "primary_reset_at": 10_000u64,
        "primary_window_minutes": 10_080u64,
        "secondary_used_percent": 20.0,
        "secondary_reset_at": 2_000u64,
        "secondary_window_minutes": 300u64
    });

    let outcome = merge_codex_quota(
        Some(&monthly),
        &delayed_paid,
        210,
        100_000,
        CodexQuotaWindowCoverage::AccountSnapshot,
    );

    assert!(!outcome.changed);
    assert_eq!(outcome.metadata, monthly);
}

#[test]
fn codex_quota_merge_matches_duration_when_windows_move_slots() {
    let free = json!({
        "primary_used_percent": 40.0,
        "primary_reset_at": 2_000u64,
        "primary_window_minutes": 300u64,
        "account_quota_request_started_at_unix_ms": 100_000u64,
        "updated_at": 100u64
    });
    let paid = json!({
        "primary_used_percent": 10.0,
        "primary_reset_at": 10_000u64,
        "primary_window_minutes": 10_080u64,
        "secondary_used_percent": 30.0,
        "secondary_reset_at": 2_005u64,
        "secondary_window_minutes": 300u64
    });

    let outcome = merge_codex_quota(
        Some(&free),
        &paid,
        110,
        110_000,
        CodexQuotaWindowCoverage::AccountSnapshot,
    );

    assert!(outcome.changed);
    assert_eq!(outcome.metadata["primary_window_minutes"], json!(10_080u64));
    assert_eq!(outcome.metadata["secondary_window_minutes"], json!(300u64));
    assert_eq!(outcome.metadata["secondary_used_percent"], json!(40.0));
}

#[test]
fn codex_quota_merge_uses_deadline_to_disambiguate_equal_duration_slot_swap() {
    let current = json!({
        "primary_used_percent": 90.0,
        "primary_reset_at": 1_000u64,
        "primary_window_minutes": 300u64,
        "secondary_used_percent": 10.0,
        "secondary_reset_at": 2_000u64,
        "secondary_window_minutes": 300u64,
        "account_quota_request_started_at_unix_ms": 100_000u64,
        "updated_at": 100u64
    });
    let swapped = json!({
        "primary_used_percent": 20.0,
        "primary_reset_at": 2_005u64,
        "primary_window_minutes": 300u64,
        "secondary_used_percent": 95.0,
        "secondary_reset_at": 1_005u64,
        "secondary_window_minutes": 300u64
    });

    let outcome = merge_codex_quota(
        Some(&current),
        &swapped,
        110,
        110_000,
        CodexQuotaWindowCoverage::AccountSnapshot,
    );

    assert!(outcome.changed);
    assert_eq!(outcome.metadata["primary_used_percent"], json!(20.0));
    assert_eq!(outcome.metadata["primary_reset_at"], json!(2_000u64));
    assert_eq!(outcome.metadata["secondary_used_percent"], json!(95.0));
    assert_eq!(outcome.metadata["secondary_reset_at"], json!(1_000u64));
}

#[test]
fn codex_quota_patch_ignores_primary_only_header_for_paid_windows() {
    let current = json!({
        "primary_used_percent": 20.0,
        "primary_reset_at": 10_000u64,
        "primary_window_minutes": 10_080u64,
        "secondary_used_percent": 40.0,
        "secondary_reset_at": 2_000u64,
        "secondary_window_minutes": 300u64,
        "account_quota_request_started_at_unix_ms": 100_000u64,
        "updated_at": 100u64
    });
    let headers = BTreeMap::from([("x-codex-primary-used-percent".to_string(), "80".to_string())]);
    let partial =
        parse_codex_usage_headers(&headers, 110).expect("partial Codex usage headers should parse");

    let outcome = merge_codex_quota(
        Some(&current),
        &partial,
        110,
        110_000,
        CodexQuotaWindowCoverage::Patch,
    );

    assert!(!outcome.changed);
    assert_eq!(outcome.metadata, current);
}

#[test]
fn codex_quota_patch_without_duration_uses_slot_for_single_window() {
    let current = json!({
        "primary_used_percent": 60.0,
        "primary_reset_at": 2_000u64,
        "primary_window_minutes": 300u64,
        "account_quota_request_started_at_unix_ms": 100_000u64,
        "updated_at": 100u64
    });

    let outcome = merge_codex_quota(
        Some(&current),
        &json!({ "primary_used_percent": 70.0 }),
        110,
        110_000,
        CodexQuotaWindowCoverage::Patch,
    );

    assert!(outcome.changed);
    assert_eq!(outcome.metadata["primary_used_percent"], json!(70.0));
    assert_eq!(outcome.metadata["primary_window_minutes"], json!(300u64));
}

#[test]
fn codex_quota_patch_with_duration_matches_across_paid_slots() {
    let current = json!({
        "primary_used_percent": 20.0,
        "primary_reset_at": 10_000u64,
        "primary_window_minutes": 10_080u64,
        "secondary_used_percent": 40.0,
        "secondary_reset_at": 2_000u64,
        "secondary_window_minutes": 300u64,
        "account_quota_request_started_at_unix_ms": 100_000u64,
        "updated_at": 100u64
    });

    let outcome = merge_codex_quota(
        Some(&current),
        &json!({
            "primary_used_percent": 70.0,
            "primary_reset_at": 2_005u64,
            "primary_window_minutes": 300u64
        }),
        110,
        110_000,
        CodexQuotaWindowCoverage::Patch,
    );

    assert!(outcome.changed);
    assert_eq!(outcome.metadata["primary_used_percent"], json!(20.0));
    assert_eq!(outcome.metadata["secondary_used_percent"], json!(70.0));
    assert_eq!(outcome.metadata["secondary_window_minutes"], json!(300u64));
}

#[test]
fn codex_quota_account_snapshot_preserves_spark_windows() {
    let current = json!({
        "primary_used_percent": 50.0,
        "primary_reset_at": 2_000u64,
        "primary_window_minutes": 300u64,
        "spark_primary_used_percent": 25.0,
        "spark_primary_reset_at": 3_000u64,
        "spark_primary_window_minutes": 300u64,
        "updated_at": 100u64
    });
    let outcome = merge_codex_quota(
        Some(&current),
        &json!({
            "primary_used_percent": 60.0,
            "primary_reset_at": 2_000u64,
            "primary_window_minutes": 300u64
        }),
        110,
        110_000,
        CodexQuotaWindowCoverage::AccountSnapshot,
    );

    assert!(outcome.changed);
    assert_eq!(outcome.metadata["spark_primary_used_percent"], json!(25.0));
    assert_eq!(outcome.metadata["spark_primary_reset_at"], json!(3_000u64));
}

#[test]
fn codex_quota_full_snapshot_removes_absent_spark_and_zero_windows() {
    let current = json!({
        "primary_used_percent": 50.0,
        "primary_reset_at": 2_000u64,
        "primary_window_minutes": 300u64,
        "secondary_used_percent": 20.0,
        "secondary_window_minutes": 10_080u64,
        "spark_primary_used_percent": 25.0,
        "spark_primary_window_minutes": 300u64,
        "updated_at": 100u64
    });
    let outcome = merge_codex_quota(
        Some(&current),
        &json!({
            "primary_used_percent": 55.0,
            "primary_reset_at": 2_000u64,
            "primary_window_minutes": 300u64,
            "secondary_window_minutes": 0u64
        }),
        110,
        110_000,
        CodexQuotaWindowCoverage::FullSnapshot,
    );

    assert!(outcome.changed);
    assert!(outcome.metadata.get("secondary_window_minutes").is_none());
    assert!(outcome.metadata.get("spark_primary_used_percent").is_none());
    assert!(outcome
        .metadata
        .get("spark_primary_window_minutes")
        .is_none());
}

#[test]
fn codex_quota_merge_legacy_slot_without_identity_is_monotonic() {
    let current = json!({
        "primary_used_percent": 60.0,
        "account_quota_request_started_at_unix_ms": 110_000u64,
        "updated_at": 100u64
    });
    let outcome = merge_codex_quota(
        Some(&current),
        &json!({ "primary_used_percent": 50.0 }),
        110,
        110_000,
        CodexQuotaWindowCoverage::Patch,
    );

    assert!(!outcome.changed);
    assert_eq!(outcome.metadata, current);
}

#[test]
fn codex_quota_stale_request_cannot_overwrite_account_metadata() {
    let current = json!({
        "plan_type": "free",
        "credits_balance": 20.0,
        "primary_used_percent": 10.0,
        "primary_reset_at": 20_000u64,
        "primary_window_minutes": 43_800u64,
        "quota_metadata_request_started_at_unix_ms": 200_000u64,
        "account_quota_request_started_at_unix_ms": 200_000u64,
        "updated_at": 200u64
    });
    let stale = json!({
        "plan_type": "plus",
        "credits_balance": 2.0,
        "primary_used_percent": 80.0,
        "primary_reset_at": 2_000u64,
        "primary_window_minutes": 300u64
    });

    let outcome = merge_codex_quota(
        Some(&current),
        &stale,
        210,
        100_000,
        CodexQuotaWindowCoverage::AccountSnapshot,
    );

    assert!(!outcome.changed);
    assert_eq!(outcome.metadata, current);
}

#[test]
fn codex_quota_metadata_uses_the_newest_family_watermark() {
    let current = json!({
        "plan_type": "free",
        "credits_balance": 20.0,
        "primary_used_percent": 40.0,
        "primary_reset_at": 20_000u64,
        "primary_window_minutes": 300u64,
        "quota_metadata_request_started_at_unix_ms": 100_000u64,
        "account_quota_request_started_at_unix_ms": 200_000u64,
        "updated_at": 200u64
    });
    let delayed = json!({
        "plan_type": "plus",
        "credits_balance": 2.0,
        "primary_used_percent": 50.0,
        "primary_reset_at": 20_000u64,
        "primary_window_minutes": 300u64
    });

    let outcome = merge_codex_quota(
        Some(&current),
        &delayed,
        210,
        150_000,
        CodexQuotaWindowCoverage::AccountSnapshot,
    );

    assert!(outcome.changed);
    assert_eq!(outcome.metadata["plan_type"], json!("free"));
    assert_eq!(outcome.metadata["credits_balance"], json!(20.0));
    assert_eq!(outcome.metadata["primary_used_percent"], json!(50.0));
    assert_eq!(
        outcome.metadata["quota_metadata_request_started_at_unix_ms"],
        json!(100_000u64)
    );
    assert_eq!(
        outcome.metadata["account_quota_request_started_at_unix_ms"],
        json!(200_000u64)
    );
}

#[test]
fn codex_account_status_ignores_a_newer_spark_only_watermark() {
    let current = json!({
        "allowed": true,
        "limit_reached": false,
        "primary_used_percent": 99.0,
        "primary_reset_at": 20_000u64,
        "primary_window_minutes": 10_080u64,
        "spark_secondary_used_percent": 40.0,
        "spark_secondary_reset_at": 30_000u64,
        "spark_secondary_window_minutes": 10_080u64,
        "account_quota_request_started_at_unix_ms": 100_000u64,
        "spark_quota_request_started_at_unix_ms": 200_000u64,
        "updated_at": 200u64
    });
    let wham = json!({
        "allowed": false,
        "limit_reached": true,
        "primary_used_percent": 99.0,
        "primary_reset_at": 20_000u64,
        "primary_window_minutes": 10_080u64,
        "spark_secondary_used_percent": 40.0,
        "spark_secondary_reset_at": 30_000u64,
        "spark_secondary_window_minutes": 10_080u64
    });

    let outcome = merge_codex_quota(
        Some(&current),
        &wham,
        210,
        150_000,
        CodexQuotaWindowCoverage::FullSnapshot,
    );

    assert!(outcome.changed);
    assert_eq!(outcome.metadata["primary_used_percent"], json!(99.0));
    assert_eq!(outcome.metadata["allowed"], json!(false));
    assert_eq!(outcome.metadata["limit_reached"], json!(true));
    assert!(codex_rate_limit_metadata_exhausted(&outcome.metadata));
    assert_eq!(
        outcome.metadata["account_quota_request_started_at_unix_ms"],
        json!(150_000u64)
    );
    assert_eq!(
        outcome.metadata["spark_quota_request_started_at_unix_ms"],
        json!(200_000u64)
    );
}

#[test]
fn stale_codex_account_status_cannot_override_newer_account_signal() {
    let current = json!({
        "allowed": false,
        "limit_reached": true,
        "primary_used_percent": 99.0,
        "primary_reset_at": 20_000u64,
        "primary_window_minutes": 10_080u64,
        "account_quota_request_started_at_unix_ms": 200_000u64,
        "updated_at": 200u64
    });
    let stale = json!({
        "allowed": true,
        "limit_reached": false,
        "primary_used_percent": 99.0,
        "primary_reset_at": 20_000u64,
        "primary_window_minutes": 10_080u64
    });

    let outcome = merge_codex_quota(
        Some(&current),
        &stale,
        210,
        150_000,
        CodexQuotaWindowCoverage::AccountSnapshot,
    );

    assert!(!outcome.changed);
    assert_eq!(outcome.metadata, current);
}

#[test]
fn execution_error_detail_preserves_structured_code_and_message() {
    let result = ExecutionResult {
        request_id: "quota-agent-identity".to_string(),
        candidate_id: None,
        status_code: 401,
        headers: BTreeMap::new(),
        response_observation: None,
        body: Some(ResponseBody {
            json_body: Some(json!({
                "error": {
                    "code": "invalid_task_id",
                    "message": "registered task is no longer valid"
                }
            })),
            body_bytes_b64: None,
        }),
        telemetry: None,
        error: None,
    };

    let detail = extract_execution_error_detail(&result)
        .expect("structured execution error should be retained");
    assert!(detail.contains(r#""code":"invalid_task_id""#));
    assert!(detail.contains(r#""message":"registered task is no longer valid""#));
    assert!(
        aether_provider_transport::is_codex_agent_identity_invalid_task_response(
            result.status_code,
            Some(&detail),
        )
    );
}

#[test]
fn provider_auto_remove_quota_exhausted_keys_defaults_to_false() {
    assert!(!provider_auto_remove_quota_exhausted_keys(None));
    assert!(!provider_auto_remove_quota_exhausted_keys(Some(&json!({
        "pool_advanced": {}
    }))));
}

#[test]
fn provider_auto_remove_quota_exhausted_keys_reads_pool_advanced_flag() {
    assert!(provider_auto_remove_quota_exhausted_keys(Some(&json!({
        "pool_advanced": {
            "auto_remove_quota_exhausted_keys": true
        }
    }))));
}

#[test]
fn codex_runtime_invalid_reason_marks_401_as_expired() {
    assert_eq!(
        codex_runtime_invalid_reason(401, Some("session expired")),
        Some(format!("{OAUTH_EXPIRED_PREFIX}Codex Token 已过期"))
    );
}

#[test]
fn codex_runtime_invalid_reason_marks_account_deactivated_403() {
    assert_eq!(
        codex_runtime_invalid_reason(403, Some("account has been deactivated")),
        Some(format!("{OAUTH_ACCOUNT_BLOCK_PREFIX}OpenAI 账号已停用"))
    );
}

#[test]
fn codex_runtime_invalid_reason_marks_inactive_pat_owner_403_as_token_invalid() {
    assert_eq!(
        codex_runtime_invalid_reason(403, Some("Personal access token owner is inactive.")),
        Some(format!("{OAUTH_EXPIRED_PREFIX}Codex Token 已失效"))
    );
    assert_eq!(
        codex_runtime_invalid_reason(
            403,
            Some("biscuit_baker_service_auth_credential_error_status")
        ),
        Some(format!("{OAUTH_EXPIRED_PREFIX}Codex Token 已失效"))
    );
}

#[test]
fn codex_runtime_invalid_reason_marks_deleted_agent_runtime_as_invalid() {
    assert_eq!(
        codex_runtime_invalid_reason(403, Some("Agent runtime has been deleted.")),
        Some(format!("{OAUTH_EXPIRED_PREFIX}Codex Token 已失效"))
    );
}

#[test]
fn codex_runtime_invalid_reason_marks_402_as_account_blocked() {
    assert_eq!(
        codex_runtime_invalid_reason(402, Some("payment required")),
        Some(format!(
            "{OAUTH_ACCOUNT_BLOCK_PREFIX}Codex 账户需要付款 (402)"
        ))
    );
}

#[test]
fn codex_runtime_invalid_reason_marks_generic_403_as_token_invalid() {
    assert_eq!(
        codex_runtime_invalid_reason(403, Some("forbidden")),
        Some(format!("{OAUTH_EXPIRED_PREFIX}Codex Token 已失效 (403)"))
    );
}

#[test]
fn codex_invalid_reason_does_not_persist_upstream_credentials() {
    let reason = codex_runtime_invalid_reason(
        401,
        Some("authorization=Bearer upstream-secret https://user:pass@example.test?q=secret"),
    )
    .expect("401 should produce a reason");

    assert_eq!(
        reason,
        format!("{OAUTH_EXPIRED_PREFIX}Codex Token 已过期 (401)")
    );
    assert!(!reason.contains("upstream-secret"));
    assert!(!reason.contains("user:pass"));
}

#[test]
fn codex_invalid_state_appends_refresh_failure_to_oauth_expired() {
    let mut key = StoredProviderCatalogKey::new(
        "key-1".to_string(),
        "provider-1".to_string(),
        "key-1".to_string(),
        "oauth".to_string(),
        None,
        true,
    )
    .expect("key should build");
    key.oauth_invalid_at_unix_secs = Some(100);
    key.oauth_invalid_reason = Some(format!("{OAUTH_EXPIRED_PREFIX}session expired"));

    assert_eq!(
        codex_build_invalid_state(
            &key,
            format!("{OAUTH_REFRESH_FAILED_PREFIX}Token 续期失败"),
            200,
        ),
        (
            Some(200),
            Some(format!(
                "{OAUTH_EXPIRED_PREFIX}session expired\n{OAUTH_REFRESH_FAILED_PREFIX}Token 续期失败"
            ))
        )
    );
}

#[test]
fn codex_invalid_state_preserves_refresh_failure_when_oauth_expired_arrives_later() {
    let mut key = StoredProviderCatalogKey::new(
        "key-1".to_string(),
        "provider-1".to_string(),
        "key-1".to_string(),
        "oauth".to_string(),
        None,
        true,
    )
    .expect("key should build");
    key.oauth_invalid_at_unix_secs = Some(100);
    key.oauth_invalid_reason = Some(format!(
        "{OAUTH_REFRESH_FAILED_PREFIX}Token 续期失败 (401): refresh_token 无效"
    ));
    let expected = format!(
        "{OAUTH_EXPIRED_PREFIX}session expired\n{OAUTH_REFRESH_FAILED_PREFIX}Token 续期失败 (401): refresh_token 无效"
    );

    assert_eq!(
        codex_build_invalid_state(&key, format!("{OAUTH_EXPIRED_PREFIX}session expired"), 200,),
        (Some(200), Some(expected.clone()))
    );

    key.oauth_invalid_reason = Some(format!(
        "{OAUTH_REFRESH_FAILED_PREFIX}Token 续期失败 (401): refresh_token 无效"
    ));
    assert_eq!(
        codex_build_invalid_state(&key, expected.clone(), 200),
        (Some(200), Some(expected))
    );

    key.oauth_invalid_reason = Some(format!(
        "{OAUTH_EXPIRED_PREFIX}old session expired\n{OAUTH_REFRESH_FAILED_PREFIX}Token 续期失败 (401): refresh_token 无效"
    ));
    assert_eq!(
        codex_build_invalid_state(
            &key,
            format!("{OAUTH_EXPIRED_PREFIX}new session expired"),
            300,
        ),
        (
            Some(300),
            Some(format!(
                "{OAUTH_EXPIRED_PREFIX}new session expired\n{OAUTH_REFRESH_FAILED_PREFIX}Token 续期失败 (401): refresh_token 无效"
            ))
        )
    );
}

#[test]
fn codex_invalid_state_keeps_oauth_expired_over_request_failure() {
    let mut key = StoredProviderCatalogKey::new(
        "key-1".to_string(),
        "provider-1".to_string(),
        "key-1".to_string(),
        "oauth".to_string(),
        None,
        true,
    )
    .expect("key should build");
    key.oauth_invalid_at_unix_secs = Some(100);
    key.oauth_invalid_reason = Some(format!("{OAUTH_EXPIRED_PREFIX}session expired"));

    assert_eq!(
        codex_build_invalid_state(
            &key,
            format!("{OAUTH_REQUEST_FAILED_PREFIX}账号状态检查失败"),
            200,
        ),
        (
            Some(100),
            Some(format!("{OAUTH_EXPIRED_PREFIX}session expired"))
        )
    );
}

#[test]
fn codex_invalid_state_keeps_refresh_failure_over_request_failure() {
    let mut key = StoredProviderCatalogKey::new(
        "key-1".to_string(),
        "provider-1".to_string(),
        "key-1".to_string(),
        "oauth".to_string(),
        None,
        true,
    )
    .expect("key should build");
    key.oauth_invalid_at_unix_secs = Some(100);
    key.oauth_invalid_reason = Some(format!(
        "{OAUTH_REFRESH_FAILED_PREFIX}Token 续期失败 (401): refresh_token 无效"
    ));

    assert_eq!(
        codex_build_invalid_state(
            &key,
            format!("{OAUTH_REQUEST_FAILED_PREFIX}账号状态检查失败"),
            200,
        ),
        (
            Some(100),
            Some(format!(
                "{OAUTH_REFRESH_FAILED_PREFIX}Token 续期失败 (401): refresh_token 无效"
            ))
        )
    );
}

#[test]
fn codex_invalid_state_allows_account_block_to_override_oauth_expired() {
    let mut key = StoredProviderCatalogKey::new(
        "key-1".to_string(),
        "provider-1".to_string(),
        "key-1".to_string(),
        "oauth".to_string(),
        None,
        true,
    )
    .expect("key should build");
    key.oauth_invalid_at_unix_secs = Some(100);
    key.oauth_invalid_reason = Some(format!("{OAUTH_EXPIRED_PREFIX}session expired"));

    assert_eq!(
        codex_build_invalid_state(
            &key,
            format!("{OAUTH_ACCOUNT_BLOCK_PREFIX}account has been deactivated"),
            200,
        ),
        (
            Some(200),
            Some(format!(
                "{OAUTH_ACCOUNT_BLOCK_PREFIX}account has been deactivated"
            ))
        )
    );
}

#[test]
fn auto_remove_structured_reason_removes_oauth_token_invalidated() {
    assert!(should_auto_remove_structured_reason(Some(
        "[OAUTH_EXPIRED] token invalidated"
    )));
}

#[test]
fn auto_remove_structured_reason_keeps_oauth_token_expired() {
    assert!(!should_auto_remove_structured_reason(Some(
        "[OAUTH_EXPIRED] session expired"
    )));
}

#[test]
fn auto_remove_refresh_failed_after_access_token_expiry() {
    let mut key = StoredProviderCatalogKey::new(
        "key-1".to_string(),
        "provider-1".to_string(),
        "key-1".to_string(),
        "oauth".to_string(),
        None,
        true,
    )
    .expect("key should build");
    key.expires_at_unix_secs = Some(1_000);
    key.oauth_invalid_reason = Some(format!(
        "{OAUTH_REFRESH_FAILED_PREFIX}Token 续期失败 (401): refresh_token 无效、已过期或已撤销，请重新登录授权"
    ));

    assert!(!super::should_auto_remove_oauth_invalid_key(
        &key, None, false, 999
    ));
    assert!(super::should_auto_remove_oauth_invalid_key(
        &key, None, false, 1_000
    ));
}

#[test]
fn auto_remove_combined_refresh_and_access_token_failure() {
    let mut key = StoredProviderCatalogKey::new(
        "key-1".to_string(),
        "provider-1".to_string(),
        "key-1".to_string(),
        "oauth".to_string(),
        None,
        true,
    )
    .expect("key should build");
    key.expires_at_unix_secs = Some(2_000);
    key.oauth_invalid_reason = Some(format!(
        "{OAUTH_REFRESH_FAILED_PREFIX}Token 续期失败 (401): refresh_token 无效、已过期或已撤销，请重新登录授权"
    ));

    assert!(super::should_auto_remove_oauth_invalid_key(
        &key, None, true, 1_000,
    ));
}

#[test]
fn auto_remove_existing_oauth_expired_after_terminal_refresh_failure() {
    let mut key = StoredProviderCatalogKey::new(
        "key-1".to_string(),
        "provider-1".to_string(),
        "key-1".to_string(),
        "oauth".to_string(),
        None,
        true,
    )
    .expect("key should build");
    key.expires_at_unix_secs = Some(2_000);
    key.oauth_invalid_reason = Some(format!(
        "{OAUTH_EXPIRED_PREFIX}access token invalid\n{OAUTH_REFRESH_FAILED_PREFIX}Token 续期失败 (401): refresh_token 无效、已过期或已撤销，请重新登录授权"
    ));

    assert!(super::should_auto_remove_oauth_invalid_key(
        &key, None, false, 1_000,
    ));
}

#[test]
fn candidate_oauth_expired_is_not_auto_remove_proof_by_itself() {
    let mut key = StoredProviderCatalogKey::new(
        "key-1".to_string(),
        "provider-1".to_string(),
        "key-1".to_string(),
        "oauth".to_string(),
        None,
        true,
    )
    .expect("key should build");
    key.expires_at_unix_secs = Some(2_000);
    key.oauth_invalid_reason = Some(format!(
        "{OAUTH_REFRESH_FAILED_PREFIX}Token 续期失败 (401): refresh_token 无效、已过期或已撤销，请重新登录授权"
    ));

    assert!(!super::should_auto_remove_oauth_invalid_key(
        &key,
        Some("[OAUTH_EXPIRED] access token invalid"),
        false,
        1_000,
    ));
}

#[test]
fn oauth_token_invalid_is_auto_remove_proof_by_itself() {
    let mut key = StoredProviderCatalogKey::new(
        "key-1".to_string(),
        "provider-1".to_string(),
        "key-1".to_string(),
        "oauth".to_string(),
        None,
        true,
    )
    .expect("key should build");
    key.expires_at_unix_secs = Some(1_000);
    key.oauth_invalid_reason = Some("oauth_token_invalid".to_string());

    assert!(super::should_auto_remove_oauth_invalid_key(
        &key,
        Some("oauth_token_invalid"),
        false,
        1_001,
    ));
}

#[test]
fn does_not_auto_remove_access_token_failure_without_refresh_failure() {
    let mut key = StoredProviderCatalogKey::new(
        "key-1".to_string(),
        "provider-1".to_string(),
        "key-1".to_string(),
        "oauth".to_string(),
        None,
        true,
    )
    .expect("key should build");
    key.expires_at_unix_secs = Some(1_000);
    key.oauth_invalid_reason = Some(format!("{OAUTH_EXPIRED_PREFIX}session expired"));

    assert!(!super::should_auto_remove_oauth_invalid_key(
        &key, None, false, 1_001
    ));
}

#[test]
fn does_not_auto_remove_non_terminal_refresh_failure() {
    let mut key = StoredProviderCatalogKey::new(
        "key-1".to_string(),
        "provider-1".to_string(),
        "key-1".to_string(),
        "oauth".to_string(),
        None,
        true,
    )
    .expect("key should build");
    key.expires_at_unix_secs = Some(1_000);
    key.oauth_invalid_reason = Some(format!("{OAUTH_REFRESH_FAILED_PREFIX}Token 续期失败"));

    assert!(!super::should_auto_remove_oauth_invalid_key(
        &key, None, true, 1_001
    ));
}

#[test]
fn quota_refresh_success_clears_refresh_failed_marker() {
    let mut key = StoredProviderCatalogKey::new(
        "key-1".to_string(),
        "provider-1".to_string(),
        "key-1".to_string(),
        "oauth".to_string(),
        None,
        true,
    )
    .expect("key should build");
    key.oauth_invalid_reason = Some("[REFRESH_FAILED] Token 续期失败".to_string());

    assert_eq!(quota_refresh_success_invalid_state(&key), (None, None));
}

#[test]
fn auto_remove_structured_reason_keeps_request_and_refresh_failures() {
    assert!(!should_auto_remove_structured_reason(Some(
        "[REQUEST_FAILED] 账号状态检查失败"
    )));
    assert!(!should_auto_remove_structured_reason(Some(
        "[REFRESH_FAILED] Token 续期失败 (401): refresh_token 已失效"
    )));
}

/// Header set captured from a `gpt-5.6-sol` response: the request was metered against the
/// plan's own limit, so the unprefixed windows are the account's and the Spark limit is
/// announced separately.
fn codex_premium_response_headers() -> BTreeMap<String, String> {
    BTreeMap::from([
        ("x-codex-plan-type".to_string(), "pro".to_string()),
        ("x-codex-active-limit".to_string(), "premium".to_string()),
        ("x-codex-primary-used-percent".to_string(), "50".to_string()),
        (
            "x-codex-primary-window-minutes".to_string(),
            "10080".to_string(),
        ),
        (
            "x-codex-primary-reset-at".to_string(),
            "1787801347".to_string(),
        ),
        (
            "x-codex-secondary-used-percent".to_string(),
            "0".to_string(),
        ),
        (
            "x-codex-secondary-window-minutes".to_string(),
            "0".to_string(),
        ),
        ("x-codex-secondary-reset-at".to_string(), "".to_string()),
        (
            "x-codex-secondary-reset-after-seconds".to_string(),
            "0".to_string(),
        ),
        (
            "x-codex-bengalfox-limit-name".to_string(),
            "GPT-5.3-Codex-Spark".to_string(),
        ),
        (
            "x-codex-bengalfox-primary-used-percent".to_string(),
            "17".to_string(),
        ),
        (
            "x-codex-bengalfox-primary-window-minutes".to_string(),
            "300".to_string(),
        ),
        (
            "x-codex-bengalfox-primary-reset-at".to_string(),
            "1787410252".to_string(),
        ),
        (
            "x-codex-bengalfox-secondary-used-percent".to_string(),
            "8".to_string(),
        ),
        (
            "x-codex-bengalfox-secondary-window-minutes".to_string(),
            "10080".to_string(),
        ),
        (
            "x-codex-bengalfox-secondary-reset-at".to_string(),
            "1787833868".to_string(),
        ),
    ])
}

/// Header set captured from a `gpt-5.3-codex-spark` response three seconds later on the same
/// key: the unprefixed windows now repeat the Spark limit verbatim, and only
/// `x-codex-active-limit` says so.
fn codex_spark_response_headers() -> BTreeMap<String, String> {
    BTreeMap::from([
        ("x-codex-plan-type".to_string(), "pro".to_string()),
        (
            "x-codex-active-limit".to_string(),
            "codex_bengalfox".to_string(),
        ),
        ("x-codex-primary-used-percent".to_string(), "22".to_string()),
        (
            "x-codex-primary-window-minutes".to_string(),
            "300".to_string(),
        ),
        (
            "x-codex-primary-reset-at".to_string(),
            "1787410252".to_string(),
        ),
        (
            "x-codex-secondary-used-percent".to_string(),
            "11".to_string(),
        ),
        (
            "x-codex-secondary-window-minutes".to_string(),
            "10080".to_string(),
        ),
        (
            "x-codex-secondary-reset-at".to_string(),
            "1787833868".to_string(),
        ),
        (
            "x-codex-secondary-reset-after-seconds".to_string(),
            "424336".to_string(),
        ),
        (
            "x-codex-bengalfox-limit-name".to_string(),
            "GPT-5.3-Codex-Spark".to_string(),
        ),
        (
            "x-codex-bengalfox-primary-used-percent".to_string(),
            "22".to_string(),
        ),
        (
            "x-codex-bengalfox-primary-window-minutes".to_string(),
            "300".to_string(),
        ),
        (
            "x-codex-bengalfox-primary-reset-at".to_string(),
            "1787410252".to_string(),
        ),
        (
            "x-codex-bengalfox-secondary-used-percent".to_string(),
            "11".to_string(),
        ),
        (
            "x-codex-bengalfox-secondary-window-minutes".to_string(),
            "10080".to_string(),
        ),
        (
            "x-codex-bengalfox-secondary-reset-at".to_string(),
            "1787833868".to_string(),
        ),
    ])
}

#[test]
fn codex_usage_headers_keep_the_account_window_when_the_plan_limit_is_active() {
    let parsed = parse_codex_usage_headers(&codex_premium_response_headers(), 1_787_409_530)
        .expect("Codex usage headers should parse");

    assert_eq!(parsed.get("primary_used_percent"), Some(&json!(50.0)));
    assert_eq!(
        parsed.get("primary_window_minutes"),
        Some(&json!(10_080u64))
    );
    assert_eq!(
        parsed.get("primary_reset_at"),
        Some(&json!(1_787_801_347u64))
    );
    assert_eq!(parsed.get("secondary_window_minutes"), Some(&json!(0u64)));
}

#[test]
fn codex_usage_headers_capture_named_limit_windows_alongside_the_account_window() {
    let parsed = parse_codex_usage_headers(&codex_premium_response_headers(), 1_787_409_530)
        .expect("Codex usage headers should parse");

    assert_eq!(parsed.get("spark_primary_used_percent"), Some(&json!(17.0)));
    assert_eq!(
        parsed.get("spark_primary_window_minutes"),
        Some(&json!(300u64))
    );
    assert_eq!(
        parsed.get("spark_primary_reset_at"),
        Some(&json!(1_787_410_252u64))
    );
    assert_eq!(
        parsed.get("spark_secondary_used_percent"),
        Some(&json!(8.0))
    );
    assert_eq!(
        parsed.get("spark_secondary_window_minutes"),
        Some(&json!(10_080u64))
    );
}

#[test]
fn codex_usage_headers_keep_model_scoped_windows_out_of_the_account_family() {
    let parsed = parse_codex_usage_headers(&codex_spark_response_headers(), 1_787_409_530)
        .expect("Codex usage headers should parse");

    // Without this the paid-window swap promotes the Spark weekly window into the account's
    // own weekly slot, which is what the scheduler reads.
    assert!(
        parsed.get("primary_used_percent").is_none(),
        "the Spark limit governed this request, so it must not claim the account window: {parsed}"
    );
    assert!(parsed.get("primary_window_minutes").is_none());
    assert!(parsed.get("primary_reset_at").is_none());
    assert!(parsed.get("secondary_used_percent").is_none());
    assert!(parsed.get("primary_over_secondary_limit_percent").is_none());

    assert_eq!(parsed.get("spark_primary_used_percent"), Some(&json!(22.0)));
    assert_eq!(
        parsed.get("spark_secondary_used_percent"),
        Some(&json!(11.0))
    );
    assert_eq!(parsed.get("plan_type"), Some(&json!("pro")));
}

#[test]
fn codex_usage_headers_without_an_active_limit_still_fill_the_account_window() {
    let mut headers = codex_premium_response_headers();
    headers.remove("x-codex-active-limit");

    let parsed = parse_codex_usage_headers(&headers, 1_787_409_530)
        .expect("Codex usage headers should parse");

    assert_eq!(parsed.get("primary_used_percent"), Some(&json!(50.0)));
    assert_eq!(parsed.get("spark_primary_used_percent"), Some(&json!(17.0)));
}

#[test]
fn codex_usage_headers_ignore_an_active_limit_that_matches_no_announced_family() {
    let mut headers = codex_premium_response_headers();
    headers.insert(
        "x-codex-active-limit".to_string(),
        "codex_unknown_feature".to_string(),
    );

    let parsed = parse_codex_usage_headers(&headers, 1_787_409_530)
        .expect("Codex usage headers should parse");

    assert_eq!(parsed.get("primary_used_percent"), Some(&json!(50.0)));
}

#[test]
fn parses_codex_spark_quota_from_additional_rate_limits() {
    let parsed = parse_codex_wham_usage_response(
        &json!({
            "plan_type": "plus",
            "rate_limit": {
                "allowed": false,
                "limit_reached": true,
                "primary_window": {
                    "used_percent": 25.0,
                    "reset_after_seconds": 604800,
                    "reset_at": 1_900_000_000u64
                },
                "secondary_window": {
                    "used_percent": 10.0,
                    "reset_after_seconds": 18000,
                    "reset_at": 1_800_000_000u64
                }
            },
            "additional_rate_limits": [{
                "limit_name": "GPT-5.3-Codex-Spark",
                "metered_feature": "codex_bengalfox",
                "rate_limit": {
                    "primary_window": {
                        "used_percent": 40.0,
                        "limit_window_seconds": 18000,
                        "reset_after_seconds": 9000,
                        "reset_at": 1_780_000_000u64
                    },
                    "secondary_window": {
                        "used_percent": 5.0,
                        "limit_window_seconds": 604800,
                        "reset_after_seconds": 300000,
                        "reset_at": 1_790_000_000u64
                    }
                }
            }]
        }),
        1_777_000_000,
    )
    .expect("codex wham usage should parse");

    assert_eq!(parsed.get("allowed"), Some(&json!(false)));
    assert_eq!(parsed.get("limit_reached"), Some(&json!(true)));
    assert_eq!(parsed.get("primary_used_percent"), Some(&json!(10.0)));
    assert_eq!(parsed.get("secondary_used_percent"), Some(&json!(25.0)));
    assert_eq!(parsed.get("spark_primary_used_percent"), Some(&json!(40.0)));
    assert_eq!(
        parsed.get("spark_primary_window_minutes"),
        Some(&json!(300u64))
    );
    assert_eq!(
        parsed.get("spark_secondary_used_percent"),
        Some(&json!(5.0))
    );
    assert_eq!(
        parsed.get("spark_secondary_window_minutes"),
        Some(&json!(10_080u64))
    );
    let additional = parsed
        .get("additional_quota_windows")
        .and_then(serde_json::Value::as_array)
        .expect("all additional quota windows should be preserved");
    assert!(
        additional.is_empty(),
        "the Spark windows are already normalized into the dedicated spark fields"
    );
}

#[test]
fn parses_unknown_additional_quota_buckets_without_model_specific_code() {
    let parsed = parse_codex_wham_usage_response(
        &json!({
            "rate_limit": {
                "primary_window": {"used_percent": 100.0},
                "secondary_window": {"used_percent": 0.0}
            },
            "additional_rate_limits": [{
                "limit_name": "Future-Model-Limit",
                "rate_limit": {
                    "primary_window": {
                        "used_percent": 100.0,
                        "limit_window_seconds": 3600
                    }
                }
            }]
        }),
        1_700_000_000,
    )
    .expect("quota response should parse");
    let additional = parsed["additional_quota_windows"]
        .as_array()
        .expect("additional windows should be an array");
    assert_eq!(additional.len(), 1);
    assert_eq!(additional[0]["model"], json!("Future-Model-Limit"));
    assert_eq!(additional[0]["is_exhausted"], json!(true));
}

#[test]
fn codex_quota_parses_monthly_header_with_zero_secondary_tombstone() {
    let headers = BTreeMap::from([
        ("x-codex-plan-type".to_string(), "team".to_string()),
        ("x-codex-primary-used-percent".to_string(), "14".to_string()),
        (
            "x-codex-primary-reset-after-seconds".to_string(),
            "2627672".to_string(),
        ),
        (
            "x-codex-primary-reset-at".to_string(),
            "1786915122".to_string(),
        ),
        (
            "x-codex-primary-window-minutes".to_string(),
            "43800".to_string(),
        ),
        (
            "x-codex-secondary-used-percent".to_string(),
            "0".to_string(),
        ),
        (
            "x-codex-secondary-reset-after-seconds".to_string(),
            "0".to_string(),
        ),
        ("x-codex-secondary-reset-at".to_string(), "".to_string()),
        (
            "x-codex-secondary-window-minutes".to_string(),
            "0".to_string(),
        ),
    ]);

    let parsed = parse_codex_usage_headers(&headers, 1_784_287_450)
        .expect("Codex usage headers should parse");

    assert_eq!(parsed.get("primary_used_percent"), Some(&json!(14.0)));
    assert_eq!(
        parsed.get("primary_window_minutes"),
        Some(&json!(43_800u64))
    );
    assert!(parsed.get("secondary_used_percent").is_none());
    assert_eq!(parsed.get("secondary_window_minutes"), Some(&json!(0u64)));
}

#[test]
fn codex_quota_monthly_header_patch_removes_previous_five_hour_window() {
    let current = json!({
        "primary_used_percent": 30.0,
        "primary_reset_at": 1_790_000_000u64,
        "primary_window_minutes": 10_080u64,
        "secondary_used_percent": 70.0,
        "secondary_reset_at": 1_785_000_000u64,
        "secondary_window_minutes": 300u64,
        "account_quota_request_started_at_unix_ms": 100_000u64,
        "updated_at": 1_784_000_000u64
    });
    let headers = BTreeMap::from([
        ("x-codex-plan-type".to_string(), "team".to_string()),
        ("x-codex-primary-used-percent".to_string(), "14".to_string()),
        (
            "x-codex-primary-reset-after-seconds".to_string(),
            "2627672".to_string(),
        ),
        (
            "x-codex-primary-reset-at".to_string(),
            "1786915122".to_string(),
        ),
        (
            "x-codex-primary-window-minutes".to_string(),
            "43800".to_string(),
        ),
        (
            "x-codex-secondary-used-percent".to_string(),
            "0".to_string(),
        ),
        (
            "x-codex-secondary-reset-after-seconds".to_string(),
            "0".to_string(),
        ),
        ("x-codex-secondary-reset-at".to_string(), "".to_string()),
        (
            "x-codex-secondary-window-minutes".to_string(),
            "0".to_string(),
        ),
    ]);
    let monthly = parse_codex_usage_headers(&headers, 1_784_287_450)
        .expect("monthly Codex headers should parse");

    let outcome = merge_codex_quota(
        Some(&current),
        &monthly,
        1_784_287_450,
        110_000,
        CodexQuotaWindowCoverage::Patch,
    );

    assert!(outcome.changed);
    assert_eq!(outcome.metadata["primary_used_percent"], json!(14.0));
    assert_eq!(outcome.metadata["primary_window_minutes"], json!(43_800u64));
    assert!(outcome.metadata.get("secondary_used_percent").is_none());
    assert!(outcome.metadata.get("secondary_window_minutes").is_none());
}

#[test]
fn codex_quota_parses_monthly_body_with_complete_zero_secondary_tombstone() {
    let parsed = parse_codex_wham_usage_response(
        &json!({
            "plan_type": "team",
            "rate_limit": {
                "primary_window": {
                    "used_percent": 14.0,
                    "reset_after_seconds": 2_627_672u64,
                    "reset_at": 1_786_915_122u64,
                    "window_minutes": 43_800u64
                },
                "secondary_window": {
                    "used_percent": 0.0,
                    "reset_after_seconds": 0u64,
                    "reset_at": null,
                    "window_minutes": 0u64
                }
            }
        }),
        1_784_287_450,
    )
    .expect("monthly Codex body should parse");

    assert_eq!(parsed.get("secondary_window_minutes"), Some(&json!(0u64)));
}

#[test]
fn codex_quota_partial_zero_secondary_header_does_not_emit_tombstone() {
    let headers = BTreeMap::from([
        ("x-codex-primary-used-percent".to_string(), "14".to_string()),
        (
            "x-codex-primary-window-minutes".to_string(),
            "43800".to_string(),
        ),
        (
            "x-codex-secondary-window-minutes".to_string(),
            "0".to_string(),
        ),
    ]);
    let parsed = parse_codex_usage_headers(&headers, 1_784_287_450)
        .expect("partial Codex headers should parse");

    assert!(parsed.get("secondary_window_minutes").is_none());
}

#[test]
fn parses_codex_websocket_rate_limits_from_chunk_envelope() {
    let parsed = parse_codex_websocket_rate_limits_response(
        &json!({
            "chunks": [
                {"type": "response.output_text.delta", "delta": "ignored"},
                {
                    "type": "codex.rate_limits",
                    "plan_type": "free",
                    "rate_limits": {
                        "allowed": true,
                        "limit_reached": false,
                        "primary": {
                            "used_percent": 91,
                            "window_minutes": 43200,
                            "reset_after_seconds": 2590791,
                            "reset_at": 1787154563u64
                        }
                    }
                }
            ]
        }),
        1_787_000_000,
    )
    .expect("Codex WebSocket quota chunk should parse");

    assert_eq!(parsed.get("plan_type"), Some(&json!("free")));
    assert_eq!(parsed.get("allowed"), Some(&json!(true)));
    assert_eq!(parsed.get("limit_reached"), Some(&json!(false)));
    assert_eq!(parsed.get("primary_used_percent"), Some(&json!(91.0)));
    assert_eq!(
        parsed.get("primary_window_minutes"),
        Some(&json!(43_200u64))
    );
    assert_eq!(
        parsed.get("primary_reset_after_seconds"),
        Some(&json!(2_590_791u64))
    );
    assert!(parsed.get("secondary_used_percent").is_none());
}

#[test]
fn parses_codex_websocket_usage_limit_error_headers() {
    let parsed = parse_codex_websocket_rate_limits_response(
        &json!({
            "type": "error",
            "error": {
                "type": "usage_limit_reached",
                "plan_type": "free",
                "resets_at": 1_787_274_385u64,
                "resets_in_seconds": 2_590_077u64,
            },
            "status_code": 429,
            "headers": {
                "X-Codex-Plan-Type": "free",
                "X-Codex-Primary-Used-Percent": "100",
                "X-Codex-Primary-Window-Minutes": "43200",
                "X-Codex-Primary-Reset-After-Seconds": "2590078",
                "X-Codex-Primary-Reset-At": "1787274385",
                "X-Codex-Credits-Has-Credits": "False",
            },
        }),
        1_787_000_000,
    )
    .expect("Codex usage-limit error should parse as quota metadata");

    assert_eq!(parsed.get("allowed"), Some(&json!(false)));
    assert_eq!(parsed.get("limit_reached"), Some(&json!(true)));
    assert_eq!(parsed.get("plan_type"), Some(&json!("free")));
    assert_eq!(parsed.get("primary_used_percent"), Some(&json!(100.0)));
    assert_eq!(
        parsed.get("primary_reset_at"),
        Some(&json!(1_787_274_385u64))
    );
    assert!(codex_rate_limit_metadata_exhausted(&parsed));
}

#[test]
fn codex_usage_limit_error_does_not_backfill_a_model_scoped_reset_into_the_account() {
    let event = json!({
        "type": "error",
        "error": {
            "type": "usage_limit_reached",
            "plan_type": "pro",
            "resets_at": 1_787_430_252u64,
            "resets_in_seconds": 20_722u64,
        },
        "status_code": 429,
        "headers": {
            "X-Codex-Plan-Type": "pro",
            "X-Codex-Active-Limit": "codex_bengalfox",
            "X-Codex-Primary-Used-Percent": "100",
            "X-Codex-Primary-Window-Minutes": "300",
            "X-Codex-Primary-Reset-At": "1787430252",
            "X-Codex-Bengalfox-Limit-Name": "GPT-5.3-Codex-Spark",
            "X-Codex-Bengalfox-Primary-Used-Percent": "100",
            "X-Codex-Bengalfox-Primary-Window-Minutes": "300",
            "X-Codex-Bengalfox-Primary-Reset-At": "1787430252",
        },
    });
    let parsed = parse_codex_websocket_rate_limits_response(&event, 1_787_409_530)
        .expect("Codex usage-limit error should parse as quota metadata");

    assert!(
        parsed.get("primary_reset_at").is_none(),
        "the Spark limit rejected this request, so its reset timing must not claim the account window: {parsed}"
    );
    assert!(parsed.get("primary_reset_after_seconds").is_none());
    assert_eq!(
        parsed.get("spark_primary_reset_at"),
        Some(&json!(1_787_430_252u64))
    );
    assert_eq!(
        parsed.get("spark_primary_used_percent"),
        Some(&json!(100.0))
    );
    assert!(parsed.get("allowed").is_none());
    assert!(parsed.get("limit_reached").is_none());
    assert!(!codex_rate_limit_metadata_exhausted(&parsed));
    assert!(codex_websocket_response_has_usage_limit_error(&event));
    assert_eq!(
        codex_websocket_usage_limit_reset_at(&event, 1_787_409_530),
        Some(1_787_430_252u64)
    );
}

#[test]
fn codex_rate_limit_metadata_detects_explicit_and_window_exhaustion() {
    assert!(codex_rate_limit_metadata_exhausted(&json!({
        "allowed": false
    })));
    assert!(codex_rate_limit_metadata_exhausted(&json!({
        "limit_reached": true
    })));
    assert!(codex_rate_limit_metadata_exhausted(&json!({
        "primary_used_percent": 100
    })));
    assert!(!codex_rate_limit_metadata_exhausted(&json!({
        "allowed": true,
        "primary_used_percent": 100
    })));
    assert!(!codex_rate_limit_metadata_exhausted(&json!({
        "limit_reached": false,
        "secondary_used_percent": 100
    })));
}

#[test]
fn parses_codex_reset_credit_count_from_wham_usage() {
    let parsed = parse_codex_wham_usage_response(
        &json!({
            "plan_type": "plus",
            "rate_limit": {
                "primary_window": {
                    "used_percent": 25.0,
                    "reset_after_seconds": 604800
                }
            },
            "rate_limit_reset_credits": {
                "available_count": 2
            }
        }),
        1_777_000_000,
    )
    .expect("codex wham usage should parse");

    assert_eq!(
        parsed.pointer("/reset_credits/available_count"),
        Some(&json!(2u64))
    );
    assert_eq!(
        parsed.pointer("/reset_credits/detail_status"),
        Some(&json!("not_requested"))
    );
    assert_eq!(
        parsed.pointer("/reset_credits/updated_at"),
        Some(&json!(1_777_000_000u64))
    );
}

#[test]
fn parses_codex_reset_credit_detail_sorted_by_expiry() {
    let parsed = parse_codex_wham_reset_credits_detail_response(
        &json!({
            "credits": [
                {
                    "idempotencyKey": "bbbbbbbb-1111-2222-3333-444444444444",
                    "status": "available",
                    "expiresAt": "2030-01-04T00:00:00Z"
                },
                {
                    "idempotencyKey": "aaaaaaaa-1111-2222-3333-444444444444",
                    "status": "available",
                    "grantedAt": 1_893_456_000_000u64,
                    "expiresAt": "2030-01-02T00:00:00Z"
                }
            ]
        }),
        1_777_000_000,
    )
    .expect("detail should parse");

    assert_eq!(
        parsed.pointer("/reset_credits/detail_status"),
        Some(&json!("available"))
    );
    assert_eq!(
        parsed.pointer("/reset_credits/credits/0/display_key"),
        Some(&json!("aaaaaaaa"))
    );
    assert_eq!(
        parsed.pointer("/reset_credits/credits/0/granted_at"),
        Some(&json!(1_893_456_000u64))
    );
    assert_eq!(
        parsed.pointer("/reset_credits/credits/1/display_key"),
        Some(&json!("bbbbbbbb"))
    );
}

#[test]
fn parses_codex_reset_credit_detail_without_explicit_count_or_ids() {
    let parsed = parse_codex_wham_reset_credits_detail_response(
        &json!({
            "rate_limit_reset_credits": [
                {
                    "resetType": "codex_rate_limits",
                    "status": "available",
                    "expiresAt": "2030-01-02T00:00:00Z"
                },
                {
                    "reset_type": "codex_rate_limits",
                    "status": "available"
                },
                {
                    "reset_type": "codex_rate_limits",
                    "status": "redeemed",
                    "expires_at": "2030-01-03T00:00:00Z"
                }
            ]
        }),
        1_777_000_000,
    )
    .expect("detail array should parse");

    assert_eq!(
        parsed.pointer("/reset_credits/available_count"),
        Some(&json!(2u64))
    );
    assert_eq!(
        parsed.pointer("/reset_credits/credits/0/expires_at"),
        Some(&json!(1_893_542_400u64))
    );
    assert_eq!(parsed.pointer("/reset_credits/credits/0/id"), None);
}

#[test]
fn parses_codex_reset_credit_detail_from_top_level_array() {
    let parsed = parse_codex_wham_reset_credits_detail_response(
        &json!([
            {
                "status": "available",
                "expires_at": "2030-01-04T00:00:00Z"
            }
        ]),
        1_777_000_000,
    )
    .expect("top-level detail array should parse");

    assert_eq!(
        parsed.pointer("/reset_credits/available_count"),
        Some(&json!(1u64))
    );
}

#[test]
fn normalizes_codex_reset_credit_consume_outcome() {
    assert_eq!(
        normalize_codex_reset_credit_consume_outcome(Some(&json!({
            "outcome": "alreadyRedeemed"
        }))),
        Some("already_redeemed".to_string())
    );
    assert_eq!(
        normalize_codex_reset_credit_consume_outcome(Some(&json!({
            "noCredit": true
        }))),
        Some("no_credit".to_string())
    );
}

#[test]
fn parses_codex_backend_me_identity_metadata_without_quota_windows() {
    let parsed = parse_codex_backend_me_response(
        &json!({
            "user": {
                "id": "user-codex-123",
                "email": "codex@example.com",
                "name": "Codex User"
            },
            "account": {
                "id": "acct-codex-123",
                "name": "Personal",
                "plan_type": "plus"
            },
            "plan": {
                "type": "Plus",
                "title": "ChatGPT Plus"
            }
        }),
        1_777_000_000,
    )
    .expect("codex backend me should parse");

    assert_eq!(parsed.get("user_id"), Some(&json!("user-codex-123")));
    assert_eq!(parsed.get("email"), Some(&json!("codex@example.com")));
    assert_eq!(parsed.get("account_id"), Some(&json!("acct-codex-123")));
    assert_eq!(parsed.get("account_name"), Some(&json!("Personal")));
    assert_eq!(parsed.get("plan_type"), Some(&json!("plus")));
    assert_eq!(parsed.get("plan_title"), Some(&json!("ChatGPT Plus")));
    assert_eq!(parsed.get("updated_at"), Some(&json!(1_777_000_000u64)));
    assert!(parsed.get("primary_used_percent").is_none());
    assert!(parsed.get("secondary_used_percent").is_none());
}

#[test]
fn parses_antigravity_usage_response_labels_opaque_reset_credit_keys() {
    let parsed = parse_antigravity_usage_response(
        &json!({
            "models": {
                "RateLimitResetCredit_05cbb6eeeb9c81918e011d8300f9ebfb": {
                    "quotaInfo": {
                        "remainingFraction": 0.75,
                        "resetTime": "2030-01-01T00:00:00Z"
                    }
                },
                "gemini-3-pro-preview": {
                    "displayName": "Gemini 3 Pro Preview",
                    "quotaInfo": {
                        "remainingFraction": 0.25
                    }
                }
            }
        }),
        1_777_000_000,
    )
    .expect("antigravity quota should parse");

    assert_eq!(
        parsed["quota_by_model"]["RateLimitResetCredit_05cbb6eeeb9c81918e011d8300f9ebfb"]
            ["display_name"],
        json!("Key-1")
    );
    assert_eq!(
        parsed["quota_by_model"]["RateLimitResetCredit_05cbb6eeeb9c81918e011d8300f9ebfb"]
            ["reset_time"],
        json!("2030-01-01T00:00:00Z")
    );
    assert_eq!(
        parsed["quota_by_model"]["gemini-3-pro-preview"]["display_name"],
        json!("Gemini 3 Pro Preview")
    );
}

#[test]
fn parses_antigravity_grouped_weekly_and_five_hour_quota() {
    let groups = parse_antigravity_quota_summary_response(&json!({
        "groups": [{
            "displayName": "Claude and GPT models",
            "description": "Shared quota",
            "buckets": [{
                "bucketId": "3p-5h",
                "window": "5h",
                "remainingFraction": 0.25,
                "resetTime": "2026-05-05T05:00:00Z",
                "displayName": "5 hour"
            }, {
                "bucketId": "3p-weekly",
                "window": "weekly",
                "remainingFraction": 0.8,
                "resetTime": "2026-05-11T00:00:00Z"
            }]
        }]
    }))
    .expect("grouped Antigravity quota should parse");

    assert_eq!(groups[0]["display_name"], json!("Claude and GPT models"));
    assert_eq!(groups[0]["description"], json!("Shared quota"));
    assert_eq!(groups[0]["buckets"][0]["bucket_id"], json!("3p-5h"));
    assert_eq!(groups[0]["buckets"][0]["window"], json!("5h"));
    assert_eq!(groups[0]["buckets"][0]["remaining_fraction"], json!(0.25));
    assert_eq!(groups[0]["buckets"][0]["used_percent"], json!(75.0));
    assert_eq!(groups[0]["buckets"][1]["bucket_id"], json!("3p-weekly"));
}

#[test]
fn parses_gemini_cli_retrieve_user_quota_buckets() {
    let parsed = parse_gemini_cli_retrieve_user_quota_response(
        &json!({
            "buckets": [
                {
                    "modelId": "gemini-2.5-pro",
                    "tokenType": "model",
                    "displayName": "Gemini 2.5 Pro",
                    "remainingFraction": 0.25,
                    "remainingAmount": "25",
                    "resetTime": "2030-01-01T00:00:00Z",
                    "isExhausted": false
                },
                {
                    "modelId": "gemini-2.5-flash",
                    "tokenType": "model",
                    "displayName": "Gemini 2.5 Flash",
                    "quotaInfo": {
                        "remainingFraction": 0.0,
                        "resetTime": 1_893_459_600_000u64,
                        "isExhausted": true
                    }
                }
            ]
        }),
        1_777_000_000,
    )
    .expect("gemini cli quota should parse");

    assert_eq!(parsed.get("updated_at"), Some(&json!(1_777_000_000u64)));
    assert_eq!(
        parsed["quota_by_model"]["gemini-2.5-pro"]["remaining_fraction"],
        json!(0.25)
    );
    assert_eq!(
        parsed["quota_by_model"]["gemini-2.5-pro"]["remaining"],
        json!(25.0)
    );
    assert_eq!(
        parsed["quota_by_model"]["gemini-2.5-pro"]["total"],
        json!(100.0)
    );
    assert_eq!(
        parsed["quota_by_model"]["gemini-2.5-pro"]["reset_at"],
        json!(1_893_456_000u64)
    );
    assert_eq!(
        parsed["quota_by_model"]["gemini-2.5-flash"]["is_exhausted"],
        json!(true)
    );
    assert_eq!(
        parsed["quota_by_model"]["gemini-2.5-flash"]["used_percent"],
        json!(100.0)
    );
}

#[test]
fn parses_gemini_cli_quota_buckets_labels_opaque_reset_credit_keys() {
    let parsed = parse_gemini_cli_retrieve_user_quota_response(
        &json!({
            "buckets": [
                {
                    "tokenType": "RateLimitResetCredit_05cbb6eeeb9c81918e011d8300f9ebfb",
                    "remainingFraction": 0.5
                },
                {
                    "modelId": "RateLimitResetCredit_d18b8aac4ec2472697ad747a14975ac8",
                    "displayName": "RateLimitResetCredit_d18b8aac4ec2472697ad747a14975ac8",
                    "remainingFraction": 0.25
                }
            ]
        }),
        1_777_000_000,
    )
    .expect("gemini cli quota should parse");

    assert_eq!(
        parsed["quota_by_model"]["RateLimitResetCredit_05cbb6eeeb9c81918e011d8300f9ebfb"]
            ["display_name"],
        json!("Key-1")
    );
    assert_eq!(
        parsed["quota_by_model"]["RateLimitResetCredit_d18b8aac4ec2472697ad747a14975ac8"]
            ["display_name"],
        json!("Key-2")
    );
}

#[test]
fn parses_gemini_cli_v1internal_credits() {
    let parsed = parse_gemini_cli_v1internal_credits_response(
        &json!({
            "response": {"candidates": []},
            "remainingCredits": "41.5",
            "consumedCredits": 1,
            "traceId": "trace-upstream-sync-1"
        }),
        1_777_000_123,
    )
    .expect("gemini cli credits should parse");

    assert_eq!(parsed.get("remaining"), Some(&json!(41.5)));
    assert_eq!(parsed.get("consumed"), Some(&json!(1.0)));
    assert_eq!(
        parsed.get("trace_id"),
        Some(&json!("trace-upstream-sync-1"))
    );
    assert_eq!(parsed.get("updated_at"), Some(&json!(1_777_000_123u64)));
}

#[test]
fn parses_windsurf_user_status_response() {
    let parsed = parse_windsurf_user_status_response(
        &json!({
            "userStatus": {
                "email": "windsurf@example.com",
                "isQuarantined": true,
                "quarantineReason": "quota review",
                "planStatus": {
                    "dailyQuotaRemainingPercent": 45.5,
                    "weeklyQuotaRemainingPercent": 80,
                    "dailyQuotaResetAtUnix": "1775553285",
                    "weeklyQuotaResetAtUnix": 1776158085u64,
                    "availablePromptCredits": 900,
                    "usedPromptCredits": 100,
                    "availableFlexCredits": 250,
                    "usedFlexCredits": 50,
                    "overageBalanceMicros": 1250000,
                    "planInfo": {
                        "planName": "Pro",
                        "monthlyPromptCredits": 1000,
                        "monthlyFlexCreditPurchaseAmount": 300
                    }
                }
            }
        }),
        1_770_000_000,
    )
    .expect("windsurf user status should parse");

    assert_eq!(parsed.get("plan_name"), Some(&json!("Pro")));
    assert_eq!(parsed.get("daily_remaining_percent"), Some(&json!(45.5)));
    assert_eq!(parsed.get("weekly_remaining_percent"), Some(&json!(80.0)));
    assert_eq!(parsed.get("daily_reset_at"), Some(&json!(1_775_553_285u64)));
    assert_eq!(
        parsed.get("weekly_reset_at"),
        Some(&json!(1_776_158_085u64))
    );
    assert_eq!(parsed.get("prompt_remaining"), Some(&json!(9.0)));
    assert_eq!(parsed.get("prompt_used"), Some(&json!(1.0)));
    assert_eq!(parsed.get("prompt_limit"), Some(&json!(10.0)));
    assert_eq!(parsed.get("flex_remaining"), Some(&json!(2.5)));
    assert_eq!(parsed.get("flex_used"), Some(&json!(0.5)));
    assert_eq!(parsed.get("flex_limit"), Some(&json!(3.0)));
    assert_eq!(parsed.get("overage_balance"), Some(&json!(1.25)));
    assert_eq!(parsed.get("email"), Some(&json!("windsurf@example.com")));
    assert_eq!(parsed.get("quarantined"), Some(&json!(true)));
    assert_eq!(
        parsed.get("quarantine_reason"),
        Some(&json!("Windsurf account is quarantined"))
    );
    assert_eq!(parsed.get("updated_at"), Some(&json!(1_770_000_000u64)));
}

#[test]
fn windsurf_status_parser_never_persists_upstream_reason_or_message() {
    let parsed = parse_windsurf_user_status_response(
        &json!({
            "userStatus": {
                "isBanned": true,
                "reason": "Authorization: Bearer upstream-secret",
                "message": "https://user:password@internal.test?q=secret",
                "planStatus": {"dailyQuotaRemainingPercent": 10}
            }
        }),
        1_770_000_000,
    )
    .expect("windsurf status should parse");

    assert_eq!(
        parsed.get("ban_reason"),
        Some(&json!("Windsurf account is suspended"))
    );
    let serialized = parsed.to_string();
    assert!(!serialized.contains("upstream-secret"));
    assert!(!serialized.contains("user:password"));
    assert!(!serialized.contains("q=secret"));
}

#[test]
fn parses_windsurf_model_configs_response() {
    let parsed = parse_windsurf_model_configs_response(
        &json!({
            "clientModelConfigs": [
                {
                    "modelUid": "claude-sonnet-4-5",
                    "label": "Claude Sonnet 4.5",
                    "provider": "anthropic",
                    "supportsImages": true,
                    "creditMultiplier": 2
                },
                {
                    "modelUid": "gpt-5-mini",
                    "label": "GPT-5 mini"
                }
            ],
            "defaultOverrideModelConfig": {
                "modelUid": "claude-sonnet-4-5"
            }
        }),
        1_770_000_100,
    )
    .expect("windsurf model configs should parse");

    assert_eq!(parsed.get("allowed_models_count"), Some(&json!(2u64)));
    assert_eq!(
        parsed.get("default_model_uid"),
        Some(&json!("claude-sonnet-4-5"))
    );
    assert_eq!(parsed.get("updated_at"), Some(&json!(1_770_000_100u64)));
}

#[test]
fn parses_windsurf_rate_limit_response() {
    let parsed = parse_windsurf_rate_limit_response(
        &json!({
            "hasCapacity": false,
            "messagesRemaining": 0,
            "maxMessages": 25,
            "retryAfterMs": 45000
        }),
        1_770_000_200,
    )
    .expect("windsurf rate limit should parse");

    assert_eq!(parsed.get("updated_at"), Some(&json!(1_770_000_200u64)));
    assert_eq!(parsed.pointer("/rate_limit/limited"), Some(&json!(true)));
    assert_eq!(
        parsed.pointer("/rate_limit/messages_remaining"),
        Some(&json!(0.0))
    );
    assert_eq!(
        parsed.pointer("/rate_limit/retry_after_ms"),
        Some(&json!(45000u64))
    );
}

#[test]
fn parses_chatgpt_web_image_quota_from_conversation_init() {
    let parsed = parse_chatgpt_web_conversation_init_response(
        &json!({
            "default_model_slug": "auto",
            "blocked_features": [],
            "limits_progress": [
                {
                    "feature_name": "image_gen",
                    "remaining": 24,
                    "reset_after": "2026-05-07T12:32:52.826482+00:00"
                }
            ]
        }),
        1_778_067_246,
    )
    .expect("chatgpt web quota should parse");

    assert_eq!(parsed.get("default_model_slug"), Some(&json!("auto")));
    assert_eq!(parsed.get("image_quota_remaining"), Some(&json!(24.0)));
    assert_eq!(
        parsed.get("image_quota_reset_at"),
        Some(&json!(1_778_157_172u64))
    );
}

#[test]
fn parses_chatgpt_web_blocked_image_feature_as_zero_remaining() {
    let parsed = parse_chatgpt_web_conversation_init_response(
        &json!({
            "blocked_features": [
                "image_generation",
                "Authorization: Bearer upstream-secret",
                "https://user:password@internal.test?q=secret"
            ],
            "limits_progress": []
        }),
        1_778_067_246,
    )
    .expect("blocked image feature should produce metadata");

    assert_eq!(parsed.get("image_quota_blocked"), Some(&json!(true)));
    assert_eq!(parsed.get("image_quota_remaining"), Some(&json!(0.0)));
    assert_eq!(
        parsed.get("blocked_features"),
        Some(&json!(["image_generation"]))
    );
    assert!(parsed.get("limits_progress").is_none());
    assert!(!parsed.to_string().contains("upstream-secret"));
}

#[test]
fn chatgpt_web_parser_projects_image_limit_scalars_only() {
    let parsed = parse_chatgpt_web_conversation_init_response(
        &json!({
            "limits_progress": [{
                "feature_name": "image_gen",
                "remaining": 8,
                "total": 12,
                "reset_after": "60",
                "message": "Authorization: Bearer upstream-secret",
                "nested": {
                    "url": "https://user:password@internal.test?q=secret"
                }
            }]
        }),
        1_778_067_246,
    )
    .expect("image quota should parse");

    assert_eq!(parsed.get("image_quota_remaining"), Some(&json!(8.0)));
    assert_eq!(parsed.get("image_quota_total"), Some(&json!(12.0)));
    assert_eq!(
        parsed.get("image_quota_reset_at"),
        Some(&json!(1_778_067_306u64))
    );
    assert!(parsed.get("limits_progress").is_none());
    assert!(parsed.get("image_quota_reset_after").is_none());
    let serialized = parsed.to_string();
    assert!(!serialized.contains("upstream-secret"));
    assert!(!serialized.contains("user:password"));
}
