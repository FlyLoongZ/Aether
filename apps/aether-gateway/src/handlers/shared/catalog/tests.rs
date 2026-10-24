use super::*;

fn sample_catalog_key() -> StoredProviderCatalogKey {
    let encrypted_api_key =
        encrypt_python_fernet_plaintext(DEVELOPMENT_ENCRYPTION_KEY, "sk-test-123")
            .expect("api key ciphertext should build");
    StoredProviderCatalogKey::new(
        "key-test".to_string(),
        "provider-test".to_string(),
        "default".to_string(),
        "api_key".to_string(),
        None,
        true,
    )
    .expect("key should build")
    .with_transport_fields(
        Some(json!(["openai:chat"])),
        encrypted_api_key,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
    )
    .expect("key transport should build")
}

#[test]
fn admin_provider_key_health_response_preserves_v0_7_13_defaults() {
    let state = AppState::new().expect("gateway should build");
    for (health, expected_score) in [
        (None, json!(1.0)),
        (Some(json!({})), json!(1.0)),
        (
            Some(json!({"openai:chat": {"consecutive_failures": 0}})),
            json!(1.0),
        ),
        (
            Some(json!({"openai:chat": {"health_score": 0.0}})),
            json!(0.0),
        ),
        (
            Some(json!({"openai:chat": {"health_score": 1.0}})),
            json!(1.0),
        ),
        (
            Some(json!({
                "openai:chat": {"health_score": 0.25},
                "openai:responses": {"health_score": 0.75},
            })),
            json!(0.25),
        ),
    ] {
        let mut key = sample_catalog_key();
        key.health_by_format = health;
        let payload = build_admin_provider_key_response(
            &state,
            &key,
            "openai",
            &["openai:chat".to_string()],
            1_000,
        );
        assert_eq!(payload["health_score"], expected_score);
    }
}

#[test]
fn responses_key_scope_covers_search_in_one_direction() {
    let mut responses_key = sample_catalog_key();
    responses_key.api_formats = Some(json!(["openai:responses"]));
    assert!(provider_catalog_key_supports_format(
        &responses_key,
        "codex",
        "openai:search",
    ));

    let mut search_key = sample_catalog_key();
    search_key.api_formats = Some(json!(["openai:search"]));
    assert!(!provider_catalog_key_supports_format(
        &search_key,
        "codex",
        "openai:responses",
    ));
}

#[test]
fn masked_catalog_api_key_handles_unicode_plaintext_without_panicking() {
    let state = AppState::new().expect("gateway should build");
    let encrypted_api_key =
        encrypt_python_fernet_plaintext(DEVELOPMENT_ENCRYPTION_KEY, "测试-密钥-1234567890")
            .expect("api key ciphertext should build");
    let key = StoredProviderCatalogKey::new(
        "key-unicode".to_string(),
        "provider-test".to_string(),
        "default".to_string(),
        "api_key".to_string(),
        None,
        true,
    )
    .expect("key should build")
    .with_transport_fields(
        Some(json!(["openai:chat"])),
        encrypted_api_key,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
    )
    .expect("key transport should build");

    let masked = masked_catalog_api_key(&state, &key);
    assert!(masked.contains("***"));
    assert_ne!(masked, "***ERROR***");
}

#[test]
fn masked_catalog_api_key_never_returns_a_complete_short_secret() {
    let state = AppState::new().expect("gateway should build");
    let plaintext = "sk-test-a";
    let encrypted_api_key = encrypt_python_fernet_plaintext(DEVELOPMENT_ENCRYPTION_KEY, plaintext)
        .expect("api key ciphertext should build");
    let key = StoredProviderCatalogKey::new(
        "key-short".to_string(),
        "provider-test".to_string(),
        "default".to_string(),
        "api_key".to_string(),
        None,
        true,
    )
    .expect("key should build")
    .with_transport_fields(
        Some(json!(["openai:chat"])),
        encrypted_api_key,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
    )
    .expect("key transport should build");

    let masked = masked_catalog_api_key(&state, &key);
    assert_ne!(masked, plaintext);
    assert!(!masked.contains(plaintext));
}

#[test]
fn provider_aware_mask_labels_agent_identity_without_exposing_placeholder() {
    let state = AppState::new().expect("gateway should build");
    let encrypted_placeholder =
        encrypt_python_fernet_plaintext(DEVELOPMENT_ENCRYPTION_KEY, "__placeholder__")
            .expect("placeholder ciphertext should build");
    let encrypted_auth_config = encrypt_python_fernet_plaintext(
        DEVELOPMENT_ENCRYPTION_KEY,
        r#"{"provider_type":"codex","auth_mode":"agentIdentity","agent_runtime_id":"runtime-1","agent_private_key":"base64-private-key","task_id":"task-1"}"#,
    )
    .expect("auth config ciphertext should build");
    let key = StoredProviderCatalogKey::new(
        "key-agent".to_string(),
        "provider-codex".to_string(),
        "agent".to_string(),
        "oauth".to_string(),
        None,
        true,
    )
    .expect("key should build")
    .with_transport_fields(
        Some(json!(["openai:responses"])),
        encrypted_placeholder,
        Some(encrypted_auth_config),
        None,
        None,
        None,
        None,
        None,
        None,
    )
    .expect("key transport should build");

    assert_eq!(
        masked_catalog_api_key_for_provider(&state, &key, "codex"),
        "[Agent Identity]"
    );
    assert!(!masked_catalog_api_key_for_provider(&state, &key, "codex").contains("placeholder"));
}

#[test]
fn provider_key_status_snapshot_payload_backfills_missing_quota_from_upstream_metadata() {
    let mut key = sample_catalog_key();
    key.upstream_metadata = Some(json!({
        "codex": {
            "updated_at": 1_775_553_285u64,
            "plan_type": "plus",
            "primary_used_percent": 55.0,
            "primary_reset_at": 1_900_000_000u64,
            "secondary_used_percent": 12.5,
            "secondary_reset_at": 1_900_500_000u64,
            "has_credits": true,
            "credits_balance": 42.0
        }
    }));

    let payload = provider_key_status_snapshot_payload(&key, "codex");
    let quota = payload
        .get("quota")
        .and_then(Value::as_object)
        .expect("quota snapshot should be object");

    assert_eq!(quota.get("provider_type"), Some(&json!("codex")));
    assert_eq!(quota.get("plan_type"), Some(&json!("plus")));
    assert_eq!(quota.get("updated_at"), Some(&json!(1_775_553_285u64)));
    assert_eq!(quota.get("reset_at"), Some(&json!(1_900_000_000u64)));
    assert_eq!(
        quota
            .get("credits")
            .and_then(Value::as_object)
            .and_then(|credits| credits.get("balance")),
        Some(&json!(42.0))
    );
    assert_eq!(
        quota.get("windows").and_then(Value::as_array).map(Vec::len),
        Some(2usize)
    );
}

#[test]
fn provider_key_status_snapshot_payload_backfills_codex_reset_credits() {
    let mut key = sample_catalog_key();
    key.upstream_metadata = Some(json!({
        "codex": {
            "updated_at": 1_775_553_285u64,
            "plan_type": "plus",
            "primary_used_percent": 55.0,
            "primary_reset_at": 1_900_000_000u64,
            "has_credits": true,
            "credits_balance": 42.0,
            "reset_credits": {
                "available_count": 2,
                "updated_at": 1_775_553_285u64,
                "detail_source": "wham_readonly",
                "detail_status": "available",
                "credits": [
                    {
                        "id": "bbbbbbbb-1111-2222-3333-444444444444",
                        "display_key": "bbbbbbbb",
                        "status": "available",
                        "expires_at": 1_775_900_000u64
                    },
                    {
                        "id": "aaaaaaaa-1111-2222-3333-444444444444",
                        "display_key": "aaaaaaaa",
                        "status": "available",
                        "expires_at": 1_775_700_000u64
                    }
                ]
            }
        }
    }));

    let payload = provider_key_status_snapshot_payload(&key, "codex");
    let quota = payload
        .get("quota")
        .and_then(Value::as_object)
        .expect("quota snapshot should be object");

    assert_eq!(quota.get("exhausted"), Some(&json!(false)));
    assert_eq!(
        payload.pointer("/quota/reset_credits/available_count"),
        Some(&json!(2u64))
    );
    assert_eq!(
        payload.pointer("/quota/reset_credits/credits/0/display_key"),
        Some(&json!("aaaaaaaa"))
    );
    assert_eq!(
        payload.pointer("/quota/reset_credits/credits/0/remaining_seconds"),
        Some(&json!(146_715u64))
    );
    assert_eq!(
        quota
            .get("credits")
            .and_then(Value::as_object)
            .and_then(|credits| credits.get("balance")),
        Some(&json!(42.0))
    );
}

#[test]
fn provider_key_status_snapshot_payload_backfills_codex_spark_windows() {
    let mut key = sample_catalog_key();
    key.upstream_metadata = Some(json!({
        "codex": {
            "updated_at": 1_775_553_285u64,
            "plan_type": "plus",
            "primary_used_percent": 55.0,
            "primary_reset_at": 1_900_000_000u64,
            "secondary_used_percent": 12.5,
            "secondary_reset_at": 1_900_500_000u64,
            "spark_primary_used_percent": 40.0,
            "spark_primary_reset_at": 1_900_100_000u64,
            "spark_primary_window_minutes": 300u64,
            "spark_secondary_used_percent": 5.0,
            "spark_secondary_reset_at": 1_900_600_000u64,
            "spark_secondary_window_minutes": 10_080u64
        }
    }));

    let payload = provider_key_status_snapshot_payload(&key, "codex");
    let windows = payload["quota"]["windows"]
        .as_array()
        .expect("quota windows should exist");
    let spark_5h = windows
        .iter()
        .filter_map(Value::as_object)
        .find(|window| window.get("code") == Some(&json!("spark_5h")))
        .expect("Spark 5H window should exist");
    let spark_weekly = windows
        .iter()
        .filter_map(Value::as_object)
        .find(|window| window.get("code") == Some(&json!("spark_weekly")))
        .expect("Spark weekly window should exist");

    assert_eq!(windows.len(), 4);
    assert_eq!(spark_5h.get("label"), Some(&json!("Spark 5H")));
    assert_eq!(spark_5h.get("remaining_ratio"), Some(&json!(0.6)));
    assert_eq!(spark_weekly.get("label"), Some(&json!("Spark 周")));
    assert_eq!(spark_weekly.get("remaining_ratio"), Some(&json!(0.95)));
}

#[test]
fn codex_wham_snapshot_does_not_duplicate_normalized_spark_windows() {
    let codex = admin_provider_quota_pure::parse_codex_wham_usage_response(
        &json!({
            "plan_type": "plus",
            "rate_limit": {
                "primary_window": {"used_percent": 25.0},
                "secondary_window": {"used_percent": 10.0}
            },
            "additional_rate_limits": [{
                "limit_name": "GPT-5.3-Codex-Spark",
                "rate_limit": {
                    "primary_window": {
                        "used_percent": 40.0,
                        "limit_window_seconds": 18_000
                    },
                    "secondary_window": {
                        "used_percent": 5.0,
                        "limit_window_seconds": 604_800
                    }
                }
            }]
        }),
        1_777_000_000,
    )
    .expect("Codex WHAM quota should parse");
    let upstream_metadata = json!({"codex": codex});
    let payload = sync_provider_key_quota_status_snapshot(
        None,
        "codex",
        Some(&upstream_metadata),
        "refresh_api",
    )
    .expect("Codex quota snapshot should sync");
    let windows = payload["quota"]["windows"]
        .as_array()
        .expect("quota windows should exist");
    let spark_codes = windows
        .iter()
        .filter_map(|window| window.get("code").and_then(Value::as_str))
        .filter(|code| code.starts_with("spark_"))
        .collect::<Vec<_>>();

    assert_eq!(spark_codes, vec!["spark_5h", "spark_weekly"]);
}

#[test]
fn provider_key_status_snapshot_payload_keeps_codex_free_window_quota_available() {
    let mut key = sample_catalog_key();
    key.upstream_metadata = Some(json!({
        "codex": {
            "updated_at": 1_775_553_285u64,
            "plan_type": "free",
            "primary_used_percent": 64.0,
            "primary_reset_at": 1_900_000_000u64,
            "secondary_used_percent": 3.0,
            "secondary_reset_at": 1_900_500_000u64,
            "has_credits": false,
            "credits_balance": 0.0,
            "credits_unlimited": false
        }
    }));

    let payload = provider_key_status_snapshot_payload(&key, "codex");
    let quota = payload
        .get("quota")
        .and_then(Value::as_object)
        .expect("quota snapshot should be object");

    assert_eq!(quota.get("code"), Some(&json!("ok")));
    assert_eq!(quota.get("exhausted"), Some(&json!(false)));
    assert_eq!(quota.get("usage_ratio"), Some(&json!(0.64)));
    assert_eq!(
        quota
            .get("credits")
            .and_then(Value::as_object)
            .and_then(|credits| credits.get("has_credits")),
        Some(&json!(false))
    );
    assert_eq!(
        quota.get("windows").and_then(Value::as_array).map(Vec::len),
        Some(2usize)
    );
}

#[test]
fn provider_key_status_snapshot_payload_derives_codex_reset_at_from_countdown() {
    let mut key = sample_catalog_key();
    key.upstream_metadata = Some(json!({
        "codex": {
            "updated_at": 1_775_553_285u64,
            "primary_used_percent": 55.0,
            "primary_reset_after_seconds": 3_600u64
        }
    }));

    let payload = provider_key_status_snapshot_payload(&key, "codex");
    let quota = payload
        .get("quota")
        .and_then(Value::as_object)
        .expect("quota snapshot should be object");
    let window = quota
        .get("windows")
        .and_then(Value::as_array)
        .and_then(|windows| windows.first())
        .and_then(Value::as_object)
        .expect("quota window should exist");

    assert_eq!(quota.get("reset_at"), Some(&json!(1_775_556_885u64)));
    assert_eq!(quota.get("reset_seconds"), Some(&json!(3_600u64)));
    assert_eq!(window.get("reset_at"), Some(&json!(1_775_556_885u64)));
    assert_eq!(window.get("reset_seconds"), Some(&json!(3_600u64)));
}

#[test]
fn provider_key_status_snapshot_payload_normalizes_expired_codex_quota_windows() {
    let mut key = sample_catalog_key();
    key.status_snapshot = Some(json!({
        "quota": {
            "version": 2,
            "provider_type": "codex",
            "code": "ok",
            "exhausted": false,
            "updated_at": 1_700_000_000u64,
            "windows": [
                {
                    "code": "5h",
                    "label": "5H",
                    "scope": "account",
                    "unit": "percent",
                    "used_ratio": 0.88,
                    "remaining_ratio": 0.12,
                    "reset_at": 1_700_003_600u64,
                    "window_minutes": 300
                },
                {
                    "code": "weekly",
                    "label": "周",
                    "scope": "account",
                    "unit": "percent",
                    "used_ratio": 0.5,
                    "remaining_ratio": 0.5,
                    "reset_at": 2_000_000_000u64,
                    "window_minutes": 10_080
                },
                {
                    "code": "spark_5h",
                    "label": "Spark 5H",
                    "scope": "account",
                    "unit": "percent",
                    "reset_at": 1_700_003_600u64,
                    "window_minutes": 300
                },
                {
                    "code": "unlimited",
                    "label": "无限",
                    "scope": "account",
                    "unit": "percent",
                    "used_ratio": 0.3,
                    "remaining_ratio": 0.7,
                    "reset_at": 1_700_003_600u64,
                    "window_minutes": 0
                }
            ]
        }
    }));

    let payload = provider_key_status_snapshot_payload(&key, "codex");

    // 已到期的窗口按“已重置”口径归一化：用量清零、剩余 100%。
    assert_eq!(payload.pointer("/quota/windows/0/code"), Some(&json!("5h")));
    assert_eq!(
        payload.pointer("/quota/windows/0/used_ratio"),
        Some(&json!(0.0))
    );
    assert_eq!(
        payload.pointer("/quota/windows/0/remaining_ratio"),
        Some(&json!(1.0))
    );
    // 未到期的窗口保留原始观测值。
    assert_eq!(
        payload.pointer("/quota/windows/1/code"),
        Some(&json!("weekly"))
    );
    assert_eq!(
        payload.pointer("/quota/windows/1/used_ratio"),
        Some(&json!(0.5))
    );
    assert_eq!(
        payload.pointer("/quota/windows/1/remaining_ratio"),
        Some(&json!(0.5))
    );
    // 没有用量观测的窗口不会被凭空补成 100%。
    assert_eq!(
        payload.pointer("/quota/windows/2/code"),
        Some(&json!("spark_5h"))
    );
    assert_eq!(payload.pointer("/quota/windows/2/used_ratio"), None);
    assert_eq!(payload.pointer("/quota/windows/2/remaining_ratio"), None);
    // window_minutes=0 不是真实配额窗口，与同类 Codex 窗口处理保持一致，不归一化。
    assert_eq!(
        payload.pointer("/quota/windows/3/code"),
        Some(&json!("unlimited"))
    );
    assert_eq!(
        payload.pointer("/quota/windows/3/used_ratio"),
        Some(&json!(0.3))
    );
    assert_eq!(
        payload.pointer("/quota/windows/3/remaining_ratio"),
        Some(&json!(0.7))
    );
}

#[test]
fn provider_key_status_snapshot_payload_clears_expired_codex_window_exhausted_state() {
    let mut key = sample_catalog_key();
    key.status_snapshot = Some(json!({
        "quota": {
            "version": 2,
            "provider_type": "codex",
            "code": "exhausted",
            "exhausted": true,
            "usage_ratio": 1.0,
            "updated_at": 1_700_000_000u64,
            "windows": [
                {
                    "code": "weekly",
                    "label": "周",
                    "scope": "account",
                    "unit": "percent",
                    "used_ratio": 1.0,
                    "remaining_ratio": 0.0,
                    "reset_at": 1_700_003_600u64,
                    "window_minutes": 10_080,
                    "is_exhausted": true
                }
            ]
        }
    }));

    let payload = provider_key_status_snapshot_payload(&key, "codex");

    // 汇总标志沿用既有调度口径（窗口已到期 → 不再视为耗尽）。
    assert_eq!(payload.pointer("/quota/exhausted"), Some(&json!(false)));
    assert_eq!(payload.pointer("/quota/code"), Some(&json!("ok")));
    // 窗口级耗尽标记与剩余比例同步归一化。
    assert_eq!(
        payload.pointer("/quota/windows/0/is_exhausted"),
        Some(&json!(false))
    );
    assert_eq!(
        payload.pointer("/quota/windows/0/used_ratio"),
        Some(&json!(0.0))
    );
    assert_eq!(
        payload.pointer("/quota/windows/0/remaining_ratio"),
        Some(&json!(1.0))
    );
}

#[test]
fn provider_key_status_snapshot_payload_normalizes_expired_quota_windows_for_other_providers() {
    let mut key = sample_catalog_key();
    key.status_snapshot = Some(json!({
        "quota": {
            "version": 2,
            "provider_type": "kiro",
            "code": "ok",
            "exhausted": false,
            "observed_at": 1_700_000_000u64,
            "updated_at": 1_700_000_000u64,
            "windows": [
                {
                    "code": "usage",
                    "label": "额度",
                    "scope": "account",
                    "unit": "count",
                    "used_ratio": 0.4,
                    "remaining_ratio": 0.6,
                    "used_value": 60.0,
                    "remaining_value": 90.0,
                    "limit_value": 150.0,
                    "reset_at": 1_700_003_600u64,
                    "reset_seconds": 3_600u64
                },
                {
                    "code": "usage_null_ratio",
                    "label": "额度",
                    "scope": "account",
                    "unit": "count",
                    "used_ratio": null,
                    "remaining_ratio": null,
                    "used_value": 60.0,
                    "remaining_value": 90.0,
                    "limit_value": 150.0,
                    "reset_at": 1_700_003_600u64,
                    "reset_seconds": 3_600u64
                },
                {
                    "code": "usage_active",
                    "label": "额度",
                    "scope": "account",
                    "unit": "count",
                    "used_ratio": 0.4,
                    "remaining_ratio": 0.6,
                    "used_value": 60.0,
                    "remaining_value": 90.0,
                    "limit_value": 150.0,
                    "reset_at": 2_000_000_000u64,
                    "reset_seconds": 300_000_000u64
                },
                {
                    "code": "usage_no_observation",
                    "label": "额度",
                    "scope": "account",
                    "unit": "count",
                    "reset_at": 1_700_003_600u64,
                    "reset_seconds": 3_600u64
                },
                {
                    "code": "usage_no_deadline",
                    "label": "额度",
                    "scope": "account",
                    "unit": "count",
                    "used_ratio": 0.4,
                    "remaining_ratio": 0.6,
                    "used_value": 60.0,
                    "remaining_value": 90.0,
                    "limit_value": 150.0
                },
                {
                    "code": "usage_remaining_without_limit",
                    "label": "额度",
                    "scope": "account",
                    "unit": "count",
                    "used_ratio": null,
                    "remaining_ratio": null,
                    "remaining_value": 0.0,
                    "reset_at": 1_700_003_600u64,
                    "reset_seconds": 3_600u64
                }
            ]
        }
    }));

    let payload = provider_key_status_snapshot_payload(&key, "kiro");

    // 已到期窗口：比例与数值一起按“已重置”归一化（不再只覆盖 Codex）。
    assert_eq!(
        payload.pointer("/quota/windows/0/used_ratio"),
        Some(&json!(0.0))
    );
    assert_eq!(
        payload.pointer("/quota/windows/0/remaining_ratio"),
        Some(&json!(1.0))
    );
    assert_eq!(
        payload.pointer("/quota/windows/0/used_value"),
        Some(&json!(0.0))
    );
    assert_eq!(
        payload.pointer("/quota/windows/0/remaining_value"),
        Some(&json!(150.0))
    );
    // 比例字段为 null 时补齐为已重置口径，保证优先读比例的展示实现一致。
    assert_eq!(
        payload.pointer("/quota/windows/1/used_ratio"),
        Some(&json!(0.0))
    );
    assert_eq!(
        payload.pointer("/quota/windows/1/remaining_ratio"),
        Some(&json!(1.0))
    );
    assert_eq!(
        payload.pointer("/quota/windows/1/used_value"),
        Some(&json!(0.0))
    );
    assert_eq!(
        payload.pointer("/quota/windows/1/remaining_value"),
        Some(&json!(150.0))
    );
    // 未到期窗口保留原始观测值。
    assert_eq!(
        payload.pointer("/quota/windows/2/used_ratio"),
        Some(&json!(0.4))
    );
    assert_eq!(
        payload.pointer("/quota/windows/2/remaining_value"),
        Some(&json!(90.0))
    );
    // 没有用量观测的窗口不会被凭空补成 100%。
    assert_eq!(payload.pointer("/quota/windows/3/used_ratio"), None);
    assert_eq!(payload.pointer("/quota/windows/3/remaining_ratio"), None);
    // 没有重置时间的窗口无法判定是否已重置，保持原样。
    assert_eq!(
        payload.pointer("/quota/windows/4/used_ratio"),
        Some(&json!(0.4))
    );
    assert_eq!(
        payload.pointer("/quota/windows/4/remaining_ratio"),
        Some(&json!(0.6))
    );
    // 只有 remaining_value 却没有上限的窗口观测不完整，跳过归一化，
    // 避免比例已恢复 100% 而数值仍停留在旧值。
    assert_eq!(
        payload.pointer("/quota/windows/5/remaining_ratio"),
        Some(&json!(null))
    );
    assert_eq!(
        payload.pointer("/quota/windows/5/remaining_value"),
        Some(&json!(0.0))
    );
}

#[test]
fn provider_key_status_snapshot_payload_clears_expired_quota_exhausted_state_for_other_providers() {
    let mut key = sample_catalog_key();
    key.status_snapshot = Some(json!({
        "quota": {
            "version": 2,
            "provider_type": "kiro",
            "code": "exhausted",
            "label": "额度耗尽",
            "reason": "额度已耗尽",
            "exhausted": true,
            "usage_ratio": 1.0,
            "observed_at": 1_700_000_000u64,
            "updated_at": 1_700_000_000u64,
            "windows": [
                {
                    "code": "usage",
                    "label": "额度",
                    "scope": "account",
                    "unit": "count",
                    "used_ratio": 1.0,
                    "remaining_ratio": 0.0,
                    "used_value": 150.0,
                    "remaining_value": 0.0,
                    "limit_value": 150.0,
                    "reset_at": 1_700_003_600u64,
                    "reset_seconds": 3_600u64,
                    "is_exhausted": true
                }
            ]
        }
    }));

    let payload = provider_key_status_snapshot_payload(&key, "kiro");

    // 汇总状态沿用调度口径（窗口已到期 → 不再视为耗尽），对所有提供商生效。
    assert_eq!(payload.pointer("/quota/exhausted"), Some(&json!(false)));
    assert_eq!(payload.pointer("/quota/code"), Some(&json!("ok")));
    // 窗口比例、数值与耗尽标记同步归一化。
    assert_eq!(
        payload.pointer("/quota/windows/0/is_exhausted"),
        Some(&json!(false))
    );
    assert_eq!(
        payload.pointer("/quota/windows/0/used_ratio"),
        Some(&json!(0.0))
    );
    assert_eq!(
        payload.pointer("/quota/windows/0/remaining_ratio"),
        Some(&json!(1.0))
    );
    assert_eq!(
        payload.pointer("/quota/windows/0/used_value"),
        Some(&json!(0.0))
    );
    assert_eq!(
        payload.pointer("/quota/windows/0/remaining_value"),
        Some(&json!(150.0))
    );
}

#[test]
fn provider_key_status_snapshot_payload_keeps_active_quota_exhausted_state_for_other_providers() {
    let mut key = sample_catalog_key();
    key.status_snapshot = Some(json!({
        "quota": {
            "version": 2,
            "provider_type": "kiro",
            "code": "exhausted",
            "label": "额度耗尽",
            "exhausted": true,
            "usage_ratio": 1.0,
            "observed_at": 1_700_000_000u64,
            "updated_at": 1_700_000_000u64,
            "windows": [
                {
                    "code": "usage",
                    "label": "额度",
                    "scope": "account",
                    "unit": "count",
                    "used_ratio": 1.0,
                    "remaining_ratio": 0.0,
                    "used_value": 150.0,
                    "remaining_value": 0.0,
                    "limit_value": 150.0,
                    "reset_at": 2_000_000_000u64,
                    "reset_seconds": 300_000_000u64,
                    "is_exhausted": true
                }
            ]
        }
    }));

    let payload = provider_key_status_snapshot_payload(&key, "kiro");

    // 未到期的耗尽状态保留，不能被读取层提前“恢复”。
    assert_eq!(payload.pointer("/quota/exhausted"), Some(&json!(true)));
    assert_eq!(payload.pointer("/quota/code"), Some(&json!("exhausted")));
    assert_eq!(
        payload.pointer("/quota/windows/0/is_exhausted"),
        Some(&json!(true))
    );
    assert_eq!(
        payload.pointer("/quota/windows/0/used_ratio"),
        Some(&json!(1.0))
    );
    assert_eq!(
        payload.pointer("/quota/windows/0/remaining_ratio"),
        Some(&json!(0.0))
    );
}

#[test]
fn provider_key_status_snapshot_payload_backfills_chatgpt_web_image_quota() {
    let mut key = sample_catalog_key();
    key.upstream_metadata = Some(json!({
        "chatgpt_web": {
            "updated_at": 1_778_067_246u64,
            "plan_type": "free",
            "image_quota_remaining": 24.0,
            "image_quota_reset_at": 1_778_157_172u64
        }
    }));

    let payload = provider_key_status_snapshot_payload(&key, "chatgpt_web");
    let quota = payload
        .get("quota")
        .and_then(Value::as_object)
        .expect("quota snapshot should be object");
    let window = quota
        .get("windows")
        .and_then(Value::as_array)
        .and_then(|windows| windows.first())
        .and_then(Value::as_object)
        .expect("image quota window should exist");

    assert_eq!(quota.get("provider_type"), Some(&json!("chatgpt_web")));
    assert_eq!(quota.get("code"), Some(&json!("ok")));
    assert_eq!(quota.get("plan_type"), Some(&json!("free")));
    assert_eq!(quota.get("reset_at"), Some(&json!(1_778_157_172u64)));
    assert_eq!(quota.get("usage_ratio"), Some(&json!(0.0)));
    assert_eq!(window.get("code"), Some(&json!("image_gen")));
    assert_eq!(window.get("remaining_value"), Some(&json!(24.0)));
    assert_eq!(window.get("limit_value"), Some(&json!(24.0)));
    assert_eq!(window.get("used_value"), Some(&json!(0.0)));
    assert_eq!(window.get("remaining_ratio"), Some(&json!(1.0)));
}

#[test]
fn provider_key_status_snapshot_payload_ignores_chatgpt_web_legacy_free_25_limit() {
    let mut key = sample_catalog_key();
    key.upstream_metadata = Some(json!({
        "chatgpt_web": {
            "updated_at": 1_778_067_246u64,
            "plan_type": "free",
            "image_quota_remaining": 19.0,
            "image_quota_total": 25.0
        }
    }));

    let payload = provider_key_status_snapshot_payload(&key, "chatgpt_web");
    let window = payload
        .get("quota")
        .and_then(Value::as_object)
        .and_then(|quota| quota.get("windows"))
        .and_then(Value::as_array)
        .and_then(|windows| windows.first())
        .and_then(Value::as_object)
        .expect("image quota window should exist");

    assert_eq!(window.get("remaining_value"), Some(&json!(19.0)));
    assert_eq!(window.get("limit_value"), Some(&json!(19.0)));
    assert_eq!(window.get("used_value"), Some(&json!(0.0)));
}

#[test]
fn provider_key_status_snapshot_payload_backfills_claude_code_usage_windows() {
    let observed_at = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("current time should be after epoch")
        .as_secs();
    let mut key = sample_catalog_key();
    key.upstream_metadata = Some(json!({
        "claude_code": {
            "updated_at": observed_at,
            "five_hour_used_percent": 100.0,
            "five_hour_reset_at": observed_at + 3_600,
            "seven_day_used_percent": 40.0,
            "seven_day_reset_at": observed_at + 400_000,
            "seven_day_sonnet_used_percent": 10.0,
            "seven_day_sonnet_reset_at": observed_at + 400_000,
            "reset_credits": {
                "available_count": 2,
                "updated_at": observed_at,
                "detail_source": "claude_oauth_usage",
                "credits": [{
                    "display_key": "Key-1",
                    "status": "available",
                    "expires_at": observed_at + 144_000
                }]
            }
        }
    }));

    let payload = provider_key_status_snapshot_payload(&key, "claude_code");
    let quota = payload
        .get("quota")
        .and_then(Value::as_object)
        .expect("quota snapshot should be object");
    assert_eq!(quota.get("provider_type"), Some(&json!("claude_code")));
    // An exhausted 5h window blocks the whole account until it resets.
    assert_eq!(quota.get("exhausted"), Some(&json!(true)));
    assert_eq!(quota.get("code"), Some(&json!("exhausted")));
    assert_eq!(quota.get("reset_at"), Some(&json!(observed_at + 3_600)));
    let windows = quota
        .get("windows")
        .and_then(Value::as_array)
        .expect("windows should exist");
    assert_eq!(windows.len(), 3);
    assert_eq!(windows[0]["code"], json!("5h"));
    assert_eq!(windows[0]["scope"], json!("account"));
    assert_eq!(windows[0]["window_minutes"], json!(300));
    assert_eq!(windows[0]["is_exhausted"], json!(true));
    assert_eq!(windows[1]["code"], json!("weekly"));
    assert_eq!(windows[1]["used_ratio"], json!(0.4));
    assert_eq!(windows[2]["code"], json!("weekly_sonnet"));
    assert_eq!(windows[2]["scope"], json!("model"));
    assert_eq!(quota["reset_credits"]["available_count"], json!(2));
    assert_eq!(
        quota["reset_credits"]["credits"][0]["remaining_seconds"],
        json!(144_000u64)
    );

    // An incomplete stored summary must not override the backfilled usage.
    key.status_snapshot = Some(json!({
        "quota": {
            "provider_type": "claude_code",
            "code": "ok",
            "exhausted": false,
            "updated_at": observed_at - 3_600,
            "windows": []
        }
    }));
    let payload = provider_key_status_snapshot_payload(&key, "claude_code");
    assert_eq!(payload["quota"]["exhausted"], json!(true));
    assert_eq!(payload["quota"]["code"], json!("exhausted"));
    assert_eq!(payload["quota"]["windows"][0]["is_exhausted"], json!(true));
}

#[test]
fn provider_key_status_snapshot_payload_clears_expired_backfilled_claude_code_usage() {
    let observed_at = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("current time should be after epoch")
        .as_secs()
        - 7_200;
    let mut key = sample_catalog_key();
    key.upstream_metadata = Some(json!({
        "claude_code": {
            "updated_at": observed_at,
            "five_hour_used_percent": 100.0,
            "five_hour_reset_at": observed_at + 3_600,
            "seven_day_used_percent": 40.0,
            "seven_day_reset_at": observed_at + 400_000
        }
    }));

    let payload = provider_key_status_snapshot_payload(&key, "claude_code");
    let quota = &payload["quota"];
    assert_eq!(quota["exhausted"], json!(false));
    assert_eq!(quota["code"], json!("ok"));
    assert_eq!(quota["windows"][0]["is_exhausted"], json!(false));
    assert_eq!(quota["windows"][0]["used_ratio"], json!(0.0));
    assert_eq!(quota["windows"][0]["remaining_ratio"], json!(1.0));
    assert_eq!(quota["windows"][1]["used_ratio"], json!(0.4));
}

#[test]
fn provider_key_status_snapshot_payload_backfills_grok_model_quota() {
    let mut key = sample_catalog_key();
    key.upstream_metadata = Some(json!({
        "grok": {
            "updated_at": 1_778_067_246u64,
            "pool_tier": "heavy",
            "plan_type": "heavy",
            "quota_by_model": {
                "quota_auto": {
                    "display_name": "auto",
                    "remaining_fraction": 0.4,
                    "used_percent": 60.0,
                    "remaining": 60.0,
                    "total": 150.0,
                    "reset_at": 2_000_000_000u64,
                    "is_exhausted": false
                },
                "quota_heavy": {
                    "display_name": "heavy",
                    "remaining_fraction": 0.0,
                    "used_percent": 100.0,
                    "reset_at": 2_000_000_000u64,
                    "is_exhausted": true
                }
            }
        }
    }));

    let payload = provider_key_status_snapshot_payload(&key, "grok");
    let quota = payload
        .get("quota")
        .and_then(Value::as_object)
        .expect("quota snapshot should be object");
    let windows = quota
        .get("windows")
        .and_then(Value::as_array)
        .expect("grok quota windows should exist");

    assert_eq!(quota.get("provider_type"), Some(&json!("grok")));
    assert_eq!(quota.get("code"), Some(&json!("ok")));
    assert_eq!(quota.get("plan_type"), Some(&json!("heavy")));
    assert_eq!(quota.get("pool_tier"), Some(&json!("heavy")));
    assert_eq!(quota.get("exhausted"), Some(&json!(false)));
    assert_eq!(quota.get("usage_ratio"), Some(&json!(1.0)));
    assert_eq!(quota.get("reset_at"), Some(&json!(2_000_000_000u64)));
    assert_eq!(windows.len(), 2);
    assert!(windows.iter().any(|window| {
        window
            .get("code")
            .and_then(Value::as_str)
            .is_some_and(|code| code == "model:quota_auto")
    }));
    let auto = windows
        .iter()
        .filter_map(Value::as_object)
        .find(|window| window.get("code") == Some(&json!("model:quota_auto")))
        .expect("auto quota window should exist");
    assert_eq!(auto.get("remaining_value"), Some(&json!(60.0)));
    assert_eq!(auto.get("limit_value"), Some(&json!(150.0)));
    assert_eq!(auto.get("used_value"), Some(&json!(90.0)));
}

#[test]
fn provider_key_status_snapshot_payload_backfills_xai_weekly_credits() {
    let mut key = sample_catalog_key();
    key.upstream_metadata = Some(json!({
        "xai": {
            "updated_at": 1_778_067_246u64,
            "usage_percentage": 46.0,
            "period_type": "weekly",
            "next_reset_at": 1_778_157_172u64,
            "subscription_title": "SuperGrok",
            "prepaid_balance": 0.0,
            "on_demand_cap": 0.0,
            "on_demand_used": 0.0
        }
    }));

    let payload = provider_key_status_snapshot_payload(&key, "xai");
    let quota = payload
        .get("quota")
        .and_then(Value::as_object)
        .expect("quota snapshot should be object");
    let windows = quota
        .get("windows")
        .and_then(Value::as_array)
        .expect("xai quota windows should exist");

    assert_eq!(quota.get("provider_type"), Some(&json!("xai")));
    assert_eq!(quota.get("code"), Some(&json!("ok")));
    assert_eq!(quota.get("exhausted"), Some(&json!(false)));
    assert_eq!(quota.get("plan_type"), Some(&json!("SuperGrok")));
    assert_eq!(quota.get("usage_ratio"), Some(&json!(0.46)));
    assert_eq!(quota.get("reset_at"), Some(&json!(1_778_157_172u64)));
    assert_eq!(windows.len(), 1);
    assert_eq!(windows[0].get("code"), Some(&json!("usage")));
    assert_eq!(windows[0].get("label"), Some(&json!("周额度")));
}

#[test]
fn provider_key_status_snapshot_payload_backfills_gemini_cli_account_credits() {
    let mut key = sample_catalog_key();
    key.upstream_metadata = Some(json!({
        "gemini_cli": {
            "updated_at": 1_778_067_246u64,
            "plan_type": "g1-pro-tier",
            "paidTier": {
                "availableCredits": 123.5,
                "consumedCredits": 7.0,
                "totalCredits": 200.0
            },
            "quota_by_model": {
                "gemini-2.5-pro": {
                    "display_name": "Gemini 2.5 Pro",
                    "remaining_fraction": 0.75,
                    "is_exhausted": false
                }
            }
        }
    }));

    let payload = provider_key_status_snapshot_payload(&key, "gemini_cli");
    let quota = payload
        .get("quota")
        .and_then(Value::as_object)
        .expect("quota snapshot should be object");
    let credits = quota
        .get("credits")
        .and_then(Value::as_object)
        .expect("credits snapshot should exist");
    let windows = quota
        .get("windows")
        .and_then(Value::as_array)
        .expect("Gemini CLI model windows should exist");

    assert_eq!(quota.get("provider_type"), Some(&json!("gemini_cli")));
    assert_eq!(quota.get("code"), Some(&json!("ok")));
    assert_eq!(quota.get("plan_type"), Some(&json!("g1-pro-tier")));
    assert_eq!(quota.get("updated_at"), Some(&json!(1_778_067_246u64)));
    assert_eq!(credits.get("remaining"), Some(&json!(123.5)));
    assert_eq!(credits.get("balance"), Some(&json!(123.5)));
    assert_eq!(credits.get("consumed"), Some(&json!(7.0)));
    assert_eq!(credits.get("total"), Some(&json!(200.0)));
    assert_eq!(credits.get("has_credits"), Some(&json!(true)));
    assert_eq!(windows.len(), 1);
    assert_eq!(windows[0].get("remaining_ratio"), Some(&json!(0.75)));
}

#[test]
fn provider_key_status_snapshot_payload_backfills_windsurf_daily_and_weekly_quota() {
    let mut key = sample_catalog_key();
    key.upstream_metadata = Some(json!({
        "windsurf": {
            "updated_at": 1_778_067_246u64,
            "plan_name": "Pro",
            "daily_remaining_percent": 40.0,
            "weekly_remaining_percent": 65.0,
            "daily_reset_at": 2_000_000_000u64,
            "weekly_reset_at": 2_000_600_000u64,
            "prompt_used": 12.0,
            "prompt_limit": 100.0,
            "prompt_remaining": 88.0,
            "flex_used": 3.0,
            "flex_limit": 10.0,
            "flex_remaining": 7.0,
            "allowed_models_count": 82
        }
    }));

    let payload = provider_key_status_snapshot_payload(&key, "windsurf");
    let quota = payload
        .get("quota")
        .and_then(Value::as_object)
        .expect("quota snapshot should be object");
    let windows = quota
        .get("windows")
        .and_then(Value::as_array)
        .expect("windsurf quota windows should exist");
    let daily = windows
        .iter()
        .filter_map(Value::as_object)
        .find(|window| window.get("code") == Some(&json!("daily")))
        .expect("daily quota window should exist");
    let weekly = windows
        .iter()
        .filter_map(Value::as_object)
        .find(|window| window.get("code") == Some(&json!("weekly")))
        .expect("weekly quota window should exist");

    assert_eq!(quota.get("provider_type"), Some(&json!("windsurf")));
    assert_eq!(quota.get("code"), Some(&json!("ok")));
    assert_eq!(quota.get("plan_type"), Some(&json!("Pro")));
    assert_eq!(quota.get("usage_ratio"), Some(&json!(0.6)));
    assert_eq!(quota.get("reset_at"), Some(&json!(2_000_000_000u64)));
    assert_eq!(daily.get("remaining_ratio"), Some(&json!(0.4)));
    assert_eq!(daily.get("used_ratio"), Some(&json!(0.6)));
    assert_eq!(daily.get("reset_seconds"), Some(&json!(221_932_754u64)));
    assert_eq!(weekly.get("remaining_ratio"), Some(&json!(0.65)));
    assert_eq!(weekly.get("used_ratio"), Some(&json!(0.35)));
    assert_eq!(weekly.get("reset_seconds"), Some(&json!(222_532_754u64)));
    assert_eq!(quota.get("allowed_models_count"), Some(&json!(82)));
}

#[test]
fn provider_key_status_snapshot_payload_treats_windsurf_rate_limit_as_cooldown() {
    let mut key = sample_catalog_key();
    key.upstream_metadata = Some(json!({
        "windsurf": {
            "updated_at": 1_778_067_246u64,
            "daily_remaining_percent": 80.0,
            "rate_limit": {
                "limited": true,
                "retry_after_ms": 60_001u64,
                "message": "slow down"
            },
            "last_error": "slow down"
        }
    }));

    let payload = provider_key_status_snapshot_payload(&key, "windsurf");
    let quota = payload
        .get("quota")
        .and_then(Value::as_object)
        .expect("quota snapshot should be object");
    let rate_window = quota
        .get("windows")
        .and_then(Value::as_array)
        .and_then(|windows| {
            windows
                .iter()
                .filter_map(Value::as_object)
                .find(|window| window.get("code") == Some(&json!("rate_limit")))
        })
        .expect("rate limit window should exist");

    assert_eq!(quota.get("code"), Some(&json!("cooldown")));
    assert_eq!(quota.get("exhausted"), Some(&json!(false)));
    assert_eq!(quota.get("reset_seconds"), Some(&json!(61u64)));
    assert_eq!(rate_window.get("is_exhausted"), Some(&json!(false)));
    assert_eq!(rate_window.get("reset_seconds"), Some(&json!(61u64)));
}

#[test]
fn provider_key_status_snapshot_payload_keeps_windsurf_capacity_probe_without_retry_after_ok() {
    let mut key = sample_catalog_key();
    key.upstream_metadata = Some(json!({
        "windsurf": {
            "updated_at": 1_778_067_246u64,
            "daily_remaining_percent": 100.0,
            "weekly_remaining_percent": 100.0,
            "rate_limit": {
                "limited": true,
                "has_capacity": false,
                "messages_remaining": 0.0,
                "max_messages": 100.0
            }
        }
    }));

    let payload = provider_key_status_snapshot_payload(&key, "windsurf");
    let quota = payload
        .get("quota")
        .and_then(Value::as_object)
        .expect("quota snapshot should be object");
    let has_rate_limit_window =
        quota
            .get("windows")
            .and_then(Value::as_array)
            .is_some_and(|windows| {
                windows
                    .iter()
                    .filter_map(Value::as_object)
                    .any(|window| window.get("code") == Some(&json!("rate_limit")))
            });

    assert_eq!(quota.get("code"), Some(&json!("ok")));
    assert_eq!(quota.get("label"), Some(&Value::Null));
    assert_eq!(quota.get("exhausted"), Some(&json!(false)));
    assert_eq!(
        payload.pointer("/quota/rate_limit/limited"),
        Some(&json!(true))
    );
    assert!(!has_rate_limit_window);
}

#[test]
fn provider_key_status_snapshot_payload_refreshes_stale_windsurf_cooldown_when_probe_has_capacity()
{
    let mut key = sample_catalog_key();
    key.status_snapshot = Some(json!({
        "quota": {
            "version": 2,
            "provider_type": "windsurf",
            "code": "cooldown",
            "label": "冷却中",
            "exhausted": false,
            "windows": [
                {
                    "code": "daily",
                    "unit": "percent",
                    "label": "日",
                    "scope": "account",
                    "remaining_ratio": 0.99,
                    "is_exhausted": false
                },
                {
                    "code": "rate_limit",
                    "unit": "count",
                    "label": "速率",
                    "scope": "account",
                    "is_exhausted": false,
                    "reset_seconds": null
                }
            ],
            "rate_limit": {
                "limited": true,
                "has_capacity": true,
                "messages_remaining": -1,
                "max_messages": -1
            }
        }
    }));
    key.upstream_metadata = Some(json!({
        "windsurf": {
            "updated_at": 1_778_067_246u64,
            "daily_remaining_percent": 99.0,
            "weekly_remaining_percent": 100.0,
            "allowed_models_count": 118,
            "rate_limit": {
                "limited": true,
                "has_capacity": true,
                "messages_remaining": -1,
                "max_messages": -1
            }
        }
    }));

    let payload = provider_key_status_snapshot_payload(&key, "windsurf");
    let quota = payload
        .get("quota")
        .and_then(Value::as_object)
        .expect("quota snapshot should be object");
    let has_rate_limit_window =
        quota
            .get("windows")
            .and_then(Value::as_array)
            .is_some_and(|windows| {
                windows
                    .iter()
                    .filter_map(Value::as_object)
                    .any(|window| window.get("code") == Some(&json!("rate_limit")))
            });

    assert_eq!(quota.get("code"), Some(&json!("ok")));
    assert_eq!(quota.get("label"), Some(&Value::Null));
    assert_eq!(quota.get("allowed_models_count"), Some(&json!(118u64)));
    assert!(!has_rate_limit_window);
}

#[test]
fn provider_key_status_snapshot_payload_marks_windsurf_banned_and_quarantined_blocking() {
    let mut banned_key = sample_catalog_key();
    banned_key.upstream_metadata = Some(json!({
        "windsurf": {
            "updated_at": 1_778_067_246u64,
            "banned": true,
            "reason": "forbidden"
        }
    }));
    let banned_payload = provider_key_status_snapshot_payload(&banned_key, "windsurf");

    assert_eq!(
        banned_payload.pointer("/quota/code"),
        Some(&json!("banned"))
    );
    assert_eq!(
        banned_payload.pointer("/quota/exhausted"),
        Some(&json!(true))
    );
    assert_eq!(
        banned_payload.pointer("/account/code"),
        Some(&json!("account_banned"))
    );
    assert_eq!(
        banned_payload.pointer("/account/blocked"),
        Some(&json!(true))
    );

    let mut quarantined_key = sample_catalog_key();
    quarantined_key.upstream_metadata = Some(json!({
        "windsurf": {
            "updated_at": 1_778_067_246u64,
            "quarantined": true
        }
    }));
    let quarantined_payload = provider_key_status_snapshot_payload(&quarantined_key, "windsurf");

    assert_eq!(
        quarantined_payload.pointer("/quota/code"),
        Some(&json!("quarantined"))
    );
    assert_eq!(
        quarantined_payload.pointer("/quota/exhausted"),
        Some(&json!(true))
    );
    assert_eq!(
        quarantined_payload.pointer("/account/code"),
        Some(&json!("account_quarantined"))
    );
    assert_eq!(
        quarantined_payload.pointer("/account/blocked"),
        Some(&json!(true))
    );
}

#[test]
fn provider_key_status_snapshot_payload_preserves_existing_materialized_quota_snapshot() {
    let mut key = sample_catalog_key();
    key.upstream_metadata = Some(json!({
        "codex": {
            "updated_at": 100u64,
            "primary_used_percent": 100.0
        }
    }));
    key.status_snapshot = Some(json!({
        "oauth": {
            "code": "none",
            "label": serde_json::Value::Null,
            "reason": serde_json::Value::Null,
            "expires_at": serde_json::Value::Null,
            "invalid_at": serde_json::Value::Null,
            "source": serde_json::Value::Null,
            "requires_reauth": false,
            "expiring_soon": false
        },
        "account": {
            "code": "ok",
            "label": serde_json::Value::Null,
            "reason": serde_json::Value::Null,
            "blocked": false,
            "source": serde_json::Value::Null,
            "recoverable": false
        },
        "quota": {
            "version": 2,
            "provider_type": "codex",
            "code": "ok",
            "label": serde_json::Value::Null,
            "reason": serde_json::Value::Null,
            "freshness": "fresh",
            "source": "refresh_api",
            "observed_at": 200u64,
            "exhausted": false,
            "usage_ratio": 0.25,
            "updated_at": 200u64,
            "reset_seconds": 3600u64,
            "plan_type": "team",
            "windows": [{
                "code": "weekly",
                "label": "周",
                "scope": "account",
                "unit": "percent",
                "used_ratio": 0.25,
                "remaining_ratio": 0.75,
                "reset_at": 1_900_000_000u64,
                "reset_seconds": 3600u64
            }]
        }
    }));

    let payload = provider_key_status_snapshot_payload(&key, "codex");
    let quota = payload
        .get("quota")
        .and_then(Value::as_object)
        .expect("quota snapshot should be object");

    assert_eq!(quota.get("updated_at"), Some(&json!(200u64)));
    assert_eq!(quota.get("plan_type"), Some(&json!("team")));
    assert_eq!(
        quota
            .get("windows")
            .and_then(Value::as_array)
            .and_then(|windows| windows.first())
            .and_then(Value::as_object)
            .and_then(|window| window.get("used_ratio")),
        Some(&json!(0.25))
    );
}

#[test]
fn provider_key_status_snapshot_payload_clears_stale_codex_exhaustion_summary() {
    let mut key = sample_catalog_key();
    key.status_snapshot = Some(json!({
        "quota": {
            "provider_type": "codex",
            "code": "exhausted",
            "exhausted": true,
            "windows": [{
                "code": "weekly",
                "scope": "account",
                "used_ratio": 0.83,
                "remaining_ratio": 0.17,
                "reset_at": 4_102_444_800u64
            }]
        }
    }));
    let payload = provider_key_status_snapshot_payload(&key, "codex");
    assert_eq!(payload["quota"]["code"], "ok");
    assert_eq!(payload["quota"]["exhausted"], false);
    assert!(payload["quota"]["label"].is_null());

    // An explicit current upstream refusal is not a stale percentage summary.
    key.status_snapshot.as_mut().unwrap()["quota"]["allowed"] = json!(false);
    let payload = provider_key_status_snapshot_payload(&key, "codex");
    assert_eq!(payload["quota"]["code"], "exhausted");
    assert_eq!(payload["quota"]["exhausted"], true);

    // Missing capacity evidence must not clear an exhausted summary either.
    key.status_snapshot = Some(json!({"quota": {
        "provider_type": "codex", "code": "exhausted", "exhausted": true
    }}));
    let payload = provider_key_status_snapshot_payload(&key, "codex");
    assert_eq!(payload["quota"]["code"], "exhausted");
    assert_eq!(payload["quota"]["exhausted"], true);
}

#[test]
fn provider_key_status_snapshot_payload_refreshes_codex_timestamp_formats() {
    for updated_at in [
        json!(1_900_000_000u64),
        json!(1_900_000_000_000u64),
        json!("2030-03-17T17:46:40Z"),
    ] {
        let mut key = sample_catalog_key();
        key.upstream_metadata = Some(json!({
            "codex": {
                "updated_at": updated_at,
                "primary_used_percent": 83.0,
                "primary_reset_at": 4_102_444_800u64
            }
        }));
        key.status_snapshot = Some(json!({
            "quota": {
                "provider_type": "codex",
                "updated_at": 1_899_999_000u64,
                "code": "exhausted",
                "exhausted": true,
                "allowed": false,
                "windows": [{
                    "code": "weekly",
                    "scope": "account",
                    "used_ratio": 1.0,
                    "remaining_ratio": 0.0,
                    "reset_at": 4_102_444_800u64
                }]
            }
        }));
        let payload = provider_key_status_snapshot_payload(&key, "codex");
        assert_eq!(payload["quota"]["code"], "ok");
        assert_eq!(payload["quota"]["updated_at"], 1_900_000_000u64);
        assert_eq!(payload["quota"]["windows"][0]["used_ratio"], 0.83);
        assert!(payload["quota"]["allowed"].is_null());
    }
}

#[test]
fn provider_key_status_snapshot_payload_preserves_codex_account_quota_on_unrelated_updates() {
    for patch in [
        json!({"plan_type": "pro"}),
        json!({"spark_primary_used_percent": 83.0}),
        json!({"credits_unlimited": false}),
        json!({"windows": [{"code": "weekly", "reset_at": 4_102_444_800u64}]}),
    ] {
        let mut metadata = patch;
        metadata["updated_at"] = json!("2030-03-17T17:46:40Z");
        let mut key = sample_catalog_key();
        key.upstream_metadata = Some(json!({"codex": metadata}));
        key.status_snapshot = Some(json!({"quota": {
            "provider_type": "codex", "updated_at": 200,
            "code": "exhausted", "exhausted": true, "allowed": false,
            "windows": [{"code": "weekly", "scope": "account", "used_ratio": 1.0,
                "remaining_ratio": 0.0, "reset_at": 4_102_444_800u64}]
        }}));
        let payload = provider_key_status_snapshot_payload(&key, "codex");
        assert_eq!(payload["quota"]["code"], "exhausted", "{metadata}");
        assert_eq!(payload["quota"]["exhausted"], true, "{metadata}");
        assert_eq!(payload["quota"]["allowed"], false, "{metadata}");
        assert_eq!(payload["quota"]["updated_at"], 200, "{metadata}");
        assert_eq!(
            payload["quota"]["windows"][0]["code"], "weekly",
            "{metadata}"
        );
        assert_eq!(
            payload["quota"]["windows"][0]["used_ratio"], 1.0,
            "{metadata}"
        );
    }
}

#[test]
fn provider_key_status_snapshot_payload_restores_complete_codex_cache() {
    let mut key = sample_catalog_key();
    key.upstream_metadata = Some(json!({
        "codex": {
            "updated_at": 200u64,
            "plan_type": "plus",
            "primary_used_percent": 89.0,
            "primary_reset_at": 1_900_000_000u64,
            "spark_primary_used_percent": 40.0,
            "spark_primary_reset_at": 1_900_100_000u64,
            "reset_credits": {
                "available_count": 3,
                "updated_at": 200u64,
                "detail_status": "available",
                "credits": []
            }
        }
    }));
    key.status_snapshot = Some(json!({
        "quota": {
            "version": 2,
            "provider_type": "codex",
            "updated_at": 200u64,
            "windows": [{
                "code": "weekly",
                "used_ratio": 1.0,
                "reset_at": 1_900_000_000u64
            }]
        }
    }));

    let payload = provider_key_status_snapshot_payload(&key, "codex");
    let windows = payload["quota"]["windows"]
        .as_array()
        .expect("quota windows should exist");

    assert_eq!(payload.pointer("/quota/plan_type"), Some(&json!("plus")));
    assert_eq!(
        payload.pointer("/quota/reset_credits/available_count"),
        Some(&json!(3u64))
    );
    assert!(windows.iter().any(|window| window["code"] == "spark_5h"));
    assert_eq!(
        windows
            .iter()
            .find(|window| window["code"] == "weekly")
            .and_then(|window| window.get("used_ratio")),
        Some(&json!(0.89))
    );
}

#[test]
fn sync_provider_key_quota_status_snapshot_preserves_codex_usage_state() {
    let current_status_snapshot = json!({
        "quota": {
            "version": 2,
            "provider_type": "codex",
            "windows": [
                {
                    "code": "weekly",
                    "usage_reset_at": 1_775_600_000u64,
                    "usage": {
                        "request_count": 3,
                        "total_tokens": 375,
                        "total_cost_usd": "0.60000000"
                    },
                    "reset_at": 1_900_000_000u64,
                    "window_minutes": 10_080u64
                },
                {
                    "code": "5h",
                    "usage_reset_at": 1_775_700_000u64,
                    "usage": {
                        "request_count": 2,
                        "total_tokens": 225,
                        "total_cost_usd": "0.30000000"
                    },
                    "reset_at": 1_900_500_000u64,
                    "window_minutes": 300u64
                }
            ]
        }
    });
    let upstream_metadata = json!({
        "codex": {
            "updated_at": 1_775_800_000u64,
            "plan_type": "plus",
            "primary_used_percent": 5.0,
            "primary_reset_at": 1_900_000_000u64,
            "primary_window_minutes": 10_080u64,
            "secondary_used_percent": 1.0,
            "secondary_reset_at": 1_900_500_000u64,
            "secondary_window_minutes": 300u64
        }
    });

    let payload = sync_provider_key_quota_status_snapshot(
        Some(&current_status_snapshot),
        "codex",
        Some(&upstream_metadata),
        "refresh_api",
    )
    .expect("quota snapshot should sync");
    let windows = payload
        .get("quota")
        .and_then(Value::as_object)
        .and_then(|quota| quota.get("windows"))
        .and_then(Value::as_array)
        .expect("quota windows should exist");
    let weekly = windows
        .iter()
        .filter_map(Value::as_object)
        .find(|window| window.get("code") == Some(&json!("weekly")))
        .expect("weekly window should exist");
    let five_h = windows
        .iter()
        .filter_map(Value::as_object)
        .find(|window| window.get("code") == Some(&json!("5h")))
        .expect("5h window should exist");

    assert_eq!(weekly.get("usage_reset_at"), Some(&json!(1_775_600_000u64)));
    assert_eq!(
        weekly
            .get("usage")
            .and_then(|usage| usage.get("request_count")),
        Some(&json!(3))
    );
    assert_eq!(
        weekly
            .get("usage")
            .and_then(|usage| usage.get("total_tokens")),
        Some(&json!(375))
    );
    assert_eq!(
        weekly
            .get("usage")
            .and_then(|usage| usage.get("total_cost_usd")),
        Some(&json!("0.60000000"))
    );
    assert_eq!(five_h.get("usage_reset_at"), Some(&json!(1_775_700_000u64)));
    assert_eq!(
        five_h
            .get("usage")
            .and_then(|usage| usage.get("request_count")),
        Some(&json!(2))
    );
    assert_eq!(
        five_h
            .get("usage")
            .and_then(|usage| usage.get("total_tokens")),
        Some(&json!(225))
    );
    assert_eq!(
        five_h
            .get("usage")
            .and_then(|usage| usage.get("total_cost_usd")),
        Some(&json!("0.30000000"))
    );
}

#[test]
fn sync_provider_key_quota_status_snapshot_honors_codex_limit_signal() {
    let payload = sync_provider_key_quota_status_snapshot(
        None,
        "codex",
        Some(&json!({
            "codex": {
                "updated_at": 1_775_800_000u64,
                "allowed": false,
                "limit_reached": true
            }
        })),
        "websocket_response_body",
    )
    .expect("explicit Codex limit signal should build a quota snapshot");

    assert_eq!(payload.pointer("/quota/code"), Some(&json!("exhausted")));
    assert_eq!(payload.pointer("/quota/exhausted"), Some(&json!(true)));
    assert_eq!(payload.pointer("/quota/allowed"), Some(&json!(false)));
    assert_eq!(payload.pointer("/quota/limit_reached"), Some(&json!(true)));
}

#[test]
fn sync_provider_key_quota_status_snapshot_drops_codex_usage_state_when_window_resets() {
    let current_status_snapshot = json!({
        "quota": {
            "version": 2,
            "provider_type": "codex",
            "windows": [
                {
                    "code": "weekly",
                    "usage_reset_at": 1_775_600_000u64,
                    "usage": {
                        "request_count": 3,
                        "total_tokens": 375,
                        "total_cost_usd": "0.60000000"
                    },
                    "reset_at": 1_900_000_000u64,
                    "window_minutes": 10_080u64
                }
            ]
        }
    });
    let upstream_metadata = json!({
        "codex": {
            "updated_at": 1_900_000_100u64,
            "plan_type": "plus",
            "primary_used_percent": 0.0,
            "primary_reset_at": 1_960_480_100u64,
            "primary_window_minutes": 10_080u64
        }
    });

    let payload = sync_provider_key_quota_status_snapshot(
        Some(&current_status_snapshot),
        "codex",
        Some(&upstream_metadata),
        "refresh_api",
    )
    .expect("quota snapshot should sync");
    let weekly = payload["quota"]["windows"]
        .as_array()
        .expect("quota windows should exist")
        .iter()
        .filter_map(Value::as_object)
        .find(|window| window.get("code") == Some(&json!("weekly")))
        .expect("weekly window should exist");

    assert_eq!(weekly.get("reset_at"), Some(&json!(1_960_480_100u64)));
    assert!(weekly.get("usage_reset_at").is_none());
    assert!(weekly.get("usage").is_none());
}

#[test]
fn sync_provider_key_quota_status_snapshot_defaults_codex_window_minutes() {
    let upstream_metadata = json!({
        "codex": {
            "updated_at": 1_775_800_000u64,
            "plan_type": "plus",
            "primary_used_percent": 5.0,
            "primary_reset_at": 1_900_000_000u64,
            "secondary_used_percent": 1.0,
            "secondary_reset_at": 1_900_500_000u64
        }
    });

    let payload = sync_provider_key_quota_status_snapshot(
        None,
        "codex",
        Some(&upstream_metadata),
        "response_headers",
    )
    .expect("quota snapshot should sync");
    let windows = payload["quota"]["windows"]
        .as_array()
        .expect("quota windows should exist");
    let weekly = windows
        .iter()
        .filter_map(Value::as_object)
        .find(|window| window.get("code") == Some(&json!("weekly")))
        .expect("weekly window should exist");
    let five_h = windows
        .iter()
        .filter_map(Value::as_object)
        .find(|window| window.get("code") == Some(&json!("5h")))
        .expect("5h window should exist");

    assert_eq!(weekly.get("window_minutes"), Some(&json!(10_080u64)));
    assert_eq!(five_h.get("window_minutes"), Some(&json!(300u64)));
}

#[test]
fn sync_provider_key_quota_status_snapshot_labels_actual_monthly_window() {
    let upstream_metadata = json!({
        "codex": {
            "updated_at": 1_784_287_450u64,
            "plan_type": "team",
            "primary_used_percent": 14.0,
            "primary_reset_at": 1_786_915_122u64,
            "primary_window_minutes": 43_800u64,
            "secondary_used_percent": 0.0,
            "secondary_reset_after_seconds": 0u64,
            "secondary_window_minutes": 0u64
        }
    });

    let payload = sync_provider_key_quota_status_snapshot(
        None,
        "codex",
        Some(&upstream_metadata),
        "response_headers",
    )
    .expect("quota snapshot should sync");
    let windows = payload["quota"]["windows"]
        .as_array()
        .expect("quota windows should exist");

    assert_eq!(windows.len(), 1);
    assert_eq!(windows[0]["code"], json!("monthly"));
    assert_eq!(windows[0]["label"], json!("月"));
    assert_eq!(windows[0]["window_minutes"], json!(43_800u64));
    assert_eq!(windows[0]["remaining_ratio"], json!(0.86));
}

#[test]
fn provider_key_status_snapshot_payload_backfills_thin_ok_snapshot_from_upstream_metadata() {
    let mut key = sample_catalog_key();
    key.upstream_metadata = Some(json!({
        "antigravity": {
            "updated_at": 1_775_553_285u64,
            "quota_by_model": {
                "gemini-2.5-pro": { "used_percent": 0.0 },
                "gemini-2.5-flash": { "used_percent": 25.0 }
            }
        }
    }));
    key.status_snapshot = Some(json!({
        "oauth": {
            "code": "none",
            "label": serde_json::Value::Null,
            "reason": serde_json::Value::Null,
            "expires_at": serde_json::Value::Null,
            "invalid_at": serde_json::Value::Null,
            "source": serde_json::Value::Null,
            "requires_reauth": false,
            "expiring_soon": false
        },
        "account": {
            "code": "ok",
            "label": serde_json::Value::Null,
            "reason": serde_json::Value::Null,
            "blocked": false,
            "source": serde_json::Value::Null,
            "recoverable": false
        },
        "quota": {
            "version": 2,
            "provider_type": "antigravity",
            "code": "ok",
            "label": serde_json::Value::Null,
            "reason": serde_json::Value::Null,
            "freshness": "fresh",
            "source": "refresh_api",
            "observed_at": 100u64,
            "exhausted": false,
            "usage_ratio": 0.0,
            "updated_at": 100u64,
            "reset_seconds": serde_json::Value::Null,
            "plan_type": serde_json::Value::Null
        }
    }));

    let payload = provider_key_status_snapshot_payload(&key, "antigravity");
    let quota = payload
        .get("quota")
        .and_then(Value::as_object)
        .expect("quota snapshot should be object");

    assert_eq!(quota.get("provider_type"), Some(&json!("antigravity")));
    assert_eq!(quota.get("updated_at"), Some(&json!(1_775_553_285u64)));
    assert_eq!(
        quota.get("windows").and_then(Value::as_array).map(Vec::len),
        Some(2usize)
    );
}

#[test]
fn sync_provider_key_quota_status_snapshot_labels_antigravity_models_by_model_id() {
    let upstream_metadata = json!({
        "antigravity": {
            "updated_at": 1_775_553_285u64,
            "quota_by_model": {
                "gemini-3.5-flash-extra-low": {
                    "display_name": "Gemini 3.5 Flash (Low)",
                    "remaining_fraction": 1.0
                },
                "gemini-3.5-flash-low": {
                    "display_name": "Gemini 3.5 Flash (Medium)",
                    "remaining_fraction": 0.75
                },
                "gemini-3-flash-agent": {
                    "display_name": "Gemini 3.5 Flash (High)",
                    "remaining_fraction": 0.5
                },
                "gemini-2.5-flash": {
                    "display_name": "Gemini 3.1 Flash Lite",
                    "remaining_fraction": 0.4
                },
                "claude-sonnet-4-6": {
                    "display_name": "Claude Sonnet 4.6 (Thinking)",
                    "remaining_fraction": 0.3,
                    "reset_time": "2026-04-07T12:34:56Z"
                }
            }
        }
    });

    let payload = sync_provider_key_quota_status_snapshot(
        None,
        "antigravity",
        Some(&upstream_metadata),
        "refresh_api",
    )
    .expect("quota snapshot should sync");
    let windows = payload["quota"]["windows"]
        .as_array()
        .expect("quota windows should exist");
    let label_for_model = |model: &str| {
        windows
            .iter()
            .filter_map(Value::as_object)
            .find(|window| window.get("model") == Some(&json!(model)))
            .and_then(|window| window.get("label"))
            .cloned()
    };

    assert_eq!(
        label_for_model("gemini-3.5-flash-extra-low"),
        Some(json!("Gemini 3.5 Flash (Low)"))
    );
    assert_eq!(
        label_for_model("gemini-3.5-flash-low"),
        Some(json!("Gemini 3.5 Flash (Medium)"))
    );
    assert_eq!(
        label_for_model("gemini-3-flash-agent"),
        Some(json!("Gemini 3.5 Flash (High)"))
    );
    assert_eq!(
        label_for_model("gemini-2.5-flash"),
        Some(json!("Gemini 3.1 Flash Lite"))
    );
    assert_eq!(
        label_for_model("claude-sonnet-4-6"),
        Some(json!("Claude Sonnet 4.6 (Thinking)"))
    );
    let claude_window = windows
        .iter()
        .filter_map(Value::as_object)
        .find(|window| window.get("model") == Some(&json!("claude-sonnet-4-6")))
        .expect("Claude quota window should exist");
    assert_eq!(
        claude_window.get("reset_at"),
        Some(&json!(1_775_565_296u64))
    );
    assert_eq!(claude_window.get("reset_seconds"), Some(&json!(12_011u64)));
}

#[test]
fn sync_provider_key_quota_status_snapshot_materializes_antigravity_group_windows() {
    let upstream_metadata = json!({
        "antigravity": {
            "updated_at": 1_777_000_000u64,
            "models": {
                "gemini-3.7-flash-tiered": {
                    "remaining_fraction": 0.9
                }
            },
            "quota_groups": [{
                "display_name": "Claude and GPT models",
                "description": "Shared quota",
                "buckets": [{
                    "bucket_id": "3p-5h",
                    "window": "5h",
                    "remaining_fraction": 0.25,
                    "reset_time": "2026-05-05T05:00:00Z",
                    "display_name": "5 hour"
                }, {
                    "bucket_id": "3p-weekly",
                    "window": "weekly",
                    "remaining_fraction": 0.8,
                    "reset_time": "2026-05-11T00:00:00Z"
                }]
            }]
        }
    });

    let payload = sync_provider_key_quota_status_snapshot(
        None,
        "antigravity",
        Some(&upstream_metadata),
        "refresh_api",
    )
    .expect("Antigravity quota snapshot should sync");
    let windows = payload["quota"]["windows"]
        .as_array()
        .expect("quota windows should exist");
    let five_hour = windows
        .iter()
        .find(|window| window["code"] == "group:0:3p-5h")
        .expect("5h grouped quota window should exist");
    let weekly = windows
        .iter()
        .find(|window| window["code"] == "group:0:3p-weekly")
        .expect("weekly grouped quota window should exist");

    assert_eq!(windows.len(), 3);
    assert_eq!(five_hour["scope"], json!("quota_group"));
    assert_eq!(five_hour["bucket_id"], json!("3p-5h"));
    assert_eq!(five_hour["label"], json!("Claude and GPT models · 5 hour"));
    assert_eq!(
        five_hour["quota_group_label"],
        json!("Claude and GPT models")
    );
    assert_eq!(five_hour["remaining_ratio"], json!(0.25));
    assert_eq!(five_hour["used_ratio"], json!(0.75));
    assert!(five_hour.get("model").is_none());
    assert_eq!(weekly["window"], json!("weekly"));
}

#[test]
fn provider_key_status_snapshot_payload_backfills_account_block_from_oauth_invalid_reason() {
    let mut key = sample_catalog_key();
    key.oauth_invalid_reason = Some("[ACCOUNT_BLOCK] account has been deactivated".to_string());

    let payload = provider_key_status_snapshot_payload(&key, "codex");
    let account = payload
        .get("account")
        .and_then(Value::as_object)
        .expect("account snapshot should be object");

    assert_eq!(account.get("code"), Some(&json!("account_disabled")));
    assert_eq!(account.get("label"), Some(&json!("账号停用")));
    assert_eq!(account.get("reason"), Some(&json!("Account is disabled")));
    assert_eq!(account.get("blocked"), Some(&json!(true)));
    assert_eq!(account.get("source"), Some(&json!("oauth_invalid")));
}

#[test]
fn provider_key_status_snapshot_payload_upgrades_deleted_agent_runtime_to_invalid() {
    let mut key = sample_catalog_key();
    key.auth_type = "oauth".to_string();
    key.oauth_invalid_at_unix_secs = Some(1_784_728_663);
    key.oauth_invalid_reason = Some("[REQUEST_FAILED] Agent runtime has been deleted.".to_string());

    let payload = provider_key_status_snapshot_payload(&key, "codex");
    let oauth = payload
        .get("oauth")
        .and_then(Value::as_object)
        .expect("oauth snapshot should be object");

    assert_eq!(oauth.get("code"), Some(&json!("invalid")));
    assert_eq!(oauth.get("label"), Some(&json!("已失效")));
    assert_eq!(oauth.get("invalid_at"), Some(&json!(1_784_728_663u64)));
    assert_eq!(oauth.get("requires_reauth"), Some(&json!(true)));
}

#[test]
fn provider_key_status_snapshot_payload_backfills_workspace_deactivated_from_metadata() {
    let mut key = sample_catalog_key();
    key.upstream_metadata = Some(json!({
        "codex": {
            "account_disabled": true,
            "reason": "deactivated_workspace"
        }
    }));

    let payload = provider_key_status_snapshot_payload(&key, "codex");
    let account = payload
        .get("account")
        .and_then(Value::as_object)
        .expect("account snapshot should be object");

    assert_eq!(account.get("code"), Some(&json!("workspace_deactivated")));
    assert_eq!(account.get("label"), Some(&json!("工作区停用")));
    assert_eq!(account.get("blocked"), Some(&json!(true)));
    assert_eq!(account.get("source"), Some(&json!("metadata")));
}

#[test]
fn admin_provider_key_response_projects_historical_sensitive_diagnostics() {
    let state = AppState::new().expect("gateway should build");
    let mut key = sample_catalog_key();
    key.auth_type = "oauth".to_string();
    key.oauth_invalid_at_unix_secs = Some(1_777_000_000);
    key.oauth_invalid_reason = Some(
        "[ACCOUNT_BLOCK] account has been deactivated: Authorization: Bearer upstream-secret https://user:password@internal.test?q=secret"
            .to_string(),
    );
    key.last_models_fetch_error = Some(
        "request failed for https://user:password@internal.test/models?q=secret; Authorization: Bearer upstream-secret"
            .to_string(),
    );
    key.status_snapshot = Some(json!({
        "oauth": {
            "code": "invalid",
            "reason": "Authorization: Bearer upstream-secret"
        },
        "account": {
            "code": "account_disabled",
            "reason": "https://user:password@internal.test?q=secret",
            "blocked": true
        },
        "quota": {
            "provider_type": "codex",
            "code": "cooldown",
            "reason": "Authorization: Bearer upstream-secret",
            "exhausted": false,
            "reset_credits": {
                "detail_error": "https://user:password@internal.test?q=secret"
            },
            "unknown": {"body": "upstream-secret"}
        }
    }));
    key.upstream_metadata = Some(json!({
        "codex": {
            "primary_used_percent": 25.0,
            "message": "Authorization: Bearer upstream-secret",
            "reset_credits": {
                "detail_error": "https://user:password@internal.test?q=secret"
            }
        }
    }));

    let payload = build_admin_provider_key_response(
        &state,
        &key,
        "codex",
        &["openai:responses".to_string()],
        1_777_000_001,
    );

    assert_eq!(
        payload["oauth_invalid_reason"],
        json!("[ACCOUNT_BLOCK] Account is disabled")
    );
    assert_eq!(
        payload["last_models_fetch_error"],
        json!("Upstream models fetch failed")
    );
    assert_eq!(
        payload.pointer("/status_snapshot/account/reason"),
        Some(&json!("Account is disabled"))
    );
    assert_eq!(
        payload.pointer("/upstream_metadata/codex/primary_used_percent"),
        Some(&json!(25.0))
    );
    let serialized = payload.to_string();
    assert!(!serialized.contains("upstream-secret"));
    assert!(!serialized.contains("user:password"));
    assert!(!serialized.contains("q=secret"));
}
