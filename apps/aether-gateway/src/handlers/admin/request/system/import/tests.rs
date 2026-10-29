use std::sync::Arc;

use aether_data::repository::pool_scores::PostgresPoolMemberScoreRepository;
use aether_data::repository::provider_catalog::InMemoryProviderCatalogReadRepository;
use aether_data_contracts::repository::provider_catalog::{
    StoredProviderCatalogKey, StoredProviderCatalogProvider,
};
use serde_json::json;

use super::{
    build_imported_user_usage_total_aggregates, imported_api_key_id_for_mode,
    imported_credential_tombstone, imported_existing_user_is_protected,
    imported_oauth_auth_config_has_credentials, imported_oauth_expiry_after_import,
    imported_optional_bool, imported_optional_f64, imported_optional_i32, imported_optional_u64,
    imported_rfc3339_to_unix_secs, imported_string_list_from_value,
    normalize_import_endpoint_format, normalize_import_key_formats,
    normalize_import_key_raw_payload, normalize_imported_system_user_role,
    normalize_imported_wallet_target, prepare_imported_secret_safe_body_rules,
    prepare_imported_secret_safe_header_rules, prepare_imported_secret_safe_json,
    resolve_imported_password_hash, seed_imported_oauth_pool_score,
    validate_imported_provider_key_credential_state, validate_imported_system_users_export_version,
    validate_imported_system_users_export_version_for_mode, ImportedProviderKey, SystemImportMode,
};
use crate::admin_api::AdminAppState;
use crate::data::GatewayDataState;
use crate::AppState;

#[test]
fn users_import_requires_supported_export_version() {
    assert!(validate_imported_system_users_export_version(Some(&json!("1.3"))).is_ok());
    assert!(validate_imported_system_users_export_version(Some(&json!("1.4"))).is_ok());
    assert!(validate_imported_system_users_export_version(Some(&json!("1.5"))).is_ok());
    assert!(validate_imported_system_users_export_version(Some(&json!("1.6"))).is_ok());
    assert_eq!(
        validate_imported_system_users_export_version(Some(&json!("2.2"))).unwrap_err(),
        "不支持的用户数据版本: 2.2，支持的版本: 1.3, 1.4, 1.5, 1.6"
    );
    assert_eq!(
        validate_imported_system_users_export_version(Some(&json!(null))).unwrap_err(),
        "version 必须是 x.y 字符串"
    );
}

#[test]
fn recovery_users_import_requires_v15_and_valid_bcrypt() {
    assert_eq!(
        validate_imported_system_users_export_version_for_mode(
            Some(&json!("1.5")),
            SystemImportMode::RecoveryBackup,
        ),
        Ok((1, 5)),
    );
    assert!(validate_imported_system_users_export_version_for_mode(
        Some(&json!("1.4")),
        SystemImportMode::RecoveryBackup,
    )
    .is_err());
    assert!(resolve_imported_password_hash(
        json!({ "password_hash": "attacker-controlled" })
            .as_object()
            .expect("fixture should be an object"),
        (1, 5),
        SystemImportMode::RecoveryBackup,
    )
    .is_err());
}

#[test]
fn system_user_role_interactive_import_cannot_assign_admin_console_roles() {
    assert_eq!(
        normalize_imported_system_user_role(None, SystemImportMode::InteractiveUpload),
        Ok(Some("user".to_string()))
    );
    assert_eq!(
        normalize_imported_system_user_role(
            Some(&json!(" ADMIN ")),
            SystemImportMode::InteractiveUpload,
        ),
        Ok(None)
    );
    assert_eq!(
        normalize_imported_system_user_role(
            Some(&json!("audit_admin")),
            SystemImportMode::InteractiveUpload,
        ),
        Ok(None)
    );
    assert_eq!(
        normalize_imported_system_user_role(
            Some(&json!("audit_admin")),
            SystemImportMode::RollbackCheckpoint,
        ),
        Ok(None)
    );
    assert!(normalize_imported_system_user_role(
        Some(&json!("owner")),
        SystemImportMode::InteractiveUpload,
    )
    .expect_err("unknown roles must be rejected before import writes")
    .contains("不支持的用户角色"));
}

#[test]
fn system_user_role_authenticated_recovery_restores_only_audit_admin() {
    for mode in [
        SystemImportMode::RecoveryBackup,
        SystemImportMode::RecoveryRollbackCheckpoint,
    ] {
        assert_eq!(
            normalize_imported_system_user_role(Some(&json!("audit_admin")), mode),
            Ok(Some("audit_admin".to_string()))
        );
        assert_eq!(
            normalize_imported_system_user_role(Some(&json!("admin")), mode),
            Ok(None)
        );
    }
}

#[test]
fn system_user_role_ordinary_import_protects_existing_admin_console_users() {
    for mode in [
        SystemImportMode::InteractiveUpload,
        SystemImportMode::RollbackCheckpoint,
    ] {
        assert!(imported_existing_user_is_protected("admin", mode));
        assert!(imported_existing_user_is_protected("audit_admin", mode));
        assert!(!imported_existing_user_is_protected("user", mode));
    }

    assert!(imported_existing_user_is_protected(
        "admin",
        SystemImportMode::RecoveryBackup,
    ));
    assert!(!imported_existing_user_is_protected(
        "audit_admin",
        SystemImportMode::RecoveryBackup,
    ));
    assert!(!imported_existing_user_is_protected(
        "audit_admin",
        SystemImportMode::RecoveryRollbackCheckpoint,
    ));
}

#[test]
fn rollback_checkpoint_body_forces_overwrite_and_omits_local_proxy_nodes() {
    let checkpoint = json!({
        "version": "2.3",
        "credential_state": "not_exported",
        "proxy_nodes": [{"id": "local-node"}],
        "system_configs": []
    });
    let body = super::build_aggregate_rollback_body(&checkpoint, true)
        .expect("rollback body should serialize");
    let parsed: serde_json::Value = serde_json::from_slice(&body).expect("body is JSON");

    assert_eq!(parsed["merge_mode"], json!("overwrite"));
    assert_eq!(parsed["proxy_nodes"], json!([]));
    assert_eq!(parsed["credential_state"], json!("not_exported"));
}

#[test]
fn rollback_checkpoint_can_skip_ldap_without_dropping_other_config_sections() {
    let checkpoint = json!({
        "version": "2.3",
        "ldap_config": {
            "server_url": "ldaps://checkpoint.example.test",
            "bind_dn": "cn=admin,dc=example,dc=test",
            "base_dn": "dc=example,dc=test"
        },
        "system_configs": [{"key": "module.example.enabled", "value": true}],
    });
    let body = super::build_aggregate_rollback_body_with_options(&checkpoint, true, true)
        .expect("rollback body should serialize");
    let parsed: serde_json::Value = serde_json::from_slice(&body).expect("body is JSON");

    assert!(parsed.get("ldap_config").is_none());
    assert_eq!(parsed["system_configs"], checkpoint["system_configs"]);
    assert_eq!(parsed["merge_mode"], json!("overwrite"));
    assert_eq!(parsed["proxy_nodes"], json!([]));
}

#[test]
fn rollback_checkpoint_body_rejects_non_object() {
    let error = super::build_aggregate_rollback_body(&json!([1, 2, 3]), false)
        .expect_err("non-object checkpoint must be rejected");
    assert!(error
        .into_message()
        .contains("checkpoint must be a JSON object"));
}

#[test]
fn aggregate_users_rollback_body_excludes_all_wallet_snapshots() {
    let checkpoint = json!({
        "version": "1.5",
        "users": [{
            "id": "user-1",
            "username": "checkpoint-user",
            "request_count": 100,
            "total_tokens": 200,
            "wallet": {"balance": 10.0},
            "api_keys": [{
                "api_key_id": "key-1",
                "name": "user key",
                "total_requests": 101,
                "total_tokens": 201,
                "total_cost_usd": 1.25,
                "wallet": {"balance": 20.0}
            }]
        }],
        "standalone_keys": [{
            "api_key_id": "standalone-1",
            "name": "standalone key",
            "total_requests": 102,
            "total_tokens": 202,
            "total_cost_usd": 2.5,
            "wallet": {"balance": 30.0}
        }],
        "usage_aggregates": {
            "stats_daily": [{"date_unix_secs": 1}]
        }
    });

    let body = super::build_aggregate_users_rollback_body(&checkpoint)
        .expect("users rollback body should serialize");
    let parsed: serde_json::Value = serde_json::from_slice(&body).expect("body is JSON");

    assert_eq!(parsed["merge_mode"], json!("overwrite"));
    assert_eq!(parsed["users"][0]["username"], json!("checkpoint-user"));
    assert!(parsed["users"][0].get("request_count").is_none());
    assert!(parsed["users"][0].get("total_tokens").is_none());
    assert!(parsed["users"][0].get("wallet").is_none());
    assert!(parsed["users"][0]["api_keys"][0].get("wallet").is_none());
    assert!(parsed["users"][0]["api_keys"][0]
        .get("total_requests")
        .is_none());
    assert!(parsed["users"][0]["api_keys"][0]
        .get("total_tokens")
        .is_none());
    assert!(parsed["users"][0]["api_keys"][0]
        .get("total_cost_usd")
        .is_none());
    assert!(parsed["standalone_keys"][0].get("wallet").is_none());
    assert!(parsed["standalone_keys"][0].get("total_requests").is_none());
    assert!(parsed["standalone_keys"][0].get("total_tokens").is_none());
    assert!(parsed["standalone_keys"][0].get("total_cost_usd").is_none());
    assert!(parsed.get("usage_aggregates").is_none());
}

#[test]
fn imported_api_key_tombstones_fit_legacy_columns_and_cannot_authenticate() {
    use sha2::{Digest, Sha256};

    let identity = "api-key-id:public-source-key-id";
    let tombstone = imported_credential_tombstone(identity);
    let normal_auth_hash = format!("{:x}", Sha256::digest(identity.as_bytes()));

    assert_eq!(tombstone.len(), 64);
    assert!(tombstone.starts_with("$aether-import-revoked$"));
    assert!(!tombstone
        .chars()
        .all(|character| character.is_ascii_hexdigit()));
    assert_ne!(tombstone, normal_auth_hash);
}

#[test]
fn rollback_checkpoint_reuses_api_key_id_while_interactive_imports_rotate_it() {
    let source_id = Some("checkpoint-api-key-id");
    let rollback_id = imported_api_key_id_for_mode(source_id, SystemImportMode::RollbackCheckpoint);
    assert_eq!(rollback_id, "checkpoint-api-key-id");

    let interactive_id =
        imported_api_key_id_for_mode(source_id, SystemImportMode::InteractiveUpload);
    assert_ne!(interactive_id, "checkpoint-api-key-id");
    assert!(!interactive_id.is_empty());
}

#[test]
fn recovery_rollback_checkpoint_keeps_credentials_and_stable_ids() {
    let mode = SystemImportMode::RecoveryRollbackCheckpoint;
    assert!(mode.restores_credentials());
    assert!(mode.preserves_active_state());
    assert!(mode.is_rollback_checkpoint());
    assert_eq!(
        imported_api_key_id_for_mode(Some("recovery-key-id"), mode),
        "recovery-key-id"
    );
    assert!(!SystemImportMode::InteractiveUpload.restores_credentials());
}

#[test]
fn rollback_checkpoint_requires_stable_user_id_instead_of_identifier_guessing() {
    assert!(
        super::validate_rollback_user_source_id(SystemImportMode::RollbackCheckpoint, None,)
            .expect_err("rollback without a source ID must be rejected")
            .contains("拒绝按 email/username 猜测用户")
    );
    assert!(super::validate_rollback_user_source_id(
        SystemImportMode::RecoveryRollbackCheckpoint,
        None,
    )
    .is_err());
    assert!(super::validate_rollback_user_source_id(
        SystemImportMode::RollbackCheckpoint,
        Some("stable-user-id"),
    )
    .is_ok());
    assert!(
        super::validate_rollback_user_source_id(SystemImportMode::InteractiveUpload, None,).is_ok()
    );
}

#[test]
fn users_import_builds_supplemental_usage_aggregates_from_summary_fields() {
    let users = vec![
        json!({
            "id": "source-user-1",
            "username": "alice",
            "request_count": 12,
            "total_tokens": 3456
        }),
        json!({
            "id": "source-user-zero",
            "username": "zero",
            "request_count": 0,
            "total_tokens": 0
        }),
        json!({
            "username": "no-source-id",
            "request_count": 5,
            "total_tokens": 6
        }),
    ];

    let rows =
        build_imported_user_usage_total_aggregates(&users, Some(&json!("2026-05-25T12:34:56Z")))
            .expect("supplemental usage aggregates should build");

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].user_id, "source-user-1");
    assert_eq!(rows[0].username.as_deref(), Some("alice"));
    assert_eq!(rows[0].total_requests, 12);
    assert_eq!(rows[0].success_requests, 12);
    assert_eq!(rows[0].input_tokens, 3456);
    assert_eq!(rows[0].date_unix_secs % 86_400, 0);
}

#[test]
fn config_import_normalizes_python_cli_api_format_aliases() {
    for (raw, expected) in [
        ("openai:cli", "openai:responses"),
        ("openai:compact", "openai:responses:compact"),
        ("openai_image", "openai:image"),
        ("images", "openai:image"),
        ("/v1/images/generations", "openai:image"),
        ("/v1/images/edits", "openai:image"),
        ("claude:chat", "claude:messages"),
        ("claude:cli", "claude:messages"),
        ("gemini:chat", "gemini:generate_content"),
    ] {
        assert_eq!(normalize_import_endpoint_format(raw).unwrap(), expected);
    }
}

#[test]
fn config_import_normalizes_key_formats_against_imported_endpoint_aliases() {
    let endpoint_formats = ["claude:messages", "openai:responses:compact"]
        .into_iter()
        .map(ToOwned::to_owned)
        .collect();
    let item = ImportedProviderKey {
        api_key: None,
        auth_type: None,
        auth_config: None,
        name: None,
        note: None,
        api_formats: Some(vec!["claude:cli".to_string(), "openai:compact".to_string()]),
        supported_endpoints: None,
        rate_multipliers: None,
        internal_priority: None,
        global_priority_by_format: None,
        auth_type_by_format: None,
        allow_auth_channel_mismatch_formats: None,
        rpm_limit: None,
        allowed_models: None,
        capabilities: None,
        cache_ttl_minutes: None,
        max_probe_interval_minutes: None,
        auto_fetch_models: None,
        locked_models: None,
        model_include_patterns: None,
        model_exclude_patterns: None,
        is_active: true,
        proxy: None,
        fingerprint: None,
        credential_state: None,
    };

    let (formats, missing) = normalize_import_key_formats(&item, &endpoint_formats);

    assert_eq!(formats, vec!["claude:messages", "openai:responses:compact"]);
    assert!(missing.is_empty());
}

#[test]
fn config_import_filters_key_format_scoped_fields_to_selected_api_formats() {
    let raw_key = json!({
        "name": "test-key",
        "api_key": "sk-test",
        "api_formats": ["openai:responses", "openai:video"],
        "auth_type_by_format": {
            "openai:responses": "api_key",
            "openai:video": "bearer"
        },
        "allow_auth_channel_mismatch_formats": [
            "openai:responses",
            "openai:video"
        ]
    });
    let raw_key = raw_key.as_object().expect("key should be object");

    let payload = normalize_import_key_raw_payload(
        raw_key,
        "api_key",
        &["openai:responses".to_string()],
        None,
        false,
    );

    assert_eq!(payload["api_formats"], json!(["openai:responses"]));
    assert_eq!(
        payload["auth_type_by_format"],
        json!({ "openai:responses": "api_key" })
    );
    assert_eq!(
        payload["allow_auth_channel_mismatch_formats"],
        json!(["openai:responses"])
    );
}

#[test]
fn config_import_preserves_explicit_empty_mismatch_scope_after_filtering() {
    let raw_key = json!({
        "name": "test-key",
        "api_key": "sk-test",
        "api_formats": ["openai:responses"],
        "allow_auth_channel_mismatch_formats": ["openai:video"]
    });
    let raw_key = raw_key.as_object().expect("key should be object");

    let payload = normalize_import_key_raw_payload(
        raw_key,
        "api_key",
        &["openai:responses".to_string()],
        None,
        false,
    );

    assert_eq!(payload["allow_auth_channel_mismatch_formats"], json!([]));
}

#[test]
fn config_import_never_turns_redaction_markers_into_credentials() {
    let mut item = ImportedProviderKey {
        api_key: None,
        auth_type: Some("api_key".to_string()),
        auth_config: None,
        name: Some("primary".to_string()),
        note: None,
        api_formats: Some(vec!["openai:chat".to_string()]),
        supported_endpoints: None,
        rate_multipliers: None,
        internal_priority: None,
        global_priority_by_format: None,
        auth_type_by_format: None,
        allow_auth_channel_mismatch_formats: None,
        rpm_limit: None,
        allowed_models: None,
        capabilities: None,
        cache_ttl_minutes: None,
        max_probe_interval_minutes: None,
        auto_fetch_models: None,
        locked_models: None,
        model_include_patterns: None,
        model_exclude_patterns: None,
        is_active: false,
        proxy: None,
        fingerprint: None,
        credential_state: Some("not_exported".to_string()),
    };

    assert!(validate_imported_provider_key_credential_state(&item).unwrap());
    item.api_key = Some("***".to_string());
    assert!(validate_imported_provider_key_credential_state(&item).is_err());
    item.api_key = None;
    item.credential_state = Some("unknown".to_string());
    assert!(validate_imported_provider_key_credential_state(&item).is_err());
}

#[test]
fn config_import_restores_existing_secret_safe_json_and_strips_new_placeholders() {
    let existing = json!({
        "credentials": {"refresh_token": "old-refresh"},
        "region": "us-east-1"
    });
    let incoming = json!({
        "credentials": "***",
        "region": "eu-west-1"
    });

    let restored = prepare_imported_secret_safe_json(Some(&existing), Some(incoming.clone()), true)
        .expect("existing config should restore");
    assert_eq!(
        restored["credentials"],
        json!({"refresh_token": "old-refresh"})
    );
    assert_eq!(restored["region"], "eu-west-1");

    let created = prepare_imported_secret_safe_json(None, Some(incoming), true)
        .expect("safe config should remain");
    assert!(created.get("credentials").is_none());
    assert_eq!(created["region"], "eu-west-1");
}

#[test]
fn config_import_restores_matching_endpoint_rules_without_persisting_markers() {
    let existing_headers = json!([{
        "action": "set",
        "key": "Authorization",
        "value": "Bearer old-secret"
    }]);
    let incoming_headers = json!([{
        "action": "set",
        "key": "Authorization",
        "value": "***",
        "has_value": true
    }]);
    let restored_headers = prepare_imported_secret_safe_header_rules(
        Some(&existing_headers),
        Some(incoming_headers),
        true,
    )
    .expect("matching header rule should remain");
    assert_eq!(restored_headers[0]["value"], "Bearer old-secret");
    assert!(restored_headers[0].get("has_value").is_none());

    let existing_body = json!([{
        "action": "set",
        "path": "$.credentials.token",
        "value": "old-body-secret"
    }]);
    let incoming_body = json!([{
        "action": "set",
        "path": "$.credentials.token",
        "value": "***",
        "has_value": true
    }]);
    let restored_body =
        prepare_imported_secret_safe_body_rules(Some(&existing_body), Some(incoming_body), true)
            .expect("matching body rule should remain");
    assert_eq!(restored_body[0]["value"], "old-body-secret");
    assert!(restored_body[0].get("has_value").is_none());
}

#[test]
fn config_import_drops_unrecoverable_endpoint_rule_placeholders() {
    let incoming_headers = json!([{
        "action": "set",
        "key": "Authorization",
        "value": "***",
        "has_value": true
    }]);
    assert_eq!(
        prepare_imported_secret_safe_header_rules(None, Some(incoming_headers.clone()), true),
        Some(json!([]))
    );
    let markerless_placeholder = json!([{
        "action": "set",
        "key": "Authorization",
        "value": "***"
    }]);
    assert_eq!(
        prepare_imported_secret_safe_header_rules(None, Some(markerless_placeholder), true,),
        Some(json!([]))
    );

    let different_existing = json!([{
        "action": "set",
        "key": "X-Different",
        "value": "must-not-move"
    }]);
    assert_eq!(
        prepare_imported_secret_safe_header_rules(
            Some(&different_existing),
            Some(incoming_headers),
            true,
        ),
        Some(json!([]))
    );

    let incoming_body = json!([{
        "action": "regex_replace",
        "path": "$.credentials.token",
        "pattern": "***",
        "replacement": "***",
        "has_pattern": true,
        "has_replacement": true
    }]);
    assert_eq!(
        prepare_imported_secret_safe_body_rules(None, Some(incoming_body), true),
        Some(json!([]))
    );
}

#[test]
fn oauth_import_only_treats_non_empty_secret_fields_as_credentials() {
    assert!(!imported_oauth_auth_config_has_credentials(&json!({})));
    assert!(!imported_oauth_auth_config_has_credentials(&json!({
        "provider_type": "codex",
        "expires_at": 4_102_444_800u64,
        "account_id": "acct-1",
        "refresh_token": "  "
    })));
    assert!(imported_oauth_auth_config_has_credentials(&json!({
        "provider_type": "codex",
        "refresh_token": "refresh-1"
    })));
    assert!(imported_oauth_auth_config_has_credentials(&json!({
        "session": {"sso_token": "sso-1"}
    })));
    for field in [
        "sso_rw_token",
        "ssoRwToken",
        "cf_cookies",
        "cfCookies",
        "cf_clearance",
        "cfClearance",
        "cookieHeader",
    ] {
        let mut config = serde_json::Map::new();
        config.insert(field.to_string(), json!("credential-1"));
        assert!(
            imported_oauth_auth_config_has_credentials(&serde_json::Value::Object(config)),
            "{field} is transport credential material"
        );
    }
}

#[test]
fn oauth_import_expiry_tracks_the_supplied_credential_source() {
    let old_expiry = Some(1_700_000_000);
    assert_eq!(
        imported_oauth_expiry_after_import(old_expiry, false, None, true),
        None,
        "a new top-level api_key replaces the old session and clears its expiry"
    );
    assert_eq!(
        imported_oauth_expiry_after_import(old_expiry, false, None, false),
        old_expiry,
        "metadata-only imports preserve the current OAuth expiry"
    );
    assert_eq!(
        imported_oauth_expiry_after_import(
            old_expiry,
            true,
            Some(&json!({"expires_at": 4_102_444_800u64})),
            false,
        ),
        Some(4_102_444_800),
        "an explicit auth_config owns the replacement expiry"
    );
}

#[tokio::test]
async fn oauth_pool_score_persistence_failure_is_propagated() {
    let mut provider = StoredProviderCatalogProvider::new(
        "provider-1".to_string(),
        "Provider One".to_string(),
        None,
        "codex".to_string(),
    )
    .expect("provider should build");
    provider.config = Some(json!({"pool_advanced": {}}));
    let key = StoredProviderCatalogKey::new(
        "key-1".to_string(),
        provider.id.clone(),
        "OAuth Key".to_string(),
        "oauth".to_string(),
        None,
        true,
    )
    .expect("key should build");
    let provider_repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![provider],
        Vec::new(),
        vec![key.clone()],
    ));
    let no_writer_app = AppState::new()
        .expect("app state should build")
        .with_data_state_for_tests(
            GatewayDataState::with_provider_catalog_repository_for_tests(Arc::clone(
                &provider_repository,
            )),
        );
    seed_imported_oauth_pool_score(&AdminAppState::new(&no_writer_app), "provider-1", &key, 99)
        .await
        .expect("a disabled score writer remains an allowed no-op");

    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect_lazy("postgres://localhost/aether")
        .expect("postgres pool should build");
    let score_repository = Arc::new(PostgresPoolMemberScoreRepository::new(pool.clone()));
    pool.close().await;
    let app = AppState::new()
        .expect("app state should build")
        .with_data_state_for_tests(
            GatewayDataState::with_provider_catalog_repository_for_tests(provider_repository)
                .with_pool_score_repository_for_tests(score_repository),
        );

    let error = seed_imported_oauth_pool_score(&AdminAppState::new(&app), "provider-1", &key, 100)
        .await
        .expect_err("closed pool must fail OAuth score recovery");
    assert!(error
        .into_message()
        .contains("failed to recover OAuth pool score for key 'key-1'"));
}

#[test]
fn import_handles_legacy_string_scalars() {
    assert_eq!(
        imported_optional_bool(Some(&json!("true"))).unwrap_err(),
        "字段必须是布尔值"
    );
    assert_eq!(
        imported_optional_i32(Some(&json!("5")), "rate_limit").unwrap_err(),
        "rate_limit 必须是整数"
    );
    assert_eq!(
        imported_optional_u64(Some(&json!("5")), "total_requests").unwrap_err(),
        "total_requests 必须是非负整数"
    );
    assert_eq!(
        imported_optional_f64(Some(&json!("1.25000000")), "total_cost_usd").unwrap(),
        Some(1.25)
    );
    assert_eq!(
        imported_optional_f64(Some(&json!("not-a-number")), "total_cost_usd").unwrap_err(),
        "total_cost_usd 必须是有限数值"
    );
}

#[test]
fn import_handles_python_isoformat_timestamps() {
    assert_eq!(
        imported_rfc3339_to_unix_secs(Some(&json!("2099-01-01T00:00:00+00:00")), "expires_at")
            .unwrap(),
        Some(4_070_908_800)
    );
    assert_eq!(
        imported_rfc3339_to_unix_secs(Some(&json!("2099-01-01T00:00:00")), "expires_at").unwrap(),
        Some(4_070_908_800)
    );
    assert_eq!(
        imported_rfc3339_to_unix_secs(Some(&json!("invalid")), "expires_at").unwrap_err(),
        "expires_at 必须是 RFC3339 时间"
    );
}

#[test]
fn import_preserves_python_wallet_negative_recharge_balance() {
    let wallet = json!({
        "balance": -4.5,
        "recharge_balance": -5.25,
        "gift_balance": 0.75,
        "limit_mode": "finite"
    });
    let wallet = wallet.as_object().expect("wallet should be object");

    let target = normalize_imported_wallet_target(Some(wallet), false).unwrap();
    assert_eq!(target.recharge_balance, -5.25);
    assert_eq!(target.gift_balance, 0.75);
    assert_eq!(target.total_recharged, 0.0);
}

#[test]
fn import_preserves_python_wallet_negative_balance_fallback() {
    let wallet = json!({
        "balance": -4.5,
        "gift_balance": 0.75,
        "limit_mode": "finite"
    });
    let wallet = wallet.as_object().expect("wallet should be object");

    let target = normalize_imported_wallet_target(Some(wallet), false).unwrap();
    assert_eq!(target.recharge_balance, -5.25);
    assert_eq!(target.gift_balance, 0.75);
}

#[test]
fn import_rejects_negative_wallet_history_totals() {
    for (field, value) in [
        ("total_recharged", -1.0),
        ("total_consumed", -1.0),
        ("total_refunded", -1.0),
    ] {
        let mut wallet = serde_json::Map::new();
        wallet.insert(field.to_string(), json!(value));
        let error = normalize_imported_wallet_target(Some(&wallet), false)
            .expect_err("negative wallet history total must be rejected");
        assert!(error.contains(field), "error should identify {field}");
        assert!(
            error.contains("非负"),
            "error should require non-negative {field}"
        );
    }
}

#[test]
fn import_allows_signed_wallet_adjustment_total() {
    let wallet = json!({"total_adjusted": -3.5});
    let wallet = wallet.as_object().expect("wallet should be object");
    let target = normalize_imported_wallet_target(Some(wallet), false)
        .expect("signed adjustment history should remain valid");
    assert_eq!(target.total_adjusted, -3.5);
}

#[test]
fn import_rejects_legacy_string_lists() {
    assert_eq!(
        imported_string_list_from_value(Some(&json!("openai")), "allowed_providers").unwrap_err(),
        "allowed_providers 必须是字符串列表"
    );
    assert_eq!(
        imported_string_list_from_value(Some(&json!("[\"openai:chat\"]")), "allowed_api_formats")
            .unwrap_err(),
        "allowed_api_formats 必须是字符串列表"
    );
}
