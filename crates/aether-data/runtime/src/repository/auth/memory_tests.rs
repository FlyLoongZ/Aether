use super::InMemoryAuthApiKeySnapshotRepository;
use crate::repository::auth::{
    AuthApiKeyLookupKey, AuthApiKeyReadRepository, AuthApiKeyWriteRepository,
    CompareAndSwapAuthApiKeyCiphertext, CreateUserApiKeyRecord, StandaloneApiKeyExportListQuery,
    StoredAuthApiKeyExportRecord, StoredAuthApiKeySnapshot, UpdateStandaloneApiKeyBasicRecord,
    UpdateUserApiKeyBasicRecord,
};
use crate::repository::users::StoredUserAuthRecord;

fn sample_snapshot(api_key_id: &str, user_id: &str) -> StoredAuthApiKeySnapshot {
    StoredAuthApiKeySnapshot::new(
        user_id.to_string(),
        "alice".to_string(),
        Some("alice@example.com".to_string()),
        "user".to_string(),
        "local".to_string(),
        true,
        false,
        Some(serde_json::json!(["openai"])),
        Some(serde_json::json!(["openai:chat"])),
        Some(serde_json::json!(["gpt-4.1"])),
        api_key_id.to_string(),
        Some("default".to_string()),
        true,
        false,
        false,
        Some(60),
        Some(5),
        Some(200),
        Some(serde_json::json!(["openai"])),
        Some(serde_json::json!(["openai:chat"])),
        Some(serde_json::json!(["gpt-4.1"])),
    )
    .expect("snapshot should build")
}

fn sample_create_user_api_key_record(user_id: &str, api_key_id: &str) -> CreateUserApiKeyRecord {
    CreateUserApiKeyRecord {
        user_id: user_id.to_string(),
        api_key_id: api_key_id.to_string(),
        key_hash: format!("hash-{api_key_id}"),
        key_encrypted: Some(format!("encrypted-{api_key_id}")),
        name: Some("Created".to_string()),
        allowed_providers: None,
        allowed_api_formats: None,
        allowed_models: None,
        ip_rules: None,
        rate_limit: 0,
        concurrent_limit: None,
        force_capabilities: None,
        feature_settings: None,
        is_active: true,
        expires_at_unix_secs: None,
        auto_delete_on_expiry: false,
        total_requests: 0,
        total_tokens: 0,
        total_cost_usd: 0.0,
    }
}

fn sample_authoritative_user(
    user_id: &str,
    is_active: bool,
    is_deleted: bool,
) -> StoredUserAuthRecord {
    StoredUserAuthRecord::new(
        user_id.to_string(),
        Some(format!("{user_id}@example.com")),
        true,
        format!("owner-{user_id}"),
        Some("server-managed-password-hash".to_string()),
        "admin".to_string(),
        "oauth".to_string(),
        Some(serde_json::json!(["openai"])),
        Some(serde_json::json!(["openai:chat"])),
        Some(serde_json::json!(["gpt-5"])),
        is_active,
        is_deleted,
        None,
        None,
    )
    .expect("authoritative user should build")
    .with_security_version(37)
    .expect("security version should be valid")
}

#[tokio::test]
async fn authoritative_owner_sync_allows_first_key_without_synthesizing_owner_fields() {
    let repository = InMemoryAuthApiKeySnapshotRepository::default();
    let user = sample_authoritative_user("authoritative-user", true, false);
    repository
        .synchronize_user_api_key_owner_for_tests(&user)
        .await
        .expect("authoritative owner should synchronize");

    let mut record = sample_create_user_api_key_record("authoritative-user", "authoritative-key");
    record.allowed_providers = Some(vec!["anthropic".to_string()]);
    repository
        .create_user_api_key(record)
        .await
        .expect("first key creation should resolve")
        .expect("active authoritative owner should allow its first key");

    let snapshot = repository
        .find_api_key_snapshot(AuthApiKeyLookupKey::ApiKeyId("authoritative-key"))
        .await
        .expect("created snapshot lookup should resolve")
        .expect("created snapshot should exist");
    assert_eq!(snapshot.username, user.username);
    assert_eq!(snapshot.email, user.email);
    assert_eq!(snapshot.user_role, "admin");
    assert_eq!(snapshot.user_auth_source, "oauth");
    assert!(snapshot.user_is_active);
    assert!(!snapshot.user_is_deleted);
    assert_eq!(
        snapshot.user_allowed_providers,
        Some(vec!["openai".to_string()])
    );
    assert_eq!(
        snapshot.api_key_allowed_providers,
        Some(vec!["anthropic".to_string()])
    );
    assert_eq!(user.security_version, 37);
}

#[tokio::test]
async fn api_key_ciphertext_cas_fences_complete_identity_and_exact_old_value() {
    let repository = InMemoryAuthApiKeySnapshotRepository::default();
    repository
        .synchronize_user_api_key_owner_for_tests(&sample_authoritative_user(
            "cipher-owner",
            true,
            false,
        ))
        .await
        .expect("owner should synchronize");
    repository
        .create_user_api_key(sample_create_user_api_key_record(
            "cipher-owner",
            "cipher-key",
        ))
        .await
        .expect("key creation should succeed")
        .expect("key should be created");

    let expected = CompareAndSwapAuthApiKeyCiphertext {
        user_id: "cipher-owner".to_string(),
        api_key_id: "cipher-key".to_string(),
        key_hash: "hash-cipher-key".to_string(),
        is_standalone: false,
        expected_key_encrypted: "encrypted-cipher-key".to_string(),
        key_encrypted: "bound-ciphertext".to_string(),
    };
    for mutation in [
        CompareAndSwapAuthApiKeyCiphertext {
            user_id: "other-owner".to_string(),
            ..expected.clone()
        },
        CompareAndSwapAuthApiKeyCiphertext {
            key_hash: "other-hash".to_string(),
            ..expected.clone()
        },
        CompareAndSwapAuthApiKeyCiphertext {
            is_standalone: true,
            ..expected.clone()
        },
        CompareAndSwapAuthApiKeyCiphertext {
            expected_key_encrypted: "ENCRYPTED-cipher-key".to_string(),
            ..expected.clone()
        },
    ] {
        assert!(!repository
            .compare_and_swap_api_key_ciphertext(&mutation)
            .await
            .expect("CAS should execute"));
    }
    assert!(repository
        .compare_and_swap_api_key_ciphertext(&expected)
        .await
        .expect("matching CAS should execute"));
    assert_eq!(
        repository
            .list_export_api_keys_by_ids(&["cipher-key".to_string()])
            .await
            .expect("key should reload")[0]
            .key_encrypted
            .as_deref(),
        Some("bound-ciphertext")
    );
    assert!(!repository
        .compare_and_swap_api_key_ciphertext(&expected)
        .await
        .expect("stale CAS should execute"));
}

#[tokio::test]
async fn authoritative_owner_sync_preserves_inactive_state_and_rejects_deleted_owner() {
    let repository = InMemoryAuthApiKeySnapshotRepository::default();
    let inactive = sample_authoritative_user("inactive-user", false, false);
    repository
        .synchronize_user_api_key_owner_for_tests(&inactive)
        .await
        .expect("inactive owner should synchronize without privilege elevation");
    repository
        .create_user_api_key(sample_create_user_api_key_record(
            "inactive-user",
            "inactive-key",
        ))
        .await
        .expect("inactive owner creation should resolve")
        .expect("low-level repository should retain database write parity");
    let inactive_snapshot = repository
        .find_api_key_snapshot(AuthApiKeyLookupKey::ApiKeyId("inactive-key"))
        .await
        .expect("inactive key lookup should resolve")
        .expect("inactive key should be stored");
    assert!(!inactive_snapshot.user_is_active);
    assert!(!inactive_snapshot.is_currently_usable(0));

    let deleted = sample_authoritative_user("deleted-user", true, true);
    repository
        .synchronize_user_api_key_owner_for_tests(&deleted)
        .await
        .expect("deleted owner tombstone should synchronize");
    assert!(repository
        .create_user_api_key(sample_create_user_api_key_record(
            "deleted-user",
            "deleted-key",
        ))
        .await
        .expect("deleted owner creation should resolve")
        .is_none());
    assert!(repository
        .find_api_key_snapshot(AuthApiKeyLookupKey::ApiKeyId("deleted-key"))
        .await
        .expect("rejected deleted key lookup should resolve")
        .is_none());
}

#[tokio::test]
async fn create_user_api_key_persists_feature_settings_in_initial_record() {
    let repository = InMemoryAuthApiKeySnapshotRepository::default()
        .with_owner_snapshots([sample_snapshot("owner-fixture", "user-1")]);
    let mut record = sample_create_user_api_key_record("user-1", "key-created");
    record.feature_settings = Some(serde_json::json!({"compact": true}));
    let created = repository
        .create_user_api_key(record)
        .await
        .expect("create should succeed")
        .expect("created key should be returned");

    assert_eq!(
        created.feature_settings,
        Some(serde_json::json!({"compact": true}))
    );
    assert!(repository
        .find_api_key_snapshot(AuthApiKeyLookupKey::ApiKeyId("owner-fixture"))
        .await
        .expect("owner fixture lookup should succeed")
        .is_none());
}

#[tokio::test]
async fn create_user_api_key_rejects_unknown_deleted_and_conflicted_owners() {
    let mut deleted_owner = sample_snapshot("deleted-owner-fixture", "deleted-user");
    deleted_owner.user_is_deleted = true;
    let conflicted_owner = sample_snapshot("conflicted-owner-fixture-a", "conflicted-user");
    let mut conflicting_owner = sample_snapshot("conflicted-owner-fixture-b", "conflicted-user");
    conflicting_owner.username = "different-owner-state".to_string();
    let cases = [
        (
            "unknown",
            "unknown-user",
            InMemoryAuthApiKeySnapshotRepository::default(),
        ),
        (
            "deleted",
            "deleted-user",
            InMemoryAuthApiKeySnapshotRepository::default().with_owner_snapshots([deleted_owner]),
        ),
        (
            "conflicted",
            "conflicted-user",
            InMemoryAuthApiKeySnapshotRepository::default()
                .with_owner_snapshots([conflicted_owner, conflicting_owner]),
        ),
    ];

    for (case, user_id, repository) in cases {
        let api_key_id = format!("key-{case}");
        let key_hash = format!("hash-{api_key_id}");
        let record = sample_create_user_api_key_record(user_id, &api_key_id);
        assert!(repository
            .create_user_api_key(record)
            .await
            .expect("fail-closed creation should resolve")
            .is_none());
        assert!(repository
            .find_api_key_snapshot(AuthApiKeyLookupKey::ApiKeyId(&api_key_id))
            .await
            .expect("rejected key lookup should resolve")
            .is_none());
        assert!(repository
            .find_api_key_snapshot(AuthApiKeyLookupKey::KeyHash(&key_hash))
            .await
            .expect("rejected hash lookup should resolve")
            .is_none());
        assert!(repository
            .list_export_api_keys_by_user_ids(&[user_id.to_string()])
            .await
            .expect("rejected exports should list")
            .is_empty());
    }
}

#[tokio::test]
async fn create_user_api_key_preserves_disabled_owner_state() {
    let mut disabled_owner = sample_snapshot("disabled-owner-fixture", "disabled-user");
    disabled_owner.user_is_active = false;
    let repository =
        InMemoryAuthApiKeySnapshotRepository::default().with_owner_snapshots([disabled_owner]);

    repository
        .create_user_api_key(sample_create_user_api_key_record(
            "disabled-user",
            "key-disabled",
        ))
        .await
        .expect("disabled-owner creation should resolve")
        .expect("non-deleted disabled owner should retain database parity");

    let snapshot = repository
        .find_api_key_snapshot(AuthApiKeyLookupKey::ApiKeyId("key-disabled"))
        .await
        .expect("created key lookup should resolve")
        .expect("created key snapshot should exist");
    assert!(!snapshot.user_is_active);
    assert!(!snapshot.is_currently_usable(0));
}

#[tokio::test]
async fn invalid_owner_cannot_probe_duplicate_credential_indexes() {
    let repository = InMemoryAuthApiKeySnapshotRepository::seed([(
        Some("hash-key-existing".to_string()),
        sample_snapshot("key-existing", "trusted-user"),
    )]);

    assert!(repository
        .create_user_api_key(sample_create_user_api_key_record(
            "unknown-user",
            "key-existing",
        ))
        .await
        .expect("invalid owner should fail closed before duplicate checks")
        .is_none());

    let existing = repository
        .find_api_key_snapshot(AuthApiKeyLookupKey::ApiKeyId("key-existing"))
        .await
        .expect("existing key lookup should resolve")
        .expect("existing key should remain unchanged");
    assert_eq!(existing.user_id, "trusted-user");
}

#[tokio::test]
async fn usage_total_replacement_rejects_unpersistable_values_without_mutation() {
    let repository = InMemoryAuthApiKeySnapshotRepository::seed([(
        Some("hash-key-existing".to_string()),
        sample_snapshot("key-existing", "trusted-user"),
    )]);
    let before = repository
        .list_export_api_keys_by_ids(&["key-existing".to_string()])
        .await
        .expect("existing export should load")
        .pop()
        .expect("existing export should exist");

    for (total_requests, total_tokens, total_cost_usd) in [
        (u64::MAX, 0, 0.0),
        (0, u64::MAX, 0.0),
        (0, 0, f64::NAN),
        (0, 0, f64::INFINITY),
    ] {
        assert!(matches!(
            repository
                .set_api_key_usage_totals(
                    "key-existing",
                    total_requests,
                    total_tokens,
                    total_cost_usd,
                )
                .await,
            Err(crate::DataLayerError::InvalidInput(_))
        ));
    }

    let after = repository
        .list_export_api_keys_by_ids(&["key-existing".to_string()])
        .await
        .expect("existing export should reload")
        .pop()
        .expect("existing export should remain");
    assert_eq!(after, before);
}

#[tokio::test]
async fn unlocked_user_mutations_atomically_reject_locked_keys() {
    let mut snapshot = sample_snapshot("key-locked", "user-1");
    snapshot.api_key_is_locked = true;
    let repository = InMemoryAuthApiKeySnapshotRepository::seed(vec![(
        Some("hash-locked".to_string()),
        snapshot,
    )]);

    assert!(repository
        .update_user_api_key_basic_if_unlocked(UpdateUserApiKeyBasicRecord {
            user_id: "user-1".to_string(),
            api_key_id: "key-locked".to_string(),
            key_encrypted: None,
            key_encrypted_present: false,
            name: Some("must-not-change".to_string()),
            name_present: true,
            rate_limit: None,
            rate_limit_present: false,
            concurrent_limit: None,
            concurrent_limit_present: false,
            ip_rules: None,
            feature_settings: Some(Some(serde_json::json!({"must_not_change": true}))),
            routing_group_selection: None,
        })
        .await
        .expect("locked basic update should resolve")
        .is_none());
    assert!(repository
        .set_user_api_key_active_if_unlocked("user-1", "key-locked", false)
        .await
        .expect("locked status update should resolve")
        .is_none());
    assert!(repository
        .set_user_api_key_allowed_providers_if_unlocked(
            "user-1",
            "key-locked",
            Some(vec!["must-not-change".to_string()]),
        )
        .await
        .expect("locked provider update should resolve")
        .is_none());
    assert!(repository
        .set_user_api_key_force_capabilities_if_unlocked(
            "user-1",
            "key-locked",
            Some(serde_json::json!({"must_not_change": true})),
        )
        .await
        .expect("locked capability update should resolve")
        .is_none());
    assert!(repository
        .set_user_api_key_feature_settings_if_unlocked(
            "user-1",
            "key-locked",
            Some(serde_json::json!({"must_not_change": true})),
        )
        .await
        .expect("locked feature update should resolve")
        .is_none());
    assert!(!repository
        .delete_user_api_key_if_unlocked("user-1", "key-locked")
        .await
        .expect("locked deletion should resolve"));

    let unchanged = repository
        .list_export_api_keys_by_ids(&["key-locked".to_string()])
        .await
        .expect("locked key should reload")
        .pop()
        .expect("locked key should remain");
    assert_ne!(unchanged.name.as_deref(), Some("must-not-change"));
    assert!(unchanged.is_active);
    assert!(unchanged.feature_settings.is_none());

    // Administrative repository operations deliberately retain authority
    // over locked keys; only the self-service variants enforce the fence.
    let admin_updated = repository
        .update_user_api_key_basic(UpdateUserApiKeyBasicRecord {
            user_id: "user-1".to_string(),
            api_key_id: "key-locked".to_string(),
            key_encrypted: None,
            key_encrypted_present: false,
            name: Some("admin-change".to_string()),
            name_present: true,
            rate_limit: None,
            rate_limit_present: false,
            concurrent_limit: None,
            concurrent_limit_present: false,
            ip_rules: None,
            feature_settings: Some(Some(serde_json::json!({"admin": true}))),
            routing_group_selection: None,
        })
        .await
        .expect("administrator update should resolve")
        .expect("administrator may update a locked key");
    assert_eq!(admin_updated.name.as_deref(), Some("admin-change"));
    assert_eq!(
        admin_updated.feature_settings,
        Some(serde_json::json!({"admin": true}))
    );
    assert!(repository
        .set_user_api_key_active("user-1", "key-locked", false)
        .await
        .expect("admin status update should resolve")
        .is_some());
}

#[tokio::test]
async fn standalone_flags_cover_export_only_keys_and_remove_deleted_keys() {
    let mut standalone = sample_snapshot("standalone-key", "user-1");
    standalone.api_key_is_standalone = true;
    let repository = InMemoryAuthApiKeySnapshotRepository::seed([
        (None, sample_snapshot("member-key", "user-1")),
        (None, standalone),
    ]);
    let mut export = repository
        .list_export_api_keys_by_ids(&["standalone-key".into()])
        .await
        .unwrap()
        .remove(0);
    export.api_key_id = "export-only-key".into();
    let repository = repository.with_export_records([export]);

    assert_eq!(
        repository.standalone_flags(),
        std::collections::BTreeMap::from([
            ("member-key".into(), false),
            ("standalone-key".into(), true),
            ("export-only-key".into(), true),
        ])
    );
    assert!(repository
        .delete_standalone_api_key("standalone-key")
        .await
        .unwrap());
    assert_eq!(
        repository.standalone_flags(),
        std::collections::BTreeMap::from([
            ("member-key".into(), false),
            ("export-only-key".into(), true),
        ])
    );
}

#[tokio::test]
async fn reads_auth_snapshot_by_all_supported_keys() {
    let repository = InMemoryAuthApiKeySnapshotRepository::seed(vec![(
        Some("hash-1".to_string()),
        sample_snapshot("key-1", "user-1"),
    )]);

    assert!(repository
        .find_api_key_snapshot(AuthApiKeyLookupKey::KeyHash("hash-1"))
        .await
        .expect("find by hash should succeed")
        .is_some());
    assert!(repository
        .find_api_key_snapshot(AuthApiKeyLookupKey::ApiKeyId("key-1"))
        .await
        .expect("find by api key id should succeed")
        .is_some());
    assert!(repository
        .find_api_key_snapshot(AuthApiKeyLookupKey::UserApiKeyIds {
            user_id: "user-1",
            api_key_id: "key-1",
        })
        .await
        .expect("find by user/api key ids should succeed")
        .is_some());
    let snapshots = repository
        .list_api_key_snapshots_by_ids(&["key-1".to_string(), "missing".to_string()])
        .await
        .expect("batch lookup should succeed");
    assert_eq!(snapshots.len(), 1);
    assert_eq!(snapshots[0].api_key_id, "key-1");
}

#[tokio::test]
async fn touches_last_used_for_existing_key() {
    let repository = InMemoryAuthApiKeySnapshotRepository::seed(vec![(
        Some("hash-1".to_string()),
        sample_snapshot("key-1", "user-1"),
    )]);

    assert!(repository
        .touch_last_used_at("key-1")
        .await
        .expect("touch should succeed"));
    assert_eq!(repository.touch_count("key-1"), 1);
    assert!(!repository
        .touch_last_used_at("missing")
        .await
        .expect("missing touch should succeed"));
}

#[tokio::test]
async fn delete_is_owner_scoped_and_removes_all_credential_indexes() {
    let repository = InMemoryAuthApiKeySnapshotRepository::seed(vec![(
        Some("hash-1".to_string()),
        sample_snapshot("key-1", "user-1"),
    )]);

    assert!(repository
        .find_api_key_snapshot(AuthApiKeyLookupKey::KeyHash("hash-1"))
        .await
        .expect("hash lookup should succeed")
        .is_some());
    assert!(repository
        .find_api_key_snapshot(AuthApiKeyLookupKey::ApiKeyId("key-1"))
        .await
        .expect("id lookup should succeed")
        .is_some());
    assert!(repository
        .touch_last_used_at("key-1")
        .await
        .expect("touch should succeed"));

    assert!(!repository
        .delete_user_api_key("other-user", "key-1")
        .await
        .expect("wrong-owner delete should resolve"));
    assert!(repository
        .find_api_key_snapshot(AuthApiKeyLookupKey::ApiKeyId("key-1"))
        .await
        .expect("wrong-owner delete must preserve the key")
        .is_some());

    assert!(repository
        .delete_user_api_key("user-1", "key-1")
        .await
        .expect("owner-scoped delete should succeed"));
    assert!(repository
        .find_api_key_snapshot(AuthApiKeyLookupKey::ApiKeyId("key-1"))
        .await
        .expect("deleted id lookup should resolve")
        .is_none());
    assert!(repository
        .find_api_key_snapshot(AuthApiKeyLookupKey::KeyHash("hash-1"))
        .await
        .expect("deleted hash lookup should resolve")
        .is_none());
    assert_eq!(repository.touch_count("key-1"), 0);
    assert_eq!(repository.snapshot_lookup_count("key-1"), 1);
    assert_eq!(repository.key_hash_lookup_count("hash-1"), 1);
}

#[tokio::test]
async fn lists_export_records_for_user_bound_and_standalone_keys() {
    let repository = InMemoryAuthApiKeySnapshotRepository::seed(vec![
        (
            Some("hash-user".to_string()),
            sample_snapshot("key-user", "user-1"),
        ),
        (
            Some("hash-standalone".to_string()),
            sample_snapshot("key-standalone", "admin-1"),
        ),
    ])
    .with_export_records(vec![
        StoredAuthApiKeyExportRecord::new(
            "user-1".to_string(),
            "key-user".to_string(),
            "hash-user".to_string(),
            Some("enc-user".to_string()),
            Some("default".to_string()),
            Some(serde_json::json!(["openai"])),
            Some(serde_json::json!(["openai:chat"])),
            Some(serde_json::json!(["gpt-5"])),
            Some(120),
            Some(7),
            Some(serde_json::json!({"cache_1h": true})),
            true,
            Some(200),
            false,
            14,
            1_400,
            1.5,
            false,
        )
        .expect("user export record should build"),
        StoredAuthApiKeyExportRecord::new(
            "admin-1".to_string(),
            "key-standalone".to_string(),
            "hash-standalone".to_string(),
            Some("enc-standalone".to_string()),
            Some("standalone".to_string()),
            None,
            None,
            None,
            None,
            Some(1),
            None,
            true,
            None,
            true,
            2,
            25,
            0.25,
            true,
        )
        .expect("standalone export record should build"),
    ]);

    let user_records = repository
        .list_export_api_keys_by_user_ids(&["user-1".to_string()])
        .await
        .expect("user export lookup should succeed");
    assert_eq!(user_records.len(), 1);
    assert_eq!(user_records[0].api_key_id, "key-user");
    assert_eq!(user_records[0].key_encrypted.as_deref(), Some("enc-user"));
    assert_eq!(user_records[0].total_requests, 14);

    let standalone_records = repository
        .list_export_standalone_api_keys()
        .await
        .expect("standalone export lookup should succeed");
    assert_eq!(standalone_records.len(), 1);
    assert_eq!(standalone_records[0].api_key_id, "key-standalone");
    assert!(standalone_records[0].is_standalone);

    let selected_records = repository
        .list_export_api_keys_by_ids(&[
            "key-standalone".to_string(),
            "missing".to_string(),
            "key-user".to_string(),
        ])
        .await
        .expect("api key id export lookup should succeed");
    assert_eq!(selected_records.len(), 2);
    assert_eq!(selected_records[0].api_key_id, "key-standalone");
    assert_eq!(selected_records[1].api_key_id, "key-user");

    let paged_records = repository
        .list_export_standalone_api_keys_page(&StandaloneApiKeyExportListQuery {
            skip: 0,
            limit: 10,
            is_active: Some(true),
        })
        .await
        .expect("standalone export page should succeed");
    assert_eq!(paged_records.len(), 1);
    assert_eq!(paged_records[0].api_key_id, "key-standalone");
    assert_eq!(
        repository
            .count_export_standalone_api_keys(Some(true))
            .await
            .expect("standalone export count should succeed"),
        1
    );
}

#[tokio::test]
async fn user_key_feature_and_group_updates_merge_against_the_locked_current_record() {
    use super::super::UpdateApiKeyRoutingGroupSelection;

    fn patch() -> UpdateUserApiKeyBasicRecord {
        UpdateUserApiKeyBasicRecord {
            user_id: "user-1".into(),
            api_key_id: "key-1".into(),
            key_encrypted: None,
            key_encrypted_present: false,
            name: None,
            name_present: false,
            rate_limit: None,
            rate_limit_present: false,
            concurrent_limit: None,
            concurrent_limit_present: false,
            ip_rules: None,
            feature_settings: None,
            routing_group_selection: Some(UpdateApiKeyRoutingGroupSelection { group_id: None }),
        }
    }

    // Both repository entry points must apply the same merge, with the
    // self-service entry point additionally fencing locked keys.
    for require_unlocked in [false, true] {
        let repository = InMemoryAuthApiKeySnapshotRepository::seed([(
            None,
            sample_snapshot("key-1", "user-1"),
        )]);
        repository.set_user_api_key_feature_settings("user-1", "key-1", Some(serde_json::json!({
            "routing_group_id": "group-a", "routing_group_name": "stale-name", "pii": {"enabled": false},
        }))).await.unwrap().unwrap();
        async fn apply(
            repository: &InMemoryAuthApiKeySnapshotRepository,
            record: UpdateUserApiKeyBasicRecord,
            require_unlocked: bool,
        ) -> StoredAuthApiKeyExportRecord {
            if require_unlocked {
                repository
                    .update_user_api_key_basic_if_unlocked(record)
                    .await
                    .unwrap()
                    .unwrap()
            } else {
                repository
                    .update_user_api_key_basic(record)
                    .await
                    .unwrap()
                    .unwrap()
            }
        }

        // The PII request is prepared while A is selected, but another
        // request selects B before that prepared replacement is committed.
        let mut prepared_pii = patch();
        prepared_pii.feature_settings = Some(Some(serde_json::json!({
            "pii": {"enabled": true}, "routing_group_id": "group-a", "routing_group_name": "injected-name",
        })));
        let mut select_b = patch();
        select_b.routing_group_selection.as_mut().unwrap().group_id = Some(Some("group-b".into()));
        apply(&repository, select_b, require_unlocked).await;
        let merged = apply(&repository, prepared_pii, require_unlocked).await;
        assert_eq!(
            merged.feature_settings,
            Some(serde_json::json!({
                "pii": {"enabled": true}, "routing_group_id": "group-b",
            }))
        );

        // Conversely a group-only request prepared before a feature change
        // must preserve the latest feature object when it reaches storage.
        let mut prepared_group = patch();
        prepared_group
            .routing_group_selection
            .as_mut()
            .unwrap()
            .group_id = Some(Some("group-c".into()));
        let mut latest_pii = patch();
        latest_pii.feature_settings = Some(Some(
            serde_json::json!({ "pii": { "enabled": false, "mode": "strict" } }),
        ));
        apply(&repository, latest_pii, require_unlocked).await;
        let merged = apply(&repository, prepared_group, require_unlocked).await;
        assert_eq!(
            merged.feature_settings,
            Some(serde_json::json!({
                "pii": {"enabled": false, "mode": "strict"}, "routing_group_id": "group-c",
            }))
        );

        let mut clear_features = patch();
        clear_features.feature_settings = Some(None);
        let cleared = apply(&repository, clear_features, require_unlocked).await;
        assert_eq!(
            cleared.feature_settings,
            Some(serde_json::json!({"routing_group_id": "group-c"}))
        );
        let mut clear_group = patch();
        clear_group
            .routing_group_selection
            .as_mut()
            .unwrap()
            .group_id = Some(None);
        assert!(apply(&repository, clear_group, require_unlocked)
            .await
            .feature_settings
            .is_none());

        // Administrative callers can still replace the complete document.
        let mut admin = patch();
        admin.routing_group_selection = None;
        admin.feature_settings = Some(Some(
            serde_json::json!({ "routing_group_id": "admin-group", "admin": true }),
        ));
        assert_eq!(
            apply(&repository, admin, require_unlocked)
                .await
                .feature_settings,
            Some(serde_json::json!({ "routing_group_id": "admin-group", "admin": true }))
        );
    }
}

#[tokio::test]
async fn update_user_api_key_basic_updates_concurrent_limit() {
    let repository = InMemoryAuthApiKeySnapshotRepository::seed(vec![(
        Some("hash-1".to_string()),
        sample_snapshot("key-1", "user-1"),
    )]);

    let updated = repository
        .update_user_api_key_basic(UpdateUserApiKeyBasicRecord {
            user_id: "user-1".to_string(),
            api_key_id: "key-1".to_string(),
            key_encrypted: None,
            key_encrypted_present: false,
            name: None,
            name_present: false,
            rate_limit: None,
            rate_limit_present: false,
            concurrent_limit: Some(11),
            concurrent_limit_present: true,
            ip_rules: None,
            feature_settings: None,
            routing_group_selection: None,
        })
        .await
        .expect("update should succeed")
        .expect("record should exist");
    assert_eq!(updated.concurrent_limit, Some(11));

    let snapshot = repository
        .find_api_key_snapshot(AuthApiKeyLookupKey::ApiKeyId("key-1"))
        .await
        .expect("find should succeed")
        .expect("snapshot should exist");
    assert_eq!(snapshot.api_key_concurrent_limit, Some(11));
}

#[tokio::test]
async fn update_user_api_key_basic_restores_nullable_values_and_zero() {
    let repository = InMemoryAuthApiKeySnapshotRepository::seed(vec![(
        Some("hash-1".to_string()),
        sample_snapshot("key-1", "user-1"),
    )]);

    let cleared = repository
        .update_user_api_key_basic(UpdateUserApiKeyBasicRecord {
            user_id: "user-1".to_string(),
            api_key_id: "key-1".to_string(),
            key_encrypted: None,
            key_encrypted_present: true,
            name: None,
            name_present: true,
            rate_limit: None,
            rate_limit_present: true,
            concurrent_limit: None,
            concurrent_limit_present: true,
            ip_rules: None,
            feature_settings: None,
            routing_group_selection: None,
        })
        .await
        .expect("nullable values should clear")
        .expect("record should exist");
    assert!(cleared.name.is_none());
    assert!(cleared.rate_limit.is_none());
    assert!(cleared.concurrent_limit.is_none());

    let zero = repository
        .update_user_api_key_basic(UpdateUserApiKeyBasicRecord {
            user_id: "user-1".to_string(),
            api_key_id: "key-1".to_string(),
            key_encrypted: None,
            key_encrypted_present: false,
            name: None,
            name_present: false,
            rate_limit: Some(0),
            rate_limit_present: true,
            concurrent_limit: None,
            concurrent_limit_present: false,
            ip_rules: None,
            feature_settings: None,
            routing_group_selection: None,
        })
        .await
        .expect("zero rate limit should persist")
        .expect("record should exist");
    assert_eq!(zero.rate_limit, Some(0));
}

#[tokio::test]
async fn update_standalone_api_key_basic_updates_concurrent_limit_when_present() {
    let mut standalone = sample_snapshot("key-standalone", "admin-1");
    standalone.api_key_is_standalone = true;
    let repository = InMemoryAuthApiKeySnapshotRepository::seed(vec![(
        Some("hash-standalone".to_string()),
        standalone,
    )]);

    let updated = repository
        .update_standalone_api_key_basic(UpdateStandaloneApiKeyBasicRecord {
            api_key_id: "key-standalone".to_string(),
            key_encrypted: None,
            key_encrypted_present: false,
            name: None,
            name_present: false,
            force_capabilities: None,
            rate_limit_present: false,
            rate_limit: None,
            concurrent_limit_present: true,
            concurrent_limit: Some(13),
            allowed_providers: None,
            allowed_api_formats: None,
            allowed_models: None,
            ip_rules: None,
            expires_at_present: false,
            expires_at_unix_secs: None,
            auto_delete_on_expiry_present: false,
            auto_delete_on_expiry: false,
        })
        .await
        .expect("update should succeed")
        .expect("record should exist");
    assert_eq!(updated.concurrent_limit, Some(13));

    let snapshot = repository
        .find_api_key_snapshot(AuthApiKeyLookupKey::ApiKeyId("key-standalone"))
        .await
        .expect("find should succeed")
        .expect("snapshot should exist");
    assert_eq!(snapshot.api_key_concurrent_limit, Some(13));
}

#[tokio::test]
async fn restores_standalone_nullable_fields_and_force_capabilities_atomically() {
    let mut standalone = sample_snapshot("key-standalone", "admin-1");
    standalone.api_key_is_standalone = true;
    let before = StoredAuthApiKeyExportRecord::new(
        "admin-1".to_string(),
        "key-standalone".to_string(),
        "hash-standalone".to_string(),
        None,
        None,
        Some(serde_json::json!(["openai"])),
        Some(serde_json::json!(["openai:chat"])),
        Some(serde_json::json!(["gpt-4.1"])),
        Some(60),
        Some(5),
        None,
        true,
        Some(200),
        false,
        2,
        25,
        0.25,
        true,
    )
    .expect("before export should build")
    .with_ip_rules(Some(serde_json::json!(["10.0.0.0/24"])))
    .expect("before ip rules should build")
    .with_feature_settings(Some(serde_json::json!({"compact": true})));
    let repository = InMemoryAuthApiKeySnapshotRepository::seed(vec![(
        Some("hash-standalone".to_string()),
        standalone,
    )])
    .with_export_records([before.clone()]);

    let after = repository
        .update_standalone_api_key_basic(UpdateStandaloneApiKeyBasicRecord {
            api_key_id: "key-standalone".to_string(),
            key_encrypted: Some("enc-after".to_string()),
            key_encrypted_present: true,
            name: Some("after".to_string()),
            name_present: true,
            force_capabilities: Some(Some(serde_json::json!({"vision": true}))),
            rate_limit_present: true,
            rate_limit: Some(99),
            concurrent_limit_present: true,
            concurrent_limit: Some(8),
            allowed_providers: Some(Some(vec!["anthropic".to_string()])),
            allowed_api_formats: None,
            allowed_models: None,
            ip_rules: None,
            expires_at_present: false,
            expires_at_unix_secs: None,
            auto_delete_on_expiry_present: false,
            auto_delete_on_expiry: false,
        })
        .await
        .expect("after update should succeed")
        .expect("after record should exist");
    assert!(repository
        .restore_api_key_if_matches(&after, &before)
        .await
        .expect("restore should succeed"));
    let restored = repository
        .list_export_api_keys_by_ids(&["key-standalone".to_string()])
        .await
        .expect("restored export should load")
        .pop()
        .expect("restored key should exist");
    assert_eq!(restored, before);

    // A changed post-state must make the CAS fail without touching the newer value.
    let concurrent = repository
        .update_standalone_api_key_basic(UpdateStandaloneApiKeyBasicRecord {
            api_key_id: "key-standalone".to_string(),
            key_encrypted: None,
            key_encrypted_present: false,
            name: Some("concurrent".to_string()),
            name_present: true,
            force_capabilities: Some(None),
            rate_limit_present: false,
            rate_limit: None,
            concurrent_limit_present: false,
            concurrent_limit: None,
            allowed_providers: None,
            allowed_api_formats: None,
            allowed_models: None,
            ip_rules: None,
            expires_at_present: false,
            expires_at_unix_secs: None,
            auto_delete_on_expiry_present: false,
            auto_delete_on_expiry: false,
        })
        .await
        .expect("concurrent update should succeed")
        .expect("concurrent record should exist");
    assert!(!repository
        .restore_api_key_if_matches(&after, &before)
        .await
        .expect("conflicting restore should return false"));
    assert_eq!(
        repository
            .list_export_api_keys_by_ids(&["key-standalone".to_string()])
            .await
            .expect("current export should load")
            .pop()
            .expect("current key should exist")
            .name,
        concurrent.name
    );
}
