use super::InMemoryProviderCatalogReadRepository;
use crate::repository::provider_catalog::{
    ProviderCatalogKeyAdaptiveState, ProviderCatalogKeyAdaptiveStateUpdate,
    ProviderCatalogKeyAdminCasUpdate, ProviderCatalogKeyHealthStateUpdate,
    ProviderCatalogKeyListOrder, ProviderCatalogKeyListQuery,
    ProviderCatalogKeyOAuthCredentialCasDelete, ProviderCatalogKeyOAuthCredentialFence,
    ProviderCatalogKeyOAuthRuntimeStateCasUpdate, ProviderCatalogKeyRuntimeMetadataUpdate,
    ProviderCatalogReadRepository, ProviderCatalogUpstreamMetadataNamespaceExpectation,
    ProviderCatalogWriteRepository, StoredProviderCatalogEndpoint, StoredProviderCatalogKey,
    StoredProviderCatalogProvider,
};
use crate::repository::usage::ProviderApiKeyUsageDelta;
use crate::DataLayerError;
use serde_json::{json, Value};
use std::sync::Arc;
use tokio::sync::Barrier;

fn sample_provider(id: &str) -> StoredProviderCatalogProvider {
    StoredProviderCatalogProvider::new(
        id.to_string(),
        format!("provider-{id}"),
        Some("https://example.com".to_string()),
        "custom".to_string(),
    )
    .expect("provider should build")
}

fn sample_endpoint(id: &str, provider_id: &str) -> StoredProviderCatalogEndpoint {
    StoredProviderCatalogEndpoint::new(
        id.to_string(),
        provider_id.to_string(),
        "openai:chat".to_string(),
        Some("openai".to_string()),
        Some("chat".to_string()),
        true,
    )
    .expect("endpoint should build")
    .with_health_score(0.9)
}

fn sample_key(id: &str, provider_id: &str) -> StoredProviderCatalogKey {
    StoredProviderCatalogKey::new(
        id.to_string(),
        provider_id.to_string(),
        "default".to_string(),
        "api_key".to_string(),
        Some(serde_json::json!({"cache_1h": true})),
        true,
    )
    .expect("key should build")
}

#[tokio::test]
async fn scoped_provider_creation_is_atomic_and_rejects_stale_group_updates() {
    use crate::repository::routing_profiles::InMemoryRoutingGroupRepository;
    use aether_data_contracts::repository::routing_profiles::{
        CreateRoutingGroupRecord, RoutingGroupReadRepository, RoutingGroupWriteRepository,
        UpdateRoutingGroupRecord,
    };
    let groups = Arc::new(InMemoryRoutingGroupRepository::default());
    for id in ["selected", "other", "disabled"] {
        groups
            .create_routing_group(CreateRoutingGroupRecord {
                id: id.into(),
                name: id.into(),
                description: None,
                enabled: id != "disabled",
                is_system_default: id == "selected",
                sort_order: 0,
                config_json: json!({"disabled_providers": ["already-disabled"], "rules": []}),
                version: 1,
                created_at: 1,
                updated_at: 1,
                published_at: None,
            })
            .await
            .unwrap();
    }
    let repository =
        InMemoryProviderCatalogReadRepository::default().with_routing_groups(groups.clone());
    let provider = sample_provider("new");
    assert!(repository
        .create_provider_in_routing_group(&provider, Some(0), "missing")
        .await
        .is_err());
    assert!(repository.list_providers(false).await.unwrap().is_empty());
    assert!(groups
        .list_routing_groups()
        .await
        .unwrap()
        .iter()
        .all(|group| group.version == 1));

    repository
        .create_provider_in_routing_group(&provider, None, "selected")
        .await
        .unwrap();
    for group in groups.list_routing_groups().await.unwrap() {
        assert_eq!(group.version, if group.id == "selected" { 1 } else { 2 });
        assert_eq!(
            group.config_json["disabled_providers"],
            if group.id == "selected" {
                json!(["already-disabled"])
            } else {
                json!(["already-disabled", "new"])
            }
        );
    }
    let before = groups.list_routing_groups().await.unwrap();
    assert!(repository
        .create_provider_in_routing_group(&provider, Some(0), "other")
        .await
        .is_err());
    assert_eq!(groups.list_routing_groups().await.unwrap(), before);
    let stale = groups
        .update_routing_group(
            "other",
            UpdateRoutingGroupRecord {
                expected_version: Some(1),
                config_json: Some(json!({"disabled_providers": []})),
                ..Default::default()
            },
        )
        .await;
    assert!(
        matches!(stale, Err(DataLayerError::InvalidInput(message)) if message == "routing_group_version_conflict")
    );
    assert_eq!(groups.list_routing_groups().await.unwrap(), before);
    let updated = groups
        .update_routing_group(
            "other",
            UpdateRoutingGroupRecord {
                expected_version: Some(2),
                config_json: Some(json!({"disabled_providers": ["already-disabled"]})),
                version: Some(2),
                ..Default::default()
            },
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(updated.version, 3);
    assert_eq!(
        updated.config_json["disabled_providers"],
        json!(["already-disabled"])
    );
}

#[tokio::test]
async fn reads_provider_catalog_items_by_id() {
    let repository = InMemoryProviderCatalogReadRepository::seed(
        vec![sample_provider("provider-1")],
        vec![sample_endpoint("endpoint-1", "provider-1")],
        vec![sample_key("key-1", "provider-1")],
    );

    assert_eq!(
        repository
            .list_providers_by_ids(&["provider-1".to_string()])
            .await
            .expect("providers should read")
            .len(),
        1
    );
    assert_eq!(
        repository
            .list_endpoints_by_ids(&["endpoint-1".to_string()])
            .await
            .expect("endpoints should read")
            .len(),
        1
    );
    assert_eq!(
        repository
            .list_keys_by_ids(&["key-1".to_string()])
            .await
            .expect("keys should read")
            .len(),
        1
    );
}

#[tokio::test]
async fn lists_active_providers_in_priority_order() {
    let repository = InMemoryProviderCatalogReadRepository::seed(
        vec![
            sample_provider("provider-2").with_routing_fields(20),
            sample_provider("provider-1").with_routing_fields(10),
            sample_provider("provider-3")
                .with_routing_fields(5)
                .with_transport_fields(false, false, false, None, None, None, None, None, None),
        ],
        vec![],
        vec![],
    );

    let providers = repository
        .list_providers(true)
        .await
        .expect("providers should list");
    assert_eq!(
        providers
            .iter()
            .map(|provider| provider.id.as_str())
            .collect::<Vec<_>>(),
        vec!["provider-1", "provider-2"]
    );
}

#[tokio::test]
async fn unfenced_oauth_runtime_state_update_preserves_credentials() {
    let repository = InMemoryProviderCatalogReadRepository::seed(
        vec![sample_provider("provider-1")],
        vec![sample_endpoint("endpoint-1", "provider-1")],
        vec![sample_key("key-1", "provider-1")
            .with_transport_fields(
                None,
                "ciphertext-api".to_string(),
                Some("ciphertext-auth".to_string()),
                None,
                None,
                None,
                None,
                None,
                None,
            )
            .expect("key transport should build")],
    );

    assert!(repository
        .update_key_oauth_runtime_state("key-1", Some(123), Some("refresh failed"), Some(456),)
        .await
        .expect("runtime state should update"));

    let stored = repository
        .list_keys_by_ids(&["key-1".to_string()])
        .await
        .expect("key should read")
        .pop()
        .expect("key should exist");
    assert_eq!(stored.encrypted_api_key.as_deref(), Some("ciphertext-api"));
    assert_eq!(
        stored.encrypted_auth_config.as_deref(),
        Some("ciphertext-auth")
    );
    assert_eq!(stored.oauth_invalid_at_unix_secs, Some(123));
    assert_eq!(
        stored.oauth_invalid_reason.as_deref(),
        Some("refresh failed")
    );
}

#[tokio::test]
async fn oauth_runtime_cas_preserves_admin_fields_and_rejects_stale_config() {
    let mut key = sample_key("key-1", "provider-1")
        .with_transport_fields(
            None,
            "ciphertext-placeholder".to_string(),
            Some("ciphertext-auth-1".to_string()),
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .expect("key transport should build");
    key.note = Some("admin-owned-note".to_string());
    key.status_snapshot = Some(json!({"quota":{"remaining":7}}));
    let repository = InMemoryProviderCatalogReadRepository::seed(
        vec![sample_provider("provider-1")],
        vec![],
        vec![key],
    );
    let update = ProviderCatalogKeyOAuthRuntimeStateCasUpdate {
        key_id: "key-1".to_string(),
        expected_encrypted_auth_config: Some("ciphertext-auth-1".to_string()),
        expected_credential: Some(ProviderCatalogKeyOAuthCredentialFence {
            encrypted_api_key: Some("ciphertext-placeholder".to_string()),
            auth_type: "api_key".to_string(),
            provider_id: "provider-1".to_string(),
            provider_type: "custom".to_string(),
        }),
        expected_upstream_metadata_namespace: None,
        encrypted_auth_config: "ciphertext-auth-2".to_string(),
        encrypted_api_key_update: Some("ciphertext-api-2".to_string()),
        expires_at_unix_secs_update: Some(Some(456)),
        oauth_invalid_at_unix_secs: None,
        oauth_invalid_reason: None,
        upstream_metadata_patch: Some(json!({"codex":{"remaining":3}})),
        upstream_metadata_namespace_to_remove: None,
        status_snapshot_patch: json!({"oauth":{"code":"none"}}),
        reset_error_count: false,
        updated_at_unix_secs: Some(123),
    };
    assert!(repository
        .compare_and_update_key_oauth_runtime_state(&update)
        .await
        .expect("CAS should succeed"));
    assert!(!repository
        .compare_and_update_key_oauth_runtime_state(&update)
        .await
        .expect("stale CAS should not fail"));
    let stored = repository
        .list_keys_by_ids(&["key-1".to_string()])
        .await
        .expect("key should read")
        .pop()
        .expect("key should exist");
    assert_eq!(stored.note.as_deref(), Some("admin-owned-note"));
    assert_eq!(
        stored.encrypted_api_key.as_deref(),
        Some("ciphertext-api-2")
    );
    assert_eq!(stored.expires_at_unix_secs, Some(456));
    assert_eq!(
        stored.status_snapshot.as_ref().unwrap()["quota"]["remaining"],
        7
    );
    assert_eq!(
        stored.status_snapshot.as_ref().unwrap()["oauth"]["code"],
        "none"
    );
    assert_eq!(
        stored.upstream_metadata.as_ref().unwrap()["codex"]["remaining"],
        3
    );
}

#[tokio::test]
async fn oauth_runtime_cas_fences_metadata_namespace_value_and_absence() {
    let repository_with_metadata = |metadata: Value| {
        let mut key = sample_key("key-1", "provider-1")
            .with_transport_fields(
                None,
                "ciphertext-api-1".to_string(),
                Some("ciphertext-auth-1".to_string()),
                None,
                None,
                None,
                None,
                None,
                None,
            )
            .expect("key transport should build");
        key.upstream_metadata = Some(metadata);
        key.status_snapshot = Some(json!({"oauth":{"generation":1}}));
        InMemoryProviderCatalogReadRepository::seed(
            vec![sample_provider("provider-1")],
            vec![],
            vec![key],
        )
    };
    let update = |expected_value: Option<Value>, remaining: u64| {
        ProviderCatalogKeyOAuthRuntimeStateCasUpdate {
            key_id: "key-1".to_string(),
            expected_encrypted_auth_config: Some("ciphertext-auth-1".to_string()),
            expected_credential: None,
            expected_upstream_metadata_namespace: Some(
                ProviderCatalogUpstreamMetadataNamespaceExpectation {
                    namespace: "codex".to_string(),
                    expected_value,
                },
            ),
            encrypted_auth_config: "ciphertext-auth-1".to_string(),
            encrypted_api_key_update: None,
            expires_at_unix_secs_update: None,
            oauth_invalid_at_unix_secs: None,
            oauth_invalid_reason: None,
            upstream_metadata_patch: Some(json!({"codex":{"remaining":remaining}})),
            upstream_metadata_namespace_to_remove: None,
            status_snapshot_patch: json!({"oauth":{"generation":remaining}}),
            reset_error_count: false,
            updated_at_unix_secs: Some(123),
        }
    };

    let exact_repository = repository_with_metadata(json!({
        "codex":{"remaining":5},
        "other":{"keep":true}
    }));
    assert!(exact_repository
        .compare_and_update_key_oauth_runtime_state(&update(Some(json!({"remaining":5})), 4,))
        .await
        .expect("matching metadata namespace CAS should succeed"));
    assert!(!exact_repository
        .compare_and_update_key_oauth_runtime_state(&update(Some(json!({"remaining":5})), 0,))
        .await
        .expect("stale metadata namespace CAS should conflict"));
    let stored = exact_repository
        .list_keys_by_ids(&["key-1".to_string()])
        .await
        .expect("key should reload")
        .pop()
        .expect("key should exist");
    assert_eq!(
        stored.upstream_metadata.as_ref().unwrap()["codex"],
        json!({"remaining":4})
    );
    assert_eq!(
        stored.upstream_metadata.as_ref().unwrap()["other"],
        json!({"keep":true})
    );
    assert_eq!(
        stored.status_snapshot.as_ref().unwrap()["oauth"],
        json!({"generation":4})
    );

    let absent_repository = repository_with_metadata(json!({"other":{"keep":true}}));
    assert!(absent_repository
        .compare_and_update_key_oauth_runtime_state(&update(None, 1))
        .await
        .expect("absent metadata namespace CAS should succeed"));
    let stored = absent_repository
        .list_keys_by_ids(&["key-1".to_string()])
        .await
        .expect("key should reload")
        .pop()
        .expect("key should exist");
    assert_eq!(
        stored.upstream_metadata.as_ref().unwrap()["codex"],
        json!({"remaining":1})
    );

    let null_repository = repository_with_metadata(json!({"codex":null}));
    assert!(!null_repository
        .compare_and_update_key_oauth_runtime_state(&update(None, 1))
        .await
        .expect("JSON null must not compare equal to an absent namespace"));
    let stored = null_repository
        .list_keys_by_ids(&["key-1".to_string()])
        .await
        .expect("key should reload")
        .pop()
        .expect("key should exist");
    assert_eq!(
        stored.upstream_metadata.as_ref().unwrap()["codex"],
        Value::Null
    );
    assert_eq!(
        stored.status_snapshot.as_ref().unwrap()["oauth"],
        json!({"generation":1})
    );

    let removal_repository = repository_with_metadata(json!({
        "codex":{"remaining":5},
        "admin":{"keep":true}
    }));
    let removal_update = |expected_remaining: u64| {
        let mut update = update(
            Some(json!({"remaining":expected_remaining})),
            expected_remaining,
        );
        update.upstream_metadata_patch = Some(json!({"runtime":{"generation":2}}));
        update.upstream_metadata_namespace_to_remove = Some("codex".to_string());
        update.status_snapshot_patch = json!({"quota":null});
        update
    };
    assert!(!removal_repository
        .compare_and_update_key_oauth_runtime_state(&removal_update(4))
        .await
        .expect("stale removal fence should be a CAS miss"));
    assert!(removal_repository
        .compare_and_update_key_oauth_runtime_state(&removal_update(5))
        .await
        .expect("matching removal fence should succeed"));
    let stored = removal_repository
        .list_keys_by_ids(&["key-1".to_string()])
        .await
        .expect("key should reload after namespace removal")
        .pop()
        .expect("key should exist");
    let metadata = stored
        .upstream_metadata
        .as_ref()
        .and_then(Value::as_object)
        .expect("metadata should remain an object");
    assert!(!metadata.contains_key("codex"));
    assert_eq!(metadata["admin"], json!({"keep":true}));
    assert_eq!(metadata["runtime"], json!({"generation":2}));
    assert_eq!(
        stored.status_snapshot.as_ref().unwrap()["quota"],
        Value::Null
    );

    let mut ambiguous_update = removal_update(5);
    ambiguous_update.upstream_metadata_patch = Some(json!({"codex":{"remaining":0}}));
    assert!(matches!(
        removal_repository
            .compare_and_update_key_oauth_runtime_state(&ambiguous_update)
            .await,
        Err(DataLayerError::InvalidInput(_))
    ));
}

#[tokio::test]
async fn oauth_runtime_cas_rejects_changed_credential_context() {
    let repository = || {
        InMemoryProviderCatalogReadRepository::seed(
            vec![sample_provider("provider-1")],
            vec![],
            vec![sample_key("key-1", "provider-1")
                .with_transport_fields(
                    None,
                    "ciphertext-api-1".to_string(),
                    Some("ciphertext-auth-1".to_string()),
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                )
                .expect("key transport should build")],
        )
    };
    let update = || ProviderCatalogKeyOAuthRuntimeStateCasUpdate {
        key_id: "key-1".to_string(),
        expected_encrypted_auth_config: Some("ciphertext-auth-1".to_string()),
        expected_credential: Some(ProviderCatalogKeyOAuthCredentialFence {
            encrypted_api_key: Some("ciphertext-api-1".to_string()),
            auth_type: "api_key".to_string(),
            provider_id: "provider-1".to_string(),
            provider_type: "custom".to_string(),
        }),
        expected_upstream_metadata_namespace: None,
        encrypted_auth_config: "ciphertext-auth-2".to_string(),
        encrypted_api_key_update: Some("ciphertext-api-2".to_string()),
        expires_at_unix_secs_update: None,
        oauth_invalid_at_unix_secs: None,
        oauth_invalid_reason: None,
        upstream_metadata_patch: None,
        upstream_metadata_namespace_to_remove: None,
        status_snapshot_patch: json!({}),
        reset_error_count: false,
        updated_at_unix_secs: Some(123),
    };

    let api_key_repository = repository();
    let mut key = api_key_repository
        .list_keys_by_ids(&["key-1".to_string()])
        .await
        .expect("key should load")
        .pop()
        .expect("key should exist");
    key.encrypted_api_key = Some("ciphertext-admin".to_string());
    assert!(api_key_repository
        .compare_and_update_key_admin_state(&ProviderCatalogKeyAdminCasUpdate {
            expected_encrypted_auth_config: Some("ciphertext-auth-1".to_string()),
            expected_credential: ProviderCatalogKeyOAuthCredentialFence {
                encrypted_api_key: Some("ciphertext-api-1".to_string()),
                auth_type: "api_key".to_string(),
                provider_id: "provider-1".to_string(),
                provider_type: "custom".to_string(),
            },
            key,
            codex_rotation: None,
            reset_oauth_runtime: true,
        })
        .await
        .expect("api key replacement should persist"));
    assert!(!api_key_repository
        .compare_and_update_key_oauth_runtime_state(&update())
        .await
        .expect("API key mismatch should be a CAS miss"));

    let auth_type_repository = repository();
    let mut key = auth_type_repository
        .list_keys_by_ids(&["key-1".to_string()])
        .await
        .expect("key should load")
        .pop()
        .expect("key should exist");
    key.auth_type = "oauth".to_string();
    assert!(auth_type_repository
        .compare_and_update_key_admin_state(&ProviderCatalogKeyAdminCasUpdate {
            expected_encrypted_auth_config: Some("ciphertext-auth-1".to_string()),
            expected_credential: ProviderCatalogKeyOAuthCredentialFence {
                encrypted_api_key: Some("ciphertext-api-1".to_string()),
                auth_type: "api_key".to_string(),
                provider_id: "provider-1".to_string(),
                provider_type: "custom".to_string(),
            },
            key,
            codex_rotation: None,
            reset_oauth_runtime: true,
        })
        .await
        .expect("auth type replacement should persist"));
    assert!(!auth_type_repository
        .compare_and_update_key_oauth_runtime_state(&update())
        .await
        .expect("auth type mismatch should be a CAS miss"));

    let provider_repository = repository();
    let mut provider = provider_repository
        .list_providers_by_ids(&["provider-1".to_string()])
        .await
        .expect("provider should load")
        .pop()
        .expect("provider should exist");
    provider.provider_type = "codex".to_string();
    provider_repository
        .update_provider(&provider)
        .await
        .expect("provider type replacement should persist");
    assert!(!provider_repository
        .compare_and_update_key_oauth_runtime_state(&update())
        .await
        .expect("provider type mismatch should be a CAS miss"));
}

#[tokio::test]
async fn oauth_credential_cas_delete_rejects_replacement_generation() {
    let repository = InMemoryProviderCatalogReadRepository::seed(
        vec![sample_provider("provider-1")],
        vec![],
        vec![sample_key("key-1", "provider-1")
            .with_transport_fields(
                None,
                "ciphertext-api-1".to_string(),
                Some("ciphertext-auth-1".to_string()),
                None,
                None,
                None,
                None,
                None,
                None,
            )
            .expect("key transport should build")],
    );
    let stale_delete = ProviderCatalogKeyOAuthCredentialCasDelete {
        key_id: "key-1".to_string(),
        expected_encrypted_auth_config: Some("ciphertext-auth-1".to_string()),
        expected_credential: ProviderCatalogKeyOAuthCredentialFence {
            encrypted_api_key: Some("ciphertext-api-1".to_string()),
            auth_type: "api_key".to_string(),
            provider_id: "provider-1".to_string(),
            provider_type: "custom".to_string(),
        },
        expected_upstream_metadata_namespace: None,
    };

    let mut replacement = repository
        .list_keys_by_ids(&["key-1".to_string()])
        .await
        .expect("key should load")
        .pop()
        .expect("key should exist");
    replacement.encrypted_api_key = Some("ciphertext-api-2".to_string());
    replacement.encrypted_auth_config = Some("ciphertext-auth-2".to_string());
    replacement.auth_type = "oauth".to_string();
    assert!(repository
        .compare_and_update_key_admin_state(&ProviderCatalogKeyAdminCasUpdate {
            expected_encrypted_auth_config: Some("ciphertext-auth-1".to_string()),
            expected_credential: ProviderCatalogKeyOAuthCredentialFence {
                encrypted_api_key: Some("ciphertext-api-1".to_string()),
                auth_type: "api_key".to_string(),
                provider_id: "provider-1".to_string(),
                provider_type: "custom".to_string(),
            },
            key: replacement,
            codex_rotation: None,
            reset_oauth_runtime: true,
        })
        .await
        .expect("replacement should persist"));

    assert!(!repository
        .compare_and_delete_key_oauth_credential(&stale_delete)
        .await
        .expect("stale delete should be a CAS miss"));
    assert_eq!(
        repository
            .list_keys_by_ids(&["key-1".to_string()])
            .await
            .expect("replacement should load")[0]
            .encrypted_auth_config
            .as_deref(),
        Some("ciphertext-auth-2")
    );

    let current_delete = ProviderCatalogKeyOAuthCredentialCasDelete {
        key_id: "key-1".to_string(),
        expected_encrypted_auth_config: Some("ciphertext-auth-2".to_string()),
        expected_credential: ProviderCatalogKeyOAuthCredentialFence {
            encrypted_api_key: Some("ciphertext-api-2".to_string()),
            auth_type: "oauth".to_string(),
            provider_id: "provider-1".to_string(),
            provider_type: "custom".to_string(),
        },
        expected_upstream_metadata_namespace: None,
    };
    assert!(repository
        .compare_and_delete_key_oauth_credential(&current_delete)
        .await
        .expect("current generation should delete"));
    assert!(repository
        .list_keys_by_ids(&["key-1".to_string()])
        .await
        .expect("deleted key lookup should succeed")
        .is_empty());
}

#[tokio::test]
async fn oauth_credential_cas_delete_rejects_changed_metadata_namespace() {
    let mut key = sample_key("key-1", "provider-1")
        .with_transport_fields(
            None,
            "ciphertext-api-1".to_string(),
            Some("ciphertext-auth-1".to_string()),
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .expect("key transport should build");
    key.upstream_metadata = Some(json!({
        "codex": {"oauth_state_request_started_at_unix_ms": 100},
        "admin": {"keep": true}
    }));
    let repository = InMemoryProviderCatalogReadRepository::seed(
        vec![sample_provider("provider-1")],
        vec![],
        vec![key],
    );
    let delete = |expected_value: Value| ProviderCatalogKeyOAuthCredentialCasDelete {
        key_id: "key-1".to_string(),
        expected_encrypted_auth_config: Some("ciphertext-auth-1".to_string()),
        expected_credential: ProviderCatalogKeyOAuthCredentialFence {
            encrypted_api_key: Some("ciphertext-api-1".to_string()),
            auth_type: "api_key".to_string(),
            provider_id: "provider-1".to_string(),
            provider_type: "custom".to_string(),
        },
        expected_upstream_metadata_namespace: Some(
            ProviderCatalogUpstreamMetadataNamespaceExpectation {
                namespace: "codex".to_string(),
                expected_value: Some(expected_value),
            },
        ),
    };
    let stale_delete = delete(json!({"oauth_state_request_started_at_unix_ms": 100}));
    let current_codex = json!({"oauth_state_request_started_at_unix_ms": 200});
    assert!(repository
        .upsert_key_upstream_metadata_namespace("key-1", "codex", &current_codex, Some(200))
        .await
        .expect("newer codex namespace should persist"));

    assert!(!repository
        .compare_and_delete_key_oauth_credential(&stale_delete)
        .await
        .expect("stale namespace delete should be a CAS miss"));
    assert_eq!(
        repository
            .list_keys_by_ids(&["key-1".to_string()])
            .await
            .expect("key should remain after stale delete")[0]
            .upstream_metadata
            .as_ref()
            .unwrap()["codex"],
        current_codex
    );

    assert!(repository
        .compare_and_delete_key_oauth_credential(&delete(current_codex))
        .await
        .expect("current namespace delete should succeed"));
}

#[tokio::test]
async fn materializes_codex_window_usage_stats_delta_in_memory() {
    let mut key = sample_key("key-1", "provider-1");
    key.status_snapshot = Some(json!({
        "quota": {
            "provider_type": "codex",
            "windows": [
                {
                    "code": "5h",
                    "reset_at": 120_000u64,
                    "window_minutes": 300u64,
                    "usage": {
                        "request_count": 1,
                        "total_tokens": 10,
                        "total_cost_usd": "0.10000000"
                    }
                },
                {
                    "code": "weekly",
                    "reset_at": 700_000u64
                }
            ]
        }
    }));
    let repository = InMemoryProviderCatalogReadRepository::seed(
        vec![sample_provider("provider-1")],
        vec![],
        vec![key],
    );

    repository.apply_usage_stats_delta(
        "key-1",
        &ProviderApiKeyUsageDelta {
            request_count: 2,
            total_tokens: 25,
            total_cost_usd: 0.25,
            usage_created_at_unix_secs: Some(110_000),
            ..ProviderApiKeyUsageDelta::default()
        },
        None,
    );

    let stored = repository
        .list_keys_by_ids(&["key-1".to_string()])
        .await
        .expect("keys should read");
    let windows = stored[0].status_snapshot.as_ref().expect("snapshot")["quota"]["windows"]
        .as_array()
        .expect("windows");
    let five_h = windows
        .iter()
        .find(|window| window["code"] == json!("5h"))
        .expect("5h window should exist");
    let weekly = windows
        .iter()
        .find(|window| window["code"] == json!("weekly"))
        .expect("weekly window should exist");

    assert_eq!(five_h["usage"]["request_count"], json!(3));
    assert_eq!(five_h["usage"]["total_tokens"], json!(35));
    assert_eq!(five_h["usage"]["total_cost_usd"], json!("0.35000000"));
    assert_eq!(weekly["usage"]["request_count"], json!(2));
    assert_eq!(weekly["usage"]["total_tokens"], json!(25));
    assert_eq!(weekly["usage"]["total_cost_usd"], json!("0.25000000"));
}

#[tokio::test]
async fn paginates_provider_keys_with_search_and_active_filter() {
    let mut alpha = sample_key("key-1", "provider-1");
    alpha.name = "alpha".to_string();
    alpha.internal_priority = 20;
    let mut beta = sample_key("key-2", "provider-1");
    beta.name = "beta".to_string();
    beta.internal_priority = 10;
    let mut gamma = sample_key("key-3", "provider-1");
    gamma.name = "gamma".to_string();
    gamma.internal_priority = 30;
    gamma.is_active = false;
    let repository = InMemoryProviderCatalogReadRepository::seed(
        vec![sample_provider("provider-1"), sample_provider("provider-2")],
        vec![],
        vec![alpha, beta, gamma, sample_key("key-4", "provider-2")],
    );

    let page = repository
        .list_keys_page(&ProviderCatalogKeyListQuery {
            provider_id: "provider-1".to_string(),
            search: Some("a".to_string()),
            is_active: Some(true),
            offset: 0,
            limit: 10,
            order: ProviderCatalogKeyListOrder::Name,
        })
        .await
        .expect("keys should page");

    assert_eq!(page.total, 2);
    assert_eq!(page.items.len(), 2);
    assert_eq!(
        page.items
            .iter()
            .map(|item| item.name.as_str())
            .collect::<Vec<_>>(),
        vec!["beta", "alpha"]
    );
}

#[tokio::test]
async fn paginates_provider_keys_by_created_at_when_requested() {
    let mut early = sample_key("key-1", "provider-1");
    early.name = "zeta".to_string();
    early.internal_priority = 10;
    early.created_at_unix_ms = Some(10);
    let mut late = sample_key("key-2", "provider-1");
    late.name = "alpha".to_string();
    late.internal_priority = 10;
    late.created_at_unix_ms = Some(20);
    let repository = InMemoryProviderCatalogReadRepository::seed(
        vec![sample_provider("provider-1")],
        vec![],
        vec![late, early],
    );

    let page = repository
        .list_keys_page(&ProviderCatalogKeyListQuery {
            provider_id: "provider-1".to_string(),
            search: None,
            is_active: None,
            offset: 0,
            limit: 10,
            order: ProviderCatalogKeyListOrder::CreatedAt,
        })
        .await
        .expect("keys should page");

    assert_eq!(page.total, 2);
    assert_eq!(
        page.items
            .iter()
            .map(|item| item.id.as_str())
            .collect::<Vec<_>>(),
        vec!["key-1", "key-2"]
    );
}

#[tokio::test]
async fn paginates_provider_keys_by_pool_sort_fields() {
    let mut old = sample_key("key-1", "provider-1");
    old.name = "old".to_string();
    old.created_at_unix_ms = Some(10);
    old.last_used_at_unix_secs = Some(30);
    let mut fresh = sample_key("key-2", "provider-1");
    fresh.name = "fresh".to_string();
    fresh.created_at_unix_ms = Some(20);
    fresh.last_used_at_unix_secs = Some(10);
    let mut unused = sample_key("key-3", "provider-1");
    unused.name = "unused".to_string();
    unused.created_at_unix_ms = None;
    unused.last_used_at_unix_secs = None;
    let repository = InMemoryProviderCatalogReadRepository::seed(
        vec![sample_provider("provider-1")],
        vec![],
        vec![old, fresh, unused],
    );

    let imported = repository
        .list_keys_page(&ProviderCatalogKeyListQuery {
            provider_id: "provider-1".to_string(),
            search: None,
            is_active: None,
            offset: 0,
            limit: 2,
            order: ProviderCatalogKeyListOrder::CreatedAtDesc,
        })
        .await
        .expect("keys should page");

    assert_eq!(imported.total, 3);
    assert_eq!(
        imported
            .items
            .iter()
            .map(|item| item.id.as_str())
            .collect::<Vec<_>>(),
        vec!["key-2", "key-1"]
    );

    let last_used = repository
        .list_keys_page(&ProviderCatalogKeyListQuery {
            provider_id: "provider-1".to_string(),
            search: None,
            is_active: None,
            offset: 0,
            limit: 3,
            order: ProviderCatalogKeyListOrder::LastUsedAtDesc,
        })
        .await
        .expect("keys should page");

    assert_eq!(
        last_used
            .items
            .iter()
            .map(|item| item.id.as_str())
            .collect::<Vec<_>>(),
        vec!["key-1", "key-2", "key-3"]
    );
}

#[tokio::test]
async fn summarizes_provider_key_stats() {
    let mut inactive = sample_key("key-2", "provider-1");
    inactive.is_active = false;
    let repository = InMemoryProviderCatalogReadRepository::seed(
        vec![sample_provider("provider-1"), sample_provider("provider-2")],
        vec![],
        vec![
            sample_key("key-1", "provider-1"),
            inactive,
            sample_key("key-3", "provider-2"),
        ],
    );

    let stats = repository
        .list_key_stats_by_provider_ids(&["provider-1".to_string(), "provider-2".to_string()])
        .await
        .expect("stats should list");
    assert_eq!(stats.len(), 2);
    assert_eq!(stats[0].provider_id, "provider-1");
    assert_eq!(stats[0].total_keys, 2);
    assert_eq!(stats[0].active_keys, 1);
    assert_eq!(stats[1].provider_id, "provider-2");
    assert_eq!(stats[1].total_keys, 1);
    assert_eq!(stats[1].active_keys, 1);
}

#[tokio::test]
async fn creates_key() {
    let repository = InMemoryProviderCatalogReadRepository::seed(
        vec![sample_provider("provider-1")],
        vec![],
        vec![],
    );
    let key = sample_key("key-1", "provider-1");

    let created = repository
        .create_key(&key)
        .await
        .expect("key should create");

    assert_eq!(created.id, "key-1");
    let stored = repository
        .list_keys_by_ids(&["key-1".to_string()])
        .await
        .expect("keys should read");
    assert_eq!(stored.len(), 1);
    assert_eq!(stored[0].provider_id, "provider-1");
}

#[tokio::test]
async fn provider_api_keys_concurrent_limit_defaults_and_round_trips_in_memory() {
    let repository = InMemoryProviderCatalogReadRepository::seed(
        vec![sample_provider("test-provider-a")],
        vec![],
        vec![],
    );
    let mut key = sample_key("provider-key-a", "test-provider-a");
    assert_eq!(key.concurrent_limit, None);
    key.concurrent_limit = Some(1);

    let created = repository
        .create_key(&key)
        .await
        .expect("key should create");
    assert_eq!(created.concurrent_limit, Some(1));

    let mut updated = created.clone();
    updated.concurrent_limit = None;
    repository
        .update_key(&updated)
        .await
        .expect("key should update");
    let reloaded = repository
        .list_keys_by_ids(&["provider-key-a".to_string()])
        .await
        .expect("keys should read");
    assert_eq!(reloaded[0].concurrent_limit, None);
}

#[tokio::test]
async fn creates_endpoint() {
    let repository = InMemoryProviderCatalogReadRepository::seed(
        vec![sample_provider("provider-1")],
        vec![],
        vec![],
    );
    let endpoint = sample_endpoint("endpoint-1", "provider-1");

    let created = repository
        .create_endpoint(&endpoint)
        .await
        .expect("endpoint should create");

    assert_eq!(created.id, "endpoint-1");
    let stored = repository
        .list_endpoints_by_ids(&["endpoint-1".to_string()])
        .await
        .expect("endpoints should read");
    assert_eq!(stored.len(), 1);
    assert_eq!(stored[0].provider_id, "provider-1");
}

#[tokio::test]
async fn updates_key() {
    let repository = InMemoryProviderCatalogReadRepository::seed(
        vec![sample_provider("provider-1")],
        vec![],
        vec![sample_key("key-1", "provider-1")],
    );
    let mut updated = sample_key("key-1", "provider-1");
    updated.name = "updated".to_string();
    updated.internal_priority = 7;

    let stored = repository
        .update_key(&updated)
        .await
        .expect("key should update");

    assert_eq!(stored.name, "updated");
    assert_eq!(stored.internal_priority, 7);
    let reloaded = repository
        .list_keys_by_ids(&["key-1".to_string()])
        .await
        .expect("keys should read");
    assert_eq!(reloaded[0].name, "updated");
    assert_eq!(reloaded[0].internal_priority, 7);
}

#[tokio::test]
async fn runtime_health_cas_preserves_admin_activation_and_rejects_stale_state() {
    let mut key = sample_key("key-1", "provider-1");
    key.is_active = false;
    key.health_by_format = Some(json!({"openai:chat":{"consecutive_failures":1}}));
    let repository = InMemoryProviderCatalogReadRepository::seed(
        vec![sample_provider("provider-1")],
        vec![],
        vec![key],
    );
    let update = ProviderCatalogKeyHealthStateUpdate {
        key_id: "key-1".to_string(),
        expected_encrypted_auth_config: None,
        expected_health_by_format: Some(json!({"openai:chat":{"consecutive_failures":1}})),
        expected_circuit_breaker_by_format: None,
        health_by_format: Some(json!({"openai:chat":{"consecutive_failures":2}})),
        circuit_breaker_by_format: None,
    };

    assert!(repository
        .compare_and_update_key_health_state(&update)
        .await
        .expect("health CAS should succeed"));
    assert!(!repository
        .compare_and_update_key_health_state(&update)
        .await
        .expect("stale health CAS should report a conflict"));

    let stored = repository
        .list_keys_by_ids(&["key-1".to_string()])
        .await
        .expect("key should reload")
        .pop()
        .expect("key should exist");
    assert!(!stored.is_active);
    assert_eq!(
        stored.health_by_format,
        Some(json!({"openai:chat":{"consecutive_failures":2}}))
    );
}

#[tokio::test]
async fn adaptive_cas_detects_conflicts_and_merges_only_owned_status_fields() {
    let mut key = sample_key("key-1", "provider-1");
    key.encrypted_auth_config = Some("auth-current".to_string());
    key.learned_rpm_limit = Some(10);
    key.rpm_429_count = Some(1);
    key.status_snapshot = Some(json!({
        "quota": {"remaining": 9},
        "oauth": {"invalid": false},
        "observation_count": 1
    }));
    let expected = ProviderCatalogKeyAdaptiveState::from(&key);
    let mut next = expected.clone();
    next.learned_rpm_limit = Some(8);
    next.rpm_429_count = Some(2);
    let repository = InMemoryProviderCatalogReadRepository::seed(
        vec![sample_provider("provider-1")],
        vec![],
        vec![key],
    );
    let update = ProviderCatalogKeyAdaptiveStateUpdate {
        key_id: "key-1".to_string(),
        expected_encrypted_auth_config: Some("auth-current".to_string()),
        expected,
        next,
        status_snapshot_patch: json!({
            "observation_count": 2,
            "learning_confidence": 0.5,
            "quota": {"remaining": 0}
        }),
        updated_at_unix_secs: Some(10),
    };

    let stale_generation_update = ProviderCatalogKeyAdaptiveStateUpdate {
        expected_encrypted_auth_config: Some("auth-stale".to_string()),
        ..update.clone()
    };
    assert!(!repository
        .compare_and_update_key_adaptive_state(&stale_generation_update)
        .await
        .expect("stale auth generation should conflict"));
    assert!(repository
        .compare_and_update_key_adaptive_state(&update)
        .await
        .expect("adaptive CAS should succeed"));
    assert!(!repository
        .compare_and_update_key_adaptive_state(&update)
        .await
        .expect("stale adaptive CAS should report a conflict"));

    let stored = repository
        .list_keys_by_ids(&["key-1".to_string()])
        .await
        .expect("key should reload")
        .pop()
        .expect("key should exist");
    let status = stored.status_snapshot.expect("status should exist");
    assert_eq!(stored.learned_rpm_limit, Some(8));
    assert_eq!(stored.rpm_429_count, Some(2));
    assert_eq!(status["quota"], json!({"remaining": 9}));
    assert_eq!(status["oauth"], json!({"invalid": false}));
    assert_eq!(status["observation_count"], json!(2));
    assert_eq!(status["learning_confidence"], json!(0.5));
}

#[tokio::test]
async fn runtime_metadata_update_preserves_adaptive_state_and_other_namespaces() {
    let mut key = sample_key("key-1", "provider-1");
    key.learned_rpm_limit = Some(12);
    key.upstream_metadata = Some(json!({
        "codex": {"remaining": 5},
        "grok": {"remaining": 7}
    }));
    key.status_snapshot = Some(json!({
        "quota": {"remaining": 5},
        "observation_count": 4,
        "oauth": {"invalid": false}
    }));
    let repository = InMemoryProviderCatalogReadRepository::seed(
        vec![sample_provider("provider-1")],
        vec![],
        vec![key],
    );

    assert!(repository
        .update_key_runtime_metadata(&ProviderCatalogKeyRuntimeMetadataUpdate {
            key_id: "key-1".to_string(),
            namespace: "codex".to_string(),
            expected_upstream_metadata_value: Some(json!({"remaining": 5})),
            upstream_metadata_value: json!({"remaining": 3}),
            status_snapshot_patch: json!({"quota":{"remaining":3}}),
            updated_at_unix_secs: Some(20),
        })
        .await
        .expect("runtime metadata should update"));

    let stored = repository
        .list_keys_by_ids(&["key-1".to_string()])
        .await
        .expect("key should reload")
        .pop()
        .expect("key should exist");
    assert_eq!(stored.learned_rpm_limit, Some(12));
    assert_eq!(
        stored.upstream_metadata,
        Some(json!({
            "codex": {"remaining": 3},
            "grok": {"remaining": 7}
        }))
    );
    let status = stored.status_snapshot.expect("status should exist");
    assert_eq!(status["quota"], json!({"remaining": 3}));
    assert_eq!(status["observation_count"], json!(4));
    assert_eq!(status["oauth"], json!({"invalid": false}));
}

#[tokio::test]
async fn runtime_metadata_namespace_cas_serializes_concurrent_read_modify_writes() {
    let mut key = sample_key("key-1", "provider-1");
    key.upstream_metadata = Some(json!({"grok": {"remaining": 10, "updates": 0}}));
    let repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![sample_provider("provider-1")],
        vec![],
        vec![key],
    ));
    let barrier = Arc::new(Barrier::new(2));
    let mut handles = Vec::new();
    for _ in 0..2 {
        let repository = Arc::clone(&repository);
        let barrier = Arc::clone(&barrier);
        handles.push(tokio::spawn(async move {
            let observed = repository
                .list_keys_by_ids(&["key-1".to_string()])
                .await
                .expect("key should read")
                .pop()
                .expect("key should exist");
            let expected = observed
                .upstream_metadata
                .as_ref()
                .and_then(Value::as_object)
                .and_then(|metadata| metadata.get("grok"))
                .cloned();
            let mut next = expected
                .as_ref()
                .and_then(Value::as_object)
                .cloned()
                .expect("grok namespace should be an object");
            let updates = next
                .get("updates")
                .and_then(Value::as_u64)
                .unwrap_or_default();
            next.insert("updates".to_string(), json!(updates + 1));
            barrier.wait().await;

            let first = repository
                .update_key_runtime_metadata(&ProviderCatalogKeyRuntimeMetadataUpdate {
                    key_id: "key-1".to_string(),
                    namespace: "grok".to_string(),
                    expected_upstream_metadata_value: expected,
                    upstream_metadata_value: Value::Object(next),
                    status_snapshot_patch: json!({}),
                    updated_at_unix_secs: Some(100),
                })
                .await
                .expect("CAS should run");
            if first {
                return (true, true);
            }

            // A stale writer must reload and retry from the winner's value;
            // otherwise one of two concurrent quota deltas is lost.
            let refreshed = repository
                .list_keys_by_ids(&["key-1".to_string()])
                .await
                .expect("key should reload")
                .pop()
                .expect("key should exist");
            let expected = refreshed
                .upstream_metadata
                .as_ref()
                .and_then(Value::as_object)
                .and_then(|metadata| metadata.get("grok"))
                .cloned();
            let mut next = expected
                .as_ref()
                .and_then(Value::as_object)
                .cloned()
                .expect("grok namespace should remain an object");
            let updates = next
                .get("updates")
                .and_then(Value::as_u64)
                .unwrap_or_default();
            next.insert("updates".to_string(), json!(updates + 1));
            let retried = repository
                .update_key_runtime_metadata(&ProviderCatalogKeyRuntimeMetadataUpdate {
                    key_id: "key-1".to_string(),
                    namespace: "grok".to_string(),
                    expected_upstream_metadata_value: expected,
                    upstream_metadata_value: Value::Object(next),
                    status_snapshot_patch: json!({}),
                    updated_at_unix_secs: Some(101),
                })
                .await
                .expect("retry CAS should run");
            (false, retried)
        }));
    }

    let outcomes = futures_util::future::join_all(handles)
        .await
        .into_iter()
        .map(|result| result.expect("CAS task should finish"))
        .collect::<Vec<_>>();
    assert_eq!(outcomes.iter().filter(|(first, _)| !*first).count(), 1);
    assert!(outcomes.iter().all(|(_, persisted)| *persisted));
    let stored = repository
        .list_keys_by_ids(&["key-1".to_string()])
        .await
        .expect("key should reload")
        .pop()
        .expect("key should exist");
    assert_eq!(
        stored.upstream_metadata.unwrap()["grok"]["updates"],
        json!(2)
    );
}

#[tokio::test]
async fn runtime_metadata_namespace_cas_distinguishes_missing_and_stale_values() {
    let repository = InMemoryProviderCatalogReadRepository::seed(
        vec![sample_provider("provider-1")],
        vec![],
        vec![sample_key("key-1", "provider-1")],
    );
    let create = ProviderCatalogKeyRuntimeMetadataUpdate {
        key_id: "key-1".to_string(),
        namespace: "new_namespace".to_string(),
        expected_upstream_metadata_value: None,
        upstream_metadata_value: json!({"value": 1}),
        status_snapshot_patch: json!({}),
        updated_at_unix_secs: Some(1),
    };
    assert!(repository
        .update_key_runtime_metadata(&create)
        .await
        .expect("missing namespace CAS should succeed"));
    assert!(!repository
        .update_key_runtime_metadata(&create)
        .await
        .expect("stale missing namespace CAS should conflict"));
    let stale = ProviderCatalogKeyRuntimeMetadataUpdate {
        expected_upstream_metadata_value: Some(json!({"value": 1})),
        upstream_metadata_value: json!({"value": 2}),
        ..create
    };
    assert!(repository
        .update_key_runtime_metadata(&stale)
        .await
        .expect("matching namespace CAS should succeed"));
}

#[tokio::test]
async fn runtime_metadata_namespace_cas_rejects_non_object_metadata_roots() {
    for invalid_root in [json!(null), json!([]), json!("invalid"), json!(1)] {
        let mut key = sample_key("key-1", "provider-1");
        key.upstream_metadata = Some(invalid_root.clone());
        key.status_snapshot = Some(json!({"quota":{"remaining":9}}));
        key.updated_at_unix_secs = Some(10);
        let repository = InMemoryProviderCatalogReadRepository::seed(
            vec![sample_provider("provider-1")],
            vec![],
            vec![key],
        );

        assert!(!repository
            .update_key_runtime_metadata(&ProviderCatalogKeyRuntimeMetadataUpdate {
                key_id: "key-1".to_string(),
                namespace: "codex".to_string(),
                expected_upstream_metadata_value: None,
                upstream_metadata_value: json!({"remaining":1}),
                status_snapshot_patch: json!({"quota":{"remaining":1}}),
                updated_at_unix_secs: Some(20),
            })
            .await
            .expect("non-object root should be a CAS miss"));
        assert!(!repository
            .compare_and_update_key_oauth_runtime_state(
                &ProviderCatalogKeyOAuthRuntimeStateCasUpdate {
                    key_id: "key-1".to_string(),
                    expected_encrypted_auth_config: None,
                    expected_credential: None,
                    expected_upstream_metadata_namespace: Some(
                        ProviderCatalogUpstreamMetadataNamespaceExpectation {
                            namespace: "codex".to_string(),
                            expected_value: None,
                        },
                    ),
                    encrypted_auth_config: "next-auth".to_string(),
                    encrypted_api_key_update: None,
                    expires_at_unix_secs_update: None,
                    oauth_invalid_at_unix_secs: None,
                    oauth_invalid_reason: None,
                    upstream_metadata_patch: Some(json!({"codex":{"remaining":1}})),
                    upstream_metadata_namespace_to_remove: None,
                    status_snapshot_patch: json!({"quota":{"remaining":1}}),
                    reset_error_count: false,
                    updated_at_unix_secs: Some(20),
                },
            )
            .await
            .expect("OAuth metadata update should be a CAS miss"));
        assert!(!repository
            .compare_and_delete_key_oauth_credential(&ProviderCatalogKeyOAuthCredentialCasDelete {
                key_id: "key-1".to_string(),
                expected_encrypted_auth_config: None,
                expected_credential: ProviderCatalogKeyOAuthCredentialFence {
                    encrypted_api_key: None,
                    auth_type: "api_key".to_string(),
                    provider_id: "provider-1".to_string(),
                    provider_type: "custom".to_string(),
                },
                expected_upstream_metadata_namespace: Some(
                    ProviderCatalogUpstreamMetadataNamespaceExpectation {
                        namespace: "codex".to_string(),
                        expected_value: None,
                    },
                ),
            },)
            .await
            .expect("OAuth credential delete should be a CAS miss"));

        let stored = repository
            .list_keys_by_ids(&["key-1".to_string()])
            .await
            .expect("key should reload")
            .pop()
            .expect("key should exist");
        assert_eq!(stored.upstream_metadata, Some(invalid_root));
        assert_eq!(
            stored.status_snapshot,
            Some(json!({"quota":{"remaining":9}}))
        );
        assert_eq!(stored.encrypted_auth_config, None);
        assert_eq!(stored.updated_at_unix_secs, Some(10));
    }
}

#[tokio::test]
async fn stale_admin_update_preserves_concurrent_runtime_owned_fields() {
    let mut key = sample_key("key-1", "provider-1");
    key.learned_rpm_limit = Some(10);
    key.rpm_429_count = Some(1);
    key.health_by_format = Some(json!({"openai:chat":{"consecutive_failures":1}}));
    key.upstream_metadata = Some(json!({"codex":{"remaining":5}}));
    key.status_snapshot = Some(json!({
        "quota":{"remaining":5},
        "observation_count":1
    }));
    let mut stale_admin_key = key.clone();
    let repository = InMemoryProviderCatalogReadRepository::seed(
        vec![sample_provider("provider-1")],
        vec![],
        vec![key.clone()],
    );

    repository
        .compare_and_update_key_health_state(&ProviderCatalogKeyHealthStateUpdate {
            key_id: key.id.clone(),
            expected_encrypted_auth_config: None,
            expected_health_by_format: key.health_by_format.clone(),
            expected_circuit_breaker_by_format: None,
            health_by_format: Some(json!({"openai:chat":{"consecutive_failures":2}})),
            circuit_breaker_by_format: None,
        })
        .await
        .expect("health CAS should run");
    let expected = ProviderCatalogKeyAdaptiveState::from(&key);
    let mut next = expected.clone();
    next.learned_rpm_limit = Some(8);
    next.rpm_429_count = Some(2);
    repository
        .compare_and_update_key_adaptive_state(&ProviderCatalogKeyAdaptiveStateUpdate {
            key_id: key.id.clone(),
            expected_encrypted_auth_config: None,
            expected,
            next,
            status_snapshot_patch: json!({"observation_count":2}),
            updated_at_unix_secs: Some(20),
        })
        .await
        .expect("adaptive CAS should run");
    repository
        .update_key_runtime_metadata(&ProviderCatalogKeyRuntimeMetadataUpdate {
            key_id: key.id.clone(),
            namespace: "codex".to_string(),
            expected_upstream_metadata_value: Some(json!({"remaining": 5})),
            upstream_metadata_value: json!({"remaining":3}),
            status_snapshot_patch: json!({"quota":{"remaining":3}}),
            updated_at_unix_secs: Some(21),
        })
        .await
        .expect("runtime metadata should update");

    stale_admin_key.name = "admin-renamed".to_string();
    stale_admin_key.is_active = false;
    repository
        .update_key(&stale_admin_key)
        .await
        .expect("admin update should succeed");

    let stored = repository
        .list_keys_by_ids(&[key.id])
        .await
        .expect("key should reload")
        .pop()
        .expect("key should exist");
    assert_eq!(stored.name, "admin-renamed");
    assert!(!stored.is_active);
    assert_eq!(stored.learned_rpm_limit, Some(8));
    assert_eq!(stored.rpm_429_count, Some(2));
    assert_eq!(
        stored.health_by_format,
        Some(json!({"openai:chat":{"consecutive_failures":2}}))
    );
    assert_eq!(
        stored.upstream_metadata,
        Some(json!({"codex":{"remaining":3}}))
    );
    assert_eq!(
        stored.status_snapshot,
        Some(json!({"quota":{"remaining":3},"observation_count":2}))
    );
}

#[tokio::test]
async fn admin_cas_rotates_codex_namespace_and_rejects_stale_credentials() {
    let mut key = sample_key("key-1", "provider-1")
        .with_transport_fields(
            None,
            "api-old".to_string(),
            Some("auth-old".to_string()),
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .expect("key transport should build");
    key.auth_type = "oauth".to_string();
    key.upstream_metadata = Some(json!({
        "codex": {"credential_generation":"generation-old","used_percent":80},
        "runtime": {"keep":true}
    }));
    key.status_snapshot = Some(json!({
        "quota":{"used_ratio":0.8},
        "oauth":{"invalid":true}
    }));
    key.oauth_invalid_at_unix_secs = Some(123);
    key.oauth_invalid_reason = Some("expired".to_string());
    key.error_count = Some(9);
    key.health_by_format = Some(json!({"openai:chat":{"failures":2}}));
    let mut provider = sample_provider("provider-1");
    provider.provider_type = "codex".to_string();
    let repository =
        InMemoryProviderCatalogReadRepository::seed(vec![provider], vec![], vec![key.clone()]);

    let mut requested = key.clone();
    requested.name = "rotated".to_string();
    requested.encrypted_api_key = Some("api-new".to_string());
    requested.encrypted_auth_config = Some("auth-new".to_string());
    requested.upstream_metadata = Some(json!({"caller":"must-not-replace-runtime"}));
    requested.status_snapshot = Some(json!({"caller":"must-not-replace-runtime"}));
    let update = ProviderCatalogKeyAdminCasUpdate {
        expected_encrypted_auth_config: Some("auth-old".to_string()),
        expected_credential: ProviderCatalogKeyOAuthCredentialFence {
            encrypted_api_key: Some("api-old".to_string()),
            auth_type: "oauth".to_string(),
            provider_id: "provider-1".to_string(),
            provider_type: "codex".to_string(),
        },
        key: requested,
        codex_rotation: Some(json!({"credential_generation":"generation-new"})),
        reset_oauth_runtime: true,
    };
    assert!(repository
        .compare_and_update_key_admin_state(&update)
        .await
        .expect("matching admin CAS should succeed"));
    assert!(!repository
        .compare_and_update_key_admin_state(&update)
        .await
        .expect("stale admin CAS should miss"));

    let stored = repository
        .list_keys_by_ids(&["key-1".to_string()])
        .await
        .expect("key should reload")
        .pop()
        .expect("key should exist");
    assert_eq!(stored.name, "rotated");
    assert_eq!(
        stored.upstream_metadata,
        Some(json!({
            "codex":{"credential_generation":"generation-new"},
            "runtime":{"keep":true}
        }))
    );
    assert_eq!(
        stored.status_snapshot,
        Some(json!({"quota":null,"oauth":null}))
    );
    assert_eq!(stored.oauth_invalid_at_unix_secs, None);
    assert_eq!(stored.oauth_invalid_reason, None);
    assert_eq!(stored.error_count, Some(0));
    assert_eq!(
        stored.health_by_format,
        Some(json!({"openai:chat":{"failures":2}}))
    );
}

#[tokio::test]
async fn admin_rotation_fails_closed_for_malformed_runtime_roots() {
    for malformed_field in ["upstream_metadata", "status_snapshot"] {
        let mut key = sample_key("key-1", "provider-1");
        key.auth_type = "oauth".to_string();
        key.encrypted_api_key = Some("api-old".to_string());
        key.encrypted_auth_config = Some("auth-old".to_string());
        key.upstream_metadata = Some(json!({"runtime":{"keep":true}}));
        key.status_snapshot = Some(json!({"oauth":{"keep":true}}));
        if malformed_field == "upstream_metadata" {
            key.upstream_metadata = Some(json!([]));
        } else {
            key.status_snapshot = Some(json!(null));
        }
        let original = key.clone();
        let mut requested = key.clone();
        requested.name = "must-not-persist".to_string();
        requested.encrypted_api_key = Some("api-new".to_string());
        requested.encrypted_auth_config = Some("auth-new".to_string());
        let mut provider = sample_provider("provider-1");
        provider.provider_type = "codex".to_string();
        let repository =
            InMemoryProviderCatalogReadRepository::seed(vec![provider], vec![], vec![key]);

        assert!(!repository
            .compare_and_update_key_admin_state(&ProviderCatalogKeyAdminCasUpdate {
                expected_encrypted_auth_config: Some("auth-old".to_string()),
                expected_credential: ProviderCatalogKeyOAuthCredentialFence {
                    encrypted_api_key: Some("api-old".to_string()),
                    auth_type: "oauth".to_string(),
                    provider_id: "provider-1".to_string(),
                    provider_type: "codex".to_string(),
                },
                key: requested,
                codex_rotation: Some(json!({
                    "credential_generation":"generation-new"
                })),
                reset_oauth_runtime: true,
            })
            .await
            .expect("malformed runtime root should be a CAS miss"));
        let stored = repository
            .list_keys_by_ids(&["key-1".to_string()])
            .await
            .expect("key should reload")
            .pop()
            .expect("key should exist");
        assert_eq!(stored, original);
    }
}

#[tokio::test]
async fn ordinary_admin_update_rejects_stale_credential_snapshot() {
    let mut key = sample_key("key-1", "provider-1");
    key.encrypted_api_key = Some("api-old".to_string());
    key.encrypted_auth_config = Some("auth-old".to_string());
    let mut stale = key.clone();
    stale.name = "stale-name".to_string();
    let repository = InMemoryProviderCatalogReadRepository::seed(
        vec![sample_provider("provider-1")],
        vec![],
        vec![key],
    );

    let mut replacement = stale.clone();
    replacement.encrypted_api_key = Some("api-new".to_string());
    replacement.encrypted_auth_config = Some("auth-new".to_string());
    assert!(repository
        .compare_and_update_key_admin_state(&ProviderCatalogKeyAdminCasUpdate {
            expected_encrypted_auth_config: Some("auth-old".to_string()),
            expected_credential: ProviderCatalogKeyOAuthCredentialFence {
                encrypted_api_key: Some("api-old".to_string()),
                auth_type: "api_key".to_string(),
                provider_id: "provider-1".to_string(),
                provider_type: "custom".to_string(),
            },
            key: replacement,
            codex_rotation: None,
            reset_oauth_runtime: true,
        })
        .await
        .expect("credential replacement should succeed"));

    assert!(matches!(
        repository.update_key(&stale).await,
        Err(DataLayerError::UnexpectedValue(_))
    ));
    let stored = repository
        .list_keys_by_ids(&["key-1".to_string()])
        .await
        .expect("key should reload")
        .pop()
        .expect("key should exist");
    assert_eq!(stored.name, "stale-name");
    assert_eq!(stored.encrypted_api_key.as_deref(), Some("api-new"));
    assert_eq!(stored.encrypted_auth_config.as_deref(), Some("auth-new"));
}

#[tokio::test]
async fn admin_rotation_rejects_noop_credential_and_reset_rejects_bad_status_root() {
    let mut key = sample_key("key-1", "provider-1");
    key.auth_type = "oauth".to_string();
    key.encrypted_api_key = Some("api-old".to_string());
    key.encrypted_auth_config = Some("auth-old".to_string());
    key.error_count = Some(7);
    key.upstream_metadata = Some(json!({"codex":{"credential_generation":"old"}}));
    key.status_snapshot = Some(json!({"quota":{"used_ratio":0.4}}));
    let mut provider = sample_provider("provider-1");
    provider.provider_type = "codex".to_string();
    let repository =
        InMemoryProviderCatalogReadRepository::seed(vec![provider], vec![], vec![key.clone()]);
    let no_op = ProviderCatalogKeyAdminCasUpdate {
        expected_encrypted_auth_config: Some("auth-old".to_string()),
        expected_credential: ProviderCatalogKeyOAuthCredentialFence {
            encrypted_api_key: Some("api-old".to_string()),
            auth_type: "oauth".to_string(),
            provider_id: "provider-1".to_string(),
            provider_type: "codex".to_string(),
        },
        key: key.clone(),
        codex_rotation: Some(json!({"credential_generation":"new"})),
        reset_oauth_runtime: true,
    };
    assert!(!repository
        .compare_and_update_key_admin_state(&no_op)
        .await
        .expect("no-op rotation should be a CAS miss"));

    let reset_valid = ProviderCatalogKeyAdminCasUpdate {
        expected_encrypted_auth_config: Some("auth-old".to_string()),
        expected_credential: no_op.expected_credential.clone(),
        key: key.clone(),
        codex_rotation: None,
        reset_oauth_runtime: true,
    };
    assert!(repository
        .compare_and_update_key_admin_state(&reset_valid)
        .await
        .expect("valid OAuth runtime reset should succeed"));
    let stored = repository
        .list_keys_by_ids(&["key-1".to_string()])
        .await
        .expect("key should reload")
        .pop()
        .expect("key should exist");
    assert_eq!(stored.error_count, Some(0));

    let mut malformed = key.clone();
    malformed.status_snapshot = Some(json!("invalid"));
    let reset_only = ProviderCatalogKeyAdminCasUpdate {
        expected_encrypted_auth_config: Some("auth-old".to_string()),
        expected_credential: ProviderCatalogKeyOAuthCredentialFence {
            encrypted_api_key: Some("api-old".to_string()),
            auth_type: "oauth".to_string(),
            provider_id: "provider-1".to_string(),
            provider_type: "codex".to_string(),
        },
        key: malformed.clone(),
        codex_rotation: None,
        reset_oauth_runtime: true,
    };
    let malformed_repository = InMemoryProviderCatalogReadRepository::seed(
        vec![sample_provider("provider-1")],
        vec![],
        vec![malformed],
    );
    assert!(!malformed_repository
        .compare_and_update_key_admin_state(&reset_only)
        .await
        .expect("reset with malformed status root should be a CAS miss"));

    let stored = malformed_repository
        .list_keys_by_ids(&["key-1".to_string()])
        .await
        .expect("key should reload")
        .pop()
        .expect("key should exist");
    assert_eq!(stored.error_count, Some(7));
}

#[tokio::test]
async fn updates_endpoint() {
    let repository = InMemoryProviderCatalogReadRepository::seed(
        vec![sample_provider("provider-1")],
        vec![sample_endpoint("endpoint-1", "provider-1")],
        vec![],
    );
    let updated = sample_endpoint("endpoint-1", "provider-1")
        .with_transport_fields(
            "https://updated.example".to_string(),
            None,
            None,
            Some(5),
            Some("/v1/chat/completions".to_string()),
            Some(serde_json::json!({"foo":"bar"})),
            None,
            None,
        )
        .expect("endpoint transport should build");

    let stored = repository
        .update_endpoint(&updated)
        .await
        .expect("endpoint should update");

    assert_eq!(stored.base_url, "https://updated.example");
    assert_eq!(stored.max_retries, Some(5));
    let reloaded = repository
        .list_endpoints_by_ids(&["endpoint-1".to_string()])
        .await
        .expect("endpoints should read");
    assert_eq!(reloaded[0].base_url, "https://updated.example");
    assert_eq!(reloaded[0].max_retries, Some(5));
}

#[tokio::test]
async fn deletes_key() {
    let repository = InMemoryProviderCatalogReadRepository::seed(
        vec![sample_provider("provider-1")],
        vec![],
        vec![sample_key("key-1", "provider-1")],
    );

    assert!(repository
        .delete_key("key-1")
        .await
        .expect("delete should succeed"));
    let reloaded = repository
        .list_keys_by_ids(&["key-1".to_string()])
        .await
        .expect("keys should read");
    assert!(reloaded.is_empty());
}

#[tokio::test]
async fn deletes_endpoint() {
    let repository = InMemoryProviderCatalogReadRepository::seed(
        vec![sample_provider("provider-1")],
        vec![sample_endpoint("endpoint-1", "provider-1")],
        vec![],
    );

    assert!(repository
        .delete_endpoint("endpoint-1")
        .await
        .expect("delete should succeed"));
    let reloaded = repository
        .list_endpoints_by_ids(&["endpoint-1".to_string()])
        .await
        .expect("endpoints should read");
    assert!(reloaded.is_empty());
}
