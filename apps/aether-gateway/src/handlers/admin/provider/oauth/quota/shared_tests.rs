use super::*;
use crate::data::GatewayDataState;
use crate::AppState;
use aether_crypto::DEVELOPMENT_ENCRYPTION_KEY;
use aether_data::repository::provider_catalog::InMemoryProviderCatalogReadRepository;
use aether_data_contracts::repository::provider_catalog::{
    ProviderCatalogReadRepository, ProviderCatalogWriteRepository, StoredProviderCatalogKey,
    StoredProviderCatalogProvider,
};
use serde_json::json;
use std::sync::Arc;

#[test]
fn quota_plans_cannot_enable_redirects_through_header_overrides() {
    let mut headers = std::collections::BTreeMap::from([
        (
            "X-Aether-Execution-Follow-Redirects".to_string(),
            "true".to_string(),
        ),
        ("authorization".to_string(), "Bearer secret".to_string()),
    ]);

    force_provider_quota_redirects_disabled(&mut headers);

    assert_eq!(
        headers
            .get(EXECUTION_REQUEST_FOLLOW_REDIRECTS_HEADER)
            .map(String::as_str),
        Some("false")
    );
    assert_eq!(
        headers
            .keys()
            .filter(|name| { name.eq_ignore_ascii_case(EXECUTION_REQUEST_FOLLOW_REDIRECTS_HEADER) })
            .count(),
        1
    );
    assert_eq!(
        headers.get("authorization").map(String::as_str),
        Some("Bearer secret")
    );
}

#[test]
fn quota_origin_policy_accepts_only_provider_owned_https_origins() {
    for (provider_name, url) in [
        (
            "antigravity",
            "https://cloudcode-pa.googleapis.com/v1internal:fetchAvailableModels",
        ),
        (
            "antigravity",
            "https://daily-cloudcode-pa.googleapis.com/v1internal:fetchAvailableModels",
        ),
        (
            "antigravity",
            "https://daily-cloudcode-pa.sandbox.googleapis.com/v1internal:fetchAvailableModels",
        ),
        (
            "gemini_cli",
            "https://cloudcode-pa.googleapis.com/v1internal:retrieveUserQuota",
        ),
        (
            "chatgpt_web",
            "https://chatgpt.com/backend-api/conversation/init",
        ),
        ("codex", "https://chatgpt.com/backend-api/wham/usage"),
        ("grok", "https://grok.com/rest/rate-limits"),
        (
            "xai",
            "https://cli-chat-proxy.grok.com/v1/billing?format=credits",
        ),
        ("xai", "https://cli-chat-proxy.grok.com/v1/user"),
        (
            "windsurf",
            "https://server.codeium.com/exa.seat_management_pb.SeatManagementService/GetUserStatus",
        ),
        (
            "kiro",
            "https://q.us-east-1.amazonaws.com/getUsageLimits?origin=AI_EDITOR",
        ),
    ] {
        assert!(
            provider_quota_url_has_allowed_origin(provider_name, url),
            "expected {provider_name} quota URL to be allowed: {url}"
        );
    }
}

#[test]
fn quota_origin_policy_rejects_ssrf_and_credential_redirect_origins() {
    for (provider_name, url) in [
        (
            "chatgpt_web",
            "http://chatgpt.com/backend-api/conversation/init",
        ),
        ("codex", "https://chatgpt.com:444/backend-api/wham/usage"),
        (
            "codex",
            "https://user:secret@chatgpt.com/backend-api/wham/usage",
        ),
        (
            "codex",
            "https://chatgpt.com.attacker.test/backend-api/wham/usage",
        ),
        ("grok", "https://grok.com.attacker.test/rest/rate-limits"),
        (
            "xai",
            "https://cli-chat-proxy.grok.com.attacker.test/v1/billing",
        ),
        ("xai", "https://api.x.ai/v1/billing?format=credits"),
        ("windsurf", "https://server.codeium.com.attacker.test/quota"),
        (
            "gemini_cli",
            "https://quota-proxy.internal/retrieveUserQuota",
        ),
        (
            "antigravity",
            "https://cloudcode-pa.googleapis.com.attacker.test/quota",
        ),
        ("kiro", "https://q.localhost:8443/getUsageLimits"),
        (
            "kiro",
            "https://q.us-east-1.evil.amazonaws.com/getUsageLimits",
        ),
        (
            "kiro",
            "https://q.us-east-1.amazonaws.com.attacker.test/getUsageLimits",
        ),
        ("unknown", "https://chatgpt.com/backend-api/wham/usage"),
    ] {
        assert!(
            !provider_quota_url_has_allowed_origin(provider_name, url),
            "expected {provider_name} quota URL to be rejected: {url}"
        );
    }
}

fn codex_merge_context(
    request_started_at_unix_ms: u64,
) -> admin_provider_quota_pure::CodexQuotaMergeContext<'static> {
    codex_merge_context_with_id(request_started_at_unix_ms, None)
}

fn codex_merge_context_with_id(
    request_started_at_unix_ms: u64,
    request_order_id: Option<&'static str>,
) -> admin_provider_quota_pure::CodexQuotaMergeContext<'static> {
    admin_provider_quota_pure::CodexQuotaMergeContext {
        observed_at_unix_secs: request_started_at_unix_ms / 1_000,
        request_started_at_unix_ms: Some(request_started_at_unix_ms),
        request_order_id,
        observed_reset_generation: Some(0),
        authoritative_reset_generation: None,
        observed_credential_generation: None,
        account_reset_fence_id: None,
        coverage: admin_provider_quota_pure::CodexQuotaWindowCoverage::AccountSnapshot,
    }
}

fn codex_reset_merge_context(
    request_started_at_unix_ms: u64,
    fence_id: &'static str,
) -> admin_provider_quota_pure::CodexQuotaMergeContext<'static> {
    admin_provider_quota_pure::CodexQuotaMergeContext {
        observed_at_unix_secs: request_started_at_unix_ms / 1_000,
        request_started_at_unix_ms: Some(request_started_at_unix_ms),
        request_order_id: Some("reset-refresh"),
        observed_reset_generation: Some(0),
        authoritative_reset_generation: None,
        observed_credential_generation: None,
        account_reset_fence_id: Some(fence_id),
        coverage: admin_provider_quota_pure::CodexQuotaWindowCoverage::AccountSnapshot,
    }
}

fn codex_refresh_test_state(
    key_id: &str,
    auth_config: Option<&str>,
) -> (
    AppState,
    Arc<InMemoryProviderCatalogReadRepository>,
    Option<ProviderTransportCredentialFence>,
) {
    let provider = StoredProviderCatalogProvider::new(
        "provider-codex-refresh".to_string(),
        "Codex Refresh".to_string(),
        None,
        "codex".to_string(),
    )
    .expect("provider should build");
    let bootstrap = crate::tests::support::credential_fixture_state();
    let encrypted_auth_config = auth_config
        .map(|plaintext| {
            bootstrap.seal_provider_catalog_key_auth_config(&provider.id, key_id, plaintext)
        })
        .transpose()
        .expect("auth config should seal");
    let mut key = StoredProviderCatalogKey::new(
        key_id.to_string(),
        provider.id.clone(),
        "Codex Refresh".to_string(),
        "oauth".to_string(),
        None,
        true,
    )
    .expect("key should build");
    key.encrypted_auth_config = encrypted_auth_config.clone();
    key.upstream_metadata = Some(json!({
        "codex": {
            "plan_type": "plus",
            "primary_used_percent": 60.0,
            "primary_reset_at": 2_000_000_000u64,
            "primary_window_minutes": 300u64,
            "account_quota_request_started_at_unix_ms": 200_000u64,
            "updated_at": 200u64
        }
    }));
    let credential_fence =
        encrypted_auth_config.map(|encrypted_auth_config| ProviderTransportCredentialFence {
            encrypted_auth_config,
            credential: ProviderCatalogKeyOAuthCredentialFence {
                encrypted_api_key: key.encrypted_api_key.clone(),
                auth_type: key.auth_type.clone(),
                provider_id: key.provider_id.clone(),
                provider_type: provider.provider_type.clone(),
            },
        });
    let repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![provider],
        vec![],
        vec![key],
    ));
    let app = AppState::new()
        .expect("app should build")
        .with_data_state_for_tests(
            GatewayDataState::with_provider_catalog_repository_for_tests(Arc::clone(&repository))
                .with_encryption_key_for_tests(DEVELOPMENT_ENCRYPTION_KEY),
        );
    (app, repository, credential_fence)
}

fn codex_reset_state_machine_test_state(
    key_id: &str,
) -> (
    AppState,
    Arc<InMemoryProviderCatalogReadRepository>,
    ProviderTransportCredentialFence,
) {
    let provider = StoredProviderCatalogProvider::new(
        "provider-codex-reset-state".to_string(),
        "Codex Reset State".to_string(),
        None,
        "codex".to_string(),
    )
    .expect("provider should build");
    let bootstrap = crate::tests::support::credential_fixture_state();
    let encrypted_auth_config = bootstrap
        .seal_provider_catalog_key_auth_config(&provider.id, key_id, "auth-v1")
        .expect("auth config should seal");
    let mut key = StoredProviderCatalogKey::new(
        key_id.to_string(),
        provider.id.clone(),
        "Codex Reset".to_string(),
        "oauth".to_string(),
        None,
        true,
    )
    .expect("key should build");
    key.encrypted_auth_config = Some(encrypted_auth_config.clone());
    key.upstream_metadata = Some(json!({
        "codex": {
            "credential_generation": "credential-v1",
            "reset_credits": {
                "available_count": 2,
                "updated_at": 100u64,
                "detail_source": "wham_readonly",
                "detail_status": "available",
                "credits": [
                    {"id": "credit-1", "expires_at": 20_000u64},
                    {"id": "credit-2", "expires_at": 30_000u64}
                ]
            }
        }
    }));
    let credential_fence = ProviderTransportCredentialFence {
        encrypted_auth_config,
        credential: ProviderCatalogKeyOAuthCredentialFence {
            encrypted_api_key: None,
            auth_type: key.auth_type.clone(),
            provider_id: provider.id.clone(),
            provider_type: provider.provider_type.clone(),
        },
    };
    let repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![provider],
        vec![],
        vec![key],
    ));
    let app = AppState::new()
        .expect("app should build")
        .with_data_state_for_tests(
            GatewayDataState::with_provider_catalog_repository_for_tests(Arc::clone(&repository))
                .with_encryption_key_for_tests(DEVELOPMENT_ENCRYPTION_KEY),
        );
    (app, repository, credential_fence)
}

#[tokio::test]
async fn codex_reset_reservation_serializes_ids_and_reuses_same_generation() {
    let key_id = "key-codex-reset-reservation";
    let (app, repository, credential) = codex_reset_state_machine_test_state(key_id);
    let admin_state = AdminAppState::new(&app);

    let first = reserve_codex_account_reset(
        &admin_state,
        key_id,
        credential.encrypted_auth_config.as_str(),
        &credential.credential,
        Some("credential-v1"),
        "reset-a",
    )
    .await
    .expect("reservation should complete")
    .expect("reservation should exist");
    let first = match first {
        CodexAccountResetReserveResult::Reserved(value) => value,
        other => panic!("unexpected first reservation: {other:?}"),
    };
    let same = reserve_codex_account_reset(
        &admin_state,
        key_id,
        credential.encrypted_auth_config.as_str(),
        &credential.credential,
        Some("credential-v1"),
        "reset-a",
    )
    .await
    .expect("same reservation should complete")
    .expect("same reservation should exist");
    assert_eq!(
        same,
        CodexAccountResetReserveResult::Reserved(first.clone())
    );
    let other = reserve_codex_account_reset(
        &admin_state,
        key_id,
        credential.encrypted_auth_config.as_str(),
        &credential.credential,
        Some("credential-v1"),
        "reset-b",
    )
    .await
    .expect("busy check should complete")
    .expect("busy reservation should exist");
    assert_eq!(other, CodexAccountResetReserveResult::Busy(first.clone()));

    // An ambiguous upstream result does not call complete; the durable
    // reservation continues to block another id while the same id resumes.
    let stored = repository
        .list_keys_by_ids(&[key_id.to_string()])
        .await
        .expect("key should reload")
        .pop()
        .expect("key should exist");
    let codex = &stored.upstream_metadata.unwrap()["codex"];
    assert_eq!(codex["account_quota_reset_sequence"], json!(1u64));
    assert_eq!(
        codex["account_quota_reset_reservation"]["idempotency_key"],
        json!("reset-a")
    );
    assert!(codex.get("account_quota_reset_generation").is_none());
}

#[tokio::test]
async fn codex_reset_reservation_rejects_replaced_credential_generation() {
    let key_id = "key-codex-reset-credential-generation";
    let (app, repository, credential) = codex_reset_state_machine_test_state(key_id);
    let admin_state = AdminAppState::new(&app);
    let original_metadata = repository
        .list_keys_by_ids(&[key_id.to_string()])
        .await
        .expect("key should load before reservation")
        .pop()
        .expect("key should exist before reservation")
        .upstream_metadata;

    let result = reserve_codex_account_reset(
        &admin_state,
        key_id,
        credential.encrypted_auth_config.as_str(),
        &credential.credential,
        Some("credential-before-rebind"),
        "reset-from-old-account",
    )
    .await
    .expect("generation fence should complete")
    .expect("generation mismatch should be explicit");
    assert_eq!(
        result,
        CodexAccountResetReserveResult::CredentialGenerationMismatch
    );

    let stored = repository
        .list_keys_by_ids(&[key_id.to_string()])
        .await
        .expect("key should reload")
        .pop()
        .expect("key should exist");
    assert_eq!(stored.upstream_metadata, original_metadata);
}

#[tokio::test]
async fn codex_reset_noop_does_not_activate_but_later_id_gets_new_generation() {
    let key_id = "key-codex-reset-noop";
    let (app, repository, credential) = codex_reset_state_machine_test_state(key_id);
    let admin_state = AdminAppState::new(&app);
    let reservation = match reserve_codex_account_reset(
        &admin_state,
        key_id,
        credential.encrypted_auth_config.as_str(),
        &credential.credential,
        Some("credential-v1"),
        "reset-noop",
    )
    .await
    .unwrap()
    .unwrap()
    {
        CodexAccountResetReserveResult::Reserved(value) => value,
        other => panic!("unexpected reservation: {other:?}"),
    };
    assert!(matches!(
        complete_codex_account_reset(
            &admin_state,
            key_id,
            credential.encrypted_auth_config.as_str(),
            &credential.credential,
            &reservation,
            "nothing_to_reset",
            200_000,
        )
        .await
        .unwrap(),
        Some(CodexAccountResetCompleteResult::Noop(_))
    ));
    let next = reserve_codex_account_reset(
        &admin_state,
        key_id,
        credential.encrypted_auth_config.as_str(),
        &credential.credential,
        Some("credential-v1"),
        "reset-next",
    )
    .await
    .unwrap()
    .unwrap();
    assert!(matches!(
        next,
        CodexAccountResetReserveResult::Reserved(CodexAccountResetReservation {
            generation: 2,
            ..
        })
    ));
    let stored = repository
        .list_keys_by_ids(&[key_id.to_string()])
        .await
        .unwrap()
        .pop()
        .unwrap();
    let codex = &stored.upstream_metadata.unwrap()["codex"];
    assert!(codex.get("account_quota_reset_generation").is_none());
}

async fn complete_codex_reset_in_order(
    first_outcome: &str,
    second_outcome: &str,
) -> serde_json::Value {
    let key_id = format!("key-codex-reset-order-{first_outcome}");
    let (app, repository, credential) = codex_reset_state_machine_test_state(&key_id);
    let admin_state = AdminAppState::new(&app);
    let reservation = match reserve_codex_account_reset(
        &admin_state,
        &key_id,
        credential.encrypted_auth_config.as_str(),
        &credential.credential,
        Some("credential-v1"),
        "same-id",
    )
    .await
    .unwrap()
    .unwrap()
    {
        CodexAccountResetReserveResult::Reserved(value) => value,
        other => panic!("unexpected reservation: {other:?}"),
    };
    complete_codex_account_reset(
        &admin_state,
        &key_id,
        credential.encrypted_auth_config.as_str(),
        &credential.credential,
        &reservation,
        first_outcome,
        200_000,
    )
    .await
    .unwrap()
    .expect("first completion should persist");
    complete_codex_account_reset(
        &admin_state,
        &key_id,
        credential.encrypted_auth_config.as_str(),
        &credential.credential,
        &reservation,
        second_outcome,
        210_000,
    )
    .await
    .unwrap()
    .expect("second completion should converge");
    repository
        .list_keys_by_ids(&[key_id])
        .await
        .unwrap()
        .pop()
        .unwrap()
        .upstream_metadata
        .unwrap()["codex"]
        .clone()
}

#[tokio::test]
async fn codex_reset_activation_wins_over_noop_in_both_completion_orders() {
    for codex in [
        complete_codex_reset_in_order("nothing_to_reset", "reset").await,
        complete_codex_reset_in_order("reset", "nothing_to_reset").await,
    ] {
        assert_eq!(codex["account_quota_reset_generation"], json!(1u64));
        assert_eq!(codex["account_quota_reset_pending_generation"], json!(1u64));
        assert_eq!(codex["account_quota_reset_pending"], json!(true));
        assert_eq!(
            codex["account_quota_reset_history"][0]["outcome"],
            json!("reset")
        );
        assert_eq!(codex["reset_credits"]["available_count"], json!(1u64));
        assert_eq!(
            codex["reset_credits"]["credits"],
            json!([{"id": "credit-2", "expires_at": 30_000u64}])
        );
    }
}

#[tokio::test]
async fn delayed_reset_upgrade_preserves_the_next_generation_reservation() {
    let key_id = "key-codex-reset-upgrade-next-generation";
    let (app, repository, credential) = codex_reset_state_machine_test_state(key_id);
    let admin_state = AdminAppState::new(&app);
    let first = match reserve_codex_account_reset(
        &admin_state,
        key_id,
        credential.encrypted_auth_config.as_str(),
        &credential.credential,
        Some("credential-v1"),
        "reset-first",
    )
    .await
    .unwrap()
    .unwrap()
    {
        CodexAccountResetReserveResult::Reserved(value) => value,
        other => panic!("unexpected first reservation: {other:?}"),
    };
    complete_codex_account_reset(
        &admin_state,
        key_id,
        credential.encrypted_auth_config.as_str(),
        &credential.credential,
        &first,
        "nothing_to_reset",
        200_000,
    )
    .await
    .unwrap()
    .expect("first noop should complete");
    let second = match reserve_codex_account_reset(
        &admin_state,
        key_id,
        credential.encrypted_auth_config.as_str(),
        &credential.credential,
        Some("credential-v1"),
        "reset-second",
    )
    .await
    .unwrap()
    .unwrap()
    {
        CodexAccountResetReserveResult::Reserved(value) => value,
        other => panic!("unexpected second reservation: {other:?}"),
    };
    assert_eq!(second.generation, 2);

    assert!(matches!(
        complete_codex_account_reset(
            &admin_state,
            key_id,
            credential.encrypted_auth_config.as_str(),
            &credential.credential,
            &first,
            "reset",
            210_000,
        )
        .await
        .unwrap(),
        Some(CodexAccountResetCompleteResult::Activated(
            CodexAccountResetFence { generation: 1, .. }
        ))
    ));
    let after_upgrade = repository
        .list_keys_by_ids(&[key_id.to_string()])
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(
        after_upgrade.upstream_metadata.as_ref().unwrap()["codex"]
            ["account_quota_reset_reservation"],
        json!({
            "idempotency_key": "reset-second",
            "generation": 2,
        })
    );

    assert!(matches!(
        complete_codex_account_reset(
            &admin_state,
            key_id,
            credential.encrypted_auth_config.as_str(),
            &credential.credential,
            &second,
            "reset",
            220_000,
        )
        .await
        .unwrap(),
        Some(CodexAccountResetCompleteResult::Activated(
            CodexAccountResetFence { generation: 2, .. }
        ))
    ));
    let stored = repository
        .list_keys_by_ids(&[key_id.to_string()])
        .await
        .unwrap()
        .pop()
        .unwrap();
    let codex = &stored.upstream_metadata.unwrap()["codex"];
    assert_eq!(codex["account_quota_reset_generation"], json!(2u64));
    assert!(codex.get("account_quota_reset_reservation").is_none());
}

#[tokio::test]
async fn stale_codex_refresh_cannot_lower_realtime_usage() {
    let key_id = "key-codex-refresh-monotonic";
    let (app, repository, _) = codex_refresh_test_state(key_id, None);
    let admin_state = AdminAppState::new(&app);
    let stale_refresh = json!({"codex": {
        "plan_type": "plus",
        "primary_used_percent": 50.0,
        "primary_reset_at": 2_000_000_000u64,
        "primary_window_minutes": 300u64,
        "updated_at": 100u64
    }});

    assert!(persist_codex_provider_quota_refresh_state(
        &admin_state,
        key_id,
        Some(&stale_refresh),
        codex_merge_context(100_000),
    )
    .await
    .expect("refresh persistence should complete"));

    let stored = repository
        .list_keys_by_ids(&[key_id.to_string()])
        .await
        .expect("key should reload")
        .pop()
        .expect("key should exist");
    let codex = &stored.upstream_metadata.as_ref().unwrap()["codex"];
    assert_eq!(codex["primary_used_percent"], json!(60.0));
    assert_eq!(
        codex["account_quota_request_started_at_unix_ms"],
        json!(200_000u64)
    );
}

#[tokio::test]
async fn codex_reset_fence_is_idempotent_and_rejects_pre_reset_response() {
    let key_id = "key-codex-reset-fence";
    let (app, repository, _) = codex_refresh_test_state(key_id, None);
    let admin_state = AdminAppState::new(&app);

    let initial_fence = persist_codex_account_reset_fence(
        &admin_state,
        key_id,
        None,
        None,
        250_000,
        "fence-a",
        "redeem-once",
    )
    .await
    .expect("reset fence should persist")
    .expect("reset fence should be returned");
    let initial_fence = match initial_fence {
        CodexAccountResetFenceInstall::Owned(fence) => fence,
        CodexAccountResetFenceInstall::Superseded => {
            panic!("initial reset should own its fence")
        }
    };
    let duplicate_fence = persist_codex_account_reset_fence(
        &admin_state,
        key_id,
        None,
        None,
        300_000,
        "fence-a",
        "redeem-once",
    )
    .await
    .expect("duplicate reset fence should be idempotent")
    .expect("duplicate should return the installed fence");
    let duplicate_fence = match duplicate_fence {
        CodexAccountResetFenceInstall::Owned(fence) => fence,
        CodexAccountResetFenceInstall::Superseded => {
            panic!("duplicate active reset should retain ownership")
        }
    };
    assert_eq!(duplicate_fence, initial_fence);

    let stale = json!({"codex": {
        "primary_used_percent": 100.0,
        "primary_reset_at": 2_000_000_000u64,
        "primary_window_minutes": 300u64
    }});
    assert!(persist_codex_provider_quota_refresh_state(
        &admin_state,
        key_id,
        Some(&stale),
        codex_merge_context(200_000),
    )
    .await
    .expect("stale response should be harmlessly acknowledged"));

    let baseline = json!({"codex": {
        "primary_used_percent": 0.0,
        "primary_reset_at": 2_000_000_000u64,
        "primary_window_minutes": 300u64
    }});
    assert!(persist_codex_provider_quota_refresh_state(
        &admin_state,
        key_id,
        Some(&baseline),
        codex_reset_merge_context(260_000, "fence-a"),
    )
    .await
    .expect("reset baseline should persist"));

    let stored = repository
        .list_keys_by_ids(&[key_id.to_string()])
        .await
        .expect("key should reload")
        .pop()
        .expect("key should exist");
    let codex = &stored.upstream_metadata.as_ref().unwrap()["codex"];
    assert_eq!(codex["primary_used_percent"], json!(0.0));
    assert_eq!(codex["account_quota_reset_fence_id"], json!("fence-a"));
    assert_eq!(codex["account_quota_reset_pending"], json!(false));
}

#[tokio::test]
async fn codex_reset_fence_barrier_never_moves_backward_and_remembers_processed_ids() {
    let key_id = "key-codex-reset-fence-order";
    let (app, repository, _) = codex_refresh_test_state(key_id, None);
    let admin_state = AdminAppState::new(&app);

    let newer = persist_codex_account_reset_fence(
        &admin_state,
        key_id,
        None,
        None,
        300_000,
        "fence-newer",
        "redeem-newer",
    )
    .await
    .expect("newer reset fence should persist")
    .expect("newer reset fence should be returned");
    assert!(matches!(
        newer,
        CodexAccountResetFenceInstall::Owned(CodexAccountResetFence {
            unix_ms: 300_000,
            ref id,
            ..
        }) if id == "fence-newer"
    ));
    let delayed_older = persist_codex_account_reset_fence(
        &admin_state,
        key_id,
        None,
        None,
        250_000,
        "fence-older",
        "redeem-older",
    )
    .await
    .expect("older reset should be recorded")
    .expect("install result should be returned");
    assert_eq!(delayed_older, CodexAccountResetFenceInstall::Superseded);

    let stored = repository
        .list_keys_by_ids(&[key_id.to_string()])
        .await
        .expect("key should reload")
        .pop()
        .expect("key should exist");
    let codex = &stored.upstream_metadata.as_ref().unwrap()["codex"];
    assert_eq!(
        codex["account_quota_reset_fence_unix_ms"],
        json!(300_000u64)
    );
    assert_eq!(codex["account_quota_reset_fence_id"], json!("fence-newer"));
    let processed_ids = codex["account_quota_reset_processed_ids"]
        .as_array()
        .expect("processed reset ids should be an array");
    assert_eq!(processed_ids.len(), 2);
    assert!(processed_ids.contains(&json!("redeem-older")));
    assert!(processed_ids.contains(&json!("redeem-newer")));
}

#[tokio::test]
async fn concurrent_codex_reset_fences_converge_on_newest_barrier() {
    let key_id = "key-codex-reset-fence-concurrent";
    let (app, repository, _) = codex_refresh_test_state(key_id, None);
    let admin_state = AdminAppState::new(&app);

    let (older, newer) = tokio::join!(
        persist_codex_account_reset_fence(
            &admin_state,
            key_id,
            None,
            None,
            250_000,
            "fence-older",
            "redeem-older",
        ),
        persist_codex_account_reset_fence(
            &admin_state,
            key_id,
            None,
            None,
            300_000,
            "fence-newer",
            "redeem-newer",
        ),
    );
    let older = older
        .expect("older reset should complete")
        .expect("older reset should return an install result");
    let newer = newer
        .expect("newer reset should complete")
        .expect("newer reset should return an install result");
    assert!(matches!(
        (older, newer),
        (
            CodexAccountResetFenceInstall::Owned(_),
            CodexAccountResetFenceInstall::Owned(_)
        ) | (
            CodexAccountResetFenceInstall::Superseded,
            CodexAccountResetFenceInstall::Owned(_)
        )
    ));

    let stored = repository
        .list_keys_by_ids(&[key_id.to_string()])
        .await
        .expect("key should reload")
        .pop()
        .expect("key should exist");
    let codex = &stored.upstream_metadata.as_ref().unwrap()["codex"];
    assert_eq!(
        codex["account_quota_reset_fence_unix_ms"],
        json!(300_000u64)
    );
    assert_eq!(codex["account_quota_reset_fence_id"], json!("fence-newer"));
    assert_eq!(codex["account_quota_reset_pending"], json!(true));
    let processed_ids = codex["account_quota_reset_processed_ids"]
        .as_array()
        .expect("processed reset ids should be an array");
    assert_eq!(processed_ids.len(), 2);
    assert!(processed_ids.contains(&json!("redeem-older")));
    assert!(processed_ids.contains(&json!("redeem-newer")));
}

#[tokio::test]
async fn superseded_codex_reset_refresh_cannot_confirm_newer_fence() {
    let key_id = "key-codex-reset-fence-stale-refresh";
    let (app, repository, _) = codex_refresh_test_state(key_id, None);
    let admin_state = AdminAppState::new(&app);

    for (fence_unix_ms, fence_id, redeem_id) in [
        (250_000, "fence-older", "redeem-older"),
        (300_000, "fence-newer", "redeem-newer"),
    ] {
        let install = persist_codex_account_reset_fence(
            &admin_state,
            key_id,
            None,
            None,
            fence_unix_ms,
            fence_id,
            redeem_id,
        )
        .await
        .expect("reset fence should persist")
        .expect("reset fence should return an install result");
        assert!(matches!(install, CodexAccountResetFenceInstall::Owned(_)));
    }

    let stale_baseline = json!({"codex": {
        "primary_used_percent": 0.0,
        "primary_reset_at": 2_000_000_000u64,
        "primary_window_minutes": 300u64
    }});
    assert!(persist_codex_provider_quota_refresh_state(
        &admin_state,
        key_id,
        Some(&stale_baseline),
        admin_provider_quota_pure::CodexQuotaMergeContext {
            observed_at_unix_secs: 310,
            request_started_at_unix_ms: Some(310_000),
            request_order_id: Some("older-reset-late-refresh"),
            observed_reset_generation: Some(0),
            authoritative_reset_generation: None,
            observed_credential_generation: None,
            account_reset_fence_id: Some("fence-older"),
            coverage: admin_provider_quota_pure::CodexQuotaWindowCoverage::AccountSnapshot,
        },
    )
    .await
    .expect("superseded reset refresh should be harmlessly acknowledged"));

    let stored = repository
        .list_keys_by_ids(&[key_id.to_string()])
        .await
        .expect("key should reload")
        .pop()
        .expect("key should exist");
    let codex = &stored.upstream_metadata.as_ref().unwrap()["codex"];
    assert_eq!(codex["primary_used_percent"], json!(60.0));
    assert_eq!(codex["account_quota_reset_fence_id"], json!("fence-newer"));
    assert_eq!(codex["account_quota_reset_pending"], json!(true));
}

#[tokio::test]
async fn replaying_historical_codex_reset_does_not_reopen_pending() {
    let key_id = "key-codex-reset-fence-replay";
    let (app, repository, _) = codex_refresh_test_state(key_id, None);
    let admin_state = AdminAppState::new(&app);

    for (fence_unix_ms, fence_id, redeem_id, request_started_at_unix_ms, usage) in [
        (250_000, "fence-a", "redeem-a", 260_000, 20.0),
        (300_000, "fence-b", "redeem-b", 310_000, 0.0),
    ] {
        let install = persist_codex_account_reset_fence(
            &admin_state,
            key_id,
            None,
            None,
            fence_unix_ms,
            fence_id,
            redeem_id,
        )
        .await
        .expect("reset fence should persist")
        .expect("reset fence should be returned");
        assert!(matches!(install, CodexAccountResetFenceInstall::Owned(_)));
        let baseline = json!({"codex": {
            "primary_used_percent": usage,
            "primary_reset_at": 2_000_000_000u64,
            "primary_window_minutes": 300u64
        }});
        assert!(persist_codex_provider_quota_refresh_state(
            &admin_state,
            key_id,
            Some(&baseline),
            admin_provider_quota_pure::CodexQuotaMergeContext {
                observed_at_unix_secs: request_started_at_unix_ms / 1_000,
                request_started_at_unix_ms: Some(request_started_at_unix_ms),
                request_order_id: Some("reset-refresh"),
                observed_reset_generation: Some(0),
                authoritative_reset_generation: None,
                observed_credential_generation: None,
                account_reset_fence_id: Some(fence_id),
                coverage: admin_provider_quota_pure::CodexQuotaWindowCoverage::AccountSnapshot,
            },
        )
        .await
        .expect("reset baseline should persist"));
    }

    let replay = persist_codex_account_reset_fence(
        &admin_state,
        key_id,
        None,
        None,
        350_000,
        "fence-a-replay",
        "redeem-a",
    )
    .await
    .expect("historical replay should be idempotent")
    .expect("historical replay should return an install result");
    assert_eq!(replay, CodexAccountResetFenceInstall::Superseded);

    let stored = repository
        .list_keys_by_ids(&[key_id.to_string()])
        .await
        .expect("key should reload")
        .pop()
        .expect("key should exist");
    let codex = &stored.upstream_metadata.as_ref().unwrap()["codex"];
    assert_eq!(codex["primary_used_percent"], json!(0.0));
    assert_eq!(codex["account_quota_reset_fence_id"], json!("fence-b"));
    assert_eq!(codex["account_quota_reset_pending"], json!(false));
    assert_eq!(
        codex["account_quota_reset_processed_ids"],
        json!(["redeem-a", "redeem-b"])
    );
}

#[tokio::test]
async fn fenced_stale_codex_refresh_keeps_usage_and_oauth_state() {
    let key_id = "key-codex-fenced-refresh-monotonic";
    let (app, repository, credential_fence) = codex_refresh_test_state(key_id, Some("auth-v1"));
    let credential_fence = credential_fence.expect("credential fence should exist");
    let admin_state = AdminAppState::new(&app);
    let stale_refresh = json!({"codex": {
        "primary_used_percent": 50.0,
        "primary_reset_at": 2_000_000_000u64,
        "primary_window_minutes": 300u64
    }});

    assert!(persist_fenced_provider_quota_refresh_state(
        &admin_state,
        key_id,
        credential_fence.encrypted_auth_config.as_str(),
        Some(&stale_refresh),
        Some(300),
        Some("refresh-state".to_string()),
        codex_merge_context(100_000),
        &credential_fence.credential,
    )
    .await
    .expect("fenced refresh persistence should complete"));

    let stored = repository
        .list_keys_by_ids(&[key_id.to_string()])
        .await
        .expect("key should reload")
        .pop()
        .expect("key should exist");
    assert_eq!(
        stored.upstream_metadata.as_ref().unwrap()["codex"]["primary_used_percent"],
        json!(60.0)
    );
    assert_eq!(stored.oauth_invalid_at_unix_secs, None);
    assert_eq!(stored.oauth_invalid_reason, None);
}

#[tokio::test]
async fn fenced_older_refresh_cannot_overwrite_newer_oauth_state() {
    let key_id = "key-codex-fenced-oauth-watermark";
    let (app, repository, credential_fence) = codex_refresh_test_state(key_id, Some("auth-v1"));
    let credential_fence = credential_fence.expect("credential fence should exist");
    let admin_state = AdminAppState::new(&app);
    let quota = |used_percent| {
        json!({"codex": {
            "primary_used_percent": used_percent,
            "primary_reset_at": 2_000_000_000u64,
            "primary_window_minutes": 300u64
        }})
    };

    assert!(persist_fenced_provider_quota_refresh_state(
        &admin_state,
        key_id,
        credential_fence.encrypted_auth_config.as_str(),
        Some(&quota(70.0)),
        None,
        None,
        codex_merge_context(300_000),
        &credential_fence.credential,
    )
    .await
    .expect("newer refresh should persist"));
    assert!(persist_fenced_provider_quota_refresh_state(
        &admin_state,
        key_id,
        credential_fence.encrypted_auth_config.as_str(),
        Some(&quota(65.0)),
        Some(250),
        Some("stale-invalid".to_string()),
        codex_merge_context(250_000),
        &credential_fence.credential,
    )
    .await
    .expect("older refresh should merge without replacing OAuth state"));

    let stored = repository
        .list_keys_by_ids(&[key_id.to_string()])
        .await
        .expect("key should reload")
        .pop()
        .expect("key should exist");
    assert_eq!(stored.oauth_invalid_at_unix_secs, None);
    assert_eq!(stored.oauth_invalid_reason, None);
    assert_eq!(
        stored.upstream_metadata.as_ref().unwrap()["codex"]
            [CODEX_OAUTH_STATE_REQUEST_WATERMARK_KEY],
        json!(300_000u64)
    );
}

#[tokio::test]
async fn fenced_same_millisecond_refresh_uses_request_id_for_oauth_order() {
    let key_id = "key-codex-fenced-oauth-id-watermark";
    let (app, repository, credential_fence) = codex_refresh_test_state(key_id, Some("auth-v1"));
    let credential_fence = credential_fence.expect("credential fence should exist");
    let admin_state = AdminAppState::new(&app);
    let quota = json!({"codex": {
        "primary_used_percent": 70.0,
        "primary_reset_at": 2_000_000_000u64,
        "primary_window_minutes": 300u64
    }});

    assert!(persist_fenced_provider_quota_refresh_state(
        &admin_state,
        key_id,
        credential_fence.encrypted_auth_config.as_str(),
        Some(&quota),
        None,
        None,
        codex_merge_context_with_id(300_000, Some("request-b")),
        &credential_fence.credential,
    )
    .await
    .expect("newer same-millisecond refresh should persist"));
    assert!(persist_fenced_provider_quota_refresh_state(
        &admin_state,
        key_id,
        credential_fence.encrypted_auth_config.as_str(),
        Some(&quota),
        Some(300),
        Some("stale-invalid".to_string()),
        codex_merge_context_with_id(300_000, Some("request-a")),
        &credential_fence.credential,
    )
    .await
    .expect("older same-millisecond refresh should merge without replacing OAuth state"));

    let stored = repository
        .list_keys_by_ids(&[key_id.to_string()])
        .await
        .expect("key should reload")
        .pop()
        .expect("key should exist");
    assert_eq!(stored.oauth_invalid_at_unix_secs, None);
    assert_eq!(stored.oauth_invalid_reason, None);
    let codex = &stored.upstream_metadata.as_ref().unwrap()["codex"];
    assert_eq!(
        codex[CODEX_OAUTH_STATE_REQUEST_WATERMARK_KEY],
        json!(300_000u64)
    );
    assert_eq!(
        codex[CODEX_OAUTH_STATE_REQUEST_WATERMARK_ID_KEY],
        json!("request-b")
    );
}

#[tokio::test]
async fn fenced_same_millisecond_newer_request_id_can_replace_oauth_state() {
    let key_id = "key-codex-fenced-oauth-id-watermark-newer-invalid";
    let (app, repository, credential_fence) = codex_refresh_test_state(key_id, Some("auth-v1"));
    let credential_fence = credential_fence.expect("credential fence should exist");
    let admin_state = AdminAppState::new(&app);
    let quota = json!({"codex": {
        "primary_used_percent": 70.0,
        "primary_reset_at": 2_000_000_000u64,
        "primary_window_minutes": 300u64
    }});

    assert!(persist_fenced_provider_quota_refresh_state(
        &admin_state,
        key_id,
        credential_fence.encrypted_auth_config.as_str(),
        Some(&quota),
        None,
        None,
        codex_merge_context_with_id(300_000, Some("request-a")),
        &credential_fence.credential,
    )
    .await
    .expect("older same-millisecond refresh should persist"));
    assert!(persist_fenced_provider_quota_refresh_state(
        &admin_state,
        key_id,
        credential_fence.encrypted_auth_config.as_str(),
        Some(&quota),
        Some(300),
        Some("newer-invalid".to_string()),
        codex_merge_context_with_id(300_000, Some("request-b")),
        &credential_fence.credential,
    )
    .await
    .expect("newer same-millisecond refresh should replace OAuth state"));

    let stored = repository
        .list_keys_by_ids(&[key_id.to_string()])
        .await
        .expect("key should reload")
        .pop()
        .expect("key should exist");
    assert_eq!(stored.oauth_invalid_at_unix_secs, Some(300));
    assert_eq!(
        stored.oauth_invalid_reason.as_deref(),
        Some("newer-invalid")
    );
    let codex = &stored.upstream_metadata.as_ref().unwrap()["codex"];
    assert_eq!(
        codex[CODEX_OAUTH_STATE_REQUEST_WATERMARK_KEY],
        json!(300_000u64)
    );
    assert_eq!(
        codex[CODEX_OAUTH_STATE_REQUEST_WATERMARK_ID_KEY],
        json!("request-b")
    );
}

#[tokio::test]
async fn fenced_older_success_cannot_clear_newer_oauth_invalid_state() {
    let key_id = "key-codex-newer-invalid-older-success";
    let (app, repository, credential_fence) = codex_refresh_test_state(key_id, Some("auth-v1"));
    let credential_fence = credential_fence.expect("credential fence should exist");
    let admin_state = AdminAppState::new(&app);
    let quota = json!({"codex": {
        "primary_used_percent": 70.0,
        "primary_reset_at": 2_000_000_000u64,
        "primary_window_minutes": 300u64
    }});

    assert!(persist_fenced_provider_quota_refresh_state(
        &admin_state,
        key_id,
        credential_fence.encrypted_auth_config.as_str(),
        Some(&quota),
        Some(300),
        Some("newer-invalid".to_string()),
        codex_merge_context_with_id(300_000, Some("request-newer")),
        &credential_fence.credential,
    )
    .await
    .expect("newer invalid response should persist"));
    assert!(persist_fenced_provider_quota_refresh_state(
        &admin_state,
        key_id,
        credential_fence.encrypted_auth_config.as_str(),
        Some(&quota),
        None,
        None,
        codex_merge_context_with_id(250_000, Some("request-older")),
        &credential_fence.credential,
    )
    .await
    .expect("older success should be harmlessly acknowledged"));

    let stored = repository
        .list_keys_by_ids(&[key_id.to_string()])
        .await
        .expect("key should reload")
        .pop()
        .expect("key should exist");
    assert_eq!(stored.oauth_invalid_at_unix_secs, Some(300));
    assert_eq!(
        stored.oauth_invalid_reason.as_deref(),
        Some("newer-invalid")
    );
    let codex = &stored.upstream_metadata.as_ref().unwrap()["codex"];
    assert_eq!(
        codex[CODEX_OAUTH_STATE_REQUEST_WATERMARK_KEY],
        json!(300_000u64)
    );
    assert_eq!(
        codex[CODEX_OAUTH_STATE_REQUEST_WATERMARK_ID_KEY],
        json!("request-newer")
    );
}

#[tokio::test]
async fn fenced_same_millisecond_older_success_cannot_clear_newer_invalid_state() {
    let key_id = "key-codex-same-ms-newer-invalid-older-success";
    let (app, repository, credential_fence) = codex_refresh_test_state(key_id, Some("auth-v1"));
    let credential_fence = credential_fence.expect("credential fence should exist");
    let admin_state = AdminAppState::new(&app);
    let quota = json!({"codex": {
        "primary_used_percent": 70.0,
        "primary_reset_at": 2_000_000_000u64,
        "primary_window_minutes": 300u64
    }});

    assert!(persist_fenced_provider_quota_refresh_state(
        &admin_state,
        key_id,
        credential_fence.encrypted_auth_config.as_str(),
        Some(&quota),
        Some(300),
        Some("newer-invalid".to_string()),
        codex_merge_context_with_id(300_000, Some("request-b")),
        &credential_fence.credential,
    )
    .await
    .expect("newer same-millisecond invalid response should persist"));
    assert!(persist_fenced_provider_quota_refresh_state(
        &admin_state,
        key_id,
        credential_fence.encrypted_auth_config.as_str(),
        Some(&quota),
        None,
        None,
        codex_merge_context_with_id(300_000, Some("request-a")),
        &credential_fence.credential,
    )
    .await
    .expect("older same-millisecond success should be acknowledged"));

    let stored = repository
        .list_keys_by_ids(&[key_id.to_string()])
        .await
        .expect("key should reload")
        .pop()
        .expect("key should exist");
    assert_eq!(stored.oauth_invalid_at_unix_secs, Some(300));
    assert_eq!(
        stored.oauth_invalid_reason.as_deref(),
        Some("newer-invalid")
    );
    let codex = &stored.upstream_metadata.as_ref().unwrap()["codex"];
    assert_eq!(
        codex[CODEX_OAUTH_STATE_REQUEST_WATERMARK_ID_KEY],
        json!("request-b")
    );
}

#[tokio::test]
async fn metadata_cas_conflict_does_not_persist_stale_oauth_runtime_state() {
    let provider_id = "provider-codex-cas";
    let key_id = "key-codex-cas";
    let bootstrap = crate::tests::support::credential_fixture_state();
    let old_auth_config = bootstrap
        .seal_provider_catalog_key_auth_config(provider_id, key_id, "old-auth-config")
        .expect("old auth config should seal");
    let new_auth_config = bootstrap
        .seal_provider_catalog_key_auth_config(provider_id, key_id, "new-auth-config")
        .expect("new auth config should seal");
    let mut key = StoredProviderCatalogKey::new(
        key_id.to_string(),
        provider_id.to_string(),
        "Codex CAS".to_string(),
        "oauth".to_string(),
        None,
        true,
    )
    .expect("key should build");
    key.encrypted_auth_config = Some(old_auth_config.clone());
    key.oauth_invalid_at_unix_secs = Some(100);
    key.oauth_invalid_reason = Some("old-invalid-reason".to_string());
    key.upstream_metadata = Some(json!({"codex":{"remaining":5}}));
    key.status_snapshot = Some(json!({"oauth":{"invalid":true}}));

    let repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![StoredProviderCatalogProvider::new(
            provider_id.to_string(),
            "Codex CAS".to_string(),
            None,
            "codex".to_string(),
        )
        .expect("provider should build")],
        vec![],
        vec![key],
    ));
    let app = AppState::new()
        .expect("app should build")
        .with_data_state_for_tests(
            GatewayDataState::with_provider_catalog_repository_for_tests(Arc::clone(&repository))
                .with_encryption_key_for_tests(DEVELOPMENT_ENCRYPTION_KEY),
        );
    let admin_state = AdminAppState::new(&app);
    let credential_fence = ProviderTransportCredentialFence {
        encrypted_auth_config: old_auth_config.clone(),
        credential: ProviderCatalogKeyOAuthCredentialFence {
            encrypted_api_key: None,
            auth_type: "oauth".to_string(),
            provider_id: provider_id.to_string(),
            provider_type: "codex".to_string(),
        },
    };
    let concurrent_repository = Arc::clone(&repository);
    let metadata_update = json!({"codex":{"remaining":3}});

    let persisted = persist_provider_quota_refresh_state_after_read(
        &admin_state,
        key_id,
        Some(&metadata_update),
        Some(200),
        Some("new-invalid-reason".to_string()),
        Some(new_auth_config),
        Some(&credential_fence),
        async move {
            assert!(concurrent_repository
                .update_key_runtime_metadata(&ProviderCatalogKeyRuntimeMetadataUpdate {
                    key_id: "key-codex-cas".to_string(),
                    namespace: "codex".to_string(),
                    expected_upstream_metadata_value: Some(json!({"remaining":5})),
                    upstream_metadata_value: json!({"remaining":4}),
                    status_snapshot_patch: json!({}),
                    updated_at_unix_secs: Some(150),
                })
                .await
                .expect("concurrent metadata update should execute"));
        },
    )
    .await
    .expect("quota refresh persistence should not error");

    assert!(!persisted, "stale namespace should report a CAS conflict");
    let stored = repository
        .list_keys_by_ids(&["key-codex-cas".to_string()])
        .await
        .expect("key should reload")
        .pop()
        .expect("key should remain");
    assert_eq!(
        stored.encrypted_auth_config.as_deref(),
        Some(old_auth_config.as_str())
    );
    assert_eq!(stored.oauth_invalid_at_unix_secs, Some(100));
    assert_eq!(
        stored.oauth_invalid_reason.as_deref(),
        Some("old-invalid-reason")
    );
    assert_eq!(
        stored.upstream_metadata.as_ref().unwrap()["codex"],
        json!({"remaining":4})
    );
}

#[tokio::test]
async fn quota_refresh_strong_read_bypasses_stale_provider_catalog_cache() {
    let mut key = StoredProviderCatalogKey::new(
        "key-antigravity-stale-cache".to_string(),
        "provider-antigravity-stale-cache".to_string(),
        "Antigravity stale cache".to_string(),
        "oauth".to_string(),
        None,
        true,
    )
    .expect("key should build");
    key.upstream_metadata = Some(json!({
        "antigravity": {
            "project_id": "project-1",
            "quota_by_model": {
                "gemini-3.7-flash-tiered": {"remaining_fraction": 0.9}
            }
        }
    }));

    // The whole-catalog snapshot only answers for providers it has loaded, so the
    // fixture has to seed the owning provider for the stale-read assertion below.
    let provider = StoredProviderCatalogProvider::new(
        "provider-antigravity-stale-cache".to_string(),
        "Antigravity stale cache".to_string(),
        None,
        "antigravity".to_string(),
    )
    .expect("provider should build");
    let repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![provider],
        vec![],
        vec![key],
    ));
    let data =
        GatewayDataState::with_provider_catalog_repository_for_tests(Arc::clone(&repository))
            .with_cached_provider_catalog_reader_for_tests(Arc::clone(&repository));
    let app = AppState::new()
        .expect("app should build")
        .with_data_state_for_tests(data);
    let admin_state = AdminAppState::new(&app);
    let key_ids = ["key-antigravity-stale-cache".to_string()];

    let cached = app
        .read_provider_catalog_keys_by_ids(&key_ids)
        .await
        .expect("initial cached read should succeed");
    assert_eq!(
        cached[0].upstream_metadata.as_ref().unwrap()["antigravity"]["quota_by_model"]
            ["gemini-3.7-flash-tiered"]["remaining_fraction"],
        json!(0.9)
    );

    let current_namespace = json!({
        "project_id": "project-1",
        "model_fetch_revision": 2,
        "quota_by_model": {
            "gemini-3.7-flash-tiered": {"remaining_fraction": 0.7}
        }
    });
    assert!(repository
        .upsert_key_upstream_metadata_namespace(
            "key-antigravity-stale-cache",
            "antigravity",
            &current_namespace,
            None,
        )
        .await
        .expect("out-of-band metadata update should succeed"));
    let still_cached = app
        .read_provider_catalog_keys_by_ids(&key_ids)
        .await
        .expect("stale cached read should succeed");
    assert_eq!(
        still_cached[0].upstream_metadata.as_ref().unwrap()["antigravity"]["quota_by_model"]
            ["gemini-3.7-flash-tiered"]["remaining_fraction"],
        json!(0.9),
        "regression setup must keep the ordinary read stale"
    );

    let metadata_update = json!({
        "antigravity": {
            "project_id": "project-1",
            "quota_by_model": {
                "gemini-3.7-flash-tiered": {"remaining_fraction": 0.6}
            },
            "quota_groups": [{
                "display_name": "Gemini models",
                "buckets": [{"bucket_id": "gemini-weekly", "window": "weekly"}]
            }]
        }
    });
    assert!(persist_provider_quota_refresh_state(
        &admin_state,
        "key-antigravity-stale-cache",
        Some(&metadata_update),
        None,
        None,
        None,
    )
    .await
    .expect("quota refresh persistence should not error"));

    let stored = repository
        .list_keys_by_ids(&key_ids)
        .await
        .expect("key should reload")
        .pop()
        .expect("key should exist");
    assert_eq!(
        stored.upstream_metadata.as_ref().unwrap()["antigravity"]["quota_groups"][0]["buckets"][0]
            ["bucket_id"],
        json!("gemini-weekly")
    );
    assert_eq!(
        stored.upstream_metadata.as_ref().unwrap()["antigravity"]["model_fetch_revision"],
        json!(2),
        "quota refresh must preserve fields written by another Antigravity metadata producer"
    );
    assert_eq!(
        stored.upstream_metadata.as_ref().unwrap()["antigravity"]["quota_by_model"]
            ["gemini-3.7-flash-tiered"]["remaining_fraction"],
        json!(0.6)
    );
}
