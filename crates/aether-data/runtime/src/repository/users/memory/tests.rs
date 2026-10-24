use std::sync::Arc;

use super::*;
use crate::repository::users::{UserExportListQuery, UserReadRepository};

fn user_group_record(name: &str, priority: i32) -> UpsertUserGroupRecord {
    UpsertUserGroupRecord {
        name: name.to_string(),
        description: Some(format!("{name} description")),
        priority,
        allowed_providers: Some(vec!["provider-a".to_string()]),
        allowed_providers_mode: "specific".to_string(),
        allowed_api_formats: Some(vec!["chat".to_string()]),
        allowed_api_formats_mode: "specific".to_string(),
        allowed_models: Some(vec!["model-a".to_string()]),
        allowed_models_mode: "specific".to_string(),
        rate_limit: Some(10),
        rate_limit_mode: "custom".to_string(),
    }
}

#[tokio::test]
async fn lists_seeded_users() {
    let user = StoredUserSummary::new(
        "user-1".to_string(),
        "alice".to_string(),
        Some("alice@example.com".to_string()),
        "user".to_string(),
        true,
        false,
    )
    .expect("user should build");
    let repository = InMemoryUserReadRepository::seed(vec![user.clone()]);
    let rows = repository
        .list_users_by_ids(&["user-1".to_string()])
        .await
        .expect("lookup should succeed");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0], user);
}

#[tokio::test]
async fn restores_user_group_only_when_complete_snapshot_matches() {
    let repository = InMemoryUserReadRepository::default();
    let before = repository
        .create_user_group(user_group_record("cas-group", 1))
        .await
        .expect("group creation should succeed")
        .expect("group should be created");
    let after = repository
        .update_user_group(&before.id, user_group_record("cas-group-imported", 2))
        .await
        .expect("group update should succeed")
        .expect("group should exist");

    assert!(repository
        .restore_user_group_if_matches(&after, &before)
        .await
        .expect("matching group restore should succeed"));
    assert_eq!(
        repository
            .find_user_group_by_id(&before.id)
            .await
            .expect("group lookup should succeed"),
        Some(before.clone())
    );

    let current_after_second_update = repository
        .update_user_group(&before.id, user_group_record("cas-group-concurrent", 3))
        .await
        .expect("second group update should succeed")
        .expect("group should exist");
    assert!(!repository
        .restore_user_group_if_matches(&after, &before)
        .await
        .expect("stale group restore should return a conflict"));
    assert_eq!(
        repository
            .find_user_group_by_id(&before.id)
            .await
            .expect("group lookup should succeed"),
        Some(current_after_second_update)
    );
}

#[tokio::test]
async fn user_group_restore_rejects_identity_mismatch_and_missing_rows() {
    let repository = InMemoryUserReadRepository::default();
    let expected = repository
        .create_user_group(user_group_record("cas-identity", 1))
        .await
        .expect("group creation should succeed")
        .expect("group should be created");
    let mut different_id = expected.clone();
    different_id.id = "different-group-id".to_string();
    assert!(!repository
        .restore_user_group_if_matches(&expected, &different_id)
        .await
        .expect("identity mismatch should return false"));

    let mut missing = expected.clone();
    missing.id = "missing-group-id".to_string();
    assert!(!repository
        .restore_user_group_if_matches(&missing, &missing)
        .await
        .expect("missing row should return false"));
}

#[tokio::test]
async fn lists_seeded_non_admin_export_users() {
    let user = StoredUserExportRow::new(
        "user-1".to_string(),
        Some("alice@example.com".to_string()),
        true,
        "alice".to_string(),
        Some("hash".to_string()),
        "user".to_string(),
        "local".to_string(),
        Some(serde_json::json!(["openai"])),
        Some(serde_json::json!(["openai:chat"])),
        Some(serde_json::json!(["gpt-4.1"])),
        Some(60),
        Some(serde_json::json!({"gpt-4.1": {"cache_1h": true}})),
        true,
    )
    .expect("user export row should build");
    let repository = InMemoryUserReadRepository::seed_export_users(vec![user.clone()]);

    let rows = repository
        .list_non_admin_export_users()
        .await
        .expect("export should succeed");

    assert_eq!(rows, vec![user]);
}

#[tokio::test]
async fn finds_seeded_auth_user_by_id_and_identifier() {
    let user = StoredUserAuthRecord::new(
        "user-1".to_string(),
        Some("alice@example.com".to_string()),
        true,
        "alice".to_string(),
        Some("hash".to_string()),
        "user".to_string(),
        "local".to_string(),
        None,
        None,
        None,
        true,
        false,
        None,
        None,
    )
    .expect("auth user should build");
    let repository = InMemoryUserReadRepository::seed_auth_users(vec![user.clone()]);

    let by_id = repository
        .find_user_auth_by_id("user-1")
        .await
        .expect("lookup by id should succeed");
    let by_email = repository
        .find_user_auth_by_identifier("alice@example.com")
        .await
        .expect("lookup by email should succeed");
    let by_username = repository
        .find_user_auth_by_identifier("alice")
        .await
        .expect("lookup by username should succeed");
    let by_exact_email = repository
        .find_user_auth_by_email("alice@example.com")
        .await
        .expect("exact email lookup should succeed");
    let by_exact_username = repository
        .find_user_auth_by_username("alice")
        .await
        .expect("exact username lookup should succeed");
    let email_should_not_match_username = repository
        .find_user_auth_by_email("alice")
        .await
        .expect("exact email lookup should succeed");

    assert_eq!(by_id, Some(user.clone()));
    assert_eq!(by_email, Some(user.clone()));
    assert_eq!(by_username, Some(user.clone()));
    assert_eq!(by_exact_email, Some(user.clone()));
    assert_eq!(by_exact_username, Some(user));
    assert_eq!(email_should_not_match_username, None);
}

#[tokio::test]
async fn touches_auth_user_last_login_in_memory() {
    let user = StoredUserAuthRecord::new(
        "user-1".to_string(),
        Some("alice@example.com".to_string()),
        true,
        "alice".to_string(),
        Some("hash".to_string()),
        "user".to_string(),
        "local".to_string(),
        None,
        None,
        None,
        true,
        false,
        None,
        None,
    )
    .expect("auth user should build");
    let repository = InMemoryUserReadRepository::seed_auth_users(vec![user]);
    let logged_in_at = chrono::Utc::now();

    assert!(repository
        .touch_auth_user_last_login("user-1", logged_in_at)
        .await
        .expect("touch should succeed"));
    assert!(!repository
        .touch_auth_user_last_login("missing-user", logged_in_at)
        .await
        .expect("missing touch should succeed"));
    assert_eq!(
        repository
            .find_user_auth_by_id("user-1")
            .await
            .expect("auth lookup should succeed")
            .expect("auth user should exist")
            .last_login_at,
        Some(logged_in_at)
    );
}

#[tokio::test]
async fn updates_local_auth_user_profile_and_password_in_memory() {
    let user = StoredUserAuthRecord::new(
        "user-1".to_string(),
        Some("alice@example.com".to_string()),
        true,
        "alice".to_string(),
        Some("old-hash".to_string()),
        "user".to_string(),
        "local".to_string(),
        None,
        None,
        None,
        true,
        false,
        None,
        None,
    )
    .expect("auth user should build");
    let export_user = StoredUserExportRow::new(
        "user-1".to_string(),
        Some("alice@example.com".to_string()),
        true,
        "alice".to_string(),
        Some("old-hash".to_string()),
        "user".to_string(),
        "local".to_string(),
        None,
        None,
        None,
        Some(10),
        None,
        true,
    )
    .expect("export user should build");
    let repository =
        InMemoryUserReadRepository::seed_auth_users(vec![user]).with_export_users([export_user]);

    let updated = repository
        .update_local_auth_user_profile(
            "user-1",
            true,
            Some("alice2@example.com".to_string()),
            Some(true),
            Some("alice2".to_string()),
        )
        .await
        .expect("profile update should succeed")
        .expect("profile update should return user");
    assert_eq!(updated.email.as_deref(), Some("alice2@example.com"));
    assert!(updated.email_verified);
    assert_eq!(updated.username, "alice2");

    let cleared = repository
        .update_local_auth_user_profile("user-1", true, None, Some(false), None)
        .await
        .expect("nullable email update should succeed")
        .expect("user should exist");
    assert!(cleared.email.is_none());
    assert!(!cleared.email_verified);
    assert!(repository
        .find_user_auth_by_identifier("alice@example.com")
        .await
        .expect("old email lookup should succeed")
        .is_none());
    assert_eq!(
        repository
            .find_user_auth_by_identifier("alice2")
            .await
            .expect("new username lookup should succeed")
            .expect("new username should resolve")
            .id,
        "user-1"
    );

    let password_updated = repository
        .update_local_auth_user_password_hash("user-1", "new-hash".to_string(), chrono::Utc::now())
        .await
        .expect("password update should succeed")
        .expect("password update should return user");
    assert_eq!(password_updated.password_hash.as_deref(), Some("new-hash"));
    let admin_updated = repository
        .update_local_auth_user_admin_fields(
            "user-1",
            Some("admin".to_string()),
            true,
            Some(vec!["openai".to_string()]),
            true,
            None,
            true,
            Some(vec!["gpt-4.1".to_string()]),
            true,
            Some(50),
            Some(false),
        )
        .await
        .expect("admin fields update should succeed")
        .expect("admin fields update should return user");
    assert_eq!(admin_updated.role, "admin");
    assert_eq!(
        admin_updated.allowed_providers,
        Some(vec!["openai".to_string()])
    );
    assert_eq!(admin_updated.allowed_api_formats, None);
    assert_eq!(
        admin_updated.allowed_models,
        Some(vec!["gpt-4.1".to_string()])
    );
    assert!(!admin_updated.is_active);
    assert_eq!(
        repository
            .find_export_user_by_id("user-1")
            .await
            .expect("export lookup should succeed")
            .expect("export row should exist")
            .rate_limit,
        Some(50)
    );
    repository
        .update_local_auth_user_admin_fields(
            "user-1", None, false, None, false, None, false, None, true, None, None,
        )
        .await
        .expect("rate limit clear should succeed")
        .expect("rate limit clear should return user");
    assert_eq!(
        repository
            .find_export_user_by_id("user-1")
            .await
            .expect("export lookup should succeed")
            .expect("export row should exist")
            .rate_limit,
        None
    );
    assert_eq!(
        repository
            .update_user_model_capability_settings(
                "user-1",
                Some(serde_json::json!({"gpt-4.1": {"enabled": true}})),
            )
            .await
            .expect("model settings update should succeed"),
        Some(serde_json::json!({"gpt-4.1": {"enabled": true}}))
    );
    assert_eq!(
        repository
            .update_user_model_capability_settings("user-1", Some(serde_json::Value::Null))
            .await
            .expect("model settings clear should succeed"),
        None
    );
    assert!(repository
        .update_local_auth_user_profile("missing-user", false, None, None, None)
        .await
        .expect("missing profile update should succeed")
        .is_none());
    assert!(repository
        .delete_local_auth_user("user-1")
        .await
        .expect("delete should succeed"));
    assert!(!repository
        .delete_local_auth_user("user-1")
        .await
        .expect("second delete should succeed"));
    assert!(repository
        .find_user_auth_by_identifier("alice2")
        .await
        .expect("deleted username lookup should succeed")
        .is_none());
}

#[tokio::test]
async fn restores_nullable_password_only_when_expected_hash_matches() {
    let user = StoredUserAuthRecord::new(
        "user-password-cas".to_string(),
        Some("password-cas@example.com".to_string()),
        true,
        "password-cas".to_string(),
        Some("imported-hash".to_string()),
        "user".to_string(),
        "local".to_string(),
        None,
        None,
        None,
        true,
        false,
        None,
        None,
    )
    .expect("auth user should build");
    let repository = InMemoryUserReadRepository::seed_auth_users([user]);

    assert!(repository
        .restore_local_auth_user_password_hash_if_matches(
            "user-password-cas",
            Some("imported-hash"),
            None,
            chrono::Utc::now(),
        )
        .await
        .expect("nullable restore should succeed"));
    assert!(repository
        .find_user_auth_by_id("user-password-cas")
        .await
        .expect("user lookup should succeed")
        .expect("user should exist")
        .password_hash
        .is_none());

    assert!(!repository
        .restore_local_auth_user_password_hash_if_matches(
            "user-password-cas",
            Some("stale-hash"),
            Some("old-hash".to_string()),
            chrono::Utc::now(),
        )
        .await
        .expect("conflicting restore should return false"));
    assert!(repository
        .find_user_auth_by_id("user-password-cas")
        .await
        .expect("user lookup should succeed")
        .expect("user should exist")
        .password_hash
        .is_none());
}

#[tokio::test]
async fn hard_delete_preserves_last_admin_then_cleans_owned_memory_state() {
    let now = chrono::Utc::now();
    let admin = StoredUserAuthRecord::new(
        "admin-delete-target".to_string(),
        Some("admin-delete@example.com".to_string()),
        true,
        "admin-delete".to_string(),
        Some("password-hash".to_string()),
        "admin".to_string(),
        "local".to_string(),
        None,
        None,
        None,
        true,
        false,
        Some(now),
        None,
    )
    .expect("admin should build");
    let export_row = StoredUserExportRow::new(
        admin.id.clone(),
        admin.email.clone(),
        admin.email_verified,
        admin.username.clone(),
        admin.password_hash.clone(),
        admin.role.clone(),
        admin.auth_source.clone(),
        None,
        None,
        None,
        None,
        Some(serde_json::json!({"gpt-4.1": {"enabled": true}})),
        true,
    )
    .expect("export row should build")
    .with_feature_settings(Some(serde_json::json!({"feature": true})));
    let session = StoredUserSessionRecord::new(
        "admin-delete-session".to_string(),
        admin.id.clone(),
        "admin-delete-device".to_string(),
        None,
        StoredUserSessionRecord::hash_refresh_token("admin-delete-refresh"),
        None,
        None,
        Some(now),
        Some(now + chrono::Duration::hours(1)),
        None,
        None,
        None,
        None,
        Some(now),
        Some(now),
    )
    .expect("session should build");
    let preferences = StoredUserPreferenceRecord {
        user_id: admin.id.clone(),
        avatar_url: None,
        bio: None,
        default_provider_id: None,
        default_provider_name: None,
        theme: "system".to_string(),
        language: "zh-CN".to_string(),
        timezone: "Asia/Shanghai".to_string(),
        email_notifications: true,
        usage_alerts: true,
        announcement_notifications: true,
        allow_wallet_overage: true,
    };
    let repository = InMemoryUserReadRepository::seed_auth_users([admin.clone()])
        .with_export_users([export_row])
        .with_user_preferences([preferences])
        .with_user_sessions([session]);
    repository
        .oauth_links_by_id
        .write()
        .expect("user repository lock")
        .insert(
            "admin-delete-oauth".to_string(),
            StoredMemoryOAuthLink {
                id: "admin-delete-oauth".to_string(),
                user_id: admin.id.clone(),
                provider_type: "test".to_string(),
                provider_user_id: "admin-delete-subject".to_string(),
                provider_username: None,
                provider_email: admin.email.clone(),
                extra_data: None,
                linked_at: now,
                last_login_at: None,
            },
        );
    repository
        .group_members
        .write()
        .expect("user repository lock")
        .insert(("group-1".to_string(), admin.id.clone()), now);
    repository
        .model_settings_by_user_id
        .write()
        .expect("user repository lock")
        .insert(admin.id.clone(), serde_json::json!({"model": true}));
    repository
        .feature_settings_by_user_id
        .write()
        .expect("user repository lock")
        .insert(admin.id.clone(), serde_json::json!({"feature": true}));
    repository
        .ldap_dn_by_user_id
        .write()
        .expect("user repository lock")
        .insert(admin.id.clone(), "uid=admin-delete,dc=example".to_string());
    repository
        .ldap_username_by_user_id
        .write()
        .expect("user repository lock")
        .insert(admin.id.clone(), "admin-delete-ldap".to_string());

    let error = repository
        .delete_local_auth_user(&admin.id)
        .await
        .expect_err("last active admin delete must be rejected");
    assert!(crate::repository::users::is_last_active_admin_delete_denied(&error));
    assert!(repository
        .auth_by_id
        .read()
        .expect("user repository lock")
        .contains_key(&admin.id));
    assert!(repository
        .sessions_by_id
        .read()
        .expect("user repository lock")
        .values()
        .any(|session| session.user_id == admin.id));
    assert!(repository
        .oauth_links_by_id
        .read()
        .expect("user repository lock")
        .values()
        .any(|link| link.user_id == admin.id));

    repository
        .create_local_auth_user_with_settings(
            Some("admin-keeper@example.com".to_string()),
            true,
            "admin-keeper".to_string(),
            "password-hash".to_string(),
            "admin".to_string(),
            None,
            None,
            None,
            None,
        )
        .await
        .expect("second admin creation should succeed")
        .expect("second admin should exist");
    assert!(repository
        .delete_local_auth_user(&admin.id)
        .await
        .expect("delete with another active admin should succeed"));

    assert!(!repository
        .by_id
        .read()
        .expect("user repository lock")
        .contains_key(&admin.id));
    assert!(!repository
        .auth_by_id
        .read()
        .expect("user repository lock")
        .contains_key(&admin.id));
    assert!(!repository
        .auth_by_identifier
        .read()
        .expect("user repository lock")
        .values()
        .any(|user_id| user_id == &admin.id));
    assert!(!repository
        .sessions_by_id
        .read()
        .expect("user repository lock")
        .values()
        .any(|session| session.user_id == admin.id));
    assert!(!repository
        .oauth_links_by_id
        .read()
        .expect("user repository lock")
        .values()
        .any(|link| link.user_id == admin.id));
    assert!(!repository
        .group_members
        .read()
        .expect("user repository lock")
        .keys()
        .any(|(_, user_id)| user_id == &admin.id));
    assert!(!repository
        .preferences_by_user_id
        .read()
        .expect("user repository lock")
        .contains_key(&admin.id));
    assert!(!repository
        .model_settings_by_user_id
        .read()
        .expect("user repository lock")
        .contains_key(&admin.id));
    assert!(!repository
        .feature_settings_by_user_id
        .read()
        .expect("user repository lock")
        .contains_key(&admin.id));
    assert!(!repository
        .ldap_dn_by_user_id
        .read()
        .expect("user repository lock")
        .contains_key(&admin.id));
    assert!(!repository
        .ldap_username_by_user_id
        .read()
        .expect("user repository lock")
        .contains_key(&admin.id));
    assert!(!repository
        .export_rows
        .read()
        .expect("user repository lock")
        .iter()
        .any(|row| row.id == admin.id));
}

#[tokio::test]
async fn creates_local_auth_users_in_memory() {
    let repository = InMemoryUserReadRepository::default();

    let user = repository
        .create_local_auth_user(
            Some("alice@example.com".to_string()),
            true,
            "alice".to_string(),
            "hash".to_string(),
        )
        .await
        .expect("user create should succeed")
        .expect("user create should return user");
    assert_eq!(user.email.as_deref(), Some("alice@example.com"));
    assert_eq!(user.username, "alice");
    assert_eq!(user.role, "user");
    assert_eq!(user.auth_source, "local");

    let admin = repository
        .create_local_auth_user_with_settings(
            Some("admin@example.com".to_string()),
            true,
            "admin".to_string(),
            "admin-hash".to_string(),
            "admin".to_string(),
            Some(vec!["openai".to_string()]),
            Some(vec!["chat".to_string()]),
            Some(vec!["gpt-4.1".to_string()]),
            Some(10),
        )
        .await
        .expect("admin create should succeed")
        .expect("admin create should return user");
    assert_eq!(admin.role, "admin");
    assert_eq!(admin.allowed_providers, Some(vec!["openai".to_string()]));
    assert_eq!(admin.allowed_api_formats, Some(vec!["chat".to_string()]));
    assert_eq!(admin.allowed_models, Some(vec!["gpt-4.1".to_string()]));
    assert_eq!(
        repository
            .find_user_auth_by_username("admin")
            .await
            .expect("created admin lookup should succeed")
            .expect("created admin should exist")
            .id,
        admin.id
    );
}

#[tokio::test]
async fn provisions_ldap_auth_users_in_memory() {
    let repository = InMemoryUserReadRepository::default();
    let logged_in_at = chrono::Utc::now();

    let created = repository
        .get_or_create_ldap_auth_user(
            "ldap@example.com".to_string(),
            "ldap_user".to_string(),
            Some("cn=ldap-user,dc=example".to_string()),
            Some("ldap_user".to_string()),
            logged_in_at,
        )
        .await
        .expect("ldap create should succeed")
        .expect("ldap create should return user");
    assert!(created.created);
    assert_eq!(created.user.auth_source, "ldap");
    assert_eq!(created.user.email.as_deref(), Some("ldap@example.com"));
    assert_eq!(created.user.username, "ldap_user");

    let existing = repository
        .get_or_create_ldap_auth_user(
            "ldap2@example.com".to_string(),
            "ignored".to_string(),
            Some("cn=ldap-user,dc=example".to_string()),
            Some("ldap_user".to_string()),
            logged_in_at,
        )
        .await
        .expect("ldap update should succeed")
        .expect("ldap update should return user");
    assert!(!existing.created);
    assert_eq!(existing.user.id, created.user.id);
    assert_eq!(existing.user.email.as_deref(), Some("ldap2@example.com"));
}

#[tokio::test]
async fn manages_oauth_users_and_links_in_memory() {
    let repository = InMemoryUserReadRepository::default();
    let now = chrono::Utc::now();
    let user = repository
        .create_oauth_auth_user(
            Some("OAuth@Example.com".to_string()),
            false,
            "oauth_user".to_string(),
            now,
        )
        .await
        .expect("oauth user should create")
        .expect("oauth user should exist");
    assert_eq!(user.auth_source, "oauth");
    assert!(!user.email_verified);
    assert_eq!(
        repository
            .find_active_user_auth_by_email_ci("oauth@example.com")
            .await
            .expect("ci lookup should work")
            .map(|user| user.id),
        Some(user.id.clone())
    );

    repository
        .bind_user_oauth_link(
            &user.id,
            "linuxdo",
            "subject-1",
            Some("alice"),
            Some("alice@example.com"),
            Some(serde_json::json!({"sub": "subject-1"})),
            now,
        )
        .await
        .expect("link should upsert");
    assert_eq!(
        repository
            .find_oauth_link_owner("linuxdo", "subject-1")
            .await
            .expect("owner lookup should work"),
        Some(user.id.clone())
    );
    assert!(repository
        .has_user_oauth_provider_link(&user.id, "linuxdo")
        .await
        .expect("provider link lookup should work"));
    assert_eq!(
        repository
            .list_user_oauth_links(&user.id)
            .await
            .expect("links should list")
            .len(),
        1
    );
    assert!(repository
        .touch_oauth_link(
            "linuxdo",
            "subject-1",
            Some("alice2"),
            None,
            Some(serde_json::json!({"fresh": true})),
            now + chrono::Duration::seconds(10),
        )
        .await
        .expect("link should touch"));
    assert_eq!(
        repository
            .delete_user_oauth_link(&user.id, "linuxdo", false, &["linuxdo".to_string()],)
            .await
            .expect("last link deletion should resolve"),
        DeleteUserOAuthLinkOutcome::LastOAuthBinding
    );
    repository
        .bind_user_oauth_link(
            &user.id,
            "github",
            "subject-2",
            Some("alice"),
            Some("alice@example.com"),
            None,
            now,
        )
        .await
        .expect("second link should upsert");
    assert_eq!(
        repository
            .delete_user_oauth_link(
                &user.id,
                "linuxdo",
                false,
                &["linuxdo".to_string(), "github".to_string()],
            )
            .await
            .expect("link should delete"),
        DeleteUserOAuthLinkOutcome::Deleted
    );
}

#[tokio::test]
async fn oauth_bind_rejects_unavailable_session_without_creating_link_in_memory() {
    let now = chrono::Utc::now();
    let user = StoredUserAuthRecord::new(
        "oauth-bind-user".to_string(),
        Some("oauth-bind@example.com".to_string()),
        true,
        "oauth-bind-user".to_string(),
        None,
        "user".to_string(),
        "oauth".to_string(),
        None,
        None,
        None,
        true,
        false,
        Some(now),
        None,
    )
    .expect("oauth bind user should build")
    .with_security_version(7)
    .expect("user security version should be valid");
    let valid_session = StoredUserSessionRecord::new(
        "oauth-bind-session".to_string(),
        user.id.clone(),
        "oauth-bind-device".to_string(),
        None,
        StoredUserSessionRecord::hash_refresh_token("oauth-bind-refresh"),
        None,
        None,
        Some(now),
        Some(now + chrono::Duration::hours(1)),
        None,
        None,
        None,
        None,
        Some(now),
        Some(now),
    )
    .expect("oauth bind session should build")
    .with_security_version(7)
    .expect("session security version should be valid");

    let mut revoked_session = valid_session.clone();
    revoked_session.revoked_at = Some(now);
    let mut expired_session = valid_session.clone();
    expired_session.expires_at = Some(now - chrono::Duration::seconds(1));
    let cases = [
        (
            "revoked",
            revoked_session,
            BindUserOAuthLinkSessionExpectation::new(
                "oauth-bind-session",
                "oauth-bind-device",
                7,
                now,
            )
            .expect("revoked expectation should build"),
        ),
        (
            "expired",
            expired_session,
            BindUserOAuthLinkSessionExpectation::new(
                "oauth-bind-session",
                "oauth-bind-device",
                7,
                now,
            )
            .expect("expired expectation should build"),
        ),
        (
            "device-mismatch",
            valid_session.clone(),
            BindUserOAuthLinkSessionExpectation::new("oauth-bind-session", "other-device", 7, now)
                .expect("device expectation should build"),
        ),
        (
            "security-version-mismatch",
            valid_session,
            BindUserOAuthLinkSessionExpectation::new(
                "oauth-bind-session",
                "oauth-bind-device",
                6,
                now,
            )
            .expect("security expectation should build"),
        ),
    ];

    for (case, session, expectation) in cases {
        let repository = InMemoryUserReadRepository::seed_auth_users([user.clone()])
            .with_user_sessions([session]);
        let subject = format!("subject-{case}");
        assert_eq!(
            repository
                .bind_user_oauth_link_if_provider_enabled(
                    &user.id,
                    "linuxdo",
                    &subject,
                    None,
                    None,
                    None,
                    now,
                    true,
                    Some(&expectation),
                )
                .await
                .expect("session-bound OAuth bind should resolve"),
            BindUserOAuthLinkOutcome::SessionUnavailable,
            "case {case} should reject",
        );
        assert_eq!(
            repository
                .count_user_oauth_links(&user.id)
                .await
                .expect("OAuth links should count"),
            0,
            "case {case} must not create a link",
        );
    }
}

#[tokio::test]
async fn concurrent_oauth_binds_preserve_single_identity_owner_in_memory() {
    let repository = Arc::new(InMemoryUserReadRepository::default());
    let now = chrono::Utc::now();
    let first_user = repository
        .create_oauth_auth_user(None, false, "bind-first".to_string(), now)
        .await
        .expect("first user should create")
        .expect("first user should exist");
    let second_user = repository
        .create_oauth_auth_user(None, false, "bind-second".to_string(), now)
        .await
        .expect("second user should create")
        .expect("second user should exist");
    let barrier = Arc::new(tokio::sync::Barrier::new(3));

    let first_repository = Arc::clone(&repository);
    let first_barrier = Arc::clone(&barrier);
    let first_id = first_user.id.clone();
    let first = tokio::spawn(async move {
        first_barrier.wait().await;
        first_repository
            .bind_user_oauth_link(
                &first_id,
                "linuxdo",
                "shared-subject",
                None,
                None,
                None,
                now,
            )
            .await
            .expect("first bind should resolve")
    });
    let second_repository = Arc::clone(&repository);
    let second_barrier = Arc::clone(&barrier);
    let second_id = second_user.id.clone();
    let second = tokio::spawn(async move {
        second_barrier.wait().await;
        second_repository
            .bind_user_oauth_link(
                &second_id,
                "linuxdo",
                "shared-subject",
                None,
                None,
                None,
                now,
            )
            .await
            .expect("second bind should resolve")
    });
    barrier.wait().await;
    let outcomes = [
        first.await.expect("first bind task should join"),
        second.await.expect("second bind task should join"),
    ];

    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| **outcome == BindUserOAuthLinkOutcome::Bound)
            .count(),
        1
    );
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| { **outcome == BindUserOAuthLinkOutcome::IdentityBoundToAnotherUser })
            .count(),
        1
    );
    assert!(repository
        .find_oauth_link_owner("linuxdo", "shared-subject")
        .await
        .expect("identity owner should load")
        .is_some());
}

#[tokio::test]
async fn concurrent_oauth_unbinds_preserve_one_login_method_in_memory() {
    let now = chrono::Utc::now();
    let repository = Arc::new(InMemoryUserReadRepository::default());
    let user = repository
        .create_oauth_auth_user(
            Some("concurrent-oauth@example.com".to_string()),
            true,
            "concurrent-oauth".to_string(),
            now,
        )
        .await
        .expect("oauth user should create")
        .expect("oauth user should exist");
    for (provider_type, subject) in [("linuxdo", "subject-1"), ("github", "subject-2")] {
        repository
            .bind_user_oauth_link(&user.id, provider_type, subject, None, None, None, now)
            .await
            .expect("oauth link should upsert");
    }

    let barrier = Arc::new(tokio::sync::Barrier::new(3));
    let first_repository = Arc::clone(&repository);
    let first_barrier = Arc::clone(&barrier);
    let first_user_id = user.id.clone();
    let first = tokio::spawn(async move {
        first_barrier.wait().await;
        first_repository
            .delete_user_oauth_link(
                &first_user_id,
                "linuxdo",
                false,
                &["linuxdo".to_string(), "github".to_string()],
            )
            .await
            .expect("first unlink should resolve")
    });
    let second_repository = Arc::clone(&repository);
    let second_barrier = Arc::clone(&barrier);
    let second_user_id = user.id.clone();
    let second = tokio::spawn(async move {
        second_barrier.wait().await;
        second_repository
            .delete_user_oauth_link(
                &second_user_id,
                "github",
                false,
                &["linuxdo".to_string(), "github".to_string()],
            )
            .await
            .expect("second unlink should resolve")
    });
    barrier.wait().await;
    let outcomes = [
        first.await.expect("first unlink task should join"),
        second.await.expect("second unlink task should join"),
    ];

    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| **outcome == DeleteUserOAuthLinkOutcome::Deleted)
            .count(),
        1
    );
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| **outcome == DeleteUserOAuthLinkOutcome::LastOAuthBinding)
            .count(),
        1
    );
    assert_eq!(
        repository
            .count_user_oauth_links(&user.id)
            .await
            .expect("remaining links should count"),
        1
    );
}

#[tokio::test]
async fn oauth_unbind_in_memory_only_counts_enabled_provider_links() {
    let repository = InMemoryUserReadRepository::default();
    let now = chrono::Utc::now();
    let user = repository
        .create_oauth_auth_user(
            Some("enabled-link@example.com".to_string()),
            true,
            "enabled-link".to_string(),
            now,
        )
        .await
        .expect("oauth user should create")
        .expect("oauth user should exist");
    for (provider_type, subject) in [("linuxdo", "subject-1"), ("github", "subject-2")] {
        repository
            .bind_user_oauth_link(&user.id, provider_type, subject, None, None, None, now)
            .await
            .expect("oauth link should upsert");
    }
    let enabled_provider_types_snapshot = ["linuxdo".to_string()];

    assert_eq!(
        repository
            .delete_user_oauth_link(&user.id, "linuxdo", false, &enabled_provider_types_snapshot,)
            .await
            .expect("enabled link deletion should resolve"),
        DeleteUserOAuthLinkOutcome::LastOAuthBinding
    );
    assert_eq!(
        repository
            .delete_user_oauth_link(&user.id, "github", false, &enabled_provider_types_snapshot,)
            .await
            .expect("disabled link deletion should resolve"),
        DeleteUserOAuthLinkOutcome::Deleted
    );
    assert!(repository
        .has_user_oauth_provider_link(&user.id, "linuxdo")
        .await
        .expect("enabled provider link lookup should work"));
}

#[tokio::test]
async fn oauth_unbind_in_memory_respects_ldap_exclusive_local_login_policy() {
    let valid_hash = "$2b$12$4qL4tdcsFwVaDTw5Ck3xzu8GpNdre56DiNR6Dnw7t6gCXaEnqAe7G".to_string();
    let user = StoredUserAuthRecord::new(
        "ldap-exclusive-local".to_string(),
        Some("ldap-exclusive-local@example.com".to_string()),
        true,
        "ldap-exclusive-local".to_string(),
        Some(valid_hash),
        "user".to_string(),
        "local".to_string(),
        None,
        None,
        None,
        true,
        false,
        None,
        None,
    )
    .expect("local user should build");
    let repository = InMemoryUserReadRepository::seed_auth_users([user.clone()]);
    repository
        .bind_user_oauth_link(
            &user.id,
            "linuxdo",
            "subject-1",
            None,
            None,
            None,
            chrono::Utc::now(),
        )
        .await
        .expect("oauth link should upsert");

    assert_eq!(
        repository
            .delete_user_oauth_link(&user.id, "linuxdo", false, &["linuxdo".to_string()],)
            .await
            .expect("unlink should resolve"),
        DeleteUserOAuthLinkOutcome::LastLoginMethod
    );
    assert!(repository
        .has_user_oauth_provider_link(&user.id, "linuxdo")
        .await
        .expect("oauth link lookup should work"));
}

#[tokio::test]
async fn counts_active_admin_auth_users() {
    let valid_hash = "$2b$12$4qL4tdcsFwVaDTw5Ck3xzu8GpNdre56DiNR6Dnw7t6gCXaEnqAe7G".to_string();
    let admin = StoredUserAuthRecord::new(
        "admin-1".to_string(),
        Some("admin@example.com".to_string()),
        true,
        "admin".to_string(),
        Some(valid_hash),
        "admin".to_string(),
        "local".to_string(),
        None,
        None,
        None,
        true,
        false,
        None,
        None,
    )
    .expect("admin should build");
    let invalid_admin = StoredUserAuthRecord::new(
        "admin-2".to_string(),
        Some("admin2@example.com".to_string()),
        true,
        "admin2".to_string(),
        Some("not-bcrypt".to_string()),
        "admin".to_string(),
        "local".to_string(),
        None,
        None,
        None,
        true,
        false,
        None,
        None,
    )
    .expect("admin should build");
    let repository = InMemoryUserReadRepository::seed_auth_users(vec![admin, invalid_admin]);

    assert_eq!(
        repository
            .count_active_admin_users()
            .await
            .expect("active admin count should succeed"),
        2
    );
    assert_eq!(
        repository
            .count_active_local_admin_users_with_valid_password()
            .await
            .expect("valid local admin count should succeed"),
        1
    );
}

#[tokio::test]
async fn reads_and_writes_user_preferences_in_memory() {
    let repository = InMemoryUserReadRepository::default();
    let preferences = StoredUserPreferenceRecord {
        user_id: "user-1".to_string(),
        avatar_url: Some("https://example.test/avatar.png".to_string()),
        bio: Some("hello".to_string()),
        default_provider_id: Some("provider-1".to_string()),
        default_provider_name: Some("Provider One".to_string()),
        theme: "dark".to_string(),
        language: "en-US".to_string(),
        timezone: "UTC".to_string(),
        email_notifications: false,
        usage_alerts: true,
        announcement_notifications: false,
        allow_wallet_overage: true,
    };

    assert!(repository
        .read_user_preferences("user-1")
        .await
        .expect("preferences read should succeed")
        .is_none());
    assert_eq!(
        repository
            .write_user_preferences(&preferences)
            .await
            .expect("preferences write should succeed"),
        Some(preferences.clone())
    );
    assert_eq!(
        repository
            .read_user_preferences("user-1")
            .await
            .expect("preferences read should succeed"),
        Some(preferences)
    );
}

#[tokio::test]
async fn manages_user_sessions_in_memory() {
    let now = chrono::Utc::now();
    let user = StoredUserAuthRecord::new(
        "user-1".to_string(),
        Some("session-user@example.com".to_string()),
        true,
        "session-user".to_string(),
        None,
        "user".to_string(),
        "oauth".to_string(),
        None,
        None,
        None,
        true,
        false,
        Some(now),
        None,
    )
    .expect("session user should build");
    let session = StoredUserSessionRecord::new(
        "session-1".to_string(),
        "user-1".to_string(),
        "device-1".to_string(),
        Some("Laptop".to_string()),
        StoredUserSessionRecord::hash_refresh_token("refresh-1"),
        None,
        None,
        Some(now),
        Some(now + chrono::Duration::hours(1)),
        None,
        None,
        Some("127.0.0.1".to_string()),
        Some("agent".to_string()),
        Some(now),
        Some(now),
    )
    .expect("session should build");
    let repository = InMemoryUserReadRepository::seed_auth_users([user]);

    assert_eq!(
        repository
            .create_user_session(&session)
            .await
            .expect("session should create"),
        Some(session.clone())
    );
    assert_eq!(
        repository
            .list_user_sessions("user-1")
            .await
            .expect("sessions should list")
            .len(),
        1
    );
    assert!(repository
        .touch_user_session(
            "user-1",
            "session-1",
            now + chrono::Duration::minutes(1),
            Some("127.0.0.2"),
            Some("updated-agent"),
        )
        .await
        .expect("session should touch"));
    assert!(repository
        .rotate_user_session_refresh_token(
            "user-1",
            "session-1",
            &StoredUserSessionRecord::hash_refresh_token("refresh-1"),
            &StoredUserSessionRecord::hash_refresh_token("refresh-2"),
            now + chrono::Duration::minutes(2),
            now + chrono::Duration::hours(2),
            None,
            None,
        )
        .await
        .expect("session should rotate"));
    assert!(!repository
        .rotate_user_session_refresh_token(
            "user-1",
            "session-1",
            &StoredUserSessionRecord::hash_refresh_token("refresh-1"),
            &StoredUserSessionRecord::hash_refresh_token("refresh-race-loser"),
            now + chrono::Duration::minutes(2),
            now + chrono::Duration::hours(2),
            None,
            None,
        )
        .await
        .expect("stale session rotation should be rejected"));
    let rotated = repository
        .find_user_session("user-1", "session-1")
        .await
        .expect("session lookup should succeed")
        .expect("session should exist");
    assert_eq!(
        rotated.refresh_token_hash,
        StoredUserSessionRecord::hash_refresh_token("refresh-2")
    );
    assert!(repository
        .revoke_user_session("user-1", "session-1", now, "logout")
        .await
        .expect("session should revoke"));
    assert!(repository
        .list_user_sessions("user-1")
        .await
        .expect("sessions should list")
        .is_empty());
}

#[tokio::test]
async fn security_state_change_revokes_sessions_without_reactivation() {
    let now = chrono::Utc::now();
    let user = StoredUserAuthRecord::new(
        "security-state-user".to_string(),
        Some("security-state@example.com".to_string()),
        true,
        "security-state-user".to_string(),
        None,
        "user".to_string(),
        "oauth".to_string(),
        None,
        None,
        None,
        true,
        false,
        Some(now),
        None,
    )
    .expect("security-state user should build");
    let session = StoredUserSessionRecord::new(
        "security-state-session".to_string(),
        user.id.clone(),
        "security-state-device".to_string(),
        None,
        StoredUserSessionRecord::hash_refresh_token("security-state-refresh"),
        None,
        None,
        Some(now),
        Some(now + chrono::Duration::hours(1)),
        None,
        None,
        None,
        None,
        Some(now),
        Some(now),
    )
    .expect("security-state session should build");
    let repository =
        InMemoryUserReadRepository::seed_auth_users([user]).with_user_sessions([session.clone()]);

    repository
        .update_local_auth_user_admin_fields(
            "security-state-user",
            None,
            false,
            None,
            false,
            None,
            false,
            None,
            false,
            None,
            Some(false),
        )
        .await
        .expect("disable should succeed")
        .expect("user should exist");
    let revoked = repository
        .find_user_session("security-state-user", "security-state-session")
        .await
        .expect("revoked session should load")
        .expect("revoked session should remain stored");
    assert!(revoked.is_revoked());
    assert_eq!(
        revoked.revoke_reason.as_deref(),
        Some("user_security_state_changed")
    );

    repository
        .update_local_auth_user_admin_fields(
            "security-state-user",
            None,
            false,
            None,
            false,
            None,
            false,
            None,
            false,
            None,
            Some(true),
        )
        .await
        .expect("reactivation should succeed")
        .expect("user should exist");
    assert!(repository
        .list_user_sessions("security-state-user")
        .await
        .expect("sessions should list")
        .is_empty());
    assert!(repository
        .create_user_session(&session)
        .await
        .expect("stale login should resolve")
        .is_none());
    let current_version = repository
        .find_user_auth_by_id("security-state-user")
        .await
        .expect("user lookup should resolve")
        .expect("user should exist")
        .security_version;
    let fresh_session = session
        .with_security_version(current_version)
        .expect("security version should be valid");
    assert!(repository
        .create_user_session(&fresh_session)
        .await
        .expect("fresh login should resolve")
        .is_some());

    repository
        .update_local_auth_user_admin_fields(
            "security-state-user",
            Some("audit_admin".to_string()),
            false,
            None,
            false,
            None,
            false,
            None,
            false,
            None,
            None,
        )
        .await
        .expect("role update should succeed")
        .expect("user should exist");
    assert!(repository
        .list_user_sessions("security-state-user")
        .await
        .expect("sessions should list")
        .is_empty());
}

#[tokio::test]
async fn unchanged_security_state_preserves_sessions() {
    let now = chrono::Utc::now();
    let user = StoredUserAuthRecord::new(
        "unchanged-security-user".to_string(),
        None,
        false,
        "unchanged-security-user".to_string(),
        None,
        "user".to_string(),
        "oauth".to_string(),
        None,
        None,
        None,
        true,
        false,
        Some(now),
        None,
    )
    .expect("unchanged-security user should build");
    let session = StoredUserSessionRecord::new(
        "unchanged-security-session".to_string(),
        user.id.clone(),
        "unchanged-security-device".to_string(),
        None,
        StoredUserSessionRecord::hash_refresh_token("unchanged-security-refresh"),
        None,
        None,
        Some(now),
        Some(now + chrono::Duration::hours(1)),
        None,
        None,
        None,
        None,
        Some(now),
        Some(now),
    )
    .expect("unchanged-security session should build");
    let repository =
        InMemoryUserReadRepository::seed_auth_users([user]).with_user_sessions([session]);

    repository
        .update_local_auth_user_admin_fields(
            "unchanged-security-user",
            Some("USER".to_string()),
            false,
            None,
            false,
            None,
            false,
            None,
            false,
            None,
            Some(true),
        )
        .await
        .expect("idempotent security update should succeed")
        .expect("user should exist");
    assert_eq!(
        repository
            .list_user_sessions("unchanged-security-user")
            .await
            .expect("sessions should list")
            .len(),
        1
    );
}

#[tokio::test]
async fn password_change_revokes_sessions_and_fences_stale_login() {
    let now = chrono::Utc::now();
    let user = StoredUserAuthRecord::new(
        "user-password-fence".to_string(),
        Some("fence@example.com".to_string()),
        true,
        "fence-user".to_string(),
        Some("old-password-hash".to_string()),
        "user".to_string(),
        "local".to_string(),
        None,
        None,
        None,
        true,
        false,
        Some(now),
        None,
    )
    .expect("user should build");
    let current = StoredUserSessionRecord::new(
        "session-current".to_string(),
        user.id.clone(),
        "device-current".to_string(),
        None,
        StoredUserSessionRecord::hash_refresh_token("refresh-current"),
        None,
        None,
        Some(now),
        Some(now + chrono::Duration::hours(1)),
        None,
        None,
        None,
        None,
        Some(now),
        Some(now),
    )
    .expect("current session should build");
    let stale_login = StoredUserSessionRecord::new(
        "session-stale-login".to_string(),
        user.id.clone(),
        "device-stale-login".to_string(),
        None,
        StoredUserSessionRecord::hash_refresh_token("refresh-stale"),
        None,
        None,
        Some(now),
        Some(now + chrono::Duration::hours(1)),
        None,
        None,
        None,
        None,
        Some(now),
        Some(now),
    )
    .expect("stale login session should build");
    let repository =
        InMemoryUserReadRepository::seed_auth_users([user]).with_user_sessions([current]);

    assert!(repository
        .change_local_auth_password_and_revoke_sessions(
            "user-password-fence",
            "session-current",
            Some("old-password-hash"),
            "new-password-hash".to_string(),
            now,
        )
        .await
        .expect("password change should succeed"));
    assert!(repository
        .list_user_sessions("user-password-fence")
        .await
        .expect("sessions should list")
        .is_empty());
    assert!(repository
        .create_user_session_if_password_matches(&stale_login, "old-password-hash")
        .await
        .expect("stale login should resolve")
        .is_none());
}

#[tokio::test]
async fn admin_password_reset_revokes_sessions_and_fences_stale_login() {
    let now = chrono::Utc::now();
    let user = StoredUserAuthRecord::new(
        "user-admin-reset-fence".to_string(),
        Some("admin-reset@example.com".to_string()),
        true,
        "admin-reset-user".to_string(),
        Some("old-password-hash".to_string()),
        "user".to_string(),
        "local".to_string(),
        None,
        None,
        None,
        true,
        false,
        Some(now),
        None,
    )
    .expect("user should build");
    let active = StoredUserSessionRecord::new(
        "session-before-admin-reset".to_string(),
        user.id.clone(),
        "device-before-admin-reset".to_string(),
        None,
        StoredUserSessionRecord::hash_refresh_token("refresh-before-admin-reset"),
        None,
        None,
        Some(now),
        Some(now + chrono::Duration::hours(1)),
        None,
        None,
        None,
        None,
        Some(now),
        Some(now),
    )
    .expect("active session should build");
    let stale_login = StoredUserSessionRecord::new(
        "session-stale-admin-reset-login".to_string(),
        user.id.clone(),
        "device-stale-admin-reset-login".to_string(),
        None,
        StoredUserSessionRecord::hash_refresh_token("refresh-stale-admin-reset"),
        None,
        None,
        Some(now),
        Some(now + chrono::Duration::hours(1)),
        None,
        None,
        None,
        None,
        Some(now),
        Some(now),
    )
    .expect("stale login session should build");
    let repository =
        InMemoryUserReadRepository::seed_auth_users([user]).with_user_sessions([active]);

    assert!(repository
        .reset_local_auth_user_password_and_revoke_sessions(
            "user-admin-reset-fence",
            "new-password-hash".to_string(),
            now,
        )
        .await
        .expect("admin password reset should succeed"));
    assert!(repository
        .list_user_sessions("user-admin-reset-fence")
        .await
        .expect("sessions should list")
        .is_empty());
    assert!(repository
        .create_user_session_if_password_matches(&stale_login, "old-password-hash")
        .await
        .expect("stale login should resolve")
        .is_none());
    assert_eq!(
        repository
            .find_user_auth_by_id("user-admin-reset-fence")
            .await
            .expect("user lookup should succeed")
            .expect("user should exist")
            .password_hash
            .as_deref(),
        Some("new-password-hash")
    );
}

#[tokio::test]
async fn paginates_export_users_in_memory() {
    let repository = InMemoryUserReadRepository::seed_export_users(vec![
        StoredUserExportRow::new(
            "user-1".to_string(),
            Some("alice@example.com".to_string()),
            true,
            "alice".to_string(),
            Some("hash".to_string()),
            "user".to_string(),
            "local".to_string(),
            None,
            None,
            None,
            Some(60),
            None,
            true,
        )
        .expect("user export row should build"),
        StoredUserExportRow::new(
            "user-2".to_string(),
            Some("bob@example.com".to_string()),
            true,
            "bob".to_string(),
            Some("hash".to_string()),
            "admin".to_string(),
            "local".to_string(),
            None,
            None,
            None,
            Some(30),
            None,
            true,
        )
        .expect("user export row should build"),
        StoredUserExportRow::new(
            "user-3".to_string(),
            Some("carol@example.com".to_string()),
            true,
            "carol".to_string(),
            Some("hash".to_string()),
            "user".to_string(),
            "local".to_string(),
            None,
            None,
            None,
            Some(10),
            None,
            false,
        )
        .expect("user export row should build"),
    ]);

    let rows = repository
        .list_export_users_page(&UserExportListQuery {
            skip: 0,
            limit: 10,
            role: Some("user".to_string()),
            is_active: Some(true),
            search: None,
            group_id: None,
            ..Default::default()
        })
        .await
        .expect("paged export should succeed");

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].id, "user-1");
}
