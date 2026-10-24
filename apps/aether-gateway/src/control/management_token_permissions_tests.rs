use super::*;

#[test]
fn catalog_covers_known_admin_auth_scopes() {
    let scopes = [
        "adaptive",
        "announcements",
        "api_keys",
        "billing",
        "endpoints_health",
        "endpoints_manage",
        "endpoints_rpm",
        "gemini_files",
        "ldap",
        "management_tokens",
        "models",
        "modules",
        "monitoring",
        "oauth",
        "payments",
        "pool",
        "provider_oauth",
        "provider_ops",
        "provider_query",
        "provider_strategy",
        "providers",
        "proxy_nodes",
        "security",
        "stats",
        "system",
        "tasks",
        "usage",
        "users",
        "video_tasks",
        "wallets",
    ];

    for scope in scopes {
        assert!(
            is_known_management_token_permission_scope(scope),
            "missing scope {scope}"
        );
        if scope == "management_tokens" {
            continue;
        }
        for access in ["read", "write", "admin"] {
            let key = format!("admin:{scope}:{access}");
            assert!(
                is_assignable_management_token_permission(&key),
                "missing permission key {key}"
            );
        }
    }
}

#[test]
fn catalog_covers_admin_route_signatures_from_route_sources() {
    let route_sources = [
        ("route/admin.rs", include_str!("route/admin.rs")),
        ("route/oauth.rs", include_str!("route/oauth.rs")),
        (
            "route/public_support.rs",
            include_str!("route/public_support.rs"),
        ),
        (
            "route/admin/basic_families.rs",
            include_str!("route/admin/basic_families.rs"),
        ),
        (
            "route/admin/endpoints_families.rs",
            include_str!("route/admin/endpoints_families.rs"),
        ),
        (
            "route/admin/model_provider_families.rs",
            include_str!("route/admin/model_provider_families.rs"),
        ),
        (
            "route/admin/observability_families.rs",
            include_str!("route/admin/observability_families.rs"),
        ),
        (
            "route/admin/operations_families.rs",
            include_str!("route/admin/operations_families.rs"),
        ),
        (
            "route/admin/provider_ops_routes.rs",
            include_str!("route/admin/provider_ops_routes.rs"),
        ),
        (
            "route/admin/system_families.rs",
            include_str!("route/admin/system_families.rs"),
        ),
    ];
    let mut route_scopes = BTreeSet::new();

    for (file, source) in route_sources {
        for scope in extract_admin_route_scopes(source) {
            assert!(
                is_known_management_token_permission_scope(scope),
                "missing management token permission scope {scope} referenced by {file}"
            );
            route_scopes.insert(scope);
        }
    }

    assert!(
        !route_scopes.is_empty(),
        "admin route scope scanner did not find any route signatures"
    );
}

#[test]
fn full_assignable_token_permissions_cannot_cover_management_tokens_scope() {
    let decision = GatewayControlDecision::synthetic(
        "/api/admin/management-tokens".to_string(),
        Some("admin_proxy".to_string()),
        Some("management_tokens_manage".to_string()),
        Some("list_tokens".to_string()),
        Some("admin:management_tokens".to_string()),
    );
    let permissions = all_assignable_management_token_permissions();

    assert_eq!(
        validate_management_token_admin_route_permission(
            &http::Method::GET,
            &decision,
            Some(&permissions),
        )
        .expect_err("management-token administration is not assignable")
        .required_permission,
        "admin:management_tokens:read"
    );
}

#[test]
fn legacy_tokens_cannot_administer_management_tokens() {
    let decision = GatewayControlDecision::synthetic(
        "/api/admin/management-tokens".to_string(),
        Some("admin_proxy".to_string()),
        Some("management_tokens_manage".to_string()),
        Some("create_token".to_string()),
        Some("admin:management_tokens".to_string()),
    );
    let permissions = legacy_full_management_token_permissions();

    assert_eq!(
        validate_management_token_admin_route_permission(
            &http::Method::POST,
            &decision,
            Some(&permissions),
        )
        .expect_err("legacy tokens must not mint replacement credentials")
        .required_permission,
        "admin:management_tokens:admin"
    );
    assert!(!permissions
        .iter()
        .any(|permission| permission.starts_with("admin:management_tokens:")));
}

#[test]
fn read_only_permissions_allow_reads_and_reject_writes() {
    let decision = GatewayControlDecision::synthetic(
        "/api/admin/providers".to_string(),
        Some("admin_proxy".to_string()),
        Some("providers_manage".to_string()),
        Some("create_provider".to_string()),
        Some("admin:providers".to_string()),
    );
    let permissions = read_only_management_token_permissions();

    assert!(validate_management_token_admin_route_permission(
        &http::Method::GET,
        &decision,
        Some(&permissions),
    )
    .is_ok());
    assert_eq!(
        validate_management_token_admin_route_permission(
            &http::Method::POST,
            &decision,
            Some(&permissions),
        )
        .expect_err("read-only permissions should reject writes")
        .required_permission,
        "admin:providers:write"
    );
}

#[test]
fn scoped_tokens_fail_closed_for_unclassified_admin_permissions() {
    let decision = GatewayControlDecision::synthetic(
        "/api/admin/unclassified".to_string(),
        Some("admin_proxy".to_string()),
        Some("unclassified_manage".to_string()),
        Some("unclassified".to_string()),
        None,
    );
    let permissions = vec!["admin:system:admin".to_string()];

    assert_eq!(
        validate_management_token_admin_route_permission(
            &http::Method::GET,
            &decision,
            Some(&permissions),
        )
        .expect_err("scoped tokens must not bypass an unclassified admin route")
        .required_permission,
        "admin:unknown:admin"
    );
    assert!(
        validate_management_token_admin_route_permission(&http::Method::GET, &decision, None,)
            .is_ok()
    );
}

#[test]
fn legacy_full_permissions_are_frozen_to_the_original_scope_catalog() {
    let permissions = legacy_full_management_token_permissions();

    for permission in [
        "admin:system:admin",
        "admin:oauth:admin",
        "admin:proxy_nodes:admin",
    ] {
        assert!(permissions.iter().any(|item| item == permission));
    }
    for permission in [
        "admin:routing_profiles:read",
        "admin:routing_profiles:admin",
        "admin:tasks:read",
        "admin:tasks:admin",
    ] {
        assert!(
            !permissions.iter().any(|item| item == permission),
            "legacy token unexpectedly gained {permission}"
        );
    }
}

#[test]
fn json_null_permissions_never_inherit_legacy_full_access() {
    assert!(normalize_assignable_management_token_permissions(Some(&Value::Null)).is_err());
    assert!(management_token_permission_keys_from_value(Some(&Value::Null)).is_err());

    assert_eq!(
        management_token_permission_keys_from_value(None)
            .expect("SQL NULL remains the explicit legacy representation"),
        None
    );
}

#[test]
fn sensitive_action_permission_distinguishes_sessions_and_scoped_tokens() {
    let mut decision = GatewayControlDecision::synthetic(
        "/api/admin/users/user-1".to_string(),
        Some("admin_proxy".to_string()),
        Some("users_manage".to_string()),
        Some("update_user".to_string()),
        Some("admin:users".to_string()),
    );

    assert!(!management_token_principal_has_permission(
        &decision,
        "admin:users:admin"
    ));

    decision.admin_principal = Some(crate::control::GatewayAdminPrincipalContext {
        user_id: "admin-1".to_string(),
        user_role: "admin".to_string(),
        session_id: Some("session-1".to_string()),
        management_token_id: None,
        management_token_permissions: None,
    });
    assert!(management_token_principal_has_permission(
        &decision,
        "admin:users:admin"
    ));

    let principal = decision
        .admin_principal
        .as_mut()
        .expect("principal should exist");
    principal.session_id = None;
    principal.management_token_id = Some("management-token-1".to_string());
    principal.management_token_permissions = Some(vec!["admin:users:write".to_string()]);
    assert!(!management_token_principal_has_permission(
        &decision,
        "admin:users:admin"
    ));

    decision
        .admin_principal
        .as_mut()
        .expect("principal should exist")
        .management_token_permissions = Some(vec!["admin:users:admin".to_string()]);
    assert!(management_token_principal_has_permission(
        &decision,
        "admin:users:admin"
    ));
}

#[test]
fn sensitive_system_data_transfers_require_admin_permission() {
    let delegated_permissions = vec![
        "admin:system:read".to_string(),
        "admin:system:write".to_string(),
    ];
    let admin_permissions = vec!["admin:system:admin".to_string()];

    for (method, route_kind) in [
        (http::Method::GET, "config_export"),
        (http::Method::GET, "users_export"),
        (http::Method::GET, "data_export"),
        (http::Method::POST, "s3_backup_run"),
        (http::Method::POST, "config_import"),
        (http::Method::POST, "users_import"),
        (http::Method::POST, "data_import"),
    ] {
        let decision = GatewayControlDecision::synthetic(
            format!("/api/admin/system/{route_kind}"),
            Some("admin_proxy".to_string()),
            Some("system_manage".to_string()),
            Some(route_kind.to_string()),
            Some("admin:system".to_string()),
        );

        assert_eq!(
            management_token_required_permission(&method, &decision).as_deref(),
            Some("admin:system:admin")
        );
        assert_eq!(
            validate_management_token_admin_route_permission(
                &method,
                &decision,
                Some(&delegated_permissions),
            )
            .expect_err("delegated system access must not transfer sensitive data")
            .required_permission,
            "admin:system:admin"
        );
        assert!(validate_management_token_admin_route_permission(
            &method,
            &decision,
            Some(&admin_permissions),
        )
        .is_ok());
    }
}

#[test]
fn oauth_provider_upsert_requires_oauth_admin_permission() {
    let decision = GatewayControlDecision::synthetic(
        "/api/admin/oauth/providers/custom".to_string(),
        Some("admin_proxy".to_string()),
        Some("oauth_manage".to_string()),
        Some("upsert_provider".to_string()),
        Some("admin:oauth".to_string()),
    );
    let write_permissions = vec!["admin:oauth:write".to_string()];
    let admin_permissions = vec!["admin:oauth:admin".to_string()];

    assert_eq!(
        management_token_required_permission(&http::Method::PUT, &decision).as_deref(),
        Some("admin:oauth:admin")
    );
    assert_eq!(
        validate_management_token_admin_route_permission(
            &http::Method::PUT,
            &decision,
            Some(&write_permissions),
        )
        .expect_err("oauth write must not replace security-sensitive provider configuration")
        .required_permission,
        "admin:oauth:admin"
    );
    assert!(validate_management_token_admin_route_permission(
        &http::Method::PUT,
        &decision,
        Some(&admin_permissions),
    )
    .is_ok());
}

#[test]
fn oauth_provider_delete_requires_oauth_admin_permission() {
    let decision = GatewayControlDecision::synthetic(
        "/api/admin/oauth/providers/custom".to_string(),
        Some("admin_proxy".to_string()),
        Some("oauth_manage".to_string()),
        Some("delete_provider".to_string()),
        Some("admin:oauth".to_string()),
    );
    let write_permissions = vec!["admin:oauth:write".to_string()];
    let admin_permissions = vec!["admin:oauth:admin".to_string()];

    assert_eq!(
        management_token_required_permission(&http::Method::DELETE, &decision).as_deref(),
        Some("admin:oauth:admin")
    );
    assert_eq!(
        validate_management_token_admin_route_permission(
            &http::Method::DELETE,
            &decision,
            Some(&write_permissions),
        )
        .expect_err("oauth write must not delete an authentication provider")
        .required_permission,
        "admin:oauth:admin"
    );
    assert!(validate_management_token_admin_route_permission(
        &http::Method::DELETE,
        &decision,
        Some(&admin_permissions),
    )
    .is_ok());
}

#[test]
fn destructive_system_actions_require_system_admin_permission() {
    let write_permissions = vec!["admin:system:write".to_string()];
    let admin_permissions = vec!["admin:system:admin".to_string()];

    for (method, route_kind) in [
        (http::Method::POST, "prepare_update"),
        (http::Method::POST, "apply_update"),
        (http::Method::POST, "rollback"),
        (http::Method::POST, "cleanup"),
        (http::Method::POST, "cleanup_usage_manual"),
        (http::Method::POST, "purge_config"),
        (http::Method::POST, "purge_users"),
        (http::Method::POST, "purge_usage"),
        (http::Method::POST, "purge_audit_logs"),
        (http::Method::POST, "purge_request_bodies"),
        (http::Method::POST, "purge_request_bodies_task"),
        (http::Method::POST, "purge_stats"),
        (http::Method::PUT, "settings_set"),
        (http::Method::PUT, "config_set"),
        (http::Method::DELETE, "config_delete"),
    ] {
        let decision = GatewayControlDecision::synthetic(
            format!("/api/admin/system/{route_kind}"),
            Some("admin_proxy".to_string()),
            Some("system_manage".to_string()),
            Some(route_kind.to_string()),
            Some("admin:system".to_string()),
        );

        assert_eq!(
            management_token_required_permission(&method, &decision).as_deref(),
            Some("admin:system:admin"),
            "unexpected permission for {method} {route_kind}"
        );
        assert_eq!(
            validate_management_token_admin_route_permission(
                &method,
                &decision,
                Some(&write_permissions),
            )
            .expect_err("system write must not perform destructive or security-critical action")
            .required_permission,
            "admin:system:admin"
        );
        assert!(validate_management_token_admin_route_permission(
            &method,
            &decision,
            Some(&admin_permissions),
        )
        .is_ok());
    }
}

#[test]
fn provider_operations_that_use_stored_credentials_require_provider_ops_admin_permission() {
    let read_permissions = vec!["admin:provider_ops:read".to_string()];
    let write_permissions = vec!["admin:provider_ops:write".to_string()];
    let admin_permissions = vec!["admin:provider_ops:admin".to_string()];

    for (method, route_kind) in [
        (http::Method::POST, "connect_provider"),
        (http::Method::POST, "verify_provider"),
        (http::Method::GET, "get_provider_balance"),
        (http::Method::POST, "refresh_provider_balance"),
        (http::Method::POST, "provider_checkin"),
        (http::Method::POST, "execute_provider_action"),
        (http::Method::POST, "batch_balance"),
    ] {
        let decision = GatewayControlDecision::synthetic(
            format!("/api/admin/provider-ops/{route_kind}"),
            Some("admin_proxy".to_string()),
            Some("provider_ops_manage".to_string()),
            Some(route_kind.to_string()),
            Some("admin:provider_ops".to_string()),
        );

        assert_eq!(
            management_token_required_permission(&method, &decision).as_deref(),
            Some("admin:provider_ops:admin")
        );
        for delegated_permissions in [&read_permissions, &write_permissions] {
            assert_eq!(
                validate_management_token_admin_route_permission(
                    &method,
                    &decision,
                    Some(delegated_permissions),
                )
                .expect_err("delegated access must not execute stored provider credentials")
                .required_permission,
                "admin:provider_ops:admin"
            );
        }
        assert!(validate_management_token_admin_route_permission(
            &method,
            &decision,
            Some(&admin_permissions),
        )
        .is_ok());
    }
}

#[test]
fn provider_queries_that_execute_stored_credentials_require_admin_permission() {
    let write_permissions = vec!["admin:provider_query:write".to_string()];
    let admin_permissions = vec!["admin:provider_query:admin".to_string()];

    for route_kind in ["query_models", "test_model", "test_model_failover"] {
        let decision = GatewayControlDecision::synthetic(
            format!("/api/admin/provider-query/{route_kind}"),
            Some("admin_proxy".to_string()),
            Some("provider_query_manage".to_string()),
            Some(route_kind.to_string()),
            Some("admin:provider_query".to_string()),
        );

        assert_eq!(
            management_token_required_permission(&http::Method::POST, &decision).as_deref(),
            Some("admin:provider_query:admin")
        );
        assert!(validate_management_token_admin_route_permission(
            &http::Method::POST,
            &decision,
            Some(&write_permissions),
        )
        .is_err());
        assert!(validate_management_token_admin_route_permission(
            &http::Method::POST,
            &decision,
            Some(&admin_permissions),
        )
        .is_ok());
    }
}

#[test]
fn endpoint_actions_that_execute_stored_credentials_require_admin_permission() {
    let write_permissions = vec!["admin:endpoints_manage:write".to_string()];
    let admin_permissions = vec!["admin:endpoints_manage:admin".to_string()];

    for (method, route_kind) in [
        (http::Method::PUT, "update_key"),
        (http::Method::POST, "create_endpoint"),
        (http::Method::PUT, "update_endpoint"),
        (http::Method::POST, "refresh_quota"),
        (http::Method::POST, "codex_reset_credit_consume"),
    ] {
        let decision = GatewayControlDecision::synthetic(
            format!("/api/admin/endpoints/{route_kind}"),
            Some("admin_proxy".to_string()),
            Some("endpoints_manage".to_string()),
            Some(route_kind.to_string()),
            Some("admin:endpoints_manage".to_string()),
        );

        assert_eq!(
            management_token_required_permission(&method, &decision).as_deref(),
            Some("admin:endpoints_manage:admin")
        );
        assert!(validate_management_token_admin_route_permission(
            &method,
            &decision,
            Some(&write_permissions),
        )
        .is_err());
        assert!(validate_management_token_admin_route_permission(
            &method,
            &decision,
            Some(&admin_permissions),
        )
        .is_ok());
    }
}

#[test]
fn stored_credential_egress_configuration_requires_admin_permission() {
    for (method, signature, route_kind) in [
        (http::Method::PATCH, "admin:providers", "update_provider"),
        (http::Method::POST, "admin:pool", "batch_action_keys"),
        (http::Method::PATCH, "admin:pool", "batch_update_keys"),
    ] {
        let scope = signature.trim_start_matches("admin:");
        let decision = GatewayControlDecision::synthetic(
            format!("/api/admin/{scope}/{route_kind}"),
            Some("admin_proxy".to_string()),
            Some(format!("{scope}_manage")),
            Some(route_kind.to_string()),
            Some(signature.to_string()),
        );
        let write_permissions = vec![format!("{signature}:write")];
        let admin_permissions = vec![format!("{signature}:admin")];

        assert_eq!(
            management_token_required_permission(&method, &decision).as_deref(),
            Some(format!("{signature}:admin").as_str())
        );
        assert_eq!(
            validate_management_token_admin_route_permission(
                &method,
                &decision,
                Some(&write_permissions),
            )
            .expect_err("delegated writes must not redirect stored provider credentials")
            .required_permission,
            format!("{signature}:admin")
        );
        assert!(validate_management_token_admin_route_permission(
            &method,
            &decision,
            Some(&admin_permissions),
        )
        .is_ok());
    }
}

#[test]
fn connection_tests_that_transmit_stored_secrets_require_admin_permission() {
    for (signature, route_kind, read_permission, write_permission, admin_permission) in [
        (
            "admin:system",
            "smtp_test",
            "admin:system:read",
            "admin:system:write",
            "admin:system:admin",
        ),
        (
            "admin:system",
            "important_notification_test",
            "admin:system:read",
            "admin:system:write",
            "admin:system:admin",
        ),
        (
            "admin:ldap",
            "test_connection",
            "admin:ldap:read",
            "admin:ldap:write",
            "admin:ldap:admin",
        ),
    ] {
        let decision = GatewayControlDecision::synthetic(
            "/api/admin/security-sensitive-test".to_string(),
            Some("admin_proxy".to_string()),
            Some("security_test".to_string()),
            Some(route_kind.to_string()),
            Some(signature.to_string()),
        );
        let read_permissions = vec![read_permission.to_string()];
        let write_permissions = vec![write_permission.to_string()];
        let admin_permissions = vec![admin_permission.to_string()];

        assert_eq!(
            management_token_required_permission(&http::Method::POST, &decision).as_deref(),
            Some(admin_permission)
        );
        for delegated_permissions in [&read_permissions, &write_permissions] {
            assert_eq!(
                validate_management_token_admin_route_permission(
                    &http::Method::POST,
                    &decision,
                    Some(delegated_permissions),
                )
                .expect_err("delegated access must not transmit stored service secrets")
                .required_permission,
                admin_permission
            );
        }
        assert!(validate_management_token_admin_route_permission(
            &http::Method::POST,
            &decision,
            Some(&admin_permissions),
        )
        .is_ok());
    }
}

#[test]
fn security_control_plane_mutations_require_admin_permission() {
    for (method, signature, route_kind) in [
        (http::Method::POST, "admin:security", "blacklist_add"),
        (http::Method::DELETE, "admin:security", "blacklist_remove"),
        (http::Method::POST, "admin:security", "whitelist_add"),
        (http::Method::DELETE, "admin:security", "whitelist_remove"),
        (http::Method::PUT, "admin:ldap", "set_config"),
        (http::Method::PUT, "admin:modules", "set_enabled"),
    ] {
        let scope = signature.trim_start_matches("admin:");
        let decision = GatewayControlDecision::synthetic(
            format!("/api/admin/{scope}/{route_kind}"),
            Some("admin_proxy".to_string()),
            Some(format!("{scope}_manage")),
            Some(route_kind.to_string()),
            Some(signature.to_string()),
        );
        let read_permissions = vec![format!("{signature}:read")];
        let write_permissions = vec![format!("{signature}:write")];
        let admin_permissions = vec![format!("{signature}:admin")];

        assert_eq!(
            management_token_required_permission(&method, &decision).as_deref(),
            Some(format!("{signature}:admin").as_str())
        );
        for delegated_permissions in [&read_permissions, &write_permissions] {
            assert_eq!(
                validate_management_token_admin_route_permission(
                    &method,
                    &decision,
                    Some(delegated_permissions),
                )
                .expect_err("delegated tokens must not change security controls")
                .required_permission,
                format!("{signature}:admin")
            );
        }
        assert!(validate_management_token_admin_route_permission(
            &method,
            &decision,
            Some(&admin_permissions),
        )
        .is_ok());
    }
}

#[test]
fn payment_gateway_updates_require_payments_admin_permission() {
    for route_kind in ["update_epay_gateway", "update_payment_gateway"] {
        let decision = GatewayControlDecision::synthetic(
            "/api/admin/payments/gateways/epay".to_string(),
            Some("admin_proxy".to_string()),
            Some("payments_manage".to_string()),
            Some(route_kind.to_string()),
            Some("admin:payments".to_string()),
        );
        let write_permissions = vec!["admin:payments:write".to_string()];
        let admin_permissions = vec!["admin:payments:admin".to_string()];

        assert_eq!(
            management_token_required_permission(&http::Method::PUT, &decision).as_deref(),
            Some("admin:payments:admin")
        );
        assert!(validate_management_token_admin_route_permission(
            &http::Method::PUT,
            &decision,
            Some(&write_permissions),
        )
        .is_err());
        assert!(validate_management_token_admin_route_permission(
            &http::Method::PUT,
            &decision,
            Some(&admin_permissions),
        )
        .is_ok());
    }
}

#[test]
fn plaintext_credential_reads_require_admin_permission() {
    let read_only_permissions = read_only_management_token_permissions();
    let cases = [
        (
            "admin:endpoints_manage",
            "reveal_endpoint_rules",
            None,
            "admin:endpoints_manage:admin",
        ),
        (
            "admin:endpoints_manage",
            "reveal_key",
            None,
            "admin:endpoints_manage:admin",
        ),
        (
            "admin:endpoints_manage",
            "export_key",
            None,
            "admin:endpoints_manage:admin",
        ),
        (
            "admin:users",
            "reveal_user_api_key",
            None,
            "admin:users:admin",
        ),
        (
            "admin:api_keys",
            "api_key_detail",
            Some("include_key=true"),
            "admin:api_keys:admin",
        ),
        (
            "admin:api_keys",
            "create_api_key_install_session",
            None,
            "admin:api_keys:admin",
        ),
        (
            "admin:proxy_nodes",
            "create_proxy_node_install_session",
            None,
            "admin:proxy_nodes:admin",
        ),
    ];

    for (signature, route_kind, query, expected) in cases {
        let mut decision = GatewayControlDecision::synthetic(
            "/api/admin/sensitive-read".to_string(),
            Some("admin_proxy".to_string()),
            Some("security_test".to_string()),
            Some(route_kind.to_string()),
            Some(signature.to_string()),
        );
        decision.public_query_string = query.map(str::to_string);

        let method = if route_kind.ends_with("install_session") {
            http::Method::POST
        } else {
            http::Method::GET
        };
        assert_eq!(
            management_token_required_permission(&method, &decision).as_deref(),
            Some(expected)
        );
        assert_eq!(
            validate_management_token_admin_route_permission(
                &method,
                &decision,
                Some(&read_only_permissions),
            )
            .expect_err("read-only access must not reveal plaintext credentials")
            .required_permission,
            expected
        );
    }
}

#[test]
fn raw_usage_and_trace_reads_require_admin_permission() {
    let cases = [
        ("admin:usage", "detail", http::Method::GET),
        ("admin:usage", "curl", http::Method::GET),
        ("admin:usage", "replay", http::Method::POST),
        ("admin:monitoring", "trace_request", http::Method::GET),
    ];

    for (signature, route_kind, method) in cases {
        let scope = signature.trim_start_matches("admin:");
        let read_permissions = vec![format!("{signature}:read")];
        let admin_permissions = vec![format!("{signature}:admin")];
        let decision = GatewayControlDecision::synthetic(
            format!("/api/admin/{scope}/sensitive-record"),
            Some("admin_proxy".to_string()),
            Some(format!("{scope}_manage")),
            Some(route_kind.to_string()),
            Some(signature.to_string()),
        );

        assert_eq!(
            management_token_required_permission(&method, &decision).as_deref(),
            Some(format!("{signature}:admin").as_str())
        );
        assert_eq!(
            validate_management_token_admin_route_permission(
                &method,
                &decision,
                Some(&read_permissions),
            )
            .expect_err("delegated read access must not expose raw request diagnostics")
            .required_permission,
            format!("{signature}:admin")
        );
        assert!(validate_management_token_admin_route_permission(
            &method,
            &decision,
            Some(&admin_permissions),
        )
        .is_ok());
    }

    for (signature, route_kind) in [
        ("admin:usage", "records"),
        ("admin:monitoring", "trace_provider_stats"),
    ] {
        let decision = GatewayControlDecision::synthetic(
            "/api/admin/summary".to_string(),
            Some("admin_proxy".to_string()),
            Some("summary".to_string()),
            Some(route_kind.to_string()),
            Some(signature.to_string()),
        );
        assert_eq!(
            management_token_required_permission(&http::Method::GET, &decision).as_deref(),
            Some(format!("{signature}:read").as_str())
        );
    }
}

#[test]
fn task_read_permission_excludes_raw_detail_and_events() {
    let read_permissions = vec!["admin:tasks:read".to_string()];
    let admin_permissions = vec!["admin:tasks:admin".to_string()];

    for route_kind in ["list_tasks", "stats"] {
        let decision = GatewayControlDecision::synthetic(
            "/api/admin/tasks".to_string(),
            Some("admin_proxy".to_string()),
            Some("tasks_manage".to_string()),
            Some(route_kind.to_string()),
            Some("admin:tasks".to_string()),
        );
        assert_eq!(
            management_token_required_permission(&http::Method::GET, &decision).as_deref(),
            Some("admin:tasks:read")
        );
        assert!(validate_management_token_admin_route_permission(
            &http::Method::GET,
            &decision,
            Some(&read_permissions),
        )
        .is_ok());
    }

    for route_kind in ["detail", "events"] {
        let decision = GatewayControlDecision::synthetic(
            "/api/admin/tasks/run-1".to_string(),
            Some("admin_proxy".to_string()),
            Some("tasks_manage".to_string()),
            Some(route_kind.to_string()),
            Some("admin:tasks".to_string()),
        );
        assert_eq!(
            management_token_required_permission(&http::Method::GET, &decision).as_deref(),
            Some("admin:tasks:admin")
        );
        assert_eq!(
            validate_management_token_admin_route_permission(
                &http::Method::GET,
                &decision,
                Some(&read_permissions),
            )
            .expect_err("task read access must not expose raw task diagnostics")
            .required_permission,
            "admin:tasks:admin"
        );
        assert!(validate_management_token_admin_route_permission(
            &http::Method::GET,
            &decision,
            Some(&admin_permissions),
        )
        .is_ok());
        assert!(validate_management_token_admin_route_permission(
            &http::Method::GET,
            &decision,
            None,
        )
        .is_ok());
    }
}

#[test]
fn user_lifecycle_routes_preserve_write_permission_for_field_level_checks() {
    let write_permissions = vec!["admin:users:write".to_string()];
    let cases = [
        (http::Method::POST, "/api/admin/users", "create_user"),
        (http::Method::PUT, "/api/admin/users/user-1", "update_user"),
        (
            http::Method::POST,
            "/api/admin/users/batch-action",
            "batch_action_users",
        ),
    ];

    for (method, path, route_kind) in cases {
        let decision = GatewayControlDecision::synthetic(
            path.to_string(),
            Some("admin_proxy".to_string()),
            Some("users_manage".to_string()),
            Some(route_kind.to_string()),
            Some("admin:users".to_string()),
        );

        assert_eq!(
            management_token_required_permission(&method, &decision).as_deref(),
            Some("admin:users:write"),
            "unexpected permission for {method} {path}"
        );
        assert!(validate_management_token_admin_route_permission(
            &method,
            &decision,
            Some(&write_permissions),
        )
        .is_ok());
    }
}

#[test]
fn ordinary_user_resource_mutations_still_require_users_write_permission() {
    let write_permissions = vec!["admin:users:write".to_string()];

    for (method, route_kind) in [
        (http::Method::PUT, "update_user_api_key"),
        (http::Method::DELETE, "delete_user_session"),
    ] {
        let decision = GatewayControlDecision::synthetic(
            "/api/admin/users/user-1/resource".to_string(),
            Some("admin_proxy".to_string()),
            Some("users_manage".to_string()),
            Some(route_kind.to_string()),
            Some("admin:users".to_string()),
        );

        assert_eq!(
            management_token_required_permission(&method, &decision).as_deref(),
            Some("admin:users:write")
        );
        assert!(validate_management_token_admin_route_permission(
            &method,
            &decision,
            Some(&write_permissions),
        )
        .is_ok());
    }
}

#[test]
fn usable_credential_creation_requires_admin_permission() {
    for (signature, route_kind, write_permission, admin_permission) in [
        (
            "admin:management_tokens",
            "create_token",
            "admin:management_tokens:write",
            "admin:management_tokens:admin",
        ),
        (
            "admin:management_tokens",
            "regenerate_token",
            "admin:management_tokens:write",
            "admin:management_tokens:admin",
        ),
        (
            "admin:endpoints_manage",
            "create_provider_key",
            "admin:endpoints_manage:write",
            "admin:endpoints_manage:admin",
        ),
        (
            "admin:users",
            "create_user_api_key",
            "admin:users:write",
            "admin:users:admin",
        ),
        (
            "admin:api_keys",
            "create_api_key",
            "admin:api_keys:write",
            "admin:api_keys:admin",
        ),
    ] {
        let decision = GatewayControlDecision::synthetic(
            "/api/admin/credential".to_string(),
            Some("admin_proxy".to_string()),
            Some("credential_security_test".to_string()),
            Some(route_kind.to_string()),
            Some(signature.to_string()),
        );
        let write_permissions = vec![write_permission.to_string()];
        let admin_permissions = vec![admin_permission.to_string()];

        assert_eq!(
            management_token_required_permission(&http::Method::POST, &decision).as_deref(),
            Some(admin_permission)
        );
        assert_eq!(
            validate_management_token_admin_route_permission(
                &http::Method::POST,
                &decision,
                Some(&write_permissions),
            )
            .expect_err("delegated write access must not issue usable credentials")
            .required_permission,
            admin_permission
        );
        assert!(validate_management_token_admin_route_permission(
            &http::Method::POST,
            &decision,
            Some(&admin_permissions),
        )
        .is_ok());
    }
}

#[test]
fn ordinary_system_reads_still_require_read_permission() {
    let decision = GatewayControlDecision::synthetic(
        "/api/admin/system/settings".to_string(),
        Some("admin_proxy".to_string()),
        Some("system_manage".to_string()),
        Some("settings_get".to_string()),
        Some("admin:system".to_string()),
    );

    assert_eq!(
        management_token_required_permission(&http::Method::GET, &decision).as_deref(),
        Some("admin:system:read")
    );

    let mut api_key_detail = GatewayControlDecision::synthetic(
        "/api/admin/api-keys/key-1".to_string(),
        Some("admin_proxy".to_string()),
        Some("api_keys_manage".to_string()),
        Some("api_key_detail".to_string()),
        Some("admin:api_keys".to_string()),
    );
    for query in [None, Some("include_key=false"), Some("include_key=invalid")] {
        api_key_detail.public_query_string = query.map(str::to_string);
        assert_eq!(
            management_token_required_permission(&http::Method::GET, &api_key_detail).as_deref(),
            Some("admin:api_keys:read")
        );
    }
}

#[test]
fn audit_admin_read_only_permissions_allow_management_tokens_reads_and_reject_writes() {
    let decision = GatewayControlDecision::synthetic(
        "/api/admin/management-tokens".to_string(),
        Some("admin_proxy".to_string()),
        Some("management_tokens_manage".to_string()),
        Some("list_tokens".to_string()),
        Some("admin:management_tokens".to_string()),
    );
    let permissions = audit_admin_read_only_management_token_permissions();

    assert!(validate_management_token_admin_route_permission(
        &http::Method::GET,
        &decision,
        Some(&permissions),
    )
    .is_ok());
    assert_eq!(
        validate_management_token_admin_route_permission(
            &http::Method::POST,
            &decision,
            Some(&permissions),
        )
        .expect_err("read-only permissions should reject management token writes")
        .required_permission,
        "admin:management_tokens:write"
    );
}

fn extract_admin_route_scopes(source: &'static str) -> BTreeSet<&'static str> {
    let mut scopes = BTreeSet::new();
    let mut remaining = source;

    while let Some(start) = remaining.find("\"admin:") {
        let signature_start = start + 1;
        let after_start = &remaining[signature_start..];
        let Some(end) = after_start.find('"') else {
            break;
        };
        let signature = &after_start[..end];
        let mut parts = signature.split(':');
        if parts.next() == Some("admin") {
            if let (Some(scope), None) = (parts.next(), parts.next()) {
                scopes.insert(scope);
            }
        }
        remaining = &after_start[end + 1..];
    }

    scopes
}
