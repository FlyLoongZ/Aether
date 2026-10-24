use std::sync::Arc;

use super::*;
use aether_data::repository::auth::{
    InMemoryAuthApiKeySnapshotRepository, StoredAuthApiKeySnapshot,
};
use aether_data::repository::routing_profiles::InMemoryRoutingGroupRepository;
use aether_data_contracts::repository::routing_profiles::{
    CreateRoutingGroupBindingRecord, CreateRoutingGroupRecord, RoutingGroupBindingSubject,
    RoutingGroupWriteRepository, UpdateRoutingGroupRecord,
};
use aether_provider_transport::snapshot::{
    GatewayProviderTransportEndpoint, GatewayProviderTransportKey, GatewayProviderTransportProvider,
};

#[test]
fn explicit_routing_selection_cache_key_is_principal_specific() {
    let first = routing_group_selection_cache_key(
        Some("private"),
        None,
        Some("user-1"),
        Some("key-1"),
        &["team-1".to_string()],
    );
    let second = routing_group_selection_cache_key(
        Some("private"),
        None,
        Some("user-2"),
        Some("key-2"),
        &["team-2".to_string()],
    );

    assert_ne!(first, second);
    assert!(first.contains("user=user-1"));
    assert!(first.contains("api_key=key-1"));
    assert!(first.contains("groups=team-1"));
}

#[test]
fn routing_repository_failure_maps_to_internal_gateway_error() {
    let error = routing_selection_error(GatewayRoutingSelectionError::Repository(
        "sql error: database unavailable".to_string(),
    ));

    match error {
        GatewayError::Internal(message) => {
            assert!(message.contains("routing group repository lookup failed"));
            assert!(message.contains("database unavailable"));
        }
        other => panic!("unexpected routing repository error mapping: {other:?}"),
    }
}

#[test]
fn routing_selection_errors_do_not_echo_explicit_group() {
    let secret = "private-group?token=Bearer-secret";

    for error in [
        GatewayRoutingSelectionError::NotFound(secret.to_string()),
        GatewayRoutingSelectionError::Disabled(secret.to_string()),
        GatewayRoutingSelectionError::Forbidden(secret.to_string()),
    ] {
        let error = routing_selection_error(error);
        assert!(matches!(
            error,
            GatewayError::Client {
                status: StatusCode::FORBIDDEN,
                ref message,
            } if !message.contains(secret)
        ));
    }
}

#[test]
fn routing_provider_errors_do_not_echo_dynamic_details() {
    let secret = "https://internal.example/?token=Bearer-secret";
    let contract_error = invalid_routing_provider_contract();
    let header_error = btree_headers_to_header_map(&BTreeMap::from([(
        format!("Authorization: {secret}"),
        secret.to_string(),
    )]))
    .expect_err("invalid header should fail");

    for (error, expected_message) in [
        (contract_error, INVALID_ROUTING_PROVIDER_CONTRACT_MESSAGE),
        (header_error, INVALID_ROUTING_PROVIDER_HEADERS_MESSAGE),
    ] {
        assert!(matches!(
            error,
            GatewayError::Client {
                status: StatusCode::BAD_REQUEST,
                ref message,
            } if message == expected_message && !message.contains(secret)
        ));
    }
}

#[tokio::test]
async fn explicit_auth_snapshot_override_does_not_fall_back_to_the_planner_cache() {
    // AppState::new has no auth snapshot repository. Without the explicit
    // override this resolver returns None; a WebSocket strong snapshot must
    // therefore be the exact value used to build the planner input.
    let state = AppState::new().expect("test state should build");
    let mut strong_snapshot = sample_auth_snapshot();
    strong_snapshot.api_key_allowed_models = Some(vec!["gpt-live-only".to_string()]);

    let resolved = resolve_local_authenticated_decision_input_with_snapshot(
        &state,
        sample_auth_context(),
        Some(strong_snapshot.clone()),
        Some("gpt-live-only"),
        Some("openai:responses"),
        None,
        &Default::default(),
    )
    .await
    .expect("snapshot override should resolve")
    .expect("the explicit snapshot should replace the missing cached value");

    assert_eq!(resolved.auth_snapshot, strong_snapshot);
}

#[tokio::test]
async fn explicit_auth_snapshot_override_rejects_an_identity_mismatch() {
    let state = AppState::new().expect("test state should build");
    let mut wrong_snapshot = sample_auth_snapshot();
    wrong_snapshot.api_key_id = "another-key".to_string();

    let error = resolve_local_authenticated_decision_input_with_snapshot(
        &state,
        sample_auth_context(),
        Some(wrong_snapshot),
        Some("gpt-live-only"),
        Some("openai:responses"),
        None,
        &Default::default(),
    )
    .await
    .expect_err("a snapshot for another API key must never be injected");

    assert!(matches!(error, GatewayError::Internal(message) if message.contains("identity")));
}

#[tokio::test]
async fn api_key_routing_selection_applies_at_planner_and_invalidates_after_changes() {
    let auth_repository = Arc::new(InMemoryAuthApiKeySnapshotRepository::seed(
        ["api-key-1", "api-key-2"].map(|key_id| {
            (
                None,
                StoredAuthApiKeySnapshot::new(
                    "user-1".into(),
                    "alice".into(),
                    None,
                    "user".into(),
                    "local".into(),
                    true,
                    false,
                    None,
                    None,
                    None,
                    key_id.into(),
                    Some(key_id.into()),
                    true,
                    false,
                    false,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                )
                .unwrap(),
            )
        }),
    ));
    let groups = Arc::new(InMemoryRoutingGroupRepository::default());
    for (id, visible, is_default, multiplier) in [
        ("default", false, true, 1.0),
        ("discount", true, false, 0.5),
        ("premium", true, false, 2.0),
    ] {
        groups
            .create_routing_group(CreateRoutingGroupRecord {
                id: id.into(),
                name: format!("{id}-name"),
                description: None,
                enabled: true,
                is_system_default: is_default,
                sort_order: 0,
                config_json: json!({ "user_visible": visible, "billing_multiplier": multiplier }),
                version: 1,
                created_at: 1,
                updated_at: 1,
                published_at: None,
            })
            .await
            .unwrap();
    }
    let state = AppState::new().unwrap().with_data_state_for_tests(
        crate::data::GatewayDataState::with_auth_api_key_repository_for_tests(auth_repository)
            .with_routing_group_repository_for_tests(groups.clone()),
    );
    for (key_id, group_id) in [("api-key-1", "discount"), ("api-key-2", "premium")] {
        assert!(state
            .set_user_api_key_feature_settings(
                "user-1",
                key_id,
                Some(json!({ "routing_group_id": group_id }))
            )
            .await
            .unwrap()
            .is_some());
    }
    let (parts, _) = http::Request::builder().body(()).unwrap().into_parts();
    let (header_parts, _) = http::Request::builder()
        .header(ROUTING_GROUP_HEADER, "premium")
        .body(())
        .unwrap()
        .into_parts();

    async fn attach(
        state: &AppState,
        parts: &http::request::Parts,
        key_id: &str,
    ) -> Result<LocalRequestedModelDecisionInput, GatewayError> {
        let mut input = sample_decision_input();
        input.auth_context.api_key_id = key_id.into();
        input.auth_snapshot.api_key_id = key_id.into();
        attach_routing_policy_to_local_requested_model_input(
            state,
            parts,
            &mut input,
            &json!({ "model": "gpt-5" }),
            "openai:chat",
        )
        .await?;
        Ok(input)
    }

    // Revisit the first key after the second to exercise both cached choices.
    for (key_id, group_id, multiplier) in [
        ("api-key-1", "discount", 0.5),
        ("api-key-2", "premium", 2.0),
        ("api-key-1", "discount", 0.5),
    ] {
        let input = attach(&state, &parts, key_id).await.unwrap();
        let policy = input.routing_policy.as_ref().unwrap();
        assert_eq!(policy.group_id.as_deref(), Some(group_id));
        assert_eq!(policy.selection_source, "api_key_selection");
        assert_eq!(policy.billing_multiplier, multiplier);
        assert_eq!(
            input
                .routing_trace_seed
                .as_ref()
                .unwrap()
                .billing_multiplier,
            Some(multiplier)
        );
    }
    let header = attach(&state, &header_parts, "api-key-1").await.unwrap();
    let policy = header.routing_policy.unwrap();
    assert_eq!(policy.group_id.as_deref(), Some("premium"));
    assert_eq!(policy.selection_source, "explicit_header");

    groups
        .update_routing_group(
            "discount",
            UpdateRoutingGroupRecord {
                config_json: Some(json!({ "user_visible": false, "billing_multiplier": 0.5 })),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    state.invalidate_provider_routing_caches();
    assert!(matches!(
        attach(&state, &parts, "api-key-1").await,
        Err(GatewayError::Client {
            status: StatusCode::FORBIDDEN,
            ..
        })
    ));
    let header = attach(&state, &header_parts, "api-key-1").await.unwrap();
    assert_eq!(
        header.routing_policy.unwrap().group_id.as_deref(),
        Some("premium")
    );

    assert!(state
        .set_user_api_key_feature_settings("user-1", "api-key-1", None)
        .await
        .unwrap()
        .is_some());
    let cleared = attach(&state, &parts, "api-key-1").await.unwrap();
    let policy = cleared.routing_policy.unwrap();
    assert_eq!(policy.group_id.as_deref(), Some("default"));
    assert_eq!(policy.selection_source, "system_default");
    assert_eq!(policy.billing_multiplier, 1.0);
    // Clearing one key's preference must not disturb the other key's selection.
    let other = attach(&state, &parts, "api-key-2").await.unwrap();
    assert_eq!(
        other.routing_policy.unwrap().group_id.as_deref(),
        Some("premium")
    );
}

#[tokio::test]
async fn explicit_routing_attachment_authorizes_and_caches_per_principal() {
    let repository = Arc::new(InMemoryRoutingGroupRepository::default());
    repository
        .create_routing_group(CreateRoutingGroupRecord {
            id: "private-group".to_string(),
            name: "private".to_string(),
            description: None,
            enabled: true,
            is_system_default: false,
            sort_order: 0,
            config_json: json!({"billing_multiplier": 0.5}),
            version: 1,
            created_at: 1,
            updated_at: 1,
            published_at: None,
        })
        .await
        .unwrap();
    repository
        .create_routing_group_binding(CreateRoutingGroupBindingRecord {
            id: "binding-user-1".to_string(),
            group_id: "private-group".to_string(),
            subject_type: RoutingGroupBindingSubject::User,
            subject_id: "user-1".to_string(),
            is_default: false,
            allow_explicit_select: true,
            created_at: 1,
            updated_at: 1,
        })
        .await
        .unwrap();
    let state = AppState::new().unwrap().with_data_state_for_tests(
        crate::data::GatewayDataState::disabled()
            .with_routing_group_repository_for_tests(repository),
    );
    let (parts, _) = http::Request::builder()
        .header(ROUTING_GROUP_HEADER, "private-group")
        .body(())
        .unwrap()
        .into_parts();

    let mut allowed = sample_decision_input();
    attach_routing_policy_to_local_requested_model_input(
        &state,
        &parts,
        &mut allowed,
        &json!({"model": "gpt-5"}),
        "openai:chat",
    )
    .await
    .expect("bound principal should explicitly select the private group");
    let policy = allowed
        .routing_policy
        .as_ref()
        .expect("explicit selection should attach routing policy");
    assert_eq!(policy.group_id.as_deref(), Some("private-group"));
    assert_eq!(policy.group_name.as_deref(), Some("private"));
    assert_eq!(policy.billing_multiplier, 0.5);
    let trace = allowed.routing_trace_seed.as_ref().unwrap();
    assert_eq!(trace.group_name.as_deref(), Some("private"));
    assert_eq!(trace.billing_multiplier, Some(0.5));
    assert_eq!(policy.selection_source, "explicit_header");

    let mut denied = sample_decision_input();
    denied.auth_context.user_id = "user-2".to_string();
    denied.auth_context.api_key_id = "api-key-2".to_string();
    let error = attach_routing_policy_to_local_requested_model_input(
        &state,
        &parts,
        &mut denied,
        &json!({"model": "gpt-5"}),
        "openai:chat",
    )
    .await
    .expect_err("another principal must not reuse the authorized cache entry");
    match error {
        GatewayError::Client { status, message } => {
            assert_eq!(status, StatusCode::FORBIDDEN);
            assert!(message.contains("not allowed for this principal"));
        }
        other => panic!("unexpected explicit selection error: {other:?}"),
    }
}

fn sample_auth_context() -> ExecutionRuntimeAuthContext {
    ExecutionRuntimeAuthContext {
        user_id: "user-1".to_string(),
        api_key_id: "api-key-1".to_string(),
        username: None,
        api_key_name: None,
        balance_remaining: None,
        access_allowed: true,
        api_key_is_standalone: false,
    }
}

fn sample_auth_snapshot() -> GatewayAuthApiKeySnapshot {
    GatewayAuthApiKeySnapshot {
        user_id: "user-1".to_string(),
        username: "alice".to_string(),
        email: None,
        user_role: "user".to_string(),
        user_auth_source: "local".to_string(),
        user_is_active: true,
        user_is_deleted: false,
        user_rate_limit: None,
        user_allowed_providers: None,
        user_allowed_api_formats: None,
        user_allowed_models: None,
        api_key_id: "api-key-1".to_string(),
        api_key_name: Some("default".to_string()),
        api_key_is_active: true,
        api_key_is_locked: false,
        api_key_is_standalone: false,
        api_key_rate_limit: None,
        api_key_concurrent_limit: None,
        api_key_expires_at_unix_secs: None,
        api_key_allowed_providers: None,
        api_key_allowed_api_formats: None,
        api_key_allowed_models: None,
        api_key_ip_rules: None,
        currently_usable: true,
    }
}

fn sample_decision_input() -> LocalRequestedModelDecisionInput {
    LocalRequestedModelDecisionInput {
        auth_context: sample_auth_context(),
        requested_model: "gpt-5".to_string(),
        auth_snapshot: sample_auth_snapshot(),
        required_capabilities: None,
        request_auth_channel: None,
        client_surface: None,
        gateway_credential_carrier: None,
        client_session_affinity: None,
        provider_outbound_context: None,
        routing_policy: None,
        routing_trace_seed: None,
        model_directive_policy: Default::default(),
        routing_context: Some(LocalRoutingRequestContext {
            group_id: Some("group-1".to_string()),
            group_version: Some(3),
            selection_source: "explicit_header".to_string(),
            client_api_format: "openai:chat".to_string(),
            effective_body_json: json!({"model":"gpt-5"}),
            effective_headers: HeaderMap::new(),
            group_config_json: json!({
                "allowed_models": ["gpt-5"],
                "rules": [{
                    "id": "provider-patch",
                    "priority": 1,
                    "enabled": true,
                    "phase": "provider_request",
                    "conditions": {},
                    "actions": [
                        {
                            "type": "json_patch_body",
                            "patch": [{
                                "op": "add",
                                "path": "/metadata/routing",
                                "value": "provider"
                            }]
                        },
                        {
                            "type": "patch_headers",
                            "patch": [{
                                "op": "set",
                                "name": "x-provider-route",
                                "value": "provider"
                            }]
                        }
                    ]
                }]
            }),
        }),
    }
}

fn sample_decision() -> AiExecutionDecision {
    AiExecutionDecision {
        action: "execution_runtime_sync_decision".to_string(),
        decision_kind: Some("openai_chat_sync".to_string()),
        execution_strategy: None,
        conversion_mode: None,
        request_id: Some("trace-1".to_string()),
        candidate_id: Some("candidate-1".to_string()),
        provider_name: Some("provider".to_string()),
        provider_type: Some("openai".to_string()),
        provider_id: Some("provider-1".to_string()),
        endpoint_id: Some("endpoint-1".to_string()),
        key_id: Some("key-1".to_string()),
        upstream_base_url: None,
        upstream_url: None,
        provider_request_method: None,
        auth_header: None,
        auth_value: None,
        provider_api_format: Some("openai:chat".to_string()),
        client_api_format: Some("openai:chat".to_string()),
        provider_contract: None,
        client_contract: None,
        model_name: Some("gpt-5".to_string()),
        mapped_model: Some("gpt-5".to_string()),
        prompt_cache_key: None,
        extra_headers: BTreeMap::new(),
        provider_request_headers: BTreeMap::from([(
            "content-type".to_string(),
            "application/json".to_string(),
        )]),
        provider_request_body: Some(json!({"model":"gpt-5","metadata":{}})),
        provider_request_body_base64: None,
        content_type: Some("application/json".to_string()),
        content_encoding: None,
        request_gzip: None,
        proxy: None,
        transport_profile: None,
        timeouts: None,
        upstream_is_stream: false,
        report_kind: Some("local_sync_success".to_string()),
        report_context: Some(json!({
            "candidate_index": 0,
            "retry_index": 0,
            "model_id": "model-1"
        })),
        auth_context: Some(sample_auth_context()),
    }
}

fn sample_codex_transport_with_card() -> GatewayProviderTransportSnapshot {
    let card = json!({
        "id": "gpt-future-agent",
        "slug": "gpt-future-agent",
        "use_responses_lite": true,
        "supports_reasoning_summary_parameter": true,
        "default_reasoning_level": "low",
        "default_reasoning_summary": "none",
        "supported_reasoning_levels": [{"effort": "low"}, {"effort": "high"}]
    });
    GatewayProviderTransportSnapshot {
        provider: GatewayProviderTransportProvider {
            id: "provider-codex".to_string(),
            name: "Codex".to_string(),
            provider_type: "codex".to_string(),
            website: None,
            is_active: true,
            keep_priority_on_conversion: false,
            enable_format_conversion: true,
            concurrent_limit: None,
            max_retries: None,
            proxy: None,
            request_timeout_secs: None,
            stream_first_byte_timeout_secs: None,
            config: None,
        },
        endpoint: GatewayProviderTransportEndpoint {
            id: "endpoint-codex".to_string(),
            provider_id: "provider-codex".to_string(),
            api_format: "openai:responses:compact".to_string(),
            api_family: Some("openai".to_string()),
            endpoint_kind: Some("compact".to_string()),
            is_active: true,
            base_url: "https://chatgpt.com/backend-api/codex".to_string(),
            header_rules: None,
            body_rules: None,
            max_retries: None,
            custom_path: None,
            config: None,
            format_acceptance_config: None,
            proxy: None,
        },
        key: GatewayProviderTransportKey {
            id: "key-codex".to_string(),
            provider_id: "provider-codex".to_string(),
            name: "Codex key".to_string(),
            auth_type: "oauth".to_string(),
            is_active: true,
            api_formats: Some(vec!["openai:responses:compact".to_string()]),
            auth_type_by_format: None,
            allow_auth_channel_mismatch_formats: None,
            allowed_models: Some(vec!["gpt-future-agent".to_string()]),
            capabilities: None,
            rate_multipliers: None,
            global_priority_by_format: None,
            expires_at_unix_secs: None,
            proxy: None,
            fingerprint: None,
            upstream_metadata: Some(crate::ai_serving::build_codex_model_catalog_metadata(&[
                card,
            ])),
            decrypted_api_key: "access-token".to_string(),
            decrypted_auth_config: None,
        },
    }
}

fn sample_codex_fingerprint_transport() -> GatewayProviderTransportSnapshot {
    let mut transport = sample_codex_transport_with_card();
    transport.provider.config = Some(json!({
        "codex": {"fingerprint_convergence_enabled": true}
    }));
    transport.endpoint.api_format = "openai:responses".to_string();
    transport.endpoint.endpoint_kind = Some("responses".to_string());
    transport.key.api_formats = Some(vec!["openai:responses".to_string()]);
    transport.key.decrypted_auth_config =
        Some(json!({"account_id": "account-codex-1"}).to_string());
    transport
}

fn sample_codex_fingerprint_decision() -> AiExecutionDecision {
    let prompt_cache_key = "172c39e6-c0a0-5a70-8b63-e0f8e0d185a3";
    let mut decision = sample_decision();
    decision.provider_type = Some("codex".to_string());
    decision.provider_api_format = Some("openai:responses".to_string());
    decision.client_api_format = Some("openai:responses".to_string());
    decision.provider_request_headers.extend([
        ("session-id".to_string(), "spoofed-session".to_string()),
        ("thread-id".to_string(), "spoofed-thread".to_string()),
        (
            "x-codex-turn-metadata".to_string(),
            json!({
                "installation_id": "spoofed-installation",
                "session_id": "spoofed-session",
                "thread_source": "cli"
            })
            .to_string(),
        ),
    ]);
    decision.provider_request_body = Some(json!({
        "model": "gpt-5",
        "input": [],
        "metadata": {},
        "prompt_cache_key": prompt_cache_key,
        "client_metadata": {
            "session_id": "spoofed-session",
            "thread_id": "spoofed-thread",
            "caller": "sdk",
            "x-codex-turn-metadata": json!({
                "installation_id": "spoofed-installation",
                "session_id": "spoofed-session",
                "sandbox": "workspace-write"
            }).to_string()
        }
    }));
    decision
}

fn set_provider_request_rules(
    input: &mut LocalRequestedModelDecisionInput,
    allowed_models: &[&str],
    actions: Value,
) {
    let config = json!({
        "allowed_models": allowed_models,
        "rules": [{
            "id": "provider-patch",
            "priority": 1,
            "enabled": true,
            "phase": "provider_request",
            "conditions": {},
            "actions": actions
        }]
    });
    input
        .routing_context
        .as_mut()
        .expect("sample input should include routing context")
        .group_config_json = config;
}

#[test]
fn static_default_routing_policy_attaches_without_request_context() {
    let request = http::Request::builder()
        .header("content-type", "application/json")
        .body(())
        .expect("request should build");
    let (parts, _) = request.into_parts();
    let mut input = LocalRequestedModelDecisionInput {
        auth_context: sample_auth_context(),
        requested_model: "mock-model".to_string(),
        auth_snapshot: sample_auth_snapshot(),
        required_capabilities: None,
        request_auth_channel: None,
        client_surface: None,
        gateway_credential_carrier: None,
        client_session_affinity: None,
        provider_outbound_context: None,
        routing_policy: None,
        routing_trace_seed: None,
        model_directive_policy: Default::default(),
        routing_context: Some(LocalRoutingRequestContext {
            group_id: Some("stale".to_string()),
            group_version: Some(1),
            group_config_json: json!({}),
            selection_source: "stale".to_string(),
            client_api_format: "openai:chat".to_string(),
            effective_body_json: json!({}),
            effective_headers: HeaderMap::new(),
        }),
    };
    let group_config_json = json!({
        "default_policy": {
            "priority_mode": "global_key",
            "scheduling_mode": "load_balance",
            "keep_priority_on_conversion": true
        },
        "allowed_models": [],
        "model_policies": [],
        "rules": []
    });

    let attached = try_attach_static_default_routing_policy_to_input(
        &mut input,
        &parts,
        &json!({"model": "mock-model"}),
        "openai:chat",
        Some("group-1"),
        Some(4),
        &group_config_json,
        "system_default",
    )
    .expect("static routing should attach");

    assert!(attached);
    assert!(input.routing_context.is_none());
    let policy = input.routing_policy.as_ref().expect("policy should be set");
    assert_eq!(policy.group_id.as_deref(), Some("group-1"));
    assert_eq!(policy.group_version, Some(4));
    assert_eq!(
        policy.priority_mode,
        aether_routing_core::RoutingSetPriorityMode::Provider
    );
    assert_eq!(
        policy.scheduling_mode,
        aether_routing_core::RoutingSchedulingMode::LoadBalance
    );
    assert!(policy.keep_priority_on_conversion);
    assert!(policy.mutation_plan.is_empty());
    assert!(input.routing_trace_seed.is_some());
}

#[test]
fn dynamic_routing_policy_does_not_attach_static_fast_path() {
    let request = http::Request::builder()
        .body(())
        .expect("request should build");
    let (parts, _) = request.into_parts();
    let mut input = LocalRequestedModelDecisionInput {
        auth_context: sample_auth_context(),
        requested_model: "mock-model".to_string(),
        auth_snapshot: sample_auth_snapshot(),
        required_capabilities: None,
        request_auth_channel: None,
        client_surface: None,
        gateway_credential_carrier: None,
        client_session_affinity: None,
        provider_outbound_context: None,
        routing_policy: None,
        routing_trace_seed: None,
        routing_context: None,
        model_directive_policy: Default::default(),
    };
    let group_config_json = json!({
        "rules": [{
            "id": "rule-1",
            "conditions": {},
            "actions": [{
                "type": "restrict_providers",
                "provider_ids": ["provider-1"]
            }]
        }]
    });

    let attached = try_attach_static_default_routing_policy_to_input(
        &mut input,
        &parts,
        &json!({"model": "mock-model"}),
        "openai:chat",
        Some("group-1"),
        Some(4),
        &group_config_json,
        "system_default",
    )
    .expect("dynamic config should not fail static detection");

    assert!(!attached);
    assert!(input.routing_policy.is_none());
    assert!(input.routing_trace_seed.is_none());
    assert!(input.routing_context.is_none());
}

#[test]
fn provider_request_routing_policy_mutates_decision_body_headers_and_report_context() {
    let input = sample_decision_input();
    let mut decision = sample_decision();

    apply_provider_request_routing_policy_to_decision(&input, &mut decision, None)
        .expect("provider routing mutation should apply");

    assert_eq!(
        decision.provider_request_body.as_ref().unwrap()["metadata"]["routing"],
        json!("provider")
    );
    assert_eq!(
        decision
            .provider_request_headers
            .get("x-provider-route")
            .map(String::as_str),
        Some("provider")
    );
    let report_context = decision.report_context.as_ref().unwrap();
    assert_eq!(
        report_context["routing_provider_request_patch_summary"]["matched_rules"],
        json!(["provider-patch"])
    );
    assert_eq!(
        report_context["routing_trace"]["provider_request_patch_summary"]["body_paths"],
        json!(["/metadata/routing"])
    );
    assert_eq!(
        report_context["routing_trace"]["global_candidates"][0]["provider_id"],
        json!("provider-1")
    );
}

#[test]
fn non_codex_provider_outbound_policies_are_terminal_noop() {
    let mut input = sample_decision_input();
    input.routing_context = None;
    input.provider_outbound_context = Some(ProviderOutboundRequestContext::new(
        "logical-turn",
        1_700_000_000_123,
    ));
    let mut decision = sample_codex_fingerprint_decision();
    decision.provider_type = Some("openai".to_string());
    decision.provider_api_format = Some("openai:responses".to_string());
    decision.client_api_format = Some("openai:responses".to_string());
    let mut transport = sample_codex_fingerprint_transport();
    transport.provider.provider_type = "openai".to_string();

    let original_headers = decision.provider_request_headers.clone();
    let original_body = decision.provider_request_body.clone();
    apply_provider_request_routing_policy_to_decision(&input, &mut decision, Some(&transport))
        .expect("non-Codex terminal finalization should succeed");

    assert_eq!(decision.provider_request_headers, original_headers);
    assert_eq!(decision.provider_request_body, original_body);
    assert!(decision
        .report_context
        .as_ref()
        .and_then(|context| context.get("provider_outbound_policies"))
        .is_none());
}

#[test]
fn codex_fingerprint_convergence_runs_at_every_provider_routing_success_exit() {
    let transport = sample_codex_fingerprint_transport();
    let mut no_context = sample_decision_input();
    no_context.routing_context = None;
    let mut empty_mutation = sample_decision_input();
    empty_mutation
        .routing_context
        .as_mut()
        .expect("routing context")
        .group_config_json = json!({
        "allowed_models": ["gpt-5"],
        "rules": []
    });
    let mut with_mutation = sample_decision_input();
    for input in [&mut no_context, &mut empty_mutation, &mut with_mutation] {
        input.provider_outbound_context = Some(
            ProviderOutboundRequestContext::new(
                uuid::Uuid::new_v4().to_string(),
                1_756_668_000_000,
            )
            .with_original_client_session_id("client-session-1".to_string()),
        );
    }

    let mut stable_identity = None;
    let mut turn_ids = std::collections::BTreeSet::new();
    for (exit_name, input) in [
        ("no_context", no_context),
        ("empty_mutation", empty_mutation),
        ("with_mutation", with_mutation),
    ] {
        let mut decision = sample_codex_fingerprint_decision();

        apply_provider_request_routing_policy_to_decision(&input, &mut decision, Some(&transport))
            .unwrap_or_else(|error| panic!("{exit_name} should converge: {error:?}"));

        let session_id = decision.provider_request_headers["session-id"].clone();
        let thread_id = decision.provider_request_headers["thread-id"].clone();
        let installation_id = decision.provider_request_headers["x-codex-installation-id"].clone();
        let window_id = decision.provider_request_headers["x-codex-window-id"].clone();
        assert_eq!(decision.provider_request_headers["session_id"], session_id);
        assert_eq!(
            decision.provider_request_headers["x-client-request-id"],
            thread_id
        );
        assert_eq!(window_id, format!("{thread_id}:0"));
        assert_eq!(
            uuid::Uuid::parse_str(&session_id)
                .expect("session UUID")
                .get_version_num(),
            4
        );
        assert_eq!(
            uuid::Uuid::parse_str(&thread_id)
                .expect("thread UUID")
                .get_version_num(),
            4
        );

        let body = decision
            .provider_request_body
            .as_ref()
            .expect("request body");
        assert_eq!(
            decision.prompt_cache_key.as_deref(),
            body.get("prompt_cache_key").and_then(Value::as_str)
        );
        assert_eq!(
            body["prompt_cache_key"],
            "172c39e6-c0a0-5a70-8b63-e0f8e0d185a3"
        );
        assert_eq!(body["client_metadata"]["session_id"], session_id);
        assert_eq!(body["client_metadata"]["thread_id"], thread_id);
        assert_eq!(body["client_metadata"]["caller"], "sdk");
        assert_eq!(
            body["client_metadata"]["x-codex-installation-id"],
            installation_id
        );
        assert_eq!(body["client_metadata"]["x-codex-window-id"], window_id);

        let policy_results = decision
            .report_context
            .as_ref()
            .and_then(|context| context.get("provider_outbound_policies"))
            .and_then(|policies| policies.get("results"))
            .and_then(Value::as_array)
            .expect("provider policy results");
        assert_eq!(
            policy_results.len(),
            1,
            "policy result count at {exit_name}"
        );
        assert_eq!(
            policy_results[0]["policy"],
            json!("codex_fingerprint_convergence")
        );
        assert_eq!(policy_results[0]["outcome"], json!("applied"));
        assert_eq!(policy_results[0]["reason"], json!("applied"));

        let header_metadata: Value =
            serde_json::from_str(&decision.provider_request_headers["x-codex-turn-metadata"])
                .expect("header turn metadata");
        let body_metadata: Value = serde_json::from_str(
            body["client_metadata"]["x-codex-turn-metadata"]
                .as_str()
                .expect("embedded turn metadata"),
        )
        .expect("embedded turn metadata JSON");
        assert_eq!(header_metadata["thread_source"], "cli");
        assert_eq!(body_metadata["sandbox"], "workspace-write");
        assert_eq!(
            header_metadata["turn_id"],
            body["client_metadata"]["turn_id"]
        );
        assert_eq!(body_metadata["turn_id"], body["client_metadata"]["turn_id"]);
        assert_eq!(
            header_metadata["turn_started_at_unix_ms"],
            body_metadata["turn_started_at_unix_ms"]
        );
        let turn_id = body["client_metadata"]["turn_id"]
            .as_str()
            .expect("turn ID")
            .to_string();
        assert_eq!(
            uuid::Uuid::parse_str(&turn_id)
                .expect("turn UUID")
                .get_version_num(),
            7
        );
        turn_ids.insert(turn_id);

        let identity = (installation_id, session_id, thread_id);
        if let Some(expected) = stable_identity.as_ref() {
            assert_eq!(&identity, expected, "identity changed at {exit_name}");
        } else {
            stable_identity = Some(identity);
        }
    }
    assert_eq!(turn_ids.len(), 3, "each request needs a fresh turn ID");
}

#[test]
fn provider_request_routing_policy_cannot_restore_credentials_or_aether_internal_headers() {
    for header_name in [
        "authorization",
        "proxy-authorization",
        "api-key",
        "x-api-key",
        "x-goog-api-key",
        "cookie",
        "cookie2",
        "set-cookie",
        "x-aether-auth-user-id",
        "x-aether-control-future",
    ] {
        let mut input = sample_decision_input();
        set_provider_request_rules(
            &mut input,
            &["gpt-5"],
            json!([{
                "type": "patch_headers",
                "patch": [{
                    "op": "set",
                    "name": header_name,
                    "value": "must-not-reach-upstream"
                }]
            }]),
        );
        let mut decision = sample_decision();

        let error = apply_provider_request_routing_policy_to_decision(&input, &mut decision, None)
            .expect_err("reserved provider header mutation should fail closed");

        assert!(
            matches!(
                &error,
                GatewayError::Client {
                    status: StatusCode::BAD_REQUEST,
                    ..
                }
            ),
            "unexpected error for {header_name}: {error:?}"
        );
        assert!(
            !decision
                .provider_request_headers
                .keys()
                .any(|name| name.eq_ignore_ascii_case(header_name)),
            "reserved header reached the provider decision: {header_name}"
        );
    }
}

#[test]
fn codex_prompt_cache_identity_headers_are_terminal_after_routing_mutations() {
    let mut input = sample_decision_input();
    input
        .routing_context
        .as_mut()
        .expect("routing context")
        .client_api_format = "openai:responses".to_string();
    set_provider_request_rules(
        &mut input,
        &["gpt-5"],
        json!([{
            "type": "patch_headers",
            "patch": [
                {"op": "remove", "name": "session-id"},
                {"op": "remove", "name": "thread-id"}
            ]
        }]),
    );
    let identity = "172c39e6-c0a0-5a70-8b63-e0f8e0d185a3";
    let mut decision = sample_decision();
    decision.provider_type = Some("codex".to_string());
    decision.provider_api_format = Some("openai:responses".to_string());
    decision.client_api_format = Some("openai:responses".to_string());
    decision.provider_request_body = Some(json!({
        "model": "gpt-5",
        "input": [],
        "prompt_cache_key": identity,
        "client_metadata": {
            "session_id": identity,
            "thread_id": identity
        }
    }));
    assert!(!decision.provider_request_headers.contains_key("session-id"));
    assert!(!decision.provider_request_headers.contains_key("thread-id"));

    apply_provider_request_routing_policy_to_decision(&input, &mut decision, None)
        .expect("terminal Codex identity contract should be restored");

    assert_eq!(
        decision
            .provider_request_headers
            .get("session-id")
            .map(String::as_str),
        Some(identity)
    );
    assert_eq!(
        decision
            .provider_request_headers
            .get("thread-id")
            .map(String::as_str),
        Some(identity)
    );
}

#[test]
fn codex_prompt_cache_identity_headers_fail_closed_after_body_identity_removal() {
    let mut input = sample_decision_input();
    input
        .routing_context
        .as_mut()
        .expect("routing context")
        .client_api_format = "openai:responses".to_string();
    set_provider_request_rules(
        &mut input,
        &["gpt-5"],
        json!([{
            "type": "json_patch_body",
            "patch": [
                {"op": "remove", "path": "/prompt_cache_key"},
                {"op": "remove", "path": "/client_metadata"}
            ]
        }]),
    );
    let identity = "172c39e6-c0a0-5a70-8b63-e0f8e0d185a3";
    let mut decision = sample_decision();
    decision.provider_type = Some("codex".to_string());
    decision.provider_api_format = Some("openai:responses".to_string());
    decision.client_api_format = Some("openai:responses".to_string());
    decision.provider_request_body = Some(json!({
        "model": "gpt-5",
        "input": [],
        "prompt_cache_key": identity,
        "client_metadata": {
            "session_id": identity,
            "thread_id": identity
        }
    }));
    assert!(!decision.provider_request_headers.contains_key("session-id"));
    assert!(!decision.provider_request_headers.contains_key("thread-id"));

    apply_provider_request_routing_policy_to_decision(&input, &mut decision, None)
        .expect("terminal Codex identity contract should fail closed");

    let body = decision.provider_request_body.as_ref().expect("body");
    assert!(body.get("prompt_cache_key").is_none());
    assert!(decision.prompt_cache_key.is_none());
    assert!(body.get("client_metadata").is_none());
    assert!(!decision.provider_request_headers.contains_key("session-id"));
    assert!(!decision.provider_request_headers.contains_key("thread-id"));
}

#[test]
fn codex_compact_contract_is_terminal_after_routing_mutations() {
    let mut input = sample_decision_input();
    input
        .routing_context
        .as_mut()
        .expect("routing context")
        .client_api_format = "openai:responses:compact".to_string();
    set_provider_request_rules(
        &mut input,
        &["gpt-5"],
        json!([
            {
                "type": "json_patch_body",
                "patch": [
                    {"op": "add", "path": "/store", "value": true},
                    {"op": "add", "path": "/top_logprobs", "value": 5},
                    {"op": "add", "path": "/custom_extension", "value": true},
                    {"op": "replace", "path": "/input", "value": "routed compact input"},
                    {"op": "replace", "path": "/tools", "value": [{
                        "type": "function",
                        "name": "lookup",
                        "cache_control": {"type": "ephemeral"}
                    }]}
                ]
            },
            {
                "type": "patch_headers",
                "patch": [
                    {"op": "set", "name": "chatgpt-account-id", "value": "spoofed"},
                    {"op": "set", "name": "x-openai-fedramp", "value": "false"},
                    {"op": "set", "name": "x-client-request-id", "value": "spoofed"},
                    {"op": "set", "name": "accept", "value": "text/event-stream"},
                    {"op": "set", "name": "content-encoding", "value": "zstd"}
                ]
            }
        ]),
    );
    let mut decision = sample_decision();
    decision.provider_type = Some("codex".to_string());
    decision.provider_api_format = Some("openai:responses:compact".to_string());
    decision.client_api_format = Some("openai:responses:compact".to_string());
    decision.provider_request_body = Some(json!({
        "model": "gpt-5",
        "input": [],
        "tools": [{"type": "function", "name": "lookup"}]
    }));
    decision
        .provider_request_headers
        .insert(CODEX_ACCOUNT_ID_HEADER.to_string(), "account-1".to_string());
    decision
        .provider_request_headers
        .insert(CODEX_FEDRAMP_HEADER.to_string(), "true".to_string());

    apply_provider_request_routing_policy_to_decision(&input, &mut decision, None)
        .expect("terminal contract should accept the projected request");

    let body = decision.provider_request_body.as_ref().expect("body");
    assert_eq!(body["parallel_tool_calls"], false);
    assert_eq!(body["input"][0]["type"], "message");
    assert_eq!(
        body["input"][0]["content"][0]["text"],
        "routed compact input"
    );
    assert!(body["tools"][0].get("cache_control").is_none());
    for field in ["store", "top_logprobs", "custom_extension"] {
        assert!(
            body.get(field).is_none(),
            "unexpected Compact field: {field}"
        );
    }
    assert_eq!(
        decision
            .provider_request_headers
            .get(CODEX_ACCOUNT_ID_HEADER),
        Some(&"account-1".to_string())
    );
    assert_eq!(
        decision.provider_request_headers.get(CODEX_FEDRAMP_HEADER),
        Some(&"true".to_string())
    );
    for header in ["x-client-request-id", "accept", "content-encoding"] {
        assert!(
            !decision.provider_request_headers.contains_key(header),
            "unexpected Compact header: {header}"
        );
    }
}

#[test]
fn codex_responses_lite_contract_is_terminal_after_routing_mutations() {
    let mut input = sample_decision_input();
    input.requested_model = "gpt-future-agent".to_string();
    input
        .routing_context
        .as_mut()
        .expect("routing context")
        .client_api_format = "openai:responses:compact".to_string();
    set_provider_request_rules(
        &mut input,
        &["gpt-future-agent"],
        json!([
            {
                "type": "json_patch_body",
                "patch": [
                    {"op": "replace", "path": "/input", "value": "routed compact input"},
                    {"op": "add", "path": "/instructions", "value": "Routed instructions"},
                    {"op": "replace", "path": "/tools", "value": [{
                        "type": "function",
                        "name": "lookup",
                        "parameters": {},
                        "cache_control": {"type": "ephemeral"}
                    }]},
                    {"op": "add", "path": "/parallel_tool_calls", "value": true},
                    {"op": "add", "path": "/reasoning", "value": {
                        "effort": "high",
                        "context": "current_turn"
                    }}
                ]
            },
            {
                "type": "patch_headers",
                "patch": [{
                    "op": "set",
                    "name": "x-openai-internal-codex-responses-lite",
                    "value": "false"
                }]
            }
        ]),
    );
    let mut decision = sample_decision();
    decision.provider_type = Some("codex".to_string());
    decision.provider_api_format = Some("openai:responses:compact".to_string());
    decision.client_api_format = Some("openai:responses:compact".to_string());
    decision.mapped_model = Some("gpt-future-agent".to_string());
    decision.provider_request_body = Some(json!({
        "model": "gpt-future-agent",
        "input": [],
        "tools": []
    }));
    let transport = sample_codex_transport_with_card();

    apply_provider_request_routing_policy_to_decision(&input, &mut decision, Some(&transport))
        .expect("terminal Lite contract should accept the projected request");

    let body = decision.provider_request_body.as_ref().expect("body");
    assert_eq!(body["input"][0]["type"], "additional_tools");
    assert_eq!(body["input"][0]["tools"][0]["name"], "lookup");
    assert!(body["input"][0]["tools"][0].get("cache_control").is_none());
    assert_eq!(body["input"][1]["role"], "developer");
    assert_eq!(
        body["input"][1]["content"][0]["text"],
        "Routed instructions"
    );
    assert_eq!(body["input"][2]["role"], "user");
    assert_eq!(
        body["input"][2]["content"][0]["text"],
        "routed compact input"
    );
    assert!(body.get("tools").is_none());
    assert!(body.get("instructions").is_none());
    assert_eq!(body["parallel_tool_calls"], false);
    assert_eq!(body["reasoning"]["effort"], "high");
    assert_eq!(body["reasoning"]["context"], "all_turns");
    assert_eq!(
        decision
            .provider_request_headers
            .get(CODEX_RESPONSES_LITE_HEADER)
            .map(String::as_str),
        Some("true")
    );
}

#[test]
fn provider_request_routing_policy_rejects_body_patch_without_json_body() {
    let input = sample_decision_input();
    let mut decision = sample_decision();
    decision.provider_request_body = None;
    decision.provider_request_body_base64 = Some("AA==".to_string());

    let error = apply_provider_request_routing_policy_to_decision(&input, &mut decision, None)
        .expect_err("provider body patch should reject binary upstream bodies");

    match error {
        GatewayError::Client { status, message } => {
            assert_eq!(status, StatusCode::BAD_REQUEST);
            assert!(message.contains("binary or empty upstream body"));
        }
        other => panic!("unexpected error: {other:?}"),
    }
    assert!(
        decision
            .report_context
            .as_ref()
            .and_then(|context| context.get("routing_trace"))
            .is_some(),
        "failed provider_request mutation should still seed routing trace"
    );
}

#[test]
fn provider_request_routing_policy_allows_header_patch_without_json_body() {
    let mut input = sample_decision_input();
    set_provider_request_rules(
        &mut input,
        &["gpt-5"],
        json!([{
            "type": "patch_headers",
            "patch": [{
                "op": "set",
                "name": "x-provider-route",
                "value": "header-only"
            }]
        }]),
    );
    let mut decision = sample_decision();
    decision.provider_request_body = None;
    decision.provider_request_body_base64 = Some("AA==".to_string());

    apply_provider_request_routing_policy_to_decision(&input, &mut decision, None)
        .expect("header-only provider routing mutation should apply without JSON body");

    assert_eq!(decision.provider_request_body, None);
    assert_eq!(
        decision
            .provider_request_headers
            .get("x-provider-route")
            .map(String::as_str),
        Some("header-only")
    );
    assert_eq!(
        decision.report_context.as_ref().unwrap()["routing_trace"]
            ["provider_request_patch_summary"]["header_names"],
        json!(["x-provider-route"])
    );
}

#[test]
fn provider_request_routing_trace_records_pool_expansion_candidate() {
    let input = sample_decision_input();
    let mut decision = sample_decision();
    decision.report_context = Some(json!({
        "candidate_index": 2,
        "retry_index": 2,
        "model_id": "model-1",
        "candidate_group_id": "pool-group-1",
        "pool_key_index": 1,
        "provider_priority": 7,
        "priority_slot": 3
    }));

    apply_provider_request_routing_policy_to_decision(&input, &mut decision, None)
        .expect("provider routing mutation should seed pool trace");

    let routing_trace = &decision.report_context.as_ref().unwrap()["routing_trace"];
    assert_eq!(
        routing_trace["global_candidates"][0]["candidate_kind"],
        json!("pool_group")
    );
    assert_eq!(
        routing_trace["global_candidates"][0]["provider_id"],
        json!("pool-group-1")
    );
    assert_eq!(routing_trace["global_candidates"][0]["key_id"], Value::Null);
    assert_eq!(
        routing_trace["pool_expansion"][0]["pool_group_id"],
        json!("pool-group-1")
    );
    assert_eq!(routing_trace["pool_expansion"][0]["key_id"], json!("key-1"));
    assert_eq!(
        routing_trace["pool_expansion"][0]["selected_order"],
        json!(1)
    );
}
