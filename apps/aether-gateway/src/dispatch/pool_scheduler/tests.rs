use super::{
    admin_provider_pool_quota_probe_active_members_key, apply_local_execution_pool_scheduler,
    apply_local_execution_pool_scheduler_with_runtime_map,
    apply_local_execution_pool_scheduler_with_runtime_map_outcome,
    apply_local_execution_pool_scheduler_with_runtime_map_outcome_and_configs,
    build_pool_catalog_key_context, effective_pool_config_for_group, pool_config_for_candidate,
    pool_key_candidate_order_for_group, pool_key_requires_reauth_for_scheduling,
    prune_unschedulable_active_probe_members_for_request, remove_active_probe_members_for_request,
    should_trigger_active_probe_burst_for_request, PoolCatalogKeyContext, PoolKeyCursor,
    POOL_ACTIVE_PROBE_SEALED_SKIP_REASON, ROUTING_PROFILE_DISALLOWED_KEY_SKIP_REASON,
};
use crate::ai_serving::{
    apply_local_runtime_candidate_terminal_reason, provider_key_pool_score_id,
    provider_key_pool_score_scope, EligibleLocalExecutionCandidate, LocalExecutionCandidateKind,
    PlannerAppState,
};
use crate::data::GatewayDataState;
use crate::handlers::shared::provider_pool::{
    admin_provider_pool_cache_affinity_enabled, admin_provider_pool_config_from_config_value,
    read_admin_provider_pool_runtime_state, read_provider_pool_scheduling_runtime_state,
    record_admin_provider_pool_error, record_admin_provider_pool_success,
    AdminProviderPoolRuntimeState,
};
use crate::orchestration::LocalExecutionCandidateMetadata;
use crate::{AppState, LocalExecutionRuntimeMissDiagnostic};
use aether_data::repository::candidate_selection::InMemoryMinimalCandidateSelectionReadRepository;
use aether_data::repository::pool_scores::InMemoryPoolMemberScoreRepository;
use aether_data::repository::provider_catalog::InMemoryProviderCatalogReadRepository;
use aether_data_contracts::repository::candidate_selection::{
    StoredMinimalCandidateSelectionRow, StoredPoolKeyCandidateOrder,
};
use aether_data_contracts::repository::pool_scores::{
    PoolMemberHardState, PoolMemberIdentity, PoolMemberProbeStatus, StoredPoolMemberScore,
};
use aether_data_contracts::repository::provider_catalog::{
    StoredProviderCatalogEndpoint, StoredProviderCatalogKey, StoredProviderCatalogProvider,
};
use aether_pool_core::POOL_ACCOUNT_EXHAUSTED_SKIP_REASON;
use aether_provider_pool::ProviderPoolService;
use aether_provider_transport::snapshot::{
    GatewayProviderTransportEndpoint, GatewayProviderTransportKey, GatewayProviderTransportProvider,
};
use aether_routing_core::{
    RankingOverlay, ResolvedRoutingPolicy, RoutingPoolPolicyOverride, RoutingSchedulingMode,
    RoutingSchedulingPreset, RoutingSetPriorityMode,
};
use aether_scheduler_core::SchedulerMinimalCandidateSelectionCandidate;
use serde_json::json;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::Arc;

#[tokio::test]
async fn scheduling_runtime_preserves_pool_ranking_and_cost_rejections() {
    let runtime = aether_runtime_state::RuntimeState::memory(
        aether_runtime_state::MemoryRuntimeStateConfig::default(),
    );
    let writer_config = admin_provider_pool_config_from_config_value(Some(&json!({
        "pool_advanced": {
            "cost_limit_per_key_tokens": 100,
            "scheduling_presets": [
                {"preset": "cache_affinity", "enabled": true},
                {"preset": "latency_first", "enabled": true}
            ]
        }
    })))
    .expect("writer pool config");
    for (key_id, cost, latency) in [("key-a", 100, 10), ("key-b", 20, 100)] {
        record_admin_provider_pool_success(
            &runtime,
            "provider-pool",
            key_id,
            &writer_config,
            Some(key_id),
            cost,
            Some(latency),
        )
        .await;
    }
    let key_ids = vec!["key-a".to_string(), "key-b".to_string()];
    for (preset, cost_limit) in [
        ("cache_affinity", None),
        ("priority_first", None),
        ("latency_first", None),
        ("cost_first", None),
        ("quota_balanced", None),
        ("latency_first", Some(100)),
    ] {
        let provider_config = json!({
            "pool_advanced": {
                "cost_limit_per_key_tokens": cost_limit,
                "scheduling_presets": [{"preset": preset, "enabled": true}]
            }
        });
        let pool_config = admin_provider_pool_config_from_config_value(Some(&provider_config))
            .expect("reader pool config");
        let admin = read_admin_provider_pool_runtime_state(
            &runtime,
            "provider-pool",
            &key_ids,
            &pool_config,
            Some("key-a"),
        )
        .await;
        let scheduling = read_provider_pool_scheduling_runtime_state(
            &runtime,
            "provider-pool",
            &key_ids,
            &pool_config,
            Some("key-a"),
        )
        .await;
        let run = |snapshot| {
            let candidates = key_ids
                .iter()
                .map(|key_id| {
                    sample_eligible_candidate(
                        "provider-pool",
                        "endpoint-1",
                        key_id,
                        10,
                        Some(provider_config.clone()),
                    )
                })
                .collect();
            let (scheduled, skipped) = apply_local_execution_pool_scheduler_with_runtime_map(
                candidates,
                &BTreeMap::from([("provider-pool".to_string(), snapshot)]),
                &BTreeMap::new(),
            );
            (
                scheduled
                    .into_iter()
                    .map(|item| item.candidate.key_id)
                    .collect::<Vec<_>>(),
                skipped
                    .into_iter()
                    .map(|item| (item.candidate.key_id, item.skip_reason))
                    .collect::<Vec<_>>(),
            )
        };
        let expected = run(admin);
        let actual = run(scheduling);
        assert_eq!(
            actual, expected,
            "preset: {preset}, cost limit: {cost_limit:?}"
        );
        if cost_limit.is_some() {
            assert_eq!(actual.0, vec!["key-b"]);
            assert_eq!(
                actual.1,
                vec![("key-a".to_string(), "pool_cost_limit_reached")]
            );
        }
    }
}

#[test]
fn pool_scheduler_groups_interleaved_candidates_and_reorders_internal_keys() {
    let pool_first = sample_eligible_candidate(
        "provider-pool",
        "endpoint-1",
        "key-pool-a",
        10,
        Some(json!({ "pool_advanced": { "lru_enabled": true } })),
    );
    let other = sample_eligible_candidate("provider-other", "endpoint-2", "key-other", 10, None);
    let pool_second = sample_eligible_candidate(
        "provider-pool",
        "endpoint-1",
        "key-pool-b",
        10,
        Some(json!({ "pool_advanced": { "lru_enabled": true } })),
    );

    let mut runtime_by_provider = BTreeMap::new();
    runtime_by_provider.insert(
        "provider-pool".to_string(),
        AdminProviderPoolRuntimeState {
            lru_score_by_key: BTreeMap::from([
                ("key-pool-a".to_string(), 20.0),
                ("key-pool-b".to_string(), 10.0),
            ]),
            ..AdminProviderPoolRuntimeState::default()
        },
    );

    let (reordered, skipped) = apply_local_execution_pool_scheduler_with_runtime_map(
        vec![pool_first, other, pool_second],
        &runtime_by_provider,
        &BTreeMap::new(),
    );

    assert!(skipped.is_empty());
    assert_eq!(
        reordered
            .iter()
            .map(|item| item.candidate.key_id.as_str())
            .collect::<Vec<_>>(),
        vec!["key-pool-b", "key-pool-a", "key-other"]
    );
}

#[test]
fn pool_scheduler_uses_catalog_last_used_when_runtime_lru_is_missing() {
    let recent_key = sample_eligible_candidate(
        "provider-pool",
        "endpoint-1",
        "key-recent",
        10,
        Some(json!({ "pool_advanced": { "lru_enabled": true } })),
    );
    let older_key = sample_eligible_candidate(
        "provider-pool",
        "endpoint-1",
        "key-older",
        10,
        Some(json!({ "pool_advanced": { "lru_enabled": true } })),
    );

    let key_context_by_id = BTreeMap::from([
        (
            "key-recent".to_string(),
            PoolCatalogKeyContext {
                catalog_lru_score: Some(200.0),
                ..PoolCatalogKeyContext::default()
            },
        ),
        (
            "key-older".to_string(),
            PoolCatalogKeyContext {
                catalog_lru_score: Some(100.0),
                ..PoolCatalogKeyContext::default()
            },
        ),
    ]);

    let (reordered, skipped) = apply_local_execution_pool_scheduler_with_runtime_map(
        vec![recent_key, older_key],
        &BTreeMap::new(),
        &key_context_by_id,
    );

    assert!(skipped.is_empty());
    assert_eq!(
        reordered
            .iter()
            .map(|item| item.candidate.key_id.as_str())
            .collect::<Vec<_>>(),
        vec!["key-older", "key-recent"]
    );
}

#[test]
fn pool_scheduler_allows_quota_exhausted_key_when_ignored() {
    let exhausted = sample_eligible_candidate(
        "provider-pool",
        "endpoint-1",
        "key-exhausted",
        10,
        Some(json!({ "pool_advanced": { "ignore_exhausted_accounts": true } })),
    );
    let contexts = BTreeMap::from([(
        "key-exhausted".to_string(),
        PoolCatalogKeyContext {
            quota_exhausted: true,
            ..PoolCatalogKeyContext::default()
        },
    )]);
    let (scheduled, skipped) = apply_local_execution_pool_scheduler_with_runtime_map(
        vec![exhausted],
        &BTreeMap::new(),
        &contexts,
    );
    assert_eq!(scheduled.len(), 1);
    assert!(skipped.is_empty());
}

#[test]
fn pool_scheduler_skips_quota_exhausted_key_when_flag_is_false() {
    let ready = sample_eligible_candidate(
        "provider-pool",
        "endpoint-1",
        "key-ready",
        10,
        Some(json!({ "pool_advanced": {} })),
    );
    let exhausted = sample_eligible_candidate(
        "provider-pool",
        "endpoint-1",
        "key-exhausted",
        10,
        Some(json!({ "pool_advanced": { "skip_exhausted_accounts": false } })),
    );
    let key_context_by_id = BTreeMap::from([
        ("key-ready".to_string(), PoolCatalogKeyContext::default()),
        (
            "key-exhausted".to_string(),
            PoolCatalogKeyContext {
                quota_exhausted: true,
                ..PoolCatalogKeyContext::default()
            },
        ),
    ]);

    let (scheduled, skipped) = apply_local_execution_pool_scheduler_with_runtime_map(
        vec![ready, exhausted],
        &BTreeMap::new(),
        &key_context_by_id,
    );

    assert_eq!(
        scheduled
            .iter()
            .map(|item| item.candidate.key_id.as_str())
            .collect::<Vec<_>>(),
        vec!["key-ready"]
    );
    assert_eq!(
        skipped
            .iter()
            .map(|item| (item.candidate.key_id.as_str(), item.skip_reason))
            .collect::<Vec<_>>(),
        vec![("key-exhausted", POOL_ACCOUNT_EXHAUSTED_SKIP_REASON)]
    );
}

#[test]
fn pool_scheduler_attaches_group_and_pool_metadata_to_ranked_candidates() {
    let pool_first = sample_eligible_candidate(
        "provider-pool",
        "endpoint-1",
        "key-pool-a",
        10,
        Some(json!({ "pool_advanced": { "lru_enabled": true } })),
    );
    let other = sample_eligible_candidate("provider-other", "endpoint-2", "key-other", 10, None);
    let pool_second = sample_eligible_candidate(
        "provider-pool",
        "endpoint-1",
        "key-pool-b",
        10,
        Some(json!({ "pool_advanced": { "lru_enabled": true } })),
    );

    let mut runtime_by_provider = BTreeMap::new();
    runtime_by_provider.insert(
        "provider-pool".to_string(),
        AdminProviderPoolRuntimeState {
            lru_score_by_key: BTreeMap::from([
                ("key-pool-a".to_string(), 20.0),
                ("key-pool-b".to_string(), 10.0),
            ]),
            ..AdminProviderPoolRuntimeState::default()
        },
    );

    let (reordered, skipped) = apply_local_execution_pool_scheduler_with_runtime_map(
        vec![pool_first, other, pool_second],
        &runtime_by_provider,
        &BTreeMap::new(),
    );

    assert!(skipped.is_empty());
    assert_eq!(reordered.len(), 3);
    assert_eq!(
        reordered[0].orchestration,
        LocalExecutionCandidateMetadata {
            candidate_group_id: Some(
                "provider=provider-pool|endpoint=endpoint-1|model=model-1|selected_model=gpt-5|api_format=openai:chat|singleton_key=*"
                    .to_string(),
            ),
            pool_key_index: Some(0),
            pool_key_lease: None,
            scheduler_affinity_epoch: None,
            sticky_key_attempts: None,
        }
    );
    assert_eq!(reordered[1].orchestration.pool_key_index, Some(1));
    assert_eq!(
        reordered[1].orchestration.candidate_group_id,
        reordered[0].orchestration.candidate_group_id
    );
    assert_eq!(
        reordered[2].orchestration,
        LocalExecutionCandidateMetadata {
            candidate_group_id: Some(
                "provider=provider-other|endpoint=endpoint-2|model=model-1|selected_model=gpt-5|api_format=openai:chat|singleton_key=key-other"
                    .to_string(),
            ),
            pool_key_index: None,
            pool_key_lease: None,
            scheduler_affinity_epoch: None,
            sticky_key_attempts: None,
        }
    );
}

#[test]
fn pool_scheduler_promotes_sticky_hit_before_lru_secondary_order() {
    let key_a = sample_eligible_candidate(
        "provider-pool",
        "endpoint-1",
        "key-a",
        10,
        Some(json!({
            "pool_advanced": {
                "scheduling_presets": [{
                    "preset": "cache_affinity",
                    "enabled": true,
                    "mode": "lru"
                }]
            }
        })),
    );
    let key_b = sample_eligible_candidate(
        "provider-pool",
        "endpoint-1",
        "key-b",
        10,
        Some(json!({
            "pool_advanced": {
                "scheduling_presets": [{
                    "preset": "cache_affinity",
                    "enabled": true,
                    "mode": "lru"
                }]
            }
        })),
    );

    let mut runtime_by_provider = BTreeMap::new();
    runtime_by_provider.insert(
        "provider-pool".to_string(),
        AdminProviderPoolRuntimeState {
            sticky_bound_key_id: Some("key-a".to_string()),
            lru_score_by_key: BTreeMap::from([
                ("key-a".to_string(), 50.0),
                ("key-b".to_string(), 10.0),
            ]),
            ..AdminProviderPoolRuntimeState::default()
        },
    );

    let (reordered, skipped) = apply_local_execution_pool_scheduler_with_runtime_map(
        vec![key_a, key_b],
        &runtime_by_provider,
        &BTreeMap::new(),
    );

    assert!(skipped.is_empty());
    assert_eq!(
        reordered
            .iter()
            .map(|item| item.candidate.key_id.as_str())
            .collect::<Vec<_>>(),
        vec!["key-a", "key-b"]
    );
}

#[test]
fn cache_affinity_secondary_modes_select_distinct_candidate_orders() {
    for (mode, expected) in [
        ("single_account", StoredPoolKeyCandidateOrder::SingleAccount),
        ("lru", StoredPoolKeyCandidateOrder::Lru),
    ] {
        let group = sample_eligible_candidate(
            "provider-pool",
            "endpoint-1",
            "key-a",
            10,
            Some(json!({
                "pool_advanced": {
                    "scheduling_presets": [{
                        "preset": "cache_affinity",
                        "enabled": true,
                        "mode": mode
                    }]
                }
            })),
        );
        let config = pool_config_for_candidate(&group).expect("pool config should parse");

        assert!(admin_provider_pool_cache_affinity_enabled(&config));
        assert_eq!(
            pool_key_candidate_order_for_group(&group, Some(&config)),
            expected
        );
    }
}

#[test]
fn pool_scheduler_ignores_sticky_hit_without_cache_affinity() {
    let key_a = sample_eligible_candidate(
        "provider-pool",
        "endpoint-1",
        "key-a",
        10,
        Some(json!({
            "pool_advanced": {
                "scheduling_presets": [{"preset": "quota_balanced", "enabled": true}]
            }
        })),
    );
    let key_b = sample_eligible_candidate(
        "provider-pool",
        "endpoint-1",
        "key-b",
        10,
        Some(json!({
            "pool_advanced": {
                "scheduling_presets": [{"preset": "quota_balanced", "enabled": true}]
            }
        })),
    );

    let runtime_by_provider = BTreeMap::from([(
        "provider-pool".to_string(),
        AdminProviderPoolRuntimeState {
            sticky_bound_key_id: Some("key-a".to_string()),
            ..AdminProviderPoolRuntimeState::default()
        },
    )]);

    let (reordered, skipped) = apply_local_execution_pool_scheduler_with_runtime_map(
        vec![key_b, key_a],
        &runtime_by_provider,
        &BTreeMap::new(),
    );

    assert!(skipped.is_empty());
    assert_eq!(
        reordered
            .iter()
            .map(|item| item.candidate.key_id.as_str())
            .collect::<Vec<_>>(),
        vec!["key-b", "key-a"]
    );
}

#[test]
fn pool_scheduler_skips_cooldown_and_cost_exhausted_keys() {
    let key_ready = sample_eligible_candidate(
        "provider-pool",
        "endpoint-1",
        "key-ready",
        10,
        Some(json!({
            "pool_advanced": {
                "cost_limit_per_key_tokens": 100
            }
        })),
    );
    let key_cooldown = sample_eligible_candidate(
        "provider-pool",
        "endpoint-1",
        "key-cooldown",
        10,
        Some(json!({
            "pool_advanced": {
                "cost_limit_per_key_tokens": 100
            }
        })),
    );
    let key_cost = sample_eligible_candidate(
        "provider-pool",
        "endpoint-1",
        "key-cost",
        10,
        Some(json!({
            "pool_advanced": {
                "cost_limit_per_key_tokens": 100
            }
        })),
    );

    let mut runtime_by_provider = BTreeMap::new();
    runtime_by_provider.insert(
        "provider-pool".to_string(),
        AdminProviderPoolRuntimeState {
            cooldown_reason_by_key: BTreeMap::from([(
                "key-cooldown".to_string(),
                "429".to_string(),
            )]),
            cost_window_usage_by_key: BTreeMap::from([("key-cost".to_string(), 100)]),
            ..AdminProviderPoolRuntimeState::default()
        },
    );

    let (reordered, skipped) = apply_local_execution_pool_scheduler_with_runtime_map(
        vec![key_ready, key_cooldown, key_cost],
        &runtime_by_provider,
        &BTreeMap::new(),
    );

    assert_eq!(
        reordered
            .iter()
            .map(|item| item.candidate.key_id.as_str())
            .collect::<Vec<_>>(),
        vec!["key-ready"]
    );
    assert_eq!(
        skipped
            .iter()
            .map(|item| (item.candidate.key_id.as_str(), item.skip_reason))
            .collect::<Vec<_>>(),
        vec![
            ("key-cooldown", "pool_cooldown"),
            ("key-cost", "pool_cost_limit_reached"),
        ]
    );
}

#[test]
fn pool_scheduler_uses_only_active_probe_members_when_active_probe_enabled() {
    let provider_config = Some(json!({
        "pool_advanced": {
            "probing_enabled": true
        }
    }));
    let key_active = sample_eligible_candidate(
        "provider-pool",
        "endpoint-1",
        "key-active",
        10,
        provider_config.clone(),
    );
    let key_sealed = sample_eligible_candidate(
        "provider-pool",
        "endpoint-1",
        "key-sealed",
        10,
        provider_config,
    );

    let runtime_by_provider = BTreeMap::from([(
        "provider-pool".to_string(),
        AdminProviderPoolRuntimeState {
            active_probe_member_ids: BTreeSet::from(["key-active".to_string()]),
            ..AdminProviderPoolRuntimeState::default()
        },
    )]);

    let (scheduled, skipped) = apply_local_execution_pool_scheduler_with_runtime_map(
        vec![key_sealed, key_active],
        &runtime_by_provider,
        &BTreeMap::new(),
    );

    assert_eq!(
        scheduled
            .iter()
            .map(|item| item.candidate.key_id.as_str())
            .collect::<Vec<_>>(),
        vec!["key-active"]
    );
    assert_eq!(
        skipped
            .iter()
            .map(|item| (item.candidate.key_id.as_str(), item.skip_reason))
            .collect::<Vec<_>>(),
        vec![("key-sealed", POOL_ACTIVE_PROBE_SEALED_SKIP_REASON)]
    );
}

#[test]
fn pool_scheduler_falls_back_when_active_probe_members_are_unschedulable() {
    let provider_config = Some(json!({
        "pool_advanced": {
            "probing_enabled": true
        }
    }));
    let key_hot = sample_eligible_candidate(
        "provider-pool",
        "endpoint-1",
        "key-hot",
        10,
        provider_config.clone(),
    );
    let key_cold = sample_eligible_candidate(
        "provider-pool",
        "endpoint-1",
        "key-cold",
        10,
        provider_config,
    );

    let runtime_by_provider = BTreeMap::from([(
        "provider-pool".to_string(),
        AdminProviderPoolRuntimeState {
            active_probe_member_ids: BTreeSet::from(["key-hot".to_string()]),
            cooldown_reason_by_key: BTreeMap::from([("key-hot".to_string(), "429".to_string())]),
            provider_desired_hot: 1,
            ..AdminProviderPoolRuntimeState::default()
        },
    )]);

    let outcome = apply_local_execution_pool_scheduler_with_runtime_map_outcome(
        vec![key_hot, key_cold],
        &runtime_by_provider,
        &BTreeMap::new(),
    );
    let scheduled = outcome.candidates;
    let skipped = outcome.skipped;

    assert_eq!(
        scheduled
            .iter()
            .map(|item| item.candidate.key_id.as_str())
            .collect::<Vec<_>>(),
        vec!["key-cold"]
    );
    assert_eq!(
        skipped
            .iter()
            .map(|item| (item.candidate.key_id.as_str(), item.skip_reason))
            .collect::<Vec<_>>(),
        vec![("key-hot", "pool_cooldown")]
    );
    assert_eq!(
        outcome
            .active_probe_evicted_members_by_provider
            .get("provider-pool"),
        Some(&BTreeSet::from(["key-hot".to_string()]))
    );
}

#[test]
fn pool_scheduler_prunes_cold_active_probe_members_before_scheduling() {
    let provider_config = Some(json!({
        "pool_advanced": {
            "probing_enabled": true
        }
    }));
    let key_hot = sample_eligible_candidate(
        "provider-pool",
        "endpoint-1",
        "key-hot",
        10,
        provider_config,
    );
    let mut runtime_by_provider = BTreeMap::from([(
        "provider-pool".to_string(),
        AdminProviderPoolRuntimeState {
            active_probe_member_ids: BTreeSet::from(["key-hot".to_string()]),
            cooldown_reason_by_key: BTreeMap::from([("key-hot".to_string(), "429".to_string())]),
            provider_desired_hot: 1,
            ..AdminProviderPoolRuntimeState::default()
        },
    )]);

    let evicted = prune_unschedulable_active_probe_members_for_request(
        &mut runtime_by_provider,
        &[key_hot],
        &BTreeMap::new(),
    );

    assert_eq!(
        evicted.get("provider-pool"),
        Some(&BTreeSet::from(["key-hot".to_string()]))
    );
    assert!(runtime_by_provider
        .get("provider-pool")
        .expect("runtime should exist")
        .active_probe_member_ids
        .is_empty());
}

#[tokio::test]
async fn pool_scheduler_removes_unschedulable_member_from_active_probe_set() {
    let app = AppState::new().expect("state should build");
    let set_key = admin_provider_pool_quota_probe_active_members_key("provider-pool");
    app.runtime_state
        .set_add(&set_key, "key-hot")
        .await
        .expect("active member should insert");

    remove_active_probe_members_for_request(
        PlannerAppState::new(&app),
        &BTreeMap::from([(
            "provider-pool".to_string(),
            BTreeSet::from(["key-hot".to_string()]),
        )]),
    )
    .await;

    let members = app
        .runtime_state
        .set_members(&set_key)
        .await
        .expect("active members should read");
    assert!(members.is_empty());
}

#[test]
fn pool_scheduler_allows_cold_start_when_active_probe_pool_is_empty() {
    let provider_config = Some(json!({
        "pool_advanced": {
            "probing_enabled": true
        }
    }));
    let key_a = sample_eligible_candidate(
        "provider-pool",
        "endpoint-1",
        "key-a",
        10,
        provider_config.clone(),
    );
    let key_b =
        sample_eligible_candidate("provider-pool", "endpoint-1", "key-b", 10, provider_config);

    let runtime_by_provider = BTreeMap::from([(
        "provider-pool".to_string(),
        AdminProviderPoolRuntimeState::default(),
    )]);

    let (scheduled, skipped) = apply_local_execution_pool_scheduler_with_runtime_map(
        vec![key_a, key_b],
        &runtime_by_provider,
        &BTreeMap::new(),
    );

    assert_eq!(
        scheduled
            .iter()
            .map(|item| item.candidate.key_id.as_str())
            .collect::<Vec<_>>(),
        vec!["key-a", "key-b"]
    );
    assert_eq!(
        skipped
            .iter()
            .map(|item| (item.candidate.key_id.as_str(), item.skip_reason))
            .collect::<Vec<_>>(),
        Vec::<(&str, &str)>::new()
    );
}

#[test]
fn pool_scheduler_triggers_burst_when_auto_hot_target_has_gap() {
    let provider_config = Some(json!({
        "pool_advanced": {
            "probing_enabled": true
        }
    }));
    let candidate =
        sample_eligible_candidate("provider-pool", "endpoint-1", "key-a", 10, provider_config);
    let pool_config = pool_config_for_candidate(&candidate).expect("pool config should parse");
    let runtime = AdminProviderPoolRuntimeState {
        active_probe_member_ids: BTreeSet::from(["key-a".to_string()]),
        provider_desired_hot: 3,
        ..AdminProviderPoolRuntimeState::default()
    };

    assert!(should_trigger_active_probe_burst_for_request(
        &pool_config,
        &runtime
    ));
}

#[test]
fn pool_scheduler_ignores_legacy_threshold_fields_for_burst_target() {
    let provider_config = Some(json!({
        "pool_advanced": {
            "probing_enabled": true,
            "probing_target_percent": 60,
            "probing_target_count": 10
        }
    }));
    let candidate =
        sample_eligible_candidate("provider-pool", "endpoint-1", "key-a", 10, provider_config);
    let pool_config = pool_config_for_candidate(&candidate).expect("pool config should parse");
    let runtime = AdminProviderPoolRuntimeState {
        active_probe_member_ids: BTreeSet::from(["key-a".to_string(), "key-b".to_string()]),
        provider_desired_hot: 2,
        ..AdminProviderPoolRuntimeState::default()
    };

    assert!(!should_trigger_active_probe_burst_for_request(
        &pool_config,
        &runtime
    ));
}

#[test]
fn pool_scheduler_skips_burst_when_active_probe_target_is_met() {
    let provider_config = Some(json!({
        "pool_advanced": {
            "probing_enabled": true,
            "probing_target_count": 1
        }
    }));
    let candidate =
        sample_eligible_candidate("provider-pool", "endpoint-1", "key-a", 10, provider_config);
    let pool_config = pool_config_for_candidate(&candidate).expect("pool config should parse");
    let runtime = AdminProviderPoolRuntimeState {
        active_probe_member_ids: BTreeSet::from(["key-a".to_string()]),
        provider_desired_hot: 1,
        ..AdminProviderPoolRuntimeState::default()
    };

    assert!(!should_trigger_active_probe_burst_for_request(
        &pool_config,
        &runtime
    ));
}

#[test]
fn pool_scheduler_applies_distribution_mode_before_strategy_presets() {
    let key_a = sample_eligible_candidate(
        "provider-pool",
        "endpoint-1",
        "key-a",
        50,
        Some(json!({
            "pool_advanced": {
                "scheduling_presets": [
                    {"preset": "cache_affinity", "enabled": true},
                    {"preset": "priority_first", "enabled": true}
                ]
            }
        })),
    );
    let key_b = sample_eligible_candidate(
        "provider-pool",
        "endpoint-1",
        "key-b",
        10,
        Some(json!({
            "pool_advanced": {
                "scheduling_presets": [
                    {"preset": "cache_affinity", "enabled": true},
                    {"preset": "priority_first", "enabled": true}
                ]
            }
        })),
    );

    let mut runtime_by_provider = BTreeMap::new();
    runtime_by_provider.insert(
        "provider-pool".to_string(),
        AdminProviderPoolRuntimeState {
            lru_score_by_key: BTreeMap::from([
                ("key-a".to_string(), 100.0),
                ("key-b".to_string(), 5.0),
            ]),
            ..AdminProviderPoolRuntimeState::default()
        },
    );

    let (reordered, skipped) = apply_local_execution_pool_scheduler_with_runtime_map(
        vec![key_a, key_b],
        &runtime_by_provider,
        &BTreeMap::new(),
    );

    assert!(skipped.is_empty());
    assert_eq!(
        reordered
            .iter()
            .map(|item| item.candidate.key_id.as_str())
            .collect::<Vec<_>>(),
        vec!["key-a", "key-b"]
    );
}

#[test]
fn pool_scheduler_uses_plan_preset_with_catalog_context() {
    let key_free = sample_eligible_candidate(
        "provider-pool",
        "endpoint-1",
        "key-free",
        10,
        Some(json!({
            "pool_advanced": {
                "scheduling_presets": [{"preset": "plus_first", "enabled": true}]
            }
        })),
    );
    let key_plus = sample_eligible_candidate(
        "provider-pool",
        "endpoint-1",
        "key-plus",
        10,
        Some(json!({
            "pool_advanced": {
                "scheduling_presets": [{"preset": "plus_first", "enabled": true}]
            }
        })),
    );

    let key_context_by_id = BTreeMap::from([
        (
            "key-free".to_string(),
            PoolCatalogKeyContext {
                plan_tier: Some("free".to_string()),
                ..PoolCatalogKeyContext::default()
            },
        ),
        (
            "key-plus".to_string(),
            PoolCatalogKeyContext {
                plan_tier: Some("plus".to_string()),
                ..PoolCatalogKeyContext::default()
            },
        ),
    ]);

    let (reordered, skipped) = apply_local_execution_pool_scheduler_with_runtime_map(
        vec![key_free, key_plus],
        &BTreeMap::new(),
        &key_context_by_id,
    );

    assert!(skipped.is_empty());
    assert_eq!(
        reordered
            .iter()
            .map(|item| item.candidate.key_id.as_str())
            .collect::<Vec<_>>(),
        vec!["key-plus", "key-free"]
    );
}

#[test]
fn pool_scheduler_plus_first_treats_plus_and_pro_as_top_tier() {
    let key_plus = sample_eligible_candidate(
        "provider-pool",
        "endpoint-1",
        "key-plus",
        10,
        Some(json!({
            "pool_advanced": {
                "scheduling_presets": [{"preset": "plus_first", "enabled": true}]
            }
        })),
    );
    let key_pro = sample_eligible_candidate(
        "provider-pool",
        "endpoint-1",
        "key-pro",
        10,
        Some(json!({
            "pool_advanced": {
                "scheduling_presets": [{"preset": "plus_first", "enabled": true}]
            }
        })),
    );
    let key_team = sample_eligible_candidate(
        "provider-pool",
        "endpoint-1",
        "key-team",
        10,
        Some(json!({
            "pool_advanced": {
                "scheduling_presets": [{"preset": "plus_first", "enabled": true}]
            }
        })),
    );

    let key_context_by_id = BTreeMap::from([
        (
            "key-plus".to_string(),
            PoolCatalogKeyContext {
                plan_tier: Some("plus".to_string()),
                catalog_lru_score: Some(300.0),
                ..PoolCatalogKeyContext::default()
            },
        ),
        (
            "key-pro".to_string(),
            PoolCatalogKeyContext {
                plan_tier: Some("pro".to_string()),
                catalog_lru_score: Some(100.0),
                ..PoolCatalogKeyContext::default()
            },
        ),
        (
            "key-team".to_string(),
            PoolCatalogKeyContext {
                plan_tier: Some("team".to_string()),
                catalog_lru_score: Some(50.0),
                ..PoolCatalogKeyContext::default()
            },
        ),
    ]);

    let (reordered, skipped) = apply_local_execution_pool_scheduler_with_runtime_map(
        vec![key_plus, key_pro, key_team],
        &BTreeMap::new(),
        &key_context_by_id,
    );

    assert!(skipped.is_empty());
    assert_eq!(
        reordered
            .iter()
            .map(|item| item.candidate.key_id.as_str())
            .collect::<Vec<_>>(),
        vec!["key-pro", "key-plus", "key-team"]
    );
}

#[test]
fn pool_scheduler_supports_pro_first_plan_preset() {
    let key_plus = sample_eligible_candidate(
        "provider-pool",
        "endpoint-1",
        "key-plus",
        10,
        Some(json!({
            "pool_advanced": {
                "scheduling_presets": [{"preset": "pro_first", "enabled": true}]
            }
        })),
    );
    let key_pro = sample_eligible_candidate(
        "provider-pool",
        "endpoint-1",
        "key-pro",
        10,
        Some(json!({
            "pool_advanced": {
                "scheduling_presets": [{"preset": "pro_first", "enabled": true}]
            }
        })),
    );
    let key_team = sample_eligible_candidate(
        "provider-pool",
        "endpoint-1",
        "key-team",
        10,
        Some(json!({
            "pool_advanced": {
                "scheduling_presets": [{"preset": "pro_first", "enabled": true}]
            }
        })),
    );

    let key_context_by_id = BTreeMap::from([
        (
            "key-plus".to_string(),
            PoolCatalogKeyContext {
                plan_tier: Some("plus".to_string()),
                ..PoolCatalogKeyContext::default()
            },
        ),
        (
            "key-pro".to_string(),
            PoolCatalogKeyContext {
                plan_tier: Some("pro".to_string()),
                ..PoolCatalogKeyContext::default()
            },
        ),
        (
            "key-team".to_string(),
            PoolCatalogKeyContext {
                plan_tier: Some("team".to_string()),
                ..PoolCatalogKeyContext::default()
            },
        ),
    ]);

    let (reordered, skipped) = apply_local_execution_pool_scheduler_with_runtime_map(
        vec![key_plus, key_team, key_pro],
        &BTreeMap::new(),
        &key_context_by_id,
    );

    assert!(skipped.is_empty());
    assert_eq!(
        reordered
            .iter()
            .map(|item| item.candidate.key_id.as_str())
            .collect::<Vec<_>>(),
        vec!["key-pro", "key-plus", "key-team"]
    );
}

#[test]
fn pool_scheduler_defaults_empty_pool_advanced_to_cache_affinity() {
    let key_a = sample_eligible_candidate(
        "provider-pool",
        "endpoint-1",
        "key-a",
        10,
        Some(json!({ "pool_advanced": {} })),
    );
    let key_b = sample_eligible_candidate(
        "provider-pool",
        "endpoint-1",
        "key-b",
        10,
        Some(json!({ "pool_advanced": {} })),
    );

    let runtime_by_provider = BTreeMap::from([(
        "provider-pool".to_string(),
        AdminProviderPoolRuntimeState {
            lru_score_by_key: BTreeMap::from([
                ("key-a".to_string(), 10.0),
                ("key-b".to_string(), 200.0),
            ]),
            ..AdminProviderPoolRuntimeState::default()
        },
    )]);

    let (reordered, skipped) = apply_local_execution_pool_scheduler_with_runtime_map(
        vec![key_a, key_b],
        &runtime_by_provider,
        &BTreeMap::new(),
    );

    assert!(skipped.is_empty());
    assert_eq!(
        reordered
            .iter()
            .map(|item| item.candidate.key_id.as_str())
            .collect::<Vec<_>>(),
        vec!["key-b", "key-a"]
    );
}

#[test]
fn pool_key_cursor_uses_distribution_order_for_page_queries() {
    let app = AppState::new().expect("state should build");
    let load_balance_group = sample_eligible_candidate(
        "provider-pool",
        "endpoint-1",
        "pool-group",
        10,
        Some(json!({
            "pool_advanced": {
                "scheduling_presets": [
                    {"preset": "load_balance", "enabled": true},
                    {"preset": "priority_first", "enabled": true}
                ]
            }
        })),
    );
    let load_balance_cursor = PoolKeyCursor::new(
        PlannerAppState::new(&app),
        load_balance_group,
        None,
        None,
        None,
    );
    assert!(matches!(
        load_balance_cursor.pool_key_order,
        StoredPoolKeyCandidateOrder::LoadBalance { ref seed } if !seed.is_empty()
    ));

    let lru_group = sample_eligible_candidate(
        "provider-pool",
        "endpoint-1",
        "pool-group",
        10,
        Some(json!({ "pool_advanced": { "lru_enabled": true } })),
    );
    let lru_cursor = PoolKeyCursor::new(PlannerAppState::new(&app), lru_group, None, None, None);
    assert_eq!(lru_cursor.pool_key_order, StoredPoolKeyCandidateOrder::Lru);
}

#[test]
fn routing_pool_override_is_effective_for_page_scheduling_and_sticky_mode() {
    let base_config = Some(json!({
        "pool_advanced": {
            "scheduling_presets": [{"preset": "plus_first", "enabled": true}]
        }
    }));
    let key_plus = sample_eligible_candidate(
        "provider-pool",
        "endpoint-1",
        "key-plus",
        10,
        base_config.clone(),
    );
    let key_pro =
        sample_eligible_candidate("provider-pool", "endpoint-1", "key-pro", 10, base_config);
    let mut routing_policy = routing_policy_with_allowed_keys([]);
    routing_policy.pool_policy_overrides.insert(
        "provider-pool".to_string(),
        RoutingPoolPolicyOverride {
            scheduling_presets: vec![RoutingSchedulingPreset {
                preset: "pro_first".to_string(),
                enabled: true,
                mode: Some("pro_only".to_string()),
            }],
        },
    );
    let effective_config = effective_pool_config_for_group(&key_plus, Some(&routing_policy))
        .expect("effective pool config should parse");
    let effective_configs = BTreeMap::from([("provider-pool".to_string(), effective_config)]);
    let key_context_by_id = BTreeMap::from([
        (
            "key-plus".to_string(),
            PoolCatalogKeyContext {
                plan_tier: Some("plus".to_string()),
                ..PoolCatalogKeyContext::default()
            },
        ),
        (
            "key-pro".to_string(),
            PoolCatalogKeyContext {
                plan_tier: Some("pro".to_string()),
                ..PoolCatalogKeyContext::default()
            },
        ),
    ]);

    let outcome = apply_local_execution_pool_scheduler_with_runtime_map_outcome_and_configs(
        vec![key_plus, key_pro],
        &BTreeMap::new(),
        &key_context_by_id,
        &effective_configs,
    );

    assert!(outcome.skipped.is_empty());
    assert_eq!(
        outcome
            .candidates
            .iter()
            .map(|candidate| candidate.candidate.key_id.as_str())
            .collect::<Vec<_>>(),
        ["key-pro", "key-plus"]
    );

    let app = AppState::new().expect("state should build");
    let cache_affinity_group = sample_eligible_candidate(
        "provider-pool",
        "endpoint-1",
        "pool-group",
        10,
        Some(json!({
            "pool_advanced": {
                "scheduling_presets": [{"preset": "load_balance", "enabled": true}]
            }
        })),
    );
    routing_policy.pool_policy_overrides.insert(
        "provider-pool".to_string(),
        RoutingPoolPolicyOverride {
            scheduling_presets: vec![RoutingSchedulingPreset {
                preset: "cache_affinity".to_string(),
                enabled: true,
                mode: None,
            }],
        },
    );
    let cursor = PoolKeyCursor::new_with_routing_policy(
        PlannerAppState::new(&app),
        cache_affinity_group,
        None,
        None,
        None,
        Some(&routing_policy),
    );
    assert!(admin_provider_pool_cache_affinity_enabled(
        cursor
            .effective_pool_config
            .as_ref()
            .expect("effective config should exist")
    ));
}

#[test]
fn routing_pool_lru_override_recomputes_derived_lru_state() {
    let group = sample_eligible_candidate(
        "provider-pool",
        "endpoint-1",
        "pool-group",
        10,
        Some(json!({
            "pool_advanced": {
                "scheduling_presets": [
                    {"preset": "cache_affinity", "enabled": true}
                ]
            }
        })),
    );
    let mut routing_policy = routing_policy_with_allowed_keys([]);
    routing_policy.pool_policy_overrides.insert(
        "provider-pool".to_string(),
        RoutingPoolPolicyOverride {
            scheduling_presets: vec![RoutingSchedulingPreset {
                preset: "lru".to_string(),
                enabled: true,
                mode: None,
            }],
        },
    );

    let effective_config = effective_pool_config_for_group(&group, Some(&routing_policy))
        .expect("effective pool config should parse");

    assert!(effective_config.lru_enabled);
    assert_eq!(
        pool_key_candidate_order_for_group(&group, Some(&effective_config)),
        StoredPoolKeyCandidateOrder::Lru
    );
}

#[test]
fn pool_key_cursor_records_runtime_miss_when_exhausted_without_returning_key() {
    let app = AppState::new().expect("state should build");
    let trace_id = "trace-pool-exhausted-runtime-miss";
    app.set_local_execution_runtime_miss_diagnostic(
        trace_id,
        LocalExecutionRuntimeMissDiagnostic {
            reason: "candidate_evaluation_incomplete".to_string(),
            requested_model: Some("gpt-5".to_string()),
            candidate_count: Some(1),
            ..LocalExecutionRuntimeMissDiagnostic::default()
        },
    );
    let group = sample_eligible_candidate(
        "provider-pool",
        "endpoint-1",
        "pool-group",
        10,
        Some(json!({ "pool_advanced": { "lru_enabled": true } })),
    );
    let mut cursor = PoolKeyCursor::new(PlannerAppState::new(&app), group, None, None, None)
        .with_runtime_miss_diagnostic(trace_id, true);
    cursor.record_skip_reason("pool_cooldown");
    cursor.record_skip_reason("pool_cooldown");
    cursor.record_skip_reason("transport_snapshot_missing");

    cursor.log_exhausted();
    apply_local_runtime_candidate_terminal_reason(&app, trace_id, "no_local_sync_plans");

    let diagnostic = app
        .take_local_execution_runtime_miss_diagnostic(trace_id)
        .expect("runtime miss diagnostic should exist");
    assert_eq!(diagnostic.reason, "all_candidates_skipped");
    assert_eq!(diagnostic.skipped_candidate_count, Some(2));
    assert_eq!(diagnostic.skip_reasons.get("pool_cooldown"), Some(&1));
    assert_eq!(
        diagnostic.skip_reasons.get("transport_snapshot_missing"),
        Some(&1)
    );
}

#[tokio::test]
async fn pool_key_cursor_rechecks_cooldown_for_frozen_window_candidates() {
    let app = AppState::new().expect("state should build");
    let provider_config = Some(json!({
        "pool_advanced": {
            "rate_limit_cooldown_seconds": 300
        }
    }));
    let group = sample_eligible_candidate(
        "provider-pool",
        "endpoint-1",
        "pool-group",
        10,
        provider_config.clone(),
    );
    let mut cursor =
        PoolKeyCursor::new(PlannerAppState::new(&app), group.clone(), None, None, None);
    cursor.queued_candidates = VecDeque::from([
        sample_eligible_candidate(
            "provider-pool",
            "endpoint-1",
            "key-a",
            10,
            provider_config.clone(),
        ),
        sample_eligible_candidate(
            "provider-pool",
            "endpoint-1",
            "key-b",
            10,
            provider_config.clone(),
        ),
        sample_eligible_candidate("provider-pool", "endpoint-1", "key-c", 10, provider_config),
    ]);

    let first = cursor.next_key().await.expect("first key should schedule");
    assert_eq!(first.candidate.key_id, "key-a");

    let pool_config = pool_config_for_candidate(&group).expect("pool config should parse");
    record_admin_provider_pool_error(
        app.runtime_state.as_ref(),
        "provider-pool",
        "key-b",
        &pool_config,
        429,
        None,
        None,
    )
    .await;

    let second = cursor
        .next_key()
        .await
        .expect("second key should skip the cooled-down frozen key");
    assert_eq!(second.candidate.key_id, "key-c");
    assert_eq!(second.orchestration.pool_key_index, Some(1));

    let skipped = cursor.take_skipped_candidates();
    assert_eq!(
        skipped
            .iter()
            .map(|item| (item.candidate.key_id.as_str(), item.skip_reason))
            .collect::<Vec<_>>(),
        vec![("key-b", "pool_cooldown")]
    );
}

#[tokio::test]
async fn pool_key_cursor_filters_expanded_keys_by_routing_profile_allowed_keys() {
    let app = AppState::new().expect("state should build");
    let provider_config = Some(json!({ "pool_advanced": { "lru_enabled": true } }));
    let group = sample_eligible_candidate(
        "provider-pool",
        "endpoint-1",
        "pool-group",
        10,
        provider_config.clone(),
    );
    let routing_policy = routing_policy_with_allowed_keys(["key-b"]);
    let mut cursor = PoolKeyCursor::new_with_routing_policy(
        PlannerAppState::new(&app),
        group,
        None,
        None,
        None,
        Some(&routing_policy),
    );
    cursor.queued_candidates = VecDeque::from([
        sample_eligible_candidate(
            "provider-pool",
            "endpoint-1",
            "key-a",
            10,
            provider_config.clone(),
        ),
        sample_eligible_candidate("provider-pool", "endpoint-1", "key-b", 10, provider_config),
    ]);

    let candidate = cursor
        .next_key()
        .await
        .expect("cursor should skip disallowed pool key and return allowed key");
    assert_eq!(candidate.candidate.key_id, "key-b");
    assert_eq!(candidate.orchestration.pool_key_index, Some(0));
    assert_eq!(
        candidate.orchestration.candidate_group_id.as_deref(),
        Some(
            "provider=provider-pool|endpoint=endpoint-1|model=model-1|selected_model=gpt-5|api_format=openai:chat|singleton_key=*"
        )
    );
    assert_eq!(
        cursor
            .skip_reason_counts
            .get(ROUTING_PROFILE_DISALLOWED_KEY_SKIP_REASON),
        Some(&1)
    );
    let skipped = cursor.take_skipped_candidates();
    assert_eq!(
        skipped
            .iter()
            .map(|item| (item.candidate.key_id.as_str(), item.skip_reason))
            .collect::<Vec<_>>(),
        vec![("key-a", ROUTING_PROFILE_DISALLOWED_KEY_SKIP_REASON)]
    );
}

#[tokio::test]
async fn routing_allowed_key_outside_score_top_n_is_materialized_directly() {
    let provider_config = Some(json!({
        "pool_advanced": {
            "score_top_n": 16,
            "score_fallback_scan_limit": 16
        }
    }));
    let (provider, endpoint, keys, rows) = large_pool_fixture(32, provider_config.clone());
    let scores = (0..16)
        .map(|index| {
            sample_provider_key_pool_score(
                "provider-pool",
                &format!("key-{index:05}"),
                1_000.0 - index as f64,
            )
        })
        .collect::<Vec<_>>();
    let data_state =
        GatewayDataState::with_provider_catalog_and_minimal_candidate_selection_for_tests(
            Arc::new(InMemoryProviderCatalogReadRepository::seed(
                vec![provider],
                vec![endpoint],
                keys,
            )),
            Arc::new(InMemoryMinimalCandidateSelectionReadRepository::seed(rows)),
        )
        .with_pool_score_repository_for_tests(Arc::new(InMemoryPoolMemberScoreRepository::seed(
            scores,
        )))
        .with_encryption_key_for_tests(aether_crypto::DEVELOPMENT_ENCRYPTION_KEY);
    let app = AppState::new()
        .expect("state should build")
        .with_data_state_for_tests(data_state);
    let group = sample_eligible_candidate(
        "provider-pool",
        "endpoint-1",
        "pool-group",
        10,
        provider_config,
    );
    let routing_policy = routing_policy_with_allowed_keys(["key-00031"]);
    let mut cursor = PoolKeyCursor::new_with_routing_policy(
        PlannerAppState::new(&app),
        group,
        None,
        None,
        None,
        Some(&routing_policy),
    );

    let candidate = cursor
        .next_key()
        .await
        .expect("the only routing-allowed key should not be lost to score preselection");

    assert_eq!(candidate.candidate.key_id, "key-00031");
    assert_eq!(cursor.scanned_keys, 1);
    assert_eq!(cursor.budget_scanned_keys, 1);
}

#[tokio::test]
async fn routing_allowed_keys_continue_across_pool_pages() {
    let provider_config = Some(json!({
        "pool_advanced": {
            "scheduling_presets": [
                {"preset": "lru", "enabled": false}
            ],
            "score_top_n": 16,
            "score_fallback_scan_limit": 128
        }
    }));
    let (provider, endpoint, keys, rows) = large_pool_fixture(80, provider_config.clone());
    let data_state =
        GatewayDataState::with_provider_catalog_and_minimal_candidate_selection_for_tests(
            Arc::new(InMemoryProviderCatalogReadRepository::seed(
                vec![provider],
                vec![endpoint],
                keys,
            )),
            Arc::new(InMemoryMinimalCandidateSelectionReadRepository::seed(rows)),
        )
        .with_encryption_key_for_tests(aether_crypto::DEVELOPMENT_ENCRYPTION_KEY);
    let app = AppState::new()
        .expect("state should build")
        .with_data_state_for_tests(data_state);
    let group = sample_eligible_candidate(
        "provider-pool",
        "endpoint-1",
        "pool-group",
        10,
        provider_config,
    );
    let mut routing_policy = routing_policy_with_allowed_keys([]);
    routing_policy.ranking_overlay.allowed_keys =
        (0..80).map(|index| format!("key-{index:05}")).collect();
    let mut cursor = PoolKeyCursor::new_with_routing_policy(
        PlannerAppState::new(&app),
        group,
        None,
        None,
        None,
        Some(&routing_policy),
    );

    let mut returned_key_ids = Vec::new();
    while let Some(candidate) = cursor.next_key().await {
        returned_key_ids.push(candidate.candidate.key_id);
    }

    assert_eq!(returned_key_ids.len(), 32);
    assert!(returned_key_ids.iter().any(|key_id| key_id == "key-00064"));
    assert!(returned_key_ids.iter().any(|key_id| key_id == "key-00079"));
    assert_eq!(cursor.scanned_keys, 80);
    assert_eq!(cursor.budget_scanned_keys, 80);
    assert_eq!(cursor.routing_allowed_key_offset, 80);
}

#[tokio::test]
async fn routing_allowed_keys_preserve_internal_priority_without_active_presets() {
    let provider_config = Some(json!({
        "pool_advanced": {
            "scheduling_presets": [
                {"preset": "lru", "enabled": false}
            ],
            "score_top_n": 2,
            "score_fallback_scan_limit": 2
        }
    }));
    let (provider, endpoint, mut keys, mut rows) = large_pool_fixture(2, provider_config.clone());
    keys[0].internal_priority = 100;
    keys[1].internal_priority = 1;
    rows[0].key_internal_priority = 100;
    rows[1].key_internal_priority = 1;
    let data_state =
        GatewayDataState::with_provider_catalog_and_minimal_candidate_selection_for_tests(
            Arc::new(InMemoryProviderCatalogReadRepository::seed(
                vec![provider],
                vec![endpoint],
                keys,
            )),
            Arc::new(InMemoryMinimalCandidateSelectionReadRepository::seed(rows)),
        )
        .with_encryption_key_for_tests(aether_crypto::DEVELOPMENT_ENCRYPTION_KEY);
    let app = AppState::new()
        .expect("state should build")
        .with_data_state_for_tests(data_state);
    let group = sample_eligible_candidate(
        "provider-pool",
        "endpoint-1",
        "pool-group",
        10,
        provider_config,
    );
    let routing_policy = routing_policy_with_allowed_keys(["key-00000", "key-00001"]);
    let mut cursor = PoolKeyCursor::new_with_routing_policy(
        PlannerAppState::new(&app),
        group,
        None,
        None,
        None,
        Some(&routing_policy),
    );

    let candidate = cursor
        .next_key()
        .await
        .expect("an allowed key should be schedulable");

    assert_eq!(candidate.candidate.key_id, "key-00001");
}

#[tokio::test]
async fn pool_key_cursor_allows_parallel_requests_to_use_same_healthy_key() {
    let app = AppState::new().expect("state should build");
    let provider_config = Some(json!({ "pool_advanced": { "lru_enabled": true } }));
    let group = sample_eligible_candidate(
        "provider-pool",
        "endpoint-1",
        "pool-group",
        10,
        provider_config.clone(),
    );
    let mut cursor = PoolKeyCursor::new(PlannerAppState::new(&app), group, None, None, None);
    cursor.queued_candidates = VecDeque::from([
        sample_eligible_candidate(
            "provider-pool",
            "endpoint-1",
            "key-a",
            10,
            provider_config.clone(),
        ),
        sample_eligible_candidate(
            "provider-pool",
            "endpoint-1",
            "key-b",
            10,
            provider_config.clone(),
        ),
    ]);

    let candidate = cursor
        .next_key()
        .await
        .expect("cursor should return the first healthy key");
    assert_eq!(candidate.candidate.key_id, "key-a");
    assert_eq!(candidate.orchestration.pool_key_index, Some(0));
    assert!(candidate.orchestration.pool_key_lease.is_none());
    assert!(!cursor
        .skip_reason_counts
        .contains_key("pool_key_lease_busy"));

    let group = sample_eligible_candidate(
        "provider-pool",
        "endpoint-1",
        "pool-group",
        10,
        provider_config,
    );
    let mut second_cursor = PoolKeyCursor::new(PlannerAppState::new(&app), group, None, None, None);
    second_cursor.queued_candidates = VecDeque::from([
        sample_eligible_candidate(
            "provider-pool",
            "endpoint-1",
            "key-a",
            10,
            Some(json!({ "pool_advanced": { "lru_enabled": true } })),
        ),
        sample_eligible_candidate(
            "provider-pool",
            "endpoint-1",
            "key-b",
            10,
            Some(json!({ "pool_advanced": { "lru_enabled": true } })),
        ),
    ]);
    let second_candidate = second_cursor
        .next_key()
        .await
        .expect("second request should also be allowed to pick the same healthy key");
    assert_eq!(second_candidate.candidate.key_id, "key-a");
    assert!(second_candidate.orchestration.pool_key_lease.is_none());
}

#[tokio::test]
async fn pool_key_cursor_skips_key_after_account_cooldown_is_recorded() {
    let app = AppState::new().expect("state should build");
    let provider_config = Some(json!({ "pool_advanced": { "lru_enabled": true } }));
    let group = sample_eligible_candidate(
        "provider-pool",
        "endpoint-1",
        "pool-group",
        10,
        provider_config.clone(),
    );
    let pool_config = pool_config_for_candidate(&group).expect("pool config should parse");
    let mut cursor = PoolKeyCursor::new(PlannerAppState::new(&app), group, None, None, None);
    cursor.queued_candidates = VecDeque::from([
        sample_eligible_candidate(
            "provider-pool",
            "endpoint-1",
            "key-a",
            10,
            provider_config.clone(),
        ),
        sample_eligible_candidate("provider-pool", "endpoint-1", "key-b", 10, provider_config),
    ]);

    record_admin_provider_pool_error(
        app.runtime_state.as_ref(),
        "provider-pool",
        "key-a",
        &pool_config,
        429,
        None,
        None,
    )
    .await;

    let candidate = cursor
        .next_key()
        .await
        .expect("cursor should skip cooled-down key and return next key");
    assert_eq!(candidate.candidate.key_id, "key-b");
    assert_eq!(candidate.orchestration.pool_key_index, Some(0));
    assert!(candidate.orchestration.pool_key_lease.is_none());
    assert_eq!(cursor.skip_reason_counts.get("pool_cooldown"), Some(&1));
}

#[tokio::test]
async fn pool_key_cursor_continues_after_exhausted_window() {
    let provider_config = Some(json!({
        "pool_advanced": {
            "skip_exhausted_accounts": true
        }
    }));
    let (provider, endpoint, mut keys, rows) = large_pool_fixture(3, provider_config.clone());
    for key in keys.iter_mut().take(2) {
        key.status_snapshot = Some(json!({
            "quota": {
                "provider_type": "openai",
                "exhausted": true,
                "usage_ratio": 1.0,
                "windows": [
                    {
                        "code": "daily",
                        "used_ratio": 1.0,
                        "remaining_ratio": 0.0
                    }
                ]
            }
        }));
    }

    let data_state =
        GatewayDataState::with_provider_catalog_and_minimal_candidate_selection_for_tests(
            Arc::new(InMemoryProviderCatalogReadRepository::seed(
                vec![provider],
                vec![endpoint],
                keys,
            )),
            Arc::new(InMemoryMinimalCandidateSelectionReadRepository::seed(rows)),
        )
        .with_encryption_key_for_tests(aether_crypto::DEVELOPMENT_ENCRYPTION_KEY);
    let app = AppState::new()
        .expect("state should build")
        .with_data_state_for_tests(data_state);
    let group = sample_eligible_candidate(
        "provider-pool",
        "endpoint-1",
        "pool-group",
        10,
        provider_config,
    );

    let mut cursor = PoolKeyCursor::new(PlannerAppState::new(&app), group, None, None, None);
    cursor.window_size = 2;
    cursor.page_size = 2;
    cursor.max_scanned_keys = 4;

    let candidate = cursor
        .next_key()
        .await
        .expect("cursor should scan past an exhausted window");
    assert_eq!(candidate.candidate.key_id, "key-00002");
    assert_eq!(candidate.orchestration.pool_key_index, Some(0));
    assert!(candidate.orchestration.pool_key_lease.is_none());
    assert_eq!(
        cursor
            .skip_reason_counts
            .get(aether_pool_core::POOL_ACCOUNT_EXHAUSTED_SKIP_REASON),
        Some(&2)
    );

    let skipped = cursor.take_skipped_candidates();
    assert_eq!(skipped.len(), 2);
    assert!(skipped.iter().all(|candidate| {
        candidate.skip_reason == aether_pool_core::POOL_ACCOUNT_EXHAUSTED_SKIP_REASON
    }));
}

#[tokio::test]
async fn pool_key_cursor_reserve_minimum_quota_filters_pages_and_sticky_hits() {
    for reserve_enabled in [false, true] {
        for sticky in [false, true] {
            for used_percent in [99.0, 98.0, 83.0] {
                let provider_config = Some(json!({
                    "pool_advanced": {
                        "reserve_minimum_quota": reserve_enabled,
                        "skip_exhausted_accounts": false
                    }
                }));
                let provider =
                    sample_codex_pool_provider("provider-pool", 0, provider_config.clone());
                let endpoint = sample_codex_pool_endpoint("provider-pool", "endpoint-1");
                let mut reserved = sample_codex_pool_key("provider-pool", "key-low");
                reserved.status_snapshot = Some(json!({
                    "quota": {
                        "provider_type": "codex",
                        "updated_at": 100,
                        "allowed": false,
                        "exhausted": true,
                        "code": "exhausted",
                        "windows": [{
                            "code": "weekly",
                            "scope": "account",
                            "used_ratio": 1.0,
                            "reset_at": 4_102_444_800u64
                        }]
                    }
                }));
                reserved.upstream_metadata = Some(json!({
                    "codex": {
                        "updated_at": 200,
                        "primary_used_percent": used_percent,
                        "primary_reset_at": 4_102_444_800u64
                    }
                }));
                let ready = sample_codex_pool_key("provider-pool", "key-ready");
                let rows = vec![
                    sample_codex_pool_row("provider-pool", "endpoint-1", "key-low", 0),
                    sample_codex_pool_row("provider-pool", "endpoint-1", "key-ready", 0),
                ];
                let data_state = GatewayDataState::with_provider_catalog_and_minimal_candidate_selection_for_tests(
                    Arc::new(InMemoryProviderCatalogReadRepository::seed(
                        vec![provider], vec![endpoint], vec![reserved, ready],
                    )),
                    Arc::new(InMemoryMinimalCandidateSelectionReadRepository::seed(rows)),
                )
                .with_encryption_key_for_tests(aether_crypto::DEVELOPMENT_ENCRYPTION_KEY);
                let app = AppState::new()
                    .expect("state should build")
                    .with_data_state_for_tests(data_state);
                let group =
                    sample_codex_pool_group("provider-pool", "endpoint-1", 0, provider_config);
                let pool_config =
                    pool_config_for_candidate(&group).expect("pool config should parse");
                let sticky_token = sticky.then_some("reserve-session");
                if sticky {
                    record_admin_provider_pool_success(
                        app.runtime_state.as_ref(),
                        "provider-pool",
                        "key-low",
                        &pool_config,
                        sticky_token,
                        0,
                        None,
                    )
                    .await;
                }
                let mut cursor =
                    PoolKeyCursor::new(PlannerAppState::new(&app), group, sticky_token, None, None);
                cursor.window_size = 1;
                cursor.page_size = 1;
                let mut returned = Vec::new();
                while let Some(candidate) = cursor.next_key().await {
                    returned.push(candidate.candidate.key_id);
                }
                let reserve_reached = reserve_enabled && used_percent >= 99.0;
                assert_eq!(
                    returned.contains(&"key-low".to_string()),
                    !reserve_reached,
                    "reserve={reserve_enabled}, sticky={sticky}, used={used_percent}"
                );
                assert!(returned.contains(&"key-ready".to_string()));
                if reserve_reached {
                    assert_eq!(
                        cursor
                            .skip_reason_counts
                            .get(POOL_ACCOUNT_EXHAUSTED_SKIP_REASON),
                        Some(&1)
                    );
                } else if sticky {
                    assert_eq!(returned.first().map(String::as_str), Some("key-low"));
                }
            }
        }
    }
}

#[tokio::test]
async fn pool_key_cursor_does_not_spend_effective_scan_budget_on_exhausted_accounts() {
    let provider_config = Some(json!({
        "pool_advanced": {
            "skip_exhausted_accounts": true
        }
    }));
    let (provider, endpoint, mut keys, rows) = large_pool_fixture(700, provider_config.clone());
    for key in keys.iter_mut().take(600) {
        key.status_snapshot = Some(json!({
            "quota": {
                "provider_type": "openai",
                "exhausted": true,
                "usage_ratio": 1.0,
                "windows": [
                    {
                        "code": "daily",
                        "used_ratio": 1.0,
                        "remaining_ratio": 0.0
                    }
                ]
            }
        }));
    }

    let data_state =
        GatewayDataState::with_provider_catalog_and_minimal_candidate_selection_for_tests(
            Arc::new(InMemoryProviderCatalogReadRepository::seed(
                vec![provider],
                vec![endpoint],
                keys,
            )),
            Arc::new(InMemoryMinimalCandidateSelectionReadRepository::seed(rows)),
        )
        .with_encryption_key_for_tests(aether_crypto::DEVELOPMENT_ENCRYPTION_KEY);
    let app = AppState::new()
        .expect("state should build")
        .with_data_state_for_tests(data_state);
    let group = sample_eligible_candidate(
        "provider-pool",
        "endpoint-1",
        "pool-group",
        10,
        provider_config,
    );

    let mut cursor = PoolKeyCursor::new(PlannerAppState::new(&app), group, None, None, None);
    assert_eq!(
        cursor.max_scanned_keys,
        aether_dispatch_core::DEFAULT_POOL_MAX_SCAN
    );
    assert!(
        cursor.absolute_max_scanned_keys > cursor.max_scanned_keys,
        "pool config scan limit should be retained as the absolute cap"
    );

    let candidate = cursor
        .next_key()
        .await
        .expect("cursor should scan past exhausted accounts within the absolute cap");
    let key_index = candidate
        .candidate
        .key_id
        .strip_prefix("key-")
        .and_then(|value| value.parse::<usize>().ok())
        .expect("fixture key id should contain a numeric suffix");
    assert!(
        key_index >= 600,
        "cursor should not return one of the exhausted leading keys"
    );
    assert_eq!(candidate.orchestration.pool_key_index, Some(0));
    assert_eq!(cursor.scanned_keys, 640);
    assert_eq!(cursor.budget_scanned_keys, 40);
    assert_eq!(
        cursor
            .skip_reason_counts
            .get(aether_pool_core::POOL_ACCOUNT_EXHAUSTED_SKIP_REASON),
        Some(&600)
    );
}

#[tokio::test]
async fn pool_key_cursor_does_not_spend_effective_scan_budget_on_blocked_accounts() {
    const BLOCKED_COUNT: usize = 1_600;
    let provider_config = Some(json!({ "pool_advanced": {} }));
    let (provider, endpoint, mut keys, rows) =
        large_pool_fixture(BLOCKED_COUNT + 100, provider_config.clone());
    for key in keys.iter_mut().take(BLOCKED_COUNT) {
        key.oauth_invalid_reason = Some("blocked account".to_string());
    }

    let data_state =
        GatewayDataState::with_provider_catalog_and_minimal_candidate_selection_for_tests(
            Arc::new(InMemoryProviderCatalogReadRepository::seed(
                vec![provider],
                vec![endpoint],
                keys,
            )),
            Arc::new(InMemoryMinimalCandidateSelectionReadRepository::seed(rows)),
        )
        .with_encryption_key_for_tests(aether_crypto::DEVELOPMENT_ENCRYPTION_KEY);
    let app = AppState::new()
        .expect("state should build")
        .with_data_state_for_tests(data_state);
    let group = sample_eligible_candidate(
        "provider-pool",
        "endpoint-1",
        "pool-group",
        10,
        provider_config,
    );

    let mut cursor = PoolKeyCursor::new(PlannerAppState::new(&app), group, None, None, None);
    assert_eq!(
        cursor.max_scanned_keys,
        aether_dispatch_core::DEFAULT_POOL_MAX_SCAN
    );
    assert!(
        cursor.absolute_max_scanned_keys >= u32::try_from(BLOCKED_COUNT + 1).unwrap(),
        "default absolute scan cap should allow scanning past a large blocked prefix"
    );

    let candidate = cursor
        .next_key()
        .await
        .expect("cursor should scan past blocked accounts within the absolute cap");
    let key_index = candidate
        .candidate
        .key_id
        .strip_prefix("key-")
        .and_then(|value| value.parse::<usize>().ok())
        .expect("fixture key id should contain a numeric suffix");
    assert!(
        key_index >= BLOCKED_COUNT,
        "cursor should not return one of the blocked leading keys"
    );
    assert_eq!(candidate.orchestration.pool_key_index, Some(0));
    assert!(
        cursor.budget_scanned_keys <= aether_dispatch_core::DEFAULT_POOL_PAGE_SIZE,
        "blocked accounts should not consume effective scan budget"
    );
    assert_eq!(
        cursor
            .skip_reason_counts
            .get(aether_pool_core::POOL_ACCOUNT_BLOCKED_SKIP_REASON),
        Some(&(BLOCKED_COUNT as u32))
    );
}

#[tokio::test]
async fn pool_key_cursor_does_not_spend_scan_budget_on_missing_score_rows() {
    let provider_config = Some(json!({
        "pool_advanced": {
            "score_top_n": 128
        }
    }));
    let (provider, endpoint, keys, rows) = large_pool_fixture(1, provider_config.clone());
    let scores = (0..128)
        .map(|index| {
            sample_provider_key_pool_score(
                "provider-pool",
                &format!("missing-key-{index:03}"),
                1_000.0 - index as f64,
            )
        })
        .collect::<Vec<_>>();

    let data_state =
        GatewayDataState::with_provider_catalog_and_minimal_candidate_selection_for_tests(
            Arc::new(InMemoryProviderCatalogReadRepository::seed(
                vec![provider],
                vec![endpoint],
                keys,
            )),
            Arc::new(InMemoryMinimalCandidateSelectionReadRepository::seed(rows)),
        )
        .with_pool_score_repository_for_tests(Arc::new(InMemoryPoolMemberScoreRepository::seed(
            scores,
        )))
        .with_encryption_key_for_tests(aether_crypto::DEVELOPMENT_ENCRYPTION_KEY);
    let app = AppState::new()
        .expect("state should build")
        .with_data_state_for_tests(data_state);
    let group = sample_eligible_candidate(
        "provider-pool",
        "endpoint-1",
        "pool-group",
        10,
        provider_config,
    );

    let mut cursor = PoolKeyCursor::new(PlannerAppState::new(&app), group, None, None, None);
    let candidate = cursor
        .next_key()
        .await
        .expect("cursor should fall back to catalog rows after stale scores");

    assert_eq!(candidate.candidate.key_id, "key-00000");
    assert_eq!(cursor.scanned_keys, 1);
    assert_eq!(cursor.budget_scanned_keys, 1);
    assert_eq!(
        cursor.skip_reason_counts.get("pool_score_member_missing"),
        Some(&128)
    );
}

#[tokio::test]
async fn inactive_pool_key_with_stale_score_does_not_exhaust_pool() {
    let provider_config = Some(json!({
        "pool_advanced": {
            "score_top_n": 128,
            "scheduling_presets": [
                {"preset": "single_account", "enabled": true},
                {"preset": "priority_first", "enabled": true}
            ]
        }
    }));
    let (provider, endpoint, mut keys, mut rows) = large_pool_fixture(2, provider_config.clone());
    keys[1].is_active = false;
    rows.retain(|row| row.key_id != "key-00001");
    let scores = vec![
        sample_provider_key_pool_score("provider-pool", "key-00000", 5.0),
        sample_provider_key_pool_score("provider-pool", "key-00001", 20.0),
    ];
    let data_state =
        GatewayDataState::with_provider_catalog_and_minimal_candidate_selection_for_tests(
            Arc::new(InMemoryProviderCatalogReadRepository::seed(
                vec![provider],
                vec![endpoint],
                keys,
            )),
            Arc::new(InMemoryMinimalCandidateSelectionReadRepository::seed(rows)),
        )
        .with_pool_score_repository_for_tests(Arc::new(InMemoryPoolMemberScoreRepository::seed(
            scores,
        )))
        .with_encryption_key_for_tests(aether_crypto::DEVELOPMENT_ENCRYPTION_KEY);
    let app = AppState::new()
        .expect("state should build")
        .with_data_state_for_tests(data_state);
    let group = sample_eligible_candidate(
        "provider-pool",
        "endpoint-1",
        "pool-group",
        10,
        provider_config,
    );
    let mut cursor = PoolKeyCursor::new(PlannerAppState::new(&app), group, None, None, None);

    let candidate = cursor
        .next_key()
        .await
        .expect("active key must stay schedulable beside a stale inactive score");

    assert_eq!(candidate.candidate.key_id, "key-00000");
    assert_eq!(
        cursor.skip_reason_counts.get("pool_score_member_missing"),
        Some(&1)
    );
}

#[tokio::test]
async fn stale_inactive_score_only_does_not_exhaust_pool() {
    let provider_config = Some(json!({
        "pool_advanced": {
            "score_top_n": 128,
            "scheduling_presets": [
                {"preset": "single_account", "enabled": true},
                {"preset": "priority_first", "enabled": true}
            ]
        }
    }));
    let (provider, endpoint, mut keys, mut rows) = large_pool_fixture(2, provider_config.clone());
    keys[1].is_active = false;
    rows.retain(|row| row.key_id != "key-00001");
    let scores = vec![sample_provider_key_pool_score(
        "provider-pool",
        "key-00001",
        20.0,
    )];
    let data_state =
        GatewayDataState::with_provider_catalog_and_minimal_candidate_selection_for_tests(
            Arc::new(InMemoryProviderCatalogReadRepository::seed(
                vec![provider],
                vec![endpoint],
                keys,
            )),
            Arc::new(InMemoryMinimalCandidateSelectionReadRepository::seed(rows)),
        )
        .with_pool_score_repository_for_tests(Arc::new(InMemoryPoolMemberScoreRepository::seed(
            scores,
        )))
        .with_encryption_key_for_tests(aether_crypto::DEVELOPMENT_ENCRYPTION_KEY);
    let app = AppState::new()
        .expect("state should build")
        .with_data_state_for_tests(data_state);
    let group = sample_eligible_candidate(
        "provider-pool",
        "endpoint-1",
        "pool-group",
        10,
        provider_config,
    );
    let mut cursor = PoolKeyCursor::new(PlannerAppState::new(&app), group, None, None, None);

    let candidate = cursor
        .next_key()
        .await
        .expect("catalog rows must remain schedulable when the only score is stale");

    assert_eq!(candidate.candidate.key_id, "key-00000");
}

#[tokio::test]
async fn score_candidates_continue_across_pool_windows() {
    let provider_config = Some(json!({
        "pool_advanced": {
            "score_top_n": 128,
            "score_fallback_scan_limit": 128
        }
    }));
    let (provider, endpoint, keys, rows) = large_pool_fixture(128, provider_config.clone());
    let scores = (0..128)
        .map(|index| {
            sample_provider_key_pool_score(
                "provider-pool",
                &format!("key-{index:05}"),
                1_000.0 - index as f64,
            )
        })
        .collect::<Vec<_>>();
    let data_state =
        GatewayDataState::with_provider_catalog_and_minimal_candidate_selection_for_tests(
            Arc::new(InMemoryProviderCatalogReadRepository::seed(
                vec![provider],
                vec![endpoint],
                keys,
            )),
            Arc::new(InMemoryMinimalCandidateSelectionReadRepository::seed(rows)),
        )
        .with_pool_score_repository_for_tests(Arc::new(InMemoryPoolMemberScoreRepository::seed(
            scores,
        )))
        .with_encryption_key_for_tests(aether_crypto::DEVELOPMENT_ENCRYPTION_KEY);
    let app = AppState::new()
        .expect("state should build")
        .with_data_state_for_tests(data_state);
    let group = sample_eligible_candidate(
        "provider-pool",
        "endpoint-1",
        "pool-group",
        10,
        provider_config,
    );
    let mut cursor = PoolKeyCursor::new(PlannerAppState::new(&app), group, None, None, None);

    let mut returned_key_ids = Vec::new();
    while let Some(candidate) = cursor.next_key().await {
        returned_key_ids.push(candidate.candidate.key_id);
    }

    assert_eq!(returned_key_ids.len(), 32);
    assert!(returned_key_ids.iter().any(|key_id| key_id == "key-00112"));
    assert!(returned_key_ids.iter().any(|key_id| key_id == "key-00127"));
    assert_eq!(cursor.score_next_offset, 128);
    assert!(cursor.score_phase_exhausted);
    assert_eq!(cursor.score_schedule_interest_count, 16);
    assert_eq!(cursor.scanned_keys, 128);
    assert_eq!(cursor.budget_scanned_keys, 128);
}

#[tokio::test]
async fn score_and_fallback_duplicate_does_not_consume_scan_budget_twice() {
    let provider_config = Some(json!({
        "pool_advanced": {
            "score_top_n": 1,
            "score_fallback_scan_limit": 2
        }
    }));
    let (provider, endpoint, keys, rows) = large_pool_fixture(2, provider_config.clone());
    let data_state =
        GatewayDataState::with_provider_catalog_and_minimal_candidate_selection_for_tests(
            Arc::new(InMemoryProviderCatalogReadRepository::seed(
                vec![provider],
                vec![endpoint],
                keys,
            )),
            Arc::new(InMemoryMinimalCandidateSelectionReadRepository::seed(rows)),
        )
        .with_pool_score_repository_for_tests(Arc::new(InMemoryPoolMemberScoreRepository::seed(
            vec![sample_provider_key_pool_score(
                "provider-pool",
                "key-00000",
                1_000.0,
            )],
        )))
        .with_encryption_key_for_tests(aether_crypto::DEVELOPMENT_ENCRYPTION_KEY);
    let app = AppState::new()
        .expect("state should build")
        .with_data_state_for_tests(data_state);
    let group = sample_eligible_candidate(
        "provider-pool",
        "endpoint-1",
        "pool-group",
        10,
        provider_config,
    );
    let mut cursor = PoolKeyCursor::new(PlannerAppState::new(&app), group, None, None, None);

    let mut returned_key_ids = vec![
        cursor
            .next_key()
            .await
            .expect("one candidate should schedule")
            .candidate
            .key_id,
        cursor
            .next_key()
            .await
            .expect("the other candidate should schedule")
            .candidate
            .key_id,
    ];
    returned_key_ids.sort();

    assert_eq!(returned_key_ids, ["key-00000", "key-00001"]);
    assert_eq!(cursor.scanned_keys, 3);
    assert_eq!(cursor.budget_scanned_keys, 2);
}

#[tokio::test]
async fn pool_scheduler_skips_invalid_and_exhausted_high_priority_hot_pool_before_fallback_provider(
) {
    let provider_config = Some(json!({
        "pool_advanced": {
            "probing_enabled": true,
            "skip_exhausted_accounts": true,
            "scheduling_presets": [
                {"preset": "single_account", "enabled": true}
            ]
        }
    }));
    let provider_a = sample_codex_pool_provider("provider-a", 0, provider_config.clone());
    let provider_b = sample_codex_pool_provider("provider-b", 10, provider_config.clone());
    let endpoint_a = sample_codex_pool_endpoint("provider-a", "endpoint-a");
    let endpoint_b = sample_codex_pool_endpoint("provider-b", "endpoint-b");

    let mut key_a_invalid = sample_codex_pool_key("provider-a", "key-a-invalid");
    key_a_invalid.oauth_invalid_at_unix_secs = Some(1_710_000_000);
    key_a_invalid.oauth_invalid_reason = Some("[OAUTH_EXPIRED] token invalidated".to_string());
    let exhausted_status_snapshot = json!({
        "quota": {
            "provider_type": "codex",
            "exhausted": true,
            "usage_ratio": 1.0,
            "windows": [
                {
                    "code": "daily",
                    "used_ratio": 1.0,
                    "remaining_ratio": 0.0
                }
            ]
        }
    });
    key_a_invalid.status_snapshot = Some(exhausted_status_snapshot.clone());
    let mut key_a_exhausted = sample_codex_pool_key("provider-a", "key-a-exhausted");
    key_a_exhausted.status_snapshot = Some(exhausted_status_snapshot);
    let key_b_ready = sample_codex_pool_key("provider-b", "key-b-ready");

    let rows = vec![
        sample_codex_pool_row("provider-a", "endpoint-a", "key-a-invalid", 0),
        sample_codex_pool_row("provider-a", "endpoint-a", "key-a-exhausted", 0),
        sample_codex_pool_row("provider-b", "endpoint-b", "key-b-ready", 10),
    ];

    let data_state =
        GatewayDataState::with_provider_catalog_and_minimal_candidate_selection_for_tests(
            Arc::new(InMemoryProviderCatalogReadRepository::seed(
                vec![provider_a, provider_b],
                vec![endpoint_a, endpoint_b],
                vec![key_a_invalid, key_a_exhausted, key_b_ready],
            )),
            Arc::new(InMemoryMinimalCandidateSelectionReadRepository::seed(rows)),
        )
        .with_encryption_key_for_tests(aether_crypto::DEVELOPMENT_ENCRYPTION_KEY);
    let app = AppState::new()
        .expect("state should build")
        .with_data_state_for_tests(data_state);
    app.runtime_state
        .set_add(
            &admin_provider_pool_quota_probe_active_members_key("provider-a"),
            "key-a-invalid",
        )
        .await
        .expect("provider-a hot member should insert");
    app.runtime_state
        .set_add(
            &admin_provider_pool_quota_probe_active_members_key("provider-b"),
            "key-b-ready",
        )
        .await
        .expect("provider-b hot member should insert");

    let group_a = sample_codex_pool_group("provider-a", "endpoint-a", 0, provider_config.clone());
    let group_b = sample_codex_pool_group("provider-b", "endpoint-b", 10, provider_config);

    let (scheduled, skipped) = apply_local_execution_pool_scheduler(
        PlannerAppState::new(&app),
        vec![group_a, group_b],
        None,
        Some("gpt-5"),
        None,
    )
    .await;

    assert_eq!(
        scheduled
            .iter()
            .map(|item| item.candidate.key_id.as_str())
            .collect::<Vec<_>>(),
        vec!["key-b-ready"]
    );
    let skipped_pairs = skipped
        .iter()
        .map(|item| (item.candidate.key_id.as_str(), item.skip_reason))
        .collect::<Vec<_>>();
    assert!(skipped_pairs.contains(&("key-a-invalid", "pool_account_blocked")));
    assert!(skipped_pairs.contains(&(
        "key-a-exhausted",
        aether_pool_core::POOL_ACCOUNT_EXHAUSTED_SKIP_REASON
    )));
}

#[tokio::test]
async fn pool_scheduler_skips_invalid_high_priority_hot_pool_account_even_with_remaining_quota() {
    let provider_config = Some(json!({
        "pool_advanced": {
            "probing_enabled": true,
            "skip_exhausted_accounts": true,
            "scheduling_presets": [
                {"preset": "single_account", "enabled": true}
            ]
        }
    }));
    let provider_a = sample_codex_pool_provider("provider-a", 0, provider_config.clone());
    let provider_b = sample_codex_pool_provider("provider-b", 10, provider_config.clone());
    let endpoint_a = sample_codex_pool_endpoint("provider-a", "endpoint-a");
    let endpoint_b = sample_codex_pool_endpoint("provider-b", "endpoint-b");

    let mut key_a_invalid = sample_codex_pool_key("provider-a", "key-a-invalid");
    key_a_invalid.oauth_invalid_at_unix_secs = Some(1_710_000_000);
    key_a_invalid.oauth_invalid_reason = Some("[OAUTH_EXPIRED] token invalidated".to_string());
    key_a_invalid.status_snapshot = Some(json!({
        "quota": {
            "provider_type": "codex",
            "exhausted": false,
            "usage_ratio": 0.25,
            "windows": [
                {
                    "code": "daily",
                    "used_ratio": 0.25,
                    "remaining_ratio": 0.75
                }
            ]
        }
    }));
    let key_b_ready = sample_codex_pool_key("provider-b", "key-b-ready");

    let rows = vec![
        sample_codex_pool_row("provider-a", "endpoint-a", "key-a-invalid", 0),
        sample_codex_pool_row("provider-b", "endpoint-b", "key-b-ready", 10),
    ];

    let data_state =
        GatewayDataState::with_provider_catalog_and_minimal_candidate_selection_for_tests(
            Arc::new(InMemoryProviderCatalogReadRepository::seed(
                vec![provider_a, provider_b],
                vec![endpoint_a, endpoint_b],
                vec![key_a_invalid, key_b_ready],
            )),
            Arc::new(InMemoryMinimalCandidateSelectionReadRepository::seed(rows)),
        )
        .with_encryption_key_for_tests(aether_crypto::DEVELOPMENT_ENCRYPTION_KEY);
    let app = AppState::new()
        .expect("state should build")
        .with_data_state_for_tests(data_state);
    app.runtime_state
        .set_add(
            &admin_provider_pool_quota_probe_active_members_key("provider-a"),
            "key-a-invalid",
        )
        .await
        .expect("provider-a hot member should insert");
    app.runtime_state
        .set_add(
            &admin_provider_pool_quota_probe_active_members_key("provider-b"),
            "key-b-ready",
        )
        .await
        .expect("provider-b hot member should insert");

    let group_a = sample_codex_pool_group("provider-a", "endpoint-a", 0, provider_config.clone());
    let group_b = sample_codex_pool_group("provider-b", "endpoint-b", 10, provider_config);

    let (scheduled, skipped) = apply_local_execution_pool_scheduler(
        PlannerAppState::new(&app),
        vec![group_a, group_b],
        None,
        Some("gpt-5"),
        None,
    )
    .await;

    assert_eq!(
        scheduled
            .iter()
            .map(|item| item.candidate.key_id.as_str())
            .collect::<Vec<_>>(),
        vec!["key-b-ready"]
    );
    let skipped_pairs = skipped
        .iter()
        .map(|item| (item.candidate.key_id.as_str(), item.skip_reason))
        .collect::<Vec<_>>();
    assert!(skipped_pairs.contains(&("key-a-invalid", "pool_account_blocked")));
}

#[test]
fn pool_key_reauth_scheduling_keeps_recoverable_oauth_markers_usable() {
    let mut key = sample_codex_pool_key("provider-a", "key-refresh-failed");
    key.expires_at_unix_secs = Some(200);
    key.oauth_invalid_reason = Some(
        "[REFRESH_FAILED] Token 续期失败 (401): refresh_token 已被使用并轮换，请重新登录授权"
            .to_string(),
    );

    assert!(!pool_key_requires_reauth_for_scheduling(&key, 100));
    assert!(pool_key_requires_reauth_for_scheduling(&key, 200));

    key.oauth_invalid_reason = Some("[REQUEST_FAILED] 账号状态检查失败".to_string());
    key.oauth_invalid_at_unix_secs = Some(100);
    assert!(!pool_key_requires_reauth_for_scheduling(&key, 300));

    key.oauth_invalid_reason = Some("[OAUTH_EXPIRED] session expired".to_string());
    assert!(!pool_key_requires_reauth_for_scheduling(&key, 300));
}

#[test]
fn pool_key_reauth_scheduling_blocks_invalid_oauth_markers_without_affecting_non_oauth_keys() {
    let mut key = sample_codex_pool_key("provider-a", "key-invalid");
    key.oauth_invalid_reason = Some("[ACCOUNT_BLOCK] account has been deactivated".to_string());
    assert!(pool_key_requires_reauth_for_scheduling(&key, 100));

    key.oauth_invalid_reason = Some("[OAUTH_EXPIRED] token invalidated".to_string());
    key.oauth_invalid_at_unix_secs = None;
    assert!(pool_key_requires_reauth_for_scheduling(&key, 100));

    key.oauth_invalid_reason = Some("Kiro Token 无效或已过期".to_string());
    key.oauth_invalid_at_unix_secs = None;
    assert!(pool_key_requires_reauth_for_scheduling(&key, 100));

    key.oauth_invalid_reason = None;
    key.oauth_invalid_at_unix_secs = Some(100);
    assert!(pool_key_requires_reauth_for_scheduling(&key, 100));

    key.auth_type = "api_key".to_string();
    assert!(!pool_key_requires_reauth_for_scheduling(&key, 100));
}

#[tokio::test]
async fn pool_key_cursor_simulates_large_lru_pool_with_lazy_pages_and_dynamic_skips() {
    const KEY_COUNT: usize = 2048;
    let provider_config = Some(json!({
        "pool_advanced": {
            "lru_enabled": true,
            "rate_limit_cooldown_seconds": 300
        }
    }));
    let (provider, endpoint, keys, rows) = large_pool_fixture(KEY_COUNT, provider_config.clone());
    let data_state =
        GatewayDataState::with_provider_catalog_and_minimal_candidate_selection_for_tests(
            Arc::new(InMemoryProviderCatalogReadRepository::seed(
                vec![provider],
                vec![endpoint],
                keys,
            )),
            Arc::new(InMemoryMinimalCandidateSelectionReadRepository::seed(rows)),
        )
        .with_encryption_key_for_tests(aether_crypto::DEVELOPMENT_ENCRYPTION_KEY);
    let app = AppState::new()
        .expect("state should build")
        .with_data_state_for_tests(data_state);
    let group = sample_eligible_candidate(
        "provider-pool",
        "endpoint-1",
        "pool-group",
        10,
        provider_config.clone(),
    );
    let pool_config = pool_config_for_candidate(&group).expect("pool config should parse");
    for key_id in ["key-00000", "key-00001"] {
        record_admin_provider_pool_error(
            app.runtime_state.as_ref(),
            "provider-pool",
            key_id,
            &pool_config,
            429,
            None,
            None,
        )
        .await;
    }

    let mut cursor = PoolKeyCursor::new(PlannerAppState::new(&app), group, None, None, None);
    assert_eq!(
        cursor.window_size,
        aether_dispatch_core::DEFAULT_POOL_WINDOW_SIZE
    );
    assert_eq!(
        cursor.page_size,
        aether_dispatch_core::DEFAULT_POOL_PAGE_SIZE
    );
    assert_eq!(
        cursor.max_scanned_keys,
        aether_dispatch_core::DEFAULT_POOL_MAX_SCAN
    );

    let mut returned_ids = Vec::new();
    for _ in 0..10 {
        let candidate = cursor
            .next_key()
            .await
            .expect("large pool should return first page candidates");
        returned_ids.push(candidate.candidate.key_id.clone());
        assert!(candidate.orchestration.pool_key_lease.is_none());
    }
    assert_eq!(returned_ids.first().map(String::as_str), Some("key-00002"));
    assert_eq!(returned_ids.last().map(String::as_str), Some("key-00011"));
    assert_eq!(cursor.scanned_keys, 64);
    assert!(
        cursor.queued_candidates.len() <= cursor.window_size as usize,
        "cursor should only retain the current page window"
    );

    record_admin_provider_pool_error(
        app.runtime_state.as_ref(),
        "provider-pool",
        "key-00014",
        &pool_config,
        429,
        None,
        None,
    )
    .await;

    let candidate = cursor
        .next_key()
        .await
        .expect("cursor should keep the frozen window despite later runtime changes");
    assert_eq!(candidate.candidate.key_id, "key-00012");
    returned_ids.push(candidate.candidate.key_id.clone());
    assert!(candidate.orchestration.pool_key_lease.is_none());

    while let Some(candidate) = cursor.next_key().await {
        returned_ids.push(candidate.candidate.key_id.clone());
        assert!(candidate.orchestration.pool_key_lease.is_none());
    }

    let max_returned_windows =
        aether_dispatch_core::DEFAULT_POOL_MAX_SCAN / aether_dispatch_core::DEFAULT_POOL_PAGE_SIZE;
    assert!(
        returned_ids.len()
            <= (max_returned_windows * aether_dispatch_core::DEFAULT_POOL_WINDOW_SIZE) as usize,
        "cursor should only return bounded frozen windows per request"
    );
    assert_eq!(returned_ids.len(), 127);
    assert_eq!(returned_ids.last().map(String::as_str), Some("key-00463"));
    assert_eq!(cursor.scanned_keys, 512);
    assert_eq!(cursor.skip_reason_counts.get("pool_cooldown"), Some(&3));
    assert!(!cursor
        .skip_reason_counts
        .contains_key("pool_key_lease_busy"));
    for skipped in ["key-00000", "key-00001", "key-00014"] {
        assert!(
            !returned_ids.iter().any(|key_id| key_id == skipped),
            "{skipped} should have been skipped"
        );
    }
    assert!(
        returned_ids.iter().any(|key_id| key_id == "key-00015"),
        "key-00015 should not be blocked by request-scoped leases"
    );
}

#[test]
fn builds_pool_catalog_context_from_status_snapshot_and_auth_config() {
    let mut key = StoredProviderCatalogKey::new(
        "key-1".to_string(),
        "provider-1".to_string(),
        "key-1".to_string(),
        "oauth".to_string(),
        None,
        true,
    )
    .expect("key should build")
    .with_transport_fields(
        None,
        "secret".to_string(),
        None,
        None,
        None,
        None,
        None,
        None,
        None,
    )
    .expect("transport fields should build");
    key.status_snapshot = Some(json!({
        "account": {"blocked": false},
        "quota": {
            "usage_ratio": 0.25,
            "reset_seconds": 3600,
            "exhausted": false,
            "plan_type": "team"
        }
    }));
    key.success_count = Some(4);
    key.total_response_time_ms = Some(200);
    key.last_used_at_unix_secs = Some(1_711_000_123);

    let app = AppState::new()
        .expect("state should build")
        .with_data_state_for_tests(GatewayDataState::with_provider_catalog_reader_for_tests(
            Arc::new(InMemoryProviderCatalogReadRepository::seed(
                Vec::new(),
                Vec::new(),
                vec![key.clone()],
            )),
        ));

    let context = build_pool_catalog_key_context(
        PlannerAppState::new(&app),
        &ProviderPoolService::with_builtin_adapters(),
        &key,
        "codex",
        None,
    );

    assert_eq!(context.plan_tier.as_deref(), Some("team"));
    assert_eq!(context.quota_usage_ratio, Some(0.25));
    assert_eq!(context.quota_reset_seconds, Some(3600.0));
    assert_eq!(context.latency_avg_ms, Some(50.0));
    assert_eq!(context.catalog_lru_score, Some(1_711_000_123.0));
}

#[test]
fn pool_catalog_context_ignores_stale_codex_exhausted_snapshot_when_windows_have_capacity() {
    let mut key = sample_catalog_oauth_key("key-stale-exhausted");
    key.upstream_metadata = Some(json!({
        "codex": {
            "primary_used_percent": 100.0
        }
    }));
    key.status_snapshot = Some(json!({
        "quota": {
            "version": 2,
            "provider_type": "codex",
            "code": "exhausted",
            "exhausted": true,
            "usage_ratio": 0.0,
            "windows": [
                {
                    "code": "weekly",
                    "used_ratio": 0.0,
                    "remaining_ratio": 1.0
                },
                {
                    "code": "5h",
                    "used_ratio": 0.0,
                    "remaining_ratio": 1.0
                }
            ]
        }
    }));

    let app = app_state_with_catalog_key(key.clone());
    let context = build_pool_catalog_key_context(
        PlannerAppState::new(&app),
        &ProviderPoolService::with_builtin_adapters(),
        &key,
        "codex",
        None,
    );

    assert!(!context.quota_exhausted);
}

#[test]
fn pool_catalog_context_marks_codex_metadata_exhausted() {
    let mut key = sample_catalog_oauth_key("key-metadata-exhausted");
    key.upstream_metadata = Some(json!({
        "codex": {
            "secondary_used_percent": 100.0
        }
    }));

    let app = app_state_with_catalog_key(key.clone());
    let context = build_pool_catalog_key_context(
        PlannerAppState::new(&app),
        &ProviderPoolService::with_builtin_adapters(),
        &key,
        "codex",
        None,
    );

    assert!(context.quota_exhausted);
}

#[test]
fn pool_catalog_context_preserves_snapshot_exhaustion_for_snapshot_only_providers() {
    let mut key = sample_catalog_oauth_key("key-antigravity-exhausted");
    key.status_snapshot = Some(json!({
        "quota": {
            "version": 2,
            "provider_type": "antigravity",
            "code": "exhausted",
            "exhausted": true,
            "windows": [
                {
                    "code": "gemini-2.5-pro",
                    "used_ratio": 1.0,
                    "remaining_ratio": 0.0
                }
            ]
        }
    }));

    let app = app_state_with_catalog_key(key.clone());
    let context = build_pool_catalog_key_context(
        PlannerAppState::new(&app),
        &ProviderPoolService::with_builtin_adapters(),
        &key,
        "antigravity",
        None,
    );

    assert!(context.quota_exhausted);
}

#[test]
fn pool_catalog_context_scopes_antigravity_exhaustion_to_requested_model() {
    let mut key = sample_catalog_oauth_key("key-antigravity-model-quota");
    key.status_snapshot = Some(json!({
        "quota": {
            "version": 2,
            "provider_type": "antigravity",
            "exhausted": false,
            "windows": [
                {
                    "code": "model:gemini-3.1-pro-high",
                    "scope": "model",
                    "model": "gemini-3.1-pro-high",
                    "used_ratio": 1.0,
                    "is_exhausted": true
                },
                {
                    "code": "model:gemini-3-flash-agent",
                    "scope": "model",
                    "model": "gemini-3-flash-agent",
                    "used_ratio": 0.1,
                    "is_exhausted": false
                }
            ]
        }
    }));

    let app = app_state_with_catalog_key(key.clone());
    let exhausted = build_pool_catalog_key_context(
        PlannerAppState::new(&app),
        &ProviderPoolService::with_builtin_adapters(),
        &key,
        "antigravity",
        Some("gemini-3.1-pro-high"),
    );
    let available = build_pool_catalog_key_context(
        PlannerAppState::new(&app),
        &ProviderPoolService::with_builtin_adapters(),
        &key,
        "antigravity",
        Some("gemini-3-flash-agent"),
    );

    assert!(exhausted.quota_exhausted);
    assert!(!available.quota_exhausted);
}

#[test]
fn pool_catalog_context_marks_known_banned_account_from_metadata() {
    let mut key = sample_catalog_oauth_key("key-account-banned");
    key.upstream_metadata = Some(json!({
        "codex": {
            "account_disabled": true,
            "reason": "deactivated_workspace"
        }
    }));

    let app = app_state_with_catalog_key(key.clone());
    let context = build_pool_catalog_key_context(
        PlannerAppState::new(&app),
        &ProviderPoolService::with_builtin_adapters(),
        &key,
        "codex",
        None,
    );

    assert!(context.account_blocked);
}

fn sample_catalog_oauth_key(key_id: &str) -> StoredProviderCatalogKey {
    StoredProviderCatalogKey::new(
        key_id.to_string(),
        "provider-1".to_string(),
        key_id.to_string(),
        "oauth".to_string(),
        None,
        true,
    )
    .expect("key should build")
    .with_transport_fields(
        None,
        "secret".to_string(),
        None,
        None,
        None,
        None,
        None,
        None,
        None,
    )
    .expect("transport fields should build")
}

fn app_state_with_catalog_key(key: StoredProviderCatalogKey) -> AppState {
    AppState::new()
        .expect("state should build")
        .with_data_state_for_tests(GatewayDataState::with_provider_catalog_reader_for_tests(
            Arc::new(InMemoryProviderCatalogReadRepository::seed(
                Vec::new(),
                Vec::new(),
                vec![key],
            )),
        ))
}

fn large_pool_fixture(
    key_count: usize,
    provider_config: Option<serde_json::Value>,
) -> (
    StoredProviderCatalogProvider,
    StoredProviderCatalogEndpoint,
    Vec<StoredProviderCatalogKey>,
    Vec<StoredMinimalCandidateSelectionRow>,
) {
    let provider = StoredProviderCatalogProvider::new(
        "provider-pool".to_string(),
        "provider-pool".to_string(),
        Some("https://example.com".to_string()),
        "openai".to_string(),
    )
    .expect("provider should build")
    .with_routing_fields(0)
    .with_transport_fields(
        true,
        false,
        false,
        None,
        None,
        None,
        None,
        None,
        provider_config,
    );
    let endpoint = StoredProviderCatalogEndpoint::new(
        "endpoint-1".to_string(),
        "provider-pool".to_string(),
        "openai:chat".to_string(),
        Some("openai".to_string()),
        Some("chat".to_string()),
        true,
    )
    .expect("endpoint should build")
    .with_health_score(1.0)
    .with_transport_fields(
        "https://example.com/v1/chat/completions".to_string(),
        None,
        None,
        None,
        None,
        None,
        None,
        None,
    )
    .expect("endpoint transport should build");

    // 这些用例只验证池扫描、跳过计数和游标预算，不会发起请求或读取凭据。
    // 留空凭据可跳过无关的 Fernet 加解密，同时避免复用绑定密文破坏 key_id AAD。
    let mut keys = Vec::with_capacity(key_count);
    let mut rows = Vec::with_capacity(key_count);
    for index in 0..key_count {
        let key_id = format!("key-{index:05}");
        let mut key = StoredProviderCatalogKey::new(
            key_id.clone(),
            "provider-pool".to_string(),
            key_id.clone(),
            "api_key".to_string(),
            None,
            true,
        )
        .expect("key should build")
        .with_transport_fields(
            Some(json!(["openai:chat"])),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .expect("key transport should build");
        key.internal_priority = 10;
        key.last_used_at_unix_secs = Some(index as u64);
        keys.push(key);
        rows.push(StoredMinimalCandidateSelectionRow {
            provider_id: "provider-pool".to_string(),
            provider_name: "provider-pool".to_string(),
            provider_type: "openai".to_string(),
            provider_priority: 0,
            provider_is_active: true,
            endpoint_id: "endpoint-1".to_string(),
            endpoint_api_format: "openai:chat".to_string(),
            endpoint_api_family: Some("openai".to_string()),
            endpoint_kind: Some("chat".to_string()),
            endpoint_is_active: true,
            key_id: key_id.clone(),
            key_name: key_id,
            key_auth_type: "api_key".to_string(),
            key_is_active: true,
            key_api_formats: Some(vec!["openai:chat".to_string()]),
            key_allowed_models: None,
            key_capabilities: None,
            key_internal_priority: 10,
            key_global_priority_by_format: None,
            model_id: "model-1".to_string(),
            global_model_id: "global-model-1".to_string(),
            global_model_name: "gpt-5".to_string(),
            global_model_mappings: None,
            global_model_supports_streaming: Some(true),
            model_provider_model_name: "gpt-5".to_string(),
            model_provider_model_mappings: None,
            model_supports_streaming: Some(true),
            model_is_active: true,
            model_is_available: true,
        });
    }
    (provider, endpoint, keys, rows)
}

fn sample_provider_key_pool_score(
    provider_id: &str,
    key_id: &str,
    score: f64,
) -> StoredPoolMemberScore {
    let identity = PoolMemberIdentity::provider_api_key(provider_id, key_id);
    let scope = provider_key_pool_score_scope();
    StoredPoolMemberScore {
        id: provider_key_pool_score_id(&identity, &scope),
        pool_kind: identity.pool_kind,
        pool_id: identity.pool_id,
        member_kind: identity.member_kind,
        member_id: identity.member_id,
        capability: scope.capability,
        scope_kind: scope.scope_kind,
        scope_id: scope.scope_id,
        score,
        hard_state: PoolMemberHardState::Available,
        score_version: 1,
        score_reason: json!({}),
        last_ranked_at: Some(1_000),
        last_scheduled_at: None,
        last_success_at: None,
        last_failure_at: None,
        failure_count: 0,
        last_probe_attempt_at: None,
        last_probe_success_at: None,
        last_probe_failure_at: None,
        probe_failure_count: 0,
        probe_status: PoolMemberProbeStatus::Ok,
        updated_at: 1_000,
    }
}

fn sample_codex_pool_provider(
    provider_id: &str,
    provider_priority: i32,
    provider_config: Option<serde_json::Value>,
) -> StoredProviderCatalogProvider {
    StoredProviderCatalogProvider::new(
        provider_id.to_string(),
        provider_id.to_string(),
        Some("https://example.com".to_string()),
        "codex".to_string(),
    )
    .expect("provider should build")
    .with_routing_fields(provider_priority)
    .with_transport_fields(
        true,
        false,
        false,
        None,
        None,
        None,
        None,
        None,
        provider_config,
    )
}

fn sample_codex_pool_endpoint(
    provider_id: &str,
    endpoint_id: &str,
) -> StoredProviderCatalogEndpoint {
    StoredProviderCatalogEndpoint::new(
        endpoint_id.to_string(),
        provider_id.to_string(),
        "openai:responses".to_string(),
        Some("openai".to_string()),
        Some("responses".to_string()),
        true,
    )
    .expect("endpoint should build")
    .with_health_score(1.0)
    .with_transport_fields(
        "https://example.com/v1/responses".to_string(),
        None,
        None,
        None,
        None,
        None,
        None,
        None,
    )
    .expect("endpoint transport should build")
}

/// 这些测试只检查池调度状态，不涉及凭据解密，因此不构造无关的密文。
fn sample_codex_pool_key(provider_id: &str, key_id: &str) -> StoredProviderCatalogKey {
    let mut key = StoredProviderCatalogKey::new(
        key_id.to_string(),
        provider_id.to_string(),
        key_id.to_string(),
        "oauth".to_string(),
        None,
        true,
    )
    .expect("key should build")
    .with_transport_fields(
        Some(json!(["openai:responses"])),
        None,
        None,
        None,
        Some(json!({"openai:responses": 1})),
        None,
        Some(4_102_444_800),
        None,
        None,
    )
    .expect("key transport should build");
    key.internal_priority = 10;
    key
}

fn sample_codex_pool_row(
    provider_id: &str,
    endpoint_id: &str,
    key_id: &str,
    provider_priority: i32,
) -> StoredMinimalCandidateSelectionRow {
    StoredMinimalCandidateSelectionRow {
        provider_id: provider_id.to_string(),
        provider_name: provider_id.to_string(),
        provider_type: "codex".to_string(),
        provider_priority,
        provider_is_active: true,
        endpoint_id: endpoint_id.to_string(),
        endpoint_api_format: "openai:responses".to_string(),
        endpoint_api_family: Some("openai".to_string()),
        endpoint_kind: Some("responses".to_string()),
        endpoint_is_active: true,
        key_id: key_id.to_string(),
        key_name: key_id.to_string(),
        key_auth_type: "oauth".to_string(),
        key_is_active: true,
        key_api_formats: Some(vec!["openai:responses".to_string()]),
        key_allowed_models: None,
        key_capabilities: None,
        key_internal_priority: 10,
        key_global_priority_by_format: Some(json!({"openai:responses": 1})),
        model_id: "model-1".to_string(),
        global_model_id: "global-model-1".to_string(),
        global_model_name: "gpt-5".to_string(),
        global_model_mappings: None,
        global_model_supports_streaming: Some(true),
        model_provider_model_name: "gpt-5".to_string(),
        model_provider_model_mappings: None,
        model_supports_streaming: Some(true),
        model_is_active: true,
        model_is_available: true,
    }
}

fn sample_codex_pool_group(
    provider_id: &str,
    endpoint_id: &str,
    provider_priority: i32,
    provider_config: Option<serde_json::Value>,
) -> EligibleLocalExecutionCandidate {
    EligibleLocalExecutionCandidate {
        kind: LocalExecutionCandidateKind::PoolGroup,
        candidate: SchedulerMinimalCandidateSelectionCandidate {
            provider_id: provider_id.to_string(),
            provider_name: provider_id.to_string(),
            provider_type: "codex".to_string(),
            provider_priority,
            endpoint_id: endpoint_id.to_string(),
            endpoint_api_format: "openai:responses".to_string(),
            key_id: format!("{provider_id}-pool-group"),
            key_name: format!("{provider_id}-pool-group"),
            key_auth_type: "oauth".to_string(),
            key_internal_priority: 10,
            key_global_priority_for_format: Some(1),
            key_capabilities: None,
            model_id: "model-1".to_string(),
            global_model_id: "global-model-1".to_string(),
            global_model_name: "gpt-5".to_string(),
            selected_provider_model_name: "gpt-5".to_string(),
            supports_streaming: true,
            mapping_matched_model: None,
        },
        provider_api_format: "openai:responses".to_string(),
        orchestration: LocalExecutionCandidateMetadata::default(),
        ranking: None,
        transport: Arc::new(crate::ai_serving::GatewayProviderTransportSnapshot {
            provider: GatewayProviderTransportProvider {
                id: provider_id.to_string(),
                name: provider_id.to_string(),
                provider_type: "codex".to_string(),
                website: None,
                is_active: true,
                keep_priority_on_conversion: false,
                enable_format_conversion: false,
                concurrent_limit: None,
                max_retries: None,
                proxy: None,
                request_timeout_secs: None,
                stream_first_byte_timeout_secs: None,
                config: provider_config,
            },
            endpoint: GatewayProviderTransportEndpoint {
                id: endpoint_id.to_string(),
                provider_id: provider_id.to_string(),
                api_format: "openai:responses".to_string(),
                api_family: Some("openai".to_string()),
                endpoint_kind: Some("responses".to_string()),
                is_active: true,
                base_url: "https://example.com/v1/responses".to_string(),
                header_rules: None,
                body_rules: None,
                max_retries: None,
                custom_path: None,
                config: None,
                format_acceptance_config: None,
                proxy: None,
            },
            key: GatewayProviderTransportKey {
                id: format!("{provider_id}-pool-group"),
                provider_id: provider_id.to_string(),
                name: format!("{provider_id}-pool-group"),
                auth_type: "oauth".to_string(),
                is_active: true,
                api_formats: Some(vec!["openai:responses".to_string()]),
                auth_type_by_format: None,
                allow_auth_channel_mismatch_formats: None,
                allowed_models: None,
                capabilities: None,
                rate_multipliers: None,
                global_priority_by_format: None,
                expires_at_unix_secs: None,
                proxy: None,
                fingerprint: None,
                upstream_metadata: None,
                decrypted_api_key: "secret".to_string(),
                decrypted_auth_config: None,
            },
        }),
    }
}

fn routing_policy_with_allowed_keys<const N: usize>(key_ids: [&str; N]) -> ResolvedRoutingPolicy {
    ResolvedRoutingPolicy {
        billing_multiplier: 1.0,
        group_name: None,
        group_id: Some("routing-group-1".to_string()),
        group_version: Some(1),
        selection_source: "test".to_string(),
        requested_model: "gpt-5".to_string(),
        resolved_model: "gpt-5".to_string(),
        priority_mode: RoutingSetPriorityMode::Provider,
        scheduling_mode: RoutingSchedulingMode::CacheAffinity,
        keep_priority_on_conversion: false,
        sticky_key_attempts: aether_routing_core::DEFAULT_STICKY_KEY_ATTEMPTS,
        execution_policy: Default::default(),
        ranking_overlay: RankingOverlay {
            allowed_keys: key_ids.into_iter().map(str::to_string).collect(),
            ..RankingOverlay::default()
        },
        mutation_plan: Default::default(),
        pool_policy_overrides: BTreeMap::new(),
        matched_rules: Vec::new(),
    }
}

fn sample_eligible_candidate(
    provider_id: &str,
    endpoint_id: &str,
    key_id: &str,
    internal_priority: i32,
    provider_config: Option<serde_json::Value>,
) -> EligibleLocalExecutionCandidate {
    EligibleLocalExecutionCandidate {
        kind: if provider_config.is_some() {
            LocalExecutionCandidateKind::PoolGroup
        } else {
            LocalExecutionCandidateKind::SingleKey
        },
        candidate: SchedulerMinimalCandidateSelectionCandidate {
            provider_id: provider_id.to_string(),
            provider_name: provider_id.to_string(),
            provider_type: "codex".to_string(),
            provider_priority: 10,
            endpoint_id: endpoint_id.to_string(),
            endpoint_api_format: "openai:chat".to_string(),
            key_id: key_id.to_string(),
            key_name: key_id.to_string(),
            key_auth_type: "api_key".to_string(),
            key_internal_priority: internal_priority,
            key_global_priority_for_format: Some(1),
            key_capabilities: None,
            model_id: "model-1".to_string(),
            global_model_id: "global-model-1".to_string(),
            global_model_name: "gpt-5".to_string(),
            selected_provider_model_name: "gpt-5".to_string(),
            supports_streaming: true,
            mapping_matched_model: None,
        },
        provider_api_format: "openai:chat".to_string(),
        orchestration: LocalExecutionCandidateMetadata::default(),
        ranking: None,
        transport: Arc::new(crate::ai_serving::GatewayProviderTransportSnapshot {
            provider: GatewayProviderTransportProvider {
                id: provider_id.to_string(),
                name: provider_id.to_string(),
                provider_type: "codex".to_string(),
                website: None,
                is_active: true,
                keep_priority_on_conversion: false,
                enable_format_conversion: false,
                concurrent_limit: None,
                max_retries: None,
                proxy: None,
                request_timeout_secs: None,
                stream_first_byte_timeout_secs: None,
                config: provider_config,
            },
            endpoint: GatewayProviderTransportEndpoint {
                id: endpoint_id.to_string(),
                provider_id: provider_id.to_string(),
                api_format: "openai:chat".to_string(),
                api_family: Some("openai".to_string()),
                endpoint_kind: Some("chat".to_string()),
                is_active: true,
                base_url: "https://example.com".to_string(),
                header_rules: None,
                body_rules: None,
                max_retries: None,
                custom_path: None,
                config: None,
                format_acceptance_config: None,
                proxy: None,
            },
            key: GatewayProviderTransportKey {
                id: key_id.to_string(),
                provider_id: provider_id.to_string(),
                name: key_id.to_string(),
                auth_type: "api_key".to_string(),
                is_active: true,
                api_formats: Some(vec!["openai:chat".to_string()]),
                auth_type_by_format: None,
                allow_auth_channel_mismatch_formats: None,

                allowed_models: None,
                capabilities: None,
                rate_multipliers: None,
                global_priority_by_format: None,
                expires_at_unix_secs: None,
                proxy: None,
                fingerprint: None,
                upstream_metadata: None,
                decrypted_api_key: "secret".to_string(),
                decrypted_auth_config: None,
            },
        }),
    }
}
