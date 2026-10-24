use super::*;
use crate::data::GatewayDataState;
use crate::AppState;
use aether_crypto::DEVELOPMENT_ENCRYPTION_KEY;
use aether_data::repository::candidate_selection::InMemoryMinimalCandidateSelectionReadRepository;
use aether_data::repository::global_models::InMemoryGlobalModelReadRepository;
use aether_data::repository::provider_catalog::InMemoryProviderCatalogReadRepository;
use aether_data::DataLayerError;
use aether_data_contracts::repository::candidate_selection::{
    MinimalCandidateSelectionReadRepository, StoredPoolKeyCandidateRowsByKeyIdsQuery,
    StoredPoolKeyCandidateRowsQuery, StoredProviderModelMapping,
};
use aether_data_contracts::repository::global_models::{
    GlobalModelReadRepository, StoredPublicGlobalModel,
};
use aether_data_contracts::repository::provider_catalog::{
    StoredProviderCatalogEndpoint, StoredProviderCatalogKey, StoredProviderCatalogProvider,
};
use async_trait::async_trait;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

#[test]
fn compaction_operation_excludes_non_responses_provider_formats() {
    assert_eq!(
        request_candidate_api_formats_for_operation("openai:responses", true, Some("compact"),),
        vec!["openai:responses"]
    );
    assert_eq!(
        request_candidate_api_formats_for_operation("openai:responses", true, None),
        vec![
            "openai:responses",
            "openai:chat",
            "claude:messages",
            "gemini:generate_content"
        ]
    );
    assert_eq!(
        request_candidate_api_formats_for_operation(
            "openai:responses:compact",
            false,
            Some("compact"),
        ),
        vec!["openai:responses:compact"]
    );
}

#[derive(Default)]
struct EmptyFallbackCountingRepository {
    fallback_reads: AtomicUsize,
}

struct PagedFallbackRepository {
    total_rows: u32,
    page_queries: Mutex<Vec<StoredApiFormatCandidateRowsQuery>>,
}

impl PagedFallbackRepository {
    fn new(total_rows: u32) -> Self {
        Self {
            total_rows,
            page_queries: Mutex::new(Vec::new()),
        }
    }

    fn page_queries(&self) -> Vec<StoredApiFormatCandidateRowsQuery> {
        self.page_queries
            .lock()
            .expect("fallback query lock")
            .clone()
    }
}

#[async_trait]
impl MinimalCandidateSelectionReadRepository for PagedFallbackRepository {
    async fn list_for_exact_api_format(
        &self,
        _api_format: &str,
    ) -> Result<Vec<StoredMinimalCandidateSelectionRow>, DataLayerError> {
        panic!("routing fallback must not use the unbounded API-format query")
    }

    async fn list_for_exact_api_format_page(
        &self,
        query: &StoredApiFormatCandidateRowsQuery,
    ) -> Result<Vec<StoredMinimalCandidateSelectionRow>, DataLayerError> {
        self.page_queries
            .lock()
            .expect("fallback query lock")
            .push(query.clone());
        if normalize_api_format(&query.api_format) != "openai:chat" {
            return Ok(Vec::new());
        }
        let end = query
            .offset
            .saturating_add(query.limit)
            .min(self.total_rows);
        Ok((query.offset..end)
            .map(|index| {
                standard_candidate_row(
                    format!("fallback-provider-{index:04}").as_str(),
                    "openai:chat",
                    i32::try_from(index).expect("test provider priority should fit"),
                )
            })
            .collect())
    }

    async fn list_for_exact_api_format_and_global_model(
        &self,
        _api_format: &str,
        _global_model_name: &str,
    ) -> Result<Vec<StoredMinimalCandidateSelectionRow>, DataLayerError> {
        Ok(Vec::new())
    }

    async fn list_for_exact_api_format_and_requested_model(
        &self,
        _api_format: &str,
        _requested_model_name: &str,
    ) -> Result<Vec<StoredMinimalCandidateSelectionRow>, DataLayerError> {
        Ok(Vec::new())
    }

    async fn list_for_exact_api_format_and_requested_model_page(
        &self,
        _query: &StoredRequestedModelCandidateRowsQuery,
    ) -> Result<Vec<StoredMinimalCandidateSelectionRow>, DataLayerError> {
        Ok(Vec::new())
    }

    async fn list_pool_key_rows_for_group(
        &self,
        _query: &StoredPoolKeyCandidateRowsQuery,
    ) -> Result<Vec<StoredMinimalCandidateSelectionRow>, DataLayerError> {
        Ok(Vec::new())
    }

    async fn list_pool_key_rows_for_group_key_ids(
        &self,
        _query: &StoredPoolKeyCandidateRowsByKeyIdsQuery,
    ) -> Result<Vec<StoredMinimalCandidateSelectionRow>, DataLayerError> {
        Ok(Vec::new())
    }
}

impl EmptyFallbackCountingRepository {
    fn fallback_reads(&self) -> usize {
        self.fallback_reads.load(Ordering::Acquire)
    }
}

#[async_trait]
impl MinimalCandidateSelectionReadRepository for EmptyFallbackCountingRepository {
    async fn list_for_exact_api_format(
        &self,
        _api_format: &str,
    ) -> Result<Vec<StoredMinimalCandidateSelectionRow>, DataLayerError> {
        self.fallback_reads.fetch_add(1, Ordering::AcqRel);
        Ok(Vec::new())
    }

    async fn list_for_exact_api_format_and_global_model(
        &self,
        _api_format: &str,
        _global_model_name: &str,
    ) -> Result<Vec<StoredMinimalCandidateSelectionRow>, DataLayerError> {
        Ok(Vec::new())
    }

    async fn list_for_exact_api_format_and_requested_model(
        &self,
        _api_format: &str,
        _requested_model_name: &str,
    ) -> Result<Vec<StoredMinimalCandidateSelectionRow>, DataLayerError> {
        Ok(Vec::new())
    }

    async fn list_for_exact_api_format_and_requested_model_page(
        &self,
        _query: &StoredRequestedModelCandidateRowsQuery,
    ) -> Result<Vec<StoredMinimalCandidateSelectionRow>, DataLayerError> {
        Ok(Vec::new())
    }

    async fn list_pool_key_rows_for_group(
        &self,
        _query: &StoredPoolKeyCandidateRowsQuery,
    ) -> Result<Vec<StoredMinimalCandidateSelectionRow>, DataLayerError> {
        Ok(Vec::new())
    }

    async fn list_pool_key_rows_for_group_key_ids(
        &self,
        _query: &StoredPoolKeyCandidateRowsByKeyIdsQuery,
    ) -> Result<Vec<StoredMinimalCandidateSelectionRow>, DataLayerError> {
        Ok(Vec::new())
    }
}

fn unrestricted_auth_snapshot() -> GatewayAuthApiKeySnapshot {
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

#[tokio::test]
async fn empty_fallback_is_scanned_once_and_then_skipped_in_memory() {
    let repository = Arc::new(EmptyFallbackCountingRepository::default());
    let data_state =
        GatewayDataState::with_minimal_candidate_selection_reader_for_tests(repository.clone());
    let app = AppState::new()
        .expect("gateway state should build")
        .with_data_state_for_tests(data_state);
    let auth_snapshot = unrestricted_auth_snapshot();
    let model_directive_policy =
        crate::system_features::ModelDirectivePolicySnapshot::load(&app).await;
    let mut cursor = LocalCandidatePreselectionPageCursor::new(
        PlannerAppState::new(&app),
        &model_directive_policy,
        "openai:search",
        "missing-model",
        None,
        false,
        None,
        &auth_snapshot,
        None,
        None,
        None,
        true,
        LocalCandidatePreselectionKeyMode::ProviderEndpointKeyModelAndApiFormat,
        false,
        None,
    )
    .await;

    assert!(cursor
        .next_page()
        .await
        .expect("empty preselection should succeed")
        .is_none());
    assert_eq!(repository.fallback_reads(), 1);
    assert!(cursor.api_format_is_exhausted("openai:search"));

    // The second target must not reacquire the gate or rescan the empty
    // fallback after the format was proven exhausted.
    assert!(cursor
        .next_page()
        .await
        .expect("exhausted preselection should succeed")
        .is_none());
    assert_eq!(repository.fallback_reads(), 1);
}

#[tokio::test]
async fn restart_scan_clears_exhaustion_and_allows_fallback_to_be_read_again() {
    let repository = Arc::new(EmptyFallbackCountingRepository::default());
    let data_state =
        GatewayDataState::with_minimal_candidate_selection_reader_for_tests(repository.clone());
    let app = AppState::new()
        .expect("gateway state should build")
        .with_data_state_for_tests(data_state);
    let auth_snapshot = unrestricted_auth_snapshot();
    let model_directive_policy =
        crate::system_features::ModelDirectivePolicySnapshot::load(&app).await;
    let mut cursor = LocalCandidatePreselectionPageCursor::new(
        PlannerAppState::new(&app),
        &model_directive_policy,
        "openai:search",
        "missing-model",
        None,
        false,
        None,
        &auth_snapshot,
        None,
        None,
        None,
        true,
        LocalCandidatePreselectionKeyMode::ProviderEndpointKeyModelAndApiFormat,
        false,
        None,
    )
    .await;

    assert!(cursor
        .next_page()
        .await
        .expect("initial scan should succeed")
        .is_none());
    assert_eq!(repository.fallback_reads(), 1);
    cursor.restart_scan();
    assert!(!cursor.api_format_is_exhausted("openai:search"));
    assert!(cursor
        .next_page()
        .await
        .expect("restarted scan should succeed")
        .is_none());
    assert_eq!(repository.fallback_reads(), 2);
}

#[tokio::test]
async fn fallback_can_supply_a_real_second_candidate_after_fast_path_page() {
    let mut first = standard_candidate_row("provider-first", "openai:chat", 0);
    first.global_model_name = "gpt-5".to_string();
    first.global_model_mappings = Some(vec!["gpt-5(?:\\.\\d+)?".to_string()]);
    first.model_provider_model_name = "gpt-5.1".to_string();

    let mut second = standard_candidate_row("provider-second", "openai:chat", 1);
    second.global_model_name = "gpt-5".to_string();
    second.global_model_mappings = Some(vec!["gpt-5(?:\\.\\d+)?".to_string()]);
    second.model_provider_model_name = "gpt-5-secondary".to_string();

    let repository: Arc<dyn MinimalCandidateSelectionReadRepository> =
        Arc::new(InMemoryMinimalCandidateSelectionReadRepository::seed([
            first, second,
        ]));
    let data_state =
        GatewayDataState::with_minimal_candidate_selection_reader_for_tests(repository);
    let app = AppState::new()
        .expect("gateway state should build")
        .with_data_state_for_tests(data_state);
    let auth_snapshot = unrestricted_auth_snapshot();
    let model_directive_policy =
        crate::system_features::ModelDirectivePolicySnapshot::load(&app).await;
    let mut cursor = LocalCandidatePreselectionPageCursor::new(
        PlannerAppState::new(&app),
        &model_directive_policy,
        "openai:chat",
        "gpt-5.1",
        None,
        false,
        None,
        &auth_snapshot,
        None,
        None,
        None,
        true,
        LocalCandidatePreselectionKeyMode::ProviderEndpointKeyModelAndApiFormat,
        false,
        None,
    )
    .await;

    let first_page = cursor
        .next_page()
        .await
        .expect("fast-path candidate should load")
        .expect("first candidate should be present");
    assert_eq!(first_page.candidates.len(), 1);
    assert_eq!(first_page.candidates[0].provider_id, "provider-first");

    let second_page = cursor
        .next_page()
        .await
        .expect("fallback candidate should load")
        .expect("second candidate should not be skipped");
    assert_eq!(second_page.candidates.len(), 1);
    assert_eq!(second_page.candidates[0].provider_id, "provider-second");
    assert!(cursor
        .next_page()
        .await
        .expect("exhausted formats should finish in memory")
        .is_none());
}

#[tokio::test]
async fn routing_policy_excludes_group_disabled_providers_from_candidate_pages() {
    let repository: Arc<dyn MinimalCandidateSelectionReadRepository> =
        Arc::new(InMemoryMinimalCandidateSelectionReadRepository::seed([
            standard_candidate_row("provider-disabled", "openai:chat", 0),
            standard_candidate_row("provider-enabled", "openai:chat", 1),
        ]));
    let app = AppState::new()
        .expect("gateway state should build")
        .with_data_state_for_tests(
            GatewayDataState::with_minimal_candidate_selection_reader_for_tests(repository),
        );
    let auth_snapshot = unrestricted_auth_snapshot();
    let model_directive_policy =
        crate::system_features::ModelDirectivePolicySnapshot::load(&app).await;
    let config = serde_json::from_value(serde_json::json!({
        "disabled_providers": ["provider-disabled"],
        "model_policies": [{
            "model": "*",
            "allowed_providers": ["provider-disabled", "provider-enabled"]
        }]
    }))
    .expect("routing config should parse");
    let routing_policy = aether_routing_core::resolve_routing_policy(
        &config,
        aether_routing_core::RoutingPolicyInput {
            group_id: Some("routing-group-1"),
            group_version: Some(1),
            selection_source: "test",
            requested_model: "gpt-5",
            resolved_model: "gpt-5",
            api_format: "openai:chat",
            user_id: None,
            api_key_id: None,
            headers: &serde_json::json!({}),
            body: &serde_json::json!({}),
            phase: aether_routing_core::RoutingRulePhase::ClientRequest,
        },
    )
    .expect("routing policy should resolve");
    let mut cursor = LocalCandidatePreselectionPageCursor::new(
        PlannerAppState::new(&app),
        &model_directive_policy,
        "openai:chat",
        "gpt-5",
        None,
        false,
        None,
        &auth_snapshot,
        Some(&routing_policy),
        None,
        None,
        true,
        LocalCandidatePreselectionKeyMode::ProviderEndpointKeyModelAndApiFormat,
        false,
        None,
    )
    .await;

    let page = cursor
        .next_page()
        .await
        .expect("routing candidate scan should succeed")
        .expect("the enabled provider should remain");
    assert_eq!(
        page.candidates
            .iter()
            .map(|candidate| candidate.provider_id.as_str())
            .collect::<Vec<_>>(),
        vec!["provider-enabled"]
    );
    assert!(cursor
        .next_page()
        .await
        .expect("routing scan should finish")
        .is_none());
}

#[tokio::test]
async fn model_provider_enablement_filters_candidate_pages_without_affecting_other_models() {
    let mut rows = Vec::new();
    for model in ["model-a", "model-b", "model-c"] {
        for (provider, priority) in [
            ("provider-legacy-disabled", 0),
            ("provider-model-disabled", 1),
            ("provider-other", 2),
            ("provider-inactive", 3),
        ] {
            let mut row = standard_candidate_row(provider, "openai:chat", priority);
            row.global_model_id = format!("global-{model}");
            row.global_model_name = model.into();
            row.model_provider_model_name = model.into();
            row.model_id = format!("{provider}-{model}");
            row.provider_is_active = provider != "provider-inactive";
            rows.push(row);
        }
    }
    let repository: Arc<dyn MinimalCandidateSelectionReadRepository> =
        Arc::new(InMemoryMinimalCandidateSelectionReadRepository::seed(rows));
    let app = AppState::new().unwrap().with_data_state_for_tests(
        GatewayDataState::with_minimal_candidate_selection_reader_for_tests(repository),
    );
    let auth = unrestricted_auth_snapshot();
    let directives = crate::system_features::ModelDirectivePolicySnapshot::load(&app).await;
    let config = serde_json::from_value(serde_json::json!({
        "disabled_providers": ["provider-legacy-disabled"],
        "model_policies": [
            { "model": "model-a", "provider_enabled_overrides": {
                "provider-model-disabled": false, "provider-inactive": true
            } },
            { "model": "model-b", "provider_enabled_overrides": {
                "provider-legacy-disabled": true, "provider-inactive": true
            } }
        ],
        "rules": [{ "id": "legacy-allowlist", "actions": [{
            "type": "restrict_providers", "provider_ids": [
                "provider-legacy-disabled", "provider-model-disabled", "provider-other", "provider-inactive"
            ]
        }] }]
    })).unwrap();
    // Revisit A after B to exercise candidate caches shared by the app.
    for (model, expected) in [
        ("model-a", vec!["provider-other"]),
        (
            "model-b",
            vec![
                "provider-legacy-disabled",
                "provider-model-disabled",
                "provider-other",
            ],
        ),
        ("model-c", vec!["provider-model-disabled", "provider-other"]),
        ("model-a", vec!["provider-other"]),
    ] {
        let policy = aether_routing_core::resolve_routing_policy(
            &config,
            aether_routing_core::RoutingPolicyInput {
                group_id: Some("group-1"),
                group_version: Some(1),
                selection_source: "test",
                requested_model: model,
                resolved_model: model,
                api_format: "openai:chat",
                user_id: None,
                api_key_id: None,
                headers: &serde_json::json!({}),
                body: &serde_json::json!({}),
                phase: aether_routing_core::RoutingRulePhase::ClientRequest,
            },
        )
        .unwrap();
        let mut cursor = LocalCandidatePreselectionPageCursor::new(
            PlannerAppState::new(&app),
            &directives,
            "openai:chat",
            model,
            None,
            false,
            None,
            &auth,
            Some(&policy),
            None,
            None,
            true,
            LocalCandidatePreselectionKeyMode::ProviderEndpointKeyModelAndApiFormat,
            true,
            None,
        )
        .await;
        let mut providers = Vec::new();
        while let Some(page) = cursor.next_page().await.unwrap() {
            providers.extend(
                page.candidates
                    .into_iter()
                    .map(|candidate| candidate.provider_id),
            );
        }
        providers.sort();
        assert_eq!(
            providers, expected,
            "provider enablement must remain isolated for {model}"
        );
    }
}

#[tokio::test]
async fn routing_policy_collects_candidate_pages_before_final_ranking() {
    let rows = (0..300)
        .map(|index| {
            standard_candidate_row(
                format!("provider-{index:03}").as_str(),
                "openai:chat",
                index,
            )
        })
        .collect::<Vec<_>>();
    let repository: Arc<dyn MinimalCandidateSelectionReadRepository> =
        Arc::new(InMemoryMinimalCandidateSelectionReadRepository::seed(rows));
    let app = AppState::new()
        .expect("gateway state should build")
        .with_data_state_for_tests(
            GatewayDataState::with_minimal_candidate_selection_reader_for_tests(repository),
        );
    let auth_snapshot = unrestricted_auth_snapshot();
    let model_directive_policy =
        crate::system_features::ModelDirectivePolicySnapshot::load(&app).await;
    let routing_policy = ResolvedRoutingPolicy {
        billing_multiplier: 1.0,
        group_name: None,
        group_id: Some("routing-group-1".to_string()),
        group_version: Some(1),
        selection_source: "test".to_string(),
        requested_model: "gpt-5".to_string(),
        resolved_model: "gpt-5".to_string(),
        priority_mode: aether_routing_core::RoutingSetPriorityMode::Provider,
        scheduling_mode: aether_routing_core::RoutingSchedulingMode::FixedOrder,
        keep_priority_on_conversion: false,
        sticky_key_attempts: aether_routing_core::DEFAULT_STICKY_KEY_ATTEMPTS,
        execution_policy: Default::default(),
        ranking_overlay: Default::default(),
        mutation_plan: Default::default(),
        pool_policy_overrides: Default::default(),
        matched_rules: Vec::new(),
    };
    let mut cursor = LocalCandidatePreselectionPageCursor::new(
        PlannerAppState::new(&app),
        &model_directive_policy,
        "openai:chat",
        "gpt-5",
        None,
        false,
        None,
        &auth_snapshot,
        Some(&routing_policy),
        None,
        None,
        true,
        LocalCandidatePreselectionKeyMode::ProviderEndpointKeyModelAndApiFormat,
        false,
        None,
    )
    .await;

    let candidates = cursor
        .next_page()
        .await
        .expect("routing candidate scan should succeed")
        .expect("routing candidates should be present")
        .candidates;

    assert_eq!(candidates.len(), 300);
    assert!(cursor
        .next_page()
        .await
        .expect("routing scan should be exhausted")
        .is_none());
}

#[tokio::test]
async fn routing_fallback_uses_bounded_api_format_pages() {
    let repository = Arc::new(PagedFallbackRepository::new(
        REQUESTED_MODEL_MAX_COLLECTED_ROWS + REQUESTED_MODEL_CANDIDATE_PAGE_SIZE,
    ));
    let app = AppState::new()
        .expect("gateway state should build")
        .with_data_state_for_tests(
            GatewayDataState::with_minimal_candidate_selection_reader_for_tests(repository.clone()),
        );
    let auth_snapshot = unrestricted_auth_snapshot();
    let model_directive_policy =
        crate::system_features::ModelDirectivePolicySnapshot::load(&app).await;
    let routing_policy = ResolvedRoutingPolicy {
        billing_multiplier: 1.0,
        group_name: None,
        group_id: Some("routing-group-fallback".to_string()),
        group_version: Some(1),
        selection_source: "test".to_string(),
        requested_model: "gpt-5".to_string(),
        resolved_model: "gpt-5".to_string(),
        priority_mode: aether_routing_core::RoutingSetPriorityMode::Provider,
        scheduling_mode: aether_routing_core::RoutingSchedulingMode::FixedOrder,
        keep_priority_on_conversion: false,
        sticky_key_attempts: aether_routing_core::DEFAULT_STICKY_KEY_ATTEMPTS,
        execution_policy: Default::default(),
        ranking_overlay: Default::default(),
        mutation_plan: Default::default(),
        pool_policy_overrides: Default::default(),
        matched_rules: Vec::new(),
    };
    let mut cursor = LocalCandidatePreselectionPageCursor::new(
        PlannerAppState::new(&app),
        &model_directive_policy,
        "openai:chat",
        "gpt-5",
        None,
        false,
        None,
        &auth_snapshot,
        Some(&routing_policy),
        None,
        None,
        true,
        LocalCandidatePreselectionKeyMode::ProviderEndpointKeyModelAndApiFormat,
        false,
        None,
    )
    .await;

    let candidates = cursor
        .next_page()
        .await
        .expect("routing fallback scan should succeed")
        .expect("routing fallback candidates should be present")
        .candidates;

    assert_eq!(
        candidates.len(),
        REQUESTED_MODEL_MAX_COLLECTED_ROWS as usize
    );
    assert!(cursor
        .next_page()
        .await
        .expect("bounded routing fallback should be exhausted")
        .is_none());
    let page_queries = repository
        .page_queries()
        .into_iter()
        .filter(|query| normalize_api_format(&query.api_format) == "openai:chat")
        .collect::<Vec<_>>();
    assert_eq!(
        page_queries.len(),
        (REQUESTED_MODEL_MAX_COLLECTED_ROWS / REQUESTED_MODEL_CANDIDATE_PAGE_SIZE) as usize
    );
    for (index, query) in page_queries.iter().enumerate() {
        assert_eq!(query.limit, REQUESTED_MODEL_CANDIDATE_PAGE_SIZE);
        assert_eq!(
            query.offset,
            u32::try_from(index).expect("page index should fit")
                * REQUESTED_MODEL_CANDIDATE_PAGE_SIZE
        );
    }
}

#[tokio::test]
async fn priority_page_cache_requires_fixed_order_or_explicit_affinity() {
    let repository: Arc<dyn MinimalCandidateSelectionReadRepository> =
        Arc::new(InMemoryMinimalCandidateSelectionReadRepository::seed(
            Vec::<StoredMinimalCandidateSelectionRow>::new(),
        ));
    let data_state =
        GatewayDataState::with_minimal_candidate_selection_reader_for_tests(repository);
    let app = AppState::new()
        .expect("gateway state should build")
        .with_data_state_for_tests(data_state);
    let auth_snapshot = unrestricted_auth_snapshot();
    let model_directive_policy =
        crate::system_features::ModelDirectivePolicySnapshot::load(&app).await;
    let mut cursor = LocalCandidatePreselectionPageCursor::new(
        PlannerAppState::new(&app),
        &model_directive_policy,
        "openai:chat",
        "gpt-5",
        None,
        true,
        None,
        &auth_snapshot,
        None,
        None,
        None,
        true,
        LocalCandidatePreselectionKeyMode::ProviderEndpointKeyModelAndApiFormat,
        true,
        None,
    )
    .await;
    cursor.mark_priority_page_emitted_for_tests();

    cursor.ordering_config.scheduling_mode = SchedulerSchedulingMode::CacheAffinity;
    assert!(!cursor.should_cache_current_priority_resolved_page());

    cursor.client_session_affinity =
        Some(aether_scheduler_core::ClientSessionAffinity::from_session_key("session-1"));
    assert!(cursor.should_cache_current_priority_resolved_page());

    cursor.ordering_config.scheduling_mode = SchedulerSchedulingMode::FixedOrder;
    assert!(cursor.should_cache_current_priority_resolved_page());

    cursor.ordering_config.scheduling_mode = SchedulerSchedulingMode::LoadBalance;
    assert!(!cursor.should_cache_current_priority_resolved_page());
}

fn openai_responses_mapping_row() -> StoredMinimalCandidateSelectionRow {
    StoredMinimalCandidateSelectionRow {
        provider_id: "provider-openai-responses-mapped-1".to_string(),
        provider_name: "openai".to_string(),
        provider_type: "custom".to_string(),
        provider_priority: 10,
        provider_is_active: true,
        endpoint_id: "endpoint-openai-responses-mapped-1".to_string(),
        endpoint_api_format: "openai:responses".to_string(),
        endpoint_api_family: Some("openai".to_string()),
        endpoint_kind: Some("chat".to_string()),
        endpoint_is_active: true,
        key_id: "key-openai-responses-mapped-1".to_string(),
        key_name: "prod".to_string(),
        key_auth_type: "bearer".to_string(),
        key_is_active: true,
        key_api_formats: Some(vec!["openai:responses".to_string()]),
        key_allowed_models: None,
        key_capabilities: None,
        key_internal_priority: 5,
        key_global_priority_by_format: Some(serde_json::json!({"openai:responses": 1})),
        model_id: "model-openai-responses-mapped-1".to_string(),
        global_model_id: "global-model-openai-responses-mapped-1".to_string(),
        global_model_name: "gpt-5".to_string(),
        global_model_mappings: Some(vec!["gpt-5(?:\\.\\d+)?".to_string()]),
        global_model_supports_streaming: Some(true),
        model_provider_model_name: "gpt-5-upstream".to_string(),
        model_provider_model_mappings: None,
        model_supports_streaming: Some(true),
        model_is_active: true,
        model_is_available: true,
    }
}

fn public_global_model(name: &str) -> StoredPublicGlobalModel {
    StoredPublicGlobalModel {
        id: format!("global-model-{name}"),
        name: name.to_string(),
        display_name: None,
        is_active: true,
        default_price_per_request: None,
        default_tiered_pricing: None,
        supported_capabilities: None,
        config: None,
        usage_count: 0,
    }
}

/// The cursor provider reaches its upstream under a name that belongs to another
/// global model. A `claude:messages` client asking for `gemini-3.8-flash` has to
/// land on the provider bound to that global model — format conversion and all —
/// rather than on the one that only borrows the name on the way out, which is the
/// one an API-format-ordered scan reaches first.
#[tokio::test]
async fn paged_preselection_keeps_a_global_model_name_from_a_provider_alias() {
    let mut aliasing = standard_candidate_row("ursor", "claude:messages", 1);
    aliasing.global_model_id = "global-model-gemini-3.8-flash-cursor".to_string();
    aliasing.global_model_name = "gemini-3.8-flash-cursor".to_string();
    aliasing.model_provider_model_name = "gemini-3.8-flash-cursor".to_string();
    aliasing.model_provider_model_mappings = Some(vec![StoredProviderModelMapping {
        name: "gemini-3.8-flash".to_string(),
        priority: 1,
        api_formats: None,
        endpoint_ids: None,
        operations: None,
    }]);

    let mut bound = standard_candidate_row("anti", "gemini:generate_content", 2);
    bound.global_model_id = "global-model-gemini-3.8-flash".to_string();
    bound.global_model_name = "gemini-3.8-flash".to_string();
    bound.model_provider_model_name = "gemini-3.8-flash".to_string();

    let repository: Arc<dyn MinimalCandidateSelectionReadRepository> =
        Arc::new(InMemoryMinimalCandidateSelectionReadRepository::seed([
            aliasing, bound,
        ]));
    let global_models: Arc<dyn GlobalModelReadRepository> =
        Arc::new(InMemoryGlobalModelReadRepository::seed([
            public_global_model("gemini-3.8-flash"),
            public_global_model("gemini-3.8-flash-cursor"),
        ]));
    let data_state =
        GatewayDataState::with_minimal_candidate_selection_reader_for_tests(repository)
            .with_global_model_reader(global_models);
    let app = AppState::new()
        .expect("gateway state should build")
        .with_data_state_for_tests(data_state);
    let auth_snapshot = unrestricted_auth_snapshot();
    let model_directive_policy =
        crate::system_features::ModelDirectivePolicySnapshot::load(&app).await;
    let mut cursor = LocalCandidatePreselectionPageCursor::new(
        PlannerAppState::new(&app),
        &model_directive_policy,
        "claude:messages",
        "gemini-3.8-flash",
        None,
        false,
        None,
        &auth_snapshot,
        None,
        None,
        None,
        true,
        LocalCandidatePreselectionKeyMode::ProviderEndpointKeyModelAndApiFormat,
        true,
        None,
    )
    .await;

    let page = cursor
        .next_page()
        .await
        .expect("preselection should succeed")
        .expect("the bound provider should still be reachable");

    assert_eq!(page.candidates.len(), 1);
    assert_eq!(page.candidates[0].provider_name, "anti");
    assert_eq!(page.candidates[0].global_model_name, "gemini-3.8-flash");
    assert_eq!(
        page.candidates[0].endpoint_api_format,
        "gemini:generate_content"
    );
}

fn standard_candidate_row(
    provider_id: &str,
    api_format: &str,
    provider_priority: i32,
) -> StoredMinimalCandidateSelectionRow {
    StoredMinimalCandidateSelectionRow {
        provider_id: provider_id.to_string(),
        provider_name: provider_id.to_string(),
        provider_type: "custom".to_string(),
        provider_priority,
        provider_is_active: true,
        endpoint_id: format!("endpoint-{provider_id}"),
        endpoint_api_format: api_format.to_string(),
        endpoint_api_family: api_format.split(':').next().map(ToOwned::to_owned),
        endpoint_kind: api_format.split(':').nth(1).map(ToOwned::to_owned),
        endpoint_is_active: true,
        key_id: format!("key-{provider_id}"),
        key_name: format!("{provider_id}-key"),
        key_auth_type: "api_key".to_string(),
        key_is_active: true,
        key_api_formats: Some(vec![api_format.to_string()]),
        key_allowed_models: None,
        key_capabilities: None,
        key_internal_priority: 0,
        key_global_priority_by_format: None,
        model_id: format!("model-{provider_id}"),
        global_model_id: "global-model-gpt-5".to_string(),
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

fn provider_catalog_for_standard_row(
    row: &StoredMinimalCandidateSelectionRow,
    keep_priority_on_conversion: bool,
) -> (
    StoredProviderCatalogProvider,
    StoredProviderCatalogEndpoint,
    StoredProviderCatalogKey,
) {
    let provider = StoredProviderCatalogProvider::new(
        row.provider_id.clone(),
        row.provider_name.clone(),
        Some("https://provider.example".to_string()),
        row.provider_type.clone(),
    )
    .expect("provider should build")
    .with_transport_fields(
        true,
        keep_priority_on_conversion,
        true,
        None,
        None,
        None,
        None,
        None,
        None,
    )
    .with_routing_fields(row.provider_priority);
    let endpoint = StoredProviderCatalogEndpoint::new(
        row.endpoint_id.clone(),
        row.provider_id.clone(),
        row.endpoint_api_format.clone(),
        row.endpoint_api_family.clone(),
        row.endpoint_kind.clone(),
        row.endpoint_is_active,
    )
    .expect("endpoint should build")
    .with_transport_fields(
        "https://provider.example/v1".to_string(),
        None,
        None,
        None,
        None,
        None,
        None,
        None,
    )
    .expect("endpoint transport should build");
    let credential_state = crate::tests::support::credential_fixture_state();
    let encrypted_api_key = credential_state
        .seal_provider_catalog_key_api_key(
            row.provider_id.as_str(),
            row.key_id.as_str(),
            "plain-upstream-key",
        )
        .expect("api key should encrypt");
    let key = StoredProviderCatalogKey::new(
        row.key_id.clone(),
        row.provider_id.clone(),
        row.key_name.clone(),
        row.key_auth_type.clone(),
        None,
        row.key_is_active,
    )
    .expect("key should build")
    .with_transport_fields(
        Some(serde_json::json!([row.endpoint_api_format.clone()])),
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
    (provider, endpoint, key)
}

fn opg_deepseek_row(
    endpoint_id: &str,
    api_format: &str,
    key_id: &str,
    key_name: &str,
    key_allowed_models: Vec<&str>,
    key_internal_priority: i32,
) -> StoredMinimalCandidateSelectionRow {
    StoredMinimalCandidateSelectionRow {
        provider_id: "provider-opg".to_string(),
        provider_name: "OpenCode Go".to_string(),
        provider_type: "custom".to_string(),
        provider_priority: 1,
        provider_is_active: true,
        endpoint_id: endpoint_id.to_string(),
        endpoint_api_format: api_format.to_string(),
        endpoint_api_family: None,
        endpoint_kind: Some("chat".to_string()),
        endpoint_is_active: true,
        key_id: key_id.to_string(),
        key_name: key_name.to_string(),
        key_auth_type: "api_key".to_string(),
        key_is_active: true,
        key_api_formats: Some(vec![api_format.to_string()]),
        key_allowed_models: Some(
            key_allowed_models
                .into_iter()
                .map(ToOwned::to_owned)
                .collect(),
        ),
        key_capabilities: None,
        key_internal_priority,
        key_global_priority_by_format: None,
        model_id: "model-opg-deepseek-v4-pro".to_string(),
        global_model_id: "global-model-deepseek-v4-pro".to_string(),
        global_model_name: "deepseek-v4-pro".to_string(),
        global_model_mappings: None,
        global_model_supports_streaming: Some(true),
        model_provider_model_name: "deepseek-v4-pro".to_string(),
        model_provider_model_mappings: Some(vec![StoredProviderModelMapping {
            name: "deepseek-v4-pro".to_string(),
            priority: 1,
            api_formats: None,
            endpoint_ids: Some(vec!["endpoint-opg-openai".to_string()]),
            operations: None,
        }]),
        model_supports_streaming: Some(true),
        model_is_active: true,
        model_is_available: true,
    }
}

#[tokio::test]
async fn paged_preselection_falls_back_to_format_scan_for_directive_mapping_match() {
    let repository: Arc<dyn MinimalCandidateSelectionReadRepository> =
        Arc::new(InMemoryMinimalCandidateSelectionReadRepository::seed([
            openai_responses_mapping_row(),
        ]));
    let data_state =
        GatewayDataState::with_minimal_candidate_selection_reader_for_tests(repository)
            .with_system_config_values_for_tests([(
                crate::system_features::ENABLE_MODEL_DIRECTIVES_CONFIG_KEY.to_string(),
                serde_json::json!(true),
            )]);
    let app = AppState::new()
        .expect("gateway state should build")
        .with_data_state_for_tests(data_state);
    let auth_snapshot = unrestricted_auth_snapshot();
    let model_directive_policy =
        crate::system_features::ModelDirectivePolicySnapshot::load(&app).await;
    let mut cursor = LocalCandidatePreselectionPageCursor::new(
        PlannerAppState::new(&app),
        &model_directive_policy,
        "claude:messages",
        "gpt-5.5-xhigh",
        None,
        false,
        None,
        &auth_snapshot,
        None,
        None,
        None,
        true,
        LocalCandidatePreselectionKeyMode::ProviderEndpointKeyModelAndApiFormat,
        true,
        None,
    )
    .await;

    let page = cursor
        .next_page()
        .await
        .expect("preselection should succeed")
        .expect("mapping fallback should find a provider");

    assert_eq!(page.skipped_candidates.len(), 0);
    assert_eq!(page.candidates.len(), 1);
    assert_eq!(page.candidates[0].endpoint_api_format, "openai:responses");
    assert_eq!(page.candidates[0].global_model_name, "gpt-5");
    assert_eq!(
        page.candidates[0].selected_provider_model_name,
        "gpt-5-upstream"
    );
}

#[tokio::test]
async fn paged_preselection_prefers_operation_scoped_mapping_for_compaction() {
    let mut row = openai_responses_mapping_row();
    row.global_model_mappings = None;
    row.global_model_name = "gpt-5.6-sol".to_string();
    row.model_provider_model_name = "gpt-5.6-sol".to_string();
    row.model_provider_model_mappings = Some(vec![
        StoredProviderModelMapping {
            name: "gpt-5.6-sol".to_string(),
            priority: 1,
            api_formats: Some(vec!["openai:responses".to_string()]),
            endpoint_ids: None,
            operations: None,
        },
        StoredProviderModelMapping {
            name: "gpt-5.6-terra".to_string(),
            priority: 1,
            api_formats: Some(vec!["openai:responses".to_string()]),
            endpoint_ids: None,
            operations: Some(vec!["compact".to_string()]),
        },
    ]);
    let repository: Arc<dyn MinimalCandidateSelectionReadRepository> =
        Arc::new(InMemoryMinimalCandidateSelectionReadRepository::seed([row]));
    let data_state =
        GatewayDataState::with_minimal_candidate_selection_reader_for_tests(repository);
    let app = AppState::new()
        .expect("gateway state should build")
        .with_data_state_for_tests(data_state);
    let auth_snapshot = unrestricted_auth_snapshot();
    let model_directive_policy =
        crate::system_features::ModelDirectivePolicySnapshot::load(&app).await;
    let mut cursor = LocalCandidatePreselectionPageCursor::new(
        PlannerAppState::new(&app),
        &model_directive_policy,
        "openai:responses",
        "gpt-5.6-sol",
        Some("compact"),
        false,
        None,
        &auth_snapshot,
        None,
        None,
        None,
        true,
        LocalCandidatePreselectionKeyMode::ProviderEndpointKeyModelAndApiFormat,
        true,
        None,
    )
    .await;

    let page = cursor
        .next_page()
        .await
        .expect("preselection should succeed")
        .expect("compact mapping should find a provider");

    assert_eq!(page.candidates.len(), 1);
    assert_eq!(
        page.candidates[0].selected_provider_model_name,
        "gpt-5.6-terra"
    );
}

#[tokio::test]
async fn custom_policy_suffix_uses_the_same_base_model_for_candidate_selection() {
    let mut row = openai_responses_mapping_row();
    row.global_model_name = "deployment-alias".to_string();
    row.global_model_mappings = None;
    row.model_provider_model_name = "gpt-5.6-sol".to_string();
    let repository: Arc<dyn MinimalCandidateSelectionReadRepository> =
        Arc::new(InMemoryMinimalCandidateSelectionReadRepository::seed([row]));
    let data_state =
        GatewayDataState::with_minimal_candidate_selection_reader_for_tests(repository)
            .with_system_config_values_for_tests([
                (
                    crate::system_features::ENABLE_MODEL_DIRECTIVES_CONFIG_KEY.to_string(),
                    serde_json::json!(true),
                ),
                (
                    crate::system_features::MODEL_DIRECTIVES_CONFIG_KEY.to_string(),
                    serde_json::json!({
                        "reasoning_effort": {
                            "api_formats": {
                                "openai:responses": {
                                    "suffixes": ["VendorFuture"],
                                    "mappings": {
                                        "VendorFuture": {
                                            "reasoning": { "context": "all_turns" }
                                        }
                                    }
                                }
                            }
                        }
                    }),
                ),
            ]);
    let app = AppState::new()
        .expect("gateway state should build")
        .with_data_state_for_tests(data_state);
    let auth_snapshot = unrestricted_auth_snapshot();
    let model_directive_policy =
        crate::system_features::ModelDirectivePolicySnapshot::load(&app).await;
    let mut cursor = LocalCandidatePreselectionPageCursor::new(
        PlannerAppState::new(&app),
        &model_directive_policy,
        "openai:responses",
        "deployment-alias-VendorFuture",
        None,
        false,
        None,
        &auth_snapshot,
        None,
        None,
        None,
        true,
        LocalCandidatePreselectionKeyMode::ProviderEndpointKeyModelAndApiFormat,
        true,
        None,
    )
    .await;

    let page = cursor
        .next_page()
        .await
        .expect("preselection should succeed")
        .expect("custom directive base model should resolve a candidate");

    assert_eq!(page.candidates.len(), 1);
    assert_eq!(page.candidates[0].global_model_name, "deployment-alias");
    assert_eq!(
        page.candidates[0].selected_provider_model_name,
        "gpt-5.6-sol"
    );
}

#[tokio::test]
async fn claude_request_uses_cross_format_key_when_same_provider_messages_key_lacks_model() {
    let repository: Arc<dyn MinimalCandidateSelectionReadRepository> =
        Arc::new(InMemoryMinimalCandidateSelectionReadRepository::seed([
            opg_deepseek_row(
                "endpoint-opg-claude",
                "claude:messages",
                "key-opg-messages",
                "OPG Key Messages",
                vec!["glm-5", "glm-5.1", "minimax-m2.5", "minimax-m2.7"],
                1,
            ),
            opg_deepseek_row(
                "endpoint-opg-openai",
                "openai:chat",
                "key-opg-completions",
                "OPG Key Completions",
                vec!["deepseek-v4-pro", "glm-5", "glm-5.1", "minimax-m2.7"],
                10,
            ),
        ]));
    let data_state =
        GatewayDataState::with_minimal_candidate_selection_reader_for_tests(repository);
    let app = AppState::new()
        .expect("gateway state should build")
        .with_data_state_for_tests(data_state);
    let auth_snapshot = unrestricted_auth_snapshot();
    let model_directive_policy =
        crate::system_features::ModelDirectivePolicySnapshot::load(&app).await;
    let mut cursor = LocalCandidatePreselectionPageCursor::new(
        PlannerAppState::new(&app),
        &model_directive_policy,
        "claude:messages",
        "deepseek-v4-pro",
        None,
        false,
        None,
        &auth_snapshot,
        None,
        None,
        None,
        true,
        LocalCandidatePreselectionKeyMode::ProviderEndpointKeyModelAndApiFormat,
        true,
        None,
    )
    .await;

    let page = cursor
        .next_page()
        .await
        .expect("preselection should succeed")
        .expect("openai chat candidate should be found via conversion");

    assert_eq!(page.skipped_candidates.len(), 0);
    assert_eq!(page.candidates.len(), 1);
    assert_eq!(page.candidates[0].endpoint_api_format, "openai:chat");
    assert_eq!(page.candidates[0].key_name, "OPG Key Completions");
    assert_eq!(
        page.candidates[0].selected_provider_model_name,
        "deepseek-v4-pro"
    );
}

#[tokio::test]
async fn first_page_includes_cross_format_candidates_that_keep_conversion_priority() {
    let same_format = standard_candidate_row("provider-claude", "claude:messages", 10);
    let keep_priority_cross =
        standard_candidate_row("provider-openai-responses-keep", "openai:responses", 0);
    let regular_cross =
        standard_candidate_row("provider-openai-responses-regular", "openai:responses", 1);
    let candidate_repository: Arc<dyn MinimalCandidateSelectionReadRepository> =
        Arc::new(InMemoryMinimalCandidateSelectionReadRepository::seed([
            same_format.clone(),
            keep_priority_cross.clone(),
            regular_cross.clone(),
        ]));
    let catalog_items = [
        provider_catalog_for_standard_row(&same_format, false),
        provider_catalog_for_standard_row(&keep_priority_cross, true),
        provider_catalog_for_standard_row(&regular_cross, false),
    ];
    let provider_repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        catalog_items
            .iter()
            .map(|(provider, _, _)| provider.clone())
            .collect(),
        catalog_items
            .iter()
            .map(|(_, endpoint, _)| endpoint.clone())
            .collect(),
        catalog_items
            .iter()
            .map(|(_, _, key)| key.clone())
            .collect(),
    ));
    let data_state =
        GatewayDataState::with_provider_catalog_and_minimal_candidate_selection_for_tests(
            provider_repository,
            candidate_repository,
        )
        .with_encryption_key_for_tests(DEVELOPMENT_ENCRYPTION_KEY);
    let app = AppState::new()
        .expect("gateway state should build")
        .with_data_state_for_tests(data_state);
    let auth_snapshot = unrestricted_auth_snapshot();
    let model_directive_policy =
        crate::system_features::ModelDirectivePolicySnapshot::load(&app).await;
    let mut cursor = LocalCandidatePreselectionPageCursor::new(
        PlannerAppState::new(&app),
        &model_directive_policy,
        "claude:messages",
        "gpt-5",
        None,
        false,
        None,
        &auth_snapshot,
        None,
        None,
        None,
        true,
        LocalCandidatePreselectionKeyMode::ProviderEndpointKeyModelAndApiFormat,
        true,
        None,
    )
    .await;

    let first_page = cursor
        .next_page()
        .await
        .expect("preselection should succeed")
        .expect("same-format and keep-priority conversion candidates should share first page");

    assert_eq!(
        first_page
            .candidates
            .iter()
            .map(|candidate| candidate.provider_id.as_str())
            .collect::<Vec<_>>(),
        vec!["provider-claude", "provider-openai-responses-keep"]
    );

    let (ranked, skipped) =
        super::super::candidate_resolution::resolve_and_rank_logical_local_execution_candidates(
            PlannerAppState::new(&app),
            first_page.candidates,
            "claude:messages",
            Some("gpt-5"),
            Some(&auth_snapshot),
            None,
            None,
            None,
            None,
            None,
            aether_ai_serving::AiCandidateResolutionMode::Standard,
        )
        .await;

    assert!(skipped.is_empty());
    assert_eq!(
        ranked
            .iter()
            .map(|candidate| candidate.candidate.provider_id.as_str())
            .collect::<Vec<_>>(),
        vec!["provider-openai-responses-keep", "provider-claude"]
    );

    let second_page = cursor
        .next_page()
        .await
        .expect("preselection should continue")
        .expect("regular conversion candidate should remain in a later page");
    assert_eq!(
        second_page
            .candidates
            .iter()
            .map(|candidate| candidate.provider_id.as_str())
            .collect::<Vec<_>>(),
        vec!["provider-openai-responses-regular"]
    );
}

#[tokio::test]
async fn fixed_order_prefers_codex_responses_when_conversion_keeps_priority() {
    let mut codex = standard_candidate_row("provider-codex", "openai:responses", 0);
    codex.provider_type = "codex".to_string();
    codex.key_auth_type = "oauth".to_string();
    codex.global_model_name = "gpt-5.4-mini".to_string();
    codex.model_provider_model_name = "gpt-5.4-mini".to_string();
    let mut custom_chat = standard_candidate_row("provider-custom", "openai:chat", 10);
    custom_chat.global_model_name = "gpt-5.4-mini".to_string();
    custom_chat.model_provider_model_name = "gpt-5.4-mini".to_string();

    let candidate_repository: Arc<dyn MinimalCandidateSelectionReadRepository> =
        Arc::new(InMemoryMinimalCandidateSelectionReadRepository::seed([
            codex.clone(),
            custom_chat.clone(),
        ]));
    let catalog_items = [
        provider_catalog_for_standard_row(&codex, false),
        provider_catalog_for_standard_row(&custom_chat, false),
    ];
    let provider_repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        catalog_items
            .iter()
            .map(|(provider, _, _)| provider.clone())
            .collect(),
        catalog_items
            .iter()
            .map(|(_, endpoint, _)| endpoint.clone())
            .collect(),
        catalog_items
            .iter()
            .map(|(_, _, key)| key.clone())
            .collect(),
    ));
    let data_state =
        GatewayDataState::with_provider_catalog_and_minimal_candidate_selection_for_tests(
            provider_repository,
            candidate_repository,
        )
        .with_encryption_key_for_tests(DEVELOPMENT_ENCRYPTION_KEY)
        // Legacy keys deliberately disagree with the routing policy: the
        // resolved policy must be the only source of scheduler ordering.
        .with_system_config_values_for_tests([
            (
                "scheduling_mode".to_string(),
                serde_json::json!("cache_affinity"),
            ),
            (
                "keep_priority_on_conversion".to_string(),
                serde_json::json!(false),
            ),
        ]);
    let app = AppState::new()
        .expect("gateway state should build")
        .with_data_state_for_tests(data_state);
    let auth_snapshot = unrestricted_auth_snapshot();
    let model_directive_policy =
        crate::system_features::ModelDirectivePolicySnapshot::load(&app).await;
    let routing_policy = ResolvedRoutingPolicy {
        billing_multiplier: 1.0,
        group_name: None,
        group_id: Some("routing-group-codex-first".to_string()),
        group_version: Some(1),
        selection_source: "test".to_string(),
        requested_model: "gpt-5.4-mini".to_string(),
        resolved_model: "gpt-5.4-mini".to_string(),
        priority_mode: aether_routing_core::RoutingSetPriorityMode::Provider,
        scheduling_mode: aether_routing_core::RoutingSchedulingMode::FixedOrder,
        keep_priority_on_conversion: true,
        sticky_key_attempts: aether_routing_core::DEFAULT_STICKY_KEY_ATTEMPTS,
        execution_policy: Default::default(),
        ranking_overlay: Default::default(),
        mutation_plan: Default::default(),
        pool_policy_overrides: Default::default(),
        matched_rules: Vec::new(),
    };
    let mut cursor = LocalCandidatePreselectionPageCursor::new(
        PlannerAppState::new(&app),
        &model_directive_policy,
        "openai:chat",
        "gpt-5.4-mini",
        None,
        false,
        None,
        &auth_snapshot,
        Some(&routing_policy),
        None,
        None,
        true,
        LocalCandidatePreselectionKeyMode::ProviderEndpointKeyModelAndApiFormat,
        false,
        None,
    )
    .await;

    let first_page = cursor
        .next_page()
        .await
        .expect("preselection should succeed")
        .expect("Codex and custom candidates should share the priority page");
    assert!(
        first_page.skipped_candidates.is_empty(),
        "priority page unexpectedly skipped candidates: {:?}",
        first_page
            .skipped_candidates
            .iter()
            .map(|candidate| {
                (
                    candidate.candidate.provider_id.as_str(),
                    candidate.skip_reason,
                )
            })
            .collect::<Vec<_>>()
    );
    assert_eq!(
        first_page
            .candidates
            .iter()
            .map(|candidate| candidate.provider_id.as_str())
            .collect::<Vec<_>>(),
        vec!["provider-custom", "provider-codex"]
    );
    let (ranked, skipped) =
        super::super::candidate_resolution::resolve_and_rank_logical_local_execution_candidates(
            PlannerAppState::new(&app),
            first_page.candidates,
            "openai:chat",
            Some("gpt-5.4-mini"),
            Some(&auth_snapshot),
            None,
            None,
            Some(&routing_policy),
            None,
            None,
            aether_ai_serving::AiCandidateResolutionMode::Standard,
        )
        .await;

    assert!(skipped.is_empty());
    assert_eq!(
        ranked
            .iter()
            .map(|candidate| candidate.candidate.provider_id.as_str())
            .collect::<Vec<_>>(),
        vec!["provider-codex", "provider-custom"]
    );
}
