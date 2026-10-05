use aether_data_contracts::repository::routing_profiles::{
    RoutingGroupBindingQuery, RoutingGroupBindingSubject, RoutingGroupLookupKey,
    RoutingGroupReadRepository, StoredRoutingGroup,
};
use aether_routing_core::{config_scopes_model, RoutingGroupConfig};
use thiserror::Error;

pub(crate) const ROUTING_GROUP_HEADER: &str = "x-aether-scheduler-group";

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub(crate) enum GatewayRoutingSelectionError {
    #[error("no enabled routing strategy is configured for this request")]
    NoDefault,
    #[error("routing group was explicitly requested but was not found: {0}")]
    NotFound(String),
    #[error("routing group was explicitly requested but is not enabled: {0}")]
    Disabled(String),
    #[error("routing group was explicitly requested but is not allowed for this principal: {0}")]
    Forbidden(String),
    #[error("routing group repository lookup failed: {0}")]
    Repository(String),
}

#[derive(Debug, Clone, Default)]
pub(crate) struct GatewayRoutingSelectionInput<'a> {
    pub explicit_group: Option<&'a str>,
    pub user_id: Option<&'a str>,
    pub api_key_id: Option<&'a str>,
    pub user_group_ids: &'a [String],
    pub requested_model: Option<&'a str>,
}

const MODEL_CHAIN_SOURCE: &str = "model_chain";

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct GatewayRoutingGroupSelection {
    pub group: Option<StoredRoutingGroup>,
    pub source: String,
}

pub(crate) async fn select_gateway_routing_group(
    repository: &(impl RoutingGroupReadRepository + ?Sized),
    input: GatewayRoutingSelectionInput<'_>,
) -> Result<GatewayRoutingGroupSelection, GatewayRoutingSelectionError> {
    if let Some(explicit) = input
        .explicit_group
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        let group = repository
            .find_routing_group(RoutingGroupLookupKey::Id(explicit))
            .await
            .map_err(repository_selection_error)?;
        let group = match group {
            Some(group) => Some(group),
            None => repository
                .find_routing_group(RoutingGroupLookupKey::Name(explicit))
                .await
                .map_err(repository_selection_error)?,
        };
        let Some(group) = group else {
            return Err(GatewayRoutingSelectionError::NotFound(explicit.to_string()));
        };
        if !group.enabled {
            return Err(GatewayRoutingSelectionError::Disabled(group.id));
        }
        if !explicit_group_allowed(repository, &group, &input).await? {
            return Err(GatewayRoutingSelectionError::Forbidden(group.id));
        }
        return Ok(GatewayRoutingGroupSelection {
            group: Some(group),
            source: "explicit_header".to_string(),
        });
    }

    let has_bindings = repository
        .has_any_routing_group_binding()
        .await
        .map_err(repository_selection_error)?;
    if has_bindings {
        for (subject_type, subject_id, source) in default_binding_candidates(&input) {
            let bindings = repository
                .list_routing_group_bindings(&RoutingGroupBindingQuery {
                    group_id: None,
                    subject_type: Some(subject_type),
                    subject_id: Some(subject_id.to_string()),
                })
                .await
                .map_err(repository_selection_error)?;
            for binding in bindings.into_iter().filter(|binding| binding.is_default) {
                let group = repository
                    .find_routing_group(RoutingGroupLookupKey::Id(&binding.group_id))
                    .await
                    .map_err(repository_selection_error)?;
                if let Some(group) = group.filter(|group| group.enabled) {
                    return Ok(GatewayRoutingGroupSelection {
                        group: Some(group),
                        source: source.to_string(),
                    });
                }
            }
        }
    }

    if let Some(requested_model) = input.requested_model {
        if let Some(group) = select_model_scoped_group(repository, requested_model).await? {
            return Ok(GatewayRoutingGroupSelection {
                group: Some(group),
                source: MODEL_CHAIN_SOURCE.to_string(),
            });
        }
    }

    let system_default = repository
        .find_routing_group(RoutingGroupLookupKey::SystemDefault)
        .await
        .map_err(repository_selection_error)?
        .filter(|group| group.enabled);
    Ok(GatewayRoutingGroupSelection {
        group: system_default,
        source: "system_default".to_string(),
    })
}

pub(crate) fn model_scoped_group_from(
    groups: &[StoredRoutingGroup],
    requested_model: &str,
) -> Option<StoredRoutingGroup> {
    let requested_model = requested_model.trim();
    if requested_model.is_empty() {
        return None;
    }
    let mut ordered = groups.iter().collect::<Vec<_>>();
    ordered.sort_by(|left, right| {
        left.sort_order
            .cmp(&right.sort_order)
            .then_with(|| left.name.cmp(&right.name))
            .then_with(|| left.id.cmp(&right.id))
    });
    ordered.into_iter().find_map(|group| {
        if !group.enabled || group.is_system_default {
            return None;
        }
        let config =
            serde_json::from_value::<RoutingGroupConfig>(group.config_json.clone()).ok()?;
        config_scopes_model(&config, requested_model).then(|| group.clone())
    })
}

async fn select_model_scoped_group(
    repository: &(impl RoutingGroupReadRepository + ?Sized),
    requested_model: &str,
) -> Result<Option<StoredRoutingGroup>, GatewayRoutingSelectionError> {
    let groups = repository
        .list_routing_groups()
        .await
        .map_err(repository_selection_error)?;
    Ok(model_scoped_group_from(&groups, requested_model))
}

async fn explicit_group_allowed(
    repository: &(impl RoutingGroupReadRepository + ?Sized),
    group: &StoredRoutingGroup,
    input: &GatewayRoutingSelectionInput<'_>,
) -> Result<bool, GatewayRoutingSelectionError> {
    if group.is_system_default {
        return Ok(true);
    }
    for (subject_type, subject_id, _) in default_binding_candidates(input) {
        let bindings = repository
            .list_routing_group_bindings(&RoutingGroupBindingQuery {
                group_id: Some(group.id.clone()),
                subject_type: Some(subject_type),
                subject_id: Some(subject_id.to_string()),
            })
            .await
            .map_err(repository_selection_error)?;
        if bindings.iter().any(|binding| binding.allow_explicit_select) {
            return Ok(true);
        }
    }
    Ok(false)
}

fn repository_selection_error(error: impl std::fmt::Display) -> GatewayRoutingSelectionError {
    GatewayRoutingSelectionError::Repository(error.to_string())
}

fn default_binding_candidates<'a>(
    input: &'a GatewayRoutingSelectionInput<'a>,
) -> Vec<(RoutingGroupBindingSubject, &'a str, &'static str)> {
    let mut candidates = Vec::new();
    if let Some(api_key_id) = input.api_key_id {
        candidates.push((
            RoutingGroupBindingSubject::ApiKey,
            api_key_id,
            "api_key_default",
        ));
    }
    if let Some(user_id) = input.user_id {
        candidates.push((RoutingGroupBindingSubject::User, user_id, "user_default"));
    }
    for group_id in input.user_group_ids {
        candidates.push((
            RoutingGroupBindingSubject::UserGroup,
            group_id.as_str(),
            "user_group_default",
        ));
    }
    candidates
}

#[cfg(test)]
mod tests {
    use aether_data::repository::routing_profiles::InMemoryRoutingGroupRepository;
    use aether_data_contracts::repository::routing_profiles::{
        CreateRoutingGroupBindingRecord, CreateRoutingGroupRecord, RoutingGroupWriteRepository,
        StoredRoutingGroupBinding, StoredRoutingGroupVersion,
    };
    use aether_data_contracts::DataLayerError;
    use async_trait::async_trait;
    use serde_json::json;

    use super::*;

    struct FailingRoutingGroupRepository {
        id_lookup_is_missing: bool,
    }

    impl FailingRoutingGroupRepository {
        fn failure<T>() -> Result<T, DataLayerError> {
            Err(DataLayerError::Sql(
                "routing repository unavailable".to_string(),
            ))
        }
    }

    #[async_trait]
    impl RoutingGroupReadRepository for FailingRoutingGroupRepository {
        async fn list_routing_groups(&self) -> Result<Vec<StoredRoutingGroup>, DataLayerError> {
            Self::failure()
        }

        async fn find_routing_group(
            &self,
            lookup: RoutingGroupLookupKey<'_>,
        ) -> Result<Option<StoredRoutingGroup>, DataLayerError> {
            if self.id_lookup_is_missing && matches!(lookup, RoutingGroupLookupKey::Id(_)) {
                return Ok(None);
            }
            Self::failure()
        }

        async fn list_routing_group_bindings(
            &self,
            _query: &RoutingGroupBindingQuery,
        ) -> Result<Vec<StoredRoutingGroupBinding>, DataLayerError> {
            Self::failure()
        }

        async fn list_routing_group_versions(
            &self,
            _group_id: &str,
        ) -> Result<Vec<StoredRoutingGroupVersion>, DataLayerError> {
            Self::failure()
        }
    }

    #[tokio::test]
    async fn selects_api_key_default_binding() {
        let repository = InMemoryRoutingGroupRepository::default();
        repository
            .create_routing_group(CreateRoutingGroupRecord {
                id: "group-1".to_string(),
                name: "default".to_string(),
                description: None,
                enabled: true,
                is_system_default: false,
                sort_order: 0,
                config_json: json!({}),
                version: 1,
                created_at: 1,
                updated_at: 1,
                published_at: None,
            })
            .await
            .unwrap();
        repository
            .create_routing_group_binding(CreateRoutingGroupBindingRecord {
                id: "binding-1".to_string(),
                group_id: "group-1".to_string(),
                subject_type: RoutingGroupBindingSubject::ApiKey,
                subject_id: "api-key-1".to_string(),
                is_default: true,
                allow_explicit_select: true,
                created_at: 1,
                updated_at: 1,
            })
            .await
            .unwrap();

        let selection = select_gateway_routing_group(
            &repository,
            GatewayRoutingSelectionInput {
                explicit_group: None,
                user_id: None,
                api_key_id: Some("api-key-1"),
                user_group_ids: &[],
                requested_model: None,
            },
        )
        .await
        .unwrap();

        assert_eq!(selection.source, "api_key_default");
        assert_eq!(selection.group.unwrap().id, "group-1");
    }

    #[tokio::test]
    async fn selects_system_default_when_no_bindings_exist() {
        let repository = InMemoryRoutingGroupRepository::default();
        repository
            .create_routing_group(CreateRoutingGroupRecord {
                id: "system-default".to_string(),
                name: "system-default".to_string(),
                description: None,
                enabled: true,
                is_system_default: true,
                sort_order: 0,
                config_json: json!({}),
                version: 1,
                created_at: 1,
                updated_at: 1,
                published_at: None,
            })
            .await
            .unwrap();

        let selection = select_gateway_routing_group(
            &repository,
            GatewayRoutingSelectionInput {
                explicit_group: None,
                user_id: Some("user-1"),
                api_key_id: Some("api-key-1"),
                user_group_ids: &["user-group-1".to_string()],
                requested_model: None,
            },
        )
        .await
        .unwrap();

        assert_eq!(selection.source, "system_default");
        assert_eq!(selection.group.unwrap().id, "system-default");
    }

    #[tokio::test]
    async fn selects_explicit_group_allowed_by_user_group_binding() {
        let repository = InMemoryRoutingGroupRepository::default();
        repository
            .create_routing_group(CreateRoutingGroupRecord {
                id: "private-group".to_string(),
                name: "private".to_string(),
                description: None,
                enabled: true,
                is_system_default: false,
                sort_order: 0,
                config_json: json!({}),
                version: 1,
                created_at: 1,
                updated_at: 1,
                published_at: None,
            })
            .await
            .unwrap();
        repository
            .create_routing_group_binding(CreateRoutingGroupBindingRecord {
                id: "binding-explicit".to_string(),
                group_id: "private-group".to_string(),
                subject_type: RoutingGroupBindingSubject::UserGroup,
                subject_id: "team-1".to_string(),
                is_default: false,
                allow_explicit_select: true,
                created_at: 1,
                updated_at: 1,
            })
            .await
            .unwrap();

        let selection = select_gateway_routing_group(
            &repository,
            GatewayRoutingSelectionInput {
                explicit_group: Some("private-group"),
                user_id: Some("user-1"),
                api_key_id: Some("api-key-1"),
                user_group_ids: &["team-1".to_string()],
                requested_model: None,
            },
        )
        .await
        .unwrap();

        assert_eq!(selection.source, "explicit_header");
        assert_eq!(selection.group.unwrap().id, "private-group");
    }

    #[tokio::test]
    async fn rejects_explicit_group_that_does_not_exist() {
        let repository = InMemoryRoutingGroupRepository::default();

        let error = select_gateway_routing_group(
            &repository,
            GatewayRoutingSelectionInput {
                explicit_group: Some("missing"),
                user_id: Some("user-1"),
                api_key_id: Some("api-key-1"),
                user_group_ids: &[],
                requested_model: None,
            },
        )
        .await
        .unwrap_err();

        assert_eq!(
            error,
            GatewayRoutingSelectionError::NotFound("missing".to_string())
        );
    }

    #[tokio::test]
    async fn propagates_explicit_name_lookup_failure_after_missing_id() {
        let repository = FailingRoutingGroupRepository {
            id_lookup_is_missing: true,
        };

        let error = select_gateway_routing_group(
            &repository,
            GatewayRoutingSelectionInput {
                explicit_group: Some("group-name"),
                user_id: Some("user-1"),
                api_key_id: Some("api-key-1"),
                user_group_ids: &[],
                requested_model: None,
            },
        )
        .await
        .unwrap_err();

        assert_eq!(
            error,
            GatewayRoutingSelectionError::Repository(
                "sql error: routing repository unavailable".to_string()
            )
        );
    }

    #[tokio::test]
    async fn propagates_implicit_binding_lookup_failure() {
        let repository = FailingRoutingGroupRepository {
            id_lookup_is_missing: false,
        };

        let error = select_gateway_routing_group(
            &repository,
            GatewayRoutingSelectionInput {
                explicit_group: None,
                user_id: Some("user-1"),
                api_key_id: Some("api-key-1"),
                user_group_ids: &[],
                requested_model: None,
            },
        )
        .await
        .unwrap_err();

        assert_eq!(
            error,
            GatewayRoutingSelectionError::Repository(
                "sql error: routing repository unavailable".to_string()
            )
        );
    }

    #[tokio::test]
    async fn rejects_explicit_disabled_group() {
        let repository = InMemoryRoutingGroupRepository::default();
        repository
            .create_routing_group(CreateRoutingGroupRecord {
                id: "disabled-group".to_string(),
                name: "disabled".to_string(),
                description: None,
                enabled: false,
                is_system_default: false,
                sort_order: 0,
                config_json: json!({}),
                version: 1,
                created_at: 1,
                updated_at: 1,
                published_at: None,
            })
            .await
            .unwrap();

        let error = select_gateway_routing_group(
            &repository,
            GatewayRoutingSelectionInput {
                explicit_group: Some("disabled-group"),
                user_id: Some("user-1"),
                api_key_id: Some("api-key-1"),
                user_group_ids: &[],
                requested_model: None,
            },
        )
        .await
        .unwrap_err();

        assert_eq!(
            error,
            GatewayRoutingSelectionError::Disabled("disabled-group".to_string())
        );
    }

    #[tokio::test]
    async fn rejects_explicit_group_without_binding_permission() {
        let repository = InMemoryRoutingGroupRepository::default();
        repository
            .create_routing_group(CreateRoutingGroupRecord {
                id: "private-group".to_string(),
                name: "private".to_string(),
                description: None,
                enabled: true,
                is_system_default: false,
                sort_order: 0,
                config_json: json!({}),
                version: 1,
                created_at: 1,
                updated_at: 1,
                published_at: None,
            })
            .await
            .unwrap();
        repository
            .create_routing_group_binding(CreateRoutingGroupBindingRecord {
                id: "binding-1".to_string(),
                group_id: "private-group".to_string(),
                subject_type: RoutingGroupBindingSubject::ApiKey,
                subject_id: "api-key-1".to_string(),
                is_default: true,
                allow_explicit_select: false,
                created_at: 1,
                updated_at: 1,
            })
            .await
            .unwrap();

        let error = select_gateway_routing_group(
            &repository,
            GatewayRoutingSelectionInput {
                explicit_group: Some("private-group"),
                user_id: Some("user-1"),
                api_key_id: Some("api-key-1"),
                user_group_ids: &[],
                requested_model: None,
            },
        )
        .await
        .unwrap_err();

        assert_eq!(
            error,
            GatewayRoutingSelectionError::Forbidden("private-group".to_string())
        );
    }

    fn model_scoped_config(model: &str) -> serde_json::Value {
        json!({
            "default_policy": {},
            "model_policies": [{ "model": model }],
            "rules": []
        })
    }

    async fn create_group(
        repository: &InMemoryRoutingGroupRepository,
        id: &str,
        enabled: bool,
        is_system_default: bool,
        sort_order: i64,
        config_json: serde_json::Value,
    ) {
        repository
            .create_routing_group(CreateRoutingGroupRecord {
                id: id.to_string(),
                name: id.to_string(),
                description: None,
                enabled,
                is_system_default,
                sort_order,
                config_json,
                version: 1,
                created_at: 1,
                updated_at: 1,
                published_at: None,
            })
            .await
            .unwrap();
    }

    async fn select_for_model(
        repository: &InMemoryRoutingGroupRepository,
        model: &str,
    ) -> GatewayRoutingGroupSelection {
        select_gateway_routing_group(
            repository,
            GatewayRoutingSelectionInput {
                requested_model: Some(model),
                ..Default::default()
            },
        )
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn model_scoped_group_above_default_captures_only_its_model() {
        let repository = InMemoryRoutingGroupRepository::default();
        create_group(&repository, "system-default", true, true, 5, json!({})).await;
        create_group(
            &repository,
            "model-scoped",
            true,
            false,
            0,
            model_scoped_config("deepseek-flash"),
        )
        .await;

        let matched = select_for_model(&repository, "deepseek-flash").await;
        assert_eq!(matched.source, "model_chain");
        assert_eq!(matched.group.unwrap().id, "model-scoped");

        let other = select_for_model(&repository, "gpt-5").await;
        assert_eq!(other.source, "system_default");
        assert_eq!(other.group.unwrap().id, "system-default");
    }

    #[tokio::test]
    async fn display_order_decides_between_model_scoped_groups() {
        let repository = InMemoryRoutingGroupRepository::default();
        create_group(
            &repository,
            "lower",
            true,
            false,
            9,
            model_scoped_config("gpt-5"),
        )
        .await;
        create_group(
            &repository,
            "higher",
            true,
            false,
            1,
            model_scoped_config("gpt-5"),
        )
        .await;

        let selection = select_for_model(&repository, "gpt-5").await;
        assert_eq!(selection.group.unwrap().id, "higher");
    }

    #[tokio::test]
    async fn disabled_model_scoped_group_never_serves_traffic() {
        let repository = InMemoryRoutingGroupRepository::default();
        create_group(&repository, "system-default", true, true, 1, json!({})).await;
        create_group(
            &repository,
            "disabled-scoped",
            false,
            false,
            0,
            model_scoped_config("gpt-5"),
        )
        .await;

        let selection = select_for_model(&repository, "gpt-5").await;
        assert_eq!(selection.source, "system_default");
        assert_eq!(selection.group.unwrap().id, "system-default");
    }

    #[tokio::test]
    async fn catch_all_model_policy_does_not_capture_a_single_model() {
        let repository = InMemoryRoutingGroupRepository::default();
        create_group(&repository, "system-default", true, true, 1, json!({})).await;
        create_group(
            &repository,
            "all-models",
            true,
            false,
            0,
            model_scoped_config("*"),
        )
        .await;

        let selection = select_for_model(&repository, "gpt-5").await;
        assert_eq!(selection.source, "system_default");
        assert_eq!(selection.group.unwrap().id, "system-default");
    }

    #[tokio::test]
    async fn principal_binding_outranks_the_model_scoped_chain() {
        let repository = InMemoryRoutingGroupRepository::default();
        create_group(&repository, "bound", true, false, 5, json!({})).await;
        create_group(
            &repository,
            "model-scoped",
            true,
            false,
            0,
            model_scoped_config("gpt-5"),
        )
        .await;
        repository
            .create_routing_group_binding(CreateRoutingGroupBindingRecord {
                id: "binding-1".to_string(),
                group_id: "bound".to_string(),
                subject_type: RoutingGroupBindingSubject::ApiKey,
                subject_id: "api-key-1".to_string(),
                is_default: true,
                allow_explicit_select: true,
                created_at: 1,
                updated_at: 1,
            })
            .await
            .unwrap();

        let selection = select_gateway_routing_group(
            &repository,
            GatewayRoutingSelectionInput {
                api_key_id: Some("api-key-1"),
                requested_model: Some("gpt-5"),
                ..Default::default()
            },
        )
        .await
        .unwrap();

        assert_eq!(selection.source, "api_key_default");
        assert_eq!(selection.group.unwrap().id, "bound");
    }

    #[tokio::test]
    async fn model_chain_failure_surfaces_as_repository_error() {
        let repository = FailingRoutingGroupRepository {
            id_lookup_is_missing: false,
        };

        let error = select_gateway_routing_group(
            &repository,
            GatewayRoutingSelectionInput {
                requested_model: Some("gpt-5"),
                ..Default::default()
            },
        )
        .await
        .unwrap_err();

        assert!(matches!(error, GatewayRoutingSelectionError::Repository(_)));
    }
}
