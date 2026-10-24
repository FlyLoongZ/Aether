use super::json;
use aether_crypto::DEVELOPMENT_ENCRYPTION_KEY;
use aether_data::repository::announcements::{
    InMemoryAnnouncementReadRepository, StoredAnnouncement,
};
use aether_data::repository::auth::{
    InMemoryAuthApiKeySnapshotRepository, StoredAuthApiKeySnapshot,
};
use aether_data::repository::candidate_selection::InMemoryMinimalCandidateSelectionReadRepository;
use aether_data::repository::candidates::InMemoryRequestCandidateRepository;
use aether_data::repository::global_models::InMemoryGlobalModelReadRepository;
use aether_data::repository::provider_catalog::InMemoryProviderCatalogReadRepository;
use aether_data::repository::video_tasks::InMemoryVideoTaskRepository;
use aether_data_contracts::repository::candidate_selection::{
    StoredMinimalCandidateSelectionRow, StoredProviderModelMapping,
};
use aether_data_contracts::repository::candidates::RequestCandidateStatus;
use aether_data_contracts::repository::global_models::{
    StoredPublicCatalogModel, StoredPublicGlobalModel,
};

use aether_data_contracts::repository::video_tasks::{
    UpsertVideoTask, VideoTaskLookupKey, VideoTaskReadRepository, VideoTaskStatus,
    VideoTaskWriteRepository,
};

fn run_frontdoor_async_test<F>(name: &'static str, future: F)
where
    F: std::future::Future<Output = ()> + Send + 'static,
{
    crate::tests::run_async_test_on_large_stack(name, crate::tests::LARGE_TEST_STACK_BYTES, || {
        future
    });
}

pub(super) use crate::tests::hash_api_key;

pub(super) use super::support::{
    sample_endpoint, sample_key, sample_provider, sample_request_candidate,
};

fn unrestricted_models_snapshot(api_key_id: &str, user_id: &str) -> StoredAuthApiKeySnapshot {
    StoredAuthApiKeySnapshot::new(
        user_id.to_string(),
        "alice".to_string(),
        Some("alice@example.com".to_string()),
        "user".to_string(),
        "local".to_string(),
        true,
        false,
        None,
        None,
        None,
        api_key_id.to_string(),
        Some("default".to_string()),
        true,
        false,
        false,
        Some(10),
        Some(5),
        Some(4_102_444_800),
        None,
        None,
        None,
    )
    .expect("snapshot should build")
}

fn sample_models_candidate_row(
    provider_id: &str,
    provider_name: &str,
    api_format: &str,
    global_model_name: &str,
    provider_priority: i32,
) -> StoredMinimalCandidateSelectionRow {
    StoredMinimalCandidateSelectionRow {
        provider_id: provider_id.to_string(),
        provider_name: provider_name.to_string(),
        provider_type: "custom".to_string(),
        provider_priority,
        provider_is_active: true,
        endpoint_id: format!("endpoint-{provider_id}"),
        endpoint_api_format: api_format.to_string(),
        endpoint_api_family: Some("openai".to_string()),
        endpoint_kind: Some("chat".to_string()),
        endpoint_is_active: true,
        key_id: format!("key-{provider_id}"),
        key_name: "prod".to_string(),
        key_auth_type: "api_key".to_string(),
        key_is_active: true,
        key_api_formats: Some(vec![api_format.to_string()]),
        key_allowed_models: None,
        key_capabilities: None,
        key_internal_priority: 50,
        key_global_priority_by_format: None,
        model_id: format!("model-{provider_id}-{global_model_name}"),
        global_model_id: format!("global-{global_model_name}"),
        global_model_name: global_model_name.to_string(),
        global_model_mappings: None,
        global_model_supports_streaming: Some(true),
        model_provider_model_name: global_model_name.to_string(),
        model_provider_model_mappings: Some(vec![StoredProviderModelMapping {
            name: global_model_name.to_string(),
            priority: 1,
            api_formats: Some(vec![api_format.to_string()]),
            endpoint_ids: None,
            operations: None,
        }]),
        model_supports_streaming: Some(true),
        model_is_active: true,
        model_is_available: true,
    }
}

fn sample_public_global_model(
    id: &str,
    name: &str,
    display_name: &str,
    is_active: bool,
) -> StoredPublicGlobalModel {
    StoredPublicGlobalModel::new(
        id.to_string(),
        name.to_string(),
        Some(display_name.to_string()),
        is_active,
        Some(0.02),
        Some(json!({"tiers":[{"up_to": null, "input_price_per_1m": 3.0, "output_price_per_1m": 15.0}]})),
        Some(json!(["vision"])),
        Some(json!({"family": "test"})),
        0,
    )
    .expect("global model should build")
}

fn sample_public_global_model_with_capabilities(
    id: &str,
    name: &str,
    display_name: &str,
    supported_capabilities: serde_json::Value,
) -> StoredPublicGlobalModel {
    StoredPublicGlobalModel::new(
        id.to_string(),
        name.to_string(),
        Some(display_name.to_string()),
        true,
        Some(0.02),
        Some(json!({"tiers":[{"up_to": null, "input_price_per_1m": 3.0, "output_price_per_1m": 15.0}]})),
        Some(supported_capabilities),
        Some(json!({"family": "test"})),
        0,
    )
    .expect("global model should build")
}

fn sample_public_catalog_model(
    id: &str,
    provider_id: &str,
    provider_name: &str,
    provider_model_name: &str,
    name: &str,
    display_name: &str,
) -> StoredPublicCatalogModel {
    StoredPublicCatalogModel::new(
        id.to_string(),
        provider_id.to_string(),
        provider_name.to_string(),
        provider_model_name.to_string(),
        name.to_string(),
        display_name.to_string(),
        Some(format!("{display_name} description")),
        Some(format!("https://cdn.example/{name}.png")),
        Some(3.0),
        Some(15.0),
        Some(1.5),
        Some(0.3),
        Some(true),
        Some(true),
        Some(true),
        Some(false),
        true,
    )
    .expect("public catalog model should build")
}

mod ai;
mod core;
mod internal;
mod oauth;
mod ops;
mod public_support;
