use std::sync::Arc;

use axum::Router;

use aether_data::repository::routing_profiles::InMemoryRoutingGroupRepository;
use aether_data_contracts::repository::routing_profiles::{
    StoredRoutingGroup, StoredRoutingGroupBinding, StoredRoutingGroupVersion,
};
use http::StatusCode;
use serde_json::json;

use super::super::super::{build_router_with_state, start_server, AppState};
use crate::constants::{
    GATEWAY_HEADER, TRUSTED_ADMIN_SESSION_ID_HEADER, TRUSTED_ADMIN_USER_ID_HEADER,
    TRUSTED_ADMIN_USER_ROLE_HEADER,
};
use crate::data::GatewayDataState;

fn group(id: &str, name: &str) -> StoredRoutingGroup {
    StoredRoutingGroup {
        id: id.to_string(),
        name: name.to_string(),
        description: None,
        enabled: true,
        is_system_default: false,
        sort_order: 0,
        config_json: json!({}),
        version: 1,
        created_at: 1,
        updated_at: 1,
        published_at: Some(1),
    }
}

async fn routing_groups_router(groups: Vec<StoredRoutingGroup>) -> Router {
    let repository = Arc::new(InMemoryRoutingGroupRepository::seed(
        groups,
        std::iter::empty::<StoredRoutingGroupBinding>(),
        std::iter::empty::<StoredRoutingGroupVersion>(),
    ));
    build_router_with_state(
        AppState::new()
            .expect("gateway should build")
            .with_data_state_for_tests(
                GatewayDataState::disabled().with_routing_group_repository_for_tests(repository),
            ),
    )
}

async fn post_group(gateway_url: &str, name: &str) -> (StatusCode, serde_json::Value) {
    let response = reqwest::Client::new()
        .post(format!("{gateway_url}/api/admin/routing/groups"))
        .header(GATEWAY_HEADER, "rust-phase3b")
        .header(TRUSTED_ADMIN_USER_ID_HEADER, "admin-user-123")
        .header(TRUSTED_ADMIN_USER_ROLE_HEADER, "admin")
        .header(TRUSTED_ADMIN_SESSION_ID_HEADER, "session-123")
        .json(&json!({ "name": name, "enabled": true }))
        .send()
        .await
        .expect("request should succeed");
    let status = response.status();
    let payload = response.json().await.expect("json body should parse");
    (status, payload)
}

#[tokio::test]
async fn rejects_a_duplicate_routing_group_name_with_a_specific_reason() {
    let (gateway_url, gateway_handle) =
        start_server(routing_groups_router(vec![group("group-1", "翻译策略")]).await).await;

    let (status, payload) = post_group(&gateway_url, "翻译策略").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(
        payload["detail"],
        json!("调度策略名称「翻译策略」已存在，请换一个名称")
    );

    gateway_handle.abort();
}

#[tokio::test]
async fn accepts_a_routing_group_name_that_is_still_free() {
    let (gateway_url, gateway_handle) =
        start_server(routing_groups_router(vec![group("group-1", "翻译策略")]).await).await;

    let (status, payload) = post_group(&gateway_url, "翻译策略 2").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(payload["name"], json!("翻译策略 2"));

    gateway_handle.abort();
}
