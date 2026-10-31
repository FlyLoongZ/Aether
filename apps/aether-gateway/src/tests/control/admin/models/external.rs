use std::sync::{Arc, Mutex};
use std::time::Duration;

use aether_data::repository::proxy_nodes::{InMemoryProxyNodeRepository, StoredProxyNode};
use axum::body::Body;
use axum::routing::any;
use axum::{extract::Request, Router};
use base64::Engine as _;
use http::StatusCode;
use serde_json::json;

use super::super::super::{build_router_with_state, sample_proxy_node, start_server, AppState};
use crate::data::GatewayDataState;
use crate::handlers::admin::{
    set_admin_external_models_source_url_for_tests, ADMIN_EXTERNAL_MODELS_CONFIG_MUTATION_LOCK_KEY,
};

fn trusted_admin(request: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
    request
        .bearer_auth(crate::tests::test_admin_access_token())
        .header("x-client-device-id", crate::tests::TEST_ADMIN_DEVICE_ID)
}

fn online_manual_proxy_node(node_id: &str, proxy_url: impl Into<String>) -> StoredProxyNode {
    let mut node = sample_proxy_node(node_id);
    node.name = node_id.to_string();
    node.status = "online".to_string();
    node.proxy_url = Some(proxy_url.into());
    node.last_used_at_unix_secs = None;
    node
}

#[tokio::test]
async fn gateway_manages_admin_external_models_proxy_config_locally() {
    let manual_node = online_manual_proxy_node("manual-node", "http://127.0.0.1:8899");
    let mut offline_node = online_manual_proxy_node("offline-node", "http://127.0.0.1:8900");
    offline_node.status = "offline".to_string();
    let repository = Arc::new(InMemoryProxyNodeRepository::seed(vec![
        manual_node,
        offline_node,
    ]));
    let data_state = GatewayDataState::with_proxy_node_repository_for_tests(repository)
        .with_system_config_values_for_tests(Vec::<(String, serde_json::Value)>::new());
    let gateway = build_router_with_state(
        AppState::new()
            .expect("gateway should build")
            .with_data_state_for_tests(data_state),
    );
    let (gateway_url, gateway_handle) = start_server(gateway).await;
    let client = reqwest::Client::new();
    let config_url = format!("{gateway_url}/api/admin/models/external/config");

    let response = trusted_admin(client.get(&config_url))
        .send()
        .await
        .expect("initial config request should succeed");
    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["proxy_node_id"], serde_json::Value::Null);

    for invalid_payload in [
        json!({}),
        json!({ "proxy_node_id": true }),
        json!({ "proxy_node_id": "" }),
        json!({ "proxy_node_id": "   " }),
    ] {
        let response = trusted_admin(client.put(&config_url))
            .json(&invalid_payload)
            .send()
            .await
            .expect("invalid config request should complete");
        assert_eq!(
            response.status(),
            StatusCode::BAD_REQUEST,
            "invalid payload should be rejected: {invalid_payload}"
        );
    }

    let response = trusted_admin(client.put(&config_url))
        .json(&json!({ "proxy_node_id": "missing-node" }))
        .send()
        .await
        .expect("missing-node config request should complete");
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let response = trusted_admin(client.put(&config_url))
        .json(&json!({ "proxy_node_id": "offline-node" }))
        .send()
        .await
        .expect("offline-node config request should complete");
    assert_eq!(response.status(), StatusCode::CONFLICT);

    let response = trusted_admin(client.get(&config_url))
        .send()
        .await
        .expect("config should remain readable");
    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["proxy_node_id"], serde_json::Value::Null);

    let response = trusted_admin(client.put(&config_url))
        .json(&json!({ "proxy_node_id": "manual-node" }))
        .send()
        .await
        .expect("manual-node config request should succeed");
    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["proxy_node_id"], "manual-node");
    assert!(payload["cache_cleared"].is_boolean());

    let response = trusted_admin(client.get(&config_url))
        .send()
        .await
        .expect("saved config request should succeed");
    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["proxy_node_id"], "manual-node");

    let response = trusted_admin(client.put(&config_url))
        .json(&json!({ "proxy_node_id": null }))
        .send()
        .await
        .expect("direct config request should succeed");
    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["proxy_node_id"], serde_json::Value::Null);
    assert!(payload["cache_cleared"].is_boolean());

    gateway_handle.abort();
}

#[tokio::test]
async fn gateway_serializes_external_models_proxy_selection_with_proxy_node_deletion() {
    let node_a = online_manual_proxy_node("node-a", "http://127.0.0.1:8898");
    let node_b = online_manual_proxy_node("node-b", "http://127.0.0.1:8899");
    let repository = Arc::new(InMemoryProxyNodeRepository::seed(vec![node_a, node_b]));
    let data_state = GatewayDataState::with_proxy_node_repository_for_tests(repository)
        .with_system_config_values_for_tests([(
            "external_models_proxy_node_id".to_string(),
            json!("node-a"),
        )]);
    let state = AppState::new()
        .expect("gateway should build")
        .with_data_state_for_tests(data_state.clone());
    let control_state = state.clone();
    let lock = control_state
        .runtime_state()
        .lock_try_acquire(
            ADMIN_EXTERNAL_MODELS_CONFIG_MUTATION_LOCK_KEY,
            "external-models-race-test",
            Duration::from_secs(60),
        )
        .await
        .expect("test mutation lock should be available")
        .expect("test mutation lock should be acquired");

    let gateway = build_router_with_state(state);
    let (gateway_url, gateway_handle) = start_server(gateway).await;
    let client = reqwest::Client::new();
    let config_url = format!("{gateway_url}/api/admin/models/external/config");

    let response = trusted_admin(client.put(&config_url))
        .json(&json!({ "proxy_node_id": "node-b" }))
        .send()
        .await
        .expect("contended config request should complete");
    assert_eq!(response.status(), StatusCode::CONFLICT);

    let response =
        trusted_admin(client.delete(format!("{gateway_url}/api/admin/proxy-nodes/node-a")))
            .send()
            .await
            .expect("contended delete request should complete");
    assert_eq!(response.status(), StatusCode::CONFLICT);
    assert_eq!(
        data_state
            .find_system_config_value("external_models_proxy_node_id")
            .await
            .expect("selector lookup should succeed"),
        Some(json!("node-a"))
    );
    assert!(data_state
        .find_proxy_node("node-a")
        .await
        .expect("node lookup should succeed")
        .is_some());

    assert!(control_state
        .runtime_state()
        .lock_release(&lock)
        .await
        .expect("test mutation lock should release"));

    let response = trusted_admin(client.put(&config_url))
        .json(&json!({ "proxy_node_id": "node-b" }))
        .send()
        .await
        .expect("config request should succeed after lock release");
    assert_eq!(response.status(), StatusCode::OK);

    let response =
        trusted_admin(client.delete(format!("{gateway_url}/api/admin/proxy-nodes/node-a")))
            .send()
            .await
            .expect("delete request should succeed after lock release");
    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["cleared_external_models_proxy"], false);
    assert_eq!(
        data_state
            .find_system_config_value("external_models_proxy_node_id")
            .await
            .expect("selector lookup should succeed"),
        Some(json!("node-b")),
        "deleting node A must not overwrite a concurrently chosen node B"
    );

    let response = trusted_admin(client.put(&config_url))
        .json(&json!({ "proxy_node_id": "node-a" }))
        .send()
        .await
        .expect("deleted-node config request should complete");
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    assert_eq!(
        data_state
            .find_system_config_value("external_models_proxy_node_id")
            .await
            .expect("selector lookup should succeed"),
        Some(json!("node-b")),
        "a failed save must not leave a dangling deleted node ID"
    );

    gateway_handle.abort();
}

#[tokio::test]
async fn gateway_reports_unavailable_external_models_proxy_and_fails_closed() {
    let direct_source_hits = Arc::new(Mutex::new(0usize));
    let direct_source_hits_clone = Arc::clone(&direct_source_hits);
    let direct_source = Router::new().route(
        "/api.json",
        any(move |_request: Request| {
            let direct_source_hits_inner = Arc::clone(&direct_source_hits_clone);
            async move {
                *direct_source_hits_inner.lock().expect("mutex should lock") += 1;
                (
                    StatusCode::OK,
                    axum::Json(json!({
                        "unexpected-direct-provider": {
                            "name": "Unexpected direct fallback",
                            "models": {}
                        }
                    })),
                )
            }
        }),
    );
    let (direct_source_url, direct_source_handle) = start_server(direct_source).await;
    let _guard =
        set_admin_external_models_source_url_for_tests(&format!("{direct_source_url}/api.json"));

    let mut offline_node = online_manual_proxy_node("offline-node", "http://127.0.0.1:8900");
    offline_node.status = "offline".to_string();
    let repository = Arc::new(InMemoryProxyNodeRepository::seed(vec![offline_node]));
    let data_state = GatewayDataState::with_proxy_node_repository_for_tests(repository)
        .with_system_config_values_for_tests([(
            "external_models_proxy_node_id".to_string(),
            json!("offline-node"),
        )]);
    let gateway = build_router_with_state(
        AppState::new()
            .expect("gateway should build")
            .with_data_state_for_tests(data_state),
    );
    let (gateway_url, gateway_handle) = start_server(gateway).await;
    let client = reqwest::Client::new();

    let response =
        trusted_admin(client.get(format!("{gateway_url}/api/admin/models/external/config")))
            .send()
            .await
            .expect("config request should succeed");
    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["proxy_node_id"], "offline-node");

    let response = trusted_admin(client.get(format!("{gateway_url}/api/admin/models/external")))
        .send()
        .await
        .expect("catalog request should complete");
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(*direct_source_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    direct_source_handle.abort();
}

#[tokio::test]
async fn gateway_rejects_external_models_proxy_config_when_data_stores_are_unavailable() {
    let gateway = build_router_with_state(AppState::new().expect("gateway should build"));
    let (gateway_url, gateway_handle) = start_server(gateway).await;
    let client = reqwest::Client::new();

    let response =
        trusted_admin(client.put(format!("{gateway_url}/api/admin/models/external/config")))
            .json(&json!({ "proxy_node_id": null }))
            .send()
            .await
            .expect("missing config store request should complete");
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    gateway_handle.abort();

    let data_state =
        GatewayDataState::disabled()
            .with_system_config_values_for_tests(Vec::<(String, serde_json::Value)>::new());
    let gateway = build_router_with_state(
        AppState::new()
            .expect("gateway should build")
            .with_data_state_for_tests(data_state),
    );
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response =
        trusted_admin(client.put(format!("{gateway_url}/api/admin/models/external/config")))
            .json(&json!({ "proxy_node_id": "manual-node" }))
            .send()
            .await
            .expect("missing proxy reader request should complete");
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);

    gateway_handle.abort();
}

#[tokio::test]
async fn gateway_handles_admin_external_models_locally_with_trusted_admin_principal() {
    let upstream_hits = Arc::new(Mutex::new(0usize));
    let upstream_hits_clone = Arc::clone(&upstream_hits);
    let upstream = Router::new().route(
        "/api/admin/models/external",
        any(move |_request: Request| {
            let upstream_hits_inner = Arc::clone(&upstream_hits_clone);
            async move {
                *upstream_hits_inner.lock().expect("mutex should lock") += 1;
                (StatusCode::OK, Body::from("unexpected upstream hit"))
            }
        }),
    );

    let (_upstream_url, upstream_handle) = start_server(upstream).await;
    let external_source_hits = Arc::new(Mutex::new(0usize));
    let external_source_hits_clone = Arc::clone(&external_source_hits);
    let external_source = Router::new().route(
        "/api.json",
        any(move |_request: Request| {
            let external_source_hits_inner = Arc::clone(&external_source_hits_clone);
            async move {
                *external_source_hits_inner
                    .lock()
                    .expect("mutex should lock") += 1;
                (
                    StatusCode::OK,
                    axum::Json(json!({
                        "direct-provider": {
                            "name": "Direct",
                            "models": {
                                "direct": {
                                    "name": "DIRECT"
                                }
                            }
                        }
                    })),
                )
            }
        }),
    );
    let (external_source_url, external_source_handle) = start_server(external_source).await;
    let proxy_hits = Arc::new(Mutex::new(0usize));
    let proxy_hits_clone = Arc::clone(&proxy_hits);
    let proxy_auths = Arc::new(Mutex::new(Vec::<Option<String>>::new()));
    let proxy_auths_clone = Arc::clone(&proxy_auths);
    let proxy = Router::new().fallback(any(move |request: Request| {
        let proxy_hits_inner = Arc::clone(&proxy_hits_clone);
        let proxy_auths_inner = Arc::clone(&proxy_auths_clone);
        async move {
            *proxy_hits_inner.lock().expect("mutex should lock") += 1;
            proxy_auths_inner.lock().expect("mutex should lock").push(
                request
                    .headers()
                    .get("proxy-authorization")
                    .and_then(|value| value.to_str().ok())
                    .map(str::to_string),
            );
            (
                StatusCode::OK,
                axum::Json(json!({
                    "manual-provider": {
                        "name": "Manual",
                        "models": {
                            "manual": {
                                "name": "MANUAL"
                            }
                        }
                    }
                })),
            )
        }
    }));
    let (proxy_url, proxy_handle) = start_server(proxy).await;
    let _guard =
        set_admin_external_models_source_url_for_tests(&format!("{external_source_url}/api.json"));

    let mut manual_node = online_manual_proxy_node("manual-node", proxy_url);
    manual_node.proxy_username = Some("alice".to_string());
    manual_node.proxy_password = Some("supersecret".to_string());
    let repository = Arc::new(InMemoryProxyNodeRepository::seed(vec![manual_node]));
    let data_state = GatewayDataState::with_proxy_node_repository_for_tests(repository)
        .with_system_config_values_for_tests(Vec::<(String, serde_json::Value)>::new());
    let gateway = build_router_with_state(
        AppState::new()
            .expect("gateway should build")
            .with_data_state_for_tests(data_state),
    );
    let (gateway_url, gateway_handle) = start_server(gateway).await;
    let client = reqwest::Client::new();

    let response = trusted_admin(client.get(format!("{gateway_url}/api/admin/models/external")))
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(
        payload["direct-provider"]["models"]["direct"]["name"],
        json!("DIRECT")
    );
    assert_eq!(*external_source_hits.lock().expect("mutex should lock"), 1);
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    let response =
        trusted_admin(client.put(format!("{gateway_url}/api/admin/models/external/config")))
            .json(&json!({ "proxy_node_id": "manual-node" }))
            .send()
            .await
            .expect("proxy config request should succeed");
    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["proxy_node_id"], "manual-node");
    assert_eq!(payload["cache_cleared"], true);

    let response = trusted_admin(client.get(format!("{gateway_url}/api/admin/models/external")))
        .send()
        .await
        .expect("proxied request should succeed");
    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(
        payload["manual-provider"]["models"]["manual"]["name"],
        json!("MANUAL")
    );
    assert_eq!(*external_source_hits.lock().expect("mutex should lock"), 1);
    assert_eq!(*proxy_hits.lock().expect("mutex should lock"), 1);
    let expected_proxy_auth = format!(
        "Basic {}",
        base64::engine::general_purpose::STANDARD.encode("alice:supersecret")
    );
    assert_eq!(
        proxy_auths.lock().expect("mutex should lock").as_slice(),
        [Some(expected_proxy_auth)]
    );

    gateway_handle.abort();
    proxy_handle.abort();
    external_source_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_clears_admin_external_models_cache_locally_with_trusted_admin_principal() {
    let upstream_hits = Arc::new(Mutex::new(0usize));
    let upstream_hits_clone = Arc::clone(&upstream_hits);
    let upstream = Router::new().route(
        "/api/admin/models/external/cache",
        any(move |_request: Request| {
            let upstream_hits_inner = Arc::clone(&upstream_hits_clone);
            async move {
                *upstream_hits_inner.lock().expect("mutex should lock") += 1;
                (StatusCode::OK, Body::from("unexpected upstream hit"))
            }
        }),
    );

    let (_upstream_url, upstream_handle) = start_server(upstream).await;
    let gateway = build_router_with_state(AppState::new().expect("gateway should build"));
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = trusted_admin(
        reqwest::Client::new().delete(format!("{gateway_url}/api/admin/models/external/cache")),
    )
    .send()
    .await
    .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["cleared"], false);
    assert_eq!(payload["message"], "缓存不存在");
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}
