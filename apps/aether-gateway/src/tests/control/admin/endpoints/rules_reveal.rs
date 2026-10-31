use std::sync::Arc;

use aether_data::repository::provider_catalog::InMemoryProviderCatalogReadRepository;
use axum::body::Body;
use http::{HeaderMap, HeaderValue, Method, Request, StatusCode};
use http_body_util::BodyExt;
use serde_json::{json, Value};

use super::super::super::{build_router_with_state, sample_endpoint, sample_provider, AppState};
use crate::admin_api::{maybe_build_local_admin_response, AdminRouteRequest};
use crate::audit::AdminAuditEvent;
use crate::control::resolve_public_request_context;
use crate::data::GatewayDataState;
use crate::tests::send_request;

fn seeded_state() -> AppState {
    let mut endpoint = sample_endpoint(
        "endpoint-rules",
        "provider-rules",
        "openai:chat",
        "https://example.test",
    );
    endpoint.header_rules =
        Some(json!([{"action": "set", "key": "x-auth", "value": "request-secret"}]));
    endpoint.body_rules =
        Some(json!([{"action": "set", "path": "auth.token", "value": "body-secret"}]));
    endpoint.config = Some(json!({
        "private_token": "unrelated-secret",
        "response_header_rules": [{"action": "set", "key": "x-auth", "value": "response-secret"}]
    }));
    let repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![sample_provider("provider-rules", "custom", 10)],
        vec![endpoint],
        vec![],
    ));
    AppState::new().unwrap().with_data_state_for_tests(
        GatewayDataState::with_provider_catalog_reader_for_tests(repository),
    )
}

fn admin_headers() -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(
        http::header::AUTHORIZATION,
        HeaderValue::from_str(&format!(
            "Bearer {}",
            crate::tests::test_admin_access_token()
        ))
        .expect("admin token should be a valid header value"),
    );
    headers.insert(
        "x-client-device-id",
        HeaderValue::from_static(crate::tests::TEST_ADMIN_DEVICE_ID),
    );
    headers
}

#[tokio::test]
async fn endpoint_rules_reveal_is_scoped_audited_and_not_cached() {
    let state = seeded_state();
    let context = resolve_public_request_context(
        &state,
        &Method::GET,
        &"/api/admin/endpoints/endpoint-rules/rules/reveal"
            .parse()
            .unwrap(),
        &admin_headers(),
        "reveal-test",
    )
    .await
    .unwrap();
    let response = maybe_build_local_admin_response(AdminRouteRequest::new(
        &state,
        &context,
        &"127.0.0.1:12345".parse().unwrap(),
        &admin_headers(),
        None,
    ))
    .await
    .unwrap()
    .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()[http::header::CACHE_CONTROL], "no-store");
    assert_eq!(response.headers()[http::header::PRAGMA], "no-cache");
    let audit = response.extensions().get::<AdminAuditEvent>().unwrap();
    assert_eq!(audit.event_name, "admin_endpoint_rules_revealed");
    assert_eq!(audit.action, "reveal_endpoint_rules");
    assert_eq!(audit.target_id, "endpoint-rules");
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let payload: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(payload["header_rules"][0]["value"], "request-secret");
    assert_eq!(payload["body_rules"][0]["value"], "body-secret");
    assert_eq!(
        payload["response_header_rules"][0]["value"],
        "response-secret"
    );
    assert_eq!(payload.as_object().unwrap().len(), 3);
    assert!(!payload.to_string().contains("unrelated-secret"));
}

#[tokio::test]
async fn endpoint_rules_reveal_denies_anonymous_and_non_admin_requests() {
    const NON_ADMIN_DEVICE_ID: &str = "device-non-admin-rules-reveal";
    let state = seeded_state();
    let non_admin_token = crate::tests::operational_auth::issue_operational_session_access_token(
        &state,
        NON_ADMIN_DEVICE_ID,
        "user",
    )
    .await;
    let router = build_router_with_state(state);

    let anonymous = send_request(
        router.clone(),
        Request::builder()
            .uri("/api/admin/endpoints/endpoint-rules/rules/reveal")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(anonymous.status(), StatusCode::UNAUTHORIZED);
    let body = anonymous.into_body().collect().await.unwrap().to_bytes();
    assert!(!String::from_utf8_lossy(&body).contains("request-secret"));

    let non_admin = send_request(
        router,
        Request::builder()
            .uri("/api/admin/endpoints/endpoint-rules/rules/reveal")
            .header(
                http::header::AUTHORIZATION,
                format!("Bearer {non_admin_token}"),
            )
            .header("x-client-device-id", NON_ADMIN_DEVICE_ID)
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(non_admin.status(), StatusCode::UNAUTHORIZED);
    let body = non_admin.into_body().collect().await.unwrap().to_bytes();
    assert!(!String::from_utf8_lossy(&body).contains("request-secret"));
}

#[tokio::test]
async fn endpoint_rules_reveal_returns_not_found_and_data_unavailable_without_fallback() {
    for (state, expected) in [
        (seeded_state(), StatusCode::NOT_FOUND),
        (AppState::new().unwrap(), StatusCode::SERVICE_UNAVAILABLE),
    ] {
        let context = resolve_public_request_context(
            &state,
            &Method::GET,
            &"/api/admin/endpoints/missing/rules/reveal".parse().unwrap(),
            &admin_headers(),
            "reveal-missing-test",
        )
        .await
        .unwrap();
        let response = maybe_build_local_admin_response(AdminRouteRequest::new(
            &state,
            &context,
            &"127.0.0.1:12345".parse().unwrap(),
            &admin_headers(),
            None,
        ))
        .await
        .unwrap()
        .unwrap();
        assert_eq!(response.status(), expected);
    }
}
