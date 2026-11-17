use std::sync::{Arc, Mutex};

use axum::body::Body;
use axum::routing::any;
use axum::{extract::Request, Router};
use http::StatusCode;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use super::super::{build_router_with_state, start_server, AppState};

#[tokio::test]
async fn admin_encoded_path_authenticates_before_route_lookup() {
    let upstream_hits = Arc::new(Mutex::new(0usize));
    let upstream_hits_clone = Arc::clone(&upstream_hits);
    let upstream = Router::new().route(
        "/{*path}",
        any(move |_request: Request| {
            let upstream_hits_inner = Arc::clone(&upstream_hits_clone);
            async move {
                *upstream_hits_inner.lock().expect("mutex should lock") += 1;
                (StatusCode::OK, Body::from("proxied"))
            }
        }),
    );

    let (_upstream_url, upstream_handle) = start_server(upstream).await;
    let gateway = build_router_with_state(AppState::new().expect("gateway should build"));
    let (gateway_url, gateway_handle) = start_server(gateway).await;
    let client = reqwest::Client::new();

    let unauthenticated = client
        .get(format!("{gateway_url}/api/admin/users%2F123"))
        .send()
        .await
        .expect("request should succeed");
    assert_eq!(unauthenticated.status(), StatusCode::UNAUTHORIZED);
    // Unauthenticated responses must not leak gateway-internal metadata headers.
    assert!(unauthenticated
        .headers()
        .get("x-aether-execution-path")
        .is_none());
    assert!(unauthenticated
        .headers()
        .get("x-aether-control-route-class")
        .is_none());
    let payload: serde_json::Value = unauthenticated
        .json()
        .await
        .expect("json body should parse");
    assert_eq!(payload["detail"], "admin authentication required");
    assert!(payload.get("route_family").is_none());
    assert!(payload.get("route_kind").is_none());

    let encoded_prefix_unauthenticated = client
        .get(format!("{gateway_url}/api/admin%2Fusers"))
        .send()
        .await
        .expect("request should succeed");
    assert_eq!(
        encoded_prefix_unauthenticated.status(),
        StatusCode::UNAUTHORIZED
    );

    let authenticated = client
        .get(format!("{gateway_url}/api/admin/users%2F123"))
        .bearer_auth(crate::tests::test_admin_access_token())
        .header("x-client-device-id", crate::tests::TEST_ADMIN_DEVICE_ID)
        .send()
        .await
        .expect("request should succeed");
    assert_eq!(authenticated.status(), StatusCode::NOT_FOUND);
    let payload: serde_json::Value = authenticated.json().await.expect("json body should parse");
    assert_eq!(payload["detail"], "admin route not found");
    assert!(payload.get("route_family").is_none());
    assert!(payload.get("route_kind").is_none());

    let encoded_prefix_authenticated = client
        .get(format!("{gateway_url}/api/admin%2Fusers"))
        .bearer_auth(crate::tests::test_admin_access_token())
        .header("x-client-device-id", crate::tests::TEST_ADMIN_DEVICE_ID)
        .send()
        .await
        .expect("request should succeed");
    assert_eq!(encoded_prefix_authenticated.status(), StatusCode::NOT_FOUND);
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn admin_namespace_variants_never_reach_an_admin_handler_or_upstream() {
    let upstream_hits = Arc::new(Mutex::new(0usize));
    let upstream_hits_clone = Arc::clone(&upstream_hits);
    let upstream = Router::new().route(
        "/{*path}",
        any(move |_request: Request| {
            let upstream_hits_inner = Arc::clone(&upstream_hits_clone);
            async move {
                *upstream_hits_inner.lock().expect("mutex should lock") += 1;
                (StatusCode::OK, Body::from("proxied"))
            }
        }),
    );

    let (_upstream_url, upstream_handle) = start_server(upstream).await;
    let gateway = build_router_with_state(AppState::new().expect("gateway should build"));
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    // The admin gate matches the decoded path by exact prefix, so `.` segments
    // and repeated slashes fall through to the local route-not-found response
    // instead of being forwarded anywhere. A double-encoded traversal still
    // matches the admin prefix after one decode, so it stops at the gate.
    for (path, expected) in [
        ("/api/./admin/users", StatusCode::NOT_FOUND),
        ("//api/admin/users", StatusCode::NOT_FOUND),
        ("/api/admin/%252e%252e/users", StatusCode::UNAUTHORIZED),
    ] {
        let status = raw_request_status(&gateway_url, path).await;
        assert_eq!(status, expected.as_u16(), "path {path}");
    }
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn admin_path_rejects_traversal_backslash_and_malformed_encoding() {
    let gateway = build_router_with_state(AppState::new().expect("gateway should build"));
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    for path in [
        "/api/admin/%2e%2e/users",
        "/api/admin/%5cusers",
        "/api/admin/%zz",
        "/api/admin/%2",
    ] {
        let status = raw_request_status(&gateway_url, path).await;
        assert_eq!(status, StatusCode::BAD_REQUEST.as_u16(), "path {path}");
    }

    gateway_handle.abort();
}

async fn raw_request_status(gateway_url: &str, path: &str) -> u16 {
    let authority = gateway_url
        .strip_prefix("http://")
        .expect("gateway url should use http");
    let mut stream = TcpStream::connect(authority)
        .await
        .expect("raw request should connect");
    let request = format!("GET {path} HTTP/1.1\r\nHost: {authority}\r\nConnection: close\r\n\r\n");
    stream
        .write_all(request.as_bytes())
        .await
        .expect("raw request should write");
    let mut response = Vec::new();
    stream
        .read_to_end(&mut response)
        .await
        .expect("raw response should read");
    let response = String::from_utf8_lossy(&response);
    response
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|status| status.parse().ok())
        .expect("raw response should contain a status code")
}

#[tokio::test]
async fn admin_encoded_path_rate_limits_authentication_attempts() {
    let gateway = build_router_with_state(AppState::new().expect("gateway should build"));
    let (gateway_url, gateway_handle) = start_server(gateway).await;
    let client = reqwest::Client::new();

    for _ in 0..30 {
        let response = client
            .get(format!("{gateway_url}/api/admin/users%2F123"))
            .bearer_auth("not-a-valid-token")
            .send()
            .await
            .expect("request should succeed");
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    let limited = client
        .get(format!("{gateway_url}/api/admin/users%2F123"))
        .bearer_auth("not-a-valid-token")
        .send()
        .await
        .expect("request should succeed");
    assert_eq!(limited.status(), StatusCode::TOO_MANY_REQUESTS);
    assert!(limited.headers().get(http::header::RETRY_AFTER).is_some());

    gateway_handle.abort();
}
