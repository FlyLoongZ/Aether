use std::sync::{Arc, Mutex};

use aether_contracts::ExecutionPlan;
use aether_crypto::DEVELOPMENT_ENCRYPTION_KEY;
use aether_data::repository::candidates::InMemoryRequestCandidateRepository;
use aether_data::repository::global_models::InMemoryGlobalModelReadRepository;
use aether_data::repository::provider_catalog::InMemoryProviderCatalogReadRepository;
use aether_data_contracts::repository::candidates::{
    RequestCandidateReadRepository, RequestCandidateStatus,
};
use aether_data_contracts::repository::provider_catalog::{
    ProviderCatalogReadRepository, StoredProviderCatalogEndpoint,
};
use axum::body::Body;
use axum::routing::any;
use axum::{extract::Request, Json, Router};
use base64::Engine as _;
use http::StatusCode;
use serde_json::json;

use super::super::{
    build_router_with_state, build_state_with_execution_runtime_override,
    sample_admin_provider_model, sample_bound_auth_config, sample_bound_key, sample_endpoint,
    sample_provider, start_server, AppState,
};
use crate::constants::{
    GATEWAY_HEADER, TRUSTED_ADMIN_SESSION_ID_HEADER, TRUSTED_ADMIN_USER_ID_HEADER,
    TRUSTED_ADMIN_USER_ROLE_HEADER,
};
use crate::data::GatewayDataState;

mod embeddings;
mod failover;
mod misc;
mod models;
mod providers;
mod test_model;

const PROVIDER_QUERY_TEST_STACK_BYTES: usize = 16 * 1024 * 1024;

fn run_provider_query_test<F, Fut>(test_name: &'static str, make_future: F)
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: std::future::Future<Output = ()> + 'static,
{
    crate::tests::run_async_test_on_large_stack(
        test_name,
        PROVIDER_QUERY_TEST_STACK_BYTES,
        make_future,
    );
}

fn crc32(data: &[u8]) -> u32 {
    let mut crc = 0xffff_ffffu32;
    for &byte in data {
        crc ^= byte as u32;
        for _ in 0..8 {
            let mask = if crc & 1 == 1 { 0xedb8_8320 } else { 0 };
            crc = (crc >> 1) ^ mask;
        }
    }
    !crc
}

fn encode_string_header(name: &str, value: &str) -> Vec<u8> {
    let mut out = Vec::new();
    out.push(name.len() as u8);
    out.extend_from_slice(name.as_bytes());
    out.push(7);
    out.extend_from_slice(&(value.len() as u16).to_be_bytes());
    out.extend_from_slice(value.as_bytes());
    out
}

fn encode_frame(headers: Vec<u8>, payload: Vec<u8>) -> Vec<u8> {
    let total_len = 12 + headers.len() + payload.len() + 4;
    let header_len = headers.len();
    let mut out = Vec::with_capacity(total_len);
    out.extend_from_slice(&(total_len as u32).to_be_bytes());
    out.extend_from_slice(&(header_len as u32).to_be_bytes());
    let prelude_crc = crc32(&out[..8]);
    out.extend_from_slice(&prelude_crc.to_be_bytes());
    out.extend_from_slice(&headers);
    out.extend_from_slice(&payload);
    let message_crc = crc32(&out);
    out.extend_from_slice(&message_crc.to_be_bytes());
    out
}

fn encode_kiro_event_frame(event_type: &str, payload: serde_json::Value) -> Vec<u8> {
    let mut headers = encode_string_header(":message-type", "event");
    headers.extend_from_slice(&encode_string_header(":event-type", event_type));
    let payload = serde_json::to_vec(&payload).expect("payload should encode");
    encode_frame(headers, payload)
}

fn encode_kiro_exception_frame(exception_type: &str) -> Vec<u8> {
    let mut headers = encode_string_header(":message-type", "exception");
    headers.extend_from_slice(&encode_string_header(":exception-type", exception_type));
    encode_frame(headers, Vec::new())
}

async fn assert_admin_provider_query_route(
    path: &str,
    request_payload: serde_json::Value,
    expected_status: StatusCode,
    expected_payload_assertions: impl FnOnce(&serde_json::Value),
) {
    let upstream_hits = Arc::new(Mutex::new(0usize));
    let upstream_hits_clone = Arc::clone(&upstream_hits);
    let upstream = Router::new().route(
        path,
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

    let response = reqwest::Client::new()
        .post(format!("{gateway_url}{path}"))
        .header(crate::constants::GATEWAY_HEADER, "rust-phase3b")
        .header(TRUSTED_ADMIN_USER_ID_HEADER, "admin-user-123")
        .header(TRUSTED_ADMIN_USER_ROLE_HEADER, "admin")
        .header(TRUSTED_ADMIN_SESSION_ID_HEADER, "session-123")
        .json(&request_payload)
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), expected_status);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    expected_payload_assertions(&payload);
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}
