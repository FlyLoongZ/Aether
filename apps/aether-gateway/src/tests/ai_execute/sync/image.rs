use super::{
    any, build_router_with_state, build_state_with_execution_runtime_override, json, start_server,
    to_bytes, Arc, Json, Mutex, Request, Router, StatusCode, TRACE_ID_HEADER,
};
use crate::tests::hash_api_key;
use crate::tests::DEV_FERNET_KEY;
use aether_crypto::encrypt_python_fernet_plaintext;
use aether_data::repository::auth::{
    InMemoryAuthApiKeySnapshotRepository, StoredAuthApiKeySnapshot,
};
use aether_data::repository::candidate_selection::InMemoryMinimalCandidateSelectionReadRepository;
use aether_data::repository::candidates::InMemoryRequestCandidateRepository;
use aether_data::repository::provider_catalog::InMemoryProviderCatalogReadRepository;
use aether_data_contracts::repository::candidate_selection::StoredMinimalCandidateSelectionRow;
use aether_data_contracts::repository::provider_catalog::{
    StoredProviderCatalogEndpoint, StoredProviderCatalogKey, StoredProviderCatalogProvider,
};
use base64::Engine as _;

fn run_image_sync_test<F, Fut>(test_name: &'static str, make_future: F)
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: std::future::Future<Output = ()> + 'static,
{
    crate::tests::run_async_test_on_large_stack(
        test_name,
        crate::tests::LARGE_TEST_STACK_BYTES,
        make_future,
    );
}

#[test]
fn gateway_converts_openai_image_sync_to_gemini_image_provider() {
    run_image_sync_test(
        "gateway_converts_openai_image_sync_to_gemini_image_provider",
        gateway_converts_openai_image_sync_to_gemini_image_provider_impl,
    );
}

async fn gateway_converts_openai_image_sync_to_gemini_image_provider_impl() {
    #[derive(Debug, Clone)]
    struct SeenExecutionRuntimeSyncRequest {
        trace_id: String,
        url: String,
        auth_header_value: String,
        has_model_field: bool,
        prompt: String,
        response_modalities: Vec<String>,
        image_size: String,
    }

    fn sample_auth_snapshot(api_key_id: &str, user_id: &str) -> StoredAuthApiKeySnapshot {
        crate::tests::support::sample_auth_snapshot_with(
            api_key_id,
            user_id,
            "alice",
            "alice@example.com",
            serde_json::json!(["openai", "google"]),
            serde_json::json!(["openai:image"]),
            serde_json::json!(["gpt-image-2"]),
        )
    }

    fn sample_candidate_row() -> StoredMinimalCandidateSelectionRow {
        crate::tests::support::minimal_candidate_row(&crate::tests::support::CandidateRowSpec {
            id_suffix: "gemini-image-bridge-1",
            provider_name: "google",
            provider_type: "google",
            api_format: "gemini:generate_content",
            api_family: "gemini",
            endpoint_kind: Some("chat"),
            key_name: "prod",
            key_auth_type: "api_key",
            global_model_name: "gpt-image-2",
            upstream_model: "gemini-2.5-flash-image-upstream",
        })
    }

    fn sample_provider_catalog_provider() -> StoredProviderCatalogProvider {
        StoredProviderCatalogProvider::new(
            "provider-gemini-image-bridge-1".to_string(),
            "google".to_string(),
            Some("https://generativelanguage.googleapis.com".to_string()),
            "google".to_string(),
        )
        .expect("provider should build")
        .with_transport_fields(
            true,
            false,
            false,
            None,
            Some(2),
            None,
            Some(20.0),
            None,
            None,
        )
    }

    fn sample_provider_catalog_endpoint() -> StoredProviderCatalogEndpoint {
        StoredProviderCatalogEndpoint::new(
            "endpoint-gemini-image-bridge-1".to_string(),
            "provider-gemini-image-bridge-1".to_string(),
            "gemini:generate_content".to_string(),
            Some("gemini".to_string()),
            Some("chat".to_string()),
            true,
        )
        .expect("endpoint should build")
        .with_transport_fields(
            "https://generativelanguage.googleapis.com".to_string(),
            None,
            Some(serde_json::json!([
                {"action":"drop","path":"model"}
            ])),
            Some(2),
            None,
            None,
            None,
            None,
        )
        .expect("endpoint transport should build")
    }

    fn sample_provider_catalog_key() -> StoredProviderCatalogKey {
        StoredProviderCatalogKey::new(
            "key-gemini-image-bridge-1".to_string(),
            "provider-gemini-image-bridge-1".to_string(),
            "prod".to_string(),
            "api_key".to_string(),
            None,
            true,
        )
        .expect("key should build")
        .with_transport_fields(
            Some(serde_json::json!(["gemini:generate_content"])),
            encrypt_python_fernet_plaintext(DEV_FERNET_KEY, "sk-upstream-gemini-image")
                .expect("api key should encrypt"),
            None,
            None,
            Some(serde_json::json!({"gemini:generate_content": 1})),
            None,
            None,
            None,
            None,
        )
        .expect("key transport should build")
    }

    let seen_execution_runtime = Arc::new(Mutex::new(None::<SeenExecutionRuntimeSyncRequest>));
    let seen_execution_runtime_clone = Arc::clone(&seen_execution_runtime);
    let execution_runtime = Router::new().route(
        "/v1/execute/sync",
        any(move |request: Request| {
            let seen_execution_runtime_inner = Arc::clone(&seen_execution_runtime_clone);
            async move {
                let (parts, body) = request.into_parts();
                let raw_body = to_bytes(body, usize::MAX).await.expect("body should read");
                let payload: serde_json::Value = serde_json::from_slice(&raw_body)
                    .expect("execution runtime payload should parse");
                let body_json = payload
                    .get("body")
                    .and_then(|value| value.get("json_body"))
                    .cloned()
                    .unwrap_or_else(|| json!({}));
                *seen_execution_runtime_inner
                    .lock()
                    .expect("mutex should lock") = Some(SeenExecutionRuntimeSyncRequest {
                    trace_id: crate::tests::support::header_string(&parts.headers, TRACE_ID_HEADER),
                    url: crate::tests::support::payload_string(&payload, &["url"]),
                    auth_header_value: crate::tests::support::payload_string(&payload, &["headers", "x-goog-api-key"]),
                    has_model_field: body_json.get("model").is_some(),
                    prompt: body_json
                        .get("contents")
                        .and_then(|value| value.get(0))
                        .and_then(|value| value.get("parts"))
                        .and_then(|value| value.get(0))
                        .and_then(|value| value.get("text"))
                        .and_then(|value| value.as_str())
                        .unwrap_or_default()
                        .to_string(),
                    response_modalities: body_json
                        .get("generationConfig")
                        .and_then(|value| value.get("responseModalities"))
                        .and_then(|value| value.as_array())
                        .map(|items| {
                            items
                                .iter()
                                .filter_map(|item| item.as_str().map(ToOwned::to_owned))
                                .collect()
                        })
                        .unwrap_or_default(),
                    image_size: body_json
                        .get("generationConfig")
                        .and_then(|value| value.get("imageSize"))
                        .and_then(|value| value.as_str())
                        .unwrap_or_default()
                        .to_string(),
                });
                Json(json!({
                    "request_id": "trace-openai-image-to-gemini-123",
                    "status_code": 200,
                    "headers": {
                        "content-type": "application/json"
                    },
                    "body": {
                        "json_body": {
                            "modelVersion": "gemini-2.5-flash-image-upstream",
                            "usageMetadata": {
                                "promptTokenCount": 11,
                                "candidatesTokenCount": 22,
                                "totalTokenCount": 33
                            },
                            "candidates": [{
                                "content": {
                                    "role": "model",
                                    "parts": [
                                        {"text": "revised kite prompt"},
                                        {"inlineData": {"mimeType": "image/png", "data": "aGVsbG8="}}
                                    ]
                                },
                                "finishReason": "STOP"
                            }]
                        }
                    },
                    "telemetry": {
                        "elapsed_ms": 37
                    }
                }))
            }
        }),
    );

    let client_api_key = "sk-client-openai-image-to-gemini";
    let auth_repository = Arc::new(InMemoryAuthApiKeySnapshotRepository::seed(vec![(
        Some(hash_api_key(client_api_key)),
        sample_auth_snapshot(
            "key-openai-image-client-bridge-1",
            "user-openai-image-bridge-1",
        ),
    )]));
    let candidate_selection_repository =
        Arc::new(InMemoryMinimalCandidateSelectionReadRepository::seed(vec![
            sample_candidate_row(),
        ]));
    let provider_catalog_repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![sample_provider_catalog_provider()],
        vec![sample_provider_catalog_endpoint()],
        vec![sample_provider_catalog_key()],
    ));

    let (execution_runtime_url, execution_runtime_handle) = start_server(execution_runtime).await;
    let gateway_state = build_state_with_execution_runtime_override(execution_runtime_url)
        .with_data_state_for_tests(
            crate::data::GatewayDataState::with_auth_candidate_selection_provider_catalog_and_request_candidate_repository_for_tests(
                auth_repository,
                candidate_selection_repository,
                provider_catalog_repository,
                Arc::new(InMemoryRequestCandidateRepository::default()),
                DEV_FERNET_KEY,
            ),
        );
    let gateway = build_router_with_state(gateway_state);
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .post(format!("{gateway_url}/v1/images/generations"))
        .header(http::header::CONTENT_TYPE, "application/json")
        .header(
            http::header::AUTHORIZATION,
            format!("Bearer {client_api_key}"),
        )
        .header(TRACE_ID_HEADER, "trace-openai-image-to-gemini-123")
        .body(
            "{\"model\":\"gpt-image-2\",\"prompt\":\"Draw a red kite\",\"size\":\"1024x1024\",\"response_format\":\"b64_json\"}",
        )
        .send()
        .await
        .expect("request should succeed");

    let response_status = response.status();
    let response_body = response.text().await.expect("body should read");
    assert_eq!(response_status, StatusCode::OK, "{response_body}");
    let response_json: serde_json::Value =
        serde_json::from_str(&response_body).expect("body should parse");
    assert_eq!(response_json["data"][0]["b64_json"], "aGVsbG8=");
    assert_eq!(
        response_json["data"][0]["revised_prompt"],
        "revised kite prompt"
    );
    assert_eq!(response_json["model"], "gemini-2.5-flash-image-upstream");
    assert_eq!(response_json["usage"]["input_tokens"], 11);
    assert_eq!(response_json["usage"]["output_tokens"], 22);

    let seen_execution_runtime_request = seen_execution_runtime
        .lock()
        .expect("mutex should lock")
        .clone()
        .expect("execution runtime sync should be captured");
    assert_eq!(
        seen_execution_runtime_request.trace_id,
        "trace-openai-image-to-gemini-123"
    );
    assert_eq!(
        seen_execution_runtime_request.url,
        "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash-image-upstream:generateContent"
    );
    assert_eq!(
        seen_execution_runtime_request.auth_header_value,
        "sk-upstream-gemini-image"
    );
    assert!(!seen_execution_runtime_request.has_model_field);
    assert_eq!(seen_execution_runtime_request.prompt, "Draw a red kite");
    assert_eq!(
        seen_execution_runtime_request.response_modalities,
        vec!["TEXT".to_string(), "IMAGE".to_string()]
    );
    assert_eq!(seen_execution_runtime_request.image_size, "1024x1024");

    gateway_handle.abort();
    execution_runtime_handle.abort();
}

#[test]
fn gateway_converts_gemini_image_sync_to_openai_image_provider() {
    run_image_sync_test(
        "gateway_converts_gemini_image_sync_to_openai_image_provider",
        gateway_converts_gemini_image_sync_to_openai_image_provider_impl,
    );
}

async fn gateway_converts_gemini_image_sync_to_openai_image_provider_impl() {
    #[derive(Debug, Clone)]
    struct SeenExecutionRuntimeSyncRequest {
        trace_id: String,
        url: String,
        authorization: String,
        model: String,
        prompt: String,
        images: serde_json::Value,
        has_legacy_image_field: bool,
        request_stream: bool,
        body_stream: Option<bool>,
    }

    fn sample_auth_snapshot(api_key_id: &str, user_id: &str) -> StoredAuthApiKeySnapshot {
        crate::tests::support::sample_auth_snapshot_with(
            api_key_id,
            user_id,
            "alice",
            "alice@example.com",
            serde_json::json!(["gemini", "openai"]),
            serde_json::json!(["gemini:generate_content"]),
            serde_json::json!(["gemini-image"]),
        )
    }

    fn sample_candidate_row() -> StoredMinimalCandidateSelectionRow {
        crate::tests::support::minimal_candidate_row(&crate::tests::support::CandidateRowSpec {
            id_suffix: "openai-image-bridge-1",
            provider_name: "openai",
            provider_type: "openai",
            api_format: "openai:image",
            api_family: "openai",
            endpoint_kind: Some("image"),
            key_name: "prod",
            key_auth_type: "api_key",
            global_model_name: "gemini-image",
            upstream_model: "gpt-image-2-upstream",
        })
    }

    fn sample_provider_catalog_provider() -> StoredProviderCatalogProvider {
        StoredProviderCatalogProvider::new(
            "provider-openai-image-bridge-1".to_string(),
            "openai".to_string(),
            Some("https://api.openai.com/v1".to_string()),
            "openai".to_string(),
        )
        .expect("provider should build")
        .with_transport_fields(
            true,
            false,
            false,
            None,
            Some(2),
            None,
            Some(20.0),
            None,
            None,
        )
    }

    fn sample_provider_catalog_endpoint() -> StoredProviderCatalogEndpoint {
        StoredProviderCatalogEndpoint::new(
            "endpoint-openai-image-bridge-1".to_string(),
            "provider-openai-image-bridge-1".to_string(),
            "openai:image".to_string(),
            Some("openai".to_string()),
            Some("image".to_string()),
            true,
        )
        .expect("endpoint should build")
        .with_transport_fields(
            "https://api.openai.com/v1".to_string(),
            None,
            None,
            Some(2),
            None,
            None,
            None,
            None,
        )
        .expect("endpoint transport should build")
    }

    fn sample_provider_catalog_key() -> StoredProviderCatalogKey {
        StoredProviderCatalogKey::new(
            "key-openai-image-bridge-1".to_string(),
            "provider-openai-image-bridge-1".to_string(),
            "prod".to_string(),
            "api_key".to_string(),
            None,
            true,
        )
        .expect("key should build")
        .with_transport_fields(
            Some(serde_json::json!(["openai:image"])),
            encrypt_python_fernet_plaintext(DEV_FERNET_KEY, "sk-upstream-openai-image")
                .expect("api key should encrypt"),
            None,
            None,
            Some(serde_json::json!({"openai:image": 1})),
            None,
            None,
            None,
            None,
        )
        .expect("key transport should build")
    }

    let seen_execution_runtime = Arc::new(Mutex::new(None::<SeenExecutionRuntimeSyncRequest>));
    let seen_execution_runtime_clone = Arc::clone(&seen_execution_runtime);
    let execution_runtime = Router::new().route(
        "/v1/execute/sync",
        any(move |request: Request| {
            let seen_execution_runtime_inner = Arc::clone(&seen_execution_runtime_clone);
            async move {
                let (parts, body) = request.into_parts();
                let raw_body = to_bytes(body, usize::MAX).await.expect("body should read");
                let payload: serde_json::Value = serde_json::from_slice(&raw_body)
                    .expect("execution runtime payload should parse");
                let body_json = payload
                    .get("body")
                    .and_then(|value| value.get("json_body"))
                    .cloned()
                    .unwrap_or_else(|| json!({}));
                *seen_execution_runtime_inner
                    .lock()
                    .expect("mutex should lock") = Some(SeenExecutionRuntimeSyncRequest {
                    trace_id: crate::tests::support::header_string(&parts.headers, TRACE_ID_HEADER),
                    url: crate::tests::support::payload_string(&payload, &["url"]),
                    authorization: crate::tests::support::payload_string(
                        &payload,
                        &["headers", "authorization"],
                    ),
                    model: body_json
                        .get("model")
                        .and_then(|value| value.as_str())
                        .unwrap_or_default()
                        .to_string(),
                    prompt: body_json
                        .get("prompt")
                        .and_then(|value| value.as_str())
                        .unwrap_or_default()
                        .to_string(),
                    images: body_json.get("images").cloned().unwrap_or_default(),
                    has_legacy_image_field: body_json.get("image").is_some(),
                    request_stream: payload
                        .get("stream")
                        .and_then(|value| value.as_bool())
                        .unwrap_or(true),
                    body_stream: body_json.get("stream").and_then(serde_json::Value::as_bool),
                });
                Json(json!({
                    "request_id": "trace-gemini-image-to-openai-123",
                    "status_code": 200,
                    "headers": {
                        "content-type": "application/json"
                    },
                    "body": {
                        "json_body": {
                            "created": 1776839946,
                            "model": "gpt-image-2-upstream",
                            "usage": {
                                "input_tokens": 3,
                                "output_tokens": 4,
                                "total_tokens": 7
                            },
                            "data": [{
                                "revised_prompt": "converted gemini prompt",
                                "b64_json": "aGVsbG8="
                            }]
                        }
                    },
                    "telemetry": {
                        "elapsed_ms": 43
                    }
                }))
            }
        }),
    );

    let client_api_key = "client-gemini-image-to-openai";
    let auth_repository = Arc::new(InMemoryAuthApiKeySnapshotRepository::seed(vec![(
        Some(hash_api_key(client_api_key)),
        sample_auth_snapshot(
            "key-gemini-image-client-bridge-1",
            "user-gemini-image-bridge-1",
        ),
    )]));
    let candidate_selection_repository =
        Arc::new(InMemoryMinimalCandidateSelectionReadRepository::seed(vec![
            sample_candidate_row(),
        ]));
    let provider_catalog_repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![sample_provider_catalog_provider()],
        vec![sample_provider_catalog_endpoint()],
        vec![sample_provider_catalog_key()],
    ));

    let (execution_runtime_url, execution_runtime_handle) = start_server(execution_runtime).await;
    let gateway_state = build_state_with_execution_runtime_override(execution_runtime_url)
        .with_data_state_for_tests(
            crate::data::GatewayDataState::with_auth_candidate_selection_provider_catalog_and_request_candidate_repository_for_tests(
                auth_repository,
                candidate_selection_repository,
                provider_catalog_repository,
                Arc::new(InMemoryRequestCandidateRepository::default()),
                DEV_FERNET_KEY,
            ),
        );
    let gateway = build_router_with_state(gateway_state);
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .post(format!(
            "{gateway_url}/v1beta/models/gemini-image:generateContent?key={client_api_key}"
        ))
        .header(http::header::CONTENT_TYPE, "application/json")
        .header(TRACE_ID_HEADER, "trace-gemini-image-to-openai-123")
        .body(
            "{\"generationConfig\":{\"responseModalities\":[\"TEXT\",\"IMAGE\"]},\"contents\":[{\"role\":\"user\",\"parts\":[{\"text\":\"Change the background\"},{\"inlineData\":{\"mimeType\":\"image/png\",\"data\":\"aGVsbG8=\"}}]}]}",
        )
        .send()
        .await
        .expect("request should succeed");

    let response_status = response.status();
    let response_body = response.text().await.expect("body should read");
    assert_eq!(response_status, StatusCode::OK, "{response_body}");
    let response_json: serde_json::Value =
        serde_json::from_str(&response_body).expect("body should parse");
    assert_eq!(response_json["modelVersion"], "gpt-image-2-upstream");
    assert_eq!(
        response_json["candidates"][0]["content"]["parts"][0]["text"],
        "converted gemini prompt"
    );
    assert_eq!(
        response_json["candidates"][0]["content"]["parts"][1]["inlineData"]["mimeType"],
        "image/png"
    );
    assert_eq!(
        response_json["candidates"][0]["content"]["parts"][1]["inlineData"]["data"],
        "aGVsbG8="
    );
    assert_eq!(response_json["usageMetadata"]["promptTokenCount"], 3);
    assert_eq!(response_json["usageMetadata"]["candidatesTokenCount"], 4);

    let seen_execution_runtime_request = seen_execution_runtime
        .lock()
        .expect("mutex should lock")
        .clone()
        .expect("execution runtime sync should be captured");
    assert_eq!(
        seen_execution_runtime_request.trace_id,
        "trace-gemini-image-to-openai-123"
    );
    assert_eq!(
        seen_execution_runtime_request.url,
        "https://api.openai.com/v1/images/edits"
    );
    assert_eq!(
        seen_execution_runtime_request.authorization,
        "Bearer sk-upstream-openai-image"
    );
    assert_eq!(seen_execution_runtime_request.model, "gpt-image-2-upstream");
    assert_eq!(
        seen_execution_runtime_request.prompt,
        "Change the background"
    );
    assert_eq!(
        seen_execution_runtime_request.images,
        json!([{"image_url": "data:image/png;base64,aGVsbG8="}])
    );
    assert!(!seen_execution_runtime_request.has_legacy_image_field);
    assert!(!seen_execution_runtime_request.request_stream);
    assert_eq!(seen_execution_runtime_request.body_stream, None);

    gateway_handle.abort();
    execution_runtime_handle.abort();
}

#[test]
fn gateway_executes_codex_image_sync_via_local_decision_gate_after_oauth_refresh() {
    run_image_sync_test(
        "gateway_executes_codex_image_sync_via_local_decision_gate_after_oauth_refresh",
        gateway_executes_codex_image_sync_via_local_decision_gate_after_oauth_refresh_impl,
    );
}

async fn gateway_executes_codex_image_sync_via_local_decision_gate_after_oauth_refresh_impl() {
    #[derive(Debug, Clone)]
    struct SeenExecutionRuntimeSyncRequest {
        trace_id: String,
        url: String,
        authorization: String,
        headers: serde_json::Value,
        body: serde_json::Value,
        plan_stream: bool,
    }

    #[derive(Debug, Clone)]
    struct SeenRefreshRequest {
        content_type: String,
        body: String,
    }

    fn sample_auth_snapshot(api_key_id: &str, user_id: &str) -> StoredAuthApiKeySnapshot {
        crate::tests::support::sample_auth_snapshot_with(
            api_key_id,
            user_id,
            "alice",
            "alice@example.com",
            serde_json::json!(["openai", "codex"]),
            serde_json::json!(["openai:image"]),
            serde_json::json!(["gpt-image-2"]),
        )
    }

    fn sample_candidate_row() -> StoredMinimalCandidateSelectionRow {
        crate::tests::support::minimal_candidate_row(&crate::tests::support::CandidateRowSpec {
            id_suffix: "codex-image-local-1",
            provider_name: "codex",
            provider_type: "codex",
            api_format: "openai:image",
            api_family: "openai",
            endpoint_kind: Some("image"),
            key_name: "oauth",
            key_auth_type: "oauth",
            global_model_name: "gpt-image-2",
            upstream_model: "gpt-image-2",
        })
    }

    fn sample_provider_catalog_provider() -> StoredProviderCatalogProvider {
        StoredProviderCatalogProvider::new(
            "provider-codex-image-local-1".to_string(),
            "codex".to_string(),
            Some("https://chatgpt.com".to_string()),
            "codex".to_string(),
        )
        .expect("provider should build")
        .with_transport_fields(
            true,
            false,
            false,
            None,
            Some(2),
            None,
            Some(20.0),
            None,
            None,
        )
    }

    fn sample_provider_catalog_endpoint() -> StoredProviderCatalogEndpoint {
        StoredProviderCatalogEndpoint::new(
            "endpoint-codex-image-local-1".to_string(),
            "provider-codex-image-local-1".to_string(),
            "openai:image".to_string(),
            Some("openai".to_string()),
            Some("image".to_string()),
            true,
        )
        .expect("endpoint should build")
        .with_transport_fields(
            "https://chatgpt.com/backend-api/codex".to_string(),
            None,
            None,
            Some(2),
            None,
            Some(serde_json::json!({"upstream_stream_policy":"force_stream"})),
            None,
            None,
        )
        .expect("endpoint transport should build")
    }

    fn sample_provider_catalog_key() -> StoredProviderCatalogKey {
        let encrypted_auth_config = encrypt_python_fernet_plaintext(
            DEV_FERNET_KEY,
            r#"{"provider_type":"codex","refresh_token":"rt-codex-image-local-123"}"#,
        )
        .expect("auth config should encrypt");
        StoredProviderCatalogKey::new(
            "key-codex-image-local-1".to_string(),
            "provider-codex-image-local-1".to_string(),
            "oauth".to_string(),
            "oauth".to_string(),
            None,
            true,
        )
        .expect("key should build")
        .with_transport_fields(
            Some(serde_json::json!(["openai:image"])),
            encrypt_python_fernet_plaintext(DEV_FERNET_KEY, "__placeholder__")
                .expect("placeholder api key should encrypt"),
            Some(encrypted_auth_config),
            None,
            Some(serde_json::json!({"openai:image": 1})),
            None,
            None,
            None,
            None,
        )
        .expect("key transport should build")
    }

    let seen_execution_runtime = Arc::new(Mutex::new(None::<SeenExecutionRuntimeSyncRequest>));
    let seen_execution_runtime_clone = Arc::clone(&seen_execution_runtime);
    let seen_refresh = Arc::new(Mutex::new(None::<SeenRefreshRequest>));
    let seen_refresh_clone = Arc::clone(&seen_refresh);
    let refresh_hits = Arc::new(Mutex::new(0usize));
    let refresh_hits_clone = Arc::clone(&refresh_hits);

    let refresh = Router::new().route(
        "/oauth/token",
        any(move |request: Request| {
            let seen_refresh_inner = Arc::clone(&seen_refresh_clone);
            let refresh_hits_inner = Arc::clone(&refresh_hits_clone);
            async move {
                *refresh_hits_inner.lock().expect("mutex should lock") += 1;
                let (parts, body) = request.into_parts();
                let raw_body = to_bytes(body, usize::MAX).await.expect("body should read");
                *seen_refresh_inner.lock().expect("mutex should lock") = Some(SeenRefreshRequest {
                    content_type: crate::tests::support::header_string(
                        &parts.headers,
                        http::header::CONTENT_TYPE,
                    ),
                    body: String::from_utf8(raw_body.to_vec())
                        .expect("refresh body should be utf8"),
                });
                Json(json!({
                    "access_token": "refreshed-codex-image-access-token",
                    "refresh_token": "rt-codex-image-local-456",
                    "token_type": "Bearer",
                    "expires_in": 3600
                }))
            }
        }),
    );

    let execution_runtime = Router::new().route(
        "/v1/execute/sync",
        any(move |request: Request| {
            let seen_execution_runtime_inner = Arc::clone(&seen_execution_runtime_clone);
            async move {
                let (parts, body) = request.into_parts();
                let raw_body = to_bytes(body, usize::MAX).await.expect("body should read");
                let payload: serde_json::Value = serde_json::from_slice(&raw_body)
                    .expect("execution runtime payload should parse");
                *seen_execution_runtime_inner
                    .lock()
                    .expect("mutex should lock") = Some(SeenExecutionRuntimeSyncRequest {
                    trace_id: crate::tests::support::header_string(&parts.headers, TRACE_ID_HEADER),
                    url: crate::tests::support::payload_string(&payload, &["url"]),
                    authorization: crate::tests::support::payload_string(&payload, &["headers", "authorization"]),
                    headers: payload.get("headers").cloned().unwrap_or_default(),
                    body: payload
                        .get("body")
                        .and_then(|value| value.get("json_body"))
                        .cloned()
                        .unwrap_or_default(),
                    plan_stream: payload
                        .get("stream")
                        .and_then(|value| value.as_bool())
                        .unwrap_or(false),
                });
                Json(json!({
                    "request_id": "trace-codex-image-local-123",
                    "status_code": 200,
                    "headers": {
                        "content-type": "application/json"
                    },
                    "body": {
                        "body_bytes_b64": base64::engine::general_purpose::STANDARD.encode(
                            r#"{"created":1776839946,"data":[{"b64_json":"aGVsbG8=","revised_prompt":"水墨视觉海报"}],"usage":{"input_tokens":171,"output_tokens":1372,"total_tokens":1543}}"#
                        )
                    },
                    "telemetry": {
                        "elapsed_ms": 41
                    }
                }))
            }
        }),
    );

    let client_api_key = "sk-client-codex-image-local";
    let auth_repository = Arc::new(InMemoryAuthApiKeySnapshotRepository::seed(vec![(
        Some(hash_api_key(client_api_key)),
        sample_auth_snapshot("key-codex-image-client-123", "user-codex-image-client-123"),
    )]));
    let candidate_selection_repository =
        Arc::new(InMemoryMinimalCandidateSelectionReadRepository::seed(vec![
            sample_candidate_row(),
        ]));
    let provider_catalog_repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![sample_provider_catalog_provider()],
        vec![sample_provider_catalog_endpoint()],
        vec![sample_provider_catalog_key()],
    ));

    let (refresh_url, refresh_handle) = start_server(refresh).await;
    let (execution_runtime_url, execution_runtime_handle) = start_server(execution_runtime).await;
    let oauth_refresh =
        crate::provider_transport::LocalOAuthRefreshCoordinator::with_adapters_for_tests(vec![
            Arc::new(
                crate::provider_transport::oauth_refresh::GenericOAuthRefreshAdapter::default()
                    .with_token_url_for_tests("codex", format!("{refresh_url}/oauth/token")),
            ),
        ]);
    let gateway_state = build_state_with_execution_runtime_override(execution_runtime_url.clone())
        .with_data_state_for_tests(
            crate::data::GatewayDataState::with_auth_candidate_selection_provider_catalog_and_request_candidate_repository_for_tests(
                auth_repository,
                candidate_selection_repository,
                provider_catalog_repository.clone(),
                Arc::new(InMemoryRequestCandidateRepository::default()),
                DEV_FERNET_KEY,
            ),
        )
        .with_oauth_refresh_coordinator_for_tests(oauth_refresh);
    let gateway = build_router_with_state(gateway_state);
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .post(format!("{gateway_url}/v1/images/generations"))
        .header(http::header::CONTENT_TYPE, "application/json")
        .header(
            http::header::AUTHORIZATION,
            format!("Bearer {client_api_key}"),
        )
        .header(TRACE_ID_HEADER, "trace-codex-image-local-123")
        .body("{\"model\":\"gpt-image-2\",\"prompt\":\"生成一张水墨视觉海报\",\"background\":\"auto\",\"quality\":\"auto\",\"size\":\"auto\",\"n\":1,\"response_format\":\"b64_json\"}")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let response_json: serde_json::Value = response.json().await.expect("body should parse");
    assert_eq!(response_json["created"], 1776839946);
    assert_eq!(response_json["data"][0]["b64_json"], "aGVsbG8=");
    assert_eq!(response_json["data"][0]["revised_prompt"], "水墨视觉海报");
    assert_eq!(response_json["usage"]["input_tokens"], 171);
    assert_eq!(response_json["usage"]["output_tokens"], 1372);

    let seen_refresh_request = seen_refresh
        .lock()
        .expect("mutex should lock")
        .clone()
        .expect("refresh request should be captured");
    assert_eq!(
        seen_refresh_request.content_type,
        "application/x-www-form-urlencoded"
    );
    assert!(seen_refresh_request
        .body
        .contains("grant_type=refresh_token"));
    assert!(seen_refresh_request
        .body
        .contains("client_id=app_EMoamEEZ73f0CkXaXp7hrann"));
    assert!(seen_refresh_request
        .body
        .contains("refresh_token=rt-codex-image-local-123"));
    assert_eq!(*refresh_hits.lock().expect("mutex should lock"), 1);

    let seen_execution_runtime_request = seen_execution_runtime
        .lock()
        .expect("mutex should lock")
        .clone()
        .expect("execution runtime sync should be captured");
    assert_eq!(
        seen_execution_runtime_request.trace_id,
        "trace-codex-image-local-123"
    );
    assert_eq!(
        seen_execution_runtime_request.url,
        "https://chatgpt.com/backend-api/codex/images/generations"
    );
    assert_eq!(
        seen_execution_runtime_request.authorization,
        "Bearer refreshed-codex-image-access-token"
    );
    assert_eq!(
        seen_execution_runtime_request.body,
        json!({
            "prompt": "生成一张水墨视觉海报",
            "background": "auto",
            "model": "gpt-image-2",
            "n": 1,
            "quality": "auto",
            "size": "auto"
        })
    );
    assert_eq!(
        seen_execution_runtime_request.headers["user-agent"],
        aether_ai_formats::codex_client_user_agent()
    );
    assert_eq!(
        seen_execution_runtime_request.headers["originator"],
        "codex_cli_rs"
    );
    for header in ["x-client-request-id", "session-id", "thread-id"] {
        assert!(seen_execution_runtime_request.headers.get(header).is_none());
    }
    assert!(!seen_execution_runtime_request.plan_stream);

    let persisted_transport_state =
        crate::data::GatewayDataState::with_provider_transport_reader_for_tests(
            provider_catalog_repository,
            DEV_FERNET_KEY,
        );
    let persisted_transport = persisted_transport_state
        .read_provider_transport_snapshot(
            "provider-codex-image-local-1",
            "endpoint-codex-image-local-1",
            "key-codex-image-local-1",
        )
        .await
        .expect("provider transport should read")
        .expect("provider transport should exist");
    assert_eq!(
        persisted_transport.key.decrypted_api_key,
        "refreshed-codex-image-access-token"
    );
    assert!(persisted_transport.key.expires_at_unix_secs.is_some());

    gateway_handle.abort();
    execution_runtime_handle.abort();
    refresh_handle.abort();
}
