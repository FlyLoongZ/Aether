use super::*;
use std::sync::Arc;

use aether_data::repository::provider_catalog::InMemoryProviderCatalogReadRepository;
use aether_data_contracts::repository::provider_catalog::{
    ProviderCatalogReadRepository, StoredProviderCatalogEndpoint, StoredProviderCatalogKey,
    StoredProviderCatalogProvider,
};
use axum::body::Body;
use axum::extract::Request;
use axum::routing::any;
use axum::Router;
use http::{Method, StatusCode};

use crate::data::GatewayDataState;

fn sample_plan(base_url: &str, body: Value, stream: bool) -> ExecutionPlan {
    ExecutionPlan {
        request_id: "req-chatgpt-web-image-test".to_string(),
        candidate_id: Some("cand-chatgpt-web-image-test".to_string()),
        provider_name: Some("ChatGPT Web".to_string()),
        provider_id: "provider-chatgpt-web-image-test".to_string(),
        endpoint_id: "endpoint-chatgpt-web-image-test".to_string(),
        key_id: "key-chatgpt-web-image-test".to_string(),
        method: "POST".to_string(),
        url: format!("{base_url}/__aether/chatgpt-web-image"),
        headers: BTreeMap::from([
            (CHATGPT_WEB_INTERNAL_HEADER.to_string(), "1".to_string()),
            (
                "authorization".to_string(),
                "Bearer test-access-token".to_string(),
            ),
        ]),
        content_type: Some("application/json".to_string()),
        content_encoding: None,
        body: RequestBody::from_json(body),
        stream,
        client_api_format: "openai:image".to_string(),
        provider_api_format: "openai:image".to_string(),
        model_name: Some("gpt-image-2".to_string()),
        proxy: None,
        transport_profile: None,
        timeouts: None,
    }
}

fn sample_provider_catalog_provider() -> StoredProviderCatalogProvider {
    StoredProviderCatalogProvider::new(
        "provider-chatgpt-web-image-test".to_string(),
        "ChatGPT Web".to_string(),
        Some(CHATGPT_WEB_DEFAULT_BASE_URL.to_string()),
        "chatgpt_web".to_string(),
    )
    .expect("provider should build")
}

fn sample_provider_catalog_endpoint(base_url: &str) -> StoredProviderCatalogEndpoint {
    StoredProviderCatalogEndpoint::new(
        "endpoint-chatgpt-web-image-test".to_string(),
        "provider-chatgpt-web-image-test".to_string(),
        "openai:image".to_string(),
        Some("openai".to_string()),
        Some("image".to_string()),
        true,
    )
    .expect("endpoint should build")
    .with_transport_fields(
        base_url.to_string(),
        None,
        None,
        None,
        None,
        None,
        None,
        None,
    )
    .expect("endpoint transport fields should build")
}

fn sample_provider_catalog_key(upstream_metadata: Value) -> StoredProviderCatalogKey {
    let mut key = StoredProviderCatalogKey::new(
        "key-chatgpt-web-image-test".to_string(),
        "provider-chatgpt-web-image-test".to_string(),
        "ChatGPT Web test key".to_string(),
        "oauth".to_string(),
        None,
        true,
    )
    .expect("key should build")
    .with_transport_fields(
        Some(json!(["openai:image"])),
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
    )
    .expect("key transport fields should build");
    key.upstream_metadata = Some(upstream_metadata);
    key
}

fn state_with_chatgpt_web_key(
    base_url: &str,
    upstream_metadata: Value,
) -> (AppState, Arc<InMemoryProviderCatalogReadRepository>) {
    let repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![sample_provider_catalog_provider()],
        vec![sample_provider_catalog_endpoint(base_url)],
        vec![sample_provider_catalog_key(upstream_metadata)],
    ));
    let state = crate::AppState::new()
        .expect("state should build")
        .with_data_state_for_tests(
            GatewayDataState::with_provider_catalog_repository_for_tests(Arc::clone(&repository)),
        );
    (state, repository)
}

async fn reloaded_chatgpt_web_metadata(
    repository: &InMemoryProviderCatalogReadRepository,
) -> Map<String, Value> {
    repository
        .list_keys_by_ids(&["key-chatgpt-web-image-test".to_string()])
        .await
        .expect("key reload should succeed")
        .into_iter()
        .next()
        .expect("key should exist")
        .upstream_metadata
        .and_then(|value| value.get("chatgpt_web").cloned())
        .and_then(|value| value.as_object().cloned())
        .expect("chatgpt_web metadata should exist")
}

fn completed_response_from_sse(sse: &str) -> Value {
    sse.lines()
        .find_map(|line| {
            let payload = line.strip_prefix("data: ")?;
            let event = serde_json::from_str::<Value>(payload).ok()?;
            (event.get("type").and_then(Value::as_str) == Some("response.completed"))
                .then_some(event)
        })
        .and_then(|event| event.get("response").cloned())
        .expect("completed response should be present")
}

#[test]
fn gpt_image2_output_token_estimator_matches_pricing_calculator_examples() {
    assert_eq!(gpt_image2_output_tokens(1024, 1024, "low"), 196);
    assert_eq!(gpt_image2_output_tokens(1024, 1024, "medium"), 1756);
    assert_eq!(gpt_image2_output_tokens(1536, 1024, "medium"), 1372);
    assert_eq!(gpt_image2_output_tokens(1024, 1536, "medium"), 1372);
    assert_eq!(gpt_image2_output_tokens(1024, 1024, "high"), 7024);
}

#[test]
fn chatgpt_web_http_status_error_omits_upstream_body() {
    let result = ExecutionResult {
        request_id: "req-chatgpt-web-image-test".to_string(),
        candidate_id: None,
        status_code: 502,
        headers: BTreeMap::new(),
        response_observation: None,
        body: Some(ResponseBody {
            json_body: Some(json!({"error": "Bearer secret-chatgpt-web-body"})),
            body_bytes_b64: None,
        }),
        telemetry: None,
        error: None,
    };

    let error = ensure_success(&result, "ChatGPT-Web bootstrap")
        .expect_err("non-success result should be rejected");
    let message = error.to_string();

    assert!(matches!(
        error,
        ExecutionRuntimeTransportError::UpstreamHttpStatus {
            status_code: 502,
            ..
        }
    ));
    assert_eq!(message, "ChatGPT-Web bootstrap returned HTTP 502");
    assert!(!message.contains("secret-chatgpt-web-body"));
}

#[test]
fn chatgpt_web_success_sse_includes_estimated_image_usage() {
    let request = ChatGptWebImageRequest {
        operation: "generate".to_string(),
        model: "gpt-image-2".to_string(),
        web_model: "gpt-5-5-thinking".to_string(),
        prompt: "draw a test image".to_string(),
        size: "1024x1024".to_string(),
        ratio: "1:1".to_string(),
        output_format: "png".to_string(),
        quality: Some("low".to_string()),
        partial_images: 0,
        images: Vec::new(),
    };
    let image = DownloadedImage {
        b64_json: "aGVsbG8=".repeat(128),
        mime: "image/png".to_string(),
        width: Some(1024),
        height: Some(1024),
    };
    let body = build_success_sse(
        &request,
        &image,
        Some(&json!({
            "image_request": {
                "size": "1024x1024",
                "quality": "low"
            }
        })),
    );
    let completed = completed_response_from_sse(body.as_str());
    let input_tokens = estimate_text_tokens("draw a test image");
    let output_tokens = 196;

    assert_eq!(completed["usage"]["input_tokens"], json!(input_tokens));
    assert_eq!(completed["usage"]["output_tokens"], json!(output_tokens));
    assert_eq!(
        completed["tool_usage"]["image_gen"]["output_tokens"],
        json!(output_tokens)
    );
    assert_eq!(
        completed["tool_usage"]["image_gen"]["input_tokens_details"]["text_tokens"],
        json!(input_tokens)
    );
    assert_eq!(
        completed["tool_usage"]["image_gen"]["output_tokens_details"]["image_tokens"],
        json!(output_tokens)
    );
    assert_eq!(
        completed["usage"]["total_tokens"],
        json!(input_tokens.saturating_add(output_tokens))
    );
}

#[test]
fn chatgpt_web_success_sse_uses_image_dimensions_not_output_text() {
    let request = ChatGptWebImageRequest {
        operation: "generate".to_string(),
        model: "gpt-image-2".to_string(),
        web_model: "gpt-5-5-thinking".to_string(),
        prompt: "draw a test image".to_string(),
        size: "1024x1024".to_string(),
        ratio: "1:1".to_string(),
        output_format: "png".to_string(),
        quality: Some("low".to_string()),
        partial_images: 0,
        images: Vec::new(),
    };
    let image = DownloadedImage {
        b64_json: "iVBORw0KGgoAAAANSUhEUgAA".repeat(64),
        mime: "image/png".to_string(),
        width: Some(1402),
        height: Some(1122),
    };
    let body = build_success_sse(
        &request,
        &image,
        Some(&json!({
            "image_request": {
                "size": "1024x1024",
                "quality": "low"
            }
        })),
    );
    let completed = completed_response_from_sse(body.as_str());

    assert_eq!(
        completed["usage"]["output_tokens"],
        json!(gpt_image2_output_tokens(1402, 1122, "low"))
    );
}

#[test]
fn chatgpt_web_image_subrequests_default_to_browser_wreq_transport() {
    let plan = sample_plan(
        CHATGPT_WEB_DEFAULT_BASE_URL,
        json!({"prompt": "draw a small test image"}),
        false,
    );

    let profile = chatgpt_web_image_transport_profile(&plan).expect("transport profile");

    assert_eq!(profile.backend, TRANSPORT_BACKEND_BROWSER_WREQ);
    assert_eq!(profile.profile_id, CHATGPT_WEB_BROWSER_PROFILE);
    assert_eq!(profile.http_mode, TRANSPORT_HTTP_MODE_AUTO);
    assert_eq!(profile.pool_scope, TRANSPORT_POOL_SCOPE_KEY);
    assert_eq!(
        profile
            .extra
            .as_ref()
            .and_then(|value| value.get("source"))
            .and_then(Value::as_str),
        Some("chatgpt_web_image_default")
    );
}

#[test]
fn chatgpt_web_image_request_context_preserves_edit_operation() {
    let plan = sample_plan(
        CHATGPT_WEB_DEFAULT_BASE_URL,
        json!({
            "operation": "edit",
            "model": "gpt-image-2",
            "web_model": "gpt-5-5-thinking",
            "prompt": "adjust this image",
            "size": "512x512",
            "ratio": "1:1",
            "images": ["data:image/png;base64,aW1hZ2U="],
            "count": 1,
            "output_format": "png"
        }),
        true,
    );

    let context = chatgpt_web_stream_observer_context(&plan, None);

    assert_eq!(context["image_request"]["operation"], json!("edit"));
    assert_eq!(context["image_request"]["model"], json!("gpt-image-2"));
    assert_eq!(context["image_request"]["size"], json!("512x512"));
    assert_eq!(context["provider_api_format"], json!("openai:image"));
}

#[test]
fn chatgpt_web_image_quota_refresh_plan_uses_conversation_init() {
    let plan = sample_plan(
        CHATGPT_WEB_DEFAULT_BASE_URL,
        json!({"prompt": "draw a small test image"}),
        false,
    );
    let spec = build_chatgpt_web_pool_quota_request(
        &plan.key_id,
        CHATGPT_WEB_DEFAULT_BASE_URL,
        (
            "authorization".to_string(),
            "Bearer test-access-token".to_string(),
        ),
    );

    let quota_plan = build_chatgpt_web_image_quota_refresh_plan(&plan, spec);

    assert_eq!(quota_plan.method, "POST");
    assert_eq!(
        quota_plan.url,
        "https://chatgpt.com/backend-api/conversation/init"
    );
    assert_eq!(
        quota_plan.provider_api_format,
        "chatgpt_web:conversation_init"
    );
    assert_eq!(
        quota_plan.headers.get("authorization").map(String::as_str),
        Some("Bearer test-access-token")
    );
    assert_eq!(
        quota_plan
            .transport_profile
            .as_ref()
            .map(|profile| profile.backend.as_str()),
        Some(TRANSPORT_BACKEND_BROWSER_WREQ)
    );
    assert_eq!(
        quota_plan
            .timeouts
            .as_ref()
            .and_then(|timeouts| timeouts.total_ms),
        Some(CHATGPT_WEB_QUOTA_REFRESH_TIMEOUT_MS)
    );
}

#[test]
fn chatgpt_web_image_quota_request_delta_decrements_remaining_count() {
    let mut metadata = Map::from_iter([
        ("image_quota_remaining".to_string(), json!(25.0)),
        ("image_quota_total".to_string(), json!(25.0)),
        ("image_quota_used".to_string(), json!(0.0)),
        ("image_quota_reset_at".to_string(), json!(2_000u64)),
    ]);

    assert!(apply_chatgpt_web_image_quota_request_delta_to_metadata(
        &mut metadata,
        None,
        1_000,
        None,
    ));

    assert_eq!(metadata["image_quota_remaining"], json!(24.0));
    assert_eq!(metadata["image_quota_total"], json!(25.0));
    assert_eq!(metadata["image_quota_used"], json!(1.0));
    assert_eq!(metadata["image_quota_local_request_count"], json!(1u64));
}

#[test]
fn chatgpt_web_image_quota_request_delta_can_use_status_snapshot() {
    let mut metadata = Map::new();
    let status_snapshot = json!({
        "quota": {
            "provider_type": "chatgpt_web",
            "windows": [{
                "code": "image_gen",
                "scope": "account",
                "remaining_value": 19.0,
                "limit_value": 25.0,
                "used_value": 6.0,
                "reset_at": 2_000u64
            }]
        }
    });

    assert!(apply_chatgpt_web_image_quota_request_delta_to_metadata(
        &mut metadata,
        Some(&status_snapshot),
        1_000,
        None,
    ));

    assert_eq!(metadata["image_quota_remaining"], json!(18.0));
    assert_eq!(metadata["image_quota_total"], json!(25.0));
    assert_eq!(metadata["image_quota_used"], json!(7.0));
    assert_eq!(metadata["image_quota_reset_at"], json!(2_000u64));
}

#[test]
fn chatgpt_web_image_quota_request_delta_records_unknown_quota_use() {
    let mut metadata = Map::new();

    assert!(apply_chatgpt_web_image_quota_request_delta_to_metadata(
        &mut metadata,
        None,
        1_000,
        None,
    ));

    assert_eq!(metadata.get("image_quota_remaining"), None);
    assert_eq!(metadata.get("image_quota_total"), None);
    assert_eq!(metadata["image_quota_used"], json!(1.0));
    assert_eq!(metadata["image_quota_local_request_count"], json!(1u64));
    assert_eq!(metadata["updated_at"], json!(1_000u64));
}

#[test]
fn chatgpt_web_image_quota_request_delta_derives_remaining_from_limit_only() {
    let mut metadata = Map::from_iter([("image_quota_total".to_string(), json!(10.0))]);

    assert!(apply_chatgpt_web_image_quota_request_delta_to_metadata(
        &mut metadata,
        None,
        1_000,
        None,
    ));

    assert_eq!(metadata["image_quota_remaining"], json!(9.0));
    assert_eq!(metadata["image_quota_total"], json!(10.0));
    assert_eq!(metadata["image_quota_used"], json!(1.0));
}

#[test]
fn chatgpt_web_image_quota_request_delta_ignores_legacy_free_25_limit() {
    let mut metadata = Map::from_iter([
        ("plan_type".to_string(), json!("free")),
        ("image_quota_remaining".to_string(), json!(19.0)),
        ("image_quota_total".to_string(), json!(25.0)),
        ("image_quota_used".to_string(), json!(6.0)),
    ]);

    assert!(apply_chatgpt_web_image_quota_request_delta_to_metadata(
        &mut metadata,
        None,
        1_000,
        None,
    ));

    assert_eq!(metadata["image_quota_remaining"], json!(18.0));
    assert_eq!(metadata["image_quota_total"], json!(19.0));
    assert_eq!(metadata["image_quota_used"], json!(1.0));
    assert_eq!(
        metadata["image_quota_limit_source"],
        json!("first_remaining")
    );
}

#[test]
fn chatgpt_web_image_quota_request_delta_ignores_legacy_free_25_without_remaining() {
    let mut metadata = Map::from_iter([
        ("plan_type".to_string(), json!("free")),
        ("image_quota_total".to_string(), json!(25.0)),
    ]);

    assert!(apply_chatgpt_web_image_quota_request_delta_to_metadata(
        &mut metadata,
        None,
        1_000,
        None,
    ));

    assert_eq!(metadata.get("image_quota_remaining"), None);
    assert_eq!(metadata.get("image_quota_total"), None);
    assert_eq!(metadata["image_quota_used"], json!(1.0));
    assert_eq!(
        metadata.get("image_quota_limit_source"),
        None,
        "legacy free default should not become a first observed limit without remaining"
    );
}

#[test]
fn chatgpt_web_image_quota_request_delta_dedupes_same_candidate_start() {
    let mut metadata = Map::from_iter([
        ("plan_type".to_string(), json!("free")),
        ("image_quota_remaining".to_string(), json!(25.0)),
        ("image_quota_total".to_string(), json!(25.0)),
        ("image_quota_used".to_string(), json!(0.0)),
    ]);

    assert!(apply_chatgpt_web_image_quota_request_delta_to_metadata(
        &mut metadata,
        None,
        1_000,
        Some("request-1:candidate-1"),
    ));
    assert!(!apply_chatgpt_web_image_quota_request_delta_to_metadata(
        &mut metadata,
        None,
        1_001,
        Some("request-1:candidate-1"),
    ));
    assert!(apply_chatgpt_web_image_quota_request_delta_to_metadata(
        &mut metadata,
        None,
        1_002,
        Some("request-1:candidate-2"),
    ));

    assert_eq!(metadata["image_quota_remaining"], json!(23.0));
    assert_eq!(metadata["image_quota_total"], json!(25.0));
    assert_eq!(metadata["image_quota_used"], json!(2.0));
    assert_eq!(metadata["image_quota_local_request_count"], json!(2u64));
    assert_eq!(
        metadata["image_quota_last_local_request_key"],
        json!("request-1:candidate-2")
    );
}

async fn start_mock_chatgpt_web() -> (String, tokio::task::JoinHandle<()>) {
    let app = Router::new().fallback(any(|request: Request| async move {
        let path = request.uri().path().to_string();
        let method = request.method().clone();
        match (method, path.as_str()) {
            (Method::GET, "/") => response(StatusCode::OK, "text/html", "ok"),
            (Method::POST, "/backend-api/sentinel/chat-requirements") => json_response(json!({
                "token": "requirements-token",
                "proofofwork": {"required": false},
                "arkose": {"required": false}
            })),
            (Method::POST, "/backend-api/f/conversation/prepare") => {
                json_response(json!({"conduit_token": "conduit-token"}))
            }
            (Method::POST, "/backend-api/f/conversation") => response(
                StatusCode::OK,
                "text/event-stream",
                concat!(
                    "data: {\"conversation_id\":\"conv-test-1\"}\n\n",
                    "data: {\"message\":{\"content\":{\"parts\":[\"working\"]}},\"asset\":\"file-generated-123456\"}\n\n",
                    "data: [DONE]\n\n"
                ),
            ),
            (Method::GET, "/backend-api/files/download/file-generated-123456") => {
                json_response(json!({"download_url": "/generated.png"}))
            }
            (Method::GET, "/generated.png") => response(
                StatusCode::OK,
                "image/png",
                png_header_bytes(2, 3),
            ),
            _ => response(StatusCode::NOT_FOUND, "text/plain", "not found"),
        }
    }));
    let listener = crate::test_support::bind_loopback_listener()
        .await
        .expect("listener should bind");
    let addr = listener.local_addr().expect("local addr should resolve");
    let handle = tokio::spawn(async move {
        axum::serve(listener, app)
            .await
            .expect("mock server should run");
    });
    (format!("http://{addr}"), handle)
}

async fn start_bootstrap_failing_chatgpt_web() -> (String, tokio::task::JoinHandle<()>) {
    let app = Router::new().fallback(any(|_request: Request| async move {
        response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "text/plain",
            "bootstrap failed",
        )
    }));
    let listener = crate::test_support::bind_loopback_listener()
        .await
        .expect("listener should bind");
    let addr = listener.local_addr().expect("local addr should resolve");
    let handle = tokio::spawn(async move {
        axum::serve(listener, app)
            .await
            .expect("mock server should run");
    });
    (format!("http://{addr}"), handle)
}

fn response(
    status: StatusCode,
    content_type: &'static str,
    body: impl Into<Body>,
) -> http::Response<Body> {
    http::Response::builder()
        .status(status)
        .header(http::header::CONTENT_TYPE, content_type)
        .body(body.into())
        .expect("response should build")
}

fn json_response(body: Value) -> http::Response<Body> {
    response(
        StatusCode::OK,
        "application/json",
        serde_json::to_vec(&body).expect("json should encode"),
    )
}

fn png_header_bytes(width: u32, height: u32) -> Vec<u8> {
    let mut bytes = Vec::from(&b"\x89PNG\r\n\x1a\n\0\0\0\rIHDR"[..]);
    bytes.extend_from_slice(&width.to_be_bytes());
    bytes.extend_from_slice(&height.to_be_bytes());
    bytes
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[test]
fn parse_web_image_sse_extracts_completed_output_result() {
    let summary = parse_web_image_sse(
        br#"data: {"type":"response.completed","response":{"output":[{"type":"image_generation_call","result":"ZmFrZS1pbWFnZQ==","output_format":"webp"}]}}

data: [DONE]

"#,
    );

    assert_eq!(
        summary.direct_urls,
        vec!["data:image/webp;base64,ZmFrZS1pbWFnZQ=="]
    );
}

#[test]
fn parse_web_image_sse_extracts_partial_image_result() {
    let summary = parse_web_image_sse(
        br#"data: {"type":"response.image_generation_call.partial_image","partial_image_b64":"cGFydGlhbA==","output_format":"jpeg"}

data: [DONE]

"#,
    );

    assert_eq!(
        summary.direct_urls,
        vec!["data:image/jpeg;base64,cGFydGlhbA=="]
    );
}

#[test]
fn parse_web_image_sse_preserves_inline_output_format() {
    let jpeg_payload = base64::engine::general_purpose::STANDARD.encode([0xff, 0xd8, 0xff, 0xd9]);
    let event = format!(
        "data: {{\"type\":\"response.output_item.done\",\"item\":{{\"type\":\"image_generation_call\",\"result\":\"{jpeg_payload}\",\"output_format\":\"jpeg\"}}}}\n\n"
    );

    let summary = parse_web_image_sse(event.as_bytes());

    assert_eq!(
        summary.direct_urls,
        vec![format!("data:image/jpeg;base64,{jpeg_payload}")]
    );
}

#[test]
fn parse_web_image_sse_preserves_response_failed_event() {
    let summary = parse_web_image_sse(
        br#"data: {"type":"response.failed","response":{"status":"failed","error":{"code":"rate_limit_exceeded","message":"limited"}}}

data: [DONE]

"#,
    );

    assert_eq!(
        summary
            .failure
            .as_ref()
            .and_then(|value| value.get("type"))
            .and_then(Value::as_str),
        Some("response.failed")
    );
}

#[test]
fn generated_asset_filter_does_not_drop_icon_or_logo_outputs() {
    for accepted in [
        "https://files.oaiusercontent.com/generated/icon-logo-output.png",
        "https://cdn.files.oaiusercontent.com/generated/image.png",
        "https://oaidalleapiprodscus.blob.core.windows.net/generated/image.png",
        "https://tenant.blob.core.windows.net/generated/image.png",
        "https://files.oaiusercontent.com./generated/image.png",
    ] {
        assert!(
            is_generated_web_asset_url(accepted),
            "generated image host should be accepted: {accepted}"
        );
    }
    assert!(!is_generated_web_asset_url(
        "https://openaiassets.blob.core.windows.net/$web/chatgpt/filled-plus-icon.svg"
    ));

    for rejected in [
        "https://notfiles.oaiusercontent.com/generated/image.png",
        "https://files.oaiusercontent.com.attacker.invalid/generated/image.png",
        "https://not-oaidalleapiprodscus.blob.core.windows.net.attacker.invalid/image.png",
        "https://blob.core.windows.net/generated/image.png",
        "https://openaiassets.blob.core.windows.net/generated/image.png",
        "https://sub.openaiassets.blob.core.windows.net/generated/image.png",
        "ftp://files.oaiusercontent.com/generated/image.png",
        "https://user@files.oaiusercontent.com/generated/image.png",
    ] {
        assert!(
            !is_generated_web_asset_url(rejected),
            "lookalike or static asset host should be rejected: {rejected}"
        );
    }
}

#[test]
fn web_opaque_ids_reject_path_and_query_injection() {
    for accepted in ["conv-test_123", "file-generated-123456", "sediment_123"] {
        assert!(web_opaque_id_is_safe(accepted));
    }
    assert!(is_web_file_id("file-generated-123456"));

    for rejected in [
        "../admin",
        "conv/other",
        "conv?inline=true",
        "conv#fragment",
        "conv%2fadmin",
        "conv&inline=true",
        "conv=value",
        "conv value",
        "\r\nX-Injected: true",
    ] {
        assert!(
            !web_opaque_id_is_safe(rejected),
            "unsafe opaque ID should be rejected: {rejected:?}"
        );
    }
    assert!(!web_opaque_id_is_safe(
        "a".repeat(CHATGPT_WEB_OPAQUE_ID_MAX_BYTES + 1).as_str()
    ));
    assert!(!is_web_file_id("file-generated-123456/../../admin"));
    assert!(validated_web_file_id("file-generated-123456?download=1").is_err());
}

#[test]
fn web_image_value_extraction_keeps_only_safe_opaque_ids() {
    let mut summary = WebImageSseSummary::default();
    extract_web_image_values(
        &json!({
            "conversation_id": "conv-test_123",
            "file": "file-generated-123456",
            "sediment": "sediment://sediment_123"
        }),
        &mut summary,
    );
    assert_eq!(summary.conversation_id.as_deref(), Some("conv-test_123"));
    assert_eq!(summary.file_ids, vec!["file-generated-123456"]);
    assert_eq!(summary.sediment_ids, vec!["sediment_123"]);

    let mut malicious = WebImageSseSummary::default();
    extract_web_image_values(
        &json!({
            "conversation_id": "conv-test?inline=true",
            "file": "file-generated-123456/../../admin",
            "sediment": "sediment://sediment_123?download=1"
        }),
        &mut malicious,
    );
    assert!(malicious.conversation_id.is_none());
    assert!(malicious.file_ids.is_empty());
    assert!(malicious.sediment_ids.is_empty());
}

#[test]
fn chatgpt_web_image_url_validation_requires_absolute_http_without_credentials() {
    assert!(parse_absolute_web_image_url("https://cdn.example/image.png").is_ok());

    for rejected in [
        "/relative.png",
        "file:///etc/passwd",
        "ftp://cdn.example/image.png",
        "https://user:password@cdn.example/image.png",
    ] {
        assert!(
            parse_absolute_web_image_url(rejected).is_err(),
            "URL should be rejected: {rejected}"
        );
    }
}

#[test]
fn chatgpt_web_upload_url_is_restricted_to_signed_storage_origins() {
    for accepted in [
        "https://files.oaiusercontent.com/upload/blob?sig=abc&se=123",
        "https://cdn.files.oaiusercontent.com/upload/blob?sig=abc",
        "https://oaidalleapiprodscus.blob.core.windows.net/container/blob?sig=abc",
        "https://tenant.blob.core.windows.net/container/blob?sig=abc",
        "https://tenant.blob.core.windows.net:443/container/blob?sig=abc",
    ] {
        assert!(
            validate_chatgpt_web_upload_url(accepted).is_ok(),
            "valid storage URL should be accepted: {accepted}"
        );
    }

    for rejected in [
        "http://files.oaiusercontent.com/upload/blob?sig=abc",
        "https://127.0.0.1/upload/blob?sig=abc",
        "https://user:pass@files.oaiusercontent.com/upload/blob?sig=abc",
        "https://files.oaiusercontent.com.attacker.invalid/upload/blob?sig=abc",
        "https://attacker.invalid/upload/blob?sig=abc",
        "https://blob.core.windows.net/upload/blob?sig=abc",
        "https://openaiassets.blob.core.windows.net/upload/blob?sig=abc",
        "https://tenant.blob.core.windows.net:8443/upload/blob?sig=abc",
        "https://tenant.blob.core.windows.net/upload/blob?sig=abc#fragment",
    ] {
        assert!(
            validate_chatgpt_web_upload_url(rejected).is_err(),
            "unsafe storage URL should be rejected: {rejected}"
        );
    }

    let oversized = format!(
        "https://files.oaiusercontent.com/upload/blob?sig={}",
        "a".repeat(CHATGPT_WEB_IMAGE_MAX_UPLOAD_URL_BYTES)
    );
    assert!(validate_chatgpt_web_upload_url(&oversized).is_err());
}

#[test]
fn chatgpt_web_image_request_fields_are_bounded() {
    let oversized_prompt = json!({
        "prompt": "x".repeat(CHATGPT_WEB_IMAGE_MAX_PROMPT_BYTES + 1)
    });
    assert!(ChatGptWebImageRequest::from_body(&oversized_prompt).is_err());

    let oversized_images = json!({
        "images": vec!["data:image/png;base64,AA=="; CHATGPT_WEB_IMAGE_MAX_INPUT_IMAGES + 1]
    });
    assert!(ChatGptWebImageRequest::from_body(&oversized_images).is_err());

    let too_many_partial_images = json!({"partial_images": 4});
    assert!(ChatGptWebImageRequest::from_body(&too_many_partial_images).is_err());
}

#[test]
fn chatgpt_web_image_summary_bounds_assets_across_merges() {
    let mut summary = WebImageSseSummary::default();
    for round in 0..32 {
        let mut poll = WebImageSseSummary::default();
        poll.add_values(
            WebImageSummaryCollection::FileId,
            (0..16).map(|index| format!("file-{round}-{index}")),
        );
        poll.add_values(
            WebImageSummaryCollection::SedimentId,
            (0..16).map(|index| format!("sediment-{round}-{index}")),
        );
        poll.add_values(
            WebImageSummaryCollection::DirectUrl,
            (0..16).map(|index| format!("https://files.oaiusercontent.com/{round}/{index}")),
        );
        merge_web_summary(&mut summary, &mut poll);
    }
    assert!(summary.retained_item_count() <= CHATGPT_WEB_IMAGE_SUMMARY_MAX_ITEMS);
    assert!(summary.retained_value_bytes() <= chatgpt_web_image_sse_envelope_limit_bytes());
}

#[test]
fn chatgpt_web_image_data_url_is_bounded_before_formatting() {
    let oversized = "A".repeat(
        maximum_base64_len_for_decoded_limit(chatgpt_web_image_raw_payload_limit_bytes())
            .saturating_add(1),
    );
    assert!(bounded_web_image_data_url("image/png", &oversized).is_none());
    assert!(bounded_web_image_data_url("image/png", "AAAA").is_some());
}

#[test]
fn chatgpt_web_image_same_origin_requires_scheme_host_and_effective_port() {
    let base = url::Url::parse("https://chatgpt.example").expect("base URL should parse");

    for same_origin in [
        "https://chatgpt.example/backend-api/files/download/file-1",
        "https://CHATGPT.example:443/backend-api/files/download/file-1",
    ] {
        let target = url::Url::parse(same_origin).expect("target URL should parse");
        assert!(web_download_url_is_same_origin(&base, &target));
    }

    for cross_origin in [
        "http://chatgpt.example/backend-api/files/download/file-1",
        "https://chatgpt.example:444/backend-api/files/download/file-1",
        "https://cdn.chatgpt.example/backend-api/files/download/file-1",
    ] {
        let target = url::Url::parse(cross_origin).expect("target URL should parse");
        assert!(!web_download_url_is_same_origin(&base, &target));
    }
}

#[test]
fn chatgpt_web_image_authentication_is_limited_to_same_origin_backend_api_paths() {
    let base = url::Url::parse("https://chatgpt.example").expect("base URL should parse");
    let authenticated =
        url::Url::parse("https://chatgpt.example/backend-api/files/download/file-1")
            .expect("authenticated URL should parse");
    assert!(is_authenticated_web_download_url(&base, &authenticated));

    for unauthenticated in [
        "https://chatgpt.example/generated.png",
        "https://chatgpt.example/backend-api-impersonator/image.png",
        "https://cdn.example/backend-api/files/download/file-1",
        "http://chatgpt.example/backend-api/files/download/file-1",
        "https://chatgpt.example:444/backend-api/files/download/file-1",
        "https://user@chatgpt.example/backend-api/files/download/file-1",
    ] {
        let target = url::Url::parse(unauthenticated).expect("target URL should parse");
        assert!(
            !is_authenticated_web_download_url(&base, &target),
            "provider credentials must not be sent to {unauthenticated}"
        );
    }
}

#[test]
fn chatgpt_web_data_url_parser_accepts_only_bounded_supported_image_types() {
    let payload = base64::engine::general_purpose::STANDARD.encode(png_header_bytes(2, 3));
    let png = parse_data_url_with_limit(format!("data:image/png;base64,{payload}").as_str(), 64)
        .expect("png data URL should parse");
    assert_eq!(png.mime, "image/png");
    assert_eq!(png.b64_json, payload);

    let jpeg_payload = base64::engine::general_purpose::STANDARD.encode([0xff, 0xd8, 0xff, 0xd9]);
    let jpeg = parse_data_url_with_limit(
        format!("DATA:IMAGE/JPEG;BASE64,{jpeg_payload}").as_str(),
        64,
    )
    .expect("jpeg data URL should parse");
    assert_eq!(jpeg.mime, "image/jpeg");

    for rejected in [
        "data:text/html;base64,PGh0bWw+",
        "data:image/svg+xml;base64,PHN2Zz4=",
        "data:image/gif;base64,R0lGODlh",
        "data:image/png;base64,",
        "data:image/png;base64,!!!!",
        "data:image/png;charset=utf-8;base64,aW1hZ2U=",
        "data:image/png;base64,aW1h\nZ2U=",
        "data:image/png;base64,PHN2Zz4=",
    ] {
        assert!(
            parse_data_url_with_limit(rejected, 64).is_none(),
            "unsafe data URL should be rejected: {rejected}"
        );
    }
}

#[test]
fn chatgpt_web_data_url_parser_enforces_decoded_limit_before_allocation() {
    let exact_bytes = png_header_bytes(2, 3);
    let exact_payload = base64::engine::general_purpose::STANDARD.encode(&exact_bytes);
    let exact = parse_data_url_with_limit(
        format!("data:image/png;base64,{exact_payload}").as_str(),
        exact_bytes.len(),
    )
    .expect("payload at the decoded limit should parse");
    assert_eq!(exact.b64_json, exact_payload);

    let exact_len = exact_bytes.len();
    let mut over_bytes = exact_bytes.clone();
    over_bytes.push(0);
    let over_payload = base64::engine::general_purpose::STANDARD.encode(over_bytes);
    assert!(
        parse_data_url_with_limit(
            format!("data:image/png;base64,{over_payload}").as_str(),
            exact_len,
        )
        .is_none(),
        "payload over the decoded limit must be rejected"
    );
}

#[test]
fn chatgpt_web_image_payload_requires_supported_magic_and_matching_mime() {
    let png = png_header_bytes(2, 3);
    assert_eq!(
        validate_web_image_payload(&png, Some("image/png; charset=binary"))
            .expect("valid png should pass"),
        "image/png"
    );
    assert_eq!(
        validate_web_image_payload(&png, Some("application/octet-stream"))
            .expect("octet-stream with a valid signature should pass"),
        "image/png"
    );
    assert!(validate_web_image_payload(&png, Some("image/jpeg")).is_err());
    assert!(validate_web_image_payload(&png, Some("image/svg+xml")).is_err());
    assert!(validate_web_image_payload(b"<svg><script>x</script></svg>", None).is_err());
    assert!(validate_web_image_payload(b"<html>not an image</html>", Some("image/png")).is_err());
    assert_eq!(
        validate_web_image_payload(&[0xff, 0xd8, 0xff, 0xd9], Some("image/jpg"))
            .expect("jpeg signature should pass"),
        "image/jpeg"
    );
    assert_eq!(
        validate_web_image_payload(b"RIFF\x04\0\0\0WEBP", None)
            .expect("webp signature should pass"),
        "image/webp"
    );
    assert!(validate_web_image_payload(&[0xff, 0xd8], Some("image/jpeg")).is_err());
}

#[test]
fn chatgpt_web_image_sse_envelope_budget_covers_base64_expansion() {
    let raw_limit = crate::headers::max_internal_buffered_body_bytes();
    let expected_minimum = maximum_base64_len_for_decoded_limit(raw_limit)
        .saturating_add(CHATGPT_WEB_IMAGE_SSE_WRAPPER_OVERHEAD_BYTES)
        .min(CHATGPT_WEB_IMAGE_SSE_HARD_MAX_BYTES);
    assert!(chatgpt_web_image_sse_envelope_limit_bytes() >= expected_minimum);
    assert!(chatgpt_web_image_sse_envelope_limit_bytes() >= raw_limit.min(64 * 1024 * 1024));
}

#[test]
fn chatgpt_web_execution_result_body_decode_is_bounded() {
    let result = ExecutionResult {
        request_id: "req-chatgpt-web-image-test".to_string(),
        candidate_id: None,
        status_code: 200,
        headers: BTreeMap::new(),
        response_observation: None,
        body: Some(ResponseBody {
            json_body: None,
            body_bytes_b64: Some("!!!!".to_string()),
        }),
        telemetry: None,
        error: None,
    };
    assert!(execution_result_bytes(&result).is_err());
    assert!(execution_result_body_bytes_lossy(&result).is_empty());
}

#[tokio::test]
async fn chatgpt_web_public_image_resolution_rejects_private_ip_literals() {
    for private_url in [
        "http://127.0.0.1/image.png",
        "http://169.254.169.254/latest/meta-data",
    ] {
        let url = url::Url::parse(private_url).expect("private URL should parse");
        let error = resolve_public_web_image_addrs(&url, Duration::from_secs(5), false)
            .await
            .expect_err("private address must be rejected");
        assert!(
            error.to_string().contains("private or reserved"),
            "unexpected error for {private_url}: {error}"
        );
    }

    let public_url = url::Url::parse("https://8.8.8.8/image.png").expect("public URL should parse");
    let (host, addresses) =
        resolve_public_web_image_addrs(&public_url, Duration::from_secs(5), false)
            .await
            .expect("public IP literal should be accepted");
    assert_eq!(host, "8.8.8.8");
    assert_eq!(addresses, vec!["8.8.8.8:443".parse().unwrap()]);
}

#[test]
fn chatgpt_web_image_fake_ip_exception_is_limited_to_storage_origins() {
    let storage = url::Url::parse("https://files.oaiusercontent.com/generated/image.png?sig=test")
        .expect("storage URL should parse");
    let fake = vec!["198.18.75.234:443".parse().unwrap()];
    assert!(validate_public_web_image_addresses(&storage, &fake, true).is_ok());
    assert!(validate_public_web_image_addresses(&storage, &fake, false).is_err());

    let arbitrary = url::Url::parse("https://cdn.example/generated/image.png")
        .expect("arbitrary URL should parse");
    assert!(validate_public_web_image_addresses(&arbitrary, &fake, true).is_err());

    let mixed = vec![
        "198.18.75.234:443".parse().unwrap(),
        "10.0.0.1:443".parse().unwrap(),
    ];
    assert!(validate_public_web_image_addresses(&storage, &mixed, true).is_err());
}

#[test]
fn sha3_512_matches_standard_empty_input_vector() {
    assert_eq!(
        hex(&sha3_512(b"")),
        concat!(
            "a69f73cca23a9ac5c8b567dc185a756e97c982164fe25859e0d1dcc1475c80a",
            "615b2123af1f5f94c11e3e9402c3ac558f500199d95b6d3e301758586281dcd26"
        )
    );
}

#[test]
fn pow_generate_solves_easy_target() {
    let (answer, solved) = pow_generate("seed", "ff", pow_config(CHATGPT_WEB_USER_AGENT));
    assert!(solved);
    assert!(!answer.is_empty());
}

#[test]
fn pow_generate_rejects_difficulty_larger_than_digest() {
    let seed = "seed";
    let (answer, solved) = pow_generate(
        seed,
        "f".repeat(130).as_str(),
        pow_config(CHATGPT_WEB_USER_AGENT),
    );
    assert!(!solved);
    assert_eq!(answer, encode_pow_seed(seed));
}

#[tokio::test]
async fn chatgpt_web_image_executor_downloads_file_id_result_as_openai_image_sse() {
    let (base_url, handle) = start_mock_chatgpt_web().await;
    let state = crate::AppState::new().expect("state should build");
    let plan = sample_plan(
        base_url.as_str(),
        json!({
            "operation": "generate",
            "model": "gpt-image-2",
            "web_model": "gpt-5-5-thinking",
            "prompt": "draw a precise test image",
            "size": "512x512",
            "ratio": "1:1",
            "size_best_effort": true,
            "images": [],
            "count": 1,
            "output_format": "png"
        }),
        false,
    );

    let result = maybe_execute_chatgpt_web_image_sync(
        &state,
        &plan,
        Some(&json!({"chatgpt_web_image": true})),
    )
    .await
    .expect("executor should run")
    .expect("plan should be intercepted");

    assert_eq!(result.status_code, 200);
    assert_eq!(
        result.headers.get("content-type").map(String::as_str),
        Some("text/event-stream")
    );
    let body = String::from_utf8(execution_result_body_bytes_lossy(&result))
        .expect("sse body should be utf8");
    assert!(body.contains("response.output_item.done"));
    assert!(body.contains("\"type\":\"image_generation_call\""));
    assert!(body.contains("\"width\":2"));
    assert!(body.contains("\"height\":3"));
    let expected_output_text =
        base64::engine::general_purpose::STANDARD.encode(png_header_bytes(2, 3));
    assert!(body.contains(&expected_output_text));
    let completed = completed_response_from_sse(body.as_str());
    assert_eq!(completed["usage"]["output_tokens"], json!(1756));
    assert_eq!(
        completed["tool_usage"]["image_gen"]["output_tokens"],
        json!(1756)
    );

    handle.abort();
}

#[tokio::test]
async fn chatgpt_web_image_executor_decrements_quota_after_conversation_start_once() {
    let (base_url, handle) = start_mock_chatgpt_web().await;
    let (state, repository) = state_with_chatgpt_web_key(
        base_url.as_str(),
        json!({
            "chatgpt_web": {
                "plan_type": "free",
                "image_quota_remaining": 25.0,
                "image_quota_total": 25.0,
                "image_quota_used": 0.0
            }
        }),
    );
    let plan = sample_plan(
        base_url.as_str(),
        json!({
            "operation": "generate",
            "model": "gpt-image-2",
            "web_model": "gpt-5-5-thinking",
            "prompt": "draw a precise test image",
            "size": "512x512",
            "ratio": "1:1",
            "images": [],
            "count": 1,
            "output_format": "png"
        }),
        false,
    );

    let result = maybe_execute_chatgpt_web_image_sync(
        &state,
        &plan,
        Some(&json!({"chatgpt_web_image": true})),
    )
    .await
    .expect("executor should run")
    .expect("plan should be intercepted");

    assert_eq!(result.status_code, 200);
    let metadata = reloaded_chatgpt_web_metadata(repository.as_ref()).await;
    assert_eq!(metadata["image_quota_remaining"], json!(24.0));
    assert_eq!(metadata["image_quota_used"], json!(1.0));
    assert_eq!(metadata["image_quota_local_request_count"], json!(1u64));
    assert_eq!(
        metadata["image_quota_last_local_request_key"],
        json!("req-chatgpt-web-image-test:cand-chatgpt-web-image-test")
    );

    handle.abort();
}

#[tokio::test]
async fn chatgpt_web_image_executor_does_not_decrement_quota_before_conversation_start() {
    let (base_url, handle) = start_bootstrap_failing_chatgpt_web().await;
    let (state, repository) = state_with_chatgpt_web_key(
        base_url.as_str(),
        json!({
            "chatgpt_web": {
                "plan_type": "free",
                "image_quota_remaining": 25.0,
                "image_quota_total": 25.0,
                "image_quota_used": 0.0
            }
        }),
    );
    let plan = sample_plan(
        base_url.as_str(),
        json!({
            "operation": "generate",
            "model": "gpt-image-2",
            "web_model": "gpt-5-5-thinking",
            "prompt": "draw a precise test image",
            "size": "512x512",
            "ratio": "1:1",
            "images": [],
            "count": 1,
            "output_format": "png"
        }),
        false,
    );

    let result = maybe_execute_chatgpt_web_image_sync(
        &state,
        &plan,
        Some(&json!({"chatgpt_web_image": true})),
    )
    .await
    .expect("executor should preserve the upstream HTTP response")
    .expect("plan should be intercepted");

    assert_eq!(result.status_code, 500);
    assert_eq!(
        execution_result_json(&result).expect("error response should be json")["error"]["code"],
        json!("chatgpt_web_image_execution_unavailable")
    );
    let metadata = reloaded_chatgpt_web_metadata(repository.as_ref()).await;
    assert_eq!(metadata["image_quota_remaining"], json!(25.0));
    assert_eq!(metadata["image_quota_used"], json!(0.0));
    assert_eq!(metadata.get("image_quota_local_request_count"), None);
    assert_eq!(metadata.get("image_quota_last_local_request_key"), None);

    handle.abort();
}

#[tokio::test]
async fn chatgpt_web_image_sync_propagates_network_failure_without_synthetic_503() {
    let listener = crate::test_support::bind_loopback_listener()
        .await
        .expect("listener should bind");
    let base_url = format!(
        "http://{}",
        listener.local_addr().expect("local addr should resolve")
    );
    drop(listener);
    let state = crate::AppState::new().expect("state should build");
    let plan = sample_plan(
        base_url.as_str(),
        json!({"prompt": "draw a small test image"}),
        false,
    );

    let error = maybe_execute_chatgpt_web_image_sync(
        &state,
        &plan,
        Some(&json!({"chatgpt_web_image": true})),
    )
    .await
    .expect_err("connection failure should propagate to the candidate loop");

    assert!(matches!(
        error,
        ExecutionRuntimeTransportError::UpstreamRequest(_)
    ));
}

#[tokio::test]
async fn chatgpt_web_image_stream_propagates_network_failure_without_synthetic_503() {
    let listener = crate::test_support::bind_loopback_listener()
        .await
        .expect("listener should bind");
    let base_url = format!(
        "http://{}",
        listener.local_addr().expect("local addr should resolve")
    );
    drop(listener);
    let state = crate::AppState::new().expect("state should build");
    let plan = sample_plan(
        base_url.as_str(),
        json!({"prompt": "draw a small test image"}),
        true,
    );

    let error = match maybe_execute_chatgpt_web_image_stream(
        &state,
        &plan,
        Some(&json!({"chatgpt_web_image": true})),
    )
    .await
    {
        Err(error) => error,
        Ok(_) => panic!("connection failure should propagate to the candidate loop"),
    };

    assert!(matches!(
        error,
        ExecutionRuntimeTransportError::UpstreamRequest(_)
    ));
}

#[tokio::test]
async fn chatgpt_web_image_stream_path_wraps_success_sse_as_ndjson_frames() {
    let (base_url, handle) = start_mock_chatgpt_web().await;
    let state = crate::AppState::new().expect("state should build");
    let plan = sample_plan(
        base_url.as_str(),
        json!({
            "operation": "generate",
            "model": "gpt-image-2",
            "web_model": "gpt-5-5-thinking",
            "prompt": "draw a streamed test image",
            "size": "1024x1024",
            "ratio": "1:1",
            "images": [],
            "count": 1,
            "output_format": "png"
        }),
        true,
    );

    let stream = maybe_execute_chatgpt_web_image_stream(
        &state,
        &plan,
        Some(&json!({"chatgpt_web_image": true})),
    )
    .await
    .expect("executor should run")
    .expect("plan should be intercepted");
    let chunks = stream
        .frame_stream
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .map(|chunk| chunk.expect("frame should encode"))
        .collect::<Vec<_>>();
    let text = String::from_utf8(
        chunks
            .iter()
            .flat_map(|chunk| chunk.iter().copied())
            .collect::<Vec<_>>(),
    )
    .expect("ndjson should be utf8");
    let decoded_data = text
        .lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .filter_map(|frame| {
            frame
                .get("payload")
                .and_then(|payload| payload.get("chunk_b64"))
                .and_then(Value::as_str)
                .and_then(|chunk| base64::engine::general_purpose::STANDARD.decode(chunk).ok())
        })
        .flat_map(|bytes| String::from_utf8(bytes).ok())
        .collect::<String>();

    assert!(text.contains("\"status_code\":200"));
    assert!(decoded_data.contains("response.output_item.done"));
    assert!(decoded_data.contains("\"width\":2"));
    assert!(decoded_data.contains("\"height\":3"));
    assert!(text.contains("\"type\":\"eof\""));
    let eof_frame = text
        .lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .find(|frame| frame.get("type").and_then(Value::as_str) == Some("eof"))
        .expect("eof frame should exist");
    assert_eq!(
        eof_frame
            .get("payload")
            .and_then(|payload| payload.get("summary"))
            .and_then(|summary| summary.get("standardized_usage"))
            .and_then(|usage| usage.get("output_tokens"))
            .and_then(Value::as_i64),
        Some(1756)
    );
    assert_eq!(
        eof_frame
            .get("payload")
            .and_then(|payload| payload.get("summary"))
            .and_then(|summary| summary.get("standardized_usage"))
            .and_then(|usage| usage.get("dimensions"))
            .and_then(|dimensions| dimensions.get("image_count"))
            .and_then(Value::as_u64),
        Some(1)
    );
    assert_eq!(
        eof_frame
            .get("payload")
            .and_then(|payload| payload.get("summary"))
            .and_then(|summary| summary.get("standardized_usage"))
            .and_then(|usage| usage.get("dimensions"))
            .and_then(|dimensions| dimensions.get("image_size"))
            .and_then(Value::as_str),
        Some("1024x1024")
    );

    handle.abort();
}

#[tokio::test]
async fn chatgpt_web_image_executor_returns_embedded_resolution_error_as_400() {
    let state = crate::AppState::new().expect("state should build");
    let plan = sample_plan(
        CHATGPT_WEB_DEFAULT_BASE_URL,
        json!({
            "error": {
                "message": "ChatGPT-Web 不支持该分辨率",
                "type": "invalid_request_error",
                "code": "chatgpt_web_image_unsupported"
            }
        }),
        false,
    );

    let result = maybe_execute_chatgpt_web_image_sync(
        &state,
        &plan,
        Some(&json!({"chatgpt_web_image": true})),
    )
    .await
    .expect("executor should run")
    .expect("plan should be intercepted");

    assert_eq!(result.status_code, 400);
    let body = execution_result_json(&result).expect("error should be json");
    assert_eq!(body["error"]["type"], "invalid_request_error");
    assert_eq!(body["error"]["code"], "chatgpt_web_image_unsupported");
}

#[tokio::test]
async fn chatgpt_web_image_executor_accepts_marked_responses_client_plan() {
    let state = crate::AppState::new().expect("state should build");
    let mut plan = sample_plan(
        CHATGPT_WEB_DEFAULT_BASE_URL,
        json!({
            "error": {
                "message": "ChatGPT-Web 不支持该分辨率",
                "type": "invalid_request_error",
                "code": "chatgpt_web_image_unsupported"
            }
        }),
        false,
    );
    plan.client_api_format = "openai:responses".to_string();

    let result = maybe_execute_chatgpt_web_image_sync(&state, &plan, None)
        .await
        .expect("executor should run")
        .expect("marked image provider plan should be intercepted");

    assert_eq!(result.status_code, 400);
    let body = execution_result_json(&result).expect("error should be json");
    assert_eq!(body["error"]["code"], "chatgpt_web_image_unsupported");
}

#[tokio::test]
async fn chatgpt_web_image_stream_path_wraps_executor_result_as_ndjson_frames() {
    let state = crate::AppState::new().expect("state should build");
    let plan = sample_plan(
        CHATGPT_WEB_DEFAULT_BASE_URL,
        json!({
            "error": {
                "message": "ChatGPT-Web 不支持该分辨率",
                "type": "invalid_request_error",
                "code": "chatgpt_web_image_unsupported"
            }
        }),
        true,
    );

    let stream = maybe_execute_chatgpt_web_image_stream(
        &state,
        &plan,
        Some(&json!({"chatgpt_web_image": true})),
    )
    .await
    .expect("executor should run")
    .expect("plan should be intercepted");
    let chunks = stream
        .frame_stream
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .map(|chunk| chunk.expect("frame should encode"))
        .collect::<Vec<_>>();
    let text = String::from_utf8(
        chunks
            .iter()
            .flat_map(|chunk| chunk.iter().copied())
            .collect::<Vec<_>>(),
    )
    .expect("ndjson should be utf8");

    assert!(text.contains("\"status_code\":400"));
    let decoded_data = text
        .lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .filter_map(|frame| {
            frame
                .get("payload")
                .and_then(|payload| payload.get("chunk_b64"))
                .and_then(Value::as_str)
                .and_then(|chunk| base64::engine::general_purpose::STANDARD.decode(chunk).ok())
        })
        .flat_map(|bytes| String::from_utf8(bytes).ok())
        .collect::<String>();
    assert!(decoded_data.contains("chatgpt_web_image_unsupported"));
    assert!(text.contains("\"type\":\"eof\""));
}
