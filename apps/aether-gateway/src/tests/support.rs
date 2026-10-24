use aether_crypto::{encrypt_python_fernet_plaintext, DEVELOPMENT_ENCRYPTION_KEY};

/// Credential-only gateway state for fixtures that just seal or open a
/// provider secret. Avoids repeating the five-line AppState::new() chain in
/// every module that needs a key.
pub(crate) fn credential_fixture_state() -> crate::AppState {
    crate::AppState::new()
        .expect("credential fixture state should build")
        .with_data_state_for_tests(
            crate::data::GatewayDataState::disabled()
                .with_encryption_key_for_tests(DEVELOPMENT_ENCRYPTION_KEY),
        )
}
use aether_data::repository::auth::StoredAuthApiKeySnapshot;
use aether_data_contracts::repository::candidate_selection::StoredMinimalCandidateSelectionRow;
use aether_data_contracts::repository::candidates::{
    RequestCandidateStatus, StoredRequestCandidate,
};
use aether_data_contracts::repository::provider_catalog::{
    StoredProviderCatalogEndpoint, StoredProviderCatalogKey, StoredProviderCatalogProvider,
};
use serde_json::json;

pub(crate) fn test_auth_secret() -> String {
    std::env::var("JWT_SECRET_KEY")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "aether-rust-test-jwt-secret-32-bytes-minimum".to_string())
}

fn test_base64url_encode(bytes: &[u8]) -> String {
    use base64::Engine as _;

    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
}

pub(crate) fn build_test_auth_token(
    token_type: &str,
    mut payload: serde_json::Map<String, serde_json::Value>,
    expires_at: chrono::DateTime<chrono::Utc>,
) -> String {
    use hmac::Mac;

    let header = json!({ "alg": "HS256", "typ": "JWT" });
    payload.insert("exp".to_string(), json!(expires_at.timestamp()));
    payload.insert("type".to_string(), json!(token_type));
    let header_segment = test_base64url_encode(
        serde_json::to_vec(&header)
            .expect("jwt header should serialize")
            .as_slice(),
    );
    let payload_segment = test_base64url_encode(
        serde_json::to_vec(&payload)
            .expect("jwt payload should serialize")
            .as_slice(),
    );
    let signing_input = format!("{header_segment}.{payload_segment}");
    let mut mac = hmac::Hmac::<sha2::Sha256>::new_from_slice(test_auth_secret().as_bytes())
        .expect("jwt secret should build");
    mac.update(signing_input.as_bytes());
    let signature = mac.finalize().into_bytes();
    format!(
        "{header_segment}.{payload_segment}.{}",
        test_base64url_encode(signature.as_slice())
    )
}

pub(crate) fn sample_auth_snapshot(api_key_id: &str, user_id: &str) -> StoredAuthApiKeySnapshot {
    crate::tests::support::sample_auth_snapshot_with(
        api_key_id,
        user_id,
        "alice",
        "alice@example.com",
        serde_json::json!(["openai"]),
        serde_json::json!(["openai:chat"]),
        serde_json::json!(["gpt-5"]),
    )
}

pub(crate) fn sample_auth_snapshot_with(
    api_key_id: &str,
    user_id: &str,
    username: &str,
    email: &str,
    allowed_providers: serde_json::Value,
    allowed_api_formats: serde_json::Value,
    allowed_models: serde_json::Value,
) -> StoredAuthApiKeySnapshot {
    StoredAuthApiKeySnapshot::new(
        user_id.to_string(),
        username.to_string(),
        Some(email.to_string()),
        "user".to_string(),
        "local".to_string(),
        true,
        false,
        Some(allowed_providers.clone()),
        Some(allowed_api_formats.clone()),
        Some(allowed_models.clone()),
        api_key_id.to_string(),
        Some("default".to_string()),
        true,
        false,
        false,
        Some(60),
        Some(5),
        Some(4_102_444_800),
        Some(allowed_providers),
        Some(allowed_api_formats),
        Some(allowed_models),
    )
    .expect("auth snapshot should build")
}

pub(crate) fn sample_auth_snapshot_openai_gemini(
    api_key_id: &str,
    user_id: &str,
) -> StoredAuthApiKeySnapshot {
    sample_auth_snapshot_with(
        api_key_id,
        user_id,
        "alice",
        "alice@example.com",
        json!(["openai", "gemini"]),
        json!(["openai:chat"]),
        json!(["gpt-5"]),
    )
}

pub(crate) fn sample_auth_snapshot_kiro(
    api_key_id: &str,
    user_id: &str,
) -> StoredAuthApiKeySnapshot {
    sample_auth_snapshot_with(
        api_key_id,
        user_id,
        "alice",
        "alice@example.com",
        json!(["claude", "kiro"]),
        json!(["claude:messages"]),
        json!(["claude-sonnet-4"]),
    )
}

pub(crate) fn sample_auth_snapshot_claude(
    api_key_id: &str,
    user_id: &str,
) -> StoredAuthApiKeySnapshot {
    sample_auth_snapshot_with(
        api_key_id,
        user_id,
        "alice",
        "alice@example.com",
        json!(["claude"]),
        json!(["claude:messages"]),
        json!(["claude-sonnet-4-5"]),
    )
}

pub(crate) fn sample_auth_snapshot_video(
    api_key_id: &str,
    user_id: &str,
) -> StoredAuthApiKeySnapshot {
    sample_auth_snapshot_with(
        api_key_id,
        user_id,
        "video-user",
        "video@example.com",
        json!(["openai"]),
        json!(["openai:video"]),
        json!(["sora-2"]),
    )
}

pub(crate) fn sample_currently_usable_auth_snapshot(
    api_key_id: &str,
    user_id: &str,
) -> StoredAuthApiKeySnapshot {
    sample_auth_snapshot(api_key_id, user_id)
}

pub(crate) fn sample_provider(
    id: &str,
    name: &str,
    priority: i32,
) -> StoredProviderCatalogProvider {
    StoredProviderCatalogProvider::new(
        id.to_string(),
        name.to_string(),
        Some("https://example.com".to_string()),
        "custom".to_string(),
    )
    .expect("provider should build")
    .with_routing_fields(priority)
}

pub(crate) fn sample_endpoint(
    id: &str,
    provider_id: &str,
    api_format: &str,
    base_url: &str,
) -> StoredProviderCatalogEndpoint {
    sample_endpoint_with_health_score(id, provider_id, api_format, base_url, None)
}

pub(crate) fn sample_endpoint_with_health_score(
    id: &str,
    provider_id: &str,
    api_format: &str,
    base_url: &str,
    health_score: Option<f64>,
) -> StoredProviderCatalogEndpoint {
    let endpoint = StoredProviderCatalogEndpoint::new(
        id.to_string(),
        provider_id.to_string(),
        api_format.to_string(),
        None,
        None,
        true,
    )
    .expect("endpoint should build");
    let endpoint = match health_score {
        Some(health_score) => endpoint.with_health_score(health_score),
        None => endpoint,
    };
    endpoint
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
        .expect("endpoint transport should build")
}

pub(crate) fn sample_key(
    id: &str,
    provider_id: &str,
    api_format: &str,
    secret: &str,
) -> StoredProviderCatalogKey {
    let encrypted_api_key = encrypt_python_fernet_plaintext(DEVELOPMENT_ENCRYPTION_KEY, secret)
        .expect("api key ciphertext should build");
    StoredProviderCatalogKey::new(
        id.to_string(),
        provider_id.to_string(),
        "default".to_string(),
        "api_key".to_string(),
        None,
        true,
    )
    .expect("key should build")
    .with_transport_fields(
        Some(json!([api_format])),
        encrypted_api_key,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
    )
    .expect("key transport should build")
}

pub(crate) fn sample_request_candidate(
    id: &str,
    request_id: &str,
    endpoint_id: &str,
    status: RequestCandidateStatus,
    created_at_unix_secs: i64,
    finished_at_unix_secs: Option<i64>,
) -> StoredRequestCandidate {
    let created_at_unix_ms = created_at_unix_secs * 1_000;
    let finished_at_unix_ms = finished_at_unix_secs.map(|v| v * 1_000);
    StoredRequestCandidate::new(
        id.to_string(),
        request_id.to_string(),
        Some("user-1".to_string()),
        Some("api-key-1".to_string()),
        Some("alice".to_string()),
        Some("default".to_string()),
        0,
        0,
        Some("provider-1".to_string()),
        Some(endpoint_id.to_string()),
        Some("key-1".to_string()),
        status,
        None,
        false,
        Some(200),
        matches!(status, RequestCandidateStatus::Failed).then_some("rate_limit".to_string()),
        None,
        Some(120),
        Some(1),
        None,
        None,
        created_at_unix_ms,
        Some(created_at_unix_ms),
        finished_at_unix_ms,
    )
    .expect("request candidate should build")
}

/// Read a nested JSON string field, returning an empty string when absent.
///
/// Test fixtures used to spell out the full `payload.get(..).and_then(..)
/// .unwrap_or_default().to_string()` chain for every observed field.
pub(crate) fn payload_string(payload: &serde_json::Value, path: &[&str]) -> String {
    let mut cursor = payload;
    for key in path {
        let Some(next) = cursor.get(*key) else {
            return String::new();
        };
        cursor = next;
    }
    cursor.as_str().unwrap_or_default().to_string()
}

/// Read a header as UTF-8, returning an empty string when absent or invalid.
pub(crate) fn header_string(
    headers: &http::HeaderMap,
    name: impl http::header::AsHeaderName,
) -> String {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_string()
}

/// Minimal candidate-selection row shaped like the rows the scheduler consumes.
///
/// Every ai_execute suite previously hand-wrote a 30-field literal that only
/// differed in the provider/key/model identity and API format. The suffix keeps
/// ids readable per suite (`provider-<suffix>`, `endpoint-<suffix>`, ...).
pub(crate) struct CandidateRowSpec<'a> {
    pub id_suffix: &'a str,
    pub provider_name: &'a str,
    pub provider_type: &'a str,
    pub api_format: &'a str,
    pub api_family: &'a str,
    pub endpoint_kind: Option<&'a str>,
    pub key_name: &'a str,
    pub key_auth_type: &'a str,
    pub global_model_name: &'a str,
    pub upstream_model: &'a str,
}

pub(crate) fn minimal_candidate_row(
    spec: &CandidateRowSpec<'_>,
) -> aether_data_contracts::repository::candidate_selection::StoredMinimalCandidateSelectionRow {
    use aether_data_contracts::repository::candidate_selection::StoredProviderModelMapping;

    let id = |prefix: &str| format!("{prefix}-{}", spec.id_suffix);
    let mut global_priority = serde_json::Map::new();
    global_priority.insert(spec.api_format.to_string(), serde_json::json!(1));

    aether_data_contracts::repository::candidate_selection::StoredMinimalCandidateSelectionRow {
        provider_id: id("provider"),
        provider_name: spec.provider_name.to_string(),
        provider_type: spec.provider_type.to_string(),
        provider_priority: 10,
        provider_is_active: true,
        endpoint_id: id("endpoint"),
        endpoint_api_format: spec.api_format.to_string(),
        endpoint_api_family: Some(spec.api_family.to_string()),
        endpoint_kind: spec.endpoint_kind.map(str::to_string),
        endpoint_is_active: true,
        key_id: id("key"),
        key_name: spec.key_name.to_string(),
        key_auth_type: spec.key_auth_type.to_string(),
        key_is_active: true,
        key_api_formats: Some(vec![spec.api_format.to_string()]),
        key_allowed_models: None,
        key_capabilities: None,
        key_internal_priority: 5,
        key_global_priority_by_format: Some(serde_json::Value::Object(global_priority)),
        model_id: id("model"),
        global_model_id: id("global-model"),
        global_model_name: spec.global_model_name.to_string(),
        global_model_mappings: None,
        global_model_supports_streaming: Some(true),
        model_provider_model_name: spec.upstream_model.to_string(),
        model_provider_model_mappings: Some(vec![StoredProviderModelMapping {
            name: spec.upstream_model.to_string(),
            priority: 1,
            api_formats: Some(vec![spec.api_format.to_string()]),
            endpoint_ids: None,
            operations: None,
        }]),
        model_supports_streaming: Some(true),
        model_is_active: true,
        model_is_available: true,
    }
}

/// Provider catalog key shaped like the seeded keys the scheduler resolves.
pub(crate) struct CatalogKeySpec<'a> {
    pub id_suffix: &'a str,
    pub key_name: &'a str,
    pub auth_type: &'a str,
    pub api_format: &'a str,
    pub secret: &'a str,
}

pub(crate) fn provider_catalog_key(spec: &CatalogKeySpec<'_>) -> StoredProviderCatalogKey {
    let mut priority = serde_json::Map::new();
    priority.insert(spec.api_format.to_string(), serde_json::json!(1));
    let encrypted_api_key = encrypt_python_fernet_plaintext(DEVELOPMENT_ENCRYPTION_KEY, spec.secret)
        .expect("api key should encrypt");

    StoredProviderCatalogKey::new(
        format!("key-{}", spec.id_suffix),
        format!("provider-{}", spec.id_suffix),
        spec.key_name.to_string(),
        spec.auth_type.to_string(),
        None,
        true,
    )
    .expect("key should build")
    .with_transport_fields(
        Some(serde_json::json!([spec.api_format])),
        encrypted_api_key,
        None,
        None,
        Some(serde_json::Value::Object(priority)),
        None,
        None,
        None,
        None,
    )
    .expect("key transport should build")
}

// --- AWS EventStream framing shared by the Kiro protocol tests ---
//
// The same ~80 lines of binary frame encoding used to be copied into five
// gateway test modules; keeping one copy means a framing bug is fixed once.

pub(crate) fn crc32(data: &[u8]) -> u32 {
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

pub(crate) fn encode_string_header(name: &str, value: &str) -> Vec<u8> {
    let mut out = Vec::new();
    out.push(name.len() as u8);
    out.extend_from_slice(name.as_bytes());
    out.push(7);
    out.extend_from_slice(&(value.len() as u16).to_be_bytes());
    out.extend_from_slice(value.as_bytes());
    out
}

pub(crate) fn encode_frame(headers: Vec<u8>, payload: Vec<u8>) -> Vec<u8> {
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

pub(crate) fn encode_event_frame(
    message_type: &str,
    event_type: Option<&str>,
    payload: serde_json::Value,
) -> Vec<u8> {
    let mut headers = encode_string_header(":message-type", message_type);
    if let Some(event_type) = event_type {
        headers.extend_from_slice(&encode_string_header(":event-type", event_type));
    }
    let payload = serde_json::to_vec(&payload).expect("payload should encode");
    encode_frame(headers, payload)
}

pub(crate) fn encode_kiro_event_frame(event_type: &str, payload: serde_json::Value) -> Vec<u8> {
    encode_event_frame("event", Some(event_type), payload)
}

pub(crate) fn encode_kiro_exception_frame(exception_type: &str) -> Vec<u8> {
    let mut headers = encode_string_header(":message-type", "exception");
    headers.extend_from_slice(&encode_string_header(":exception-type", exception_type));
    encode_frame(headers, Vec::new())
}

pub(crate) fn encode_kiro_error_frame(error_code: &str) -> Vec<u8> {
    let mut headers = encode_string_header(":message-type", "error");
    headers.extend_from_slice(&encode_string_header(":error-code", error_code));
    encode_frame(headers, Vec::new())
}

/// Shared fixture spec for the `sample_local_openai_*` sets.
pub(crate) struct LocalOpenAiSpec<'a> {
    pub id_suffix: &'a str,
    pub provider_name: &'a str,
    pub provider_priority: i32,
    pub secret: &'a str,
}

pub(crate) fn local_openai_auth_snapshot(
    api_key_id: &str,
    user_id: &str,
) -> StoredAuthApiKeySnapshot {
    StoredAuthApiKeySnapshot::new(
        user_id.to_string(),
        "alice".to_string(),
        Some("alice@example.com".to_string()),
        "user".to_string(),
        "local".to_string(),
        true,
        false,
        Some(serde_json::json!(["openai"])),
        Some(serde_json::json!(["openai:chat"])),
        Some(serde_json::json!(["gpt-5"])),
        api_key_id.to_string(),
        Some("default".to_string()),
        true,
        false,
        false,
        Some(60),
        Some(5),
        Some(4_102_444_800),
        Some(serde_json::json!(["openai"])),
        Some(serde_json::json!(["openai:chat"])),
        Some(serde_json::json!(["gpt-5"])),
    )
    .expect("auth snapshot should build")
}

pub(crate) fn local_openai_candidate_row(
    spec: &LocalOpenAiSpec<'_>,
) -> StoredMinimalCandidateSelectionRow {
    use aether_data_contracts::repository::candidate_selection::StoredProviderModelMapping;

    let id = |prefix: &str| format!("{prefix}-{}", spec.id_suffix);
    StoredMinimalCandidateSelectionRow {
        provider_id: id("provider"),
        provider_name: spec.provider_name.to_string(),
        provider_type: "custom".to_string(),
        provider_priority: 10,
        provider_is_active: true,
        endpoint_id: id("endpoint"),
        endpoint_api_format: "openai:chat".to_string(),
        endpoint_api_family: Some("openai".to_string()),
        endpoint_kind: Some("chat".to_string()),
        endpoint_is_active: true,
        key_id: id("key"),
        key_name: "prod".to_string(),
        key_auth_type: "api_key".to_string(),
        key_is_active: true,
        key_api_formats: Some(vec!["openai:chat".to_string()]),
        key_allowed_models: None,
        key_capabilities: None,
        key_internal_priority: 5,
        key_global_priority_by_format: Some(serde_json::json!({"openai:chat": 1})),
        model_id: id("model"),
        global_model_id: id("global-model"),
        global_model_name: "gpt-5".to_string(),
        global_model_mappings: None,
        global_model_supports_streaming: Some(true),
        model_provider_model_name: "gpt-5-upstream".to_string(),
        model_provider_model_mappings: Some(vec![StoredProviderModelMapping {
            name: "gpt-5-upstream".to_string(),
            priority: 1,
            api_formats: Some(vec!["openai:chat".to_string()]),
            endpoint_ids: None,
            operations: None,
        }]),
        model_supports_streaming: Some(true),
        model_is_active: true,
        model_is_available: true,
    }
}

pub(crate) fn local_openai_provider(spec: &LocalOpenAiSpec<'_>) -> StoredProviderCatalogProvider {
    StoredProviderCatalogProvider::new(
        format!("provider-{}", spec.id_suffix),
        spec.provider_name.to_string(),
        Some("https://example.com".to_string()),
        "custom".to_string(),
    )
    .expect("provider should build")
    .with_transport_fields(
        true,
        false,
        false,
        None,
        Some(spec.provider_priority),
        None,
        Some(20.0),
        None,
        None,
    )
}

pub(crate) fn local_openai_endpoint(
    spec: &LocalOpenAiSpec<'_>,
    base_url: &str,
) -> StoredProviderCatalogEndpoint {
    StoredProviderCatalogEndpoint::new(
        format!("endpoint-{}", spec.id_suffix),
        format!("provider-{}", spec.id_suffix),
        "openai:chat".to_string(),
        Some("openai".to_string()),
        Some("chat".to_string()),
        true,
    )
    .expect("endpoint should build")
    .with_transport_fields(
        base_url.to_string(),
        None,
        None,
        Some(spec.provider_priority),
        None,
        None,
        None,
        None,
    )
    .expect("endpoint transport should build")
}

pub(crate) fn local_openai_key(spec: &LocalOpenAiSpec<'_>) -> StoredProviderCatalogKey {
    let encrypted_api_key = encrypt_python_fernet_plaintext(DEVELOPMENT_ENCRYPTION_KEY, spec.secret)
        .expect("api key should encrypt");
    StoredProviderCatalogKey::new(
        format!("key-{}", spec.id_suffix),
        format!("provider-{}", spec.id_suffix),
        "prod".to_string(),
        "api_key".to_string(),
        None,
        true,
    )
    .expect("key should build")
    .with_transport_fields(
        Some(serde_json::json!(["openai:chat"])),
        encrypted_api_key,
        None,
        None,
        Some(serde_json::json!({"openai:chat": 1})),
        None,
        None,
        None,
        None,
    )
    .expect("key transport should build")
}
