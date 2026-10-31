use std::time::{SystemTime, UNIX_EPOCH};

use axum::body::Bytes;
use axum::http::Uri;
use sha2::{Digest, Sha256};
use url::form_urlencoded;

use crate::{
    ai_serving::extract_gemini_model_from_path,
    headers::{decoded_request_body_bytes, header_value_str, is_json_request},
};

use super::super::GatewayControlDecision;
use super::types::{
    GatewayCredentialBundle, GatewayCredentialCarrier, GatewayExtractedCredentials,
    GatewayPrimaryCredential,
};

pub(crate) fn extract_requested_model(
    decision: &GatewayControlDecision,
    uri: &Uri,
    headers: &http::HeaderMap,
    body: &Bytes,
) -> Option<String> {
    if decision.route_family.as_deref() == Some("gemini") {
        if let Some(model) = extract_gemini_requested_model_from_path(uri.path()) {
            return Some(model);
        }
    }

    if !is_json_request(headers) || body.is_empty() {
        return None;
    }
    let body = decoded_request_body_bytes(headers, body.as_ref()).ok()?;
    serde_json::from_slice::<serde_json::Value>(body.as_ref())
        .ok()
        .and_then(|payload| {
            payload
                .get("model")
                .and_then(|value| value.as_str())
                .map(|value| value.trim().to_string())
        })
        .filter(|value| !value.is_empty())
}

fn extract_gemini_requested_model_from_path(path: &str) -> Option<String> {
    let model = extract_gemini_model_from_path(path)?;
    Some(
        model
            .split_once("/operations/")
            .map(|(model, _)| model)
            .unwrap_or(model.as_str())
            .to_string(),
    )
}

pub(super) fn extract_request_credentials(
    headers: &http::HeaderMap,
    uri: &Uri,
    auth_endpoint_signature: &str,
) -> GatewayExtractedCredentials {
    let bundle = GatewayCredentialBundle {
        authorization_bearer: unique_header_value_str(
            headers,
            http::header::AUTHORIZATION.as_str(),
        )
        .as_deref()
        .and_then(extract_bearer_token)
        .map(ToOwned::to_owned),
        x_api_key: header_value_str(headers, "x-api-key"),
        api_key: header_value_str(headers, "api-key"),
        x_goog_api_key: header_value_str(headers, "x-goog-api-key"),
        query_key: extract_query_api_key(uri),
        cookie_header: header_value_str(headers, http::header::COOKIE.as_str()),
    };
    let primary = select_primary_credential(auth_endpoint_signature, &bundle);

    GatewayExtractedCredentials { bundle, primary }
}

fn unique_header_value_str(headers: &http::HeaderMap, key: &str) -> Option<String> {
    let mut values = headers.get_all(key).iter();
    let value = values.next()?;
    if values.next().is_some() {
        return None;
    }
    value
        .to_str()
        .ok()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

pub(in crate::control) fn resolve_gateway_credential_carrier(
    headers: &http::HeaderMap,
    uri: &Uri,
    auth_endpoint_signature: &str,
) -> Option<GatewayCredentialCarrier> {
    extract_request_credentials(headers, uri, auth_endpoint_signature)
        .primary
        .map(|credential| match credential {
            GatewayPrimaryCredential::ProviderApiKey { carrier, .. }
            | GatewayPrimaryCredential::BearerToken { carrier, .. }
            | GatewayPrimaryCredential::CookieHeader { carrier, .. } => carrier,
        })
}

pub(super) fn build_auth_context_cache_key(
    headers: &http::HeaderMap,
    uri: &Uri,
    auth_endpoint_signature: &str,
) -> Option<String> {
    let signature = auth_endpoint_signature.trim();
    if signature.is_empty() {
        return None;
    }

    let bundle = extract_request_credentials(headers, uri, signature).bundle;
    if bundle.authorization_bearer.is_none()
        && bundle.x_api_key.is_none()
        && bundle.api_key.is_none()
        && bundle.x_goog_api_key.is_none()
        && bundle.query_key.is_none()
        && bundle.cookie_header.is_none()
    {
        return None;
    }

    let raw_cache_identity = format!(
        "{signature}\n{}\n{}\n{}\n{}\n{}\n{}",
        bundle.authorization_bearer.unwrap_or_default(),
        bundle.x_api_key.unwrap_or_default(),
        bundle.api_key.unwrap_or_default(),
        bundle.x_goog_api_key.unwrap_or_default(),
        bundle.query_key.unwrap_or_default(),
        bundle.cookie_header.unwrap_or_default(),
    );
    let mut hasher = Sha256::new();
    hasher.update(raw_cache_identity.as_bytes());
    Some(format!(
        "auth-context:sha256:{}",
        hex::encode(hasher.finalize())
    ))
}

fn select_primary_credential(
    auth_endpoint_signature: &str,
    bundle: &GatewayCredentialBundle,
) -> Option<GatewayPrimaryCredential> {
    let signature = auth_endpoint_signature.trim().to_ascii_lowercase();
    if signature.starts_with("gemini:") {
        return select_gemini_credential(bundle);
    }
    if signature.starts_with("antigravity:") {
        return select_antigravity_credential(bundle);
    }
    if signature.starts_with("claude:") {
        return select_claude_messages_credential(bundle);
    }
    if signature.starts_with("openai:") || signature.starts_with("codex:") {
        return select_openai_credential(bundle);
    }
    if signature.starts_with("aether:") {
        return select_openai_credential(bundle);
    }

    select_generic_credential(bundle)
}

fn select_antigravity_credential(
    bundle: &GatewayCredentialBundle,
) -> Option<GatewayPrimaryCredential> {
    first_provider_api_key(
        bundle,
        &[
            GatewayCredentialCarrier::XApiKey,
            GatewayCredentialCarrier::ApiKey,
        ],
    )
    .or_else(|| first_bearer_token(bundle))
    .or_else(|| select_cookie_credential(bundle))
}

fn select_openai_credential(bundle: &GatewayCredentialBundle) -> Option<GatewayPrimaryCredential> {
    first_provider_api_key(
        bundle,
        &[
            GatewayCredentialCarrier::AuthorizationBearer,
            GatewayCredentialCarrier::XApiKey,
            GatewayCredentialCarrier::ApiKey,
            GatewayCredentialCarrier::XGoogApiKey,
            GatewayCredentialCarrier::QueryKey,
        ],
    )
    .or_else(|| select_cookie_credential(bundle))
}

fn select_claude_messages_credential(
    bundle: &GatewayCredentialBundle,
) -> Option<GatewayPrimaryCredential> {
    first_provider_api_key(
        bundle,
        &[
            GatewayCredentialCarrier::XApiKey,
            GatewayCredentialCarrier::ApiKey,
            GatewayCredentialCarrier::AuthorizationBearer,
        ],
    )
    .or_else(|| first_bearer_token(bundle))
    .or_else(|| select_cookie_credential(bundle))
}

fn select_gemini_credential(bundle: &GatewayCredentialBundle) -> Option<GatewayPrimaryCredential> {
    first_provider_api_key(
        bundle,
        &[
            GatewayCredentialCarrier::QueryKey,
            GatewayCredentialCarrier::XGoogApiKey,
            GatewayCredentialCarrier::XApiKey,
            GatewayCredentialCarrier::ApiKey,
        ],
    )
    .or_else(|| first_bearer_token(bundle))
    .or_else(|| select_cookie_credential(bundle))
}

fn select_generic_credential(bundle: &GatewayCredentialBundle) -> Option<GatewayPrimaryCredential> {
    first_bearer_token(bundle)
        .or_else(|| {
            first_provider_api_key(
                bundle,
                &[
                    GatewayCredentialCarrier::XApiKey,
                    GatewayCredentialCarrier::ApiKey,
                    GatewayCredentialCarrier::XGoogApiKey,
                    GatewayCredentialCarrier::QueryKey,
                ],
            )
        })
        .or_else(|| select_cookie_credential(bundle))
}

fn first_provider_api_key(
    bundle: &GatewayCredentialBundle,
    carriers: &[GatewayCredentialCarrier],
) -> Option<GatewayPrimaryCredential> {
    for carrier in carriers {
        if let Some(raw) = credential_value(bundle, *carrier) {
            return Some(GatewayPrimaryCredential::ProviderApiKey {
                raw,
                carrier: *carrier,
            });
        }
    }
    None
}

fn first_bearer_token(bundle: &GatewayCredentialBundle) -> Option<GatewayPrimaryCredential> {
    credential_value(bundle, GatewayCredentialCarrier::AuthorizationBearer).map(|raw| {
        GatewayPrimaryCredential::BearerToken {
            raw,
            carrier: GatewayCredentialCarrier::AuthorizationBearer,
        }
    })
}

fn select_cookie_credential(bundle: &GatewayCredentialBundle) -> Option<GatewayPrimaryCredential> {
    credential_value(bundle, GatewayCredentialCarrier::CookieHeader).map(|raw| {
        GatewayPrimaryCredential::CookieHeader {
            raw,
            carrier: GatewayCredentialCarrier::CookieHeader,
        }
    })
}

fn credential_value(
    bundle: &GatewayCredentialBundle,
    carrier: GatewayCredentialCarrier,
) -> Option<String> {
    match carrier {
        GatewayCredentialCarrier::AuthorizationBearer => bundle.authorization_bearer.clone(),
        GatewayCredentialCarrier::XApiKey => bundle.x_api_key.clone(),
        GatewayCredentialCarrier::ApiKey => bundle.api_key.clone(),
        GatewayCredentialCarrier::XGoogApiKey => bundle.x_goog_api_key.clone(),
        GatewayCredentialCarrier::QueryKey => bundle.query_key.clone(),
        GatewayCredentialCarrier::CookieHeader => bundle.cookie_header.clone(),
    }
}

fn extract_query_api_key(uri: &Uri) -> Option<String> {
    let query = uri.query()?;
    form_urlencoded::parse(query.as_bytes())
        .find(|(key, value)| key == "key" && !value.trim().is_empty())
        .map(|(_, value)| value.into_owned())
}

fn extract_bearer_token(value: &str) -> Option<&str> {
    let trimmed = value.trim();
    let (scheme, token) = trimmed.split_once(' ')?;
    if !scheme.eq_ignore_ascii_case("bearer") {
        return None;
    }
    let token = token.trim();
    if token.is_empty() || token.chars().any(char::is_whitespace) {
        None
    } else {
        Some(token)
    }
}

pub(super) fn hash_api_key(value: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(value.as_bytes());
    hex::encode(hasher.finalize())
}

pub(super) fn contains_string(items: &[String], target: &str) -> bool {
    items
        .iter()
        .any(|item| item.trim().eq_ignore_ascii_case(target.trim()))
}

pub(super) fn current_unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::{
        build_auth_context_cache_key, extract_request_credentials, extract_requested_model,
        GatewayCredentialCarrier, GatewayPrimaryCredential,
    };
    use crate::control::GatewayControlDecision;
    use axum::body::Bytes;
    use axum::http::{self, Uri};

    fn uri(path: &str) -> Uri {
        path.parse().expect("uri should parse")
    }

    #[test]
    fn extract_requested_model_reads_zstd_encoded_json_body() {
        let decision = GatewayControlDecision::synthetic(
            "/v1/responses",
            Some("ai_public".to_string()),
            Some("openai".to_string()),
            Some("responses".to_string()),
            Some("openai:responses".to_string()),
        );
        let mut headers = http::HeaderMap::new();
        headers.insert(
            http::header::CONTENT_TYPE,
            http::HeaderValue::from_static("application/json"),
        );
        headers.insert(
            http::header::CONTENT_ENCODING,
            http::HeaderValue::from_static("zstd"),
        );
        let encoded =
            zstd::stream::encode_all(br#"{"model":"gpt-5.4","input":"hello"}"#.as_slice(), 0)
                .expect("zstd body should encode");

        let requested_model = extract_requested_model(
            &decision,
            &uri("/v1/responses"),
            &headers,
            &Bytes::from(encoded),
        );

        assert_eq!(requested_model.as_deref(), Some("gpt-5.4"));
    }

    #[test]
    fn extract_requested_model_handles_gemini_generation_and_operation_paths() {
        let generation_decision = GatewayControlDecision::synthetic(
            "/v1beta/models/gemini-2.5-pro:generateContent",
            Some("ai_public".to_string()),
            Some("gemini".to_string()),
            Some("generate_content".to_string()),
            Some("gemini:generate_content".to_string()),
        );
        let operation_decision = GatewayControlDecision::synthetic(
            "/v1beta/models/veo-3/operations/task-123:cancel",
            Some("ai_public".to_string()),
            Some("gemini".to_string()),
            Some("video".to_string()),
            Some("gemini:video".to_string()),
        );
        let headers = http::HeaderMap::new();

        assert_eq!(
            extract_requested_model(
                &generation_decision,
                &uri("/v1beta/models/gemini-2.5-pro:generateContent"),
                &headers,
                &Bytes::new(),
            )
            .as_deref(),
            Some("gemini-2.5-pro")
        );
        assert_eq!(
            extract_requested_model(
                &operation_decision,
                &uri("/v1beta/models/veo-3/operations/task-123:cancel"),
                &headers,
                &Bytes::new(),
            )
            .as_deref(),
            Some("veo-3")
        );
    }

    #[test]
    fn selects_openai_bearer_as_provider_api_key() {
        let mut headers = http::HeaderMap::new();
        headers.insert(
            http::header::AUTHORIZATION,
            "Bearer sk-openai".parse().unwrap(),
        );

        let extracted =
            extract_request_credentials(&headers, &uri("/v1/chat/completions"), "openai:chat");
        assert_eq!(
            extracted.primary,
            Some(GatewayPrimaryCredential::ProviderApiKey {
                raw: "sk-openai".to_string(),
                carrier: GatewayCredentialCarrier::AuthorizationBearer,
            })
        );
    }

    #[test]
    fn rejects_duplicate_or_combined_authorization_credentials() {
        let mut duplicate = http::HeaderMap::new();
        duplicate.append(
            http::header::AUTHORIZATION,
            "Bearer first-token".parse().unwrap(),
        );
        duplicate.append(
            http::header::AUTHORIZATION,
            "Bearer second-token".parse().unwrap(),
        );
        let extracted =
            extract_request_credentials(&duplicate, &uri("/api/admin/system"), "admin:operational");
        assert!(extracted.bundle.authorization_bearer.is_none());
        assert!(extracted.primary.is_none());

        let mut combined = http::HeaderMap::new();
        combined.insert(
            http::header::AUTHORIZATION,
            "Bearer first-token, Bearer second-token".parse().unwrap(),
        );
        let extracted =
            extract_request_credentials(&combined, &uri("/api/admin/system"), "admin:operational");
        assert!(extracted.bundle.authorization_bearer.is_none());
        assert!(extracted.primary.is_none());
    }

    #[test]
    fn selects_codex_live_bearer_as_provider_api_key() {
        let mut headers = http::HeaderMap::new();
        headers.insert(
            http::header::AUTHORIZATION,
            "Bearer sk-codex-live".parse().unwrap(),
        );

        let extracted =
            extract_request_credentials(&headers, &uri("/v1/live?model=gpt-live"), "codex:live");
        assert_eq!(
            extracted.primary,
            Some(GatewayPrimaryCredential::ProviderApiKey {
                raw: "sk-codex-live".to_string(),
                carrier: GatewayCredentialCarrier::AuthorizationBearer,
            })
        );
    }

    #[test]
    fn prefers_claude_chat_x_api_key_over_bearer() {
        let mut headers = http::HeaderMap::new();
        headers.insert(
            http::header::AUTHORIZATION,
            "Bearer cli-token".parse().unwrap(),
        );
        headers.insert("x-api-key", "claude-key".parse().unwrap());

        let extracted =
            extract_request_credentials(&headers, &uri("/v1/messages"), "claude:messages");
        assert_eq!(
            extracted.primary,
            Some(GatewayPrimaryCredential::ProviderApiKey {
                raw: "claude-key".to_string(),
                carrier: GatewayCredentialCarrier::XApiKey,
            })
        );
    }

    #[test]
    fn selects_claude_cli_bearer_as_provider_api_key() {
        let mut headers = http::HeaderMap::new();
        headers.insert(
            http::header::AUTHORIZATION,
            "Bearer cli-token".parse().unwrap(),
        );

        let extracted =
            extract_request_credentials(&headers, &uri("/v1/messages"), "claude:messages");
        assert_eq!(
            extracted.primary,
            Some(GatewayPrimaryCredential::ProviderApiKey {
                raw: "cli-token".to_string(),
                carrier: GatewayCredentialCarrier::AuthorizationBearer,
            })
        );
    }

    #[test]
    fn prefers_antigravity_aether_api_key_over_google_bearer() {
        let mut headers = http::HeaderMap::new();
        headers.insert(
            http::header::AUTHORIZATION,
            "Bearer google-oauth-access-token".parse().unwrap(),
        );
        headers.insert("x-api-key", "sk-aether-antigravity".parse().unwrap());

        let extracted = extract_request_credentials(
            &headers,
            &uri("/v1internal:streamGenerateContent?alt=sse"),
            "antigravity:v1internal",
        );
        assert_eq!(
            extracted.primary,
            Some(GatewayPrimaryCredential::ProviderApiKey {
                raw: "sk-aether-antigravity".to_string(),
                carrier: GatewayCredentialCarrier::XApiKey,
            })
        );
    }

    #[test]
    fn prefers_gemini_query_key_over_header_key() {
        let mut headers = http::HeaderMap::new();
        headers.insert("x-goog-api-key", "gemini-header".parse().unwrap());

        let extracted = extract_request_credentials(
            &headers,
            &uri("/v1beta/models?key=gemini-query"),
            "gemini:generate_content",
        );
        assert_eq!(
            extracted.primary,
            Some(GatewayPrimaryCredential::ProviderApiKey {
                raw: "gemini-query".to_string(),
                carrier: GatewayCredentialCarrier::QueryKey,
            })
        );
    }

    #[test]
    fn extracts_cookie_as_fallback_credential() {
        let mut headers = http::HeaderMap::new();
        headers.insert(http::header::COOKIE, "session=abc123".parse().unwrap());

        let extracted =
            extract_request_credentials(&headers, &uri("/v1/chat/completions"), "internal:session");
        assert_eq!(
            extracted.primary,
            Some(GatewayPrimaryCredential::CookieHeader {
                raw: "session=abc123".to_string(),
                carrier: GatewayCredentialCarrier::CookieHeader,
            })
        );
    }

    #[test]
    fn cache_key_hashes_cookie_header_instead_of_retaining_session_secret() {
        let mut headers = http::HeaderMap::new();
        headers.insert(http::header::COOKIE, "session=abc123".parse().unwrap());

        let cache_key = build_auth_context_cache_key(
            &headers,
            &uri("/v1/chat/completions"),
            "internal:session",
        )
        .expect("cache key should exist");
        assert!(cache_key.starts_with("auth-context:sha256:"));
        assert!(!cache_key.contains("session=abc123"));
    }

    #[test]
    fn cache_key_ignores_identity_headers() {
        let mut headers = http::HeaderMap::new();
        headers.insert("x-aether-auth-user-id", "user-1".parse().unwrap());
        headers.insert("x-aether-auth-api-key-id", "key-1".parse().unwrap());
        headers.insert("x-aether-admin-user-id", "admin-user-1".parse().unwrap());
        headers.insert("x-aether-admin-user-role", "admin".parse().unwrap());

        let cache_key =
            build_auth_context_cache_key(&headers, &uri("/v1/chat/completions"), "openai:chat");
        assert_eq!(cache_key, None);
    }

    #[test]
    fn ignores_unsolicited_identity_headers() {
        let mut headers = http::HeaderMap::new();
        for (name, value) in [
            ("x-aether-gateway", "rust-phase3b"),
            ("x-aether-auth-user-id", "user-1"),
            ("x-aether-auth-api-key-id", "key-1"),
            ("x-aether-auth-balance-remaining", "1.5"),
            ("x-aether-auth-access-allowed", "true"),
            ("x-aether-admin-user-id", "admin-user-1"),
            ("x-aether-admin-user-role", "admin"),
            ("x-aether-admin-session-id", "sess-1"),
        ] {
            headers.insert(name, value.parse().unwrap());
        }

        let extracted = extract_request_credentials(
            &headers,
            &uri("/api/admin/endpoints/health/api-formats"),
            "admin:endpoints_health",
        );
        assert_eq!(extracted.bundle.authorization_bearer, None);
        assert_eq!(extracted.primary, None);
    }
}
