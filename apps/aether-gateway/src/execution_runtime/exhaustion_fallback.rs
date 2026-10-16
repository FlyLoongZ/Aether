use std::collections::BTreeMap;

use aether_contracts::ExecutionPlan;
use axum::body::Body;
use axum::http::Response;
use serde_json::Value;

use crate::api::response::{attach_control_metadata_headers, build_client_response_from_parts};
use crate::control::GatewayControlDecision;
use crate::{AppState, GatewayError};

use super::apply_endpoint_response_header_rules;
use super::submission::{
    align_gemini_error_code_with_status, build_client_error_body_for_provider_error,
};

/// Provider headers that describe the response bytes themselves. They travel
/// with the body, so they are the only upstream headers allowed to reach the
/// client on a preserved error. Everything else (`retry-after`, `x-ratelimit-*`,
/// `x-request-id`, `server`, `cf-ray`, organization ids, ...) is dropped.
const BODY_REPRESENTATION_HEADERS: [&str; 2] = ["content-type", "content-encoding"];

pub(crate) struct ExhaustionFallbackInput<'a> {
    pub(crate) plan: &'a ExecutionPlan,
    pub(crate) report_context: Option<&'a Value>,
    pub(crate) trace_id: &'a str,
    pub(crate) decision: &'a GatewayControlDecision,
    pub(crate) status_code: u16,
    pub(crate) provider_headers: &'a BTreeMap<String, String>,
    pub(crate) provider_body: &'a [u8],
    pub(crate) provider_body_json: Option<&'a Value>,
    /// Rebuild the error body in the client protocol even when the formats
    /// match. Needed when the provider body is a private stream envelope rather
    /// than a client-format error: byte passthrough would hand the client an SSE
    /// stream under an error status.
    pub(crate) rebuild_client_body: bool,
    pub(crate) request_id: Option<&'a str>,
    pub(crate) candidate_id: Option<&'a str>,
}

/// Both sides of a preserved upstream error, captured while both are still
/// available so the terminal usage row can show the upstream response and the
/// response the client actually received.
#[derive(Debug, Clone)]
pub(crate) struct ExhaustionFallbackCapture {
    pub(crate) status_code: u16,
    pub(crate) provider_headers: Value,
    pub(crate) provider_body: Option<Value>,
    pub(crate) client_headers: Value,
    pub(crate) client_body: Option<Value>,
}

fn resolve_format<'a>(report_context: Option<&'a Value>, key: &str, fallback: &'a str) -> &'a str {
    report_context
        .and_then(|value| value.get(key))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| fallback.trim())
}

fn retain_body_representation_headers(
    headers: &BTreeMap<String, String>,
) -> BTreeMap<String, String> {
    headers
        .iter()
        .filter(|(name, _)| {
            BODY_REPRESENTATION_HEADERS
                .iter()
                .any(|allowed| name.eq_ignore_ascii_case(allowed))
        })
        .map(|(name, value)| (name.clone(), value.clone()))
        .collect()
}

/// Builds the client-facing response for a preserved upstream error.
///
/// Same-format plans keep the provider bytes untouched unless
/// `rebuild_client_body` asks for a client-format error body; cross-format plans
/// are always rebuilt in the client protocol. Only body-representation headers
/// survive, and endpoint response header rules still apply because they are
/// operator configuration rather than upstream data.
pub(crate) async fn build_exhaustion_fallback(
    state: &AppState,
    input: ExhaustionFallbackInput<'_>,
) -> Result<Response<Body>, GatewayError> {
    let ExhaustionFallbackInput {
        plan,
        report_context,
        trace_id,
        decision,
        status_code,
        provider_headers,
        provider_body,
        provider_body_json,
        rebuild_client_body,
        request_id,
        candidate_id,
    } = input;

    let client_api_format = resolve_format(
        report_context,
        "client_api_format",
        plan.client_api_format.as_str(),
    );
    let provider_api_format = resolve_format(
        report_context,
        "provider_api_format",
        plan.provider_api_format.as_str(),
    );
    let same_format = client_api_format.eq_ignore_ascii_case(provider_api_format);

    // Only cross-format plans are rebuilt; same-format plans must keep the exact
    // provider bytes so unknown fields and key order survive. The override is
    // for private stream envelopes, where the provider bytes are not a
    // client-format error body even when the format names match.
    let mut converted = if same_format && !rebuild_client_body {
        None
    } else {
        provider_body_json.and_then(|body_json| {
            build_client_error_body_for_provider_error(
                client_api_format,
                provider_api_format,
                status_code,
                body_json,
            )
        })
    };

    // Gemini's numeric `error.code` describes the status the client receives.
    // The rebuilt body derives it from the error class, which can differ (for
    // example 502 is classified as a server error and would otherwise carry 500).
    if let Some(body) = converted.as_mut() {
        align_gemini_error_code_with_status(client_api_format, body, status_code);
    }

    let (client_body_bytes, mut headers) = match converted.as_ref() {
        Some(converted) => {
            let body = serde_json::to_vec(converted)
                .map_err(|err| GatewayError::Internal(err.to_string()))?;
            let mut headers = retain_body_representation_headers(provider_headers);
            // The rebuilt body is plain JSON, so a provider content-encoding
            // would make the client decode bytes that are no longer encoded.
            headers.retain(|name, _| !name.eq_ignore_ascii_case("content-encoding"));
            // Replace the retained content-type by a case-insensitive removal:
            // the upstream key is lowercase today, but a transport that keeps the
            // provider's casing would otherwise leave two content-type headers.
            headers.retain(|name, _| !name.eq_ignore_ascii_case("content-type"));
            headers.insert("content-type".to_string(), "application/json".to_string());
            (body, headers)
        }
        None => (
            provider_body.to_vec(),
            retain_body_representation_headers(provider_headers),
        ),
    };

    apply_endpoint_response_header_rules(state, plan, &mut headers, provider_body_json).await?;

    let response = build_client_response_from_parts(
        status_code,
        &headers,
        Body::from(client_body_bytes.clone()),
        trace_id,
        Some(decision),
    )?;
    let mut response = attach_control_metadata_headers(response, request_id, candidate_id)?;

    // Record the headers the gateway assembled for the client, not the wire
    // headers: building the response also adds gateway-internal control and
    // security headers, which no other report path records either.
    let capture = ExhaustionFallbackCapture {
        status_code,
        provider_headers: serde_json::to_value(provider_headers).unwrap_or(Value::Null),
        provider_body: crate::orchestration::trace_upstream_response_body(
            provider_body_json,
            provider_body,
        ),
        client_headers: serde_json::to_value(&headers).unwrap_or(Value::Null),
        client_body: crate::orchestration::trace_upstream_response_body(
            converted.as_ref(),
            &client_body_bytes,
        ),
    };
    response.extensions_mut().insert(capture);

    Ok(response)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use aether_contracts::{ExecutionPlan, RequestBody};
    use axum::body::{to_bytes, Body};
    use axum::http::{HeaderMap, Response};
    use serde_json::json;

    use super::{build_exhaustion_fallback, ExhaustionFallbackInput};
    use crate::control::GatewayControlDecision;
    use crate::AppState;

    fn test_plan(client_api_format: &str, provider_api_format: &str) -> ExecutionPlan {
        ExecutionPlan {
            request_id: "req-exhaustion-fallback".to_string(),
            candidate_id: Some("cand-exhaustion-fallback".to_string()),
            provider_name: Some("test-provider".to_string()),
            provider_id: "provider-test".to_string(),
            endpoint_id: "endpoint-test".to_string(),
            key_id: "key-test".to_string(),
            method: "POST".to_string(),
            url: "https://example.test/v1/messages".to_string(),
            headers: BTreeMap::new(),
            content_type: Some("application/json".to_string()),
            content_encoding: None,
            body: RequestBody::from_json(json!({"model": "test-model"})),
            stream: false,
            client_api_format: client_api_format.to_string(),
            provider_api_format: provider_api_format.to_string(),
            model_name: Some("test-model".to_string()),
            proxy: None,
            transport_profile: None,
            timeouts: None,
        }
    }

    fn test_decision() -> GatewayControlDecision {
        GatewayControlDecision::synthetic(
            "/v1/chat/completions",
            Some("ai_public".to_string()),
            Some("openai".to_string()),
            Some("chat".to_string()),
            Some("openai:chat".to_string()),
        )
    }

    async fn response_parts(response: Response<Body>) -> (HeaderMap, Vec<u8>) {
        let (parts, body) = response.into_parts();
        let bytes = to_bytes(body, usize::MAX)
            .await
            .expect("response body should read")
            .to_vec();
        (parts.headers, bytes)
    }

    fn header<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
        headers.get(name).and_then(|value| value.to_str().ok())
    }

    #[tokio::test]
    async fn same_format_keeps_provider_bytes_and_only_body_headers() {
        let state = AppState::new().expect("app state should build");
        let plan = test_plan("openai:chat", "openai:chat");
        let decision = test_decision();
        let raw = br#"{ "error" : {"message":"upstream exploded","type":"server_error"} }"#;
        let provider_headers = BTreeMap::from([
            ("content-type".to_string(), "application/json".to_string()),
            ("content-encoding".to_string(), "identity".to_string()),
            ("retry-after".to_string(), "30".to_string()),
            ("x-request-id".to_string(), "req_upstream".to_string()),
            ("cf-ray".to_string(), "8f2c1d".to_string()),
            ("server".to_string(), "cloudflare".to_string()),
        ]);
        let provider_body_json: serde_json::Value =
            serde_json::from_slice(raw).expect("provider body should parse");

        let response = build_exhaustion_fallback(
            &state,
            ExhaustionFallbackInput {
                plan: &plan,
                report_context: None,
                trace_id: "trace-same-format",
                decision: &decision,
                status_code: 500,
                provider_headers: &provider_headers,
                provider_body: raw,
                provider_body_json: Some(&provider_body_json),
                rebuild_client_body: false,
                request_id: Some(plan.request_id.as_str()),
                candidate_id: plan.candidate_id.as_deref(),
            },
        )
        .await
        .expect("fallback should build");

        assert_eq!(response.status().as_u16(), 500);
        let (headers, body) = response_parts(response).await;
        assert_eq!(body, raw);
        assert_eq!(header(&headers, "content-type"), Some("application/json"));
        assert_eq!(header(&headers, "content-encoding"), Some("identity"));
        assert!(header(&headers, "retry-after").is_none());
        assert!(header(&headers, "x-request-id").is_none());
        assert!(header(&headers, "cf-ray").is_none());
        assert!(header(&headers, "server").is_none());
    }

    #[tokio::test]
    async fn cross_format_rebuilds_body_and_drops_content_encoding() {
        let state = AppState::new().expect("app state should build");
        let plan = test_plan("openai:chat", "claude:messages");
        let decision = test_decision();
        let raw = br#"{"type":"error","error":{"type":"rate_limit_error","message":"slow down"}}"#;
        let provider_headers = BTreeMap::from([
            ("content-type".to_string(), "application/json".to_string()),
            ("content-encoding".to_string(), "gzip".to_string()),
            ("retry-after".to_string(), "12".to_string()),
        ]);
        let provider_body_json: serde_json::Value =
            serde_json::from_slice(raw).expect("provider body should parse");

        let response = build_exhaustion_fallback(
            &state,
            ExhaustionFallbackInput {
                plan: &plan,
                report_context: None,
                trace_id: "trace-cross-format",
                decision: &decision,
                status_code: 429,
                provider_headers: &provider_headers,
                provider_body: raw,
                provider_body_json: Some(&provider_body_json),
                rebuild_client_body: false,
                request_id: Some(plan.request_id.as_str()),
                candidate_id: plan.candidate_id.as_deref(),
            },
        )
        .await
        .expect("fallback should build");

        assert_eq!(response.status().as_u16(), 429);
        let (headers, body) = response_parts(response).await;
        assert_eq!(header(&headers, "content-type"), Some("application/json"));
        assert!(header(&headers, "content-encoding").is_none());
        assert!(header(&headers, "retry-after").is_none());

        let body: serde_json::Value = serde_json::from_slice(&body).expect("json body");
        assert_eq!(
            body,
            json!({"error": {"message": "slow down", "type": "rate_limit_error"}})
        );
    }

    #[tokio::test]
    async fn capture_records_upstream_and_client_sides_separately() {
        let state = AppState::new().expect("app state should build");
        let plan = test_plan("openai:chat", "claude:messages");
        let decision = test_decision();
        let raw = br#"{"type":"error","error":{"type":"rate_limit_error","message":"slow down"}}"#;
        let provider_headers = BTreeMap::from([
            ("content-type".to_string(), "application/json".to_string()),
            ("retry-after".to_string(), "12".to_string()),
        ]);
        let provider_body_json: serde_json::Value =
            serde_json::from_slice(raw).expect("provider body should parse");

        let response = build_exhaustion_fallback(
            &state,
            ExhaustionFallbackInput {
                plan: &plan,
                report_context: None,
                trace_id: "trace-capture",
                decision: &decision,
                status_code: 429,
                provider_headers: &provider_headers,
                provider_body: raw,
                provider_body_json: Some(&provider_body_json),
                rebuild_client_body: false,
                request_id: Some(plan.request_id.as_str()),
                candidate_id: plan.candidate_id.as_deref(),
            },
        )
        .await
        .expect("fallback should build");

        let capture = response
            .extensions()
            .get::<super::ExhaustionFallbackCapture>()
            .expect("fallback must record both sides for the usage row")
            .clone();

        assert_eq!(capture.status_code, 429);
        // Upstream side keeps the provider's own error body and headers.
        assert_eq!(capture.provider_headers["retry-after"], json!("12"));
        assert_eq!(
            capture.provider_body,
            Some(json!({
                "type": "error",
                "error": {"type": "rate_limit_error", "message": "slow down"}
            }))
        );
        // Client side is the rebuilt body without the upstream-only header.
        assert_eq!(
            capture.client_body,
            Some(json!({"error": {"message": "slow down", "type": "rate_limit_error"}}))
        );
        assert!(capture.client_headers.get("retry-after").is_none());
        // Gateway-internal plumbing must not leak into the recorded client side.
        assert!(capture
            .client_headers
            .get("x-aether-control-candidate-id")
            .is_none());
        assert!(capture
            .client_headers
            .get("content-security-policy")
            .is_none());
    }

    #[tokio::test]
    async fn cross_format_replaces_a_mixed_case_content_type_once() {
        let state = AppState::new().expect("app state should build");
        let plan = test_plan("openai:chat", "claude:messages");
        let decision = test_decision();
        let raw = br#"{"type":"error","error":{"type":"overloaded_error","message":"busy"}}"#;
        // A transport that keeps the provider's casing would leave `Content-Type`
        // next to the rebuilt `content-type` unless the value is replaced.
        let provider_headers = BTreeMap::from([
            ("Content-Type".to_string(), "text/html".to_string()),
            ("Content-Encoding".to_string(), "gzip".to_string()),
        ]);
        let provider_body_json: serde_json::Value =
            serde_json::from_slice(raw).expect("provider body should parse");

        let response = build_exhaustion_fallback(
            &state,
            ExhaustionFallbackInput {
                plan: &plan,
                report_context: None,
                trace_id: "trace-mixed-case",
                decision: &decision,
                status_code: 529,
                provider_headers: &provider_headers,
                provider_body: raw,
                provider_body_json: Some(&provider_body_json),
                rebuild_client_body: false,
                request_id: Some(plan.request_id.as_str()),
                candidate_id: plan.candidate_id.as_deref(),
            },
        )
        .await
        .expect("fallback should build");

        let (headers, _) = response_parts(response).await;
        assert_eq!(
            headers.get_all("content-type").iter().count(),
            1,
            "the rebuilt body must advertise exactly one content type"
        );
        assert_eq!(header(&headers, "content-type"), Some("application/json"));
        assert!(header(&headers, "content-encoding").is_none());
    }

    #[tokio::test]
    async fn non_json_provider_error_keeps_raw_body() {
        let state = AppState::new().expect("app state should build");
        let plan = test_plan("openai:chat", "claude:messages");
        let decision = test_decision();
        let raw = b"<html><body>502 Bad Gateway</body></html>";
        let provider_headers = BTreeMap::from([
            ("content-type".to_string(), "text/html".to_string()),
            ("x-request-id".to_string(), "req_upstream".to_string()),
        ]);

        let response = build_exhaustion_fallback(
            &state,
            ExhaustionFallbackInput {
                plan: &plan,
                report_context: None,
                trace_id: "trace-non-json",
                decision: &decision,
                status_code: 502,
                provider_headers: &provider_headers,
                provider_body: raw,
                provider_body_json: None,
                rebuild_client_body: false,
                request_id: Some(plan.request_id.as_str()),
                candidate_id: plan.candidate_id.as_deref(),
            },
        )
        .await
        .expect("fallback should build");

        assert_eq!(response.status().as_u16(), 502);
        let (headers, body) = response_parts(response).await;
        assert_eq!(body, raw);
        assert_eq!(header(&headers, "content-type"), Some("text/html"));
        assert!(header(&headers, "x-request-id").is_none());
    }

    #[tokio::test]
    async fn same_format_rebuilds_client_body_when_requested() {
        let state = AppState::new().expect("app state should build");
        let plan = test_plan("claude:messages", "claude:messages");
        let decision = test_decision();
        // The provider body is a stream envelope, not a client-format error.
        let raw = br#"event: error
data: {"type":"error","error":{"type":"rate_limit_error","message":"slow down"}}

"#;
        let provider_headers = BTreeMap::from([
            ("content-type".to_string(), "text/event-stream".to_string()),
            ("content-encoding".to_string(), "gzip".to_string()),
        ]);
        let provider_body_json = json!({
            "type": "error",
            "error": {"type": "rate_limit_error", "message": "slow down"}
        });

        let response = build_exhaustion_fallback(
            &state,
            ExhaustionFallbackInput {
                plan: &plan,
                report_context: None,
                trace_id: "trace-rebuild-same-format",
                decision: &decision,
                status_code: 429,
                provider_headers: &provider_headers,
                provider_body: raw,
                provider_body_json: Some(&provider_body_json),
                rebuild_client_body: true,
                request_id: Some(plan.request_id.as_str()),
                candidate_id: plan.candidate_id.as_deref(),
            },
        )
        .await
        .expect("fallback should build");

        assert_eq!(response.status().as_u16(), 429);
        let (headers, body) = response_parts(response).await;
        assert_eq!(header(&headers, "content-type"), Some("application/json"));
        assert!(header(&headers, "content-encoding").is_none());
        let body: serde_json::Value = serde_json::from_slice(&body).expect("json body");
        assert_eq!(
            body,
            json!({
                "type": "error",
                "error": {"type": "rate_limit_error", "message": "slow down"}
            })
        );
    }

    #[tokio::test]
    async fn gemini_cross_format_error_code_matches_http_status() {
        let state = AppState::new().expect("app state should build");
        let plan = test_plan("gemini:generate_content", "openai:chat");
        let decision = test_decision();
        let raw = br#"{"error":{"message":"bad gateway","type":"server_error"}}"#;
        let provider_headers =
            BTreeMap::from([("content-type".to_string(), "application/json".to_string())]);
        let provider_body_json: serde_json::Value =
            serde_json::from_slice(raw).expect("provider body should parse");

        let response = build_exhaustion_fallback(
            &state,
            ExhaustionFallbackInput {
                plan: &plan,
                report_context: None,
                trace_id: "trace-gemini-code",
                decision: &decision,
                status_code: 502,
                provider_headers: &provider_headers,
                provider_body: raw,
                provider_body_json: Some(&provider_body_json),
                rebuild_client_body: false,
                request_id: Some(plan.request_id.as_str()),
                candidate_id: plan.candidate_id.as_deref(),
            },
        )
        .await
        .expect("fallback should build");

        assert_eq!(response.status().as_u16(), 502);
        let (_, body) = response_parts(response).await;
        let body: serde_json::Value = serde_json::from_slice(&body).expect("json body");
        assert_eq!(body["error"]["code"], json!(502));
        assert_eq!(body["error"]["message"], json!("bad gateway"));
    }
}
