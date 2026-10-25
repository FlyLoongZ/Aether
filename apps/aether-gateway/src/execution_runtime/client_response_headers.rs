use std::collections::BTreeMap;

use aether_contracts::ExecutionPlan;
use serde_json::Value;

use crate::{AppState, GatewayError};

use super::apply_endpoint_response_header_rules;

/// Describes the body the gateway will actually send to the client.
///
/// Representation headers (`content-type`, `content-encoding`, `content-length`)
/// describe the bytes, not the request outcome. Normalizing them from the final
/// body shape lets success and error responses share one filtering pipeline.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ClientResponseBodyMode {
    /// Provider bytes are forwarded unchanged; representation headers stay.
    UpstreamBytes,
    /// The gateway serialized a JSON value; old encodings no longer apply.
    RebuiltJson,
    /// The gateway produced an SSE stream, possibly rewritten.
    RebuiltSse,
    /// No body is sent; representation headers are dropped.
    Empty,
}

fn remove_header_case_insensitive(headers: &mut BTreeMap<String, String>, name: &str) {
    headers.retain(|key, _| !key.eq_ignore_ascii_case(name));
}

pub(crate) fn normalize_content_headers(
    headers: &mut BTreeMap<String, String>,
    body_mode: ClientResponseBodyMode,
) {
    match body_mode {
        ClientResponseBodyMode::UpstreamBytes => {}
        ClientResponseBodyMode::RebuiltJson => {
            remove_header_case_insensitive(headers, "content-encoding");
            remove_header_case_insensitive(headers, "content-length");
            remove_header_case_insensitive(headers, "content-type");
            headers.insert("content-type".to_string(), "application/json".to_string());
        }
        ClientResponseBodyMode::RebuiltSse => {
            remove_header_case_insensitive(headers, "content-encoding");
            remove_header_case_insensitive(headers, "content-length");
            remove_header_case_insensitive(headers, "content-type");
            headers.insert("content-type".to_string(), "text/event-stream".to_string());
        }
        ClientResponseBodyMode::Empty => {
            remove_header_case_insensitive(headers, "content-encoding");
            remove_header_case_insensitive(headers, "content-length");
        }
    }
}

/// Carries the request-scoped inputs needed to produce the final client header
/// set. `None` is reserved for internal/admin finalizers that only have an
/// already-prepared header snapshot.
#[derive(Clone, Copy)]
pub(crate) struct ResponseHeaderContext<'a> {
    state: Option<&'a AppState>,
    plan: Option<&'a ExecutionPlan>,
}

impl<'a> ResponseHeaderContext<'a> {
    pub(crate) const fn new(state: &'a AppState, plan: &'a ExecutionPlan) -> Self {
        Self {
            state: Some(state),
            plan: Some(plan),
        }
    }

    pub(crate) const fn none() -> Self {
        Self {
            state: None,
            plan: None,
        }
    }
}

/// Produces the headers for the body that is actually sent. Public requests
/// always re-run representation normalization and endpoint rules from the raw
/// provider header map after the finalizer has chosen the final body, so a rule
/// result is never overwritten by a later body rebuild.
pub(crate) async fn prepare_final_client_headers(
    context: ResponseHeaderContext<'_>,
    provider_headers: &BTreeMap<String, String>,
    fallback_client_headers: Option<&BTreeMap<String, String>>,
    body_mode: ClientResponseBodyMode,
    response_body: Option<&Value>,
) -> Result<BTreeMap<String, String>, GatewayError> {
    if let (Some(state), Some(plan)) = (context.state, context.plan) {
        return prepare_client_response_headers(
            state,
            Some(plan),
            provider_headers.clone(),
            body_mode,
            response_body,
        )
        .await;
    }

    // Internal/admin finalizers may not have an execution plan. Prefer their
    // already-prepared client snapshot; otherwise normalize the provider map.
    if let Some(headers) = fallback_client_headers {
        return Ok(crate::api::response::filter_client_response_headers(
            headers,
        ));
    }
    let mut headers = provider_headers.clone();
    normalize_content_headers(&mut headers, body_mode);
    Ok(crate::api::response::filter_client_response_headers(
        &headers,
    ))
}

/// Aligns representation headers with the final body, then applies endpoint
/// response header rules. Plans without an endpoint (local gateway errors) skip
/// the endpoint-specific rules, but still get the shared normalization.
pub(crate) async fn prepare_client_response_headers(
    state: &AppState,
    plan: Option<&ExecutionPlan>,
    mut headers: BTreeMap<String, String>,
    body_mode: ClientResponseBodyMode,
    response_body: Option<&Value>,
) -> Result<BTreeMap<String, String>, GatewayError> {
    normalize_content_headers(&mut headers, body_mode);
    if let Some(plan) = plan {
        apply_endpoint_response_header_rules(state, plan, &mut headers, response_body).await?;
    }
    // Record and expose the business headers that survive the client boundary.
    // Gateway-added trace/control/security headers are still added later by the
    // response builder and intentionally excluded from this map.
    Ok(crate::api::response::filter_client_response_headers(
        &headers,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn headers() -> BTreeMap<String, String> {
        BTreeMap::from([
            ("content-type".to_string(), "application/json".to_string()),
            ("content-encoding".to_string(), "gzip".to_string()),
            ("content-length".to_string(), "999".to_string()),
            ("x-provider-id".to_string(), "abc".to_string()),
        ])
    }

    #[test]
    fn upstream_bytes_keep_representation_headers() {
        let mut value = headers();
        normalize_content_headers(&mut value, ClientResponseBodyMode::UpstreamBytes);
        assert_eq!(value, headers());
    }

    #[test]
    fn rebuilt_json_drops_stale_encoding_and_length() {
        let mut value = headers();
        normalize_content_headers(&mut value, ClientResponseBodyMode::RebuiltJson);
        assert_eq!(value["content-type"], "application/json");
        assert!(!value.contains_key("content-encoding"));
        assert!(!value.contains_key("content-length"));
        assert_eq!(value["x-provider-id"], "abc");
    }

    #[test]
    fn rebuilt_sse_sets_event_stream_content_type() {
        let mut value = headers();
        normalize_content_headers(&mut value, ClientResponseBodyMode::RebuiltSse);
        assert_eq!(value["content-type"], "text/event-stream");
        assert!(!value.contains_key("content-encoding"));
        assert!(!value.contains_key("content-length"));
    }

    #[test]
    fn empty_body_drops_representation_headers() {
        let mut value = headers();
        normalize_content_headers(&mut value, ClientResponseBodyMode::Empty);
        assert!(!value.contains_key("content-encoding"));
        assert!(!value.contains_key("content-length"));
        assert_eq!(value["content-type"], "application/json");
    }

    #[tokio::test]
    async fn prepared_headers_are_the_ones_that_survive_the_client_boundary() {
        let state = crate::AppState::new().expect("state should build");
        let headers = BTreeMap::from([
            ("content-type".to_string(), "application/json".to_string()),
            (
                "location".to_string(),
                "https://provider.example".to_string(),
            ),
            ("set-cookie".to_string(), "session=provider".to_string()),
            ("access-control-allow-origin".to_string(), "*".to_string()),
            ("x-aether-control".to_string(), "1".to_string()),
            ("x-provider".to_string(), "kept".to_string()),
        ]);
        let prepared = prepare_client_response_headers(
            &state,
            None,
            headers,
            ClientResponseBodyMode::UpstreamBytes,
            None,
        )
        .await
        .expect("headers should prepare");
        assert_eq!(prepared["x-provider"], "kept");
        for blocked in [
            "location",
            "set-cookie",
            "access-control-allow-origin",
            "x-aether-control",
        ] {
            assert!(
                !prepared.contains_key(blocked),
                "{blocked} must be filtered"
            );
        }
    }
}
