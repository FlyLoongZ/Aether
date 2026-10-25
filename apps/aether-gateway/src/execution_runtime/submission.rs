use std::collections::BTreeMap;

use crate::ai_serving::api::{
    build_core_error_body_for_client_format, core_error_background_report_kind,
    core_error_default_client_api_format, core_success_background_report_kind,
    is_core_error_finalize_kind, maybe_compile_sync_finalize_response,
    normalize_provider_private_response_value as unwrap_local_finalize_response_value,
    LocalCoreSyncErrorKind,
};
use crate::api::response::build_client_response_from_parts;
use crate::control::GatewayControlDecision;
use crate::execution_runtime::{
    prepare_final_client_headers, ClientResponseBodyMode, ResponseHeaderContext,
};
use crate::usage::spawn_sync_report;
use crate::{usage::GatewaySyncReportRequest, AppState, GatewayError};
use aether_contracts::ExecutionPlan;
use aether_usage_runtime::decode_internal_report_body_base64;
use axum::body::Body;
use axum::http::{Response, StatusCode};
use tracing::warn;

#[derive(Clone, Debug)]
struct LocalSyncErrorDetails {
    message: String,
    code: Option<String>,
    kind: LocalCoreSyncErrorKind,
}

/// Derive the headers for a rebuilt JSON body from the raw provider map. This
/// runs after the finalizer has chosen the body, so endpoint rules always see
/// the final body and are never overwritten by a later rebuild.
async fn local_json_response_headers(
    context: ResponseHeaderContext<'_>,
    payload: &GatewaySyncReportRequest,
    body_json: &serde_json::Value,
) -> Result<BTreeMap<String, String>, GatewayError> {
    prepare_final_client_headers(
        context,
        &payload.provider_headers,
        payload.client_headers.as_ref(),
        ClientResponseBodyMode::RebuiltJson,
        Some(body_json),
    )
    .await
}

pub(super) async fn maybe_build_local_core_error_response(
    context: ResponseHeaderContext<'_>,
    trace_id: &str,
    decision: &GatewayControlDecision,
    payload: &mut GatewaySyncReportRequest,
) -> Result<Option<Response<Body>>, GatewayError> {
    if !is_core_error_finalize_kind(payload.report_kind.as_str()) {
        return Ok(None);
    }

    let Some(response_body_json) = resolve_local_core_error_response_body_json(payload)? else {
        return Ok(None);
    };
    let status_source_json = resolve_local_sync_source_body_json(payload)?;
    if payload.status_code < 400
        && !has_nested_error(&response_body_json)
        && !status_source_json.as_ref().is_some_and(has_nested_error)
    {
        return Ok(None);
    }

    let response_headers =
        local_json_response_headers(context, payload, &response_body_json).await?;
    payload.client_headers = Some(response_headers.clone());
    let body_bytes = serde_json::to_vec(&response_body_json)
        .map_err(|err| GatewayError::Internal(err.to_string()))?;

    Ok(Some(build_client_response_from_parts(
        status_source_json
            .as_ref()
            .map_or(payload.status_code, |body_json| {
                resolve_local_sync_error_status_code(payload.status_code, body_json)
            }),
        &response_headers,
        Body::from(body_bytes),
        trace_id,
        Some(decision),
    )?))
}

fn maybe_resolve_local_sync_response_body_json(
    payload: &GatewaySyncReportRequest,
) -> Result<Option<serde_json::Value>, GatewayError> {
    if let Some(client_body_json) = payload.client_body_json.clone() {
        return Ok(Some(client_body_json));
    }

    if is_core_error_finalize_kind(payload.report_kind.as_str()) {
        if let Some(converted) = resolve_local_core_error_response_body_json(payload)? {
            return Ok(Some(converted));
        }
    }

    resolve_local_sync_source_body_json(payload)
}

async fn build_local_sync_response_from_json(
    context: ResponseHeaderContext<'_>,
    trace_id: &str,
    decision: &GatewayControlDecision,
    payload: &mut GatewaySyncReportRequest,
    body_json: serde_json::Value,
) -> Result<Response<Body>, GatewayError> {
    let body_has_error = has_nested_error(&body_json);
    let status_code = if body_has_error
        || (payload.status_code >= 400 && is_core_error_finalize_kind(payload.report_kind.as_str()))
    {
        resolve_local_sync_error_status_code(payload.status_code, &body_json)
    } else {
        payload.status_code
    };

    let response_headers = local_json_response_headers(context, payload, &body_json).await?;
    payload.client_headers = Some(response_headers.clone());
    let body_bytes =
        serde_json::to_vec(&body_json).map_err(|err| GatewayError::Internal(err.to_string()))?;

    build_client_response_from_parts(
        status_code,
        &response_headers,
        Body::from(body_bytes),
        trace_id,
        Some(decision),
    )
}

async fn build_local_sync_response_from_bytes(
    context: ResponseHeaderContext<'_>,
    trace_id: &str,
    decision: &GatewayControlDecision,
    payload: &mut GatewaySyncReportRequest,
    body_bytes: Vec<u8>,
) -> Result<Response<Body>, GatewayError> {
    let body_mode = if body_bytes.is_empty() {
        ClientResponseBodyMode::Empty
    } else {
        ClientResponseBodyMode::UpstreamBytes
    };
    let response_headers = prepare_final_client_headers(
        context,
        &payload.provider_headers,
        payload.client_headers.as_ref(),
        body_mode,
        None,
    )
    .await?;
    payload.client_headers = Some(response_headers.clone());

    build_client_response_from_parts(
        payload.status_code,
        &response_headers,
        Body::from(body_bytes),
        trace_id,
        Some(decision),
    )
}

async fn build_local_core_sync_finalize_fallback_response(
    context: ResponseHeaderContext<'_>,
    trace_id: &str,
    decision: &GatewayControlDecision,
    payload: &mut GatewaySyncReportRequest,
) -> Result<Response<Body>, GatewayError> {
    if let Some(body_json) = maybe_resolve_local_sync_response_body_json(payload)? {
        return build_local_sync_response_from_json(
            context, trace_id, decision, payload, body_json,
        )
        .await;
    }

    if let Some(body_base64) = payload.body_base64.as_ref() {
        let body_bytes =
            decode_internal_report_body_base64(body_base64).map_err(GatewayError::Internal)?;
        return build_local_sync_response_from_bytes(
            context, trace_id, decision, payload, body_bytes,
        )
        .await;
    }

    build_local_sync_response_from_bytes(context, trace_id, decision, payload, Vec::new()).await
}

async fn maybe_build_invalid_provider_success_finalize_response(
    context: ResponseHeaderContext<'_>,
    trace_id: &str,
    decision: &GatewayControlDecision,
    payload: &mut GatewaySyncReportRequest,
) -> Result<Option<Response<Body>>, GatewayError> {
    if !local_core_sync_finalize_has_invalid_provider_success(payload)? {
        return Ok(None);
    }

    let client_api_format = resolve_local_sync_client_api_format(payload);
    let message = "Provider returned HTTP 200 but the Gemini response did not contain visible model output; refusing to finalize it as a successful response.";
    let body_json = build_core_error_body_for_client_format(
        &client_api_format,
        message,
        Some("invalid_provider_success_response"),
        LocalCoreSyncErrorKind::ServerError,
    )
    .unwrap_or_else(|| {
        serde_json::json!({
            "error": {
                "message": message,
                "type": "server_error",
                "code": "invalid_provider_success_response"
            }
        })
    });

    let response_headers = local_json_response_headers(context, payload, &body_json).await?;
    payload.client_headers = Some(response_headers.clone());
    let body_bytes =
        serde_json::to_vec(&body_json).map_err(|err| GatewayError::Internal(err.to_string()))?;

    Ok(Some(build_client_response_from_parts(
        StatusCode::BAD_GATEWAY.as_u16(),
        &response_headers,
        Body::from(body_bytes),
        trace_id,
        Some(decision),
    )?))
}

fn local_sync_needs_conversion(payload: &GatewaySyncReportRequest) -> bool {
    payload
        .report_context
        .as_ref()
        .and_then(|value| value.get("needs_conversion"))
        .and_then(|value| value.as_bool())
        .unwrap_or(false)
}

/// A successful upstream response that needed conversion but could not be
/// converted must not reach the client in the provider's own format.
async fn maybe_build_unconverted_cross_format_success_response(
    context: ResponseHeaderContext<'_>,
    trace_id: &str,
    decision: &GatewayControlDecision,
    payload: &mut GatewaySyncReportRequest,
) -> Result<Option<Response<Body>>, GatewayError> {
    if payload.status_code >= 400
        || !local_sync_needs_conversion(payload)
        || !is_core_error_finalize_kind(payload.report_kind.as_str())
    {
        return Ok(None);
    }

    let client_api_format = resolve_local_sync_client_api_format(payload);
    let provider_api_format = resolve_local_sync_provider_api_format(payload);
    warn!(
        event_name = "local_core_finalize_cross_format_success_unconverted",
        log_type = "event",
        trace_id = %trace_id,
        report_kind = %payload.report_kind,
        status_code = payload.status_code,
        client_api_format = %client_api_format,
        provider_api_format = %provider_api_format,
        "gateway could not convert a successful provider response to the client format"
    );
    let message = format!(
        "Provider returned HTTP {} but its {provider_api_format} response could not be converted to {client_api_format}.",
        payload.status_code
    );
    let body_json = build_core_error_body_for_client_format(
        &client_api_format,
        &message,
        Some("response_conversion_failed"),
        LocalCoreSyncErrorKind::ServerError,
    )
    .unwrap_or_else(|| {
        serde_json::json!({
            "error": {
                "message": message,
                "type": "server_error",
                "code": "response_conversion_failed"
            }
        })
    });

    let response_headers = local_json_response_headers(context, payload, &body_json).await?;
    payload.client_headers = Some(response_headers.clone());
    let body_bytes =
        serde_json::to_vec(&body_json).map_err(|err| GatewayError::Internal(err.to_string()))?;

    Ok(Some(build_client_response_from_parts(
        StatusCode::BAD_GATEWAY.as_u16(),
        &response_headers,
        Body::from(body_bytes),
        trace_id,
        Some(decision),
    )?))
}

fn local_core_sync_finalize_has_invalid_provider_success(
    payload: &GatewaySyncReportRequest,
) -> Result<bool, GatewayError> {
    if payload.status_code >= 400 || !is_core_error_finalize_kind(payload.report_kind.as_str()) {
        return Ok(false);
    }
    let provider_api_format = resolve_local_sync_provider_api_format(payload);
    if crate::ai_serving::normalize_api_format_alias(&provider_api_format)
        != "gemini:generate_content"
    {
        return Ok(false);
    }
    let Some(body_json) = resolve_local_sync_source_body_json(payload)? else {
        return Ok(false);
    };
    if has_nested_error(&body_json) {
        return Ok(false);
    }
    Ok(!crate::ai_serving::gemini_generate_content_response_has_visible_output(&body_json))
}

pub(crate) fn build_best_effort_local_core_error_body(
    payload: &GatewaySyncReportRequest,
    body_json: &serde_json::Value,
) -> Result<Option<serde_json::Value>, GatewayError> {
    let default_api_format = core_error_default_client_api_format(payload.report_kind.as_str())
        .unwrap_or_default()
        .to_string();
    let client_api_format = payload
        .report_context
        .as_ref()
        .and_then(|value| value.get("client_api_format"))
        .and_then(|value| value.as_str())
        .unwrap_or(default_api_format.as_str());
    let provider_api_format = payload
        .report_context
        .as_ref()
        .and_then(|value| value.get("provider_api_format"))
        .and_then(|value| value.as_str())
        .unwrap_or(client_api_format);

    Ok(build_client_error_body_for_provider_error(
        client_api_format,
        provider_api_format,
        payload.status_code,
        body_json,
    ))
}

/// Shared provider-error conversion used by both the terminal error finalize
/// path and the candidate-exhaustion fallback, so the two can never disagree
/// about the client-facing error shape.
///
/// Returns `None` when the client format is unknown or the error cannot be
/// rebuilt for it; callers then keep the provider body as-is.
pub(crate) fn build_client_error_body_for_provider_error(
    client_api_format: &str,
    provider_api_format: &str,
    status_code: u16,
    body_json: &serde_json::Value,
) -> Option<serde_json::Value> {
    let client_api_format = client_api_format.trim().to_ascii_lowercase();
    let provider_api_format = provider_api_format.trim().to_ascii_lowercase();

    if client_api_format.is_empty() {
        return None;
    }
    if client_api_format == provider_api_format {
        return Some(body_json.clone());
    }

    let details = extract_local_sync_error_details(status_code, body_json);
    build_core_error_body_for_client_format(
        &client_api_format,
        &details.message,
        details.code.as_deref(),
        details.kind,
    )
}

/// Gemini's numeric `error.code` describes the HTTP status the client receives.
/// A rebuilt error body derives the code from the error class, which can differ
/// (for example a 502 is classified as a server error and would carry 500).
pub(crate) fn align_gemini_error_code_with_status(
    client_api_format: &str,
    body: &mut serde_json::Value,
    status_code: u16,
) {
    if crate::ai_serving::normalize_api_format_alias(client_api_format) != "gemini:generate_content"
    {
        return;
    }
    if let Some(error) = body
        .get_mut("error")
        .and_then(serde_json::Value::as_object_mut)
    {
        error.insert("code".to_string(), serde_json::Value::from(status_code));
    }
}

pub(crate) fn resolve_local_core_error_response_body_json(
    payload: &GatewaySyncReportRequest,
) -> Result<Option<serde_json::Value>, GatewayError> {
    if !is_core_error_finalize_kind(payload.report_kind.as_str()) {
        return Ok(None);
    }

    if let Some(client_body_json) = payload.client_body_json.clone() {
        return Ok(Some(client_body_json));
    }

    if let Some(body_json) = resolve_local_sync_source_body_json(payload)? {
        if let Some(converted) = build_best_effort_local_core_error_body(payload, &body_json)? {
            return Ok(Some(converted));
        }
        return Ok(Some(body_json));
    }

    // A 2xx cross-format body that is not JSON (e.g. an aggregated SSE capture)
    // carries no upstream error; wrapping it as one would ship raw provider
    // bytes to the client under the success status.
    if payload.status_code < 400 && local_sync_needs_conversion(payload) {
        return Ok(None);
    }
    let Some(body_text) = decode_local_sync_body_text(payload)? else {
        return Ok(None);
    };
    let client_api_format = resolve_local_sync_client_api_format(payload);
    if client_api_format.is_empty() {
        return Ok(None);
    }

    let kind =
        classify_local_sync_error_kind(payload.status_code, None, None, None, body_text.as_str());
    Ok(build_core_error_body_for_client_format(
        &client_api_format,
        body_text.as_str(),
        None,
        kind,
    ))
}

fn resolve_local_sync_source_body_json(
    payload: &GatewaySyncReportRequest,
) -> Result<Option<serde_json::Value>, GatewayError> {
    let body_json = if let Some(body_json) = payload.body_json.clone() {
        body_json
    } else if let Some(body_base64) = payload.body_base64.as_deref() {
        let body_bytes =
            decode_internal_report_body_base64(body_base64).map_err(GatewayError::Internal)?;
        let stripped = strip_utf8_bom_and_ws(&body_bytes);
        let Ok(body_json) = serde_json::from_slice::<serde_json::Value>(stripped) else {
            return Ok(None);
        };
        body_json
    } else {
        return Ok(None);
    };

    if let Some(report_context) = payload.report_context.as_ref() {
        if let Some(unwrapped) =
            unwrap_local_finalize_response_value(body_json.clone(), report_context)
        {
            return Ok(Some(unwrapped));
        }
    }

    Ok(Some(body_json))
}

fn decode_local_sync_body_text(
    payload: &GatewaySyncReportRequest,
) -> Result<Option<String>, GatewayError> {
    let Some(body_base64) = payload.body_base64.as_deref() else {
        return Ok(None);
    };
    let body_bytes =
        decode_internal_report_body_base64(body_base64).map_err(GatewayError::Internal)?;
    let stripped = strip_utf8_bom_and_ws(&body_bytes);
    let body_text = String::from_utf8_lossy(stripped).trim().to_string();
    if body_text.is_empty() {
        return Ok(None);
    }
    Ok(Some(body_text))
}

fn resolve_local_sync_client_api_format(payload: &GatewaySyncReportRequest) -> String {
    let default_api_format = core_error_default_client_api_format(payload.report_kind.as_str())
        .unwrap_or_default()
        .to_string();
    payload
        .report_context
        .as_ref()
        .and_then(|value| value.get("client_api_format"))
        .and_then(|value| value.as_str())
        .unwrap_or(default_api_format.as_str())
        .trim()
        .to_ascii_lowercase()
}

fn resolve_local_sync_provider_api_format(payload: &GatewaySyncReportRequest) -> String {
    payload
        .report_context
        .as_ref()
        .and_then(|value| value.get("provider_api_format"))
        .and_then(|value| value.as_str())
        .map(|value| value.trim().to_ascii_lowercase())
        .unwrap_or_else(|| resolve_local_sync_client_api_format(payload))
}

pub(crate) fn resolve_core_error_background_report_kind(report_kind: &str) -> Option<String> {
    core_error_background_report_kind(report_kind).map(ToOwned::to_owned)
}

#[cfg(test)]
pub(crate) fn resolve_core_success_background_report_kind(report_kind: &str) -> Option<String> {
    core_success_background_report_kind(report_kind).map(ToOwned::to_owned)
}

pub(crate) fn resolve_local_sync_error_status_code(
    status_code: u16,
    body_json: &serde_json::Value,
) -> u16 {
    if (400..600).contains(&status_code) {
        return status_code;
    }

    let body_object = body_json.as_object();
    let error_object = body_object
        .and_then(|object| object.get("error"))
        .and_then(|value| value.as_object());

    let raw_code = first_non_empty_error_text(error_object, body_object, &["code"]);
    let raw_status = first_non_empty_error_text(error_object, body_object, &["status"]);
    for numeric_hint in [raw_code.as_deref(), raw_status.as_deref()]
        .into_iter()
        .flatten()
    {
        if let Ok(number) = numeric_hint.parse::<u16>() {
            if (400..600).contains(&number) {
                return number;
            }
        }
    }

    let raw_type = first_non_empty_error_text(error_object, body_object, &["type", "__type"]);
    let message = first_non_empty_error_text(
        error_object,
        body_object,
        &["message", "detail", "reason", "status", "type", "__type"],
    )
    .unwrap_or_else(|| "HTTP 400".to_string());
    let kind = classify_local_sync_error_kind(
        status_code,
        raw_type.as_deref(),
        raw_status.as_deref(),
        raw_code.as_deref(),
        message.as_str(),
    );
    default_status_code_for_local_sync_error_kind(kind)
}

fn extract_local_sync_error_details(
    status_code: u16,
    body_json: &serde_json::Value,
) -> LocalSyncErrorDetails {
    let resolved_status_code = resolve_local_sync_error_status_code(status_code, body_json);
    let body_object = body_json.as_object();
    let error_object = body_object
        .and_then(|object| object.get("error"))
        .and_then(|value| value.as_object());

    let message = first_non_empty_error_text(
        error_object,
        body_object,
        &["message", "detail", "reason", "status", "type", "__type"],
    )
    .unwrap_or_else(|| format!("HTTP {resolved_status_code}"));
    let code = first_non_empty_error_text(error_object, body_object, &["code", "status"]);
    let raw_type = first_non_empty_error_text(error_object, body_object, &["type", "__type"]);
    let raw_status = first_non_empty_error_text(error_object, body_object, &["status"]);
    let kind = classify_local_sync_error_kind(
        resolved_status_code,
        raw_type.as_deref(),
        raw_status.as_deref(),
        code.as_deref(),
        message.as_str(),
    );

    LocalSyncErrorDetails {
        message,
        code,
        kind,
    }
}

fn first_non_empty_error_text(
    error_object: Option<&serde_json::Map<String, serde_json::Value>>,
    body_object: Option<&serde_json::Map<String, serde_json::Value>>,
    keys: &[&str],
) -> Option<String> {
    for object in [error_object, body_object].into_iter().flatten() {
        for key in keys {
            let Some(value) = object.get(*key) else {
                continue;
            };
            match value {
                serde_json::Value::String(text) if !text.trim().is_empty() => {
                    return Some(text.trim().to_string());
                }
                serde_json::Value::Number(number) => return Some(number.to_string()),
                _ => {}
            }
        }
    }
    None
}

fn classify_local_sync_error_kind(
    status_code: u16,
    raw_type: Option<&str>,
    raw_status: Option<&str>,
    raw_code: Option<&str>,
    message: &str,
) -> LocalCoreSyncErrorKind {
    let mut fingerprint = String::new();
    for segment in [raw_type, raw_status, raw_code, Some(message)] {
        if let Some(segment) = segment.map(str::trim).filter(|value| !value.is_empty()) {
            if !fingerprint.is_empty() {
                fingerprint.push(' ');
            }
            fingerprint.push_str(&segment.to_ascii_lowercase());
        }
    }

    if status_code == 429
        || fingerprint.contains("rate_limit")
        || fingerprint.contains("rate limited")
        || fingerprint.contains("resource_exhausted")
        || fingerprint.contains("throttl")
    {
        return LocalCoreSyncErrorKind::RateLimit;
    }
    if status_code == 413
        || fingerprint.contains("request_too_large")
        || fingerprint.contains("request too large")
        || fingerprint.contains("payload_too_large")
        || fingerprint.contains("payload too large")
        || fingerprint.contains("request entity too large")
    {
        return LocalCoreSyncErrorKind::RequestTooLarge;
    }
    if fingerprint.contains("contextlength")
        || fingerprint.contains("contentlengthexceeded")
        || fingerprint.contains("context window")
        || fingerprint.contains("context length")
        || fingerprint.contains("max_tokens")
        || (fingerprint.contains("context") && fingerprint.contains("token"))
    {
        return LocalCoreSyncErrorKind::ContextLengthExceeded;
    }
    if status_code == 401
        || fingerprint.contains("unauth")
        || fingerprint.contains("authentication")
    {
        return LocalCoreSyncErrorKind::Authentication;
    }
    if status_code == 403 || fingerprint.contains("permission") || fingerprint.contains("forbidden")
    {
        return LocalCoreSyncErrorKind::PermissionDenied;
    }
    if status_code == 404 || fingerprint.contains("not_found") || fingerprint.contains("not found")
    {
        return LocalCoreSyncErrorKind::NotFound;
    }
    if status_code == 503 || fingerprint.contains("overload") || fingerprint.contains("unavailable")
    {
        return LocalCoreSyncErrorKind::Overloaded;
    }
    if (500..600).contains(&status_code)
        || raw_type.is_some_and(|value| {
            ["server_error", "internal_error", "api_error"]
                .iter()
                .any(|kind| value.trim().eq_ignore_ascii_case(kind))
        })
    {
        return LocalCoreSyncErrorKind::ServerError;
    }
    LocalCoreSyncErrorKind::InvalidRequest
}

fn default_status_code_for_local_sync_error_kind(kind: LocalCoreSyncErrorKind) -> u16 {
    match kind {
        LocalCoreSyncErrorKind::InvalidRequest | LocalCoreSyncErrorKind::ContextLengthExceeded => {
            400
        }
        LocalCoreSyncErrorKind::RequestTooLarge => 413,
        LocalCoreSyncErrorKind::Authentication => 401,
        LocalCoreSyncErrorKind::PermissionDenied => 403,
        LocalCoreSyncErrorKind::NotFound => 404,
        LocalCoreSyncErrorKind::RateLimit => 429,
        LocalCoreSyncErrorKind::Overloaded => 503,
        LocalCoreSyncErrorKind::ServerError => 500,
    }
}

pub(crate) fn strip_utf8_bom_and_ws(mut body: &[u8]) -> &[u8] {
    loop {
        while let Some(first) = body.first() {
            if first.is_ascii_whitespace() {
                body = &body[1..];
            } else {
                break;
            }
        }
        if body.starts_with(&[0xEF, 0xBB, 0xBF]) {
            body = &body[3..];
        } else {
            break;
        }
    }
    body
}

pub(crate) fn has_nested_error(value: &serde_json::Value) -> bool {
    let Some(object) = value.as_object() else {
        return false;
    };

    if object.get("error").is_some_and(|error| !error.is_null()) {
        return true;
    }
    if object
        .get("type")
        .and_then(|value| value.as_str())
        .is_some_and(|value| value == "error")
    {
        return true;
    }

    object
        .get("chunks")
        .and_then(|value| value.as_array())
        .is_some_and(|chunks| {
            chunks.iter().any(|chunk| {
                chunk.as_object().is_some_and(|chunk_object| {
                    chunk_object
                        .get("error")
                        .is_some_and(|error| !error.is_null())
                        || chunk_object
                            .get("type")
                            .and_then(|value| value.as_str())
                            .is_some_and(|value| value == "error")
                })
            })
        })
}

pub(crate) async fn submit_local_core_error_or_sync_finalize(
    state: &AppState,
    plan: &ExecutionPlan,
    trace_id: &str,
    decision: &GatewayControlDecision,
    payload: GatewaySyncReportRequest,
) -> Result<Response<Body>, GatewayError> {
    submit_local_core_error_or_sync_finalize_inner(
        state,
        ResponseHeaderContext::new(state, plan),
        trace_id,
        decision,
        payload,
    )
    .await
}

async fn submit_local_core_error_or_sync_finalize_inner(
    state: &AppState,
    context: ResponseHeaderContext<'_>,
    trace_id: &str,
    decision: &GatewayControlDecision,
    mut payload: GatewaySyncReportRequest,
) -> Result<Response<Body>, GatewayError> {
    let response = if let Some(outcome) =
        maybe_compile_sync_finalize_response(context, trace_id, decision, &payload).await?
    {
        payload.client_headers = Some(outcome.client_headers.clone());
        outcome.response
    } else if let Some(response) = maybe_build_invalid_provider_success_finalize_response(
        context,
        trace_id,
        decision,
        &mut payload,
    )
    .await?
    {
        response
    } else if let Some(response) =
        maybe_build_local_core_error_response(context, trace_id, decision, &mut payload).await?
    {
        response
    } else if let Some(response) = maybe_build_unconverted_cross_format_success_response(
        context,
        trace_id,
        decision,
        &mut payload,
    )
    .await?
    {
        response
    } else {
        warn!(
            event_name = "local_core_finalize_fallback_raw_response_body",
            log_type = "event",
            trace_id = %trace_id,
            report_kind = %payload.report_kind,
            status_code = payload.status_code,
            client_api_format = payload.report_context.as_ref().and_then(|value| value.get("client_api_format")).and_then(|value| value.as_str()).unwrap_or(""),
            provider_api_format = payload.report_context.as_ref().and_then(|value| value.get("provider_api_format")).and_then(|value| value.as_str()).unwrap_or(""),
            envelope_name = payload.report_context.as_ref().and_then(|value| value.get("envelope_name")).and_then(|value| value.as_str()).unwrap_or(""),
            needs_conversion = payload.report_context.as_ref().and_then(|value| value.get("needs_conversion")).and_then(|value| value.as_bool()).unwrap_or(false),
            "gateway local core finalize fell back to raw response body"
        );
        build_local_core_sync_finalize_fallback_response(context, trace_id, decision, &mut payload)
            .await?
    };

    let response_status = response.status();
    if response_status.is_success() {
        if let Some(success_report_kind) =
            core_success_background_report_kind(payload.report_kind.as_str())
        {
            let mut report_payload = payload.clone();
            report_payload.report_kind = success_report_kind.to_string();
            report_payload.status_code = response_status.as_u16();
            spawn_sync_report(state.clone(), report_payload);
        } else {
            warn!(
                event_name = "local_core_finalize_missing_success_report_mapping",
                log_type = "event",
                trace_id = %trace_id,
                report_kind = %payload.report_kind,
                "gateway built local core finalize success response without background success report mapping"
            );
        }
    } else if let Some(error_report_kind) =
        resolve_core_error_background_report_kind(payload.report_kind.as_str())
    {
        let mut report_payload = payload.clone();
        report_payload.report_kind = error_report_kind;
        report_payload.status_code = response_status.as_u16();
        spawn_sync_report(state.clone(), report_payload);
    } else {
        warn!(
            event_name = "local_core_finalize_missing_error_report_mapping",
            log_type = "event",
            trace_id = %trace_id,
            report_kind = %payload.report_kind,
            "gateway built local core finalize response without background error report mapping"
        );
    }

    Ok(response)
}

#[cfg(test)]
mod tests {
    #[test]
    fn success_http_status_does_not_misclassify_explicit_server_errors_as_bad_requests() {
        for error_type in ["server_error", "internal_error", "api_error"] {
            let body = serde_json::json!({ "error": { "type": error_type, "message": "failed" } });
            assert_eq!(super::resolve_local_sync_error_status_code(200, &body), 500);
        }
    }
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use aether_data::repository::provider_catalog::InMemoryProviderCatalogReadRepository;
    use aether_data_contracts::repository::provider_catalog::StoredProviderCatalogEndpoint;
    use axum::body::to_bytes;
    use serde_json::json;

    use super::{maybe_build_local_core_error_response, submit_local_core_error_or_sync_finalize};
    use crate::control::GatewayControlDecision;
    use crate::execution_runtime::ResponseHeaderContext;
    use crate::usage::GatewaySyncReportRequest;
    use crate::AppState;

    fn test_decision() -> GatewayControlDecision {
        GatewayControlDecision::synthetic(
            "/v1/chat/completions",
            Some("ai_public".to_string()),
            Some("openai".to_string()),
            Some("chat".to_string()),
            Some("openai:chat".to_string()),
        )
        .with_execution_runtime_candidate(true)
    }

    fn test_plan() -> aether_contracts::ExecutionPlan {
        aether_video_tasks_core::build_internal_finalize_video_plan(
            "trace-submit-finalize-tests",
            "openai:video",
            None,
        )
        .expect("test plan should build")
    }

    fn core_finalize_payload(
        report_kind: &str,
        client_api_format: &str,
        provider_api_format: &str,
        status_code: u16,
        body_json: serde_json::Value,
    ) -> GatewaySyncReportRequest {
        GatewaySyncReportRequest {
            trace_id: "trace-core-error-status-123".to_string(),
            report_kind: report_kind.to_string(),
            report_context: Some(json!({
                "client_api_format": client_api_format,
                "provider_api_format": provider_api_format,
            })),
            status_code,
            provider_headers: Default::default(),
            client_headers: None,
            body_json: Some(body_json),
            client_body_json: None,
            body_base64: None,
            telemetry: None,
        }
    }

    #[tokio::test]
    async fn error_finalize_preserves_content_type_decided_by_response_rules() {
        let mut payload = core_finalize_payload(
            "openai_chat_sync_finalize",
            "openai:chat",
            "openai:chat",
            400,
            json!({"error": {"type": "invalid_request_error", "message": "bad"}}),
        );
        payload.client_headers = Some(BTreeMap::from([(
            "content-type".to_string(),
            "application/problem+json".to_string(),
        )]));

        let response = maybe_build_local_core_error_response(
            ResponseHeaderContext::none(),
            "trace-sync-content-type",
            &test_decision(),
            &mut payload,
        )
        .await
        .expect("response build should not error")
        .expect("response should exist");
        assert_eq!(
            response.headers()["content-type"],
            "application/problem+json"
        );

        payload.client_headers = Some(BTreeMap::new());
        let response = maybe_build_local_core_error_response(
            ResponseHeaderContext::none(),
            "trace-sync-content-type-dropped",
            &test_decision(),
            &mut payload,
        )
        .await
        .expect("response build should not error")
        .expect("response should exist");
        assert!(response.headers().get("content-type").is_none());
    }

    fn state_and_plan_with_response_header_rules() -> (AppState, aether_contracts::ExecutionPlan) {
        let plan = aether_video_tasks_core::build_internal_finalize_video_plan(
            "trace-error-finalize-rules",
            "openai:video",
            None,
        )
        .expect("plan should build");
        let mut endpoint = StoredProviderCatalogEndpoint::new(
            plan.endpoint_id.clone(),
            plan.provider_id.clone(),
            plan.provider_api_format.clone(),
            None,
            None,
            true,
        )
        .expect("endpoint should build");
        endpoint.config = Some(json!({"response_header_rules": [
            {"action": "set", "key": "content-type", "value": "application/problem+json"},
            {"action": "set", "key": "x-marker", "value": "rule"},
            {"action": "drop", "key": "x-private"}
        ]}));
        let state = AppState::new()
            .expect("state should build")
            .with_data_state_for_tests(
                crate::data::GatewayDataState::with_provider_catalog_repository_for_tests(
                    Arc::new(InMemoryProviderCatalogReadRepository::seed(
                        vec![],
                        vec![endpoint],
                        vec![],
                    )),
                ),
            );
        (state, plan)
    }

    #[tokio::test]
    async fn error_finalize_recomputes_headers_from_provider_headers_and_rules() {
        let (state, plan) = state_and_plan_with_response_header_rules();
        let mut payload = core_finalize_payload(
            "openai_chat_sync_finalize",
            "openai:chat",
            "openai:chat",
            400,
            json!({"error": {"type": "invalid_request_error", "message": "bad"}}),
        );
        payload.provider_headers = BTreeMap::from([
            ("content-type".to_string(), "text/event-stream".to_string()),
            ("x-private".to_string(), "provider-only".to_string()),
        ]);
        // Stale prepared headers must not be treated as the rule source.
        payload.client_headers = Some(BTreeMap::from([
            ("content-type".to_string(), "application/json".to_string()),
            ("x-private".to_string(), "stale".to_string()),
        ]));

        let response = maybe_build_local_core_error_response(
            ResponseHeaderContext::new(&state, &plan),
            "trace-sync-rule-recompute",
            &test_decision(),
            &mut payload,
        )
        .await
        .expect("response build should not error")
        .expect("response should exist");

        assert_eq!(
            response.headers()["content-type"],
            "application/problem+json"
        );
        assert_eq!(response.headers()["x-marker"], "rule");
        assert!(response.headers().get("x-private").is_none());
        assert_eq!(
            payload.client_headers.as_ref().unwrap()["content-type"],
            "application/problem+json"
        );
    }

    #[tokio::test]
    async fn maybe_build_local_core_error_response_infers_status_from_semantic_error_type() {
        let mut payload = core_finalize_payload(
            "openai_chat_sync_finalize",
            "openai:chat",
            "claude:messages",
            200,
            json!({
                "type": "error",
                "error": {
                    "type": "rate_limit_error",
                    "message": "slow down"
                }
            }),
        );

        let response = maybe_build_local_core_error_response(
            ResponseHeaderContext::none(),
            "trace-sync-status-type",
            &test_decision(),
            &mut payload,
        )
        .await
        .expect("response build should not error")
        .expect("response should exist");

        assert_eq!(response.status(), http::StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(
                &to_bytes(response.into_body(), usize::MAX)
                    .await
                    .expect("body should read"),
            )
            .expect("body should decode"),
            json!({
                "error": {
                    "message": "slow down",
                    "type": "rate_limit_error"
                }
            })
        );
    }

    #[tokio::test]
    async fn maybe_build_local_core_error_response_infers_status_from_gemini_status_text() {
        let mut payload = core_finalize_payload(
            "gemini_chat_sync_finalize",
            "gemini:generate_content",
            "gemini:generate_content",
            200,
            json!({
                "error": {
                    "message": "quota reached",
                    "status": "RESOURCE_EXHAUSTED"
                }
            }),
        );

        let response = maybe_build_local_core_error_response(
            ResponseHeaderContext::none(),
            "trace-sync-status-gemini",
            &test_decision(),
            &mut payload,
        )
        .await
        .expect("response build should not error")
        .expect("response should exist");

        assert_eq!(response.status(), http::StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(
                &to_bytes(response.into_body(), usize::MAX)
                    .await
                    .expect("body should read"),
            )
            .expect("body should decode"),
            json!({
                "error": {
                    "message": "quota reached",
                    "status": "RESOURCE_EXHAUSTED"
                }
            })
        );
    }

    #[tokio::test]
    async fn local_core_error_maps_request_too_large_without_changing_openai_shape() {
        let mut claude_payload = core_finalize_payload(
            "claude_chat_sync_finalize",
            "claude:messages",
            "openai:chat",
            413,
            json!({
                "error": {
                    "type": "invalid_request_error",
                    "message": "request body is too large"
                }
            }),
        );
        let claude_response = maybe_build_local_core_error_response(
            ResponseHeaderContext::none(),
            "trace-sync-claude-too-large",
            &test_decision(),
            &mut claude_payload,
        )
        .await
        .expect("response build should not error")
        .expect("response should exist");
        assert_eq!(
            claude_response.status(),
            http::StatusCode::PAYLOAD_TOO_LARGE
        );
        let claude_body: serde_json::Value = serde_json::from_slice(
            &to_bytes(claude_response.into_body(), usize::MAX)
                .await
                .expect("body should read"),
        )
        .expect("body should decode");
        assert_eq!(claude_body["type"], "error");
        assert_eq!(claude_body["error"]["type"], "request_too_large");

        let mut openai_payload = core_finalize_payload(
            "openai_chat_sync_finalize",
            "openai:chat",
            "claude:messages",
            200,
            json!({
                "type": "error",
                "error": {
                    "type": "request_too_large",
                    "message": "request body is too large"
                }
            }),
        );
        let openai_response = maybe_build_local_core_error_response(
            ResponseHeaderContext::none(),
            "trace-sync-openai-too-large",
            &test_decision(),
            &mut openai_payload,
        )
        .await
        .expect("response build should not error")
        .expect("response should exist");
        assert_eq!(
            openai_response.status(),
            http::StatusCode::PAYLOAD_TOO_LARGE
        );
        let openai_body: serde_json::Value = serde_json::from_slice(
            &to_bytes(openai_response.into_body(), usize::MAX)
                .await
                .expect("body should read"),
        )
        .expect("body should decode");
        assert_eq!(
            openai_body["error"]["type"], "context_length_exceeded",
            "OpenAI compatibility shape should remain unchanged"
        );
    }

    #[tokio::test]
    async fn local_core_sync_finalize_rejects_gemini_http_200_without_visible_output() {
        let mut payload = core_finalize_payload(
            "openai_chat_sync_finalize",
            "openai:chat",
            "gemini:generate_content",
            200,
            json!({
                "candidates": [{
                    "content": {"role": "model"},
                    "finishReason": "MAX_TOKENS"
                }],
                "usageMetadata": {
                    "promptTokenCount": 8,
                    "candidatesTokenCount": 1,
                    "thoughtsTokenCount": 25,
                    "totalTokenCount": 34
                },
                "modelVersion": "gemini-3-flash-preview",
                "responseId": "resp-empty"
            }),
        );
        payload.report_context = Some(json!({
            "client_api_format": "openai:chat",
            "provider_api_format": "gemini:generate_content",
            "needs_conversion": true,
            "has_envelope": false
        }));

        let state = AppState::new().expect("state should build");
        let response = submit_local_core_error_or_sync_finalize(
            &state,
            &test_plan(),
            "trace-invalid-gemini-200",
            &test_decision(),
            payload,
        )
        .await
        .expect("response should build");

        assert_eq!(response.status(), http::StatusCode::BAD_GATEWAY);
        let body: serde_json::Value = serde_json::from_slice(
            &to_bytes(response.into_body(), usize::MAX)
                .await
                .expect("body should read"),
        )
        .expect("body should decode");
        let message = body["error"]["message"]
            .as_str()
            .expect("error message should exist");
        assert!(
            message.contains("visible model output"),
            "unexpected message: {message}"
        );
    }

    #[tokio::test]
    async fn local_core_sync_finalize_converts_forced_responses_stream_for_gemini_client() {
        use base64::Engine as _;

        // Forced-stream xAI shape: the terminal response echoes request
        // metadata and encrypted reasoning next to the real answer.
        let raw_sse = concat!(
            "event: response.created\n",
            "data: {\"type\":\"response.created\",\"sequence_number\":0,\"response\":{\"id\":\"resp_xai_123\",\"object\":\"response\",\"status\":\"in_progress\",\"model\":\"grok-4.7-build\",\"output\":[],\"parallel_tool_calls\":true,\"tools\":[]}}\n\n",
            "event: response.output_item.done\n",
            "data: {\"type\":\"response.output_item.done\",\"sequence_number\":1,\"output_index\":0,\"item\":{\"id\":\"rs_xai_123\",\"type\":\"reasoning\",\"status\":\"completed\",\"summary\":[],\"encrypted_content\":\"opaque-xai-reasoning\"}}\n\n",
            "event: response.output_text.delta\n",
            "data: {\"type\":\"response.output_text.delta\",\"sequence_number\":2,\"item_id\":\"msg_xai_123\",\"output_index\":1,\"content_index\":0,\"delta\":\"Hi there, friend\"}\n\n",
            "event: response.output_item.done\n",
            "data: {\"type\":\"response.output_item.done\",\"sequence_number\":3,\"output_index\":1,\"item\":{\"id\":\"msg_xai_123\",\"type\":\"message\",\"status\":\"completed\",\"role\":\"assistant\",\"content\":[{\"type\":\"output_text\",\"text\":\"Hi there, friend\",\"annotations\":[]}]}}\n\n",
            "event: response.completed\n",
            "data: {\"type\":\"response.completed\",\"sequence_number\":4,\"response\":{\"id\":\"resp_xai_123\",\"object\":\"response\",\"status\":\"completed\",\"model\":\"grok-4.7-build\",\"output\":[],\"parallel_tool_calls\":true,\"tool_choice\":\"auto\",\"tools\":[],\"text\":{\"format\":{\"type\":\"text\"}},\"temperature\":0.7,\"store\":false,\"usage\":{\"input_tokens\":1249,\"output_tokens\":12,\"total_tokens\":1261}}}\n\n",
        );
        let mut payload = core_finalize_payload(
            "gemini_chat_sync_finalize",
            "gemini:generate_content",
            "openai:responses",
            200,
            json!(null),
        );
        payload.body_json = None;
        payload.body_base64 = Some(base64::engine::general_purpose::STANDARD.encode(raw_sse));
        payload.report_context = Some(json!({
            "client_api_format": "gemini:generate_content",
            "provider_api_format": "openai:responses",
            "provider_stream_event_api_format": "openai:responses",
            "model": "grok-4.7",
            "mapped_model": "grok-4.7",
            "needs_conversion": true,
        }));

        let state = AppState::new().expect("state should build");
        let response = submit_local_core_error_or_sync_finalize(
            &state,
            &test_plan(),
            "trace-forced-responses-gemini",
            &test_decision(),
            payload,
        )
        .await
        .expect("finalize should build a response");

        assert_eq!(response.status(), http::StatusCode::OK);
        let body_bytes = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body should read");
        let body =
            serde_json::from_slice::<serde_json::Value>(&body_bytes).expect("body should decode");
        assert!(body.get("error").is_none(), "unexpected error body: {body}");
        let parts = body["candidates"][0]["content"]["parts"]
            .as_array()
            .expect("gemini parts");
        assert!(parts.iter().any(|part| part["text"] == "Hi there, friend"));
        let text = String::from_utf8_lossy(&body_bytes);
        assert!(!text.contains("opaque-xai-reasoning") && !text.contains("response.created"));
    }

    #[tokio::test]
    async fn local_core_sync_finalize_never_wraps_unconvertible_success_sse_as_client_error() {
        use base64::Engine as _;

        // A complete stream whose output the Gemini client cannot represent.
        let raw_sse = concat!(
            "event: response.output_item.done\n",
            "data: {\"type\":\"response.output_item.done\",\"output_index\":0,\"item\":{\"id\":\"future_item_123\",\"type\":\"future_output\",\"payload\":\"must-not-drop\"}}\n\n",
            "event: response.completed\n",
            "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_raw_123\",\"object\":\"response\",\"status\":\"completed\",\"model\":\"grok-4.7\",\"output\":[]}}\n\n",
        );
        let mut payload = core_finalize_payload(
            "gemini_chat_sync_finalize",
            "gemini:generate_content",
            "openai:responses",
            200,
            json!(null),
        );
        payload.body_json = None;
        payload.body_base64 = Some(base64::engine::general_purpose::STANDARD.encode(raw_sse));
        payload.report_context = Some(json!({
            "client_api_format": "gemini:generate_content",
            "provider_api_format": "openai:responses",
            "provider_stream_event_api_format": "openai:responses",
            "needs_conversion": true,
        }));

        assert!(maybe_build_local_core_error_response(
            ResponseHeaderContext::none(),
            "trace-raw-success-sse",
            &test_decision(),
            &mut payload,
        )
        .await
        .expect("response build should not error")
        .is_none());

        let state = AppState::new().expect("state should build");
        let response = submit_local_core_error_or_sync_finalize(
            &state,
            &test_plan(),
            "trace-raw-success-sse",
            &test_decision(),
            payload,
        )
        .await
        .expect("finalize should build a response");

        assert_eq!(response.status(), http::StatusCode::BAD_GATEWAY);
        let body = serde_json::from_slice::<serde_json::Value>(
            &to_bytes(response.into_body(), usize::MAX)
                .await
                .expect("body should read"),
        )
        .expect("body should decode");
        let message = body["error"]["message"]
            .as_str()
            .expect("error message should exist");
        assert!(
            message.contains("could not be converted") && !message.contains("must-not-drop"),
            "unexpected message: {message}"
        );
    }

    #[tokio::test]
    async fn submit_local_core_finalize_keeps_http_200_for_success_image_body() {
        let payload = core_finalize_payload(
            "openai_image_sync_finalize",
            "openai:image",
            "openai:image",
            200,
            json!({
                "created": 1779273523,
                "data": [{
                    "b64_json": "aGVsbG8="
                }]
            }),
        );

        let state = AppState::new().expect("state should build");
        let response = submit_local_core_error_or_sync_finalize(
            &state,
            &test_plan(),
            "trace-image-success-200",
            &test_decision(),
            payload,
        )
        .await
        .expect("response should build");

        assert_eq!(response.status(), http::StatusCode::OK);
        let body: serde_json::Value = serde_json::from_slice(
            &to_bytes(response.into_body(), usize::MAX)
                .await
                .expect("body should read"),
        )
        .expect("body should decode");
        assert_eq!(body["data"][0]["b64_json"], "aGVsbG8=");
    }
}
