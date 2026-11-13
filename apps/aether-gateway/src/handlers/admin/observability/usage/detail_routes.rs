use super::analytics::admin_usage_api_key_names;
use super::analytics::admin_usage_provider_key_names;
use super::replay::{
    admin_usage_curl_headers, admin_usage_curl_url, admin_usage_headers_from_value,
    admin_usage_id_from_action_path, admin_usage_id_from_detail_path,
    admin_usage_resolve_body_value, admin_usage_resolve_request_capture_body,
    admin_usage_resolve_request_capture_body_for_item, build_admin_usage_curl_response,
    build_admin_usage_detail_payload, build_admin_usage_replay_response,
};
use super::summary_routes::{
    admin_usage_terminal_candidate_state_override, apply_admin_usage_state_override,
};
use crate::handlers::admin::request::{AdminAppState, AdminRequestContext};
use crate::handlers::admin::shared::{attach_admin_audit_response, query_param_bool};
use crate::GatewayError;
use aether_admin::observability::usage::{
    admin_usage_bad_request_response, admin_usage_data_unavailable_response,
    admin_usage_provider_key_name, ADMIN_USAGE_DATA_UNAVAILABLE_DETAIL,
};
use aether_data_contracts::repository::usage::{
    BodyEncoding, CapturedBody, StoredRequestUsageAudit, UsageBodyField, MAX_USAGE_BODY_BYTES,
};
use axum::{
    body::Body,
    http,
    response::{IntoResponse, Response},
    Json,
};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use tokio::try_join;

#[derive(Default)]
struct AdminUsageDetailBodyValue {
    value: Option<Value>,
    error_code: Option<&'static str>,
}

impl AdminUsageDetailBodyValue {
    fn resolved(
        item: &StoredRequestUsageAudit,
        field: UsageBodyField,
        value: Option<Value>,
    ) -> Self {
        // "available but absent" is the retention case: the ledger recorded a
        // capture, the payload table no longer holds it. That is an expired
        // body, not a missing one.
        let error_code =
            (value.is_none() && item.body_capture_result(field).available).then_some("expired");
        Self { value, error_code }
    }
}

fn resolve_admin_usage_detail_field(
    item: &StoredRequestUsageAudit,
    field: UsageBodyField,
    selected_field: Option<UsageBodyField>,
) -> AdminUsageDetailBodyValue {
    if selected_field.is_some_and(|selected| selected != field) {
        return AdminUsageDetailBodyValue::default();
    }
    if field == UsageBodyField::RequestBody {
        resolve_admin_usage_detail_request_body(item)
    } else {
        resolve_admin_usage_detail_body_value(item, field)
    }
}

fn resolve_admin_usage_detail_request_body(
    item: &StoredRequestUsageAudit,
) -> AdminUsageDetailBodyValue {
    let body = admin_usage_resolve_request_capture_body_for_item(item, None);
    AdminUsageDetailBodyValue::resolved(item, UsageBodyField::RequestBody, body)
}

fn resolve_admin_usage_detail_body_value(
    item: &StoredRequestUsageAudit,
    field: UsageBodyField,
) -> AdminUsageDetailBodyValue {
    let body = admin_usage_resolve_body_value(item, field);
    AdminUsageDetailBodyValue::resolved(item, field, body)
}

/// The stored bytes of one captured side, exactly as they arrived.
struct AdminUsageRawBody {
    bytes: Vec<u8>,
    encoding: BodyEncoding,
    truncated: bool,
}

enum AdminUsageRawBodyOutcome {
    Present(AdminUsageRawBody),
    /// The capture ledger says the side was captured, but the payload table no
    /// longer holds it: retention retired the bytes. Distinct from "never
    /// captured" so a viewer can say the body expired instead of guessing.
    Expired,
    /// The side was never captured, or capture was turned off for it.
    Absent,
}

fn read_admin_usage_raw_body(
    item: &StoredRequestUsageAudit,
    field: UsageBodyField,
) -> AdminUsageRawBodyOutcome {
    if let Some(CapturedBody::Bytes {
        bytes,
        encoding,
        truncated,
    }) = item.body(field)
    {
        return AdminUsageRawBodyOutcome::Present(AdminUsageRawBody {
            bytes: bytes.clone(),
            encoding: *encoding,
            truncated: *truncated,
        });
    }
    // No bytes in the row: the ledger decides between "never captured" and
    // "captured once, since swept". Inline and truncated ledger states describe
    // sides whose bytes used to exist.
    if item.body_capture_result(field).available {
        return AdminUsageRawBodyOutcome::Expired;
    }
    AdminUsageRawBodyOutcome::Absent
}

fn build_admin_usage_raw_body_response(
    item: &StoredRequestUsageAudit,
    field: UsageBodyField,
) -> Response<Body> {
    let result = read_admin_usage_raw_body(item, field);
    let payload_truncated = matches!(
        &result,
        AdminUsageRawBodyOutcome::Present(payload) if payload.truncated
    );
    let mut response = match result {
        AdminUsageRawBodyOutcome::Present(payload) => admin_usage_raw_payload_response(payload),
        AdminUsageRawBodyOutcome::Expired => {
            admin_usage_raw_body_error(http::StatusCode::GONE, "expired")
        }
        AdminUsageRawBodyOutcome::Absent => {
            admin_usage_raw_body_error(http::StatusCode::NOT_FOUND, "missing")
        }
    };
    let headers = response.headers_mut();
    headers.insert(
        http::header::CACHE_CONTROL,
        http::HeaderValue::from_static("no-store, no-transform"),
    );
    headers.insert(
        "x-content-type-options",
        http::HeaderValue::from_static("nosniff"),
    );
    headers.insert(
        "x-aether-body-field",
        http::HeaderValue::from_static(field.as_storage_field()),
    );
    if let Ok(value) = http::HeaderValue::from_str(&item.id) {
        headers.insert("x-aether-usage-id", value);
    }
    if payload_truncated {
        // The capture stopped early; the viewer can say so before parsing
        // the bytes instead of presenting a cut stream as complete.
        headers.insert(
            "x-aether-body-truncated",
            http::HeaderValue::from_static("true"),
        );
    }
    attach_admin_audit_response(
        response,
        "admin_usage_detail_viewed",
        "view_usage_detail",
        "usage_record",
        &item.id,
    )
}

fn admin_usage_raw_payload_response(payload: AdminUsageRawBody) -> Response<Body> {
    if payload.bytes.len() > MAX_USAGE_BODY_BYTES {
        return admin_usage_raw_body_error(http::StatusCode::PAYLOAD_TOO_LARGE, "too_large");
    }
    // The label is whatever the bytes really are, so a reader can parse them
    // without guessing: JSON stays JSON, an SSE stream stays an SSE stream.
    let content_type = match payload.encoding {
        BodyEncoding::Json => "application/json",
        BodyEncoding::Sse => "text/event-stream",
        BodyEncoding::Text => "text/plain; charset=utf-8",
        BodyEncoding::Binary => "application/octet-stream",
    };
    let body_size = payload.bytes.len();
    let mut response = (
        [
            ("content-type", content_type),
            ("content-encoding", "identity"),
            ("x-aether-body-encoding", payload.encoding.as_str()),
        ],
        payload.bytes,
    )
        .into_response();
    // Proxies may drop `Content-Length`; the reader uses this header instead.
    if let Ok(value) = http::HeaderValue::from_str(&body_size.to_string()) {
        response.headers_mut().insert("x-aether-body-size", value);
    }
    response
}

fn admin_usage_raw_body_error(status: http::StatusCode, code: &'static str) -> Response<Body> {
    (
        status,
        [("x-aether-body-error", code)],
        Json(json!({ "body_load_error_code": code })),
    )
        .into_response()
}

pub(super) async fn maybe_build_local_admin_usage_detail_response(
    state: &AdminAppState<'_>,
    request_context: &AdminRequestContext<'_>,
    request_body: Option<&axum::body::Bytes>,
) -> Result<Option<Response<Body>>, GatewayError> {
    let route_kind = request_context
        .control_decision
        .as_ref()
        .and_then(|decision| decision.route_kind.as_deref());

    match route_kind {
        Some("curl")
            if request_context.request_method == http::Method::GET
                && request_context
                    .request_path
                    .starts_with("/api/admin/usage/")
                && request_context.request_path.ends_with("/curl") =>
        {
            if !state.has_usage_data_reader() {
                return Ok(Some(admin_usage_data_unavailable_response(
                    ADMIN_USAGE_DATA_UNAVAILABLE_DETAIL,
                )));
            }

            let Some(usage_id) =
                admin_usage_id_from_action_path(&request_context.request_path, "/curl")
            else {
                return Ok(Some(admin_usage_bad_request_response("usage_id 无效")));
            };

            let Some(item) = state.find_request_usage_by_id(&usage_id).await? else {
                return Ok(Some(
                    (
                        http::StatusCode::NOT_FOUND,
                        Json(json!({ "detail": "Usage record not found" })),
                    )
                        .into_response(),
                ));
            };

            let endpoint = if let Some(endpoint_id) = item.provider_endpoint_id.as_ref() {
                state
                    .read_provider_catalog_endpoints_by_ids(std::slice::from_ref(endpoint_id))
                    .await?
                    .into_iter()
                    .next()
            } else {
                None
            };
            let url = endpoint
                .as_ref()
                .map(|endpoint| admin_usage_curl_url(state, endpoint, &item));
            let headers_json = item
                .provider_request_headers
                .clone()
                .or_else(|| item.request_headers.clone());
            let headers = headers_json
                .as_ref()
                .and_then(admin_usage_headers_from_value)
                .filter(|headers| !headers.is_empty())
                .unwrap_or_else(admin_usage_curl_headers);
            let provider_request_body =
                admin_usage_resolve_body_value(&item, UsageBodyField::ProviderRequestBody);
            let request_body = admin_usage_resolve_body_value(&item, UsageBodyField::RequestBody);
            let body = provider_request_body
                .or(request_body)
                .or_else(|| admin_usage_resolve_request_capture_body(&item, None));
            return Ok(Some(attach_admin_audit_response(
                build_admin_usage_curl_response(&item, url, headers_json, &headers, body.as_ref()),
                "admin_usage_curl_viewed",
                "view_usage_curl_replay",
                "usage_record",
                &item.id,
            )));
        }
        Some("replay") => {
            let mut response =
                build_admin_usage_replay_response(state, request_context, request_body).await?;
            if response.status().is_success() {
                if let Some(usage_id) =
                    admin_usage_id_from_action_path(&request_context.request_path, "/replay")
                {
                    response = attach_admin_audit_response(
                        response,
                        "admin_usage_replay_plan_generated",
                        "generate_usage_replay_plan",
                        "usage_record",
                        &usage_id,
                    );
                }
            }
            return Ok(Some(response));
        }
        Some("detail")
            if request_context.request_method == http::Method::GET
                && request_context
                    .request_path
                    .starts_with("/api/admin/usage/") =>
        {
            if !state.has_usage_data_reader() {
                return Ok(Some(admin_usage_data_unavailable_response(
                    ADMIN_USAGE_DATA_UNAVAILABLE_DETAIL,
                )));
            }

            let Some(usage_id) = admin_usage_id_from_detail_path(&request_context.request_path)
            else {
                return Ok(Some(admin_usage_bad_request_response("usage_id 无效")));
            };
            let include_bodies = query_param_bool(
                request_context.request_query_string.as_deref(),
                "include_bodies",
                true,
            );
            let body_field =
                match request_context
                    .request_query_string
                    .as_deref()
                    .and_then(|query| {
                        url::form_urlencoded::parse(query.as_bytes())
                            .find(|(key, _)| key == "body_field")
                            .map(|(_, value)| value.into_owned())
                    }) {
                    Some(value) => {
                        match UsageBodyField::from_storage_field(value.trim()) {
                            Some(field) if include_bodies => Some(field),
                            _ => return Ok(Some(admin_usage_bad_request_response(
                                "body_field 必须是有效的正文字段，且 include_bodies 必须为 true",
                            ))),
                        }
                    }
                    None => None,
                };

            let Some(item) = state.find_request_usage_by_id(&usage_id).await? else {
                return Ok(Some(
                    (
                        http::StatusCode::NOT_FOUND,
                        Json(json!({ "detail": "Usage record not found" })),
                    )
                        .into_response(),
                ));
            };

            let body_format = request_context
                .request_query_string
                .as_deref()
                .and_then(|query| {
                    url::form_urlencoded::parse(query.as_bytes())
                        .find(|(key, _)| key == "body_format")
                        .map(|(_, value)| value.into_owned())
                });
            if let Some(format) = body_format {
                if format != "raw" || body_field.is_none() {
                    return Ok(Some(admin_usage_bad_request_response(
                        "body_format=raw 必须指定 body_field",
                    )));
                }
                if let Some(field) = body_field {
                    return Ok(Some(build_admin_usage_raw_body_response(&item, field)));
                }
            }

            let user_ids = item.user_id.clone().into_iter().collect::<Vec<_>>();
            let (users_by_id, provider_key_names, api_key_names): (
                BTreeMap<String, aether_data::repository::users::StoredUserSummary>,
                BTreeMap<String, String>,
                BTreeMap<String, String>,
            ) = try_join!(
                state.resolve_auth_user_summaries_by_ids(&user_ids),
                admin_usage_provider_key_names(state, std::slice::from_ref(&item)),
                admin_usage_api_key_names(state, std::slice::from_ref(&item)),
            )?;
            let provider_key_name = admin_usage_provider_key_name(&item, &provider_key_names);

            let mut detail_item = item.clone();
            if matches!(detail_item.status.as_str(), "pending" | "streaming")
                && state.has_request_candidate_data_reader()
            {
                let candidates = state
                    .app()
                    .read_request_candidates_by_request_id(&detail_item.request_id)
                    .await?;
                if let Some(override_payload) =
                    admin_usage_terminal_candidate_state_override(&candidates)
                {
                    apply_admin_usage_state_override(&mut detail_item, &override_payload);
                }
            }
            let mut body_load_errors = serde_json::Map::new();
            let mut body_load_error_codes = serde_json::Map::new();
            let mut request_body = if include_bodies {
                let (request_body, provider_request_body, response_body, client_response_body) = (
                    resolve_admin_usage_detail_field(
                        &item,
                        UsageBodyField::RequestBody,
                        body_field,
                    ),
                    resolve_admin_usage_detail_field(
                        &item,
                        UsageBodyField::ProviderRequestBody,
                        body_field,
                    ),
                    resolve_admin_usage_detail_field(
                        &item,
                        UsageBodyField::ResponseBody,
                        body_field,
                    ),
                    resolve_admin_usage_detail_field(
                        &item,
                        UsageBodyField::ClientResponseBody,
                        body_field,
                    ),
                );
                for (field, resolved) in [
                    (UsageBodyField::RequestBody, &request_body),
                    (UsageBodyField::ProviderRequestBody, &provider_request_body),
                    (UsageBodyField::ResponseBody, &response_body),
                    (UsageBodyField::ClientResponseBody, &client_response_body),
                ] {
                    if let Some(error_code) = resolved.error_code {
                        body_load_errors.insert(field.as_storage_field().to_string(), json!(true));
                        body_load_error_codes
                            .insert(field.as_storage_field().to_string(), json!(error_code));
                    }
                }
                // The detail payload reports which sides exist; the bytes
                // themselves are always fetched through the `/body` route, so a
                // multi-megabyte body never travels inside the JSON view.
                request_body.value
            } else {
                None
            };
            let default_headers = admin_usage_curl_headers();
            let mut payload = build_admin_usage_detail_payload(
                &detail_item,
                &users_by_id,
                &api_key_names,
                state.has_auth_user_data_reader(),
                state.has_auth_api_key_data_reader(),
                provider_key_name.as_deref(),
                include_bodies && body_field.is_none(),
                if body_field.is_none() {
                    request_body.take()
                } else {
                    None
                },
                &default_headers,
            );
            if let Some(field) = body_field {
                payload[field.as_storage_field()] = Value::Null;
            }
            payload["body_load_errors"] = if include_bodies && !body_load_errors.is_empty() {
                Value::Object(body_load_errors)
            } else {
                Value::Null
            };
            payload["body_load_error_codes"] = if body_load_error_codes.is_empty() {
                Value::Null
            } else {
                Value::Object(body_load_error_codes)
            };

            return Ok(Some(attach_admin_audit_response(
                Json(payload).into_response(),
                "admin_usage_detail_viewed",
                "view_usage_detail",
                "usage_record",
                &item.id,
            )));
        }
        _ => {}
    }

    Ok(None)
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn admin_usage_raw_body_does_not_decode_or_reencode_stored_bytes() {
        use super::{admin_usage_raw_payload_response, AdminUsageRawBody};
        let response = admin_usage_raw_payload_response(AdminUsageRawBody {
            bytes: b"{ \"untouched\" : true }".to_vec(),
            encoding: aether_data_contracts::repository::usage::BodyEncoding::Json,
            truncated: false,
        });
        assert_eq!(response.headers()["content-encoding"], "identity");
        assert_eq!(response.headers()["x-aether-body-encoding"], "json");
        assert!(response.headers().get("x-aether-body-truncated").is_none());
        let bytes = axum::body::to_bytes(response.into_body(), 1024)
            .await
            .unwrap();
        assert_eq!(bytes.as_ref(), b"{ \"untouched\" : true }".as_slice());
    }
}
