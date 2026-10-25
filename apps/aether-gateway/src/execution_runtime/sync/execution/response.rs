use std::collections::BTreeMap;

use aether_contracts::ExecutionPlan;
use axum::body::Body;
use axum::http::Response;
use serde_json::json;

use crate::api::response::build_client_response_from_parts;
use crate::control::GatewayControlDecision;
use crate::video_tasks::{
    build_local_sync_finalize_read_response, LocalVideoTaskSnapshot, VideoTaskSyncReportMode,
};
pub(crate) use crate::video_tasks::{
    resolve_local_sync_error_background_report_kind,
    resolve_local_sync_success_background_report_kind,
};
use crate::{usage::GatewaySyncReportRequest, AppState, GatewayError};

pub(crate) enum LocalVideoSyncSuccessBuild {
    Handled(LocalVideoSyncSuccessOutcome),
    NotHandled(GatewaySyncReportRequest),
}

pub(crate) struct LocalVideoSyncSuccessOutcome {
    pub(crate) response: Response<Body>,
    pub(crate) report_payload: GatewaySyncReportRequest,
    pub(crate) original_report_context: Option<serde_json::Value>,
    pub(crate) report_mode: VideoTaskSyncReportMode,
    pub(crate) local_task_snapshot: Option<LocalVideoTaskSnapshot>,
}

fn cloned_report_context_object(
    payload: &GatewaySyncReportRequest,
) -> serde_json::Map<String, serde_json::Value> {
    payload
        .report_context
        .as_ref()
        .and_then(serde_json::Value::as_object)
        .cloned()
        .unwrap_or_default()
}

async fn build_local_video_response(
    state: &AppState,
    plan: &ExecutionPlan,
    trace_id: &str,
    decision: &GatewayControlDecision,
    payload: &mut GatewaySyncReportRequest,
    status_code: u16,
    body_json: &serde_json::Value,
) -> Result<Response<Body>, GatewayError> {
    let body_bytes =
        serde_json::to_vec(body_json).map_err(|err| GatewayError::Internal(err.to_string()))?;
    // Rebuild representation headers from the local JSON body, then apply the
    // same header pipeline used by upstream responses and preserved errors.
    let headers = crate::execution_runtime::prepare_client_response_headers(
        state,
        Some(plan),
        BTreeMap::from([("content-type".to_string(), "application/json".to_string())]),
        crate::execution_runtime::ClientResponseBodyMode::RebuiltJson,
        Some(body_json),
    )
    .await?;
    let response = build_client_response_from_parts(
        status_code,
        &headers,
        Body::from(body_bytes),
        trace_id,
        Some(decision),
    )?;
    payload.client_headers = Some(headers);
    payload.client_body_json = Some(body_json.clone());
    Ok(response)
}

pub(crate) async fn maybe_build_local_video_success_outcome(
    state: &AppState,
    trace_id: &str,
    decision: &GatewayControlDecision,
    mut payload: GatewaySyncReportRequest,
    plan: &ExecutionPlan,
) -> Result<LocalVideoSyncSuccessBuild, GatewayError> {
    if payload.status_code >= 400 {
        return Ok(LocalVideoSyncSuccessBuild::NotHandled(payload));
    }

    let report_context = cloned_report_context_object(&payload);
    let prepared_plan = {
        let provider_body = match payload
            .body_json
            .as_ref()
            .and_then(serde_json::Value::as_object)
        {
            Some(value) => value,
            None => return Ok(LocalVideoSyncSuccessBuild::NotHandled(payload)),
        };
        state.video_tasks.prepare_sync_success(
            payload.report_kind.as_str(),
            provider_body,
            &report_context,
            plan,
        )
    };
    let Some(success_plan) = prepared_plan else {
        return Ok(LocalVideoSyncSuccessBuild::NotHandled(payload));
    };
    let client_body_json = success_plan.client_body_json();
    let response = build_local_video_response(
        state,
        plan,
        trace_id,
        decision,
        &mut payload,
        http::StatusCode::OK.as_u16(),
        &client_body_json,
    )
    .await?;
    let mut report_context = cloned_report_context_object(&payload);
    let original_report_context = payload.report_context.take();
    success_plan.apply_to_report_context(&mut report_context);
    payload.report_kind = success_plan.success_report_kind().to_string();
    payload.report_context = Some(serde_json::Value::Object(report_context));

    Ok(LocalVideoSyncSuccessBuild::Handled(
        LocalVideoSyncSuccessOutcome {
            response,
            report_payload: payload,
            original_report_context,
            report_mode: success_plan.report_mode(),
            local_task_snapshot: matches!(
                success_plan.report_mode(),
                VideoTaskSyncReportMode::Background
            )
            .then(|| success_plan.to_snapshot()),
        },
    ))
}

pub(crate) async fn maybe_build_local_sync_finalize_response(
    state: &AppState,
    plan: &ExecutionPlan,
    trace_id: &str,
    decision: &GatewayControlDecision,
    payload: &mut GatewaySyncReportRequest,
) -> Result<Option<Response<Body>>, GatewayError> {
    let Some(read_response) = build_local_sync_finalize_read_response(
        payload.report_kind.as_str(),
        payload.status_code,
        payload.report_context.as_ref(),
    ) else {
        return Ok(None);
    };

    Ok(Some(
        build_local_video_response(
            state,
            plan,
            trace_id,
            decision,
            payload,
            read_response.status_code,
            &read_response.body_json,
        )
        .await?,
    ))
}

pub(crate) async fn maybe_build_local_video_error_response(
    state: &AppState,
    plan: &ExecutionPlan,
    trace_id: &str,
    decision: &GatewayControlDecision,
    payload: &mut GatewaySyncReportRequest,
) -> Result<Option<Response<Body>>, GatewayError> {
    if resolve_local_sync_error_background_report_kind(payload.report_kind.as_str()).is_none() {
        return Ok(None);
    }

    if payload.status_code < 400 {
        return Ok(None);
    }

    let response_body =
        local_video_error_response_body(payload.report_kind.as_str(), payload.status_code);
    Ok(Some(
        build_local_video_response(
            state,
            plan,
            trace_id,
            decision,
            payload,
            payload.status_code,
            &response_body,
        )
        .await?,
    ))
}

fn local_video_error_response_body(report_kind: &str, status_code: u16) -> serde_json::Value {
    let (code, gemini_status) = match status_code {
        400 => ("invalid_request", "INVALID_ARGUMENT"),
        401 => ("authentication_error", "UNAUTHENTICATED"),
        403 => ("permission_denied", "PERMISSION_DENIED"),
        404 => ("not_found", "NOT_FOUND"),
        429 => ("rate_limit_exceeded", "RESOURCE_EXHAUSTED"),
        503 => ("server_error", "UNAVAILABLE"),
        500..=599 => ("server_error", "INTERNAL"),
        _ => ("provider_error", "UNKNOWN"),
    };

    if report_kind.starts_with("gemini_video_") {
        json!({
            "error": {
                "code": status_code,
                "message": "Video generation failed",
                "status": gemini_status,
            }
        })
    } else {
        json!({
            "error": {
                "message": "Video generation failed",
                "type": code,
                "code": code,
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use axum::body::to_bytes;
    use serde_json::json;

    fn test_plan() -> ExecutionPlan {
        aether_video_tasks_core::build_internal_finalize_video_plan(
            "trace-video-rules",
            "openai:video",
            None,
        )
        .expect("video plan should build")
    }

    fn state_with_response_rules(plan: &ExecutionPlan) -> AppState {
        use aether_data::repository::provider_catalog::InMemoryProviderCatalogReadRepository;
        use aether_data_contracts::repository::provider_catalog::StoredProviderCatalogEndpoint;
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
            {"action": "set", "key": "x-marker", "value": "configured"},
            {"action": "rename", "from": "x-marker", "to": "x-video-marker"},
            {"action": "set", "key": "x-removed", "value": "remove-me"},
            {"action": "drop", "key": "x-removed"},
            {"action": "set", "key": "x-local-body", "value": "yes", "condition": {
                "source": "body", "path": "deleted", "op": "eq", "value": true
            }},
            {"action": "set", "key": "set-cookie", "value": "blocked=true"}
        ]}));
        AppState::new()
            .expect("state should build")
            .with_data_state_for_tests(
                crate::data::GatewayDataState::with_provider_catalog_repository_for_tests(
                    std::sync::Arc::new(InMemoryProviderCatalogReadRepository::seed(
                        vec![],
                        vec![endpoint],
                        vec![],
                    )),
                ),
            )
    }

    fn assert_video_response_headers(
        response: &Response<Body>,
        payload: &GatewaySyncReportRequest,
    ) {
        assert_eq!(response.headers()["x-video-marker"], "configured");
        for name in [
            "x-marker",
            "x-removed",
            "x-upstream-debug",
            "content-encoding",
            "set-cookie",
        ] {
            assert!(
                response.headers().get(name).is_none(),
                "unexpected header: {name}"
            );
        }
        let capture = aether_usage_runtime::build_sync_terminal_usage_payload_seed(payload);
        assert_eq!(
            capture.client_response_headers.as_ref().unwrap()["x-video-marker"],
            "configured"
        );
        assert!(capture
            .client_response_headers
            .as_ref()
            .unwrap()
            .get("x-upstream-debug")
            .is_none());
        assert_eq!(
            capture.provider_response_headers.as_ref().unwrap()["x-upstream-debug"],
            "provider-only"
        );
    }

    #[tokio::test]
    async fn local_video_success_and_finalize_apply_response_rules_after_rebuilding() {
        let plan = test_plan();
        let state = state_with_response_rules(&plan);
        let decision = GatewayControlDecision::synthetic(
            "/v1/videos",
            Some("ai_public".to_string()),
            None,
            None,
            Some("openai:video".to_string()),
        );
        let payload = GatewaySyncReportRequest {
            trace_id: "trace-video-rules".to_string(),
            report_kind: "openai_video_create_sync_finalize".to_string(),
            report_context: Some(json!({"task_id": "local-task", "local_task_id": "local-task"})),
            status_code: 200,
            provider_headers: BTreeMap::from([
                ("content-encoding".to_string(), "gzip".to_string()),
                ("x-upstream-debug".to_string(), "provider-only".to_string()),
            ]),
            client_headers: None,
            body_json: Some(json!({"id": "provider-task"})),
            client_body_json: None,
            body_base64: None,
            telemetry: None,
        };
        let LocalVideoSyncSuccessBuild::Handled(outcome) = maybe_build_local_video_success_outcome(
            &state,
            "trace-video-rules",
            &decision,
            payload.clone(),
            &plan,
        )
        .await
        .expect("success response should build") else {
            panic!("video success should be handled");
        };
        assert_video_response_headers(&outcome.response, &outcome.report_payload);

        for kind in [
            "openai_video_delete_sync_finalize",
            "openai_video_cancel_sync_finalize",
            "gemini_video_cancel_sync_finalize",
        ] {
            let mut payload = payload.clone();
            payload.report_kind = kind.to_string();
            let response = maybe_build_local_sync_finalize_response(
                &state,
                &plan,
                "trace-video-rules",
                &decision,
                &mut payload,
            )
            .await
            .expect("finalize should build")
            .expect("finalize should match");
            assert_video_response_headers(&response, &payload);
            assert_eq!(
                response.headers().get("x-local-body").is_some(),
                kind == "openai_video_delete_sync_finalize"
            );
        }

        for kind in [
            "openai_video_create_sync_finalize",
            "gemini_video_create_sync_finalize",
        ] {
            let mut payload = payload.clone();
            payload.report_kind = kind.to_string();
            payload.status_code = 502;
            let response = maybe_build_local_video_error_response(
                &state,
                &plan,
                "trace-video-rules",
                &decision,
                &mut payload,
            )
            .await
            .expect("error should build")
            .expect("error should match");
            assert_video_response_headers(&response, &payload);
        }
    }

    #[tokio::test]
    async fn local_video_error_response_does_not_expose_upstream_payload_or_headers() {
        let decision = GatewayControlDecision::synthetic(
            "/v1/videos",
            Some("ai_public".to_string()),
            Some("openai".to_string()),
            Some("video".to_string()),
            Some("openai:video".to_string()),
        )
        .with_execution_runtime_candidate(true);
        let mut payload = GatewaySyncReportRequest {
            trace_id: "trace-payload".to_string(),
            report_kind: "openai_video_create_sync_finalize".to_string(),
            report_context: Some(json!({
                "request_id": "req_123",
            })),
            status_code: http::StatusCode::BAD_GATEWAY.as_u16(),
            provider_headers: BTreeMap::from([
                ("content-encoding".to_string(), "gzip".to_string()),
                ("content-length".to_string(), "999".to_string()),
                (
                    "x-upstream-debug".to_string(),
                    "Authorization: Bearer header-secret".to_string(),
                ),
            ]),
            client_headers: None,
            body_json: Some(json!({
                "error": {
                    "type": "video_backend_error",
                    "message": "Authorization: Bearer body-secret at https://internal.test/?key=secret",
                }
            })),
            client_body_json: None,
            body_base64: None,
            telemetry: None,
        };

        let response = maybe_build_local_video_error_response(
            &AppState::new().expect("state should build"),
            &test_plan(),
            "trace-response",
            &decision,
            &mut payload,
        )
        .await
        .expect("video error response should build")
        .expect("video error response should match local video error kinds");

        assert_eq!(response.status(), http::StatusCode::BAD_GATEWAY);
        assert_eq!(
            response
                .headers()
                .get(http::header::CONTENT_TYPE)
                .and_then(|value| value.to_str().ok()),
            Some("application/json")
        );
        assert_eq!(response.headers().get(http::header::CONTENT_ENCODING), None);
        assert_eq!(response.headers().get("x-upstream-debug"), None);
        assert_eq!(
            payload
                .provider_headers
                .get("content-encoding")
                .map(String::as_str),
            Some("gzip")
        );
        assert_eq!(
            payload
                .provider_headers
                .get("content-length")
                .map(String::as_str),
            Some("999")
        );

        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("response body should read");
        let response_body =
            serde_json::from_slice::<serde_json::Value>(&body).expect("response body should parse");
        assert_eq!(
            response_body,
            json!({
                "error": {
                    "message": "Video generation failed",
                    "type": "server_error",
                    "code": "server_error",
                }
            })
        );
        let encoded = response_body.to_string();
        for sensitive in ["Bearer", "body-secret", "header-secret", "internal.test"] {
            assert!(!encoded.contains(sensitive));
        }
        assert!(payload.body_json.is_some());
    }

    #[test]
    fn gemini_video_error_response_uses_fixed_schema() {
        assert_eq!(
            local_video_error_response_body("gemini_video_create_sync_finalize", 429),
            json!({
                "error": {
                    "code": 429,
                    "message": "Video generation failed",
                    "status": "RESOURCE_EXHAUSTED",
                }
            })
        );
    }
}
