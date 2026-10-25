use std::collections::BTreeMap;

use axum::body::Body;
use axum::http::Response;
use serde_json::Value;

pub(crate) use crate::ai_serving::api::{
    normalize_provider_private_response_value as unwrap_local_finalize_response_value,
    provider_private_response_allows_sync_finalize as local_finalize_allows_envelope,
};
use crate::ai_serving::{
    build_local_success_background_report as build_local_success_background_report_impl,
    build_local_success_conversion_background_report as build_local_success_conversion_background_report_impl,
    GatewayControlDecision, LocalSyncReportParts,
};
use crate::api::response::build_client_response_from_parts;
use crate::execution_runtime::{
    prepare_final_client_headers, ClientResponseBodyMode, ResponseHeaderContext,
};
use crate::{usage::GatewaySyncReportRequest, GatewayError};

pub(crate) struct LocalCoreSyncFinalizeOutcome {
    pub(crate) response: Response<Body>,
    pub(crate) client_headers: BTreeMap<String, String>,
    pub(crate) background_report: Option<GatewaySyncReportRequest>,
}

fn build_local_success_response(
    trace_id: &str,
    decision: &GatewayControlDecision,
    status_code: u16,
    body_bytes: Vec<u8>,
    headers: &BTreeMap<String, String>,
) -> Result<Response<Body>, GatewayError> {
    build_client_response_from_parts(
        status_code,
        headers,
        Body::from(body_bytes),
        trace_id,
        Some(decision),
    )
}

fn surface_report_parts_from_gateway(
    payload: &GatewaySyncReportRequest,
    client_headers: &BTreeMap<String, String>,
) -> LocalSyncReportParts {
    LocalSyncReportParts {
        trace_id: payload.trace_id.clone(),
        report_kind: payload.report_kind.clone(),
        report_context: payload.report_context.clone(),
        status_code: payload.status_code,
        provider_headers: payload.provider_headers.clone(),
        client_headers: Some(client_headers.clone()),
        body_json: payload.body_json.clone(),
        client_body_json: payload.client_body_json.clone(),
        body_base64: payload.body_base64.clone(),
    }
}

fn gateway_report_from_surface(
    source: &GatewaySyncReportRequest,
    report: LocalSyncReportParts,
) -> GatewaySyncReportRequest {
    GatewaySyncReportRequest {
        trace_id: report.trace_id,
        report_kind: report.report_kind,
        report_context: report.report_context,
        status_code: report.status_code,
        provider_headers: report.provider_headers,
        client_headers: report.client_headers,
        body_json: report.body_json,
        client_body_json: report.client_body_json,
        body_base64: report.body_base64,
        telemetry: source.telemetry.clone(),
    }
}

fn build_local_success_outcome_with_report(
    trace_id: &str,
    decision: &GatewayControlDecision,
    status_code: u16,
    body_bytes: Vec<u8>,
    client_headers: BTreeMap<String, String>,
    background_report: Option<GatewaySyncReportRequest>,
) -> Result<LocalCoreSyncFinalizeOutcome, GatewayError> {
    let response =
        build_local_success_response(trace_id, decision, status_code, body_bytes, &client_headers)?;
    Ok(LocalCoreSyncFinalizeOutcome {
        response,
        client_headers,
        background_report,
    })
}

pub(crate) async fn build_local_success_outcome(
    context: ResponseHeaderContext<'_>,
    trace_id: &str,
    decision: &GatewayControlDecision,
    payload: &GatewaySyncReportRequest,
    body_json: Value,
) -> Result<LocalCoreSyncFinalizeOutcome, GatewayError> {
    let client_headers = prepare_final_client_headers(
        context,
        &payload.provider_headers,
        payload.client_headers.as_ref(),
        ClientResponseBodyMode::RebuiltJson,
        Some(&body_json),
    )
    .await?;
    let body_bytes =
        serde_json::to_vec(&body_json).map_err(|err| GatewayError::Internal(err.to_string()))?;
    let surface_payload = surface_report_parts_from_gateway(payload, &client_headers);
    let background_report = build_local_success_background_report_impl(
        &surface_payload,
        body_json,
        payload.provider_headers.clone(),
        client_headers.clone(),
    )
    .map(|report| gateway_report_from_surface(payload, report));
    build_local_success_outcome_with_report(
        trace_id,
        decision,
        payload.status_code,
        body_bytes,
        client_headers,
        background_report,
    )
}

pub(crate) async fn build_local_success_outcome_with_conversion_report(
    context: ResponseHeaderContext<'_>,
    trace_id: &str,
    decision: &GatewayControlDecision,
    payload: &GatewaySyncReportRequest,
    client_body_json: Value,
    provider_body_json: Value,
) -> Result<LocalCoreSyncFinalizeOutcome, GatewayError> {
    let client_headers = prepare_final_client_headers(
        context,
        &payload.provider_headers,
        payload.client_headers.as_ref(),
        ClientResponseBodyMode::RebuiltJson,
        Some(&client_body_json),
    )
    .await?;
    let body_bytes = serde_json::to_vec(&client_body_json)
        .map_err(|err| GatewayError::Internal(err.to_string()))?;
    let surface_payload = surface_report_parts_from_gateway(payload, &client_headers);
    let report_payload = build_local_success_conversion_background_report_impl(
        &surface_payload,
        client_body_json,
        provider_body_json,
    )
    .map(|report| gateway_report_from_surface(payload, report));

    build_local_success_outcome_with_report(
        trace_id,
        decision,
        payload.status_code,
        body_bytes,
        client_headers,
        report_payload,
    )
}
