use super::{
    build_api_format_health_monitor_payload, build_model_health_monitor_payload,
    build_public_auth_modules_status_payload, build_public_user_modules_status_payload,
    build_related_health_monitor_payload, ldap_module_config_is_valid,
    sanitize_public_model_capabilities, sanitize_public_model_config_for_user,
    sanitize_public_tiered_pricing, ApiFormatHealthMonitorOptions, HealthMonitorRelationDimension,
    ModelHealthMonitorOptions, PUBLIC_CAPABILITY_DEFINITIONS,
};
use crate::control::GatewayPublicRequestContext;
use crate::handlers::shared::{
    decrypt_or_migrate_ldap_bind_password, decrypt_or_migrate_system_config_secret,
    escape_admin_email_template_html, module_available_from_env, query_param_optional_bool,
    query_param_value, read_admin_email_template_payload, render_admin_email_template_html,
    system_config_bool, system_config_string, unix_secs_to_rfc3339,
};
use crate::{AppState, GatewayError};
use axum::body::{Body, Bytes};
use axum::http::{self, Response};
use axum::response::IntoResponse;
use axum::Json;
use serde_json::json;

#[path = "support/announcements.rs"]
mod support_announcements;
#[path = "support/auth.rs"]
mod support_auth;
#[path = "support/auth_cookie_policy.rs"]
mod support_auth_cookie_policy;
#[path = "support/base_url.rs"]
mod support_base_url;
#[path = "support/billing.rs"]
mod support_billing;
#[path = "support/ccswitch.rs"]
mod support_ccswitch;
#[path = "support/dashboard.rs"]
mod support_dashboard;
#[path = "support/models.rs"]
mod support_models;
#[path = "support/monitoring.rs"]
mod support_monitoring;
#[path = "support/oauth.rs"]
mod support_oauth;
#[path = "support/payment.rs"]
mod support_payment;
#[path = "support/test_connection.rs"]
mod support_test_connection;
#[path = "support/user_me.rs"]
mod support_user_me;
#[path = "support/wallet.rs"]
mod support_wallet;

pub(crate) use self::support_announcements::maybe_build_local_admin_announcements_response;
#[cfg(test)]
pub(crate) use self::support_models::filter_eligible_model_rows;
pub(crate) use self::support_models::matches_model_mapping_for_models;

use self::support_announcements::{
    maybe_build_local_announcement_user_response, maybe_build_local_public_announcements_response,
};
use self::support_auth::auth_registration::{
    auth_password_policy_level, validate_auth_register_password,
};
use self::support_auth::auth_session::{
    build_auth_wallet_summary_payload, handle_auth_me, resolve_authenticated_local_user,
    AuthenticatedLocalUserContext,
};
pub(crate) use self::support_auth::enforce_admin_auth_ip_rate_limit;
use self::support_auth::{auth_email_is_verified, consume_auth_email_registration_proof};
use self::support_auth::{
    build_auth_error_response, build_auth_json_response, build_auth_refresh_cookie_clear_header,
    build_auth_registration_settings_payload, build_auth_settings_payload,
    extract_client_device_id, mark_sensitive_response_no_store, maybe_build_local_auth_response,
};
pub(crate) use self::support_base_url::base_url_from_request;
use self::support_billing::maybe_build_local_billing_response;
use self::support_ccswitch::maybe_build_local_ccswitch_response;
use self::support_dashboard::maybe_build_local_dashboard_response;
use self::support_models::{
    build_models_auth_error_response, maybe_build_local_models_response, models_api_format,
};
use self::support_monitoring::maybe_build_local_user_monitoring_response;
use self::support_oauth::maybe_build_local_oauth_response;
use self::support_payment::maybe_build_local_payment_callback_response;
use self::support_test_connection::maybe_build_local_test_connection_response;
use self::support_user_me::maybe_build_local_users_me_response;
#[cfg(test)]
pub(crate) use self::support_wallet::prepare_wallet_gateway_response_for_storage;
use self::support_wallet::{
    build_wallet_balance_payload_for_auth_scope, build_wallet_live_today_usage_payload_for_api_key,
    build_wallet_live_today_usage_payload_for_user, maybe_build_local_wallet_response,
    prepare_billing_gateway_response_for_storage, resolve_direct_gateway_channel,
    sanitize_wallet_gateway_response, wallet_normalize_optional_string_field,
    wallet_payment_instructions_from_checkout, wallet_payment_instructions_from_stored,
};

pub(crate) fn build_unhandled_public_support_response(
    request_context: &GatewayPublicRequestContext,
) -> Response<Body> {
    let decision = request_context
        .control_decision
        .as_ref()
        .expect("public support response requires control decision");
    let mut payload = serde_json::Map::new();
    payload.insert(
        "detail".to_string(),
        serde_json::Value::String(
            "public support route not implemented in rust frontdoor".to_string(),
        ),
    );
    if crate::handlers::shared::should_expose_internal_metadata(
        Some(decision),
        http::StatusCode::NOT_IMPLEMENTED,
    ) {
        payload.insert(
            "route_family".to_string(),
            decision
                .route_family
                .as_deref()
                .map_or(serde_json::Value::Null, |value| {
                    serde_json::Value::String(value.to_string())
                }),
        );
        payload.insert(
            "route_kind".to_string(),
            decision
                .route_kind
                .as_deref()
                .map_or(serde_json::Value::Null, |value| {
                    serde_json::Value::String(value.to_string())
                }),
        );
    }
    payload.insert(
        "request_path".to_string(),
        serde_json::Value::String(request_context.request_path.clone()),
    );
    (
        http::StatusCode::NOT_IMPLEMENTED,
        Json(serde_json::Value::Object(payload)),
    )
        .into_response()
}

pub(crate) async fn maybe_build_local_public_support_response(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
    headers: &http::HeaderMap,
    remote_addr: &std::net::SocketAddr,
    client_ip: std::net::IpAddr,
    request_body: Option<&Bytes>,
) -> Option<Response<Body>> {
    let response = build_local_public_support_response(
        state,
        request_context,
        headers,
        remote_addr,
        client_ip,
        request_body,
    )
    .await?;
    Some(support_auth_cookie_policy::finalize_refresh_cookie(
        response,
        headers,
        request_context.host_header.as_deref(),
        remote_addr,
    ))
}

async fn build_local_public_support_response(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
    headers: &http::HeaderMap,
    remote_addr: &std::net::SocketAddr,
    client_ip: std::net::IpAddr,
    request_body: Option<&Bytes>,
) -> Option<Response<Body>> {
    let decision = request_context.control_decision.as_ref()?;
    if decision.route_class.as_deref() != Some("public_support") {
        return None;
    }

    if decision.route_family.as_deref() == Some("auth") {
        return maybe_build_local_auth_response(
            state,
            request_context,
            headers,
            client_ip,
            request_body,
        )
        .await;
    }

    if decision.route_family.as_deref() == Some("oauth") {
        return maybe_build_local_oauth_response(
            state,
            request_context,
            headers,
            client_ip,
            request_body,
        )
        .await;
    }

    if decision.route_family.as_deref() == Some("dashboard") {
        return Some(maybe_build_local_dashboard_response(state, request_context, headers).await);
    }

    if decision.route_family.as_deref() == Some("monitoring_user") {
        return maybe_build_local_user_monitoring_response(state, request_context, headers).await;
    }

    if decision.route_family.as_deref() == Some("announcement_user") {
        return maybe_build_local_announcement_user_response(
            state,
            request_context,
            headers,
            request_body,
        )
        .await;
    }

    if decision.route_family.as_deref() == Some("wallet") {
        if let Some(response) = maybe_build_local_wallet_response(
            state,
            request_context,
            headers,
            client_ip,
            request_body,
        )
        .await
        {
            return Some(response);
        }
        return Some(build_unhandled_public_support_response(request_context));
    }

    if decision.route_family.as_deref() == Some("billing") {
        if let Some(response) = maybe_build_local_billing_response(
            state,
            request_context,
            headers,
            client_ip,
            request_body,
        )
        .await
        {
            return Some(response);
        }
        return Some(build_unhandled_public_support_response(request_context));
    }

    if decision.route_family.as_deref() == Some("ccswitch") {
        if let Some(response) = maybe_build_local_ccswitch_response(state, request_context).await {
            return Some(response);
        }
        return Some(build_unhandled_public_support_response(request_context));
    }

    if decision.route_family.as_deref() == Some("users_me") {
        return maybe_build_local_users_me_response(
            state,
            request_context,
            headers,
            remote_addr,
            request_body,
        )
        .await;
    }

    if decision.route_family.as_deref() == Some("payment_callback") {
        return maybe_build_local_payment_callback_response(
            state,
            request_context,
            headers,
            request_body,
        )
        .await;
    }

    if decision.route_family.as_deref() == Some("models") {
        if decision.auth_context.is_none() {
            return Some(build_models_auth_error_response(
                models_api_format(request_context).unwrap_or("openai:chat"),
            ));
        }
        if let Some(response) = maybe_build_local_models_response(state, request_context).await {
            return Some(response);
        }
    }

    if decision.route_family.as_deref() == Some("auth_public") {
        if decision.route_kind.as_deref() == Some("registration_settings")
            && request_context.request_path == "/api/auth/registration-settings"
        {
            let payload = build_auth_registration_settings_payload(state).await.ok()?;
            return Some(Json(payload).into_response());
        }

        if decision.route_kind.as_deref() == Some("settings")
            && request_context.request_path == "/api/auth/settings"
        {
            let payload = build_auth_settings_payload(state).await.ok()?;
            return Some(Json(payload).into_response());
        }
    }

    if decision.route_family.as_deref() == Some("announcements") {
        return maybe_build_local_public_announcements_response(state, request_context, headers)
            .await;
    }

    if decision.route_family.as_deref() == Some("public_catalog") {
        if matches!(
            decision.route_kind.as_deref(),
            Some("health_api_formats" | "health_models" | "health_related")
        ) {
            if let Err(response) =
                resolve_authenticated_local_user(state, request_context, headers).await
            {
                return Some(response.into());
            }
        }

        if decision.route_kind.as_deref() == Some("site_info")
            && request_context.request_path == "/api/public/site-info"
        {
            let site_name = state
                .read_system_config_json_value("site_name")
                .await
                .ok()
                .flatten()
                .and_then(|value| value.as_str().map(ToOwned::to_owned))
                .unwrap_or_else(|| "Aether".to_string());
            let site_subtitle = state
                .read_system_config_json_value("site_subtitle")
                .await
                .ok()
                .flatten()
                .and_then(|value| value.as_str().map(ToOwned::to_owned))
                .unwrap_or_else(|| "AI Gateway".to_string());
            return Some(
                Json(json!({
                    "site_name": site_name,
                    "site_subtitle": site_subtitle,
                }))
                .into_response(),
            );
        }

        if decision.route_kind.as_deref() == Some("health_api_formats")
            && request_context.request_path == "/api/public/health/api-formats"
        {
            let lookback_hours = query_param_value(
                request_context.request_query_string.as_deref(),
                "lookback_hours",
            )
            .and_then(|value| value.parse::<u64>().ok())
            .filter(|value| (1..=168).contains(value))
            .unwrap_or(6);
            let per_format_limit = query_param_value(
                request_context.request_query_string.as_deref(),
                "per_format_limit",
            )
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|value| (10..=500).contains(value))
            .unwrap_or(100);
            let payload = build_api_format_health_monitor_payload(
                state,
                lookback_hours,
                per_format_limit,
                ApiFormatHealthMonitorOptions {
                    include_api_path: true,
                    include_provider_count: false,
                    include_key_count: false,
                },
            )
            .await?;
            return Some(Json(payload).into_response());
        }

        if decision.route_kind.as_deref() == Some("health_models")
            && request_context.request_path == "/api/public/health/models"
        {
            let lookback_hours = query_param_value(
                request_context.request_query_string.as_deref(),
                "lookback_hours",
            )
            .and_then(|value| value.parse::<u64>().ok())
            .filter(|value| (1..=168).contains(value))
            .unwrap_or(6);
            let model_limit = query_param_value(
                request_context.request_query_string.as_deref(),
                "model_limit",
            )
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|value| (1..=50).contains(value))
            .unwrap_or(12);
            let per_model_limit = query_param_value(
                request_context.request_query_string.as_deref(),
                "per_model_limit",
            )
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|value| (10..=500).contains(value))
            .unwrap_or(100);
            let payload = build_model_health_monitor_payload(
                state,
                lookback_hours,
                model_limit,
                per_model_limit,
                ModelHealthMonitorOptions {
                    include_provider_count: false,
                },
            )
            .await?;
            return Some(Json(payload).into_response());
        }

        if decision.route_kind.as_deref() == Some("health_related")
            && request_context.request_path == "/api/public/health/related"
        {
            let Some(dimension) =
                query_param_value(request_context.request_query_string.as_deref(), "dimension")
                    .and_then(|value| HealthMonitorRelationDimension::parse(&value))
            else {
                return Some(
                    (
                        http::StatusCode::BAD_REQUEST,
                        Json(json!({ "detail": "dimension 必须是 endpoint 或 model" })),
                    )
                        .into_response(),
                );
            };
            if matches!(dimension, HealthMonitorRelationDimension::Provider) {
                return Some(
                    (
                        http::StatusCode::BAD_REQUEST,
                        Json(json!({ "detail": "公开健康监控不支持 provider 维度" })),
                    )
                        .into_response(),
                );
            }
            let Some(value) =
                query_param_value(request_context.request_query_string.as_deref(), "value")
                    .map(|value| value.trim().to_string())
                    .filter(|value| !value.is_empty())
            else {
                return Some(
                    (
                        http::StatusCode::BAD_REQUEST,
                        Json(json!({ "detail": "value 不能为空" })),
                    )
                        .into_response(),
                );
            };
            let lookback_hours = query_param_value(
                request_context.request_query_string.as_deref(),
                "lookback_hours",
            )
            .and_then(|value| value.parse::<u64>().ok())
            .filter(|value| (1..=168).contains(value))
            .unwrap_or(6);
            let related_limit = query_param_value(
                request_context.request_query_string.as_deref(),
                "related_limit",
            )
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|value| (1..=50).contains(value))
            .unwrap_or(8);
            let per_item_limit = query_param_value(
                request_context.request_query_string.as_deref(),
                "per_item_limit",
            )
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|value| (10..=500).contains(value))
            .unwrap_or(100);
            let payload = build_related_health_monitor_payload(
                state,
                lookback_hours,
                dimension,
                &value,
                related_limit,
                per_item_limit,
                false,
            )
            .await?;
            return Some(Json(payload).into_response());
        }
    }

    if decision.route_family.as_deref() == Some("modules") {
        if decision.route_kind.as_deref() == Some("user_status")
            && request_context.request_path == "/api/modules/user-status"
        {
            return Some(
                match build_public_user_modules_status_payload(state).await {
                    Ok(payload) => Json(payload).into_response(),
                    Err(error) => error.into_response(),
                },
            );
        }
        if decision.route_kind.as_deref() == Some("auth_status")
            && request_context.request_path == "/api/modules/auth-status"
        {
            let payload = build_public_auth_modules_status_payload(state).await.ok()?;
            return Some(Json(payload).into_response());
        }
    }

    if decision.route_family.as_deref() == Some("system_catalog") {
        if decision.route_kind.as_deref() == Some("health")
            && request_context.request_path == "/health"
        {
            return Some(Json(json!({ "status": "healthy" })).into_response());
        }

        if decision.route_kind.as_deref() == Some("health")
            && request_context.request_path == "/v1/health"
        {
            return Some(Json(json!({ "status": "ok" })).into_response());
        }

        if decision.route_kind.as_deref() == Some("root") && request_context.request_path == "/" {
            return Some(Json(json!({ "status": "running" })).into_response());
        }

        if decision.route_kind.as_deref() == Some("test_connection")
            && request_context.request_path == "/v1/test-connection"
        {
            return maybe_build_local_test_connection_response(state, request_context, headers)
                .await;
        }

        if decision.route_kind.as_deref() == Some("test_connection")
            && request_context.request_path == "/test-connection"
        {
            return Some(
                (
                    http::StatusCode::GONE,
                    Json(json!({
                        "detail": "Deprecated endpoint. Please use /v1/test-connection.",
                    })),
                )
                    .into_response(),
            );
        }
    }

    None
}
