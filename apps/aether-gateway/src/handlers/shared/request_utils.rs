#[cfg(test)]
use crate::control::GatewayControlDecision;
use crate::control::GatewayPublicRequestContext;
use axum::http::{self};
use chrono::{SecondsFormat, Utc};
use url::form_urlencoded;

use super::OFFICIAL_EXTERNAL_MODEL_PROVIDERS;

#[cfg(test)]
pub(crate) fn rust_auth_terminates_provider_credentials(
    decision: Option<&GatewayControlDecision>,
) -> bool {
    decision.is_some_and(|decision| {
        decision.route_class.as_deref() == Some("ai_public") && decision.auth_context.is_some()
    })
}

pub(crate) fn mark_external_models_official_providers(
    value: &serde_json::Value,
) -> Option<serde_json::Value> {
    let providers = value.as_object()?;
    Some(serde_json::Value::Object(
        providers
            .iter()
            .map(|(provider_id, provider_value)| {
                let updated = match provider_value.as_object() {
                    Some(object) => {
                        let mut cloned = object.clone();
                        cloned.insert(
                            "official".to_string(),
                            serde_json::Value::Bool(
                                OFFICIAL_EXTERNAL_MODEL_PROVIDERS
                                    .iter()
                                    .any(|value| value == provider_id),
                            ),
                        );
                        serde_json::Value::Object(cloned)
                    }
                    None => provider_value.clone(),
                };
                (provider_id.clone(), updated)
            })
            .collect(),
    ))
}

#[cfg(test)]
pub(crate) fn sanitize_upstream_path_and_query(
    decision: Option<&GatewayControlDecision>,
    default_path_and_query: &str,
) -> String {
    let base = decision
        .map(GatewayControlDecision::proxy_path_and_query)
        .unwrap_or_else(|| default_path_and_query.to_string());
    let Some(decision) = decision else {
        return base;
    };
    if !rust_auth_terminates_provider_credentials(Some(decision)) {
        return base;
    }

    strip_query_param(&base, "key")
}

pub(crate) fn security_log_url_origin(value: &str) -> String {
    let Ok(parsed) = url::Url::parse(value.trim()) else {
        return "-".to_string();
    };
    if !matches!(parsed.scheme(), "http" | "https") {
        return "-".to_string();
    }
    let Some(host) = parsed.host_str() else {
        return "-".to_string();
    };
    match parsed.port() {
        Some(port) => format!("{}://{host}:{port}", parsed.scheme()),
        None => format!("{}://{host}", parsed.scheme()),
    }
}

#[cfg(test)]
pub(crate) fn strip_query_param(path_and_query: &str, key_to_strip: &str) -> String {
    let Some((path, query)) = path_and_query.split_once('?') else {
        return path_and_query.to_string();
    };

    let mut serializer = form_urlencoded::Serializer::new(String::new());
    let mut kept_any = false;
    for (key, value) in form_urlencoded::parse(query.as_bytes()) {
        if key == key_to_strip {
            continue;
        }
        serializer.append_pair(key.as_ref(), value.as_ref());
        kept_any = true;
    }

    if !kept_any {
        return path.to_string();
    }

    format!("{path}?{}", serializer.finish())
}

pub(crate) fn query_param_bool(query: Option<&str>, key: &str, default: bool) -> bool {
    let Some(query) = query else {
        return default;
    };
    for (entry_key, value) in form_urlencoded::parse(query.as_bytes()) {
        if entry_key == key {
            let normalized = value.trim().to_ascii_lowercase();
            return matches!(normalized.as_str(), "1" | "true" | "yes" | "on");
        }
    }
    default
}

pub(crate) fn query_param_optional_bool(query: Option<&str>, key: &str) -> Option<bool> {
    let query = query?;
    for (entry_key, value) in form_urlencoded::parse(query.as_bytes()) {
        if entry_key == key {
            let normalized = value.trim().to_ascii_lowercase();
            return match normalized.as_str() {
                "1" | "true" | "yes" | "on" => Some(true),
                "0" | "false" | "no" | "off" => Some(false),
                _ => None,
            };
        }
    }
    None
}

pub(crate) fn query_param_value(query: Option<&str>, key: &str) -> Option<String> {
    let query = query?;
    for (entry_key, value) in form_urlencoded::parse(query.as_bytes()) {
        if entry_key == key {
            let value = value.trim();
            if !value.is_empty() {
                return Some(value.to_string());
            }
        }
    }
    None
}

pub(crate) fn unix_secs_to_rfc3339(unix_secs: u64) -> Option<String> {
    let timestamp = i64::try_from(unix_secs).ok()?;
    Some(
        chrono::DateTime::<Utc>::from_timestamp(timestamp, 0)?
            .to_rfc3339_opts(SecondsFormat::Secs, true),
    )
}

pub(crate) fn unix_ms_to_rfc3339(unix_ms: u64) -> Option<String> {
    let secs = i64::try_from(unix_ms / 1000).ok()?;
    let nanos = ((unix_ms % 1000) * 1_000_000) as u32;
    Some(
        chrono::DateTime::<Utc>::from_timestamp(secs, nanos)?
            .to_rfc3339_opts(SecondsFormat::Millis, true),
    )
}

pub(crate) fn json_string_list(value: Option<&serde_json::Value>) -> Vec<String> {
    value
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(serde_json::Value::as_str)
        .map(ToOwned::to_owned)
        .collect()
}

/// Whether an admin request body has to be buffered before the handler runs.
///
/// The local admin handlers take `Option<&Bytes>`, so a body that was not buffered
/// is simply absent and the request fails validation with a generic 400 — a
/// symptom that points nowhere near the cause. This used to be a hand-maintained
/// allowlist of `(family, method, kind)` triples: every new write route that
/// forgot to register was broken, and the failure only showed up as "请求数据验证
/// 失败" at runtime.
///
/// Buffer every admin request that can carry a body instead, and keep an explicit
/// exemption list for the rare route that must stream. None exists today: the
/// multipart upload parses the buffered body, and the import routes are bounded by
/// the body buffer policy like any other admin payload.
pub(crate) fn admin_proxy_local_requires_buffered_body(
    request_context: &GatewayPublicRequestContext,
) -> bool {
    request_context
        .control_decision
        .as_ref()
        .is_some_and(|decision| {
            if decision.route_class.as_deref() != Some("admin_proxy") {
                return false;
            }
            if matches!(
                request_context.request_method,
                http::Method::GET | http::Method::HEAD | http::Method::OPTIONS
            ) {
                return false;
            }
            !admin_route_streams_request_body(
                decision.route_family.as_deref(),
                decision.route_kind.as_deref(),
            )
        })
}

/// Admin routes whose request body must stay a stream. Empty today; kept so a
/// future streaming endpoint is a deliberate, reviewable exception rather than a
/// silent regression of the default.
fn admin_route_streams_request_body(
    _route_family: Option<&str>,
    _route_kind: Option<&str>,
) -> bool {
    false
}

pub(crate) fn public_support_local_requires_buffered_body(
    request_context: &GatewayPublicRequestContext,
) -> bool {
    request_context
        .control_decision
        .as_ref()
        .is_some_and(|decision| {
            if decision.route_class.as_deref() != Some("public_support") {
                return false;
            }

            matches!(
                (
                    decision.route_family.as_deref(),
                    request_context.request_method.clone(),
                    decision.route_kind.as_deref(),
                ),
                (
                    Some("auth"),
                    http::Method::POST,
                    Some(
                        "login"
                            | "register"
                            | "send_verification_code"
                            | "verify_email"
                            | "verification_status"
                    ),
                ) | (
                    Some("users_me"),
                    http::Method::PUT,
                    Some(
                        "update_detail"
                            | "model_capabilities_update"
                            | "preferences_update"
                            | "api_key_update"
                            | "management_token_update"
                            | "api_key_providers_update"
                            | "api_key_capabilities_update",
                    ),
                ) | (
                    Some("users_me"),
                    http::Method::PATCH,
                    Some("password" | "session_update" | "api_key_patch"),
                ) | (
                    Some("users_me"),
                    http::Method::POST,
                    Some("api_keys_create" | "management_tokens_create",),
                ) | (
                    Some("wallet"),
                    http::Method::POST,
                    Some("create_refund" | "create_recharge_order" | "redeem"),
                ) | (Some("billing"), http::Method::POST, Some("plan_checkout"),)
                    | (
                        Some("payment_callback"),
                        http::Method::POST,
                        Some(
                            "callback"
                                | "epay_notify"
                                | "epay_return"
                                | "alipay_notify"
                                | "wxpay_notify"
                                | "stripe_webhook",
                        ),
                    )
            )
        })
}

pub(crate) fn local_proxy_route_requires_buffered_body(
    request_context: &GatewayPublicRequestContext,
) -> bool {
    admin_proxy_local_requires_buffered_body(request_context)
        || public_support_local_requires_buffered_body(request_context)
}

#[cfg(test)]
mod tests {
    use super::sanitize_upstream_path_and_query;
    use crate::control::{GatewayControlAuthContext, GatewayControlDecision};

    fn authenticated_ai_decision(
        route_family: &str,
        path_and_query: &str,
    ) -> GatewayControlDecision {
        let (path, query) = path_and_query
            .split_once('?')
            .map_or((path_and_query, None), |(path, query)| (path, Some(query)));
        let mut decision = GatewayControlDecision::synthetic(
            path,
            Some("ai_public".to_string()),
            Some(route_family.to_string()),
            Some("chat".to_string()),
            Some(format!("{route_family}:chat")),
        );
        decision.public_query_string = query.map(str::to_string);
        decision.auth_context = Some(GatewayControlAuthContext {
            user_id: "user-1".to_string(),
            api_key_id: "key-1".to_string(),
            username: None,
            api_key_name: None,
            balance_remaining: None,
            access_allowed: true,
            user_rate_limit: None,
            api_key_rate_limit: None,
            api_key_is_standalone: false,
            admin_bypass_limits: false,
            local_rejection: None,
            allowed_models: None,
            ip_rules: None,
            verified_api_key_hash: None,
        });
        decision
    }

    #[test]
    fn authenticated_ai_routes_strip_query_api_keys_across_formats() {
        for route_family in ["openai", "claude", "gemini"] {
            let decision = authenticated_ai_decision(
                route_family,
                "/v1/chat/completions?key=client-secret&stream=true",
            );
            assert_eq!(
                sanitize_upstream_path_and_query(
                    Some(&decision),
                    "/v1/chat/completions?key=client-secret&stream=true",
                ),
                "/v1/chat/completions?stream=true"
            );
        }
    }

    #[test]
    fn unauthenticated_routes_preserve_query_parameters() {
        assert_eq!(
            sanitize_upstream_path_and_query(None, "/v1/chat/completions?key=passthrough"),
            "/v1/chat/completions?key=passthrough"
        );
    }

    #[test]
    fn admin_mutation_routes_are_buffered_by_default() {
        use crate::control::GatewayPublicRequestContext;

        // A write route that is not in any list still needs its body: this used to
        // return false and the handler saw `None`, producing a generic 400.
        let mut decision = GatewayControlDecision::synthetic(
            "/api/admin/brand-new/route",
            Some("admin_proxy".to_string()),
            Some("brand_new_family".to_string()),
            Some("brand_new_route_kind".to_string()),
            Some("admin:brand_new".to_string()),
        );
        decision.route_class = Some("admin_proxy".to_string());
        let uri: http::Uri = "/api/admin/brand-new/route".parse().unwrap();
        let headers = http::HeaderMap::new();
        let post = GatewayPublicRequestContext::from_request_parts(
            "trace-brand-new",
            &http::Method::POST,
            &uri,
            &headers,
            Some(decision.clone()),
        );
        assert!(super::admin_proxy_local_requires_buffered_body(&post));

        // Reads have no body to buffer, and non-admin routes are unaffected.
        let get = GatewayPublicRequestContext::from_request_parts(
            "trace-brand-new-get",
            &http::Method::GET,
            &uri,
            &headers,
            Some(decision.clone()),
        );
        assert!(!super::admin_proxy_local_requires_buffered_body(&get));

        let mut public = decision;
        public.route_class = Some("public_support".to_string());
        let public_post = GatewayPublicRequestContext::from_request_parts(
            "trace-public",
            &http::Method::POST,
            &uri,
            &headers,
            Some(public),
        );
        assert!(!super::admin_proxy_local_requires_buffered_body(
            &public_post
        ));
    }

    #[test]
    fn manual_cleanup_route_requires_buffered_body() {
        use crate::control::GatewayPublicRequestContext;
        let mut decision = GatewayControlDecision::synthetic(
            "/api/admin/system/cleanup/usage/manual",
            Some("admin_proxy".to_string()),
            Some("system_manage".to_string()),
            Some("cleanup_usage_manual".to_string()),
            Some("system_manage:cleanup_usage_manual".to_string()),
        );
        decision.route_class = Some("admin_proxy".to_string());
        let uri: http::Uri = "/api/admin/system/cleanup/usage/manual".parse().unwrap();
        let headers = http::HeaderMap::new();
        let context = GatewayPublicRequestContext::from_request_parts(
            "trace-manual-cleanup",
            &http::Method::POST,
            &uri,
            &headers,
            Some(decision),
        );
        assert!(super::admin_proxy_local_requires_buffered_body(&context));
    }
}
