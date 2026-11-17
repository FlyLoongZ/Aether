use axum::http::{HeaderMap, StatusCode};

use crate::control::GatewayControlDecision;

/// Returns whether gateway-internal metadata may be exposed on a client-facing
/// response.
///
/// Authentication and authorization failures never expose internal metadata.
/// Other responses only expose it when the request carried a resolved local
/// user or admin principal.
///
/// This helper governs response headers and the `route_family` / `route_kind`
/// fields of the unhandled-route bodies. Permission-denied bodies follow a
/// separate, stricter rule: they never carry `route_family` / `route_kind`,
/// even when the caller is authenticated, because a scoped management token is
/// usually handed to third-party automation and must not be able to map the
/// gateway's internal route taxonomy by probing paths. They keep
/// `request_path`, which is the caller's own input, and the
/// `required_permission` that explains the denial.
pub(crate) fn should_expose_internal_metadata(
    control_decision: Option<&GatewayControlDecision>,
    status: StatusCode,
) -> bool {
    if matches!(status, StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN) {
        return false;
    }

    control_decision.is_some_and(|decision| {
        decision.auth_context.is_some() || decision.admin_principal.is_some()
    })
}

/// Removes every `x-aether-*` response header while leaving the public
/// `x-trace-id` header untouched.
pub(crate) fn strip_internal_metadata_headers(headers: &mut HeaderMap) {
    let names = headers
        .keys()
        .filter(|name| name.as_str().starts_with("x-aether-"))
        .cloned()
        .collect::<Vec<_>>();
    for name in names {
        headers.remove(&name);
    }
}

#[cfg(test)]
mod tests {
    use super::{should_expose_internal_metadata, strip_internal_metadata_headers};
    use axum::http::{HeaderMap, HeaderValue, StatusCode};

    #[test]
    fn auth_failures_never_expose_internal_metadata() {
        assert!(!should_expose_internal_metadata(
            None,
            StatusCode::UNAUTHORIZED
        ));
        assert!(!should_expose_internal_metadata(
            None,
            StatusCode::FORBIDDEN
        ));
    }

    #[test]
    fn unauthenticated_non_auth_failures_do_not_expose_internal_metadata() {
        assert!(!should_expose_internal_metadata(
            None,
            StatusCode::NOT_FOUND
        ));
        assert!(!should_expose_internal_metadata(
            None,
            StatusCode::NOT_IMPLEMENTED
        ));
    }

    #[test]
    fn strip_removes_aether_headers_but_keeps_trace_id() {
        let mut headers = HeaderMap::new();
        headers.insert("x-trace-id", HeaderValue::from_static("trace-1"));
        headers.insert(
            "x-aether-control-route-class",
            HeaderValue::from_static("public_support"),
        );
        headers.insert(
            "x-aether-execution-path",
            HeaderValue::from_static("public_proxy_passthrough"),
        );
        headers.insert(
            "x-aether-body-field",
            HeaderValue::from_static("request_body"),
        );

        strip_internal_metadata_headers(&mut headers);

        assert_eq!(
            headers.get("x-trace-id"),
            Some(&HeaderValue::from_static("trace-1"))
        );
        assert!(headers.get("x-aether-control-route-class").is_none());
        assert!(headers.get("x-aether-execution-path").is_none());
        assert!(headers.get("x-aether-body-field").is_none());
    }
}
