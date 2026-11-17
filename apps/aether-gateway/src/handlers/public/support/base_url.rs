use axum::http;

use super::GatewayPublicRequestContext;

pub(crate) fn base_url_from_request(
    headers: &http::HeaderMap,
    request_context: &GatewayPublicRequestContext,
    remote_addr: &std::net::SocketAddr,
) -> String {
    if let Some(value) = std::env::var("AETHER_PUBLIC_BASE_URL")
        .ok()
        .or_else(|| std::env::var("PUBLIC_BASE_URL").ok())
        .and_then(|value| normalize_public_base_url(&value))
    {
        return value;
    }

    base_url_from_request_metadata(
        headers,
        request_context.host_header.as_deref(),
        crate::headers::trusted_proxy_ip(remote_addr.ip()),
    )
}

fn base_url_from_request_metadata(
    headers: &http::HeaderMap,
    host_header: Option<&str>,
    trusted_peer: bool,
) -> String {
    if !trusted_peer {
        return "http://localhost".to_string();
    }

    let host = forwarded_header_last(headers, "x-forwarded-host")
        .or_else(|| {
            host_header
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(ToOwned::to_owned)
        })
        .unwrap_or_else(|| "localhost".to_string());
    let proto = forwarded_header_last(headers, "x-forwarded-proto")
        .map(|value| value.trim_end_matches(':').to_ascii_lowercase())
        .filter(|value| value == "http" || value == "https")
        .unwrap_or_else(|| "http".to_string());
    normalize_public_base_url(&format!("{proto}://{host}"))
        .unwrap_or_else(|| "http://localhost".to_string())
}

fn forwarded_header_last(headers: &http::HeaderMap, name: &str) -> Option<String> {
    headers
        .get_all(name)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .map(str::trim)
        .rfind(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

fn normalize_public_base_url(value: &str) -> Option<String> {
    let value = value.trim().trim_end_matches('/');
    let parsed = url::Url::parse(value).ok()?;
    if !aether_http::is_https_or_loopback_http_url(&parsed)
        || parsed.host_str().is_none()
        || !parsed.username().is_empty()
        || parsed.password().is_some()
        || parsed.query().is_some()
        || parsed.fragment().is_some()
    {
        return None;
    }
    Some(parsed.as_str().trim_end_matches('/').to_string())
}
