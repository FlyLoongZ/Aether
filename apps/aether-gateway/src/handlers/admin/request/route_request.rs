use super::{AdminAppState, AdminRequestContext};
use crate::{AppState, GatewayError};
use axum::body::{Body, Bytes};
use axum::http::{HeaderMap, Response};
use std::net::SocketAddr;

pub(crate) enum AdminCancelVideoTaskError {
    NotFound,
    InvalidStatus(aether_data_contracts::repository::video_tasks::VideoTaskStatus),
    Response(Response<Body>),
    Gateway(GatewayError),
}

#[derive(Clone, Copy)]
pub(crate) struct AdminRouteRequest<'a> {
    state: AdminAppState<'a>,
    request_context: AdminRequestContext<'a>,
    remote_addr: &'a SocketAddr,
    request_headers: &'a HeaderMap,
    request_body: Option<&'a Bytes>,
}

impl<'a> AdminRouteRequest<'a> {
    pub(crate) fn new(
        state: &'a AppState,
        request_context: &'a crate::control::GatewayPublicRequestContext,
        remote_addr: &'a SocketAddr,
        request_headers: &'a HeaderMap,
        request_body: Option<&'a Bytes>,
    ) -> Self {
        Self {
            state: AdminAppState::new(state),
            request_context: AdminRequestContext::new(request_context),
            remote_addr,
            request_headers,
            request_body,
        }
    }

    pub(crate) fn state(self) -> AdminAppState<'a> {
        self.state
    }

    pub(crate) fn request_context(self) -> AdminRequestContext<'a> {
        self.request_context
    }

    pub(crate) fn remote_addr(self) -> &'a SocketAddr {
        self.remote_addr
    }

    pub(crate) fn request_headers(self) -> &'a HeaderMap {
        self.request_headers
    }

    pub(crate) fn request_body(self) -> Option<&'a Bytes> {
        self.request_body
    }

    /// Whether the client accepts a zstd frame it would decompress itself.
    pub(crate) fn accepts_zstd(self) -> bool {
        accepts_zstd(self.request_headers)
    }
}

/// Whether an `Accept-Encoding` header lists zstd as acceptable.
///
/// Browsers set this header on their own — scripts cannot — so it is what
/// decides whether a stored frame can be handed over as `Content-Encoding:
/// zstd` or has to be decoded first.
pub(crate) fn accepts_zstd(headers: &HeaderMap) -> bool {
    headers
        .get(http::header::ACCEPT_ENCODING)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| {
            value.split(',').any(|entry| {
                let mut parameters = entry.split(';');
                let coding = parameters.next().unwrap_or_default().trim();
                let refused = parameters.any(|parameter| {
                    parameter
                        .trim()
                        .strip_prefix("q=")
                        .is_some_and(|quality| quality.trim().parse::<f32>() == Ok(0.0))
                });
                !refused && (coding.eq_ignore_ascii_case("zstd") || coding == "*")
            })
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_the_accept_encoding_list() {
        for (value, expected) in [
            ("gzip, deflate, br, zstd", true),
            ("zstd", true),
            ("*", true),
            ("ZSTD", true),
            ("gzip, deflate, br", false),
            ("zstd;q=0", false),
            ("", false),
        ] {
            let mut headers = HeaderMap::new();
            if !value.is_empty() {
                headers.insert(http::header::ACCEPT_ENCODING, value.parse().unwrap());
            }
            assert_eq!(accepts_zstd(&headers), expected, "{value}");
        }
    }
}
