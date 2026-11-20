use super::{AdminAppState, AdminRequestContext};
use crate::AppState;
use axum::body::Bytes;
use axum::http::HeaderMap;
use std::net::SocketAddr;

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
}
