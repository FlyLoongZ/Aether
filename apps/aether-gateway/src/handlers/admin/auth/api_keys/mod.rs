use crate::handlers::admin::request::{AdminAppState, AdminRequestContext};
use crate::handlers::admin::shared::{query_param_bool, query_param_optional_bool};
use crate::GatewayError;
use axum::{body::Body, http, response::Response};

mod install_routes;
mod mutation_routes;
mod read_routes;
mod routes;
mod shared;

pub(crate) async fn maybe_build_local_admin_api_keys_response(
    state: &AdminAppState<'_>,
    request_context: &AdminRequestContext<'_>,
    request_headers: &http::HeaderMap,
    remote_addr: &std::net::SocketAddr,
    request_body: Option<&axum::body::Bytes>,
) -> Result<Option<Response<Body>>, GatewayError> {
    routes::maybe_build_local_admin_api_keys_routes_response(
        state,
        request_context,
        request_headers,
        remote_addr,
        request_body,
    )
    .await
}
