use std::convert::Infallible;
use std::sync::{Arc, Mutex};

use axum::body::{to_bytes, Body, Bytes};
use axum::response::Response;
use axum::routing::any;
use axum::{extract::Request, Json, Router};
use http::header::HeaderValue;

use http::StatusCode;
use serde_json::json;

use crate::constants::{
    EXECUTION_PATH_EXECUTION_RUNTIME_STREAM, EXECUTION_PATH_HEADER, TRACE_ID_HEADER,
};

use super::{
    build_router_with_state, build_state_with_execution_runtime_override, start_server,
    strip_sse_keepalive_comments, wait_until, AppState,
};

mod decision;
mod image;
mod pii_redaction;
