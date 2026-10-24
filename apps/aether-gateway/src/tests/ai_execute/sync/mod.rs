use std::sync::{Arc, Mutex};

use axum::body::{to_bytes, Body};
use axum::routing::any;
use axum::{extract::Request, Json, Router};
use http::StatusCode;
use serde_json::json;

use crate::constants::{
    EXECUTION_PATH_EXECUTION_RUNTIME_SYNC, EXECUTION_PATH_HEADER, TRACE_ID_HEADER,
};

use super::{
    build_router_with_state, build_state_with_execution_runtime_override, start_server, wait_until,
    AppState,
};

mod chat;
mod claude;
mod cli;
mod gemini;
mod image;
mod pii_redaction_formats;
mod search;
