use std::sync::{Arc, Mutex};

use axum::body::{to_bytes, Body};

use axum::routing::any;
use axum::{extract::Request, Json, Router};

use http::StatusCode;
use serde_json::json;

use crate::constants::TRACE_ID_HEADER;

use super::{build_router_with_state, build_state_with_execution_runtime_override, start_server};

mod claude;
mod gemini;
