use std::sync::{Arc, Mutex};

use axum::body::{to_bytes, Body};

use axum::routing::any;
use axum::{extract::Request, Json, Router};

use http::StatusCode;
use serde_json::json;

use crate::constants::TRACE_ID_HEADER;

use super::{build_router_with_state, build_state_with_execution_runtime_override, start_server};

fn run_finalize_local_cli_test<F, Fut>(test_name: &'static str, make_future: F)
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: std::future::Future<Output = ()> + 'static,
{
    crate::tests::run_async_test_on_large_stack(
        test_name,
        crate::tests::LARGE_TEST_STACK_BYTES,
        make_future,
    );
}

mod compact;
mod cross_format;
mod direct;
