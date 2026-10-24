use std::sync::{Arc, Mutex};

use axum::body::{to_bytes, Body};

use axum::routing::any;
use axum::{extract::Request, Json, Router};

use http::StatusCode;
use serde_json::json;

use crate::constants::TRACE_ID_HEADER;

use super::{build_router_with_state, build_state_with_execution_runtime_override, start_server};

const FINALIZE_LOCAL_CLI_TEST_STACK_BYTES: usize = 16 * 1024 * 1024;

fn run_finalize_local_cli_test<F, Fut>(test_name: &'static str, make_future: F)
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: std::future::Future<Output = ()> + 'static,
{
    let handle = std::thread::Builder::new()
        .name(test_name.to_string())
        .stack_size(FINALIZE_LOCAL_CLI_TEST_STACK_BYTES)
        .spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("test runtime should build");
            runtime.block_on(make_future());
        })
        .expect("finalize local cli test thread should spawn");

    if let Err(payload) = handle.join() {
        std::panic::resume_unwind(payload);
    }
}

mod compact;
mod cross_format;
mod direct;
