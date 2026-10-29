use std::sync::{Arc, Mutex};

use axum::body::{to_bytes, Body};

use axum::response::Response;
use axum::routing::any;
use axum::{extract::Request, Json, Router};
use http::header::HeaderValue;

use http::StatusCode;
use serde_json::json;

use crate::constants::TRACE_ID_HEADER;

use super::{
    build_router_with_state, build_state_with_execution_runtime_override, start_server,
    strip_sse_keepalive_comments,
};
use aether_crypto::encrypt_python_fernet_plaintext;
use aether_data::repository::auth::{
    InMemoryAuthApiKeySnapshotRepository, StoredAuthApiKeySnapshot,
};
use aether_data::repository::candidate_selection::InMemoryMinimalCandidateSelectionReadRepository;
use aether_data::repository::candidates::InMemoryRequestCandidateRepository;
use aether_data::repository::provider_catalog::InMemoryProviderCatalogReadRepository;
use aether_data_contracts::repository::candidate_selection::StoredMinimalCandidateSelectionRow;
use aether_data_contracts::repository::candidates::{
    RequestCandidateReadRepository, RequestCandidateStatus,
};
use aether_data_contracts::repository::provider_catalog::{
    StoredProviderCatalogEndpoint, StoredProviderCatalogKey, StoredProviderCatalogProvider,
};

fn run_stream_provider_gemini_test<F, Fut>(test_name: &'static str, make_future: F)
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

mod local_chat;
