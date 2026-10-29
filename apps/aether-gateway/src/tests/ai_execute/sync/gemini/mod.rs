use std::sync::{Arc, Mutex};

use axum::body::{to_bytes, Body};
use axum::routing::any;
use axum::{extract::Request, Json, Router};
use http::StatusCode;
use serde_json::json;

use crate::constants::{
    EXECUTION_PATH_EXECUTION_RUNTIME_SYNC, EXECUTION_PATH_HEADER, TRACE_ID_HEADER,
};

use super::{build_router_with_state, build_state_with_execution_runtime_override, start_server};
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

mod local_chat;
