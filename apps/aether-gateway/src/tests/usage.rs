use std::sync::Arc;

use aether_crypto::encrypt_python_fernet_plaintext;
use aether_data::repository::auth::{
    InMemoryAuthApiKeySnapshotRepository, StoredAuthApiKeySnapshot,
};
use aether_data::repository::billing::InMemoryBillingReadRepository;
use aether_data::repository::candidate_selection::InMemoryMinimalCandidateSelectionReadRepository;
use aether_data::repository::candidates::InMemoryRequestCandidateRepository;
use aether_data::repository::provider_catalog::InMemoryProviderCatalogReadRepository;
use aether_data::repository::usage::InMemoryUsageReadRepository;
use aether_data::repository::wallet::{
    InMemoryWalletRepository, StoredWalletSnapshot, WalletLookupKey, WalletReadRepository,
};
use aether_data_contracts::repository::billing::StoredBillingModelContext;
use aether_data_contracts::repository::candidate_selection::{
    StoredMinimalCandidateSelectionRow, StoredProviderModelMapping,
};
use aether_data_contracts::repository::candidates::{
    RequestCandidateReadRepository, RequestCandidateStatus,
};
use aether_data_contracts::repository::provider_catalog::{
    StoredProviderCatalogEndpoint, StoredProviderCatalogKey, StoredProviderCatalogProvider,
};
use aether_data_contracts::repository::usage::UsageReadRepository;
use serde_json::json;

use super::{
    any, build_router_with_state, build_state_with_execution_runtime_override, send_request,
    start_server, strip_sse_keepalive_comments, Body, HeaderValue, Json, Mutex, Request, Response,
    Router, StatusCode, UsageRuntimeConfig, TRACE_ID_HEADER,
};
use crate::data::GatewayDataState;

pub(super) use crate::tests::hash_api_key;

const LOCAL_OPENAI_SPEC: crate::tests::support::LocalOpenAiSpec<'_> =
    crate::tests::support::LocalOpenAiSpec {
        id_suffix: "openai-usage-local-1",
        provider_name: "openai",
        provider_priority: 1,
        secret: "sk-upstream-openai",
    };

pub(super) fn sample_local_openai_auth_snapshot(
    api_key_id: &str,
    user_id: &str,
) -> StoredAuthApiKeySnapshot {
    crate::tests::support::local_openai_auth_snapshot(api_key_id, user_id)
}

pub(super) fn sample_local_openai_candidate_row() -> StoredMinimalCandidateSelectionRow {
    crate::tests::support::local_openai_candidate_row(&LOCAL_OPENAI_SPEC)
}

pub(super) fn sample_local_openai_provider() -> StoredProviderCatalogProvider {
    crate::tests::support::local_openai_provider(&LOCAL_OPENAI_SPEC)
}

pub(super) fn sample_local_openai_key() -> StoredProviderCatalogKey {
    crate::tests::support::local_openai_key(&LOCAL_OPENAI_SPEC)
}

pub(super) fn sample_local_openai_endpoint() -> StoredProviderCatalogEndpoint {
    crate::tests::support::local_openai_endpoint(
        &LOCAL_OPENAI_SPEC,
        "https://api.openai.example/v1",
    )
}
mod direct;
mod local;
mod pricing;
mod wallet;
