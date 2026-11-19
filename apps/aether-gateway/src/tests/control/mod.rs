use aether_data_contracts::repository::auth_modules::{
    StoredLdapModuleConfig, StoredOAuthProviderModuleConfig,
};
use aether_data_contracts::repository::management_tokens::{
    StoredManagementToken, StoredManagementTokenUserSummary, StoredManagementTokenWithUser,
};
use aether_data_contracts::repository::oauth_providers::StoredOAuthProviderConfig;
use aether_data_contracts::repository::proxy_nodes::StoredProxyNode;
use aether_data_contracts::repository::{
    candidates::{RequestCandidateStatus, StoredRequestCandidate},
    global_models::{
        StoredAdminGlobalModel, StoredAdminProviderModel, StoredProviderActiveGlobalModel,
        StoredProviderModelStats, StoredPublicGlobalModel,
    },
    provider_catalog::{StoredProviderCatalogEndpoint, StoredProviderCatalogKey},
    quota::StoredProviderQuotaSnapshot,
};
use aether_data_memory::auth::InMemoryAuthApiKeySnapshotRepository;
use aether_data_memory::provider_catalog::InMemoryProviderCatalogReadRepository;
use axum::routing::any;
use axum::{Json, Router};

mod admin;
mod helpers;
mod proxy;

pub(super) use helpers::issue_test_admin_access_token as issue_shared_test_admin_access_token;

use super::{
    build_router_with_state, build_state_with_execution_runtime_override, start_server, wait_until,
    AppState,
};
use crate::data::GatewayDataState;
use helpers::*;
