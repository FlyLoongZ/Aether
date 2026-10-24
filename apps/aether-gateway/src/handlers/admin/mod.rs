mod announcements;
pub(crate) mod auth;
mod billing;
pub(super) mod endpoint;
pub(crate) mod features;
mod model;
pub(crate) mod observability;
pub(crate) mod provider;
mod referrals;
mod routing;
pub(crate) mod system;
mod users;

pub(super) mod request;
pub(super) mod routes;
mod shared;

pub(crate) use self::endpoint::build_admin_endpoint_health_status_payload;
#[cfg(test)]
pub(crate) use self::model::{
    set_admin_external_models_source_url_for_tests, ADMIN_EXTERNAL_MODELS_CONFIG_MUTATION_LOCK_KEY,
};
pub(crate) use self::observability::{
    admin_stats_bad_request_response, parse_bounded_u32, round_to, AdminStatsTimeRange,
};
pub(crate) use self::provider::oauth::duplicates::find_duplicate_provider_oauth_key;
pub(crate) use self::provider::oauth::errors::build_internal_control_error_response;
pub(crate) use self::provider::oauth::provisioning::{
    create_provider_oauth_catalog_key, update_existing_provider_oauth_catalog_key,
};
pub(crate) use self::provider::oauth::quota::dispatch::refresh_provider_pool_quota_locally;
pub(crate) use self::provider::oauth::quota::shared::{
    provider_quota_refresh_endpoint_for_provider, provider_type_supports_quota_refresh,
};
pub(crate) use self::provider::oauth::runtime::{
    provider_oauth_maintenance_endpoint_for_provider,
    refresh_provider_oauth_account_state_after_update,
};
pub(crate) use self::provider::ops::providers::actions::admin_provider_ops_local_action_response;
pub(crate) use self::provider::ops::providers::admin_provider_ops_credential_snapshot;
pub(crate) use self::provider::ops::providers::store_admin_provider_ops_balance_cache;
pub(crate) use self::provider::pool::config::admin_provider_pool_config;
pub(crate) use self::provider::write::provider::reconcile_admin_fixed_provider_template_endpoints;
pub(crate) use self::request::{
    AdminAppState, AdminGatewayProviderTransportSnapshot, AdminLocalOAuthRefreshError,
    AdminRequestContext, AdminRouteRequest, SystemExportMode,
};
pub(crate) use self::routes::maybe_build_local_admin_response;
pub(crate) use self::system::{
    execute_admin_system_import_exclusively, AdminSystemImportLockError,
};
