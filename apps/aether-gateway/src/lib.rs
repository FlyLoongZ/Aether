#![recursion_limit = "256"]

mod admin_api;
mod ai_serving;
mod allocator_metrics;
mod api;
mod async_task;
mod audit;
mod auth;
mod backup;
mod bark_push;
mod cache;
mod cli_client_profile;
mod client_session_affinity;
mod clock;
mod constants;
mod control;
mod data;
mod dispatch;
mod email_delivery;
mod error;
mod execution_activity;
mod execution_runtime;
mod executor;
mod fallback_metrics;
mod frontdoor_loop_guard;
mod handlers;
mod headers;
mod image_capabilities;
mod important_notification;
mod internal_gateway_auth;
mod local_auth_token;
mod log_ids;
mod maintenance;
mod management_token_auth;
pub(crate) mod middleware;
mod model_fetch;
mod oauth;
mod orchestration;
mod plan_usage_policy;
mod privacy;
mod process_metrics;
mod provider_key_auth;
mod provider_pool_demand;
pub(crate) use aether_provider_transport as provider_transport;
mod rate_limit;
mod request_activity;
mod request_candidate_queue;
mod request_candidate_runtime;
mod request_diagnostics;
mod request_lifecycle;
mod roles;
mod router;
mod routing;
mod scheduler;
mod server_chan_push;
mod stage_metrics;
mod state;
mod system_features;
mod task_runtime;
#[cfg(feature = "testkit")]
pub mod testkit;
mod tls;
mod tokio_metrics;
mod upstream_admission;
mod usage;
mod video_tasks;
mod wallet_runtime;
pub use cli_client_profile::ClientProfileSyncGuard;

pub use self::ai_serving::api::{codex_client_originator, codex_client_user_agent};
pub(crate) use self::ai_serving::api::{
    GEMINI_FILES_DOWNLOAD_PLAN_KIND, OPENAI_VIDEO_CONTENT_PLAN_KIND,
};
pub(crate) use self::ai_serving::{AiExecutionDecision, AiExecutionPlanPayload};
pub use self::async_task::VideoTaskTruthSourceMode;
pub use self::backup::{
    apply_restored_backup, restore_backup_json, BackupApplyError, BackupDecryptionKey,
    BackupRestoreError, BackupRestoreLimits, BackupRestoreScope, RestoredBackupJson,
    DEFAULT_BACKUP_MAX_ENCRYPTED_BYTES, DEFAULT_BACKUP_MAX_JSON_BYTES,
};
pub use self::data::GatewayDataConfig;
pub(crate) use self::error::{ApiError, GatewayError};
pub(crate) use self::execution_runtime::{
    append_execution_contract_fields_to_value, append_local_failover_policy_to_value,
    MAX_ERROR_BODY_BYTES, MAX_STREAM_PREFETCH_FRAMES,
};
pub use self::execution_runtime::{
    build_execution_runtime_router, build_execution_runtime_router_with_request_concurrency_limit,
    build_execution_runtime_router_with_request_gates,
    prewarm_direct_h2c_sender_cache_from_env_for_startup, serve_execution_runtime_tcp,
    serve_execution_runtime_unix,
};
pub(crate) use self::fallback_metrics::{GatewayFallbackMetricKind, GatewayFallbackReason};
pub use self::frontdoor_loop_guard::set_gateway_frontdoor_app_port;
pub use self::middleware::strip_cf_headers_middleware;
pub use self::rate_limit::FrontdoorUserRpmConfig;
pub(crate) use self::rate_limit::FrontdoorUserRpmOutcome;
pub use self::router::{attach_static_frontend, build_router, build_router_with_state, serve_tcp};
pub(crate) use self::state::{
    AdminBillingCollectorRecord, AdminBillingCollectorWriteInput, AdminBillingRuleRecord,
    AdminBillingRuleWriteInput, AdminWalletMutationOutcome, AdminWalletPaymentOrderRecord,
    AdminWalletRefundRecord, AdminWalletTransactionRecord, GatewayAdminPaymentCallbackView,
    GatewayUserPreferenceView, GatewayUserSessionView, LocalExecutionRuntimeMissDiagnostic,
    LocalMutationOutcome, LocalProviderDeleteTaskState,
};
pub use self::state::{AppState, FrontdoorCorsConfig};
pub use self::tls::ensure_rustls_crypto_provider;
pub use self::usage::UsageRuntimeConfig;

use axum::http::header::{HeaderName, HeaderValue};

fn insert_header_if_missing(
    headers: &mut http::HeaderMap,
    key: &'static str,
    value: &str,
) -> Result<(), GatewayError> {
    if headers.contains_key(key) {
        return Ok(());
    }
    let name = HeaderName::from_bytes(key.as_bytes())
        .map_err(|err| GatewayError::Internal(err.to_string()))?;
    let value =
        HeaderValue::from_str(value).map_err(|err| GatewayError::Internal(err.to_string()))?;
    headers.insert(name, value);
    Ok(())
}

#[cfg(test)]
#[path = "execution_runtime/tests.rs"]
mod execution_runtime_contract_tests;

#[cfg(test)]
pub(crate) mod test_support;

#[cfg(test)]
pub(crate) mod tests;
