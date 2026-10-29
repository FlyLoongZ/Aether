mod capability;
mod plan;
mod presets;
mod provider;
mod quota;
mod quota_refresh;
mod service;

pub mod providers;

pub use capability::{ProviderPoolCapabilities, ProviderPoolCapability};
pub use plan::{derive_oauth_plan_type, derive_plan_tier, normalize_provider_plan_tier};
pub use presets::{
    build_admin_pool_scheduling_presets_payload, normalize_provider_scheduling_presets,
};
pub use provider::{ProviderPoolAdapter, ProviderPoolMemberInput};
pub use providers::{
    build_antigravity_pool_quota_request, build_antigravity_pool_quota_summary_request,
    build_claude_code_pool_quota_request,
    build_codex_pool_quota_request, build_codex_pool_reset_credit_consume_request,
    build_codex_pool_reset_credits_request, build_gemini_cli_pool_quota_request,
    build_kiro_pool_quota_request, build_windsurf_pool_model_configs_request,
    build_windsurf_pool_model_configs_request_with_base_url, build_windsurf_pool_quota_request,
    build_windsurf_pool_quota_request_with_base_url, build_windsurf_pool_rate_limit_request,
    build_windsurf_pool_rate_limit_request_with_base_url, build_xai_pool_billing_request,
    build_xai_pool_user_request,
    AntigravityProviderPoolAdapter, ClaudeCodeProviderPoolAdapter,
    CodexProviderPoolAdapter, DefaultProviderPoolAdapter, GeminiCliProviderPoolAdapter,
    KiroPoolQuotaAuthInput, KiroProviderPoolAdapter,
    UnsupportedQuotaProviderPoolAdapter, XaiProviderPoolAdapter,
    ANTIGRAVITY_FETCH_AVAILABLE_MODELS_PATH, ANTIGRAVITY_RETRIEVE_USER_QUOTA_SUMMARY_PATH,
    CODEX_WHAM_RESET_CREDITS_CONSUME_URL, CODEX_WHAM_RESET_CREDITS_URL, CODEX_WHAM_USAGE_URL,
    GEMINI_CLI_RETRIEVE_USER_QUOTA_PATH, GEMINI_CLI_USER_AGENT, KIRO_USAGE_LIMITS_PATH,
    KIRO_USAGE_SDK_VERSION, WINDSURF_MODEL_CONFIGS_PATH, WINDSURF_RATE_LIMIT_PATH,
    WINDSURF_USER_STATUS_PATH, XAI_BILLING_PATH, XAI_USER_PATH,
};
pub use quota::{
    provider_pool_codex_metadata_has_account_quota, provider_pool_key_account_quota_exhausted,
    provider_pool_key_minimum_quota_reached, provider_pool_key_model_quota_exhausted,
    provider_pool_key_model_quota_hard_blocked, provider_pool_key_quota_hard_blocked,
    provider_pool_key_scheduling_label, provider_pool_member_quota_snapshot,
    provider_pool_quota_metadata_provider_type, provider_pool_quota_metadata_updated_at,
    provider_pool_quota_snapshot_updated_at, provider_pool_reset_deadline_elapsed,
};
pub use quota_refresh::ProviderPoolQuotaRequestSpec;
pub use service::ProviderPoolService;

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
