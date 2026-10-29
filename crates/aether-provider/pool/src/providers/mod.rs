pub mod antigravity;
pub mod claude_code;
pub mod codex;
pub mod default;
pub mod gemini_cli;
pub mod kiro;
pub mod unsupported;
pub mod windsurf;
pub mod xai;

pub use antigravity::AntigravityProviderPoolAdapter;
pub use antigravity::{
    build_antigravity_pool_quota_request, build_antigravity_pool_quota_summary_request,
    ANTIGRAVITY_FETCH_AVAILABLE_MODELS_PATH, ANTIGRAVITY_RETRIEVE_USER_QUOTA_SUMMARY_PATH,
};
pub use claude_code::ClaudeCodeProviderPoolAdapter;
pub use claude_code::{
    build_claude_code_pool_quota_request, CLAUDE_CODE_OAUTH_BETA, CLAUDE_CODE_OAUTH_USAGE_URL,
};
pub use codex::CodexProviderPoolAdapter;
pub use codex::{
    build_codex_pool_quota_request, build_codex_pool_reset_credit_consume_request,
    build_codex_pool_reset_credits_request, CODEX_WHAM_RESET_CREDITS_CONSUME_URL,
    CODEX_WHAM_RESET_CREDITS_URL, CODEX_WHAM_USAGE_URL,
};
pub use default::DefaultProviderPoolAdapter;
pub use gemini_cli::GeminiCliProviderPoolAdapter;
pub use gemini_cli::{
    build_gemini_cli_pool_quota_request, GEMINI_CLI_RETRIEVE_USER_QUOTA_PATH, GEMINI_CLI_USER_AGENT,
};
pub use kiro::KiroProviderPoolAdapter;
pub use kiro::{
    build_kiro_pool_quota_request, KiroPoolQuotaAuthInput, KIRO_USAGE_LIMITS_PATH,
    KIRO_USAGE_SDK_VERSION,
};
pub use unsupported::{UnsupportedQuotaProviderPoolAdapter, VERTEX_AI_PROVIDER_POOL_ADAPTER};
pub use windsurf::{
    build_windsurf_pool_model_configs_request,
    build_windsurf_pool_model_configs_request_with_base_url, build_windsurf_pool_quota_request,
    build_windsurf_pool_quota_request_with_base_url, build_windsurf_pool_rate_limit_request,
    build_windsurf_pool_rate_limit_request_with_base_url, WindsurfProviderPoolAdapter,
    WINDSURF_DEFAULT_BASE_URL, WINDSURF_MODEL_CONFIGS_PATH, WINDSURF_RATE_LIMIT_PATH,
    WINDSURF_USER_STATUS_PATH,
};
pub use xai::{
    build_xai_pool_billing_request, build_xai_pool_user_request, XaiProviderPoolAdapter,
    XAI_BILLING_PATH, XAI_USER_PATH,
};
