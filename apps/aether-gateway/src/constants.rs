pub(crate) const TRACE_ID_HEADER: &str = "x-trace-id";
pub(crate) const READYZ_PATH: &str = "/readyz";
pub(crate) const EXECUTION_PATH_HEADER: &str = "x-aether-execution-path";
pub(crate) const DEPENDENCY_REASON_HEADER: &str = "x-aether-dependency-reason";
pub(crate) const LOCAL_EXECUTION_RUNTIME_MISS_REASON_HEADER: &str =
    "x-aether-local-execution-runtime-miss-reason";
pub(crate) const EXECUTION_PATH_PUBLIC_PROXY_PASSTHROUGH: &str = "public_proxy_passthrough";
pub(crate) const EXECUTION_PATH_LOCAL_PROXY_PASSTHROUGH_REMOVED: &str =
    "local_proxy_passthrough_removed";
pub(crate) const EXECUTION_PATH_EXECUTION_RUNTIME_SYNC: &str = "execution_runtime_sync";
pub(crate) const EXECUTION_PATH_EXECUTION_RUNTIME_STREAM: &str = "execution_runtime_stream";
pub(crate) const EXECUTION_PATH_LOCAL_EXECUTION_RUNTIME_MISS: &str = "local_execution_runtime_miss";
pub(crate) const EXECUTION_PATH_LOCAL_EXECUTION_PLANNING_TIMEOUT: &str =
    "local_execution_planning_timeout";
pub(crate) const EXECUTION_PATH_LOCAL_API_KEY_CONCURRENCY_LIMITED: &str =
    "local_api_key_concurrency_limited";
pub(crate) const API_KEY_CONCURRENCY_WAIT_TIMEOUT_MS: u64 = 150;
pub(crate) const API_KEY_CONCURRENCY_WAIT_POLL_INTERVAL_MS: u64 = 10;
pub(crate) const EXECUTION_PATH_LOCAL_AUTH_DENIED: &str = "local_auth_denied";
pub(crate) const EXECUTION_PATH_LOCAL_RATE_LIMITED: &str = "local_rate_limited";
pub(crate) const EXECUTION_PATH_LOCAL_INVALID_REQUEST: &str = "local_invalid_request";
pub(crate) const EXECUTION_PATH_LOCAL_ROUTE_NOT_FOUND: &str = "local_route_not_found";
pub(crate) const EXECUTION_PATH_LOCAL_OVERLOADED: &str = "local_overloaded";
pub(crate) const EXECUTION_PATH_DISTRIBUTED_OVERLOADED: &str = "distributed_overloaded";
pub(crate) const EXECUTION_PATH_LOCAL_AI_PUBLIC: &str = "local_ai_public";
pub(crate) const CONTROL_ROUTE_CLASS_HEADER: &str = "x-aether-control-route-class";
pub(crate) const CONTROL_REQUEST_ID_HEADER: &str = "x-aether-control-request-id";
pub(crate) const CONTROL_CANDIDATE_ID_HEADER: &str = "x-aether-control-candidate-id";
pub(crate) const CONTROL_ENDPOINT_SIGNATURE_HEADER: &str = "x-aether-control-endpoint-signature";

/// Response headers a browser client is allowed to read.
///
/// `Access-Control-Expose-Headers` carries this list verbatim. It is emitted
/// unconditionally instead of `*` because `*` is only a wildcard for requests
/// without credentials, while an explicit list works for every credentials
/// mode. Keep it in sync with the headers the frontend actually reads
/// (`frontend/src/api/dashboard.ts`, `requestTrace.ts`,
/// `features/usage/components/RequestDetailDrawer.vue`,
/// `features/usage/utils/body-load-error.ts`) plus the non-safelisted headers
/// the gateway sets for downloads and backoff (`content-disposition`,
/// `retry-after`).
pub(crate) const CLIENT_READABLE_RESPONSE_HEADERS: &[&str] = &[
    TRACE_ID_HEADER,
    "retry-after",
    "content-disposition",
    "x-aether-body-encoding",
    "x-aether-body-field",
    "x-aether-body-error",
    "x-aether-body-truncated",
    "x-aether-body-size",
    "x-aether-usage-id",
    "x-aether-build-version",
];
pub(crate) const DEFAULT_USER_GROUP_CONFIG_KEY: &str = "default_user_group_id";
pub(crate) const ANTIGRAVITY_BEARER_BRIDGE_CONFIG_KEY: &str = "module.antigravity.bearer_bridge";
pub(crate) const BUILTIN_DEFAULT_USER_GROUP_ID: &str = "00000000-0000-0000-0000-000000000001";
