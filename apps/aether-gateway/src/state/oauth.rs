use super::{provider_transport_snapshot_looks_refreshed, AppState, GatewayError};
use crate::handlers::shared::default_provider_key_status_snapshot;
use crate::provider_transport::LocalOAuthHttpExecutor;

use super::super::provider_transport;
use crate::provider_key_auth::provider_key_is_oauth_managed;
use aether_admin::provider::quota as admin_provider_quota_pure;
use aether_contracts::{
    ExecutionPlan, ExecutionTimeouts, ProxySnapshot, RequestBody,
    EXECUTION_REQUEST_FOLLOW_REDIRECTS_HEADER, EXECUTION_REQUEST_HTTP1_ONLY_HEADER,
};
use aether_data_contracts::repository::provider_catalog::{
    ProviderCatalogKeyOAuthCredentialCasDelete, ProviderCatalogKeyOAuthCredentialFence,
    ProviderCatalogKeyOAuthRuntimeStateCasUpdate, ProviderCatalogKeyStatusSnapshotUpdate,
    ProviderCatalogUpstreamMetadataNamespaceExpectation, StoredProviderCatalogKey,
};
use aether_runtime_state::RuntimeLockLease;
use base64::{engine::general_purpose::STANDARD, Engine as _};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const LOCAL_OAUTH_HTTP_TIMEOUT_MS: u64 = 30_000;
const LOCAL_OAUTH_RESPONSE_BODY_LIMIT_BYTES: usize = 4 * 1024 * 1024;
const REMOTE_OAUTH_REFRESH_WAIT_TIMEOUT: Duration = Duration::from_secs(35);
const REMOTE_OAUTH_REFRESH_POLL_INTERVAL: Duration = Duration::from_millis(100);
const OAUTH_ACCOUNT_BLOCK_PREFIX: &str = "[ACCOUNT_BLOCK] ";
const OAUTH_EXPIRED_PREFIX: &str = "[OAUTH_EXPIRED] ";
const OAUTH_REFRESH_FAILED_PREFIX: &str = "[REFRESH_FAILED] ";
const OAUTH_REQUEST_FAILED_PREFIX: &str = "[REQUEST_FAILED] ";
const CODEX_OAUTH_INVALIDATION_CAS_MAX_ATTEMPTS: usize = 16;

#[derive(Clone, PartialEq)]
pub(crate) struct ProviderTransportCredentialFence {
    pub(crate) encrypted_auth_config: String,
    pub(crate) credential: ProviderCatalogKeyOAuthCredentialFence,
}

impl std::fmt::Debug for ProviderTransportCredentialFence {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ProviderTransportCredentialFence")
            .field("encrypted_auth_config", &"[REDACTED]")
            .field("credential", &self.credential)
            .finish()
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct CodexRuntimeOAuthObservation<'a> {
    pub(crate) request_started_at_unix_ms: u64,
    pub(crate) request_order_id: &'a str,
    pub(crate) observed_credential_generation: Option<&'a str>,
    pub(crate) runtime_invalid_reason: Option<&'a str>,
}

struct GatewayLocalOAuthHttpExecutor<'a> {
    state: &'a AppState,
}

pub(crate) enum AgentIdentityAuthConfigFence {
    NotAgentIdentity,
    Current,
    StaleGeneration,
}

fn trimmed_reason(reason: Option<&str>) -> Option<String> {
    reason
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

fn tagged_reason(reason: Option<&str>, prefix: &str) -> Option<String> {
    reason.and_then(|value| {
        value
            .lines()
            .map(str::trim)
            .find_map(|line| line.strip_prefix(prefix))
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned)
    })
}

fn oauth_access_token_expired(expires_at_unix_secs: Option<u64>, now_unix_secs: u64) -> bool {
    expires_at_unix_secs.is_none_or(|expires_at| expires_at == 0 || expires_at <= now_unix_secs)
}

fn local_oauth_refresh_entry_should_stay_memory_only(
    transport: &provider_transport::GatewayProviderTransportSnapshot,
    entry: &provider_transport::CachedOAuthEntry,
) -> bool {
    entry
        .provider_type
        .trim()
        .eq_ignore_ascii_case(provider_transport::vertex::VERTEX_SERVICE_ACCOUNT_PROVIDER_TYPE)
        && provider_transport::is_vertex_service_account_transport_context(transport)
}

fn local_oauth_transport_context_allows_reload(
    initial: &provider_transport::GatewayProviderTransportSnapshot,
    current: &provider_transport::GatewayProviderTransportSnapshot,
) -> bool {
    let initial_is_agent = provider_transport::is_codex_agent_identity_transport(initial);
    let current_is_agent = provider_transport::is_codex_agent_identity_transport(current);
    if initial_is_agent || current_is_agent {
        return initial_is_agent
            && current_is_agent
            && provider_transport::codex_agent_identity_transport_allows_task_rotation_from(
                initial, current,
            );
    }
    true
}

pub(crate) fn provider_transport_context_allows_credential_rotation(
    initial: &provider_transport::GatewayProviderTransportSnapshot,
    current: &provider_transport::GatewayProviderTransportSnapshot,
) -> bool {
    initial.provider.id == current.provider.id
        && initial.endpoint.id == current.endpoint.id
        && initial.key.id == current.key.id
        && initial.key.auth_type == current.key.auth_type
        && initial.provider.provider_type == current.provider.provider_type
        && local_oauth_transport_context_allows_reload(initial, current)
}

fn discard_failed_local_oauth_refresh_resolution(
    resolution: &mut Option<provider_transport::LocalOAuthResolution>,
) {
    if let Some(resolution) = resolution.as_mut() {
        resolution.auth = None;
        resolution.refreshed_entry = None;
    }
}

fn oauth_auth_config_refresh_token_fingerprint(auth_config: Option<&str>) -> Option<String> {
    let parsed = auth_config
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .and_then(|value| serde_json::from_str::<Value>(value).ok())?;
    oauth_metadata_refresh_token_fingerprint(Some(&parsed))
}

fn oauth_metadata_refresh_token_fingerprint(metadata: Option<&Value>) -> Option<String> {
    metadata
        .and_then(Value::as_object)
        .and_then(|object| object.get("refresh_token"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(secret_fingerprint)
}

fn local_oauth_request_refresh_token_fingerprint(
    request: &provider_transport::LocalOAuthHttpRequest,
) -> (Option<String>, Option<usize>) {
    if let Some(json_body) = request.json_body.as_ref() {
        return json_body
            .as_object()
            .and_then(|object| object.get("refresh_token"))
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(|value| (Some(secret_fingerprint(value)), Some(value.len())))
            .unwrap_or((None, None));
    }

    let Some(body_bytes) = request.body_bytes.as_ref() else {
        return (None, None);
    };
    for (key, value) in url::form_urlencoded::parse(body_bytes) {
        if key == "refresh_token" {
            let value = value.trim();
            if !value.is_empty() {
                return (Some(secret_fingerprint(value)), Some(value.len()));
            }
        }
    }
    (None, None)
}

fn local_oauth_log_excerpt(body: &str) -> String {
    if body.trim().is_empty() {
        "-".to_string()
    } else {
        "[redacted]".to_string()
    }
}

fn local_oauth_proxy_is_tunnel(proxy: Option<&ProxySnapshot>) -> bool {
    let Some(proxy) = proxy else {
        return false;
    };
    if proxy.enabled == Some(false) {
        return false;
    }
    proxy
        .mode
        .as_deref()
        .map(str::trim)
        .is_some_and(|mode| mode.eq_ignore_ascii_case("tunnel"))
}

fn local_oauth_proxy_extra_string<'a>(
    proxy: Option<&'a ProxySnapshot>,
    key: &str,
) -> Option<&'a str> {
    proxy?
        .extra
        .as_ref()
        .and_then(|extra| extra.get(key))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

fn secret_fingerprint(value: &str) -> String {
    let digest = Sha256::digest(value.as_bytes());
    let mut fingerprint = String::with_capacity(16);
    for byte in digest.iter().take(8) {
        use std::fmt::Write as _;
        let _ = write!(&mut fingerprint, "{byte:02x}");
    }
    fingerprint
}

fn oauth_invalid_reason_is_account_block(reason: Option<&str>) -> bool {
    let Some(reason) = reason.map(str::trim).filter(|value| !value.is_empty()) else {
        return false;
    };
    if reason.starts_with(OAUTH_ACCOUNT_BLOCK_PREFIX) {
        return true;
    }
    let snapshot =
        aether_admin::provider::status::resolve_account_status_snapshot(None, None, Some(reason));
    snapshot.blocked
        && !matches!(
            snapshot.code.trim().to_ascii_lowercase().as_str(),
            "oauth_token_invalid"
                | "oauth_token_expired"
                | "oauth_expired"
                | "oauth_refresh_failed"
        )
}

fn normalize_local_oauth_refresh_error_message(
    status_code: Option<u16>,
    body_excerpt: Option<&str>,
) -> String {
    let mut message = None::<String>;
    let mut error_code = None::<String>;

    if let Some(body_excerpt) = body_excerpt {
        if let Ok(value) = serde_json::from_str::<serde_json::Value>(body_excerpt) {
            if let Some(object) = value.as_object() {
                if let Some(error_object) =
                    object.get("error").and_then(serde_json::Value::as_object)
                {
                    message = error_object
                        .get("message")
                        .or_else(|| error_object.get("error_description"))
                        .and_then(serde_json::Value::as_str)
                        .map(str::trim)
                        .filter(|value| !value.is_empty())
                        .map(ToOwned::to_owned);
                    error_code = error_object
                        .get("code")
                        .and_then(serde_json::Value::as_str)
                        .map(str::trim)
                        .filter(|value| !value.is_empty())
                        .map(|value| value.to_ascii_lowercase());
                }
                if message.is_none() {
                    message = object
                        .get("message")
                        .or_else(|| object.get("error_description"))
                        .and_then(serde_json::Value::as_str)
                        .map(str::trim)
                        .filter(|value| !value.is_empty())
                        .map(ToOwned::to_owned);
                }
                if error_code.is_none() {
                    error_code = object
                        .get("code")
                        .and_then(serde_json::Value::as_str)
                        .map(str::trim)
                        .filter(|value| !value.is_empty())
                        .map(|value| value.to_ascii_lowercase());
                }
            }
        }
    }

    let message = message
        .or_else(|| {
            body_excerpt
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(|value| value.chars().take(300).collect::<String>())
        })
        .unwrap_or_default();
    let lowered = message.to_ascii_lowercase();
    let error_code = error_code.unwrap_or_default();

    if error_code == "refresh_token_reused"
        || lowered.contains("already been used to generate a new access token")
    {
        return "refresh_token 已被使用并轮换，请重新登录授权".to_string();
    }
    if error_code == "invalid_grant"
        || error_code == "invalid_refresh_token"
        || error_code == "refresh_token_expired"
        || lowered.contains("could not validate your refresh token")
        || (lowered.contains("refresh token")
            && ["expired", "revoked", "invalid"]
                .iter()
                .any(|keyword| lowered.contains(keyword)))
    {
        return "refresh_token 无效、已过期或已撤销，请重新登录授权".to_string();
    }
    status_code
        .map(|status_code| format!("HTTP {status_code}"))
        .unwrap_or_else(|| "Token 刷新失败".to_string())
}

fn local_oauth_refresh_gateway_error(
    error: &provider_transport::LocalOAuthRefreshError,
) -> GatewayError {
    // Keep a bounded, credential-redacted reason for internal diagnostics.
    // GatewayError::Internal still returns the generic error response to clients.
    GatewayError::Internal(format!(
        "local oauth refresh failed: {}",
        crate::error::redact_error_detail(error)
    ))
}

fn merge_local_oauth_refresh_failure_reason(
    current_reason: Option<&str>,
    refresh_reason: &str,
) -> Option<String> {
    let current_reason = current_reason.map(str::trim).unwrap_or_default();
    let refresh_reason = refresh_reason.trim();
    if refresh_reason.is_empty() {
        return (!current_reason.is_empty()).then(|| current_reason.to_string());
    }
    if current_reason.is_empty() {
        return Some(refresh_reason.to_string());
    }
    if current_reason.starts_with(OAUTH_EXPIRED_PREFIX) {
        if refresh_reason.starts_with(OAUTH_REFRESH_FAILED_PREFIX)
            && !current_reason
                .lines()
                .map(str::trim)
                .any(|line| line.starts_with(OAUTH_REFRESH_FAILED_PREFIX))
        {
            return Some(format!("{current_reason}\n{refresh_reason}"));
        }
        return Some(current_reason.to_string());
    }
    if oauth_invalid_reason_is_account_block(Some(current_reason)) {
        return None;
    }
    Some(refresh_reason.to_string())
}

fn local_oauth_refresh_success_invalid_state(
    key: &StoredProviderCatalogKey,
) -> (Option<u64>, Option<String>) {
    let current_reason = key
        .oauth_invalid_reason
        .as_deref()
        .map(str::trim)
        .unwrap_or_default();
    if oauth_invalid_reason_is_account_block(Some(current_reason)) {
        return (
            key.oauth_invalid_at_unix_secs,
            Some(current_reason.to_string()),
        );
    }
    (None, None)
}

fn default_oauth_status_snapshot_value() -> Value {
    default_provider_key_status_snapshot()
        .get("oauth")
        .cloned()
        .unwrap_or_else(|| {
            json!({
                "code": "none",
                "label": Value::Null,
                "reason": Value::Null,
                "expires_at": Value::Null,
                "invalid_at": Value::Null,
                "source": Value::Null,
                "requires_reauth": false,
                "expiring_soon": false,
            })
        })
}

fn build_oauth_status_snapshot_value(key: &StoredProviderCatalogKey) -> Value {
    if !key.auth_type.trim().eq_ignore_ascii_case("oauth") {
        return default_oauth_status_snapshot_value();
    }

    let now_unix_secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .map(|duration| duration.as_secs())
        .unwrap_or(0);
    let expires_at_unix_secs = key.expires_at_unix_secs;
    let invalid_at_unix_secs = key.oauth_invalid_at_unix_secs;
    let invalid_reason = trimmed_reason(key.oauth_invalid_reason.as_deref());

    if let Some(reason) = tagged_reason(invalid_reason.as_deref(), OAUTH_EXPIRED_PREFIX) {
        let (code, label) =
            aether_admin::provider::status::oauth_token_snapshot_status_parts(reason.as_str());
        return json!({
            "code": code,
            "label": label,
            "reason": reason,
            "expires_at": expires_at_unix_secs,
            "invalid_at": invalid_at_unix_secs,
            "source": "oauth_invalid",
            "requires_reauth": code == "invalid",
            "expiring_soon": false,
        });
    }
    if let Some(reason) = tagged_reason(invalid_reason.as_deref(), OAUTH_REFRESH_FAILED_PREFIX) {
        let access_token_expired = oauth_access_token_expired(expires_at_unix_secs, now_unix_secs);
        return json!({
            "code": if access_token_expired { "invalid" } else { "reauth_required" },
            "label": if access_token_expired { "已失效" } else { "续期失败" },
            "reason": reason,
            "expires_at": expires_at_unix_secs,
            "invalid_at": invalid_at_unix_secs,
            "source": "oauth_refresh",
            "requires_reauth": true,
            "usable_until_expiry": !access_token_expired,
            "expiring_soon": false,
        });
    }
    if let Some(reason) = tagged_reason(invalid_reason.as_deref(), OAUTH_REQUEST_FAILED_PREFIX) {
        if admin_provider_quota_pure::codex_looks_like_token_invalidated(Some(&reason)) {
            return json!({
                "code": "invalid",
                "label": "已失效",
                "reason": reason,
                "expires_at": expires_at_unix_secs,
                "invalid_at": invalid_at_unix_secs,
                "source": "oauth_invalid",
                "requires_reauth": true,
                "expiring_soon": false,
            });
        }
        return json!({
            "code": "check_failed",
            "label": "检查失败",
            "reason": reason,
            "expires_at": expires_at_unix_secs,
            "invalid_at": Value::Null,
            "source": "oauth_request",
            "requires_reauth": false,
            "expiring_soon": false,
        });
    }
    if invalid_reason
        .as_deref()
        .is_some_and(|reason| !reason.starts_with(OAUTH_ACCOUNT_BLOCK_PREFIX))
        || invalid_at_unix_secs.is_some()
    {
        return json!({
            "code": "invalid",
            "label": "已失效",
            "reason": invalid_reason,
            "expires_at": expires_at_unix_secs,
            "invalid_at": invalid_at_unix_secs,
            "source": "oauth_invalid",
            "requires_reauth": true,
            "expiring_soon": false,
        });
    }

    let Some(expires_at_unix_secs) = expires_at_unix_secs else {
        return default_oauth_status_snapshot_value();
    };
    if expires_at_unix_secs <= now_unix_secs {
        return json!({
            "code": "expired",
            "label": "已过期",
            "reason": "Access Token 已过期，等待自动续期",
            "expires_at": expires_at_unix_secs,
            "invalid_at": Value::Null,
            "source": "expires_at",
            "requires_reauth": false,
            "expiring_soon": false,
        });
    }

    let expiring_soon = expires_at_unix_secs.saturating_sub(now_unix_secs) < 24 * 60 * 60;
    json!({
        "code": if expiring_soon { "expiring" } else { "valid" },
        "label": if expiring_soon { "即将过期" } else { "有效" },
        "reason": Value::Null,
        "expires_at": expires_at_unix_secs,
        "invalid_at": Value::Null,
        "source": "expires_at",
        "requires_reauth": false,
        "expiring_soon": expiring_soon,
    })
}

fn sync_provider_key_oauth_status_snapshot(
    status_snapshot: Option<Value>,
    key: &StoredProviderCatalogKey,
) -> Option<Value> {
    let mut snapshot = status_snapshot
        .and_then(|value| match value {
            Value::Object(object) => Some(object),
            _ => None,
        })
        .or_else(|| default_provider_key_status_snapshot().as_object().cloned())
        .unwrap_or_default();
    snapshot.insert("oauth".to_string(), build_oauth_status_snapshot_value(key));
    Some(Value::Object(snapshot))
}

#[async_trait::async_trait]
impl<'a> provider_transport::LocalOAuthHttpExecutor for GatewayLocalOAuthHttpExecutor<'a> {
    async fn execute(
        &self,
        provider_type: &'static str,
        transport: &provider_transport::GatewayProviderTransportSnapshot,
        request: &provider_transport::LocalOAuthHttpRequest,
    ) -> Result<
        provider_transport::LocalOAuthHttpResponse,
        provider_transport::LocalOAuthRefreshError,
    > {
        self.state
            .execute_local_oauth_http_request(provider_type, transport, request)
            .await
    }
}

impl AppState {
    pub(crate) fn clear_provider_transport_snapshot_cache(&self) {
        self.provider_transport_snapshot_cache.clear();
    }

    /// Loads one transport snapshot, bypassing the cache and the
    /// format-conversion override.
    pub(crate) async fn load_provider_transport_snapshot(
        &self,
        provider_id: &str,
        endpoint_id: &str,
        key_id: &str,
    ) -> Result<Option<Arc<provider_transport::GatewayProviderTransportSnapshot>>, GatewayError>
    {
        let loaded = self
            .read_provider_transport_snapshot_uncached(provider_id, endpoint_id, key_id)
            .await?;
        let Some(snapshot) = loaded else {
            return Ok(None);
        };
        Ok(Some(Arc::new(
            self.apply_global_format_conversion_override(snapshot).await,
        )))
    }

    pub(crate) async fn read_provider_transport_snapshot_uncached(
        &self,
        provider_id: &str,
        endpoint_id: &str,
        key_id: &str,
    ) -> Result<Option<crate::provider_transport::GatewayProviderTransportSnapshot>, GatewayError>
    {
        crate::provider_transport::read_provider_transport_snapshot(
            self,
            provider_id,
            endpoint_id,
            key_id,
        )
        .await
        .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    async fn apply_global_format_conversion_override(
        &self,
        mut snapshot: crate::provider_transport::GatewayProviderTransportSnapshot,
    ) -> crate::provider_transport::GatewayProviderTransportSnapshot {
        let global_config =
            Box::pin(self.read_system_config_json_value("enable_format_conversion"))
                .await
                .ok()
                .flatten();
        let global_enabled = global_config
            .and_then(|value| value.as_bool())
            .unwrap_or(false);
        if global_enabled {
            snapshot.provider.enable_format_conversion = true;
        }
        snapshot
    }

    pub(crate) async fn list_enabled_oauth_module_providers(
        &self,
    ) -> Result<
        Vec<aether_data::repository::auth_modules::StoredOAuthProviderModuleConfig>,
        GatewayError,
    > {
        self.data
            .list_enabled_oauth_module_providers()
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    pub(crate) async fn get_ldap_module_config(
        &self,
    ) -> Result<Option<aether_data::repository::auth_modules::StoredLdapModuleConfig>, GatewayError>
    {
        self.data
            .get_ldap_module_config()
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    pub(crate) async fn compare_and_swap_ldap_module_config(
        &self,
        expected: Option<&aether_data::repository::auth_modules::StoredLdapModuleConfig>,
        replacement: &aether_data::repository::auth_modules::StoredLdapModuleConfig,
        bind_password_update: &aether_data::repository::auth_modules::LdapBindPasswordUpdate,
    ) -> Result<
        Option<aether_data::repository::auth_modules::CompareAndSwapLdapConfigResult>,
        GatewayError,
    > {
        self.data
            .compare_and_swap_ldap_module_config(expected, replacement, bind_password_update)
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    pub(crate) async fn delete_ldap_module_config_if_matches(
        &self,
        expected: &aether_data::repository::auth_modules::StoredLdapModuleConfig,
    ) -> Result<bool, GatewayError> {
        self.data
            .delete_ldap_module_config_if_matches(expected)
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    pub(crate) async fn compare_and_swap_ldap_bind_password(
        &self,
        expected: &str,
        replacement: &str,
    ) -> Result<bool, GatewayError> {
        self.data
            .compare_and_swap_ldap_bind_password(expected, replacement)
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    pub(crate) async fn count_active_local_admin_users_with_valid_password(
        &self,
    ) -> Result<u64, GatewayError> {
        self.data
            .count_active_local_admin_users_with_valid_password()
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    pub(crate) async fn list_oauth_provider_configs(
        &self,
    ) -> Result<
        Vec<aether_data::repository::oauth_providers::StoredOAuthProviderConfig>,
        GatewayError,
    > {
        self.data
            .list_oauth_provider_configs()
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    pub(crate) async fn has_oauth_links_for_provider(
        &self,
        provider_type: &str,
    ) -> Result<bool, GatewayError> {
        self.data
            .has_oauth_links_for_provider(provider_type)
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    pub(crate) async fn get_oauth_provider_config(
        &self,
        provider_type: &str,
    ) -> Result<
        Option<aether_data::repository::oauth_providers::StoredOAuthProviderConfig>,
        GatewayError,
    > {
        self.data
            .get_oauth_provider_config(provider_type)
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    pub(crate) async fn compare_and_swap_oauth_provider_client_secret(
        &self,
        provider_type: &str,
        expected: &str,
        replacement: &str,
    ) -> Result<bool, GatewayError> {
        self.data
            .compare_and_swap_oauth_provider_client_secret(provider_type, expected, replacement)
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    pub(crate) async fn count_locked_users_if_oauth_provider_disabled(
        &self,
        provider_type: &str,
        ldap_exclusive: bool,
    ) -> Result<usize, GatewayError> {
        self.data
            .count_locked_users_if_oauth_provider_disabled(provider_type, ldap_exclusive)
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    pub(crate) async fn upsert_oauth_provider_config(
        &self,
        record: &aether_data::repository::oauth_providers::UpsertOAuthProviderConfigRecord,
    ) -> Result<
        Option<aether_data::repository::oauth_providers::StoredOAuthProviderConfig>,
        GatewayError,
    > {
        let _mutation_guard = crate::oauth::lock_identity_oauth_mutation().await;
        let ldap_exclusive = self.get_ldap_module_config().await?.is_some_and(|config| {
            config.is_enabled
                && config.is_exclusive
                && config
                    .bind_password_encrypted
                    .as_deref()
                    .map(str::trim)
                    .is_some_and(|value| !value.is_empty())
        });
        let locked_users_snapshot = if record.is_enabled {
            0
        } else {
            self.count_locked_users_if_oauth_provider_disabled(
                &record.provider_type,
                ldap_exclusive,
            )
            .await?
        };
        match self
            .data
            .upsert_oauth_provider_config(record, ldap_exclusive, false, locked_users_snapshot)
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))?
        {
            Some(
                aether_data::repository::oauth_providers::UpsertOAuthProviderConfigOutcome::Upserted(
                    provider,
                ),
            ) => Ok(Some(provider)),
            Some(
                aether_data::repository::oauth_providers::UpsertOAuthProviderConfigOutcome::DisableRequiresConfirmation {
                    affected_count,
                },
            ) => Err(GatewayError::Client {
                status: axum::http::StatusCode::CONFLICT,
                message: format!(
                    "禁用 OAuth Provider '{}' 会导致 {affected_count} 个用户无法登录",
                    record.provider_type
                ),
            }),
            None => Ok(None),
        }
    }

    pub(crate) async fn upsert_oauth_provider_config_with_force_disable(
        &self,
        record: &aether_data::repository::oauth_providers::UpsertOAuthProviderConfigRecord,
        force_disable: bool,
    ) -> Result<
        Option<aether_data::repository::oauth_providers::UpsertOAuthProviderConfigOutcome>,
        GatewayError,
    > {
        let _mutation_guard = crate::oauth::lock_identity_oauth_mutation().await;
        let ldap_exclusive = self.get_ldap_module_config().await?.is_some_and(|config| {
            config.is_enabled
                && config.is_exclusive
                && config
                    .bind_password_encrypted
                    .as_deref()
                    .map(str::trim)
                    .is_some_and(|value| !value.is_empty())
        });
        let locked_users_snapshot = if record.is_enabled || force_disable {
            0
        } else {
            self.count_locked_users_if_oauth_provider_disabled(
                &record.provider_type,
                ldap_exclusive,
            )
            .await?
        };
        self.data
            .upsert_oauth_provider_config(
                record,
                ldap_exclusive,
                force_disable,
                locked_users_snapshot,
            )
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    pub(crate) async fn delete_oauth_provider_config_if_unlinked(
        &self,
        provider_type: &str,
    ) -> Result<bool, GatewayError> {
        self.data
            .delete_oauth_provider_config_if_unlinked(provider_type)
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    pub(crate) fn encryption_key(&self) -> Option<&str> {
        self.data.encryption_key()
    }

    pub(crate) fn has_auth_module_writer(&self) -> bool {
        self.data.has_auth_module_writer()
    }

    pub(crate) fn provider_oauth_token_url(
        &self,
        _provider_type: &str,
        default_token_url: &str,
    ) -> String {
        #[cfg(test)]
        {
            if let Some(value) = self
                .provider_oauth_token_url_overrides
                .lock()
                .expect("provider oauth token url overrides should lock")
                .get(_provider_type.trim())
                .cloned()
            {
                return value;
            }
        }

        default_token_url.to_string()
    }

    pub(crate) fn save_provider_oauth_state_for_tests(&self, _key: &str, _value: &str) -> bool {
        #[cfg(test)]
        {
            if let Some(store) = self.provider_oauth_state_store.as_ref() {
                store
                    .lock()
                    .expect("provider oauth state store should lock")
                    .insert(_key.to_string(), _value.to_string());
                return true;
            }
        }

        false
    }

    pub(crate) fn save_provider_oauth_device_session_for_tests(
        &self,
        _key: &str,
        _value: &str,
    ) -> bool {
        #[cfg(test)]
        {
            if let Some(store) = self.provider_oauth_device_session_store.as_ref() {
                store
                    .lock()
                    .expect("provider oauth device session store should lock")
                    .insert(_key.to_string(), _value.to_string());
                return true;
            }
        }

        false
    }

    #[cfg(test)]
    pub(crate) fn load_provider_oauth_device_session_for_tests(
        &self,
        _key: &str,
    ) -> Option<String> {
        #[cfg(test)]
        {
            return self
                .provider_oauth_device_session_store
                .as_ref()
                .and_then(|store| {
                    store
                        .lock()
                        .expect("provider oauth device session store should lock")
                        .get(_key)
                        .cloned()
                });
        }

        #[allow(unreachable_code)]
        None
    }

    pub(crate) fn save_provider_oauth_batch_task_for_tests(
        &self,
        _key: &str,
        _value: &str,
    ) -> bool {
        #[cfg(test)]
        {
            if let Some(store) = self.provider_oauth_batch_task_store.as_ref() {
                store
                    .lock()
                    .expect("provider oauth batch task store should lock")
                    .insert(_key.to_string(), _value.to_string());
                return true;
            }
        }

        false
    }

    #[cfg(test)]
    pub(crate) fn load_provider_oauth_batch_task_for_tests(&self, _key: &str) -> Option<String> {
        #[cfg(test)]
        {
            return self
                .provider_oauth_batch_task_store
                .as_ref()
                .and_then(|store| {
                    store
                        .lock()
                        .expect("provider oauth batch task store should lock")
                        .get(_key)
                        .cloned()
                });
        }

        #[allow(unreachable_code)]
        None
    }

    pub(crate) async fn read_provider_transport_snapshot_arc(
        &self,
        provider_id: &str,
        endpoint_id: &str,
        key_id: &str,
    ) -> Result<
        Option<Arc<crate::provider_transport::GatewayProviderTransportSnapshot>>,
        GatewayError,
    > {
        let background_state = self.clone();
        let background_provider = provider_id.to_string();
        let background_endpoint = endpoint_id.to_string();
        let background_key = key_id.to_string();
        self.provider_transport_snapshot_cache
            .get_or_load(
                provider_id,
                endpoint_id,
                key_id,
                || self.load_provider_transport_snapshot(provider_id, endpoint_id, key_id),
                move || async move {
                    background_state
                        .load_provider_transport_snapshot(
                            &background_provider,
                            &background_endpoint,
                            &background_key,
                        )
                        .await
                },
            )
            .await
    }

    pub(crate) async fn read_provider_transport_snapshot(
        &self,
        provider_id: &str,
        endpoint_id: &str,
        key_id: &str,
    ) -> Result<Option<crate::provider_transport::GatewayProviderTransportSnapshot>, GatewayError>
    {
        Ok(self
            .read_provider_transport_snapshot_arc(provider_id, endpoint_id, key_id)
            .await?
            .map(|snapshot| (*snapshot).clone()))
    }

    pub(crate) async fn update_provider_catalog_key_oauth_runtime_state(
        &self,
        key_id: &str,
        oauth_invalid_at_unix_secs: Option<u64>,
        oauth_invalid_reason: Option<&str>,
        updated_at_unix_secs: Option<u64>,
    ) -> Result<bool, GatewayError> {
        let updated = self
            .data
            .update_provider_catalog_key_oauth_runtime_state(
                key_id,
                oauth_invalid_at_unix_secs,
                oauth_invalid_reason,
                updated_at_unix_secs,
            )
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))?;
        if updated {
            self.clear_provider_transport_snapshot_cache();
        }
        Ok(updated)
    }

    pub(crate) async fn compare_and_update_provider_catalog_key_oauth_runtime_state(
        &self,
        update: &ProviderCatalogKeyOAuthRuntimeStateCasUpdate,
    ) -> Result<bool, GatewayError> {
        let mut protected = update.clone();
        let expected_credential = update.expected_credential.clone().ok_or_else(|| {
            GatewayError::Internal(
                "provider catalog OAuth credential update requires a complete credential fence"
                    .to_string(),
            )
        })?;
        self.validate_protected_provider_catalog_key_auth_config(
            &expected_credential.provider_id,
            &update.key_id,
            &update.encrypted_auth_config,
        )?;
        if let Some(encrypted_api_key) = update.encrypted_api_key_update.as_deref() {
            self.validate_protected_provider_catalog_key_api_key(
                &expected_credential.provider_id,
                &update.key_id,
                encrypted_api_key,
            )?;
        }
        protected.expected_credential = Some(expected_credential);
        let updated = self
            .data
            .compare_and_update_provider_catalog_key_oauth_runtime_state(&protected)
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))?;
        // A conflict means another instance/admin changed the credential.
        self.clear_provider_transport_snapshot_cache();
        Ok(updated)
    }

    pub(crate) async fn resolve_local_oauth_request_auth(
        &self,
        transport: &provider_transport::GatewayProviderTransportSnapshot,
    ) -> Result<Option<provider_transport::LocalResolvedOAuthRequestAuth>, GatewayError> {
        let distributed_lock = self.runtime_state.as_ref();
        let lock_owner = format!("aether-gateway-{}", std::process::id());
        let initial_transport = transport.clone();
        let mut current_transport = transport.clone();
        let executor = GatewayLocalOAuthHttpExecutor { state: self };

        for _ in 0..2 {
            if !local_oauth_transport_context_allows_reload(&initial_transport, &current_transport)
            {
                return Ok(None);
            }
            let expected_credential_fence = if current_transport
                .key
                .decrypted_auth_config
                .as_deref()
                .map(str::trim)
                .is_some_and(|value| !value.is_empty())
            {
                match self
                    .capture_provider_transport_credential_fence(&current_transport)
                    .await?
                {
                    Some(fence) => Some(fence),
                    None => {
                        let Some(reloaded) = self
                            .read_provider_transport_snapshot_uncached(
                                &current_transport.provider.id,
                                &current_transport.endpoint.id,
                                &current_transport.key.id,
                            )
                            .await?
                        else {
                            return Ok(None);
                        };
                        current_transport = reloaded;
                        continue;
                    }
                }
            } else {
                None
            };
            let mut resolution = match self
                .oauth_refresh
                .resolve_with_result(
                    &executor,
                    &current_transport,
                    Some(distributed_lock),
                    Some(lock_owner.as_str()),
                )
                .await
            {
                Ok(resolution) => resolution,
                Err(provider_transport::LocalOAuthRefreshError::HttpStatus {
                    status_code,
                    body_excerpt,
                    ..
                }) if matches!(status_code, 400 | 401 | 403) => {
                    if self
                        .persist_local_oauth_refresh_failure_state(
                            &current_transport,
                            status_code,
                            body_excerpt.as_str(),
                            false,
                        )
                        .await
                        .is_err()
                    {
                        tracing::warn!(
                            key_id = %current_transport.key.id,
                            provider_type = %current_transport.provider.provider_type,
                            "gateway local oauth refresh failure persistence failed"
                        );
                    }
                    return Ok(None);
                }
                Err(err) => {
                    return Err(local_oauth_refresh_gateway_error(&err));
                }
            };

            if resolution
                .as_ref()
                .is_some_and(|resolution| resolution.refresh_in_flight)
            {
                let Some(reloaded_transport) = self
                    .wait_for_remote_oauth_refresh(&current_transport)
                    .await?
                else {
                    continue;
                };
                current_transport = reloaded_transport;
                continue;
            }

            if let Some(refreshed_entry) = resolution
                .as_ref()
                .and_then(|resolution| resolution.refreshed_entry.as_ref())
                .cloned()
            {
                if provider_transport::is_codex_agent_identity_transport(&initial_transport)
                    && !provider_transport::codex_agent_identity_entry_allows_task_rotation_from(
                        &initial_transport,
                        &refreshed_entry,
                    )
                {
                    discard_failed_local_oauth_refresh_resolution(&mut resolution);
                    self.release_local_oauth_refresh_lease(
                        resolution
                            .as_mut()
                            .and_then(|resolution| resolution.distributed_lease.take()),
                    )
                    .await;
                    return Ok(None);
                }
                if self
                    .persist_local_oauth_refresh_entry(
                        &current_transport,
                        &refreshed_entry,
                        expected_credential_fence.as_ref(),
                    )
                    .await
                    .is_err()
                {
                    tracing::warn!(
                        key_id = %current_transport.key.id,
                        provider_type = %current_transport.provider.provider_type,
                        "gateway local oauth refresh persistence failed"
                    );
                    let _ = self
                        .invalidate_local_oauth_refresh_entry(&current_transport.key.id)
                        .await;
                    discard_failed_local_oauth_refresh_resolution(&mut resolution);
                } else {
                    self.oauth_refresh
                        .store_cached_entry(
                            current_transport.key.id.trim(),
                            refreshed_entry.clone(),
                        )
                        .await;
                }
            }

            self.release_local_oauth_refresh_lease(
                resolution
                    .as_mut()
                    .and_then(|resolution| resolution.distributed_lease.take()),
            )
            .await;

            return Ok(resolution.and_then(|resolution| resolution.auth));
        }

        Ok(None)
    }

    pub(crate) async fn force_local_oauth_refresh_entry(
        &self,
        transport: &provider_transport::GatewayProviderTransportSnapshot,
    ) -> Result<
        Option<provider_transport::CachedOAuthEntry>,
        provider_transport::LocalOAuthRefreshError,
    > {
        let distributed_lock = self.runtime_state.as_ref();
        let lock_owner = format!("aether-gateway-admin-{}", std::process::id());
        let initial_transport = transport.clone();
        let mut current_transport = transport.clone();
        let expected_refresh_fingerprint = self
            .oauth_refresh
            .refresh_fingerprint_for_transport(&initial_transport);
        let executor = GatewayLocalOAuthHttpExecutor { state: self };
        let transport_refresh_token_fingerprint = oauth_auth_config_refresh_token_fingerprint(
            current_transport.key.decrypted_auth_config.as_deref(),
        )
        .unwrap_or_else(|| "-".to_string());
        tracing::info!(
            key_id = %current_transport.key.id,
            provider_id = %current_transport.provider.id,
            provider_type = %current_transport.provider.provider_type,
            transport_refresh_token_fingerprint = %transport_refresh_token_fingerprint,
            has_transport_auth_config = current_transport
                .key
                .decrypted_auth_config
                .as_deref()
                .map(str::trim)
                .is_some_and(|value| !value.is_empty()),
            "gateway manual oauth refresh starting"
        );

        for _ in 0..2 {
            if !local_oauth_transport_context_allows_reload(&initial_transport, &current_transport)
            {
                return Ok(None);
            }
            let expected_credential_fence = match self
                .capture_provider_transport_credential_fence(&current_transport)
                .await
                .map_err(
                    |err| provider_transport::LocalOAuthRefreshError::InvalidResponse {
                        provider_type: "gateway",
                        message: format!("{err:?}"),
                    },
                )? {
                Some(fence) => Some(fence),
                None if current_transport.key.decrypted_auth_config.is_some() => {
                    let Some(reloaded) = self
                        .read_provider_transport_snapshot_uncached(
                            &current_transport.provider.id,
                            &current_transport.endpoint.id,
                            &current_transport.key.id,
                        )
                        .await
                        .map_err(|err| {
                            provider_transport::LocalOAuthRefreshError::InvalidResponse {
                                provider_type: "gateway",
                                message: format!("{err:?}"),
                            }
                        })?
                    else {
                        return Ok(None);
                    };
                    current_transport = reloaded;
                    continue;
                }
                None => None,
            };
            let mut resolution = self
                .oauth_refresh
                .force_refresh_with_result_fenced(
                    &executor,
                    &current_transport,
                    Some(distributed_lock),
                    Some(lock_owner.as_str()),
                    expected_refresh_fingerprint.as_deref(),
                )
                .await?;

            if resolution
                .as_ref()
                .is_some_and(|resolution| resolution.refresh_in_flight)
            {
                let Some(reloaded_transport) = self
                    .wait_for_remote_oauth_refresh(&current_transport)
                    .await
                    .map_err(
                        |err| provider_transport::LocalOAuthRefreshError::InvalidResponse {
                            provider_type: "gateway",
                            message: format!("{err:?}"),
                        },
                    )?
                else {
                    continue;
                };
                current_transport = reloaded_transport;
                continue;
            }

            if resolution
                .as_ref()
                .is_some_and(|resolution| resolution.reused_refresh)
            {
                let reused_entry = resolution
                    .as_ref()
                    .and_then(|resolution| resolution.refreshed_entry.clone());
                if reused_entry.as_ref().is_some_and(|entry| {
                    provider_transport::is_codex_agent_identity_transport(&initial_transport)
                        && !provider_transport::codex_agent_identity_entry_allows_task_rotation_from(
                            &initial_transport,
                            entry,
                        )
                }) {
                    self.release_local_oauth_refresh_lease(
                        resolution
                            .as_mut()
                            .and_then(|resolution| resolution.distributed_lease.take()),
                    )
                    .await;
                    return Ok(None);
                }
                if let Some(entry) = reused_entry.as_ref() {
                    self.oauth_refresh
                        .store_cached_entry(current_transport.key.id.trim(), entry.clone())
                        .await;
                }
                self.release_local_oauth_refresh_lease(
                    resolution
                        .as_mut()
                        .and_then(|resolution| resolution.distributed_lease.take()),
                )
                .await;
                return Ok(reused_entry);
            }

            if let Some(refreshed_entry) = resolution
                .as_ref()
                .and_then(|resolution| resolution.refreshed_entry.as_ref())
                .cloned()
            {
                if provider_transport::is_codex_agent_identity_transport(&initial_transport)
                    && !provider_transport::codex_agent_identity_entry_allows_task_rotation_from(
                        &initial_transport,
                        &refreshed_entry,
                    )
                {
                    self.release_local_oauth_refresh_lease(
                        resolution
                            .as_mut()
                            .and_then(|resolution| resolution.distributed_lease.take()),
                    )
                    .await;
                    return Ok(None);
                }
                if self
                    .persist_local_oauth_refresh_entry(
                        &current_transport,
                        &refreshed_entry,
                        expected_credential_fence.as_ref(),
                    )
                    .await
                    .is_err()
                {
                    tracing::warn!(
                        key_id = %current_transport.key.id,
                        provider_type = %current_transport.provider.provider_type,
                        "gateway manual oauth refresh persistence failed"
                    );
                    let _ = self
                        .invalidate_local_oauth_refresh_entry(&current_transport.key.id)
                        .await;
                    self.release_local_oauth_refresh_lease(
                        resolution
                            .as_mut()
                            .and_then(|resolution| resolution.distributed_lease.take()),
                    )
                    .await;
                    return Err(
                        provider_transport::LocalOAuthRefreshError::InvalidResponse {
                            provider_type: "gateway",
                            message: "local oauth refresh persistence failed".to_string(),
                        },
                    );
                }
                self.oauth_refresh
                    .store_cached_entry(current_transport.key.id.trim(), refreshed_entry.clone())
                    .await;
                self.release_local_oauth_refresh_lease(
                    resolution
                        .as_mut()
                        .and_then(|resolution| resolution.distributed_lease.take()),
                )
                .await;
                return Ok(Some(refreshed_entry));
            }

            self.release_local_oauth_refresh_lease(
                resolution
                    .as_mut()
                    .and_then(|resolution| resolution.distributed_lease.take()),
            )
            .await;

            return Ok(None);
        }

        Ok(None)
    }

    pub(crate) async fn invalidate_local_oauth_refresh_entry(&self, key_id: &str) -> bool {
        self.oauth_refresh.invalidate_cached_entry(key_id).await
    }

    async fn release_local_oauth_refresh_lease(&self, lease: Option<RuntimeLockLease>) {
        let Some(lease) = lease else {
            return;
        };
        if self.runtime_state.lock_release(&lease).await.is_err() {
            tracing::warn!(
                key_id = %lease.key,
                "gateway local oauth refresh distributed lease release failed"
            );
        }
    }

    pub(crate) async fn capture_agent_identity_auth_config_fence(
        &self,
        transport: &provider_transport::GatewayProviderTransportSnapshot,
    ) -> Result<AgentIdentityAuthConfigFence, GatewayError> {
        if !provider_transport::is_codex_agent_identity_transport(transport) {
            return Ok(AgentIdentityAuthConfigFence::NotAgentIdentity);
        }
        match self
            .capture_provider_transport_auth_config_fence(transport)
            .await?
        {
            Some(_) => Ok(AgentIdentityAuthConfigFence::Current),
            None => Ok(AgentIdentityAuthConfigFence::StaleGeneration),
        }
    }

    pub(crate) async fn capture_provider_transport_auth_config_fence(
        &self,
        transport: &provider_transport::GatewayProviderTransportSnapshot,
    ) -> Result<Option<String>, GatewayError> {
        Ok(self
            .capture_provider_transport_credential_fence(transport)
            .await?
            .map(|fence| fence.encrypted_auth_config))
    }

    pub(crate) async fn capture_provider_transport_credential_fence(
        &self,
        transport: &provider_transport::GatewayProviderTransportSnapshot,
    ) -> Result<Option<ProviderTransportCredentialFence>, GatewayError> {
        let key_id = transport.key.id.trim();
        let stored = self
            .data
            .list_provider_catalog_keys_by_ids(&[key_id.to_string()])
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))?
            .into_iter()
            .next();
        let Some(stored) = stored else {
            return Ok(None);
        };
        if stored.provider_id != transport.provider.id
            || stored.auth_type != transport.key.auth_type
        {
            return Ok(None);
        }
        let provider = self
            .data
            .list_provider_catalog_providers_by_ids(std::slice::from_ref(&stored.provider_id))
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))?
            .into_iter()
            .next();
        let Some(provider) = provider else {
            return Ok(None);
        };
        if provider.provider_type != transport.provider.provider_type {
            return Ok(None);
        }

        let stored_api_key = self
            .decrypt_provider_catalog_key_api_key(&stored)
            .map_err(|_| {
                GatewayError::Internal(
                    "provider api_key could not be verified for runtime fencing".to_string(),
                )
            })?;
        let transport_api_key = (!transport.key.decrypted_api_key.is_empty())
            .then_some(transport.key.decrypted_api_key.as_str());
        if stored_api_key.as_deref() != transport_api_key {
            return Ok(None);
        }

        let Some(ciphertext) = stored.encrypted_auth_config.as_deref() else {
            return Ok(None);
        };
        let plaintext = self
            .decrypt_provider_catalog_key_auth_config(&stored)
            .map_err(|_| {
                GatewayError::Internal(
                    "provider auth_config could not be verified for runtime fencing".to_string(),
                )
            })?
            .ok_or_else(|| {
                GatewayError::Internal(
                    "provider auth_config could not be verified for runtime fencing".to_string(),
                )
            })?;
        let config = serde_json::from_str::<Value>(&plaintext)
            .map_err(|err| GatewayError::Internal(err.to_string()))?;
        let transport_config = transport
            .key
            .decrypted_auth_config
            .as_deref()
            .and_then(|value| serde_json::from_str::<Value>(value).ok())
            .ok_or_else(|| {
                GatewayError::Internal(
                    "provider transport auth_config could not be verified for runtime fencing"
                        .to_string(),
                )
            })?;
        // Fingerprints intentionally ignore unrelated JSON fields. The fence,
        // however, must reject an admin rewrite that keeps the same key pair
        // and task while changing metadata or policy fields.
        if config != transport_config {
            return Ok(None);
        }
        Ok(Some(ProviderTransportCredentialFence {
            encrypted_auth_config: ciphertext.to_string(),
            credential: ProviderCatalogKeyOAuthCredentialFence {
                encrypted_api_key: stored.encrypted_api_key,
                auth_type: stored.auth_type,
                provider_id: stored.provider_id,
                provider_type: provider.provider_type,
            },
        }))
    }

    pub(crate) async fn mark_provider_transport_oauth_invalid_fenced(
        &self,
        transport: &provider_transport::GatewayProviderTransportSnapshot,
        invalid_reason: &str,
        request_started_at_unix_ms: Option<u64>,
        request_order_id: Option<&str>,
        observed_credential_generation: Option<&str>,
    ) -> Result<bool, GatewayError> {
        let invalid_reason = invalid_reason.trim();
        let key_id = transport.key.id.trim();
        let provider_type = transport.provider.provider_type.as_str();
        if invalid_reason.is_empty() || key_id.is_empty() {
            return Ok(false);
        }
        let Some(expected_credential_fence) = self
            .capture_provider_transport_credential_fence(transport)
            .await?
        else {
            return Ok(false);
        };
        let request_order_id = request_order_id
            .map(str::trim)
            .filter(|value| !value.is_empty());
        let is_codex = provider_type.trim().eq_ignore_ascii_case("codex");
        let now_unix_secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .ok()
            .map(|duration| duration.as_secs())
            .unwrap_or(0);
        for attempt in 0..CODEX_OAUTH_INVALIDATION_CAS_MAX_ATTEMPTS {
            let Some(mut latest_key) = self
                .data
                .list_provider_catalog_keys_by_ids(&[key_id.to_string()])
                .await
                .map_err(|err| GatewayError::Internal(err.to_string()))?
                .into_iter()
                .next()
            else {
                return Ok(false);
            };
            if latest_key.encrypted_auth_config.as_deref()
                != Some(expected_credential_fence.encrypted_auth_config.as_str())
                || latest_key.encrypted_api_key
                    != expected_credential_fence.credential.encrypted_api_key
                || latest_key.auth_type != expected_credential_fence.credential.auth_type
                || latest_key.provider_id != expected_credential_fence.credential.provider_id
                || !provider_key_is_oauth_managed(&latest_key, provider_type)
            {
                return Ok(false);
            }

            let expected_codex = is_codex
                .then(|| {
                    latest_key
                        .upstream_metadata
                        .as_ref()
                        .and_then(Value::as_object)
                        .and_then(|metadata| metadata.get("codex"))
                        .cloned()
                })
                .flatten();
            let expected_codex_object = expected_codex.as_ref().and_then(Value::as_object);
            if is_codex
                && !admin_provider_quota_pure::codex_credential_generation_matches(
                    expected_codex.as_ref(),
                    observed_credential_generation,
                )
            {
                return Ok(false);
            }
            if is_codex
                && request_started_at_unix_ms.is_some()
                && admin_provider_quota_pure::codex_oauth_state_request_order_is_stale(
                    expected_codex_object,
                    request_started_at_unix_ms,
                    request_order_id,
                )
            {
                return Ok(false);
            }

            let (oauth_invalid_at_unix_secs, oauth_invalid_reason) =
                merge_runtime_oauth_invalid_state(
                    provider_type,
                    &latest_key,
                    invalid_reason,
                    now_unix_secs,
                );
            let invalid_state_changed = oauth_invalid_at_unix_secs
                != latest_key.oauth_invalid_at_unix_secs
                || oauth_invalid_reason != latest_key.oauth_invalid_reason;
            let next_codex = (is_codex && request_started_at_unix_ms.is_some()).then(|| {
                let mut codex = expected_codex_object.cloned().unwrap_or_default();
                codex.insert(
                    admin_provider_quota_pure::CODEX_OAUTH_STATE_REQUEST_WATERMARK_KEY.to_string(),
                    json!(request_started_at_unix_ms.unwrap_or_default()),
                );
                if let Some(request_order_id) = request_order_id {
                    codex.insert(
                        admin_provider_quota_pure::CODEX_OAUTH_STATE_REQUEST_WATERMARK_ID_KEY
                            .to_string(),
                        json!(request_order_id),
                    );
                } else {
                    codex.remove(
                        admin_provider_quota_pure::CODEX_OAUTH_STATE_REQUEST_WATERMARK_ID_KEY,
                    );
                }
                Value::Object(codex)
            });
            if !invalid_state_changed && next_codex.is_none() {
                return Ok(false);
            }

            latest_key.oauth_invalid_at_unix_secs = oauth_invalid_at_unix_secs;
            latest_key.oauth_invalid_reason = oauth_invalid_reason;
            latest_key.updated_at_unix_secs = Some(now_unix_secs);
            let current_status_snapshot = latest_key.status_snapshot.take();
            latest_key.status_snapshot =
                sync_provider_key_oauth_status_snapshot(current_status_snapshot, &latest_key);

            let updated = self
                .compare_and_update_provider_catalog_key_oauth_runtime_state(
                    &ProviderCatalogKeyOAuthRuntimeStateCasUpdate {
                        key_id: key_id.to_string(),
                        expected_encrypted_auth_config: Some(
                            expected_credential_fence.encrypted_auth_config.clone(),
                        ),
                        expected_credential: Some(expected_credential_fence.credential.clone()),
                        expected_upstream_metadata_namespace: is_codex.then(|| {
                            ProviderCatalogUpstreamMetadataNamespaceExpectation {
                                namespace: "codex".to_string(),
                                expected_value: expected_codex.clone(),
                            }
                        }),
                        encrypted_auth_config: expected_credential_fence
                            .encrypted_auth_config
                            .clone(),
                        encrypted_api_key_update: None,
                        expires_at_unix_secs_update: None,
                        oauth_invalid_at_unix_secs: latest_key.oauth_invalid_at_unix_secs,
                        oauth_invalid_reason: latest_key.oauth_invalid_reason.clone(),
                        upstream_metadata_patch: next_codex
                            .as_ref()
                            .map(|codex| json!({"codex": codex})),
                        upstream_metadata_namespace_to_remove: None,
                        status_snapshot_patch: provider_key_oauth_status_snapshot_update(
                            &latest_key,
                        )
                        .status_snapshot_patch,
                        reset_error_count: false,
                        updated_at_unix_secs: latest_key.updated_at_unix_secs,
                    },
                )
                .await?;
            if !updated {
                if attempt + 1 < CODEX_OAUTH_INVALIDATION_CAS_MAX_ATTEMPTS {
                    tokio::task::yield_now().await;
                }
                continue;
            }

            let delete_namespace_expectation =
                is_codex.then(|| ProviderCatalogUpstreamMetadataNamespaceExpectation {
                    namespace: "codex".to_string(),
                    expected_value: next_codex.or(expected_codex),
                });
            let auto_removed = if admin_provider_quota_pure::provider_auto_remove_banned_keys(
                transport.provider.config.as_ref(),
            ) && (!is_codex
                || oauth_invalid_reason_is_account_block(
                    latest_key.oauth_invalid_reason.as_deref(),
                ))
                && admin_provider_quota_pure::should_auto_remove_oauth_invalid_key(
                    &latest_key,
                    None,
                    true,
                    now_unix_secs,
                ) {
                self.delete_provider_transport_oauth_credential_fenced(
                    key_id,
                    &expected_credential_fence,
                    delete_namespace_expectation,
                )
                .await?
            } else {
                false
            };
            let _ = self.invalidate_local_oauth_refresh_entry(key_id).await;
            tracing::info!(
                key_id,
                provider_id = %transport.provider.id,
                provider_type,
                updated = true,
                auto_removed,
                "gateway fenced OAuth invalidation persisted"
            );
            return Ok(true);
        }
        Ok(false)
    }

    pub(crate) async fn mark_provider_transport_oauth_success_fenced(
        &self,
        transport: &provider_transport::GatewayProviderTransportSnapshot,
        request_started_at_unix_ms: Option<u64>,
        request_order_id: Option<&str>,
        observed_credential_generation: Option<&str>,
    ) -> Result<bool, GatewayError> {
        let key_id = transport.key.id.trim();
        if key_id.is_empty()
            || !transport
                .provider
                .provider_type
                .trim()
                .eq_ignore_ascii_case("codex")
            || !transport.key.auth_type.trim().eq_ignore_ascii_case("oauth")
            || provider_transport::is_codex_agent_identity_transport(transport)
        {
            return Ok(false);
        }
        let Some(request_started_at_unix_ms) = request_started_at_unix_ms else {
            // Without an order fence, an older 2xx could clear a newer 401.
            return Ok(false);
        };
        let Some(request_order_id) = request_order_id
            .map(str::trim)
            .filter(|value| !value.is_empty())
        else {
            return Ok(false);
        };
        let Some(expected_credential_fence) = self
            .capture_provider_transport_credential_fence(transport)
            .await?
        else {
            return Ok(false);
        };
        let now_unix_secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .ok()
            .map(|duration| duration.as_secs())
            .unwrap_or(0);

        for attempt in 0..CODEX_OAUTH_INVALIDATION_CAS_MAX_ATTEMPTS {
            let Some(mut latest_key) = self
                .data
                .list_provider_catalog_keys_by_ids(&[key_id.to_string()])
                .await
                .map_err(|err| GatewayError::Internal(err.to_string()))?
                .into_iter()
                .next()
            else {
                return Ok(false);
            };
            if latest_key.encrypted_auth_config.as_deref()
                != Some(expected_credential_fence.encrypted_auth_config.as_str())
                || latest_key.encrypted_api_key
                    != expected_credential_fence.credential.encrypted_api_key
                || latest_key.auth_type != expected_credential_fence.credential.auth_type
                || latest_key.provider_id != expected_credential_fence.credential.provider_id
                || !provider_key_is_oauth_managed(&latest_key, "codex")
            {
                return Ok(false);
            }

            let expected_codex = match latest_key.upstream_metadata.as_ref() {
                Some(Value::Object(metadata)) => metadata.get("codex").cloned(),
                Some(_) => return Ok(false),
                None => None,
            };
            let expected_codex_object = match expected_codex.as_ref() {
                Some(Value::Object(codex)) => Some(codex),
                Some(_) => return Ok(false),
                None => None,
            };
            if !admin_provider_quota_pure::codex_credential_generation_matches(
                expected_codex.as_ref(),
                observed_credential_generation,
            ) || admin_provider_quota_pure::codex_oauth_success_request_order_is_stale(
                expected_codex_object,
                Some(request_started_at_unix_ms),
                Some(request_order_id),
            ) {
                return Ok(false);
            }

            let mut next_codex = expected_codex_object.cloned().unwrap_or_default();
            next_codex.insert(
                admin_provider_quota_pure::CODEX_OAUTH_STATE_REQUEST_WATERMARK_KEY.to_string(),
                json!(request_started_at_unix_ms),
            );
            next_codex.insert(
                admin_provider_quota_pure::CODEX_OAUTH_STATE_REQUEST_WATERMARK_ID_KEY.to_string(),
                json!(request_order_id),
            );
            let next_codex = Value::Object(next_codex);

            (
                latest_key.oauth_invalid_at_unix_secs,
                latest_key.oauth_invalid_reason,
            ) = local_oauth_refresh_success_invalid_state(&latest_key);
            latest_key.updated_at_unix_secs = Some(now_unix_secs);
            let current_status_snapshot = latest_key.status_snapshot.take();
            latest_key.status_snapshot =
                sync_provider_key_oauth_status_snapshot(current_status_snapshot, &latest_key);

            let updated = self
                .compare_and_update_provider_catalog_key_oauth_runtime_state(
                    &ProviderCatalogKeyOAuthRuntimeStateCasUpdate {
                        key_id: key_id.to_string(),
                        expected_encrypted_auth_config: Some(
                            expected_credential_fence.encrypted_auth_config.clone(),
                        ),
                        expected_credential: Some(expected_credential_fence.credential.clone()),
                        expected_upstream_metadata_namespace: Some(
                            ProviderCatalogUpstreamMetadataNamespaceExpectation {
                                namespace: "codex".to_string(),
                                expected_value: expected_codex,
                            },
                        ),
                        encrypted_auth_config: expected_credential_fence
                            .encrypted_auth_config
                            .clone(),
                        encrypted_api_key_update: None,
                        expires_at_unix_secs_update: None,
                        oauth_invalid_at_unix_secs: latest_key.oauth_invalid_at_unix_secs,
                        oauth_invalid_reason: latest_key.oauth_invalid_reason.clone(),
                        upstream_metadata_patch: Some(json!({"codex": next_codex})),
                        upstream_metadata_namespace_to_remove: None,
                        status_snapshot_patch: provider_key_oauth_status_snapshot_update(
                            &latest_key,
                        )
                        .status_snapshot_patch,
                        reset_error_count: false,
                        updated_at_unix_secs: latest_key.updated_at_unix_secs,
                    },
                )
                .await?;
            if updated {
                tracing::info!(
                    key_id,
                    provider_id = %transport.provider.id,
                    request_started_at_unix_ms,
                    request_order_id,
                    "gateway fenced OAuth success persisted"
                );
                return Ok(true);
            }
            if attempt + 1 < CODEX_OAUTH_INVALIDATION_CAS_MAX_ATTEMPTS {
                tokio::task::yield_now().await;
            }
        }
        Ok(false)
    }

    async fn delete_provider_transport_oauth_credential_fenced(
        &self,
        key_id: &str,
        expected: &ProviderTransportCredentialFence,
        expected_upstream_metadata_namespace: Option<
            ProviderCatalogUpstreamMetadataNamespaceExpectation,
        >,
    ) -> Result<bool, GatewayError> {
        let deleted = self
            .compare_and_delete_provider_catalog_key_oauth_credential(
                &ProviderCatalogKeyOAuthCredentialCasDelete {
                    key_id: key_id.to_string(),
                    expected_encrypted_auth_config: Some(expected.encrypted_auth_config.clone()),
                    expected_credential: expected.credential.clone(),
                    expected_upstream_metadata_namespace,
                },
            )
            .await?;
        if !deleted {
            return Ok(false);
        }
        let deleted_key_ids = [key_id.to_string()];
        self.cleanup_deleted_provider_catalog_refs(
            &expected.credential.provider_id,
            false,
            &[],
            &deleted_key_ids,
        )
        .await?;
        self.clear_provider_transport_snapshot_cache();
        let _ = self.invalidate_local_oauth_refresh_entry(key_id).await;
        Ok(true)
    }

    async fn persist_local_oauth_refresh_entry(
        &self,
        transport: &provider_transport::GatewayProviderTransportSnapshot,
        entry: &provider_transport::CachedOAuthEntry,
        expected_credential_fence: Option<&ProviderTransportCredentialFence>,
    ) -> Result<(), GatewayError> {
        let key_id = transport.key.id.trim();
        if key_id.is_empty() {
            return Ok(());
        }

        if local_oauth_refresh_entry_should_stay_memory_only(transport, entry) {
            let expected_credential_fence = expected_credential_fence.ok_or_else(|| {
                GatewayError::Internal(
                    "memory-only OAuth refresh has no starting credential fence".to_string(),
                )
            })?;
            let current_credential_fence = self
                .capture_provider_transport_credential_fence(transport)
                .await?;
            if current_credential_fence.as_ref() != Some(expected_credential_fence) {
                return Err(GatewayError::Internal(
                    "OAuth credential changed while memory-only refresh was in flight".to_string(),
                ));
            }
            tracing::info!(
                key_id = %key_id,
                provider_id = %transport.provider.id,
                provider_type = %transport.provider.provider_type,
                expires_at_unix_secs = ?entry.expires_at_unix_secs,
                "gateway local oauth refresh entry kept in memory only"
            );
            return Ok(());
        }

        if self.data.encryption_key().is_none() {
            return Ok(());
        }

        if provider_transport::is_codex_agent_identity_cached_entry(entry) {
            let metadata = entry.metadata.as_ref().ok_or_else(|| {
                GatewayError::Internal(
                    "Agent Identity task registration produced no auth_config".to_string(),
                )
            })?;
            provider_transport::validate_codex_agent_identity_auth_config(metadata)
                .map_err(GatewayError::Internal)?;
            let auth_config = serde_json::to_string(metadata)
                .map_err(|err| GatewayError::Internal(err.to_string()))?;
            let encrypted_auth_config = self.seal_provider_catalog_key_auth_config(
                &transport.provider.id,
                key_id,
                &auth_config,
            )?;

            let source_fingerprint = entry.source_fingerprint.as_deref().ok_or_else(|| {
                GatewayError::Internal(
                    "Agent Identity task registration omitted its credential fingerprint"
                        .to_string(),
                )
            })?;
            let transport_fingerprint =
                provider_transport::codex_agent_identity_transport_credential_fingerprint(
                    transport,
                )
                .ok_or_else(|| {
                    GatewayError::Internal(
                        "Agent Identity transport credential fingerprint is unavailable"
                            .to_string(),
                    )
                })?;
            if source_fingerprint != transport_fingerprint {
                return Err(GatewayError::Internal(
                    "Agent Identity credential changed while task registration was in flight"
                        .to_string(),
                ));
            }

            let Some(mut latest_key) = self
                .data
                .list_provider_catalog_keys_by_ids(&[key_id.to_string()])
                .await
                .map_err(|err| GatewayError::Internal(err.to_string()))?
                .into_iter()
                .next()
            else {
                return Ok(());
            };
            let expected_credential_fence = expected_credential_fence.ok_or_else(|| {
                GatewayError::Internal(
                    "Agent Identity task registration has no starting credential fence".to_string(),
                )
            })?;
            let expected_encrypted_auth_config =
                expected_credential_fence.encrypted_auth_config.clone();
            if latest_key.encrypted_auth_config.as_deref()
                != Some(expected_encrypted_auth_config.as_str())
                || latest_key.encrypted_api_key
                    != expected_credential_fence.credential.encrypted_api_key
                || latest_key.auth_type != expected_credential_fence.credential.auth_type
                || latest_key.provider_id != expected_credential_fence.credential.provider_id
            {
                return Err(GatewayError::Internal(
                    "Agent Identity credential changed while task registration was in flight"
                        .to_string(),
                ));
            }
            let latest_auth_config = self
                .decrypt_provider_catalog_key_auth_config(&latest_key)?
                .and_then(|value| serde_json::from_str::<Value>(&value).ok())
                .ok_or_else(|| {
                    GatewayError::Internal(
                        "Agent Identity current auth_config could not be verified".to_string(),
                    )
                })?;
            let latest_fingerprint =
                provider_transport::codex_agent_identity_credential_fingerprint(
                    &latest_auth_config,
                )
                .ok_or_else(|| {
                    GatewayError::Internal(
                        "Agent Identity current credential fingerprint is unavailable".to_string(),
                    )
                })?;
            if latest_fingerprint != source_fingerprint {
                return Err(GatewayError::Internal(
                    "Agent Identity credential changed before task registration persistence"
                        .to_string(),
                ));
            }
            let (oauth_invalid_at_unix_secs, oauth_invalid_reason) =
                local_oauth_refresh_success_invalid_state(&latest_key);
            latest_key.oauth_invalid_at_unix_secs = oauth_invalid_at_unix_secs;
            latest_key.oauth_invalid_reason = oauth_invalid_reason;
            latest_key.updated_at_unix_secs = Some(
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .ok()
                    .map(|duration| duration.as_secs())
                    .unwrap_or(0),
            );
            let current_status_snapshot = latest_key.status_snapshot.take();
            latest_key.status_snapshot =
                sync_provider_key_oauth_status_snapshot(current_status_snapshot, &latest_key);
            let updated = self
                .compare_and_update_provider_catalog_key_oauth_runtime_state(
                    &ProviderCatalogKeyOAuthRuntimeStateCasUpdate {
                        key_id: key_id.to_string(),
                        expected_encrypted_auth_config: Some(expected_encrypted_auth_config),
                        expected_credential: Some(expected_credential_fence.credential.clone()),
                        expected_upstream_metadata_namespace: None,
                        encrypted_auth_config,
                        encrypted_api_key_update: None,
                        expires_at_unix_secs_update: None,
                        oauth_invalid_at_unix_secs: latest_key.oauth_invalid_at_unix_secs,
                        oauth_invalid_reason: latest_key.oauth_invalid_reason.clone(),
                        upstream_metadata_patch: None,
                        upstream_metadata_namespace_to_remove: None,
                        status_snapshot_patch: provider_key_oauth_status_snapshot_update(
                            &latest_key,
                        )
                        .status_snapshot_patch,
                        reset_error_count: false,
                        updated_at_unix_secs: latest_key.updated_at_unix_secs,
                    },
                )
                .await?;
            if !updated {
                return Err(GatewayError::Internal(
                    "Agent Identity credential changed during task registration persistence"
                        .to_string(),
                ));
            }
            tracing::info!(
                key_id = %key_id,
                provider_id = %transport.provider.id,
                provider_type = %transport.provider.provider_type,
                updated,
                "gateway Agent Identity task registration persisted"
            );
            return Ok(());
        }

        let access_token = entry
            .auth_header_value
            .trim()
            .strip_prefix("Bearer ")
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| {
                GatewayError::Internal(
                    "local oauth refresh produced non-bearer auth header".to_string(),
                )
            })?;

        let encrypted_api_key =
            self.seal_provider_catalog_key_api_key(&transport.provider.id, key_id, access_token)?;
        let encrypted_auth_config = entry
            .metadata
            .as_ref()
            .map(serde_json::to_string)
            .transpose()
            .map_err(|err| GatewayError::Internal(err.to_string()))?
            .map(|value| {
                self.seal_provider_catalog_key_auth_config(
                    &transport.provider.id,
                    key_id,
                    value.as_str(),
                )
            })
            .transpose()?;
        let expected_credential_fence = expected_credential_fence.ok_or_else(|| {
            GatewayError::Internal(
                "OAuth refresh persistence is missing its credential fence".to_string(),
            )
        })?;
        let encrypted_auth_config = encrypted_auth_config.ok_or_else(|| {
            GatewayError::Internal(
                "OAuth refresh persistence is missing its auth_config".to_string(),
            )
        })?;

        let Some(mut latest_key) = self
            .data
            .list_provider_catalog_keys_by_ids(&[key_id.to_string()])
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))?
            .into_iter()
            .next()
        else {
            return Ok(());
        };

        let observed_credential_matches = latest_key.encrypted_auth_config.as_deref()
            == Some(expected_credential_fence.encrypted_auth_config.as_str())
            && latest_key.encrypted_api_key
                == expected_credential_fence.credential.encrypted_api_key
            && latest_key.auth_type == expected_credential_fence.credential.auth_type
            && latest_key.provider_id == expected_credential_fence.credential.provider_id;
        latest_key.encrypted_api_key = Some(encrypted_api_key.clone());
        latest_key.encrypted_auth_config = Some(encrypted_auth_config.clone());
        latest_key.expires_at_unix_secs = entry.expires_at_unix_secs;
        let (oauth_invalid_at_unix_secs, oauth_invalid_reason) =
            local_oauth_refresh_success_invalid_state(&latest_key);
        latest_key.oauth_invalid_at_unix_secs = oauth_invalid_at_unix_secs;
        latest_key.oauth_invalid_reason = oauth_invalid_reason;
        latest_key.updated_at_unix_secs = Some(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .ok()
                .map(|duration| duration.as_secs())
                .unwrap_or(0),
        );
        let current_status_snapshot = latest_key.status_snapshot.take();
        latest_key.status_snapshot =
            sync_provider_key_oauth_status_snapshot(current_status_snapshot, &latest_key);
        let updated = if !observed_credential_matches {
            false
        } else {
            self.compare_and_update_provider_catalog_key_oauth_runtime_state(
                &ProviderCatalogKeyOAuthRuntimeStateCasUpdate {
                    key_id: key_id.to_string(),
                    expected_encrypted_auth_config: Some(
                        expected_credential_fence.encrypted_auth_config.clone(),
                    ),
                    expected_credential: Some(expected_credential_fence.credential.clone()),
                    expected_upstream_metadata_namespace: None,
                    encrypted_auth_config,
                    encrypted_api_key_update: Some(encrypted_api_key.clone()),
                    expires_at_unix_secs_update: Some(entry.expires_at_unix_secs),
                    oauth_invalid_at_unix_secs: latest_key.oauth_invalid_at_unix_secs,
                    oauth_invalid_reason: latest_key.oauth_invalid_reason.clone(),
                    upstream_metadata_patch: None,
                    upstream_metadata_namespace_to_remove: None,
                    status_snapshot_patch: provider_key_oauth_status_snapshot_update(&latest_key)
                        .status_snapshot_patch,
                    reset_error_count: false,
                    updated_at_unix_secs: latest_key.updated_at_unix_secs,
                },
            )
            .await?
        };
        if !updated {
            return Err(GatewayError::Internal(
                "OAuth credential changed during refresh persistence".to_string(),
            ));
        }
        let metadata_refresh_token_fingerprint =
            oauth_metadata_refresh_token_fingerprint(entry.metadata.as_ref())
                .unwrap_or_else(|| "-".to_string());
        tracing::info!(
            key_id = %key_id,
            provider_id = %transport.provider.id,
            provider_type = %transport.provider.provider_type,
            updated,
            metadata_has_refresh_token = entry
                .metadata
                .as_ref()
                .and_then(|value| value.as_object())
                .and_then(|object| object.get("refresh_token"))
                .and_then(|value| value.as_str())
                .map(str::trim)
                .is_some_and(|value| !value.is_empty()),
            metadata_refresh_token_fingerprint = %metadata_refresh_token_fingerprint,
            expires_at_unix_secs = ?entry.expires_at_unix_secs,
            cleared_provider_transport_snapshot_cache = updated,
            "gateway local oauth refresh entry persisted"
        );
        Ok(())
    }

    pub(crate) async fn persist_local_oauth_refresh_failure_state_observed(
        &self,
        transport: &provider_transport::GatewayProviderTransportSnapshot,
        status_code: u16,
        body_excerpt: &str,
        access_token_invalid_proven: bool,
        observation: CodexRuntimeOAuthObservation<'_>,
    ) -> Result<bool, GatewayError> {
        let key_id = transport.key.id.trim();
        let request_order_id = observation.request_order_id.trim();
        if key_id.is_empty()
            || request_order_id.is_empty()
            || !transport
                .provider
                .provider_type
                .trim()
                .eq_ignore_ascii_case("codex")
            || !transport.key.auth_type.trim().eq_ignore_ascii_case("oauth")
            || provider_transport::is_codex_agent_identity_transport(transport)
        {
            return Ok(false);
        }
        let Some(expected_credential_fence) = self
            .capture_provider_transport_credential_fence(transport)
            .await?
        else {
            return Ok(false);
        };
        let refresh_reason = format!(
            "{OAUTH_REFRESH_FAILED_PREFIX}Token 续期失败 ({status_code}): {}",
            normalize_local_oauth_refresh_error_message(Some(status_code), Some(body_excerpt))
        );
        let now_unix_secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .ok()
            .map(|duration| duration.as_secs())
            .unwrap_or(0);

        for attempt in 0..CODEX_OAUTH_INVALIDATION_CAS_MAX_ATTEMPTS {
            let Some(mut latest_key) = self
                .data
                .list_provider_catalog_keys_by_ids(&[key_id.to_string()])
                .await
                .map_err(|err| GatewayError::Internal(err.to_string()))?
                .into_iter()
                .next()
            else {
                return Ok(false);
            };
            if latest_key.encrypted_auth_config.as_deref()
                != Some(expected_credential_fence.encrypted_auth_config.as_str())
                || latest_key.encrypted_api_key
                    != expected_credential_fence.credential.encrypted_api_key
                || latest_key.auth_type != expected_credential_fence.credential.auth_type
                || latest_key.provider_id != expected_credential_fence.credential.provider_id
                || !provider_key_is_oauth_managed(&latest_key, "codex")
            {
                return Ok(false);
            }

            let expected_codex = match latest_key.upstream_metadata.as_ref() {
                Some(Value::Object(metadata)) => metadata.get("codex").cloned(),
                Some(_) => return Ok(false),
                None => None,
            };
            let expected_codex_object = match expected_codex.as_ref() {
                Some(Value::Object(codex)) => Some(codex),
                Some(_) => return Ok(false),
                None => None,
            };
            if !admin_provider_quota_pure::codex_credential_generation_matches(
                expected_codex.as_ref(),
                observation.observed_credential_generation,
            ) || admin_provider_quota_pure::codex_oauth_state_request_order_is_stale(
                expected_codex_object,
                Some(observation.request_started_at_unix_ms),
                Some(request_order_id),
            ) {
                return Ok(false);
            }

            let mut next_codex = expected_codex_object.cloned().unwrap_or_default();
            next_codex.insert(
                admin_provider_quota_pure::CODEX_OAUTH_STATE_REQUEST_WATERMARK_KEY.to_string(),
                json!(observation.request_started_at_unix_ms),
            );
            next_codex.insert(
                admin_provider_quota_pure::CODEX_OAUTH_STATE_REQUEST_WATERMARK_ID_KEY.to_string(),
                json!(request_order_id),
            );
            let next_codex = Value::Object(next_codex);

            if let Some(runtime_invalid_reason) = observation
                .runtime_invalid_reason
                .map(str::trim)
                .filter(|value| !value.is_empty())
            {
                (
                    latest_key.oauth_invalid_at_unix_secs,
                    latest_key.oauth_invalid_reason,
                ) = merge_runtime_oauth_invalid_state(
                    "codex",
                    &latest_key,
                    runtime_invalid_reason,
                    now_unix_secs,
                );
            }
            if let Some(merged_reason) = merge_local_oauth_refresh_failure_reason(
                latest_key.oauth_invalid_reason.as_deref(),
                &refresh_reason,
            ) {
                latest_key.oauth_invalid_at_unix_secs = latest_key
                    .oauth_invalid_at_unix_secs
                    .or(Some(now_unix_secs));
                latest_key.oauth_invalid_reason = Some(merged_reason);
            }
            latest_key.updated_at_unix_secs = Some(now_unix_secs);
            let current_status_snapshot = latest_key.status_snapshot.take();
            latest_key.status_snapshot =
                sync_provider_key_oauth_status_snapshot(current_status_snapshot, &latest_key);

            let updated = self
                .compare_and_update_provider_catalog_key_oauth_runtime_state(
                    &ProviderCatalogKeyOAuthRuntimeStateCasUpdate {
                        key_id: key_id.to_string(),
                        expected_encrypted_auth_config: Some(
                            expected_credential_fence.encrypted_auth_config.clone(),
                        ),
                        expected_credential: Some(expected_credential_fence.credential.clone()),
                        expected_upstream_metadata_namespace: Some(
                            ProviderCatalogUpstreamMetadataNamespaceExpectation {
                                namespace: "codex".to_string(),
                                expected_value: expected_codex,
                            },
                        ),
                        encrypted_auth_config: expected_credential_fence
                            .encrypted_auth_config
                            .clone(),
                        encrypted_api_key_update: None,
                        expires_at_unix_secs_update: None,
                        oauth_invalid_at_unix_secs: latest_key.oauth_invalid_at_unix_secs,
                        oauth_invalid_reason: latest_key.oauth_invalid_reason.clone(),
                        upstream_metadata_patch: Some(json!({"codex": next_codex.clone()})),
                        upstream_metadata_namespace_to_remove: None,
                        status_snapshot_patch: provider_key_oauth_status_snapshot_update(
                            &latest_key,
                        )
                        .status_snapshot_patch,
                        reset_error_count: false,
                        updated_at_unix_secs: latest_key.updated_at_unix_secs,
                    },
                )
                .await?;
            if !updated {
                if attempt + 1 < CODEX_OAUTH_INVALIDATION_CAS_MAX_ATTEMPTS {
                    tokio::task::yield_now().await;
                }
                continue;
            }

            let auto_removed = if admin_provider_quota_pure::provider_auto_remove_banned_keys(
                transport.provider.config.as_ref(),
            ) && oauth_invalid_reason_is_account_block(
                latest_key.oauth_invalid_reason.as_deref(),
            )
                && admin_provider_quota_pure::should_auto_remove_oauth_invalid_key(
                    &latest_key,
                    None,
                    access_token_invalid_proven,
                    now_unix_secs,
                ) {
                self.delete_provider_transport_oauth_credential_fenced(
                    key_id,
                    &expected_credential_fence,
                    Some(ProviderCatalogUpstreamMetadataNamespaceExpectation {
                        namespace: "codex".to_string(),
                        expected_value: Some(next_codex),
                    }),
                )
                .await?
            } else {
                false
            };
            let _ = self.invalidate_local_oauth_refresh_entry(key_id).await;
            tracing::info!(
                key_id,
                provider_id = %transport.provider.id,
                status_code,
                request_started_at_unix_ms = observation.request_started_at_unix_ms,
                request_order_id,
                updated = true,
                auto_removed,
                "gateway observed local OAuth refresh failure state persisted"
            );
            return Ok(auto_removed);
        }
        Ok(false)
    }

    pub(crate) async fn persist_local_oauth_refresh_failure_state(
        &self,
        transport: &provider_transport::GatewayProviderTransportSnapshot,
        status_code: u16,
        body_excerpt: &str,
        access_token_invalid_proven: bool,
    ) -> Result<bool, GatewayError> {
        let key_id = transport.key.id.trim();
        if key_id.is_empty() {
            return Ok(false);
        }

        let transport_has_auth_config = transport
            .key
            .decrypted_auth_config
            .as_deref()
            .map(str::trim)
            .is_some_and(|value| !value.is_empty());
        let expected_credential_fence = if transport_has_auth_config {
            self.capture_provider_transport_credential_fence(transport)
                .await?
        } else {
            None
        };
        if transport_has_auth_config && expected_credential_fence.is_none() {
            return Ok(false);
        }

        let Some(mut latest_key) = self
            .data
            .list_provider_catalog_keys_by_ids(&[key_id.to_string()])
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))?
            .into_iter()
            .next()
        else {
            return Ok(false);
        };

        if !provider_key_is_oauth_managed(&latest_key, transport.provider.provider_type.as_str()) {
            return Ok(false);
        }

        if expected_credential_fence.as_ref().is_some_and(|expected| {
            latest_key.encrypted_auth_config.as_deref()
                != Some(expected.encrypted_auth_config.as_str())
                || latest_key.encrypted_api_key != expected.credential.encrypted_api_key
                || latest_key.auth_type != expected.credential.auth_type
                || latest_key.provider_id != expected.credential.provider_id
        }) {
            return Ok(false);
        }

        let refresh_reason = format!(
            "{OAUTH_REFRESH_FAILED_PREFIX}Token 续期失败 ({status_code}): {}",
            normalize_local_oauth_refresh_error_message(Some(status_code), Some(body_excerpt))
        );
        let Some(merged_reason) = merge_local_oauth_refresh_failure_reason(
            latest_key.oauth_invalid_reason.as_deref(),
            &refresh_reason,
        ) else {
            return Ok(false);
        };

        let now_unix_secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .ok()
            .map(|duration| duration.as_secs())
            .unwrap_or(0);
        let mut updated = false;
        if latest_key.oauth_invalid_reason.as_deref() != Some(merged_reason.as_str())
            || latest_key.oauth_invalid_at_unix_secs.is_none()
        {
            latest_key.oauth_invalid_at_unix_secs = latest_key
                .oauth_invalid_at_unix_secs
                .or(Some(now_unix_secs));
            latest_key.oauth_invalid_reason = Some(merged_reason);
            latest_key.updated_at_unix_secs = Some(now_unix_secs);
            let current_status_snapshot = latest_key.status_snapshot.take();
            latest_key.status_snapshot =
                sync_provider_key_oauth_status_snapshot(current_status_snapshot, &latest_key);

            if let Some(expected_credential_fence) = expected_credential_fence.as_ref() {
                updated = self
                    .compare_and_update_provider_catalog_key_oauth_runtime_state(
                        &ProviderCatalogKeyOAuthRuntimeStateCasUpdate {
                            key_id: key_id.to_string(),
                            expected_encrypted_auth_config: Some(
                                expected_credential_fence.encrypted_auth_config.clone(),
                            ),
                            expected_credential: Some(expected_credential_fence.credential.clone()),
                            expected_upstream_metadata_namespace: None,
                            encrypted_auth_config: expected_credential_fence
                                .encrypted_auth_config
                                .clone(),
                            encrypted_api_key_update: None,
                            expires_at_unix_secs_update: None,
                            oauth_invalid_at_unix_secs: latest_key.oauth_invalid_at_unix_secs,
                            oauth_invalid_reason: latest_key.oauth_invalid_reason.clone(),
                            upstream_metadata_patch: None,
                            upstream_metadata_namespace_to_remove: None,
                            status_snapshot_patch: provider_key_oauth_status_snapshot_update(
                                &latest_key,
                            )
                            .status_snapshot_patch,
                            reset_error_count: false,
                            updated_at_unix_secs: latest_key.updated_at_unix_secs,
                        },
                    )
                    .await?;
                if !updated {
                    return Ok(false);
                }
            } else {
                updated = self
                    .update_provider_catalog_key_oauth_runtime_state(
                        key_id,
                        latest_key.oauth_invalid_at_unix_secs,
                        latest_key.oauth_invalid_reason.as_deref(),
                        latest_key.updated_at_unix_secs,
                    )
                    .await?;
                if updated {
                    updated = self
                        .update_provider_catalog_key_status_snapshot(
                            &provider_key_oauth_status_snapshot_update(&latest_key),
                        )
                        .await?;
                }
            }
            if updated {
                self.clear_provider_transport_snapshot_cache();
                let _ = self.invalidate_local_oauth_refresh_entry(key_id).await;
            }
        }

        let should_auto_remove = admin_provider_quota_pure::provider_auto_remove_banned_keys(
            transport.provider.config.as_ref(),
        )
            && admin_provider_quota_pure::should_auto_remove_oauth_invalid_key(
                &latest_key,
                None,
                access_token_invalid_proven,
                now_unix_secs,
            );
        let auto_removed = if !should_auto_remove {
            false
        } else if let Some(expected_credential_fence) = expected_credential_fence {
            self.delete_provider_transport_oauth_credential_fenced(
                key_id,
                &expected_credential_fence,
                None,
            )
            .await?
        } else if !transport
            .provider
            .provider_type
            .trim()
            .eq_ignore_ascii_case("codex")
        {
            self.clear_provider_transport_snapshot_cache();
            if self.delete_provider_catalog_key(key_id).await? {
                let deleted_key_ids = [key_id.to_string()];
                self.cleanup_deleted_provider_catalog_refs(
                    &transport.provider.id,
                    false,
                    &[],
                    &deleted_key_ids,
                )
                .await?;
                let _ = self.invalidate_local_oauth_refresh_entry(key_id).await;
                true
            } else {
                false
            }
        } else {
            false
        };
        tracing::info!(
            key_id = %key_id,
            provider_id = %transport.provider.id,
            provider_type = %transport.provider.provider_type,
            status_code,
            updated,
            auto_removed,
            cleared_provider_transport_snapshot_cache = updated || auto_removed,
            "gateway local oauth refresh failure state persisted"
        );
        Ok(auto_removed)
    }

    async fn execute_local_oauth_http_request(
        &self,
        provider_type: &'static str,
        transport: &provider_transport::GatewayProviderTransportSnapshot,
        request: &provider_transport::LocalOAuthHttpRequest,
    ) -> Result<
        provider_transport::LocalOAuthHttpResponse,
        provider_transport::LocalOAuthRefreshError,
    > {
        if local_oauth_request_uses_direct_client(request.url.as_str()) {
            let executor =
                provider_transport::ReqwestLocalOAuthHttpExecutor::new(self.client.clone());
            return executor.execute(provider_type, transport, request).await;
        }

        let body = if let Some(json_body) = request.json_body.clone() {
            RequestBody::from_json(json_body)
        } else {
            RequestBody {
                json_body: None,
                body_bytes_b64: request
                    .body_bytes
                    .as_ref()
                    .map(|bytes| STANDARD.encode(bytes)),
                body_ref: None,
            }
        };
        let proxy_snapshot = self
            .resolve_transport_proxy_snapshot_with_tunnel_affinity(transport)
            .await;
        let proxy_is_tunnel = local_oauth_proxy_is_tunnel(proxy_snapshot.as_ref());
        let mut headers = request.headers.clone();
        headers.insert(
            EXECUTION_REQUEST_FOLLOW_REDIRECTS_HEADER.to_string(),
            "true".to_string(),
        );
        if proxy_is_tunnel {
            headers.insert(
                EXECUTION_REQUEST_HTTP1_ONLY_HEADER.to_string(),
                "true".to_string(),
            );
        }
        let plan = ExecutionPlan {
            request_id: request.request_id.to_string(),
            candidate_id: None,
            provider_name: Some(transport.provider.name.clone()),
            provider_id: transport.provider.id.clone(),
            endpoint_id: transport.endpoint.id.clone(),
            key_id: transport.key.id.clone(),
            method: request.method.as_str().to_string(),
            url: request.url.clone(),
            headers,
            content_type: request
                .headers
                .get("content-type")
                .map(String::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(ToOwned::to_owned),
            content_encoding: None,
            body,
            stream: false,
            client_api_format: "provider_oauth:local_refresh".to_string(),
            provider_api_format: "provider_oauth:local_refresh".to_string(),
            model_name: Some(provider_type.to_string()),
            proxy: proxy_snapshot,
            transport_profile: None,
            timeouts: Some(ExecutionTimeouts {
                connect_ms: Some(LOCAL_OAUTH_HTTP_TIMEOUT_MS),
                read_ms: Some(LOCAL_OAUTH_HTTP_TIMEOUT_MS),
                write_ms: Some(LOCAL_OAUTH_HTTP_TIMEOUT_MS),
                pool_ms: Some(LOCAL_OAUTH_HTTP_TIMEOUT_MS),
                total_ms: Some(LOCAL_OAUTH_HTTP_TIMEOUT_MS),
                ..ExecutionTimeouts::default()
            }),
        };
        let (request_refresh_token_fingerprint, request_refresh_token_len) =
            local_oauth_request_refresh_token_fingerprint(request);
        tracing::info!(
            key_id = %transport.key.id,
            provider_id = %transport.provider.id,
            endpoint_id = %transport.endpoint.id,
            provider_type,
            request_id = %request.request_id,
            method = %plan.method,
            token_origin = %crate::handlers::shared::security_log_url_origin(&plan.url),
            content_type = plan.content_type.as_deref().unwrap_or("-"),
            body_bytes_len = ?request.body_bytes.as_ref().map(Vec::len),
            json_body_present = request.json_body.is_some(),
            request_refresh_token_fingerprint = request_refresh_token_fingerprint
                .as_deref()
                .unwrap_or("-"),
            request_refresh_token_len = ?request_refresh_token_len,
            proxy_node_id = ?plan.proxy.as_ref().and_then(|proxy| proxy.node_id.as_deref()),
            proxy_mode = plan.proxy.as_ref().and_then(|proxy| proxy.mode.as_deref()).unwrap_or("-"),
            proxy_enabled = ?plan.proxy.as_ref().and_then(|proxy| proxy.enabled),
            proxy_url_present = plan
                .proxy
                .as_ref()
                .and_then(|proxy| proxy.url.as_deref())
                .map(str::trim)
                .is_some_and(|value| !value.is_empty()),
            proxy_is_tunnel,
            tunnel_base_url_present = local_oauth_proxy_extra_string(
                plan.proxy.as_ref(),
                "tunnel_base_url"
            )
            .is_some(),
            tunnel_owner_instance_id = local_oauth_proxy_extra_string(
                plan.proxy.as_ref(),
                "tunnel_owner_instance_id"
            )
            .unwrap_or("-"),
            follow_redirects = plan
                .headers
                .get(EXECUTION_REQUEST_FOLLOW_REDIRECTS_HEADER)
                .map(String::as_str)
                .unwrap_or("-"),
            http1_only = plan
                .headers
                .get(EXECUTION_REQUEST_HTTP1_ONLY_HEADER)
                .map(String::as_str)
                .unwrap_or("-"),
            "gateway local oauth execution request prepared"
        );
        let bounded_plan = crate::execution_runtime::transport::with_upstream_response_body_limit(
            &plan,
            LOCAL_OAUTH_RESPONSE_BODY_LIMIT_BYTES,
        );
        let result = crate::execution_runtime::execute_execution_runtime_sync_plan(
            self,
            None,
            &bounded_plan,
        )
        .await
        .map_err(
            |err| provider_transport::LocalOAuthRefreshError::InvalidResponse {
                provider_type,
                message: err.into_message(),
            },
        )?;
        let response_body_text = local_oauth_execution_body_text(&result);
        if (200..300).contains(&result.status_code) {
            tracing::info!(
                key_id = %transport.key.id,
                provider_id = %transport.provider.id,
                endpoint_id = %transport.endpoint.id,
                provider_type,
                request_id = %request.request_id,
                status_code = result.status_code,
                request_refresh_token_fingerprint = request_refresh_token_fingerprint
                    .as_deref()
                    .unwrap_or("-"),
                "gateway local oauth execution response received"
            );
        } else {
            tracing::warn!(
                key_id = %transport.key.id,
                provider_id = %transport.provider.id,
                endpoint_id = %transport.endpoint.id,
                provider_type,
                request_id = %request.request_id,
                status_code = result.status_code,
                request_refresh_token_fingerprint = request_refresh_token_fingerprint
                    .as_deref()
                    .unwrap_or("-"),
                body_excerpt = %if request.request_id
                    == provider_transport::CODEX_AGENT_IDENTITY_TASK_REGISTRATION_REQUEST_ID
                {
                    "[redacted]".to_string()
                } else {
                    local_oauth_log_excerpt(response_body_text.as_str())
                },
                "gateway local oauth execution response returned error"
            );
        }
        Ok(provider_transport::LocalOAuthHttpResponse {
            status_code: result.status_code,
            body_text: response_body_text,
        })
    }

    async fn wait_for_remote_oauth_refresh(
        &self,
        transport: &provider_transport::GatewayProviderTransportSnapshot,
    ) -> Result<Option<provider_transport::GatewayProviderTransportSnapshot>, GatewayError> {
        if !self.data.has_provider_catalog_reader() {
            return Ok(None);
        }

        let deadline = tokio::time::Instant::now() + REMOTE_OAUTH_REFRESH_WAIT_TIMEOUT;
        loop {
            let Some(reloaded_transport) = self
                .read_provider_transport_snapshot_uncached(
                    &transport.provider.id,
                    &transport.endpoint.id,
                    &transport.key.id,
                )
                .await?
            else {
                return Ok(None);
            };

            if provider_transport_snapshot_looks_refreshed(transport, &reloaded_transport) {
                return Ok(Some(reloaded_transport));
            }

            let now = tokio::time::Instant::now();
            if now >= deadline {
                break;
            }
            tokio::time::sleep(REMOTE_OAUTH_REFRESH_POLL_INTERVAL.min(deadline - now)).await;
        }

        Ok(None)
    }
}

fn provider_key_oauth_status_snapshot_update(
    key: &StoredProviderCatalogKey,
) -> ProviderCatalogKeyStatusSnapshotUpdate {
    let oauth = key
        .status_snapshot
        .as_ref()
        .and_then(Value::as_object)
        .and_then(|snapshot| snapshot.get("oauth"))
        .cloned()
        .unwrap_or(Value::Null);
    ProviderCatalogKeyStatusSnapshotUpdate {
        key_id: key.id.clone(),
        status_snapshot_patch: json!({"oauth":oauth}),
        updated_at_unix_secs: key.updated_at_unix_secs,
    }
}

fn merge_runtime_oauth_invalid_state(
    provider_type: &str,
    key: &StoredProviderCatalogKey,
    invalid_reason: &str,
    now_unix_secs: u64,
) -> (Option<u64>, Option<String>) {
    let candidate_reason = invalid_reason.trim();
    if candidate_reason.is_empty() {
        return (
            key.oauth_invalid_at_unix_secs,
            key.oauth_invalid_reason.clone(),
        );
    }

    if provider_type.trim().eq_ignore_ascii_case("codex") {
        return admin_provider_quota_pure::codex_build_invalid_state(
            key,
            candidate_reason.to_string(),
            now_unix_secs,
        );
    }

    let current_reason = key
        .oauth_invalid_reason
        .as_deref()
        .map(str::trim)
        .unwrap_or_default();
    if current_reason == candidate_reason {
        return (
            key.oauth_invalid_at_unix_secs,
            (!current_reason.is_empty()).then_some(current_reason.to_string()),
        );
    }

    (Some(now_unix_secs), Some(candidate_reason.to_string()))
}

fn local_oauth_execution_body_text(result: &aether_contracts::ExecutionResult) -> String {
    result
        .body
        .as_ref()
        .and_then(|body| local_oauth_execution_body_bytes(&result.headers, body))
        .map(|bytes| String::from_utf8_lossy(&bytes).to_string())
        .or_else(|| {
            result
                .body
                .as_ref()
                .and_then(|body| body.json_body.as_ref())
                .and_then(|value| serde_json::to_string(value).ok())
        })
        .unwrap_or_default()
}

fn local_oauth_execution_body_bytes(
    headers: &BTreeMap<String, String>,
    body: &aether_contracts::ResponseBody,
) -> Option<Vec<u8>> {
    let bytes = body.body_bytes_b64.as_deref().and_then(|value| {
        crate::execution_runtime::transport::decode_base64_body_with_limit(
            value,
            LOCAL_OAUTH_RESPONSE_BODY_LIMIT_BYTES,
        )
        .ok()
    })?;
    crate::execution_runtime::transport::decode_response_body_bytes_with_limit(
        headers,
        &bytes,
        LOCAL_OAUTH_RESPONSE_BODY_LIMIT_BYTES,
    )
    .ok()
    .map(std::borrow::Cow::into_owned)
}

fn local_oauth_request_uses_direct_client(url: &str) -> bool {
    reqwest::Url::parse(url)
        .ok()
        .and_then(|parsed| parsed.host_str().map(str::to_owned))
        .is_some_and(|host| {
            host.eq_ignore_ascii_case("localhost")
                || host
                    .parse::<std::net::IpAddr>()
                    .map(|addr| addr.is_loopback())
                    .unwrap_or(false)
        })
}

#[cfg(test)]
#[path = "oauth/tests.rs"]
mod tests;
