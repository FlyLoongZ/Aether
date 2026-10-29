use crate::handlers::admin::request::AdminAppState;
use crate::provider_key_auth::provider_key_is_oauth_managed;
use aether_data_contracts::repository::provider_catalog::StoredProviderCatalogKey;
use aether_runtime_state::RuntimeLockLease;
use axum::http;
use sha2::{Digest, Sha256};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use uuid::Uuid;

const CODEX_OAUTH_ACCOUNT_LOCK_TTL: Duration = Duration::from_secs(180);
const CLAUDE_OAUTH_ACCOUNT_LOCK_TTL: Duration = Duration::from_secs(180);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CodexOAuthAccountLockError {
    MissingIdentity,
    Contended,
    Unavailable,
}

impl CodexOAuthAccountLockError {
    pub(crate) const fn status_code(self) -> http::StatusCode {
        match self {
            Self::MissingIdentity => http::StatusCode::BAD_REQUEST,
            Self::Contended => http::StatusCode::CONFLICT,
            Self::Unavailable => http::StatusCode::SERVICE_UNAVAILABLE,
        }
    }

    pub(crate) const fn detail(self) -> &'static str {
        match self {
            Self::MissingIdentity => "Codex 账号身份字段缺失，无法安全写入授权",
            Self::Contended => "该 ChatGPT 账号正在更新授权，请稍后重试",
            Self::Unavailable => "Codex 账号授权锁暂不可用，请稍后重试",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ClaudeOAuthAccountLockError {
    MissingIdentity,
    Contended,
    Unavailable,
}

impl ClaudeOAuthAccountLockError {
    pub(crate) const fn status_code(self) -> http::StatusCode {
        match self {
            Self::MissingIdentity => http::StatusCode::BAD_REQUEST,
            Self::Contended => http::StatusCode::CONFLICT,
            Self::Unavailable => http::StatusCode::SERVICE_UNAVAILABLE,
        }
    }

    pub(crate) const fn detail(self) -> &'static str {
        match self {
            Self::MissingIdentity => "Claude 账号身份字段缺失，无法安全写入授权",
            Self::Contended => "该 Claude 账号正在更新授权，请稍后重试",
            Self::Unavailable => "Claude 账号授权锁暂不可用，请稍后重试",
        }
    }
}

fn normalize_codex_plan_group_for_provider_oauth(
    plan_type: Option<&serde_json::Value>,
) -> Option<String> {
    let normalized = plan_type
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())?
        .to_ascii_lowercase();
    match normalized.as_str() {
        "free" => Some("free".to_string()),
        "team" | "plus" | "enterprise" => Some("team_plus_enterprise".to_string()),
        _ => None,
    }
}

fn normalize_provider_oauth_identity_value(value: Option<&serde_json::Value>) -> Option<String> {
    value
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

fn normalize_provider_oauth_identity_value_from_keys(
    auth_config: &serde_json::Map<String, serde_json::Value>,
    keys: &[&str],
) -> Option<String> {
    keys.iter()
        .find_map(|key| normalize_provider_oauth_identity_value(auth_config.get(*key)))
}

fn codex_agent_identity_account_lock_key(
    provider_id: &str,
    identity_kind: &str,
    identity_parts: &[&str],
) -> String {
    let mut digest = Sha256::new();
    digest.update(provider_id.trim().as_bytes());
    digest.update([0]);
    digest.update(identity_kind.as_bytes());
    for part in identity_parts {
        digest.update([0]);
        digest.update(part.as_bytes());
    }
    format!(
        "provider_oauth_agent_identity_account:{:x}",
        digest.finalize()
    )
}

pub(crate) fn codex_agent_identity_account_lock_keys(
    provider_id: &str,
    auth_config: &serde_json::Map<String, serde_json::Value>,
) -> Vec<String> {
    let account_user_id = normalize_provider_oauth_identity_value_from_keys(
        auth_config,
        &[
            "account_user_id",
            "accountUserId",
            "chatgpt_account_user_id",
            "chatgptAccountUserId",
        ],
    );
    let account_id = normalize_provider_oauth_identity_value_from_keys(
        auth_config,
        &[
            "account_id",
            "accountId",
            "chatgpt_account_id",
            "chatgptAccountId",
        ],
    );
    let user_id = normalize_provider_oauth_identity_value_from_keys(
        auth_config,
        &["user_id", "userId", "chatgpt_user_id", "chatgptUserId"],
    );
    let email = normalize_provider_oauth_identity_value_from_keys(auth_config, &["email"]);

    let mut keys = Vec::with_capacity(5);
    if let Some(account_user_id) = account_user_id.as_deref() {
        keys.push(codex_agent_identity_account_lock_key(
            provider_id,
            "account_user_id",
            &[account_user_id],
        ));
    }
    if let (Some(account_id), Some(user_id)) = (account_id.as_deref(), user_id.as_deref()) {
        keys.push(codex_agent_identity_account_lock_key(
            provider_id,
            "account_id_user_id",
            &[account_id, user_id],
        ));
    }
    if let (Some(account_id), Some(email)) = (account_id.as_deref(), email.as_deref()) {
        keys.push(codex_agent_identity_account_lock_key(
            provider_id,
            "account_id_email",
            &[account_id, email],
        ));
    }
    if let Some(user_id) = user_id.as_deref() {
        keys.push(codex_agent_identity_account_lock_key(
            provider_id,
            "user_id",
            &[user_id],
        ));
    }
    if let Some(email) = email.as_deref() {
        keys.push(codex_agent_identity_account_lock_key(
            provider_id,
            "email",
            &[email],
        ));
    }
    keys.sort_unstable();
    keys.dedup();
    keys
}

pub(crate) async fn acquire_codex_oauth_account_locks(
    state: &AdminAppState<'_>,
    provider_id: &str,
    auth_config: &serde_json::Map<String, serde_json::Value>,
    operation: &str,
) -> Result<Vec<RuntimeLockLease>, CodexOAuthAccountLockError> {
    let lock_keys = codex_agent_identity_account_lock_keys(provider_id, auth_config);
    if lock_keys.is_empty() {
        return Err(CodexOAuthAccountLockError::MissingIdentity);
    }

    let owner = format!(
        "aether-gateway-codex-oauth-{}-{}",
        operation.trim(),
        Uuid::new_v4()
    );
    let mut leases = Vec::with_capacity(lock_keys.len());
    for lock_key in lock_keys {
        match state
            .runtime_state()
            .lock_try_acquire(
                lock_key.as_str(),
                owner.as_str(),
                CODEX_OAUTH_ACCOUNT_LOCK_TTL,
            )
            .await
        {
            Ok(Some(lease)) => leases.push(lease),
            Ok(None) => {
                release_codex_oauth_account_locks(state, leases).await;
                return Err(CodexOAuthAccountLockError::Contended);
            }
            Err(_) => {
                tracing::warn!(
                    provider_id = %provider_id,
                    lock_key = %lock_key,
                    operation,
                    "gateway Codex OAuth account lock unavailable"
                );
                release_codex_oauth_account_locks(state, leases).await;
                return Err(CodexOAuthAccountLockError::Unavailable);
            }
        }
    }

    // The lock is distributed, while the catalog cache is process-local. A
    // fresh read inside the lease is required to observe the previous holder.
    state.app().data.clear_provider_catalog_cache();
    Ok(leases)
}

pub(crate) async fn release_codex_oauth_account_locks(
    state: &AdminAppState<'_>,
    leases: Vec<RuntimeLockLease>,
) {
    release_provider_oauth_account_locks(state, leases).await;
}

pub(crate) async fn release_provider_oauth_account_locks(
    state: &AdminAppState<'_>,
    leases: Vec<RuntimeLockLease>,
) {
    for lease in leases.into_iter().rev() {
        match state.runtime_state().lock_release(&lease).await {
            Ok(true) => {}
            Ok(false) => tracing::warn!(
                lock_key = %lease.key,
                "gateway provider OAuth account lock was not owned during release"
            ),
            Err(_) => tracing::warn!(
                lock_key = %lease.key,
                "gateway provider OAuth account lock release failed"
            ),
        }
    }
}

fn claude_oauth_account_lock_key(
    provider_id: &str,
    auth_config: &serde_json::Map<String, serde_json::Value>,
) -> Option<String> {
    let (identity_kind, identity) = normalize_provider_oauth_identity_value_from_keys(
        auth_config,
        &["account_uuid", "accountUuid"],
    )
    .map(|value| ("account_uuid", value))
    .or_else(|| {
        normalize_provider_oauth_identity_value_from_keys(auth_config, &["email"])
            .map(|value| ("email", value.to_ascii_lowercase()))
    })?;
    let mut digest = Sha256::new();
    digest.update(provider_id.trim().as_bytes());
    digest.update([0]);
    digest.update(identity_kind.as_bytes());
    digest.update([0]);
    digest.update(identity.as_bytes());
    Some(format!(
        "provider_oauth_claude_account:{:x}",
        digest.finalize()
    ))
}

pub(crate) async fn acquire_claude_oauth_account_lock(
    state: &AdminAppState<'_>,
    provider_id: &str,
    auth_config: &serde_json::Map<String, serde_json::Value>,
    operation: &str,
) -> Result<Vec<RuntimeLockLease>, ClaudeOAuthAccountLockError> {
    let Some(lock_key) = claude_oauth_account_lock_key(provider_id, auth_config) else {
        return Err(ClaudeOAuthAccountLockError::MissingIdentity);
    };
    let owner = format!(
        "aether-gateway-claude-oauth-{}-{}",
        operation.trim(),
        Uuid::new_v4()
    );
    let lease = match state
        .runtime_state()
        .lock_try_acquire(
            lock_key.as_str(),
            owner.as_str(),
            CLAUDE_OAUTH_ACCOUNT_LOCK_TTL,
        )
        .await
    {
        Ok(Some(lease)) => lease,
        Ok(None) => return Err(ClaudeOAuthAccountLockError::Contended),
        Err(_) => {
            tracing::warn!(
                provider_id = %provider_id,
                lock_key = %lock_key,
                operation,
                "gateway Claude OAuth account lock unavailable"
            );
            return Err(ClaudeOAuthAccountLockError::Unavailable);
        }
    };

    state.app().data.clear_provider_catalog_cache();
    Ok(vec![lease])
}

fn is_openai_provider_oauth_provider_type(value: Option<&serde_json::Value>) -> bool {
    value
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .is_some_and(|provider_type| provider_type.eq_ignore_ascii_case("codex"))
}

fn is_claude_provider_oauth_provider_type(value: Option<&serde_json::Value>) -> bool {
    value
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .is_some_and(|provider_type| provider_type.eq_ignore_ascii_case("claude_code"))
}

fn match_claude_provider_oauth_identity(
    new_auth_config: &serde_json::Map<String, serde_json::Value>,
    existing_auth_config: &serde_json::Map<String, serde_json::Value>,
) -> Option<bool> {
    if !is_claude_provider_oauth_provider_type(new_auth_config.get("provider_type"))
        && !is_claude_provider_oauth_provider_type(existing_auth_config.get("provider_type"))
    {
        return None;
    }

    let new_account_uuid = normalize_provider_oauth_identity_value_from_keys(
        new_auth_config,
        &["account_uuid", "accountUuid"],
    );
    let existing_account_uuid = normalize_provider_oauth_identity_value_from_keys(
        existing_auth_config,
        &["account_uuid", "accountUuid"],
    );
    match (new_account_uuid, existing_account_uuid) {
        (Some(left), Some(right)) => Some(left == right),
        _ => None,
    }
}

fn match_codex_provider_oauth_identity(
    new_auth_config: &serde_json::Map<String, serde_json::Value>,
    existing_auth_config: &serde_json::Map<String, serde_json::Value>,
) -> Option<bool> {
    let new_provider_type = new_auth_config.get("provider_type");
    let existing_provider_type = existing_auth_config.get("provider_type");
    if !is_openai_provider_oauth_provider_type(new_provider_type)
        && !is_openai_provider_oauth_provider_type(existing_provider_type)
    {
        return None;
    }

    let new_agent_runtime_id = normalize_provider_oauth_identity_value(
        new_auth_config
            .get("agent_runtime_id")
            .or_else(|| new_auth_config.get("agentRuntimeId")),
    );
    let existing_agent_runtime_id = normalize_provider_oauth_identity_value(
        existing_auth_config
            .get("agent_runtime_id")
            .or_else(|| existing_auth_config.get("agentRuntimeId")),
    );
    if new_agent_runtime_id
        .as_deref()
        .zip(existing_agent_runtime_id.as_deref())
        .is_some_and(|(left, right)| left == right)
    {
        return Some(true);
    }

    let new_account_user_id =
        normalize_provider_oauth_identity_value(new_auth_config.get("account_user_id"));
    let existing_account_user_id =
        normalize_provider_oauth_identity_value(existing_auth_config.get("account_user_id"));
    if let (Some(new_account_user_id), Some(existing_account_user_id)) =
        (new_account_user_id, existing_account_user_id)
    {
        return Some(new_account_user_id == existing_account_user_id);
    }

    let new_account_id = normalize_provider_oauth_identity_value(new_auth_config.get("account_id"));
    let existing_account_id =
        normalize_provider_oauth_identity_value(existing_auth_config.get("account_id"));
    let new_user_id = normalize_provider_oauth_identity_value(new_auth_config.get("user_id"));
    let existing_user_id =
        normalize_provider_oauth_identity_value(existing_auth_config.get("user_id"));
    let new_email = normalize_provider_oauth_identity_value(new_auth_config.get("email"));
    let existing_email = normalize_provider_oauth_identity_value(existing_auth_config.get("email"));

    if let (Some(new_account_id), Some(existing_account_id)) =
        (new_account_id.as_deref(), existing_account_id.as_deref())
    {
        if new_account_id != existing_account_id {
            return Some(false);
        }
    }

    if let (
        Some(new_account_id),
        Some(existing_account_id),
        Some(new_user_id),
        Some(existing_user_id),
    ) = (
        new_account_id.as_deref(),
        existing_account_id.as_deref(),
        new_user_id.as_deref(),
        existing_user_id.as_deref(),
    ) {
        return Some(new_account_id == existing_account_id && new_user_id == existing_user_id);
    }

    if let (
        Some(new_account_id),
        Some(existing_account_id),
        Some(new_email),
        Some(existing_email),
    ) = (
        new_account_id.as_deref(),
        existing_account_id.as_deref(),
        new_email.as_deref(),
        existing_email.as_deref(),
    ) {
        return Some(new_account_id == existing_account_id && new_email == existing_email);
    }

    None
}

fn is_codex_cross_plan_group_non_duplicate(
    new_auth_config: &serde_json::Map<String, serde_json::Value>,
    existing_auth_config: &serde_json::Map<String, serde_json::Value>,
) -> bool {
    let new_provider_type = new_auth_config.get("provider_type");
    let existing_provider_type = existing_auth_config.get("provider_type");
    if !is_openai_provider_oauth_provider_type(new_provider_type)
        && !is_openai_provider_oauth_provider_type(existing_provider_type)
    {
        return false;
    }

    let new_group = normalize_codex_plan_group_for_provider_oauth(new_auth_config.get("plan_type"));
    let existing_group =
        normalize_codex_plan_group_for_provider_oauth(existing_auth_config.get("plan_type"));
    matches!(
        (new_group.as_deref(), existing_group.as_deref()),
        (Some(left), Some(right)) if left != right
    )
}

fn provider_oauth_invalid_reason_allows_replace(reason: &str) -> bool {
    reason.lines().map(str::trim).any(|line| {
        line.starts_with("[OAUTH_EXPIRED] ")
            || line.starts_with("[REFRESH_FAILED] ")
            || line.contains("Token 无效或已过期")
            || line.contains("refresh_token 无效、已过期或已撤销")
    })
}

fn existing_provider_oauth_key_is_replaceable(existing_key: &StoredProviderCatalogKey) -> bool {
    if !existing_key.is_active {
        return true;
    }

    let now_unix_secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .map(|duration| duration.as_secs())
        .unwrap_or(0);
    if existing_key
        .expires_at_unix_secs
        .is_some_and(|expires_at| expires_at <= now_unix_secs)
    {
        return true;
    }

    existing_key
        .oauth_invalid_reason
        .as_deref()
        .map(str::trim)
        .filter(|reason| !reason.is_empty())
        .is_some_and(provider_oauth_invalid_reason_allows_replace)
}

pub(crate) async fn find_duplicate_provider_oauth_key(
    state: &AdminAppState<'_>,
    provider_id: &str,
    auth_config: &serde_json::Map<String, serde_json::Value>,
    exclude_key_id: Option<&str>,
) -> Result<Option<StoredProviderCatalogKey>, String> {
    let new_email = normalize_provider_oauth_identity_value(auth_config.get("email"));
    let new_user_id = normalize_provider_oauth_identity_value(auth_config.get("user_id"));
    let new_account_id = normalize_provider_oauth_identity_value(auth_config.get("account_id"));
    let new_account_uuid = normalize_provider_oauth_identity_value_from_keys(
        auth_config,
        &["account_uuid", "accountUuid"],
    );
    let new_agent_runtime_id = normalize_provider_oauth_identity_value(
        auth_config
            .get("agent_runtime_id")
            .or_else(|| auth_config.get("agentRuntimeId")),
    );
    let new_credential_fingerprint =
        normalize_provider_oauth_identity_value(auth_config.get("credential_fingerprint"));
    let new_auth_method = normalize_provider_oauth_identity_value(auth_config.get("auth_method"));
    let new_kiro_provider = normalize_provider_oauth_identity_value(auth_config.get("provider"));

    if new_email.is_none()
        && new_user_id.is_none()
        && new_account_id.is_none()
        && new_account_uuid.is_none()
        && new_agent_runtime_id.is_none()
        && new_credential_fingerprint.is_none()
    {
        return Ok(None);
    }

    // Duplicate checks are write admission checks. Never let a process-local
    // read-through cache hide a row committed by the previous lock holder.
    state.app().data.clear_provider_catalog_cache();
    let existing_keys = state
        .list_provider_catalog_keys_by_provider_ids(&[provider_id.to_string()])
        .await
        .map_err(|err| format!("{err:?}"))?;

    let provider_type = auth_config
        .get("provider_type")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default()
        .to_string();
    for existing_key in existing_keys.into_iter().filter(|key| {
        provider_key_is_oauth_managed(key, provider_type.as_str())
            && exclude_key_id.is_none_or(|exclude| key.id != exclude)
    }) {
        let Some(existing_auth_config) = state.parse_catalog_auth_config_json(&existing_key) else {
            continue;
        };
        let existing_email =
            normalize_provider_oauth_identity_value(existing_auth_config.get("email"));
        let existing_user_id =
            normalize_provider_oauth_identity_value(existing_auth_config.get("user_id"));
        let existing_auth_method =
            normalize_provider_oauth_identity_value(existing_auth_config.get("auth_method"));
        let existing_kiro_provider =
            normalize_provider_oauth_identity_value(existing_auth_config.get("provider"));
        let mut is_duplicate = false;
        let claude_identity_match =
            match_claude_provider_oauth_identity(auth_config, &existing_auth_config);
        let codex_identity_match =
            match_codex_provider_oauth_identity(auth_config, &existing_auth_config);
        if let Some(claude_identity_match) = claude_identity_match {
            is_duplicate = claude_identity_match;
        } else if let Some(codex_identity_match) = codex_identity_match {
            is_duplicate = codex_identity_match;
        }

        if claude_identity_match.is_none()
            && codex_identity_match.is_none()
            && !is_duplicate
            && new_user_id.is_some()
            && existing_user_id.is_some()
            && new_user_id == existing_user_id
            && !is_codex_cross_plan_group_non_duplicate(auth_config, &existing_auth_config)
        {
            is_duplicate = true;
        }

        if claude_identity_match.is_none()
            && codex_identity_match.is_none()
            && !is_duplicate
            && new_email.is_some()
            && existing_email.is_some()
            && new_email == existing_email
        {
            let is_kiro = auth_config
                .get("provider_type")
                .and_then(serde_json::Value::as_str)
                .is_some_and(|value| value.eq_ignore_ascii_case("kiro"))
                || existing_auth_config
                    .get("provider_type")
                    .and_then(serde_json::Value::as_str)
                    .is_some_and(|value| value.eq_ignore_ascii_case("kiro"));
            if is_kiro {
                if new_auth_method.is_some()
                    && existing_auth_method.is_some()
                    && new_auth_method
                        .as_deref()
                        .zip(existing_auth_method.as_deref())
                        .is_some_and(|(left, right)| left.eq_ignore_ascii_case(right))
                    && new_kiro_provider
                        .as_deref()
                        .zip(existing_kiro_provider.as_deref())
                        .is_none_or(|(left, right)| left.eq_ignore_ascii_case(right))
                {
                    is_duplicate = true;
                }
            } else if !is_codex_cross_plan_group_non_duplicate(auth_config, &existing_auth_config) {
                is_duplicate = true;
            }
        }

        if !is_duplicate {
            continue;
        }
        if existing_provider_oauth_key_is_replaceable(&existing_key) {
            return Ok(Some(existing_key));
        }
        let identifier = normalize_provider_oauth_identity_value_from_keys(
            auth_config,
            &["account_uuid", "accountUuid"],
        )
        .or_else(|| normalize_provider_oauth_identity_value(auth_config.get("account_user_id")))
        .or_else(|| normalize_provider_oauth_identity_value(auth_config.get("account_id")))
        .or_else(|| new_agent_runtime_id.clone())
        .or_else(|| {
            normalize_provider_oauth_identity_value(auth_config.get("credential_fingerprint"))
                .map(|value| format!("fingerprint:{value}"))
        })
        .or_else(|| new_email.clone())
        .or_else(|| new_user_id.clone())
        .unwrap_or_default();
        return Err(format!(
            "该 OAuth 账号 ({identifier}) 已存在于当前 Provider 中（名称: {}）",
            existing_key.name
        ));
    }

    Ok(None)
}
