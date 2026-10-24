use std::{sync::OnceLock, time::Duration};

use aether_data_contracts::repository::provider_catalog::{
    StoredProviderCatalogEndpoint, StoredProviderCatalogProvider,
};
use axum::http::Uri;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tracing::{debug, info, warn};

use crate::wallet_runtime::{
    local_rejection_from_wallet_access, resolve_wallet_auth_gate_uncached,
};
use crate::{AppState, GatewayError};

use super::super::GatewayControlDecision;
use super::credentials::{
    build_auth_context_cache_key, build_auth_context_cache_key_with_trusted_auth,
    current_unix_secs, extract_request_credentials, extract_request_credentials_with_trusted_auth,
    extract_trusted_admin_headers, hash_api_key,
};
use super::gate::GatewayLocalAuthRejection;
use super::principal::derive_principal_candidate;
use super::types::{
    GatewayCredentialCarrier, GatewayPrincipalCandidate, GatewayTrustedAuthHeaders,
};
use crate::cache::{AuthContextCacheGeneration, AuthContextInflightRegistration};
use crate::headers::header_value_str;
use crate::local_auth_token::{
    decode_local_auth_token, local_auth_token_identity_matches_user, LocalAuthTokenType,
};

const AUTH_CONTEXT_CACHE_TTL: Duration = Duration::from_secs(60);
const AUTH_CONTEXT_CACHE_REFRESH_INTERVAL: Duration = Duration::from_secs(10);
const AUTH_CONTEXT_NEGATIVE_CACHE_TTL: Duration = Duration::from_secs(10);
const AUTH_CONTEXT_CACHE_MAX_ENTRIES: usize = 10_000;
const AUTH_CONTEXT_CACHE_MAX_ENTRIES_ENV: &str = "AETHER_GATEWAY_AUTH_CONTEXT_CACHE_MAX_ENTRIES";
const AUTH_CONTEXT_CACHE_REFRESH_INTERVAL_SECS_ENV: &str =
    "AETHER_GATEWAY_AUTH_CONTEXT_CACHE_REFRESH_INTERVAL_SECS";
const AUTH_CONTEXT_NEGATIVE_CACHE_TTL_SECS_ENV: &str =
    "AETHER_GATEWAY_AUTH_CONTEXT_NEGATIVE_CACHE_TTL_SECS";
const AUTH_CONTEXT_NEGATIVE_CACHE_KEY_PREFIX: &str = "negative:";

#[derive(Debug, Clone, Deserialize)]
struct AntigravityBearerBridgeConfig {
    #[serde(default)]
    enabled: bool,
    #[serde(default)]
    auth_user_id: String,
    #[serde(default)]
    auth_api_key_id: String,
    #[serde(default)]
    bearer_sha256_allowlist: Vec<String>,
    #[serde(default)]
    allow_unverified_google_bearer: bool,
}

impl AntigravityBearerBridgeConfig {
    fn bearer_validation_mode(&self, raw_bearer: &str) -> Option<&'static str> {
        if !self.bearer_sha256_allowlist.is_empty() {
            let bearer_hash = hash_api_key(raw_bearer);
            return self
                .bearer_sha256_allowlist
                .iter()
                .any(|allowed| allowed.trim().eq_ignore_ascii_case(&bearer_hash))
                .then_some("sha256_allowlist");
        }

        self.allow_unverified_google_bearer
            .then_some("explicit_unverified")
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub(crate) struct GatewayControlAuthContext {
    pub(crate) user_id: String,
    pub(crate) api_key_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) username: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) api_key_name: Option<String>,
    pub(crate) balance_remaining: Option<f64>,
    pub(crate) access_allowed: bool,
    #[serde(skip)]
    pub(crate) user_rate_limit: Option<i32>,
    #[serde(skip)]
    pub(crate) api_key_rate_limit: Option<i32>,
    #[serde(skip)]
    pub(crate) api_key_is_standalone: bool,
    #[serde(skip)]
    pub(crate) admin_bypass_limits: bool,
    #[serde(skip)]
    pub(crate) local_rejection: Option<GatewayLocalAuthRejection>,
    #[serde(skip)]
    pub(crate) allowed_models: Option<Vec<String>>,
    #[serde(skip)]
    pub(crate) ip_rules: Option<Vec<String>>,
    /// Credential verifier that established this API-key identity. Long-lived
    /// executions use it to prove that a later row with the same IDs is still
    /// the record authenticated by the original request.
    #[serde(skip)]
    pub(crate) verified_api_key_hash: Option<VerifiedApiKeyHash>,
}

#[derive(Clone)]
pub(crate) struct VerifiedApiKeyHash(String);

impl VerifiedApiKeyHash {
    fn new(value: String) -> Self {
        Self(value)
    }

    fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl std::fmt::Debug for VerifiedApiKeyHash {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("VerifiedApiKeyHash([REDACTED])")
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct GatewayAdminPrincipalContext {
    pub(crate) user_id: String,
    pub(crate) user_role: String,
    pub(crate) session_id: Option<String>,
    pub(crate) management_token_id: Option<String>,
    pub(crate) management_token_permissions: Option<Vec<String>>,
}

pub(in super::super) enum ControlDecisionAuthResolution {
    Resolved(GatewayControlDecision),
}

#[cfg(test)]
pub(in super::super) async fn resolve_control_decision_auth(
    state: &AppState,
    headers: &http::HeaderMap,
    uri: &Uri,
    trace_id: &str,
    decision: GatewayControlDecision,
) -> Result<ControlDecisionAuthResolution, GatewayError> {
    resolve_control_decision_auth_with_trusted_auth(
        state,
        headers,
        uri,
        trace_id,
        decision,
        cfg!(test),
    )
    .await
}

pub(in super::super) async fn resolve_control_decision_auth_with_trusted_auth(
    state: &AppState,
    headers: &http::HeaderMap,
    uri: &Uri,
    trace_id: &str,
    mut decision: GatewayControlDecision,
    trusted_auth_verified: bool,
) -> Result<ControlDecisionAuthResolution, GatewayError> {
    if let Some(admin_principal) =
        resolve_trusted_admin_principal(headers, decision.auth_endpoint_signature.as_deref())
    {
        log_admin_principal_resolution(trace_id, &decision, "trusted_headers", &admin_principal);
        decision.admin_principal = Some(admin_principal);
    } else if let Some(admin_principal) = resolve_local_admin_principal(
        state,
        headers,
        uri,
        decision.auth_endpoint_signature.as_deref(),
    )
    .await?
    {
        log_admin_principal_resolution(trace_id, &decision, "local_session", &admin_principal);
        decision.admin_principal = Some(admin_principal);
    }

    let auth_context_cache_key =
        decision
            .auth_endpoint_signature
            .as_deref()
            .and_then(|signature| {
                build_auth_context_cache_key_with_trusted_auth(
                    headers,
                    uri,
                    signature,
                    trusted_auth_verified,
                )
            });

    let mut resolved_auth_context = None;
    if let Some(cache_key) = auth_context_cache_key.as_deref() {
        if let Some((auth_context, age)) = get_cached_auth_context_with_age(state, cache_key) {
            if auth_context_cache_refresh_due(state, age) {
                resolved_auth_context = Some(
                    revalidate_cached_auth_context(
                        state,
                        cache_key,
                        auth_context,
                        decision.auth_endpoint_signature.as_deref(),
                        headers,
                        uri,
                        trusted_auth_verified,
                    )
                    .await?,
                );
            } else {
                // The configured refresh interval is the bounded authorization
                // freshness window. This path remains a lock-free cache hit.
                resolved_auth_context = Some(auth_context);
            }
        }
    }

    if resolved_auth_context.is_none() {
        resolved_auth_context = resolve_data_backed_auth_context_cached(
            state,
            auth_context_cache_key.as_deref(),
            headers,
            uri,
            decision.auth_endpoint_signature.as_deref(),
            true,
            trusted_auth_verified,
        )
        .await?;
    }

    if let Some(auth_context) = resolved_auth_context {
        apply_resolved_auth_context_to_decision(trace_id, &mut decision, auth_context);
    }

    if decision.local_auth_rejection.is_some() {
        log_local_auth_rejection(trace_id, &decision);
        return Ok(ControlDecisionAuthResolution::Resolved(decision));
    }

    if decision.is_execution_runtime_candidate() {
        return Ok(ControlDecisionAuthResolution::Resolved(decision));
    }

    if decision.auth_context.is_some() {
        return Ok(ControlDecisionAuthResolution::Resolved(decision));
    }

    if allows_missing_data_backed_auth_context(&decision) {
        return Ok(ControlDecisionAuthResolution::Resolved(decision));
    }

    Ok(ControlDecisionAuthResolution::Resolved(decision))
}

fn log_admin_principal_resolution(
    trace_id: &str,
    decision: &GatewayControlDecision,
    resolution: &'static str,
    admin_principal: &GatewayAdminPrincipalContext,
) {
    debug!(
        event_name = "admin_principal_resolved",
        log_type = "debug",
        debug_context = "control_auth",
        trace_id = %trace_id,
        route_class = decision.route_class.as_deref().unwrap_or("unknown"),
        route_family = decision.route_family.as_deref().unwrap_or("unknown"),
        route_kind = decision.route_kind.as_deref().unwrap_or("unknown"),
        resolution,
        admin_user_id = admin_principal.user_id.as_str(),
        admin_user_role = admin_principal.user_role.as_str(),
        admin_session_id = admin_principal.session_id.as_deref().unwrap_or("-"),
        admin_management_token_id = admin_principal.management_token_id.as_deref().unwrap_or("-"),
        "resolved admin principal for control decision"
    );
}

fn log_auth_context_resolution(
    trace_id: &str,
    decision: &GatewayControlDecision,
    auth_context: &GatewayControlAuthContext,
) {
    let balance_remaining = auth_context
        .balance_remaining
        .map(|value| format!("{value:.4}"))
        .unwrap_or_else(|| "-".to_string());
    info!(
        event_name = "auth_context_resolved",
        log_type = "event",
        status = if auth_context.access_allowed {
            "allowed"
        } else {
            "blocked"
        },
        trace_id = %trace_id,
        route_class = decision.route_class.as_deref().unwrap_or("unknown"),
        route_family = decision.route_family.as_deref().unwrap_or("unknown"),
        route_kind = decision.route_kind.as_deref().unwrap_or("unknown"),
        user_id = auth_context.user_id.as_str(),
        api_key_id = auth_context.api_key_id.as_str(),
        api_key_name = auth_context.api_key_name.as_deref().unwrap_or("-"),
        balance_remaining = balance_remaining.as_str(),
        access_allowed = auth_context.access_allowed,
        api_key_is_standalone = auth_context.api_key_is_standalone,
        has_local_rejection = auth_context.local_rejection.is_some(),
        "resolved data-backed auth context for control decision"
    );
}

fn log_local_auth_rejection(trace_id: &str, decision: &GatewayControlDecision) {
    let Some(rejection) = decision.local_auth_rejection.as_ref() else {
        return;
    };
    let (rejection_kind, rejection_detail) = match rejection {
        GatewayLocalAuthRejection::InvalidApiKey => ("invalid_api_key", "-".to_string()),
        GatewayLocalAuthRejection::LockedApiKey => ("locked_api_key", "-".to_string()),
        GatewayLocalAuthRejection::WalletUnavailable => ("wallet_unavailable", "-".to_string()),
        GatewayLocalAuthRejection::BalanceDenied { remaining } => (
            "balance_denied",
            remaining
                .map(|value| format!("remaining_usd={value:.4}"))
                .unwrap_or_else(|| "remaining_usd=unknown".to_string()),
        ),
        GatewayLocalAuthRejection::ProviderNotAllowed { provider } => {
            ("provider_not_allowed", provider.clone())
        }
        GatewayLocalAuthRejection::ApiFormatNotAllowed { api_format } => {
            ("api_format_not_allowed", api_format.clone())
        }
        GatewayLocalAuthRejection::ModelNotAllowed { model } => {
            ("model_not_allowed", model.clone())
        }
        GatewayLocalAuthRejection::IpNotAllowed { remote_ip } => {
            ("ip_not_allowed", remote_ip.clone())
        }
    };
    info!(
        event_name = "local_auth_rejected",
        log_type = "event",
        status = "rejected",
        trace_id = %trace_id,
        route_class = decision.route_class.as_deref().unwrap_or("unknown"),
        route_family = decision.route_family.as_deref().unwrap_or("unknown"),
        route_kind = decision.route_kind.as_deref().unwrap_or("unknown"),
        rejection_kind,
        rejection_detail = %rejection_detail,
        "rejected local control request during auth gate resolution"
    );
}

fn allows_missing_data_backed_auth_context(decision: &GatewayControlDecision) -> bool {
    matches!(
        decision.route_kind.as_deref(),
        Some("chat" | "cli" | "compact")
    )
}

fn resolve_trusted_admin_principal(
    headers: &http::HeaderMap,
    auth_endpoint_signature: Option<&str>,
) -> Option<GatewayAdminPrincipalContext> {
    if !auth_endpoint_signature
        .map(str::trim)
        .unwrap_or_default()
        .starts_with("admin:")
    {
        return None;
    }
    let trusted_headers = extract_trusted_admin_headers(headers)?;
    Some(GatewayAdminPrincipalContext {
        user_id: trusted_headers.user_id,
        user_role: trusted_headers.user_role,
        session_id: trusted_headers.session_id,
        management_token_id: trusted_headers.management_token_id,
        management_token_permissions: None,
    })
}

async fn resolve_local_admin_principal(
    state: &AppState,
    headers: &http::HeaderMap,
    uri: &Uri,
    auth_endpoint_signature: Option<&str>,
) -> Result<Option<GatewayAdminPrincipalContext>, GatewayError> {
    let Some(signature) = auth_endpoint_signature
        .map(str::trim)
        .filter(|value| value.starts_with("admin:"))
    else {
        return Ok(None);
    };
    let extracted = extract_request_credentials(headers, uri, signature);
    let Some(access_token) = extracted.bundle.authorization_bearer.as_deref() else {
        return Ok(None);
    };
    let claims = match decode_local_auth_token(access_token, LocalAuthTokenType::Access) {
        Ok(claims) => claims,
        Err(_) => return Ok(None),
    };
    if claims
        .get("role")
        .and_then(Value::as_str)
        .is_some_and(|role| !crate::roles::can_access_admin_console(role))
    {
        return Ok(None);
    }

    resolve_local_admin_principal_from_claims(state, headers, uri, &claims).await
}

pub(crate) async fn resolve_local_admin_session_principal(
    state: &AppState,
    headers: &http::HeaderMap,
    uri: &Uri,
) -> Result<Option<GatewayAdminPrincipalContext>, GatewayError> {
    resolve_local_admin_principal(state, headers, uri, Some("admin:operational")).await
}

async fn resolve_local_admin_principal_from_claims(
    state: &AppState,
    headers: &http::HeaderMap,
    uri: &Uri,
    claims: &serde_json::Map<String, Value>,
) -> Result<Option<GatewayAdminPrincipalContext>, GatewayError> {
    let Some(user_id) = claims.get("user_id").and_then(Value::as_str) else {
        return Ok(None);
    };
    let Some(session_id) = claims.get("session_id").and_then(Value::as_str) else {
        return Ok(None);
    };
    let Some(client_device_id) = extract_local_admin_client_device_id(headers, uri) else {
        return Ok(None);
    };

    let Some(user) = state.find_user_auth_by_id(user_id).await? else {
        return Ok(None);
    };
    if !user.is_active || user.is_deleted || !crate::roles::can_access_admin_console(&user.role) {
        return Ok(None);
    }
    if !local_auth_token_identity_matches_user(claims, &user) {
        return Ok(None);
    }

    let now = chrono::Utc::now();
    let Some(session) = state.find_user_session(user_id, session_id).await? else {
        return Ok(None);
    };
    if session.is_revoked()
        || session.is_expired(now)
        || session.security_version != user.security_version
        || session.client_device_id != client_device_id
    {
        return Ok(None);
    }

    if session.should_touch(now) {
        let _ = state
            .touch_user_session(
                user_id,
                session_id,
                now,
                None,
                local_admin_user_agent(headers).as_deref(),
            )
            .await;
    }

    Ok(Some(GatewayAdminPrincipalContext {
        user_id: user.id,
        user_role: user.role,
        session_id: Some(session.id),
        management_token_id: None,
        management_token_permissions: None,
    }))
}

fn extract_local_admin_client_device_id(headers: &http::HeaderMap, uri: &Uri) -> Option<String> {
    let header_value = header_value_str(headers, "x-client-device-id");
    let query_value = uri.query().and_then(|query| {
        url::form_urlencoded::parse(query.as_bytes())
            .find(|(key, _)| key == "client_device_id")
            .map(|(_, value)| value.into_owned())
    });
    let candidate = header_value.or(query_value)?;
    let candidate = candidate.trim();
    if candidate.is_empty()
        || candidate.len() > 128
        || !candidate
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '-' || ch == '_')
    {
        return None;
    }
    Some(candidate.to_string())
}

fn local_admin_user_agent(headers: &http::HeaderMap) -> Option<String> {
    header_value_str(headers, http::header::USER_AGENT.as_str())
        .map(|value| value.chars().take(1000).collect())
}

pub(crate) async fn resolve_execution_runtime_auth_context(
    state: &AppState,
    decision: &GatewayControlDecision,
    headers: &http::HeaderMap,
    uri: &Uri,
    trace_id: &str,
) -> Result<Option<GatewayControlAuthContext>, GatewayError> {
    let _ = trace_id;

    if let Some(auth_context) = decision.auth_context.as_ref() {
        // Control-route auth resolution already refreshed and validated this context.
        // Revalidating here would perform a second snapshot/wallet lookup per request.
        return Ok(Some(auth_context.clone()));
    }

    let Some(auth_endpoint_signature) = decision.auth_endpoint_signature.as_deref() else {
        return Ok(None);
    };
    let Some(cache_key) = build_auth_context_cache_key(headers, uri, auth_endpoint_signature)
    else {
        return Ok(None);
    };

    if let Some((auth_context, age)) = get_cached_auth_context_with_age(state, &cache_key) {
        if auth_context_cache_refresh_due(state, age) {
            return revalidate_cached_auth_context(
                state,
                &cache_key,
                auth_context,
                Some(auth_endpoint_signature),
                headers,
                uri,
                cfg!(test),
            )
            .await
            .map(Some);
        }
        return Ok(Some(auth_context));
    }

    if let Some(auth_context) = resolve_data_backed_auth_context_cached(
        state,
        Some(cache_key.as_str()),
        headers,
        uri,
        Some(auth_endpoint_signature),
        true,
        cfg!(test),
    )
    .await?
    {
        if auth_context.user_id.is_empty() || auth_context.api_key_id.is_empty() {
            return Ok(None);
        }
        return Ok(Some(auth_context));
    }

    Ok(None)
}

async fn revalidate_cached_auth_context(
    state: &AppState,
    cache_key: &str,
    auth_context: GatewayControlAuthContext,
    auth_endpoint_signature: Option<&str>,
    headers: &http::HeaderMap,
    uri: &Uri,
    trusted_auth_verified: bool,
) -> Result<GatewayControlAuthContext, GatewayError> {
    if is_negative_auth_context(&auth_context)
        || !auth_context.access_allowed
        || !state.has_auth_api_key_reader()
        || auth_context.user_id.trim().is_empty()
        || auth_context.api_key_id.trim().is_empty()
        || auth_endpoint_signature
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .is_none()
    {
        return Ok(auth_context);
    }

    loop {
        match state.auth_context_cache.register_inflight(cache_key) {
            AuthContextInflightRegistration::Leader(guard) => {
                let refreshed = match resolve_security_fresh_auth_context(
                    state,
                    headers,
                    uri,
                    auth_context.clone(),
                    auth_endpoint_signature,
                    trusted_auth_verified,
                )
                .await
                {
                    Ok(refreshed) => refreshed,
                    Err(error) => {
                        // A failed security refresh must not leave the old allow
                        // available to this flight's followers. Publish the same
                        // error so a failed backend read is not retried once per
                        // follower.
                        guard.fail(error.clone());
                        return Err(error);
                    }
                };
                if guard.generation_is_current() {
                    put_cached_auth_context(
                        state,
                        cache_key.to_string(),
                        refreshed.clone(),
                        Some(guard.generation()),
                    );
                }
                return Ok(refreshed);
            }
            AuthContextInflightRegistration::Follower(waiter) => {
                waiter.wait().await?;
                if let Some((refreshed, age)) = get_cached_auth_context_with_age(state, cache_key) {
                    // A cancelled/failed leader leaves the old due entry in
                    // place. Only a newly published security-fresh value may
                    // be reused by followers.
                    if !auth_context_cache_refresh_due(state, age) {
                        return Ok(refreshed);
                    }
                }
            }
            AuthContextInflightRegistration::Bypass => {
                let refreshed = resolve_security_fresh_auth_context(
                    state,
                    headers,
                    uri,
                    auth_context,
                    auth_endpoint_signature,
                    trusted_auth_verified,
                )
                .await;
                if refreshed.is_err() {
                    state.auth_context_cache.invalidate(cache_key);
                }
                return refreshed;
            }
        }
    }
}

async fn resolve_security_fresh_auth_context(
    state: &AppState,
    headers: &http::HeaderMap,
    uri: &Uri,
    stale: GatewayControlAuthContext,
    auth_endpoint_signature: Option<&str>,
    trusted_auth_verified: bool,
) -> Result<GatewayControlAuthContext, GatewayError> {
    if let Some(refreshed) = resolve_data_backed_auth_context_with_trusted_auth(
        state,
        headers,
        uri,
        auth_endpoint_signature,
        trusted_auth_verified,
    )
    .await?
    {
        return Ok(refreshed);
    }

    let mut denied = stale;
    denied.access_allowed = false;
    denied.local_rejection = Some(GatewayLocalAuthRejection::InvalidApiKey);
    denied.balance_remaining = None;
    Ok(denied)
}

async fn resolve_data_backed_auth_context_cached(
    state: &AppState,
    cache_key: Option<&str>,
    headers: &http::HeaderMap,
    uri: &Uri,
    auth_endpoint_signature: Option<&str>,
    cache_negative: bool,
    trusted_auth_verified: bool,
) -> Result<Option<GatewayControlAuthContext>, GatewayError> {
    let Some(cache_key) = cache_key else {
        return resolve_data_backed_auth_context_with_trusted_auth(
            state,
            headers,
            uri,
            auth_endpoint_signature,
            trusted_auth_verified,
        )
        .await;
    };
    loop {
        match state.auth_context_cache.register_inflight(cache_key) {
            AuthContextInflightRegistration::Leader(guard) => {
                let resolved = match resolve_data_backed_auth_context_with_trusted_auth(
                    state,
                    headers,
                    uri,
                    auth_endpoint_signature,
                    trusted_auth_verified,
                )
                .await
                {
                    Ok(resolved) => resolved,
                    Err(error) => {
                        guard.fail(error.clone());
                        return Err(error);
                    }
                };
                if let Some(auth_context) = resolved.as_ref() {
                    if cache_negative
                        || (!auth_context.user_id.is_empty() && !auth_context.api_key_id.is_empty())
                    {
                        if guard.generation_is_current() {
                            put_cached_auth_context(
                                state,
                                cache_key.to_string(),
                                auth_context.clone(),
                                Some(guard.generation()),
                            );
                        }
                    }
                }
                return Ok(resolved);
            }
            AuthContextInflightRegistration::Follower(notified) => {
                notified.wait().await?;
                if let Some(auth_context) = get_cached_auth_context(state, cache_key) {
                    return Ok(Some(auth_context));
                }
                if !cache_negative {
                    return Ok(None);
                }
            }
            AuthContextInflightRegistration::Bypass => {
                return resolve_data_backed_auth_context_with_trusted_auth(
                    state,
                    headers,
                    uri,
                    auth_endpoint_signature,
                    trusted_auth_verified,
                )
                .await;
            }
        }
    }
}

/// Strongly refreshes the long-lived execution authorization context and
/// returns the exact API-key snapshot that produced it.
///
/// WebSocket turns need both values: using the refreshed context for RPM and
/// balance checks while letting the planner independently read its normal
/// cache can authorize a different provider/model snapshot for up to the cache
/// TTL. Ordinary HTTP callers keep using [`refresh_execution_runtime_auth_context`].
pub(crate) async fn refresh_execution_runtime_auth_context_with_snapshot(
    state: &AppState,
    auth_context: GatewayControlAuthContext,
    auth_endpoint_signature: Option<&str>,
) -> Result<
    (
        GatewayControlAuthContext,
        Option<crate::ai_serving::GatewayAuthApiKeySnapshot>,
    ),
    GatewayError,
> {
    if auth_context.local_rejection.is_some() || !auth_context.access_allowed {
        return Ok((auth_context, None));
    }
    let Some(auth_endpoint_signature) = auth_endpoint_signature
        .map(str::trim)
        .filter(|value| !value.is_empty())
    else {
        return Ok((auth_context, None));
    };
    if !state.has_auth_api_key_reader()
        || auth_context.user_id.trim().is_empty()
        || auth_context.api_key_id.trim().is_empty()
    {
        return Ok((auth_context, None));
    }

    let verified_api_key_hash = auth_context.verified_api_key_hash.clone();
    let snapshot = {
        let _permit = state.acquire_auth_snapshot_load_gate().await?;
        if let Some(key_hash) = verified_api_key_hash.as_ref() {
            state
                .data
                .read_auth_api_key_snapshot_by_key_hash_strong(
                    key_hash.as_str(),
                    current_unix_secs(),
                )
                .await
                .map_err(|err| GatewayError::Internal(err.to_string()))?
        } else {
            state
                .data
                .read_auth_api_key_snapshot_strong(
                    &auth_context.user_id,
                    &auth_context.api_key_id,
                    current_unix_secs(),
                )
                .await
                .map_err(|err| GatewayError::Internal(err.to_string()))?
        }
    };
    let Some(snapshot) = snapshot else {
        return Ok((deny_refreshed_auth_context(auth_context), None));
    };
    if snapshot.user_id != auth_context.user_id || snapshot.api_key_id != auth_context.api_key_id {
        return Ok((deny_refreshed_auth_context(auth_context), None));
    };

    let wallet_access = resolve_wallet_auth_gate_uncached(state, &snapshot).await?;
    let mut refreshed = build_data_backed_auth_context(
        state,
        snapshot.clone(),
        auth_endpoint_signature,
        Some(true),
        auth_context.balance_remaining,
        wallet_access,
    )
    .await;
    refreshed.verified_api_key_hash = verified_api_key_hash;
    Ok((refreshed, Some(snapshot)))
}

fn deny_refreshed_auth_context(
    mut auth_context: GatewayControlAuthContext,
) -> GatewayControlAuthContext {
    auth_context.access_allowed = false;
    auth_context.local_rejection = Some(GatewayLocalAuthRejection::InvalidApiKey);
    auth_context.balance_remaining = None;
    auth_context
}

fn put_cached_auth_context(
    state: &AppState,
    cache_key: String,
    auth_context: GatewayControlAuthContext,
    generation: Option<AuthContextCacheGeneration>,
) {
    let (cache_key, ttl) = if is_negative_auth_context(&auth_context) {
        let ttl = auth_context_negative_cache_ttl();
        if ttl.is_zero() {
            return;
        }
        (
            negative_auth_context_cache_key(&cache_key),
            AUTH_CONTEXT_CACHE_TTL.max(ttl),
        )
    } else {
        (cache_key, AUTH_CONTEXT_CACHE_TTL)
    };
    if let Some(generation) = generation {
        state.auth_context_cache.insert_if_generation(
            cache_key,
            auth_context,
            ttl,
            auth_context_cache_max_entries(),
            &generation,
        );
    } else {
        state.auth_context_cache.insert(
            cache_key,
            auth_context,
            ttl,
            auth_context_cache_max_entries(),
        );
    }
}

fn auth_context_cache_max_entries() -> usize {
    static MAX_ENTRIES: OnceLock<usize> = OnceLock::new();
    *MAX_ENTRIES.get_or_init(|| {
        std::env::var(AUTH_CONTEXT_CACHE_MAX_ENTRIES_ENV)
            .ok()
            .and_then(|value| value.trim().parse::<usize>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(AUTH_CONTEXT_CACHE_MAX_ENTRIES)
    })
}

fn auth_context_cache_refresh_interval(_state: &AppState) -> Duration {
    #[cfg(test)]
    if let Some(interval) = _state.auth_context_cache.refresh_interval_for_tests() {
        return interval;
    }

    static REFRESH_INTERVAL: OnceLock<Duration> = OnceLock::new();
    *REFRESH_INTERVAL.get_or_init(|| {
        std::env::var(AUTH_CONTEXT_CACHE_REFRESH_INTERVAL_SECS_ENV)
            .ok()
            .and_then(|value| value.trim().parse::<u64>().ok())
            .filter(|value| *value > 0)
            .map(Duration::from_secs)
            .unwrap_or(AUTH_CONTEXT_CACHE_REFRESH_INTERVAL)
            // Operators may tighten the window, but cannot expand the maximum
            // authorization staleness beyond the secure default.
            .min(AUTH_CONTEXT_CACHE_REFRESH_INTERVAL)
    })
}

fn auth_context_cache_refresh_due(state: &AppState, age: Duration) -> bool {
    age >= auth_context_cache_refresh_interval(state)
}

fn auth_context_negative_cache_ttl() -> Duration {
    static NEGATIVE_TTL: OnceLock<Duration> = OnceLock::new();
    *NEGATIVE_TTL.get_or_init(|| {
        std::env::var(AUTH_CONTEXT_NEGATIVE_CACHE_TTL_SECS_ENV)
            .ok()
            .and_then(|value| value.trim().parse::<u64>().ok())
            .map(Duration::from_secs)
            .unwrap_or(AUTH_CONTEXT_NEGATIVE_CACHE_TTL)
    })
}

fn negative_auth_context_cache_key(cache_key: &str) -> String {
    format!("{AUTH_CONTEXT_NEGATIVE_CACHE_KEY_PREFIX}{cache_key}")
}

fn is_negative_auth_context(auth_context: &GatewayControlAuthContext) -> bool {
    auth_context.user_id.is_empty()
        || auth_context.api_key_id.is_empty()
        || matches!(
            auth_context.local_rejection,
            Some(GatewayLocalAuthRejection::InvalidApiKey)
        )
}

fn apply_resolved_auth_context_to_decision(
    trace_id: &str,
    decision: &mut GatewayControlDecision,
    auth_context: GatewayControlAuthContext,
) {
    log_auth_context_resolution(trace_id, decision, &auth_context);
    decision.local_auth_rejection = auth_context.local_rejection.clone();
    if !auth_context.user_id.is_empty() && !auth_context.api_key_id.is_empty() {
        decision.auth_context = Some(auth_context);
    }
}

#[cfg(test)]
pub(super) async fn resolve_data_backed_auth_context(
    state: &AppState,
    headers: &http::HeaderMap,
    uri: &Uri,
    auth_endpoint_signature: Option<&str>,
) -> Result<Option<GatewayControlAuthContext>, GatewayError> {
    resolve_data_backed_auth_context_with_trusted_auth(
        state,
        headers,
        uri,
        auth_endpoint_signature,
        cfg!(test),
    )
    .await
}

async fn resolve_data_backed_auth_context_with_trusted_auth(
    state: &AppState,
    headers: &http::HeaderMap,
    uri: &Uri,
    auth_endpoint_signature: Option<&str>,
    trusted_auth_verified: bool,
) -> Result<Option<GatewayControlAuthContext>, GatewayError> {
    let Some(signature) = auth_endpoint_signature
        .map(str::trim)
        .filter(|value| !value.is_empty())
    else {
        return Ok(None);
    };
    if !state.has_auth_api_key_reader() {
        return Ok(None);
    }
    let extracted = extract_request_credentials_with_trusted_auth(
        headers,
        uri,
        signature,
        trusted_auth_verified,
    );
    let principal = derive_principal_candidate(&extracted);
    let now_unix_secs = current_unix_secs();

    match principal {
        Some(GatewayPrincipalCandidate::TrustedHeaders(trusted_headers)) => {
            resolve_trusted_auth_context(state, signature, trusted_headers, now_unix_secs).await
        }
        Some(GatewayPrincipalCandidate::ApiKeyHash { key_hash, .. }) => {
            let snapshot = {
                let _permit = state.acquire_auth_snapshot_load_gate().await?;
                state
                    .data
                    .read_auth_api_key_snapshot_by_key_hash_strong(&key_hash, now_unix_secs)
                    .await
                    .map_err(|err| GatewayError::Internal(err.to_string()))?
            };
            let Some(snapshot) = snapshot else {
                return Ok(Some(GatewayControlAuthContext {
                    user_id: String::new(),
                    api_key_id: String::new(),
                    username: None,
                    api_key_name: None,
                    balance_remaining: None,
                    access_allowed: false,
                    user_rate_limit: None,
                    api_key_rate_limit: None,
                    api_key_is_standalone: false,
                    admin_bypass_limits: false,
                    local_rejection: Some(GatewayLocalAuthRejection::InvalidApiKey),
                    allowed_models: None,
                    ip_rules: None,
                    verified_api_key_hash: None,
                }));
            };

            state
                .touch_auth_api_key_last_used_best_effort(&snapshot.api_key_id)
                .await;

            let wallet_access = resolve_wallet_auth_gate_uncached(state, &snapshot).await?;
            let mut auth_context = build_data_backed_auth_context(
                state,
                snapshot,
                signature,
                None,
                None,
                wallet_access,
            )
            .await;
            auth_context.verified_api_key_hash = Some(VerifiedApiKeyHash::new(key_hash));
            Ok(Some(auth_context))
        }
        Some(GatewayPrincipalCandidate::DeferredBearerToken { raw, carrier }) => {
            if let Some(auth_context) = resolve_antigravity_bearer_bridge_auth_context(
                state,
                signature,
                raw.as_str(),
                carrier,
                now_unix_secs,
            )
            .await?
            {
                return Ok(Some(auth_context));
            }
            Ok(None)
        }
        Some(GatewayPrincipalCandidate::DeferredCookieHeader { .. }) => Ok(None),
        None => Ok(None),
    }
}

async fn resolve_antigravity_bearer_bridge_auth_context(
    state: &AppState,
    auth_endpoint_signature: &str,
    raw_bearer: &str,
    carrier: GatewayCredentialCarrier,
    now_unix_secs: u64,
) -> Result<Option<GatewayControlAuthContext>, GatewayError> {
    if carrier != GatewayCredentialCarrier::AuthorizationBearer
        || !auth_endpoint_signature
            .trim()
            .eq_ignore_ascii_case("antigravity:v1internal")
    {
        return Ok(None);
    }

    let config_value = {
        let _permit = state.acquire_auth_snapshot_load_gate().await?;
        state
            .data
            .find_system_config_value_strong(crate::constants::ANTIGRAVITY_BEARER_BRIDGE_CONFIG_KEY)
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))?
    };
    let Some(config_value) = config_value else {
        return Ok(None);
    };
    if config_value.is_null() {
        return Ok(None);
    }
    let config: AntigravityBearerBridgeConfig =
        serde_json::from_value(config_value).map_err(|err| {
            GatewayError::Internal(format!(
                "{} invalid: {err}",
                crate::constants::ANTIGRAVITY_BEARER_BRIDGE_CONFIG_KEY
            ))
        })?;
    if !config.enabled {
        return Ok(None);
    }
    let Some(validation_mode) = config.bearer_validation_mode(raw_bearer) else {
        return Ok(None);
    };
    let user_id = config.auth_user_id.trim();
    let api_key_id = config.auth_api_key_id.trim();
    if user_id.is_empty() || api_key_id.is_empty() {
        return Err(GatewayError::Internal(format!(
            "{} requires auth_user_id and auth_api_key_id",
            crate::constants::ANTIGRAVITY_BEARER_BRIDGE_CONFIG_KEY
        )));
    }

    let snapshot = {
        let _permit = state.acquire_auth_snapshot_load_gate().await?;
        state
            .data
            .read_auth_api_key_snapshot_strong(user_id, api_key_id, now_unix_secs)
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))?
    };
    let Some(snapshot) = snapshot else {
        return Ok(Some(GatewayControlAuthContext {
            user_id: user_id.to_string(),
            api_key_id: api_key_id.to_string(),
            username: None,
            api_key_name: None,
            balance_remaining: None,
            access_allowed: false,
            user_rate_limit: None,
            api_key_rate_limit: None,
            api_key_is_standalone: false,
            admin_bypass_limits: false,
            local_rejection: Some(GatewayLocalAuthRejection::InvalidApiKey),
            allowed_models: None,
            ip_rules: None,
            verified_api_key_hash: None,
        }));
    };

    let wallet_access = resolve_wallet_auth_gate_uncached(state, &snapshot).await?;
    let auth_context = build_data_backed_auth_context(
        state,
        snapshot,
        auth_endpoint_signature,
        None,
        None,
        wallet_access,
    )
    .await;
    info!(
        event_name = "antigravity_bearer_bridge_auth_context_resolved",
        log_type = "event",
        validation_mode,
        user_id = auth_context.user_id.as_str(),
        api_key_id = auth_context.api_key_id.as_str(),
        access_allowed = auth_context.access_allowed,
        has_local_rejection = auth_context.local_rejection.is_some(),
        "resolved Antigravity bearer bridge auth context"
    );
    Ok(Some(auth_context))
}

async fn resolve_trusted_auth_context(
    state: &AppState,
    auth_endpoint_signature: &str,
    trusted_headers: GatewayTrustedAuthHeaders,
    now_unix_secs: u64,
) -> Result<Option<GatewayControlAuthContext>, GatewayError> {
    let snapshot = {
        let _permit = state.acquire_auth_snapshot_load_gate().await?;
        state
            .data
            .read_auth_api_key_snapshot_strong(
                &trusted_headers.user_id,
                &trusted_headers.api_key_id,
                now_unix_secs,
            )
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))?
    };
    let Some(snapshot) = snapshot else {
        return Ok(Some(GatewayControlAuthContext {
            user_id: trusted_headers.user_id,
            api_key_id: trusted_headers.api_key_id,
            username: None,
            api_key_name: None,
            balance_remaining: trusted_headers.balance_remaining,
            access_allowed: false,
            user_rate_limit: None,
            api_key_rate_limit: None,
            api_key_is_standalone: false,
            admin_bypass_limits: false,
            local_rejection: Some(GatewayLocalAuthRejection::InvalidApiKey),
            allowed_models: None,
            ip_rules: None,
            verified_api_key_hash: None,
        }));
    };

    let wallet_access = resolve_wallet_auth_gate_uncached(state, &snapshot).await?;
    Ok(Some(
        build_data_backed_auth_context(
            state,
            snapshot,
            auth_endpoint_signature,
            trusted_headers.access_allowed,
            trusted_headers.balance_remaining,
            wallet_access,
        )
        .await,
    ))
}

async fn build_data_backed_auth_context(
    state: &AppState,
    snapshot: crate::data::auth::GatewayAuthApiKeySnapshot,
    auth_endpoint_signature: &str,
    header_access_allowed: Option<bool>,
    balance_remaining: Option<f64>,
    wallet_access: Option<aether_wallet::WalletAccessDecision>,
) -> GatewayControlAuthContext {
    let allowed_models = snapshot
        .effective_allowed_models()
        .map(|items| items.to_vec());
    let invalid_api_key = !snapshot.user_is_active
        || snapshot.user_is_deleted
        || !snapshot.api_key_is_active
        || api_key_is_expired(snapshot.api_key_expires_at_unix_secs, current_unix_secs());
    let locked_api_key = snapshot.api_key_is_locked && !snapshot.api_key_is_standalone;
    let key_access_allowed = header_access_allowed
        .map(|value| value && snapshot.currently_usable)
        .unwrap_or(snapshot.currently_usable);
    let wallet_remaining = wallet_access
        .as_ref()
        .and_then(|decision| decision.remaining);
    let requested_provider = auth_endpoint_signature
        .split_once(':')
        .map(|(provider, _)| provider)
        .unwrap_or(auth_endpoint_signature)
        .trim();
    let identity_only = auth_gate_identity_only(auth_endpoint_signature);
    let requested_provider_allowed = identity_only
        || auth_snapshot_allows_requested_provider(state, &snapshot, auth_endpoint_signature).await;
    let local_rejection = if invalid_api_key {
        Some(GatewayLocalAuthRejection::InvalidApiKey)
    } else if locked_api_key {
        Some(GatewayLocalAuthRejection::LockedApiKey)
    } else if let Some(rejection) = wallet_access
        .as_ref()
        .and_then(local_rejection_from_wallet_access)
    {
        Some(rejection)
    } else if header_access_allowed.is_some_and(|value| !value) && snapshot.currently_usable {
        Some(GatewayLocalAuthRejection::BalanceDenied {
            remaining: balance_remaining.or(wallet_remaining),
        })
    } else if !requested_provider.is_empty() && !requested_provider_allowed {
        Some(GatewayLocalAuthRejection::ProviderNotAllowed {
            provider: requested_provider.to_string(),
        })
    } else if !identity_only
        && snapshot
            .effective_allowed_api_formats()
            .is_some_and(|allowed| {
                !contains_api_format_or_alias(
                    allowed,
                    auth_gate_api_format(auth_endpoint_signature).as_str(),
                )
            })
    {
        Some(GatewayLocalAuthRejection::ApiFormatNotAllowed {
            api_format: auth_endpoint_signature.to_string(),
        })
    } else {
        None
    };

    GatewayControlAuthContext {
        username: Some(snapshot.username.clone()),
        api_key_name: snapshot.api_key_name.clone(),
        user_id: snapshot.user_id,
        api_key_id: snapshot.api_key_id,
        balance_remaining: wallet_remaining.or(balance_remaining),
        access_allowed: key_access_allowed && local_rejection.is_none(),
        user_rate_limit: snapshot.user_rate_limit,
        api_key_rate_limit: snapshot.api_key_rate_limit,
        api_key_is_standalone: snapshot.api_key_is_standalone,
        admin_bypass_limits: snapshot.user_role.eq_ignore_ascii_case("admin")
            && !snapshot.api_key_is_standalone,
        local_rejection,
        allowed_models,
        ip_rules: snapshot.api_key_ip_rules,
        verified_api_key_hash: None,
    }
}

fn api_key_is_expired(expires_at_unix_secs: Option<u64>, now_unix_secs: u64) -> bool {
    expires_at_unix_secs.is_some_and(|expires_at| expires_at <= now_unix_secs)
}

fn contains_api_format_or_alias(items: &[String], target: &str) -> bool {
    items.iter().any(|item| api_format_matches(item, target))
}

fn normalize_api_format_alias(value: &str) -> String {
    crate::ai_serving::normalize_api_format_alias(value)
}

fn auth_gate_api_format(auth_endpoint_signature: &str) -> String {
    let normalized = normalize_api_format_alias(auth_endpoint_signature);
    match normalized.as_str() {
        "antigravity:v1internal" => "gemini:generate_content".to_string(),
        _ => normalized,
    }
}

fn auth_gate_identity_only(auth_endpoint_signature: &str) -> bool {
    matches!(
        auth_endpoint_signature.trim().to_ascii_lowercase().as_str(),
        "aether:ccswitch_usage"
    )
}

fn api_format_matches(left: &str, right: &str) -> bool {
    aether_scheduler_core::api_format_matches_allowed_value(left, right)
}

async fn auth_snapshot_allows_requested_provider(
    state: &AppState,
    snapshot: &crate::data::auth::GatewayAuthApiKeySnapshot,
    auth_endpoint_signature: &str,
) -> bool {
    let Some(allowed_providers) = snapshot.effective_allowed_providers() else {
        return true;
    };
    let requested_api_format = normalize_api_format_alias(auth_endpoint_signature);
    let requested_provider = requested_api_format
        .split_once(':')
        .map(|(provider, _)| provider)
        .unwrap_or(requested_api_format.as_str())
        .trim();
    if requested_provider.is_empty() {
        return true;
    }
    if allowed_providers.is_empty() {
        return false;
    }
    if allowed_providers
        .iter()
        .any(|value| allowed_provider_value_matches_requested_provider(value, requested_provider))
    {
        return true;
    }
    if !state.has_provider_catalog_data_reader() {
        debug!(
            "deny requested provider {}: provider catalog is unavailable for allowlist resolution",
            requested_provider
        );
        return false;
    }

    let providers = match state.list_provider_catalog_providers(true).await {
        Ok(value) => value,
        Err(err) => {
            warn!(
                "deny requested provider {}: provider catalog lookup failed: {:?}",
                requested_provider, err
            );
            return false;
        }
    };

    let allowed_catalog_providers = providers
        .into_iter()
        .filter(|provider| {
            allowed_providers.iter().any(|value| {
                aether_scheduler_core::provider_matches_allowed_value(
                    value,
                    &provider.id,
                    &provider.name,
                    &provider.provider_type,
                )
            })
        })
        .collect::<Vec<_>>();
    if allowed_catalog_providers
        .iter()
        .any(|provider| provider_matches_requested_provider(provider, requested_provider))
    {
        return true;
    }

    let allowed_provider_ids = allowed_catalog_providers
        .iter()
        .map(|provider| provider.id.clone())
        .collect::<Vec<_>>();
    if allowed_provider_ids.is_empty() {
        return false;
    }

    let endpoints = match state
        .list_provider_catalog_endpoints_by_provider_ids(&allowed_provider_ids)
        .await
    {
        Ok(value) => value,
        Err(err) => {
            warn!(
                "deny requested provider {}: provider endpoint lookup failed: {:?}",
                requested_provider, err
            );
            return false;
        }
    };

    endpoints.iter().any(|endpoint| {
        endpoint_matches_requested_provider(endpoint, &requested_api_format, requested_provider)
    })
}

fn allowed_provider_value_matches_requested_provider(
    allowed_value: &str,
    requested_provider: &str,
) -> bool {
    aether_scheduler_core::provider_matches_allowed_value(
        allowed_value,
        requested_provider,
        requested_provider,
        requested_provider,
    )
}

fn provider_matches_requested_provider(
    provider: &StoredProviderCatalogProvider,
    requested_provider: &str,
) -> bool {
    aether_scheduler_core::provider_matches_allowed_value(
        requested_provider,
        &provider.id,
        &provider.name,
        &provider.provider_type,
    )
}

fn endpoint_matches_requested_provider(
    endpoint: &StoredProviderCatalogEndpoint,
    requested_api_format: &str,
    requested_provider: &str,
) -> bool {
    if !endpoint.is_active {
        return false;
    }
    if api_format_matches(&endpoint.api_format, requested_api_format) {
        return true;
    }
    let endpoint_api_format = normalize_api_format_alias(&endpoint.api_format);
    if crate::ai_serving::request_conversion_kind(requested_api_format, &endpoint_api_format)
        .is_some()
    {
        return true;
    }
    if endpoint.api_family.as_deref().is_some_and(|family| {
        allowed_provider_value_matches_requested_provider(family, requested_provider)
    }) {
        return true;
    }
    let endpoint_provider = endpoint_api_format
        .split_once(':')
        .map(|(provider, _)| provider)
        .unwrap_or(endpoint_api_format.as_str());
    allowed_provider_value_matches_requested_provider(endpoint_provider, requested_provider)
}

fn get_cached_auth_context(state: &AppState, cache_key: &str) -> Option<GatewayControlAuthContext> {
    get_cached_auth_context_with_age(state, cache_key).map(|(auth_context, _)| auth_context)
}

fn get_cached_auth_context_with_age(
    state: &AppState,
    cache_key: &str,
) -> Option<(GatewayControlAuthContext, Duration)> {
    let negative_ttl = auth_context_negative_cache_ttl();
    if !negative_ttl.is_zero() {
        if let Some(auth_context) = state
            .auth_context_cache
            .get_fresh_with_age(&negative_auth_context_cache_key(cache_key), negative_ttl)
        {
            return Some(auth_context);
        }
    }
    state
        .auth_context_cache
        .get_fresh_with_age(cache_key, AUTH_CONTEXT_CACHE_TTL)
}

#[cfg(test)]
#[path = "resolution_tests.rs"]
mod tests;
