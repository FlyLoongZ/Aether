use crate::handlers::admin::request::{
    AdminAppState, AdminGatewayProviderTransportSnapshot, AdminLocalOAuthRefreshError,
};
use crate::handlers::shared::{
    sync_provider_key_oauth_status_snapshot, sync_provider_key_quota_status_snapshot,
};
use crate::state::ProviderTransportCredentialFence;
use crate::GatewayError;
use aether_admin::provider::quota as admin_provider_quota_pure;
use aether_admin::provider::redaction::admin_provider_upstream_metadata_safe_json;
use aether_contracts::{
    ExecutionPlan, ExecutionResult, ExecutionTimeouts, ProxySnapshot, RequestBody,
    ResolvedTransportProfile, EXECUTION_REQUEST_FOLLOW_REDIRECTS_HEADER,
};
use aether_data_contracts::repository::provider_catalog::{
    ProviderCatalogKeyOAuthCredentialFence, ProviderCatalogKeyOAuthRuntimeStateCasUpdate,
    ProviderCatalogKeyRuntimeMetadataUpdate, ProviderCatalogKeyStatusSnapshotUpdate,
    ProviderCatalogUpstreamMetadataNamespaceExpectation, StoredProviderCatalogEndpoint,
    StoredProviderCatalogKey,
};
use aether_provider_pool::{ProviderPoolQuotaRequestSpec, ProviderPoolService};
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::warn;

const PROVIDER_QUOTA_DEFAULT_TIMEOUT_MS: u64 = 30_000;
const PROVIDER_QUOTA_PROXY_TIMEOUT_MS: u64 = 60_000;
const CODEX_QUOTA_PERSIST_CAS_MAX_ATTEMPTS: usize = 16;
const CODEX_RESET_HISTORY_LIMIT: usize = 64;
const CODEX_OAUTH_STATE_REQUEST_WATERMARK_KEY: &str =
    admin_provider_quota_pure::CODEX_OAUTH_STATE_REQUEST_WATERMARK_KEY;
const CODEX_OAUTH_STATE_REQUEST_WATERMARK_ID_KEY: &str =
    admin_provider_quota_pure::CODEX_OAUTH_STATE_REQUEST_WATERMARK_ID_KEY;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CodexAccountResetFence {
    pub unix_ms: u64,
    pub id: String,
    pub generation: u64,
}

#[cfg(test)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CodexAccountResetFenceInstall {
    Owned(CodexAccountResetFence),
    Superseded,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CodexAccountResetReservation {
    pub idempotency_key: String,
    pub generation: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CodexAccountResetTerminal {
    pub idempotency_key: String,
    pub generation: u64,
    pub outcome: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CodexAccountResetReserveResult {
    Reserved(CodexAccountResetReservation),
    Replay(CodexAccountResetTerminal),
    LegacyReplay,
    Busy(CodexAccountResetReservation),
    CredentialGenerationMismatch,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CodexAccountResetCompleteResult {
    Activated(CodexAccountResetFence),
    Noop(CodexAccountResetTerminal),
    Replay(CodexAccountResetTerminal),
}

pub(super) enum ProviderQuotaExecutionOutcome {
    Response(Box<ExecutionResult>),
    Failure(String),
}

pub(super) fn default_provider_quota_execution_timeouts(
    proxy: Option<&ProxySnapshot>,
) -> ExecutionTimeouts {
    let timeout_ms = if proxy.is_some() {
        PROVIDER_QUOTA_PROXY_TIMEOUT_MS
    } else {
        PROVIDER_QUOTA_DEFAULT_TIMEOUT_MS
    };
    ExecutionTimeouts {
        connect_ms: Some(timeout_ms),
        read_ms: Some(timeout_ms),
        write_ms: Some(timeout_ms),
        pool_ms: Some(timeout_ms),
        total_ms: Some(timeout_ms),
        ..ExecutionTimeouts::default()
    }
}

pub(super) fn resolve_provider_quota_execution_timeouts(
    configured: Option<ExecutionTimeouts>,
    proxy: Option<&ProxySnapshot>,
) -> ExecutionTimeouts {
    let defaults = default_provider_quota_execution_timeouts(proxy);
    let Some(mut timeouts) = configured else {
        return defaults;
    };
    timeouts.connect_ms = timeouts.connect_ms.or(defaults.connect_ms);
    timeouts.read_ms = timeouts.read_ms.or(defaults.read_ms);
    timeouts.write_ms = timeouts.write_ms.or(defaults.write_ms);
    timeouts.pool_ms = timeouts.pool_ms.or(defaults.pool_ms);
    timeouts.total_ms = timeouts.total_ms.or(defaults.total_ms);
    timeouts.first_byte_ms = timeouts.first_byte_ms.or(defaults.first_byte_ms);
    timeouts
}

pub(crate) fn provider_auto_remove_banned_keys(config: Option<&serde_json::Value>) -> bool {
    admin_provider_quota_pure::provider_auto_remove_banned_keys(config)
}

pub(crate) fn provider_auto_remove_quota_exhausted_keys(
    config: Option<&serde_json::Value>,
) -> bool {
    admin_provider_quota_pure::provider_auto_remove_quota_exhausted_keys(config)
}

pub(crate) fn should_auto_remove_oauth_invalid_key(
    key: &StoredProviderCatalogKey,
    candidate_reason: Option<&str>,
    access_token_invalid_proven: bool,
    now_unix_secs: u64,
) -> bool {
    admin_provider_quota_pure::should_auto_remove_oauth_invalid_key(
        key,
        candidate_reason,
        access_token_invalid_proven,
        now_unix_secs,
    )
}

pub(crate) async fn persist_quota_oauth_refresh_failure_state(
    state: &AdminAppState<'_>,
    transport: &AdminGatewayProviderTransportSnapshot,
    err: &AdminLocalOAuthRefreshError,
) -> Result<bool, GatewayError> {
    let AdminLocalOAuthRefreshError::HttpStatus {
        status_code,
        body_excerpt,
        ..
    } = err
    else {
        return Ok(false);
    };
    if !matches!(*status_code, 400 | 401 | 403) {
        return Ok(false);
    }
    state
        .app()
        .persist_local_oauth_refresh_failure_state(transport, *status_code, body_excerpt, false)
        .await
}

pub(crate) async fn quota_key_auto_removed(
    state: &AdminAppState<'_>,
    key_id: &str,
) -> Result<bool, GatewayError> {
    if key_id.trim().is_empty() {
        return Ok(false);
    }
    Ok(state
        .read_provider_catalog_keys_by_ids(&[key_id.to_string()])
        .await?
        .is_empty())
}

pub(crate) fn oauth_refresh_auto_removed_result(
    key: &StoredProviderCatalogKey,
) -> serde_json::Value {
    serde_json::json!({
        "key_id": key.id,
        "key_name": key.name,
        "status": "auto_removed",
        "message": "OAuth refresh 失败且凭证已不可用，已自动删除",
        "auto_removed": true,
    })
}

pub(crate) fn normalize_string_id_list(values: Option<Vec<String>>) -> Option<Vec<String>> {
    admin_provider_quota_pure::normalize_string_id_list(values)
}

pub(crate) fn provider_type_supports_quota_refresh(provider_type: &str) -> bool {
    ProviderPoolService::with_builtin_adapters().supports_quota_refresh(provider_type)
}

pub(crate) fn unsupported_provider_quota_refresh_message(provider_type: &str) -> String {
    ProviderPoolService::with_builtin_adapters().quota_refresh_unsupported_message(provider_type)
}

pub(crate) fn provider_quota_refresh_endpoint_for_provider(
    provider_type: &str,
    endpoints: &[StoredProviderCatalogEndpoint],
    include_inactive: bool,
) -> Option<StoredProviderCatalogEndpoint> {
    ProviderPoolService::with_builtin_adapters().quota_refresh_endpoint_for_provider(
        provider_type,
        endpoints,
        include_inactive,
    )
}

pub(crate) fn provider_quota_refresh_missing_endpoint_message(provider_type: &str) -> String {
    ProviderPoolService::with_builtin_adapters()
        .quota_refresh_missing_endpoint_message(provider_type)
}

fn merge_upstream_metadata(
    current: Option<&serde_json::Value>,
    updates: &serde_json::Value,
) -> serde_json::Value {
    let mut merged = current
        .and_then(serde_json::Value::as_object)
        .cloned()
        .unwrap_or_default();
    if let Some(update_object) = updates.as_object() {
        for (key, value) in update_object {
            let mut next = value.clone();
            if let (Some(current_namespace), Some(next_namespace)) = (
                merged.get(key).and_then(serde_json::Value::as_object),
                next.as_object_mut(),
            ) {
                let mut combined = current_namespace.clone();
                combined.extend(next_namespace.clone());
                next = serde_json::Value::Object(combined);
            }
            merged.insert(key.clone(), next);
        }
    }
    serde_json::Value::Object(merged)
}

pub(super) fn extract_execution_error_message(result: &ExecutionResult) -> Option<String> {
    admin_provider_quota_pure::extract_execution_error_message(result)
}

fn extract_execution_error_detail(result: &ExecutionResult) -> Option<String> {
    admin_provider_quota_pure::extract_execution_error_detail(result)
}

pub(super) fn quota_refresh_success_invalid_state(
    key: &StoredProviderCatalogKey,
) -> (Option<u64>, Option<String>) {
    admin_provider_quota_pure::quota_refresh_success_invalid_state(key)
}

fn merge_codex_oauth_response_state(
    latest_key: &StoredProviderCatalogKey,
    incoming_invalid_at_unix_secs: Option<u64>,
    incoming_invalid_reason: Option<&str>,
    observed_at_unix_secs: u64,
) -> (Option<u64>, Option<String>) {
    match incoming_invalid_reason {
        Some(reason) => admin_provider_quota_pure::codex_build_invalid_state(
            latest_key,
            reason.to_string(),
            incoming_invalid_at_unix_secs.unwrap_or(observed_at_unix_secs),
        ),
        None => admin_provider_quota_pure::quota_refresh_success_invalid_state(latest_key),
    }
}

fn codex_reset_credential_matches(
    key: &StoredProviderCatalogKey,
    expected_encrypted_auth_config: &str,
    expected_credential: &ProviderCatalogKeyOAuthCredentialFence,
) -> bool {
    key.encrypted_auth_config.as_deref() == Some(expected_encrypted_auth_config)
        && key.encrypted_api_key == expected_credential.encrypted_api_key
        && key.auth_type == expected_credential.auth_type
        && key.provider_id == expected_credential.provider_id
}

fn codex_reset_reservation_from_object(
    codex: &serde_json::Map<String, serde_json::Value>,
) -> Option<CodexAccountResetReservation> {
    let reservation = codex
        .get(admin_provider_quota_pure::CODEX_QUOTA_ACCOUNT_RESET_RESERVATION_KEY)?
        .as_object()?;
    let idempotency_key = reservation.get("idempotency_key")?.as_str()?.trim();
    let generation = reservation
        .get("generation")
        .and_then(admin_provider_quota_pure::coerce_json_u64)
        .filter(|generation| *generation > 0)?;
    (!idempotency_key.is_empty()).then(|| CodexAccountResetReservation {
        idempotency_key: idempotency_key.to_string(),
        generation,
    })
}

fn codex_reset_history_from_object(
    codex: &serde_json::Map<String, serde_json::Value>,
) -> Vec<CodexAccountResetTerminal> {
    codex
        .get(admin_provider_quota_pure::CODEX_QUOTA_ACCOUNT_RESET_HISTORY_KEY)
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|value| {
            let object = value.as_object()?;
            let idempotency_key = object.get("idempotency_key")?.as_str()?.trim();
            let generation = object
                .get("generation")
                .and_then(admin_provider_quota_pure::coerce_json_u64)?;
            let outcome = object.get("outcome")?.as_str()?.trim();
            (!idempotency_key.is_empty() && !outcome.is_empty()).then(|| {
                CodexAccountResetTerminal {
                    idempotency_key: idempotency_key.to_string(),
                    generation,
                    outcome: outcome.to_string(),
                }
            })
        })
        .collect()
}

fn codex_reset_write_bounded_history(
    codex: &mut serde_json::Map<String, serde_json::Value>,
    terminal: &CodexAccountResetTerminal,
) {
    let mut history = codex_reset_history_from_object(codex);
    history.retain(|entry| entry.idempotency_key != terminal.idempotency_key);
    history.push(terminal.clone());
    if history.len() > CODEX_RESET_HISTORY_LIMIT {
        history.drain(..history.len() - CODEX_RESET_HISTORY_LIMIT);
    }
    codex.insert(
        admin_provider_quota_pure::CODEX_QUOTA_ACCOUNT_RESET_HISTORY_KEY.to_string(),
        serde_json::Value::Array(
            history
                .into_iter()
                .map(|entry| {
                    serde_json::json!({
                        "idempotency_key": entry.idempotency_key,
                        "generation": entry.generation,
                        "outcome": entry.outcome,
                    })
                })
                .collect(),
        ),
    );

    codex_reset_write_processed_id(codex, &terminal.idempotency_key);
}

fn codex_reset_write_processed_id(
    codex: &mut serde_json::Map<String, serde_json::Value>,
    idempotency_key: &str,
) -> bool {
    let mut processed_ids = codex
        .get(admin_provider_quota_pure::CODEX_QUOTA_ACCOUNT_RESET_PROCESSED_IDS_KEY)
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(serde_json::Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .collect::<Vec<_>>();
    let already_processed = processed_ids.iter().any(|value| value == idempotency_key);
    processed_ids.retain(|value| value != idempotency_key);
    processed_ids.push(idempotency_key.to_string());
    codex.insert(
        admin_provider_quota_pure::CODEX_QUOTA_ACCOUNT_RESET_PROCESSED_IDS_KEY.to_string(),
        serde_json::Value::Array(
            processed_ids
                .into_iter()
                .map(serde_json::Value::String)
                .collect(),
        ),
    );
    already_processed
}

async fn persist_codex_reset_namespace(
    state: &AdminAppState<'_>,
    key: &StoredProviderCatalogKey,
    expected_codex: Option<serde_json::Value>,
    next_codex: serde_json::Value,
    expected_encrypted_auth_config: &str,
    expected_credential: &ProviderCatalogKeyOAuthCredentialFence,
    updated_at_unix_secs: u64,
) -> Result<bool, GatewayError> {
    state
        .app()
        .compare_and_update_provider_catalog_key_oauth_runtime_state(
            &ProviderCatalogKeyOAuthRuntimeStateCasUpdate {
                key_id: key.id.clone(),
                expected_encrypted_auth_config: Some(expected_encrypted_auth_config.to_string()),
                expected_credential: Some(expected_credential.clone()),
                expected_upstream_metadata_namespace: Some(
                    ProviderCatalogUpstreamMetadataNamespaceExpectation {
                        namespace: "codex".to_string(),
                        expected_value: expected_codex,
                    },
                ),
                encrypted_auth_config: expected_encrypted_auth_config.to_string(),
                encrypted_api_key_update: None,
                expires_at_unix_secs_update: None,
                oauth_invalid_at_unix_secs: key.oauth_invalid_at_unix_secs,
                oauth_invalid_reason: key.oauth_invalid_reason.clone(),
                upstream_metadata_patch: Some(serde_json::json!({"codex": next_codex})),
                upstream_metadata_namespace_to_remove: None,
                status_snapshot_patch: serde_json::json!({}),
                reset_error_count: false,
                updated_at_unix_secs: Some(updated_at_unix_secs),
            },
        )
        .await
}

pub(crate) async fn reserve_codex_account_reset(
    state: &AdminAppState<'_>,
    key_id: &str,
    expected_encrypted_auth_config: &str,
    expected_credential: &ProviderCatalogKeyOAuthCredentialFence,
    expected_credential_generation: Option<&str>,
    idempotency_key: &str,
) -> Result<Option<CodexAccountResetReserveResult>, GatewayError> {
    let idempotency_key = idempotency_key.trim();
    if idempotency_key.is_empty() {
        return Ok(None);
    }
    for attempt in 0..CODEX_QUOTA_PERSIST_CAS_MAX_ATTEMPTS {
        let Some(key) = state
            .read_provider_catalog_keys_by_ids(&[key_id.to_string()])
            .await?
            .into_iter()
            .next()
        else {
            return Ok(None);
        };
        if !codex_reset_credential_matches(
            &key,
            expected_encrypted_auth_config,
            expected_credential,
        ) {
            return Ok(None);
        }
        let expected_codex = key
            .upstream_metadata
            .as_ref()
            .and_then(serde_json::Value::as_object)
            .and_then(|metadata| metadata.get("codex"))
            .cloned();
        if !admin_provider_quota_pure::codex_credential_generation_matches(
            expected_codex.as_ref(),
            expected_credential_generation,
        ) {
            return Ok(Some(
                CodexAccountResetReserveResult::CredentialGenerationMismatch,
            ));
        }
        let mut codex = expected_codex
            .as_ref()
            .and_then(serde_json::Value::as_object)
            .cloned()
            .unwrap_or_default();
        if let Some(terminal) = codex_reset_history_from_object(&codex)
            .into_iter()
            .find(|entry| entry.idempotency_key == idempotency_key)
        {
            return Ok(Some(CodexAccountResetReserveResult::Replay(terminal)));
        }
        if codex
            .get(admin_provider_quota_pure::CODEX_QUOTA_ACCOUNT_RESET_PROCESSED_IDS_KEY)
            .and_then(serde_json::Value::as_array)
            .is_some_and(|ids| {
                ids.iter()
                    .any(|value| value.as_str() == Some(idempotency_key))
            })
        {
            let active_generation = codex
                .get(admin_provider_quota_pure::CODEX_QUOTA_ACCOUNT_RESET_GENERATION_KEY)
                .and_then(admin_provider_quota_pure::coerce_json_u64)
                .unwrap_or(0);
            let active_fence_matches = codex
                .get(admin_provider_quota_pure::CODEX_QUOTA_ACCOUNT_RESET_FENCE_ID_KEY)
                .and_then(serde_json::Value::as_str)
                == Some(format!("reset:{idempotency_key}").as_str());
            if active_generation > 0 && active_fence_matches {
                return Ok(Some(CodexAccountResetReserveResult::Replay(
                    CodexAccountResetTerminal {
                        idempotency_key: idempotency_key.to_string(),
                        generation: active_generation,
                        outcome: "already_redeemed".to_string(),
                    },
                )));
            }
            return Ok(Some(CodexAccountResetReserveResult::LegacyReplay));
        }
        if let Some(reservation) = codex_reset_reservation_from_object(&codex) {
            return Ok(Some(if reservation.idempotency_key == idempotency_key {
                CodexAccountResetReserveResult::Reserved(reservation)
            } else {
                CodexAccountResetReserveResult::Busy(reservation)
            }));
        }
        let active_generation = codex
            .get(admin_provider_quota_pure::CODEX_QUOTA_ACCOUNT_RESET_GENERATION_KEY)
            .and_then(admin_provider_quota_pure::coerce_json_u64)
            .unwrap_or(0);
        let sequence = codex
            .get(admin_provider_quota_pure::CODEX_QUOTA_ACCOUNT_RESET_SEQUENCE_KEY)
            .and_then(admin_provider_quota_pure::coerce_json_u64)
            .unwrap_or(active_generation)
            .max(active_generation);
        let Some(generation) = sequence.checked_add(1) else {
            return Err(GatewayError::Internal(
                "Codex reset generation exhausted".to_string(),
            ));
        };
        let reservation = CodexAccountResetReservation {
            idempotency_key: idempotency_key.to_string(),
            generation,
        };
        codex.insert(
            admin_provider_quota_pure::CODEX_QUOTA_ACCOUNT_RESET_SEQUENCE_KEY.to_string(),
            serde_json::json!(generation),
        );
        codex.insert(
            admin_provider_quota_pure::CODEX_QUOTA_ACCOUNT_RESET_RESERVATION_KEY.to_string(),
            serde_json::json!({
                "idempotency_key": reservation.idempotency_key,
                "generation": reservation.generation,
            }),
        );
        if persist_codex_reset_namespace(
            state,
            &key,
            expected_codex,
            serde_json::Value::Object(codex),
            expected_encrypted_auth_config,
            expected_credential,
            crate::clock::current_unix_secs(),
        )
        .await?
        {
            return Ok(Some(CodexAccountResetReserveResult::Reserved(reservation)));
        }
        if attempt + 1 < CODEX_QUOTA_PERSIST_CAS_MAX_ATTEMPTS {
            tokio::task::yield_now().await;
        }
    }
    Ok(None)
}

fn record_locally_consumed_codex_reset_credit(
    codex: &mut serde_json::Map<String, serde_json::Value>,
    observed_at_unix_secs: u64,
) {
    let Some(reset_credits) = codex
        .get_mut("reset_credits")
        .and_then(serde_json::Value::as_object_mut)
    else {
        return;
    };
    let Some(available_count) = reset_credits
        .get("available_count")
        .and_then(admin_provider_quota_pure::coerce_json_u64)
    else {
        return;
    };

    reset_credits.insert(
        "available_count".to_string(),
        serde_json::json!(available_count.saturating_sub(1)),
    );
    reset_credits.insert(
        "updated_at".to_string(),
        serde_json::json!(observed_at_unix_secs),
    );
    reset_credits.insert(
        "detail_source".to_string(),
        serde_json::json!("local_consume"),
    );
    reset_credits.insert(
        "detail_status".to_string(),
        serde_json::json!("pending_refresh"),
    );
    reset_credits.remove("detail_error");
    if let Some(credits) = reset_credits
        .get_mut("credits")
        .and_then(serde_json::Value::as_array_mut)
    {
        if !credits.is_empty() {
            credits.remove(0);
        }
    }
}

pub(crate) async fn complete_codex_account_reset(
    state: &AdminAppState<'_>,
    key_id: &str,
    expected_encrypted_auth_config: &str,
    expected_credential: &ProviderCatalogKeyOAuthCredentialFence,
    reservation: &CodexAccountResetReservation,
    outcome: &str,
    fence_unix_ms: u64,
) -> Result<Option<CodexAccountResetCompleteResult>, GatewayError> {
    let outcome = outcome.trim();
    let activates = matches!(outcome, "reset" | "already_redeemed");
    let noop = matches!(outcome, "nothing_to_reset" | "no_credit");
    if (!activates && !noop) || fence_unix_ms == 0 {
        return Ok(None);
    }
    for attempt in 0..CODEX_QUOTA_PERSIST_CAS_MAX_ATTEMPTS {
        let Some(key) = state
            .read_provider_catalog_keys_by_ids(&[key_id.to_string()])
            .await?
            .into_iter()
            .next()
        else {
            return Ok(None);
        };
        if !codex_reset_credential_matches(
            &key,
            expected_encrypted_auth_config,
            expected_credential,
        ) {
            return Ok(None);
        }
        let expected_codex = key
            .upstream_metadata
            .as_ref()
            .and_then(serde_json::Value::as_object)
            .and_then(|metadata| metadata.get("codex"))
            .cloned();
        let mut codex = expected_codex
            .as_ref()
            .and_then(serde_json::Value::as_object)
            .cloned()
            .unwrap_or_default();
        let existing_terminal = codex_reset_history_from_object(&codex)
            .into_iter()
            .find(|entry| entry.idempotency_key == reservation.idempotency_key);
        let upgrades_noop = existing_terminal.as_ref().is_some_and(|terminal| {
            terminal.generation == reservation.generation
                && !matches!(terminal.outcome.as_str(), "reset" | "already_redeemed")
                && activates
        });
        if let Some(terminal) = existing_terminal.as_ref().filter(|_| !upgrades_noop) {
            return Ok(Some(CodexAccountResetCompleteResult::Replay(
                terminal.clone(),
            )));
        }
        if !upgrades_noop
            && codex_reset_reservation_from_object(&codex).as_ref() != Some(reservation)
        {
            return Ok(None);
        }
        let terminal = CodexAccountResetTerminal {
            idempotency_key: reservation.idempotency_key.clone(),
            generation: reservation.generation,
            outcome: outcome.to_string(),
        };
        if outcome == "reset" {
            record_locally_consumed_codex_reset_credit(&mut codex, fence_unix_ms / 1_000);
        }
        codex_reset_write_bounded_history(&mut codex, &terminal);
        if codex_reset_reservation_from_object(&codex).as_ref() == Some(reservation) {
            codex.remove(admin_provider_quota_pure::CODEX_QUOTA_ACCOUNT_RESET_RESERVATION_KEY);
        }
        let active_generation = codex
            .get(admin_provider_quota_pure::CODEX_QUOTA_ACCOUNT_RESET_GENERATION_KEY)
            .and_then(admin_provider_quota_pure::coerce_json_u64)
            .unwrap_or(0);
        let completed = if activates && active_generation <= reservation.generation {
            let fence = CodexAccountResetFence {
                unix_ms: fence_unix_ms,
                id: format!("reset:{}", reservation.idempotency_key),
                generation: reservation.generation,
            };
            codex.insert(
                admin_provider_quota_pure::CODEX_QUOTA_ACCOUNT_RESET_GENERATION_KEY.to_string(),
                serde_json::json!(reservation.generation),
            );
            codex.insert(
                admin_provider_quota_pure::CODEX_QUOTA_ACCOUNT_RESET_PENDING_GENERATION_KEY
                    .to_string(),
                serde_json::json!(reservation.generation),
            );
            codex.insert(
                admin_provider_quota_pure::CODEX_QUOTA_ACCOUNT_RESET_PENDING_KEY.to_string(),
                serde_json::json!(true),
            );
            codex.insert(
                admin_provider_quota_pure::CODEX_QUOTA_ACCOUNT_RESET_FENCE_UNIX_MS_KEY.to_string(),
                serde_json::json!(fence_unix_ms),
            );
            codex.insert(
                admin_provider_quota_pure::CODEX_QUOTA_ACCOUNT_RESET_FENCE_ID_KEY.to_string(),
                serde_json::json!(fence.id),
            );
            CodexAccountResetCompleteResult::Activated(fence)
        } else if activates {
            CodexAccountResetCompleteResult::Replay(terminal)
        } else {
            CodexAccountResetCompleteResult::Noop(terminal)
        };
        if persist_codex_reset_namespace(
            state,
            &key,
            expected_codex,
            serde_json::Value::Object(codex),
            expected_encrypted_auth_config,
            expected_credential,
            fence_unix_ms / 1_000,
        )
        .await?
        {
            return Ok(Some(completed));
        }
        if attempt + 1 < CODEX_QUOTA_PERSIST_CAS_MAX_ATTEMPTS {
            tokio::task::yield_now().await;
        }
    }
    Ok(None)
}

#[cfg(test)]
pub(crate) async fn persist_codex_account_reset_fence(
    state: &AdminAppState<'_>,
    key_id: &str,
    expected_encrypted_auth_config: Option<&str>,
    expected_credential: Option<&ProviderCatalogKeyOAuthCredentialFence>,
    fence_unix_ms: u64,
    fence_id: &str,
    idempotency_key: &str,
) -> Result<Option<CodexAccountResetFenceInstall>, GatewayError> {
    if expected_encrypted_auth_config.is_some() != expected_credential.is_some() {
        return Err(GatewayError::Internal(
            "Codex reset credential fence must include auth_config and credential identity"
                .to_string(),
        ));
    }
    let fence_id = fence_id.trim();
    let idempotency_key = idempotency_key.trim();
    if fence_unix_ms == 0 || fence_id.is_empty() || idempotency_key.is_empty() {
        return Ok(None);
    }

    for attempt in 0..CODEX_QUOTA_PERSIST_CAS_MAX_ATTEMPTS {
        let Some(latest_key) = state
            .read_provider_catalog_keys_by_ids(&[key_id.to_string()])
            .await?
            .into_iter()
            .next()
        else {
            return Ok(None);
        };
        let expected_codex = latest_key
            .upstream_metadata
            .as_ref()
            .and_then(serde_json::Value::as_object)
            .and_then(|metadata| metadata.get("codex"))
            .cloned();
        let mut next_codex = expected_codex
            .as_ref()
            .and_then(serde_json::Value::as_object)
            .cloned()
            .unwrap_or_default();
        let already_processed = codex_reset_write_processed_id(&mut next_codex, idempotency_key);
        let stored_fence = next_codex
            .get(admin_provider_quota_pure::CODEX_QUOTA_ACCOUNT_RESET_FENCE_UNIX_MS_KEY)
            .and_then(admin_provider_quota_pure::coerce_json_u64)
            .zip(
                next_codex
                    .get(admin_provider_quota_pure::CODEX_QUOTA_ACCOUNT_RESET_FENCE_ID_KEY)
                    .and_then(serde_json::Value::as_str)
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .map(ToOwned::to_owned),
            );
        let owns_stored_fence = already_processed
            && stored_fence
                .as_ref()
                .is_some_and(|(_, stored_id)| stored_id == fence_id);
        let installs_fence = !already_processed
            && stored_fence
                .as_ref()
                .is_none_or(|(stored_unix_ms, stored_id)| {
                    (fence_unix_ms, fence_id) > (*stored_unix_ms, stored_id.as_str())
                });
        if installs_fence {
            next_codex.insert(
                admin_provider_quota_pure::CODEX_QUOTA_ACCOUNT_RESET_FENCE_UNIX_MS_KEY.to_string(),
                serde_json::json!(fence_unix_ms),
            );
            next_codex.insert(
                admin_provider_quota_pure::CODEX_QUOTA_ACCOUNT_RESET_FENCE_ID_KEY.to_string(),
                serde_json::json!(fence_id),
            );
            next_codex.insert(
                admin_provider_quota_pure::CODEX_QUOTA_ACCOUNT_RESET_PENDING_KEY.to_string(),
                serde_json::json!(true),
            );
        }
        let effective_fence = CodexAccountResetFence {
            unix_ms: next_codex
                .get(admin_provider_quota_pure::CODEX_QUOTA_ACCOUNT_RESET_FENCE_UNIX_MS_KEY)
                .and_then(admin_provider_quota_pure::coerce_json_u64)
                .unwrap_or(fence_unix_ms),
            id: next_codex
                .get(admin_provider_quota_pure::CODEX_QUOTA_ACCOUNT_RESET_FENCE_ID_KEY)
                .and_then(serde_json::Value::as_str)
                .unwrap_or(fence_id)
                .to_string(),
            generation: next_codex
                .get(admin_provider_quota_pure::CODEX_QUOTA_ACCOUNT_RESET_GENERATION_KEY)
                .and_then(admin_provider_quota_pure::coerce_json_u64)
                .unwrap_or(0),
        };
        let install = if installs_fence || owns_stored_fence {
            CodexAccountResetFenceInstall::Owned(effective_fence)
        } else {
            CodexAccountResetFenceInstall::Superseded
        };
        if already_processed {
            let credential_matches = match (expected_encrypted_auth_config, expected_credential) {
                (Some(expected_auth), Some(expected_credential)) => {
                    latest_key.encrypted_auth_config.as_deref() == Some(expected_auth)
                        && latest_key.encrypted_api_key == expected_credential.encrypted_api_key
                        && latest_key.auth_type == expected_credential.auth_type
                        && latest_key.provider_id == expected_credential.provider_id
                }
                (Some(expected_auth), None) => {
                    latest_key.encrypted_auth_config.as_deref() == Some(expected_auth)
                }
                (None, _) => true,
            };
            return Ok(credential_matches.then_some(install));
        }
        let next_codex = serde_json::Value::Object(next_codex);

        let persisted = if let Some(expected_encrypted_auth_config) = expected_encrypted_auth_config
        {
            if latest_key.encrypted_auth_config.as_deref() != Some(expected_encrypted_auth_config) {
                return Ok(None);
            }
            state
                .app()
                .compare_and_update_provider_catalog_key_oauth_runtime_state(
                    &ProviderCatalogKeyOAuthRuntimeStateCasUpdate {
                        key_id: key_id.to_string(),
                        expected_encrypted_auth_config: Some(
                            expected_encrypted_auth_config.to_string(),
                        ),
                        expected_credential: expected_credential.cloned(),
                        expected_upstream_metadata_namespace: Some(
                            ProviderCatalogUpstreamMetadataNamespaceExpectation {
                                namespace: "codex".to_string(),
                                expected_value: expected_codex,
                            },
                        ),
                        encrypted_auth_config: expected_encrypted_auth_config.to_string(),
                        encrypted_api_key_update: None,
                        expires_at_unix_secs_update: None,
                        oauth_invalid_at_unix_secs: latest_key.oauth_invalid_at_unix_secs,
                        oauth_invalid_reason: latest_key.oauth_invalid_reason.clone(),
                        upstream_metadata_patch: Some(serde_json::json!({
                            "codex": next_codex
                        })),
                        upstream_metadata_namespace_to_remove: None,
                        status_snapshot_patch: serde_json::json!({}),
                        reset_error_count: false,
                        updated_at_unix_secs: Some(fence_unix_ms / 1_000),
                    },
                )
                .await?
        } else {
            state
                .app()
                .update_provider_catalog_key_runtime_metadata(
                    &ProviderCatalogKeyRuntimeMetadataUpdate {
                        key_id: key_id.to_string(),
                        namespace: "codex".to_string(),
                        expected_upstream_metadata_value: expected_codex,
                        upstream_metadata_value: next_codex,
                        status_snapshot_patch: serde_json::json!({}),
                        updated_at_unix_secs: Some(fence_unix_ms / 1_000),
                    },
                )
                .await?
        };
        if persisted {
            return Ok(Some(install));
        }
        if attempt + 1 < CODEX_QUOTA_PERSIST_CAS_MAX_ATTEMPTS {
            let backoff_us = 50_u64.saturating_mul((attempt + 1) as u64).min(1_000);
            tokio::time::sleep(std::time::Duration::from_micros(backoff_us)).await;
        }
    }
    Ok(None)
}

pub(super) fn build_quota_snapshot_payload(
    provider_type: &str,
    current_status_snapshot: Option<&serde_json::Value>,
    metadata_update: Option<&serde_json::Value>,
) -> Option<serde_json::Value> {
    let updated_snapshot = sync_provider_key_quota_status_snapshot(
        current_status_snapshot,
        provider_type,
        metadata_update,
        "refresh_api",
    )?;
    updated_snapshot.get("quota").cloned()
}

pub(super) fn build_provider_quota_execution_plan(
    transport: &AdminGatewayProviderTransportSnapshot,
    spec: ProviderPoolQuotaRequestSpec,
    proxy: Option<ProxySnapshot>,
    transport_profile: Option<ResolvedTransportProfile>,
    timeouts: Option<ExecutionTimeouts>,
) -> ExecutionPlan {
    let ProviderPoolQuotaRequestSpec {
        request_id,
        provider_name,
        quota_kind: _,
        method,
        url,
        mut headers,
        content_type,
        json_body,
        client_api_format,
        provider_api_format,
        model_name,
    } = spec;
    force_provider_quota_redirects_disabled(&mut headers);
    let body = json_body
        .map(RequestBody::from_json)
        .unwrap_or(RequestBody {
            json_body: None,
            body_bytes_b64: None,
            body_ref: None,
        });
    ExecutionPlan {
        request_id,
        candidate_id: None,
        provider_name: Some(provider_name),
        provider_id: transport.provider.id.clone(),
        endpoint_id: transport.endpoint.id.clone(),
        key_id: transport.key.id.clone(),
        method,
        url,
        headers,
        content_type,
        content_encoding: None,
        body,
        stream: false,
        client_api_format,
        provider_api_format,
        model_name,
        proxy,
        transport_profile,
        timeouts,
    }
}

fn force_provider_quota_redirects_disabled(
    headers: &mut std::collections::BTreeMap<String, String>,
) {
    headers.retain(|name, _| !name.eq_ignore_ascii_case(EXECUTION_REQUEST_FOLLOW_REDIRECTS_HEADER));
    headers.insert(
        EXECUTION_REQUEST_FOLLOW_REDIRECTS_HEADER.to_string(),
        "false".to_string(),
    );
}

fn codex_reset_refresh_is_superseded(
    current: Option<&serde_json::Value>,
    context: admin_provider_quota_pure::CodexQuotaMergeContext<'_>,
) -> bool {
    let Some(incoming_fence_id) = context
        .account_reset_fence_id
        .map(str::trim)
        .filter(|value| !value.is_empty())
    else {
        return false;
    };
    current
        .and_then(serde_json::Value::as_object)
        .and_then(|codex| {
            codex.get(admin_provider_quota_pure::CODEX_QUOTA_ACCOUNT_RESET_FENCE_ID_KEY)
        })
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .is_some_and(|stored_fence_id| stored_fence_id != incoming_fence_id)
}

pub(crate) async fn persist_provider_quota_refresh_state(
    state: &AdminAppState<'_>,
    key_id: &str,
    metadata_update: Option<&serde_json::Value>,
    oauth_invalid_at_unix_secs: Option<u64>,
    oauth_invalid_reason: Option<String>,
    encrypted_auth_config: Option<String>,
) -> Result<bool, GatewayError> {
    persist_provider_quota_refresh_state_after_read(
        state,
        key_id,
        metadata_update,
        oauth_invalid_at_unix_secs,
        oauth_invalid_reason,
        encrypted_auth_config,
        None,
        std::future::ready(()),
    )
    .await
}

pub(crate) async fn persist_credential_fenced_provider_quota_refresh_state(
    state: &AdminAppState<'_>,
    key_id: &str,
    metadata_update: Option<&serde_json::Value>,
    oauth_invalid_at_unix_secs: Option<u64>,
    oauth_invalid_reason: Option<String>,
    encrypted_auth_config: Option<String>,
    expected_credential_fence: &ProviderTransportCredentialFence,
) -> Result<bool, GatewayError> {
    persist_provider_quota_refresh_state_after_read(
        state,
        key_id,
        metadata_update,
        oauth_invalid_at_unix_secs,
        oauth_invalid_reason,
        encrypted_auth_config,
        Some(expected_credential_fence),
        std::future::ready(()),
    )
    .await
}

pub(crate) async fn persist_codex_provider_quota_refresh_state(
    state: &AdminAppState<'_>,
    key_id: &str,
    metadata_update: Option<&serde_json::Value>,
    merge_context: admin_provider_quota_pure::CodexQuotaMergeContext<'_>,
) -> Result<bool, GatewayError> {
    let Some(incoming_codex) = metadata_update.and_then(|value| value.get("codex")) else {
        return persist_provider_quota_refresh_state(
            state,
            key_id,
            metadata_update,
            None,
            None,
            None,
        )
        .await;
    };

    for attempt in 0..CODEX_QUOTA_PERSIST_CAS_MAX_ATTEMPTS {
        let Some(mut latest_key) = state
            .read_provider_catalog_keys_by_ids(&[key_id.to_string()])
            .await?
            .into_iter()
            .next()
        else {
            return Ok(false);
        };
        let expected_codex = latest_key
            .upstream_metadata
            .as_ref()
            .and_then(serde_json::Value::as_object)
            .and_then(|metadata| metadata.get("codex"))
            .cloned();
        if !admin_provider_quota_pure::codex_credential_generation_matches(
            expected_codex.as_ref(),
            merge_context.observed_credential_generation,
        ) {
            return Ok(true);
        }
        if codex_reset_refresh_is_superseded(expected_codex.as_ref(), merge_context) {
            return Ok(true);
        }
        let Some(outcome) = admin_provider_quota_pure::merge_codex_quota_metadata_snapshot(
            expected_codex.as_ref(),
            incoming_codex,
            merge_context,
        ) else {
            return Ok(false);
        };
        let merged_update = serde_json::json!({"codex": outcome.metadata.clone()});
        latest_key.upstream_metadata = Some(merge_upstream_metadata(
            latest_key.upstream_metadata.as_ref(),
            &merged_update,
        ));
        latest_key.status_snapshot = sync_provider_key_quota_status_snapshot(
            latest_key.status_snapshot.as_ref(),
            "codex",
            latest_key.upstream_metadata.as_ref(),
            "refresh_api",
        );
        latest_key.updated_at_unix_secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .ok()
            .map(|duration| duration.as_secs());

        let persisted = state
            .app()
            .update_provider_catalog_key_runtime_metadata(
                &ProviderCatalogKeyRuntimeMetadataUpdate {
                    key_id: key_id.to_string(),
                    namespace: "codex".to_string(),
                    expected_upstream_metadata_value: expected_codex,
                    upstream_metadata_value: outcome.metadata,
                    status_snapshot_patch: provider_quota_refresh_status_patch(
                        latest_key.status_snapshot.as_ref(),
                    ),
                    updated_at_unix_secs: latest_key.updated_at_unix_secs,
                },
            )
            .await?;
        if persisted {
            return Ok(true);
        }
        if attempt + 1 < CODEX_QUOTA_PERSIST_CAS_MAX_ATTEMPTS {
            let backoff_us = 50_u64.saturating_mul((attempt + 1) as u64).min(1_000);
            tokio::time::sleep(std::time::Duration::from_micros(backoff_us)).await;
        }
    }
    Ok(false)
}

/// Persist a Codex Agent Identity quota response only when the exact encrypted
/// auth_config used for the request is still installed. Metadata, OAuth state,
/// and their status projection share one repository CAS so a replacement cannot
/// receive any portion of an older response.
pub(crate) async fn persist_fenced_provider_quota_refresh_state(
    state: &AdminAppState<'_>,
    key_id: &str,
    expected_encrypted_auth_config: &str,
    metadata_update: Option<&serde_json::Value>,
    oauth_invalid_at_unix_secs: Option<u64>,
    oauth_invalid_reason: Option<String>,
    merge_context: admin_provider_quota_pure::CodexQuotaMergeContext<'_>,
    expected_credential: &ProviderCatalogKeyOAuthCredentialFence,
) -> Result<bool, GatewayError> {
    let expected_encrypted_auth_config = expected_encrypted_auth_config.trim();
    if expected_encrypted_auth_config.is_empty() {
        return Ok(false);
    }
    if metadata_update.is_some_and(|value| !value.is_object()) {
        return Err(GatewayError::Internal(
            "fenced quota metadata update must be an object".to_string(),
        ));
    }
    for attempt in 0..CODEX_QUOTA_PERSIST_CAS_MAX_ATTEMPTS {
        let Some(mut latest_key) = state
            .read_provider_catalog_keys_by_ids(&[key_id.to_string()])
            .await?
            .into_iter()
            .next()
        else {
            return Ok(false);
        };
        if latest_key.encrypted_auth_config.as_deref() != Some(expected_encrypted_auth_config) {
            return Ok(false);
        }

        let expected_codex = latest_key
            .upstream_metadata
            .as_ref()
            .and_then(serde_json::Value::as_object)
            .and_then(|metadata| metadata.get("codex"))
            .cloned();
        if !admin_provider_quota_pure::codex_credential_generation_matches(
            expected_codex.as_ref(),
            merge_context.observed_credential_generation,
        ) {
            return Ok(true);
        }
        if codex_reset_refresh_is_superseded(expected_codex.as_ref(), merge_context) {
            return Ok(true);
        }
        let merged_codex = match metadata_update.and_then(|value| value.get("codex")) {
            Some(incoming_codex) => {
                let Some(outcome) = admin_provider_quota_pure::merge_codex_quota_metadata_snapshot(
                    expected_codex.as_ref(),
                    incoming_codex,
                    merge_context,
                ) else {
                    return Ok(false);
                };
                outcome.metadata
            }
            None => expected_codex
                .clone()
                .unwrap_or_else(|| serde_json::json!({})),
        };
        let expected_codex_object = expected_codex
            .as_ref()
            .and_then(serde_json::Value::as_object);
        let stale_oauth_state = admin_provider_quota_pure::codex_oauth_state_request_order_is_stale(
            expected_codex_object,
            merge_context.request_started_at_unix_ms,
            merge_context.request_order_id,
        );
        let mut merged_codex = merged_codex.as_object().cloned().unwrap_or_default();
        if !stale_oauth_state {
            if let Some(request_started_at_unix_ms) = merge_context.request_started_at_unix_ms {
                merged_codex.insert(
                    CODEX_OAUTH_STATE_REQUEST_WATERMARK_KEY.to_string(),
                    serde_json::json!(request_started_at_unix_ms),
                );
                if let Some(request_order_id) = merge_context
                    .request_order_id
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                {
                    merged_codex.insert(
                        CODEX_OAUTH_STATE_REQUEST_WATERMARK_ID_KEY.to_string(),
                        serde_json::json!(request_order_id),
                    );
                } else {
                    merged_codex.remove(CODEX_OAUTH_STATE_REQUEST_WATERMARK_ID_KEY);
                }
            }
            (
                latest_key.oauth_invalid_at_unix_secs,
                latest_key.oauth_invalid_reason,
            ) = merge_codex_oauth_response_state(
                &latest_key,
                oauth_invalid_at_unix_secs,
                oauth_invalid_reason.as_deref(),
                merge_context.observed_at_unix_secs,
            );
        }
        let merged_metadata_update = serde_json::json!({
            "codex": serde_json::Value::Object(merged_codex)
        });
        latest_key.upstream_metadata = Some(merge_upstream_metadata(
            latest_key.upstream_metadata.as_ref(),
            &merged_metadata_update,
        ));
        if metadata_update
            .and_then(|value| value.get("codex"))
            .is_some()
        {
            latest_key.status_snapshot = sync_provider_key_quota_status_snapshot(
                latest_key.status_snapshot.as_ref(),
                "codex",
                latest_key.upstream_metadata.as_ref(),
                "refresh_api",
            );
        }
        latest_key.status_snapshot = sync_provider_key_oauth_status_snapshot(
            latest_key.status_snapshot.as_ref(),
            &latest_key,
        );
        latest_key.updated_at_unix_secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .ok()
            .map(|duration| duration.as_secs());

        let updated = state
            .app()
            .compare_and_update_provider_catalog_key_oauth_runtime_state(
                &ProviderCatalogKeyOAuthRuntimeStateCasUpdate {
                    key_id: key_id.to_string(),
                    expected_encrypted_auth_config: Some(
                        expected_encrypted_auth_config.to_string(),
                    ),
                    expected_credential: Some(expected_credential.clone()),
                    expected_upstream_metadata_namespace: Some(
                        ProviderCatalogUpstreamMetadataNamespaceExpectation {
                            namespace: "codex".to_string(),
                            expected_value: expected_codex,
                        },
                    ),
                    encrypted_auth_config: expected_encrypted_auth_config.to_string(),
                    encrypted_api_key_update: None,
                    expires_at_unix_secs_update: None,
                    oauth_invalid_at_unix_secs: latest_key.oauth_invalid_at_unix_secs,
                    oauth_invalid_reason: latest_key.oauth_invalid_reason.clone(),
                    upstream_metadata_patch: Some(merged_metadata_update),
                    upstream_metadata_namespace_to_remove: None,
                    status_snapshot_patch: provider_quota_refresh_status_patch(
                        latest_key.status_snapshot.as_ref(),
                    ),
                    reset_error_count: false,
                    updated_at_unix_secs: latest_key.updated_at_unix_secs,
                },
            )
            .await?;
        if updated {
            return Ok(true);
        }
        if attempt + 1 < CODEX_QUOTA_PERSIST_CAS_MAX_ATTEMPTS {
            let backoff_us = 50_u64.saturating_mul((attempt + 1) as u64).min(1_000);
            tokio::time::sleep(std::time::Duration::from_micros(backoff_us)).await;
        }
    }
    Ok(false)
}

async fn persist_provider_quota_refresh_state_after_read<F>(
    state: &AdminAppState<'_>,
    key_id: &str,
    metadata_update: Option<&serde_json::Value>,
    oauth_invalid_at_unix_secs: Option<u64>,
    oauth_invalid_reason: Option<String>,
    encrypted_auth_config: Option<String>,
    expected_credential_fence: Option<&ProviderTransportCredentialFence>,
    after_read: F,
) -> Result<bool, GatewayError>
where
    F: std::future::Future<Output = ()>,
{
    let safe_metadata_update =
        metadata_update.map(|value| admin_provider_upstream_metadata_safe_json(Some(value)));
    let metadata_update = safe_metadata_update.as_ref();
    let Some(mut latest_key) = state
        .app()
        .list_provider_catalog_keys_by_ids_strong(&[key_id.to_string()])
        .await?
        .into_iter()
        .next()
    else {
        return Ok(false);
    };
    after_read.await;

    // Keep the namespace values observed before applying the refresh response;
    // each runtime metadata write uses them as its CAS expectation.
    let observed_encrypted_auth_config = latest_key.encrypted_auth_config.clone();
    let observed_upstream_metadata = latest_key.upstream_metadata.clone();
    let mut quota_snapshot_provider_type = None::<String>;
    if let Some(metadata_update) = metadata_update {
        latest_key.upstream_metadata = Some(merge_upstream_metadata(
            latest_key.upstream_metadata.as_ref(),
            metadata_update,
        ));
        quota_snapshot_provider_type =
            aether_provider_pool::provider_pool_quota_metadata_provider_type(metadata_update);
    }
    if let Some(encrypted_auth_config) = encrypted_auth_config.as_ref() {
        latest_key.encrypted_auth_config = Some(encrypted_auth_config.clone());
    }
    latest_key.oauth_invalid_at_unix_secs = oauth_invalid_at_unix_secs;
    latest_key.oauth_invalid_reason = oauth_invalid_reason;
    if let Some(provider_type) = quota_snapshot_provider_type.as_deref() {
        latest_key.status_snapshot = sync_provider_key_quota_status_snapshot(
            latest_key.status_snapshot.as_ref(),
            provider_type,
            latest_key.upstream_metadata.as_ref(),
            "refresh_api",
        );
    }
    latest_key.status_snapshot =
        sync_provider_key_oauth_status_snapshot(latest_key.status_snapshot.as_ref(), &latest_key);
    latest_key.updated_at_unix_secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .map(|duration| duration.as_secs());
    let status_patch = provider_quota_refresh_status_patch(latest_key.status_snapshot.as_ref());
    let metadata_updates = metadata_update
        .and_then(serde_json::Value::as_object)
        .map(|updates| {
            let merged = latest_key
                .upstream_metadata
                .as_ref()
                .and_then(serde_json::Value::as_object);
            updates
                .keys()
                .filter_map(|namespace| {
                    merged
                        .and_then(|metadata| metadata.get(namespace))
                        .cloned()
                        .map(|value| (namespace.clone(), value))
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    if let Some(expected_credential_fence) = expected_credential_fence {
        if observed_encrypted_auth_config.as_deref()
            != Some(expected_credential_fence.encrypted_auth_config.as_str())
            || latest_key.encrypted_api_key
                != expected_credential_fence.credential.encrypted_api_key
            || latest_key.auth_type != expected_credential_fence.credential.auth_type
            || latest_key.provider_id != expected_credential_fence.credential.provider_id
        {
            return Ok(false);
        }
        if metadata_updates.len() > 1 {
            return Err(GatewayError::Internal(
                "credential-fenced quota refresh may update at most one metadata namespace"
                    .to_string(),
            ));
        }
        let expected_upstream_metadata_namespace =
            metadata_updates.first().map(|(namespace, _)| {
                ProviderCatalogUpstreamMetadataNamespaceExpectation {
                    namespace: namespace.clone(),
                    expected_value: observed_upstream_metadata
                        .as_ref()
                        .and_then(serde_json::Value::as_object)
                        .and_then(|metadata| metadata.get(namespace))
                        .cloned(),
                }
            });
        return state
            .app()
            .compare_and_update_provider_catalog_key_oauth_runtime_state(
                &ProviderCatalogKeyOAuthRuntimeStateCasUpdate {
                    key_id: key_id.to_string(),
                    expected_encrypted_auth_config: Some(
                        expected_credential_fence.encrypted_auth_config.clone(),
                    ),
                    expected_credential: Some(expected_credential_fence.credential.clone()),
                    expected_upstream_metadata_namespace,
                    encrypted_auth_config: encrypted_auth_config
                        .clone()
                        .unwrap_or_else(|| expected_credential_fence.encrypted_auth_config.clone()),
                    encrypted_api_key_update: None,
                    expires_at_unix_secs_update: None,
                    oauth_invalid_at_unix_secs: latest_key.oauth_invalid_at_unix_secs,
                    oauth_invalid_reason: latest_key.oauth_invalid_reason.clone(),
                    upstream_metadata_patch: metadata_update.cloned(),
                    upstream_metadata_namespace_to_remove: None,
                    status_snapshot_patch: status_patch,
                    reset_error_count: false,
                    updated_at_unix_secs: latest_key.updated_at_unix_secs,
                },
            )
            .await;
    }
    if encrypted_auth_config.is_some() {
        return Err(GatewayError::Internal(
            "provider quota credential update requires a pre-request credential fence".to_string(),
        ));
    }
    if metadata_updates.is_empty() {
        if !state
            .update_provider_catalog_key_oauth_runtime_state(
                key_id,
                latest_key.oauth_invalid_at_unix_secs,
                latest_key.oauth_invalid_reason.as_deref(),
                latest_key.updated_at_unix_secs,
            )
            .await?
        {
            return Ok(false);
        }
        return state
            .update_provider_catalog_key_status_snapshot(&ProviderCatalogKeyStatusSnapshotUpdate {
                key_id: key_id.to_string(),
                status_snapshot_patch: status_patch,
                updated_at_unix_secs: latest_key.updated_at_unix_secs,
            })
            .await;
    }

    for (index, (namespace, value)) in metadata_updates.iter().enumerate() {
        let patch = if index + 1 == metadata_updates.len() {
            status_patch.clone()
        } else {
            serde_json::json!({})
        };
        let expected = observed_upstream_metadata
            .as_ref()
            .and_then(serde_json::Value::as_object)
            .and_then(|metadata| metadata.get(namespace))
            .cloned();
        let persisted = state
            .app()
            .update_provider_catalog_key_runtime_metadata(
                &ProviderCatalogKeyRuntimeMetadataUpdate {
                    key_id: key_id.to_string(),
                    namespace: namespace.clone(),
                    expected_upstream_metadata_value: expected.clone(),
                    upstream_metadata_value: value.clone(),
                    status_snapshot_patch: patch.clone(),
                    updated_at_unix_secs: latest_key.updated_at_unix_secs,
                },
            )
            .await?;
        if !persisted {
            // The refresh response is an authoritative snapshot.  Do not
            // replay it over a newer namespace after a CAS conflict.
            return Ok(false);
        }
    }
    state
        .update_provider_catalog_key_oauth_runtime_state(
            key_id,
            latest_key.oauth_invalid_at_unix_secs,
            latest_key.oauth_invalid_reason.as_deref(),
            latest_key.updated_at_unix_secs,
        )
        .await
}

fn provider_quota_refresh_status_patch(
    status_snapshot: Option<&serde_json::Value>,
) -> serde_json::Value {
    let mut patch = serde_json::Map::new();
    if let Some(snapshot) = status_snapshot.and_then(serde_json::Value::as_object) {
        for field in ["quota", "oauth"] {
            if let Some(value) = snapshot.get(field) {
                patch.insert(field.to_string(), value.clone());
            }
        }
    }
    serde_json::Value::Object(patch)
}

pub(super) async fn execute_provider_quota_plan(
    state: &AdminAppState<'_>,
    transport: &AdminGatewayProviderTransportSnapshot,
    plan: ExecutionPlan,
    quota_kind: &str,
) -> Result<ProviderQuotaExecutionOutcome, GatewayError> {
    let provider_name = plan.provider_name.as_deref().unwrap_or_default();
    if !provider_quota_url_has_allowed_origin(provider_name, &plan.url) {
        warn!(
            key_id = %transport.key.id,
            endpoint_id = %transport.endpoint.id,
            provider_name,
            quota_kind,
            upstream_origin = %crate::handlers::shared::security_log_url_origin(&plan.url),
            "gateway provider quota request blocked by origin policy"
        );
        return Ok(ProviderQuotaExecutionOutcome::Failure(
            "Provider quota request origin is not allowed".to_string(),
        ));
    }

    match state.execute_execution_runtime_sync_plan(None, &plan).await {
        Ok(result) => {
            if !crate::provider_transport::is_codex_agent_identity_transport(transport)
                || !crate::provider_transport::is_codex_agent_identity_invalid_task_response(
                    result.status_code,
                    extract_execution_error_detail(&result).as_deref(),
                )
            {
                return Ok(ProviderQuotaExecutionOutcome::Response(Box::new(result)));
            }

            let refreshed_entry = match state.force_local_oauth_refresh_entry(transport).await {
                Ok(Some(entry)) => entry,
                Ok(None) => {
                    return Ok(ProviderQuotaExecutionOutcome::Failure(
                        "Agent Identity 任务重注册未返回认证信息".to_string(),
                    ));
                }
                Err(_) => {
                    warn!(
                        key_id = %transport.key.id,
                        endpoint_id = %transport.endpoint.id,
                        quota_kind = %quota_kind,
                        "gateway Agent Identity quota task recovery failed"
                    );
                    return Ok(ProviderQuotaExecutionOutcome::Failure(
                        "Agent Identity 任务重注册失败".to_string(),
                    ));
                }
            };
            let header_name = refreshed_entry.auth_header_name.trim().to_ascii_lowercase();
            let header_value = refreshed_entry.auth_header_value.trim();
            if header_name.is_empty() || header_value.is_empty() {
                return Ok(ProviderQuotaExecutionOutcome::Failure(
                    "Agent Identity 任务重注册未返回有效认证信息".to_string(),
                ));
            }

            let mut retry_plan = plan.clone();
            retry_plan
                .headers
                .retain(|name, _| !name.eq_ignore_ascii_case(&header_name));
            retry_plan
                .headers
                .insert(header_name, header_value.to_string());
            match state
                .execute_execution_runtime_sync_plan(None, &retry_plan)
                .await
            {
                Ok(result) => Ok(ProviderQuotaExecutionOutcome::Response(Box::new(result))),
                Err(_) => {
                    warn!(
                        key_id = %transport.key.id,
                        endpoint_id = %transport.endpoint.id,
                        quota_kind = %quota_kind,
                        "gateway Agent Identity quota task recovery retry failed"
                    );
                    Ok(ProviderQuotaExecutionOutcome::Failure(
                        "Provider quota request failed".to_string(),
                    ))
                }
            }
        }
        Err(_) => {
            let proxy_node_id = plan
                .proxy
                .as_ref()
                .and_then(|proxy| proxy.node_id.as_deref())
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(ToOwned::to_owned);
            let proxy_source = state.resolve_transport_proxy_source(transport).await;
            let proxy_url_present = plan
                .proxy
                .as_ref()
                .and_then(|proxy| proxy.url.as_deref())
                .map(str::trim)
                .is_some_and(|value| !value.is_empty());
            warn!(
                key_id = %transport.key.id,
                endpoint_id = %transport.endpoint.id,
                upstream_origin = %crate::handlers::shared::security_log_url_origin(&plan.url),
                proxy_source = ?proxy_source,
                proxy_node_id = ?proxy_node_id,
                proxy_url_present,
                quota_kind = %quota_kind,
                "gateway provider quota execution runtime request failed"
            );
            Ok(ProviderQuotaExecutionOutcome::Failure(
                "Provider quota request failed".to_string(),
            ))
        }
    }
}

fn provider_quota_url_has_allowed_origin(provider_name: &str, value: &str) -> bool {
    let Ok(url) = url::Url::parse(value) else {
        return false;
    };
    if url.scheme() != "https"
        || !url.username().is_empty()
        || url.password().is_some()
        || url.port_or_known_default() != Some(443)
    {
        return false;
    }
    let Some(host) = url.host_str() else {
        return false;
    };

    match provider_name.trim().to_ascii_lowercase().as_str() {
        "antigravity" => matches!(
            host,
            "cloudcode-pa.googleapis.com"
                | "daily-cloudcode-pa.googleapis.com"
                | "daily-cloudcode-pa.sandbox.googleapis.com"
        ),
        "claude_code" => host == "api.anthropic.com",
        "codex" => host == "chatgpt.com",
        "xai" => host == "cli-chat-proxy.grok.com",
        "kiro" => kiro_quota_host_is_allowed(host),
        _ => false,
    }
}

fn kiro_quota_host_is_allowed(host: &str) -> bool {
    let Some(region) = host
        .strip_prefix("q.")
        .and_then(|host| host.strip_suffix(".amazonaws.com"))
    else {
        return false;
    };
    !region.is_empty()
        && region
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
        && region
            .as_bytes()
            .first()
            .is_some_and(u8::is_ascii_alphanumeric)
        && region
            .as_bytes()
            .last()
            .is_some_and(u8::is_ascii_alphanumeric)
}

#[cfg(test)]
#[path = "shared_tests.rs"]
mod tests;
