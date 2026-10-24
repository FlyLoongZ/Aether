use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock, Mutex as StdMutex, Weak};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use aether_data_contracts::repository::candidate_selection::StoredMinimalCandidateSelectionRow;
use aether_data_contracts::repository::provider_catalog::StoredProviderCatalogKey;
use aether_model_fetch::{
    fetch_models_from_transports_for_client_version, ModelFetchTransportRuntime,
};
use aether_provider_transport::GatewayProviderTransportSnapshot;
use aether_runtime_state::RuntimeState;
use async_trait::async_trait;
use futures_util::{stream, StreamExt};
use semver::Version;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use tokio::sync::{Mutex, Semaphore};
use tracing::{debug, info, warn};

use crate::ai_serving::api::codex_client_version;

const CODEX_CATALOG_SCHEMA_VERSION: u32 = 2;
const CODEX_CATALOG_CREDENTIAL_SCOPE_DOMAIN: &str = "aether-codex-catalog-credential-v2";
const CODEX_CLIENT_VERSION_MAX_LEN: usize = 64;
const CODEX_CATALOG_MAX_MODELS: usize = 512;
const CODEX_CATALOG_MAX_BODY_BYTES: usize = 8 * 1024 * 1024;
const CODEX_CATALOG_MAX_AGGREGATE_BODY_BYTES: usize = 8 * 1024 * 1024;
const CODEX_CATALOG_MAX_ETAG_LEN: usize = 256;
const CODEX_CATALOG_MAX_VERSIONS_PER_KEY: usize = 8;
const CODEX_CATALOG_MAX_FAILED_VERSIONS_PER_KEY: usize = 8;
const CODEX_CATALOG_MAX_TARGETS_PER_LOAD: usize = 256;
const CODEX_CATALOG_MAX_COLD_FETCHES_PER_LOAD: usize = 8;
const CODEX_CATALOG_MAX_STALE_REFRESHES_PER_LOAD: usize = 8;
const CODEX_CATALOG_MAX_CONCURRENT_FETCHES: usize = 8;
const CODEX_CATALOG_MAX_CONCURRENT_LOADS: usize = 16;
const CODEX_CATALOG_RETENTION_LOCK_TTL: Duration = Duration::from_secs(5);
const CODEX_CATALOG_RETENTION_LOCK_WAIT: Duration = Duration::from_secs(2);

#[cfg(not(test))]
const CODEX_CATALOG_FRESH_TTL: Duration = Duration::from_secs(5 * 60);
#[cfg(test)]
const CODEX_CATALOG_FRESH_TTL: Duration = Duration::from_secs(1);
#[cfg(not(test))]
const CODEX_CATALOG_RETRY_TTL: Duration = Duration::from_secs(30);
#[cfg(test)]
const CODEX_CATALOG_RETRY_TTL: Duration = Duration::from_millis(100);
#[cfg(not(test))]
const CODEX_CATALOG_FETCH_TIMEOUT: Duration = Duration::from_secs(8);
#[cfg(test)]
const CODEX_CATALOG_FETCH_TIMEOUT: Duration = Duration::from_secs(3);
#[cfg(not(test))]
const CODEX_CATALOG_FETCH_PACE_TTL: Duration = Duration::from_secs(1);
#[cfg(test)]
const CODEX_CATALOG_FETCH_PACE_TTL: Duration = Duration::from_millis(20);
#[cfg(not(test))]
const CODEX_CATALOG_FLIGHT_WAIT_TIMEOUT: Duration = Duration::from_secs(9);
#[cfg(test)]
const CODEX_CATALOG_FLIGHT_WAIT_TIMEOUT: Duration = Duration::from_secs(4);
#[cfg(not(test))]
const CODEX_CATALOG_FLIGHT_LOCK_TTL: Duration = Duration::from_secs(20);
#[cfg(test)]
const CODEX_CATALOG_FLIGHT_LOCK_TTL: Duration = Duration::from_secs(8);
#[cfg(not(test))]
const CODEX_CATALOG_FLIGHT_LEADER_TIMEOUT: Duration = Duration::from_secs(18);
#[cfg(test)]
const CODEX_CATALOG_FLIGHT_LEADER_TIMEOUT: Duration = Duration::from_secs(7);

static CODEX_CATALOG_FETCH_GATE: Semaphore =
    Semaphore::const_new(CODEX_CATALOG_MAX_CONCURRENT_FETCHES);
static CODEX_CATALOG_LOCAL_FALLBACK_FLIGHTS: LazyLock<StdMutex<BTreeMap<String, Weak<Mutex<()>>>>> =
    LazyLock::new(|| StdMutex::new(BTreeMap::new()));
static CODEX_CATALOG_LOCAL_RETENTION: Mutex<()> = Mutex::const_new(());

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct NormalizedCodexClientVersion {
    value: String,
    used_fallback: bool,
}

impl NormalizedCodexClientVersion {
    pub(crate) fn as_str(&self) -> &str {
        self.value.as_str()
    }

    pub(crate) fn used_fallback(&self) -> bool {
        self.used_fallback
    }
}

pub(crate) fn normalize_codex_client_version(raw: Option<&str>) -> NormalizedCodexClientVersion {
    let normalized = raw
        .map(str::trim)
        .filter(|value| !value.is_empty() && value.len() <= CODEX_CLIENT_VERSION_MAX_LEN)
        .and_then(|value| Version::parse(value).ok())
        .map(|version| format!("{}.{}.{}", version.major, version.minor, version.patch));
    match normalized {
        Some(value) => NormalizedCodexClientVersion {
            value,
            used_fallback: false,
        },
        None => NormalizedCodexClientVersion {
            value: codex_client_version(),
            used_fallback: true,
        },
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct CodexCatalogIdentity {
    provider_id: String,
    key_id: String,
}

impl CodexCatalogIdentity {
    fn new(provider_id: &str, key_id: &str) -> Self {
        Self {
            provider_id: provider_id.to_string(),
            key_id: key_id.to_string(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CodexCatalogTarget {
    identity: CodexCatalogIdentity,
    endpoint_ids: Vec<String>,
    credential_scope: Option<String>,
}

impl CodexCatalogTarget {
    fn credential_scope(&self) -> Option<&str> {
        self.credential_scope.as_deref()
    }

    fn with_credential_scope(&self, credential_scope: String) -> Self {
        Self {
            identity: self.identity.clone(),
            endpoint_ids: self.endpoint_ids.clone(),
            credential_scope: Some(credential_scope),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct CodexCatalogSnapshot {
    schema_version: u32,
    pub(crate) provider_id: String,
    pub(crate) key_id: String,
    credential_scope: String,
    pub(crate) client_version: String,
    pub(crate) models: Vec<Value>,
    pub(crate) etag: Option<String>,
    pub(crate) fetched_at_unix_secs: u64,
    pub(crate) last_checked_at_unix_secs: u64,
    pub(crate) content_sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct CodexCatalogStatus {
    schema_version: u32,
    provider_id: String,
    key_id: String,
    credential_scope: String,
    client_version: String,
    last_checked_at_unix_secs: u64,
    last_success_at_unix_secs: Option<u64>,
    last_error: Option<String>,
    last_http_status: Option<u16>,
    model_count: Option<usize>,
    fetch_duration_ms: u64,
    used_lkg: bool,
}

#[derive(Debug, Default)]
pub(crate) struct CodexCatalogLoad {
    snapshots: BTreeMap<CodexCatalogIdentity, CodexCatalogSnapshot>,
    stale_targets: Vec<CodexCatalogTarget>,
    complete: bool,
}

impl CodexCatalogLoad {
    pub(crate) fn snapshot(
        &self,
        provider_id: &str,
        key_id: &str,
    ) -> Option<&CodexCatalogSnapshot> {
        self.snapshots
            .get(&CodexCatalogIdentity::new(provider_id, key_id))
    }

    pub(crate) fn stale_targets(&self) -> &[CodexCatalogTarget] {
        &self.stale_targets
    }

    pub(crate) fn is_complete(&self) -> bool {
        self.complete
    }
}

#[async_trait]
pub(crate) trait CodexCatalogRuntime: ModelFetchTransportRuntime + Send + Sync {
    fn codex_catalog_runtime_state(&self) -> &RuntimeState;

    fn codex_catalog_fetch_pace_ttl(&self) -> Duration {
        CODEX_CATALOG_FETCH_PACE_TTL
    }

    async fn read_codex_catalog_transport_snapshot(
        &self,
        provider_id: &str,
        endpoint_id: &str,
        key_id: &str,
    ) -> Result<Option<GatewayProviderTransportSnapshot>, String>;

    async fn read_codex_catalog_credential_scope_strong(
        &self,
        provider_id: &str,
        key_id: &str,
    ) -> Result<Option<String>, String>;
}

fn credential_scope_digest(kind: &str, value: &str) -> Option<String> {
    let value = value.trim();
    (!value.is_empty()).then(|| {
        sha256_hex(format!("{CODEX_CATALOG_CREDENTIAL_SCOPE_DOMAIN}\0{kind}\0{value}").as_bytes())
    })
}

fn codex_catalog_credential_scope_from_parts(
    upstream_metadata: Option<&Value>,
    decrypted_auth_config: Option<&str>,
    decrypted_api_key: Option<&str>,
) -> Option<String> {
    let codex_metadata = upstream_metadata.and_then(|metadata| metadata.get("codex"));
    if let Some(generation) =
        aether_admin::provider::quota::codex_credential_generation(codex_metadata)
    {
        if let Some(scope) = credential_scope_digest("generation", generation) {
            return Some(scope);
        }
    }

    let auth_config = decrypted_auth_config
        .map(str::trim)
        .filter(|value| !value.is_empty());
    if let Some(account_id) = crate::ai_serving::parse_codex_auth_identity(auth_config).account_id {
        return credential_scope_digest("account_id", &account_id);
    }
    let parsed_auth_config = auth_config.and_then(|raw| serde_json::from_str::<Value>(raw).ok());
    if let Some(refresh_token) = parsed_auth_config
        .as_ref()
        .and_then(|value| value.get("refresh_token"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        return credential_scope_digest("refresh_token", refresh_token);
    }
    if let Some(auth_config) = auth_config {
        return credential_scope_digest("auth_config", auth_config);
    }
    decrypted_api_key
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .and_then(|value| credential_scope_digest("api_key", value))
}

pub(crate) fn codex_catalog_credential_scope_from_stored_key(
    key: &StoredProviderCatalogKey,
    decrypted_auth_config: Option<&str>,
    decrypted_api_key: Option<&str>,
) -> Option<String> {
    codex_catalog_credential_scope_from_parts(
        key.upstream_metadata.as_ref(),
        decrypted_auth_config,
        decrypted_api_key,
    )
}

fn codex_catalog_credential_scope_from_transport(
    transport: &GatewayProviderTransportSnapshot,
) -> Option<String> {
    codex_catalog_credential_scope_from_parts(
        transport.key.upstream_metadata.as_ref(),
        transport.key.decrypted_auth_config.as_deref(),
        Some(&transport.key.decrypted_api_key),
    )
}

pub(crate) fn codex_catalog_targets(
    rows: &[StoredMinimalCandidateSelectionRow],
) -> Vec<CodexCatalogTarget> {
    let mut endpoints_by_identity = BTreeMap::<CodexCatalogIdentity, BTreeSet<String>>::new();
    for row in rows
        .iter()
        .filter(|row| row.provider_type.trim().eq_ignore_ascii_case("codex"))
    {
        endpoints_by_identity
            .entry(CodexCatalogIdentity::new(&row.provider_id, &row.key_id))
            .or_default()
            .insert(row.endpoint_id.clone());
    }
    endpoints_by_identity
        .into_iter()
        .map(|(identity, endpoint_ids)| CodexCatalogTarget {
            identity,
            endpoint_ids: endpoint_ids.into_iter().collect(),
            credential_scope: None,
        })
        .collect()
}

async fn bind_codex_catalog_target<R>(
    runtime: &R,
    target: &CodexCatalogTarget,
) -> Option<CodexCatalogTarget>
where
    R: CodexCatalogRuntime + ?Sized,
{
    match runtime
        .read_codex_catalog_credential_scope_strong(
            &target.identity.provider_id,
            &target.identity.key_id,
        )
        .await
    {
        Ok(Some(scope)) if !scope.is_empty() => Some(target.with_credential_scope(scope)),
        Ok(_) => {
            warn!(
                event_name = "codex_catalog_credential_scope_missing",
                provider_id = %target.identity.provider_id,
                key_id = %target.identity.key_id,
                "Codex catalog key was missing, inactive, or lacked a safe credential scope"
            );
            None
        }
        Err(error) => {
            warn!(
                event_name = "codex_catalog_credential_scope_error",
                provider_id = %target.identity.provider_id,
                key_id = %target.identity.key_id,
                error = %safe_catalog_error(&error),
                "Codex catalog credential scope strong read failed"
            );
            None
        }
    }
}

pub(crate) async fn load_codex_catalogs<R>(
    runtime: &R,
    targets: &[CodexCatalogTarget],
    client_version: &NormalizedCodexClientVersion,
) -> CodexCatalogLoad
where
    R: CodexCatalogRuntime + ?Sized,
{
    let version = client_version.as_str().to_string();
    if targets.len() > CODEX_CATALOG_MAX_TARGETS_PER_LOAD {
        warn!(
            event_name = "codex_catalog_target_limit",
            client_version = %version,
            target_count = targets.len(),
            limit = CODEX_CATALOG_MAX_TARGETS_PER_LOAD,
            "Codex catalog request exceeded the target safety limit"
        );
        return CodexCatalogLoad::default();
    }
    let remember_version = !client_version.used_fallback();
    let cold_fetch_budget = Arc::new(AtomicUsize::new(CODEX_CATALOG_MAX_COLD_FETCHES_PER_LOAD));
    let results = stream::iter(targets.iter().cloned())
        .map(|target| {
            let version = version.clone();
            let cold_fetch_budget = Arc::clone(&cold_fetch_budget);
            async move {
                let Some(target) = bind_codex_catalog_target(runtime, &target).await else {
                    return (target, None);
                };
                let loaded = load_codex_catalog(
                    runtime,
                    &target,
                    &version,
                    remember_version,
                    cold_fetch_budget.as_ref(),
                )
                .await;
                (target, loaded)
            }
        })
        .buffer_unordered(CODEX_CATALOG_MAX_CONCURRENT_LOADS);
    futures_util::pin_mut!(results);

    let mut load = CodexCatalogLoad {
        complete: true,
        ..CodexCatalogLoad::default()
    };
    let mut aggregate_body_bytes = 0usize;
    while let Some((target, loaded)) = results.next().await {
        let Some((snapshot, is_stale, should_refresh)) = loaded else {
            load.complete = false;
            continue;
        };
        let snapshot_body_bytes = match serde_json::to_vec(&snapshot.models) {
            Ok(serialized) => serialized.len(),
            Err(error) => {
                warn!(
                    event_name = "codex_catalog_aggregate_serialize_error",
                    provider_id = %target.identity.provider_id,
                    key_id = %target.identity.key_id,
                    client_version = %version,
                    error = %error,
                    "Codex source catalog could not be measured for aggregate safety"
                );
                load.complete = false;
                load.snapshots.clear();
                load.stale_targets.clear();
                break;
            }
        };
        let Some(next_aggregate_body_bytes) = aggregate_body_bytes.checked_add(snapshot_body_bytes)
        else {
            warn!(
                event_name = "codex_catalog_source_aggregate_body_limit",
                provider_id = %target.identity.provider_id,
                key_id = %target.identity.key_id,
                client_version = %version,
                limit_bytes = CODEX_CATALOG_MAX_AGGREGATE_BODY_BYTES,
                "Codex source catalog aggregate size overflowed; aggregation was aborted"
            );
            load.complete = false;
            load.snapshots.clear();
            load.stale_targets.clear();
            break;
        };
        if next_aggregate_body_bytes > CODEX_CATALOG_MAX_AGGREGATE_BODY_BYTES {
            warn!(
                event_name = "codex_catalog_source_aggregate_body_limit",
                provider_id = %target.identity.provider_id,
                key_id = %target.identity.key_id,
                client_version = %version,
                aggregate_body_bytes = next_aggregate_body_bytes,
                limit_bytes = CODEX_CATALOG_MAX_AGGREGATE_BODY_BYTES,
                "Codex source catalogs exceeded the aggregate body limit; aggregation was aborted"
            );
            load.complete = false;
            load.snapshots.clear();
            load.stale_targets.clear();
            break;
        }
        aggregate_body_bytes = next_aggregate_body_bytes;
        load.snapshots.insert(target.identity.clone(), snapshot);
        if is_stale
            && should_refresh
            && load.stale_targets.len() < CODEX_CATALOG_MAX_STALE_REFRESHES_PER_LOAD
        {
            load.stale_targets.push(target);
        }
    }
    load
}

pub(crate) async fn refresh_codex_catalog_target<R>(
    runtime: &R,
    target: &CodexCatalogTarget,
    client_version: &NormalizedCodexClientVersion,
) where
    R: CodexCatalogRuntime + ?Sized,
{
    let version = client_version.as_str();
    let Some(target) = bind_codex_catalog_target(runtime, target).await else {
        return;
    };
    if runtime
        .codex_catalog_runtime_state()
        .kv_get(&catalog_retry_key(&target, version))
        .await
        .ok()
        .flatten()
        .is_some()
    {
        return;
    }
    let _ = fetch_catalog_singleflight(runtime, &target, version, false).await;
}

async fn load_codex_catalog<R>(
    runtime: &R,
    target: &CodexCatalogTarget,
    client_version: &str,
    remember_version: bool,
    cold_fetch_budget: &AtomicUsize,
) -> Option<(CodexCatalogSnapshot, bool, bool)>
where
    R: CodexCatalogRuntime + ?Sized,
{
    if remember_version {
        remember_seen_version(
            runtime.codex_catalog_runtime_state(),
            target,
            client_version,
        )
        .await;
    }
    if let Some(snapshot) = read_lkg_snapshot(runtime, target, client_version).await {
        touch_catalog_version_best_effort(
            runtime.codex_catalog_runtime_state(),
            target,
            client_version,
        );
        let fresh_hash = runtime
            .codex_catalog_runtime_state()
            .kv_get(&catalog_fresh_key(target, client_version))
            .await
            .ok()
            .flatten();
        let is_fresh = fresh_hash.as_deref() == Some(snapshot.content_sha256.as_str());
        if is_fresh {
            debug!(
                event_name = "codex_catalog_cache",
                provider_id = %target.identity.provider_id,
                key_id = %target.identity.key_id,
                client_version,
                cache_state = "fresh",
                model_count = snapshot.models.len(),
                "Codex catalog fresh cache hit"
            );
            return Some((snapshot, false, false));
        }
        let retry_suppressed = runtime
            .codex_catalog_runtime_state()
            .kv_get(&catalog_retry_key(target, client_version))
            .await
            .ok()
            .flatten()
            .is_some();
        info!(
            event_name = "codex_catalog_cache",
            provider_id = %target.identity.provider_id,
            key_id = %target.identity.key_id,
            client_version,
            cache_state = "stale",
            model_count = snapshot.models.len(),
            refresh_suppressed = retry_suppressed,
            "Codex catalog stale LKG hit"
        );
        return Some((snapshot, true, !retry_suppressed));
    }

    info!(
        event_name = "codex_catalog_cache",
        provider_id = %target.identity.provider_id,
        key_id = %target.identity.key_id,
        client_version,
        cache_state = "cold_miss",
        "Codex catalog cold cache miss"
    );
    if runtime
        .codex_catalog_runtime_state()
        .kv_get(&catalog_retry_key(target, client_version))
        .await
        .ok()
        .flatten()
        .is_some()
    {
        debug!(
            event_name = "codex_catalog_cold_retry_suppressed",
            provider_id = %target.identity.provider_id,
            key_id = %target.identity.key_id,
            client_version,
            "Codex catalog cold retry was suppressed by the failure cooldown"
        );
        return None;
    }
    if cold_fetch_budget
        .try_update(Ordering::AcqRel, Ordering::Acquire, |remaining| {
            remaining.checked_sub(1)
        })
        .is_err()
    {
        debug!(
            event_name = "codex_catalog_cold_fetch_budget_exhausted",
            provider_id = %target.identity.provider_id,
            key_id = %target.identity.key_id,
            client_version,
            limit = CODEX_CATALOG_MAX_COLD_FETCHES_PER_LOAD,
            "Codex catalog cold fetch was deferred to a later request"
        );
        return None;
    }
    if let Some(snapshot) = fetch_catalog_singleflight(runtime, target, client_version, true).await
    {
        return Some((snapshot, false, false));
    }
    None
}

async fn fetch_catalog_singleflight<R>(
    runtime: &R,
    target: &CodexCatalogTarget,
    client_version: &str,
    wait_for_existing: bool,
) -> Option<CodexCatalogSnapshot>
where
    R: CodexCatalogRuntime + ?Sized,
{
    let state = runtime.codex_catalog_runtime_state();
    let lock_key = catalog_flight_key(target, client_version);
    let owner = format!("codex-catalog:{}", std::process::id());
    match state
        .lock_try_acquire(&lock_key, &owner, CODEX_CATALOG_FLIGHT_LOCK_TTL)
        .await
    {
        Ok(Some(lease)) => {
            let snapshot = match tokio::time::timeout(
                CODEX_CATALOG_FLIGHT_LEADER_TIMEOUT,
                load_or_fetch_catalog_leader(runtime, target, client_version, wait_for_existing),
            )
            .await
            {
                Ok(snapshot) => snapshot,
                Err(_) => {
                    warn!(
                        event_name = "codex_catalog_singleflight_leader_timeout",
                        provider_id = %target.identity.provider_id,
                        key_id = %target.identity.key_id,
                        client_version,
                        "Codex catalog singleflight leader exceeded its lease-safe deadline"
                    );
                    None
                }
            };
            match state.lock_release(&lease).await {
                Ok(true) => {}
                Ok(false) => warn!(
                    event_name = "codex_catalog_singleflight_lease_lost",
                    provider_id = %target.identity.provider_id,
                    key_id = %target.identity.key_id,
                    client_version,
                    "Codex catalog singleflight lease was no longer owned at release"
                ),
                Err(error) => warn!(
                    event_name = "codex_catalog_singleflight_release_error",
                    provider_id = %target.identity.provider_id,
                    key_id = %target.identity.key_id,
                    client_version,
                    error = %error,
                    "Codex catalog singleflight lease release failed"
                ),
            }
            snapshot
        }
        Ok(None) if wait_for_existing => {
            wait_for_catalog_flight(runtime, target, client_version).await
        }
        Ok(None) => None,
        Err(error) => {
            warn!(
                event_name = "codex_catalog_singleflight_error",
                provider_id = %target.identity.provider_id,
                key_id = %target.identity.key_id,
                client_version,
                error = %error,
                "Codex catalog singleflight backend unavailable"
            );
            let local_flight = local_catalog_fallback_flight(&lock_key);
            let local_guard =
                match tokio::time::timeout(CODEX_CATALOG_FLIGHT_WAIT_TIMEOUT, local_flight.lock())
                    .await
                {
                    Ok(guard) => guard,
                    Err(_) => {
                        prune_local_catalog_fallback_flight(&lock_key, &local_flight);
                        return read_lkg_snapshot(runtime, target, client_version).await;
                    }
                };
            let snapshot =
                load_or_fetch_catalog_leader(runtime, target, client_version, wait_for_existing)
                    .await;
            drop(local_guard);
            prune_local_catalog_fallback_flight(&lock_key, &local_flight);
            snapshot
        }
    }
}

fn local_catalog_fallback_flight(lock_key: &str) -> Arc<Mutex<()>> {
    let mut flights = CODEX_CATALOG_LOCAL_FALLBACK_FLIGHTS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    flights.retain(|_, flight| flight.strong_count() > 0);
    if let Some(flight) = flights.get(lock_key).and_then(Weak::upgrade) {
        return flight;
    }
    let flight = Arc::new(Mutex::new(()));
    flights.insert(lock_key.to_string(), Arc::downgrade(&flight));
    flight
}

fn prune_local_catalog_fallback_flight(lock_key: &str, flight: &Arc<Mutex<()>>) {
    let mut flights = CODEX_CATALOG_LOCAL_FALLBACK_FLIGHTS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let weak = Arc::downgrade(flight);
    let remove = flights
        .get(lock_key)
        .is_some_and(|stored| stored.ptr_eq(&weak) && Arc::strong_count(flight) == 1);
    if remove {
        flights.remove(lock_key);
    }
}

async fn load_or_fetch_catalog_leader<R>(
    runtime: &R,
    target: &CodexCatalogTarget,
    client_version: &str,
    wait_for_existing: bool,
) -> Option<CodexCatalogSnapshot>
where
    R: CodexCatalogRuntime + ?Sized,
{
    let state = runtime.codex_catalog_runtime_state();
    let existing = read_lkg_snapshot(runtime, target, client_version).await;
    let fresh_matches = if let Some(snapshot) = existing.as_ref() {
        state
            .kv_get(&catalog_fresh_key(target, client_version))
            .await
            .ok()
            .flatten()
            .as_deref()
            == Some(snapshot.content_sha256.as_str())
    } else {
        false
    };
    let retry_suppressed = state
        .kv_get(&catalog_retry_key(target, client_version))
        .await
        .ok()
        .flatten()
        .is_some();
    if fresh_matches || retry_suppressed || (wait_for_existing && existing.is_some()) {
        return existing;
    }
    if !acquire_catalog_fetch_pace(runtime, target, client_version).await {
        return existing;
    }
    fetch_catalog_with_deadline(runtime, target, client_version).await
}

async fn acquire_catalog_fetch_pace<R>(
    runtime: &R,
    target: &CodexCatalogTarget,
    client_version: &str,
) -> bool
where
    R: CodexCatalogRuntime + ?Sized,
{
    let state = runtime.codex_catalog_runtime_state();
    let lock_key = catalog_fetch_pace_key(target);
    let owner = format!("codex-catalog-fetch-pace:{}", std::process::id());
    let pace_ttl = runtime.codex_catalog_fetch_pace_ttl();
    match state.lock_try_acquire(&lock_key, &owner, pace_ttl).await {
        Ok(Some(_lease)) => true,
        Ok(None) => {
            debug!(
                event_name = "codex_catalog_fetch_paced",
                provider_id = %target.identity.provider_id,
                key_id = %target.identity.key_id,
                client_version,
                pace_ms = duration_ms(pace_ttl),
                "Codex catalog upstream fetch was suppressed by the per-key pace limit"
            );
            false
        }
        Err(error) => {
            warn!(
                event_name = "codex_catalog_fetch_pace_error",
                provider_id = %target.identity.provider_id,
                key_id = %target.identity.key_id,
                client_version,
                error = %error,
                "Codex catalog upstream fetch was suppressed because the pace limiter was unavailable"
            );
            false
        }
    }
}

async fn fetch_catalog_with_deadline<R>(
    runtime: &R,
    target: &CodexCatalogTarget,
    client_version: &str,
) -> Option<CodexCatalogSnapshot>
where
    R: CodexCatalogRuntime + ?Sized,
{
    let started = Instant::now();
    match tokio::time::timeout(
        CODEX_CATALOG_FETCH_TIMEOUT,
        fetch_and_store_catalog(runtime, target, client_version),
    )
    .await
    {
        Ok(snapshot) => snapshot,
        Err(_) => {
            record_catalog_failure(
                runtime,
                target,
                client_version,
                current_unix_secs(),
                started.elapsed(),
                None,
                "upstream catalog fetch timed out",
            )
            .await;
            read_lkg_snapshot(runtime, target, client_version).await
        }
    }
}

async fn wait_for_catalog_flight<R>(
    runtime: &R,
    target: &CodexCatalogTarget,
    client_version: &str,
) -> Option<CodexCatalogSnapshot>
where
    R: CodexCatalogRuntime + ?Sized,
{
    let deadline = tokio::time::Instant::now() + CODEX_CATALOG_FLIGHT_WAIT_TIMEOUT;
    loop {
        if let Some(snapshot) = read_lkg_snapshot(runtime, target, client_version).await {
            return Some(snapshot);
        }
        if runtime
            .codex_catalog_runtime_state()
            .kv_get(&catalog_retry_key(target, client_version))
            .await
            .ok()
            .flatten()
            .is_some()
        {
            return None;
        }
        if tokio::time::Instant::now() >= deadline {
            warn!(
                event_name = "codex_catalog_singleflight_wait_timeout",
                provider_id = %target.identity.provider_id,
                key_id = %target.identity.key_id,
                client_version,
                "Codex catalog cold miss timed out waiting for in-flight fetch"
            );
            return None;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

async fn fetch_and_store_catalog<R>(
    runtime: &R,
    target: &CodexCatalogTarget,
    client_version: &str,
) -> Option<CodexCatalogSnapshot>
where
    R: CodexCatalogRuntime + ?Sized,
{
    let started = Instant::now();
    let now = current_unix_secs();
    let expected_scope = target.credential_scope()?;
    let mut transports = Vec::new();
    let mut transport_load_failed = false;
    for endpoint_id in &target.endpoint_ids {
        match runtime
            .read_codex_catalog_transport_snapshot(
                &target.identity.provider_id,
                endpoint_id,
                &target.identity.key_id,
            )
            .await
        {
            Ok(Some(transport)) => {
                let transport_scope = codex_catalog_credential_scope_from_transport(&transport);
                if transport_scope.as_deref() != Some(expected_scope) {
                    transport_load_failed = true;
                    warn!(
                        event_name = "codex_catalog_transport_credential_scope_mismatch",
                        provider_id = %target.identity.provider_id,
                        endpoint_id,
                        key_id = %target.identity.key_id,
                        client_version,
                        "Codex catalog cached transport did not match the strong credential scope"
                    );
                } else {
                    transports.push(transport);
                }
            }
            Ok(None) => {
                transport_load_failed = true;
                warn!(
                    event_name = "codex_catalog_transport_missing",
                    provider_id = %target.identity.provider_id,
                    endpoint_id,
                    key_id = %target.identity.key_id,
                    client_version,
                    "Codex catalog transport snapshot unavailable"
                );
            }
            Err(error) => {
                transport_load_failed = true;
                warn!(
                    event_name = "codex_catalog_transport_error",
                    provider_id = %target.identity.provider_id,
                    endpoint_id,
                    key_id = %target.identity.key_id,
                    client_version,
                    error = %safe_catalog_error(&error),
                    "Codex catalog transport snapshot failed"
                );
            }
        }
    }
    if transport_load_failed || transports.is_empty() {
        record_catalog_failure(
            runtime,
            target,
            client_version,
            now,
            started.elapsed(),
            None,
            "one or more catalog transports were unavailable",
        )
        .await;
        return None;
    }

    let permit = match CODEX_CATALOG_FETCH_GATE.acquire().await {
        Ok(permit) => permit,
        Err(_) => {
            record_catalog_failure(
                runtime,
                target,
                client_version,
                now,
                started.elapsed(),
                None,
                "catalog fetch concurrency limit unavailable",
            )
            .await;
            return None;
        }
    };

    let fetched =
        fetch_models_from_transports_for_client_version(runtime, &transports, Some(client_version))
            .await;
    drop(permit);

    let outcome = match fetched {
        Err(error) => {
            record_catalog_failure(
                runtime,
                target,
                client_version,
                now,
                started.elapsed(),
                None,
                &error,
            )
            .await;
            return None;
        }
        Ok(outcome) => outcome,
    };

    if !outcome.has_success {
        let error = if outcome.errors.is_empty() {
            "upstream catalog fetch failed".to_string()
        } else {
            outcome.errors.join("; ")
        };
        record_catalog_failure(
            runtime,
            target,
            client_version,
            now,
            started.elapsed(),
            outcome.upstream_status,
            &error,
        )
        .await;
        return None;
    }
    if !outcome.errors.is_empty() {
        let error = outcome.errors.join("; ");
        record_catalog_failure(
            runtime,
            target,
            client_version,
            now,
            started.elapsed(),
            outcome.upstream_status,
            &error,
        )
        .await;
        return None;
    }

    let models = outcome.cached_models;
    let serialized_models = match validate_catalog_models(&models) {
        Ok(serialized) => serialized,
        Err(error) => {
            record_catalog_failure(
                runtime,
                target,
                client_version,
                now,
                started.elapsed(),
                outcome.upstream_status,
                &error,
            )
            .await;
            return None;
        }
    };
    let snapshot = CodexCatalogSnapshot {
        schema_version: CODEX_CATALOG_SCHEMA_VERSION,
        provider_id: target.identity.provider_id.clone(),
        key_id: target.identity.key_id.clone(),
        credential_scope: expected_scope.to_string(),
        client_version: client_version.to_string(),
        models,
        etag: outcome.etag.as_deref().and_then(normalize_etag),
        fetched_at_unix_secs: now,
        last_checked_at_unix_secs: now,
        content_sha256: sha256_hex(&serialized_models),
    };
    if !codex_catalog_credential_scope_is_current(runtime, target).await {
        warn!(
            event_name = "codex_catalog_credential_scope_changed",
            provider_id = %target.identity.provider_id,
            key_id = %target.identity.key_id,
            client_version,
            "Codex catalog fetch result was discarded because the credential scope changed"
        );
        return None;
    }
    persist_catalog_success(
        runtime,
        target,
        client_version,
        &snapshot,
        outcome.upstream_status,
        started.elapsed(),
    )
    .await;
    if !codex_catalog_credential_scope_is_current(runtime, target).await {
        discard_catalog_success_after_scope_change(
            runtime.codex_catalog_runtime_state(),
            target,
            client_version,
        )
        .await;
        warn!(
            event_name = "codex_catalog_credential_scope_changed_after_persist",
            provider_id = %target.identity.provider_id,
            key_id = %target.identity.key_id,
            client_version,
            "Codex catalog fetch result was removed because the credential scope changed during persistence"
        );
        return None;
    }
    Some(snapshot)
}

async fn discard_catalog_success_after_scope_change(
    state: &RuntimeState,
    target: &CodexCatalogTarget,
    client_version: &str,
) {
    let keys = vec![
        catalog_lkg_key(target, client_version),
        catalog_fresh_key(target, client_version),
        catalog_status_key(target, client_version),
        catalog_retry_key(target, client_version),
    ];
    if let Err(error) = state.kv_delete_many(&keys).await {
        warn!(
            event_name = "codex_catalog_scope_changed_cleanup_error",
            provider_id = %target.identity.provider_id,
            key_id = %target.identity.key_id,
            client_version,
            error = %error,
            "Codex catalog stale credential-scope KV cleanup failed"
        );
    }
    let member = catalog_version_member(target, client_version);
    let _ = state
        .score_remove(&catalog_versions_key(&target.identity), &member)
        .await;
    let _ = state
        .score_remove(&catalog_failed_versions_key(&target.identity), &member)
        .await;
}

async fn codex_catalog_credential_scope_is_current<R>(
    runtime: &R,
    target: &CodexCatalogTarget,
) -> bool
where
    R: CodexCatalogRuntime + ?Sized,
{
    let Some(expected_scope) = target.credential_scope() else {
        return false;
    };
    matches!(
        runtime
            .read_codex_catalog_credential_scope_strong(
                &target.identity.provider_id,
                &target.identity.key_id,
            )
            .await,
        Ok(Some(current_scope)) if current_scope == expected_scope
    )
}

fn validate_catalog_models(models: &[Value]) -> Result<Vec<u8>, String> {
    if models.is_empty() {
        return Err("upstream catalog returned no models".to_string());
    }
    if models.len() > CODEX_CATALOG_MAX_MODELS {
        return Err(format!(
            "upstream catalog model count exceeds {CODEX_CATALOG_MAX_MODELS}"
        ));
    }
    if models
        .iter()
        .any(|model| !catalog_model_has_valid_identity(model))
    {
        return Err("upstream catalog contains a model without a valid id or slug".to_string());
    }
    let serialized = serde_json::to_vec(models)
        .map_err(|_| "upstream catalog models could not be serialized".to_string())?;
    if serialized.len() > CODEX_CATALOG_MAX_BODY_BYTES {
        return Err(format!(
            "upstream catalog body exceeds {CODEX_CATALOG_MAX_BODY_BYTES} bytes"
        ));
    }
    Ok(serialized)
}

fn catalog_model_has_valid_identity(model: &Value) -> bool {
    let Some(object) = model.as_object() else {
        return false;
    };
    ["slug", "id"].iter().any(|field| {
        object
            .get(*field)
            .and_then(Value::as_str)
            .filter(|value| *value == value.trim())
            .is_some_and(valid_model_identity)
    })
}

fn valid_model_identity(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 256
        && value == value.trim()
        && value
            .chars()
            .all(|character| !character.is_whitespace() && !character.is_control())
}

async fn persist_catalog_success<R>(
    runtime: &R,
    target: &CodexCatalogTarget,
    client_version: &str,
    snapshot: &CodexCatalogSnapshot,
    status_code: Option<u16>,
    elapsed: Duration,
) where
    R: CodexCatalogRuntime + ?Sized,
{
    let state = runtime.codex_catalog_runtime_state();
    let serialized = match serde_json::to_string(snapshot) {
        Ok(serialized) => serialized,
        Err(error) => {
            warn!(
                event_name = "codex_catalog_snapshot_serialize_error",
                provider_id = %target.identity.provider_id,
                key_id = %target.identity.key_id,
                client_version,
                error = %error,
                "Codex catalog snapshot serialization failed"
            );
            return;
        }
    };
    if let Err(error) = state
        .kv_set(&catalog_lkg_key(target, client_version), serialized, None)
        .await
    {
        warn!(
            event_name = "codex_catalog_lkg_write_error",
            provider_id = %target.identity.provider_id,
            key_id = %target.identity.key_id,
            client_version,
            error = %error,
            "Codex catalog LKG write failed"
        );
        return;
    }
    if let Err(error) = state
        .kv_set(
            &catalog_fresh_key(target, client_version),
            snapshot.content_sha256.clone(),
            Some(CODEX_CATALOG_FRESH_TTL),
        )
        .await
    {
        warn!(
            event_name = "codex_catalog_fresh_marker_write_error",
            provider_id = %target.identity.provider_id,
            key_id = %target.identity.key_id,
            client_version,
            error = %error,
            "Codex catalog fresh marker write failed"
        );
    }
    let _ = state
        .kv_delete(&catalog_retry_key(target, client_version))
        .await;
    let status = CodexCatalogStatus {
        schema_version: CODEX_CATALOG_SCHEMA_VERSION,
        provider_id: target.identity.provider_id.clone(),
        key_id: target.identity.key_id.clone(),
        credential_scope: target.credential_scope().unwrap_or_default().to_string(),
        client_version: client_version.to_string(),
        last_checked_at_unix_secs: snapshot.last_checked_at_unix_secs,
        last_success_at_unix_secs: Some(snapshot.fetched_at_unix_secs),
        last_error: None,
        last_http_status: status_code,
        model_count: Some(snapshot.models.len()),
        fetch_duration_ms: duration_ms(elapsed),
        used_lkg: false,
    };
    write_catalog_status(state, target, client_version, &status).await;
    retain_catalog_version(state, target, client_version, true).await;
    info!(
        event_name = "codex_catalog_fetch_success",
        provider_id = %target.identity.provider_id,
        key_id = %target.identity.key_id,
        client_version,
        status_code,
        model_count = snapshot.models.len(),
        duration_ms = duration_ms(elapsed),
        "Codex catalog upstream fetch succeeded"
    );
}

#[allow(clippy::too_many_arguments)]
async fn record_catalog_failure<R>(
    runtime: &R,
    target: &CodexCatalogTarget,
    client_version: &str,
    checked_at: u64,
    elapsed: Duration,
    status_code: Option<u16>,
    error: &str,
) where
    R: CodexCatalogRuntime + ?Sized,
{
    let state = runtime.codex_catalog_runtime_state();
    let safe_error = safe_catalog_error(error);
    let existing_snapshot = read_lkg_snapshot(runtime, target, client_version).await;
    let status = CodexCatalogStatus {
        schema_version: CODEX_CATALOG_SCHEMA_VERSION,
        provider_id: target.identity.provider_id.clone(),
        key_id: target.identity.key_id.clone(),
        credential_scope: target.credential_scope().unwrap_or_default().to_string(),
        client_version: client_version.to_string(),
        last_checked_at_unix_secs: checked_at,
        last_success_at_unix_secs: existing_snapshot
            .as_ref()
            .map(|snapshot| snapshot.fetched_at_unix_secs),
        last_error: Some(safe_error.clone()),
        last_http_status: status_code,
        model_count: existing_snapshot
            .as_ref()
            .map(|snapshot| snapshot.models.len()),
        fetch_duration_ms: duration_ms(elapsed),
        used_lkg: existing_snapshot.is_some(),
    };
    write_catalog_status(state, target, client_version, &status).await;
    if let Err(marker_error) = state
        .kv_set(
            &catalog_retry_key(target, client_version),
            checked_at.to_string(),
            Some(CODEX_CATALOG_RETRY_TTL),
        )
        .await
    {
        debug!(
            event_name = "codex_catalog_retry_marker_write_error",
            provider_id = %target.identity.provider_id,
            key_id = %target.identity.key_id,
            client_version,
            error = %marker_error,
            "Codex catalog retry marker write failed"
        );
    }
    retain_catalog_version(state, target, client_version, existing_snapshot.is_some()).await;
    warn!(
        event_name = "codex_catalog_fetch_failure",
        provider_id = %target.identity.provider_id,
        key_id = %target.identity.key_id,
        client_version,
        status_code,
        duration_ms = duration_ms(elapsed),
        used_lkg = existing_snapshot.is_some(),
        error = %safe_error,
        "Codex catalog upstream fetch failed"
    );
}

async fn read_lkg_snapshot<R>(
    runtime: &R,
    target: &CodexCatalogTarget,
    client_version: &str,
) -> Option<CodexCatalogSnapshot>
where
    R: CodexCatalogRuntime + ?Sized,
{
    let raw = match runtime
        .codex_catalog_runtime_state()
        .kv_get(&catalog_lkg_key(target, client_version))
        .await
    {
        Ok(raw) => raw?,
        Err(error) => {
            warn!(
                event_name = "codex_catalog_lkg_read_error",
                provider_id = %target.identity.provider_id,
                key_id = %target.identity.key_id,
                client_version,
                error = %error,
                "Codex catalog LKG read failed"
            );
            return None;
        }
    };
    let snapshot = match serde_json::from_str::<CodexCatalogSnapshot>(&raw) {
        Ok(snapshot) => snapshot,
        Err(error) => {
            warn!(
                event_name = "codex_catalog_lkg_decode_error",
                provider_id = %target.identity.provider_id,
                key_id = %target.identity.key_id,
                client_version,
                error = %error,
                "Codex catalog LKG snapshot is invalid"
            );
            return None;
        }
    };
    let validated_models = validate_catalog_models(&snapshot.models);
    let content_hash_matches = validated_models
        .as_ref()
        .is_ok_and(|serialized| sha256_hex(serialized) == snapshot.content_sha256);
    if snapshot.schema_version != CODEX_CATALOG_SCHEMA_VERSION
        || snapshot.provider_id != target.identity.provider_id
        || snapshot.key_id != target.identity.key_id
        || snapshot.credential_scope != target.credential_scope().unwrap_or_default()
        || snapshot.client_version != client_version
        || !content_hash_matches
    {
        warn!(
            event_name = "codex_catalog_lkg_identity_mismatch",
            provider_id = %target.identity.provider_id,
            key_id = %target.identity.key_id,
            client_version,
            "Codex catalog LKG snapshot failed validation"
        );
        return None;
    }
    if !codex_catalog_credential_scope_is_current(runtime, target).await {
        warn!(
            event_name = "codex_catalog_lkg_credential_scope_changed",
            provider_id = %target.identity.provider_id,
            key_id = %target.identity.key_id,
            client_version,
            "Codex catalog LKG was rejected because the credential scope changed during the read"
        );
        return None;
    }
    Some(snapshot)
}

async fn write_catalog_status(
    state: &RuntimeState,
    target: &CodexCatalogTarget,
    client_version: &str,
    status: &CodexCatalogStatus,
) {
    let Ok(serialized) = serde_json::to_string(status) else {
        return;
    };
    if let Err(error) = state
        .kv_set(
            &catalog_status_key(target, client_version),
            serialized,
            None,
        )
        .await
    {
        debug!(
            event_name = "codex_catalog_status_write_error",
            provider_id = %target.identity.provider_id,
            key_id = %target.identity.key_id,
            client_version,
            error = %error,
            "Codex catalog status write failed"
        );
    }
}

async fn remember_seen_version(
    state: &RuntimeState,
    target: &CodexCatalogTarget,
    client_version: &str,
) {
    if let Err(error) = state
        .kv_set(
            &catalog_recent_version_key(&target.identity),
            catalog_version_member(target, client_version),
            None,
        )
        .await
    {
        debug!(
            event_name = "codex_catalog_recent_version_write_error",
            provider_id = %target.identity.provider_id,
            key_id = %target.identity.key_id,
            client_version,
            error = %error,
            "Codex catalog recent client version write failed"
        );
    }
}

fn touch_catalog_version_best_effort(
    state: &RuntimeState,
    target: &CodexCatalogTarget,
    client_version: &str,
) {
    let state = state.clone();
    let target = target.clone();
    let client_version = client_version.to_string();
    drop(tokio::spawn(async move {
        if let Err(error) = state
            .score_set(
                &catalog_versions_key(&target.identity),
                &catalog_version_member(&target, &client_version),
                current_unix_ms() as f64,
            )
            .await
        {
            debug!(
                event_name = "codex_catalog_version_touch_error",
                provider_id = %target.identity.provider_id,
                key_id = %target.identity.key_id,
                client_version,
                error = %error,
                "Codex catalog version best-effort touch failed"
            );
        }
    }));
}

pub(crate) async fn read_recent_codex_catalog_client_version(
    state: &RuntimeState,
    provider_id: &str,
    key_id: &str,
    credential_scope: &str,
) -> Option<String> {
    let identity = CodexCatalogIdentity::new(provider_id, key_id);
    let raw = state
        .kv_get(&catalog_recent_version_key(&identity))
        .await
        .ok()
        .flatten()?;
    let (stored_scope, stored_version) = parse_catalog_version_member(&raw)?;
    if stored_scope != credential_scope {
        return None;
    }
    let normalized = normalize_codex_client_version(Some(stored_version));
    (!normalized.used_fallback()).then_some(normalized.value)
}

/// Management is not tied to a downstream client's compatibility version. Keep its
/// directory at least as new as the built-in fingerprint and successful catalogs.
pub(crate) struct CodexManagementCatalog {
    pub(crate) client_version: String,
    pub(crate) models: Option<Vec<Value>>,
    target: CodexCatalogTarget,
}

pub(crate) async fn read_codex_management_catalog<R>(
    runtime: &R,
    provider_id: &str,
    key_id: &str,
) -> Option<CodexManagementCatalog>
where
    R: CodexCatalogRuntime + ?Sized,
{
    let target = bind_codex_catalog_target(
        runtime,
        &CodexCatalogTarget {
            identity: CodexCatalogIdentity::new(provider_id, key_id),
            endpoint_ids: Vec::new(),
            credential_scope: None,
        },
    )
    .await?;
    let scope = target.credential_scope()?;
    let state = runtime.codex_catalog_runtime_state();
    let mut version = Version::parse(&codex_client_version()).ok()?;
    if let Some(recent) =
        read_recent_codex_catalog_client_version(state, provider_id, key_id, scope).await
    {
        version = version.max(Version::parse(&recent).ok()?);
    }
    // The most recently seen client can be older than an already successful catalog.
    // Only consider the current credential generation's bounded success index.
    for member in state
        .score_range_by_min(&catalog_versions_key(&target.identity), 0.0)
        .await
        .unwrap_or_default()
    {
        if let Some((stored_scope, stored_version)) = parse_catalog_version_member(&member) {
            if stored_scope == scope {
                if let Ok(candidate) = Version::parse(stored_version) {
                    version = version.max(candidate);
                }
            }
        }
    }
    let client_version = version.to_string();
    let models = if let Some(snapshot) = read_lkg_snapshot(runtime, &target, &client_version).await
    {
        let fresh = state
            .kv_get(&catalog_fresh_key(&target, &client_version))
            .await
            .ok()
            .flatten();
        (fresh.as_deref() == Some(snapshot.content_sha256.as_str())).then_some(snapshot.models)
    } else {
        None
    };
    if !codex_catalog_credential_scope_is_current(runtime, &target).await {
        return None;
    }
    Some(CodexManagementCatalog {
        client_version,
        models,
        target,
    })
}

/// Publish a successful management fetch into the same versioned directory used by
/// clients. A credential replacement while the request is in flight must not leak
/// the previous account's catalog into the new account.
pub(crate) async fn store_codex_management_catalog<R>(
    runtime: &R,
    catalog: &CodexManagementCatalog,
    transports: &[GatewayProviderTransportSnapshot],
    models: Vec<Value>,
    etag: Option<&str>,
) where
    R: CodexCatalogRuntime + ?Sized,
{
    let target = &catalog.target;
    if transports.is_empty()
        || transports.iter().any(|transport| {
            codex_catalog_credential_scope_from_transport(transport).as_deref()
                != target.credential_scope()
        })
        || !codex_catalog_credential_scope_is_current(runtime, target).await
    {
        return;
    }
    let Ok(serialized) = validate_catalog_models(&models) else {
        return;
    };
    let now = current_unix_secs();
    let snapshot = CodexCatalogSnapshot {
        schema_version: CODEX_CATALOG_SCHEMA_VERSION,
        provider_id: target.identity.provider_id.clone(),
        key_id: target.identity.key_id.clone(),
        credential_scope: target.credential_scope().unwrap_or_default().to_string(),
        client_version: catalog.client_version.clone(),
        models,
        etag: etag.and_then(normalize_etag),
        fetched_at_unix_secs: now,
        last_checked_at_unix_secs: now,
        content_sha256: sha256_hex(&serialized),
    };
    persist_catalog_success(
        runtime,
        target,
        &catalog.client_version,
        &snapshot,
        Some(200),
        Duration::ZERO,
    )
    .await;
    if !codex_catalog_credential_scope_is_current(runtime, target).await {
        discard_catalog_success_after_scope_change(
            runtime.codex_catalog_runtime_state(),
            target,
            &catalog.client_version,
        )
        .await;
    }
}

async fn retain_catalog_version(
    state: &RuntimeState,
    target: &CodexCatalogTarget,
    client_version: &str,
    has_lkg_hint: bool,
) {
    let identity = &target.identity;
    let lock_key = catalog_retention_lock_key(identity);
    let owner = format!("codex-catalog-retention:{}", std::process::id());
    let started = Instant::now();
    let lease = loop {
        match state
            .lock_try_acquire(&lock_key, &owner, CODEX_CATALOG_RETENTION_LOCK_TTL)
            .await
        {
            Ok(Some(lease)) => break Some(lease),
            Ok(None) if started.elapsed() < CODEX_CATALOG_RETENTION_LOCK_WAIT => {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            Ok(None) => break None,
            Err(error) => {
                debug!(
                    event_name = "codex_catalog_retention_lock_error",
                    provider_id = %identity.provider_id,
                    key_id = %identity.key_id,
                    client_version,
                    error = %error,
                    "Codex catalog retention lock backend was unavailable"
                );
                let _local_guard = CODEX_CATALOG_LOCAL_RETENTION.lock().await;
                retain_catalog_version_locked(state, target, client_version, has_lkg_hint).await;
                return;
            }
        }
    };

    let Some(lease) = lease else {
        let index_key = if has_lkg_hint {
            catalog_versions_key(identity)
        } else {
            catalog_failed_versions_key(identity)
        };
        let _ = state
            .score_set(
                &index_key,
                &catalog_version_member(target, client_version),
                current_unix_ms() as f64,
            )
            .await;
        warn!(
            event_name = "codex_catalog_retention_lock_timeout",
            provider_id = %identity.provider_id,
            key_id = %identity.key_id,
            client_version,
            "Codex catalog retention update was indexed without destructive trimming"
        );
        return;
    };

    retain_catalog_version_locked(state, target, client_version, has_lkg_hint).await;
    match state.lock_release(&lease).await {
        Ok(true) => {}
        Ok(false) => debug!(
            event_name = "codex_catalog_retention_lease_lost",
            provider_id = %identity.provider_id,
            key_id = %identity.key_id,
            client_version,
            "Codex catalog retention lease was no longer owned at release"
        ),
        Err(error) => debug!(
            event_name = "codex_catalog_retention_release_error",
            provider_id = %identity.provider_id,
            key_id = %identity.key_id,
            client_version,
            error = %error,
            "Codex catalog retention lease release failed"
        ),
    }
}

async fn retain_catalog_version_locked(
    state: &RuntimeState,
    target: &CodexCatalogTarget,
    client_version: &str,
    has_lkg_hint: bool,
) {
    let identity = &target.identity;
    let member = catalog_version_member(target, client_version);
    let has_lkg = has_lkg_hint
        || state
            .kv_get(&catalog_lkg_key(target, client_version))
            .await
            .ok()
            .flatten()
            .is_some();
    let index_key = if has_lkg {
        catalog_versions_key(identity)
    } else {
        catalog_failed_versions_key(identity)
    };
    if let Err(error) = state
        .score_set(&index_key, &member, current_unix_ms() as f64)
        .await
    {
        debug!(
            event_name = "codex_catalog_version_touch_error",
            provider_id = %identity.provider_id,
            key_id = %identity.key_id,
            client_version,
            has_lkg,
            error = %error,
            "Codex catalog version index update failed"
        );
        return;
    }

    if has_lkg {
        let _ = state
            .score_remove(&catalog_failed_versions_key(identity), &member)
            .await;
        trim_catalog_versions_locked(state, identity).await;
    } else {
        trim_failed_catalog_versions_locked(state, identity).await;
    }
}

async fn trim_catalog_versions_locked(state: &RuntimeState, identity: &CodexCatalogIdentity) {
    let index_key = catalog_versions_key(identity);
    let Ok(versions) = state.score_range_by_min(&index_key, 0.0).await else {
        return;
    };
    let trim_count = versions
        .len()
        .saturating_sub(CODEX_CATALOG_MAX_VERSIONS_PER_KEY);
    for member in versions.into_iter().take(trim_count) {
        let Some((credential_scope, version)) = parse_catalog_version_member(&member) else {
            let _ = state.score_remove(&index_key, &member).await;
            continue;
        };
        let keys = vec![
            catalog_versioned_key("lkg", identity, credential_scope, version),
            catalog_versioned_key("fresh", identity, credential_scope, version),
            catalog_versioned_key("status", identity, credential_scope, version),
            catalog_versioned_key("retry", identity, credential_scope, version),
        ];
        let _ = state.kv_delete_many(&keys).await;
        let _ = state.score_remove(&index_key, &member).await;
    }
}

async fn trim_failed_catalog_versions_locked(
    state: &RuntimeState,
    identity: &CodexCatalogIdentity,
) {
    let index_key = catalog_failed_versions_key(identity);
    let Ok(versions) = state.score_range_by_min(&index_key, 0.0).await else {
        return;
    };
    let trim_count = versions
        .len()
        .saturating_sub(CODEX_CATALOG_MAX_FAILED_VERSIONS_PER_KEY);
    for member in versions.into_iter().take(trim_count) {
        // The retry marker has its own short TTL. Keep it until natural expiry so rotating more
        // than the retained failure-status count cannot bypass the per-version cooldown.
        if let Some((credential_scope, version)) = parse_catalog_version_member(&member) {
            let _ = state
                .kv_delete(&catalog_versioned_key(
                    "status",
                    identity,
                    credential_scope,
                    version,
                ))
                .await;
        }
        let _ = state.score_remove(&index_key, &member).await;
    }
}

fn normalize_etag(value: &str) -> Option<String> {
    let value = value.trim();
    let opaque = value.strip_prefix("W/").unwrap_or(value);
    let valid_syntax = opaque.len() >= 2
        && opaque.starts_with('"')
        && opaque.ends_with('"')
        && opaque[1..opaque.len() - 1]
            .bytes()
            .all(|byte| byte == 0x21 || (0x23..=0x7e).contains(&byte) || byte >= 0x80);
    (!value.is_empty() && value.len() <= CODEX_CATALOG_MAX_ETAG_LEN && valid_syntax)
        .then(|| value.to_string())
}

fn safe_catalog_error(error: &str) -> String {
    let lower = error.to_ascii_lowercase();
    if lower.contains("401") || lower.contains("unauthorized") {
        "upstream catalog authentication failed (status 401)".to_string()
    } else if lower.contains("403") || lower.contains("forbidden") {
        "upstream catalog authorization failed (status 403)".to_string()
    } else if lower.contains("429") {
        "upstream catalog rate limited (status 429)".to_string()
    } else if lower.contains("timeout") || lower.contains("timed out") {
        "upstream catalog fetch timed out".to_string()
    } else if lower.contains("no models") || lower.contains("missing") || lower.contains("invalid")
    {
        "upstream catalog response was invalid".to_string()
    } else {
        "upstream catalog fetch failed".to_string()
    }
}

fn catalog_lkg_key(target: &CodexCatalogTarget, client_version: &str) -> String {
    catalog_versioned_key(
        "lkg",
        &target.identity,
        target.credential_scope().unwrap_or_default(),
        client_version,
    )
}

fn catalog_fresh_key(target: &CodexCatalogTarget, client_version: &str) -> String {
    catalog_versioned_key(
        "fresh",
        &target.identity,
        target.credential_scope().unwrap_or_default(),
        client_version,
    )
}

fn catalog_status_key(target: &CodexCatalogTarget, client_version: &str) -> String {
    catalog_versioned_key(
        "status",
        &target.identity,
        target.credential_scope().unwrap_or_default(),
        client_version,
    )
}

fn catalog_retry_key(target: &CodexCatalogTarget, client_version: &str) -> String {
    catalog_versioned_key(
        "retry",
        &target.identity,
        target.credential_scope().unwrap_or_default(),
        client_version,
    )
}

fn catalog_flight_key(target: &CodexCatalogTarget, client_version: &str) -> String {
    catalog_versioned_key(
        "flight",
        &target.identity,
        target.credential_scope().unwrap_or_default(),
        client_version,
    )
}

fn catalog_fetch_pace_key(target: &CodexCatalogTarget) -> String {
    format!(
        "codex_catalog:fetch_pace:v2:{}",
        catalog_scoped_identity_digest(target)
    )
}

fn catalog_versioned_key(
    namespace: &str,
    identity: &CodexCatalogIdentity,
    credential_scope: &str,
    client_version: &str,
) -> String {
    let digest = sha256_hex(
        format!(
            "{}\0{}\0{}\0{}",
            identity.provider_id, identity.key_id, credential_scope, client_version
        )
        .as_bytes(),
    );
    format!("codex_catalog:{namespace}:v2:{digest}")
}

fn catalog_versions_key(identity: &CodexCatalogIdentity) -> String {
    format!(
        "codex_catalog:versions:v2:{}",
        catalog_identity_digest(identity)
    )
}

fn catalog_failed_versions_key(identity: &CodexCatalogIdentity) -> String {
    format!(
        "codex_catalog:failed_versions:v2:{}",
        catalog_identity_digest(identity)
    )
}

fn catalog_retention_lock_key(identity: &CodexCatalogIdentity) -> String {
    format!(
        "codex_catalog:retention_lock:v2:{}",
        catalog_identity_digest(identity)
    )
}

fn catalog_recent_version_key(identity: &CodexCatalogIdentity) -> String {
    format!(
        "codex_catalog:recent:v2:{}",
        catalog_identity_digest(identity)
    )
}

fn catalog_scoped_identity_digest(target: &CodexCatalogTarget) -> String {
    sha256_hex(
        format!(
            "{}\0{}\0{}",
            target.identity.provider_id,
            target.identity.key_id,
            target.credential_scope().unwrap_or_default()
        )
        .as_bytes(),
    )
}

fn catalog_version_member(target: &CodexCatalogTarget, client_version: &str) -> String {
    format!(
        "{}:{client_version}",
        target.credential_scope().unwrap_or_default()
    )
}

fn parse_catalog_version_member(member: &str) -> Option<(&str, &str)> {
    let (credential_scope, client_version) = member.split_once(':')?;
    (credential_scope.len() == 64
        && credential_scope
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit())
        && !client_version.is_empty())
    .then_some((credential_scope, client_version))
}

fn catalog_identity_digest(identity: &CodexCatalogIdentity) -> String {
    sha256_hex(format!("{}\0{}", identity.provider_id, identity.key_id).as_bytes())
}

fn sha256_hex(value: &[u8]) -> String {
    let digest = Sha256::digest(value);
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn current_unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn current_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

fn duration_ms(duration: Duration) -> u64 {
    duration.as_millis().try_into().unwrap_or(u64::MAX)
}

#[cfg(test)]
#[path = "catalog/tests.rs"]
mod tests;
