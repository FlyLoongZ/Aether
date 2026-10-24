use std::collections::BTreeMap;
use std::sync::RwLock;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;

use super::{
    AuthApiKeyExportSummary, AuthApiKeyLookupKey, AuthApiKeyReadRepository,
    AuthApiKeyWriteRepository, CompareAndSwapAuthApiKeyCiphertext, CreateStandaloneApiKeyRecord,
    CreateUserApiKeyRecord, StandaloneApiKeyExportListQuery, StoredAuthApiKeyExportRecord,
    StoredAuthApiKeySnapshot, UpdateStandaloneApiKeyBasicRecord, UpdateUserApiKeyBasicRecord,
};
use crate::repository::usage::{ApiKeyUsageContribution, ApiKeyUsageDelta};
use crate::repository::users::StoredUserAuthRecord;
use crate::DataLayerError;

fn current_unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
}

#[derive(Debug, Default)]
struct MemoryAuthApiKeyIndex {
    by_api_key_id: BTreeMap<String, StoredAuthApiKeySnapshot>,
    export_by_api_key_id: BTreeMap<String, StoredAuthApiKeyExportRecord>,
    by_key_hash: BTreeMap<String, String>,
    owner_by_user_id: BTreeMap<String, MemoryAuthApiKeyOwnerRegistryEntry>,
    touch_counts: BTreeMap<String, usize>,
    snapshot_lookup_counts: BTreeMap<String, usize>,
    key_hash_lookup_counts: BTreeMap<String, usize>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct MemoryAuthApiKeyOwnerSnapshot {
    user_id: String,
    username: String,
    email: Option<String>,
    user_role: String,
    user_auth_source: String,
    user_is_active: bool,
    user_is_deleted: bool,
    user_rate_limit: Option<i32>,
    user_allowed_providers: Option<Vec<String>>,
    user_allowed_api_formats: Option<Vec<String>>,
    user_allowed_models: Option<Vec<String>>,
}

impl From<&StoredAuthApiKeySnapshot> for MemoryAuthApiKeyOwnerSnapshot {
    fn from(snapshot: &StoredAuthApiKeySnapshot) -> Self {
        Self {
            user_id: snapshot.user_id.clone(),
            username: snapshot.username.clone(),
            email: snapshot.email.clone(),
            user_role: snapshot.user_role.clone(),
            user_auth_source: snapshot.user_auth_source.clone(),
            user_is_active: snapshot.user_is_active,
            user_is_deleted: snapshot.user_is_deleted,
            user_rate_limit: snapshot.user_rate_limit,
            user_allowed_providers: snapshot.user_allowed_providers.clone(),
            user_allowed_api_formats: snapshot.user_allowed_api_formats.clone(),
            user_allowed_models: snapshot.user_allowed_models.clone(),
        }
    }
}

impl From<&StoredUserAuthRecord> for MemoryAuthApiKeyOwnerSnapshot {
    fn from(user: &StoredUserAuthRecord) -> Self {
        Self {
            user_id: user.id.clone(),
            username: user.username.clone(),
            email: user.email.clone(),
            user_role: user.role.clone(),
            user_auth_source: user.auth_source.clone(),
            user_is_active: user.is_active,
            user_is_deleted: user.is_deleted,
            user_rate_limit: None,
            user_allowed_providers: user.allowed_providers.clone(),
            user_allowed_api_formats: user.allowed_api_formats.clone(),
            user_allowed_models: user.allowed_models.clone(),
        }
    }
}

// The trusted snapshot is returned on the common path so callers can use the
// complete immutable view without another lookup. Preserve this established
// in-memory registry representation rather than introducing heap allocation.
#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
enum MemoryAuthApiKeyOwnerRegistryEntry {
    Trusted(MemoryAuthApiKeyOwnerSnapshot),
    Conflicted,
}

#[derive(Debug, Default)]
pub struct InMemoryAuthApiKeySnapshotRepository {
    index: RwLock<MemoryAuthApiKeyIndex>,
    lookup_delay: Option<Duration>,
}

impl InMemoryAuthApiKeySnapshotRepository {
    pub fn seed<I>(items: I) -> Self
    where
        I: IntoIterator<Item = (Option<String>, StoredAuthApiKeySnapshot)>,
    {
        let mut by_api_key_id = BTreeMap::new();
        let mut export_by_api_key_id = BTreeMap::new();
        let mut by_key_hash = BTreeMap::new();
        let mut owner_by_user_id = BTreeMap::new();
        for (key_hash, snapshot) in items {
            Self::register_owner_snapshot(
                &mut owner_by_user_id,
                MemoryAuthApiKeyOwnerSnapshot::from(&snapshot),
            );
            let derived_key_hash = key_hash
                .clone()
                .unwrap_or_else(|| format!("memory-{}", snapshot.api_key_id));
            export_by_api_key_id.insert(
                snapshot.api_key_id.clone(),
                StoredAuthApiKeyExportRecord::new(
                    snapshot.user_id.clone(),
                    snapshot.api_key_id.clone(),
                    derived_key_hash.clone(),
                    None,
                    snapshot.api_key_name.clone(),
                    snapshot
                        .api_key_allowed_providers
                        .as_ref()
                        .map(|value| serde_json::json!(value)),
                    snapshot
                        .api_key_allowed_api_formats
                        .as_ref()
                        .map(|value| serde_json::json!(value)),
                    snapshot
                        .api_key_allowed_models
                        .as_ref()
                        .map(|value| serde_json::json!(value)),
                    snapshot.api_key_rate_limit,
                    snapshot.api_key_concurrent_limit,
                    None,
                    snapshot.api_key_is_active,
                    snapshot
                        .api_key_expires_at_unix_secs
                        .map(|value| value as i64),
                    false,
                    0,
                    0,
                    0.0,
                    snapshot.api_key_is_standalone,
                )
                .and_then(|record| {
                    record.with_ip_rules(
                        snapshot
                            .api_key_ip_rules
                            .as_ref()
                            .map(|value| serde_json::json!(value)),
                    )
                })
                .expect("derived auth api key export record should build"),
            );
            if let Some(key_hash) = key_hash {
                by_key_hash.insert(key_hash, snapshot.api_key_id.clone());
            }
            by_api_key_id.insert(snapshot.api_key_id.clone(), snapshot);
        }
        Self {
            index: RwLock::new(MemoryAuthApiKeyIndex {
                by_api_key_id,
                export_by_api_key_id,
                by_key_hash,
                owner_by_user_id,
                touch_counts: BTreeMap::new(),
                snapshot_lookup_counts: BTreeMap::new(),
                key_hash_lookup_counts: BTreeMap::new(),
            }),
            lookup_delay: None,
        }
    }

    /// Registers trusted owner state without inserting an API key.  Only the
    /// owner fields are retained; the API-key fields of each fixture snapshot
    /// are intentionally ignored.
    pub fn with_owner_snapshots<I>(mut self, items: I) -> Self
    where
        I: IntoIterator<Item = StoredAuthApiKeySnapshot>,
    {
        let index = self
            .index
            .get_mut()
            .expect("auth api key snapshot repository lock");
        for snapshot in items {
            Self::register_owner_snapshot(
                &mut index.owner_by_user_id,
                MemoryAuthApiKeyOwnerSnapshot::from(&snapshot),
            );
        }
        self
    }

    pub fn with_lookup_delay_for_tests(mut self, delay: Duration) -> Self {
        self.lookup_delay = Some(delay);
        self
    }

    pub fn with_export_records<I>(mut self, items: I) -> Self
    where
        I: IntoIterator<Item = StoredAuthApiKeyExportRecord>,
    {
        let index = self
            .index
            .get_mut()
            .expect("auth api key snapshot repository lock");
        for item in items {
            index
                .export_by_api_key_id
                .insert(item.api_key_id.clone(), item);
        }
        self
    }

    pub fn touch_count(&self, api_key_id: &str) -> usize {
        self.index
            .read()
            .expect("auth api key snapshot repository lock")
            .touch_counts
            .get(api_key_id)
            .copied()
            .unwrap_or(0)
    }

    pub fn standalone_flags(&self) -> BTreeMap<String, bool> {
        let index = self
            .index
            .read()
            .expect("auth api key snapshot repository lock");
        index
            .export_by_api_key_id
            .iter()
            .map(|(id, record)| (id.clone(), record.is_standalone))
            .chain(
                index
                    .by_api_key_id
                    .iter()
                    .map(|(id, snapshot)| (id.clone(), snapshot.api_key_is_standalone)),
            )
            .collect()
    }

    pub fn snapshot_lookup_count(&self, api_key_id: &str) -> usize {
        self.index
            .read()
            .expect("auth api key snapshot repository lock")
            .snapshot_lookup_counts
            .get(api_key_id)
            .copied()
            .unwrap_or(0)
    }

    pub fn key_hash_lookup_count(&self, key_hash: &str) -> usize {
        self.index
            .read()
            .expect("auth api key snapshot repository lock")
            .key_hash_lookup_counts
            .get(key_hash)
            .copied()
            .unwrap_or(0)
    }

    pub(crate) fn apply_usage_stats_delta(
        &self,
        api_key_id: &str,
        delta: &ApiKeyUsageDelta,
        _recomputed_last_used_at_unix_secs: Option<u64>,
    ) {
        let mut index = self
            .index
            .write()
            .expect("auth api key snapshot repository lock");
        let Some(record) = index.export_by_api_key_id.get_mut(api_key_id) else {
            return;
        };

        record.total_requests = apply_i64_delta_to_u64(record.total_requests, delta.total_requests);
        record.total_tokens = apply_i64_delta_to_u64(record.total_tokens, delta.total_tokens);
        record.total_cost_usd = apply_f64_delta(record.total_cost_usd, delta.total_cost_usd);
    }

    pub(crate) fn rebuild_usage_stats(
        &self,
        contributions: &BTreeMap<String, ApiKeyUsageContribution>,
    ) {
        let mut index = self
            .index
            .write()
            .expect("auth api key snapshot repository lock");
        for record in index.export_by_api_key_id.values_mut() {
            record.total_requests = 0;
            record.total_tokens = 0;
            record.total_cost_usd = 0.0;
        }

        for (api_key_id, contribution) in contributions {
            let Some(record) = index.export_by_api_key_id.get_mut(api_key_id) else {
                continue;
            };
            record.total_requests = clamp_i64_to_u64(contribution.total_requests);
            record.total_tokens = clamp_i64_to_u64(contribution.total_tokens);
            record.total_cost_usd = contribution.total_cost_usd.max(0.0);
        }
    }

    fn remove_api_key(index: &mut MemoryAuthApiKeyIndex, api_key_id: &str) {
        let key_hashes = index
            .by_key_hash
            .iter()
            .filter(|(_, mapped_api_key_id)| mapped_api_key_id.as_str() == api_key_id)
            .map(|(key_hash, _)| key_hash.clone())
            .collect::<Vec<_>>();
        index.by_api_key_id.remove(api_key_id);
        index.export_by_api_key_id.remove(api_key_id);
        index.by_key_hash.retain(|_, value| value != api_key_id);
        index.touch_counts.remove(api_key_id);
        index.snapshot_lookup_counts.remove(api_key_id);
        for key_hash in key_hashes {
            index.key_hash_lookup_counts.remove(&key_hash);
        }
    }

    fn register_owner_snapshot(
        owners: &mut BTreeMap<String, MemoryAuthApiKeyOwnerRegistryEntry>,
        owner: MemoryAuthApiKeyOwnerSnapshot,
    ) {
        use std::collections::btree_map::Entry;

        match owners.entry(owner.user_id.clone()) {
            Entry::Vacant(entry) => {
                entry.insert(MemoryAuthApiKeyOwnerRegistryEntry::Trusted(owner));
            }
            Entry::Occupied(mut entry) => {
                let matches_existing = matches!(
                    entry.get(),
                    MemoryAuthApiKeyOwnerRegistryEntry::Trusted(existing) if existing == &owner
                );
                if !matches_existing {
                    entry.insert(MemoryAuthApiKeyOwnerRegistryEntry::Conflicted);
                }
            }
        }
    }
}

fn clamp_i64_to_u64(value: i64) -> u64 {
    u64::try_from(value).unwrap_or_default()
}

fn i64_from_u64(value: u64, field_name: &str) -> Result<i64, DataLayerError> {
    i64::try_from(value)
        .map_err(|_| DataLayerError::InvalidInput(format!("{field_name} exceeds i64: {value}")))
}

fn apply_i64_delta_to_u64(current: u64, delta: i64) -> u64 {
    clamp_i64_to_u64(
        i64::try_from(current)
            .unwrap_or(i64::MAX)
            .saturating_add(delta),
    )
}

fn apply_f64_delta(current: f64, delta: f64) -> f64 {
    let next = current + delta;
    if next.is_finite() {
        next.max(0.0)
    } else {
        current.max(0.0)
    }
}

#[async_trait]
impl AuthApiKeyReadRepository for InMemoryAuthApiKeySnapshotRepository {
    async fn find_api_key_snapshot(
        &self,
        key: AuthApiKeyLookupKey<'_>,
    ) -> Result<Option<StoredAuthApiKeySnapshot>, DataLayerError> {
        if let Some(delay) = self.lookup_delay {
            tokio::time::sleep(delay).await;
        }
        let mut index = self
            .index
            .write()
            .expect("auth api key snapshot repository lock");
        Ok(match key {
            AuthApiKeyLookupKey::KeyHash(key_hash) => {
                *index
                    .key_hash_lookup_counts
                    .entry(key_hash.to_string())
                    .or_insert(0) += 1;
                index
                    .by_key_hash
                    .get(key_hash)
                    .and_then(|api_key_id| index.by_api_key_id.get(api_key_id))
                    .cloned()
            }
            AuthApiKeyLookupKey::ApiKeyId(api_key_id) => {
                *index
                    .snapshot_lookup_counts
                    .entry(api_key_id.to_string())
                    .or_insert(0) += 1;
                index.by_api_key_id.get(api_key_id).cloned()
            }
            AuthApiKeyLookupKey::UserApiKeyIds {
                user_id,
                api_key_id,
            } => {
                *index
                    .snapshot_lookup_counts
                    .entry(api_key_id.to_string())
                    .or_insert(0) += 1;
                index
                    .by_api_key_id
                    .get(api_key_id)
                    .filter(|snapshot| snapshot.user_id == user_id)
                    .cloned()
            }
        })
    }

    async fn list_api_key_snapshots_by_ids(
        &self,
        api_key_ids: &[String],
    ) -> Result<Vec<StoredAuthApiKeySnapshot>, DataLayerError> {
        let index = self
            .index
            .read()
            .expect("auth api key snapshot repository lock");
        Ok(api_key_ids
            .iter()
            .filter_map(|api_key_id| index.by_api_key_id.get(api_key_id).cloned())
            .collect())
    }

    async fn list_export_api_keys_by_user_ids(
        &self,
        user_ids: &[String],
    ) -> Result<Vec<StoredAuthApiKeyExportRecord>, DataLayerError> {
        let index = self
            .index
            .read()
            .expect("auth api key snapshot repository lock");
        Ok(index
            .export_by_api_key_id
            .values()
            .filter(|record| {
                !record.is_standalone && user_ids.iter().any(|id| id == &record.user_id)
            })
            .cloned()
            .collect())
    }

    async fn list_export_api_keys_by_ids(
        &self,
        api_key_ids: &[String],
    ) -> Result<Vec<StoredAuthApiKeyExportRecord>, DataLayerError> {
        let index = self
            .index
            .read()
            .expect("auth api key snapshot repository lock");
        Ok(api_key_ids
            .iter()
            .filter_map(|api_key_id| index.export_by_api_key_id.get(api_key_id).cloned())
            .collect())
    }

    async fn list_export_api_keys_by_name_search(
        &self,
        name_search: &str,
    ) -> Result<Vec<StoredAuthApiKeyExportRecord>, DataLayerError> {
        let name_search = name_search.trim().to_ascii_lowercase();
        if name_search.is_empty() {
            return Ok(Vec::new());
        }

        let index = self
            .index
            .read()
            .expect("auth api key snapshot repository lock");
        Ok(index
            .export_by_api_key_id
            .values()
            .filter(|record| {
                record
                    .name
                    .as_deref()
                    .unwrap_or_default()
                    .to_ascii_lowercase()
                    .contains(&name_search)
            })
            .cloned()
            .collect())
    }

    async fn list_export_standalone_api_keys_page(
        &self,
        query: &StandaloneApiKeyExportListQuery,
    ) -> Result<Vec<StoredAuthApiKeyExportRecord>, DataLayerError> {
        let index = self
            .index
            .read()
            .expect("auth api key snapshot repository lock");
        Ok(index
            .export_by_api_key_id
            .values()
            .filter(|record| {
                record.is_standalone
                    && query
                        .is_active
                        .is_none_or(|is_active| record.is_active == is_active)
            })
            .skip(query.skip)
            .take(query.limit)
            .cloned()
            .collect())
    }

    async fn count_export_standalone_api_keys(
        &self,
        is_active: Option<bool>,
    ) -> Result<u64, DataLayerError> {
        let index = self
            .index
            .read()
            .expect("auth api key snapshot repository lock");
        Ok(index
            .export_by_api_key_id
            .values()
            .filter(|record| {
                record.is_standalone
                    && is_active.is_none_or(|expected| record.is_active == expected)
            })
            .count() as u64)
    }

    async fn summarize_export_api_keys_by_user_ids(
        &self,
        user_ids: &[String],
        now_unix_secs: u64,
    ) -> Result<AuthApiKeyExportSummary, DataLayerError> {
        i64_from_u64(now_unix_secs, "api_keys.summary_now")?;
        let index = self
            .index
            .read()
            .expect("auth api key snapshot repository lock");
        let mut summary = AuthApiKeyExportSummary::default();
        for record in index.export_by_api_key_id.values().filter(|record| {
            !record.is_standalone && user_ids.iter().any(|id| id == &record.user_id)
        }) {
            summary.total = summary.total.saturating_add(1);
            if record.is_active
                && record
                    .expires_at_unix_secs
                    .is_none_or(|expires_at_unix_secs| expires_at_unix_secs >= now_unix_secs)
            {
                summary.active = summary.active.saturating_add(1);
            }
        }
        Ok(summary)
    }

    async fn summarize_export_non_standalone_api_keys(
        &self,
        now_unix_secs: u64,
    ) -> Result<AuthApiKeyExportSummary, DataLayerError> {
        i64_from_u64(now_unix_secs, "api_keys.summary_now")?;
        let index = self
            .index
            .read()
            .expect("auth api key snapshot repository lock");
        let mut summary = AuthApiKeyExportSummary::default();
        for record in index
            .export_by_api_key_id
            .values()
            .filter(|record| !record.is_standalone)
        {
            summary.total = summary.total.saturating_add(1);
            if record.is_active
                && record
                    .expires_at_unix_secs
                    .is_none_or(|expires_at_unix_secs| expires_at_unix_secs >= now_unix_secs)
            {
                summary.active = summary.active.saturating_add(1);
            }
        }
        Ok(summary)
    }

    async fn find_export_standalone_api_key_by_id(
        &self,
        api_key_id: &str,
    ) -> Result<Option<StoredAuthApiKeyExportRecord>, DataLayerError> {
        let index = self
            .index
            .read()
            .expect("auth api key snapshot repository lock");
        Ok(index
            .export_by_api_key_id
            .get(api_key_id)
            .filter(|record| record.is_standalone)
            .cloned())
    }

    async fn summarize_export_standalone_api_keys(
        &self,
        now_unix_secs: u64,
    ) -> Result<AuthApiKeyExportSummary, DataLayerError> {
        i64_from_u64(now_unix_secs, "api_keys.summary_now")?;
        let index = self
            .index
            .read()
            .expect("auth api key snapshot repository lock");
        let mut summary = AuthApiKeyExportSummary::default();
        for record in index
            .export_by_api_key_id
            .values()
            .filter(|record| record.is_standalone)
        {
            summary.total = summary.total.saturating_add(1);
            if record.is_active
                && record
                    .expires_at_unix_secs
                    .is_none_or(|expires_at_unix_secs| expires_at_unix_secs >= now_unix_secs)
            {
                summary.active = summary.active.saturating_add(1);
            }
        }
        Ok(summary)
    }

    async fn list_export_standalone_api_keys(
        &self,
    ) -> Result<Vec<StoredAuthApiKeyExportRecord>, DataLayerError> {
        let index = self
            .index
            .read()
            .expect("auth api key snapshot repository lock");
        Ok(index
            .export_by_api_key_id
            .values()
            .filter(|record| record.is_standalone)
            .cloned()
            .collect())
    }
}

#[async_trait]
impl AuthApiKeyWriteRepository for InMemoryAuthApiKeySnapshotRepository {
    async fn touch_last_used_at(&self, api_key_id: &str) -> Result<bool, DataLayerError> {
        let mut index = self
            .index
            .write()
            .expect("auth api key snapshot repository lock");
        if !index.by_api_key_id.contains_key(api_key_id) {
            return Ok(false);
        }
        let counter = index
            .touch_counts
            .entry(api_key_id.to_string())
            .or_insert(0);
        *counter += 1;
        Ok(true)
    }

    async fn synchronize_user_api_key_owner_for_tests(
        &self,
        user: &StoredUserAuthRecord,
    ) -> Result<(), DataLayerError> {
        if user.id.trim().is_empty()
            || user.username.trim().is_empty()
            || user.role.trim().is_empty()
            || user.auth_source.trim().is_empty()
            || user.security_version < 0
        {
            return Err(DataLayerError::InvalidInput(
                "invalid authoritative API-key owner snapshot".to_string(),
            ));
        }

        let owner = MemoryAuthApiKeyOwnerSnapshot::from(user);
        self.index
            .write()
            .expect("auth api key snapshot repository lock")
            .owner_by_user_id
            .insert(
                owner.user_id.clone(),
                MemoryAuthApiKeyOwnerRegistryEntry::Trusted(owner),
            );
        Ok(())
    }

    async fn create_user_api_key(
        &self,
        record: CreateUserApiKeyRecord,
    ) -> Result<Option<StoredAuthApiKeyExportRecord>, DataLayerError> {
        let mut index = self
            .index
            .write()
            .expect("auth api key snapshot repository lock");
        // Match the database adapters: inactive owners may retain or receive keys for
        // administrative restore workflows, but unknown/deleted owners must fail closed.  The
        // resulting snapshot remains unusable while its trusted owner is inactive.
        let owner = match index.owner_by_user_id.get(&record.user_id) {
            Some(MemoryAuthApiKeyOwnerRegistryEntry::Trusted(owner)) if !owner.user_is_deleted => {
                owner.clone()
            }
            Some(
                MemoryAuthApiKeyOwnerRegistryEntry::Trusted(_)
                | MemoryAuthApiKeyOwnerRegistryEntry::Conflicted,
            )
            | None => return Ok(None),
        };
        if index.by_api_key_id.contains_key(&record.api_key_id) {
            return Err(DataLayerError::UnexpectedValue(format!(
                "duplicate api_keys.id: {}",
                record.api_key_id
            )));
        }
        if index.by_key_hash.contains_key(&record.key_hash) {
            return Err(DataLayerError::UnexpectedValue(format!(
                "duplicate api_keys.key_hash: {}",
                record.key_hash
            )));
        }
        let snapshot = StoredAuthApiKeySnapshot {
            user_id: owner.user_id,
            username: owner.username,
            email: owner.email,
            user_role: owner.user_role,
            user_auth_source: owner.user_auth_source,
            user_is_active: owner.user_is_active,
            user_is_deleted: owner.user_is_deleted,
            user_rate_limit: owner.user_rate_limit,
            user_allowed_providers: owner.user_allowed_providers,
            user_allowed_api_formats: owner.user_allowed_api_formats,
            user_allowed_models: owner.user_allowed_models,
            api_key_id: record.api_key_id.clone(),
            api_key_name: record.name.clone(),
            api_key_is_active: record.is_active,
            api_key_is_locked: false,
            api_key_is_standalone: false,
            api_key_rate_limit: Some(record.rate_limit),
            api_key_concurrent_limit: record.concurrent_limit,
            api_key_expires_at_unix_secs: record.expires_at_unix_secs,
            api_key_allowed_providers: record.allowed_providers.clone(),
            api_key_allowed_api_formats: record.allowed_api_formats.clone(),
            api_key_allowed_models: record.allowed_models.clone(),
            api_key_ip_rules: record.ip_rules.clone(),
        };

        let now_unix_secs = current_unix_secs() as i64;
        let export = StoredAuthApiKeyExportRecord::new(
            record.user_id.clone(),
            record.api_key_id.clone(),
            record.key_hash.clone(),
            record.key_encrypted,
            record.name,
            record
                .allowed_providers
                .as_ref()
                .map(|value| serde_json::json!(value)),
            record
                .allowed_api_formats
                .as_ref()
                .map(|value| serde_json::json!(value)),
            record
                .allowed_models
                .as_ref()
                .map(|value| serde_json::json!(value)),
            Some(record.rate_limit),
            record.concurrent_limit,
            record.force_capabilities,
            record.is_active,
            record
                .expires_at_unix_secs
                .map(|value| i64_from_u64(value, "api_keys.expires_at"))
                .transpose()?,
            record.auto_delete_on_expiry,
            i64_from_u64(record.total_requests, "api_keys.total_requests")?,
            i64_from_u64(record.total_tokens, "api_keys.total_tokens")?,
            record.total_cost_usd,
            false,
        )?
        .with_ip_rules(
            record
                .ip_rules
                .as_ref()
                .map(|value| serde_json::json!(value)),
        )?
        .with_feature_settings(record.feature_settings)
        .with_activity_timestamps(None, Some(now_unix_secs), Some(now_unix_secs))?;

        index
            .by_key_hash
            .insert(record.key_hash, record.api_key_id.clone());
        index
            .by_api_key_id
            .insert(record.api_key_id.clone(), snapshot);
        index
            .export_by_api_key_id
            .insert(record.api_key_id, export.clone());
        Ok(Some(export))
    }

    async fn create_standalone_api_key(
        &self,
        record: CreateStandaloneApiKeyRecord,
    ) -> Result<Option<StoredAuthApiKeyExportRecord>, DataLayerError> {
        let mut index = self
            .index
            .write()
            .expect("auth api key snapshot repository lock");
        if index.by_api_key_id.contains_key(&record.api_key_id) {
            return Err(DataLayerError::UnexpectedValue(format!(
                "duplicate api_keys.id: {}",
                record.api_key_id
            )));
        }
        if index.by_key_hash.contains_key(&record.key_hash) {
            return Err(DataLayerError::UnexpectedValue(format!(
                "duplicate api_keys.key_hash: {}",
                record.key_hash
            )));
        }

        let template = index
            .by_api_key_id
            .values()
            .find(|snapshot| snapshot.user_id == record.user_id)
            .cloned();
        let snapshot = if let Some(template) = template {
            StoredAuthApiKeySnapshot {
                api_key_id: record.api_key_id.clone(),
                api_key_name: record.name.clone(),
                api_key_is_active: record.is_active,
                api_key_is_locked: false,
                api_key_is_standalone: true,
                api_key_rate_limit: record.rate_limit,
                api_key_concurrent_limit: record.concurrent_limit,
                api_key_expires_at_unix_secs: record.expires_at_unix_secs,
                api_key_allowed_providers: record.allowed_providers.clone(),
                api_key_allowed_api_formats: record.allowed_api_formats.clone(),
                api_key_allowed_models: record.allowed_models.clone(),
                api_key_ip_rules: record.ip_rules.clone(),
                ..template
            }
        } else {
            StoredAuthApiKeySnapshot::new(
                record.user_id.clone(),
                format!(
                    "admin-{}",
                    &record.user_id.chars().take(8).collect::<String>()
                ),
                None,
                "admin".to_string(),
                "local".to_string(),
                true,
                false,
                None,
                None,
                None,
                record.api_key_id.clone(),
                record.name.clone(),
                record.is_active,
                false,
                true,
                record.rate_limit,
                record.concurrent_limit,
                record
                    .expires_at_unix_secs
                    .map(|value| i64_from_u64(value, "api_keys.expires_at"))
                    .transpose()?,
                record
                    .allowed_providers
                    .as_ref()
                    .map(|value| serde_json::json!(value)),
                record
                    .allowed_api_formats
                    .as_ref()
                    .map(|value| serde_json::json!(value)),
                record
                    .allowed_models
                    .as_ref()
                    .map(|value| serde_json::json!(value)),
            )?
            .with_api_key_ip_rules(
                record
                    .ip_rules
                    .as_ref()
                    .map(|value| serde_json::json!(value)),
            )?
        };

        let now_unix_secs = current_unix_secs() as i64;
        let export = StoredAuthApiKeyExportRecord::new(
            record.user_id.clone(),
            record.api_key_id.clone(),
            record.key_hash.clone(),
            record.key_encrypted,
            record.name,
            record
                .allowed_providers
                .as_ref()
                .map(|value| serde_json::json!(value)),
            record
                .allowed_api_formats
                .as_ref()
                .map(|value| serde_json::json!(value)),
            record
                .allowed_models
                .as_ref()
                .map(|value| serde_json::json!(value)),
            record.rate_limit,
            record.concurrent_limit,
            record.force_capabilities,
            record.is_active,
            record
                .expires_at_unix_secs
                .map(|value| i64_from_u64(value, "api_keys.expires_at"))
                .transpose()?,
            record.auto_delete_on_expiry,
            i64_from_u64(record.total_requests, "api_keys.total_requests")?,
            i64_from_u64(record.total_tokens, "api_keys.total_tokens")?,
            record.total_cost_usd,
            true,
        )?
        .with_ip_rules(
            record
                .ip_rules
                .as_ref()
                .map(|value| serde_json::json!(value)),
        )?
        .with_activity_timestamps(None, Some(now_unix_secs), Some(now_unix_secs))?;

        index
            .by_key_hash
            .insert(record.key_hash, record.api_key_id.clone());
        index
            .by_api_key_id
            .insert(record.api_key_id.clone(), snapshot);
        index
            .export_by_api_key_id
            .insert(record.api_key_id, export.clone());
        Ok(Some(export))
    }

    async fn compare_and_swap_api_key_ciphertext(
        &self,
        mutation: &CompareAndSwapAuthApiKeyCiphertext,
    ) -> Result<bool, DataLayerError> {
        let mut index = self
            .index
            .write()
            .expect("auth api key snapshot repository lock");
        let Some(current) = index.export_by_api_key_id.get_mut(&mutation.api_key_id) else {
            return Ok(false);
        };
        if current.user_id != mutation.user_id
            || current.key_hash != mutation.key_hash
            || current.is_standalone != mutation.is_standalone
            || current.key_encrypted.as_deref() != Some(mutation.expected_key_encrypted.as_str())
        {
            return Ok(false);
        }
        current.key_encrypted = Some(mutation.key_encrypted.clone());
        Ok(true)
    }

    async fn update_user_api_key_basic(
        &self,
        record: UpdateUserApiKeyBasicRecord,
    ) -> Result<Option<StoredAuthApiKeyExportRecord>, DataLayerError> {
        let mut index = self
            .index
            .write()
            .expect("auth api key snapshot repository lock");
        let Some(snapshot) = index.by_api_key_id.get(&record.api_key_id) else {
            return Ok(None);
        };
        if snapshot.user_id != record.user_id || snapshot.api_key_is_standalone {
            return Ok(None);
        }
        if record.key_encrypted_present {
            if let Some(export) = index.export_by_api_key_id.get_mut(&record.api_key_id) {
                export.key_encrypted = record.key_encrypted.clone();
            }
        }
        if record.name_present {
            if let Some(snapshot) = index.by_api_key_id.get_mut(&record.api_key_id) {
                snapshot.api_key_name = record.name.clone();
            }
            if let Some(export) = index.export_by_api_key_id.get_mut(&record.api_key_id) {
                export.name = record.name.clone();
            }
        }
        if record.rate_limit_present {
            if let Some(snapshot) = index.by_api_key_id.get_mut(&record.api_key_id) {
                snapshot.api_key_rate_limit = record.rate_limit;
            }
            if let Some(export) = index.export_by_api_key_id.get_mut(&record.api_key_id) {
                export.rate_limit = record.rate_limit;
            }
        }
        if record.concurrent_limit_present {
            if let Some(snapshot) = index.by_api_key_id.get_mut(&record.api_key_id) {
                snapshot.api_key_concurrent_limit = record.concurrent_limit;
            }
            if let Some(export) = index.export_by_api_key_id.get_mut(&record.api_key_id) {
                export.concurrent_limit = record.concurrent_limit;
            }
        }
        if let Some(ip_rules) = record.ip_rules {
            if let Some(snapshot) = index.by_api_key_id.get_mut(&record.api_key_id) {
                snapshot.api_key_ip_rules = ip_rules.clone();
            }
            if let Some(export) = index.export_by_api_key_id.get_mut(&record.api_key_id) {
                export.ip_rules = ip_rules;
            }
        }
        if let Some(export) = index.export_by_api_key_id.get_mut(&record.api_key_id) {
            if let Some(selection) = record.routing_group_selection {
                export.feature_settings = selection.merge_feature_settings(
                    export.feature_settings.as_ref(),
                    record.feature_settings,
                );
            } else if let Some(feature_settings) = record.feature_settings {
                export.feature_settings = match feature_settings {
                    Some(serde_json::Value::Null) | None => None,
                    Some(value) => Some(value),
                };
            }
        }
        Ok(index.export_by_api_key_id.get(&record.api_key_id).cloned())
    }

    async fn update_user_api_key_basic_if_unlocked(
        &self,
        record: UpdateUserApiKeyBasicRecord,
    ) -> Result<Option<StoredAuthApiKeyExportRecord>, DataLayerError> {
        let mut index = self
            .index
            .write()
            .expect("auth api key snapshot repository lock");
        let Some(snapshot) = index.by_api_key_id.get(&record.api_key_id) else {
            return Ok(None);
        };
        if snapshot.user_id != record.user_id
            || snapshot.api_key_is_standalone
            || snapshot.api_key_is_locked
        {
            return Ok(None);
        }
        if record.key_encrypted_present {
            if let Some(export) = index.export_by_api_key_id.get_mut(&record.api_key_id) {
                export.key_encrypted = record.key_encrypted.clone();
            }
        }
        if record.name_present {
            if let Some(snapshot) = index.by_api_key_id.get_mut(&record.api_key_id) {
                snapshot.api_key_name = record.name.clone();
            }
            if let Some(export) = index.export_by_api_key_id.get_mut(&record.api_key_id) {
                export.name = record.name.clone();
            }
        }
        if record.rate_limit_present {
            if let Some(snapshot) = index.by_api_key_id.get_mut(&record.api_key_id) {
                snapshot.api_key_rate_limit = record.rate_limit;
            }
            if let Some(export) = index.export_by_api_key_id.get_mut(&record.api_key_id) {
                export.rate_limit = record.rate_limit;
            }
        }
        if record.concurrent_limit_present {
            if let Some(snapshot) = index.by_api_key_id.get_mut(&record.api_key_id) {
                snapshot.api_key_concurrent_limit = record.concurrent_limit;
            }
            if let Some(export) = index.export_by_api_key_id.get_mut(&record.api_key_id) {
                export.concurrent_limit = record.concurrent_limit;
            }
        }
        if let Some(ip_rules) = record.ip_rules {
            if let Some(snapshot) = index.by_api_key_id.get_mut(&record.api_key_id) {
                snapshot.api_key_ip_rules = ip_rules.clone();
            }
            if let Some(export) = index.export_by_api_key_id.get_mut(&record.api_key_id) {
                export.ip_rules = ip_rules;
            }
        }
        if let Some(export) = index.export_by_api_key_id.get_mut(&record.api_key_id) {
            if let Some(selection) = record.routing_group_selection {
                export.feature_settings = selection.merge_feature_settings(
                    export.feature_settings.as_ref(),
                    record.feature_settings,
                );
            } else if let Some(feature_settings) = record.feature_settings {
                export.feature_settings = match feature_settings {
                    Some(serde_json::Value::Null) | None => None,
                    Some(value) => Some(value),
                };
            }
        }
        Ok(index.export_by_api_key_id.get(&record.api_key_id).cloned())
    }

    async fn update_standalone_api_key_basic(
        &self,
        record: UpdateStandaloneApiKeyBasicRecord,
    ) -> Result<Option<StoredAuthApiKeyExportRecord>, DataLayerError> {
        let mut index = self
            .index
            .write()
            .expect("auth api key snapshot repository lock");
        let Some(snapshot) = index.by_api_key_id.get(&record.api_key_id) else {
            return Ok(None);
        };
        if !snapshot.api_key_is_standalone {
            return Ok(None);
        }
        if record.key_encrypted_present {
            if let Some(export) = index.export_by_api_key_id.get_mut(&record.api_key_id) {
                export.key_encrypted = record.key_encrypted.clone();
            }
        }
        if record.name_present {
            if let Some(snapshot) = index.by_api_key_id.get_mut(&record.api_key_id) {
                snapshot.api_key_name = record.name.clone();
            }
            if let Some(export) = index.export_by_api_key_id.get_mut(&record.api_key_id) {
                export.name = record.name.clone();
            }
        }
        if let Some(force_capabilities) = record.force_capabilities {
            if let Some(export) = index.export_by_api_key_id.get_mut(&record.api_key_id) {
                export.force_capabilities = force_capabilities;
            }
        }
        if record.rate_limit_present {
            if let Some(snapshot) = index.by_api_key_id.get_mut(&record.api_key_id) {
                snapshot.api_key_rate_limit = record.rate_limit;
            }
            if let Some(export) = index.export_by_api_key_id.get_mut(&record.api_key_id) {
                export.rate_limit = record.rate_limit;
            }
        }
        if record.concurrent_limit_present {
            if let Some(snapshot) = index.by_api_key_id.get_mut(&record.api_key_id) {
                snapshot.api_key_concurrent_limit = record.concurrent_limit;
            }
            if let Some(export) = index.export_by_api_key_id.get_mut(&record.api_key_id) {
                export.concurrent_limit = record.concurrent_limit;
            }
        }
        if let Some(allowed_providers) = record.allowed_providers {
            if let Some(snapshot) = index.by_api_key_id.get_mut(&record.api_key_id) {
                snapshot.api_key_allowed_providers = allowed_providers.clone();
            }
            if let Some(export) = index.export_by_api_key_id.get_mut(&record.api_key_id) {
                export.allowed_providers = allowed_providers;
            }
        }
        if let Some(allowed_api_formats) = record.allowed_api_formats {
            if let Some(snapshot) = index.by_api_key_id.get_mut(&record.api_key_id) {
                snapshot.api_key_allowed_api_formats = allowed_api_formats.clone();
            }
            if let Some(export) = index.export_by_api_key_id.get_mut(&record.api_key_id) {
                export.allowed_api_formats = allowed_api_formats;
            }
        }
        if let Some(allowed_models) = record.allowed_models {
            if let Some(snapshot) = index.by_api_key_id.get_mut(&record.api_key_id) {
                snapshot.api_key_allowed_models = allowed_models.clone();
            }
            if let Some(export) = index.export_by_api_key_id.get_mut(&record.api_key_id) {
                export.allowed_models = allowed_models;
            }
        }
        if let Some(ip_rules) = record.ip_rules {
            if let Some(snapshot) = index.by_api_key_id.get_mut(&record.api_key_id) {
                snapshot.api_key_ip_rules = ip_rules.clone();
            }
            if let Some(export) = index.export_by_api_key_id.get_mut(&record.api_key_id) {
                export.ip_rules = ip_rules;
            }
        }
        if record.expires_at_present {
            if let Some(snapshot) = index.by_api_key_id.get_mut(&record.api_key_id) {
                snapshot.api_key_expires_at_unix_secs = record.expires_at_unix_secs;
            }
            if let Some(export) = index.export_by_api_key_id.get_mut(&record.api_key_id) {
                export.expires_at_unix_secs = record.expires_at_unix_secs;
            }
        }
        if record.auto_delete_on_expiry_present {
            if let Some(export) = index.export_by_api_key_id.get_mut(&record.api_key_id) {
                export.auto_delete_on_expiry = record.auto_delete_on_expiry;
            }
        }
        Ok(index.export_by_api_key_id.get(&record.api_key_id).cloned())
    }

    async fn restore_api_key_if_matches(
        &self,
        expected: &StoredAuthApiKeyExportRecord,
        restored: &StoredAuthApiKeyExportRecord,
    ) -> Result<bool, DataLayerError> {
        let mut index = self
            .index
            .write()
            .expect("auth api key snapshot repository lock");
        let Some(current) = index.export_by_api_key_id.get(&expected.api_key_id) else {
            return Ok(false);
        };
        // Immutable identity must never be changed by compensation.  The complete exported row
        // comparison below is the in-memory equivalent of the database CAS predicate.
        if current != expected
            || restored.api_key_id != expected.api_key_id
            || restored.user_id != expected.user_id
            || restored.key_hash != expected.key_hash
            || restored.is_standalone != expected.is_standalone
        {
            return Ok(false);
        }
        index
            .export_by_api_key_id
            .insert(expected.api_key_id.clone(), restored.clone());
        if let Some(snapshot) = index.by_api_key_id.get_mut(&expected.api_key_id) {
            snapshot.api_key_name = restored.name.clone();
            snapshot.api_key_is_active = restored.is_active;
            snapshot.api_key_rate_limit = restored.rate_limit;
            snapshot.api_key_concurrent_limit = restored.concurrent_limit;
            snapshot.api_key_expires_at_unix_secs = restored.expires_at_unix_secs;
            snapshot.api_key_allowed_providers = restored.allowed_providers.clone();
            snapshot.api_key_allowed_api_formats = restored.allowed_api_formats.clone();
            snapshot.api_key_allowed_models = restored.allowed_models.clone();
            snapshot.api_key_ip_rules = restored.ip_rules.clone();
        }
        Ok(true)
    }

    async fn set_user_api_key_active(
        &self,
        user_id: &str,
        api_key_id: &str,
        is_active: bool,
    ) -> Result<Option<StoredAuthApiKeyExportRecord>, DataLayerError> {
        let mut index = self
            .index
            .write()
            .expect("auth api key snapshot repository lock");
        let Some(snapshot) = index.by_api_key_id.get(api_key_id) else {
            return Ok(None);
        };
        if snapshot.user_id != user_id || snapshot.api_key_is_standalone {
            return Ok(None);
        }
        if let Some(snapshot) = index.by_api_key_id.get_mut(api_key_id) {
            snapshot.api_key_is_active = is_active;
        }
        if let Some(export) = index.export_by_api_key_id.get_mut(api_key_id) {
            export.is_active = is_active;
        }
        Ok(index.export_by_api_key_id.get(api_key_id).cloned())
    }

    async fn set_user_api_key_active_if_unlocked(
        &self,
        user_id: &str,
        api_key_id: &str,
        is_active: bool,
    ) -> Result<Option<StoredAuthApiKeyExportRecord>, DataLayerError> {
        let mut index = self
            .index
            .write()
            .expect("auth api key snapshot repository lock");
        let Some(snapshot) = index.by_api_key_id.get(api_key_id) else {
            return Ok(None);
        };
        if snapshot.user_id != user_id
            || snapshot.api_key_is_standalone
            || snapshot.api_key_is_locked
        {
            return Ok(None);
        }
        if let Some(snapshot) = index.by_api_key_id.get_mut(api_key_id) {
            snapshot.api_key_is_active = is_active;
        }
        if let Some(export) = index.export_by_api_key_id.get_mut(api_key_id) {
            export.is_active = is_active;
        }
        Ok(index.export_by_api_key_id.get(api_key_id).cloned())
    }

    async fn set_standalone_api_key_active(
        &self,
        api_key_id: &str,
        is_active: bool,
    ) -> Result<Option<StoredAuthApiKeyExportRecord>, DataLayerError> {
        let mut index = self
            .index
            .write()
            .expect("auth api key snapshot repository lock");
        let Some(snapshot) = index.by_api_key_id.get(api_key_id) else {
            return Ok(None);
        };
        if !snapshot.api_key_is_standalone {
            return Ok(None);
        }
        if let Some(snapshot) = index.by_api_key_id.get_mut(api_key_id) {
            snapshot.api_key_is_active = is_active;
        }
        if let Some(export) = index.export_by_api_key_id.get_mut(api_key_id) {
            export.is_active = is_active;
        }
        Ok(index.export_by_api_key_id.get(api_key_id).cloned())
    }

    async fn set_user_api_key_locked(
        &self,
        user_id: &str,
        api_key_id: &str,
        is_locked: bool,
    ) -> Result<bool, DataLayerError> {
        let mut index = self
            .index
            .write()
            .expect("auth api key snapshot repository lock");
        let Some(snapshot) = index.by_api_key_id.get(api_key_id) else {
            return Ok(false);
        };
        if snapshot.user_id != user_id || snapshot.api_key_is_standalone {
            return Ok(false);
        }
        if let Some(snapshot) = index.by_api_key_id.get_mut(api_key_id) {
            snapshot.api_key_is_locked = is_locked;
        }
        Ok(true)
    }

    async fn set_user_api_key_allowed_providers(
        &self,
        user_id: &str,
        api_key_id: &str,
        allowed_providers: Option<Vec<String>>,
    ) -> Result<Option<StoredAuthApiKeyExportRecord>, DataLayerError> {
        let mut index = self
            .index
            .write()
            .expect("auth api key snapshot repository lock");
        let Some(snapshot) = index.by_api_key_id.get(api_key_id) else {
            return Ok(None);
        };
        if snapshot.user_id != user_id || snapshot.api_key_is_standalone {
            return Ok(None);
        }
        if let Some(snapshot) = index.by_api_key_id.get_mut(api_key_id) {
            snapshot.api_key_allowed_providers = allowed_providers.clone();
        }
        if let Some(export) = index.export_by_api_key_id.get_mut(api_key_id) {
            export.allowed_providers = allowed_providers;
        }
        Ok(index.export_by_api_key_id.get(api_key_id).cloned())
    }

    async fn set_user_api_key_allowed_providers_if_unlocked(
        &self,
        user_id: &str,
        api_key_id: &str,
        allowed_providers: Option<Vec<String>>,
    ) -> Result<Option<StoredAuthApiKeyExportRecord>, DataLayerError> {
        let mut index = self
            .index
            .write()
            .expect("auth api key snapshot repository lock");
        let Some(snapshot) = index.by_api_key_id.get(api_key_id) else {
            return Ok(None);
        };
        if snapshot.user_id != user_id
            || snapshot.api_key_is_standalone
            || snapshot.api_key_is_locked
        {
            return Ok(None);
        }
        if let Some(snapshot) = index.by_api_key_id.get_mut(api_key_id) {
            snapshot.api_key_allowed_providers = allowed_providers.clone();
        }
        if let Some(export) = index.export_by_api_key_id.get_mut(api_key_id) {
            export.allowed_providers = allowed_providers;
        }
        Ok(index.export_by_api_key_id.get(api_key_id).cloned())
    }

    async fn set_user_api_key_force_capabilities(
        &self,
        user_id: &str,
        api_key_id: &str,
        force_capabilities: Option<serde_json::Value>,
    ) -> Result<Option<StoredAuthApiKeyExportRecord>, DataLayerError> {
        let mut index = self
            .index
            .write()
            .expect("auth api key snapshot repository lock");
        let Some(snapshot) = index.by_api_key_id.get(api_key_id) else {
            return Ok(None);
        };
        if snapshot.user_id != user_id || snapshot.api_key_is_standalone {
            return Ok(None);
        }
        let Some(export) = index.export_by_api_key_id.get_mut(api_key_id) else {
            return Ok(None);
        };
        export.force_capabilities = force_capabilities;
        Ok(Some(export.clone()))
    }

    async fn set_user_api_key_force_capabilities_if_unlocked(
        &self,
        user_id: &str,
        api_key_id: &str,
        force_capabilities: Option<serde_json::Value>,
    ) -> Result<Option<StoredAuthApiKeyExportRecord>, DataLayerError> {
        let mut index = self
            .index
            .write()
            .expect("auth api key snapshot repository lock");
        let Some(snapshot) = index.by_api_key_id.get(api_key_id) else {
            return Ok(None);
        };
        if snapshot.user_id != user_id
            || snapshot.api_key_is_standalone
            || snapshot.api_key_is_locked
        {
            return Ok(None);
        }
        let Some(export) = index.export_by_api_key_id.get_mut(api_key_id) else {
            return Ok(None);
        };
        export.force_capabilities = force_capabilities;
        Ok(Some(export.clone()))
    }

    async fn set_user_api_key_feature_settings(
        &self,
        user_id: &str,
        api_key_id: &str,
        feature_settings: Option<serde_json::Value>,
    ) -> Result<Option<StoredAuthApiKeyExportRecord>, DataLayerError> {
        let mut index = self
            .index
            .write()
            .expect("auth api key snapshot repository lock");
        let Some(snapshot) = index.by_api_key_id.get(api_key_id) else {
            return Ok(None);
        };
        if snapshot.user_id != user_id || snapshot.api_key_is_standalone {
            return Ok(None);
        }
        let Some(export) = index.export_by_api_key_id.get_mut(api_key_id) else {
            return Ok(None);
        };
        export.feature_settings = match feature_settings {
            Some(serde_json::Value::Null) | None => None,
            Some(value) => Some(value),
        };
        Ok(Some(export.clone()))
    }

    async fn set_user_api_key_feature_settings_if_unlocked(
        &self,
        user_id: &str,
        api_key_id: &str,
        feature_settings: Option<serde_json::Value>,
    ) -> Result<Option<StoredAuthApiKeyExportRecord>, DataLayerError> {
        let mut index = self
            .index
            .write()
            .expect("auth api key snapshot repository lock");
        let Some(snapshot) = index.by_api_key_id.get(api_key_id) else {
            return Ok(None);
        };
        if snapshot.user_id != user_id
            || snapshot.api_key_is_standalone
            || snapshot.api_key_is_locked
        {
            return Ok(None);
        }
        let Some(export) = index.export_by_api_key_id.get_mut(api_key_id) else {
            return Ok(None);
        };
        export.feature_settings = match feature_settings {
            Some(serde_json::Value::Null) | None => None,
            Some(value) => Some(value),
        };
        Ok(Some(export.clone()))
    }

    async fn set_api_key_usage_totals(
        &self,
        api_key_id: &str,
        total_requests: u64,
        total_tokens: u64,
        total_cost_usd: f64,
    ) -> Result<Option<StoredAuthApiKeyExportRecord>, DataLayerError> {
        i64_from_u64(total_requests, "api_keys.total_requests")?;
        i64_from_u64(total_tokens, "api_keys.total_tokens")?;
        if !total_cost_usd.is_finite() {
            return Err(DataLayerError::InvalidInput(
                "api_keys.total_cost_usd is not finite".to_string(),
            ));
        }
        let mut index = self
            .index
            .write()
            .expect("auth api key snapshot repository lock");
        let Some(export) = index.export_by_api_key_id.get_mut(api_key_id) else {
            return Ok(None);
        };
        export.total_requests = total_requests;
        export.total_tokens = total_tokens;
        export.total_cost_usd = total_cost_usd;
        Ok(Some(export.clone()))
    }

    async fn delete_user_api_key(
        &self,
        user_id: &str,
        api_key_id: &str,
    ) -> Result<bool, DataLayerError> {
        let mut index = self
            .index
            .write()
            .expect("auth api key snapshot repository lock");
        let Some(snapshot) = index.by_api_key_id.get(api_key_id) else {
            return Ok(false);
        };
        if snapshot.user_id != user_id || snapshot.api_key_is_standalone {
            return Ok(false);
        }
        Self::remove_api_key(&mut index, api_key_id);
        Ok(true)
    }

    async fn delete_user_api_key_if_unlocked(
        &self,
        user_id: &str,
        api_key_id: &str,
    ) -> Result<bool, DataLayerError> {
        let mut index = self
            .index
            .write()
            .expect("auth api key snapshot repository lock");
        let Some(snapshot) = index.by_api_key_id.get(api_key_id) else {
            return Ok(false);
        };
        if snapshot.user_id != user_id
            || snapshot.api_key_is_standalone
            || snapshot.api_key_is_locked
        {
            return Ok(false);
        }
        Self::remove_api_key(&mut index, api_key_id);
        Ok(true)
    }

    async fn delete_standalone_api_key(&self, api_key_id: &str) -> Result<bool, DataLayerError> {
        let mut index = self
            .index
            .write()
            .expect("auth api key snapshot repository lock");
        let Some(snapshot) = index.by_api_key_id.get(api_key_id) else {
            return Ok(false);
        };
        if !snapshot.api_key_is_standalone {
            return Ok(false);
        }
        Self::remove_api_key(&mut index, api_key_id);
        Ok(true)
    }

    async fn set_standalone_api_key_feature_settings(
        &self,
        api_key_id: &str,
        feature_settings: Option<serde_json::Value>,
    ) -> Result<Option<StoredAuthApiKeyExportRecord>, DataLayerError> {
        let mut index = self
            .index
            .write()
            .expect("auth api key snapshot repository lock");
        let Some(snapshot) = index.by_api_key_id.get(api_key_id) else {
            return Ok(None);
        };
        if !snapshot.api_key_is_standalone {
            return Ok(None);
        }
        let Some(export) = index.export_by_api_key_id.get_mut(api_key_id) else {
            return Ok(None);
        };
        export.feature_settings = match feature_settings {
            Some(serde_json::Value::Null) | None => None,
            Some(value) => Some(value),
        };
        Ok(Some(export.clone()))
    }
}

#[cfg(test)]
#[path = "memory_tests.rs"]
mod tests;
