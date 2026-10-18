use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use aether_cache::OnceMap;

use super::super::error::GatewayError;
use super::super::provider_transport;
use super::super::provider_transport::ProviderTransportSnapshotCacheKey;

pub(crate) const AUTH_API_KEY_LAST_USED_TTL: Duration = Duration::from_secs(60);
// Keep normal freshness short for cross-node configuration propagation. A
// stale entry is served while one background refresh runs, so an idle burst
// does not turn expiry into a synchronous database wait.
pub(crate) const PROVIDER_TRANSPORT_SNAPSHOT_CACHE_TTL: Duration = Duration::from_secs(1);
// Keep the last known transport usable across normal idle periods. The first
// stale request starts a single background refresh, and every local catalog or
// credential mutation advances the generation and clears the entry.
pub(crate) const PROVIDER_TRANSPORT_SNAPSHOT_CACHE_STALE_TTL: Duration =
    Duration::from_secs(5 * 60);
const PROVIDER_TRANSPORT_SNAPSHOT_CACHE_CAPACITY_BYTES: usize = 16 * 1024 * 1024;
const PROVIDER_TRANSPORT_SNAPSHOT_CACHE_LOAD_TIMEOUT: Duration = Duration::from_secs(10);

/// Provider transport snapshots, one per `(provider, endpoint, key)`.
///
/// A missing row is cached like a hit: without it, a provider whose transport
/// cannot be resolved would re-read the database on every request.
#[derive(Debug, Clone)]
pub(crate) struct ProviderTransportSnapshotCache {
    entries: OnceMap<
        ProviderTransportSnapshotCacheKey,
        Option<Arc<provider_transport::GatewayProviderTransportSnapshot>>,
        GatewayError,
    >,
}

impl Default for ProviderTransportSnapshotCache {
    fn default() -> Self {
        Self::new()
    }
}

impl ProviderTransportSnapshotCache {
    pub(crate) fn new() -> Self {
        Self {
            entries: OnceMap::builder()
                .capacity_bytes(PROVIDER_TRANSPORT_SNAPSHOT_CACHE_CAPACITY_BYTES)
                .load_timeout(PROVIDER_TRANSPORT_SNAPSHOT_CACHE_LOAD_TIMEOUT)
                .build(),
        }
    }

    /// Loads the snapshot for one `(provider, endpoint, key)` triple.
    ///
    /// The caller owns both loaders because the background one has to be
    /// `'static`; the cache only owns the key and the windows.
    pub(crate) async fn get_or_load<C, CF, B, BF>(
        &self,
        provider_id: &str,
        endpoint_id: &str,
        key_id: &str,
        cold_load: C,
        background_refresh: B,
    ) -> Result<Option<Arc<provider_transport::GatewayProviderTransportSnapshot>>, GatewayError>
    where
        C: FnOnce() -> CF,
        CF: Future<
            Output = Result<
                Option<Arc<provider_transport::GatewayProviderTransportSnapshot>>,
                GatewayError,
            >,
        >,
        B: FnOnce() -> BF,
        BF: Future<
                Output = Result<
                    Option<Arc<provider_transport::GatewayProviderTransportSnapshot>>,
                    GatewayError,
                >,
            > + Send
            + 'static,
    {
        let Some(cache_key) =
            ProviderTransportSnapshotCacheKey::new(provider_id, endpoint_id, key_id)
        else {
            return cold_load().await;
        };
        self.entries
            .get_or_load_revalidating(
                cache_key,
                PROVIDER_TRANSPORT_SNAPSHOT_CACHE_TTL,
                PROVIDER_TRANSPORT_SNAPSHOT_CACHE_STALE_TTL,
                cold_load,
                background_refresh,
            )
            .await
            .map(|value| (*value).clone())
            .map_err(|error| error.or_else_load(GatewayError::Internal))
    }

    pub(crate) fn clear(&self) {
        self.entries.invalidate_all();
    }
}
