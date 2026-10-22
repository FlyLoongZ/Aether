#[cfg(any(test, feature = "test-support"))]
mod memory;

#[allow(unused_imports)]
pub(crate) use aether_data_contracts::repository::provider_catalog::{
    ProviderCatalogKeyAdaptiveState, ProviderCatalogKeyAdaptiveStateUpdate,
    ProviderCatalogKeyAdminCasUpdate, ProviderCatalogKeyCredentialsCasUpdate,
    ProviderCatalogKeyHealthStateUpdate, ProviderCatalogKeyListOrder, ProviderCatalogKeyListQuery,
    ProviderCatalogKeyOAuthCredentialCasDelete, ProviderCatalogKeyOAuthCredentialFence,
    ProviderCatalogKeyOAuthRuntimeStateCasUpdate, ProviderCatalogKeyRuntimeMetadataUpdate,
    ProviderCatalogKeyStatusSnapshotUpdate, ProviderCatalogProviderConfigCasUpdate,
    ProviderCatalogProxyCasUpdate, ProviderCatalogReadRepository, ProviderCatalogSnapshot,
    ProviderCatalogUpstreamMetadataNamespaceExpectation,
    ProviderCatalogUpstreamMetadataNamespaceUpdate, ProviderCatalogWriteRepository,
    StoredProviderCatalogEndpoint, StoredProviderCatalogKey,
    StoredProviderCatalogKeyMaintenanceSummary, StoredProviderCatalogKeyPage,
    StoredProviderCatalogKeyStats, StoredProviderCatalogProvider,
};
#[cfg(feature = "postgres")]
pub use aether_data_postgres::SqlxProviderCatalogReadRepository;
#[cfg(any(test, feature = "test-support"))]
pub use memory::InMemoryProviderCatalogReadRepository;
