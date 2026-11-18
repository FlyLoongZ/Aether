#[cfg(any(test, feature = "test-support"))]
mod memory;

#[allow(unused_imports)]
pub(crate) use aether_data_contracts::repository::quota::{
    ProviderQuotaReadRepository, ProviderQuotaRepository, ProviderQuotaWriteRepository,
    StoredProviderQuotaSnapshot,
};
pub use aether_data_postgres::SqlxProviderQuotaRepository;
#[cfg(any(test, feature = "test-support"))]
pub use memory::InMemoryProviderQuotaRepository;
