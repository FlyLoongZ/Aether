#[cfg(any(test, feature = "test-support"))]
mod memory;

pub use aether_data_contracts::repository::management_tokens::{
    ActivateManagementTokenIfMatches, CreateManagementTokenRecord, ManagementTokenListQuery,
    ManagementTokenReadRepository, ManagementTokenWriteRepository, RegenerateManagementTokenSecret,
    StoredManagementToken, StoredManagementTokenListPage, StoredManagementTokenUserSummary,
    StoredManagementTokenWithUser, UpdateManagementTokenRecord,
};
#[cfg(feature = "postgres")]
pub use aether_data_postgres::SqlxManagementTokenRepository;
#[cfg(any(test, feature = "test-support"))]
pub use memory::InMemoryManagementTokenRepository;
