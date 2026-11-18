#[cfg(any(test, feature = "test-support"))]
mod memory;

pub use aether_data_contracts::repository::auth_modules::{
    AuthModuleReadRepository, AuthModuleWriteRepository, CompareAndSwapLdapConfigResult,
    LdapBindPasswordUpdate, StoredLdapModuleConfig, StoredOAuthProviderModuleConfig,
};
pub use aether_data_postgres::{SqlxAuthModuleReadRepository, SqlxAuthModuleRepository};
#[cfg(any(test, feature = "test-support"))]
pub use memory::InMemoryAuthModuleReadRepository;
