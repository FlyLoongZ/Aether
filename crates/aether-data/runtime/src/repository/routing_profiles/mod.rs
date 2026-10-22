#[cfg(any(test, feature = "test-support"))]
mod memory;

#[cfg(any(test, feature = "test-support"))]
pub(crate) use aether_data_contracts::repository::routing_profiles::{
    CreateRoutingGroupBindingRecord, CreateRoutingGroupRecord, CreateRoutingGroupVersionRecord,
    RoutingGroupBindingQuery, RoutingGroupLookupKey, StoredRoutingGroup, StoredRoutingGroupBinding,
    StoredRoutingGroupVersion, UpdateRoutingGroupBindingRecord, UpdateRoutingGroupRecord,
};
pub(crate) use aether_data_contracts::repository::routing_profiles::{
    RoutingGroupReadRepository, RoutingGroupWriteRepository,
};
#[cfg(any(test, feature = "test-support"))]
pub use memory::InMemoryRoutingGroupRepository;

#[cfg(feature = "postgres")]
pub use aether_data_postgres::PostgresRoutingGroupRepository;
