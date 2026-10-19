use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use aether_cache::Snapshot;
use aether_data::DataLayerError;
use aether_data_contracts::repository::routing_profiles::{
    RoutingGroupBindingQuery, RoutingGroupLookupKey, RoutingGroupReadRepository,
    StoredRoutingGroup, StoredRoutingGroupBinding, StoredRoutingGroupVersion,
};
use async_trait::async_trait;

// Routing profile writes drop the local version. Keep the fallback window
// bounded so a missed cross-node invalidation cannot route traffic stale for
// minutes.
const ROUTING_GROUP_SNAPSHOT_TTL: Duration = Duration::from_secs(60);

/// The whole routing configuration, loaded as one version.
///
/// Routing groups and their bindings are admin-written and read on the request
/// path. Caching them per query shape meant the same table was cut into seven
/// entries with seven independent freshness windows; here one load produces one
/// immutable version, and every read answers from it.
struct RoutingGroupTables {
    /// In the repository's own order (`enabled DESC, sort_order ASC, …`).
    groups: Vec<StoredRoutingGroup>,
    /// In the repository's own order (`created_at ASC, id ASC`).
    bindings: Vec<StoredRoutingGroupBinding>,
}

pub(super) struct CachedRoutingGroupReadRepository {
    inner: Arc<dyn RoutingGroupReadRepository>,
    tables: Snapshot<RoutingGroupTables>,
}

impl CachedRoutingGroupReadRepository {
    pub(super) fn new(inner: Arc<dyn RoutingGroupReadRepository>) -> Self {
        Self {
            inner,
            tables: Snapshot::new(),
        }
    }

    async fn tables(&self) -> Result<Arc<RoutingGroupTables>, DataLayerError> {
        self.tables
            .get_or_load(ROUTING_GROUP_SNAPSHOT_TTL, || self.load_tables())
            .await
    }

    async fn load_tables(&self) -> Result<RoutingGroupTables, DataLayerError> {
        let groups = self.inner.list_routing_groups().await?;
        let bindings = self
            .inner
            .list_routing_group_bindings(&RoutingGroupBindingQuery::default())
            .await?;
        Ok(RoutingGroupTables { groups, bindings })
    }
}

fn system_default(groups: &[StoredRoutingGroup]) -> Option<StoredRoutingGroup> {
    // Mirrors `WHERE is_system_default AND enabled
    //         ORDER BY updated_at DESC, id ASC LIMIT 1`.
    groups
        .iter()
        .filter(|group| group.is_system_default && group.enabled)
        .min_by(|left, right| {
            right
                .updated_at
                .cmp(&left.updated_at)
                .then_with(|| left.id.cmp(&right.id))
        })
        .cloned()
}

fn binding_matches(binding: &StoredRoutingGroupBinding, query: &RoutingGroupBindingQuery) -> bool {
    query
        .group_id
        .as_ref()
        .is_none_or(|group_id| group_id == &binding.group_id)
        && query
            .subject_type
            .is_none_or(|subject_type| subject_type == binding.subject_type)
        && query
            .subject_id
            .as_ref()
            .is_none_or(|subject_id| subject_id == &binding.subject_id)
}

#[async_trait]
impl RoutingGroupReadRepository for CachedRoutingGroupReadRepository {
    fn clear_local_cache(&self) {
        self.tables.invalidate();
        self.inner.clear_local_cache();
    }

    async fn list_routing_groups(&self) -> Result<Vec<StoredRoutingGroup>, DataLayerError> {
        Ok(self.tables().await?.groups.clone())
    }

    async fn find_routing_group(
        &self,
        lookup: RoutingGroupLookupKey<'_>,
    ) -> Result<Option<StoredRoutingGroup>, DataLayerError> {
        let tables = self.tables().await?;
        Ok(match lookup {
            RoutingGroupLookupKey::Id(id) => {
                tables.groups.iter().find(|group| group.id == id).cloned()
            }
            RoutingGroupLookupKey::Name(name) => tables
                .groups
                .iter()
                .find(|group| group.name == name)
                .cloned(),
            RoutingGroupLookupKey::SystemDefault => system_default(&tables.groups),
        })
    }

    async fn list_routing_group_bindings(
        &self,
        query: &RoutingGroupBindingQuery,
    ) -> Result<Vec<StoredRoutingGroupBinding>, DataLayerError> {
        let tables = self.tables().await?;
        Ok(tables
            .bindings
            .iter()
            .filter(|binding| binding_matches(binding, query))
            .cloned()
            .collect())
    }

    async fn has_any_routing_group_binding(&self) -> Result<bool, DataLayerError> {
        Ok(!self.tables().await?.bindings.is_empty())
    }

    /// Version history is an admin read: it is not on the request path, so it
    /// is not worth a cache entry per group.
    async fn list_routing_group_versions(
        &self,
        group_id: &str,
    ) -> Result<Vec<StoredRoutingGroupVersion>, DataLayerError> {
        self.inner.list_routing_group_versions(group_id).await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use aether_data_contracts::repository::routing_profiles::RoutingGroupBindingSubject;

    #[derive(Default)]
    struct CountingRoutingGroupRepository {
        list_calls: AtomicUsize,
        binding_calls: AtomicUsize,
        groups: Vec<StoredRoutingGroup>,
        bindings: Vec<StoredRoutingGroupBinding>,
    }

    impl CountingRoutingGroupRepository {
        fn with_data(
            groups: Vec<StoredRoutingGroup>,
            bindings: Vec<StoredRoutingGroupBinding>,
        ) -> Self {
            Self {
                list_calls: AtomicUsize::new(0),
                binding_calls: AtomicUsize::new(0),
                groups,
                bindings,
            }
        }
    }

    #[async_trait]
    impl RoutingGroupReadRepository for CountingRoutingGroupRepository {
        async fn list_routing_groups(&self) -> Result<Vec<StoredRoutingGroup>, DataLayerError> {
            self.list_calls.fetch_add(1, Ordering::AcqRel);
            tokio::time::sleep(Duration::from_millis(20)).await;
            Ok(self.groups.clone())
        }

        async fn find_routing_group(
            &self,
            _lookup: RoutingGroupLookupKey<'_>,
        ) -> Result<Option<StoredRoutingGroup>, DataLayerError> {
            panic!("lookups must be answered from the version");
        }

        async fn list_routing_group_bindings(
            &self,
            query: &RoutingGroupBindingQuery,
        ) -> Result<Vec<StoredRoutingGroupBinding>, DataLayerError> {
            self.binding_calls.fetch_add(1, Ordering::AcqRel);
            // The snapshot always loads the unfiltered table.
            assert!(query.group_id.is_none());
            assert!(query.subject_type.is_none());
            assert!(query.subject_id.is_none());
            Ok(self.bindings.clone())
        }

        async fn list_routing_group_versions(
            &self,
            _group_id: &str,
        ) -> Result<Vec<StoredRoutingGroupVersion>, DataLayerError> {
            Ok(Vec::new())
        }
    }

    fn group(id: &str, name: &str, updated_at: i64) -> StoredRoutingGroup {
        StoredRoutingGroup {
            id: id.to_string(),
            name: name.to_string(),
            description: None,
            enabled: true,
            is_system_default: false,
            sort_order: 0,
            config_json: serde_json::json!({}),
            version: 1,
            created_at: 0,
            updated_at,
            published_at: None,
        }
    }

    fn binding(group_id: &str, subject_id: &str) -> StoredRoutingGroupBinding {
        StoredRoutingGroupBinding {
            id: format!("{group_id}-{subject_id}"),
            group_id: group_id.to_string(),
            subject_type: RoutingGroupBindingSubject::User,
            subject_id: subject_id.to_string(),
            is_default: false,
            allow_explicit_select: false,
            created_at: 0,
            updated_at: 0,
        }
    }

    #[tokio::test]
    async fn a_repeat_read_is_served_from_the_version() {
        let inner = Arc::new(CountingRoutingGroupRepository::default());
        let repository = CachedRoutingGroupReadRepository::new(inner.clone());

        repository.list_routing_groups().await.unwrap();
        repository.list_routing_groups().await.unwrap();
        repository.has_any_routing_group_binding().await.unwrap();

        assert_eq!(1, inner.list_calls.load(Ordering::Acquire));
        assert_eq!(1, inner.binding_calls.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn clearing_the_cache_forces_a_reload() {
        let inner = Arc::new(CountingRoutingGroupRepository::default());
        let repository = CachedRoutingGroupReadRepository::new(inner.clone());

        repository.list_routing_groups().await.unwrap();
        repository.clear_local_cache();
        repository.list_routing_groups().await.unwrap();

        assert_eq!(2, inner.list_calls.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn concurrent_cold_reads_load_once() {
        let inner = Arc::new(CountingRoutingGroupRepository::default());
        let repository = Arc::new(CachedRoutingGroupReadRepository::new(inner.clone()));

        let mut tasks = Vec::new();
        for _ in 0..16 {
            let repository = Arc::clone(&repository);
            tasks.push(tokio::spawn(async move {
                repository.list_routing_groups().await.unwrap();
            }));
        }
        for task in tasks {
            task.await.unwrap();
        }

        assert_eq!(1, inner.list_calls.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn lookups_and_filters_are_answered_from_one_version() {
        let mut default_group = group("g-default", "default", 10);
        default_group.is_system_default = true;
        let inner = Arc::new(CountingRoutingGroupRepository::with_data(
            vec![group("g-1", "one", 1), default_group],
            vec![binding("g-1", "user-a"), binding("g-1", "user-b")],
        ));
        let repository = CachedRoutingGroupReadRepository::new(inner.clone());

        let by_id = repository
            .find_routing_group(RoutingGroupLookupKey::Id("g-1"))
            .await
            .unwrap()
            .expect("group should exist");
        assert_eq!("one", by_id.name);

        let by_name = repository
            .find_routing_group(RoutingGroupLookupKey::Name("default"))
            .await
            .unwrap()
            .expect("default group should exist");
        assert_eq!("g-default", by_name.id);

        let system_default = repository
            .find_routing_group(RoutingGroupLookupKey::SystemDefault)
            .await
            .unwrap()
            .expect("system default should exist");
        assert_eq!("g-default", system_default.id);

        let filtered = repository
            .list_routing_group_bindings(&RoutingGroupBindingQuery {
                group_id: Some("g-1".to_string()),
                subject_type: Some(RoutingGroupBindingSubject::User),
                subject_id: Some("user-b".to_string()),
            })
            .await
            .unwrap();
        assert_eq!(1, filtered.len());
        assert_eq!("user-b", filtered[0].subject_id);

        assert!(repository.has_any_routing_group_binding().await.unwrap());
        assert_eq!(1, inner.list_calls.load(Ordering::Acquire));
        assert_eq!(1, inner.binding_calls.load(Ordering::Acquire));
    }
}
