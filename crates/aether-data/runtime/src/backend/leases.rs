use std::fmt;

use super::PostgresBackend;
use aether_data_postgres::{PostgresLeaseRunner, PostgresLeaseRunnerConfig};
use crate::DataLayerError;

#[derive(Clone, Default)]
pub struct DataLeaseBackends {
    postgres: Option<PostgresLeaseRunner>,
}

impl fmt::Debug for DataLeaseBackends {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DataLeaseBackends")
            .field("has_postgres", &self.has_any())
            .finish()
    }
}

impl DataLeaseBackends {
    pub(crate) fn from_postgres(
        postgres: Option<&PostgresBackend>,
    ) -> Result<Self, DataLayerError> {
        Ok(Self {
            postgres: postgres
                .map(|backend| backend.lease_runner(PostgresLeaseRunnerConfig::default()))
                .transpose()?,
        })
    }

    pub fn postgres(&self) -> Option<PostgresLeaseRunner> {
        self.postgres.clone()
    }

    pub fn has_any(&self) -> bool {
        self.postgres.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::DataLeaseBackends;
    use crate::backend::PostgresBackend;
    use crate::PostgresPoolConfig;

    #[tokio::test]
    async fn builds_postgres_lease_runner_from_backend() {
        let backend = PostgresBackend::from_config(PostgresPoolConfig {
            database_url: "postgres://localhost/aether".to_string(),
            min_connections: 1,
            max_connections: 4,
            acquire_timeout_ms: 1_000,
            idle_timeout_ms: 5_000,
            max_lifetime_ms: 30_000,
            statement_cache_capacity: 64,
            require_ssl: false,
        })
        .expect("postgres backend should build");

        let leases =
            DataLeaseBackends::from_postgres(Some(&backend)).expect("lease backends should build");

        assert!(leases.has_any());
        assert!(leases.postgres().is_some());
    }
}
