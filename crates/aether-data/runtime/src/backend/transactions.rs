use std::fmt;

use super::PostgresBackend;
use crate::driver::postgres::PostgresTransactionRunner;

#[derive(Clone, Default)]
pub struct DataTransactionBackends {
    postgres: Option<PostgresTransactionRunner>,
}

impl fmt::Debug for DataTransactionBackends {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DataTransactionBackends")
            .field("has_postgres", &self.has_any())
            .finish()
    }
}

impl DataTransactionBackends {
    pub(crate) fn from_postgres(postgres: Option<&PostgresBackend>) -> Self {
        Self {
            postgres: postgres.map(PostgresBackend::transaction_runner),
        }
    }

    pub fn postgres(&self) -> Option<PostgresTransactionRunner> {
        self.postgres.clone()
    }

    pub fn has_any(&self) -> bool {
        self.postgres.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::DataTransactionBackends;
    use crate::backend::PostgresBackend;
    use crate::driver::postgres::PostgresPoolConfig;

    #[tokio::test]
    async fn builds_postgres_transaction_runner_from_backend() {
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

        let transactions = DataTransactionBackends::from_postgres(Some(&backend));

        assert!(transactions.has_any());
        assert!(transactions.postgres().is_some());
    }
}
