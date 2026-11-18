use crate::database::PostgresPoolConfig;
use crate::DataLayerError;

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct DataLayerConfig {
    pub database: Option<PostgresPoolConfig>,
}

impl DataLayerConfig {
    pub fn from_database(database: PostgresPoolConfig) -> Self {
        Self {
            database: Some(database),
        }
    }

    pub fn effective_database(&self) -> Option<PostgresPoolConfig> {
        self.database.clone()
    }

    pub fn validate(&self) -> Result<(), DataLayerError> {
        if let Some(database) = &self.database {
            database.validate()?;
        }
        Ok(())
    }

    pub fn has_persistent_backends(&self) -> bool {
        self.database.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::DataLayerConfig;
    use crate::database::PostgresPoolConfig;

    fn postgres_config(database_url: &str) -> PostgresPoolConfig {
        PostgresPoolConfig {
            database_url: database_url.to_string(),
            min_connections: 2,
            max_connections: 8,
            acquire_timeout_ms: 1_500,
            idle_timeout_ms: 5_000,
            max_lifetime_ms: 30_000,
            statement_cache_capacity: 64,
            require_ssl: false,
        }
    }

    #[test]
    fn validates_nested_backend_configs() {
        let config = DataLayerConfig {
            database: Some(postgres_config("postgres://localhost/aether")),
        };

        assert!(config.validate().is_ok());
        assert!(config.has_persistent_backends());
    }

    #[test]
    fn rejects_invalid_nested_backend_configs() {
        let config = DataLayerConfig {
            database: Some(PostgresPoolConfig {
                min_connections: 4,
                max_connections: 2,
                ..postgres_config("postgres://localhost/aether")
            }),
        };

        assert!(config.validate().is_err());
    }
}
