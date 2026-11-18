use crate::DataLayerError;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct PostgresPoolConfig {
    pub database_url: String,
    pub min_connections: u32,
    pub max_connections: u32,
    pub acquire_timeout_ms: u64,
    pub idle_timeout_ms: u64,
    pub max_lifetime_ms: u64,
    pub statement_cache_capacity: usize,
    pub require_ssl: bool,
}

impl Default for PostgresPoolConfig {
    fn default() -> Self {
        Self {
            database_url: String::new(),
            min_connections: 4,
            max_connections: 20,
            acquire_timeout_ms: 10_000,
            idle_timeout_ms: 30_000,
            max_lifetime_ms: 30 * 60_000,
            statement_cache_capacity: 100,
            require_ssl: false,
        }
    }
}

impl PostgresPoolConfig {
    pub fn validate(&self) -> Result<(), DataLayerError> {
        if self.database_url.trim().is_empty() {
            return Err(DataLayerError::InvalidConfiguration(
                "postgres database_url cannot be empty".to_string(),
            ));
        }
        validate_database_url(&self.database_url)?;
        if self.min_connections > self.max_connections {
            return Err(DataLayerError::InvalidConfiguration(
                "postgres min_connections cannot exceed max_connections".to_string(),
            ));
        }
        if self.statement_cache_capacity == 0 {
            return Err(DataLayerError::InvalidConfiguration(
                "postgres statement_cache_capacity must be positive".to_string(),
            ));
        }
        Ok(())
    }
}

fn is_postgres_url(url: &str) -> bool {
    let scheme = url
        .split_once(':')
        .map(|(scheme, _)| scheme.to_ascii_lowercase());
    matches!(scheme.as_deref(), Some("postgres" | "postgresql"))
}

fn validate_database_url(url: &str) -> Result<(), DataLayerError> {
    if !is_postgres_url(url) {
        let scheme = url
            .split_once(':')
            .map(|(scheme, _)| scheme)
            .unwrap_or("missing");
        return Err(DataLayerError::InvalidConfiguration(format!(
            "unsupported database URL scheme '{scheme}'; expected postgres or postgresql"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::PostgresPoolConfig;

    fn postgres_config(database_url: &str) -> PostgresPoolConfig {
        PostgresPoolConfig {
            database_url: database_url.to_string(),
            ..Default::default()
        }
    }

    #[test]
    fn rejects_removed_database_url_schemes() {
        for unsupported in ["mysql", "mariadb", "sqlite"] {
            let url = format!("{unsupported}://localhost/aether");
            let error = postgres_config(&url)
                .validate()
                .expect_err("unsupported URL must not fall back to another driver");
            assert!(error
                .to_string()
                .contains("unsupported database URL scheme"));
        }
    }

    #[test]
    fn validates_postgres_pool_limits() {
        assert!(postgres_config("postgres://localhost/aether")
            .validate()
            .is_ok());
        assert!(postgres_config("postgresql://localhost/aether")
            .validate()
            .is_ok());

        let mut config = postgres_config("postgres://localhost/aether");
        config.min_connections = 4;
        config.max_connections = 2;
        assert!(config.validate().is_err());

        let mut config = postgres_config("postgres://localhost/aether");
        config.statement_cache_capacity = 0;
        assert!(config.validate().is_err());
    }
}
