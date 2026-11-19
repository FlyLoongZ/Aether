use aether_data::DataLayerConfig;
use aether_data::PostgresPoolConfig;
use std::fmt;

#[derive(Clone, Default)]
pub struct GatewayDataConfig {
    database: Option<PostgresPoolConfig>,
    encryption_key: Option<String>,
}

impl fmt::Debug for GatewayDataConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GatewayDataConfig")
            .field("database", &self.database)
            .field("has_encryption_key", &self.encryption_key.is_some())
            .finish()
    }
}

impl GatewayDataConfig {
    pub fn disabled() -> Self {
        Self::default()
    }

    pub fn from_postgres_config(database: PostgresPoolConfig) -> Self {
        Self {
            database: Some(database),
            encryption_key: None,
        }
    }

    pub fn from_database_config(database: PostgresPoolConfig) -> Self {
        Self::from_postgres_config(database)
    }

    pub fn from_postgres_url(database_url: impl Into<String>, require_ssl: bool) -> Self {
        let database = PostgresPoolConfig {
            database_url: database_url.into(),
            require_ssl,
            ..Default::default()
        };
        Self::from_postgres_config(database)
    }

    pub fn database(&self) -> Option<&PostgresPoolConfig> {
        self.database.as_ref()
    }

    pub fn with_encryption_key(mut self, encryption_key: impl Into<String>) -> Self {
        let encryption_key = encryption_key.into();
        let encryption_key = encryption_key.trim();
        self.encryption_key = if encryption_key.is_empty() {
            None
        } else {
            Some(encryption_key.to_string())
        };
        self
    }

    pub fn encryption_key(&self) -> Option<&str> {
        self.encryption_key.as_deref()
    }

    pub fn with_redis_url(
        self,
        _url: impl Into<String>,
        _key_prefix: Option<impl Into<String>>,
    ) -> Self {
        self
    }

    pub fn is_enabled(&self) -> bool {
        self.database.is_some()
    }

    pub fn to_data_layer_config(&self) -> DataLayerConfig {
        DataLayerConfig {
            database: self.database.clone(),
        }
    }
}
