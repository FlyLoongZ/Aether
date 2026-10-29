pub use aether_data_contracts::DataLayerError;

pub(crate) fn redis_error(error: impl std::fmt::Display) -> DataLayerError {
    DataLayerError::redis(error)
}

pub(crate) trait RedisResultExt<T> {
    fn map_redis_err(self) -> Result<T, DataLayerError>;
}

impl<T, E: std::fmt::Display> RedisResultExt<T> for Result<T, E> {
    fn map_redis_err(self) -> Result<T, DataLayerError> {
        self.map_err(redis_error)
    }
}
