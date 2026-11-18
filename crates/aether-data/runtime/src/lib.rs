//! Runtime data access for Aether.
//!
//! This crate is the runtime composition facade. It builds the PostgreSQL
//! adapter, owns in-memory repositories and maintenance/backfill workflows,
//! and exposes application-facing backend handles. Shared repository
//! contracts live in `aether-data-contracts`; request-path SQL and executable
//! migrations live in the driver adapter crates.
//!
//! See `crates/aether-data/runtime/README.md` for the layer map.

pub mod backend;
mod config;
mod database;
pub mod driver;
mod error;
pub mod lifecycle;
pub mod maintenance;
pub mod repository;

pub use backend::PostgresBackend;
pub use backend::{
    DataBackends, DataLeaseBackends, DataReadRepositories, DataTransactionBackends,
    DataWriteRepositories,
};
pub use config::DataLayerConfig;
pub use database::PostgresPoolConfig;
pub use error::DataLayerError;
pub use maintenance::{
    DatabaseMaintenanceSummary, DatabasePoolSummary, DatabasePostgresActivityGroup,
    DatabasePostgresObservabilitySnapshot, StatsDailyAggregationInput,
    StatsDailyAggregationSummary, StatsHourlyAggregationInput, StatsHourlyAggregationSummary,
    WalletDailyUsageAggregationInput, WalletDailyUsageAggregationResult,
};
