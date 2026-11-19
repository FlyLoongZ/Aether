//! Runtime data access for Aether.
//!
//! This crate is the runtime data-access composition layer. It builds the
//! PostgreSQL backend, owns maintenance/backfill workflows, and exposes
//! application-facing backend handles. Shared repository contracts live in
//! `aether-data-contracts`; request-path SQL and executable migrations live in
//! the adapter crates, and test-only in-memory repositories live in
//! `aether-data-memory`.
//!
//! See `crates/aether-data/runtime/README.md` for the layer map.

pub mod backend;
mod config;
mod error;
pub mod lifecycle;
pub mod maintenance;

pub use aether_data_contracts::PostgresPoolConfig;
pub use backend::PostgresBackend;
pub use backend::{
    DataBackends, DataLeaseBackends, DataReadRepositories, DataTransactionBackends,
    DataWriteRepositories,
};
pub use config::DataLayerConfig;
pub use error::DataLayerError;
pub use maintenance::{
    DatabaseMaintenanceSummary, DatabasePoolSummary, DatabasePostgresActivityGroup,
    DatabasePostgresObservabilitySnapshot, StatsDailyAggregationInput,
    StatsDailyAggregationSummary, StatsHourlyAggregationInput, StatsHourlyAggregationSummary,
    WalletDailyUsageAggregationInput, WalletDailyUsageAggregationResult,
};
