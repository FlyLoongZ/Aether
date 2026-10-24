use aether_data_contracts::DataLayerError;
#[cfg(test)]
use tracing::warn;

use crate::data::GatewayDataState;

use super::{system_config_bool, DB_MAINTENANCE_TABLES};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct DbMaintenanceRunSummary {
    pub(super) attempted: usize,
    pub(super) succeeded: usize,
}

pub(super) async fn perform_db_maintenance_once(
    data: &GatewayDataState,
) -> Result<DbMaintenanceRunSummary, DataLayerError> {
    if !data.has_database_maintenance_backend() {
        return Ok(DbMaintenanceRunSummary {
            attempted: 0,
            succeeded: 0,
        });
    }

    if !system_config_bool(data, "enable_db_maintenance", true).await? {
        return Ok(DbMaintenanceRunSummary {
            attempted: 0,
            succeeded: 0,
        });
    }

    let summary = data.run_database_maintenance(DB_MAINTENANCE_TABLES).await?;
    Ok(DbMaintenanceRunSummary {
        attempted: summary.attempted,
        succeeded: summary.succeeded,
    })
}

#[cfg(test)]
pub(super) async fn run_db_maintenance_with<F, Fut>(
    data: &GatewayDataState,
    mut vacuum_table: F,
) -> Result<DbMaintenanceRunSummary, DataLayerError>
where
    F: FnMut(&'static str) -> Fut,
    Fut: std::future::Future<Output = Result<(), DataLayerError>>,
{
    if !system_config_bool(data, "enable_db_maintenance", true).await? {
        return Ok(DbMaintenanceRunSummary {
            attempted: 0,
            succeeded: 0,
        });
    }

    let mut summary = DbMaintenanceRunSummary {
        attempted: 0,
        succeeded: 0,
    };
    for table_name in DB_MAINTENANCE_TABLES {
        summary.attempted += 1;
        match vacuum_table(table_name).await {
            Ok(()) => summary.succeeded += 1,
            Err(err) => {
                warn!(table = table_name, error = %err, "gateway db maintenance table failed");
            }
        }
    }
    Ok(summary)
}
