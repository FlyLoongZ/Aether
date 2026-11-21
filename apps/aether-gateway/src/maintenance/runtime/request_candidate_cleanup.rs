use aether_data_contracts::DataLayerError;
use serde_json::json;
use std::time::Instant;
use tracing::info;

use crate::data::GatewayDataState;

use super::{
    now_unix_secs, record_completed_cleanup_run, record_failed_cleanup_run, system_config_bool,
    system_config_u64, system_config_usize,
};

pub(crate) async fn cleanup_request_candidates_once(
    data: &GatewayDataState,
) -> Result<usize, DataLayerError> {
    if !system_config_bool(data, "enable_auto_cleanup", true).await? {
        return Ok(0);
    }

    let detail_log_retention_days = system_config_u64(data, "detail_log_retention_days", 7).await?;
    let retention_days = system_config_u64(
        data,
        "request_candidates_retention_days",
        detail_log_retention_days,
    )
    .await?
    .max(3);
    let cleanup_batch_size = system_config_usize(data, "cleanup_batch_size", 1_000).await?;
    let delete_limit = system_config_usize(
        data,
        "request_candidates_cleanup_batch_size",
        cleanup_batch_size.max(1),
    )
    .await?
    .max(1);
    let cutoff_unix_secs = now_unix_secs().saturating_sub(retention_days.saturating_mul(86_400));

    // Bounded per run, and it yields between batches: a large backlog (first
    // enable, or a long-disabled worker) used to keep deleting for hours while
    // holding a pool connection, starving foreground requests. Whatever is left
    // is picked up by the next run.
    const MAX_BATCHES_PER_RUN: usize = 32;
    let mut total_deleted = 0usize;
    for _ in 0..MAX_BATCHES_PER_RUN {
        let deleted = data
            .delete_request_candidates_created_before(cutoff_unix_secs, delete_limit)
            .await?;
        total_deleted += deleted;
        if deleted < delete_limit {
            break;
        }
        if data.database_pool_summary().is_some_and(|summary| {
            GatewayDataState::database_pool_summary_under_maintenance_pressure(&summary)
        }) {
            info!("request candidate cleanup yielding: database pool under pressure");
            break;
        }
        tokio::task::yield_now().await;
    }

    Ok(total_deleted)
}

pub(super) async fn run_request_candidate_cleanup_once(
    data: &GatewayDataState,
) -> Result<(), DataLayerError> {
    let started_at_unix_secs = now_unix_secs();
    let started_at = Instant::now();
    let deleted = match cleanup_request_candidates_once(data).await {
        Ok(deleted) => deleted,
        Err(err) => {
            record_failed_cleanup_run(
                data,
                "request_candidate_cleanup",
                "auto",
                started_at_unix_secs,
                started_at,
                &err,
            )
            .await;
            return Err(err);
        }
    };
    record_completed_cleanup_run(
        data,
        "request_candidate_cleanup",
        "auto",
        started_at_unix_secs,
        started_at,
        json!({ "request_candidates_deleted": deleted }),
        format!("候选记录自动清理完成，删除 {deleted} 行"),
    )
    .await;
    if deleted > 0 {
        info!(deleted, "gateway deleted expired request candidates");
    }
    Ok(())
}
