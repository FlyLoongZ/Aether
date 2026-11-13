//! Usage retention.
//!
//! Every table this module sweeps owns a `created_at` and is retired by a plain
//! `DELETE` bounded by it. Nothing is cleared in place: a swept row leaves the
//! predicate, so the next batch starts where the last one stopped and the
//! per-run cost stays linear in the rows removed. That is why there is no
//! cursor and no stall guard here -- a batch that returns rows can only return
//! rows it has not deleted yet.

use aether_data_contracts::repository::usage::{
    UsageCleanupExecutionMode, UsageCleanupPreviewCounts, UsageCleanupSummary, UsageCleanupTargets,
    UsageCleanupWindow,
};
use chrono::{DateTime, Utc};
use futures_util::TryStreamExt;
use sqlx::Row;
use tracing::warn;

use super::SqlxUsageReadRepository;
use crate::{error::postgres_error, DataLayerError, PostgresPool};

const DELETE_OLD_USAGE_RECORDS_SQL: &str = r#"
WITH doomed AS (
    SELECT id
    FROM usage
    WHERE created_at < $1
    ORDER BY created_at ASC, id ASC
    LIMIT $2
)
DELETE FROM usage AS usage_rows
USING doomed
WHERE usage_rows.id = doomed.id
"#;

macro_rules! delete_expired_batch_sql {
    ($table:literal) => {
        concat!(
            "DELETE FROM ",
            $table,
            "\nWHERE ctid IN (\n",
            "  SELECT ctid FROM ",
            $table,
            "\n  WHERE created_at < $1",
            "\n  ORDER BY created_at ASC",
            "\n  LIMIT $2",
            "\n)"
        )
    };
}

macro_rules! count_expired_sql {
    ($table:literal) => {
        concat!(
            "SELECT COUNT(*)::bigint FROM ",
            $table,
            " WHERE created_at < $1"
        )
    };
}

const DELETE_EXPIRED_USAGE_HTTP_HEADERS_SQL: &str = delete_expired_batch_sql!("usage_http_headers");
const DELETE_EXPIRED_USAGE_HTTP_BODIES_SQL: &str = delete_expired_batch_sql!("usage_http_bodies");
const COUNT_EXPIRED_USAGE_HTTP_HEADERS_SQL: &str = count_expired_sql!("usage_http_headers");
const COUNT_EXPIRED_USAGE_HTTP_BODIES_SQL: &str = count_expired_sql!("usage_http_bodies");

const TRUNCATE_USAGE_HTTP_BODIES_SQL: &str = "TRUNCATE TABLE usage_http_bodies";

const SELECT_EXPIRED_ACTIVE_API_KEYS_SQL: &str = r#"
SELECT id, auto_delete_on_expiry
FROM api_keys
WHERE expires_at <= NOW()
  AND is_active IS TRUE
ORDER BY expires_at ASC NULLS FIRST, id ASC
"#;
const DISABLE_EXPIRED_API_KEY_WALLET_SQL: &str = r#"
UPDATE wallets
SET status = 'disabled',
    updated_at = NOW()
WHERE api_key_id = $1
  AND status <> 'disabled'
"#;
const DELETE_EXPIRED_API_KEY_SQL: &str = r#"
DELETE FROM api_keys
WHERE id = $1
"#;
const DISABLE_EXPIRED_API_KEY_SQL: &str = r#"
UPDATE api_keys
SET is_active = FALSE,
    updated_at = $2
WHERE id = $1
  AND is_active IS TRUE
"#;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ExpiredApiKeyRow<'a> {
    id: &'a str,
    auto_delete_on_expiry: Option<bool>,
}

impl SqlxUsageReadRepository {
    pub async fn cleanup_usage(
        &self,
        window: &UsageCleanupWindow,
        batch_size: usize,
        auto_delete_expired_keys: bool,
        targets: UsageCleanupTargets,
        mode: UsageCleanupExecutionMode,
    ) -> Result<UsageCleanupSummary, DataLayerError> {
        if batch_size == 0 || !targets.any_selected() {
            return Ok(UsageCleanupSummary::default());
        }
        if mode == UsageCleanupExecutionMode::BeforeNowBodyFields {
            let body_cleaned = if targets.body {
                purge_usage_http_bodies(&self.pool, batch_size).await?
            } else {
                0
            };
            return Ok(UsageCleanupSummary {
                body_cleaned,
                ..UsageCleanupSummary::default()
            });
        }

        // Deleting the accounting row cascades to both capture tables, so the
        // sweeps below only do work the record window did not already cover.
        let records_deleted = if targets.records {
            delete_old_usage_records(&self.pool, window.log_cutoff, batch_size).await?
        } else {
            0
        };
        let header_cleaned = if targets.headers {
            sweep_expired_capture(
                &self.pool,
                DELETE_EXPIRED_USAGE_HTTP_HEADERS_SQL,
                window.header_cutoff,
                batch_size,
                targets.records.then_some(window.log_cutoff),
                "headers",
            )
            .await?
        } else {
            0
        };
        let body_cleaned = if targets.body {
            sweep_expired_capture(
                &self.pool,
                DELETE_EXPIRED_USAGE_HTTP_BODIES_SQL,
                window.body_cutoff,
                batch_size,
                targets.records.then_some(window.log_cutoff),
                "bodies",
            )
            .await?
        } else {
            0
        };
        // A body file is unreachable as soon as either the accounting row (the
        // ledger) or the body row is gone, so sweep it at the earlier of the two
        // cutoffs. Records-only cleanup still has to run this: deleting the
        // usage row cascades to `usage_http_bodies`, and without the file sweep
        // the bytes would leak forever.
        let file_cutoff = body_file_cleanup_cutoff(targets, window);
        if let (Some(cutoff), Some(store)) = (file_cutoff, aether_body_store::global()) {
            match store.cleanup_before(cutoff.date_naive()).await {
                Ok(stats) if stats.removed_days > 0 => {
                    tracing::info!(
                        removed_days = stats.removed_days,
                        "usage cleanup removed body file date directories"
                    );
                }
                Ok(_) => {}
                Err(err) => {
                    warn!(error = %err, "usage cleanup body file sweep failed");
                }
            }
        }
        let keys_cleaned = if targets.expired_keys {
            match cleanup_expired_api_keys(&self.pool, auto_delete_expired_keys).await {
                Ok(count) => count,
                Err(err) => {
                    warn!(error = %err, "usage cleanup expired api key sweep failed");
                    0
                }
            }
        } else {
            0
        };

        Ok(UsageCleanupSummary {
            body_cleaned,
            header_cleaned,
            keys_cleaned,
            records_deleted,
            cost_reservations_deleted: 0,
            request_admissions_deleted: 0,
        })
    }
}

pub async fn preview_usage_cleanup_impl(
    pool: &PostgresPool,
    window: &UsageCleanupWindow,
    targets: UsageCleanupTargets,
    mode: UsageCleanupExecutionMode,
) -> Result<UsageCleanupPreviewCounts, DataLayerError> {
    if mode == UsageCleanupExecutionMode::BeforeNowBodyFields {
        let body = if targets.body {
            count_all_usage_http_bodies(pool).await?
        } else {
            0
        };
        return Ok(UsageCleanupPreviewCounts {
            body,
            header: 0,
            log: 0,
        });
    }

    let body = if targets.body {
        count_expired_capture(
            pool,
            COUNT_EXPIRED_USAGE_HTTP_BODIES_SQL,
            window.body_cutoff,
        )
        .await?
    } else {
        0
    };
    let header = if targets.headers {
        count_expired_capture(
            pool,
            COUNT_EXPIRED_USAGE_HTTP_HEADERS_SQL,
            window.header_cutoff,
        )
        .await?
    } else {
        0
    };
    let log = if targets.records {
        let log: i64 =
            sqlx::query_scalar("SELECT COUNT(*)::bigint FROM usage WHERE created_at < $1")
                .bind(window.log_cutoff)
                .fetch_one(pool)
                .await
                .map_err(postgres_error)?;
        u64::try_from(log).unwrap_or(0)
    } else {
        0
    };

    Ok(UsageCleanupPreviewCounts { body, header, log })
}

/// Drops every captured body, keeping the headers.
///
/// `TRUNCATE` is the whole point of the manual "purge bodies now" action, so the
/// fallback only matters when the table is locked.
async fn purge_usage_http_bodies(
    pool: &PostgresPool,
    batch_size: usize,
) -> Result<usize, DataLayerError> {
    let started_at = Utc::now();
    let count = count_all_usage_http_bodies(pool).await?;
    if let Some(store) = aether_body_store::global() {
        if let Err(err) = store.purge_all().await {
            warn!(error = %err, "usage cleanup body file purge failed");
        }
    }
    match truncate_usage_http_bodies(pool).await {
        Ok(()) => Ok(usize::try_from(count).unwrap_or(0)),
        Err(err) => {
            warn!(
                error = %err,
                "usage cleanup truncate usage_http_bodies failed or timed out, falling back to batch deletion"
            );
            let mut deleted_total = 0usize;
            loop {
                let mut tx = pool.begin().await.map_err(postgres_error)?;
                sqlx::query("SET LOCAL lock_timeout = '10s'")
                    .execute(&mut *tx)
                    .await
                    .map_err(postgres_error)?;
                sqlx::query("SET LOCAL statement_timeout = '60s'")
                    .execute(&mut *tx)
                    .await
                    .map_err(postgres_error)?;
                let deleted = sqlx::query(
                    "DELETE FROM usage_http_bodies WHERE ctid IN (SELECT ctid FROM usage_http_bodies WHERE created_at < $2 LIMIT $1)",
                )
                .bind(i64::try_from(batch_size).unwrap_or(i64::MAX))
                .bind(started_at)
                .execute(&mut *tx)
                .await
                .map_err(postgres_error)?
                .rows_affected();
                tx.commit().await.map_err(postgres_error)?;
                let deleted = usize::try_from(deleted).unwrap_or(usize::MAX);
                deleted_total = deleted_total.saturating_add(deleted);
                if deleted < batch_size {
                    break;
                }
            }
            Ok(deleted_total)
        }
    }
}

async fn truncate_usage_http_bodies(pool: &PostgresPool) -> Result<(), DataLayerError> {
    let mut tx = pool.begin().await.map_err(postgres_error)?;
    sqlx::query("SET LOCAL lock_timeout = '30s'")
        .execute(&mut *tx)
        .await
        .map_err(postgres_error)?;
    sqlx::query("SET LOCAL statement_timeout = '60s'")
        .execute(&mut *tx)
        .await
        .map_err(postgres_error)?;
    sqlx::query(TRUNCATE_USAGE_HTTP_BODIES_SQL)
        .execute(&mut *tx)
        .await
        .map_err(postgres_error)?;
    tx.commit().await.map_err(postgres_error)?;
    Ok(())
}

async fn count_all_usage_http_bodies(pool: &PostgresPool) -> Result<u64, DataLayerError> {
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*)::bigint FROM usage_http_bodies")
        .fetch_one(pool)
        .await
        .map_err(postgres_error)?;
    Ok(u64::try_from(count).unwrap_or(0))
}

async fn count_expired_capture(
    pool: &PostgresPool,
    sql: &'static str,
    cutoff_time: DateTime<Utc>,
) -> Result<u64, DataLayerError> {
    let count: i64 = sqlx::query_scalar(sql)
        .bind(cutoff_time)
        .fetch_one(pool)
        .await
        .map_err(postgres_error)?;
    Ok(u64::try_from(count).unwrap_or(0))
}

/// Deletes expired capture rows a batch at a time until one comes back short.
///
/// A row that is gone cannot be selected again, so the loop terminates on its
/// own and never needs a resume position.
async fn sweep_expired_capture(
    pool: &PostgresPool,
    sql: &'static str,
    cutoff_time: DateTime<Utc>,
    batch_size: usize,
    newer_than: Option<DateTime<Utc>>,
    stage: &'static str,
) -> Result<usize, DataLayerError> {
    if matches!(newer_than, Some(value) if value >= cutoff_time) {
        warn!(
            cutoff_time = %cutoff_time,
            newer_than = ?newer_than,
            stage,
            "usage cleanup capture sweep skipped due to invalid window"
        );
        return Ok(0);
    }

    let limit = i64::try_from(batch_size).unwrap_or(i64::MAX);
    let mut total_deleted = 0usize;
    loop {
        let deleted = sqlx::query(sql)
            .bind(cutoff_time)
            .bind(limit)
            .execute(pool)
            .await
            .map_err(postgres_error)?
            .rows_affected();
        let deleted = usize::try_from(deleted).unwrap_or(usize::MAX);
        total_deleted += deleted;
        if deleted < batch_size {
            break;
        }
    }
    Ok(total_deleted)
}

async fn delete_old_usage_records(
    pool: &PostgresPool,
    cutoff_time: DateTime<Utc>,
    batch_size: usize,
) -> Result<usize, DataLayerError> {
    let mut total_deleted = 0usize;
    loop {
        let deleted = sqlx::query(DELETE_OLD_USAGE_RECORDS_SQL)
            .bind(cutoff_time)
            .bind(i64::try_from(batch_size).unwrap_or(i64::MAX))
            .execute(pool)
            .await
            .map_err(postgres_error)?
            .rows_affected();
        let deleted = usize::try_from(deleted).unwrap_or(usize::MAX);
        total_deleted += deleted;
        if deleted < batch_size {
            break;
        }
    }
    Ok(total_deleted)
}

async fn cleanup_expired_api_keys(
    pool: &PostgresPool,
    auto_delete_expired_keys: bool,
) -> Result<usize, DataLayerError> {
    let mut expired_keys = sqlx::query(SELECT_EXPIRED_ACTIVE_API_KEYS_SQL).fetch(pool);
    let mut cleaned = 0usize;
    while let Some(row) = expired_keys.try_next().await.map_err(postgres_error)? {
        let api_key_id = row.try_get::<String, _>("id").map_err(postgres_error)?;
        let key = ExpiredApiKeyRow {
            id: api_key_id.as_str(),
            auto_delete_on_expiry: row
                .try_get::<Option<bool>, _>("auto_delete_on_expiry")
                .map_err(postgres_error)?,
        };
        let should_delete = key
            .auto_delete_on_expiry
            .unwrap_or(auto_delete_expired_keys);
        if should_delete {
            sqlx::query(DISABLE_EXPIRED_API_KEY_WALLET_SQL)
                .bind(key.id)
                .execute(pool)
                .await
                .map_err(postgres_error)?;
            let deleted = sqlx::query(DELETE_EXPIRED_API_KEY_SQL)
                .bind(key.id)
                .execute(pool)
                .await
                .map_err(postgres_error)?
                .rows_affected();
            if deleted > 0 {
                cleaned += 1;
            }
        } else {
            let updated = sqlx::query(DISABLE_EXPIRED_API_KEY_SQL)
                .bind(key.id)
                .bind(Utc::now())
                .execute(pool)
                .await
                .map_err(postgres_error)?
                .rows_affected();
            if updated > 0 {
                cleaned += 1;
            }
        }
    }
    Ok(cleaned)
}

/// The cutoff for the body file sweep.
///
/// A file is unreachable once either the accounting row (the ledger in
/// `usage.request_metadata`) or the body row is gone, so the sweep uses the
/// earlier of the two selected cutoffs. Records-only cleanup must still sweep
/// files because deleting the usage row cascades to `usage_http_bodies`.
fn body_file_cleanup_cutoff(
    targets: UsageCleanupTargets,
    window: &UsageCleanupWindow,
) -> Option<DateTime<Utc>> {
    match (targets.body, targets.records) {
        (true, true) => Some(window.body_cutoff.min(window.log_cutoff)),
        (true, false) => Some(window.body_cutoff),
        (false, true) => Some(window.log_cutoff),
        (false, false) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::{
        body_file_cleanup_cutoff, UsageCleanupTargets, UsageCleanupWindow,
        COUNT_EXPIRED_USAGE_HTTP_BODIES_SQL, COUNT_EXPIRED_USAGE_HTTP_HEADERS_SQL,
        DELETE_EXPIRED_USAGE_HTTP_BODIES_SQL, DELETE_EXPIRED_USAGE_HTTP_HEADERS_SQL,
    };
    use chrono::{TimeZone, Utc};

    #[test]
    fn body_targets_select_only_body_cleanup() {
        let targets = UsageCleanupTargets::body_targets();
        assert!(targets.body);
        assert!(!targets.headers);
        assert!(!targets.records);
        assert!(!targets.expired_keys);
        assert!(targets.any_selected());
    }

    #[test]
    fn all_policy_targets_select_every_cleanup_dimension() {
        let targets = UsageCleanupTargets::all_policy_targets();
        assert!(targets.body && targets.headers && targets.records && targets.expired_keys);
    }

    #[test]
    fn stale_body_batch_covers_blobs_and_audit_refs() {
        assert!(DELETE_EXPIRED_USAGE_HTTP_BODIES_SQL.contains("usage_http_bodies"));
        assert!(DELETE_EXPIRED_USAGE_HTTP_HEADERS_SQL.contains("usage_http_headers"));
    }

    /// The sweep must be driven by the capture table's own timestamp. Driving it
    /// from `usage` is what made a cleared row stay a candidate forever, because
    /// clearing a column never removed it from the predicate.
    #[test]
    fn capture_sweeps_are_driven_by_their_own_timestamp() {
        for sql in [
            DELETE_EXPIRED_USAGE_HTTP_HEADERS_SQL,
            DELETE_EXPIRED_USAGE_HTTP_BODIES_SQL,
            COUNT_EXPIRED_USAGE_HTTP_HEADERS_SQL,
            COUNT_EXPIRED_USAGE_HTTP_BODIES_SQL,
        ] {
            assert!(sql.contains("created_at < $1"), "{sql}");
            assert!(!sql.contains(" FROM usage "), "{sql}");
            assert!(!sql.contains("EXISTS"), "{sql}");
        }
    }

    /// Sweeping has to delete, not clear: a preserved row would be selected by
    /// the next batch and rewritten on every pass.
    #[test]
    fn capture_sweeps_delete_whole_rows() {
        for sql in [
            DELETE_EXPIRED_USAGE_HTTP_HEADERS_SQL,
            DELETE_EXPIRED_USAGE_HTTP_BODIES_SQL,
        ] {
            assert!(sql.starts_with("DELETE FROM"), "{sql}");
            assert!(!sql.contains("UPDATE"), "{sql}");
            assert!(!sql.contains("= NULL"), "{sql}");
            assert!(!sql.contains("updated_at"), "{sql}");
        }
    }

    /// A sweep that is bounded only by the cutoff would re-walk the freed space
    /// every pass; the batch limit is what keeps one pass short.
    #[test]
    fn capture_sweeps_are_batch_bounded_and_ordered() {
        for sql in [
            DELETE_EXPIRED_USAGE_HTTP_HEADERS_SQL,
            DELETE_EXPIRED_USAGE_HTTP_BODIES_SQL,
        ] {
            assert!(sql.contains("ORDER BY created_at ASC"), "{sql}");
            assert!(sql.contains("LIMIT $2"), "{sql}");
        }
    }

    /// Records-only cleanup still has to sweep body files: deleting the usage
    /// row cascades to `usage_http_bodies`, and the ledger disappears with it.
    #[test]
    fn body_file_cleanup_cutoff_follows_the_selected_targets() {
        let make_window = |body, log| UsageCleanupWindow {
            body_cutoff: Utc.timestamp_opt(body, 0).unwrap(),
            header_cutoff: Utc.timestamp_opt(2_000, 0).unwrap(),
            log_cutoff: Utc.timestamp_opt(log, 0).unwrap(),
        };
        let targets = |body, records| UsageCleanupTargets {
            body,
            headers: false,
            records,
            expired_keys: false,
        };

        let window = make_window(1_000, 3_000);
        assert_eq!(
            body_file_cleanup_cutoff(targets(true, true), &window),
            Some(window.body_cutoff)
        );
        assert_eq!(
            body_file_cleanup_cutoff(targets(true, false), &window),
            Some(window.body_cutoff)
        );
        assert_eq!(
            body_file_cleanup_cutoff(targets(false, true), &window),
            Some(window.log_cutoff)
        );
        assert_eq!(
            body_file_cleanup_cutoff(targets(false, false), &window),
            None
        );

        // The earlier cutoff wins: the file is unreachable once either row is gone.
        let window = make_window(3_000, 1_000);
        assert_eq!(
            body_file_cleanup_cutoff(targets(true, true), &window),
            Some(window.log_cutoff)
        );
    }
}
