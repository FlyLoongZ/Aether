use aether_data_contracts::repository::usage::{
    UsageCleanupExecutionMode, UsageCleanupPreviewCounts, UsageCleanupSummary, UsageCleanupTargets,
    UsageCleanupWindow,
};
use chrono::{DateTime, Utc};
use futures_util::TryStreamExt;
use sqlx::{postgres::PgArguments, query::Query, Postgres, Row};
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
const SELECT_USAGE_HEADER_BATCH_SQL: &str = r#"
SELECT id, request_id
FROM usage
WHERE created_at < $1
  AND ($2::timestamptz IS NULL OR created_at >= $2)
  AND (
    request_headers IS NOT NULL
    OR response_headers IS NOT NULL
    OR provider_request_headers IS NOT NULL
    OR client_response_headers IS NOT NULL
    OR EXISTS (
      SELECT 1
      FROM usage_http_audits
      WHERE usage_http_audits.request_id = usage.request_id
        AND (
          usage_http_audits.request_headers IS NOT NULL
          OR usage_http_audits.response_headers IS NOT NULL
          OR usage_http_audits.provider_request_headers IS NOT NULL
          OR usage_http_audits.client_response_headers IS NOT NULL
        )
    )
  )
ORDER BY created_at ASC, id ASC
LIMIT $3
"#;
const CLEAR_USAGE_HEADER_FIELDS_SQL: &str = r#"
UPDATE usage
SET request_headers = NULL,
    response_headers = NULL,
    provider_request_headers = NULL,
    client_response_headers = NULL
WHERE id = ANY($1)
"#;
const CLEAR_USAGE_HTTP_AUDIT_HEADERS_SQL: &str = r#"
UPDATE usage_http_audits
SET request_headers = NULL,
    response_headers = NULL,
    provider_request_headers = NULL,
    client_response_headers = NULL,
    updated_at = NOW()
WHERE request_id = ANY($1)
"#;
const DELETE_EMPTY_USAGE_HTTP_AUDITS_SQL: &str = r#"
DELETE FROM usage_http_audits
WHERE request_id = ANY($1)
  AND request_headers IS NULL
  AND response_headers IS NULL
  AND provider_request_headers IS NULL
  AND client_response_headers IS NULL
  AND request_body_ref IS NULL
  AND provider_request_body_ref IS NULL
  AND response_body_ref IS NULL
  AND client_response_body_ref IS NULL
"#;
const SELECT_USAGE_STALE_BODY_BATCH_SQL: &str = r#"
SELECT id, request_id
FROM usage
WHERE created_at < $1
  AND ($2::timestamptz IS NULL OR created_at >= $2)
  AND (
    request_body IS NOT NULL
    OR response_body IS NOT NULL
    OR provider_request_body IS NOT NULL
    OR client_response_body IS NOT NULL
    OR request_body_compressed IS NOT NULL
    OR response_body_compressed IS NOT NULL
    OR provider_request_body_compressed IS NOT NULL
    OR client_response_body_compressed IS NOT NULL
    OR EXISTS (
      SELECT 1
      FROM usage_body_blobs
      WHERE usage_body_blobs.request_id = usage.request_id
    )
    OR EXISTS (
      SELECT 1
      FROM usage_http_audits
      WHERE usage_http_audits.request_id = usage.request_id
        AND (
          usage_http_audits.request_body_ref IS NOT NULL
          OR usage_http_audits.provider_request_body_ref IS NOT NULL
          OR usage_http_audits.response_body_ref IS NOT NULL
          OR usage_http_audits.client_response_body_ref IS NOT NULL
        )
    )
  )
ORDER BY created_at ASC, id ASC
LIMIT $3
"#;
const SELECT_USAGE_COMPRESSED_BODY_BATCH_SQL: &str = r#"
SELECT id, request_id
FROM usage
WHERE created_at < $1
  AND (
    request_body_compressed IS NOT NULL
    OR response_body_compressed IS NOT NULL
    OR provider_request_body_compressed IS NOT NULL
    OR client_response_body_compressed IS NOT NULL
    OR EXISTS (
      SELECT 1
      FROM usage_body_blobs
      WHERE usage_body_blobs.request_id = usage.request_id
    )
    OR EXISTS (
      SELECT 1
      FROM usage_http_audits
      WHERE usage_http_audits.request_id = usage.request_id
        AND (
          usage_http_audits.request_body_ref IS NOT NULL
          OR usage_http_audits.provider_request_body_ref IS NOT NULL
          OR usage_http_audits.response_body_ref IS NOT NULL
          OR usage_http_audits.client_response_body_ref IS NOT NULL
        )
    )
  )
ORDER BY created_at ASC, id ASC
LIMIT $2
"#;
const CLEAR_USAGE_COMPRESSED_BODY_FIELDS_SQL: &str = r#"
UPDATE usage
SET request_body_compressed = NULL,
    response_body_compressed = NULL,
    provider_request_body_compressed = NULL,
    client_response_body_compressed = NULL
WHERE id = ANY($1)
"#;
const CLEAR_USAGE_BODY_FIELDS_SQL: &str = r#"
UPDATE usage
SET request_body = NULL,
    response_body = NULL,
    provider_request_body = NULL,
    client_response_body = NULL,
    request_body_compressed = NULL,
    response_body_compressed = NULL,
    provider_request_body_compressed = NULL,
    client_response_body_compressed = NULL
WHERE id = ANY($1)
"#;
const DELETE_USAGE_BODY_BLOBS_SQL: &str = r#"
DELETE FROM usage_body_blobs
WHERE request_id = ANY($1)
"#;
const CLEAR_USAGE_HTTP_AUDIT_BODY_REFS_SQL: &str = r#"
UPDATE usage_http_audits
SET request_body_ref = NULL,
    provider_request_body_ref = NULL,
    response_body_ref = NULL,
    client_response_body_ref = NULL,
    body_capture_mode = 'none',
    updated_at = NOW()
WHERE request_id = ANY($1)
"#;
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

#[derive(Debug, Clone, PartialEq)]
struct UsageBodyCleanupRow {
    id: String,
    request_id: String,
}

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
                cleanup_usage_body_fields(&self.pool, window.body_cutoff, batch_size).await?
            } else {
                0
            };
            return Ok(UsageCleanupSummary {
                body_cleaned,
                ..UsageCleanupSummary::default()
            });
        }

        let records_deleted = if targets.records {
            delete_old_usage_records(&self.pool, window.log_cutoff, batch_size).await?
        } else {
            0
        };
        let header_cleaned = if targets.headers {
            cleanup_usage_header_fields(
                &self.pool,
                window.header_cutoff,
                batch_size,
                targets.records.then_some(window.log_cutoff),
            )
            .await?
        } else {
            0
        };
        let body_cleaned = if targets.body {
            cleanup_usage_stale_body_fields(
                &self.pool,
                window.body_cutoff,
                batch_size,
                targets.records.then_some(window.log_cutoff),
            )
            .await?
        } else {
            0
        };
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
            count_usage_body_candidates(pool, window.body_cutoff).await?
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
        count_usage_stale_body_candidates(
            pool,
            window.body_cutoff,
            targets.records.then_some(window.log_cutoff),
        )
        .await?
    } else {
        0
    };
    let header = if targets.headers {
        count_usage_header_candidates(
            pool,
            window.header_cutoff,
            targets.records.then_some(window.log_cutoff),
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

    Ok(UsageCleanupPreviewCounts {
        body,
        header,
        log,
    })
}

async fn truncate_usage_body_blobs_table(pool: &PostgresPool) -> Result<(), DataLayerError> {
    let mut tx = pool.begin().await.map_err(postgres_error)?;
    sqlx::query("SET LOCAL lock_timeout = '2s'")
        .execute(&mut *tx)
        .await
        .map_err(postgres_error)?;
    sqlx::query("TRUNCATE TABLE usage_body_blobs")
        .execute(&mut *tx)
        .await
        .map_err(postgres_error)?;
    sqlx::query("UPDATE usage_http_audits SET request_body_ref = NULL, provider_request_body_ref = NULL, response_body_ref = NULL, client_response_body_ref = NULL, request_body_state = NULL, provider_request_body_state = NULL, response_body_state = NULL, client_response_body_state = NULL, body_capture_mode = 'none' WHERE request_body_ref IS NOT NULL OR provider_request_body_ref IS NOT NULL OR response_body_ref IS NOT NULL OR client_response_body_ref IS NOT NULL")
        .execute(&mut *tx).await.map_err(postgres_error)?;
    tx.commit().await.map_err(postgres_error)?;
    Ok(())
}

async fn cleanup_usage_body_fields(
    pool: &PostgresPool,
    cutoff_time: DateTime<Utc>,
    batch_size: usize,
) -> Result<usize, DataLayerError> {
    if let Err(err) = truncate_usage_body_blobs_table(pool).await {
        warn!(
            error = %err,
            "usage cleanup truncate usage_body_blobs table failed or timed out, falling back to batch deletion"
        );
    }
    let mut total_cleaned = 0usize;
    loop {
        let rows = fetch_usage_cleanup_rows(
            pool,
            sqlx::query(SELECT_USAGE_COMPRESSED_BODY_BATCH_SQL)
                .bind(cutoff_time)
                .bind(i64::try_from(batch_size).unwrap_or(i64::MAX)),
        )
        .await?;
        if rows.is_empty() {
            break;
        }
        let mut tx = pool.begin().await.map_err(postgres_error)?;
        let (ids, request_ids) = lock_current_cleanup_rows(&mut tx, &rows, cutoff_time, None).await?;
        if ids.is_empty() {
            tx.commit().await.map_err(postgres_error)?;
            continue;
        }
        let cleaned = sqlx::query(CLEAR_USAGE_COMPRESSED_BODY_FIELDS_SQL)
            .bind(ids)
            .execute(&mut *tx)
            .await
            .map_err(postgres_error)?
            .rows_affected();
        clear_usage_body_storage_in_tx(&mut tx, &request_ids).await?;
        tx.commit().await.map_err(postgres_error)?;
        let cleaned = usize::try_from(cleaned).unwrap_or(usize::MAX);
        total_cleaned += cleaned;
        if rows.len() < batch_size {
            break;
        }
    }
    Ok(total_cleaned)
}

async fn fetch_usage_cleanup_rows(
    pool: &PostgresPool,
    query: Query<'_, Postgres, PgArguments>,
) -> Result<Vec<UsageBodyCleanupRow>, DataLayerError> {
    let rows = query
        .fetch_all(pool)
        .await
        .map_err(postgres_error)?
        .into_iter()
        .map(|row| {
            Ok(UsageBodyCleanupRow {
                id: row.try_get::<String, _>("id").map_err(postgres_error)?,
                request_id: row
                    .try_get::<String, _>("request_id")
                    .map_err(postgres_error)?,
            })
        })
        .collect::<Result<Vec<_>, DataLayerError>>()?;
    Ok(rows)
}

async fn clear_usage_body_storage_in_tx(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    request_ids: &[String],
) -> Result<(), DataLayerError> {
    sqlx::query(DELETE_USAGE_BODY_BLOBS_SQL)
        .bind(request_ids)
        .execute(&mut **tx)
        .await
        .map_err(postgres_error)?;
    sqlx::query(CLEAR_USAGE_HTTP_AUDIT_BODY_REFS_SQL)
        .bind(request_ids)
        .execute(&mut **tx)
        .await
        .map_err(postgres_error)?;
    sqlx::query(DELETE_EMPTY_USAGE_HTTP_AUDITS_SQL)
        .bind(request_ids)
        .execute(&mut **tx)
        .await
        .map_err(postgres_error)?;
    Ok(())
}

async fn count_usage_body_candidates(
    pool: &PostgresPool,
    cutoff_time: DateTime<Utc>,
) -> Result<u64, DataLayerError> {
    let count: i64 = sqlx::query_scalar(
        r#"
SELECT COUNT(*)::bigint
FROM usage
WHERE created_at < $1
  AND (
    request_body_compressed IS NOT NULL
    OR response_body_compressed IS NOT NULL
    OR provider_request_body_compressed IS NOT NULL
    OR client_response_body_compressed IS NOT NULL
    OR EXISTS (
      SELECT 1
      FROM usage_body_blobs
      WHERE usage_body_blobs.request_id = usage.request_id
    )
    OR EXISTS (
      SELECT 1
      FROM usage_http_audits
      WHERE usage_http_audits.request_id = usage.request_id
        AND (
          usage_http_audits.request_body_ref IS NOT NULL
          OR usage_http_audits.provider_request_body_ref IS NOT NULL
          OR usage_http_audits.response_body_ref IS NOT NULL
          OR usage_http_audits.client_response_body_ref IS NOT NULL
        )
    )
  )
"#,
    )
    .bind(cutoff_time)
    .fetch_one(pool)
    .await
    .map_err(postgres_error)?;
    Ok(u64::try_from(count).unwrap_or(0))
}

async fn count_usage_stale_body_candidates(
    pool: &PostgresPool,
    cutoff_time: DateTime<Utc>,
    newer_than: Option<DateTime<Utc>>,
) -> Result<u64, DataLayerError> {
    if matches!(newer_than, Some(value) if value >= cutoff_time) {
        return Ok(0);
    }
    let count: i64 = sqlx::query_scalar(
        r#"
SELECT COUNT(*)::bigint
FROM usage
WHERE created_at < $1
  AND ($2::timestamptz IS NULL OR created_at >= $2)
  AND (
    request_body IS NOT NULL
    OR response_body IS NOT NULL
    OR provider_request_body IS NOT NULL
    OR client_response_body IS NOT NULL
    OR request_body_compressed IS NOT NULL
    OR response_body_compressed IS NOT NULL
    OR provider_request_body_compressed IS NOT NULL
    OR client_response_body_compressed IS NOT NULL
    OR EXISTS (
      SELECT 1
      FROM usage_body_blobs
      WHERE usage_body_blobs.request_id = usage.request_id
    )
    OR EXISTS (
      SELECT 1
      FROM usage_http_audits
      WHERE usage_http_audits.request_id = usage.request_id
        AND (
          usage_http_audits.request_body_ref IS NOT NULL
          OR usage_http_audits.provider_request_body_ref IS NOT NULL
          OR usage_http_audits.response_body_ref IS NOT NULL
          OR usage_http_audits.client_response_body_ref IS NOT NULL
        )
    )
  )
"#,
    )
    .bind(cutoff_time)
    .bind(newer_than)
    .fetch_one(pool)
    .await
    .map_err(postgres_error)?;
    Ok(u64::try_from(count).unwrap_or(0))
}

async fn count_usage_header_candidates(
    pool: &PostgresPool,
    cutoff_time: DateTime<Utc>,
    newer_than: Option<DateTime<Utc>>,
) -> Result<u64, DataLayerError> {
    if matches!(newer_than, Some(value) if value >= cutoff_time) {
        return Ok(0);
    }
    let count: i64 = sqlx::query_scalar(
        r#"
SELECT COUNT(*)::bigint
FROM usage
WHERE created_at < $1
  AND ($2::timestamptz IS NULL OR created_at >= $2)
  AND (
    request_headers IS NOT NULL
    OR response_headers IS NOT NULL
    OR provider_request_headers IS NOT NULL
    OR client_response_headers IS NOT NULL
    OR EXISTS (
      SELECT 1
      FROM usage_http_audits
      WHERE usage_http_audits.request_id = usage.request_id
        AND (
          usage_http_audits.request_headers IS NOT NULL
          OR usage_http_audits.response_headers IS NOT NULL
          OR usage_http_audits.provider_request_headers IS NOT NULL
          OR usage_http_audits.client_response_headers IS NOT NULL
        )
    )
  )
"#,
    )
    .bind(cutoff_time)
    .bind(newer_than)
    .fetch_one(pool)
    .await
    .map_err(postgres_error)?;
    Ok(u64::try_from(count).unwrap_or(0))
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

async fn lock_current_cleanup_rows(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    rows: &[UsageBodyCleanupRow],
    cutoff: DateTime<Utc>,
    lower_bound: Option<DateTime<Utc>>,
) -> Result<(Vec<String>, Vec<String>), DataLayerError> {
    let ids = rows.iter().map(|row| row.id.clone()).collect::<Vec<_>>();
    let request_ids = rows.iter().map(|row| row.request_id.clone()).collect::<Vec<_>>();
    super::lock_usage_request_ids_in_tx(tx, &request_ids).await?;
    let expected = rows.iter().map(|row| (row.id.as_str(), row.request_id.as_str()))
        .collect::<std::collections::BTreeSet<_>>();
    let current = sqlx::query(
        "SELECT id, request_id FROM usage WHERE id = ANY($1) AND created_at < $2 AND ($3::timestamptz IS NULL OR created_at >= $3) ORDER BY id FOR UPDATE"
    ).bind(ids).bind(cutoff).bind(lower_bound).fetch_all(&mut **tx).await.map_err(postgres_error)?;
    let mut ids = Vec::new();
    let mut request_ids = Vec::new();
    for row in current {
        let id: String = row.try_get("id").map_err(postgres_error)?;
        let request_id: String = row.try_get("request_id").map_err(postgres_error)?;
        if expected.contains(&(id.as_str(), request_id.as_str())) {
            ids.push(id);
            request_ids.push(request_id);
        }
    }
    Ok((ids, request_ids))
}

async fn cleanup_usage_header_fields(
    pool: &PostgresPool,
    cutoff_time: DateTime<Utc>,
    batch_size: usize,
    newer_than: Option<DateTime<Utc>>,
) -> Result<usize, DataLayerError> {
    if matches!(newer_than, Some(value) if value >= cutoff_time) {
        warn!(
            cutoff_time = %cutoff_time,
            newer_than = ?newer_than,
            "usage cleanup header sweep skipped due to invalid window"
        );
        return Ok(0);
    }

    let mut total_cleaned = 0usize;
    loop {
        let rows = fetch_usage_cleanup_rows(
            pool,
            sqlx::query(SELECT_USAGE_HEADER_BATCH_SQL)
                .bind(cutoff_time)
                .bind(newer_than)
                .bind(i64::try_from(batch_size).unwrap_or(i64::MAX)),
        )
        .await?;
        if rows.is_empty() {
            break;
        }
        let mut tx = pool.begin().await.map_err(postgres_error)?;
        let (ids, request_ids) = lock_current_cleanup_rows(&mut tx, &rows, cutoff_time, newer_than).await?;
        if ids.is_empty() {
            tx.commit().await.map_err(postgres_error)?;
            continue;
        }
        let cleaned = sqlx::query(CLEAR_USAGE_HEADER_FIELDS_SQL)
            .bind(ids)
            .execute(&mut *tx)
            .await
            .map_err(postgres_error)?
            .rows_affected();
        sqlx::query(CLEAR_USAGE_HTTP_AUDIT_HEADERS_SQL)
            .bind(&request_ids)
            .execute(&mut *tx)
            .await
            .map_err(postgres_error)?;
        sqlx::query(DELETE_EMPTY_USAGE_HTTP_AUDITS_SQL)
            .bind(request_ids)
            .execute(&mut *tx)
            .await
            .map_err(postgres_error)?;
        tx.commit().await.map_err(postgres_error)?;
        let cleaned = usize::try_from(cleaned).unwrap_or(usize::MAX);
        total_cleaned += cleaned;
        if rows.len() < batch_size {
            break;
        }
    }
    Ok(total_cleaned)
}

async fn cleanup_usage_stale_body_fields(
    pool: &PostgresPool,
    cutoff_time: DateTime<Utc>,
    batch_size: usize,
    newer_than: Option<DateTime<Utc>>,
) -> Result<usize, DataLayerError> {
    if matches!(newer_than, Some(value) if value >= cutoff_time) {
        warn!(
            cutoff_time = %cutoff_time,
            newer_than = ?newer_than,
            "usage cleanup body sweep skipped due to invalid window"
        );
        return Ok(0);
    }

    let mut total_cleaned = 0usize;
    loop {
        let rows = fetch_usage_cleanup_rows(
            pool,
            sqlx::query(SELECT_USAGE_STALE_BODY_BATCH_SQL)
                .bind(cutoff_time)
                .bind(newer_than)
                .bind(i64::try_from(batch_size).unwrap_or(i64::MAX)),
        )
        .await?;
        if rows.is_empty() {
            break;
        }
        let mut tx = pool.begin().await.map_err(postgres_error)?;
        let (ids, request_ids) = lock_current_cleanup_rows(&mut tx, &rows, cutoff_time, newer_than).await?;
        if ids.is_empty() {
            tx.commit().await.map_err(postgres_error)?;
            continue;
        }
        let cleaned = sqlx::query(CLEAR_USAGE_BODY_FIELDS_SQL)
            .bind(ids)
            .execute(&mut *tx)
            .await
            .map_err(postgres_error)?
            .rows_affected();
        clear_usage_body_storage_in_tx(&mut tx, &request_ids).await?;
        tx.commit().await.map_err(postgres_error)?;
        let cleaned = usize::try_from(cleaned).unwrap_or(usize::MAX);
        total_cleaned += cleaned;
        if rows.len() < batch_size {
            break;
        }
    }
    Ok(total_cleaned)
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

#[cfg(test)]
mod tests {
    use super::UsageCleanupTargets;

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
        let sql = super::SELECT_USAGE_STALE_BODY_BATCH_SQL;
        assert!(sql.contains("usage_body_blobs"));
        assert!(sql.contains("usage_http_audits"));
        assert!(sql.contains("request_body_compressed"));
    }
}
