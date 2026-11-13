//! HTTP capture storage: `usage_http_headers` and `usage_http_bodies`.
//!
//! Both tables hold one row per request and are append-only. Retention is a
//! plain `DELETE ... WHERE created_at < cutoff` on their own timestamp, so a
//! swept row leaves the predicate behind instead of coming back on the next
//! pass. Nothing here ever clears a column to retire data.
//!
//! A row holds the exchange the accounting row describes: the client pair and
//! the upstream pair. The retry trail is deliberately absent -- which candidates
//! were tried, in what order and how each one ended is what
//! `request_candidates` records, and it already carries each attempt's status,
//! error and latency. Storing the payloads of abandoned attempts here would
//! multiply the body bytes for data no view has ever shown.
//!
//! Only the write that carries a payload creates a row: `EXCLUDED` wins where
//! it has a value and the stored one survives otherwise, so a lifecycle write
//! that knows just one side cannot blank the other -- except where that same
//! write marks a side as withdrawn, which clears the stored bytes in the same
//! statement. A write that only withdraws bodies edits the existing row
//! instead, because creating an all-NULL row here
//! would keep it in the table for its whole retention window.

use aether_data_contracts::DataLayerError;
use sqlx::{Postgres, QueryBuilder};

use crate::error::postgres_error;

/// `created_at` is taken from the request row rather than from the clock: the
/// capture tables are swept by that value, so it has to be the request's own
/// time and not the time the capture happened to be written.
const UPSERT_HEADERS_SQL: &str = r#"
INSERT INTO usage_http_headers (
  request_id,
  client_request,
  client_response,
  provider_request,
  provider_response,
  created_at
)
VALUES (
  $1,
  $2::jsonb,
  $3::jsonb,
  $4::jsonb,
  $5::jsonb,
  COALESCE((SELECT created_at FROM usage WHERE request_id = $1), NOW())
)
ON CONFLICT (request_id) DO UPDATE SET
  client_request = COALESCE(EXCLUDED.client_request, usage_http_headers.client_request),
  client_response = COALESCE(EXCLUDED.client_response, usage_http_headers.client_response),
  provider_request = COALESCE(EXCLUDED.provider_request, usage_http_headers.provider_request),
  provider_response = COALESCE(EXCLUDED.provider_response, usage_http_headers.provider_response)
"#;

/// The payload and its encoding are written in the same statement: the CHECK
/// constraint refuses a row where only one of the pair arrived, so keeping them
/// together here is what makes the constraint a bug detector rather than a
/// runtime surprise.
///
/// `$14`..`$17` are the write's withdrawals. They only matter in the `DO
/// UPDATE` arm, where a side marked as withdrawn must not let `COALESCE`
/// resurrect the bytes an earlier write had stored -- the same statement can
/// carry a payload for one side and a withdrawal for another, and both have to
/// land together. A withdrawn side also drops its truncation flag: the flag
/// only describes stored bytes, and the pair constraint refuses a `true` next
/// to a `NULL` payload.
const UPSERT_BODIES_SQL: &str = r#"
INSERT INTO usage_http_bodies (
  request_id,
  client_request,
  client_request_encoding,
  client_request_truncated,
  client_response,
  client_response_encoding,
  client_response_truncated,
  provider_request,
  provider_request_encoding,
  provider_request_truncated,
  provider_response,
  provider_response_encoding,
  provider_response_truncated,
  created_at
)
VALUES (
  $1,
  $2::bytea,
  CASE WHEN $14 THEN NULL ELSE $3::text END,
  CASE WHEN $14 THEN false ELSE $4 END,
  $5::bytea,
  CASE WHEN $15 THEN NULL ELSE $6::text END,
  CASE WHEN $15 THEN false ELSE $7 END,
  $8::bytea,
  CASE WHEN $16 THEN NULL ELSE $9::text END,
  CASE WHEN $16 THEN false ELSE $10 END,
  $11::bytea,
  CASE WHEN $17 THEN NULL ELSE $12::text END,
  CASE WHEN $17 THEN false ELSE $13 END,
  COALESCE((SELECT created_at FROM usage WHERE request_id = $1), NOW())
)
ON CONFLICT (request_id) DO UPDATE SET
  client_request = CASE WHEN $14 THEN NULL
    ELSE COALESCE(EXCLUDED.client_request, usage_http_bodies.client_request) END,
  client_request_encoding = CASE WHEN $14 THEN NULL
    ELSE COALESCE(EXCLUDED.client_request_encoding, usage_http_bodies.client_request_encoding) END,
  client_request_truncated = CASE
    WHEN $14 THEN false
    WHEN EXCLUDED.client_request IS NOT NULL THEN EXCLUDED.client_request_truncated
    ELSE usage_http_bodies.client_request_truncated
  END,
  client_response = CASE WHEN $15 THEN NULL
    ELSE COALESCE(EXCLUDED.client_response, usage_http_bodies.client_response) END,
  client_response_encoding = CASE WHEN $15 THEN NULL
    ELSE COALESCE(EXCLUDED.client_response_encoding, usage_http_bodies.client_response_encoding) END,
  client_response_truncated = CASE
    WHEN $15 THEN false
    WHEN EXCLUDED.client_response IS NOT NULL THEN EXCLUDED.client_response_truncated
    ELSE usage_http_bodies.client_response_truncated
  END,
  provider_request = CASE WHEN $16 THEN NULL
    ELSE COALESCE(EXCLUDED.provider_request, usage_http_bodies.provider_request) END,
  provider_request_encoding = CASE WHEN $16 THEN NULL
    ELSE COALESCE(EXCLUDED.provider_request_encoding, usage_http_bodies.provider_request_encoding) END,
  provider_request_truncated = CASE
    WHEN $16 THEN false
    WHEN EXCLUDED.provider_request IS NOT NULL THEN EXCLUDED.provider_request_truncated
    ELSE usage_http_bodies.provider_request_truncated
  END,
  provider_response = CASE WHEN $17 THEN NULL
    ELSE COALESCE(EXCLUDED.provider_response, usage_http_bodies.provider_response) END,
  provider_response_encoding = CASE WHEN $17 THEN NULL
    ELSE COALESCE(EXCLUDED.provider_response_encoding, usage_http_bodies.provider_response_encoding) END,
  provider_response_truncated = CASE
    WHEN $17 THEN false
    WHEN EXCLUDED.provider_response IS NOT NULL THEN EXCLUDED.provider_response_truncated
    ELSE usage_http_bodies.provider_response_truncated
  END
"#;

/// Withdrawing a side is a write of its own.
///
/// `UPSERT_BODIES_SQL` only carries payloads, so a lifecycle write that decides a
/// side is no longer stored would otherwise leave the earlier bytes in place for
/// the rest of the retention window. The columns are cleared in pairs because the
/// CHECK constraint refuses a body without its encoding.
const WITHDRAW_BODIES_SQL: &str = r#"
UPDATE usage_http_bodies SET
  client_request = CASE WHEN $2 THEN NULL ELSE client_request END,
  client_request_encoding = CASE WHEN $2 THEN NULL ELSE client_request_encoding END,
  client_request_truncated = CASE WHEN $2 THEN false ELSE client_request_truncated END,
  client_response = CASE WHEN $3 THEN NULL ELSE client_response END,
  client_response_encoding = CASE WHEN $3 THEN NULL ELSE client_response_encoding END,
  client_response_truncated = CASE WHEN $3 THEN false ELSE client_response_truncated END,
  provider_request = CASE WHEN $4 THEN NULL ELSE provider_request END,
  provider_request_encoding = CASE WHEN $4 THEN NULL ELSE provider_request_encoding END,
  provider_request_truncated = CASE WHEN $4 THEN false ELSE provider_request_truncated END,
  provider_response = CASE WHEN $5 THEN NULL ELSE provider_response END,
  provider_response_encoding = CASE WHEN $5 THEN NULL ELSE provider_response_encoding END,
  provider_response_truncated = CASE WHEN $5 THEN false ELSE provider_response_truncated END
WHERE request_id = $1
"#;

/// One captured body side, exactly as it will be stored: the bytes plus how to
/// read them, and whether this write withdraws the side instead of capturing.
/// A withdrawn side carries no bytes but still has to clear whatever an earlier
/// write had stored, which is why the flag travels separately from `bytes`.
#[derive(Debug, Default)]
pub(super) struct PreparedUsageBodyStorage {
    pub bytes: Option<Vec<u8>>,
    pub encoding: Option<String>,
    pub truncated: bool,
    pub withdrawn: bool,
    /// The bytes live in the body file store; the DB columns for this side must
    /// be cleared so a stale row cannot shadow the file.
    pub file: bool,
}

/// Everything one usage write contributes to the capture tables.
///
/// Headers arrive as the serialized JSON text the persistence layer prepared,
/// and every body side arrives as its own [`PreparedUsageBodyStorage`]. The
/// sides follow the accounting record's four directions in storage order:
/// `[client_request, client_response, provider_request, provider_response]`.
#[derive(Debug)]
pub(super) struct UsageHttpCapture<'a> {
    pub client_request_headers: Option<&'a str>,
    pub client_response_headers: Option<&'a str>,
    pub provider_request_headers: Option<&'a str>,
    pub provider_response_headers: Option<&'a str>,
    pub bodies: [&'a PreparedUsageBodyStorage; 4],
    /// The request's first write time, in whole seconds. The field is named
    /// `created_at_unix_ms` on the accounting record even though it holds
    /// seconds, so the capture carries the unit explicitly.
    pub created_at_unix_secs: Option<u64>,
    /// The request's last write time, in whole seconds. A body file is written
    /// under the day of the event that produced it, and the last event is the
    /// one that rewrites every side, so this is the day the authoritative copy
    /// lives in.
    pub updated_at_unix_secs: Option<u64>,
}

impl UsageHttpCapture<'_> {
    fn has_header_payload(&self) -> bool {
        self.client_request_headers.is_some()
            || self.client_response_headers.is_some()
            || self.provider_request_headers.is_some()
            || self.provider_response_headers.is_some()
    }

    /// Only captured bytes create the row: a side that is absent or explicitly
    /// skipped says so in `request_metadata`, and the bodies row simply has no
    /// value for it.
    fn has_body_payload(&self) -> bool {
        self.bodies.iter().any(|side| side.bytes.is_some())
    }

    /// The sides this write withdraws instead of capturing, in
    /// `[client_request, client_response, provider_request, provider_response]`
    /// order.
    ///
    /// The withdrawal decision rides with the side itself: a side marked as
    /// withdrawn must not let `COALESCE` resurrect the bytes an earlier write
    /// had stored, and the same statement can carry a payload for one side and
    /// a withdrawal for another, so both have to land together.
    fn withdrawals(&self) -> [bool; 4] {
        std::array::from_fn(|index| self.bodies[index].withdrawn || self.bodies[index].file)
    }
}

/// Whether this write makes the side's stored file unreachable.
///
/// A side that carries bytes is moving back to the database, and an explicitly
/// withdrawn side is no longer captured at all; both must drop the file. A
/// `None` side is silent, not a withdrawal: the ledger may still route reads to
/// the file an earlier write stored, so it keeps it.
pub(super) fn should_delete_body_file(side: &PreparedUsageBodyStorage) -> bool {
    !side.file && (side.withdrawn || side.bytes.is_some())
}

/// The date directories a stale body file can live in.
///
/// A request that crosses UTC midnight has one copy in the day of its first
/// write and another in the day of its last write, so both are candidates.
pub(super) fn body_file_days(
    created_at_unix_secs: Option<u64>,
    updated_at_unix_secs: Option<u64>,
) -> Vec<chrono::NaiveDate> {
    let mut days = Vec::with_capacity(2);
    for secs in [created_at_unix_secs, updated_at_unix_secs]
        .into_iter()
        .flatten()
    {
        if let Some(day) = aether_body_store::day_from_unix_secs(secs) {
            if !days.contains(&day) {
                days.push(day);
            }
        }
    }
    days
}

pub(super) async fn sync_usage_http_capture_in_tx(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    request_id: &str,
    capture: UsageHttpCapture<'_>,
) -> Result<(), DataLayerError> {
    if capture.has_header_payload() {
        sqlx::query(UPSERT_HEADERS_SQL)
            .bind(request_id)
            .bind(capture.client_request_headers)
            .bind(capture.client_response_headers)
            .bind(capture.provider_request_headers)
            .bind(capture.provider_response_headers)
            .execute(&mut **tx)
            .await
            .map_err(postgres_error)?;
    }

    // A side that is no longer file-backed must not leave a stale file behind:
    // the read path routes by the ledger, but the old bytes would still occupy
    // space until retention. File-backed sides keep the file just written, and
    // silent sides keep whatever an earlier write stored.
    if let Some(store) = aether_body_store::global() {
        let days = body_file_days(capture.created_at_unix_secs, capture.updated_at_unix_secs);
        for (index, side) in [
            aether_data_contracts::repository::usage::UsageBodyField::RequestBody,
            aether_data_contracts::repository::usage::UsageBodyField::ClientResponseBody,
            aether_data_contracts::repository::usage::UsageBodyField::ProviderRequestBody,
            aether_data_contracts::repository::usage::UsageBodyField::ResponseBody,
        ]
        .into_iter()
        .enumerate()
        {
            if !should_delete_body_file(capture.bodies[index]) {
                continue;
            }
            for day in &days {
                if let Err(error) = store.delete_side(*day, request_id, side).await {
                    tracing::warn!(
                        request_id,
                        side = side.file_stem(),
                        error = %error,
                        "usage body file delete failed"
                    );
                }
            }
        }
    }

    let withdrawals = capture.withdrawals();
    if !capture.has_body_payload() {
        if withdrawals.iter().any(|withdrawn| *withdrawn) {
            sqlx::query(WITHDRAW_BODIES_SQL)
                .bind(request_id)
                .bind(withdrawals[0])
                .bind(withdrawals[1])
                .bind(withdrawals[2])
                .bind(withdrawals[3])
                .execute(&mut **tx)
                .await
                .map_err(postgres_error)?;
        }
        return Ok(());
    }

    let [client_request, client_response, provider_request, provider_response] = capture.bodies;
    sqlx::query(UPSERT_BODIES_SQL)
        .bind(request_id)
        .bind(client_request.bytes.as_deref())
        .bind(client_request.encoding.as_deref())
        .bind(client_request.truncated)
        .bind(client_response.bytes.as_deref())
        .bind(client_response.encoding.as_deref())
        .bind(client_response.truncated)
        .bind(provider_request.bytes.as_deref())
        .bind(provider_request.encoding.as_deref())
        .bind(provider_request.truncated)
        .bind(provider_response.bytes.as_deref())
        .bind(provider_response.encoding.as_deref())
        .bind(provider_response.truncated)
        .bind(withdrawals[0])
        .bind(withdrawals[1])
        .bind(withdrawals[2])
        .bind(withdrawals[3])
        .execute(&mut **tx)
        .await
        .map_err(postgres_error)?;

    Ok(())
}

/// The client-side capture a freshly inserted pending row already knows.
///
/// Headers stay as their serialized JSON text: the pending path already holds
/// them that way, and the statement binds them straight into `CAST(... AS
/// jsonb)`.
#[derive(Debug, Default)]
pub(super) struct UsageHttpCaptureRow<'a> {
    pub client_request_headers: Option<&'a str>,
    pub client_response_headers: Option<&'a str>,
    pub client_request_body: Option<&'a [u8]>,
    pub client_request_body_encoding: Option<&'a str>,
    pub client_request_body_truncated: bool,
    pub client_response_body: Option<&'a [u8]>,
    pub client_response_body_encoding: Option<&'a str>,
    pub client_response_body_truncated: bool,
}

const INSERT_HEADERS_BATCH_HEAD_SQL: &str =
    "WITH incoming (request_id, client_request, client_response) AS (";

const INSERT_HEADERS_BATCH_TAIL_SQL: &str = r#"
)
INSERT INTO usage_http_headers (request_id, client_request, client_response, created_at)
SELECT incoming.request_id,
       incoming.client_request,
       incoming.client_response,
       COALESCE("usage".created_at, NOW())
FROM incoming
LEFT JOIN "usage" ON "usage".request_id = incoming.request_id
ON CONFLICT (request_id) DO UPDATE SET
  client_request = COALESCE(EXCLUDED.client_request, usage_http_headers.client_request),
  client_response = COALESCE(EXCLUDED.client_response, usage_http_headers.client_response)
"#;

const INSERT_BODIES_BATCH_HEAD_SQL: &str = "WITH incoming (request_id, client_request, \
     client_request_encoding, client_request_truncated, client_response, \
     client_response_encoding, client_response_truncated) AS (";

const INSERT_BODIES_BATCH_TAIL_SQL: &str = r#"
)
INSERT INTO usage_http_bodies (
  request_id,
  client_request,
  client_request_encoding,
  client_request_truncated,
  client_response,
  client_response_encoding,
  client_response_truncated,
  created_at
)
SELECT incoming.request_id,
       incoming.client_request,
       incoming.client_request_encoding,
       incoming.client_request_truncated,
       incoming.client_response,
       incoming.client_response_encoding,
       incoming.client_response_truncated,
       COALESCE("usage".created_at, NOW())
FROM incoming
LEFT JOIN "usage" ON "usage".request_id = incoming.request_id
ON CONFLICT (request_id) DO UPDATE SET
  client_request = COALESCE(EXCLUDED.client_request, usage_http_bodies.client_request),
  client_request_encoding = COALESCE(
    EXCLUDED.client_request_encoding,
    usage_http_bodies.client_request_encoding
  ),
  client_request_truncated = CASE
    WHEN EXCLUDED.client_request IS NOT NULL THEN EXCLUDED.client_request_truncated
    ELSE usage_http_bodies.client_request_truncated
  END,
  client_response = COALESCE(EXCLUDED.client_response, usage_http_bodies.client_response),
  client_response_encoding = COALESCE(
    EXCLUDED.client_response_encoding,
    usage_http_bodies.client_response_encoding
  ),
  client_response_truncated = CASE
    WHEN EXCLUDED.client_response IS NOT NULL THEN EXCLUDED.client_response_truncated
    ELSE usage_http_bodies.client_response_truncated
  END
"#;

/// Writes the client-side capture for a batch of freshly inserted requests.
///
/// The pending fast path never carries an upstream exchange, so it only needs the
/// two client columns and can stay one statement over the whole batch instead of
/// one upsert per request.
pub(super) async fn insert_pending_usage_http_capture_batch(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    rows: &[(&str, UsageHttpCaptureRow<'_>)],
) -> Result<(), DataLayerError> {
    let headers = rows
        .iter()
        .filter(|(_, row)| {
            row.client_request_headers.is_some() || row.client_response_headers.is_some()
        })
        .map(|(request_id, row)| {
            (
                *request_id,
                row.client_request_headers.map(str::to_string),
                row.client_response_headers.map(str::to_string),
            )
        })
        .collect::<Vec<_>>();
    if !headers.is_empty() {
        let mut builder = QueryBuilder::<Postgres>::new(INSERT_HEADERS_BATCH_HEAD_SQL);
        builder.push_values(&headers, |mut values, (request_id, request, response)| {
            values.push_bind(*request_id);
            values.push("CAST(");
            values.push_bind_unseparated(request.clone());
            values.push_unseparated(" AS jsonb), CAST(");
            values.push_bind_unseparated(response.clone());
            values.push_unseparated(" AS jsonb)");
        });
        builder.push(INSERT_HEADERS_BATCH_TAIL_SQL);
        builder
            .build()
            .execute(&mut **tx)
            .await
            .map_err(postgres_error)?;
    }

    let bodies = rows
        .iter()
        .filter(|(_, row)| row.client_request_body.is_some() || row.client_response_body.is_some())
        .map(|(request_id, row)| {
            (
                *request_id,
                row.client_request_body.map(<[u8]>::to_vec),
                row.client_request_body
                    .as_ref()
                    .and(row.client_request_body_encoding)
                    .map(str::to_string),
                row.client_request_body_truncated,
                row.client_response_body.map(<[u8]>::to_vec),
                row.client_response_body
                    .as_ref()
                    .and(row.client_response_body_encoding)
                    .map(str::to_string),
                row.client_response_body_truncated,
            )
        })
        .collect::<Vec<_>>();
    if !bodies.is_empty() {
        let mut builder = QueryBuilder::<Postgres>::new(INSERT_BODIES_BATCH_HEAD_SQL);
        builder.push_values(
            &bodies,
            |mut values,
             (
                request_id,
                request,
                request_encoding,
                request_truncated,
                response,
                response_encoding,
                response_truncated,
            )| {
                values
                    .push_bind(*request_id)
                    .push_bind(request.clone())
                    .push_bind(request_encoding.clone())
                    .push_bind(*request_truncated)
                    .push_bind(response.clone())
                    .push_bind(response_encoding.clone())
                    .push_bind(*response_truncated);
            },
        );
        builder.push(INSERT_BODIES_BATCH_TAIL_SQL);
        builder
            .build()
            .execute(&mut **tx)
            .await
            .map_err(postgres_error)?;
    }

    Ok(())
}
