use aether_data_contracts::repository::usage::{
    parse_usage_body_ref, usage_body_ref, StoredUsageBodyPayload, UsageBodyField,
};
use serde_json::Value;
use sqlx::{postgres::PgRow, Postgres, Row};

use super::{
    capture_codec::UsageBodyStorageBundle, lock_usage_request_ids_in_tx,
    preparation::prepare_usage_in_background, SqlxUsageReadRepository, UsageBodyStorage,
    UPSERT_USAGE_BODY_BLOB_SQL,
};
use crate::{error::SqlxResultExt, DataLayerError};

pub use super::capture_codec::encode_usage_body_delta;

const CAPTURE_ROWS_SQL: &str = r#"
SELECT b.body_ref, b.request_id, b.body_field, b.encoding, b.payload,
       base.encoding AS base_encoding, base.payload AS base_payload
FROM usage_body_blobs b
LEFT JOIN usage_body_blobs base ON b.encoding = 1 AND base.request_id = b.request_id
AND base.body_field = CASE b.body_field WHEN 'provider_request_body' THEN 'request_body' WHEN 'client_response_body' THEN 'response_body' END
WHERE b.request_id = ANY($1)
"#;

const FIND_USAGE_BODY_BLOB_BY_REF_SQL: &str = r#"
SELECT b.payload, b.encoding, b.body_field, base.payload AS base_payload, base.encoding AS base_encoding
FROM usage_body_blobs b
LEFT JOIN usage_body_blobs base ON b.encoding = 1 AND base.request_id = b.request_id
AND base.body_field = CASE b.body_field WHEN 'provider_request_body' THEN 'request_body' WHEN 'client_response_body' THEN 'response_body' END
WHERE b.body_ref = $1 AND b.request_id = $2 AND b.body_field = $3 LIMIT 1
"#;

static USAGE_BODY_DECODE_SLOTS: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(4);

fn acquire_usage_body_slot(
    slots: &'static tokio::sync::Semaphore,
) -> Result<tokio::sync::SemaphorePermit<'static>, DataLayerError> {
    slots
        .try_acquire()
        .map_err(|_| DataLayerError::TimedOut("usage body read capacity exhausted".to_string()))
}

async fn decode_usage_body_with_permit<T: Send + 'static>(
    permit: tokio::sync::SemaphorePermit<'static>,
    decode: impl FnOnce() -> Result<T, DataLayerError> + Send + 'static,
) -> Result<T, DataLayerError> {
    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        decode()
    })
    .await
    .map_err(|error| {
        DataLayerError::UnexpectedValue(format!("usage body decoder failed: {error}"))
    })?
}

impl SqlxUsageReadRepository {
    pub async fn read_body_payload(
        &self,
        body_ref: &str,
    ) -> Result<Option<StoredUsageBodyPayload>, DataLayerError> {
        let permit = acquire_usage_body_slot(&USAGE_BODY_DECODE_SLOTS)?;
        let Some(bundle) = self.read_body_storage_bundle(body_ref).await? else {
            return Ok(None);
        };
        if bundle.encoding == 0 {
            return bundle.restore().map(StoredUsageBodyPayload).map(Some);
        }
        decode_usage_body_with_permit(permit, move || {
            bundle.restore().map(StoredUsageBodyPayload).map(Some)
        })
        .await
    }

    pub(super) async fn read_body_storage_bundle(
        &self,
        body_ref: &str,
    ) -> Result<Option<UsageBodyStorageBundle>, DataLayerError> {
        let Some((request_id, field)) = parse_usage_body_ref(body_ref) else {
            return Ok(None);
        };
        if let Some(bundle) = find_body_bundle(&self.pool, &request_id, field).await? {
            return Ok(Some(bundle));
        }
        let inline_column = field.as_storage_field();
        let row = sqlx::query(&format!(
            "SELECT {inline_column}::text AS inline_body FROM \"usage\" WHERE request_id = $1 LIMIT 1"
        ))
        .bind(request_id)
        .fetch_optional(&self.pool)
        .await
        .map_postgres_err()?;
        let Some(row) = row else {
            return Ok(None);
        };
        Ok(row
            .try_get::<Option<String>, _>("inline_body")
            .map_postgres_err()?
            .map(|body| UsageBodyStorageBundle::raw(body.into_bytes())))
    }

    pub async fn resolve_body_ref(&self, body_ref: &str) -> Result<Option<Value>, DataLayerError> {
        let permit = acquire_usage_body_slot(&USAGE_BODY_DECODE_SLOTS)?;
        let Some(bundle) = self.read_body_storage_bundle(body_ref).await? else {
            return Ok(None);
        };
        decode_usage_body_with_permit(permit, move || {
            let bytes = bundle.restore()?;
            serde_json::from_slice(&bytes).map(Some).map_err(|error| {
                DataLayerError::UnexpectedValue(format!("failed to parse usage json: {error}"))
            })
        })
        .await
    }
}

async fn find_body_bundle<'e>(
    executor: impl sqlx::Executor<'e, Database = Postgres>,
    request_id: &str,
    field: UsageBodyField,
) -> Result<Option<UsageBodyStorageBundle>, DataLayerError> {
    let body_ref = usage_body_ref(request_id, field);
    let row = sqlx::query(FIND_USAGE_BODY_BLOB_BY_REF_SQL)
        .bind(&body_ref)
        .bind(request_id)
        .bind(field.as_storage_field())
        .fetch_optional(executor)
        .await
        .map_postgres_err()?;
    row.as_ref().map(bundle_from_row).transpose()
}

pub async fn prepare_usage_capture_import(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    request_ids: &[String],
) -> Result<(), DataLayerError> {
    lock_usage_request_ids_in_tx(tx, request_ids).await?;
    let rows = sqlx::query(CAPTURE_ROWS_SQL)
        .bind(request_ids)
        .fetch_all(&mut **tx)
        .await
        .map_postgres_err()?;
    for row in rows {
        let bundle = bundle_from_row(&row)?;
        if bundle.encoding == 0 {
            continue;
        }
        let payload = prepare_usage_in_background(move || bundle.restore()).await?;
        let body_ref: String = row.try_get("body_ref").map_postgres_err()?;
        sqlx::query("UPDATE usage_body_blobs SET payload = $2, encoding = 0 WHERE body_ref = $1")
            .bind(body_ref)
            .bind(payload)
            .execute(&mut **tx)
            .await
            .map_postgres_err()?;
    }
    Ok(())
}

pub async fn validate_usage_capture_import(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    request_ids: &[String],
) -> Result<(), DataLayerError> {
    let rows = sqlx::query(CAPTURE_ROWS_SQL)
        .bind(request_ids)
        .fetch_all(&mut **tx)
        .await
        .map_postgres_err()?;
    for row in rows {
        let field: String = row.try_get("body_field").map_postgres_err()?;
        let request_id: String = row.try_get("request_id").map_postgres_err()?;
        let reference: String = row.try_get("body_ref").map_postgres_err()?;
        if UsageBodyField::from_storage_field(&field)
            .is_none_or(|field| reference != usage_body_ref(&request_id, field))
        {
            return Err(DataLayerError::InvalidInput(
                "invalid imported usage body reference".to_string(),
            ));
        }
        let bundle = bundle_from_row(&row)?;
        if bundle.encoding != 0 {
            prepare_usage_in_background(move || {
                let bytes = bundle.restore()?;
                serde_json::from_slice::<Value>(&bytes).map_err(|_| {
                    DataLayerError::InvalidInput(
                        "imported usage delta does not reconstruct JSON".to_string(),
                    )
                })?;
                Ok(())
            })
            .await?;
        }
    }
    Ok(())
}

pub(super) fn prepare_body_delta(base: &UsageBodyStorage, target: &mut UsageBodyStorage) {
    if let (Some(base), Some(payload)) = (&base.detached_blob_bytes, &target.detached_blob_bytes) {
        if let Some(delta) = encode_usage_body_delta(base, payload) {
            target.encoding = 1;
            target.detached_blob_bytes = Some(delta);
        }
    }
}

fn bundle_from_row(row: &PgRow) -> Result<UsageBodyStorageBundle, DataLayerError> {
    let encoding: i32 = row.try_get("encoding").map_postgres_err()?;
    let payload: Vec<u8> = row.try_get("payload").map_postgres_err()?;
    let base = match encoding {
        0 => None,
        1 => {
            let field: String = row.try_get("body_field").map_postgres_err()?;
            if !matches!(
                field.as_str(),
                "provider_request_body" | "client_response_body"
            ) {
                return Err(DataLayerError::UnexpectedValue(
                    "invalid usage capture base".to_string(),
                ));
            }
            match row
                .try_get::<Option<i32>, _>("base_encoding")
                .map_postgres_err()?
            {
                None => {
                    return Err(DataLayerError::UnexpectedValue(
                        "missing usage capture base".to_string(),
                    ))
                }
                Some(0) => {}
                Some(_) => {
                    return Err(DataLayerError::UnexpectedValue(
                        "invalid usage capture base".to_string(),
                    ))
                }
            }
            Some(
                row.try_get::<Option<Vec<u8>>, _>("base_payload")
                    .map_postgres_err()?
                    .ok_or_else(|| {
                        DataLayerError::UnexpectedValue("missing usage capture base".to_string())
                    })?,
            )
        }
        _ => {
            return Err(DataLayerError::UnexpectedValue(
                "unsupported usage capture encoding".to_string(),
            ))
        }
    };
    Ok(UsageBodyStorageBundle {
        encoding,
        payload,
        base,
    })
}

pub(super) struct CapturePairUpdate<'a> {
    pub base: &'a UsageBodyStorage,
    pub target: &'a UsageBodyStorage,
    pub clear_base: bool,
    pub clear_target: bool,
    pub field: UsageBodyField,
}

pub(super) async fn preserve_derived_bodies(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    request_id: &str,
    pairs: [CapturePairUpdate<'_>; 2],
) -> Result<(), DataLayerError> {
    for CapturePairUpdate {
        base,
        target,
        clear_base,
        clear_target,
        field,
    } in pairs
    {
        if (!base.has_detached_blob() && !clear_base) || target.has_detached_blob() || clear_target
        {
            continue;
        }
        let Some(bundle) = find_body_bundle(&mut **tx, request_id, field).await? else {
            continue;
        };
        if bundle.encoding == 0 {
            continue;
        }
        let payload = prepare_usage_in_background(move || bundle.restore()).await?;
        sqlx::query(UPSERT_USAGE_BODY_BLOB_SQL)
            .bind(usage_body_ref(request_id, field))
            .bind(request_id)
            .bind(field.as_storage_field())
            .bind(payload)
            .bind(0i32)
            .execute(&mut **tx)
            .await
            .map_postgres_err()?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    #[tokio::test]
    async fn usage_body_decode_capacity_is_fail_fast_and_reusable() {
        static SLOTS: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(1);
        let permit = super::acquire_usage_body_slot(&SLOTS).unwrap();
        assert!(matches!(
            super::acquire_usage_body_slot(&SLOTS),
            Err(aether_data_contracts::DataLayerError::TimedOut(_))
        ));
        let error = super::decode_usage_body_with_permit(permit, || -> Result<(), _> {
            Err(aether_data_contracts::DataLayerError::UnexpectedValue(
                "corrupt body".into(),
            ))
        })
        .await
        .unwrap_err();
        assert!(error.to_string().contains("corrupt body"));
        assert!(super::acquire_usage_body_slot(&SLOTS).is_ok());
    }

    #[tokio::test]
    async fn usage_body_decode_cancellation_keeps_capacity_until_worker_exits() {
        static SLOTS: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(1);
        let permit = super::acquire_usage_body_slot(&SLOTS).unwrap();
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (finish_tx, finish_rx) = std::sync::mpsc::channel();
        let caller = tokio::spawn(super::decode_usage_body_with_permit(permit, move || {
            let _ = started_tx.send(());
            finish_rx
                .recv_timeout(std::time::Duration::from_secs(5))
                .unwrap();
            Ok(())
        }));
        tokio::time::timeout(std::time::Duration::from_secs(5), started_rx)
            .await
            .unwrap()
            .unwrap();
        caller.abort();
        assert!(caller.await.unwrap_err().is_cancelled());
        assert!(super::acquire_usage_body_slot(&SLOTS).is_err());
        finish_tx.send(()).unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                if let Ok(permit) = super::acquire_usage_body_slot(&SLOTS) {
                    drop(permit);
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn usage_body_decode_does_not_block_the_async_runtime_thread() {
        let runtime_thread = std::thread::current().id();
        let payload = json!({"message": "background decoding"});
        let bytes = serde_json::to_vec(&payload).unwrap();

        let decoded = super::decode_usage_body_with_permit(
            super::acquire_usage_body_slot(&super::USAGE_BODY_DECODE_SLOTS).unwrap(),
            move || {
                assert_ne!(std::thread::current().id(), runtime_thread);
                serde_json::from_slice::<serde_json::Value>(&bytes)
                    .map(Some)
                    .map_err(|error| {
                        aether_data_contracts::DataLayerError::UnexpectedValue(format!(
                            "failed to parse usage json: {error}"
                        ))
                    })
            },
        )
        .await
        .expect("body should decode");

        assert_eq!(decoded, Some(payload));
    }

    #[tokio::test]
    async fn usage_body_decode_preserves_storage_decode_errors() {
        let error = super::decode_usage_body_with_permit(
            super::acquire_usage_body_slot(&super::USAGE_BODY_DECODE_SLOTS).unwrap(),
            || {
                serde_json::from_slice::<serde_json::Value>(b"not json")
                    .map(Some)
                    .map_err(|error| {
                        aether_data_contracts::DataLayerError::UnexpectedValue(format!(
                            "failed to parse usage json: {error}"
                        ))
                    })
            },
        )
        .await
        .expect_err("corrupt bodies should fail");

        assert!(error.to_string().contains("failed to parse usage json:"));
    }
}
