use async_trait::async_trait;
use sqlx::{postgres::PgRow, PgPool, Row};

use aether_data_contracts::repository::proxy_nodes::{
    new_proxy_node_generation, ProxyNodeManualCreateMutation, ProxyNodeManualUpdateMutation,
    ProxyNodeReadRepository, ProxyNodeTrafficMutation, ProxyNodeWriteRepository, StoredProxyNode,
};
use aether_data_contracts::DataLayerError;

use crate::error::SqlxResultExt;

const FIND_PROXY_NODE_SQL: &str = r#"
SELECT
  id,
  node_generation,
  name,
  ip,
  port,
  region,
  proxy_url,
  proxy_username,
  proxy_password,
  CAST(status AS TEXT) AS status,
  registered_by,
  EXTRACT(EPOCH FROM last_used_at)::bigint AS last_used_at_unix_secs,
  total_requests,
  CAST(avg_latency_ms AS DOUBLE PRECISION) AS avg_latency_ms,
  failed_requests,
  dns_failures,
  stream_errors,
  EXTRACT(EPOCH FROM created_at)::bigint AS created_at_unix_ms,
  EXTRACT(EPOCH FROM updated_at)::bigint AS updated_at_unix_secs
FROM proxy_nodes
WHERE id = $1
LIMIT 1
"#;

const LIST_PROXY_NODES_SQL: &str = r#"
SELECT
  id,
  node_generation,
  name,
  ip,
  port,
  region,
  proxy_url,
  proxy_username,
  proxy_password,
  CAST(status AS TEXT) AS status,
  registered_by,
  EXTRACT(EPOCH FROM last_used_at)::bigint AS last_used_at_unix_secs,
  total_requests,
  CAST(avg_latency_ms AS DOUBLE PRECISION) AS avg_latency_ms,
  failed_requests,
  dns_failures,
  stream_errors,
  EXTRACT(EPOCH FROM created_at)::bigint AS created_at_unix_ms,
  EXTRACT(EPOCH FROM updated_at)::bigint AS updated_at_unix_secs
FROM proxy_nodes
ORDER BY created_at ASC, id ASC
"#;

const INSERT_MANUAL_PROXY_NODE_SQL: &str = r#"
INSERT INTO proxy_nodes (
  id,
  node_generation,
  name,
  ip,
  port,
  region,
  proxy_url,
  proxy_username,
  proxy_password,
  status,
  registered_by,
  last_used_at,
  total_requests,
  failed_requests,
  dns_failures,
  stream_errors
)
VALUES (
  $1,
  $2,
  $3,
  $4,
  $5,
  $6,
  $7,
  $8,
  $9,
  'online'::proxynodestatus,
  $10,
  NULL,
  0,
  0,
  0,
  0
)
"#;

const UPDATE_MANUAL_PROXY_NODE_SQL: &str = r#"
UPDATE proxy_nodes
SET
  name = COALESCE($2, name),
  ip = COALESCE($3, ip),
  port = COALESCE($4, port),
  region = COALESCE($5, region),
  proxy_url = COALESCE($6, proxy_url),
  proxy_username = COALESCE($7, proxy_username),
  proxy_password = COALESCE($8, proxy_password),
  updated_at = NOW()
WHERE id = $1
"#;

const RECORD_PROXY_NODE_TRAFFIC_SQL: &str = r#"
UPDATE proxy_nodes
SET
  total_requests = total_requests + GREATEST($2, 0),
  failed_requests = failed_requests + GREATEST($3, 0),
  dns_failures = dns_failures + GREATEST($4, 0),
  stream_errors = stream_errors + GREATEST($5, 0),
  last_used_at = NOW(),
  updated_at = NOW()
WHERE id = $1
  AND node_generation = $6
"#;

const INCREMENT_MANUAL_PROXY_NODE_REQUESTS_SQL: &str = r#"
UPDATE proxy_nodes
SET
  total_requests = total_requests + GREATEST($1::bigint, 0),
  failed_requests = failed_requests + GREATEST($2::bigint, 0),
  avg_latency_ms = COALESCE($3, avg_latency_ms),
  last_used_at = NOW(),
  updated_at = NOW()
WHERE id = $4
"#;

const COMPARE_AND_SET_PROXY_PASSWORD_SQL: &str = r#"
UPDATE proxy_nodes
SET proxy_password = $3, updated_at = NOW()
WHERE id = $1 AND proxy_password IS NOT DISTINCT FROM $2
"#;

const DELETE_PROXY_NODE_SQL: &str = r#"
DELETE FROM proxy_nodes
WHERE id = $1
"#;

#[derive(Debug, Clone)]
pub struct SqlxProxyNodeRepository {
    pool: PgPool,
}

impl SqlxProxyNodeRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    fn optional_unix_secs(value: Option<i64>) -> Option<u64> {
        value.and_then(|value| u64::try_from(value).ok())
    }

    fn row_to_stored(row: &PgRow) -> Result<StoredProxyNode, DataLayerError> {
        Ok(StoredProxyNode {
            id: row.try_get("id").map_postgres_err()?,
            node_generation: row.try_get("node_generation").map_postgres_err()?,
            name: row.try_get("name").map_postgres_err()?,
            ip: row.try_get("ip").map_postgres_err()?,
            port: row.try_get("port").map_postgres_err()?,
            region: row.try_get("region").map_postgres_err()?,
            proxy_url: row.try_get("proxy_url").map_postgres_err()?,
            proxy_username: row.try_get("proxy_username").map_postgres_err()?,
            proxy_password: row.try_get("proxy_password").map_postgres_err()?,
            status: row.try_get("status").map_postgres_err()?,
            registered_by: row.try_get("registered_by").map_postgres_err()?,
            last_used_at_unix_secs: Self::optional_unix_secs(
                row.try_get("last_used_at_unix_secs").map_postgres_err()?,
            ),
            total_requests: row.try_get("total_requests").map_postgres_err()?,
            avg_latency_ms: row.try_get("avg_latency_ms").map_postgres_err()?,
            failed_requests: row.try_get("failed_requests").map_postgres_err()?,
            dns_failures: row.try_get("dns_failures").map_postgres_err()?,
            stream_errors: row.try_get("stream_errors").map_postgres_err()?,
            created_at_unix_ms: Self::optional_unix_secs(
                row.try_get("created_at_unix_ms").map_postgres_err()?,
            ),
            updated_at_unix_secs: Self::optional_unix_secs(
                row.try_get("updated_at_unix_secs").map_postgres_err()?,
            ),
        })
    }
}

#[async_trait]
impl ProxyNodeReadRepository for SqlxProxyNodeRepository {
    async fn list_proxy_nodes(&self) -> Result<Vec<StoredProxyNode>, DataLayerError> {
        let rows = sqlx::query(LIST_PROXY_NODES_SQL)
            .fetch_all(&self.pool)
            .await
            .map_postgres_err()?;
        rows.iter().map(Self::row_to_stored).collect()
    }

    async fn find_proxy_node(
        &self,
        node_id: &str,
    ) -> Result<Option<StoredProxyNode>, DataLayerError> {
        let row = sqlx::query(FIND_PROXY_NODE_SQL)
            .bind(node_id)
            .fetch_optional(&self.pool)
            .await
            .map_postgres_err()?;
        row.as_ref().map(Self::row_to_stored).transpose()
    }
}

#[async_trait]
impl ProxyNodeWriteRepository for SqlxProxyNodeRepository {
    async fn compare_and_set_proxy_password(
        &self,
        node_id: &str,
        expected: &str,
        replacement: &str,
    ) -> Result<bool, DataLayerError> {
        let result = sqlx::query(COMPARE_AND_SET_PROXY_PASSWORD_SQL)
            .bind(node_id)
            .bind(expected)
            .bind(replacement)
            .execute(&self.pool)
            .await
            .map_postgres_err()?;
        Ok(result.rows_affected() == 1)
    }

    async fn create_manual_node(
        &self,
        mutation: &ProxyNodeManualCreateMutation,
    ) -> Result<StoredProxyNode, DataLayerError> {
        let node_id = mutation
            .node_id
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned)
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
        let node_generation = new_proxy_node_generation();
        sqlx::query(INSERT_MANUAL_PROXY_NODE_SQL)
            .bind(&node_id)
            .bind(&node_generation)
            .bind(&mutation.name)
            .bind(&mutation.ip)
            .bind(mutation.port)
            .bind(mutation.region.as_deref())
            .bind(&mutation.proxy_url)
            .bind(mutation.proxy_username.as_deref())
            .bind(mutation.proxy_password.as_deref())
            .bind(mutation.registered_by.as_deref())
            .execute(&self.pool)
            .await
            .map_err(|error| match error {
                sqlx::Error::Database(database_error)
                    if database_error.code().as_deref() == Some("23505") =>
                {
                    DataLayerError::InvalidInput(
                        "proxy node with the same ip and port already exists".to_string(),
                    )
                }
                other => postgres_error(other),
            })?;
        self.find_proxy_node(&node_id).await?.ok_or_else(|| {
            DataLayerError::UnexpectedValue("created manual proxy node missing".to_string())
        })
    }

    async fn update_manual_node(
        &self,
        mutation: &ProxyNodeManualUpdateMutation,
    ) -> Result<Option<StoredProxyNode>, DataLayerError> {
        let result = sqlx::query(UPDATE_MANUAL_PROXY_NODE_SQL)
            .bind(&mutation.node_id)
            .bind(mutation.name.as_deref())
            .bind(mutation.ip.as_deref())
            .bind(mutation.port)
            .bind(mutation.region.as_deref())
            .bind(mutation.proxy_url.as_deref())
            .bind(mutation.proxy_username.as_deref())
            .bind(mutation.proxy_password.as_deref())
            .execute(&self.pool)
            .await
            .map_err(|error| match error {
                sqlx::Error::Database(database_error)
                    if database_error.code().as_deref() == Some("23505") =>
                {
                    DataLayerError::InvalidInput(
                        "proxy node with the same ip and port already exists".to_string(),
                    )
                }
                other => postgres_error(other),
            })?;
        if result.rows_affected() == 0 {
            return Ok(None);
        }
        self.find_proxy_node(&mutation.node_id).await
    }

    async fn record_traffic(
        &self,
        mutation: &ProxyNodeTrafficMutation,
    ) -> Result<bool, DataLayerError> {
        let generation = mutation
            .expected_node_generation
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| {
                DataLayerError::UnexpectedValue(
                    "proxy node traffic mutation requires node_generation".to_string(),
                )
            })?;
        let result = sqlx::query(RECORD_PROXY_NODE_TRAFFIC_SQL)
            .bind(&mutation.node_id)
            .bind(mutation.total_requests_delta)
            .bind(mutation.failed_requests_delta)
            .bind(mutation.dns_failures_delta)
            .bind(mutation.stream_errors_delta)
            .bind(generation)
            .execute(&self.pool)
            .await
            .map_postgres_err()?;
        Ok(result.rows_affected() == 1)
    }

    async fn delete_node(&self, node_id: &str) -> Result<Option<StoredProxyNode>, DataLayerError> {
        let existing = self.find_proxy_node(node_id).await?;
        if existing.is_none() {
            return Ok(None);
        }
        sqlx::query(DELETE_PROXY_NODE_SQL)
            .bind(node_id)
            .execute(&self.pool)
            .await
            .map_postgres_err()?;
        Ok(existing)
    }

    async fn increment_manual_node_requests(
        &self,
        node_id: &str,
        total_delta: i64,
        failed_delta: i64,
        latency_ms: Option<i64>,
    ) -> Result<(), DataLayerError> {
        sqlx::query(INCREMENT_MANUAL_PROXY_NODE_REQUESTS_SQL)
            .bind(total_delta)
            .bind(failed_delta)
            .bind(latency_ms)
            .bind(node_id)
            .execute(&self.pool)
            .await
            .map_postgres_err()?;
        Ok(())
    }
}

use crate::error::postgres_error;
