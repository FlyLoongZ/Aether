//! Ciphertext inventory used by the `aether-gateway secrets` subcommands.
//!
//! The rotation tools need to answer "which rows are still sealed by an old
//! key". That question is answered from the *stored* value alone: an
//! `aether-secret:v1` envelope carries the id of the key that sealed it, so no
//! decryption (and therefore no purpose binding) is involved. This module only
//! enumerates; classification lives in the gateway.

use futures_util::TryStreamExt;
use sqlx::Row;

use crate::backend::PostgresBackend;
use crate::error::SqlxResultExt;
use crate::DataLayerError;

/// One stored value that is expected to hold an encrypted secret.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredSecretCiphertext {
    /// `table.column`-shaped label, stable enough to appear in reports.
    pub target: &'static str,
    /// Primary key of the row, or the config key for `system_configs`.
    pub record_id: String,
    /// Sub-field when the ciphertext lives inside a JSON object.
    pub field: Option<String>,
    pub value: String,
}

/// Every target the scan covers, in report order.
pub const SECRET_SCAN_TARGETS: &[&str] = &[
    "api_keys.key_encrypted",
    "oauth_providers.client_secret_encrypted",
    "provider_api_keys.credential",
    "provider_api_keys.auth_config",
    "provider_api_keys.proxy.password",
    "provider_api_keys.proxy.username",
    "providers.proxy.password",
    "providers.proxy.username",
    "provider_endpoints.proxy.password",
    "provider_endpoints.proxy.username",
    "providers.ops_credential",
    "system_configs.value",
    "ldap_configs.bind_password_encrypted",
    "proxy_nodes.proxy_password",
    "payment_gateway_configs.merchant_key_encrypted",
    "payment_orders.stripe_client_secret",
];

impl PostgresBackend {
    /// Collects every stored secret ciphertext for rotation diagnostics.
    ///
    /// Runs one statement per target. The returned values are ciphertexts (or,
    /// in the plaintext-fallback case, unprotected secrets); callers must never
    /// log them, only classify and count.
    pub async fn scan_secret_ciphertexts(
        &self,
    ) -> Result<Vec<StoredSecretCiphertext>, DataLayerError> {
        let mut rows = Vec::new();
        for &(target, sql) in SECRET_SCAN_QUERIES {
            let mut stream = sqlx::query(sql).fetch(self.pool());
            while let Some(row) = stream.try_next().await.map_postgres_err()? {
                let value: String = row.try_get("value").map_postgres_err()?;
                rows.push(StoredSecretCiphertext {
                    target,
                    record_id: row.try_get("record_id").map_postgres_err()?,
                    field: row.try_get("field").map_postgres_err()?,
                    value,
                });
            }
        }
        Ok(rows)
    }

    /// Ids of API keys that still carry a stored ciphertext.
    ///
    /// The export repository can only list keys by user id, by key id, or the
    /// standalone page, so a rotation cannot reach user-owned keys through it.
    /// Returning ids keeps the export projection (and its type) out of this
    /// crate; the caller re-reads each record through the repository, which is
    /// also what the migration's re-read loop needs.
    pub async fn list_secret_rotation_api_key_ids(&self) -> Result<Vec<String>, DataLayerError> {
        let mut stream = sqlx::query(LIST_SECRET_ROTATION_API_KEY_IDS_SQL).fetch(self.pool());
        let mut ids = Vec::new();
        while let Some(row) = stream.try_next().await.map_postgres_err()? {
            ids.push(row.try_get("api_key_id").map_postgres_err()?);
        }
        Ok(ids)
    }
}

/// One statement per encrypted location, kept literal so each is auditable.
///
/// Every statement returns `record_id`, `field`, and `value` columns; the value
/// expression is always the ciphertext itself.
const SECRET_SCAN_QUERIES: &[(&str, &str)] = &[
    (
        "api_keys.key_encrypted",
        r#"
SELECT api_keys.id AS record_id, NULL::text AS field, api_keys.key_encrypted AS value
FROM api_keys
WHERE api_keys.key_encrypted IS NOT NULL AND api_keys.key_encrypted <> ''
"#,
    ),
    (
        "oauth_providers.client_secret_encrypted",
        r#"
SELECT oauth_providers.provider_type AS record_id,
       NULL::text AS field,
       oauth_providers.client_secret_encrypted AS value
FROM oauth_providers
WHERE oauth_providers.client_secret_encrypted IS NOT NULL
  AND oauth_providers.client_secret_encrypted <> ''
"#,
    ),
    (
        "provider_api_keys.credential",
        r#"
SELECT provider_api_keys.id AS record_id,
       NULL::text AS field,
       COALESCE(provider_api_keys.api_key, provider_api_keys.encrypted_key) AS value
FROM provider_api_keys
WHERE COALESCE(provider_api_keys.api_key, provider_api_keys.encrypted_key) IS NOT NULL
  AND COALESCE(provider_api_keys.api_key, provider_api_keys.encrypted_key) <> ''
"#,
    ),
    (
        // `auth_config` is a single ciphertext (the JSON it protects is inside
        // the envelope), not a JSON object of per-field ciphertexts: the scan
        // classifies the whole value.
        "provider_api_keys.auth_config",
        r#"
SELECT provider_api_keys.id AS record_id, NULL::text AS field, provider_api_keys.auth_config AS value
FROM provider_api_keys
WHERE provider_api_keys.auth_config IS NOT NULL AND provider_api_keys.auth_config <> ''
"#,
    ),
    (
        "provider_api_keys.proxy.password",
        r#"
SELECT provider_api_keys.id AS record_id,
       'password'::text AS field,
       provider_api_keys.proxy #>> '{password}' AS value
FROM provider_api_keys
WHERE provider_api_keys.proxy #>> '{password}' IS NOT NULL
  AND provider_api_keys.proxy #>> '{password}' <> ''
"#,
    ),
    (
        "providers.proxy.password",
        r#"
SELECT providers.id AS record_id, 'password'::text AS field, providers.proxy #>> '{password}' AS value
FROM providers
WHERE providers.proxy #>> '{password}' IS NOT NULL
  AND providers.proxy #>> '{password}' <> ''
"#,
    ),
    (
        "provider_endpoints.proxy.password",
        r#"
SELECT provider_endpoints.id AS record_id,
       'password'::text AS field,
       provider_endpoints.proxy #>> '{password}' AS value
FROM provider_endpoints
WHERE provider_endpoints.proxy #>> '{password}' IS NOT NULL
  AND provider_endpoints.proxy #>> '{password}' <> ''
"#,
    ),
    (
        "provider_api_keys.proxy.username",
        r#"
SELECT provider_api_keys.id AS record_id,
       'username'::text AS field,
       provider_api_keys.proxy #>> '{username}' AS value
FROM provider_api_keys
WHERE provider_api_keys.proxy #>> '{username}' IS NOT NULL
  AND provider_api_keys.proxy #>> '{username}' <> ''
"#,
    ),
    (
        "providers.proxy.username",
        r#"
SELECT providers.id AS record_id, 'username'::text AS field, providers.proxy #>> '{username}' AS value
FROM providers
WHERE providers.proxy #>> '{username}' IS NOT NULL
  AND providers.proxy #>> '{username}' <> ''
"#,
    ),
    (
        "provider_endpoints.proxy.username",
        r#"
SELECT provider_endpoints.id AS record_id,
       'username'::text AS field,
       provider_endpoints.proxy #>> '{username}' AS value
FROM provider_endpoints
WHERE provider_endpoints.proxy #>> '{username}' IS NOT NULL
  AND provider_endpoints.proxy #>> '{username}' <> ''
"#,
    ),
    (
        "providers.ops_credential",
        r#"
SELECT providers.id AS record_id, credential.key AS field, credential.value AS value
FROM providers
CROSS JOIN LATERAL JSONB_EACH_TEXT(providers.config #> '{provider_ops,connector,credentials}')
     AS credential(key, value)
WHERE JSONB_TYPEOF(providers.config #> '{provider_ops,connector,credentials}') = 'object'
  AND credential.value IS NOT NULL
  AND credential.value <> ''
"#,
    ),
    (
        "system_configs.value",
        r#"
SELECT system_configs.key AS record_id, NULL::text AS field, system_configs.value #>> '{}' AS value
FROM system_configs
WHERE system_configs.key IN (
        'smtp_password',
        'turnstile_secret_key',
        'backup_s3_secret_access_key',
        'module.server_chan_push.send_key',
        'module.important_notification.server_chan_send_key',
        'module.bark_push.device_key'
      )
  AND JSONB_TYPEOF(system_configs.value) = 'string'
  AND system_configs.value #>> '{}' <> ''
"#,
    ),
    (
        "ldap_configs.bind_password_encrypted",
        r#"
SELECT ldap_configs.singleton_key::text AS record_id,
       NULL::text AS field,
       ldap_configs.bind_password_encrypted AS value
FROM ldap_configs
WHERE ldap_configs.bind_password_encrypted IS NOT NULL
  AND ldap_configs.bind_password_encrypted <> ''
"#,
    ),
    (
        "proxy_nodes.proxy_password",
        r#"
SELECT proxy_nodes.id AS record_id, NULL::text AS field, proxy_nodes.proxy_password AS value
FROM proxy_nodes
WHERE proxy_nodes.proxy_password IS NOT NULL AND proxy_nodes.proxy_password <> ''
"#,
    ),
    (
        "payment_gateway_configs.merchant_key_encrypted",
        r#"
SELECT payment_gateway_configs.provider AS record_id,
       NULL::text AS field,
       payment_gateway_configs.merchant_key_encrypted AS value
FROM payment_gateway_configs
WHERE payment_gateway_configs.merchant_key_encrypted IS NOT NULL
  AND payment_gateway_configs.merchant_key_encrypted <> ''
"#,
    ),
    (
        // Terminal orders keep a dead Stripe client secret that the live-order
        // migration refuses to touch; counting them would keep the rotation red
        // forever with nothing safe to rewrite.
        "payment_orders.stripe_client_secret",
        r#"
SELECT payment_orders.order_no AS record_id,
       NULL::text AS field,
       payment_orders.gateway_response #>> '{client_secret_encrypted}' AS value
FROM payment_orders
WHERE payment_orders.status = 'pending'
  AND payment_orders.expires_at > NOW()
  AND payment_orders.gateway_response #>> '{client_secret_encrypted}' IS NOT NULL
  AND payment_orders.gateway_response #>> '{client_secret_encrypted}' <> ''
"#,
    ),
];

const LIST_SECRET_ROTATION_API_KEY_IDS_SQL: &str = r#"
SELECT api_keys.id AS api_key_id
FROM api_keys
WHERE api_keys.key_encrypted IS NOT NULL
"#;

#[cfg(test)]
mod tests {
    use super::{SECRET_SCAN_QUERIES, SECRET_SCAN_TARGETS};

    /// Every scanned target must also appear in the report list, and vice versa.
    ///
    /// The two lists are written by hand; a target that only exists in one of
    /// them either scans rows nobody reports or reports a target that is never
    /// read.
    #[test]
    fn scan_queries_and_targets_stay_in_sync() {
        let mut queried: Vec<&str> = SECRET_SCAN_QUERIES.iter().map(|(t, _)| *t).collect();
        let mut listed: Vec<&str> = SECRET_SCAN_TARGETS.to_vec();
        queried.sort_unstable();
        listed.sort_unstable();
        assert_eq!(queried, listed);
    }

    /// The catalog proxy stores *two* sealed fields per record.
    ///
    /// Scanning only `password` left `username` behind on rotation: the value was
    /// re-encrypted by nothing, so after the old key was dropped the provider's
    /// proxy credentials became unreadable.
    #[test]
    fn catalog_proxy_scan_covers_username_and_password() {
        for table in ["provider_api_keys", "providers", "provider_endpoints"] {
            for field in ["username", "password"] {
                let target = format!("{table}.proxy.{field}");
                assert!(
                    SECRET_SCAN_TARGETS.contains(&target.as_str()),
                    "missing scan target {target}"
                );
                assert!(
                    SECRET_SCAN_QUERIES.iter().any(|(name, _)| *name == target),
                    "missing scan query for {target}"
                );
            }
        }
    }
}
