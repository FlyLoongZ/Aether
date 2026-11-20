//! Re-seals every stored secret under the current encryption key.
//!
//! This is the drain half of a key rotation. It deliberately does **not**
//! re-implement any derivation: every row is pushed through the same read path
//! the gateway uses, and that path already re-seals (with the current key and
//! purpose) the moment it notices `migration_required`. The only thing this
//! module adds is the enumeration — including rows nothing ever reads.

use std::collections::BTreeMap;

use aether_data_contracts::repository::auth::StoredAuthApiKeyExportRecord;

use super::AppState;
use crate::handlers::shared::{
    bark_device_key_binding, decrypt_or_migrate_auth_api_key_secret,
    decrypt_or_migrate_bark_device_key, decrypt_or_migrate_identity_oauth_provider_client_secret,
    decrypt_or_migrate_ldap_bind_password, decrypt_or_migrate_smtp_password,
    decrypt_or_migrate_system_config_secret, smtp_password_binding, system_config_bool,
    system_config_string,
};
use crate::GatewayError;

/// Payment gateway providers that store a merchant secret. Fixed in the product,
/// so a lookup per provider beats a full table scan.
const PAYMENT_GATEWAY_PROVIDERS: [&str; 4] = ["epay", "alipay", "wxpay", "stripe"];

/// `system_configs` keys that carry a secret, with the writer that owns them.
const SYSTEM_CONFIG_GENERIC_SECRET_KEYS: [&str; 4] = [
    "turnstile_secret_key",
    "backup_s3_secret_access_key",
    "module.server_chan_push.send_key",
    "module.important_notification.server_chan_send_key",
];

/// Error text for the report. `GatewayError` carries its message through
/// `into_message` instead of `Display`, so both shapes are accepted here.
pub(crate) trait SecretErrorText {
    fn secret_error_text(self) -> String;
}

impl SecretErrorText for GatewayError {
    fn secret_error_text(self) -> String {
        self.into_message()
    }
}

impl SecretErrorText for String {
    fn secret_error_text(self) -> String {
        self
    }
}

impl SecretErrorText for &str {
    fn secret_error_text(self) -> String {
        self.to_string()
    }
}

impl SecretErrorText for aether_data::DataLayerError {
    fn secret_error_text(self) -> String {
        self.to_string()
    }
}

pub(crate) const SMTP_PASSWORD_CONFIG_KEY: &str = "smtp_password";
pub(crate) const BARK_DEVICE_KEY_CONFIG_KEY: &str = "module.bark_push.device_key";

/// What one rotation pass did, per storage target.
#[derive(Debug, Default, Clone, serde::Serialize)]
pub struct SecretRotationSummary {
    /// Rows whose migration failed, per target.
    pub failed: BTreeMap<String, u64>,
    /// First few failures, bounded: target, record id, and the error text.
    pub failures: Vec<SecretRotationFailure>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct SecretRotationFailure {
    pub target: &'static str,
    pub record_id: String,
    pub error: String,
}

const MAX_RECORDED_FAILURES: usize = 20;

impl SecretRotationSummary {
    fn failed(&mut self, target: &'static str, record_id: &str, error: impl SecretErrorText) {
        *self.failed.entry(target.to_string()).or_default() += 1;
        if self.failures.len() < MAX_RECORDED_FAILURES {
            self.failures.push(SecretRotationFailure {
                target,
                record_id: record_id.to_string(),
                error: error.secret_error_text(),
            });
        }
    }
}

impl AppState {
    /// Re-seals every stored secret under the configured current key.
    ///
    /// Rows that fail are counted and skipped: one unreadable row must not stop
    /// the drain, and `secrets verify` is what decides whether the rotation is
    /// finished.
    pub(crate) async fn rotate_stored_secrets(
        &self,
    ) -> Result<SecretRotationSummary, GatewayError> {
        let mut summary = SecretRotationSummary::default();
        self.rotate_auth_api_keys(&mut summary).await;
        self.rotate_oauth_client_secrets(&mut summary).await;
        self.rotate_provider_catalog(&mut summary).await;
        self.rotate_system_config_secrets(&mut summary).await;
        self.rotate_ldap_bind_password(&mut summary).await;
        self.rotate_proxy_node_passwords(&mut summary).await;
        self.rotate_payment_gateway_secrets(&mut summary).await;
        Ok(summary)
    }

    async fn rotate_auth_api_keys(&self, summary: &mut SecretRotationSummary) {
        const TARGET: &str = "api_keys.key_encrypted";
        let ids = match self.data.list_secret_rotation_api_key_ids().await {
            Ok(ids) => ids,
            Err(error) => {
                summary.failed(TARGET, "*", error);
                return;
            }
        };
        for id in ids {
            let record = match self
                .list_auth_api_key_export_records_by_ids(std::slice::from_ref(&id))
                .await
            {
                Ok(records) => records.into_iter().find(|record| record.api_key_id == id),
                Err(error) => {
                    summary.failed(TARGET, &id, error);
                    continue;
                }
            };
            let Some(record) = record else {
                continue;
            };
            match migrate_auth_api_key(self, &record).await {
                Ok(()) => {}
                Err(error) => summary.failed(TARGET, &id, error),
            }
        }
    }

    async fn rotate_oauth_client_secrets(&self, summary: &mut SecretRotationSummary) {
        const TARGET: &str = "oauth_providers.client_secret_encrypted";
        let configs = match self.list_oauth_provider_configs().await {
            Ok(configs) => configs,
            Err(error) => {
                summary.failed(TARGET, "*", error);
                return;
            }
        };
        for config in configs {
            if config.client_secret_encrypted.is_none() {
                continue;
            }
            let id = config.provider_type.clone();
            match decrypt_or_migrate_identity_oauth_provider_client_secret(self, &config).await {
                Ok(_) => {}
                Err(error) => summary.failed(TARGET, &id, error),
            }
        }
    }

    async fn rotate_provider_catalog(&self, summary: &mut SecretRotationSummary) {
        let providers = match self.list_provider_catalog_providers(false).await {
            Ok(providers) => providers,
            Err(error) => {
                summary.failed("providers.config", "*", error);
                return;
            }
        };
        // Provider-level proxy/ops credentials.
        for provider in &providers {
            let id = provider.id.clone();
            match self.open_provider_catalog_provider(provider.clone()).await {
                Ok(_) => {}
                Err(error) => summary.failed("providers.config", &id, error),
            }
        }
        let provider_ids = providers
            .into_iter()
            .map(|provider| provider.id)
            .collect::<Vec<_>>();
        if provider_ids.is_empty() {
            return;
        }
        match self
            .list_provider_catalog_endpoints_by_provider_ids(&provider_ids)
            .await
        {
            Ok(endpoints) => {
                for endpoint in endpoints {
                    let id = endpoint.id.clone();
                    match self.open_provider_catalog_endpoint(endpoint).await {
                        Ok(_) => {}
                        Err(error) => summary.failed("provider_endpoints.proxy", &id, error),
                    }
                }
            }
            Err(error) => summary.failed("provider_endpoints.proxy", "*", error),
        }
        match self
            .list_provider_catalog_keys_by_provider_ids(&provider_ids)
            .await
        {
            Ok(keys) => {
                for key in keys {
                    let id = key.id.clone();
                    match self.open_provider_catalog_key(key).await {
                        Ok(_) => {}
                        Err(error) => summary.failed("provider_api_keys.credential", &id, error),
                    }
                }
            }
            Err(error) => summary.failed("provider_api_keys.credential", "*", error),
        }
    }

    async fn rotate_system_config_secrets(&self, summary: &mut SecretRotationSummary) {
        let entries = match self.list_system_config_entries().await {
            Ok(entries) => entries,
            Err(error) => {
                summary.failed("system_configs.value", "*", error);
                return;
            }
        };
        let value_for = |key: &str| {
            entries
                .iter()
                .find(|entry| entry.key == key)
                .and_then(|entry| system_config_string(Some(&entry.value)))
        };

        if let Some(saved) = value_for(SMTP_PASSWORD_CONFIG_KEY) {
            let binding = value_for("smtp_host").and_then(|host| {
                smtp_password_binding(
                    &host,
                    value_for("smtp_port")
                        .and_then(|port| port.parse::<u16>().ok())
                        .unwrap_or(587),
                    value_for("smtp_user").as_deref(),
                    entries
                        .iter()
                        .find(|entry| entry.key == "smtp_use_tls")
                        .map(|entry| system_config_bool(Some(&entry.value), true))
                        .unwrap_or(true),
                    entries
                        .iter()
                        .find(|entry| entry.key == "smtp_use_ssl")
                        .map(|entry| system_config_bool(Some(&entry.value), false))
                        .unwrap_or(false),
                )
            });
            match binding {
                Some(binding) => {
                    match decrypt_or_migrate_smtp_password(self, &binding, saved).await {
                        Ok(_) => {}
                        Err(error) => {
                            summary.failed("system_configs.value", SMTP_PASSWORD_CONFIG_KEY, error)
                        }
                    }
                }
                None => summary.failed(
                    "system_configs.value",
                    SMTP_PASSWORD_CONFIG_KEY,
                    "SMTP binding is incomplete; the password cannot be re-bound",
                ),
            }
        }

        if let Some(saved) = value_for(BARK_DEVICE_KEY_CONFIG_KEY) {
            let server_url = value_for("module.bark_push.server_url")
                .unwrap_or_else(|| "https://api.day.app".to_string());
            match bark_device_key_binding(&server_url) {
                Some(binding) => {
                    match decrypt_or_migrate_bark_device_key(self, &binding, saved).await {
                        Ok(_) => {}
                        Err(error) => summary.failed(
                            "system_configs.value",
                            BARK_DEVICE_KEY_CONFIG_KEY,
                            error,
                        ),
                    }
                }
                None => summary.failed(
                    "system_configs.value",
                    BARK_DEVICE_KEY_CONFIG_KEY,
                    "Bark server URL is invalid; the device key cannot be re-bound",
                ),
            }
        }

        for key in SYSTEM_CONFIG_GENERIC_SECRET_KEYS {
            let Some(saved) = value_for(key) else {
                continue;
            };
            match decrypt_or_migrate_system_config_secret(self, key, saved).await {
                Ok(_) => {}
                Err(error) => summary.failed("system_configs.value", key, error),
            }
        }
    }

    async fn rotate_ldap_bind_password(&self, summary: &mut SecretRotationSummary) {
        const TARGET: &str = "ldap_configs.bind_password_encrypted";
        let config = match self.get_ldap_module_config().await {
            Ok(config) => config,
            Err(error) => {
                summary.failed(TARGET, "singleton", error);
                return;
            }
        };
        let Some(config) = config else {
            return;
        };
        if config.bind_password_encrypted.is_none() {
            return;
        }
        match decrypt_or_migrate_ldap_bind_password(self, &config).await {
            Ok(_) => {}
            Err(error) => summary.failed(TARGET, "singleton", error),
        }
    }

    async fn rotate_proxy_node_passwords(&self, summary: &mut SecretRotationSummary) {
        const TARGET: &str = "proxy_nodes.proxy_password";
        let nodes = match self.list_proxy_nodes().await {
            Ok(nodes) => nodes,
            Err(error) => {
                summary.failed(TARGET, "*", error);
                return;
            }
        };
        for node in nodes {
            if node.proxy_password.is_none() {
                continue;
            }
            let id = node.id.clone();
            match self.decrypt_proxy_node_password(&id).await {
                Ok(_) => {}
                Err(error) => summary.failed(TARGET, &id, error),
            }
        }
    }

    async fn rotate_payment_gateway_secrets(&self, summary: &mut SecretRotationSummary) {
        const TARGET: &str = "payment_gateway_configs.merchant_key_encrypted";
        for provider in PAYMENT_GATEWAY_PROVIDERS {
            match self.find_payment_gateway_config(provider).await {
                Ok(Some(_)) => {}
                Ok(None) => {}
                Err(error) => summary.failed(TARGET, provider, error),
            }
        }
    }
}

async fn migrate_auth_api_key(
    state: &AppState,
    record: &StoredAuthApiKeyExportRecord,
) -> Result<(), GatewayError> {
    decrypt_or_migrate_auth_api_key_secret(state, record)
        .await
        .map(|_| ())
}
