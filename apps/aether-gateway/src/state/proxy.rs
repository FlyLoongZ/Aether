use aether_contracts::{ProxySnapshot, PROXY_NODE_GENERATION_EXTRA_KEY};
use aether_crypto::looks_like_python_fernet_ciphertext;
use serde_json::{Map, Value};
use std::future::Future;

use super::AppState;
use crate::handlers::shared::{
    open_runtime_secret_payload_with_encryption_key, runtime_secret_payload_is_sealed,
    seal_runtime_secret_payload_with_encryption_key,
};
use crate::provider_transport::GatewayProviderTransportSnapshot;
use crate::GatewayError;

const PROXY_NODE_PASSWORD_LEGACY_SECRET_PURPOSE: &str = "proxy-node-password";
const PROXY_NODE_SECRET_ENVELOPE_FAMILY_PREFIX: &str = "aether-proxy-node-secret-";
const PROXY_NODE_SECRET_V2_PREFIX: &str = "aether-proxy-node-secret-v2:";
const RUNTIME_SECRET_ENVELOPE_FAMILY_PREFIX: &str = "aether-runtime-secret-";
const PROXY_NODE_BOUND_PURPOSE_VERSION: &str = "proxy-node-secret-bound-v2";
const PROXY_NODE_PASSWORD_SCOPE: &str = "manual-proxy";
const PROXY_NODE_PASSWORD_FIELD: &str = "password";
const PROXY_NODE_SECRET_MIGRATION_RETRIES: usize = 8;
const PROXY_UNAVAILABLE_REASON_EXTRA_KEY: &str = "aether_proxy_unavailable_reason";

pub(crate) fn unavailable_proxy_snapshot(reason: &str) -> ProxySnapshot {
    let extra = Map::from_iter([(
        PROXY_UNAVAILABLE_REASON_EXTRA_KEY.to_string(),
        Value::String(reason.to_string()),
    )]);
    ProxySnapshot {
        enabled: Some(true),
        mode: Some("unavailable".to_string()),
        node_id: None,
        label: None,
        url: None,
        extra: Some(Value::Object(extra)),
    }
}

fn proxy_node_password_error() -> GatewayError {
    GatewayError::Internal("stored proxy node password cannot be decrypted".to_string())
}

fn proxy_node_secret_is_v2(value: &str) -> bool {
    value.starts_with(PROXY_NODE_SECRET_V2_PREFIX)
}

fn proxy_node_bound_secret_purpose(scope: &str, field: &str, node_id: &str) -> String {
    format!(
        "{PROXY_NODE_BOUND_PURPOSE_VERSION}\0scope-bytes={}\0{scope}\0field-bytes={}\0{field}\0node-id-bytes={}\0{node_id}",
        scope.len(),
        field.len(),
        node_id.len(),
    )
}

fn seal_proxy_node_secret_v2_with_encryption_key(
    encryption_key: Option<&str>,
    scope: &str,
    field: &str,
    node_id: &str,
    plaintext: &str,
) -> Option<String> {
    let purpose = proxy_node_bound_secret_purpose(scope, field, node_id);
    seal_runtime_secret_payload_with_encryption_key(encryption_key, &purpose, plaintext)
        .map(|sealed| format!("{PROXY_NODE_SECRET_V2_PREFIX}{sealed}"))
}

fn open_proxy_node_secret_v2_with_encryption_key(
    encryption_key: Option<&str>,
    scope: &str,
    field: &str,
    node_id: &str,
    stored: &str,
) -> Option<String> {
    // The distinct outer envelope is security-significant. A v2 binding
    // failure must never be retried with the unbound legacy purpose.
    let sealed = stored.strip_prefix(PROXY_NODE_SECRET_V2_PREFIX)?;
    let purpose = proxy_node_bound_secret_purpose(scope, field, node_id);
    open_runtime_secret_payload_with_encryption_key(encryption_key, &purpose, sealed)
}

fn incoming_proxy_node_secret_is_ciphertext(value: &str) -> bool {
    let value = value.trim();
    value.starts_with(PROXY_NODE_SECRET_ENVELOPE_FAMILY_PREFIX)
        || value.starts_with(RUNTIME_SECRET_ENVELOPE_FAMILY_PREFIX)
        || looks_like_python_fernet_ciphertext(value)
}

impl AppState {
    pub(super) fn protect_proxy_node_password(
        &self,
        node_id: &str,
        plaintext: &str,
    ) -> Result<String, GatewayError> {
        if incoming_proxy_node_secret_is_ciphertext(plaintext) {
            return Err(GatewayError::Internal(
                "proxy node password ciphertext must be created by the gateway".to_string(),
            ));
        }
        seal_proxy_node_secret_v2_with_encryption_key(
            self.encryption_key(),
            PROXY_NODE_PASSWORD_SCOPE,
            PROXY_NODE_PASSWORD_FIELD,
            node_id,
            plaintext,
        )
        .ok_or_else(|| {
            GatewayError::Internal("proxy node password encryption is unavailable".to_string())
        })
    }

    pub(crate) async fn decrypt_proxy_node_password(
        &self,
        node_id: &str,
    ) -> Result<Option<String>, GatewayError> {
        self.decrypt_proxy_node_password_with_before_compare(node_id, || async {})
            .await
    }

    async fn decrypt_proxy_node_password_with_before_compare<BeforeCompare, CompareFuture>(
        &self,
        node_id: &str,
        before_compare: BeforeCompare,
    ) -> Result<Option<String>, GatewayError>
    where
        BeforeCompare: Fn() -> CompareFuture,
        CompareFuture: Future<Output = ()>,
    {
        for _ in 0..PROXY_NODE_SECRET_MIGRATION_RETRIES {
            let Some(node) = self.find_proxy_node(node_id).await? else {
                return Ok(None);
            };
            let persistent_node_id = node.id.clone();
            let Some(observed) = node.proxy_password else {
                return Ok(None);
            };
            if proxy_node_secret_is_v2(&observed) {
                return open_proxy_node_secret_v2_with_encryption_key(
                    self.encryption_key(),
                    PROXY_NODE_PASSWORD_SCOPE,
                    PROXY_NODE_PASSWORD_FIELD,
                    &persistent_node_id,
                    &observed,
                )
                .map(Some)
                .ok_or_else(proxy_node_password_error);
            }
            let observed_shape = observed.trim();
            if observed_shape.starts_with(PROXY_NODE_SECRET_ENVELOPE_FAMILY_PREFIX)
                || (observed_shape.starts_with(RUNTIME_SECRET_ENVELOPE_FAMILY_PREFIX)
                    && !runtime_secret_payload_is_sealed(&observed))
                || looks_like_python_fernet_ciphertext(&observed)
            {
                return Err(proxy_node_password_error());
            }

            let plaintext = if runtime_secret_payload_is_sealed(&observed) {
                open_runtime_secret_payload_with_encryption_key(
                    self.encryption_key(),
                    PROXY_NODE_PASSWORD_LEGACY_SECRET_PURPOSE,
                    &observed,
                )
                .ok_or_else(proxy_node_password_error)?
            } else {
                observed.clone()
            };
            let encrypted = seal_proxy_node_secret_v2_with_encryption_key(
                self.encryption_key(),
                PROXY_NODE_PASSWORD_SCOPE,
                PROXY_NODE_PASSWORD_FIELD,
                &persistent_node_id,
                &plaintext,
            )
            .ok_or_else(|| {
                GatewayError::Internal("proxy node password encryption is unavailable".to_string())
            })?;
            before_compare().await;
            if self
                .data
                .compare_and_set_proxy_node_password(&persistent_node_id, &observed, &encrypted)
                .await
                .map_err(|err| GatewayError::Internal(err.to_string()))?
            {
                return Ok(Some(plaintext));
            }
        }

        Err(GatewayError::Internal(
            "proxy node password migration did not stabilize".to_string(),
        ))
    }

    pub(crate) async fn read_system_proxy_node_id(&self) -> Option<String> {
        self.read_system_config_json_value("system_proxy_node_id")
            .await
            .ok()
            .flatten()
            .and_then(|value| value.as_str().map(str::trim).map(ToOwned::to_owned))
            .filter(|value| !value.is_empty())
    }

    pub(crate) async fn resolve_proxy_node_snapshot(
        &self,
        node_id: Option<&str>,
    ) -> Option<ProxySnapshot> {
        let node_id = node_id.map(str::trim).filter(|value| !value.is_empty())?;
        let node = self.find_proxy_node(node_id).await.ok().flatten()?;
        if node.status.trim() != "online" {
            return None;
        }
        let proxy_password = match self.decrypt_proxy_node_password(&node.id).await {
            Ok(password) => password,
            Err(_) => return None,
        };
        let proxy_url = node
            .proxy_url
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())?;
        let proxy_url = proxy_url_with_node_auth(
            proxy_url,
            node.proxy_username.as_deref(),
            proxy_password.as_deref(),
        )?;
        let mut extra = Map::new();
        extra.insert(
            PROXY_NODE_GENERATION_EXTRA_KEY.to_string(),
            Value::String(node.node_generation.clone()),
        );
        Some(ProxySnapshot {
            enabled: Some(true),
            mode: proxy_mode_from_url(Some(&proxy_url)),
            node_id: Some(node.id),
            label: Some(node.name),
            url: Some(proxy_url),
            extra: Some(Value::Object(extra)),
        })
    }

    pub(crate) async fn resolve_system_proxy_snapshot(&self) -> Option<ProxySnapshot> {
        let node_id = self.read_system_proxy_node_id().await;
        let node_id = node_id.as_deref()?;
        self.resolve_proxy_node_snapshot(Some(node_id))
            .await
            .or_else(|| Some(unavailable_proxy_snapshot("system_proxy_node_unavailable")))
    }

    pub(crate) async fn resolve_transport_proxy_snapshot(
        &self,
        transport: &GatewayProviderTransportSnapshot,
    ) -> Option<ProxySnapshot> {
        self.resolve_transport_proxy_with_source(transport)
            .await
            .map(|(snapshot, _)| snapshot)
    }

    pub(crate) async fn resolve_transport_proxy_source(
        &self,
        transport: &GatewayProviderTransportSnapshot,
    ) -> Option<&'static str> {
        self.resolve_transport_proxy_with_source(transport)
            .await
            .map(|(_, source)| source)
    }

    pub(crate) async fn resolve_configured_proxy_snapshot(
        &self,
        raw: Option<&Value>,
    ) -> Option<ProxySnapshot> {
        let object = raw?.as_object()?;
        if !proxy_enabled(object) {
            return None;
        }

        if json_field_is_explicit(object, "node_id") {
            let node_id = json_string_field(object, "node_id");
            if let Some(snapshot) = self.resolve_proxy_node_snapshot(node_id.as_deref()).await {
                return Some(snapshot);
            }
            return Some(unavailable_proxy_snapshot(
                "configured_proxy_node_unavailable",
            ));
        }

        if json_field_is_explicit(object, "url") || json_field_is_explicit(object, "proxy_url") {
            let Some(snapshot) = proxy_snapshot_from_object(object) else {
                return Some(unavailable_proxy_snapshot(
                    "configured_proxy_url_unavailable",
                ));
            };
            if snapshot.url.is_none() {
                return Some(unavailable_proxy_snapshot(
                    "configured_proxy_url_unavailable",
                ));
            }
            return Some(snapshot);
        }

        None
    }

    async fn resolve_transport_proxy_with_source(
        &self,
        transport: &GatewayProviderTransportSnapshot,
    ) -> Option<(ProxySnapshot, &'static str)> {
        if let Some(snapshot) = self
            .resolve_configured_proxy_snapshot(transport.key.proxy.as_ref())
            .await
        {
            return Some((snapshot, "key"));
        }
        if let Some(snapshot) = self
            .resolve_configured_proxy_snapshot(transport.endpoint.proxy.as_ref())
            .await
        {
            return Some((snapshot, "endpoint"));
        }
        if let Some(snapshot) = self
            .resolve_configured_proxy_snapshot(transport.provider.proxy.as_ref())
            .await
        {
            return Some((snapshot, "provider"));
        }
        self.resolve_system_proxy_snapshot()
            .await
            .map(|snapshot| (snapshot, "system"))
    }
}

fn proxy_enabled(object: &Map<String, Value>) -> bool {
    object
        .get("enabled")
        .and_then(Value::as_bool)
        .unwrap_or(true)
}

fn proxy_snapshot_from_object(object: &Map<String, Value>) -> Option<ProxySnapshot> {
    let mode = json_string_field(object, "mode");
    let node_id = json_string_field(object, "node_id");
    let label = json_string_field(object, "label");
    let url = json_string_field(object, "url")
        .or_else(|| json_string_field(object, "proxy_url"))
        .and_then(|proxy_url| {
            proxy_url_with_node_auth(
                &proxy_url,
                json_proxy_credential_field(object, "username"),
                json_proxy_credential_field(object, "password"),
            )
        });

    if node_id.is_none() && url.is_none() {
        return None;
    }

    let mut extra = Map::new();
    for (key, value) in object {
        if matches!(
            key.as_str(),
            "enabled"
                | "mode"
                | "node_id"
                | "label"
                | "url"
                | "proxy_url"
                | "username"
                | "password"
        ) {
            continue;
        }
        extra.insert(key.clone(), value.clone());
    }

    Some(ProxySnapshot {
        enabled: object.get("enabled").and_then(Value::as_bool),
        mode,
        node_id,
        label,
        url,
        extra: if extra.is_empty() {
            None
        } else {
            Some(Value::Object(extra))
        },
    })
}

fn json_field_is_explicit(object: &Map<String, Value>, key: &str) -> bool {
    object.get(key).is_some_and(|value| !value.is_null())
}

fn json_string_field(object: &Map<String, Value>, key: &str) -> Option<String> {
    object
        .get(key)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

fn json_proxy_credential_field<'a>(object: &'a Map<String, Value>, key: &str) -> Option<&'a str> {
    object
        .get(key)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
}

fn proxy_mode_from_url(proxy_url: Option<&str>) -> Option<String> {
    let proxy_url = proxy_url?.trim();
    if proxy_url.is_empty() {
        return None;
    }
    let scheme = url::Url::parse(proxy_url)
        .ok()
        .map(|value| value.scheme().to_ascii_lowercase())
        .unwrap_or_default();
    if scheme.starts_with("socks") {
        Some("socks".to_string())
    } else {
        Some("http".to_string())
    }
}

fn proxy_url_with_node_auth(
    proxy_url: &str,
    username: Option<&str>,
    password: Option<&str>,
) -> Option<String> {
    let username = username.filter(|value| !value.is_empty());
    let password = password.filter(|value| !value.is_empty());
    let mut parsed = url::Url::parse(proxy_url).ok()?;
    if !matches!(parsed.scheme(), "http" | "https" | "socks5" | "socks5h")
        || parsed.host_str().is_none()
    {
        return None;
    }
    if username.is_none() && password.is_none() {
        return Some(parsed.to_string());
    }
    let username = username.unwrap_or("");
    if parsed.set_username(username).is_err() {
        return None;
    }
    if parsed.set_password(password).is_err() {
        return None;
    }
    Some(parsed.to_string())
}
