mod create;
mod endpoint;
mod template;
mod update;

fn normalize_provider_request_timeout(value: Option<f64>) -> Result<Option<f64>, String> {
    let max = aether_contracts::MAX_EXECUTION_REQUEST_TIMEOUT_SECS as f64;
    match value {
        Some(value) if (1.0..=max).contains(&value) => Ok(Some(value)),
        Some(_) => Err(format!(
            "request_timeout 必须是 1 到 {} 之间的数字",
            aether_contracts::MAX_EXECUTION_REQUEST_TIMEOUT_SECS
        )),
        None => Ok(None),
    }
}

fn normalize_provider_stream_first_byte_timeout(value: Option<f64>) -> Result<Option<f64>, String> {
    let max = aether_contracts::MAX_EXECUTION_STREAM_FIRST_BYTE_TIMEOUT_SECS as f64;
    match value {
        Some(value) if (1.0..=max).contains(&value) => Ok(Some(value)),
        Some(_) => Err(format!(
            "stream_first_byte_timeout 必须是 1 到 {} 之间的数字",
            aether_contracts::MAX_EXECUTION_STREAM_FIRST_BYTE_TIMEOUT_SECS
        )),
        None => Ok(None),
    }
}

fn remove_codex_fingerprint_config(config_map: &mut serde_json::Map<String, serde_json::Value>) {
    let namespace = crate::provider_transport::CODEX_FINGERPRINT_CONFIG_NAMESPACE;
    let key = crate::provider_transport::CODEX_FINGERPRINT_ENABLED_CONFIG_KEY;
    let mut remove_namespace = false;
    if let Some(codex_config) = config_map
        .get_mut(namespace)
        .and_then(|value| value.as_object_mut())
    {
        codex_config.remove(key);
        remove_namespace = codex_config.is_empty();
    }
    if remove_namespace {
        config_map.remove(namespace);
    }
}

pub(crate) use self::create::build_admin_create_provider_record;
pub(crate) use self::template::{
    apply_admin_fixed_provider_endpoint_template_overrides,
    reconcile_admin_fixed_provider_template_endpoints,
    reconcile_admin_fixed_provider_template_endpoints_after_update,
};
pub(crate) use self::update::build_admin_update_provider_record;

#[cfg(test)]
mod tests {
    use super::{
        normalize_provider_request_timeout, normalize_provider_stream_first_byte_timeout,
        remove_codex_fingerprint_config,
    };
    use serde_json::json;

    #[test]
    fn provider_request_timeout_accepts_the_execution_protocol_range() {
        assert_eq!(normalize_provider_request_timeout(Some(1.0)), Ok(Some(1.0)));
        assert_eq!(
            normalize_provider_request_timeout(Some(
                aether_contracts::MAX_EXECUTION_REQUEST_TIMEOUT_SECS as f64,
            )),
            Ok(Some(
                aether_contracts::MAX_EXECUTION_REQUEST_TIMEOUT_SECS as f64,
            ))
        );
        assert!(normalize_provider_request_timeout(Some(0.0)).is_err());
        assert!(normalize_provider_request_timeout(Some(
            aether_contracts::MAX_EXECUTION_REQUEST_TIMEOUT_SECS as f64 + 1.0,
        ))
        .is_err());
        assert!(normalize_provider_request_timeout(Some(f64::NAN)).is_err());
    }

    #[test]
    fn provider_stream_first_byte_timeout_keeps_its_protocol_range() {
        assert_eq!(
            normalize_provider_stream_first_byte_timeout(Some(1.0)),
            Ok(Some(1.0))
        );
        assert_eq!(
            normalize_provider_stream_first_byte_timeout(Some(
                aether_contracts::MAX_EXECUTION_STREAM_FIRST_BYTE_TIMEOUT_SECS as f64,
            )),
            Ok(Some(
                aether_contracts::MAX_EXECUTION_STREAM_FIRST_BYTE_TIMEOUT_SECS as f64,
            ))
        );
        assert!(normalize_provider_stream_first_byte_timeout(Some(
            aether_contracts::MAX_EXECUTION_STREAM_FIRST_BYTE_TIMEOUT_SECS as f64 + 1.0,
        ))
        .is_err());
    }

    #[test]
    fn removing_fingerprint_setting_preserves_other_codex_config() {
        let mut config = json!({
            "codex": {
                "fingerprint_convergence_enabled": true,
                "pass_through_cyber_flag_interrupt": true
            },
            "other": {"kept": true}
        })
        .as_object()
        .expect("config object")
        .clone();

        remove_codex_fingerprint_config(&mut config);

        assert_eq!(
            config["codex"],
            json!({"pass_through_cyber_flag_interrupt": true})
        );
        assert_eq!(config["other"], json!({"kept": true}));
    }
}
