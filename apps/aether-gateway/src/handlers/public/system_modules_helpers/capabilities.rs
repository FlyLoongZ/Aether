#[derive(Clone, Copy)]
pub(crate) struct PublicCapabilityDefinition {
    pub(crate) name: &'static str,
    pub(crate) short_name: &'static str,
    pub(crate) config_mode: &'static str,
}

pub(crate) const PUBLIC_CAPABILITY_DEFINITIONS: &[PublicCapabilityDefinition] = &[
    PublicCapabilityDefinition {
        name: "cache_1h",
        short_name: "1h缓存",
        config_mode: "request_param",
    },
    PublicCapabilityDefinition {
        name: "context_1m",
        short_name: "CLI 1M",
        config_mode: "request_param",
    },
    PublicCapabilityDefinition {
        name: "gemini_files",
        short_name: "文件API",
        config_mode: "request_param",
    },
];

fn capability_short_name_by_name(name: &str) -> Option<&'static str> {
    PUBLIC_CAPABILITY_DEFINITIONS
        .iter()
        .find(|capability| capability.name == name)
        .map(|capability| capability.short_name)
}

pub(crate) fn enabled_key_capability_short_names(
    capabilities: Option<&serde_json::Value>,
) -> Vec<String> {
    capabilities
        .and_then(serde_json::Value::as_object)
        .into_iter()
        .flatten()
        .filter_map(|(name, enabled)| {
            enabled.as_bool().filter(|value| *value).map(|_| {
                capability_short_name_by_name(name)
                    .unwrap_or(name.as_str())
                    .to_string()
            })
        })
        .collect()
}
