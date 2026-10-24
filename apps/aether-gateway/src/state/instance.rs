/// Resolve the stable identity of this gateway process.
///
/// The value is used as an owner/fencing identifier by background tasks and
/// distributed locks, so it must remain stable for the lifetime of the
/// process and should be unique across gateway instances.
pub(crate) fn resolve_gateway_instance_id() -> String {
    std::env::var("AETHER_GATEWAY_INSTANCE_ID")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .or_else(|| {
            std::env::var("HOSTNAME")
                .ok()
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty())
        })
        .unwrap_or_else(|| format!("gateway-{}", std::process::id()))
}
