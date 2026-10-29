//! Provider wire templates. Dynamic CLI versions are independent of SDK and
//! browser fingerprints; pinned templates are changed only after verification.
use aether_ai_formats::client_profile::validate_client_version;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VersionedClientIdentity {
    pub version: String,
    pub user_agent: String,
}

impl VersionedClientIdentity {
    pub fn new(version: &str, agent: &str, suffix: &str) -> Result<Self, &'static str> {
        let version = validate_client_version(version)?;
        Ok(Self {
            version: version.to_owned(),
            user_agent: format!("{agent}/{version}{suffix}"),
        })
    }
}

#[derive(Debug, Clone, Copy)]
pub struct PinnedClientIdentity {
    pub version: &'static str,
    pub user_agent: &'static str,
}

pub const GEMINI_CLI: PinnedClientIdentity = PinnedClientIdentity {
    version: "0.1.5",
    user_agent: "GeminiCLI/0.1.5 (Windows; AMD64)",
};
// Legacy standard-models fallback; dedicated Kiro requests use OAuth SDK identity.
pub const KIRO_MODELS_LEGACY_USER_AGENT: &str = "claude-code/1.0.1";

pub const ANTIGRAVITY: PinnedClientIdentity = PinnedClientIdentity {
    version: "4.3.0",
    user_agent: "vscode/1.X.X (Antigravity/4.3.0)",
};
// Kiro authentication and request adapters already share the OAuth model.
pub use aether_oauth::provider::providers::{
    DEFAULT_KIRO_VERSION, DEFAULT_NODE_VERSION, DEFAULT_SYSTEM_VERSION,
};

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn pinned_headers_and_body_versions_are_calibrated_together() {
        let identity = ANTIGRAVITY;
        assert!(identity.user_agent.contains(identity.version));
    }
}
