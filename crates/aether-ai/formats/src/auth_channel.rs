//! Which credential channel each fixed provider type accepts, and for which api
//! formats.
//!
//! This is one table because it used to be seven. The same knowledge was
//! encoded as a Rust enum keyed by provider (`ProviderApiFormatInheritance`),
//! as a `match` in the in-memory candidate repository, and as five inline
//! `OR` chains inside the candidate-selection SQL. They had already drifted:
//! `vertex_ai` and `xai` accept `api_key` credentials at the
//! transport layer and in the candidate gate, but the inheritance enum did not
//! list `api_key`, so a key of that kind evaluated to "serves no formats". That
//! answer is not only wrong on a catalog page — the key create/update handlers
//! reconcile the key's explicit `api_formats` against it, so the scope of a
//! working key could be emptied by an unrelated provider edit.
//!
//! Two questions are answered from the same rows, and they must agree:
//!
//! - may a key of this `auth_type` inherit the provider's endpoint formats
//!   (i.e. does the channel exist for this provider at all);
//! - may that channel serve this api format.
//!
//! Everything not listed here is a non-fixed provider: any non-OAuth channel
//! may serve any format, which is the rule the SQL's `NOT IN (...)` arm and the
//! in-memory `_ =>` arm both encoded by hand.

/// One credential channel of one fixed provider type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProviderAuthChannel {
    pub provider_type: &'static str,
    pub auth_type: &'static str,
    /// The channel is only usable when the key carries a non-empty
    /// `auth_config` (for example a bearer token supplied as configuration
    /// rather than obtained through a login flow).
    pub requires_auth_config: bool,
    pub api_formats: &'static [&'static str],
}

const CODEX_FORMATS: &[&str] = &[
    "openai:responses",
    "openai:responses:compact",
    "openai:search",
    "openai:image",
    "codex:live",
];
const CLAUDE_MESSAGES: &[&str] = &["claude:messages"];
const XAI_FORMATS: &[&str] = &[
    "openai:responses",
    "openai:responses:compact",
    "openai:image",
    "openai:video",
];
const GEMINI_GENERATE_CONTENT: &[&str] = &["gemini:generate_content"];
const VERTEX_FORMATS: &[&str] = &["gemini:generate_content", "gemini:embedding"];

const fn channel(
    provider_type: &'static str,
    auth_type: &'static str,
    requires_auth_config: bool,
    api_formats: &'static [&'static str],
) -> ProviderAuthChannel {
    ProviderAuthChannel {
        provider_type,
        auth_type,
        requires_auth_config,
        api_formats,
    }
}

/// The table. One row per (provider type, credential channel).
pub const PROVIDER_AUTH_CHANNELS: &[ProviderAuthChannel] = &[
    channel("codex", "oauth", false, CODEX_FORMATS),
    channel("claude_code", "oauth", false, CLAUDE_MESSAGES),
    channel("kiro", "oauth", false, CLAUDE_MESSAGES),
    channel("kiro", "bearer", true, CLAUDE_MESSAGES),
    channel("xai", "oauth", false, XAI_FORMATS),
    channel("xai", "bearer", false, XAI_FORMATS),
    channel("xai", "api_key", false, XAI_FORMATS),
    channel("antigravity", "oauth", false, GEMINI_GENERATE_CONTENT),
    channel("vertex_ai", "api_key", false, VERTEX_FORMATS),
    channel("vertex_ai", "service_account", false, VERTEX_FORMATS),
    channel("vertex_ai", "vertex_ai", false, VERTEX_FORMATS),
];

/// Every provider type the table constrains. A provider outside this list is a
/// non-fixed provider and follows [`non_fixed_provider_channel_allows`].
pub fn provider_type_is_constrained(provider_type: &str) -> bool {
    let provider_type = normalize(provider_type);
    PROVIDER_AUTH_CHANNELS
        .iter()
        .any(|channel| channel.provider_type.eq_ignore_ascii_case(provider_type))
}

/// The channel row for this provider and credential kind, if the provider
/// accepts it at all.
pub fn provider_auth_channel(
    provider_type: &str,
    auth_type: &str,
) -> Option<&'static ProviderAuthChannel> {
    let provider_type = normalize(provider_type);
    let auth_type = normalize(auth_type);
    PROVIDER_AUTH_CHANNELS.iter().find(|channel| {
        channel.provider_type.eq_ignore_ascii_case(provider_type)
            && channel.auth_type.eq_ignore_ascii_case(auth_type)
    })
}

/// Whether a key with this credential kind may inherit its provider's endpoint
/// formats.
///
/// A provider outside the table never inherits: it has no fixed endpoint set to
/// inherit from, so its keys must declare their own `api_formats`. That is a
/// different question from [`provider_auth_channel_allows`], which asks whether
/// a key may *serve* a format — a non-fixed provider's key may, it just has to
/// say so itself.
///
/// `auth_config_configured` only matters for channels the table marks as
/// requiring configuration; other channels ignore it.
pub fn provider_accepts_auth_type(
    provider_type: &str,
    auth_type: &str,
    auth_config_configured: bool,
) -> bool {
    provider_auth_channel(provider_type, auth_type)
        .is_some_and(|channel| channel.requires_auth_config <= auth_config_configured)
}

/// Whether a key of this credential kind may serve `api_format` on this
/// provider.
///
/// Formats are explicit transport capabilities, not key permissions. Normalize
/// storage aliases, but do not let `openai:responses` imply compact or search:
/// those capabilities must have their own entries, just as in the SQL gate.
pub fn provider_auth_channel_allows(
    provider_type: &str,
    auth_type: &str,
    auth_config_configured: bool,
    api_format: &str,
) -> bool {
    if !provider_type_is_constrained(provider_type) {
        return non_fixed_provider_channel_allows(auth_type);
    }
    provider_auth_channel(provider_type, auth_type).is_some_and(|channel| {
        channel.requires_auth_config <= auth_config_configured
            && channel
                .api_formats
                .iter()
                .any(|allowed| crate::api_format_alias_matches(allowed, api_format))
    })
}

/// The rule for providers the table does not constrain: any credential except
/// an OAuth session, for any format.
///
/// OAuth sessions are excluded because they are bound to a specific provider
/// integration; a non-fixed provider has no session of its own to match.
pub fn non_fixed_provider_channel_allows(auth_type: &str) -> bool {
    !normalize(auth_type).eq_ignore_ascii_case("oauth")
}

fn normalize(value: &str) -> &str {
    value.trim()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn channel_names_are_case_insensitive_like_the_sql_gate() {
        for (provider, auth) in [("codex", "OAuth"), (" Codex ", " OAUTH ")] {
            assert!(provider_type_is_constrained(provider));
            assert!(provider_accepts_auth_type(provider, auth, false));
            assert!(provider_auth_channel_allows(
                provider,
                auth,
                false,
                "openai:responses"
            ));
        }
        assert!(!provider_auth_channel_allows(
            "CODEX",
            "API_KEY",
            false,
            "openai:responses"
        ));
        assert!(!non_fixed_provider_channel_allows(" OAuth "));
        assert!(non_fixed_provider_channel_allows(" BEARER "));
    }

    #[test]
    fn response_permissions_do_not_expand_channel_capabilities() {
        assert!(!provider_auth_channel_allows(
            "xai",
            "oauth",
            false,
            "openai:search"
        ));
        assert!(provider_auth_channel_allows(
            "codex",
            "oauth",
            false,
            "/v1/responses/compact"
        ));
    }

    #[test]
    fn channel_capabilities_match_the_exact_rows_used_by_sql() {
        let formats = PROVIDER_AUTH_CHANNELS
            .iter()
            .flat_map(|channel| channel.api_formats.iter().copied())
            .chain(["unknown:format"])
            .collect::<std::collections::BTreeSet<_>>();
        for channel in PROVIDER_AUTH_CHANNELS {
            for auth in ["oauth", "bearer", "api_key", "service_account", "vertex_ai"] {
                for configured in [false, true] {
                    for format in &formats {
                        let expected = PROVIDER_AUTH_CHANNELS.iter().any(|row| {
                            row.provider_type == channel.provider_type
                                && row.auth_type == auth
                                && (!row.requires_auth_config || configured)
                                && row.api_formats.contains(format)
                        });
                        assert_eq!(
                            provider_auth_channel_allows(
                                channel.provider_type,
                                auth,
                                configured,
                                format
                            ),
                            expected,
                            "{}/{auth}, configured={configured}, format={format}",
                            channel.provider_type
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn an_api_key_channel_inherits_its_provider_formats() {
        // The providers that had drifted: the transport and the candidate
        // gate accepted `api_key` while the inheritance rule did not.
        for (provider_type, api_format) in [
            ("vertex_ai", "gemini:generate_content"),
            ("xai", "openai:responses"),
        ] {
            assert!(
                provider_accepts_auth_type(provider_type, "api_key", false),
                "{provider_type} must accept api_key credentials"
            );
            assert!(
                provider_auth_channel_allows(provider_type, "api_key", false, api_format),
                "{provider_type} api_key must serve {api_format}"
            );
        }
    }

    #[test]
    fn a_channel_that_requires_configuration_rejects_a_bare_key() {
        assert!(!provider_accepts_auth_type("kiro", "bearer", false));
        assert!(provider_accepts_auth_type("kiro", "bearer", true));
        assert!(provider_accepts_auth_type("kiro", "oauth", false));
    }

    #[test]
    fn a_channel_only_serves_the_formats_it_lists() {
        assert!(provider_auth_channel_allows(
            "codex",
            "oauth",
            false,
            "codex:live"
        ));
        assert!(!provider_auth_channel_allows(
            "codex",
            "oauth",
            false,
            "claude:messages"
        ));
        // Compact is an explicit capability of the Codex channel.
        assert!(provider_auth_channel_allows(
            "codex",
            "oauth",
            false,
            "openai:responses:compact"
        ));
    }

    #[test]
    fn a_provider_outside_the_table_serves_any_channel_but_oauth_without_inheriting() {
        // Routing: any non-OAuth channel, any format.
        assert!(provider_auth_channel_allows(
            "custom",
            "api_key",
            false,
            "openai:chat"
        ));
        assert!(provider_auth_channel_allows(
            "custom",
            "bearer",
            false,
            "openai:chat"
        ));
        assert!(!provider_auth_channel_allows(
            "custom",
            "oauth",
            false,
            "openai:chat"
        ));
        // Inheritance: none. A non-fixed provider has no endpoint set to
        // inherit, so its keys must declare their formats.
        assert!(!provider_accepts_auth_type("custom", "api_key", false));
        assert!(!provider_accepts_auth_type("custom", "bearer", false));
    }

    #[test]
    fn every_row_names_a_provider_the_policy_treats_as_fixed() {
        // Keeps the table and `provider_runtime_policy` from drifting apart:
        // a row for a provider that is not fixed would never be consulted.
        for channel in PROVIDER_AUTH_CHANNELS {
            assert!(
                crate::auth_channel::provider_type_is_constrained(channel.provider_type),
                "{} must be constrained",
                channel.provider_type
            );
            assert!(!channel.api_formats.is_empty());
        }
    }
}
