use crate::AppState;
use aether_crypto::{SecretContext, SecretKey};

#[cfg(test)]
use super::catalog::encrypt_catalog_secret_with_fallbacks;
use super::catalog::{
    decrypt_catalog_secret_with_fallbacks, effective_catalog_encryption_key_from_config,
};

const RUNTIME_SECRET_ENVELOPE_PREFIX: &str = "aether-runtime-secret-v1:";

pub(crate) fn seal_runtime_secret_payload(
    state: &AppState,
    purpose: &str,
    plaintext: &str,
) -> Option<String> {
    if purpose.is_empty() || plaintext.contains('\0') {
        return None;
    }
    let protected = format!("{purpose}\0{plaintext}");
    seal_runtime_secret_plaintext(state.encryption_key(), purpose, &protected)
}

pub(crate) fn seal_runtime_secret_payload_with_encryption_key(
    encryption_key: Option<&str>,
    purpose: &str,
    plaintext: &str,
) -> Option<String> {
    if purpose.is_empty() || plaintext.contains('\0') {
        return None;
    }
    let protected = format!("{purpose}\0{plaintext}");
    seal_runtime_secret_plaintext(encryption_key, purpose, &protected)
}

/// The purpose's leading segment, i.e. the record family without any binding.
///
/// Purposes embed record ids (`provider-id-bytes=…`, `key-id-bytes=…`), so only
/// the family is safe to log.
fn secret_purpose_family(purpose: &str) -> &str {
    purpose.split('\0').next().unwrap_or(purpose)
}

/// Records a secret that could not be opened.
///
/// The Prometheus counters that used to carry this signal were removed (the
/// deployment does not run Prometheus), but "a stored secret will not open" must
/// stay observable: it silently degrades a provider credential to "not
/// configured". The purpose family identifies the record kind without logging
/// the ciphertext or the binding.
pub(crate) fn warn_secret_open_failure(purpose: &str, reason: &'static str) {
    tracing::warn!(
        secret_domain = secret_purpose_family(purpose),
        reason,
        "gateway secret open failed"
    );
}

/// A decrypted runtime secret plus the rotation state of its envelope.
pub(crate) struct OpenedRuntimeSecret {
    pub(crate) plaintext: String,
    /// True when the stored envelope was sealed with the current root key.
    ///
    /// A row that opens under the previous key is readable but not migrated;
    /// callers fold this into their `migration_required` decision so a rotation
    /// drains the data instead of leaving it stranded on the old key.
    pub(crate) sealed_with_current_key: bool,
}

pub(crate) fn open_runtime_secret_payload(
    state: &AppState,
    purpose: &str,
    stored: &str,
) -> Option<String> {
    open_runtime_secret_payload_with_key_state(
        state.encryption_key(),
        state.previous_encryption_key(),
        purpose,
        stored,
    )
    .map(|opened| opened.plaintext)
}

pub(crate) fn open_runtime_secret_payload_with_encryption_key(
    encryption_key: Option<&str>,
    previous_encryption_key: Option<&str>,
    purpose: &str,
    stored: &str,
) -> Option<String> {
    open_runtime_secret_payload_with_key_state(
        encryption_key,
        previous_encryption_key,
        purpose,
        stored,
    )
    .map(|opened| opened.plaintext)
}

pub(crate) fn open_runtime_secret_payload_with_key_state(
    encryption_key: Option<&str>,
    previous_encryption_key: Option<&str>,
    purpose: &str,
    stored: &str,
) -> Option<OpenedRuntimeSecret> {
    if purpose.is_empty() {
        warn_secret_open_failure(purpose, "empty-purpose");
        return None;
    }
    let (protected, sealed_with_current_key) = if SecretKey::matches_envelope(stored) {
        let Some(encryption_key) = effective_catalog_encryption_key_from_config(encryption_key)
        else {
            warn_secret_open_failure(purpose, "missing-root-key");
            return None;
        };
        let key = SecretKey::from_secret(encryption_key.as_ref());
        // Read the key id before opening: `open_with_previous` succeeds for both
        // keys and would otherwise hide which one produced the plaintext.
        let sealed_with_current_key = key.sealed_envelope(stored);
        let plaintext = match key.open_with_previous(
            previous_encryption_key,
            SecretContext::Custom(purpose),
            stored,
        ) {
            Ok(plaintext) => plaintext,
            Err(_) => {
                warn_secret_open_failure(purpose, "authentication-failed");
                return None;
            }
        };
        (plaintext, sealed_with_current_key)
    } else {
        // Legacy prefixes carry no key id, so they always need a rewrite.
        let Some(ciphertext) = stored.strip_prefix(RUNTIME_SECRET_ENVELOPE_PREFIX) else {
            warn_secret_open_failure(purpose, "unrecognised-format");
            return None;
        };
        // A missing key or a failed open both mean "this value is unreadable".
        match decrypt_catalog_secret_with_fallbacks(encryption_key, ciphertext) {
            Some(plaintext) => (plaintext, false),
            None => {
                warn_secret_open_failure(purpose, "legacy-authentication-failed");
                return None;
            }
        }
    };
    let Some(plaintext) = protected.strip_prefix(purpose) else {
        warn_secret_open_failure(purpose, "purpose-mismatch");
        return None;
    };
    let Some(plaintext) = plaintext.strip_prefix('\0') else {
        warn_secret_open_failure(purpose, "purpose-mismatch");
        return None;
    };
    if plaintext.contains('\0') {
        warn_secret_open_failure(purpose, "reserved-framing");
        return None;
    }
    Some(OpenedRuntimeSecret {
        plaintext: plaintext.to_owned(),
        sealed_with_current_key,
    })
}

/// True when `stored` is an `aether-secret:v1` envelope sealed by a key other
/// than the configured current one.
///
/// Rotation is only observable through the key id: an envelope written under
/// the previous root key still decrypts, so every migration decision has to ask
/// *who* sealed it instead of whether decryption succeeded.
pub(crate) fn runtime_secret_uses_previous_key(encryption_key: Option<&str>, stored: &str) -> bool {
    if !SecretKey::matches_envelope(stored) {
        return false;
    }
    match effective_catalog_encryption_key_from_config(encryption_key) {
        Some(encryption_key) => {
            !SecretKey::from_secret(encryption_key.as_ref()).sealed_envelope(stored)
        }
        None => false,
    }
}

pub(crate) fn runtime_secret_payload_is_sealed(value: &str) -> bool {
    SecretKey::matches_envelope(value) || value.starts_with(RUNTIME_SECRET_ENVELOPE_PREFIX)
}

fn seal_runtime_secret_plaintext(
    encryption_key: Option<&str>,
    purpose: &str,
    protected: &str,
) -> Option<String> {
    let encryption_key = effective_catalog_encryption_key_from_config(encryption_key)?;
    let key = SecretKey::from_secret(encryption_key.as_ref());
    key.seal(SecretContext::Custom(purpose), protected).ok()
}

#[cfg(test)]
mod tests {
    use super::{open_runtime_secret_payload, seal_runtime_secret_payload};
    use crate::AppState;

    fn state_with_encryption_key() -> AppState {
        crate::tests::support::credential_fixture_state()
    }

    #[test]
    fn runtime_secret_envelope_hides_payload_and_binds_purpose() {
        let state = state_with_encryption_key();
        let payload = r#"{"pkce_verifier":"pkce-runtime-secret-marker"}"#;

        let sealed = seal_runtime_secret_payload(&state, "identity-oauth-state", payload)
            .expect("runtime secret should encrypt");

        assert!(sealed.starts_with("aether-secret:v1:"));
        assert!(!sealed.contains("pkce-runtime-secret-marker"));
        assert_eq!(
            open_runtime_secret_payload(&state, "identity-oauth-state", &sealed).as_deref(),
            Some(payload)
        );
        assert!(open_runtime_secret_payload(&state, "provider-oauth-state", &sealed).is_none());
    }

    #[test]
    fn previous_key_payloads_are_flagged_for_rewrite() {
        use aether_crypto::{SecretContext, SecretKey};

        let state = state_with_encryption_key();
        let purpose = "identity-oauth-state";

        let current = seal_runtime_secret_payload(&state, purpose, "current-key-marker")
            .expect("runtime secret should encrypt");
        let opened = super::open_runtime_secret_payload_with_key_state(
            state.encryption_key(),
            None,
            purpose,
            &current,
        )
        .expect("current key should open");
        assert!(opened.sealed_with_current_key);

        // A value sealed by another key opens only when the caller passes that
        // key, and must report that it still needs a rewrite. Nothing is
        // registered process-wide, so an unrelated caller cannot open it.
        let previous_secret = "previous-runtime-secret-for-rotation-test";
        let stale = SecretKey::from_secret(previous_secret)
            .seal(
                SecretContext::Custom(purpose),
                &format!("{purpose}\0previous-key-marker"),
            )
            .expect("previous key should seal");
        assert!(
            super::open_runtime_secret_payload_with_key_state(
                state.encryption_key(),
                None,
                purpose,
                &stale,
            )
            .is_none(),
            "a previous-key envelope must not open without the previous key"
        );
        let opened = super::open_runtime_secret_payload_with_key_state(
            state.encryption_key(),
            Some(previous_secret),
            purpose,
            &stale,
        )
        .expect("previous key should open");

        assert_eq!(opened.plaintext, "previous-key-marker");
        assert!(
            !opened.sealed_with_current_key,
            "a previous-key envelope must be flagged for rewrite"
        );
    }

    #[test]
    fn runtime_secret_reader_rejects_legacy_plaintext() {
        let state = state_with_encryption_key();
        let legacy = r#"{"pkce_verifier":"legacy-verifier"}"#;

        assert!(open_runtime_secret_payload(&state, "identity-oauth-state", legacy).is_none());
    }

    #[test]
    fn runtime_secret_envelope_requires_exact_purpose_boundary() {
        let state = state_with_encryption_key();

        assert!(seal_runtime_secret_payload(&state, "", "secret").is_none());
        assert!(seal_runtime_secret_payload(&state, "purpose", "prefix\0secret").is_none());

        // Structured, field-bound purposes intentionally contain NUL
        // separators. A shorter prefix must not be accepted as that same
        // purpose, while the complete structured purpose remains readable.
        let structured_purpose = "purpose\0prefix";
        let structured = seal_runtime_secret_payload(&state, structured_purpose, "secret")
            .expect("structured purpose should encrypt");
        assert_eq!(
            open_runtime_secret_payload(&state, structured_purpose, &structured).as_deref(),
            Some("secret")
        );
        assert!(open_runtime_secret_payload(&state, "purpose", &structured).is_none());

        // A legacy payload with an extra NUL in the plaintext is rejected;
        // the final NUL is not allowed to silently redefine the boundary.
        let protected = "purpose\0prefix\0secret";
        let ciphertext = super::encrypt_catalog_secret_with_fallbacks(&state, protected)
            .expect("historical ambiguous payload should encrypt");
        let stored = format!("{}{}", super::RUNTIME_SECRET_ENVELOPE_PREFIX, ciphertext);
        assert!(open_runtime_secret_payload(&state, "purpose", &stored).is_none());
    }
}
