use aether_crypto::{looks_like_python_fernet_ciphertext, SecretKey};

use crate::AppState;

use super::{
    decrypt_catalog_secret_with_fallbacks, open_runtime_secret_payload, seal_runtime_secret_payload,
};

const PROVIDER_CATALOG_CREDENTIAL_ENVELOPE_FAMILY: &str = "aether-provider-catalog-credential-";
const PROVIDER_CATALOG_CREDENTIAL_ENVELOPE_V2: &str = "aether-provider-catalog-credential-v2:";
const PROVIDER_CATALOG_CREDENTIAL_PURPOSE_V2: &str = "provider-catalog-credential-bound-v2";
const RUNTIME_SECRET_ENVELOPE_FAMILY: &str = "aether-runtime-secret-";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ProviderCatalogCredentialField {
    ApiKey,
    AuthConfig,
}

impl ProviderCatalogCredentialField {
    fn label(self) -> &'static str {
        match self {
            Self::ApiKey => "api-key",
            Self::AuthConfig => "auth-config",
        }
    }
}

#[derive(Clone, PartialEq, Eq)]
pub(crate) struct ProviderCatalogCredentialProjection {
    pub(crate) plaintext: String,
    pub(crate) protected: String,
    pub(crate) migration_required: bool,
}

fn provider_catalog_credential_purpose(
    provider_id: &str,
    key_id: &str,
    field: ProviderCatalogCredentialField,
) -> Result<String, &'static str> {
    if provider_id.is_empty() {
        return Err("provider catalog credential provider_id is empty");
    }
    if key_id.is_empty() {
        return Err("provider catalog credential key_id is empty");
    }
    for value in [provider_id, key_id] {
        if value.contains('\0') {
            return Err("provider catalog credential binding contains reserved framing");
        }
    }
    Ok(format!(
        "{PROVIDER_CATALOG_CREDENTIAL_PURPOSE_V2}\0provider-id-bytes={}\0{provider_id}\0key-id-bytes={}\0{key_id}\0field={}",
        provider_id.len(),
        key_id.len(),
        field.label(),
    ))
}

pub(crate) fn seal_provider_catalog_credential(
    state: &AppState,
    provider_id: &str,
    key_id: &str,
    field: ProviderCatalogCredentialField,
    plaintext: &str,
) -> Result<String, &'static str> {
    if plaintext.contains('\0') {
        return Err("provider catalog credential contains reserved framing");
    }
    let purpose = provider_catalog_credential_purpose(provider_id, key_id, field)?;
    seal_runtime_secret_payload(state, &purpose, plaintext)
        .ok_or("provider catalog credential encryption key is not configured")
}

pub(crate) fn open_provider_catalog_credential(
    state: &AppState,
    provider_id: &str,
    key_id: &str,
    field: ProviderCatalogCredentialField,
    stored: &str,
) -> Result<ProviderCatalogCredentialProjection, &'static str> {
    let purpose = provider_catalog_credential_purpose(provider_id, key_id, field)?;
    if SecretKey::matches_envelope(stored) {
        let opened = super::open_runtime_secret_payload_with_key_state(
            state.encryption_key(),
            state.previous_encryption_key(),
            &purpose,
            stored,
        )
        .ok_or("Provider catalog credential authentication failed")?;
        if opened.plaintext.contains('\0') {
            return Err("reserved secret framing");
        }
        if !opened.sealed_with_current_key {
            // Sealed by the previous root key: re-seal so the rotation drains.
            let protected = seal_provider_catalog_credential(
                state,
                provider_id,
                key_id,
                field,
                &opened.plaintext,
            )?;
            return Ok(ProviderCatalogCredentialProjection {
                plaintext: opened.plaintext,
                protected,
                migration_required: true,
            });
        }
        return Ok(ProviderCatalogCredentialProjection {
            plaintext: opened.plaintext,
            protected: stored.to_string(),
            migration_required: false,
        });
    }
    if let Some(sealed) = stored.strip_prefix(PROVIDER_CATALOG_CREDENTIAL_ENVELOPE_V2) {
        let plaintext = open_runtime_secret_payload(state, &purpose, sealed)
            .ok_or("provider catalog credential authentication failed")?;
        if plaintext.contains('\0') {
            return Err("provider catalog credential contains reserved framing");
        }
        // The outer wrapper only distinguishes record types, and the record type
        // is already expressed by the purpose binding. Re-seal under the current
        // format so the wrapper drains on first read instead of surviving until
        // a later release stops reading it.
        let protected =
            seal_provider_catalog_credential(state, provider_id, key_id, field, &plaintext)?;
        return Ok(ProviderCatalogCredentialProjection {
            plaintext,
            protected,
            migration_required: true,
        });
    }
    if stored.starts_with(PROVIDER_CATALOG_CREDENTIAL_ENVELOPE_FAMILY) {
        return Err("unsupported provider catalog credential envelope");
    }
    if stored.starts_with(RUNTIME_SECRET_ENVELOPE_FAMILY) || stored.starts_with("aether-") {
        return Err("Aether secret envelope has the wrong record binding");
    }
    if !looks_like_python_fernet_ciphertext(stored) {
        return Err("provider catalog credential is not an authenticated ciphertext");
    }

    let plaintext = decrypt_catalog_secret_with_fallbacks(state.encryption_key(), stored)
        .ok_or("legacy provider catalog credential authentication failed")?;
    // A stripped runtime envelope decrypts to `purpose\0payload`. Rejecting
    // reserved framing prevents it from being accepted as a legacy value.
    if plaintext.contains('\0') {
        return Err("legacy provider catalog credential contains reserved framing");
    }
    let protected =
        seal_provider_catalog_credential(state, provider_id, key_id, field, &plaintext)?;
    Ok(ProviderCatalogCredentialProjection {
        plaintext,
        protected,
        migration_required: true,
    })
}

#[cfg(test)]
mod tests {
    use super::{
        open_provider_catalog_credential, seal_provider_catalog_credential,
        ProviderCatalogCredentialField, PROVIDER_CATALOG_CREDENTIAL_ENVELOPE_V2,
    };
    use crate::handlers::shared::{
        encrypt_catalog_secret_with_fallbacks, seal_runtime_secret_payload,
    };
    use crate::AppState;

    fn state_with_encryption_key() -> AppState {
        crate::tests::support::credential_fixture_state()
    }

    #[test]
    fn purpose_rejects_reserved_framing_in_the_binding() {
        // The purpose is used as associated data, so a NUL inside a component
        // could forge the framing of a different binding.
        let state = state_with_encryption_key();
        let error = seal_provider_catalog_credential(
            &state,
            "provider-1\0key-id-bytes=2\0x",
            "key-1",
            ProviderCatalogCredentialField::ApiKey,
            "secret-value",
        )
        .expect_err("reserved framing must be rejected");
        assert!(error.contains("reserved framing"), "{error}");
    }

    #[test]
    fn wrapped_v2_shell_drains_on_first_read() {
        let state = state_with_encryption_key();
        let sealed = seal_provider_catalog_credential(
            &state,
            "provider-1",
            "key-1",
            ProviderCatalogCredentialField::ApiKey,
            "secret-value",
        )
        .expect("credential should seal");
        // The wrapper was written by 2026-09 builds and is still in deployed
        // databases. A later release stops reading it, so a read must rewrite
        // the record even when the current key already sealed it.
        let wrapped = format!("{PROVIDER_CATALOG_CREDENTIAL_ENVELOPE_V2}{sealed}");
        let opened = open_provider_catalog_credential(
            &state,
            "provider-1",
            "key-1",
            ProviderCatalogCredentialField::ApiKey,
            &wrapped,
        )
        .expect("wrapped credential should open");
        assert_eq!(opened.plaintext, "secret-value");
        assert!(opened.migration_required, "the wrapper must drain on read");
        assert!(opened.protected.starts_with("aether-secret:v1:"));

        let reopened = open_provider_catalog_credential(
            &state,
            "provider-1",
            "key-1",
            ProviderCatalogCredentialField::ApiKey,
            &opened.protected,
        )
        .expect("rewritten credential should open");
        assert!(!reopened.migration_required);
    }

    #[test]
    fn v2_round_trip_binds_provider_key_and_field() {
        let state = state_with_encryption_key();
        for field in [
            ProviderCatalogCredentialField::ApiKey,
            ProviderCatalogCredentialField::AuthConfig,
        ] {
            let sealed = seal_provider_catalog_credential(
                &state,
                "provider-1",
                "key-1",
                field,
                "secret-value",
            )
            .expect("credential should seal");
            assert!(sealed.starts_with("aether-secret:v1:"));
            assert_eq!(
                open_provider_catalog_credential(&state, "provider-1", "key-1", field, &sealed,)
                    .expect("credential should open")
                    .plaintext,
                "secret-value"
            );
            assert!(open_provider_catalog_credential(
                &state,
                "provider-2",
                "key-1",
                field,
                &sealed,
            )
            .is_err());
            assert!(open_provider_catalog_credential(
                &state,
                "provider-1",
                "key-2",
                field,
                &sealed,
            )
            .is_err());
            let other_field = match field {
                ProviderCatalogCredentialField::ApiKey => {
                    ProviderCatalogCredentialField::AuthConfig
                }
                ProviderCatalogCredentialField::AuthConfig => {
                    ProviderCatalogCredentialField::ApiKey
                }
            };
            assert!(open_provider_catalog_credential(
                &state,
                "provider-1",
                "key-1",
                other_field,
                &sealed,
            )
            .is_err());
        }
    }

    #[test]
    fn v2_reader_rejects_tampering_unknown_envelopes_and_stripping() {
        let state = state_with_encryption_key();
        let sealed = seal_provider_catalog_credential(
            &state,
            "provider-1",
            "key-1",
            ProviderCatalogCredentialField::ApiKey,
            "secret-value",
        )
        .expect("credential should seal");

        let mut tampered = sealed.clone().into_bytes();
        let last = tampered
            .last_mut()
            .expect("sealed value should not be empty");
        *last = if *last == b'A' { b'B' } else { b'A' };
        let tampered = String::from_utf8(tampered).expect("ciphertext should remain UTF-8");
        assert!(open_provider_catalog_credential(
            &state,
            "provider-1",
            "key-1",
            ProviderCatalogCredentialField::ApiKey,
            &tampered,
        )
        .is_err());

        for invalid in [
            "aether-provider-catalog-credential-v3:unknown",
            "aether-payment-gateway-secret-v2:foreign",
            "plaintext-secret",
        ] {
            assert!(open_provider_catalog_credential(
                &state,
                "provider-1",
                "key-1",
                ProviderCatalogCredentialField::ApiKey,
                invalid,
            )
            .is_err());
        }
        let other_runtime = seal_runtime_secret_payload(&state, "another-purpose", "secret")
            .expect("runtime secret should seal");
        assert!(open_provider_catalog_credential(
            &state,
            "provider-1",
            "key-1",
            ProviderCatalogCredentialField::ApiKey,
            &other_runtime,
        )
        .is_err());
    }

    #[test]
    fn only_real_legacy_fernet_values_are_migrated() {
        let state = state_with_encryption_key();
        let legacy = encrypt_catalog_secret_with_fallbacks(&state, "legacy-secret")
            .expect("legacy credential should encrypt");
        let opened = open_provider_catalog_credential(
            &state,
            "provider-1",
            "key-1",
            ProviderCatalogCredentialField::AuthConfig,
            &legacy,
        )
        .expect("legacy credential should migrate");
        assert_eq!(opened.plaintext, "legacy-secret");
        assert!(opened.migration_required);
        assert!(opened.protected.starts_with("aether-secret:v1:"));
    }
}
