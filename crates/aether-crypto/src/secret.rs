//! `aether-secret:v1` envelope encryption.
//!
//! New secrets use AES-256-GCM with a root key derived by
//! PBKDF2-HMAC-SHA256 and a per-message key derived with HKDF-SHA256. The
//! envelope binds its version, key id, and typed context as associated data so
//! a ciphertext cannot be moved between fields or storage domains.

use std::collections::{HashMap, VecDeque};
use std::sync::{LazyLock, RwLock};

use aws_lc_rs::aead::{Aad, LessSafeKey, Nonce, UnboundKey, AES_256_GCM, NONCE_LEN};
use aws_lc_rs::hkdf::{Salt, HKDF_SHA256};
use aws_lc_rs::rand;
use base64::engine::general_purpose::{STANDARD, URL_SAFE, URL_SAFE_NO_PAD};
use base64::Engine as _;
use sha2::{Digest, Sha256};
use zeroize::Zeroizing;

use crate::python_fernet::APP_SALT_SEED;

/// Envelope prefix. The `v1` identifies this secret scheme, not the old
/// Python-Fernet envelope.
pub const SECRET_ENVELOPE_PREFIX: &str = "aether-secret:v1:";
const SECRET_SCHEME_PREFIX: &str = "aether-secret:";
const ENVELOPE_VERSION: &[u8] = b"v1";
const HKDF_INFO: &[u8] = b"aether-secret:v1:aes-256-gcm";
const HKDF_SALT_SEED: &[u8] = b"aether-secret:v1";
const ROOT_KEY_ITERATIONS: u32 = 600_000;
const ROOT_KEY_SIZE: usize = 32;
const KEY_ID_SIZE: usize = 8;
const MAX_CACHED_ROOT_KEYS: usize = 8;
const MAX_SECRET_PLAINTEXT_BYTES: usize = 16 * 1024 * 1024;

static ROOT_KEY_CACHE: LazyLock<RwLock<RootKeyCache>> =
    LazyLock::new(|| RwLock::new(RootKeyCache::default()));

/// Process-wide previous root secret. Writes always use the configured current
/// key; reads fall back to this one so a rotation does not strand existing
/// envelopes. Only one previous key is kept: rotation is a two-step operation
/// (old -> new, then drop the old key once the rewrite has drained it).
static PREVIOUS_SECRET: LazyLock<RwLock<Option<Zeroizing<String>>>> =
    LazyLock::new(|| RwLock::new(None));

/// Registers the previous encryption secret used for read fallback.
pub fn configure_previous_secret(secret: Option<&str>) {
    let value = secret
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| Zeroizing::new(value.to_string()));
    *PREVIOUS_SECRET
        .write()
        .unwrap_or_else(|poisoned| poisoned.into_inner()) = value;
}

/// Whether a previous secret is registered for read fallback.
///
/// Diagnostics need this to tell "the row is broken" apart from "the operator
/// forgot to keep the previous key configured".
pub fn previous_secret_configured() -> bool {
    PREVIOUS_SECRET
        .read()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .is_some()
}

const APP_SALT: &[u8] = APP_SALT_SEED;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SecretContext<'a> {
    Runtime,
    SystemConfig(&'a str),
    ProviderApiKey,
    ProviderCatalogCredential,
    ProviderCatalogProxySecret,
    AuthApiKey,
    IdentityOAuthClientSecret,
    PaymentGatewaySecret,
    PaymentOrderStripeClientSecret,
    LdapBindPassword,
    SmtpPassword,
    BarkDeviceKey,
    ProxyNodeSecret,
    Custom(&'a str),
}

impl SecretContext<'_> {
    fn encode(self) -> Vec<u8> {
        let mut encoded = Vec::new();
        match self {
            Self::Runtime => encoded.push(1),
            Self::SystemConfig(key) => {
                encoded.push(2);
                push_len_prefixed(&mut encoded, key.as_bytes());
            }
            Self::ProviderApiKey => encoded.push(3),
            Self::ProviderCatalogCredential => encoded.push(4),
            Self::ProviderCatalogProxySecret => encoded.push(5),
            Self::AuthApiKey => encoded.push(6),
            Self::IdentityOAuthClientSecret => encoded.push(7),
            Self::PaymentGatewaySecret => encoded.push(8),
            Self::PaymentOrderStripeClientSecret => encoded.push(9),
            Self::LdapBindPassword => encoded.push(10),
            Self::SmtpPassword => encoded.push(11),
            Self::BarkDeviceKey => encoded.push(12),
            Self::ProxyNodeSecret => encoded.push(13),
            Self::Custom(purpose) => {
                encoded.push(255);
                push_len_prefixed(&mut encoded, purpose.as_bytes());
            }
        }
        encoded
    }
}

#[derive(Debug, thiserror::Error)]
pub enum SecretEnvelopeError {
    #[error("secret envelope is missing the aether-secret scheme prefix")]
    InvalidPrefix,
    #[error("secret envelope is malformed")]
    MalformedEnvelope,
    #[error("unsupported secret envelope version: {0}")]
    UnsupportedVersion(String),
    #[error("secret envelope key id does not match the active key")]
    KeyIdMismatch,
    #[error("secret envelope payload is invalid base64")]
    InvalidPayloadBase64,
    #[error("secret envelope payload is too short")]
    PayloadTooShort,
    #[error("secret plaintext exceeds {limit_bytes} bytes")]
    PlaintextTooLarge { limit_bytes: usize },
    #[error("secret encryption failed")]
    EncryptionFailed,
    #[error("secret decryption failed")]
    DecryptionFailed,
    #[error("secret envelope plaintext is not valid UTF-8")]
    InvalidUtf8(#[from] std::string::FromUtf8Error),
    #[error("secret random source failed")]
    RandomFailure,
}

/// Derives the same cached root key used by `aether-secret:v1` envelopes.
pub fn derive_root_key(secret: &str) -> [u8; ROOT_KEY_SIZE] {
    cached_root_key(secret)
}

/// Derives a domain-separated 32-byte subkey with HKDF-SHA256.
pub fn derive_subkey(root_key: &[u8], salt: &[u8], info: &[u8]) -> [u8; 32] {
    let salt = Salt::new(HKDF_SHA256, salt);
    let prk = salt.extract(root_key);
    let mut output = [0u8; 32];
    prk.expand(&[info], HKDF_SHA256)
        .expect("32-byte HKDF output should be valid")
        .fill(&mut output)
        .expect("32-byte HKDF output should fit");
    output
}

#[derive(Clone)]
pub struct SecretKey {
    root_key: Zeroizing<[u8; ROOT_KEY_SIZE]>,
    key_id: String,
}

impl std::fmt::Debug for SecretKey {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SecretKey")
            .field("root_key", &"[REDACTED]")
            .field("key_id", &self.key_id)
            .finish()
    }
}

impl SecretKey {
    /// Derives the root key from an application secret.
    pub fn from_secret(secret: &str) -> Self {
        let root_key = Zeroizing::new(cached_root_key(secret));
        let key_id = hex_lower(&Sha256::digest(root_key.as_ref())[..KEY_ID_SIZE]);
        Self { root_key, key_id }
    }

    pub fn key_id(&self) -> &str {
        &self.key_id
    }

    pub fn seal(
        &self,
        context: SecretContext<'_>,
        plaintext: &str,
    ) -> Result<String, SecretEnvelopeError> {
        if plaintext.len() > MAX_SECRET_PLAINTEXT_BYTES {
            return Err(SecretEnvelopeError::PlaintextTooLarge {
                limit_bytes: MAX_SECRET_PLAINTEXT_BYTES,
            });
        }

        let message_key = derive_message_key(&self.root_key);
        let key = LessSafeKey::new(
            UnboundKey::new(&AES_256_GCM, &message_key)
                .map_err(|_| SecretEnvelopeError::EncryptionFailed)?,
        );

        let mut nonce = [0u8; NONCE_LEN];
        rand::fill(&mut nonce).map_err(|_| SecretEnvelopeError::RandomFailure)?;
        let aad = associated_data(&self.key_id, context);

        let mut payload = plaintext.as_bytes().to_vec();
        key.seal_in_place_append_tag(
            Nonce::assume_unique_for_key(nonce),
            Aad::from(aad),
            &mut payload,
        )
        .map_err(|_| SecretEnvelopeError::EncryptionFailed)?;

        let mut envelope_payload = Vec::with_capacity(NONCE_LEN + payload.len());
        envelope_payload.extend_from_slice(&nonce);
        envelope_payload.extend_from_slice(&payload);
        Ok(format!(
            "{SECRET_ENVELOPE_PREFIX}{}:{}",
            self.key_id,
            URL_SAFE_NO_PAD.encode(envelope_payload)
        ))
    }

    /// Opens an envelope against a key ring.
    ///
    /// Rotation keeps one previous key readable: the caller passes the current
    /// key first and older keys afterwards, and the envelope's key id selects
    /// which one can decrypt it. Writes always use the first (current) key.
    pub fn open_with_candidates(
        candidates: &[SecretKey],
        context: SecretContext<'_>,
        envelope: &str,
    ) -> Result<String, SecretEnvelopeError> {
        let mut last_error = SecretEnvelopeError::KeyIdMismatch;
        for candidate in candidates {
            match candidate.open(context, envelope) {
                Ok(plaintext) => return Ok(plaintext),
                Err(error) => last_error = error,
            }
        }
        Err(last_error)
    }

    pub fn open(
        &self,
        context: SecretContext<'_>,
        envelope: &str,
    ) -> Result<String, SecretEnvelopeError> {
        let envelope = envelope
            .strip_prefix(SECRET_SCHEME_PREFIX)
            .ok_or(SecretEnvelopeError::InvalidPrefix)?;
        let (version, rest) = envelope
            .split_once(':')
            .ok_or(SecretEnvelopeError::MalformedEnvelope)?;
        if version != "v1" {
            return Err(SecretEnvelopeError::UnsupportedVersion(version.to_string()));
        }
        let (key_id, payload) = rest
            .split_once(':')
            .ok_or(SecretEnvelopeError::MalformedEnvelope)?;
        if key_id != self.key_id {
            return Err(SecretEnvelopeError::KeyIdMismatch);
        }
        // Bound the allocation before decoding: a hostile stored value must not
        // be able to request an unbounded buffer.
        if payload.len() > max_envelope_payload_chars() {
            return Err(SecretEnvelopeError::PlaintextTooLarge {
                limit_bytes: MAX_SECRET_PLAINTEXT_BYTES,
            });
        }
        let mut payload = URL_SAFE_NO_PAD
            .decode(payload)
            .map_err(|_| SecretEnvelopeError::InvalidPayloadBase64)?;
        if payload.len() < NONCE_LEN + AES_256_GCM.tag_len() {
            return Err(SecretEnvelopeError::PayloadTooShort);
        }

        let nonce_bytes: [u8; NONCE_LEN] = payload[..NONCE_LEN]
            .try_into()
            .map_err(|_| SecretEnvelopeError::PayloadTooShort)?;
        let mut ciphertext = payload.split_off(NONCE_LEN);
        let message_key = derive_message_key(&self.root_key);
        let key = LessSafeKey::new(
            UnboundKey::new(&AES_256_GCM, &message_key)
                .map_err(|_| SecretEnvelopeError::DecryptionFailed)?,
        );
        let aad = associated_data(&self.key_id, context);
        let plaintext = key
            .open_in_place(
                Nonce::assume_unique_for_key(nonce_bytes),
                Aad::from(aad),
                &mut ciphertext,
            )
            .map_err(|_| SecretEnvelopeError::DecryptionFailed)?;
        let plaintext = Zeroizing::new(plaintext.to_vec());
        String::from_utf8(plaintext.to_vec()).map_err(SecretEnvelopeError::InvalidUtf8)
    }

    /// Opens with the current key and, when the envelope belongs to the
    /// previous key, retries with the process-wide previous secret.
    pub fn open_with_previous(
        &self,
        context: SecretContext<'_>,
        envelope: &str,
    ) -> Result<String, SecretEnvelopeError> {
        match self.open(context, envelope) {
            Ok(plaintext) => Ok(plaintext),
            Err(SecretEnvelopeError::KeyIdMismatch) => {
                let previous = PREVIOUS_SECRET
                    .read()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .clone();
                match previous {
                    Some(previous) => {
                        SecretKey::from_secret(previous.as_str()).open(context, envelope)
                    }
                    None => Err(SecretEnvelopeError::KeyIdMismatch),
                }
            }
            Err(error) => Err(error),
        }
    }

    /// Returns true when the value already uses the `aether-secret:v1` format.
    pub fn matches_envelope(value: &str) -> bool {
        value.starts_with(SECRET_ENVELOPE_PREFIX)
    }

    /// Returns true when this key sealed the envelope.
    ///
    /// Rotation needs this: a row that still opens under the *previous* key is
    /// readable but not yet migrated, and only a key id comparison can tell.
    /// `open` deliberately does not report it, because a mismatch is an error
    /// there rather than information.
    pub fn sealed_envelope(&self, envelope: &str) -> bool {
        envelope_key_id(envelope).is_some_and(|key_id| key_id == self.key_id)
    }
}

/// Returns the key id stored in an `aether-secret:v1` envelope.
///
/// Returns `None` for anything that is not a well-formed envelope of this
/// scheme, so a legacy or foreign value is never mistaken for "sealed by some
/// other key".
pub fn envelope_key_id(envelope: &str) -> Option<&str> {
    let rest = envelope.strip_prefix(SECRET_SCHEME_PREFIX)?;
    let (version, rest) = rest.split_once(':')?;
    if version.as_bytes() != ENVELOPE_VERSION {
        return None;
    }
    let (key_id, _) = rest.split_once(':')?;
    (!key_id.is_empty()).then_some(key_id)
}

fn associated_data(key_id: &str, context: SecretContext<'_>) -> Vec<u8> {
    let mut aad = Vec::new();
    push_len_prefixed(&mut aad, ENVELOPE_VERSION);
    push_len_prefixed(&mut aad, key_id.as_bytes());
    let encoded_context = context.encode();
    push_len_prefixed(&mut aad, &encoded_context);
    aad
}

fn push_len_prefixed(output: &mut Vec<u8>, value: &[u8]) {
    let len = u32::try_from(value.len()).unwrap_or(u32::MAX);
    output.extend_from_slice(&len.to_be_bytes());
    output.extend_from_slice(value);
}

/// Upper bound on the base64 payload a `v1` envelope may carry. A plaintext at
/// the limit plus the nonce and tag is what the decoder is allowed to allocate.
fn max_envelope_payload_chars() -> usize {
    let binary = MAX_SECRET_PLAINTEXT_BYTES + NONCE_LEN + AES_256_GCM.tag_len();
    binary.div_ceil(3) * 4
}

fn derive_message_key(root_key: &[u8; ROOT_KEY_SIZE]) -> [u8; 32] {
    let salt = Sha256::digest(HKDF_SALT_SEED);
    let salt = Salt::new(HKDF_SHA256, &salt);
    let prk = salt.extract(root_key);
    let mut message_key = [0u8; 32];
    prk.expand(&[HKDF_INFO], HKDF_SHA256)
        .expect("32-byte HKDF output should be valid")
        .fill(&mut message_key)
        .expect("32-byte HKDF output should fit");
    message_key
}

fn cached_root_key(secret: &str) -> [u8; ROOT_KEY_SIZE] {
    // Cache by digest so the passphrase itself never has to stay resident in
    // the process heap for the lifetime of the entry.
    let cache_key: [u8; 32] = Sha256::digest(secret.as_bytes()).into();
    if let Some(cached) = cache_read().get(&cache_key) {
        return cached;
    }

    let root_key = decode_direct_root_key(secret).unwrap_or_else(|| {
        let mut derived = [0u8; ROOT_KEY_SIZE];
        pbkdf2::pbkdf2_hmac::<Sha256>(
            secret.as_bytes(),
            APP_SALT,
            ROOT_KEY_ITERATIONS,
            &mut derived,
        );
        derived
    });
    cache_write().insert(cache_key, root_key);
    root_key
}

fn cache_read() -> std::sync::RwLockReadGuard<'static, RootKeyCache> {
    ROOT_KEY_CACHE
        .read()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn cache_write() -> std::sync::RwLockWriteGuard<'static, RootKeyCache> {
    ROOT_KEY_CACHE
        .write()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Preserves Fernet-style key handling: a base64-encoded 32-byte secret is
/// already key material, while any other secret is treated as a passphrase.
fn decode_direct_root_key(secret: &str) -> Option<[u8; ROOT_KEY_SIZE]> {
    if secret.len() > 44 {
        return None;
    }
    let decoded = URL_SAFE
        .decode(secret)
        .or_else(|_| STANDARD.decode(secret))
        .or_else(|_| URL_SAFE_NO_PAD.decode(secret))
        .ok()?;
    decoded.as_slice().try_into().ok()
}

#[derive(Default)]
struct RootKeyCache {
    entries: HashMap<[u8; 32], [u8; ROOT_KEY_SIZE]>,
    insertion_order: VecDeque<[u8; 32]>,
}

impl RootKeyCache {
    fn get(&self, cache_key: &[u8; 32]) -> Option<[u8; ROOT_KEY_SIZE]> {
        self.entries.get(cache_key).copied()
    }

    fn insert(&mut self, cache_key: [u8; 32], root_key: [u8; ROOT_KEY_SIZE]) {
        if self.entries.contains_key(&cache_key) {
            return;
        }
        if self.entries.len() >= MAX_CACHED_ROOT_KEYS {
            if let Some(oldest) = self.insertion_order.pop_front() {
                self.entries.remove(&oldest);
            }
        }
        self.insertion_order.push_back(cache_key);
        self.entries.insert(cache_key, root_key);
    }
}

fn hex_lower(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        let _ = write!(output, "{byte:02x}");
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    const SECRET: &str = "test-secret-v2";

    #[test]
    fn round_trips_with_bound_context() {
        let key = SecretKey::from_secret(SECRET);
        let sealed = key
            .seal(SecretContext::SystemConfig("smtp_password"), "hunter2")
            .expect("seal");
        assert!(sealed.starts_with(SECRET_ENVELOPE_PREFIX));
        assert_eq!(
            key.open(SecretContext::SystemConfig("smtp_password"), &sealed)
                .expect("open"),
            "hunter2"
        );
        assert!(matches!(
            key.open(
                SecretContext::SystemConfig("backup_s3_secret_access_key"),
                &sealed
            ),
            Err(SecretEnvelopeError::DecryptionFailed)
        ));
        assert!(matches!(
            key.open(SecretContext::Runtime, &sealed),
            Err(SecretEnvelopeError::DecryptionFailed)
        ));
    }

    #[test]
    fn key_id_is_stable_and_envelope_specific() {
        let first = SecretKey::from_secret(SECRET);
        let second = SecretKey::from_secret(SECRET);
        assert_eq!(first.key_id(), second.key_id());
        assert_eq!(first.key_id().len(), KEY_ID_SIZE * 2);
        assert_ne!(first.key_id(), SecretKey::from_secret("other").key_id());
    }

    #[test]
    fn candidate_keys_decrypt_after_rotation() {
        let current = SecretKey::from_secret("current-rotation-secret");
        let previous = SecretKey::from_secret("previous-rotation-secret");
        let sealed = previous
            .seal(SecretContext::Runtime, "value")
            .expect("seal should succeed");

        let opened = SecretKey::open_with_candidates(
            &[current, SecretKey::from_secret("previous-rotation-secret")],
            SecretContext::Runtime,
            &sealed,
        )
        .expect("previous key must remain readable");
        assert_eq!(opened, "value");

        let error = SecretKey::open_with_candidates(
            &[SecretKey::from_secret("unrelated-secret")],
            SecretContext::Runtime,
            &sealed,
        )
        .expect_err("a key ring without the matching key must fail");
        assert!(matches!(error, SecretEnvelopeError::KeyIdMismatch));
    }

    #[test]
    fn oversized_payload_is_rejected_before_decoding() {
        let key = SecretKey::from_secret("oversize-secret");
        let envelope = format!(
            "{SECRET_ENVELOPE_PREFIX}{}:{}",
            key.key_id(),
            "A".repeat(max_envelope_payload_chars() + 4)
        );
        let error = key
            .open(SecretContext::Runtime, &envelope)
            .expect_err("oversized payload must be rejected");
        assert!(matches!(
            error,
            SecretEnvelopeError::PlaintextTooLarge { .. }
        ));
    }

    #[test]
    fn envelope_key_id_distinguishes_the_sealing_key() {
        let current = SecretKey::from_secret("current-rotation-secret");
        let previous = SecretKey::from_secret("previous-rotation-secret");
        let sealed = previous
            .seal(SecretContext::Runtime, "rotation-marker")
            .expect("previous key should seal");

        assert_eq!(
            envelope_key_id(&sealed),
            Some(previous.key_id()),
            "the envelope must expose the key that sealed it"
        );
        assert!(previous.sealed_envelope(&sealed));
        assert!(!current.sealed_envelope(&sealed));
        // Legacy and foreign values must not look like "sealed by a key".
        assert_eq!(envelope_key_id("aether-runtime-secret-v1:abc"), None);
        assert_eq!(envelope_key_id("plain-text-secret"), None);
    }

    #[test]
    fn rejects_wrong_key_and_unknown_envelope() {
        let sealed = SecretKey::from_secret(SECRET)
            .seal(SecretContext::Runtime, "value")
            .expect("seal");
        let other = SecretKey::from_secret("other");
        assert!(matches!(
            other.open(SecretContext::Runtime, &sealed),
            Err(SecretEnvelopeError::KeyIdMismatch)
        ));
        assert!(matches!(
            SecretKey::from_secret(SECRET).open(SecretContext::Runtime, "aether-secret:v9:x:y"),
            Err(SecretEnvelopeError::UnsupportedVersion(_))
        ));
    }

    #[test]
    fn direct_base64_secret_is_used_as_root_key() {
        let raw = [7u8; ROOT_KEY_SIZE];
        assert_eq!(
            derive_root_key(&URL_SAFE_NO_PAD.encode(raw)),
            raw,
            "unpadded base64 keys must bypass PBKDF2"
        );
        assert_eq!(
            derive_root_key(&STANDARD.encode(raw)),
            raw,
            "standard base64 keys must bypass PBKDF2"
        );
    }

    #[test]
    fn ignores_legacy_fernet_tokens() {
        assert!(!SecretKey::matches_envelope("gAAAAAB"));
        assert!(!SecretKey::matches_envelope("aether-smtp-password-v3:abc"));
    }
}
