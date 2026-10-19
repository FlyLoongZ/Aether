use std::sync::Once;

/// Installs the aws-lc-rs rustls crypto provider once per process.
///
/// The provider is process-global; repeated calls are intentionally ignored.
pub fn ensure_rustls_crypto_provider() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    });
}
