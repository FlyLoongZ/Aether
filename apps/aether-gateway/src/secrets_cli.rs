//! `aether-gateway secrets` — rotation diagnostics and drain.
//!
//! `verify` answers one question: how many stored secrets are still sealed by a
//! key other than the configured current one. It never decrypts, so it needs no
//! purpose binding and cannot be fooled by a purpose mismatch: the envelope
//! carries the id of the key that sealed it.
//!
//! `rotate` pushes every enumerated row through the gateway's own migration
//! path. Together they form the two halves of a key rotation: `verify` proves
//! the drain is finished, `rotate` makes it finish.

use std::collections::BTreeMap;

use aether_crypto::{envelope_key_id, looks_like_python_fernet_ciphertext, SecretKey};
use aether_data::SECRET_SCAN_TARGETS;
use serde::Serialize;

use crate::state::AppState;

/// How a stored secret relates to the configured keys.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum SecretCategory {
    /// Sealed by the current key.
    Current,
    /// Sealed by `ENCRYPTION_KEY_PREVIOUS`; readable, but not yet migrated.
    Previous,
    /// A legacy envelope family the read path migrates on use.
    LegacyFormat,
    /// Not encrypted at all: the read path accepted the value verbatim.
    Plaintext,
    /// Sealed by an unknown key, or shaped like a ciphertext that cannot be read.
    Unreadable,
}

impl SecretCategory {
    fn is_residue(self) -> bool {
        !matches!(self, Self::Current)
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct SecretTargetReport {
    pub target: &'static str,
    pub rows: u64,
    /// Category counts, only for categories that occurred.
    pub categories: BTreeMap<SecretCategory, u64>,
    /// Up to five record ids per non-current category, for triage.
    pub samples: Vec<SecretSample>,
}

#[derive(Debug, Clone, Serialize)]
pub struct SecretSample {
    pub category: SecretCategory,
    pub record_id: String,
    pub field: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct SecretVerifyReport {
    /// Whether `ENCRYPTION_KEY_PREVIOUS` was configured for this run.
    pub previous_key_configured: bool,
    pub targets: Vec<SecretTargetReport>,
    pub totals: BTreeMap<SecretCategory, u64>,
}

impl SecretVerifyReport {
    pub fn residue_rows(&self) -> u64 {
        self.totals
            .iter()
            .filter(|(category, _)| category.is_residue())
            .map(|(_, rows)| *rows)
            .sum()
    }

    /// Every row the scan looked at.
    pub fn total_rows(&self) -> u64 {
        self.totals.values().sum()
    }

    fn rows_in(&self, category: SecretCategory) -> u64 {
        self.totals.get(&category).copied().unwrap_or(0)
    }

    /// Rows that a `rotate` pass can actually fix.
    pub fn rewritable_rows(&self) -> u64 {
        self.totals
            .iter()
            .filter(|(category, _)| {
                matches!(
                    category,
                    SecretCategory::Previous | SecretCategory::LegacyFormat
                )
            })
            .map(|(_, rows)| *rows)
            .sum()
    }
}

const MAX_SAMPLES_PER_CATEGORY: usize = 5;

/// Classifies a stored value against the configured keys.
///
/// `previous_key_id` is `None` when no previous key is configured, which makes
/// every non-current envelope `Unreadable` — that is the state an operator must
/// see, because nothing in the process can open those rows.
fn classify(
    value: &str,
    current_key_id: Option<&str>,
    previous_key_id: Option<&str>,
) -> SecretCategory {
    if let Some(key_id) = envelope_key_id(value) {
        if current_key_id == Some(key_id) {
            return SecretCategory::Current;
        }
        if previous_key_id == Some(key_id) {
            return SecretCategory::Previous;
        }
        return SecretCategory::Unreadable;
    }
    if crate::handlers::shared::stored_secret_uses_known_envelope_family(value)
        || looks_like_python_fernet_ciphertext(value)
    {
        return SecretCategory::LegacyFormat;
    }
    SecretCategory::Plaintext
}

/// Scans every stored secret and reports how it is sealed.
pub async fn verify_stored_secrets(
    state: &AppState,
    previous_secret: Option<&str>,
) -> Result<SecretVerifyReport, String> {
    let current_key_id = state
        .encryption_key()
        .map(|secret| SecretKey::from_secret(secret).key_id().to_string());
    let previous_key_id = previous_secret
        .map(str::trim)
        .filter(|secret| !secret.is_empty())
        .map(|secret| SecretKey::from_secret(secret).key_id().to_string());

    let rows = state
        .data
        .scan_secret_ciphertexts()
        .await
        .map_err(|error| format!("cannot enumerate stored secrets: {error}"))?;

    let mut targets = SECRET_SCAN_TARGETS
        .iter()
        .map(|target| (target, BTreeMap::<SecretCategory, u64>::new(), Vec::new()))
        .collect::<Vec<_>>();
    let mut totals: BTreeMap<SecretCategory, u64> = BTreeMap::new();
    let mut samples: BTreeMap<&'static str, BTreeMap<SecretCategory, u64>> = BTreeMap::new();

    for row in rows {
        let category = classify(
            &row.value,
            current_key_id.as_deref(),
            previous_key_id.as_deref(),
        );
        *totals.entry(category).or_default() += 1;
        let Some((_, categories, target_samples)) = targets
            .iter_mut()
            .find(|(target, _, _)| **target == row.target)
        else {
            continue;
        };
        *categories.entry(category).or_default() += 1;
        let seen = samples
            .entry(row.target)
            .or_default()
            .entry(category)
            .or_default();
        if *seen >= MAX_SAMPLES_PER_CATEGORY as u64 {
            *seen += 1;
            continue;
        }
        *seen += 1;
        target_samples.push(SecretSample {
            category,
            record_id: row.record_id,
            field: row.field,
        });
    }

    let targets = targets
        .into_iter()
        .map(|(target, categories, samples)| SecretTargetReport {
            target,
            rows: categories.values().sum(),
            categories,
            samples,
        })
        .collect();

    Ok(SecretVerifyReport {
        previous_key_configured: previous_key_id.is_some(),
        targets,
        totals,
    })
}

/// Rewrites every enumerable secret, then re-verifies.
/// What one rotation pass actually changed.
///
/// The pass pushes every row through the gateway's read path, so "the row was
/// visited" says nothing about whether it was rewritten. These three numbers
/// come from comparing two `verify` scans around the pass, which is the only
/// honest way to answer "can I drop the old key now".
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct SecretRotationOutcome {
    /// Rows that moved from residue to the current key during this pass.
    pub resealed: u64,
    /// Rows that already needed nothing.
    pub unchanged: u64,
    /// Rows still not on the current key: unreadable, or needing a human.
    pub skipped: u64,
}

impl SecretRotationOutcome {
    fn between(before: &SecretVerifyReport, after: &SecretVerifyReport) -> Self {
        let resealed = before.residue_rows().saturating_sub(after.residue_rows());
        Self {
            resealed,
            unchanged: before.rows_in(SecretCategory::Current),
            skipped: after.residue_rows(),
        }
    }
}

pub async fn rotate_stored_secrets(
    state: &AppState,
    previous_secret: Option<&str>,
) -> Result<
    (
        crate::state::SecretRotationSummary,
        SecretRotationOutcome,
        SecretVerifyReport,
    ),
    String,
> {
    let before = verify_stored_secrets(state, previous_secret).await?;
    let summary = state
        .rotate_stored_secrets()
        .await
        .map_err(|error| format!("rotation pass failed: {}", error.into_message()))?;
    let after = verify_stored_secrets(state, previous_secret).await?;
    let outcome = SecretRotationOutcome::between(&before, &after);
    Ok((summary, outcome, after))
}

pub fn render_text(report: &SecretVerifyReport) -> String {
    let mut out = String::new();
    out.push_str(&format!(
        "previous key configured: {}\n",
        if report.previous_key_configured {
            "yes"
        } else {
            "no"
        }
    ));
    for target in &report.targets {
        if target.rows == 0 {
            continue;
        }
        let details = target
            .categories
            .iter()
            .map(|(category, rows)| format!("{}={rows}", category_label(*category)))
            .collect::<Vec<_>>()
            .join(" ");
        out.push_str(&format!("{:<48} {}\n", target.target, details));
    }
    out.push_str(&format!(
        "residue rows: {} (rewritable: {})\n",
        report.residue_rows(),
        report.rewritable_rows()
    ));
    for target in &report.targets {
        for sample in &target.samples {
            if sample.category == SecretCategory::Current {
                continue;
            }
            let field = sample
                .field
                .as_deref()
                .map(|field| format!(" field={field}"))
                .unwrap_or_default();
            out.push_str(&format!(
                "  {}: {} record={}{}\n",
                category_label(sample.category),
                target.target,
                sample.record_id,
                field
            ));
        }
    }
    out
}

pub fn category_label(category: SecretCategory) -> &'static str {
    match category {
        SecretCategory::Current => "current",
        SecretCategory::Previous => "previous",
        SecretCategory::LegacyFormat => "legacy-format",
        SecretCategory::Plaintext => "plaintext",
        SecretCategory::Unreadable => "unreadable",
    }
}

pub fn render_rotation_text(
    summary: &crate::state::SecretRotationSummary,
    outcome: &SecretRotationOutcome,
    report: &SecretVerifyReport,
) -> String {
    let mut out = String::new();
    out.push_str(&format!(
        "resealed {}  unchanged {}  skipped {}\n",
        outcome.resealed, outcome.unchanged, outcome.skipped
    ));
    for (target, failed) in &summary.failed {
        out.push_str(&format!("{:<48} failed {failed}\n", target));
    }
    for failure in &summary.failures {
        out.push_str(&format!(
            "  error: {} record={} {}\n",
            failure.target, failure.record_id, failure.error
        ));
    }
    out.push_str(&render_text(report));
    out
}

pub fn render_rotation_json(
    summary: &crate::state::SecretRotationSummary,
    outcome: &SecretRotationOutcome,
    report: &SecretVerifyReport,
) -> String {
    serde_json::to_string_pretty(&serde_json::json!({
        "outcome": outcome,
        "failed": summary.failed,
        "failures": summary.failures,
        "verify": report,
    }))
    .unwrap_or_else(|error| format!("{{\"error\":\"{error}\"}}"))
}

pub fn render_verify_json(report: &SecretVerifyReport) -> String {
    serde_json::to_string_pretty(report)
        .unwrap_or_else(|error| format!("{{\"error\":\"{error}\"}}"))
}

#[cfg(test)]
mod tests {
    use super::{classify, SecretCategory};
    use aether_crypto::{SecretContext, SecretKey};

    /// End-to-end shape of a rotation: a row sealed with the old key is
    /// reported, re-sealed by `rotate`, and then readable with the new key alone.
    #[tokio::test]
    async fn rotation_reseals_a_row_and_verifies_clean() {
        use crate::GatewayDataConfig;
        use aether_crypto::{SecretContext, SecretKey};
        use aether_data::PostgresPoolConfig;
        use serde_json::json;

        const OLD_KEY: &str = "rotation-old-key-0123456789abcdef";
        const NEW_KEY: &str = "rotation-new-key-0123456789abcdef";
        const CONFIG_KEY: &str = "turnstile_secret_key";
        const PLAINTEXT: &str = "rotation-plaintext-marker";

        let server = aether_testkit::ManagedPostgresServer::start()
            .await
            .expect("temporary PostgreSQL should start");
        let config = GatewayDataConfig::from_database_config(PostgresPoolConfig {
            database_url: server.database_url().to_string(),
            min_connections: 0,
            max_connections: 4,
            ..Default::default()
        });

        // Write the row under the old key. `with_encryption_key_for_tests` would
        // install an in-memory config store, so the key goes through the data
        // config instead and the row really lands in PostgreSQL.
        let old_state = crate::AppState::new()
            .expect("app state should build")
            .with_data_config(config.clone().with_encryption_key(OLD_KEY))
            .expect("old data config should build");
        assert!(old_state
            .data
            .run_database_migrations()
            .await
            .expect("migrations should run"));
        let sealed = crate::handlers::shared::encrypt_system_config_secret(
            &old_state, CONFIG_KEY, PLAINTEXT,
        )
        .expect("old key should seal");
        old_state
            .data
            .upsert_system_config_value(CONFIG_KEY, &json!(sealed.clone()), None)
            .await
            .expect("secret should be stored");

        // Rotate with the new key, keeping the old one readable.
        let new_state = crate::AppState::new()
            .expect("app state should build")
            .with_data_config(
                config
                    .with_encryption_key(NEW_KEY)
                    .with_previous_encryption_key(OLD_KEY),
            )
            .expect("new data config should build");

        let before = super::verify_stored_secrets(&new_state, Some(OLD_KEY))
            .await
            .expect("verify should run");
        assert_eq!(
            before.totals.get(&super::SecretCategory::Previous),
            Some(&1),
            "verify report: {before:?}"
        );

        let (summary, outcome, after) = super::rotate_stored_secrets(&new_state, Some(OLD_KEY))
            .await
            .expect("rotate should run");
        assert_eq!(
            after.residue_rows(),
            0,
            "rotation should leave no residue; summary: {summary:?}"
        );
        // The counters must describe work actually done, not rows visited.
        assert_eq!(outcome.resealed, 1, "one row moved to the current key");
        assert_eq!(outcome.skipped, 0, "nothing was left behind");

        // The row must now open with the new key alone.
        let stored = new_state
            .read_system_config_json_value_strong(CONFIG_KEY)
            .await
            .expect("read should succeed")
            .and_then(|value| value.as_str().map(ToOwned::to_owned))
            .expect("secret should be stored");
        let plaintext = SecretKey::from_secret(NEW_KEY)
            .open(SecretContext::SystemConfig(CONFIG_KEY), &stored)
            .expect("new key alone must open the re-sealed row");
        assert_eq!(plaintext, PLAINTEXT);
    }

    #[test]
    fn classification_distinguishes_the_sealing_key() {
        let current = SecretKey::from_secret("current-classify-secret");
        let previous = SecretKey::from_secret("previous-classify-secret");
        let stale = previous
            .seal(SecretContext::Runtime, "rotation-marker")
            .expect("previous key should seal");

        assert_eq!(
            classify(&stale, Some(current.key_id()), Some(previous.key_id())),
            SecretCategory::Previous
        );
        assert_eq!(
            classify(&stale, Some(current.key_id()), None),
            SecretCategory::Unreadable,
            "without a previous key nothing can open this row"
        );
        let fresh = current
            .seal(SecretContext::Runtime, "rotation-marker")
            .expect("current key should seal");
        assert_eq!(
            classify(&fresh, Some(current.key_id()), None),
            SecretCategory::Current
        );
        assert_eq!(
            classify("aether-runtime-secret-v1:abc", None, None),
            SecretCategory::LegacyFormat
        );
        assert_eq!(
            classify("plain-value", None, None),
            SecretCategory::Plaintext
        );
    }
}
