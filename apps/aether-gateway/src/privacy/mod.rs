use std::collections::{BTreeMap, HashMap, HashSet};
use std::convert::Infallible;
use std::fmt;
use std::io::Write;
use std::net::{Ipv4Addr, Ipv6Addr};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::{Duration, Instant};

use aether_data_contracts::DataLayerError;
use aether_runtime_state::RuntimeState;
use chrono::NaiveDate;
use hmac::{KeyInit, Mac};
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::Digest as _;

use crate::GatewayError;

type HmacSha256 = hmac::Hmac<sha2::Sha256>;

const DEFAULT_REDACTION_TTL_SECONDS: u64 = 300;
const HMAC96_BYTES: usize = 12;
const DEFAULT_SENTINEL_NAMESPACE: &str = "AETHER";
const MAX_SENTINEL_NAMESPACE_LEN: usize = 32;
const DIRECT_RESTORE_SENTINEL_LIMIT: usize = 32;
const MAX_CACHE_SENTINEL_BYTES: usize = 128;
const MAX_CACHE_RECORD_BYTES: usize = 512;
// A configured response buffer may be explicitly disabled, so restoration
// needs an independent ceiling for bytes introduced by sentinel replacement.
// This is not a response-body limit: an ordinary large response remains valid.
pub(crate) const MAX_SYNC_RESTORE_EXPANSION_BYTES: usize = 64 * 1024 * 1024;
const CHAT_PII_REDACTION_RUNTIME_CONFIG_CACHE_TTL: Duration = Duration::from_secs(5);

static EMAIL_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)[A-Z0-9._%+-]{1,64}@[A-Z0-9.-]{1,253}\.[A-Z]{2,63}")
        .expect("email regex should compile")
});
static CN_MOBILE_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?:\+?86[- ]?)?1[3-9]\d[- ]?\d{4}[- ]?\d{4}")
        .expect("cn mobile regex should compile")
});
static CN_LANDLINE_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?:\+?86[- ]?)?0\d{2,3}[- ]\d{7,8}(?:-\d{1,6})?")
        .expect("cn landline regex should compile")
});
static E164_PHONE_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"\+[1-9]\d(?:[ -]?\d){6,13}\d").expect("e164 phone regex should compile")
});
static CN_ID_REGEX: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\b\d{17}[\dX]\b").expect("cn id regex should compile"));
static PAYMENT_CARD_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"\b(?:\d[ -]?){12,18}\d\b").expect("payment card regex should compile")
});
static IPV4_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"\b(?:\d{1,3}\.){3}\d{1,3}\b").expect("ipv4 regex should compile")
});
static OPENAI_KEY_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"\bsk-(?:proj-)?[A-Za-z0-9_-]{20,}\b").expect("openai key regex should compile")
});
static ANTHROPIC_KEY_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"\bsk-ant-[A-Za-z0-9_-]{20,}\b").expect("anthropic key regex should compile")
});
static GITHUB_TOKEN_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"\b(?:gh[pousr]_[A-Za-z0-9_]{30,}|github_pat_[A-Za-z0-9_]{30,})\b")
        .expect("github token regex should compile")
});
static SLACK_TOKEN_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"\bxox[baprs]-[A-Za-z0-9-]{20,}\b").expect("slack token regex should compile")
});
static AWS_KEY_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"\b(?:AKIA|ASIA)[0-9A-Z]{16}\b").expect("aws key regex should compile")
});
static BEARER_TOKEN_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)\bBearer\s+[A-Za-z0-9._~+/=-]{20,}")
        .expect("bearer token regex should compile")
});
static JWT_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"\b[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\b")
        .expect("jwt regex should compile")
});
static ACCESS_TOKEN_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r#"(?i)\baccess[_-]?token\s*[:=]\s*["']?[A-Za-z0-9._~+/=-]{20,}"#)
        .expect("access token regex should compile")
});
static SECRET_KEY_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r#"(?i)\b(?:secret|agent[_-]?private)[_-]?key\s*[:=]\s*["']?[A-Za-z0-9._~+/=-]{20,}"#,
    )
    .expect("secret key regex should compile")
});
static HIGH_ENTROPY_TOKEN_REGEX: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\b[A-Za-z0-9_-]{32,}\b").expect("api key regex should compile"));
static SENTINEL_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"<[A-Z0-9_]+:[A-Z0-9_]+:[A-Z2-7]{20}>").expect("sentinel regex should compile")
});

#[derive(Clone, Copy, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum RedactionKind {
    Email,
    ChinaMobile,
    ChinaLandline,
    Phone,
    ChinaResidentId,
    PaymentCard,
    Ipv4,
    Ipv6,
    OpenAiKey,
    AnthropicKey,
    GitHubToken,
    SlackToken,
    AwsKey,
    BearerToken,
    Jwt,
    AccessToken,
    SecretKey,
    ApiKey,
}

impl RedactionKind {
    pub(crate) const fn label(self) -> &'static str {
        match self {
            Self::Email => "EMAIL",
            Self::ChinaMobile | Self::ChinaLandline => "CN_PHONE",
            Self::Phone => "PHONE",
            Self::ChinaResidentId => "CN_ID",
            Self::PaymentCard => "PAYMENT_CARD",
            Self::Ipv4 => "IPV4",
            Self::Ipv6 => "IPV6",
            Self::OpenAiKey => "OPENAI_KEY",
            Self::AnthropicKey => "ANTHROPIC_KEY",
            Self::GitHubToken => "GITHUB_TOKEN",
            Self::SlackToken => "SLACK_TOKEN",
            Self::AwsKey => "AWS_KEY",
            Self::BearerToken => "BEARER_TOKEN",
            Self::Jwt => "JWT",
            Self::AccessToken => "ACCESS_TOKEN",
            Self::SecretKey => "SECRET_KEY",
            Self::ApiKey => "API_KEY",
        }
    }
}

impl fmt::Debug for RedactionKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.label())
    }
}

#[derive(Clone, Debug)]
struct CompiledRedactionRule {
    rule_label: String,
    regex: Regex,
    kinds: Vec<RedactionKind>,
    custom_priority: u16,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct ChatPiiRedactionRuleConfig {
    id: String,
    name: String,
    pattern: String,
    #[serde(default)]
    enabled: bool,
    #[serde(default)]
    features: ChatPiiRedactionRuleFeatures,
    #[serde(default)]
    kind: Option<String>,
    #[serde(default)]
    system: bool,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
struct ChatPiiRedactionRuleFeatures {
    #[serde(default)]
    validator: Option<String>,
    #[serde(flatten)]
    extra: BTreeMap<String, Value>,
}

impl ChatPiiRedactionRuleConfig {
    fn validator(&self) -> Option<&str> {
        self.features
            .validator
            .as_deref()
            .or(self.kind.as_deref())
            .map(str::trim)
            .filter(|value| !value.is_empty())
    }
}

#[derive(Clone)]
pub(crate) struct RedactionSessionConfig {
    hmac_key: Vec<u8>,
    ttl_seconds: u64,
    now_unix_secs: u64,
    sentinel_namespace: String,
    enabled_kinds: Option<HashSet<RedactionKind>>,
    rules: Option<Arc<Vec<CompiledRedactionRule>>>,
}

impl RedactionSessionConfig {
    pub(crate) fn new(hmac_key: impl Into<Vec<u8>>, ttl_seconds: u64, now_unix_secs: u64) -> Self {
        Self {
            hmac_key: hmac_key.into(),
            ttl_seconds: ttl_seconds.max(1),
            now_unix_secs,
            sentinel_namespace: DEFAULT_SENTINEL_NAMESPACE.to_string(),
            enabled_kinds: None,
            rules: None,
        }
    }

    #[cfg(test)]
    pub(crate) fn default_ttl(hmac_key: impl Into<Vec<u8>>, now_unix_secs: u64) -> Self {
        Self::new(hmac_key, DEFAULT_REDACTION_TTL_SECONDS, now_unix_secs)
    }

    fn bucket(&self) -> u64 {
        self.now_unix_secs / self.ttl_seconds
    }

    fn expires_at_unix_secs(&self) -> u64 {
        self.bucket()
            .saturating_add(1)
            .saturating_mul(self.ttl_seconds)
    }

    fn with_sentinel_namespace(mut self, namespace: impl Into<String>) -> Self {
        self.sentinel_namespace = normalize_sentinel_namespace_or_default(namespace.into());
        self
    }

    fn with_rules(mut self, rules: Vec<CompiledRedactionRule>) -> Self {
        self.rules = Some(Arc::new(rules));
        self
    }

    fn kind_enabled(&self, kind: RedactionKind) -> bool {
        self.enabled_kinds
            .as_ref()
            .is_none_or(|enabled_kinds| enabled_kinds.contains(&kind))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RedactionMaskError {
    /// Provider-owned reasoning text cannot be rewritten independently from
    /// its encrypted continuation state. Reject the request instead of either
    /// leaking detected sensitive text or corrupting the provider binding.
    SensitiveOpaqueReasoningState,
}

impl From<Infallible> for RedactionMaskError {
    fn from(value: Infallible) -> Self {
        match value {}
    }
}

#[derive(Clone, Copy, Default)]
struct RedactionScanState;

impl RedactionScanState {
    fn record_scan(&mut self, _text: &str) -> Result<(), Infallible> {
        Ok(())
    }

    fn record_detections(&mut self, _count: usize) -> Result<(), Infallible> {
        Ok(())
    }
}

#[derive(Default)]
struct DetectorProbe {
    validator_calls: HashMap<RedactionKind, usize>,
}

impl DetectorProbe {
    fn record_validator_call(&mut self, kind: RedactionKind) {
        *self.validator_calls.entry(kind).or_default() += 1;
    }

    #[cfg(test)]
    fn validator_calls(&self, kind: RedactionKind) -> usize {
        self.validator_calls.get(&kind).copied().unwrap_or_default()
    }
}

impl fmt::Debug for RedactionSessionConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RedactionSessionConfig")
            .field("hmac_key", &"<redacted>")
            .field("ttl_seconds", &self.ttl_seconds)
            .field("now_unix_secs", &self.now_unix_secs)
            .field("sentinel_namespace", &self.sentinel_namespace)
            .field(
                "enabled_kind_count",
                &self.enabled_kinds.as_ref().map(HashSet::len),
            )
            .field(
                "enabled_rule_count",
                &self.rules.as_ref().map(|rules| rules.len()),
            )
            .finish()
    }
}

#[derive(Clone, Eq, Hash, PartialEq)]
struct MappingKey {
    rule_label: String,
    original: String,
}

#[derive(Clone)]
pub(crate) struct RedactionSession {
    config: RedactionSessionConfig,
    mappings: HashMap<MappingKey, RedactionMapping>,
    sentinel_index: HashMap<String, MappingKey>,
    collision_corpus: Vec<String>,
    preserve_deepseek_opaque_reasoning_state: bool,
}

impl RedactionSession {
    pub(crate) fn new(config: RedactionSessionConfig) -> Self {
        Self {
            config,
            mappings: HashMap::new(),
            sentinel_index: HashMap::new(),
            collision_corpus: Vec::new(),
            preserve_deepseek_opaque_reasoning_state: false,
        }
    }

    fn apply_mask_options(&mut self, options: MaskChatRequestOptions) {
        self.preserve_deepseek_opaque_reasoning_state =
            options.preserve_deepseek_opaque_reasoning_state;
    }

    /// Updates the provider-owned reasoning replay policy after the initial
    /// request has been planned and its upstream binding authenticated.
    ///
    /// A new WebSocket chain has to redact its first event before planning, so
    /// it starts with the conservative OpenAI item-id policy.  Once the
    /// planner has selected the provider, response restoration must use that
    /// trusted binding's policy as well; otherwise a DeepSeek opaque reasoning
    /// item could have a PII sentinel restored inside provider-owned state.
    pub(crate) fn set_reasoning_replay_policy(
        &mut self,
        policy: crate::ai_serving::OpenAiResponsesReasoningReplayPolicy,
    ) {
        self.apply_mask_options(
            MaskChatRequestOptions::runtime().with_reasoning_replay_policy(policy),
        );
    }

    fn preserves_deepseek_opaque_reasoning_state(&self) -> bool {
        self.preserve_deepseek_opaque_reasoning_state
    }

    fn set_collision_corpus(&mut self, collision_corpus: Vec<String>) {
        self.collision_corpus = collision_corpus;
    }

    fn text_has_redaction_candidate(&self, input: &str) -> bool {
        !select_non_overlapping(detect_candidates_for_session_config(
            input,
            &self.config,
            None,
        ))
        .is_empty()
    }

    #[cfg(test)]
    pub(crate) fn redact_text(&mut self, input: &str) -> RedactedText {
        self.redact_text_internal(input, None)
            .expect("unlimited redaction scan should not fail")
    }

    #[cfg(test)]
    fn redact_text_checked(
        &mut self,
        input: &str,
        scan_state: &mut RedactionScanState,
    ) -> Result<RedactedText, Infallible> {
        scan_state.record_scan(input)?;
        self.redact_text_internal(input, Some(scan_state))
    }

    async fn redact_text_with_cache(
        &mut self,
        input: &str,
        scan_state: &mut RedactionScanState,
        cache: Option<&RedisRedactionMappingCache<'_>>,
    ) -> Result<RedactedText, RedactionMaskError> {
        scan_state.record_scan(input)?;
        let candidates = select_non_overlapping(detect_candidates_for_session_config(
            input,
            &self.config,
            None,
        ));
        scan_state.record_detections(candidates.len())?;
        if candidates.is_empty() {
            return Ok(RedactedText {
                text: input.to_string(),
                matches: Vec::new(),
            });
        }

        let mut redacted = String::with_capacity(input.len());
        let mut cursor = 0;
        let mut matches = Vec::with_capacity(candidates.len());

        for candidate in candidates {
            redacted.push_str(&input[cursor..candidate.start]);
            let sentinel = self
                .sentinel_for_candidate_with_cache(input, &candidate, cache)
                .await;
            redacted.push_str(&sentinel);
            matches.push(RedactionMatch {
                rule_label: candidate.rule_label,
                kind: candidate.kind,
                start: candidate.start,
                end: candidate.end,
                original: candidate.value,
                sentinel,
            });
            cursor = candidate.end;
        }
        redacted.push_str(&input[cursor..]);

        Ok(RedactedText {
            text: redacted,
            matches,
        })
    }

    #[cfg(test)]
    fn redact_text_internal(
        &mut self,
        input: &str,
        mut scan_state: Option<&mut RedactionScanState>,
    ) -> Result<RedactedText, Infallible> {
        let candidates = select_non_overlapping(detect_candidates_for_session_config(
            input,
            &self.config,
            None,
        ));
        if let Some(scan_state) = scan_state.as_mut() {
            scan_state.record_detections(candidates.len())?;
        }
        if candidates.is_empty() {
            return Ok(RedactedText {
                text: input.to_string(),
                matches: Vec::new(),
            });
        }

        let mut redacted = String::with_capacity(input.len());
        let mut cursor = 0;
        let mut matches = Vec::with_capacity(candidates.len());

        for candidate in candidates {
            redacted.push_str(&input[cursor..candidate.start]);
            let sentinel = self.sentinel_for_candidate(input, &candidate);
            redacted.push_str(&sentinel);
            matches.push(RedactionMatch {
                rule_label: candidate.rule_label,
                kind: candidate.kind,
                start: candidate.start,
                end: candidate.end,
                original: candidate.value,
                sentinel,
            });
            cursor = candidate.end;
        }
        redacted.push_str(&input[cursor..]);

        Ok(RedactedText {
            text: redacted,
            matches,
        })
    }

    pub(crate) fn mapping_count(&self) -> usize {
        self.mappings.len()
    }

    #[cfg(test)]
    pub(crate) fn sentinel_for_original(&self, original: &str) -> Option<&str> {
        self.mappings
            .values()
            .find(|mapping| mapping.original == original)
            .map(|mapping| mapping.sentinel.as_str())
    }

    pub(crate) fn mappings(&self) -> impl Iterator<Item = &RedactionMapping> {
        self.mappings.values()
    }

    #[cfg(test)]
    fn restore_text(&self, input: &str) -> Result<RestoredText, GatewayError> {
        let matcher = SentinelMatcher::new(self);
        let mut budget = RestoreExpansionBudget::new(MAX_SYNC_RESTORE_EXPANSION_BYTES);
        let (bytes, restored) = restore_bytes_with_budget(input.as_bytes(), &matcher, &mut budget)?;
        let text = String::from_utf8(bytes).map_err(|_| {
            GatewayError::Internal(
                "redaction response restoration produced invalid UTF-8".to_string(),
            )
        })?;
        Ok(RestoredText { text, restored })
    }

    fn sentinel_for_candidate(&mut self, source_text: &str, candidate: &Candidate) -> String {
        let key = MappingKey {
            rule_label: candidate.rule_label.clone(),
            original: normalize_redaction_value(candidate.kind, &candidate.value),
        };
        if let Some(mapping) = self.mappings.get(&key) {
            return mapping.sentinel.clone();
        }

        let bucket = self.config.bucket();
        let mut collision_counter = 0u32;
        let sentinel = loop {
            let candidate_sentinel = self.build_sentinel(
                &candidate.rule_label,
                &key.original,
                bucket,
                collision_counter,
            );
            let collides_with_input =
                self.sentinel_collides_with_original_text(source_text, &candidate_sentinel);
            let collides_with_mapping = self
                .sentinel_index
                .get(&candidate_sentinel)
                .is_some_and(|existing_key| existing_key != &key);
            if !collides_with_input && !collides_with_mapping {
                break candidate_sentinel;
            }
            collision_counter = collision_counter.saturating_add(1);
        };

        let mapping = RedactionMapping {
            rule_label: candidate.rule_label.clone(),
            kind: candidate.kind,
            original: candidate.value.clone(),
            normalized_value: key.original.clone(),
            sentinel: sentinel.clone(),
            bucket,
            created_at_unix_secs: self.config.now_unix_secs,
            expires_at_unix_secs: self.config.expires_at_unix_secs(),
        };
        self.sentinel_index.insert(sentinel.clone(), key.clone());
        self.mappings.insert(key, mapping);
        sentinel
    }

    async fn sentinel_for_candidate_with_cache(
        &mut self,
        source_text: &str,
        candidate: &Candidate,
        cache: Option<&RedisRedactionMappingCache<'_>>,
    ) -> String {
        let key = MappingKey {
            rule_label: candidate.rule_label.clone(),
            original: normalize_redaction_value(candidate.kind, &candidate.value),
        };
        if let Some(mapping) = self.mappings.get(&key) {
            return mapping.sentinel.clone();
        }

        let bucket = self.config.bucket();
        if let Some(cache) = cache {
            if let Ok(Some(sentinel)) = cache
                .lookup_sentinel(
                    &candidate.rule_label,
                    &key.original,
                    bucket,
                    &self.config.sentinel_namespace,
                )
                .await
            {
                if sentinel.len() <= MAX_CACHE_SENTINEL_BYTES
                    && self.cached_sentinel_usable(source_text, &key, &sentinel)
                {
                    self.insert_mapping_for_sentinel(key, candidate, sentinel.clone(), bucket);
                    return sentinel;
                }
            }
        }

        let sentinel = self.sentinel_for_candidate(source_text, candidate);
        if let Some(cache) = cache {
            if let Some(mapping) = self.mappings.get(&key) {
                let ttl_seconds = self
                    .config
                    .expires_at_unix_secs()
                    .saturating_sub(self.config.now_unix_secs)
                    .max(1);
                let _ = cache
                    .store(
                        &RedactionCacheRecord::from_mapping(mapping),
                        &mapping.normalized_value,
                        ttl_seconds,
                    )
                    .await;
            }
        }
        sentinel
    }

    fn cached_sentinel_usable(&self, source_text: &str, key: &MappingKey, sentinel: &str) -> bool {
        if self.sentinel_collides_with_original_text(source_text, sentinel) {
            return false;
        }
        !self
            .sentinel_index
            .get(sentinel)
            .is_some_and(|existing_key| existing_key != key)
    }

    fn sentinel_collides_with_original_text(&self, source_text: &str, sentinel: &str) -> bool {
        source_text.contains(sentinel)
            || self
                .collision_corpus
                .iter()
                .any(|original_text| original_text.contains(sentinel))
    }

    fn insert_mapping_for_sentinel(
        &mut self,
        key: MappingKey,
        candidate: &Candidate,
        sentinel: String,
        bucket: u64,
    ) {
        let mapping = RedactionMapping {
            rule_label: candidate.rule_label.clone(),
            kind: candidate.kind,
            original: candidate.value.clone(),
            normalized_value: key.original.clone(),
            sentinel: sentinel.clone(),
            bucket,
            created_at_unix_secs: self.config.now_unix_secs,
            expires_at_unix_secs: self.config.expires_at_unix_secs(),
        };
        self.sentinel_index.insert(sentinel, key.clone());
        self.mappings.insert(key, mapping);
    }

    fn build_sentinel(
        &self,
        rule_label: &str,
        normalized_value: &str,
        bucket: u64,
        collision_counter: u32,
    ) -> String {
        let mut mac = HmacSha256::new_from_slice(&self.config.hmac_key)
            .expect("HMAC accepts keys of any size");
        mac.update(b"aether-redaction-v1\0");
        mac.update(rule_label.as_bytes());
        mac.update(b"\0");
        mac.update(bucket.to_string().as_bytes());
        mac.update(b"\0");
        mac.update(collision_counter.to_string().as_bytes());
        mac.update(b"\0");
        mac.update(normalized_value.as_bytes());
        let digest = mac.finalize().into_bytes();
        format!(
            "<{}:{}:{}>",
            self.config.sentinel_namespace,
            rule_label,
            base32_no_pad(&digest[..HMAC96_BYTES])
        )
    }
}

impl fmt::Debug for RedactionSession {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut counts: HashMap<&str, usize> = HashMap::new();
        for mapping in self.mappings.values() {
            *counts.entry(mapping.rule_label.as_str()).or_default() += 1;
        }
        formatter
            .debug_struct("RedactionSession")
            .field("ttl_seconds", &self.config.ttl_seconds)
            .field("bucket", &self.config.bucket())
            .field("mapping_count", &self.mappings.len())
            .field("type_counts", &counts)
            .field(
                "preserve_deepseek_opaque_reasoning_state",
                &self.preserve_deepseek_opaque_reasoning_state,
            )
            .finish()
    }
}

#[cfg(test)]
pub(crate) struct MaskedChatRequest {
    pub(crate) body: Vec<u8>,
    pub(crate) session: RedactionSession,
    pub(crate) redacted: bool,
}

#[cfg(test)]
impl fmt::Debug for MaskedChatRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MaskedChatRequest")
            .field("body_len", &self.body.len())
            .field("session", &self.session)
            .field("redacted", &self.redacted)
            .finish()
    }
}

pub(crate) struct MaskedChatRequestValue {
    pub(crate) body_json: Option<Value>,
    pub(crate) session: RedactionSession,
    pub(crate) redacted: bool,
}

impl fmt::Debug for MaskedChatRequestValue {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MaskedChatRequestValue")
            .field("body_json_owned", &self.body_json.is_some())
            .field("session", &self.session)
            .field("redacted", &self.redacted)
            .finish()
    }
}

#[derive(Clone)]
pub(crate) struct CachedRequestRedaction {
    pub(crate) body_json: Option<Value>,
    pub(crate) session: Option<RedactionSession>,
    pub(crate) redacted: bool,
}

impl CachedRequestRedaction {
    pub(crate) fn unredacted() -> Self {
        Self {
            body_json: None,
            session: None,
            redacted: false,
        }
    }

    pub(crate) fn redacted(body_json: Value, session: RedactionSession) -> Self {
        Self {
            body_json: Some(body_json),
            session: Some(session),
            redacted: true,
        }
    }
}

/// 标记：本轮客户端事件在进入 planner 之前就已经完成 PII 脱敏。
///
/// Responses WebSocket 路径会在规划前先把 `response.create` 脱敏（见
/// `handlers::proxy::websocket::responses::redaction`），planner 拿到的事件已是
/// 占位符态，无法再从 `request_redacted` 推出“这一轮真的替换过内容”。这个标记由
/// WS 路径写入 planning parts 的 extensions，planner 用它给使用记录打标
/// （`chat_pii_redaction = true`）。
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct ChatPiiRedactionTurnMarker;

#[derive(Default, Clone)]
pub(crate) struct RedactionSessionSlot {
    session: Arc<Mutex<Option<RedactionSession>>>,
    sessions_by_candidate: Arc<Mutex<HashMap<String, RedactionSession>>>,
    request_redactions: Arc<Mutex<HashMap<String, CachedRequestRedaction>>>,
}

impl RedactionSessionSlot {
    #[cfg(test)]
    pub(crate) fn put(&self, session: RedactionSession) {
        *self
            .session
            .lock()
            .expect("redaction session slot should lock") = Some(session);
    }

    pub(crate) fn put_for_candidate(&self, candidate_id: &str, session: RedactionSession) {
        self.sessions_by_candidate
            .lock()
            .expect("redaction session candidate slot should lock")
            .insert(candidate_id.to_string(), session);
    }

    pub(crate) fn take(&self) -> Option<RedactionSession> {
        self.session
            .lock()
            .expect("redaction session slot should lock")
            .take()
    }

    pub(crate) fn take_for_candidate(
        &self,
        candidate_id: Option<&str>,
    ) -> Option<RedactionSession> {
        let Some(candidate_id) = candidate_id else {
            return self.take_without_candidate_id();
        };
        let mut sessions_by_candidate = self
            .sessions_by_candidate
            .lock()
            .expect("redaction session candidate slot should lock");
        if let Some(session) = sessions_by_candidate.remove(candidate_id) {
            return Some(session);
        }
        if !sessions_by_candidate.is_empty() {
            return None;
        }
        drop(sessions_by_candidate);
        self.take()
    }

    fn take_without_candidate_id(&self) -> Option<RedactionSession> {
        let mut sessions_by_candidate = self
            .sessions_by_candidate
            .lock()
            .expect("redaction session candidate slot should lock");
        match sessions_by_candidate.len() {
            0 => {
                drop(sessions_by_candidate);
                self.take()
            }
            1 => sessions_by_candidate
                .drain()
                .next()
                .map(|(_, session)| session),
            _ => None,
        }
    }

    pub(crate) fn cached_request_redaction(&self, key: &str) -> Option<CachedRequestRedaction> {
        self.request_redactions
            .lock()
            .expect("redaction request cache slot should lock")
            .get(key)
            .cloned()
    }

    pub(crate) fn put_cached_request_redaction(
        &self,
        key: impl Into<String>,
        redaction: CachedRequestRedaction,
    ) {
        self.request_redactions
            .lock()
            .expect("redaction request cache slot should lock")
            .insert(key.into(), redaction);
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RedactionExecutionCandidateId(String);

impl RedactionExecutionCandidateId {
    pub(crate) fn new(candidate_id: impl Into<String>) -> Self {
        Self(candidate_id.into())
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ChatPiiRedactionRuntimeConfig {
    pub(crate) enabled: bool,
    rules: Vec<CompiledRedactionRule>,
    pub(crate) ttl_seconds: u64,
    placeholder_prefix: String,
}

impl Default for ChatPiiRedactionRuntimeConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            rules: default_chat_pii_redaction_rules(),
            ttl_seconds: DEFAULT_REDACTION_TTL_SECONDS,
            placeholder_prefix: DEFAULT_SENTINEL_NAMESPACE.to_string(),
        }
    }
}

impl ChatPiiRedactionRuntimeConfig {
    fn disabled() -> Self {
        Self {
            enabled: false,
            rules: Vec::new(),
            ttl_seconds: DEFAULT_REDACTION_TTL_SECONDS,
            placeholder_prefix: DEFAULT_SENTINEL_NAMESPACE.to_string(),
        }
    }
}

#[derive(Debug, Default)]
pub(crate) struct ChatPiiRedactionRuntimeConfigCache {
    value: Mutex<Option<(Instant, ChatPiiRedactionRuntimeConfig)>>,
    loading_generation: Mutex<Option<u64>>,
    generation: AtomicU64,
    notify: tokio::sync::Notify,
}

enum ChatPiiRedactionRuntimeConfigLoadRegistration {
    Leader(ChatPiiRedactionRuntimeConfigLoadGuard),
    Follower,
    Bypass,
}

struct ChatPiiRedactionRuntimeConfigLoadGuard {
    cache: ChatPiiRedactionRuntimeConfigCacheHandle,
    generation: u64,
    active: bool,
}

impl ChatPiiRedactionRuntimeConfigLoadGuard {
    fn generation(&self) -> u64 {
        self.generation
    }
}

impl Drop for ChatPiiRedactionRuntimeConfigLoadGuard {
    fn drop(&mut self) {
        if self.active {
            self.cache.finish_load(self.generation);
        }
    }
}

impl ChatPiiRedactionRuntimeConfigCache {
    fn get(&self) -> Option<ChatPiiRedactionRuntimeConfig> {
        self.value.lock().ok().and_then(|guard| {
            guard.as_ref().and_then(|(loaded_at, value)| {
                (loaded_at.elapsed() <= CHAT_PII_REDACTION_RUNTIME_CONFIG_CACHE_TTL)
                    .then(|| value.clone())
            })
        })
    }

    fn get_stale(&self) -> Option<ChatPiiRedactionRuntimeConfig> {
        self.value
            .lock()
            .ok()
            .and_then(|guard| guard.as_ref().map(|(_, value)| value.clone()))
    }

    fn insert(&self, value: ChatPiiRedactionRuntimeConfig) {
        if let Ok(mut guard) = self.value.lock() {
            *guard = Some((Instant::now(), value));
        }
    }

    fn insert_if_generation(&self, generation: u64, value: ChatPiiRedactionRuntimeConfig) {
        if self.generation.load(Ordering::Acquire) == generation {
            self.insert(value);
        }
    }

    fn register_load(self: &Arc<Self>) -> ChatPiiRedactionRuntimeConfigLoadRegistration {
        let generation = self.generation.load(Ordering::Acquire);
        match self.loading_generation.lock() {
            Ok(mut loading_generation) => {
                if loading_generation.is_some() {
                    ChatPiiRedactionRuntimeConfigLoadRegistration::Follower
                } else {
                    *loading_generation = Some(generation);
                    ChatPiiRedactionRuntimeConfigLoadRegistration::Leader(
                        ChatPiiRedactionRuntimeConfigLoadGuard {
                            cache: Arc::clone(self),
                            generation,
                            active: true,
                        },
                    )
                }
            }
            Err(_) => ChatPiiRedactionRuntimeConfigLoadRegistration::Bypass,
        }
    }

    fn notified(&self) -> tokio::sync::futures::Notified<'_> {
        self.notify.notified()
    }

    fn finish_load(&self, generation: u64) {
        let finished = self
            .loading_generation
            .lock()
            .map(|mut loading_generation| {
                if *loading_generation == Some(generation) {
                    *loading_generation = None;
                    true
                } else {
                    false
                }
            })
            .unwrap_or(false);
        if finished {
            self.notify.notify_waiters();
        }
    }
}

pub(crate) type ChatPiiRedactionRuntimeConfigCacheHandle = Arc<ChatPiiRedactionRuntimeConfigCache>;

pub(crate) fn new_chat_pii_redaction_runtime_config_cache(
) -> ChatPiiRedactionRuntimeConfigCacheHandle {
    Arc::new(ChatPiiRedactionRuntimeConfigCache::default())
}

pub(crate) fn clear_chat_pii_redaction_runtime_config_cache(
    cache: &ChatPiiRedactionRuntimeConfigCacheHandle,
) {
    cache.clear();
}

impl ChatPiiRedactionRuntimeConfigCache {
    fn clear(&self) {
        self.generation.fetch_add(1, Ordering::AcqRel);
        if let Ok(mut value) = self.value.lock() {
            *value = None;
        }
        if let Ok(mut loading_generation) = self.loading_generation.lock() {
            *loading_generation = None;
        }
        self.notify.notify_waiters();
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct MaskChatRequestOptions {
    /// This bit is derived from the selected provider configuration, never
    /// from request JSON. It permits byte-identical handling only for the
    /// provider-owned, id-less continuation state used by DeepSeek's
    /// Responses contract.
    preserve_deepseek_opaque_reasoning_state: bool,
}

impl MaskChatRequestOptions {
    pub(crate) fn runtime() -> Self {
        Self::default()
    }

    pub(crate) fn with_reasoning_replay_policy(
        mut self,
        policy: crate::ai_serving::OpenAiResponsesReasoningReplayPolicy,
    ) -> Self {
        self.preserve_deepseek_opaque_reasoning_state = matches!(
            policy,
            crate::ai_serving::OpenAiResponsesReasoningReplayPolicy::DeepSeekOpaque
        );
        self
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ChatPiiRedactionRequestFormat {
    OpenAiChat,
    OpenAiResponses,
    OpenAiSearch,
    ClaudeMessages,
}

impl ChatPiiRedactionRequestFormat {
    pub(crate) fn from_api_format(api_format: &str) -> Option<Self> {
        match api_format.trim().to_ascii_lowercase().as_str() {
            "openai:chat" => Some(Self::OpenAiChat),
            "openai:responses" | "openai:responses:compact" => Some(Self::OpenAiResponses),
            "openai:search" => Some(Self::OpenAiSearch),
            "claude:messages" => Some(Self::ClaudeMessages),
            _ => None,
        }
    }
}

fn sanitize_redaction_rule_label(raw: &str) -> String {
    let label = raw
        .trim()
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() {
                ch.to_ascii_uppercase()
            } else {
                '_'
            }
        })
        .collect::<String>();
    let label = label.trim_matches('_').to_string();
    if label.is_empty() {
        "CUSTOM_RULE".to_string()
    } else {
        label
    }
}

fn normalize_sentinel_namespace(raw: &str) -> Option<String> {
    let namespace = raw.trim();
    if namespace.is_empty() || namespace.len() > MAX_SENTINEL_NAMESPACE_LEN {
        return None;
    }
    if !namespace
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
    {
        return None;
    }
    Some(namespace.to_ascii_uppercase())
}

fn normalize_sentinel_namespace_or_default(raw: impl AsRef<str>) -> String {
    normalize_sentinel_namespace(raw.as_ref())
        .unwrap_or_else(|| DEFAULT_SENTINEL_NAMESPACE.to_string())
}

fn default_chat_pii_redaction_rule_configs() -> Vec<ChatPiiRedactionRuleConfig> {
    vec![
        ChatPiiRedactionRuleConfig {
            id: "email".to_string(),
            name: "邮箱".to_string(),
            pattern: r"(?i)[A-Z0-9._%+-]{1,64}@[A-Z0-9.-]{1,253}\.[A-Z]{2,63}".to_string(),
            enabled: true,
            features: ChatPiiRedactionRuleFeatures {
                validator: Some("email".to_string()),
                ..Default::default()
            },
            kind: None,
            system: true,
        },
        ChatPiiRedactionRuleConfig {
            id: "cn_phone".to_string(),
            name: "手机号".to_string(),
            pattern: r"(?:\+?86[- ]?)?(?:1[3-9]\d[- ]?\d{4}[- ]?\d{4}|0\d{2,3}[- ]\d{7,8}(?:-\d{1,6})?)"
                .to_string(),
            enabled: true,
            features: ChatPiiRedactionRuleFeatures {
                validator: Some("cn_phone".to_string()),
                ..Default::default()
            },
            kind: None,
            system: true,
        },
        ChatPiiRedactionRuleConfig {
            id: "global_phone".to_string(),
            name: "国际号码".to_string(),
            pattern: r"\+[1-9]\d(?:[ -]?\d){6,13}\d".to_string(),
            enabled: true,
            features: ChatPiiRedactionRuleFeatures {
                validator: Some("global_phone".to_string()),
                ..Default::default()
            },
            kind: None,
            system: true,
        },
        ChatPiiRedactionRuleConfig {
            id: "cn_id".to_string(),
            name: "身份证号".to_string(),
            pattern: r"(?i)\b\d{17}[\dX]\b".to_string(),
            enabled: true,
            features: ChatPiiRedactionRuleFeatures {
                validator: Some("cn_id".to_string()),
                ..Default::default()
            },
            kind: None,
            system: true,
        },
        ChatPiiRedactionRuleConfig {
            id: "payment_card".to_string(),
            name: "银行卡号".to_string(),
            pattern: r"\b(?:\d[ -]?){12,18}\d\b".to_string(),
            enabled: true,
            features: ChatPiiRedactionRuleFeatures {
                validator: Some("payment_card".to_string()),
                ..Default::default()
            },
            kind: None,
            system: true,
        },
        ChatPiiRedactionRuleConfig {
            id: "ipv4".to_string(),
            name: "IPv4".to_string(),
            pattern: r"\b(?:\d{1,3}\.){3}\d{1,3}\b".to_string(),
            enabled: true,
            features: ChatPiiRedactionRuleFeatures {
                validator: Some("ipv4".to_string()),
                ..Default::default()
            },
            kind: None,
            system: true,
        },
        ChatPiiRedactionRuleConfig {
            id: "ipv6".to_string(),
            name: "IPv6".to_string(),
            pattern: r"\b(?:[0-9A-Fa-f]{1,4}:){2,7}[0-9A-Fa-f:.]{1,39}\b".to_string(),
            enabled: true,
            features: ChatPiiRedactionRuleFeatures {
                validator: Some("ipv6".to_string()),
                ..Default::default()
            },
            kind: None,
            system: true,
        },
        ChatPiiRedactionRuleConfig {
            id: "api_key".to_string(),
            name: "API Key".to_string(),
            pattern:
                r"\b(?:sk-(?:proj-)?[A-Za-z0-9_-]{20,}|sk-ant-[A-Za-z0-9_-]{20,}|(?:gh[pousr]_[A-Za-z0-9_]{30,}|github_pat_[A-Za-z0-9_]{30,})|xox[baprs]-[A-Za-z0-9-]{20,}|(?:AKIA|ASIA)[0-9A-Z]{16}|[A-Za-z0-9_-]{32,})\b"
                    .to_string(),
            enabled: true,
            features: ChatPiiRedactionRuleFeatures {
                validator: Some("api_key".to_string()),
                ..Default::default()
            },
            kind: None,
            system: true,
        },
        ChatPiiRedactionRuleConfig {
            id: "access_token".to_string(),
            name: "Access Token".to_string(),
            pattern: r#"(?i)\baccess[_-]?token\s*[:=]\s*["']?[A-Za-z0-9._~+/=-]{20,}"#
                .to_string(),
            enabled: true,
            features: ChatPiiRedactionRuleFeatures {
                validator: Some("access_token".to_string()),
                ..Default::default()
            },
            kind: None,
            system: true,
        },
        ChatPiiRedactionRuleConfig {
            id: "secret_key".to_string(),
            name: "Secret Key".to_string(),
            pattern: r#"(?i)\bsecret[_-]?key\s*[:=]\s*["']?[A-Za-z0-9._~+/=-]{20,}"#
                .to_string(),
            enabled: true,
            features: ChatPiiRedactionRuleFeatures {
                validator: Some("secret_key".to_string()),
                ..Default::default()
            },
            kind: None,
            system: true,
        },
        ChatPiiRedactionRuleConfig {
            id: "bearer_token".to_string(),
            name: "Bearer Token".to_string(),
            pattern: r"(?i)\bBearer\s+[A-Za-z0-9._~+/=-]{20,}".to_string(),
            enabled: true,
            features: ChatPiiRedactionRuleFeatures {
                validator: Some("bearer_token".to_string()),
                ..Default::default()
            },
            kind: None,
            system: true,
        },
        ChatPiiRedactionRuleConfig {
            id: "jwt".to_string(),
            name: "JWT".to_string(),
            pattern: r"\b[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\b"
                .to_string(),
            enabled: true,
            features: ChatPiiRedactionRuleFeatures {
                validator: Some("jwt".to_string()),
                ..Default::default()
            },
            kind: None,
            system: true,
        },
    ]
}

fn default_chat_pii_redaction_rules() -> Vec<CompiledRedactionRule> {
    compile_chat_pii_redaction_rules(&default_chat_pii_redaction_rule_configs())
        .expect("default chat pii redaction rules should compile")
}

fn compile_chat_pii_redaction_rules(
    rules: &[ChatPiiRedactionRuleConfig],
) -> Result<Vec<CompiledRedactionRule>, GatewayError> {
    let mut compiled = Vec::new();
    for (index, rule) in rules.iter().enumerate() {
        if !rule.enabled {
            continue;
        }
        let pattern = rule.pattern.trim();
        if pattern.is_empty() {
            return Err(GatewayError::Internal(
                "chat pii redaction rule pattern is empty".to_string(),
            ));
        }
        let regex = Regex::new(pattern).map_err(|err| {
            GatewayError::Internal(format!("chat pii redaction rule regex failed: {err}"))
        })?;
        let kinds = match rule.validator() {
            Some(validator) => {
                let kinds = redaction_kinds_for_entity(validator).collect::<Vec<_>>();
                if kinds.is_empty() {
                    return Err(GatewayError::Internal(format!(
                        "unsupported chat pii redaction rule validator: {validator}"
                    )));
                }
                kinds
            }
            None => Vec::new(),
        };
        compiled.push(CompiledRedactionRule {
            rule_label: sanitize_redaction_rule_label(&rule.id),
            regex,
            kinds,
            custom_priority: (1000u16).saturating_add(index as u16),
        });
    }
    Ok(compiled)
}

fn parse_chat_pii_redaction_rules(
    value: Option<&Value>,
) -> Result<Vec<CompiledRedactionRule>, GatewayError> {
    let Some(value) = value else {
        return Ok(default_chat_pii_redaction_rules());
    };
    let Some(items) = value.as_array() else {
        return Err(GatewayError::Internal(
            "chat pii redaction rules must be an array".to_string(),
        ));
    };
    let parsed = items
        .iter()
        .cloned()
        .map(serde_json::from_value::<ChatPiiRedactionRuleConfig>)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| {
            GatewayError::Internal(format!("chat pii redaction rules invalid: {err}"))
        })?;
    compile_chat_pii_redaction_rules(&parsed)
}

pub(crate) async fn read_chat_pii_redaction_runtime_config(
    state: &crate::AppState,
) -> Result<ChatPiiRedactionRuntimeConfig, GatewayError> {
    let cache = Arc::clone(&state.chat_pii_redaction_runtime_config_cache);
    if let Some(value) = cache.get() {
        return Ok(value);
    }
    if let Some(value) = cache.get_stale() {
        if let ChatPiiRedactionRuntimeConfigLoadRegistration::Leader(guard) = cache.register_load()
        {
            spawn_chat_pii_redaction_runtime_config_refresh(state.clone(), cache, guard);
        }
        return Ok(value);
    }

    loop {
        let notified = cache.notified();
        match cache.register_load() {
            ChatPiiRedactionRuntimeConfigLoadRegistration::Bypass => {
                let value = load_chat_pii_redaction_runtime_config(state).await?;
                cache.insert(value.clone());
                return Ok(value);
            }
            ChatPiiRedactionRuntimeConfigLoadRegistration::Follower => {
                notified.await;
                if let Some(value) = cache.get() {
                    return Ok(value);
                }
            }
            ChatPiiRedactionRuntimeConfigLoadRegistration::Leader(_guard) => {
                let generation = _guard.generation();
                let value = load_chat_pii_redaction_runtime_config(state).await?;
                cache.insert_if_generation(generation, value.clone());
                return Ok(value);
            }
        }
    }
}

fn spawn_chat_pii_redaction_runtime_config_refresh(
    state: crate::AppState,
    cache: ChatPiiRedactionRuntimeConfigCacheHandle,
    guard: ChatPiiRedactionRuntimeConfigLoadGuard,
) {
    tokio::spawn(async move {
        let generation = guard.generation();
        match load_chat_pii_redaction_runtime_config(&state).await {
            Ok(value) => cache.insert_if_generation(generation, value),
            Err(err) => {
                tracing::warn!(
                    error = ?err,
                    "gateway failed to refresh chat pii redaction runtime config"
                );
            }
        }
        drop(guard);
    });
}

async fn load_chat_pii_redaction_runtime_config(
    state: &crate::AppState,
) -> Result<ChatPiiRedactionRuntimeConfig, GatewayError> {
    let enabled = state
        .read_system_config_json_value("module.chat_pii_redaction.enabled")
        .await?
        .as_ref()
        .and_then(Value::as_bool)
        .unwrap_or(false);
    if !enabled {
        return Ok(ChatPiiRedactionRuntimeConfig::disabled());
    }
    let mut config = ChatPiiRedactionRuntimeConfig::default();
    config.enabled = true;
    config.rules = parse_chat_pii_redaction_rules(
        state
            .read_system_config_json_value("module.chat_pii_redaction.rules")
            .await?
            .as_ref(),
    )?;
    config.ttl_seconds = state
        .read_system_config_json_value("module.chat_pii_redaction.cache_ttl_seconds")
        .await?
        .as_ref()
        .and_then(Value::as_u64)
        .unwrap_or(config.ttl_seconds)
        .max(1);
    config.placeholder_prefix = state
        .read_system_config_json_value("module.chat_pii_redaction.placeholder_prefix")
        .await?
        .as_ref()
        .and_then(Value::as_str)
        .and_then(normalize_sentinel_namespace)
        .unwrap_or(config.placeholder_prefix);
    Ok(config)
}

pub(crate) fn build_redaction_session_config(
    hmac_key: impl Into<Vec<u8>>,
    runtime_config: &ChatPiiRedactionRuntimeConfig,
    now_unix_secs: u64,
) -> RedactionSessionConfig {
    RedactionSessionConfig::new(hmac_key, runtime_config.ttl_seconds, now_unix_secs)
        .with_sentinel_namespace(&runtime_config.placeholder_prefix)
        .with_rules(runtime_config.rules.clone())
}

#[cfg(test)]
pub(crate) fn mask_chat_request_json_with_options(
    body: &[u8],
    config: RedactionSessionConfig,
    options: MaskChatRequestOptions,
) -> MaskedChatRequest {
    try_mask_chat_request_json_with_options(body, config, options)
        .expect("chat redaction masking should be infallible")
}

#[cfg(test)]
pub(crate) fn mask_chat_request_json(
    body: &[u8],
    config: RedactionSessionConfig,
) -> MaskedChatRequest {
    mask_chat_request_json_with_options(body, config, MaskChatRequestOptions::runtime())
}

#[cfg(test)]
pub(crate) fn try_mask_chat_request_json_with_options(
    body: &[u8],
    config: RedactionSessionConfig,
    options: MaskChatRequestOptions,
) -> Result<MaskedChatRequest, RedactionMaskError> {
    try_mask_chat_pii_request_json_with_options(
        body,
        ChatPiiRedactionRequestFormat::OpenAiChat,
        config,
        options,
    )
}

#[cfg(test)]
pub(crate) async fn try_mask_chat_request_json_with_cache_options(
    body: &[u8],
    config: RedactionSessionConfig,
    options: MaskChatRequestOptions,
    cache: Option<&RedisRedactionMappingCache<'_>>,
) -> Result<MaskedChatRequest, RedactionMaskError> {
    try_mask_chat_pii_request_json_with_cache_options(
        body,
        ChatPiiRedactionRequestFormat::OpenAiChat,
        config,
        options,
        cache,
    )
    .await
}

#[cfg(test)]
pub(crate) fn try_mask_chat_pii_request_json_with_options(
    body: &[u8],
    format: ChatPiiRedactionRequestFormat,
    config: RedactionSessionConfig,
    options: MaskChatRequestOptions,
) -> Result<MaskedChatRequest, RedactionMaskError> {
    let mut session = RedactionSession::new(config);
    session.apply_mask_options(options);
    let Ok(mut value) = serde_json::from_slice::<Value>(body) else {
        return Ok(MaskedChatRequest {
            body: body.to_vec(),
            session,
            redacted: false,
        });
    };

    session.set_collision_corpus(request_collision_corpus(format, &value));
    reject_sensitive_opaque_reasoning_state(format, &value, &session)?;
    let mut scan_state = RedactionScanState;
    let redacted = mask_request_value(format, &mut value, &mut session, &mut scan_state, options)
        .unwrap_or_else(|never| match never {});

    if !redacted {
        return Ok(MaskedChatRequest {
            body: body.to_vec(),
            session,
            redacted,
        });
    }

    let body = serde_json::to_vec(&value).unwrap_or_else(|_| body.to_vec());
    Ok(MaskedChatRequest {
        body,
        session,
        redacted,
    })
}

#[cfg(test)]
pub(crate) async fn try_mask_chat_pii_request_json_with_cache_options(
    body: &[u8],
    format: ChatPiiRedactionRequestFormat,
    config: RedactionSessionConfig,
    options: MaskChatRequestOptions,
    cache: Option<&RedisRedactionMappingCache<'_>>,
) -> Result<MaskedChatRequest, RedactionMaskError> {
    let mut session = RedactionSession::new(config);
    session.apply_mask_options(options);
    let Ok(mut value) = serde_json::from_slice::<Value>(body) else {
        return Ok(MaskedChatRequest {
            body: body.to_vec(),
            session,
            redacted: false,
        });
    };

    session.set_collision_corpus(request_collision_corpus(format, &value));
    reject_sensitive_opaque_reasoning_state(format, &value, &session)?;
    let mut scan_state = RedactionScanState;
    let redacted = mask_request_value_async(
        format,
        &mut value,
        &mut session,
        &mut scan_state,
        options,
        cache,
    )
    .await?;

    if !redacted {
        return Ok(MaskedChatRequest {
            body: body.to_vec(),
            session,
            redacted,
        });
    }

    let body = serde_json::to_vec(&value).unwrap_or_else(|_| body.to_vec());
    Ok(MaskedChatRequest {
        body,
        session,
        redacted,
    })
}

pub(crate) async fn try_mask_chat_pii_request_value_with_cache_options(
    body_json: &Value,
    format: ChatPiiRedactionRequestFormat,
    config: RedactionSessionConfig,
    options: MaskChatRequestOptions,
    cache: Option<&RedisRedactionMappingCache<'_>>,
) -> Result<MaskedChatRequestValue, RedactionMaskError> {
    let mut session = RedactionSession::new(config);
    session.apply_mask_options(options);
    let mut value = body_json.clone();

    session.set_collision_corpus(request_collision_corpus(format, &value));
    reject_sensitive_opaque_reasoning_state(format, &value, &session)?;
    let mut scan_state = RedactionScanState;
    let redacted = mask_request_value_async(
        format,
        &mut value,
        &mut session,
        &mut scan_state,
        options,
        cache,
    )
    .await?;

    Ok(MaskedChatRequestValue {
        body_json: redacted.then_some(value),
        session,
        redacted,
    })
}

fn request_collision_corpus(format: ChatPiiRedactionRequestFormat, value: &Value) -> Vec<String> {
    match format {
        ChatPiiRedactionRequestFormat::OpenAiChat => value
            .get("messages")
            .and_then(Value::as_array)
            .map(|messages| chat_message_collision_corpus(messages))
            .unwrap_or_default(),
        ChatPiiRedactionRequestFormat::OpenAiResponses => openai_responses_collision_corpus(value),
        ChatPiiRedactionRequestFormat::OpenAiSearch => openai_search_collision_corpus(value),
        ChatPiiRedactionRequestFormat::ClaudeMessages => claude_messages_collision_corpus(value),
    }
}

fn reject_sensitive_opaque_reasoning_state(
    format: ChatPiiRedactionRequestFormat,
    value: &Value,
    session: &RedactionSession,
) -> Result<(), RedactionMaskError> {
    if !session.preserves_deepseek_opaque_reasoning_state() {
        return Ok(());
    }
    if !matches!(
        format,
        ChatPiiRedactionRequestFormat::OpenAiResponses
            | ChatPiiRedactionRequestFormat::OpenAiSearch
    ) {
        return Ok(());
    }
    let Some(items) = value.get("input").and_then(Value::as_array) else {
        return Ok(());
    };
    if items.iter().any(|item| {
        item.as_object().is_some_and(|object| {
            openai_responses_object_is_opaque_reasoning_state(object)
                && opaque_reasoning_state_has_sensitive_text(item, session)
        })
    }) {
        return Err(RedactionMaskError::SensitiveOpaqueReasoningState);
    }
    Ok(())
}

fn opaque_reasoning_state_has_sensitive_text(value: &Value, session: &RedactionSession) -> bool {
    let Some(object) = value.as_object() else {
        return false;
    };
    object.iter().any(|(key, value)| {
        // The provider token is ciphertext/high-entropy state. Scanning it as
        // user text would reject essentially every valid continuation (the
        // generic API-key detector intentionally matches high-entropy data).
        // Only this exact top-level protocol field is excluded; every other
        // known or future string in the opaque item is scanned fail-closed.
        key != "encrypted_content" && json_value_has_redaction_candidate(value, session)
    })
}

fn json_value_has_redaction_candidate(value: &Value, session: &RedactionSession) -> bool {
    match value {
        Value::String(text) => session.text_has_redaction_candidate(text),
        Value::Array(values) => values
            .iter()
            .any(|value| json_value_has_redaction_candidate(value, session)),
        Value::Object(values) => values
            .values()
            .any(|value| json_value_has_redaction_candidate(value, session)),
        _ => false,
    }
}

#[cfg(test)]
fn mask_request_value(
    format: ChatPiiRedactionRequestFormat,
    value: &mut Value,
    session: &mut RedactionSession,
    scan_state: &mut RedactionScanState,
    options: MaskChatRequestOptions,
) -> Result<bool, Infallible> {
    match format {
        ChatPiiRedactionRequestFormat::OpenAiChat => {
            mask_openai_chat_request_value(value, session, scan_state, options)
        }
        ChatPiiRedactionRequestFormat::OpenAiResponses => {
            mask_openai_responses_request_value(value, session, scan_state)
        }
        ChatPiiRedactionRequestFormat::OpenAiSearch => {
            mask_openai_search_request_value(value, session, scan_state)
        }
        ChatPiiRedactionRequestFormat::ClaudeMessages => {
            mask_claude_messages_request_value(value, session, scan_state)
        }
    }
}

async fn mask_request_value_async(
    format: ChatPiiRedactionRequestFormat,
    value: &mut Value,
    session: &mut RedactionSession,
    scan_state: &mut RedactionScanState,
    options: MaskChatRequestOptions,
    cache: Option<&RedisRedactionMappingCache<'_>>,
) -> Result<bool, RedactionMaskError> {
    match format {
        ChatPiiRedactionRequestFormat::OpenAiChat => {
            mask_openai_chat_request_value_async(value, session, scan_state, options, cache).await
        }
        ChatPiiRedactionRequestFormat::OpenAiResponses => {
            mask_openai_responses_request_value_async(value, session, scan_state, cache).await
        }
        ChatPiiRedactionRequestFormat::OpenAiSearch => {
            mask_openai_search_request_value_async(value, session, scan_state, cache).await
        }
        ChatPiiRedactionRequestFormat::ClaudeMessages => {
            mask_claude_messages_request_value_async(value, session, scan_state, cache).await
        }
    }
}

#[cfg(test)]
fn mask_openai_chat_request_value(
    value: &mut Value,
    session: &mut RedactionSession,
    scan_state: &mut RedactionScanState,
    _optionss: MaskChatRequestOptions,
) -> Result<bool, Infallible> {
    let Some(messages) = value.get_mut("messages").and_then(Value::as_array_mut) else {
        return Ok(false);
    };

    let mut redacted = false;
    let mut index = 0;
    while index < messages.len() {
        let message_redacted = mask_chat_message_value(&mut messages[index], session, scan_state)?;
        redacted |= message_redacted;
        index += 1;
    }
    Ok(redacted)
}

async fn mask_openai_chat_request_value_async(
    value: &mut Value,
    session: &mut RedactionSession,
    scan_state: &mut RedactionScanState,
    _optionss: MaskChatRequestOptions,
    cache: Option<&RedisRedactionMappingCache<'_>>,
) -> Result<bool, RedactionMaskError> {
    let Some(messages) = value.get_mut("messages").and_then(Value::as_array_mut) else {
        return Ok(false);
    };

    let mut redacted = false;
    let mut index = 0;
    while index < messages.len() {
        let message_redacted =
            mask_chat_message_value_async(&mut messages[index], session, scan_state, cache).await?;
        redacted |= message_redacted;
        index += 1;
    }
    Ok(redacted)
}

fn chat_message_collision_corpus(messages: &[Value]) -> Vec<String> {
    let mut corpus = Vec::new();
    for message in messages {
        collect_chat_message_collision_text(message, &mut corpus);
    }
    corpus
}

fn collect_chat_message_collision_text(message: &Value, corpus: &mut Vec<String>) {
    let Some(message) = message.as_object() else {
        return;
    };
    if let Some(content) = message.get("content") {
        collect_chat_content_collision_text(content, corpus);
    }
    if let Some(tool_calls) = message.get("tool_calls").and_then(Value::as_array) {
        for tool_call in tool_calls {
            collect_tool_call_argument_collision_text(tool_call, corpus);
        }
    }
}

fn collect_chat_content_collision_text(content: &Value, corpus: &mut Vec<String>) {
    match content {
        Value::String(text) => corpus.push(text.clone()),
        Value::Array(parts) => {
            for part in parts {
                collect_chat_content_part_collision_text(part, corpus);
            }
        }
        _ => {}
    }
}

fn collect_chat_content_part_collision_text(part: &Value, corpus: &mut Vec<String>) {
    let Some(part) = part.as_object() else {
        return;
    };
    if part.get("type").and_then(Value::as_str) != Some("text") {
        return;
    }
    if let Some(text) = part.get("text").and_then(Value::as_str) {
        corpus.push(text.to_string());
    }
}

fn collect_tool_call_argument_collision_text(tool_call: &Value, corpus: &mut Vec<String>) {
    if let Some(arguments) = tool_call
        .get("function")
        .and_then(Value::as_object)
        .and_then(|function| function.get("arguments"))
        .and_then(Value::as_str)
    {
        corpus.push(arguments.to_string());
    }
}

fn claude_messages_collision_corpus(value: &Value) -> Vec<String> {
    let mut corpus = Vec::new();
    if let Some(system) = value.get("system") {
        collect_claude_text_content_collision_text(system, &mut corpus);
    }
    if let Some(messages) = value.get("messages").and_then(Value::as_array) {
        for message in messages {
            if let Some(content) = message.get("content") {
                collect_claude_text_content_collision_text(content, &mut corpus);
            }
        }
    }
    corpus
}

fn collect_claude_text_content_collision_text(value: &Value, corpus: &mut Vec<String>) {
    match value {
        Value::String(text) => corpus.push(text.clone()),
        Value::Array(parts) => {
            for part in parts {
                collect_claude_content_part_collision_text(part, corpus);
            }
        }
        _ => {}
    }
}

fn collect_claude_content_part_collision_text(part: &Value, corpus: &mut Vec<String>) {
    let Some(part) = part.as_object() else {
        return;
    };
    match part.get("type").and_then(Value::as_str) {
        Some("text") => {
            if let Some(text) = part.get("text").and_then(Value::as_str) {
                corpus.push(text.to_string());
            }
        }
        Some("tool_result") => {
            if let Some(content) = part.get("content") {
                collect_claude_text_content_collision_text(content, corpus);
            }
        }
        Some("tool_use") => {
            if let Some(input) = part.get("input").and_then(Value::as_str) {
                corpus.push(input.to_string());
            }
        }
        _ => {}
    }
}

fn openai_responses_collision_corpus(value: &Value) -> Vec<String> {
    let mut corpus = Vec::new();
    if let Some(instructions) = value.get("instructions").and_then(Value::as_str) {
        corpus.push(instructions.to_string());
    }
    if let Some(input) = value.get("input") {
        collect_openai_responses_input_collision_text(input, &mut corpus);
    }
    corpus
}

const OPENAI_SEARCH_COMMAND_TEXT_FIELDS: [(&str, &str); 4] = [
    ("search_query", "q"),
    ("image_query", "q"),
    ("find", "pattern"),
    ("weather", "location"),
];

fn openai_search_collision_corpus(value: &Value) -> Vec<String> {
    let mut corpus = openai_responses_collision_corpus(value);
    let Some(commands) = value.get("commands").and_then(Value::as_object) else {
        return corpus;
    };
    for (command, field) in OPENAI_SEARCH_COMMAND_TEXT_FIELDS {
        let Some(entries) = commands.get(command).and_then(Value::as_array) else {
            continue;
        };
        for entry in entries {
            if let Some(text) = entry.get(field).and_then(Value::as_str) {
                corpus.push(text.to_string());
            }
        }
    }
    corpus
}

fn collect_openai_responses_input_collision_text(value: &Value, corpus: &mut Vec<String>) {
    match value {
        Value::String(text) => corpus.push(text.clone()),
        Value::Array(items) => {
            for item in items {
                collect_openai_responses_input_item_collision_text(item, corpus);
            }
        }
        _ => {}
    }
}

fn collect_openai_responses_input_item_collision_text(item: &Value, corpus: &mut Vec<String>) {
    let Some(item) = item.as_object() else {
        return;
    };
    if let Some(content) = item.get("content") {
        collect_openai_responses_content_collision_text(content, corpus);
    }
    if response_textish_type(item.get("type").and_then(Value::as_str)) {
        if let Some(text) = item.get("text").and_then(Value::as_str) {
            corpus.push(text.to_string());
        }
    }
    if item.get("type").and_then(Value::as_str) == Some("function_call") {
        if let Some(arguments) = item.get("arguments").and_then(Value::as_str) {
            corpus.push(arguments.to_string());
        }
    }
}

fn collect_openai_responses_content_collision_text(content: &Value, corpus: &mut Vec<String>) {
    match content {
        Value::String(text) => corpus.push(text.clone()),
        Value::Array(parts) => {
            for part in parts {
                collect_openai_responses_content_part_collision_text(part, corpus);
            }
        }
        _ => {}
    }
}

fn collect_openai_responses_content_part_collision_text(part: &Value, corpus: &mut Vec<String>) {
    let Some(part) = part.as_object() else {
        return;
    };
    if !response_textish_type(part.get("type").and_then(Value::as_str)) {
        return;
    }
    if let Some(text) = part.get("text").and_then(Value::as_str) {
        corpus.push(text.to_string());
    }
}

fn response_textish_type(raw_type: Option<&str>) -> bool {
    matches!(
        raw_type,
        Some("text" | "input_text" | "output_text" | "summary_text" | "reasoning_text")
    )
}

#[cfg(test)]
fn mask_chat_message_value(
    message: &mut Value,
    session: &mut RedactionSession,
    scan_state: &mut RedactionScanState,
) -> Result<bool, Infallible> {
    let Some(message) = message.as_object_mut() else {
        return Ok(false);
    };

    let mut redacted = false;
    if let Some(content) = message.get_mut("content") {
        redacted |= mask_chat_content_value(content, session, scan_state)?;
    }
    if let Some(tool_calls) = message.get_mut("tool_calls").and_then(Value::as_array_mut) {
        for tool_call in tool_calls {
            redacted |= mask_tool_call_arguments(tool_call, session, scan_state)?;
        }
    }
    Ok(redacted)
}

#[cfg(test)]
fn mask_chat_content_value(
    content: &mut Value,
    session: &mut RedactionSession,
    scan_state: &mut RedactionScanState,
) -> Result<bool, Infallible> {
    match content {
        Value::String(text) => mask_json_string(text, session, scan_state),
        Value::Array(parts) => {
            let mut redacted = false;
            for part in parts {
                redacted |= mask_chat_content_part(part, session, scan_state)?;
            }
            Ok(redacted)
        }
        _ => Ok(false),
    }
}

#[cfg(test)]
fn mask_chat_content_part(
    part: &mut Value,
    session: &mut RedactionSession,
    scan_state: &mut RedactionScanState,
) -> Result<bool, Infallible> {
    let Some(part) = part.as_object_mut() else {
        return Ok(false);
    };
    if part.get("type").and_then(Value::as_str) != Some("text") {
        return Ok(false);
    }
    let Some(Value::String(text)) = part.get_mut("text") else {
        return Ok(false);
    };
    mask_json_string(text, session, scan_state)
}

#[cfg(test)]
fn mask_tool_call_arguments(
    tool_call: &mut Value,
    session: &mut RedactionSession,
    scan_state: &mut RedactionScanState,
) -> Result<bool, Infallible> {
    let Some(function) = tool_call.get_mut("function").and_then(Value::as_object_mut) else {
        return Ok(false);
    };
    let Some(Value::String(arguments)) = function.get_mut("arguments") else {
        return Ok(false);
    };
    mask_json_string(arguments, session, scan_state)
}

#[cfg(test)]
fn mask_claude_messages_request_value(
    value: &mut Value,
    session: &mut RedactionSession,
    scan_state: &mut RedactionScanState,
) -> Result<bool, Infallible> {
    let mut redacted = false;
    if let Some(system) = value.get_mut("system") {
        redacted |= mask_claude_text_content_value(system, session, scan_state)?;
    }
    if let Some(messages) = value.get_mut("messages").and_then(Value::as_array_mut) {
        for message in messages {
            if let Some(content) = message.get_mut("content") {
                redacted |= mask_claude_text_content_value(content, session, scan_state)?;
            }
        }
    }
    Ok(redacted)
}

#[cfg(test)]
fn mask_claude_text_content_value(
    value: &mut Value,
    session: &mut RedactionSession,
    scan_state: &mut RedactionScanState,
) -> Result<bool, Infallible> {
    match value {
        Value::String(text) => mask_json_string(text, session, scan_state),
        Value::Array(parts) => {
            let mut redacted = false;
            for part in parts {
                redacted |= mask_claude_content_part(part, session, scan_state)?;
            }
            Ok(redacted)
        }
        _ => Ok(false),
    }
}

#[cfg(test)]
fn mask_claude_content_part(
    part: &mut Value,
    session: &mut RedactionSession,
    scan_state: &mut RedactionScanState,
) -> Result<bool, Infallible> {
    let Some(part) = part.as_object_mut() else {
        return Ok(false);
    };
    match part.get("type").and_then(Value::as_str) {
        Some("text") => {
            let Some(Value::String(text)) = part.get_mut("text") else {
                return Ok(false);
            };
            mask_json_string(text, session, scan_state)
        }
        Some("tool_result") => {
            let Some(content) = part.get_mut("content") else {
                return Ok(false);
            };
            mask_claude_text_content_value(content, session, scan_state)
        }
        Some("tool_use") => {
            let Some(Value::String(input)) = part.get_mut("input") else {
                return Ok(false);
            };
            mask_json_string(input, session, scan_state)
        }
        _ => Ok(false),
    }
}

#[cfg(test)]
fn mask_openai_responses_request_value(
    value: &mut Value,
    session: &mut RedactionSession,
    scan_state: &mut RedactionScanState,
) -> Result<bool, Infallible> {
    let mut redacted = false;
    if let Some(Value::String(instructions)) = value.get_mut("instructions") {
        redacted |= mask_json_string(instructions, session, scan_state)?;
    }
    if let Some(input) = value.get_mut("input") {
        redacted |= mask_openai_responses_input_value(input, session, scan_state)?;
    }
    Ok(redacted)
}

#[cfg(test)]
fn mask_openai_search_request_value(
    value: &mut Value,
    session: &mut RedactionSession,
    scan_state: &mut RedactionScanState,
) -> Result<bool, Infallible> {
    let mut redacted = false;
    if let Some(input) = value.get_mut("input") {
        redacted |= mask_openai_responses_input_value(input, session, scan_state)?;
    }
    let Some(commands) = value.get_mut("commands").and_then(Value::as_object_mut) else {
        return Ok(redacted);
    };
    for (command, field) in OPENAI_SEARCH_COMMAND_TEXT_FIELDS {
        let Some(entries) = commands.get_mut(command).and_then(Value::as_array_mut) else {
            continue;
        };
        for entry in entries {
            if let Some(Value::String(text)) = entry.get_mut(field) {
                redacted |= mask_json_string(text, session, scan_state)?;
            }
        }
    }
    Ok(redacted)
}

#[cfg(test)]
fn mask_openai_responses_input_value(
    value: &mut Value,
    session: &mut RedactionSession,
    scan_state: &mut RedactionScanState,
) -> Result<bool, Infallible> {
    match value {
        Value::String(text) => mask_json_string(text, session, scan_state),
        Value::Array(items) => {
            let mut redacted = false;
            for item in items {
                redacted |= mask_openai_responses_input_item(item, session, scan_state)?;
            }
            Ok(redacted)
        }
        _ => Ok(false),
    }
}

#[cfg(test)]
fn mask_openai_responses_input_item(
    item: &mut Value,
    session: &mut RedactionSession,
    scan_state: &mut RedactionScanState,
) -> Result<bool, Infallible> {
    let Some(item) = item.as_object_mut() else {
        return Ok(false);
    };
    // Some Responses-compatible providers bind the clear-text reasoning
    // payload to sibling opaque continuation state. The top-level validation
    // rejects such an item when its text contains sensitive data; a safe item
    // can then remain byte-identical for provider replay. Unbound reasoning
    // text still follows the ordinary PII policy below.
    if session.preserves_deepseek_opaque_reasoning_state()
        && openai_responses_object_is_opaque_reasoning_state(item)
    {
        return Ok(false);
    }
    let mut redacted = false;
    if let Some(content) = item.get_mut("content") {
        redacted |= mask_openai_responses_content_value(content, session, scan_state)?;
    }
    if response_textish_type(item.get("type").and_then(Value::as_str)) {
        if let Some(Value::String(text)) = item.get_mut("text") {
            redacted |= mask_json_string(text, session, scan_state)?;
        }
    }
    if item.get("type").and_then(Value::as_str) == Some("function_call") {
        if let Some(Value::String(arguments)) = item.get_mut("arguments") {
            redacted |= mask_json_string(arguments, session, scan_state)?;
        }
    }
    Ok(redacted)
}

#[cfg(test)]
fn mask_openai_responses_content_value(
    content: &mut Value,
    session: &mut RedactionSession,
    scan_state: &mut RedactionScanState,
) -> Result<bool, Infallible> {
    match content {
        Value::String(text) => mask_json_string(text, session, scan_state),
        Value::Array(parts) => {
            let mut redacted = false;
            for part in parts {
                redacted |= mask_openai_responses_content_part(part, session, scan_state)?;
            }
            Ok(redacted)
        }
        _ => Ok(false),
    }
}

#[cfg(test)]
fn mask_openai_responses_content_part(
    part: &mut Value,
    session: &mut RedactionSession,
    scan_state: &mut RedactionScanState,
) -> Result<bool, Infallible> {
    let Some(part) = part.as_object_mut() else {
        return Ok(false);
    };
    if !response_textish_type(part.get("type").and_then(Value::as_str)) {
        return Ok(false);
    }
    let Some(Value::String(text)) = part.get_mut("text") else {
        return Ok(false);
    };
    mask_json_string(text, session, scan_state)
}

#[cfg(test)]
fn mask_json_string(
    text: &mut String,
    session: &mut RedactionSession,
    scan_state: &mut RedactionScanState,
) -> Result<bool, Infallible> {
    let redacted = session.redact_text_checked(text, scan_state)?;
    if redacted.matches.is_empty() {
        return Ok(false);
    }
    *text = redacted.text;
    Ok(true)
}

async fn mask_chat_message_value_async(
    message: &mut Value,
    session: &mut RedactionSession,
    scan_state: &mut RedactionScanState,
    cache: Option<&RedisRedactionMappingCache<'_>>,
) -> Result<bool, RedactionMaskError> {
    let Some(message) = message.as_object_mut() else {
        return Ok(false);
    };

    let mut redacted = false;
    if let Some(content) = message.get_mut("content") {
        redacted |= mask_chat_content_value_async(content, session, scan_state, cache).await?;
    }
    if let Some(tool_calls) = message.get_mut("tool_calls").and_then(Value::as_array_mut) {
        for tool_call in tool_calls {
            redacted |=
                mask_tool_call_arguments_async(tool_call, session, scan_state, cache).await?;
        }
    }
    Ok(redacted)
}

async fn mask_chat_content_value_async(
    content: &mut Value,
    session: &mut RedactionSession,
    scan_state: &mut RedactionScanState,
    cache: Option<&RedisRedactionMappingCache<'_>>,
) -> Result<bool, RedactionMaskError> {
    match content {
        Value::String(text) => mask_json_string_async(text, session, scan_state, cache).await,
        Value::Array(parts) => {
            let mut redacted = false;
            for part in parts {
                redacted |= mask_chat_content_part_async(part, session, scan_state, cache).await?;
            }
            Ok(redacted)
        }
        _ => Ok(false),
    }
}

async fn mask_chat_content_part_async(
    part: &mut Value,
    session: &mut RedactionSession,
    scan_state: &mut RedactionScanState,
    cache: Option<&RedisRedactionMappingCache<'_>>,
) -> Result<bool, RedactionMaskError> {
    let Some(part) = part.as_object_mut() else {
        return Ok(false);
    };
    if part.get("type").and_then(Value::as_str) != Some("text") {
        return Ok(false);
    }
    let Some(Value::String(text)) = part.get_mut("text") else {
        return Ok(false);
    };
    mask_json_string_async(text, session, scan_state, cache).await
}

async fn mask_tool_call_arguments_async(
    tool_call: &mut Value,
    session: &mut RedactionSession,
    scan_state: &mut RedactionScanState,
    cache: Option<&RedisRedactionMappingCache<'_>>,
) -> Result<bool, RedactionMaskError> {
    let Some(function) = tool_call.get_mut("function").and_then(Value::as_object_mut) else {
        return Ok(false);
    };
    let Some(Value::String(arguments)) = function.get_mut("arguments") else {
        return Ok(false);
    };
    mask_json_string_async(arguments, session, scan_state, cache).await
}

async fn mask_claude_messages_request_value_async(
    value: &mut Value,
    session: &mut RedactionSession,
    scan_state: &mut RedactionScanState,
    cache: Option<&RedisRedactionMappingCache<'_>>,
) -> Result<bool, RedactionMaskError> {
    let mut redacted = false;
    if let Some(system) = value.get_mut("system") {
        redacted |=
            mask_claude_text_content_value_async(system, session, scan_state, cache).await?;
    }
    if let Some(messages) = value.get_mut("messages").and_then(Value::as_array_mut) {
        for message in messages {
            if let Some(content) = message.get_mut("content") {
                redacted |=
                    mask_claude_text_content_value_async(content, session, scan_state, cache)
                        .await?;
            }
        }
    }
    Ok(redacted)
}

async fn mask_claude_text_content_value_async(
    value: &mut Value,
    session: &mut RedactionSession,
    scan_state: &mut RedactionScanState,
    cache: Option<&RedisRedactionMappingCache<'_>>,
) -> Result<bool, RedactionMaskError> {
    match value {
        Value::String(text) => mask_json_string_async(text, session, scan_state, cache).await,
        Value::Array(parts) => {
            let mut redacted = false;
            for part in parts {
                redacted |=
                    mask_claude_content_part_async(part, session, scan_state, cache).await?;
            }
            Ok(redacted)
        }
        _ => Ok(false),
    }
}

async fn mask_claude_content_part_async(
    part: &mut Value,
    session: &mut RedactionSession,
    scan_state: &mut RedactionScanState,
    cache: Option<&RedisRedactionMappingCache<'_>>,
) -> Result<bool, RedactionMaskError> {
    let Some(part) = part.as_object_mut() else {
        return Ok(false);
    };
    match part.get("type").and_then(Value::as_str) {
        Some("text") => {
            let Some(Value::String(text)) = part.get_mut("text") else {
                return Ok(false);
            };
            mask_json_string_async(text, session, scan_state, cache).await
        }
        Some("tool_result") => {
            let Some(content) = part.get_mut("content") else {
                return Ok(false);
            };
            mask_claude_tool_result_content_value_async(content, session, scan_state, cache).await
        }
        Some("tool_use") => {
            let Some(Value::String(input)) = part.get_mut("input") else {
                return Ok(false);
            };
            mask_json_string_async(input, session, scan_state, cache).await
        }
        _ => Ok(false),
    }
}

async fn mask_claude_tool_result_content_value_async(
    value: &mut Value,
    session: &mut RedactionSession,
    scan_state: &mut RedactionScanState,
    cache: Option<&RedisRedactionMappingCache<'_>>,
) -> Result<bool, RedactionMaskError> {
    match value {
        Value::String(text) => mask_json_string_async(text, session, scan_state, cache).await,
        Value::Array(parts) => {
            let mut redacted = false;
            for part in parts {
                let Some(part) = part.as_object_mut() else {
                    continue;
                };
                if part.get("type").and_then(Value::as_str) != Some("text") {
                    continue;
                }
                if let Some(Value::String(text)) = part.get_mut("text") {
                    redacted |= mask_json_string_async(text, session, scan_state, cache).await?;
                }
            }
            Ok(redacted)
        }
        _ => Ok(false),
    }
}

async fn mask_openai_responses_request_value_async(
    value: &mut Value,
    session: &mut RedactionSession,
    scan_state: &mut RedactionScanState,
    cache: Option<&RedisRedactionMappingCache<'_>>,
) -> Result<bool, RedactionMaskError> {
    let mut redacted = false;
    if let Some(Value::String(instructions)) = value.get_mut("instructions") {
        redacted |= mask_json_string_async(instructions, session, scan_state, cache).await?;
    }
    if let Some(input) = value.get_mut("input") {
        redacted |=
            mask_openai_responses_input_value_async(input, session, scan_state, cache).await?;
    }
    Ok(redacted)
}

async fn mask_openai_search_request_value_async(
    value: &mut Value,
    session: &mut RedactionSession,
    scan_state: &mut RedactionScanState,
    cache: Option<&RedisRedactionMappingCache<'_>>,
) -> Result<bool, RedactionMaskError> {
    let mut redacted = false;
    if let Some(input) = value.get_mut("input") {
        redacted |=
            mask_openai_responses_input_value_async(input, session, scan_state, cache).await?;
    }
    let Some(commands) = value.get_mut("commands").and_then(Value::as_object_mut) else {
        return Ok(redacted);
    };
    for (command, field) in OPENAI_SEARCH_COMMAND_TEXT_FIELDS {
        let Some(entries) = commands.get_mut(command).and_then(Value::as_array_mut) else {
            continue;
        };
        for entry in entries {
            if let Some(Value::String(text)) = entry.get_mut(field) {
                redacted |= mask_json_string_async(text, session, scan_state, cache).await?;
            }
        }
    }
    Ok(redacted)
}

async fn mask_openai_responses_input_value_async(
    value: &mut Value,
    session: &mut RedactionSession,
    scan_state: &mut RedactionScanState,
    cache: Option<&RedisRedactionMappingCache<'_>>,
) -> Result<bool, RedactionMaskError> {
    match value {
        Value::String(text) => mask_json_string_async(text, session, scan_state, cache).await,
        Value::Array(items) => {
            let mut redacted = false;
            for item in items {
                redacted |=
                    mask_openai_responses_input_item_async(item, session, scan_state, cache)
                        .await?;
            }
            Ok(redacted)
        }
        _ => Ok(false),
    }
}

async fn mask_openai_responses_input_item_async(
    item: &mut Value,
    session: &mut RedactionSession,
    scan_state: &mut RedactionScanState,
    cache: Option<&RedisRedactionMappingCache<'_>>,
) -> Result<bool, RedactionMaskError> {
    let Some(item) = item.as_object_mut() else {
        return Ok(false);
    };
    if session.preserves_deepseek_opaque_reasoning_state()
        && openai_responses_object_is_opaque_reasoning_state(item)
    {
        return Ok(false);
    }
    let mut redacted = false;
    if let Some(content) = item.get_mut("content") {
        redacted |=
            mask_openai_responses_content_value_async(content, session, scan_state, cache).await?;
    }
    if response_textish_type(item.get("type").and_then(Value::as_str)) {
        if let Some(Value::String(text)) = item.get_mut("text") {
            redacted |= mask_json_string_async(text, session, scan_state, cache).await?;
        }
    }
    if item.get("type").and_then(Value::as_str) == Some("function_call") {
        if let Some(Value::String(arguments)) = item.get_mut("arguments") {
            redacted |= mask_json_string_async(arguments, session, scan_state, cache).await?;
        }
    }
    Ok(redacted)
}

async fn mask_openai_responses_content_value_async(
    content: &mut Value,
    session: &mut RedactionSession,
    scan_state: &mut RedactionScanState,
    cache: Option<&RedisRedactionMappingCache<'_>>,
) -> Result<bool, RedactionMaskError> {
    match content {
        Value::String(text) => mask_json_string_async(text, session, scan_state, cache).await,
        Value::Array(parts) => {
            let mut redacted = false;
            for part in parts {
                redacted |=
                    mask_openai_responses_content_part_async(part, session, scan_state, cache)
                        .await?;
            }
            Ok(redacted)
        }
        _ => Ok(false),
    }
}

async fn mask_openai_responses_content_part_async(
    part: &mut Value,
    session: &mut RedactionSession,
    scan_state: &mut RedactionScanState,
    cache: Option<&RedisRedactionMappingCache<'_>>,
) -> Result<bool, RedactionMaskError> {
    let Some(part) = part.as_object_mut() else {
        return Ok(false);
    };
    if !response_textish_type(part.get("type").and_then(Value::as_str)) {
        return Ok(false);
    }
    let Some(Value::String(text)) = part.get_mut("text") else {
        return Ok(false);
    };
    mask_json_string_async(text, session, scan_state, cache).await
}

async fn mask_json_string_async(
    text: &mut String,
    session: &mut RedactionSession,
    scan_state: &mut RedactionScanState,
    cache: Option<&RedisRedactionMappingCache<'_>>,
) -> Result<bool, RedactionMaskError> {
    let redacted = session
        .redact_text_with_cache(text, scan_state, cache)
        .await?;
    if redacted.matches.is_empty() {
        return Ok(false);
    }
    *text = redacted.text;
    Ok(true)
}

pub(crate) struct RestoredSyncResponseBody {
    pub(crate) body: Vec<u8>,
    pub(crate) restored: bool,
}

impl fmt::Debug for RestoredSyncResponseBody {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RestoredSyncResponseBody")
            .field("body_len", &self.body.len())
            .field("restored", &self.restored)
            .finish()
    }
}

#[cfg(test)]
struct RestoredText {
    text: String,
    restored: bool,
}

pub(crate) struct RestoreExpansionBudget {
    limit: usize,
    used: usize,
}

impl RestoreExpansionBudget {
    /// The caller creates a fresh budget for one sync response, stream
    /// `push_chunk`/`finish`, SSE event, or WebSocket frame.  It intentionally
    /// never accumulates across a long-lived stream.
    pub(crate) fn new(limit: usize) -> Self {
        Self { limit, used: 0 }
    }

    fn charge(&mut self, expansion: usize) -> Result<(), GatewayError> {
        let next = self.used.checked_add(expansion).ok_or_else(|| {
            GatewayError::Internal("redaction response restoration expansion overflow".to_string())
        })?;
        if next > self.limit {
            return Err(GatewayError::Internal(format!(
                "redaction response restoration expansion exceeds {}/{} bytes",
                next, self.limit
            )));
        }
        self.used = next;
        Ok(())
    }

    fn checkpoint(&self) -> usize {
        self.used
    }

    fn available_from(&self, checkpoint: usize) -> usize {
        self.limit.saturating_sub(checkpoint)
    }

    fn replace_charge(&mut self, checkpoint: usize, expansion: usize) -> Result<(), GatewayError> {
        self.used = checkpoint.min(self.used);
        self.charge(expansion)
    }
}

struct RestoreScan {
    consumed: usize,
    output_len: usize,
    expansion: usize,
    restored: bool,
}

fn scan_restore_bytes(
    input: &[u8],
    matcher: &SentinelMatcher<'_>,
) -> Result<RestoreScan, GatewayError> {
    scan_restore_prefix(input, input.len(), matcher)
}

fn scan_restore_prefix(
    input: &[u8],
    scan_limit: usize,
    matcher: &SentinelMatcher<'_>,
) -> Result<RestoreScan, GatewayError> {
    let scan_limit = scan_limit.min(input.len());
    let mut output_len = 0usize;
    let mut index = 0usize;
    let mut restored = false;
    while index < scan_limit {
        if let Some(mapping) = matcher.matching_mapping_at(input, index) {
            let next_index = index
                .checked_add(mapping.sentinel.len())
                .filter(|next| *next > index && *next <= input.len())
                .ok_or_else(|| {
                    GatewayError::Internal(
                        "redaction response restoration sentinel length overflow".to_string(),
                    )
                })?;
            output_len = output_len
                .checked_add(mapping.original.len())
                .ok_or_else(|| {
                    GatewayError::Internal(
                        "redaction response restoration output length overflow".to_string(),
                    )
                })?;
            index = next_index;
            restored = true;
        } else {
            output_len = output_len.checked_add(1).ok_or_else(|| {
                GatewayError::Internal(
                    "redaction response restoration output length overflow".to_string(),
                )
            })?;
            index += 1;
        }
    }
    Ok(RestoreScan {
        consumed: index,
        output_len,
        expansion: output_len.saturating_sub(index),
        restored,
    })
}

fn build_restored_bytes(
    input: &[u8],
    matcher: &SentinelMatcher<'_>,
    scan: &RestoreScan,
) -> Vec<u8> {
    let mut output = Vec::with_capacity(scan.output_len);
    let mut index = 0usize;
    while index < scan.consumed {
        if let Some(mapping) = matcher.matching_mapping_at(input, index) {
            output.extend_from_slice(mapping.original.as_bytes());
            index += mapping.sentinel.len();
        } else {
            output.push(input[index]);
            index += 1;
        }
    }
    output
}

fn restore_bytes_with_budget(
    input: &[u8],
    matcher: &SentinelMatcher<'_>,
    budget: &mut RestoreExpansionBudget,
) -> Result<(Vec<u8>, bool), GatewayError> {
    let scan = scan_restore_bytes(input, matcher)?;
    if !scan.restored {
        return Ok((input.to_vec(), false));
    }
    // Charge before allocating the replacement buffer.  A provider can repeat
    // a short sentinel many times, so checking after `Vec` construction is too
    // late to protect the process from an amplification attack.
    budget.charge(scan.expansion)?;
    Ok((build_restored_bytes(input, matcher, &scan), true))
}

fn measure_json_restore(
    value: &Value,
    matcher: &SentinelMatcher<'_>,
) -> Result<(usize, bool), GatewayError> {
    match value {
        Value::String(text) => {
            let scan = scan_restore_bytes(text.as_bytes(), matcher)?;
            Ok((scan.expansion, scan.restored))
        }
        Value::Array(values) => {
            let mut expansion = 0usize;
            let mut restored = false;
            for value in values {
                let (value_expansion, value_restored) = measure_json_restore(value, matcher)?;
                expansion = expansion.checked_add(value_expansion).ok_or_else(|| {
                    GatewayError::Internal(
                        "redaction response restoration expansion overflow".to_string(),
                    )
                })?;
                restored |= value_restored;
            }
            Ok((expansion, restored))
        }
        Value::Object(values) => {
            let mut expansion = 0usize;
            let mut restored = false;
            for value in values.values() {
                let (value_expansion, value_restored) = measure_json_restore(value, matcher)?;
                expansion = expansion.checked_add(value_expansion).ok_or_else(|| {
                    GatewayError::Internal(
                        "redaction response restoration expansion overflow".to_string(),
                    )
                })?;
                restored |= value_restored;
            }
            Ok((expansion, restored))
        }
        _ => Ok((0, false)),
    }
}

fn restore_json_strings_inner(
    value: &mut Value,
    matcher: &SentinelMatcher<'_>,
    preserve_opaque_reasoning_state: bool,
) -> Result<bool, GatewayError> {
    match value {
        Value::String(text) => {
            let scan = scan_restore_bytes(text.as_bytes(), matcher)?;
            if !scan.restored {
                return Ok(false);
            }
            let bytes = build_restored_bytes(text.as_bytes(), matcher, &scan);
            *text = String::from_utf8(bytes).map_err(|_| {
                GatewayError::Internal(
                    "redaction response restoration produced invalid UTF-8".to_string(),
                )
            })?;
            Ok(true)
        }
        Value::Array(values) => {
            let mut restored = false;
            for value in values {
                restored |=
                    restore_json_strings_inner(value, matcher, preserve_opaque_reasoning_state)?;
            }
            Ok(restored)
        }
        Value::Object(values) => {
            if preserve_opaque_reasoning_state
                && openai_responses_object_is_opaque_reasoning_state(values)
            {
                return Ok(false);
            }
            let mut restored = false;
            for value in values.values_mut() {
                restored |=
                    restore_json_strings_inner(value, matcher, preserve_opaque_reasoning_state)?;
            }
            Ok(restored)
        }
        _ => Ok(false),
    }
}

pub(crate) fn restore_json_strings_with_budget(
    value: &mut Value,
    session: &RedactionSession,
    budget: &mut RestoreExpansionBudget,
) -> Result<bool, GatewayError> {
    let matcher = SentinelMatcher::new(session);
    let (expansion, restored) = measure_json_restore(value, &matcher)?;
    if !restored {
        return Ok(false);
    }
    budget.charge(expansion)?;
    restore_json_strings_inner(
        value,
        &matcher,
        session.preserves_deepseek_opaque_reasoning_state(),
    )
}

/// Compatibility wrapper for crate-local callers that only need the historical
/// boolean result.  The bounded implementation remains the source of truth;
/// failures are treated as "not restored" so callers cannot accidentally emit
/// a partially restored value.
#[cfg(test)]
pub(crate) fn restore_json_strings(value: &mut Value, session: &RedactionSession) -> bool {
    let mut budget = RestoreExpansionBudget::new(MAX_SYNC_RESTORE_EXPANSION_BYTES);
    restore_json_strings_with_budget(value, session, &mut budget).unwrap_or(false)
}

fn openai_responses_object_is_opaque_reasoning_state(
    object: &serde_json::Map<String, Value>,
) -> bool {
    let Some(item_type) = object.get("type").and_then(Value::as_str) else {
        return false;
    };
    // A provider-owned reasoning item is replayed byte-for-byte.  The
    // encrypted continuation token is only considered opaque when it is
    // paired with the reasoning text item shape; ordinary reasoning text must
    // still participate in PII masking/restoration.
    let id_is_absent_or_empty = object
        .get("id")
        .is_none_or(|id| id.is_null() || id.as_str().is_some_and(|value| value.trim().is_empty()));
    item_type == "reasoning"
        && id_is_absent_or_empty
        && object
            .get("encrypted_content")
            .and_then(Value::as_str)
            .is_some_and(|state| !state.trim().is_empty())
        && object
            .get("content")
            .and_then(Value::as_array)
            .is_some_and(|content| {
                content.iter().any(|part| {
                    part.get("type").and_then(Value::as_str) == Some("reasoning_text")
                        && part.get("text").is_some_and(Value::is_string)
                })
            })
}

fn effective_sync_restore_expansion_limit(configured_limit: u64) -> usize {
    usize::try_from(configured_limit)
        .unwrap_or(usize::MAX)
        .min(MAX_SYNC_RESTORE_EXPANSION_BYTES)
}

fn sync_restore_expansion_limit() -> usize {
    effective_sync_restore_expansion_limit(crate::headers::max_redacted_sync_response_body_bytes())
}

struct LimitedJsonWriter {
    bytes: Vec<u8>,
    limit: usize,
    exceeded: bool,
}

impl LimitedJsonWriter {
    fn new(limit: usize) -> Self {
        Self {
            bytes: Vec::with_capacity(limit.min(16 * 1024)),
            limit,
            exceeded: false,
        }
    }
}

impl Write for LimitedJsonWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if bytes.len() > self.limit.saturating_sub(self.bytes.len()) {
            self.exceeded = true;
            return Err(std::io::Error::new(
                std::io::ErrorKind::WriteZero,
                "redaction response exceeds output limit",
            ));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

pub(crate) fn serialize_json_value_with_limit(
    value: &Value,
    limit: usize,
) -> Result<String, GatewayError> {
    let mut writer = LimitedJsonWriter::new(limit);
    match serde_json::to_writer(&mut writer, value) {
        Ok(()) => String::from_utf8(writer.bytes).map_err(|_| {
            GatewayError::Internal(
                "redaction response serialization produced invalid UTF-8".to_string(),
            )
        }),
        Err(_) if writer.exceeded => Err(GatewayError::Internal(format!(
            "redaction response serialized body exceeds {limit} bytes"
        ))),
        Err(error) => Err(GatewayError::Internal(error.to_string())),
    }
}

pub(crate) fn restore_sync_response_body(
    headers: &mut BTreeMap<String, String>,
    body: &[u8],
    session: &RedactionSession,
) -> Result<RestoredSyncResponseBody, GatewayError> {
    restore_sync_response_body_with_limit(headers, body, session, sync_restore_expansion_limit())
}

fn restore_sync_response_body_with_limit(
    headers: &mut BTreeMap<String, String>,
    body: &[u8],
    session: &RedactionSession,
    expansion_limit: usize,
) -> Result<RestoredSyncResponseBody, GatewayError> {
    if session.mapping_count() == 0 {
        return Ok(RestoredSyncResponseBody {
            body: body.to_vec(),
            restored: false,
        });
    }

    ensure_identity_response_encoding(headers)?;

    let restored = if response_body_is_json(headers, body) {
        restore_json_response_body(body, session, expansion_limit)?
    } else {
        restore_text_response_body(body, session, expansion_limit)?
    };
    if restored.restored {
        set_content_length(headers, restored.body.len());
    }
    Ok(restored)
}

fn ensure_identity_response_encoding(
    headers: &BTreeMap<String, String>,
) -> Result<(), GatewayError> {
    for encoding in headers
        .iter()
        .filter(|(header_name, _)| header_name.eq_ignore_ascii_case("content-encoding"))
        .map(|(_, value)| value)
    {
        if encoding.trim().is_empty() || encoding.trim().eq_ignore_ascii_case("identity") {
            continue;
        }
        return Err(GatewayError::Internal(
            "redaction response restoration does not support encoded response bodies".to_string(),
        ));
    }
    Ok(())
}

fn response_body_is_json(headers: &BTreeMap<String, String>, body: &[u8]) -> bool {
    if header_value(headers, "content-type").is_some_and(content_type_is_json) {
        return true;
    }
    body.iter()
        .copied()
        .find(|byte| !byte.is_ascii_whitespace())
        .is_some_and(|byte| matches!(byte, b'{' | b'['))
}

fn content_type_is_json(content_type: &str) -> bool {
    content_type
        .split(';')
        .next()
        .map(str::trim)
        .map(str::to_ascii_lowercase)
        .is_some_and(|mime| mime == "application/json" || mime.ends_with("+json"))
}

fn restore_json_response_body(
    body: &[u8],
    session: &RedactionSession,
    expansion_limit: usize,
) -> Result<RestoredSyncResponseBody, GatewayError> {
    let Ok(mut value) = serde_json::from_slice::<Value>(body) else {
        return restore_text_response_body(body, session, expansion_limit);
    };
    let mut budget = RestoreExpansionBudget::new(expansion_limit);
    if !restore_json_strings_with_budget(&mut value, session, &mut budget)? {
        return Ok(RestoredSyncResponseBody {
            body: body.to_vec(),
            restored: false,
        });
    }
    let output_limit = body.len().checked_add(expansion_limit).ok_or_else(|| {
        GatewayError::Internal("redaction response output length overflow".to_string())
    })?;
    let body = serialize_json_value_with_limit(&value, output_limit)?.into_bytes();
    Ok(RestoredSyncResponseBody {
        body,
        restored: true,
    })
}

/// 递归把 JSON 里的占位符换回真实值，只认本 `session` 记录过的映射。
///
/// 同步响应体（[`restore_sync_response_body`]）和 Responses WebSocket 的
/// provider 事件帧（`handlers::proxy::websocket::responses::redaction`）共用它，
/// 两边因此保持同一套还原语义：未映射的占位符原样保留，`type` / `model` / `id`
/// 这类协议字段虽然也被遍历，但它们不可能包含本 session 派生出的 sentinel，
/// 所以不会被改写。
fn restore_text_response_body(
    body: &[u8],
    session: &RedactionSession,
    expansion_limit: usize,
) -> Result<RestoredSyncResponseBody, GatewayError> {
    let Ok(text) = std::str::from_utf8(body) else {
        return Ok(RestoredSyncResponseBody {
            body: body.to_vec(),
            restored: false,
        });
    };
    let matcher = SentinelMatcher::new(session);
    let mut budget = RestoreExpansionBudget::new(expansion_limit);
    let (body, restored) = restore_bytes_with_budget(text.as_bytes(), &matcher, &mut budget)?;
    Ok(RestoredSyncResponseBody { body, restored })
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum StreamRestoreMode {
    Sse,
    Text,
}

pub(crate) struct StreamingResponseRestorer<'a> {
    session: &'a RedactionSession,
    matcher: SentinelMatcher<'a>,
    mode: StreamRestoreMode,
    max_sentinel_len: usize,
    text_carry: Vec<u8>,
    sse_line_buffer: Vec<u8>,
    sse_json_event_lines: Vec<Vec<u8>>,
    sse_json_event_bytes: usize,
    max_expansion_bytes: usize,
}

// A provider-controlled SSE line/event must not be allowed to grow forever
// while the stream restorer waits for a newline or blank-record separator.
// This bounds parser carry state only; complete response bodies and ordinary
// streaming chunks are still passed through without a body/concurrency cap.
const MAX_STREAM_RESTORE_BUFFER_BYTES: usize = 16 * 1024 * 1024;
// This is an expansion budget, not a body or duration limit.  A normal
// pass-through chunk can be any size; only bytes introduced by replacing a
// sentinel with its original value consume this budget.
pub(crate) const MAX_STREAM_RESTORE_EXPANSION_BYTES: usize = 16 * 1024 * 1024;
pub(crate) const MAX_STREAM_RESTORE_OUTPUT_BYTES: usize =
    (16usize * 1024 * 1024).saturating_add(MAX_STREAM_RESTORE_EXPANSION_BYTES);

impl<'a> StreamingResponseRestorer<'a> {
    pub(crate) fn new(
        headers: &BTreeMap<String, String>,
        session: &'a RedactionSession,
    ) -> Result<Self, GatewayError> {
        if session.mapping_count() > 0 {
            ensure_identity_response_encoding(headers)?;
        }
        let mode = if header_value(headers, "content-type").is_some_and(content_type_is_sse) {
            StreamRestoreMode::Sse
        } else {
            StreamRestoreMode::Text
        };
        Ok(Self::with_mode(session, mode))
    }

    #[cfg(test)]
    fn for_sse(session: &'a RedactionSession) -> Self {
        Self::with_mode(session, StreamRestoreMode::Sse)
    }

    #[cfg(test)]
    fn for_text(session: &'a RedactionSession) -> Self {
        Self::with_mode(session, StreamRestoreMode::Text)
    }

    #[cfg(test)]
    fn for_text_with_expansion_limit(
        session: &'a RedactionSession,
        max_expansion_bytes: usize,
    ) -> Self {
        let mut restorer = Self::with_mode(session, StreamRestoreMode::Text);
        restorer.max_expansion_bytes = max_expansion_bytes;
        restorer
    }

    #[cfg(test)]
    fn for_sse_with_expansion_limit(
        session: &'a RedactionSession,
        max_expansion_bytes: usize,
    ) -> Self {
        let mut restorer = Self::with_mode(session, StreamRestoreMode::Sse);
        restorer.max_expansion_bytes = max_expansion_bytes;
        restorer
    }

    fn with_mode(session: &'a RedactionSession, mode: StreamRestoreMode) -> Self {
        let max_sentinel_len = session
            .mappings()
            .map(|mapping| mapping.sentinel.len())
            .max()
            .unwrap_or_default();
        Self {
            session,
            matcher: SentinelMatcher::new(session),
            mode,
            max_sentinel_len,
            text_carry: Vec::new(),
            sse_line_buffer: Vec::new(),
            sse_json_event_lines: Vec::new(),
            sse_json_event_bytes: 0,
            max_expansion_bytes: MAX_STREAM_RESTORE_EXPANSION_BYTES,
        }
    }

    pub(crate) fn push_chunk(&mut self, chunk: &[u8]) -> Result<Vec<u8>, GatewayError> {
        if self.max_sentinel_len == 0 {
            return Ok(chunk.to_vec());
        }
        // Per-call expansion protection only: do not turn this into a total
        // stream byte or duration limit.
        let mut budget = RestoreExpansionBudget::new(self.max_expansion_bytes);
        match self.mode {
            StreamRestoreMode::Sse => self.push_sse_chunk(chunk, &mut budget),
            StreamRestoreMode::Text => self.push_text_chunk(chunk, &mut budget),
        }
    }

    pub(crate) fn finish(&mut self) -> Result<Vec<u8>, GatewayError> {
        if self.max_sentinel_len == 0 {
            return Ok(Vec::new());
        }
        // `finish` is a separate bounded flush, not a cumulative stream cap.
        let mut budget = RestoreExpansionBudget::new(self.max_expansion_bytes);
        match self.mode {
            StreamRestoreMode::Sse => self.finish_sse(&mut budget),
            StreamRestoreMode::Text => self.flush_text_carry(&mut budget),
        }
    }

    #[cfg(test)]
    fn max_text_carry_len(&self) -> usize {
        self.max_sentinel_len.saturating_sub(1)
    }

    #[cfg(test)]
    fn pending_text_carry_len(&self) -> usize {
        self.text_carry.len()
    }

    fn push_text_chunk(
        &mut self,
        chunk: &[u8],
        budget: &mut RestoreExpansionBudget,
    ) -> Result<Vec<u8>, GatewayError> {
        self.text_carry.extend_from_slice(chunk);
        self.restore_available_text(false, budget)
    }

    fn flush_text_carry(
        &mut self,
        budget: &mut RestoreExpansionBudget,
    ) -> Result<Vec<u8>, GatewayError> {
        self.restore_available_text(true, budget)
    }

    fn restore_available_text(
        &mut self,
        flush: bool,
        budget: &mut RestoreExpansionBudget,
    ) -> Result<Vec<u8>, GatewayError> {
        let scan_limit = if flush {
            self.text_carry.len()
        } else {
            self.text_carry
                .len()
                .saturating_sub(self.max_sentinel_len.saturating_sub(1))
        };
        let scan = scan_restore_prefix(&self.text_carry, scan_limit, &self.matcher)?;
        budget.charge(scan.expansion)?;
        let output = build_restored_bytes(&self.text_carry, &self.matcher, &scan);
        self.text_carry.drain(..scan.consumed);
        Ok(output)
    }

    fn push_sse_chunk(
        &mut self,
        chunk: &[u8],
        budget: &mut RestoreExpansionBudget,
    ) -> Result<Vec<u8>, GatewayError> {
        let mut output = Vec::new();
        let mut remaining = chunk;
        while !remaining.is_empty() {
            let Some(line_end) = remaining.iter().position(|byte| *byte == b'\n') else {
                append_bounded_stream_restore_bytes(&mut self.sse_line_buffer, remaining)?;
                break;
            };
            let line_len = line_end + 1;
            append_bounded_stream_restore_bytes(&mut self.sse_line_buffer, &remaining[..line_len])?;
            remaining = &remaining[line_len..];
            let line = std::mem::take(&mut self.sse_line_buffer);
            self.push_sse_line(line, &mut output, budget)?;
        }
        Ok(output)
    }

    fn finish_sse(&mut self, budget: &mut RestoreExpansionBudget) -> Result<Vec<u8>, GatewayError> {
        let mut output = Vec::new();
        if !self.sse_line_buffer.is_empty() {
            let line = std::mem::take(&mut self.sse_line_buffer);
            self.push_sse_line(line, &mut output, budget)?;
        }
        self.flush_sse_json_event(&mut output, budget)?;
        Ok(output)
    }

    fn push_sse_line(
        &mut self,
        line: Vec<u8>,
        output: &mut Vec<u8>,
        budget: &mut RestoreExpansionBudget,
    ) -> Result<(), GatewayError> {
        if !self.sse_json_event_lines.is_empty() {
            if sse_line_is_blank(&line) {
                self.flush_sse_json_event(output, budget)?;
                output.extend_from_slice(&line);
            } else {
                self.push_sse_json_event_line(line)?;
            }
            return Ok(());
        }

        if sse_line_is_blank(&line) {
            output.extend_from_slice(&line);
            return Ok(());
        }

        let Some(value) = sse_data_line_value(&line) else {
            output.extend_from_slice(&line);
            return Ok(());
        };
        if sse_data_value_is_done(value) {
            output.extend_from_slice(&line);
            return Ok(());
        }
        if sse_data_value_may_be_json(value) {
            self.push_sse_json_event_line(line)?;
            return Ok(());
        }
        output.extend(self.restore_sse_text_data_line(&line, budget)?);
        Ok(())
    }

    fn flush_sse_json_event(
        &mut self,
        output: &mut Vec<u8>,
        budget: &mut RestoreExpansionBudget,
    ) -> Result<(), GatewayError> {
        if self.sse_json_event_lines.is_empty() {
            return Ok(());
        }

        let event_lines = std::mem::take(&mut self.sse_json_event_lines);
        self.sse_json_event_bytes = 0;
        let event_budget_checkpoint = budget.checkpoint();
        let Some(payload) = sse_event_data_payload(&event_lines) else {
            output.extend(event_lines.into_iter().flatten());
            return Ok(());
        };
        if payload.trim() == "[DONE]" {
            output.extend(event_lines.into_iter().flatten());
            return Ok(());
        }

        let Ok(mut value) = serde_json::from_str::<Value>(&payload) else {
            for line in event_lines {
                if sse_data_line_value(&line).is_some() {
                    output.extend(self.restore_sse_text_data_line(&line, budget)?);
                } else {
                    output.extend_from_slice(&line);
                }
            }
            return Ok(());
        };

        if !restore_json_strings_with_budget(&mut value, self.session, budget)? {
            output.extend(event_lines.into_iter().flatten());
            return Ok(());
        }

        let event_input_bytes = event_lines
            .iter()
            .map(Vec::len)
            .try_fold(0usize, |sum, len| {
                sum.checked_add(len).ok_or_else(|| {
                    GatewayError::Internal("stream redaction SSE event length overflow".to_string())
                })
            })?;
        let event_output_limit = event_input_bytes
            .checked_add(budget.available_from(event_budget_checkpoint))
            .ok_or_else(|| {
                GatewayError::Internal(
                    "stream redaction SSE restored output length overflow".to_string(),
                )
            })?;
        let restored_payload =
            serialize_json_value_with_limit(&value, event_output_limit)?.into_bytes();
        let mut wrote_data_line = false;
        let mut output_len = 0usize;
        for line in event_lines {
            if sse_data_line_value(&line).is_some() {
                if !wrote_data_line {
                    let replaced = replace_sse_data_line_value(&line, &restored_payload);
                    output_len = output_len.checked_add(replaced.len()).ok_or_else(|| {
                        GatewayError::Internal(
                            "stream redaction SSE restored output length overflow".to_string(),
                        )
                    })?;
                    if output_len > event_output_limit {
                        return Err(GatewayError::Internal(format!(
                            "stream redaction SSE restored event exceeds {event_output_limit} bytes"
                        )));
                    }
                    output.extend(replaced);
                    wrote_data_line = true;
                }
            } else {
                output_len = output_len.checked_add(line.len()).ok_or_else(|| {
                    GatewayError::Internal(
                        "stream redaction SSE restored output length overflow".to_string(),
                    )
                })?;
                if output_len > event_output_limit {
                    return Err(GatewayError::Internal(format!(
                        "stream redaction SSE restored event exceeds {event_output_limit} bytes"
                    )));
                }
                output.extend_from_slice(&line);
            }
        }
        let actual_expansion = output_len.saturating_sub(event_input_bytes);
        budget.replace_charge(event_budget_checkpoint, actual_expansion)?;
        Ok(())
    }

    fn push_sse_json_event_line(&mut self, line: Vec<u8>) -> Result<(), GatewayError> {
        let next_len = self
            .sse_json_event_bytes
            .checked_add(line.len())
            .ok_or_else(|| {
                GatewayError::Internal("stream redaction SSE event length overflow".to_string())
            })?;
        if next_len > MAX_STREAM_RESTORE_BUFFER_BYTES {
            return Err(GatewayError::Internal(format!(
                "stream redaction SSE event exceeds {MAX_STREAM_RESTORE_BUFFER_BYTES} bytes"
            )));
        }
        self.sse_json_event_lines.push(line);
        self.sse_json_event_bytes = next_len;
        Ok(())
    }

    fn restore_sse_text_data_line(
        &self,
        line: &[u8],
        budget: &mut RestoreExpansionBudget,
    ) -> Result<Vec<u8>, GatewayError> {
        let Some(range) = sse_data_line_value_range(line) else {
            return Ok(line.to_vec());
        };
        let (body, ending) = split_sse_line_ending(line);
        let (restored, _) = restore_bytes_with_budget(&body[range.clone()], &self.matcher, budget)?;
        let output_len = body
            .len()
            .saturating_sub(range.len())
            .checked_add(restored.len())
            .and_then(|len| len.checked_add(ending.len()))
            .ok_or_else(|| {
                GatewayError::Internal(
                    "stream redaction SSE restored output length overflow".to_string(),
                )
            })?;
        let mut output = Vec::with_capacity(output_len);
        output.extend_from_slice(&body[..range.start]);
        output.extend(restored);
        output.extend_from_slice(ending);
        Ok(output)
    }
}

fn append_bounded_stream_restore_bytes(
    buffered: &mut Vec<u8>,
    chunk: &[u8],
) -> Result<(), GatewayError> {
    let next_len = buffered.len().checked_add(chunk.len()).ok_or_else(|| {
        GatewayError::Internal("stream redaction buffer length overflow".to_string())
    })?;
    if next_len > MAX_STREAM_RESTORE_BUFFER_BYTES {
        return Err(GatewayError::Internal(format!(
            "stream redaction buffer exceeds {MAX_STREAM_RESTORE_BUFFER_BYTES} bytes"
        )));
    }
    buffered.extend_from_slice(chunk);
    Ok(())
}

enum SentinelMatcher<'a> {
    Direct(Vec<&'a RedactionMapping>),
    Trie(SentinelTrie<'a>),
}

impl<'a> SentinelMatcher<'a> {
    fn new(session: &'a RedactionSession) -> Self {
        let mut mappings = session.mappings().collect::<Vec<_>>();
        mappings.sort_by_key(|mapping| std::cmp::Reverse(mapping.sentinel.len()));
        if mappings.len() <= DIRECT_RESTORE_SENTINEL_LIMIT {
            Self::Direct(mappings)
        } else {
            Self::Trie(SentinelTrie::new(mappings))
        }
    }

    fn matching_mapping_at(&self, input: &[u8], index: usize) -> Option<&'a RedactionMapping> {
        match self {
            Self::Direct(mappings) => mappings.iter().copied().find(|mapping| {
                input
                    .get(index..)
                    .is_some_and(|remaining| remaining.starts_with(mapping.sentinel.as_bytes()))
            }),
            Self::Trie(trie) => trie.matching_mapping_at(input, index),
        }
    }
}

struct SentinelTrie<'a> {
    nodes: Vec<SentinelTrieNode<'a>>,
}

struct SentinelTrieNode<'a> {
    children: HashMap<u8, usize>,
    mapping: Option<&'a RedactionMapping>,
}

impl<'a> SentinelTrie<'a> {
    fn new(mappings: Vec<&'a RedactionMapping>) -> Self {
        let mut trie = Self {
            nodes: vec![SentinelTrieNode {
                children: HashMap::new(),
                mapping: None,
            }],
        };
        for mapping in mappings {
            trie.insert(mapping);
        }
        trie
    }

    fn insert(&mut self, mapping: &'a RedactionMapping) {
        let mut node_index = 0;
        for byte in mapping.sentinel.bytes() {
            let next_index = if let Some(next_index) = self.nodes[node_index].children.get(&byte) {
                *next_index
            } else {
                self.nodes.push(SentinelTrieNode {
                    children: HashMap::new(),
                    mapping: None,
                });
                let next_index = self.nodes.len() - 1;
                self.nodes[node_index].children.insert(byte, next_index);
                next_index
            };
            node_index = next_index;
        }
        self.nodes[node_index].mapping = Some(mapping);
    }

    fn matching_mapping_at(&self, input: &[u8], index: usize) -> Option<&'a RedactionMapping> {
        let mut node_index = 0;
        let mut best = None;
        for byte in input.get(index..).unwrap_or_default() {
            let Some(next_index) = self.nodes[node_index].children.get(byte).copied() else {
                break;
            };
            node_index = next_index;
            if let Some(mapping) = self.nodes[node_index].mapping {
                best = Some(mapping);
            }
        }
        best
    }
}

fn content_type_is_sse(content_type: &str) -> bool {
    content_type
        .split(';')
        .next()
        .map(str::trim)
        .is_some_and(|mime| mime.eq_ignore_ascii_case("text/event-stream"))
}

fn split_sse_line_ending(line: &[u8]) -> (&[u8], &[u8]) {
    if line.ends_with(b"\r\n") {
        (&line[..line.len() - 2], &line[line.len() - 2..])
    } else if line.ends_with(b"\n") {
        (&line[..line.len() - 1], &line[line.len() - 1..])
    } else {
        (line, &[])
    }
}

fn sse_line_is_blank(line: &[u8]) -> bool {
    split_sse_line_ending(line).0.is_empty()
}

fn sse_data_line_value_range(line: &[u8]) -> Option<std::ops::Range<usize>> {
    let (body, _) = split_sse_line_ending(line);
    if !body.starts_with(b"data:") {
        return None;
    }
    let start = if body.get(5) == Some(&b' ') { 6 } else { 5 };
    Some(start..body.len())
}

fn sse_data_line_value(line: &[u8]) -> Option<&[u8]> {
    let (body, _) = split_sse_line_ending(line);
    sse_data_line_value_range(line).map(|range| &body[range])
}

fn sse_data_value_is_done(value: &[u8]) -> bool {
    std::str::from_utf8(value)
        .map(str::trim)
        .is_ok_and(|value| value == "[DONE]")
}

fn sse_data_value_may_be_json(value: &[u8]) -> bool {
    value
        .iter()
        .copied()
        .find(|byte| !byte.is_ascii_whitespace())
        .is_some_and(|byte| matches!(byte, b'{' | b'['))
}

fn sse_event_data_payload(lines: &[Vec<u8>]) -> Option<String> {
    let mut payload = String::new();
    let mut saw_data = false;
    for line in lines {
        let Some(value) = sse_data_line_value(line) else {
            continue;
        };
        if saw_data {
            payload.push('\n');
        }
        payload.push_str(std::str::from_utf8(value).ok()?);
        saw_data = true;
    }
    saw_data.then_some(payload)
}

fn replace_sse_data_line_value(line: &[u8], value: &[u8]) -> Vec<u8> {
    let Some(range) = sse_data_line_value_range(line) else {
        return line.to_vec();
    };
    let (body, ending) = split_sse_line_ending(line);
    let mut output = Vec::with_capacity(body.len() - range.len() + value.len() + ending.len());
    output.extend_from_slice(&body[..range.start]);
    output.extend_from_slice(value);
    output.extend_from_slice(ending);
    output
}

fn header_value<'a>(headers: &'a BTreeMap<String, String>, name: &str) -> Option<&'a str> {
    headers
        .iter()
        .find(|(header_name, _)| header_name.eq_ignore_ascii_case(name))
        .map(|(_, value)| value.as_str())
}

fn set_content_length(headers: &mut BTreeMap<String, String>, body_len: usize) {
    headers.retain(|name, _| !name.eq_ignore_ascii_case("content-length"));
    headers.insert("content-length".to_string(), body_len.to_string());
}

pub(crate) struct RedactedText {
    pub(crate) text: String,
    pub(crate) matches: Vec<RedactionMatch>,
}

impl fmt::Debug for RedactedText {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RedactedText")
            .field("text_len", &self.text.len())
            .field("match_count", &self.matches.len())
            .finish()
    }
}

pub(crate) struct RedactionMatch {
    pub(crate) rule_label: String,
    pub(crate) kind: Option<RedactionKind>,
    pub(crate) start: usize,
    pub(crate) end: usize,
    pub(crate) original: String,
    pub(crate) sentinel: String,
}

impl fmt::Debug for RedactionMatch {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RedactionMatch")
            .field("rule_label", &self.rule_label)
            .field("kind", &self.kind)
            .field("start", &self.start)
            .field("end", &self.end)
            .field("original_len", &self.original.len())
            .field("sentinel", &redacted_sentinel_debug(&self.sentinel))
            .finish()
    }
}

#[derive(Clone)]
pub(crate) struct RedactionMapping {
    pub(crate) rule_label: String,
    pub(crate) kind: Option<RedactionKind>,
    pub(crate) original: String,
    pub(crate) normalized_value: String,
    pub(crate) sentinel: String,
    pub(crate) bucket: u64,
    pub(crate) created_at_unix_secs: u64,
    pub(crate) expires_at_unix_secs: u64,
}

impl fmt::Debug for RedactionMapping {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RedactionMapping")
            .field("rule_label", &self.rule_label)
            .field("kind", &self.kind)
            .field("original_len", &self.original.len())
            .field("normalized_value_len", &self.normalized_value.len())
            .field("sentinel", &redacted_sentinel_debug(&self.sentinel))
            .field("bucket", &self.bucket)
            .field("created_at_unix_secs", &self.created_at_unix_secs)
            .field("expires_at_unix_secs", &self.expires_at_unix_secs)
            .finish()
    }
}

#[derive(Deserialize, Serialize)]
pub(crate) struct RedactionCacheRecord {
    pub(crate) rule_label: String,
    pub(crate) kind: Option<RedactionKind>,
    pub(crate) sentinel: String,
    pub(crate) bucket: u64,
    pub(crate) expires_at_unix_secs: u64,
}

impl RedactionCacheRecord {
    pub(crate) fn from_mapping(mapping: &RedactionMapping) -> Self {
        Self {
            rule_label: mapping.rule_label.clone(),
            kind: mapping.kind,
            sentinel: mapping.sentinel.clone(),
            bucket: mapping.bucket,
            expires_at_unix_secs: mapping.expires_at_unix_secs,
        }
    }
}

impl fmt::Debug for RedactionCacheRecord {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RedactionCacheRecord")
            .field("rule_label", &self.rule_label)
            .field("kind", &self.kind)
            .field("sentinel", &redacted_sentinel_debug(&self.sentinel))
            .field("bucket", &self.bucket)
            .field("expires_at_unix_secs", &self.expires_at_unix_secs)
            .finish()
    }
}

pub(crate) struct RedisRedactionMappingCache<'a> {
    runtime_state: &'a RuntimeState,
    key_prefix: String,
}

impl<'a> RedisRedactionMappingCache<'a> {
    pub(crate) fn new(runtime_state: &'a RuntimeState) -> Self {
        Self {
            runtime_state,
            key_prefix: "privacy:redaction:mapping".to_string(),
        }
    }

    pub(crate) async fn store(
        &self,
        record: &RedactionCacheRecord,
        normalized_value: &str,
        ttl_seconds: u64,
    ) -> Result<(), DataLayerError> {
        if record.sentinel.len() > MAX_CACHE_SENTINEL_BYTES {
            return Err(DataLayerError::InvalidInput(
                "redaction cache sentinel is too large".to_string(),
            ));
        }
        let value = serde_json::to_string(record)
            .map_err(|err| DataLayerError::InvalidInput(err.to_string()))?;
        if value.len() > MAX_CACHE_RECORD_BYTES {
            return Err(DataLayerError::InvalidInput(
                "redaction cache record is too large".to_string(),
            ));
        }
        let ttl_seconds = ttl_seconds.max(1);
        self.runtime_state
            .kv_set(
                &self.forward_cache_key(&record.rule_label, normalized_value, record.bucket),
                record.sentinel.clone(),
                Some(Duration::from_secs(ttl_seconds)),
            )
            .await?;
        self.runtime_state
            .kv_set(
                &self.reverse_cache_key(&record.sentinel),
                value,
                Some(Duration::from_secs(ttl_seconds)),
            )
            .await?;
        Ok(())
    }

    pub(crate) async fn lookup_sentinel(
        &self,
        rule_label: &str,
        normalized_value: &str,
        bucket: u64,
        namespace: &str,
    ) -> Result<Option<String>, DataLayerError> {
        let sentinel = self
            .get_string(&self.forward_cache_key(rule_label, normalized_value, bucket))
            .await?;
        Ok(sentinel.filter(|value| {
            value.len() <= MAX_CACHE_SENTINEL_BYTES
                && sentinel_matches_rule_label(namespace, rule_label, value)
        }))
    }

    async fn get_string(&self, key: &str) -> Result<Option<String>, DataLayerError> {
        self.runtime_state.kv_get(key).await
    }

    fn forward_cache_key(&self, rule_label: &str, normalized_value: &str, bucket: u64) -> String {
        format!(
            "{}:forward:{}:{}:{}",
            self.key_prefix,
            rule_label,
            bucket,
            normalized_value_cache_digest(normalized_value)
        )
    }

    fn reverse_cache_key(&self, sentinel: &str) -> String {
        format!("{}:reverse:{}", self.key_prefix, sentinel)
    }
}

#[derive(Clone)]
struct Candidate {
    rule_label: String,
    kind: Option<RedactionKind>,
    start: usize,
    end: usize,
    value: String,
    priority: u16,
}

impl Candidate {
    fn new(
        rule_label: impl Into<String>,
        kind: Option<RedactionKind>,
        start: usize,
        end: usize,
        value: &str,
        priority: u16,
    ) -> Self {
        Self {
            rule_label: rule_label.into(),
            kind,
            start,
            end,
            value: value.to_string(),
            priority,
        }
    }

    fn len(&self) -> usize {
        self.end.saturating_sub(self.start)
    }
}

fn detect_candidates(input: &str) -> Vec<Candidate> {
    detect_candidates_with_probe(input, None)
}

fn detect_candidates_with_probe(
    input: &str,
    mut probe: Option<&mut DetectorProbe>,
) -> Vec<Candidate> {
    // Segment-level scan-bypass cache is intentionally deferred until profiling shows regex scanning is the bottleneck.
    let mut candidates = Vec::new();

    if input.contains('@') {
        push_regex_candidates(
            input,
            &EMAIL_REGEX,
            RedactionKind::Email,
            10,
            &mut candidates,
            probe.as_deref_mut(),
            |value| value.contains('@'),
        );
    }

    let digit_count = input.chars().filter(|ch| ch.is_ascii_digit()).count();
    if digit_count >= 8 {
        push_regex_candidates(
            input,
            &CN_MOBILE_REGEX,
            RedactionKind::ChinaMobile,
            20,
            &mut candidates,
            probe.as_deref_mut(),
            is_valid_cn_mobile,
        );
        push_regex_candidates(
            input,
            &CN_LANDLINE_REGEX,
            RedactionKind::ChinaLandline,
            21,
            &mut candidates,
            probe.as_deref_mut(),
            is_valid_cn_landline,
        );
        push_regex_candidates(
            input,
            &E164_PHONE_REGEX,
            RedactionKind::Phone,
            30,
            &mut candidates,
            probe.as_deref_mut(),
            is_valid_e164_phone,
        );
        push_regex_candidates(
            input,
            &CN_ID_REGEX,
            RedactionKind::ChinaResidentId,
            40,
            &mut candidates,
            probe.as_deref_mut(),
            is_valid_cn_resident_id,
        );
        push_regex_candidates(
            input,
            &PAYMENT_CARD_REGEX,
            RedactionKind::PaymentCard,
            50,
            &mut candidates,
            probe.as_deref_mut(),
            is_valid_payment_card,
        );
    }

    if input.contains('.') {
        push_regex_candidates(
            input,
            &IPV4_REGEX,
            RedactionKind::Ipv4,
            60,
            &mut candidates,
            probe.as_deref_mut(),
            |value| value.parse::<Ipv4Addr>().is_ok(),
        );
        push_regex_candidates(
            input,
            &JWT_REGEX,
            RedactionKind::Jwt,
            120,
            &mut candidates,
            probe.as_deref_mut(),
            is_valid_jwt_like,
        );
    }

    if input.contains(':') {
        candidates.extend(detect_ipv6_candidates(input));
    }

    if input.contains("sk-") {
        push_regex_candidates(
            input,
            &OPENAI_KEY_REGEX,
            RedactionKind::OpenAiKey,
            80,
            &mut candidates,
            probe.as_deref_mut(),
            |_| true,
        );
    }
    if input.contains("sk-ant-") {
        push_regex_candidates(
            input,
            &ANTHROPIC_KEY_REGEX,
            RedactionKind::AnthropicKey,
            70,
            &mut candidates,
            probe.as_deref_mut(),
            |_| true,
        );
    }
    if input.contains("ghp_")
        || input.contains("gho_")
        || input.contains("ghu_")
        || input.contains("ghs_")
        || input.contains("ghr_")
        || input.contains("github_pat_")
    {
        push_regex_candidates(
            input,
            &GITHUB_TOKEN_REGEX,
            RedactionKind::GitHubToken,
            90,
            &mut candidates,
            probe.as_deref_mut(),
            |_| true,
        );
    }
    if input.contains("xox") {
        push_regex_candidates(
            input,
            &SLACK_TOKEN_REGEX,
            RedactionKind::SlackToken,
            100,
            &mut candidates,
            probe.as_deref_mut(),
            |_| true,
        );
    }
    if input.contains("AKIA") || input.contains("ASIA") {
        push_regex_candidates(
            input,
            &AWS_KEY_REGEX,
            RedactionKind::AwsKey,
            110,
            &mut candidates,
            probe.as_deref_mut(),
            |_| true,
        );
    }
    if input.contains("Bearer ") || input.contains("bearer ") {
        push_regex_candidates(
            input,
            &BEARER_TOKEN_REGEX,
            RedactionKind::BearerToken,
            115,
            &mut candidates,
            probe.as_deref_mut(),
            is_valid_bearer_token,
        );
    }
    if input.contains("access_token")
        || input.contains("access-token")
        || input.contains("AccessToken")
    {
        push_regex_candidates(
            input,
            &ACCESS_TOKEN_REGEX,
            RedactionKind::AccessToken,
            116,
            &mut candidates,
            probe.as_deref_mut(),
            is_valid_named_token,
        );
    }
    if input.contains("secret_key")
        || input.contains("secret-key")
        || input.contains("SecretKey")
        || input.contains("agent_private_key")
        || input.contains("agent-private-key")
        || input.contains("agentPrivateKey")
    {
        push_regex_candidates(
            input,
            &SECRET_KEY_REGEX,
            RedactionKind::SecretKey,
            117,
            &mut candidates,
            probe.as_deref_mut(),
            is_valid_named_token,
        );
    }

    if input_may_contain_high_entropy_token(input) {
        push_regex_candidates(
            input,
            &HIGH_ENTROPY_TOKEN_REGEX,
            RedactionKind::ApiKey,
            200,
            &mut candidates,
            probe,
            is_strict_high_entropy_token,
        );
    }

    candidates.retain(|candidate| !looks_like_existing_sentinel(&candidate.value));
    candidates
}

fn detect_candidates_for_session_config(
    input: &str,
    config: &RedactionSessionConfig,
    probe: Option<&mut DetectorProbe>,
) -> Vec<Candidate> {
    if let Some(rules) = config.rules.as_ref() {
        return detect_candidates_from_compiled_rules(input, rules, probe);
    }
    detect_candidates(input)
        .into_iter()
        .filter(|candidate| candidate.kind.is_some_and(|kind| config.kind_enabled(kind)))
        .collect()
}

fn detect_candidates_from_compiled_rules(
    input: &str,
    rules: &[CompiledRedactionRule],
    mut probe: Option<&mut DetectorProbe>,
) -> Vec<Candidate> {
    let mut candidates = Vec::new();
    for rule in rules {
        if rule.kinds.is_empty() {
            push_compiled_rule_candidates(input, rule, &mut candidates, probe.as_deref_mut());
            continue;
        }
        for kind in &rule.kinds {
            push_compiled_rule_candidate(input, rule, *kind, &mut candidates, probe.as_deref_mut());
        }
    }
    candidates.retain(|candidate| !looks_like_existing_sentinel(&candidate.value));
    candidates
}

fn push_compiled_rule_candidates(
    input: &str,
    rule: &CompiledRedactionRule,
    candidates: &mut Vec<Candidate>,
    probe: Option<&mut DetectorProbe>,
) {
    for matched in rule.regex.find_iter(input) {
        let value = matched.as_str();
        if !has_token_boundary(input, matched.start(), matched.end()) {
            continue;
        }
        if probe.is_some() {
            // Custom rules have no validator call accounting.
            candidates.push(Candidate::new(
                rule.rule_label.clone(),
                None,
                matched.start(),
                matched.end(),
                value,
                rule.custom_priority,
            ));
            continue;
        }
        candidates.push(Candidate::new(
            rule.rule_label.clone(),
            None,
            matched.start(),
            matched.end(),
            value,
            rule.custom_priority,
        ));
    }
}

fn push_compiled_rule_candidate(
    input: &str,
    rule: &CompiledRedactionRule,
    kind: RedactionKind,
    candidates: &mut Vec<Candidate>,
    mut probe: Option<&mut DetectorProbe>,
) {
    for matched in rule.regex.find_iter(input) {
        let value = matched.as_str();
        if !has_token_boundary(input, matched.start(), matched.end()) {
            continue;
        }
        if let Some(probe) = probe.as_deref_mut() {
            probe.record_validator_call(kind);
        }
        if redaction_candidate_is_valid(kind, value) {
            candidates.push(Candidate::new(
                rule.rule_label.clone(),
                Some(kind),
                matched.start(),
                matched.end(),
                value,
                candidate_priority(Some(kind), rule.custom_priority),
            ));
        }
    }
}

fn push_regex_candidates(
    input: &str,
    regex: &Regex,
    kind: RedactionKind,
    priority: u16,
    candidates: &mut Vec<Candidate>,
    mut probe: Option<&mut DetectorProbe>,
    validator: impl Fn(&str) -> bool,
) {
    for matched in regex.find_iter(input) {
        let value = matched.as_str();
        if !has_token_boundary(input, matched.start(), matched.end()) {
            continue;
        }
        if let Some(probe) = probe.as_deref_mut() {
            probe.record_validator_call(kind);
        }
        if validator(value) {
            candidates.push(Candidate::new(
                kind.label(),
                Some(kind),
                matched.start(),
                matched.end(),
                value,
                priority,
            ));
        }
    }
}

fn detect_ipv6_candidates(input: &str) -> Vec<Candidate> {
    let mut candidates = Vec::new();
    let mut start = None;
    for (index, ch) in input.char_indices() {
        if ch.is_ascii_hexdigit() || ch == ':' || ch == '.' {
            start.get_or_insert(index);
            continue;
        }
        if let Some(token_start) = start.take() {
            push_ipv6_candidate(input, token_start, index, &mut candidates);
        }
    }
    if let Some(token_start) = start {
        push_ipv6_candidate(input, token_start, input.len(), &mut candidates);
    }
    candidates
}

fn push_ipv6_candidate(input: &str, start: usize, end: usize, candidates: &mut Vec<Candidate>) {
    let value = &input[start..end];
    if value.contains(':') && value.parse::<Ipv6Addr>().is_ok() {
        candidates.push(Candidate::new(
            RedactionKind::Ipv6.label(),
            Some(RedactionKind::Ipv6),
            start,
            end,
            value,
            65,
        ));
    }
}

fn select_non_overlapping(mut candidates: Vec<Candidate>) -> Vec<Candidate> {
    candidates.sort_by(|left, right| {
        right
            .len()
            .cmp(&left.len())
            .then_with(|| left.priority.cmp(&right.priority))
            .then_with(|| left.start.cmp(&right.start))
    });
    let mut occupied = HashSet::new();
    let mut selected = Vec::new();
    for candidate in candidates {
        if (candidate.start..candidate.end).any(|index| occupied.contains(&index)) {
            continue;
        }
        occupied.extend(candidate.start..candidate.end);
        selected.push(candidate);
    }
    selected.sort_by_key(|candidate| candidate.start);
    selected
}

fn has_token_boundary(input: &str, start: usize, end: usize) -> bool {
    !has_sensitive_token_before(input, start) && !has_sensitive_token_after(input, end)
}

fn is_sensitive_token_char(ch: char) -> bool {
    ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-' | '.' | '@')
}

fn has_sensitive_token_before(input: &str, start: usize) -> bool {
    let Some(before) = input[..start].chars().next_back() else {
        return false;
    };
    if before != '.' {
        return is_sensitive_token_char(before);
    }
    input[..start - before.len_utf8()]
        .chars()
        .next_back()
        .is_some_and(is_sensitive_token_char)
}

fn has_sensitive_token_after(input: &str, end: usize) -> bool {
    let Some(after) = input[end..].chars().next() else {
        return false;
    };
    if after != '.' {
        return is_sensitive_token_char(after);
    }
    input[end + after.len_utf8()..]
        .chars()
        .next()
        .is_some_and(is_sensitive_token_char)
}

fn input_may_contain_high_entropy_token(input: &str) -> bool {
    let mut token_len = 0usize;
    let mut class_mask = 0u8;
    for ch in input.chars() {
        let class = high_entropy_token_char_class(ch);
        if class == 0 {
            if token_len >= 32 && class_mask.count_ones() >= 3 {
                return true;
            }
            token_len = 0;
            class_mask = 0;
            continue;
        }
        token_len += 1;
        class_mask |= class;
    }
    token_len >= 32 && class_mask.count_ones() >= 3
}

fn high_entropy_token_char_class(ch: char) -> u8 {
    match ch {
        'a'..='z' => 0b0001,
        'A'..='Z' => 0b0010,
        '0'..='9' => 0b0100,
        '_' | '-' => 0b1000,
        _ => 0,
    }
}

fn digits_only(value: &str) -> String {
    value.chars().filter(|ch| ch.is_ascii_digit()).collect()
}

fn is_valid_cn_mobile(value: &str) -> bool {
    let mut digits = digits_only(value);
    if digits.len() == 13 && digits.starts_with("86") {
        digits = digits[2..].to_string();
    }
    digits.len() == 11
        && digits.starts_with('1')
        && digits
            .as_bytes()
            .get(1)
            .is_some_and(|digit| matches!(digit, b'3'..=b'9'))
}

fn is_valid_cn_landline(value: &str) -> bool {
    if !value.contains('-') {
        return false;
    }
    let mut digits = digits_only(value);
    if digits.len() >= 12 && digits.starts_with("86") {
        digits = digits[2..].to_string();
    }
    digits.starts_with('0') && (10..=17).contains(&digits.len())
}

fn is_valid_e164_phone(value: &str) -> bool {
    if !value.starts_with('+') {
        return false;
    }
    let digits = digits_only(value);
    (8..=15).contains(&digits.len()) && !digits.starts_with('0')
}

fn is_valid_cn_resident_id(value: &str) -> bool {
    if value.len() != 18 || !value[..17].chars().all(|ch| ch.is_ascii_digit()) {
        return false;
    }
    let birth = &value[6..14];
    let year = birth[0..4].parse::<i32>().ok();
    let month = birth[4..6].parse::<u32>().ok();
    let day = birth[6..8].parse::<u32>().ok();
    if year
        .zip(month)
        .zip(day)
        .and_then(|((year, month), day)| NaiveDate::from_ymd_opt(year, month, day))
        .is_none()
    {
        return false;
    }

    const WEIGHTS: [u32; 17] = [7, 9, 10, 5, 8, 4, 2, 1, 6, 3, 7, 9, 10, 5, 8, 4, 2];
    const CHECK_CODES: [char; 11] = ['1', '0', 'X', '9', '8', '7', '6', '5', '4', '3', '2'];
    let checksum = value[..17]
        .chars()
        .zip(WEIGHTS)
        .map(|(digit, weight)| digit.to_digit(10).unwrap_or_default() * weight)
        .sum::<u32>();
    let expected = CHECK_CODES[(checksum % 11) as usize];
    value
        .chars()
        .nth(17)
        .is_some_and(|actual| actual.to_ascii_uppercase() == expected)
}

fn is_valid_payment_card(value: &str) -> bool {
    let digits = digits_only(value);
    if !(13..=19).contains(&digits.len())
        || digits
            .chars()
            .all(|ch| ch == digits.chars().next().unwrap_or_default())
    {
        return false;
    }
    if !matches!(digits.as_bytes().first(), Some(b'3'..=b'6')) {
        return false;
    }
    luhn_valid(&digits)
}

fn luhn_valid(digits: &str) -> bool {
    let mut sum = 0u32;
    let mut double = false;
    for digit in digits.chars().rev().filter_map(|ch| ch.to_digit(10)) {
        let mut value = digit;
        if double {
            value *= 2;
            if value > 9 {
                value -= 9;
            }
        }
        sum += value;
        double = !double;
    }
    sum.is_multiple_of(10)
}

fn is_valid_bearer_token(value: &str) -> bool {
    value
        .split_once(char::is_whitespace)
        .map(|(_, token)| token.trim().len() >= 20)
        .unwrap_or(false)
}

fn is_valid_named_token(value: &str) -> bool {
    value
        .split_once([':', '='])
        .map(|(_, token)| {
            let token = token.trim().trim_matches(['\'', '"']);
            token.len() >= 20 && token.chars().any(|ch| ch.is_ascii_digit())
        })
        .unwrap_or(false)
}

fn is_valid_jwt_like(value: &str) -> bool {
    let parts = value.split('.').collect::<Vec<_>>();
    parts.len() == 3
        && parts.iter().all(|part| {
            part.len() >= 10
                && part
                    .chars()
                    .all(|ch| ch.is_ascii_alphanumeric() || ch == '_' || ch == '-')
        })
}

fn is_strict_high_entropy_token(value: &str) -> bool {
    if value.len() < 32 || looks_like_existing_sentinel(value) {
        return false;
    }
    let mut classes = 0;
    classes += value.chars().any(|ch| ch.is_ascii_lowercase()) as u8;
    classes += value.chars().any(|ch| ch.is_ascii_uppercase()) as u8;
    classes += value.chars().any(|ch| ch.is_ascii_digit()) as u8;
    classes += value.chars().any(|ch| matches!(ch, '_' | '-')) as u8;
    classes >= 3 && shannon_entropy(value) >= 4.0
}

fn shannon_entropy(value: &str) -> f64 {
    let mut counts = HashMap::<u8, usize>::new();
    for byte in value.bytes() {
        *counts.entry(byte).or_default() += 1;
    }
    let len = value.len() as f64;
    counts
        .values()
        .map(|count| {
            let probability = *count as f64 / len;
            -probability * probability.log2()
        })
        .sum()
}

fn looks_like_existing_sentinel(value: &str) -> bool {
    SENTINEL_REGEX.is_match(value)
}

fn sentinel_matches_rule_label(namespace: &str, rule_label: &str, sentinel: &str) -> bool {
    if !full_sentinel_match(sentinel) {
        return false;
    }
    sentinel
        .strip_prefix('<')
        .and_then(|value| value.strip_suffix('>'))
        .and_then(|value| {
            let mut parts = value.split(':');
            let actual_namespace = parts.next()?;
            let actual_rule = parts.next()?;
            let digest = parts.next()?;
            (parts.next().is_none()).then_some((actual_namespace, actual_rule, digest))
        })
        .is_some_and(|(actual_namespace, actual_rule, _)| {
            actual_namespace == namespace && actual_rule == rule_label
        })
}

fn full_sentinel_match(value: &str) -> bool {
    SENTINEL_REGEX
        .find(value)
        .is_some_and(|matched| matched.start() == 0 && matched.end() == value.len())
}

fn normalize_redaction_value(kind: Option<RedactionKind>, value: &str) -> String {
    match kind {
        Some(RedactionKind::Email) => value.trim().to_ascii_lowercase(),
        Some(RedactionKind::ChinaMobile)
        | Some(RedactionKind::ChinaLandline)
        | Some(RedactionKind::Phone)
        | Some(RedactionKind::PaymentCard) => digits_only(value),
        Some(RedactionKind::AccessToken) | Some(RedactionKind::SecretKey) => value
            .split_once([':', '='])
            .map(|(_, token)| token.trim().trim_matches(['\'', '"']).to_string())
            .unwrap_or_else(|| value.trim().to_string()),
        Some(RedactionKind::BearerToken) => value
            .split_once(char::is_whitespace)
            .map(|(_, token)| token.trim().to_string())
            .unwrap_or_else(|| value.trim().to_string()),
        _ => value.trim().to_string(),
    }
}

fn candidate_priority(kind: Option<RedactionKind>, fallback_priority: u16) -> u16 {
    match kind {
        Some(RedactionKind::Email) => 10,
        Some(RedactionKind::ChinaMobile) => 20,
        Some(RedactionKind::ChinaLandline) => 21,
        Some(RedactionKind::Phone) => 30,
        Some(RedactionKind::ChinaResidentId) => 40,
        Some(RedactionKind::PaymentCard) => 50,
        Some(RedactionKind::Ipv4) => 60,
        Some(RedactionKind::Ipv6) => 65,
        Some(RedactionKind::AnthropicKey) => 70,
        Some(RedactionKind::OpenAiKey) => 80,
        Some(RedactionKind::GitHubToken) => 90,
        Some(RedactionKind::SlackToken) => 100,
        Some(RedactionKind::AwsKey) => 110,
        Some(RedactionKind::BearerToken) => 115,
        Some(RedactionKind::AccessToken) => 116,
        Some(RedactionKind::SecretKey) => 117,
        Some(RedactionKind::Jwt) => 120,
        Some(RedactionKind::ApiKey) => 200,
        None => fallback_priority,
    }
}

fn redaction_candidate_is_valid(kind: RedactionKind, value: &str) -> bool {
    match kind {
        RedactionKind::Email => value.contains('@'),
        RedactionKind::ChinaMobile => is_valid_cn_mobile(value),
        RedactionKind::ChinaLandline => is_valid_cn_landline(value),
        RedactionKind::Phone => is_valid_e164_phone(value),
        RedactionKind::ChinaResidentId => is_valid_cn_resident_id(value),
        RedactionKind::PaymentCard => is_valid_payment_card(value),
        RedactionKind::Ipv4 => value.parse::<Ipv4Addr>().is_ok(),
        RedactionKind::Ipv6 => value.parse::<Ipv6Addr>().is_ok(),
        RedactionKind::OpenAiKey
        | RedactionKind::AnthropicKey
        | RedactionKind::GitHubToken
        | RedactionKind::SlackToken
        | RedactionKind::AwsKey => true,
        RedactionKind::BearerToken => is_valid_bearer_token(value),
        RedactionKind::Jwt => is_valid_jwt_like(value),
        RedactionKind::AccessToken => is_valid_named_token(value),
        RedactionKind::SecretKey => is_valid_named_token(value),
        RedactionKind::ApiKey => is_strict_high_entropy_token(value),
    }
}

fn normalized_value_cache_digest(normalized_value: &str) -> String {
    let digest = sha2::Sha256::digest(normalized_value.as_bytes());
    base32_no_pad(&digest[..HMAC96_BYTES])
}

fn redaction_kinds_for_entity(entity: &str) -> impl Iterator<Item = RedactionKind> {
    match entity.trim().to_ascii_lowercase().as_str() {
        "email" => vec![RedactionKind::Email],
        "cn_phone" => vec![RedactionKind::ChinaMobile, RedactionKind::ChinaLandline],
        "global_phone" => vec![RedactionKind::Phone],
        "cn_id" => vec![RedactionKind::ChinaResidentId],
        "payment_card" => vec![RedactionKind::PaymentCard],
        "ipv4" => vec![RedactionKind::Ipv4],
        "ipv6" => vec![RedactionKind::Ipv6],
        "api_key" => vec![
            RedactionKind::OpenAiKey,
            RedactionKind::AnthropicKey,
            RedactionKind::GitHubToken,
            RedactionKind::SlackToken,
            RedactionKind::AwsKey,
            RedactionKind::ApiKey,
        ],
        "access_token" => vec![RedactionKind::AccessToken],
        "secret_key" => vec![RedactionKind::SecretKey],
        "bearer_token" => vec![RedactionKind::BearerToken],
        "jwt" => vec![RedactionKind::Jwt],
        _ => Vec::new(),
    }
    .into_iter()
}

fn base32_no_pad(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 32] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";
    let mut output = String::with_capacity((bytes.len() * 8).div_ceil(5));
    let mut bit_buffer = 0u32;
    let mut bit_count = 0u8;

    for byte in bytes {
        bit_buffer = (bit_buffer << 8) | u32::from(*byte);
        bit_count += 8;
        while bit_count >= 5 {
            bit_count -= 5;
            let index = ((bit_buffer >> bit_count) & 0x1f) as usize;
            output.push(ALPHABET[index] as char);
        }
    }

    if bit_count > 0 {
        let index = ((bit_buffer << (5 - bit_count)) & 0x1f) as usize;
        output.push(ALPHABET[index] as char);
    }

    output
}

fn redacted_sentinel_debug(sentinel: &str) -> String {
    let (namespace, kind) = sentinel
        .strip_prefix('<')
        .and_then(|rest| rest.split_once(':'))
        .map(|(namespace, rest)| {
            let kind = rest
                .split_once(':')
                .map(|(kind, _)| kind)
                .unwrap_or("UNKNOWN");
            (namespace, kind)
        })
        .unwrap_or((DEFAULT_SENTINEL_NAMESPACE, "UNKNOWN"));
    format!("<{namespace}:{kind}:redacted>")
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
